#include "barrier.h"
#include "main.h"
#include "mlx5_defs.h"

// Install a UDP destination port--based flow rule
void install_flow_rule(struct ibv_qp* qp, uint16_t dst_port) {
  static constexpr size_t rule_sz =
      sizeof(ibv_exp_flow_attr) + sizeof(ibv_exp_flow_spec_eth) +
      sizeof(ibv_exp_flow_spec_ipv4_ext) + sizeof(ibv_exp_flow_spec_tcp_udp);

  uint8_t* flow_rule = new uint8_t[rule_sz];
  memset(flow_rule, 0, rule_sz);
  uint8_t* buf = flow_rule;

  auto* flow_attr = reinterpret_cast<struct ibv_exp_flow_attr*>(flow_rule);
  flow_attr->type = IBV_EXP_FLOW_ATTR_NORMAL;
  flow_attr->size = rule_sz;
  flow_attr->priority = 0;
  flow_attr->num_of_specs = 3;
  flow_attr->port = 1;
  flow_attr->flags = 0;
  flow_attr->reserved = 0;
  buf += sizeof(struct ibv_exp_flow_attr);

  // Ethernet - all wildcard
  auto* eth_spec = reinterpret_cast<struct ibv_exp_flow_spec_eth*>(buf);
  eth_spec->type = IBV_EXP_FLOW_SPEC_ETH;
  eth_spec->size = sizeof(struct ibv_exp_flow_spec_eth);
  buf += sizeof(struct ibv_exp_flow_spec_eth);

  // IPv4 - all wildcard
  auto* spec_ipv4 = reinterpret_cast<struct ibv_exp_flow_spec_ipv4_ext*>(buf);
  spec_ipv4->type = IBV_EXP_FLOW_SPEC_IPV4_EXT;
  spec_ipv4->size = sizeof(struct ibv_exp_flow_spec_ipv4_ext);
  buf += sizeof(struct ibv_exp_flow_spec_ipv4_ext);

  // UDP - match dst port
  auto* udp_spec = reinterpret_cast<struct ibv_exp_flow_spec_tcp_udp*>(buf);
  udp_spec->type = IBV_EXP_FLOW_SPEC_UDP;
  udp_spec->size = sizeof(struct ibv_exp_flow_spec_tcp_udp);
  udp_spec->val.dst_port = htons(dst_port);
  udp_spec->mask.dst_port = 0xffffu;

  rt_assert(ibv_exp_create_flow(qp, flow_attr) != nullptr);
}

void snapshot_cqe(volatile mlx5_cqe64* cqe, cqe_snapshot_t& cqe_snapshot) {
  while (true) {
    uint16_t wqe_id_0 = cqe->wqe_id;
    uint16_t wqe_counter_0 = cqe->wqe_counter;
    memory_barrier();
    uint16_t wqe_id_1 = cqe->wqe_id;

    if (likely(wqe_id_0 == wqe_id_1)) {
      cqe_snapshot.wqe_id = ntohs(wqe_id_0);
      cqe_snapshot.wqe_counter = ntohs(wqe_counter_0);
      return;
    }
  }
}

size_t get_num_comps(const cqe_snapshot_t& prev, const cqe_snapshot_t& cur) {
  size_t prev_idx = prev.get_cqe_snapshot_cycle_idx();
  size_t cur_idx = cur.get_cqe_snapshot_cycle_idx();
  assert(prev_idx < kCQESnapshotCycle && cur_idx < kCQESnapshotCycle);

  return ((cur_idx + kCQESnapshotCycle) - prev_idx) % kCQESnapshotCycle;
}

void run_server(thread_params_t params) {
  size_t thread_id = params.id;
  ctrl_blk_t* cb = init_ctx(kAppDeviceIndex);

  uint16_t dst_port = static_cast<uint16_t>(kBaseDstPort + thread_id);
  printf("Thread %zu listening on port %u\n", thread_id, dst_port);
  install_flow_rule(cb->recv_qp, dst_port);

  // Register RX ring memory
  uint8_t* ring = new uint8_t[kAppRingSize];
  memset(ring, 0, kAppRingSize);

  struct ibv_mr* mr =
      ibv_reg_mr(cb->pd, ring, kAppRingSize, IBV_ACCESS_LOCAL_WRITE);
  assert(mr != nullptr);

  // This cast works for mlx5 where ibv_cq is the first member of mlx5_cq.
  auto* _mlx5_cq = reinterpret_cast<mlx5_cq*>(cb->recv_cq);
  auto* cqe_0 = reinterpret_cast<volatile mlx5_cqe64*>(_mlx5_cq->buf_a.buf);

  // Initialize the CQE to the last CQE in the snapshot cycle
  cqe_0->wqe_id = htons(std::numeric_limits<uint16_t>::max());
  cqe_0->wqe_counter = htons(kAppStridesPerWQE - 1);
  cqe_snapshot_t prev_snapshot;
  snapshot_cqe(cqe_0, prev_snapshot);
  assert(prev_snapshot.get_cqe_snapshot_cycle_idx() == kCQESnapshotCycle - 1);

  // The multi-packet RECVs. This must be done after we've initialized the CQE.
  struct ibv_sge sge[kAppRQDepth];
  for (size_t i = 0; i < kAppRQDepth; i++) {
    size_t mpwqe_offset = i * (kAppRingMbufSize * kAppStridesPerWQE);
    sge[i].addr = reinterpret_cast<uint64_t>(&ring[mpwqe_offset]);
    sge[i].lkey = mr->lkey;
    sge[i].length = kAppRingSize;
    cb->wq_family->recv_burst(cb->wq, &sge[i], 1);
  }

  printf("Thread %zu: Listening\n", thread_id);
  size_t ring_head = 0, nb_rx = 0, nb_rx_rolling = 0, sge_idx = 0;
  struct timespec start, end;

  clock_gettime(CLOCK_REALTIME, &start);
  while (true) {
    cqe_snapshot_t cur_snapshot;
    snapshot_cqe(cqe_0, cur_snapshot);

    const size_t num_comps = get_num_comps(prev_snapshot, cur_snapshot);
    if (num_comps == 0) continue;

    const size_t orig_ring_head = ring_head;
    for (size_t i = 0; i < num_comps; i++) {
      uint8_t* buf = &ring[ring_head * kAppRingMbufSize];
      auto* data_hdr = reinterpret_cast<data_hdr_t*>(buf + kTotHdrSz);

      if (unlikely(data_hdr->seq_num == 0)) {
        printf(
            "Thread %zu: prev snapshot = %s, cur = %s, num_comps = %zu "
            "orig_ring_head = %zu, but buf %zu is empty.\n",
            thread_id, prev_snapshot.to_string().c_str(),
            cur_snapshot.to_string().c_str(), num_comps, orig_ring_head,
            ring_head);
        throw std::runtime_error("CQE logic error");
      }

      if (kAppVerbose) {
        printf(
            "Thread %zu: Buf %zu filled. Seq = %zu, nb_rx = %zu. "
            "cur_snapshot = %s\n",
            thread_id, ring_head, data_hdr->seq_num, nb_rx,
            cur_snapshot.to_string().c_str());
        // usleep(200);
      }

      assert(data_hdr->server_thread == thread_id);
      data_hdr->seq_num = 0;

      ring_head = (ring_head + 1) % kAppNumRingEntries;

      nb_rx_rolling++;
      if (nb_rx_rolling == kAppStridesPerWQE) {
        nb_rx_rolling = 0;
        if (kAppVerbose) {
          printf("Thread %zu: Posting MPWQE %zu.\n", thread_id, sge_idx);
        }
        cb->wq_family->recv_burst(cb->wq, &sge[sge_idx], 1);
        sge_idx = (sge_idx + 1) % kAppRQDepth;
      }
    }

    prev_snapshot = cur_snapshot;
    nb_rx += num_comps;
    if (nb_rx >= 1000000) {
      clock_gettime(CLOCK_REALTIME, &end);
      double sec = (end.tv_sec - start.tv_sec) +
                   (end.tv_nsec - start.tv_nsec) / 1000000000.0;
      printf("Thread %zu: RX tput = %.2f/s\n", thread_id, nb_rx / sec);

      clock_gettime(CLOCK_REALTIME, &start);
      nb_rx = 0;
    }
  }
}
