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

  // The multi-packet RECV
  struct ibv_sge sge;
  sge.addr = reinterpret_cast<uint64_t>(ring);
  sge.lkey = mr->lkey;
  sge.length = kAppRingSize;
  cb->wq_family->recv_burst(cb->wq, &sge, 1);

  printf("Thread %zu: Listening\n", thread_id);
  size_t ring_head = 0, nb_rx = 0;
  struct timespec start, end;

  // This cast works for mlx5 where ibv_cq is the first member of mlx5_cq.
  auto* _mlx5_cq = reinterpret_cast<mlx5_cq*>(cb->recv_cq);
  auto* cqe_arr = reinterpret_cast<volatile mlx5_cqe64*>(_mlx5_cq->buf_a.buf);

  clock_gettime(CLOCK_REALTIME, &start);
  while (true) {
    uint8_t* buf = &ring[ring_head * kAppRingMbufSize];
    auto* data_hdr = reinterpret_cast<data_hdr_t*>(buf + kTotHdrSz);
    if (data_hdr->seq_num > 0) {
      if (kAppVerbose) {
        printf(
            "Thread %zu: Buf %zu filled. Seq = %zu, nb_rx = %zu. "
            " ctr_0 = %u, ctr_1 = %u\n",
            thread_id, ring_head, data_hdr->seq_num, nb_rx,
            ntohs(cqe_arr[0].wqe_counter), ntohs(cqe_arr[1].wqe_counter));
        usleep(200);
      }

      assert(data_hdr->server_thread == thread_id);
      data_hdr->seq_num = 0;
      ring_head++;
      nb_rx++;
    }

    if (nb_rx == 1000000) {
      clock_gettime(CLOCK_REALTIME, &end);
      double sec = (end.tv_sec - start.tv_sec) +
                   (end.tv_nsec - start.tv_nsec) / 1000000000.0;
      printf("Thread %zu: RX tput = %.2f/s\n", thread_id, nb_rx / sec);

      clock_gettime(CLOCK_REALTIME, &start);
      nb_rx = 0;
    }

    if (ring_head == kAppNumRingEntries) {
      ring_head = 0;
      if (kAppVerbose) printf("Thread %zu: Posting MP RECV.\n", thread_id);
      cb->wq_family->recv_burst(cb->wq, &sge, 1);
    }
  }
}
