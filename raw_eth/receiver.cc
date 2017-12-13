#include "common.h"

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

  printf("Create flow for QP %p, dst port %u\n", qp, dst_port);
  auto* flow = ibv_exp_create_flow(qp, flow_attr);
  assert(flow != nullptr);
}

void thread_func(size_t thread_id) {
  ctrl_blk_t* cb = init_ctx_mp_rq(kDeviceIndex);

  uint16_t dst_port = static_cast<uint16_t>(kBaseDstPort + thread_id);
  printf("Thread %zu listening on port %u\n", thread_id, dst_port);
  install_flow_rule(cb->qp, dst_port);

  // Register RX ring memory
  size_t ring_size = kRecvBufSize * kRQDepth;
  uint8_t* ring = new uint8_t[ring_size];
  memset(ring, 0, ring_size);

  struct ibv_mr* mr =
      ibv_reg_mr(cb->pd, ring, ring_size, IBV_ACCESS_LOCAL_WRITE);
  assert(mr != nullptr);

  struct ibv_sge sge;
  sge.addr = reinterpret_cast<uint64_t>(ring);
  sge.lkey = mr->lkey;
  sge.length = kNumStrides * kStrideBytes;

  assert(ring_size >= sge.length);
  static_assert(kStrideBytes > (kTotHdrSz + kDataSize + 4), "");

  cb->wq_family->recv_burst(cb->wq, &sge, 1);

  printf("Thread %zu: Listening\n", thread_id);
  while (true) {
    for (size_t i = 0; i < kRQDepth; i++) {
      uint8_t* buf = &ring[i * kStrideBytes];
      auto* data_hdr = reinterpret_cast<data_hdr_t*>(buf + kTotHdrSz);
      if (data_hdr->seq_num > 0) {
        printf("Buffer %zu is filled. Seq num = %zu\n", i, data_hdr->seq_num);
      }
    }
    usleep(200);
  }
}

int main() {
  std::thread thread_arr[kReceiverThreads];
  for (size_t i = 0; i < kReceiverThreads; i++) {
    thread_arr[i] = std::thread(thread_func, i);
  }

  for (auto& t : thread_arr) t.join();

  printf("We are done\n");
  return 0;
}
