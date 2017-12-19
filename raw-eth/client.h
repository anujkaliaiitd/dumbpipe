#include "main.h"
#include "rand.h"

void format_packet(uint8_t* buf) {
  gen_eth_header(reinterpret_cast<eth_hdr_t*>(buf), kAppSrcMAC, kAppDstMAC);

  buf += sizeof(eth_hdr_t);
  uint32_t src_ip = ip_from_str(kAppSrcIP);
  uint32_t dst_ip = ip_from_str(kAppDstIP);
  gen_ipv4_header(reinterpret_cast<ipv4_hdr_t*>(buf), src_ip, dst_ip,
                  FLAGS_size);

  buf += sizeof(ipv4_hdr_t);
  uint16_t src_port = 0xffffu;  // It doesn't matter what your source port is!
  gen_udp_header(reinterpret_cast<udp_hdr_t*>(buf), src_port, kBaseDstPort,
                 FLAGS_size);
}

void run_client(thread_params_t params) {
  ctrl_blk_t* cb = init_ctx(kAppDeviceIndex);
  FastRand fast_rand;
  const size_t pkt_size = kTotHdrSz + FLAGS_size;
  rt_assert(pkt_size >= 60);

  struct ibv_send_wr wr[kAppMaxPostlist], *bad_send_wr;
  struct ibv_sge sgl[kAppMaxPostlist];
  size_t rolling_iter = 0;  // For throughput measurement
  size_t nb_tx[kAppNumSQ] = {0}, qp_i = 0;

  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  uint8_t* pkt_arr[kAppMaxPostlist];  // Leaked
  for (size_t i = 0; i < FLAGS_postlist; i++) {
    pkt_arr[i] = new uint8_t[pkt_size];
    memset(pkt_arr[i], 0, pkt_size);
    format_packet(pkt_arr[i]);
  }

  std::vector<size_t> seq_num(FLAGS_num_server_threads);
  for (auto& s : seq_num) s = 1;

  while (true) {
    if (rolling_iter >= MB(2)) {
      clock_gettime(CLOCK_REALTIME, &end);
      double seconds = (end.tv_sec - start.tv_sec) +
                       (end.tv_nsec - start.tv_nsec) / 1000000000.0;
      printf("Thread %zu: %.2f M/s. pkt_size = %zu.\n", params.id,
             rolling_iter / (seconds * 1000000), pkt_size);
      rolling_iter = 0;

      clock_gettime(CLOCK_REALTIME, &start);
    }

    // Reusing pkt_arr buffers across signals is OK because the payloads are
    // inlined
    for (size_t w_i = 0; w_i < FLAGS_postlist; w_i++) {
      wr[w_i].opcode = IBV_WR_SEND;
      wr[w_i].num_sge = 1;
      wr[w_i].next = (w_i == FLAGS_postlist - 1) ? nullptr : &wr[w_i + 1];
      wr[w_i].sg_list = &sgl[w_i];

      wr[w_i].send_flags = IBV_SEND_INLINE;
      wr[w_i].send_flags |=
          nb_tx[qp_i] % kAppUnsigBatch == 0 ? IBV_SEND_SIGNALED : 0;
      if (nb_tx[qp_i] % kAppUnsigBatch == kAppUnsigBatch - 1) {
        struct ibv_wc signal_wc;
        hrd_poll_cq(cb->send_cq[qp_i], 1, &signal_wc);
      }

      // Direct the packet to one of the receiver threads
      auto* udp_hdr = reinterpret_cast<udp_hdr_t*>(
          pkt_arr[w_i] + sizeof(eth_hdr_t) + sizeof(ipv4_hdr_t));
      size_t srv_thread_idx = fast_rand.next_u32() % FLAGS_num_server_threads;
      udp_hdr->dst_port = htons(kBaseDstPort + srv_thread_idx);

      // Format user info
      auto* data_hdr = reinterpret_cast<data_hdr_t*>(pkt_arr[w_i] + kTotHdrSz);
      data_hdr->server_thread = srv_thread_idx;
      data_hdr->seq_num = seq_num[srv_thread_idx]++;

      if (kAppCheckContents) {
        auto* buf = reinterpret_cast<uint8_t*>(&data_hdr[1]);
        for (size_t i = 0; i < FLAGS_size - sizeof(data_hdr_t); i++) {
          buf[i] = static_cast<uint8_t>(data_hdr->seq_num);
        }
      }

      sgl[w_i].addr = reinterpret_cast<uint64_t>(pkt_arr[w_i]);
      sgl[w_i].length = pkt_size;
      sgl[w_i].lkey = 0;

      rolling_iter++;
      nb_tx[qp_i]++;
    }

    int ret = ibv_post_send(cb->send_qp[qp_i], &wr[0], &bad_send_wr);
    qp_i = (qp_i + 1) % kAppNumSQ;

    rt_assert(ret == 0);
  }
}
