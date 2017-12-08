#include "main.h"

void run_client(thread_params_t* params) {
  // The local HID of a control block should be <= 64 to keep the SHM key low.
  // But the number of clients over all machines can be larger.
  size_t clt_gid = params->id;  // Global ID of this client thread
  size_t clt_local_hid = clt_gid % FLAGS_num_client_threads;
  size_t srv_gid = clt_gid % FLAGS_num_server_threads;
  size_t ib_port_index = FLAGS_dual_port == 0 ? 0 : srv_gid % 2;

  hrd_dgram_config_t dgram_config;
  dgram_config.num_qps = 1;
  dgram_config.buf_size = recv_mbuf_sz();
  dgram_config.buf_shm_key = -1;
  dgram_config.ignore_overrun = false;

  auto* cb = hrd_ctrl_blk_init(clt_local_hid, ib_port_index,
                               kHrdInvalidNUMANode, &dgram_config);

  // One buffer to receive all responses into
  memset(const_cast<uint8_t*>(cb->dgram_buf), 0, dgram_config.buf_size);

  // Buffers to send requests from
  auto** req_buf_arr = new uint8_t*[FLAGS_postlist];
  for (size_t i = 0; i < FLAGS_postlist; i++) {
    req_buf_arr[i] = new uint8_t[FLAGS_size];
  }

  char srv_name[kHrdQPNameSize];
  sprintf(srv_name, "server-%zu", srv_gid);
  char clt_name[kHrdQPNameSize];
  sprintf(clt_name, "client-%zu", clt_gid);

  hrd_publish_dgram_qp(cb, 0, clt_name);
  printf("main: Client %s published. Waiting for server %s\n", clt_name,
         srv_name);

  struct hrd_qp_attr_t* srv_qp = nullptr;
  while (srv_qp == nullptr) {
    srv_qp = hrd_get_published_qp(srv_name);
    if (srv_qp == nullptr) usleep(200000);
  }

  printf("main: Client %s found server! Now posting SENDs.\n", clt_name);

  struct ibv_ah* ah = hrd_create_ah(cb, srv_qp->lid);
  assert(ah != nullptr);

  struct ibv_send_wr wr[kAppMaxPostlist], *bad_send_wr;
  struct ibv_wc wc[kAppMaxPostlist];
  struct ibv_sge sgl[kAppMaxPostlist];
  size_t rolling_iter = 0;  // For throughput measurement
  size_t pkt_stamp = 1;     // For stamping requests

  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  while (true) {
    if (rolling_iter >= KB(512)) {
      clock_gettime(CLOCK_REALTIME, &end);
      double seconds = (end.tv_sec - start.tv_sec) +
                       (end.tv_nsec - start.tv_nsec) / 1000000000.0;
      printf("main: Client %zu: %.2f Mops\n", clt_gid, rolling_iter / seconds);
      rolling_iter = 0;

      clock_gettime(CLOCK_REALTIME, &start);
    }

    for (size_t w_i = 0; w_i < FLAGS_postlist; w_i++) {
      hrd_post_dgram_recv(cb->dgram_qp[0], const_cast<uint8_t*>(cb->dgram_buf),
                          recv_mbuf_sz(), cb->dgram_buf_mr->lkey);

      wr[w_i].wr.ud.ah = ah;
      wr[w_i].wr.ud.remote_qpn = srv_qp->qpn;
      wr[w_i].wr.ud.remote_qkey = kHrdDefaultQKey;

      wr[w_i].opcode = IBV_WR_SEND;
      wr[w_i].num_sge = 1;
      wr[w_i].next = (w_i == FLAGS_postlist - 1) ? nullptr : &wr[w_i + 1];
      wr[w_i].sg_list = &sgl[w_i];

      wr[w_i].send_flags = (w_i == 0) ? IBV_SEND_SIGNALED : 0;
      wr[w_i].send_flags |= IBV_SEND_INLINE;

      // Format the request
      auto* req_hdr = reinterpret_cast<req_hdr_t*>(req_buf_arr[w_i]);
      req_hdr->stamp = pkt_stamp;
      req_hdr->slid = cb->resolve.port_lid;
      req_hdr->qpn = cb->dgram_qp[0]->qp_num;

      sgl[w_i].addr = reinterpret_cast<uint64_t>(req_hdr);
      sgl[w_i].length = FLAGS_size;

      pkt_stamp++;
      rolling_iter++;
    }

    int ret = ibv_post_send(cb->dgram_qp[0], &wr[0], &bad_send_wr);
    rt_assert(ret == 0, "ibv_post_send() error");

    hrd_poll_cq(cb->dgram_send_cq[0], 1, wc);  // Poll SEND (w_i = 0)
    hrd_poll_cq(cb->dgram_recv_cq[0], FLAGS_postlist, wc);  // Receive all resps
  }
}
