#include "main.h"

static constexpr bool kIgnoreOverrun = true;

void ring_polling_server(thread_params_t* params, hrd_ctrl_blk_t* cb) {
  rt_assert(kIgnoreOverrun, "Ring polling server requires ignoring overruns");

  size_t srv_gid = params->id;               // Global ID of this server thread
  auto* resp_buf = new uint8_t[FLAGS_size];  // Garbage buffer for responses

  struct ibv_ah* ah[kHrdMaxLID] = {nullptr};  // Initialized on demand
  req_hdr_t req_hdr_arr[kAppMaxPostlist];     // The polled request headers
  struct ibv_sge sgl[kAppMaxPostlist];

  size_t rolling_iter = 0;  // For throughput measurement
  size_t nb_tx[kAppNumQPs] = {0}, nb_tx_tot = 0, nb_post_send = 0;
  size_t ring_head = 0, expected_stamp = 1;  // RX ring head

  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  size_t qp_i = 0;
  while (true) {
    if (rolling_iter >= MB(1)) {
      clock_gettime(CLOCK_REALTIME, &end);
      params->tput[srv_gid] = rolling_iter / hrd_get_seconds(start, end);
      printf("main: Server %zu: %.2f Mops. Average postlist = %.2f\n", srv_gid,
             params->tput[srv_gid], nb_tx_tot * 1.0 / nb_post_send);

      if (srv_gid == 0) {
        double tot = 0;
        for (size_t i = 0; i < FLAGS_num_server_threads; i++) {
          tot += params->tput[i];
        }
        hrd_red_printf("main: Total = %.2f\n", tot);
      }

      rolling_iter = 0;
      clock_gettime(CLOCK_REALTIME, &start);
    }

    // Poll for requests. This must not modify ring_head.
    size_t num_comps = 0;
    while (true) {
      size_t rx_offset =
          ((ring_head + num_comps) % kHrdRQDepth) * recv_mbuf_sz();
      auto* req_hdr = reinterpret_cast<volatile req_hdr_t*>(
          &cb->dgram_buf[rx_offset + sizeof(struct ibv_grh)]);

      if (req_hdr->stamp == 0) break;

      // This ring buffer contains a valid request
      assert(req_hdr->stamp == expected_stamp);

      // Save info for response
      req_hdr_arr[num_comps].slid = req_hdr->slid;
      req_hdr_arr[num_comps].qpn = req_hdr->qpn;
      req_hdr->stamp = 0;

      expected_stamp++;
      num_comps++;
      if (num_comps == 0) break;
    }

    assert(num_comps <= FLAGS_postlist);
    if (num_comps == 0) continue;

    // Post a batch of RECVs replenishing used RECVs
    struct ibv_recv_wr recv_wr[kAppMaxPostlist], *bad_recv_wr;
    for (size_t w_i = 0; w_i < num_comps; w_i++) {
      sgl[w_i].length = recv_mbuf_sz();
      sgl[w_i].lkey = cb->dgram_buf_mr->lkey;
      sgl[w_i].addr = reinterpret_cast<uint64_t>(
          &cb->dgram_buf[ring_head * recv_mbuf_sz()]);

      recv_wr[w_i].sg_list = &sgl[w_i];
      recv_wr[w_i].num_sge = 1;
      recv_wr[w_i].next = (w_i == num_comps - 1) ? nullptr : &recv_wr[w_i + 1];

      ring_head = (ring_head + 1) % kHrdRQDepth;
    }

    int ret = ibv_post_recv(cb->dgram_qp[0], &recv_wr[0], &bad_recv_wr);
    rt_assert(ret == 0, "ibv_post_recv() error");

    // Send responses
    struct ibv_send_wr wr[kAppMaxPostlist], *bad_send_wr;
    for (size_t w_i = 0; w_i < num_comps; w_i++) {
      uint32_t s_lid = req_hdr_arr[w_i].slid;  // Src LID for this request
      if (ah[s_lid] == nullptr) ah[s_lid] = hrd_create_ah(cb, s_lid);

      wr[w_i].wr.ud.ah = ah[s_lid];
      wr[w_i].wr.ud.remote_qpn = req_hdr_arr[w_i].qpn;
      wr[w_i].wr.ud.remote_qkey = kHrdDefaultQKey;

      wr[w_i].opcode = IBV_WR_SEND;
      wr[w_i].num_sge = 1;
      wr[w_i].next = (w_i == num_comps - 1) ? nullptr : &wr[w_i + 1];
      wr[w_i].sg_list = &sgl[w_i];

      wr[w_i].send_flags =
          (nb_tx[qp_i] % kAppUnsigBatch == 0) ? IBV_SEND_SIGNALED : 0;
      if (nb_tx[qp_i] % kAppUnsigBatch == kAppUnsigBatch - 1) {
        struct ibv_wc signal_wc;
        hrd_poll_cq(cb->dgram_send_cq[qp_i], 1, &signal_wc);
      }

      wr[w_i].send_flags |= IBV_SEND_INLINE;

      sgl[w_i].addr = reinterpret_cast<uint64_t>(resp_buf);
      sgl[w_i].length = FLAGS_size;

      nb_tx[qp_i]++;
      nb_tx_tot++;
      rolling_iter++;
    }

    nb_post_send++;
    ret = ibv_post_send(cb->dgram_qp[qp_i], &wr[0], &bad_send_wr);
    rt_assert(ret == 0, "ibv_post_send error");
    qp_i = (qp_i + 1) % kAppNumQPs;
  }
}

void cq_polling_server(thread_params_t* params, hrd_ctrl_blk_t* cb) {
  size_t srv_gid = params->id;               // Global ID of this server thread
  auto* resp_buf = new uint8_t[FLAGS_size];  // Garbage buffer for responses

  struct ibv_ah* ah[kHrdMaxLID] = {nullptr};  // Initialized on demand
  struct ibv_wc wc[kAppMaxPostlist];
  struct ibv_sge sgl[kAppMaxPostlist];

  size_t rolling_iter = 0;  // For throughput measurement
  size_t nb_tx[kAppNumQPs] = {0}, nb_tx_tot = 0, nb_post_send = 0;
  size_t ring_head = 0, expected_stamp = 1;  // RX ring index

  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  size_t qp_i = 0;
  while (true) {
    if (rolling_iter >= MB(1)) {
      clock_gettime(CLOCK_REALTIME, &end);
      params->tput[srv_gid] = rolling_iter / hrd_get_seconds(start, end);
      printf("main: Server %zu: %.2f Mops. Average postlist = %.2f\n", srv_gid,
             params->tput[srv_gid], nb_tx_tot * 1.0 / nb_post_send);

      if (srv_gid == 0) {
        double tot = 0;
        for (size_t i = 0; i < FLAGS_num_server_threads; i++) {
          tot += params->tput[i];
        }
        hrd_red_printf("main: Total = %.2f\n", tot);
      }

      rolling_iter = 0;
      clock_gettime(CLOCK_REALTIME, &start);
    }

    size_t num_comps = 0;
    int ret = ibv_poll_cq(cb->dgram_recv_cq[0], FLAGS_postlist, wc);
    assert(ret >= 0);
    num_comps = static_cast<size_t>(ret);
    if (num_comps == 0) continue;

    // Post a batch of RECVs
    struct ibv_recv_wr recv_wr[kAppMaxPostlist], *bad_recv_wr;
    for (size_t w_i = 0; w_i < num_comps; w_i++) {
      sgl[w_i].length = recv_mbuf_sz();
      sgl[w_i].lkey = cb->dgram_buf_mr->lkey;
      sgl[w_i].addr = reinterpret_cast<uint64_t>(
          &cb->dgram_buf[ring_head * recv_mbuf_sz()]);

      // Check the stamp
      auto* req_hdr =
          reinterpret_cast<req_hdr_t*>(sgl[w_i].addr + sizeof(struct ibv_grh));
      _unused(req_hdr);
      assert(req_hdr->stamp == expected_stamp);
      assert(req_hdr->slid == wc[w_i].slid);
      assert(req_hdr->qpn == wc[w_i].src_qp);

      expected_stamp++;
      ring_head = (ring_head + 1) % kHrdRQDepth;  // Shift

      recv_wr[w_i].sg_list = &sgl[w_i];
      recv_wr[w_i].num_sge = 1;
      recv_wr[w_i].next = (w_i == num_comps - 1) ? nullptr : &recv_wr[w_i + 1];
    }

    ret = ibv_post_recv(cb->dgram_qp[0], &recv_wr[0], &bad_recv_wr);
    rt_assert(ret == 0, "ibv_post_recv() error");

    // Send responses
    struct ibv_send_wr wr[kAppMaxPostlist], *bad_send_wr;
    for (size_t w_i = 0; w_i < num_comps; w_i++) {
      int s_lid = wc[w_i].slid;  // Src LID for this request
      if (ah[s_lid] == nullptr) ah[s_lid] = hrd_create_ah(cb, s_lid);

      wr[w_i].wr.ud.ah = ah[s_lid];
      wr[w_i].wr.ud.remote_qpn = wc[w_i].src_qp;
      wr[w_i].wr.ud.remote_qkey = kHrdDefaultQKey;

      wr[w_i].opcode = IBV_WR_SEND;
      wr[w_i].num_sge = 1;
      wr[w_i].next = (w_i == num_comps - 1) ? nullptr : &wr[w_i + 1];
      wr[w_i].sg_list = &sgl[w_i];

      wr[w_i].send_flags =
          (nb_tx[qp_i] % kAppUnsigBatch == 0) ? IBV_SEND_SIGNALED : 0;
      if (nb_tx[qp_i] % kAppUnsigBatch == kAppUnsigBatch - 1) {
        struct ibv_wc signal_wc;
        hrd_poll_cq(cb->dgram_send_cq[qp_i], 1, &signal_wc);
      }

      wr[w_i].send_flags |= IBV_SEND_INLINE;

      sgl[w_i].addr = reinterpret_cast<uint64_t>(resp_buf);
      sgl[w_i].length = FLAGS_size;

      nb_tx[qp_i]++;
      nb_tx_tot++;
      rolling_iter++;
    }

    nb_post_send++;
    ret = ibv_post_send(cb->dgram_qp[qp_i], &wr[0], &bad_send_wr);
    rt_assert(ret == 0, "ibv_post_send error");
    qp_i = (qp_i + 1) % kAppNumQPs;
  }
}

void run_server(thread_params_t* params) {
  size_t srv_gid = params->id;  // Global ID of this server thread
  size_t ib_port_index = FLAGS_dual_port == 0 ? 0 : srv_gid % 2;

  hrd_dgram_config_t dgram_config;
  dgram_config.num_qps = kAppNumQPs;
  dgram_config.buf_size = recv_mbuf_sz() * kHrdRQDepth;
  dgram_config.buf_shm_key = -1;
  dgram_config.ignore_overrun = kIgnoreOverrun;

  auto* cb = hrd_ctrl_blk_init(srv_gid, ib_port_index, kHrdInvalidNUMANode,
                               &dgram_config);

  // Ring buffers to receive requests into
  memset(const_cast<uint8_t*>(cb->dgram_buf), 0, dgram_config.buf_size);

  char srv_name[kHrdQPNameSize];
  sprintf(srv_name, "server-%zu", srv_gid);

  // We post RECVs only on the 1st QP.
  for (size_t i = 0; i < kHrdRQDepth; i++) {
    hrd_post_dgram_recv(
        cb->dgram_qp[0],
        const_cast<uint8_t*>(&cb->dgram_buf[recv_mbuf_sz() * i]),
        recv_mbuf_sz(), cb->dgram_buf_mr->lkey);
  }

  hrd_publish_dgram_qp(cb, 0, srv_name);
  printf("server: Server %s published. Now polling..\n", srv_name);

  if (kIgnoreOverrun) {
    ring_polling_server(params, cb);
  } else {
    cq_polling_server(params, cb);
  }
}
