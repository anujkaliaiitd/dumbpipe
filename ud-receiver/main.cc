#include <gflags/gflags.h>
#include <stdlib.h>
#include <string.h>
#include <limits>
#include <thread>
#include "libhrd_cpp/hrd.h"

static constexpr size_t kAppNumQPs = 1;  // UD QPs used by server for RECVs
static constexpr size_t kAppMaxPostlist = 64;
static constexpr size_t kAppMaxSize = 60;  // Max transfer size
static constexpr size_t kAppUnsigBatch = 64;
static constexpr bool kIgnoreOverrun = false;  // Ignore RQ/CQ overruns

// pkt_arr reuse at clients relies on inlining
static_assert(kAppMaxSize <= kHrdMaxInline, "");
static_assert(is_power_of_two(kAppUnsigBatch), "");

struct thread_params_t {
  size_t id;
  double* tput;

  thread_params_t(size_t id, double* tput) : id(id), tput(tput) {}
};

DEFINE_uint64(machine_id, std::numeric_limits<size_t>::max(), "Machine ID");
DEFINE_uint64(num_client_threads, 0, "Number of threads per client machine");
DEFINE_uint64(num_server_threads, 0, "Number of threads at server machine");
DEFINE_uint64(is_client, 0, "Is this process a client?");
DEFINE_uint64(dual_port, 0, "Use two ports?");
DEFINE_uint64(size, 0, "RDMA size");
DEFINE_uint64(postlist, std::numeric_limits<size_t>::max(), "Postlist size");

void run_server(thread_params_t params) {
  size_t srv_gid = params.id;  // Global ID of this server thread
  size_t ib_port_index = FLAGS_dual_port == 0 ? 0 : srv_gid % 2;

  hrd_dgram_config_t dgram_config;
  dgram_config.num_qps = kAppNumQPs;
  dgram_config.buf_size = sizeof(struct ibv_grh) + FLAGS_size;
  dgram_config.buf_shm_key = -1;
  dgram_config.ignore_overrun = kIgnoreOverrun;

  auto* cb = hrd_ctrl_blk_init(srv_gid, ib_port_index, kHrdInvalidNUMANode,
                               &dgram_config);
  assert(cb != nullptr);

  // Buffer to receive packets into. Set to zero.
  memset(const_cast<uint8_t*>(cb->dgram_buf), 0, dgram_config.buf_size);

  for (size_t qp_i = 0; qp_i < kAppNumQPs; qp_i++) {
    // Fill this QP with recvs before publishing it to clients
    for (size_t i = 0; i < kHrdRQDepth; i++) {
      hrd_post_dgram_recv(
          cb->dgram_qp[qp_i], const_cast<uint8_t*>(&cb->dgram_buf[0]),
          FLAGS_size + sizeof(struct ibv_grh), cb->dgram_buf_mr->lkey);
    }

    char srv_name[kHrdQPNameSize];
    sprintf(srv_name, "server-%zu-%zu", srv_gid, qp_i);
    hrd_publish_dgram_qp(cb, qp_i, srv_name);
  }

  printf("main: Server %zu published QPs. Now polling.\n", srv_gid);

  size_t rolling_iter = 0, qp_i = 0;
  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  while (true) {
    if (rolling_iter >= MB(8)) {
      clock_gettime(CLOCK_REALTIME, &end);
      double sec = (end.tv_sec - start.tv_sec) +
                   (end.tv_nsec - start.tv_nsec) / 1000000000.0;
      printf("main: Server %zu: %.2f MOPS. \n", srv_gid,
             rolling_iter / (sec * 1000000));

      params.tput[srv_gid] = rolling_iter / (sec * 1000000);
      if (srv_gid == 0) {
        double tot = 0;
        for (size_t i = 0; i < FLAGS_num_server_threads; i++) {
          tot += params.tput[i];
        }
        hrd_red_printf("main: Total tput %.2f MOPS.\n", tot);
      }

      rolling_iter = 0;
      clock_gettime(CLOCK_REALTIME, &start);
    }

    size_t num_comps = 0;
    if (kIgnoreOverrun) {
      // Detect completion by polling on the RX buffer
      if (cb->dgram_buf[sizeof(struct ibv_grh)] != 0) {
        num_comps++;
        cb->dgram_buf[sizeof(struct ibv_grh)] = 0;
      }
    } else {
      struct ibv_wc wc[kAppMaxPostlist];
      int ret = ibv_poll_cq(cb->dgram_recv_cq[qp_i], FLAGS_postlist, wc);
      assert(ret >= 0);
      num_comps = static_cast<size_t>(ret);
    }

    if (num_comps == 0) continue;

    // Post a batch of RECVs
    struct ibv_recv_wr recv_wr[kAppMaxPostlist], *bad_recv_wr;
    struct ibv_sge sgl[kAppMaxPostlist];
    for (size_t w_i = 0; w_i < num_comps; w_i++) {
      sgl[w_i].length = FLAGS_size + sizeof(struct ibv_grh);
      sgl[w_i].lkey = cb->dgram_buf_mr->lkey;
      sgl[w_i].addr = reinterpret_cast<uint64_t>(&cb->dgram_buf[0]);

      recv_wr[w_i].sg_list = &sgl[w_i];
      recv_wr[w_i].num_sge = 1;
      recv_wr[w_i].next = (w_i == num_comps - 1) ? nullptr : &recv_wr[w_i + 1];
    }

    int ret = ibv_post_recv(cb->dgram_qp[qp_i], &recv_wr[0], &bad_recv_wr);
    rt_assert(ret == 0, "ibv_post_recv() error " + std::to_string(ret));

    rolling_iter += static_cast<size_t>(num_comps);
    qp_i = (qp_i + 1) % kAppNumQPs;
  }
}

void run_client(thread_params_t params) {
  // The local HID of a control block should be <= 64 to keep the SHM key low.
  // But the number of clients over all machines can be larger.
  size_t clt_gid = params.id;  // Global ID of this client thread
  size_t clt_local_hid = clt_gid % FLAGS_num_client_threads;
  size_t srv_gid = clt_gid % FLAGS_num_server_threads;
  size_t ib_port_index = FLAGS_dual_port == 0 ? 0 : srv_gid % 2;

  hrd_dgram_config_t dgram_config;
  dgram_config.num_qps = 1;
  dgram_config.buf_size = 0;  // We don't need a registered buffer
  dgram_config.buf_shm_key = -1;

  auto* cb = hrd_ctrl_blk_init(clt_local_hid, ib_port_index,
                               kHrdInvalidNUMANode, &dgram_config);
  assert(cb != nullptr);

  // Buffer to send packets from. Set to a non-zero value.
  memset(const_cast<uint8_t*>(cb->dgram_buf), 1, dgram_config.buf_size);

  printf("main: Client %zu waiting for server %zu\n", clt_gid, srv_gid);

  hrd_qp_attr_t* srv_qp[kAppNumQPs] = {nullptr};
  for (size_t qp_i = 0; qp_i < kAppNumQPs; qp_i++) {
    char srv_name[kHrdQPNameSize];
    sprintf(srv_name, "server-%zu-%zu", srv_gid, qp_i);
    while (srv_qp[qp_i] == nullptr) {
      srv_qp[qp_i] = hrd_get_published_qp(srv_name);
      if (srv_qp[qp_i] == nullptr) usleep(200000);
    }
  }

  printf("main: Client %zu found server! Now posting SENDs.\n", clt_gid);

  // We need only one address handle because we contacts only one server
  struct ibv_ah* ah = hrd_create_ah(cb, srv_qp[0]->lid);
  assert(ah != nullptr);

  struct ibv_send_wr wr[kAppMaxPostlist], *bad_send_wr;
  struct ibv_wc wc[kAppMaxPostlist];
  struct ibv_sge sgl[kAppMaxPostlist];
  size_t rolling_iter = 0;  // For throughput measurement
  size_t nb_tx = 0;
  size_t qp_i = 0;

  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  uint8_t* pkt_arr[kAppMaxPostlist];  // Leaked
  for (size_t i = 0; i < kAppMaxPostlist; i++) {
    pkt_arr[i] = reinterpret_cast<uint8_t*>(malloc(FLAGS_size));
  }

  while (true) {
    if (rolling_iter >= MB(2)) {
      clock_gettime(CLOCK_REALTIME, &end);
      double seconds = (end.tv_sec - start.tv_sec) +
                       (end.tv_nsec - start.tv_nsec) / 1000000000.0;
      printf("main: Client %zu: %.2f IOPS\n", clt_gid, rolling_iter / seconds);
      rolling_iter = 0;

      clock_gettime(CLOCK_REALTIME, &start);
    }

    // Reusing pkt_arr buffers is OK because the payloads are inlined
    for (size_t w_i = 0; w_i < FLAGS_postlist; w_i++) {
      *reinterpret_cast<size_t*>(pkt_arr[w_i]) = nb_tx;

      wr[w_i].wr.ud.ah = ah;
      wr[w_i].wr.ud.remote_qpn = srv_qp[qp_i]->qpn;
      wr[w_i].wr.ud.remote_qkey = kHrdDefaultQKey;

      wr[w_i].opcode = IBV_WR_SEND_WITH_IMM;
      wr[w_i].num_sge = 1;
      wr[w_i].next = (w_i == FLAGS_postlist - 1) ? nullptr : &wr[w_i + 1];
      wr[w_i].imm_data = 3185;
      wr[w_i].sg_list = &sgl[w_i];

      wr[w_i].send_flags = IBV_SEND_INLINE;
      wr[w_i].send_flags |= nb_tx % kAppUnsigBatch == 0 ? IBV_SEND_SIGNALED : 0;
      if (nb_tx % kAppUnsigBatch == kAppUnsigBatch - 1) {
        hrd_poll_cq(cb->dgram_send_cq[0], 1, wc);
      }

      sgl[w_i].addr = reinterpret_cast<uint64_t>(pkt_arr[w_i]);
      sgl[w_i].length = FLAGS_size;

      rolling_iter++;
      nb_tx++;
    }

    int ret = ibv_post_send(cb->dgram_qp[0], &wr[0], &bad_send_wr);
    rt_assert(ret == 0, "ibv_post_send() error");
    qp_i = (qp_i + 1) % kAppNumQPs;
  }
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  double* tput = nullptr;  // Leaked

  rt_assert(FLAGS_dual_port <= 1, "Invalid dual_port");
  rt_assert(FLAGS_is_client <= 1, "Invalid is_client");
  rt_assert(FLAGS_postlist >= 1 && FLAGS_postlist <= kAppMaxPostlist,
            "Invalid postlist");
  rt_assert(FLAGS_size > 0 && FLAGS_size <= kAppMaxSize,
            "Invalid transfer size");

  if (FLAGS_is_client == 1) {
    rt_assert(FLAGS_num_client_threads >= 1, "Invalid num_client_threads");
    // Clients need to know about number of server threads
    rt_assert(FLAGS_num_server_threads >= 1, "Invalid num_server_threads");
    rt_assert(FLAGS_machine_id != std::numeric_limits<size_t>::max(),
              "Invalid machine_id");

    rt_assert(FLAGS_postlist <= kAppUnsigBatch, "Postlist check failed");
    static_assert(kHrdSQDepth >= 2 * kAppUnsigBatch, "Queue capacity check");
  } else {
    rt_assert(FLAGS_machine_id == std::numeric_limits<size_t>::max(), "");
    rt_assert(FLAGS_postlist <= kHrdRQDepth / 2, "RECV pollbatch too large");

    // Server does not need to know about number of client threads
    rt_assert(FLAGS_num_client_threads == 0, "Invalid num_client_threads");
    rt_assert(FLAGS_num_server_threads >= 1, "Invalid num_server_threads");

    tput = new double[FLAGS_num_server_threads];
    for (size_t i = 0; i < FLAGS_num_server_threads; i++) tput[i] = 0;
  }

  size_t _num_threads = (FLAGS_is_client == 1) ? FLAGS_num_client_threads
                                               : FLAGS_num_server_threads;

  printf("main: Using %zu threads\n", _num_threads);
  std::vector<std::thread> thread_arr(_num_threads);

  for (size_t i = 0; i < _num_threads; i++) {
    if (FLAGS_is_client == 1) {
      size_t id = (FLAGS_machine_id * FLAGS_num_client_threads) + i;
      thread_arr[i] = std::thread(run_client, thread_params_t(id, nullptr));
    } else {
      size_t id = i;
      thread_arr[i] = std::thread(run_server, thread_params_t(id, tput));
    }
  }

  for (auto& t : thread_arr) t.join();
  return 0;
}
