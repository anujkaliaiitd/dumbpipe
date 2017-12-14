#include "main.h"
#include "client.h"
#include "server.h"

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  rt_assert(kAppRingMbufSize >= (kTotHdrSz + FLAGS_size + 4), "");

  double* tput = nullptr;  // Leaked

  // Basic flag checks
  rt_assert(FLAGS_dual_port <= 1, "Invalid dual_port");
  rt_assert(FLAGS_is_client <= 1, "Invalid is_client");
  rt_assert(FLAGS_postlist >= 1 && FLAGS_postlist <= kAppMaxPostlist,
            "Invalid postlist");
  rt_assert(FLAGS_size > 0 && FLAGS_size <= kHrdMaxInline &&
                FLAGS_size >= sizeof(data_hdr_t),
            "Invalid transfer size");

  // More checks
  rt_assert(FLAGS_postlist <= kAppUnsigBatch, "Postlist check failed");
  static_assert(kHrdSQDepth >= 2 * kAppUnsigBatch, "Queue capacity check");
  rt_assert(FLAGS_postlist <= kHrdRQDepth / 2, "RECV pollbatch too large");

  if (FLAGS_is_client == 1) {
    rt_assert(FLAGS_num_client_threads >= 1, "Invalid num_client_threads");
    // Clients need to know about number of server threads
    rt_assert(FLAGS_num_server_threads >= 1, "Invalid num_server_threads");
    rt_assert(FLAGS_machine_id != std::numeric_limits<size_t>::max(),
              "Invalid machine_id");
  } else {
    // Server does not need to know about number of client threads
    rt_assert(FLAGS_num_client_threads == 0, "Invalid num_client_threads");
    rt_assert(FLAGS_num_server_threads >= 1, "Invalid num_server_threads");
    rt_assert(FLAGS_machine_id == std::numeric_limits<size_t>::max(), "");

    tput = new double[FLAGS_num_server_threads];
    for (size_t i = 0; i < FLAGS_num_server_threads; i++) tput[i] = 0;
  }

  size_t _num_threads = (FLAGS_is_client == 1) ? FLAGS_num_client_threads
                                               : FLAGS_num_server_threads;

  // Launch a single server thread or multiple client threads
  printf("main: Using %zu threads\n", _num_threads);
  std::vector<std::thread> thread_arr(_num_threads);

  for (size_t i = 0; i < _num_threads; i++) {
    if (FLAGS_is_client == 1) {
      size_t id = (FLAGS_machine_id * _num_threads) + i;
      thread_arr[i] = std::thread(run_client, thread_params_t(id, tput));
    } else {
      size_t id = i;
      thread_arr[i] = std::thread(run_server, thread_params_t(id, tput));
    }
  }

  for (auto& t : thread_arr) t.join();
  return 0;
}
