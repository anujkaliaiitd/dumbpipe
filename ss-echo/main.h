#pragma once

#include <gflags/gflags.h>
#include <stdlib.h>
#include <string.h>
#include <limits>
#include <thread>
#include "libhrd_cpp/hrd.h"

static constexpr size_t kAppNumQPs = 1;  // UD QPs used by server for RECVs
static constexpr size_t kAppMaxPostlist = 64;
static constexpr size_t kAppUnsigBatch = 64;
static_assert(is_power_of_two(kAppUnsigBatch), "");

struct thread_params_t {
  size_t id;
  double* tput;
};

DEFINE_uint64(machine_id, std::numeric_limits<size_t>::max(), "Machine ID");
DEFINE_uint64(num_client_threads, 0, "Number of client threads/machine");
DEFINE_uint64(num_server_threads, 0, "Number of server threads");
DEFINE_uint64(is_client, 0, "Is this process a client?");
DEFINE_uint64(dual_port, 0, "Use two ports?");
DEFINE_uint64(size, 0, "RDMA size");
DEFINE_uint64(postlist, std::numeric_limits<size_t>::max(), "Postlist size");

/// Return the size of one RECV buffer
size_t recv_mbuf_sz() { return sizeof(struct ibv_grh) + FLAGS_size; }
