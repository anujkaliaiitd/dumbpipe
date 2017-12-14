#ifndef MAIN_H
#define MAIN_H

#include <assert.h>
#include <gflags/gflags.h>
#include <infiniband/verbs_exp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sstream>
#include <thread>

#include "inet_hdrs.h"
#include "libhrd_cpp/hrd.h"
#include "mlx5_defs.h"

DEFINE_uint64(machine_id, std::numeric_limits<size_t>::max(), "Machine ID");
DEFINE_uint64(num_client_threads, 0, "Number of client threads/machine");
DEFINE_uint64(num_server_threads, 0, "Number of server threads");
DEFINE_uint64(is_client, 0, "Is this process a client?");
DEFINE_uint64(dual_port, 0, "Use two ports?");
DEFINE_uint64(size, 0, "RDMA size");
DEFINE_uint64(postlist, std::numeric_limits<size_t>::max(), "Postlist size");

static constexpr bool kAppVerbose = true;
static constexpr size_t kAppMaxPostlist = 64;
static constexpr size_t kAppUnsigBatch = 64;

static constexpr size_t kAppDeviceIndex = 2;
static constexpr size_t kAppPortIndex = 2;  // mlx5_0

static constexpr size_t kAppSQDepth = 128;
static constexpr size_t kAppRQDepth = 2;  // Multi-packet RQ depth
static_assert(kAppRQDepth <= 4, "");      // Double check - RQ must be small

static constexpr size_t kAppLogNumStrides = 9;
static constexpr size_t kAppLogStrideBytes = 10;
static constexpr size_t kAppNumRingEntries = (1ull << kAppLogNumStrides);
static constexpr size_t kAppRingMbufSize = (1ull << kAppLogStrideBytes);
static constexpr size_t kAppRingSize = (kAppNumRingEntries * kAppRingMbufSize);

uint8_t kAppDstMAC[6] = {0xec, 0x0d, 0x9a, 0x7b, 0xd7, 0xd6};
char kAppDstIP[] = "192.168.1.250";

uint8_t kAppSrcMAC[6] = {0xec, 0x0d, 0x9a, 0x7b, 0xd7, 0xe6};
char kAppSrcIP[] = "192.168.1.251";

// Server thread i uses UDP port (kBaseDstPort + i)
static constexpr uint16_t kBaseDstPort = 3185;

struct ctrl_blk_t {
  struct ibv_device* ib_dev;
  struct ibv_context* context;
  struct ibv_pd* pd;

  struct ibv_qp* send_qp;
  struct ibv_cq* send_cq;

  struct ibv_qp* recv_qp;
  struct ibv_cq* recv_cq;
  struct ibv_exp_wq* wq;
  struct ibv_exp_wq_family* wq_family;
  struct ibv_exp_rwq_ind_table* ind_tbl;
};

struct thread_params_t {
  size_t id;
  double* tput;

  thread_params_t(size_t id, double* tput) : id(id), tput(tput) {}
};

// User-defined data header
struct data_hdr_t {
  size_t server_thread;  // The server thread that's the target
  size_t seq_num;        // Sequence number to this receiver

  std::string to_string() {
    std::ostringstream ret;
    ret << "[Server thread " << std::to_string(server_thread) << ", seq num "
        << std::to_string(seq_num) << "]";
    return ret.str();
  }
};

void init_send_qp(ctrl_blk_t* cb) {
  assert(cb->context != nullptr && cb->pd != nullptr);

  struct ibv_exp_cq_init_attr cq_init_attr;
  memset(&cq_init_attr, 0, sizeof(cq_init_attr));
  cb->send_cq = ibv_exp_create_cq(cb->context, kAppSQDepth, nullptr, nullptr, 0,
                                  &cq_init_attr);
  assert(cb->send_cq != nullptr);

  struct ibv_exp_qp_init_attr qp_init_attr;
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));
  qp_init_attr.comp_mask =
      IBV_EXP_QP_INIT_ATTR_PD | IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS;

  qp_init_attr.pd = cb->pd;
  qp_init_attr.send_cq = cb->send_cq;
  qp_init_attr.recv_cq = cb->send_cq;  // We won't post RECVs
  qp_init_attr.cap.max_send_wr = kAppSQDepth;
  qp_init_attr.cap.max_inline_data = 512;
  qp_init_attr.qp_type = IBV_QPT_RAW_PACKET;
  qp_init_attr.exp_create_flags |= IBV_EXP_QP_CREATE_SCATTER_FCS;

  cb->send_qp = ibv_exp_create_qp(cb->context, &qp_init_attr);
  assert(cb->send_qp != nullptr);

  struct ibv_exp_qp_attr qp_attr;
  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.qp_state = IBV_QPS_INIT;
  qp_attr.port_num = 1;
  rt_assert(ibv_exp_modify_qp(cb->send_qp, &qp_attr,
                              IBV_QP_STATE | IBV_QP_PORT) == 0);

  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.qp_state = IBV_QPS_RTR;
  rt_assert(ibv_exp_modify_qp(cb->send_qp, &qp_attr, IBV_QP_STATE) == 0);

  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.qp_state = IBV_QPS_RTS;
  rt_assert(ibv_exp_modify_qp(cb->send_qp, &qp_attr, IBV_QP_STATE) == 0);
}

void init_recv_qp(ctrl_blk_t* cb) {
  assert(cb->context != nullptr && cb->pd != nullptr);

  // Init CQ. Its size MUST be one so that we get two CQEs in mlx5.
  struct ibv_exp_cq_init_attr cq_init_attr;
  memset(&cq_init_attr, 0, sizeof(cq_init_attr));
  cb->recv_cq =
      ibv_exp_create_cq(cb->context, 1, nullptr, nullptr, 0, &cq_init_attr);
  assert(cb->recv_cq != nullptr);

  // Modify the RECV CQ to ignore overrun
  struct ibv_exp_cq_attr cq_attr;
  memset(&cq_attr, 0, sizeof(cq_attr));
  cq_attr.comp_mask = IBV_EXP_CQ_ATTR_CQ_CAP_FLAGS;
  cq_attr.cq_cap_flags = IBV_EXP_CQ_IGNORE_OVERRUN;
  rt_assert(ibv_exp_modify_cq(cb->recv_cq, &cq_attr, IBV_EXP_CQ_CAP_FLAGS) ==
            0);

  struct ibv_exp_wq_init_attr wq_init_attr;
  memset(&wq_init_attr, 0, sizeof(wq_init_attr));

  wq_init_attr.wq_type = IBV_EXP_WQT_RQ;
  wq_init_attr.max_recv_wr = kAppRQDepth;
  wq_init_attr.max_recv_sge = 1;
  wq_init_attr.pd = cb->pd;
  wq_init_attr.cq = cb->recv_cq;

  wq_init_attr.comp_mask |= IBV_EXP_CREATE_WQ_MP_RQ;
  wq_init_attr.mp_rq.use_shift = IBV_EXP_MP_RQ_NO_SHIFT;
  wq_init_attr.mp_rq.single_wqe_log_num_of_strides = kAppLogNumStrides;
  wq_init_attr.mp_rq.single_stride_log_num_of_bytes = kAppLogStrideBytes;
  cb->wq = ibv_exp_create_wq(cb->context, &wq_init_attr);
  assert(cb->wq != nullptr);

  // Change WQ to ready state
  struct ibv_exp_wq_attr wq_attr;
  memset(&wq_attr, 0, sizeof(wq_attr));
  wq_attr.attr_mask = IBV_EXP_WQ_ATTR_STATE;
  wq_attr.wq_state = IBV_EXP_WQS_RDY;
  rt_assert(ibv_exp_modify_wq(cb->wq, &wq_attr) == 0);

  // Get the RQ burst function
  enum ibv_exp_query_intf_status intf_status = IBV_EXP_INTF_STAT_OK;
  struct ibv_exp_query_intf_params query_intf_params;
  memset(&query_intf_params, 0, sizeof(query_intf_params));
  query_intf_params.intf_scope = IBV_EXP_INTF_GLOBAL;
  query_intf_params.intf = IBV_EXP_INTF_WQ;
  query_intf_params.obj = cb->wq;
  cb->wq_family = reinterpret_cast<struct ibv_exp_wq_family*>(
      ibv_exp_query_intf(cb->context, &query_intf_params, &intf_status));
  assert(cb->wq_family != nullptr);

  // Create indirect table
  struct ibv_exp_rwq_ind_table_init_attr rwq_ind_table_init_attr;
  memset(&rwq_ind_table_init_attr, 0, sizeof(rwq_ind_table_init_attr));
  rwq_ind_table_init_attr.pd = cb->pd;
  rwq_ind_table_init_attr.log_ind_tbl_size = 0;  // Ignore hash
  rwq_ind_table_init_attr.ind_tbl = &cb->wq;     // Pointer to RECV work queue
  rwq_ind_table_init_attr.comp_mask = 0;
  cb->ind_tbl =
      ibv_exp_create_rwq_ind_table(cb->context, &rwq_ind_table_init_attr);
  assert(cb->ind_tbl != nullptr);

  // Create rx_hash_conf and indirection table for the QP
  uint8_t toeplitz_key[] = {0x6d, 0x5a, 0x56, 0xda, 0x25, 0x5b, 0x0e, 0xc2,
                            0x41, 0x67, 0x25, 0x3d, 0x43, 0xa3, 0x8f, 0xb0,
                            0xd0, 0xca, 0x2b, 0xcb, 0xae, 0x7b, 0x30, 0xb4,
                            0x77, 0xcb, 0x2d, 0xa3, 0x80, 0x30, 0xf2, 0x0c,
                            0x6a, 0x42, 0xb7, 0x3b, 0xbe, 0xac, 0x01, 0xfa};
  const int TOEPLITZ_RX_HASH_KEY_LEN =
      sizeof(toeplitz_key) / sizeof(toeplitz_key[0]);

  struct ibv_exp_rx_hash_conf rx_hash_conf;
  memset(&rx_hash_conf, 0, sizeof(rx_hash_conf));
  rx_hash_conf.rx_hash_function = IBV_EXP_RX_HASH_FUNC_TOEPLITZ;
  rx_hash_conf.rx_hash_key_len = TOEPLITZ_RX_HASH_KEY_LEN;
  rx_hash_conf.rx_hash_key = toeplitz_key;
  rx_hash_conf.rx_hash_fields_mask = IBV_EXP_RX_HASH_DST_PORT_UDP;
  rx_hash_conf.rwq_ind_tbl = cb->ind_tbl;

  struct ibv_exp_qp_init_attr qp_init_attr;
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));
  qp_init_attr.comp_mask = IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS |
                           IBV_EXP_QP_INIT_ATTR_PD |
                           IBV_EXP_QP_INIT_ATTR_RX_HASH;
  qp_init_attr.rx_hash_conf = &rx_hash_conf;
  qp_init_attr.pd = cb->pd;
  qp_init_attr.qp_type = IBV_QPT_RAW_PACKET;

  // Create the QP
  cb->recv_qp = ibv_exp_create_qp(cb->context, &qp_init_attr);
  assert(cb->recv_qp != nullptr);
}

ctrl_blk_t* init_ctx(size_t device_index) {
  auto* cb = new ctrl_blk_t();
  memset(cb, 0, sizeof(ctrl_blk_t));

  struct ibv_device** dev_list = ibv_get_device_list(nullptr);
  assert(dev_list != nullptr);

  cb->ib_dev = dev_list[device_index];
  assert(cb->ib_dev != nullptr);
  printf("Using device %s\n", cb->ib_dev->name);

  cb->context = ibv_open_device(cb->ib_dev);
  assert(cb->context != nullptr);

  cb->pd = ibv_alloc_pd(cb->context);
  assert(cb->pd != nullptr);

  init_send_qp(cb);
  init_recv_qp(cb);

  return cb;
}

#endif  // MAIN_H
