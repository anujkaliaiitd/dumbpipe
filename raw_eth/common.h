#include <arpa/inet.h>
#include <assert.h>
#include <infiniband/verbs.h>
#include <infiniband/verbs_exp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sstream>
#include <thread>

static constexpr size_t kReceiverThreads = 2;
static constexpr bool kVerbose = false;

static constexpr size_t kDeviceIndex = 2;
static constexpr size_t kPortIndex = 2;       // mlx5_0
static constexpr size_t kDataSize = 32;       // Data size, without headers
static constexpr size_t kRecvBufSize = 1500;  // RECV buffer size
static_assert(kDataSize % sizeof(size_t) == 0, "");

static constexpr size_t kSQDepth = 512;
static constexpr size_t kRQDepth = 512;

static constexpr size_t kLogNumStrides = 9;
static constexpr size_t kLogStrideBytes = 10;
static constexpr size_t kNumStrides = (1ull << kLogNumStrides);
static constexpr size_t kStrideBytes = (1ull << kLogStrideBytes);

static constexpr uint16_t kIPEtherType = 0x800;
static constexpr uint16_t kIPHdrProtocol = 0x11;

uint8_t kDstMAC[6] = {0xec, 0x0d, 0x9a, 0x7b, 0xd7, 0xd6};
char kDstIP[] = "192.168.1.250";

uint8_t kSrcMAC[6] = {0xec, 0x0d, 0x9a, 0x7b, 0xd7, 0xe6};
char kSrcIP[] = "192.168.1.251";

// Receiver thread i uses port (kBaseDstPort + i)
static constexpr uint16_t kBaseDstPort = 3185;

struct ctrl_blk_t {
  struct ibv_device* ib_dev;
  struct ibv_context* context;
  struct ibv_pd* pd;
  struct ibv_cq* send_cq;
  struct ibv_cq* recv_cq;
  struct ibv_qp* qp;

  // For multi-packet RECV queue
  struct ibv_exp_wq* wq;
  struct ibv_exp_wq_family* wq_family;
  struct ibv_exp_rwq_ind_table* ind_tbl;
};

struct eth_hdr_t {
  uint8_t dst_mac[6];
  uint8_t src_mac[6];
  uint16_t eth_type;
} __attribute__((packed));

struct ipv4_hdr_t {
  uint8_t ihl : 4;
  uint8_t version : 4;
  uint8_t tos;
  uint16_t tot_len;
  uint16_t id;
  uint16_t frag_off;
  uint8_t ttl;
  uint8_t protocol;
  uint16_t check;
  uint32_t src_ip;
  uint32_t dst_ip;
} __attribute__((packed));

struct udp_hdr_t {
  uint16_t src_port;
  uint16_t dst_port;
  uint16_t len;
  uint16_t sum;
} __attribute__((packed));

static constexpr size_t kTotHdrSz =
    sizeof(eth_hdr_t) + sizeof(ipv4_hdr_t) + sizeof(udp_hdr_t);
static_assert(kTotHdrSz == 42, "");

// User-defined data header
struct data_hdr_t {
  size_t receiver_thread;  // The receiver thread that's the target
  size_t seq_num;          // Sequence number to this receiver
  uint8_t pad[kDataSize - 2 * sizeof(size_t)];

  std::string to_string() {
    std::ostringstream ret;
    ret << "[Receiver thread " << std::to_string(receiver_thread)
        << ", seq num " << std::to_string(seq_num) << "]";
    return ret.str();
  }
};
static_assert(sizeof(data_hdr_t) == kDataSize, "");

uint32_t ip_from_str(char* ip) {
  uint32_t addr;
  int ret = inet_pton(AF_INET, ip, &addr);
  assert(ret == 1);
  return addr;
}

static uint16_t ip_checksum(ipv4_hdr_t* ipv4_hdr) {
  unsigned long sum = 0;
  const uint16_t* ip1 = reinterpret_cast<uint16_t*>(ipv4_hdr);

  size_t hdr_len = sizeof(ipv4_hdr_t);
  while (hdr_len > 1) {
    sum += *ip1++;
    if (sum & 0x80000000) sum = (sum & 0xFFFF) + (sum >> 16);
    hdr_len -= 2;
  }

  while (sum >> 16) sum = (sum & 0xFFFF) + (sum >> 16);
  return (~sum);
}

void gen_eth_header(eth_hdr_t* eth_header, uint8_t* src_mac, uint8_t* dst_mac,
                    uint16_t eth_type) {
  memcpy(eth_header->src_mac, src_mac, 6);
  memcpy(eth_header->dst_mac, dst_mac, 6);
  eth_header->eth_type = htons(eth_type);
}

void gen_ipv4_header(ipv4_hdr_t* ipv4_hdr, uint32_t src_ip, uint32_t dst_ip,
                     uint8_t protocol, uint16_t data_size) {
  ipv4_hdr->version = 4;
  ipv4_hdr->ihl = 5;
  ipv4_hdr->tos = 0;
  ipv4_hdr->tot_len = htons(sizeof(ipv4_hdr_t) + sizeof(udp_hdr_t) + data_size);
  ipv4_hdr->id = htons(0);
  ipv4_hdr->frag_off = htons(0);
  ipv4_hdr->ttl = 128;
  ipv4_hdr->protocol = protocol;
  ipv4_hdr->src_ip = src_ip;
  ipv4_hdr->dst_ip = dst_ip;
  ipv4_hdr->check = ip_checksum(ipv4_hdr);
}

void gen_udp_header(udp_hdr_t* udp_hdr, uint16_t src_port, uint16_t dst_port,
                    uint16_t data_size) {
  udp_hdr->src_port = htons(src_port);
  udp_hdr->dst_port = htons(dst_port);
  udp_hdr->len = htons(sizeof(udp_hdr_t) + data_size);
  udp_hdr->sum = 0;
}

// Create device context, PD, and CQs
ctrl_blk_t* init_ctx_common(size_t device_index) {
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

  struct ibv_exp_cq_init_attr cq_init_attr;
  memset(&cq_init_attr, 0, sizeof(cq_init_attr));
  cb->send_cq = ibv_exp_create_cq(cb->context, kSQDepth, nullptr, nullptr, 0,
                                  &cq_init_attr);
  assert(cb->send_cq != nullptr);

  cb->recv_cq =
      ibv_exp_create_cq(cb->context, 1, nullptr, nullptr, 0, &cq_init_attr);
  assert(cb->recv_cq != nullptr);

  // Modify the RECV CQ to ignore overrun
  struct ibv_exp_cq_attr cq_attr;
  memset(&cq_attr, 0, sizeof(cq_attr));
  cq_attr.comp_mask = IBV_EXP_CQ_ATTR_CQ_CAP_FLAGS;
  cq_attr.cq_cap_flags = IBV_EXP_CQ_IGNORE_OVERRUN;
  int ret = ibv_exp_modify_cq(cb->recv_cq, &cq_attr, IBV_EXP_CQ_CAP_FLAGS);
  assert(ret == 0);

  return cb;
}

ctrl_blk_t* init_ctx_non_mp_rq(size_t device_index) {
  printf("Creating control block without MP RQ.\n");
  auto* cb = init_ctx_common(device_index);
  assert(cb->context != nullptr && cb->pd != nullptr &&
         cb->send_cq != nullptr && cb->recv_cq != nullptr);

  struct ibv_exp_qp_init_attr qp_init_attr;
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));

  qp_init_attr.comp_mask =
      IBV_EXP_QP_INIT_ATTR_PD | IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS;

  qp_init_attr.pd = cb->pd;
  qp_init_attr.send_cq = cb->send_cq;
  qp_init_attr.recv_cq = cb->recv_cq;
  qp_init_attr.cap.max_send_wr = kSQDepth;
  qp_init_attr.cap.max_inline_data = 512;
  qp_init_attr.cap.max_recv_wr = kRQDepth;
  qp_init_attr.cap.max_recv_sge = 1;
  qp_init_attr.qp_type = IBV_QPT_RAW_PACKET;
  qp_init_attr.exp_create_flags |= IBV_EXP_QP_CREATE_SCATTER_FCS;

  cb->qp = ibv_exp_create_qp(cb->context, &qp_init_attr);
  assert(cb->qp != nullptr);

  // Initialize the QP and assign a port
  struct ibv_exp_qp_attr qp_attr;
  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.qp_state = IBV_QPS_INIT;
  qp_attr.port_num = 1;
  int ret = ibv_exp_modify_qp(cb->qp, &qp_attr, IBV_QP_STATE | IBV_QP_PORT);
  assert(ret >= 0);

  // Move to RTR
  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.qp_state = IBV_QPS_RTR;
  ret = ibv_exp_modify_qp(cb->qp, &qp_attr, IBV_QP_STATE);
  assert(ret >= 0);

  // Move to RTS
  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.qp_state = IBV_QPS_RTS;
  ret = ibv_exp_modify_qp(cb->qp, &qp_attr, IBV_QP_STATE);
  assert(ret >= 0);

  return cb;
}

ctrl_blk_t* init_ctx_mp_rq(size_t device_index) {
  printf("Creating control block with MP RQ.\n");
  auto* cb = init_ctx_common(device_index);
  assert(cb->context != nullptr && cb->pd != nullptr &&
         cb->send_cq != nullptr && cb->recv_cq != nullptr);

  uint8_t toeplitz_key[] = {0x6d, 0x5a, 0x56, 0xda, 0x25, 0x5b, 0x0e, 0xc2,
                            0x41, 0x67, 0x25, 0x3d, 0x43, 0xa3, 0x8f, 0xb0,
                            0xd0, 0xca, 0x2b, 0xcb, 0xae, 0x7b, 0x30, 0xb4,
                            0x77, 0xcb, 0x2d, 0xa3, 0x80, 0x30, 0xf2, 0x0c,
                            0x6a, 0x42, 0xb7, 0x3b, 0xbe, 0xac, 0x01, 0xfa};
  const int TOEPLITZ_RX_HASH_KEY_LEN =
      sizeof(toeplitz_key) / sizeof(toeplitz_key[0]);

  struct ibv_exp_wq_init_attr wq_init_attr;
  memset(&wq_init_attr, 0, sizeof(wq_init_attr));

  wq_init_attr.wq_type = IBV_EXP_WQT_RQ;
  wq_init_attr.max_recv_wr = 1;
  wq_init_attr.max_recv_sge = 1;
  wq_init_attr.pd = cb->pd;
  wq_init_attr.cq = cb->recv_cq;

  wq_init_attr.comp_mask |= IBV_EXP_CREATE_WQ_MP_RQ;
  wq_init_attr.mp_rq.use_shift = IBV_EXP_MP_RQ_NO_SHIFT;
  wq_init_attr.mp_rq.single_wqe_log_num_of_strides = kLogNumStrides;
  wq_init_attr.mp_rq.single_stride_log_num_of_bytes = kLogStrideBytes;
  cb->wq = ibv_exp_create_wq(cb->context, &wq_init_attr);
  assert(cb->wq != nullptr);

  // Change WQ to ready state
  struct ibv_exp_wq_attr wq_attr;
  memset(&wq_attr, 0, sizeof(wq_attr));
  wq_attr.attr_mask = IBV_EXP_WQ_ATTR_STATE;
  wq_attr.wq_state = IBV_EXP_WQS_RDY;

  int ret = ibv_exp_modify_wq(cb->wq, &wq_attr);
  assert(ret == 0);

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
  struct ibv_exp_rx_hash_conf rx_hash_conf;
  memset(&rx_hash_conf, 0, sizeof(rx_hash_conf));
  rx_hash_conf.rx_hash_function = IBV_EXP_RX_HASH_FUNC_TOEPLITZ;
  rx_hash_conf.rx_hash_key_len = TOEPLITZ_RX_HASH_KEY_LEN;
  rx_hash_conf.rx_hash_key = toeplitz_key;
  rx_hash_conf.rx_hash_fields_mask = IBV_EXP_RX_HASH_DST_PORT_UDP;
  rx_hash_conf.rwq_ind_tbl = cb->ind_tbl;

  struct ibv_exp_qp_init_attr exp_qp_init_attr;
  memset(&exp_qp_init_attr, 0, sizeof(exp_qp_init_attr));
  exp_qp_init_attr.comp_mask = IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS |
                               IBV_EXP_QP_INIT_ATTR_PD |
                               IBV_EXP_QP_INIT_ATTR_RX_HASH;
  exp_qp_init_attr.rx_hash_conf = &rx_hash_conf;
  exp_qp_init_attr.pd = cb->pd;
  exp_qp_init_attr.qp_type = IBV_QPT_RAW_PACKET;

  // Create the QP
  cb->qp = ibv_exp_create_qp(cb->context, &exp_qp_init_attr);
  assert(cb->qp != nullptr);

  return cb;
}
