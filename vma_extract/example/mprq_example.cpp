#include <mellanox/vma_extra.h>
#include <stdio.h>
#include <string.h>

int main() {
  /* get vma extra API */
  vma_api_t* vma_api = vma_get_api();
  if (vma_api == NULL) {
    printf("VMA Extra API not found");
    return -1;
  }
  /* create a ring profile */
  vma_ring_profile_key profile_key = 0;
  vma_ring_type_attr ring;
  ring.ring_type = VMA_RING_CYCLIC_BUFFER;
  ring.ring_cyclicb.num = (1 << 17);  // 128 MB
  ring.ring_cyclicb.stride_bytes = 1400; /* user packet size is stated not
  including eth/ip/udp header sizes */
  int res = vma_api->vma_add_ring_profile(&ring, &profile_key);
  if (res) {
    printf("failed adding ring profile");
    return  -1;
  }
  /* create a socket */
  int fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
  vma_ring_alloc_logic_attr profile;
  profile.engress = 0;
  profile.ingress = 1;                    /* this is an RX buffer */
  profile.ring_profile_key = static_cast<uint32_t>(profile_key);
  profile.comp_mask =
      VMA_RING_ALLOC_MASK_RING_PROFILE_KEY | VMA_RING_ALLOC_MASK_RING_INGRESS;
  profile.ring_alloc_logic = RING_LOGIC_PER_SOCKET; /* each sock will have a
  different buffer */
  setsockopt(fd, SOL_SOCKET, SO_VMA_RING_ALLOC_LOGIC, &profile,
             sizeof(profile));
  /* bind socket to address */

  /* get the buffer/ring fd to read data */
  int ring_fd_num = vma_api->get_socket_rings_num(fd);
  int* ring_fds = new int[ring_fd_num];
  vma_api->get_socket_rings_fds(fd, ring_fds, ring_fd_num);
  int ring_fd = *ring_fds;
  /* currently, only MSG_DONTWAIT is supported by vma_cyclic_buffer_read() */
  struct vma_completion_cb_t completion;
  memset(&completion, 0, sizeof(completion));
  int flags = MSG_DONTWAIT;
  size_t min_p = 1000, max_p = 5000;
  while (1) {
    int res = vma_api->vma_cyclic_buffer_read(ring_fd, &completion, min_p,
                                              max_p, flags);
    if (res == -1) {
      printf("vma_cyclic_buffer_read returned -1");
      return -1;
    }
    if (completion.packets == 0) continue;

    auto *data = reinterpret_cast<uint8_t *>(completion.payload_ptr);

    /* The buffer returned to the user consists of all packets
    * (headers + data + padding) in one big buffer.
    * In this example, the user expects a packet size of 1400 bytes.
    * So the total packet size is:
    * 20(eth hdr) + 14(ip hdr) + 8(udp hdr) + 1400(data) = 1442 bytes.
    * The closest power of two greater or equal to 1442 is 2048 (hence there
    * will be 606 bytes of padding at the end of every packet).
    *
    * +---------+----------+---------+----------------+-----------+---...
    * + eth hdr | ip hdr
    | udp hdr | data ...
    | padding
    |
    * +
    +
    +
    +
    +
    +
    * + 14 bytes| 20 bytes | 8 bytes | 1400 bytes
    | 606 bytes |
    * +---------+----------+---------+----------------+-----------+---...
    */
    data += 42; /* skip the eth, ip and udp headers (20 + 14 + 8) */
    for (size_t i = 0; i < completion.packets; ++i) {
      /* process packet i */
      data += 2048; /* skip to data of the next packet */
    }
  }
  delete[] ring_fds;
}
