#define _GNU_SOURCE

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

static int parse_uint64(const char *value, uint64_t *out) {
  char *end = NULL;
  errno = 0;
  unsigned long long parsed = strtoull(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0' || parsed == 0) return 0;
  *out = (uint64_t)parsed;
  return 1;
}

int main(int argc, char **argv) {
  if (argc != 5 || strlen(argv[1]) >= sizeof(((struct sockaddr_un *)0)->sun_path) ||
      strlen(argv[2]) != 32) {
    fprintf(stderr, "usage: %s SOCKET CLUSTER_ID TERM OWNER_ID\n", argv[0]);
    return 2;
  }
  uint64_t term = 0;
  uint64_t owner_id = 0;
  if (!parse_uint64(argv[3], &term) || !parse_uint64(argv[4], &owner_id)) return 2;

  int socket_fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
  if (socket_fd < 0) return 1;
  struct sockaddr_un address = {.sun_family = AF_UNIX};
  memcpy(address.sun_path, argv[1], strlen(argv[1]) + 1);
  if (connect(socket_fd, (const struct sockaddr *)&address, sizeof(address)) != 0) {
    close(socket_fd);
    return 1;
  }

  char request[160];
  int request_size = snprintf(
      request,
      sizeof(request),
      "ACQUIRE %s %" PRIu64 " %" PRIu64 "\n",
      argv[2],
      term,
      owner_id);
  if (request_size <= 0 || (size_t)request_size >= sizeof(request) ||
      send(socket_fd, request, (size_t)request_size, MSG_NOSIGNAL) != request_size) {
    close(socket_fd);
    return 1;
  }

  char response[192];
  ssize_t received = recv(socket_fd, response, sizeof(response), MSG_TRUNC);
  close(socket_fd);
  if (received <= 0 || (size_t)received > sizeof(response)) return 1;
  return fwrite(response, 1, (size_t)received, stdout) == (size_t)received ? 0 : 1;
}
