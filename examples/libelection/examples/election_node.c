#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <inttypes.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "libelection/libelection.h"

static volatile sig_atomic_t stop_requested = 0;
static atomic_bool node_failed = false;

static election_bytes_view bytes(const char *value) {
  election_bytes_view result = {
      (const uint8_t *)value,
      strlen(value),
  };
  return result;
}

static int hex_digit(char value) {
  if (value >= '0' && value <= '9') return value - '0';
  if (value >= 'a' && value <= 'f') return value - 'a' + 10;
  if (value >= 'A' && value <= 'F') return value - 'A' + 10;
  return -1;
}

static int parse_cluster_id(const char *value, uint8_t out[16]) {
  if (strlen(value) != 32) return 0;
  for (size_t index = 0; index < 16; index++) {
    int high = hex_digit(value[index * 2]);
    int low = hex_digit(value[index * 2 + 1]);
    if (high < 0 || low < 0) return 0;
    out[index] = (uint8_t)((high << 4) | low);
  }
  return 1;
}

static int parse_node_id(const char *value, uint64_t *out) {
  char *end = NULL;
  errno = 0;
  unsigned long long parsed = strtoull(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0' || parsed == 0) return 0;
  *out = (uint64_t)parsed;
  return 1;
}

static const char *event_name(uint32_t event_type) {
  switch (event_type) {
    case ELECTION_EVENT_LEADERSHIP_ACQUIRED:
      return "leadership acquired";
    case ELECTION_EVENT_LEADERSHIP_LOST:
      return "leadership lost";
    case ELECTION_EVENT_FAILED:
      return "node failed";
    default:
      return "unknown event";
  }
}

static void on_event(void *user_data, const election_event *event) {
  (void)user_data;
  fprintf(
      stderr,
      "%s: node=%" PRIu64 " term=%" PRIu64 " leader=%" PRIu64 "\n",
      event_name(event->event_type),
      event->status.node_id,
      event->status.term,
      event->status.leader_id);
  if (event->event_type == ELECTION_EVENT_FAILED) {
    atomic_store(&node_failed, true);
  }
}

static void request_stop(int signal_number) {
  (void)signal_number;
  stop_requested = 1;
}

static void usage(const char *program) {
  fprintf(
      stderr,
      "usage: %s NODE_ID CLUSTER_ID LISTEN_ADDRESS DATA_DIR "
      "PEER_ID=ADDRESS...\n",
      program);
}

int main(int argc, char **argv) {
  if (argc < 6) {
    usage(argv[0]);
    return 2;
  }

  election_node_options options = ELECTION_NODE_OPTIONS_INIT;
  if (!parse_node_id(argv[1], &options.node_id) ||
      !parse_cluster_id(argv[2], options.cluster_id)) {
    usage(argv[0]);
    return 2;
  }
  options.listen_address = bytes(argv[3]);
  options.data_dir = bytes(argv[4]);

  size_t peer_count = (size_t)(argc - 5);
  election_peer *peers = calloc(peer_count, sizeof(*peers));
  if (peers == NULL) {
    fprintf(stderr, "failed to allocate peer list\n");
    return 1;
  }

  for (size_t index = 0; index < peer_count; index++) {
    char *peer_argument = argv[index + 5];
    char *separator = strchr(peer_argument, '=');
    if (separator == NULL || separator[1] == '\0') {
      usage(argv[0]);
      free(peers);
      return 2;
    }
    *separator = '\0';
    if (!parse_node_id(peer_argument, &peers[index].id)) {
      usage(argv[0]);
      free(peers);
      return 2;
    }
    peers[index].address = bytes(separator + 1);
  }
  options.peers = peers;
  options.peer_count = peer_count;

  election_callbacks callbacks = ELECTION_CALLBACKS_INIT;
  callbacks.on_event = on_event;

  election_node *node = NULL;
  election_error error = election_node_create(&options, &callbacks, &node);
  free(peers);
  if (error != ELECTION_OK) {
    fprintf(stderr, "create failed: %s\n", election_error_string(error));
    return 1;
  }

  signal(SIGINT, request_stop);
  signal(SIGTERM, request_stop);
  error = election_node_start(node);
  if (error != ELECTION_OK) {
    fprintf(stderr, "start failed: %s\n", election_error_string(error));
    election_node_destroy(node);
    return 1;
  }

  const struct timespec delay = {1, 0};
  while (!stop_requested && !atomic_load(&node_failed)) nanosleep(&delay, NULL);

  error = election_node_shutdown(node);
  if (error != ELECTION_OK) {
    fprintf(stderr, "shutdown failed: %s\n", election_error_string(error));
  }
  election_node_destroy(node);
  return error == ELECTION_OK && !atomic_load(&node_failed) ? 0 : 1;
}
