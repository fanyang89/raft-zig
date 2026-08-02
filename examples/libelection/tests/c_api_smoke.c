#define _XOPEN_SOURCE 700

#include <ftw.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "libelection/libelection.h"

typedef struct capture {
  election_node *node;
  uint32_t acquired;
  uint32_t lost;
  uint32_t failed;
} capture;

static election_bytes_view bytes(const char *value) {
  election_bytes_view result = {
      (const uint8_t *)value,
      strlen(value),
  };
  return result;
}

static void on_event(void *user_data, const election_event *event) {
  capture *state = user_data;
  election_status status = ELECTION_STATUS_INIT;
  if (election_node_get_status(state->node, &status) != ELECTION_OK) abort();
  if (status.term != event->status.term) abort();

  switch (event->event_type) {
    case ELECTION_EVENT_LEADERSHIP_ACQUIRED:
      state->acquired++;
      break;
    case ELECTION_EVENT_LEADERSHIP_LOST:
      state->lost++;
      break;
    case ELECTION_EVENT_FAILED:
      state->failed++;
      break;
    default:
      abort();
  }
}

static int remove_entry(
    const char *path,
    const struct stat *status,
    int type,
    struct FTW *ftw) {
  (void)status;
  (void)type;
  (void)ftw;
  return remove(path);
}

int main(void) {
  if (election_abi_version() != ELECTION_ABI_VERSION) return 1;
  if (strcmp(election_error_string(ELECTION_OK), "ok") != 0) return 2;

  char data_dir[] = "/tmp/libelection-c-smoke-XXXXXX";
  if (mkdtemp(data_dir) == NULL) return 3;

  const char *address = "127.0.0.1:0";
  election_peer peer = {UINT64_C(1), bytes(address)};
  election_node_options options = ELECTION_NODE_OPTIONS_INIT;
  options.drive_mode = ELECTION_DRIVE_EXTERNAL;
  options.node_id = 1;
  options.cluster_id[0] = 1;
  options.listen_address = bytes(address);
  options.data_dir = bytes(data_dir);
  options.peers = &peer;
  options.peer_count = 1;
  options.tick_interval_ms = 10;
  options.heartbeat_ticks = 1;
  options.election_ticks = 5;

  capture events = {0};
  election_callbacks callbacks = ELECTION_CALLBACKS_INIT;
  callbacks.user_data = &events;
  callbacks.on_event = on_event;

  election_node *node = NULL;
  election_error error = election_node_create(&options, &callbacks, &node);
  if (error != ELECTION_OK || node == NULL) return 4;
  events.node = node;
  if (election_node_start(node) != ELECTION_OK) return 5;

  election_status status = ELECTION_STATUS_INIT;
  for (size_t index = 0; index < 30; index++) {
    if (election_node_tick(node, NULL) != ELECTION_OK) return 6;
    if (election_node_get_status(node, &status) != ELECTION_OK) return 7;
    if (status.leader_active != 0) break;
  }
  if (status.leader_active == 0 || events.acquired != 1) return 8;
  if (election_node_poll(node, NULL) != ELECTION_OK) return 9;
  if (election_node_shutdown(node) != ELECTION_OK) return 10;
  if (events.lost != 1 || events.failed != 0) return 11;

  election_node_destroy(node);
  if (nftw(data_dir, remove_entry, 16, FTW_DEPTH | FTW_PHYS) != 0) return 12;
  return 0;
}
