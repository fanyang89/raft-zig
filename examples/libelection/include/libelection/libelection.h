#ifndef LIBELECTION_LIBELECTION_H
#define LIBELECTION_LIBELECTION_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ELECTION_ABI_MAJOR 1u
#define ELECTION_ABI_MINOR 0u
#define ELECTION_ABI_VERSION \
  ((ELECTION_ABI_MAJOR << 16) | ELECTION_ABI_MINOR)

typedef int32_t election_error;

enum {
  ELECTION_OK = 0,
  ELECTION_ERROR_INVALID_ARGUMENT = 1,
  ELECTION_ERROR_INVALID_STATE = 2,
  ELECTION_ERROR_OUT_OF_MEMORY = 3,
  ELECTION_ERROR_IO = 4,
  ELECTION_ERROR_UNAVAILABLE = 5,
  ELECTION_ERROR_CORRUPT_STORAGE = 6,
  ELECTION_ERROR_INCOMPATIBLE_STORAGE = 7,
  ELECTION_ERROR_CLOSED = 8,
  ELECTION_ERROR_INTERNAL = 255,
};

enum {
  ELECTION_DRIVE_MANAGED = 0,
  ELECTION_DRIVE_EXTERNAL = 1,
};

enum {
  ELECTION_NODE_CREATED = 0,
  ELECTION_NODE_RUNNING = 1,
  ELECTION_NODE_STOPPED = 2,
  ELECTION_NODE_FAILED = 3,
};

enum {
  ELECTION_ROLE_FOLLOWER = 0,
  ELECTION_ROLE_CANDIDATE = 1,
  ELECTION_ROLE_LEADER = 2,
  ELECTION_ROLE_PRE_CANDIDATE = 3,
};

enum {
  ELECTION_EVENT_LEADERSHIP_ACQUIRED = 1,
  ELECTION_EVENT_LEADERSHIP_LOST = 2,
  ELECTION_EVENT_FAILED = 3,
};

typedef struct election_node election_node;

typedef struct election_bytes_view {
  const uint8_t *data;
  size_t size;
} election_bytes_view;

typedef struct election_peer {
  uint64_t id;
  election_bytes_view address;
} election_peer;

typedef struct election_node_options {
  size_t struct_size;
  uint32_t drive_mode;
  uint64_t node_id;
  uint8_t cluster_id[16];
  election_bytes_view listen_address;
  election_bytes_view data_dir;
  const election_peer *peers;
  size_t peer_count;
  uint64_t tick_interval_ms;
  uint32_t heartbeat_ticks;
  uint32_t election_ticks;
} election_node_options;

#define ELECTION_NODE_OPTIONS_INIT \
  { sizeof(election_node_options), ELECTION_DRIVE_MANAGED, UINT64_C(0), \
    { 0 }, { NULL, 0 }, { NULL, 0 }, NULL, 0, UINT64_C(100), 2, 20 }

typedef struct election_status {
  size_t struct_size;
  uint32_t state;
  uint32_t role;
  uint32_t leader_active;
  uint32_t reserved;
  uint64_t node_id;
  uint64_t term;
  uint64_t leader_id;
  uint64_t commit_index;
  uint64_t applied_index;
  int32_t last_error;
} election_status;

#define ELECTION_STATUS_INIT \
  { sizeof(election_status), ELECTION_NODE_CREATED, ELECTION_ROLE_FOLLOWER, \
    0, 0, UINT64_C(0), UINT64_C(0), UINT64_C(0), UINT64_C(0), UINT64_C(0), \
    ELECTION_OK }

typedef struct election_event {
  size_t struct_size;
  uint32_t event_type;
  election_status status;
} election_event;

typedef void (*election_event_callback)(
    void *user_data,
    const election_event *event);

typedef struct election_callbacks {
  size_t struct_size;
  void *user_data;
  election_event_callback on_event;
} election_callbacks;

#define ELECTION_CALLBACKS_INIT \
  { sizeof(election_callbacks), NULL, NULL }

uint32_t election_abi_version(void);
const char *election_library_version(void);
const char *election_error_string(election_error error_code);

election_error election_node_create(
    const election_node_options *options,
    const election_callbacks *callbacks,
    election_node **out_node);
election_error election_node_start(election_node *node);
election_error election_node_poll(election_node *node, uint32_t *out_had_work);
election_error election_node_tick(election_node *node, uint32_t *out_had_work);
election_error election_node_get_status(
    const election_node *node,
    election_status *out_status);
election_error election_node_shutdown(election_node *node);
void election_node_destroy(election_node *node);

/*
 * Serialize lifecycle functions with all other calls for the same node. Call
 * destroy only after every other call has returned, and never use the handle
 * afterward. External poll and tick calls must run on one host thread.
 */

/*
 * Callback data is borrowed for the duration of the call. In managed mode,
 * callbacks run on the library driver thread. In external mode, callbacks run
 * synchronously from poll, tick, or shutdown. Callbacks must return promptly
 * and must not call lifecycle or drive functions for the originating node.
 * get_status is permitted. No callbacks run after shutdown returns.
 */

#ifdef __cplusplus
}
#endif

#endif
