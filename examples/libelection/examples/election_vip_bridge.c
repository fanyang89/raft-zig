#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "libelection/libelection.h"

typedef struct bridge {
  election_node *node;
  uint64_t node_id;
  uint64_t tick_interval_ms;
  char cluster_id[33];
  char fencer_socket[sizeof(((struct sockaddr_un *)0)->sun_path)];
  atomic_bool leadership_active;
  atomic_bool node_failed;
  atomic_bool driver_stop;
  atomic_bool worker_stop;
  atomic_uint_fast64_t desired_term;
  atomic_uint_fast64_t granted_term;
  atomic_uint_fast64_t last_drive_ns;
  pthread_mutex_t worker_mutex;
  pthread_cond_t worker_cond;
} bridge;

static volatile sig_atomic_t stop_requested = 0;

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
  if (value[0] < '1' || value[0] > '9') return 0;
  for (size_t index = 1; value[index] != '\0'; index++) {
    if (value[index] < '0' || value[index] > '9') return 0;
  }
  char *end = NULL;
  errno = 0;
  unsigned long long parsed = strtoull(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0' || parsed == 0) return 0;
  *out = (uint64_t)parsed;
  return 1;
}

static uint64_t monotonic_ns(void) {
  struct timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) return 0;
  return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
}

static void sleep_ms(uint64_t milliseconds) {
  struct timespec delay = {
      .tv_sec = (time_t)(milliseconds / 1000),
      .tv_nsec = (long)((milliseconds % 1000) * UINT64_C(1000000)),
  };
  while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {
  }
}

static void wake_worker(bridge *app) {
  pthread_mutex_lock(&app->worker_mutex);
  pthread_cond_signal(&app->worker_cond);
  pthread_mutex_unlock(&app->worker_mutex);
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
  bridge *app = user_data;
  fprintf(
      stderr,
      "%s: node=%" PRIu64 " term=%" PRIu64 " leader=%" PRIu64 "\n",
      event_name(event->event_type),
      event->status.node_id,
      event->status.term,
      event->status.leader_id);

  if (event->event_type == ELECTION_EVENT_LEADERSHIP_ACQUIRED) {
    atomic_store(&app->desired_term, event->status.term);
    atomic_store(&app->leadership_active, true);
  } else {
    atomic_store(&app->leadership_active, false);
    atomic_store(&app->granted_term, 0);
  }
  if (event->event_type == ELECTION_EVENT_FAILED) {
    atomic_store(&app->node_failed, true);
  }
  wake_worker(app);
}

static int set_socket_timeout(int socket_fd, long milliseconds) {
  struct timeval timeout = {
      .tv_sec = milliseconds / 1000,
      .tv_usec = (milliseconds % 1000) * 1000,
  };
  if (setsockopt(
          socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0) {
    return -1;
  }
  return setsockopt(
      socket_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
}

static int request_fencing_grant(bridge *app, uint64_t term) {
  int socket_fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
  if (socket_fd < 0) return 0;
  if (set_socket_timeout(socket_fd, 1000) != 0) {
    close(socket_fd);
    return 0;
  }

  struct sockaddr_un address = {.sun_family = AF_UNIX};
  memcpy(address.sun_path, app->fencer_socket, strlen(app->fencer_socket) + 1);
  if (connect(socket_fd, (const struct sockaddr *)&address, sizeof(address)) !=
      0) {
    close(socket_fd);
    return 0;
  }

  char request[160];
  int request_size = snprintf(
      request,
      sizeof(request),
      "ACQUIRE %s %" PRIu64 " %" PRIu64 "\n",
      app->cluster_id,
      term,
      app->node_id);
  if (request_size < 0 || (size_t)request_size >= sizeof(request) ||
      send(socket_fd, request, (size_t)request_size, MSG_NOSIGNAL) !=
          request_size) {
    close(socket_fd);
    return 0;
  }

  char response[160];
  ssize_t received = recv(socket_fd, response, sizeof(response) - 1, MSG_TRUNC);
  close(socket_fd);
  if (received <= 0 || (size_t)received >= sizeof(response) ||
      response[received - 1] != '\n') {
    return 0;
  }
  response[received] = '\0';

  char response_type[8];
  char term_value[21];
  char owner_value[21];
  char extra[2];
  if (sscanf(
          response,
          "%7s %20s %20s %1s",
          response_type,
          term_value,
          owner_value,
          extra) != 3 ||
      strcmp(response_type, "GRANTED") != 0) {
    return 0;
  }
  uint64_t granted_term = 0;
  uint64_t granted_owner = 0;
  if (!parse_node_id(term_value, &granted_term) ||
      !parse_node_id(owner_value, &granted_owner)) {
    return 0;
  }
  return granted_term == term && granted_owner == app->node_id;
}

static void worker_wait(bridge *app, long milliseconds) {
  struct timespec deadline;
  if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) return;
  deadline.tv_nsec += (milliseconds % 1000) * 1000000L;
  deadline.tv_sec += milliseconds / 1000 + deadline.tv_nsec / 1000000000L;
  deadline.tv_nsec %= 1000000000L;

  pthread_mutex_lock(&app->worker_mutex);
  (void)pthread_cond_timedwait(&app->worker_cond, &app->worker_mutex, &deadline);
  pthread_mutex_unlock(&app->worker_mutex);
}

static void *fencing_worker(void *argument) {
  bridge *app = argument;
  while (!atomic_load(&app->worker_stop)) {
    bool active = atomic_load(&app->leadership_active);
    uint64_t term = atomic_load(&app->desired_term);
    if (active && term != 0 && atomic_load(&app->granted_term) != term) {
      if (request_fencing_grant(app, term)) {
        if (atomic_load(&app->leadership_active) &&
            atomic_load(&app->desired_term) == term) {
          atomic_store(&app->granted_term, term);
          fprintf(
              stderr,
              "fencing granted: node=%" PRIu64 " term=%" PRIu64 "\n",
              app->node_id,
              term);
        }
      }
    }
    worker_wait(app, 200);
  }
  return NULL;
}

static void *election_driver(void *argument) {
  bridge *app = argument;
  const uint64_t tick_ns = app->tick_interval_ms * UINT64_C(1000000);
  uint64_t next_tick = monotonic_ns() + tick_ns;

  while (!atomic_load(&app->driver_stop)) {
    election_error error = election_node_poll(app->node, NULL);
    if (error != ELECTION_OK) {
      fprintf(stderr, "poll failed: %s\n", election_error_string(error));
      atomic_store(&app->node_failed, true);
      break;
    }

    uint64_t now = monotonic_ns();
    if (now >= next_tick) {
      error = election_node_tick(app->node, NULL);
      if (error != ELECTION_OK) {
        fprintf(stderr, "tick failed: %s\n", election_error_string(error));
        atomic_store(&app->node_failed, true);
        break;
      }
      next_tick = now + tick_ns;
    }
    atomic_store(&app->last_drive_ns, now);
    sleep_ms(10);
  }
  return NULL;
}

static int parse_ipv4_address(
    const char *value,
    struct sockaddr_in *out_address) {
  const char *separator = strrchr(value, ':');
  if (separator == NULL || separator == value || separator[1] == '\0') return 0;

  size_t host_size = (size_t)(separator - value);
  if (host_size >= INET_ADDRSTRLEN) return 0;
  char host[INET_ADDRSTRLEN];
  memcpy(host, value, host_size);
  host[host_size] = '\0';

  char *end = NULL;
  errno = 0;
  unsigned long port = strtoul(separator + 1, &end, 10);
  if (errno != 0 || end == separator + 1 || *end != '\0' || port > 65535) {
    return 0;
  }

  memset(out_address, 0, sizeof(*out_address));
  out_address->sin_family = AF_INET;
  out_address->sin_port = htons((uint16_t)port);
  return inet_pton(AF_INET, host, &out_address->sin_addr) == 1;
}

static int open_http_listener(const char *listen_address) {
  struct sockaddr_in address;
  if (!parse_ipv4_address(listen_address, &address)) {
    fprintf(stderr, "invalid HTTP listen address: %s\n", listen_address);
    return -1;
  }

  int socket_fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (socket_fd < 0) return -1;
  int enabled = 1;
  if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) !=
          0 ||
      bind(socket_fd, (const struct sockaddr *)&address, sizeof(address)) != 0 ||
      listen(socket_fd, 16) != 0) {
    close(socket_fd);
    return -1;
  }
  return socket_fd;
}

static int send_all(int socket_fd, const char *data, size_t size) {
  while (size != 0) {
    ssize_t sent = send(socket_fd, data, size, MSG_NOSIGNAL);
    if (sent < 0 && errno == EINTR) continue;
    if (sent <= 0) return -1;
    data += (size_t)sent;
    size -= (size_t)sent;
  }
  return 0;
}

static void send_http_response(
    int socket_fd,
    int status_code,
    const char *status_text,
    const char *content_type,
    const char *body) {
  char header[256];
  int header_size = snprintf(
      header,
      sizeof(header),
      "HTTP/1.1 %d %s\r\n"
      "Content-Type: %s\r\n"
      "Content-Length: %zu\r\n"
      "Connection: close\r\n\r\n",
      status_code,
      status_text,
      content_type,
      strlen(body));
  if (header_size <= 0 || (size_t)header_size >= sizeof(header)) return;
  if (send_all(socket_fd, header, (size_t)header_size) != 0) return;
  (void)send_all(socket_fd, body, strlen(body));
}

static bool driver_is_fresh(const bridge *app) {
  uint64_t last_drive = atomic_load(&app->last_drive_ns);
  uint64_t now = monotonic_ns();
  uint64_t maximum_age_ms = app->tick_interval_ms * 5;
  if (maximum_age_ms < 1000) maximum_age_ms = 1000;
  return last_drive != 0 && now >= last_drive &&
         now - last_drive <= maximum_age_ms * UINT64_C(1000000);
}

static bool leader_is_ready(bridge *app, election_status *out_status) {
  *out_status = (election_status)ELECTION_STATUS_INIT;
  if (!driver_is_fresh(app) || atomic_load(&app->node_failed) ||
      !atomic_load(&app->leadership_active)) {
    return false;
  }
  uint64_t desired_term = atomic_load(&app->desired_term);
  if (desired_term == 0 || atomic_load(&app->granted_term) != desired_term) {
    return false;
  }
  if (election_node_get_status(app->node, out_status) != ELECTION_OK) return false;
  return out_status->state == ELECTION_NODE_RUNNING &&
         out_status->leader_active != 0 && out_status->term == desired_term;
}

static void serve_client(bridge *app, int client_fd) {
  if (set_socket_timeout(client_fd, 1000) != 0) return;
  char request[1024];
  ssize_t received = recv(client_fd, request, sizeof(request) - 1, 0);
  if (received <= 0) return;
  request[received] = '\0';

  char method[8];
  char path[128];
  if (sscanf(request, "%7s %127s", method, path) != 2 ||
      strcmp(method, "GET") != 0) {
    send_http_response(
        client_fd, 400, "Bad Request", "text/plain", "bad request\n");
    return;
  }

  election_status status;
  bool ready = leader_is_ready(app, &status);
  if (strcmp(path, "/leader") == 0) {
    send_http_response(
        client_fd,
        ready ? 200 : 503,
        ready ? "OK" : "Service Unavailable",
        "text/plain",
        ready ? "leader\n" : "not leader\n");
    return;
  }
  if (strcmp(path, "/healthz") == 0) {
    bool healthy = driver_is_fresh(app) && !atomic_load(&app->node_failed);
    send_http_response(
        client_fd,
        healthy ? 200 : 503,
        healthy ? "OK" : "Service Unavailable",
        "text/plain",
        healthy ? "ok\n" : "unhealthy\n");
    return;
  }
  if (strcmp(path, "/status") == 0) {
    status = (election_status)ELECTION_STATUS_INIT;
    election_error error = election_node_get_status(app->node, &status);
    char body[384];
    int body_size = snprintf(
        body,
        sizeof(body),
        "{\"node_id\":%" PRIu64 ",\"term\":%" PRIu64
        ",\"leader_id\":%" PRIu64
        ",\"leader_active\":%s,\"fencing_granted\":%s}\n",
        app->node_id,
        error == ELECTION_OK ? status.term : 0,
        error == ELECTION_OK ? status.leader_id : 0,
        error == ELECTION_OK && status.leader_active != 0 ? "true" : "false",
        ready ? "true" : "false");
    if (body_size <= 0 || (size_t)body_size >= sizeof(body)) return;
    send_http_response(client_fd, 200, "OK", "application/json", body);
    return;
  }
  send_http_response(client_fd, 404, "Not Found", "text/plain", "not found\n");
}

static int serve_http(bridge *app, const char *listen_address) {
  int listener = open_http_listener(listen_address);
  if (listener < 0) {
    fprintf(stderr, "failed to listen on %s: %s\n", listen_address, strerror(errno));
    return -1;
  }
  fprintf(stderr, "leadership HTTP endpoint listening on %s\n", listen_address);

  while (!stop_requested && !atomic_load(&app->node_failed)) {
    struct pollfd descriptor = {.fd = listener, .events = POLLIN};
    int result = poll(&descriptor, 1, 100);
    if (result < 0 && errno == EINTR) continue;
    if (result < 0) {
      close(listener);
      return -1;
    }
    if (result == 0 || (descriptor.revents & POLLIN) == 0) continue;

    int client = accept4(listener, NULL, NULL, SOCK_CLOEXEC);
    if (client < 0 && errno == EINTR) continue;
    if (client < 0) {
      close(listener);
      return -1;
    }
    serve_client(app, client);
    close(client);
  }
  close(listener);
  return 0;
}

static void request_stop(int signal_number) {
  (void)signal_number;
  stop_requested = 1;
}

static void usage(const char *program) {
  fprintf(
      stderr,
      "usage: %s NODE_ID CLUSTER_ID RAFT_LISTEN HTTP_LISTEN FENCER_SOCKET "
      "DATA_DIR PEER_ID=ADDRESS...\n",
      program);
}

int main(int argc, char **argv) {
  if (argc < 8) {
    usage(argv[0]);
    return 2;
  }

  bridge app = {0};
  app.tick_interval_ms = 100;
  if (pthread_mutex_init(&app.worker_mutex, NULL) != 0 ||
      pthread_cond_init(&app.worker_cond, NULL) != 0) {
    fprintf(stderr, "failed to initialize worker synchronization\n");
    return 1;
  }

  election_node_options options = ELECTION_NODE_OPTIONS_INIT;
  options.drive_mode = ELECTION_DRIVE_EXTERNAL;
  if (!parse_node_id(argv[1], &options.node_id) ||
      !parse_cluster_id(argv[2], options.cluster_id) ||
      strlen(argv[2]) >= sizeof(app.cluster_id) ||
      strlen(argv[5]) >= sizeof(app.fencer_socket)) {
    usage(argv[0]);
    return 2;
  }
  app.node_id = options.node_id;
  app.tick_interval_ms = options.tick_interval_ms;
  memcpy(app.cluster_id, argv[2], strlen(argv[2]) + 1);
  memcpy(app.fencer_socket, argv[5], strlen(argv[5]) + 1);
  options.listen_address = bytes(argv[3]);
  options.data_dir = bytes(argv[6]);

  size_t peer_count = (size_t)(argc - 7);
  election_peer *peers = calloc(peer_count, sizeof(*peers));
  if (peers == NULL) {
    fprintf(stderr, "failed to allocate peer list\n");
    return 1;
  }
  for (size_t index = 0; index < peer_count; index++) {
    char *peer_argument = argv[index + 7];
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
  callbacks.user_data = &app;
  callbacks.on_event = on_event;

  election_error error = election_node_create(&options, &callbacks, &app.node);
  free(peers);
  if (error != ELECTION_OK) {
    fprintf(stderr, "create failed: %s\n", election_error_string(error));
    return 1;
  }

  signal(SIGINT, request_stop);
  signal(SIGTERM, request_stop);
  error = election_node_start(app.node);
  if (error != ELECTION_OK) {
    fprintf(stderr, "start failed: %s\n", election_error_string(error));
    election_node_destroy(app.node);
    return 1;
  }

  pthread_t worker_thread;
  pthread_t driver_thread;
  if (pthread_create(&worker_thread, NULL, fencing_worker, &app) != 0) {
    fprintf(stderr, "failed to start fencing worker\n");
    (void)election_node_shutdown(app.node);
    election_node_destroy(app.node);
    return 1;
  }
  if (pthread_create(&driver_thread, NULL, election_driver, &app) != 0) {
    fprintf(stderr, "failed to start election driver\n");
    atomic_store(&app.worker_stop, true);
    wake_worker(&app);
    pthread_join(worker_thread, NULL);
    (void)election_node_shutdown(app.node);
    election_node_destroy(app.node);
    return 1;
  }

  int serve_result = serve_http(&app, argv[4]);
  atomic_store(&app.leadership_active, false);
  atomic_store(&app.granted_term, 0);
  atomic_store(&app.driver_stop, true);
  pthread_join(driver_thread, NULL);

  error = election_node_shutdown(app.node);
  if (error != ELECTION_OK) {
    fprintf(stderr, "shutdown failed: %s\n", election_error_string(error));
  }
  atomic_store(&app.worker_stop, true);
  wake_worker(&app);
  pthread_join(worker_thread, NULL);
  election_node_destroy(app.node);
  pthread_cond_destroy(&app.worker_cond);
  pthread_mutex_destroy(&app.worker_mutex);

  return serve_result == 0 && error == ELECTION_OK &&
                 !atomic_load(&app.node_failed)
             ? 0
             : 1;
}
