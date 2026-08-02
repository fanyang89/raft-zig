#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <net/if.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/ioctl.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

typedef struct node_interface {
  uint64_t node_id;
  char name[IFNAMSIZ];
} node_interface;

typedef struct fence_state {
  bool initialized;
  char cluster_id[33];
  uint64_t term;
  uint64_t owner_id;
  char owner_interface[IFNAMSIZ];
} fence_state;

typedef struct fencer {
  const char *state_path;
  const char *socket_path;
  const node_interface *interfaces;
  size_t interface_count;
  bool dry_run;
  fence_state state;
} fencer;

static volatile sig_atomic_t stop_requested = 0;

static int parse_uint64(const char *value, uint64_t *out) {
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

static int parse_node_id(const char *value, uint64_t *out) {
  return parse_uint64(value, out);
}

static int valid_cluster_id(const char *value) {
  if (strlen(value) != 32) return 0;
  for (size_t index = 0; index < 32; index++) {
    char digit = value[index];
    if (!((digit >= '0' && digit <= '9') ||
          (digit >= 'a' && digit <= 'f') ||
          (digit >= 'A' && digit <= 'F'))) {
      return 0;
    }
  }
  return 1;
}

static const node_interface *find_interface(const fencer *app, uint64_t node_id) {
  for (size_t index = 0; index < app->interface_count; index++) {
    if (app->interfaces[index].node_id == node_id) return &app->interfaces[index];
  }
  return NULL;
}

static int write_all(int file_descriptor, const char *data, size_t size) {
  while (size != 0) {
    ssize_t written = write(file_descriptor, data, size);
    if (written < 0 && errno == EINTR) continue;
    if (written <= 0) return -1;
    data += (size_t)written;
    size -= (size_t)written;
  }
  return 0;
}

static int parent_directory_path(
    const char *path,
    char directory_path[PATH_MAX]) {
  size_t path_size = strlen(path);
  if (path_size >= PATH_MAX) return -1;
  memcpy(directory_path, path, path_size + 1);

  char *separator = strrchr(directory_path, '/');
  if (separator == NULL) {
    memcpy(directory_path, ".", 2);
  } else if (separator == directory_path) {
    separator[1] = '\0';
  } else {
    *separator = '\0';
  }
  return 0;
}

static int validate_parent_directory(const char *path) {
  char directory_path[PATH_MAX];
  if (parent_directory_path(path, directory_path) != 0) return -1;
  struct stat status;
  if (stat(directory_path, &status) != 0 || !S_ISDIR(status.st_mode) ||
      status.st_uid != geteuid() ||
      (status.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
    errno = EPERM;
    return -1;
  }
  return 0;
}

static int canonical_target_path(const char *path, char output[PATH_MAX]) {
  char directory_path[PATH_MAX];
  if (parent_directory_path(path, directory_path) != 0) return -1;
  char resolved_directory[PATH_MAX];
  if (realpath(directory_path, resolved_directory) == NULL) return -1;
  const char *name = strrchr(path, '/');
  name = name == NULL ? path : name + 1;
  if (name[0] == '\0') {
    errno = EINVAL;
    return -1;
  }
  int size = snprintf(output, PATH_MAX, "%s/%s", resolved_directory, name);
  if (size < 0 || size >= PATH_MAX) {
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int sync_parent_directory(const char *path) {
  char directory_path[PATH_MAX];
  if (parent_directory_path(path, directory_path) != 0) return -1;

  int directory = open(directory_path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (directory < 0) return -1;
  int result = fsync(directory);
  close(directory);
  return result;
}

static int acquire_path_lock(const char *path) {
  char lock_path[PATH_MAX];
  int size = snprintf(lock_path, sizeof(lock_path), "%s.lock", path);
  if (size < 0 || (size_t)size >= sizeof(lock_path)) {
    errno = ENAMETOOLONG;
    return -1;
  }
  int lock = open(
      lock_path,
      O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW,
      S_IRUSR | S_IWUSR);
  if (lock < 0) return -1;
  if (fchmod(lock, S_IRUSR | S_IWUSR) != 0 ||
      flock(lock, LOCK_EX | LOCK_NB) != 0) {
    close(lock);
    return -1;
  }
  return lock;
}

static int persist_state(const fencer *app, const fence_state *state) {
  char temporary_path[PATH_MAX];
  int temporary_size = snprintf(
      temporary_path,
      sizeof(temporary_path),
      "%s.tmp.XXXXXX",
      app->state_path);
  if (temporary_size < 0 || (size_t)temporary_size >= sizeof(temporary_path)) {
    errno = ENAMETOOLONG;
    return -1;
  }

  char contents[128];
  int contents_size = snprintf(
      contents,
      sizeof(contents),
      "%s %" PRIu64 " %" PRIu64 " %s\n",
      state->cluster_id,
      state->term,
      state->owner_id,
      state->owner_interface);
  if (contents_size < 0 || (size_t)contents_size >= sizeof(contents)) return -1;

  int file = mkstemp(temporary_path);
  if (file < 0) return -1;
  if (fcntl(file, F_SETFD, FD_CLOEXEC) != 0 ||
      fchmod(file, S_IRUSR | S_IWUSR) != 0) {
    close(file);
    unlink(temporary_path);
    return -1;
  }
  int result = write_all(file, contents, (size_t)contents_size);
  if (result == 0) result = fsync(file);
  int close_result = close(file);
  if (result == 0 && close_result != 0) result = -1;
  if (result == 0) result = rename(temporary_path, app->state_path);
  if (result == 0) result = sync_parent_directory(app->state_path);
  if (result != 0) unlink(temporary_path);
  return result;
}

static int load_state(fencer *app) {
  int descriptor = open(app->state_path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (descriptor < 0 && errno == ENOENT) return 0;
  if (descriptor < 0) return -1;
  struct stat status;
  if (fstat(descriptor, &status) != 0 || !S_ISREG(status.st_mode)) {
    close(descriptor);
    errno = EINVAL;
    return -1;
  }
  FILE *file = fdopen(descriptor, "r");
  if (file == NULL) {
    close(descriptor);
    return -1;
  }

  char contents[160];
  char term_value[21];
  char owner_value[21];
  char extra[2];
  bool read_ok = fgets(contents, sizeof(contents), file) != NULL &&
                 strchr(contents, '\n') != NULL;
  fence_state loaded = {0};
  int fields = read_ok
                   ? sscanf(
                         contents,
                         "%32s %20s %20s %15s %1s",
                         loaded.cluster_id,
                         term_value,
                         owner_value,
                         loaded.owner_interface,
                         extra)
                   : 0;
  int close_result = fclose(file);
  if (fields != 4 || close_result != 0 || !valid_cluster_id(loaded.cluster_id) ||
      !parse_uint64(term_value, &loaded.term) ||
      !parse_uint64(owner_value, &loaded.owner_id) ||
      loaded.owner_interface[0] == '\0') {
    errno = EINVAL;
    return -1;
  }
  loaded.initialized = true;
  app->state = loaded;
  return 0;
}

static int set_link_state(const fencer *app, const char *name, bool up) {
  if (app->dry_run) {
    fprintf(stderr, "dry-run: set link %s %s\n", name, up ? "up" : "down");
    return 0;
  }

  int socket_fd = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
  if (socket_fd < 0) return -1;
  struct ifreq request = {0};
  memcpy(request.ifr_name, name, strlen(name) + 1);
  if (ioctl(socket_fd, SIOCGIFFLAGS, &request) != 0) {
    close(socket_fd);
    return -1;
  }
  if (up) {
    request.ifr_flags = (short)(request.ifr_flags | IFF_UP);
  } else {
    request.ifr_flags = (short)(request.ifr_flags & ~IFF_UP);
  }
  if (ioctl(socket_fd, SIOCSIFFLAGS, &request) != 0 ||
      ioctl(socket_fd, SIOCGIFFLAGS, &request) != 0) {
    close(socket_fd);
    return -1;
  }
  close(socket_fd);
  return ((request.ifr_flags & IFF_UP) != 0) == up ? 0 : -1;
}

static void send_response(int client, const char *format, ...) {
  char response[192];
  va_list arguments;
  va_start(arguments, format);
  int size = vsnprintf(response, sizeof(response), format, arguments);
  va_end(arguments);
  if (size <= 0 || (size_t)size >= sizeof(response)) return;
  (void)send(client, response, (size_t)size, MSG_NOSIGNAL);
}

static void reject_request(int client, const char *reason) {
  fprintf(stderr, "fencing rejected: %s\n", reason);
  send_response(client, "REJECTED %s\n", reason);
}

static void handle_acquire(
    fencer *app,
    int client,
    const char *cluster_id,
    uint64_t term,
    uint64_t owner_id) {
  const node_interface *new_owner = find_interface(app, owner_id);
  if (new_owner == NULL) {
    reject_request(client, "unknown-owner");
    return;
  }
  if (app->state.initialized &&
      strcasecmp(app->state.cluster_id, cluster_id) != 0) {
    reject_request(client, "cluster-mismatch");
    return;
  }
  if (app->state.initialized && owner_id == app->state.owner_id &&
      strcmp(new_owner->name, app->state.owner_interface) != 0) {
    reject_request(client, "owner-interface-mismatch");
    return;
  }
  if (app->state.initialized && term < app->state.term) {
    reject_request(client, "stale-term");
    return;
  }
  if (app->state.initialized && term == app->state.term) {
    if (owner_id != app->state.owner_id) {
      reject_request(client, "term-owner-conflict");
      return;
    }
    if (set_link_state(app, new_owner->name, true) != 0) {
      reject_request(client, "owner-enable-failed");
      return;
    }
    send_response(client, "GRANTED %" PRIu64 " %" PRIu64 "\n", term, owner_id);
    return;
  }

  if (app->state.initialized && app->state.owner_id != owner_id) {
    if (set_link_state(app, app->state.owner_interface, false) != 0) {
      reject_request(client, "previous-owner-fence-failed");
      return;
    }
    fprintf(
        stderr,
        "fenced previous owner: node=%" PRIu64 " interface=%s\n",
        app->state.owner_id,
        app->state.owner_interface);
  }

  fence_state next = {
      .initialized = true,
      .term = term,
      .owner_id = owner_id,
  };
  memcpy(next.cluster_id, cluster_id, strlen(cluster_id) + 1);
  memcpy(next.owner_interface, new_owner->name, strlen(new_owner->name) + 1);
  if (persist_state(app, &next) != 0) {
    reject_request(client, "state-persist-failed");
    return;
  }
  app->state = next;
  if (set_link_state(app, new_owner->name, true) != 0) {
    reject_request(client, "owner-enable-failed");
    return;
  }
  fprintf(
      stderr,
      "fencing granted: cluster=%s term=%" PRIu64 " owner=%" PRIu64 "\n",
      cluster_id,
      term,
      owner_id);
  send_response(client, "GRANTED %" PRIu64 " %" PRIu64 "\n", term, owner_id);
}

static void serve_client(fencer *app, int client) {
  struct ucred credentials;
  socklen_t credentials_size = sizeof(credentials);
  if (getsockopt(
          client,
          SOL_SOCKET,
          SO_PEERCRED,
          &credentials,
          &credentials_size) != 0 ||
      credentials_size != sizeof(credentials) || credentials.uid != geteuid()) {
    reject_request(client, "unauthorized-peer");
    return;
  }

  char request[192];
  ssize_t received = recv(client, request, sizeof(request) - 1, MSG_TRUNC);
  if (received <= 0 || (size_t)received >= sizeof(request) ||
      request[received - 1] != '\n') {
    reject_request(client, "invalid-frame");
    return;
  }
  request[received] = '\0';

  char command[8];
  char cluster_id[33];
  char term_value[21];
  char owner_value[21];
  char extra[2];
  int fields = sscanf(
      request,
      "%7s %32s %20s %20s %1s",
      command,
      cluster_id,
      term_value,
      owner_value,
      extra);
  uint64_t term = 0;
  uint64_t owner_id = 0;
  if (fields != 4 || strcmp(command, "ACQUIRE") != 0 ||
      !valid_cluster_id(cluster_id) || !parse_uint64(term_value, &term) ||
      !parse_uint64(owner_value, &owner_id)) {
    reject_request(client, "invalid-request");
    return;
  }
  handle_acquire(app, client, cluster_id, term, owner_id);
}

static int socket_is_active(const char *path) {
  int socket_fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
  if (socket_fd < 0) return 0;
  struct sockaddr_un address = {.sun_family = AF_UNIX};
  memcpy(address.sun_path, path, strlen(path) + 1);
  int result = connect(socket_fd, (const struct sockaddr *)&address, sizeof(address));
  close(socket_fd);
  return result == 0;
}

static int socket_matches(
    const char *path,
    const struct stat *expected_status) {
  struct stat status;
  return lstat(path, &status) == 0 && status.st_dev == expected_status->st_dev &&
         status.st_ino == expected_status->st_ino && S_ISSOCK(status.st_mode);
}

static void unlink_owned_socket(
    const char *path,
    const struct stat *expected_status) {
  if (socket_matches(path, expected_status)) (void)unlink(path);
}

static int open_listener(const char *path, struct stat *out_status) {
  if (strlen(path) >= sizeof(((struct sockaddr_un *)0)->sun_path)) {
    errno = ENAMETOOLONG;
    return -1;
  }
  struct stat socket_status;
  if (lstat(path, &socket_status) == 0) {
    if (!S_ISSOCK(socket_status.st_mode)) {
      errno = EEXIST;
      return -1;
    }
    if (socket_is_active(path)) {
      errno = EADDRINUSE;
      return -1;
    }
    if (unlink(path) != 0) return -1;
  } else if (errno != ENOENT) {
    return -1;
  }

  int listener = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
  if (listener < 0) return -1;
  struct sockaddr_un address = {.sun_family = AF_UNIX};
  memcpy(address.sun_path, path, strlen(path) + 1);
  if (bind(listener, (const struct sockaddr *)&address, sizeof(address)) != 0) {
    close(listener);
    return -1;
  }
  if (lstat(path, out_status) != 0 || !S_ISSOCK(out_status->st_mode)) {
    close(listener);
    return -1;
  }
  if (chmod(path, S_IRUSR | S_IWUSR) != 0 || listen(listener, 16) != 0) {
    close(listener);
    unlink_owned_socket(path, out_status);
    return -1;
  }
  return listener;
}

static void request_stop(int signal_number) {
  (void)signal_number;
  stop_requested = 1;
}

static void usage(const char *program) {
  fprintf(
      stderr,
      "usage: %s [--dry-run] STATE_FILE SOCKET_PATH NODE_ID=INTERFACE...\n",
      program);
}

int main(int argc, char **argv) {
  bool dry_run = argc > 1 && strcmp(argv[1], "--dry-run") == 0;
  int first_argument = dry_run ? 2 : 1;
  if (argc - first_argument < 3) {
    usage(argv[0]);
    return 2;
  }

  const char *state_argument = argv[first_argument];
  const char *socket_argument = argv[first_argument + 1];
  size_t interface_count = (size_t)(argc - first_argument - 2);
  node_interface *interfaces = calloc(interface_count, sizeof(*interfaces));
  if (interfaces == NULL) {
    fprintf(stderr, "failed to allocate interface map\n");
    return 1;
  }

  for (size_t index = 0; index < interface_count; index++) {
    char *mapping = argv[first_argument + 2 + (int)index];
    char *separator = strchr(mapping, '=');
    if (separator == NULL || separator[1] == '\0' ||
        strlen(separator + 1) >= IFNAMSIZ) {
      usage(argv[0]);
      free(interfaces);
      return 2;
    }
    *separator = '\0';
    if (!parse_node_id(mapping, &interfaces[index].node_id)) {
      usage(argv[0]);
      free(interfaces);
      return 2;
    }
    memcpy(interfaces[index].name, separator + 1, strlen(separator + 1) + 1);
    for (size_t previous = 0; previous < index; previous++) {
      if (interfaces[previous].node_id == interfaces[index].node_id) {
        fprintf(stderr, "duplicate node ID in interface map\n");
        free(interfaces);
        return 2;
      }
      if (strcmp(interfaces[previous].name, interfaces[index].name) == 0) {
        fprintf(stderr, "duplicate interface in interface map\n");
        free(interfaces);
        return 2;
      }
    }
  }

  char canonical_state[PATH_MAX];
  char canonical_socket[PATH_MAX];
  if (validate_parent_directory(state_argument) != 0 ||
      validate_parent_directory(socket_argument) != 0 ||
      canonical_target_path(state_argument, canonical_state) != 0 ||
      canonical_target_path(socket_argument, canonical_socket) != 0 ||
      validate_parent_directory(canonical_state) != 0 ||
      validate_parent_directory(canonical_socket) != 0) {
    fprintf(stderr, "fencer paths must use owner-only writable directories\n");
    free(interfaces);
    return 1;
  }
  if (strcmp(canonical_state, canonical_socket) == 0) {
    fprintf(stderr, "state and socket paths must differ\n");
    free(interfaces);
    return 2;
  }
  const char *state_path = canonical_state;
  const char *socket_path = canonical_socket;

  int state_lock = acquire_path_lock(state_path);
  if (state_lock < 0) {
    fprintf(stderr, "failed to lock fencing state: %s\n", strerror(errno));
    free(interfaces);
    return 1;
  }
  int socket_lock = acquire_path_lock(socket_path);
  if (socket_lock < 0) {
    fprintf(stderr, "failed to lock fencing socket: %s\n", strerror(errno));
    close(state_lock);
    free(interfaces);
    return 1;
  }

  fencer app = {
      .state_path = state_path,
      .socket_path = socket_path,
      .interfaces = interfaces,
      .interface_count = interface_count,
      .dry_run = dry_run,
  };
  if (load_state(&app) != 0) {
    fprintf(stderr, "failed to load fencing state: %s\n", strerror(errno));
    close(socket_lock);
    close(state_lock);
    free(interfaces);
    return 1;
  }
  if (app.state.initialized) {
    const node_interface *persisted_owner =
        find_interface(&app, app.state.owner_id);
    if (persisted_owner == NULL ||
        strcmp(persisted_owner->name, app.state.owner_interface) != 0) {
      fprintf(stderr, "persisted owner interface does not match configuration\n");
      close(socket_lock);
      close(state_lock);
      free(interfaces);
      return 1;
    }
  }

  struct stat socket_status;
  int listener = open_listener(socket_path, &socket_status);
  if (listener < 0) {
    fprintf(stderr, "failed to open fencing socket: %s\n", strerror(errno));
    close(socket_lock);
    close(state_lock);
    free(interfaces);
    return 1;
  }
  signal(SIGINT, request_stop);
  signal(SIGTERM, request_stop);
  fprintf(stderr, "fencer listening on %s\n", socket_path);

  int result = 0;
  while (!stop_requested) {
    struct pollfd descriptor = {.fd = listener, .events = POLLIN};
    int poll_result = poll(&descriptor, 1, 100);
    if (poll_result < 0 && errno == EINTR) continue;
    if (poll_result < 0) {
      result = 1;
      break;
    }
    if (poll_result == 0 || (descriptor.revents & POLLIN) == 0) continue;

    int client = accept4(listener, NULL, NULL, SOCK_CLOEXEC);
    if (client < 0 && errno == EINTR) continue;
    if (client < 0) {
      result = 1;
      break;
    }
    serve_client(&app, client);
    close(client);
  }

  close(listener);
  unlink_owned_socket(socket_path, &socket_status);
  close(socket_lock);
  close(state_lock);
  free(interfaces);
  return result;
}
