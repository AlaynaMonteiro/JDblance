// jdblance-origin: a programmable origin server for fault injection.
//
//   jdblance-origin --port 9001 --name b1 --latency-ms 3
//   curl -X POST 'localhost:9001/chaos?fail_rate=0.3&latency_ms=50'
//   curl -X POST localhost:9001/chaos -H 'content-type: application/json' -d '{"stall":true}'
//
// It is also the demo backend in the Docker stack (see docker-compose.yml).
#include <signal.h>
#include <sys/signalfd.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "origin.hpp"

namespace {
const char* kUsage =
    "usage: jdblance-origin [--host H] [--port P] [--name N] [--latency-ms MS] [--jitter-ms MS]\n"
    "                    [--capacity C] [--fail-rate R]\n"
    "  defaults: --host 0.0.0.0 --port $PORT or 9001 --name $ORIGIN_NAME/$BACKEND_NAME/hostname\n";

const char* env_or(const char* a, const char* b) {
  const char* v = std::getenv(a);
  if (v && *v) return v;
  v = b ? std::getenv(b) : nullptr;
  return v && *v ? v : nullptr;
}

bool parse_double(const char* s, double* out) {
  char* end = nullptr;
  double v = std::strtod(s, &end);
  if (end == s || *end != '\0' || !(v >= 0)) return false;
  *out = v;
  return true;
}
}  // namespace

int main(int argc, char** argv) {
  std::string host = "0.0.0.0";
  int port = 9001;
  if (const char* p = std::getenv("PORT")) port = std::atoi(p);
  std::string name;
  if (const char* n = env_or("ORIGIN_NAME", "BACKEND_NAME")) {
    name = n;
  } else {
    char buf[256] = "origin";
    ::gethostname(buf, sizeof buf - 1);
    name = buf;
  }
  double latency_ms = 0, jitter_ms = 0, fail_rate = 0, capacity = 0;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "-h" || arg == "--help") {
      std::fputs(kUsage, stdout);
      return 0;
    }
    if (i + 1 >= argc) {
      std::fprintf(stderr, "jdblance-origin: %s requires a value\n%s", arg.c_str(), kUsage);
      return 2;
    }
    const char* value = argv[++i];
    bool ok = true;
    if (arg == "--host") host = value;
    else if (arg == "--port") ok = (port = std::atoi(value)) >= 0 && port <= 65535;
    else if (arg == "--name") name = value;
    else if (arg == "--latency-ms") ok = parse_double(value, &latency_ms);
    else if (arg == "--jitter-ms") ok = parse_double(value, &jitter_ms);
    else if (arg == "--capacity") ok = parse_double(value, &capacity);
    else if (arg == "--fail-rate") ok = parse_double(value, &fail_rate) && fail_rate <= 1;
    else ok = false;
    if (!ok) {
      std::fprintf(stderr, "jdblance-origin: bad argument %s %s\n%s", arg.c_str(), value, kUsage);
      return 2;
    }
  }

  jdblance::raise_fd_limit();
  jdblance::Loop loop;
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGTERM);
  sigaddset(&set, SIGINT);
  ::sigprocmask(SIG_BLOCK, &set, nullptr);
  int sfd = ::signalfd(-1, &set, SFD_NONBLOCK | SFD_CLOEXEC);
  jdblance::FdState signals;
  signals.on_readable = [&loop] { loop.stop(); };
  loop.add_fd(sfd, &signals);

  auto origin = std::make_shared<jdblance::Origin>(name, latency_ms / 1000.0, jitter_ms / 1000.0,
                                                static_cast<int>(capacity), fail_rate);
  try {
    origin->listen(host, port);
  } catch (const std::exception& e) {
    std::fprintf(stderr, "jdblance-origin: %s\n", e.what());
    return 1;
  }
  std::printf("origin %s on %s:%d (latency=%gms jitter=%gms capacity=%s fail_rate=%g)\n",
              name.c_str(), host.c_str(), origin->port(), latency_ms, jitter_ms,
              capacity > 0 ? std::to_string(static_cast<int>(capacity)).c_str() : "inf", fail_rate);
  std::fflush(stdout);
  loop.run();
  origin->close();
  loop.del_fd(sfd);
  ::close(sfd);
  return 0;
}
