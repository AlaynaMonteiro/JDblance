#include "server.hpp"

#include <signal.h>
#include <sys/signalfd.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "balancer.hpp"
#include "log.hpp"
#include "metrics.hpp"

namespace jdblance {

namespace {
constexpr const char* kBanner = R"(
     _  ____   _      _
    | ||  _ \ | |__  | |  __ _  _ __    ___   ___    L7 load balancer
 _  | || | | || '_ \ | | / _` || '_ \  / __| / _ \   zero dependencies, C++20
| |_| || |_| || |_) || || (_| || | | || (__ |  __/   %s
 \___/ |____/ |_.__/ |_| \__,_||_| |_| \___| \___|
)";

constexpr const char* kUsage =
    "usage: jdblance [-c CONFIG] [--listen HOST:PORT] [--policy NAME] [--workers N]\n"
    "             [--endpoint NAME=HOST:PORT[:WEIGHT]]... [--check] [--version]\n"
    "\n"
    "  -c, --config PATH   config file (default: $JDBLANCE_CONFIG or lb.json)\n"
    "  --listen HOST:PORT  override listen.host / listen.port\n"
    "  --policy NAME       override the balancing policy\n"
    "  --workers N         override listen.workers\n"
    "  --endpoint SPEC     replace the endpoint list (repeatable); kept across reloads\n"
    "  --check             validate the configuration and exit\n"
    "  --version           print the version and exit\n";

bool parse_int(std::string_view s, int lo, int hi, int* out) {
  if (s.empty() || s.size() > 9) return false;
  int v = 0;
  for (char c : s) {
    if (c < '0' || c > '9') return false;
    v = v * 10 + (c - '0');
  }
  if (v < lo || v > hi) return false;
  *out = v;
  return true;
}

void block_signals(std::initializer_list<int> signals, sigset_t* set) {
  sigemptyset(set);
  for (int s : signals) sigaddset(set, s);
  ::pthread_sigmask(SIG_BLOCK, set, nullptr);
}

std::string names_list(const std::vector<std::string>& names) {
  std::string out = "[";
  for (size_t i = 0; i < names.size(); ++i) out += (i ? ", " : "") + names[i];
  return out + "]";
}
}  // namespace

// ---------------------------------------------------------------------------
// CLI overrides
// ---------------------------------------------------------------------------

void CliOverrides::apply(Config& cfg) const {
  if (listen_host) cfg.listen.host = *listen_host;
  if (listen_port) cfg.listen.port = *listen_port;
  if (policy) cfg.policy = *policy;
  if (workers) cfg.listen.workers = *workers;
  if (!endpoints.empty()) cfg.endpoints = endpoints;
}

EndpointConfig parse_endpoint_override(const std::string& spec) {
  auto fail = [&]() -> ConfigError {
    return ConfigError("--endpoint expects NAME=HOST:PORT[:WEIGHT], got \"" + spec + "\"");
  };
  size_t eq = spec.find('=');
  if (eq == std::string::npos || eq == 0) throw fail();
  EndpointConfig e;
  e.name = spec.substr(0, eq);
  std::string rest = spec.substr(eq + 1);
  std::string host;
  std::vector<std::string> tail;
  if (!rest.empty() && rest[0] == '[') {
    size_t close = rest.find(']');
    if (close == std::string::npos || close + 1 >= rest.size() || rest[close + 1] != ':') throw fail();
    host = rest.substr(0, close + 1);
    rest = rest.substr(close + 2);
  } else {
    size_t colon = rest.find(':');
    if (colon == std::string::npos) throw fail();
    host = rest.substr(0, colon);
    rest = rest.substr(colon + 1);
  }
  size_t colon = rest.find(':');
  std::string port = rest.substr(0, colon);
  if (colon != std::string::npos) {
    int weight = 0;
    if (!parse_int(std::string_view(rest).substr(colon + 1), 1, 1000000, &weight)) throw fail();
    e.weight = weight;
  }
  if (host.empty() || port.empty()) throw fail();
  e.address = host + ":" + port;
  return e;
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

Worker::Worker(Config cfg, int worker_id, CliOverrides overrides)
    : cfg_(std::move(cfg)),
      worker_id_(worker_id),
      overrides_(std::move(overrides)),
      cluster_(cfg_),
      proxy_(cfg_, cluster_, worker_id),
      admin_(cfg_, cluster_, proxy_, [this] { return reload(); }, worker_id) {}

void Worker::on_signal(int signo) {
  // Runs during event dispatch: only flags and events, never real work.
  if (signo == SIGHUP) {
    reload_requested_.set();
    return;
  }
  if (stopping_) {
    log::warning("second signal: exiting now");
    log::flush();
    ::_exit(1);
  }
  stopping_ = true;
  log::info("worker " + std::to_string(worker_id_) + " draining (" +
            format_number(cfg_.timeouts.drain) + "s budget)");
  stop_.set();
}

int Worker::run(int listen_fd) {
  reseed_rng();   // a forked child must not replay its siblings' P2C choices
  log::set_level(log::parse_level(cfg_.log.level));
  metrics::BUILD_INFO.set({kVersion, std::to_string(worker_id_)}, 1);

  sigset_t set;
  block_signals({SIGTERM, SIGINT, SIGHUP}, &set);
  int sfd = ::signalfd(-1, &set, SFD_NONBLOCK | SFD_CLOEXEC);
  if (sfd < 0) throw IoError(errno, "signalfd");
  FdState signal_state;
  signal_state.on_readable = [this, sfd] {
    signalfd_siginfo info;
    while (::read(sfd, &info, sizeof info) == static_cast<ssize_t>(sizeof info)) {
      on_signal(static_cast<int>(info.ssi_signo));
    }
  };
  loop_.add_fd(sfd, &signal_state);

  auto listener = std::make_shared<Socket>(listen_fd);
  int code = 0;
  try {
    if (cfg_.admin.enabled) {
      int port = cfg_.admin.port == 0 ? 0 : cfg_.admin.port + worker_id_;
      admin_.start(cfg_.admin.host, port);
      log::info("worker " + std::to_string(worker_id_) + " admin on http://" + cfg_.admin.host + ":" +
                std::to_string(admin_.port()));
    }
    cluster_.start_health();
    log::info("worker " + std::to_string(worker_id_) + " serving " + cfg_.listen.host + ":" +
              std::to_string(listener->local_port()) + " -> " +
              std::to_string(cluster_.endpoints().size()) + " endpoints in " +
              std::to_string(cluster_.upstreams().size()) + " upstreams (" + cluster_.primary().balancer().name() + ")");
    loop_.run_until_complete(serve(listener));
  } catch (const std::exception& e) {
    log::error("worker " + std::to_string(worker_id_) + ": " + e.what());
    code = 1;
  }
  loop_.del_fd(sfd);
  ::close(sfd);
  log::flush();
  return code;
}

Task<void> Worker::accept_loop(Proxy* proxy, SocketPtr listener) {
  for (;;) {
    SocketPtr socket;
    std::string failure;
    try {
      socket = co_await accept_one(*listener);
    } catch (const CancelledError&) {
      co_return;
    } catch (const std::exception& e) {
      failure = e.what();
    }
    if (!socket) {
      if (listener->closed()) co_return;
      log::error("accept: " + failure);
      co_await sleep_for(0.1);
      continue;
    }
    spawn(proxy->handle_client(std::move(socket)));
  }
}

Task<void> Worker::serve(SocketPtr listener) {
  spawn(accept_loop(&proxy_, listener));
  spawn(reload_loop());
  spawn(reaper_loop());
  spawn(flush_loop());

  co_await stop_.wait();
  listener->close();   // stop accepting; established connections live on
  proxy_.start_drain();
  double deadline = monotonic() + cfg_.timeouts.drain;
  while (proxy_.connections() > 0 && monotonic() < deadline) co_await sleep_for(0.05);
  if (proxy_.connections() > 0) {
    log::warning("worker " + std::to_string(worker_id_) + " forcing close of " +
                 std::to_string(proxy_.connections()) + " connections");
    proxy_.close_all();
    double grace = monotonic() + 1.0;
    while (proxy_.connections() > 0 && monotonic() < grace) co_await sleep_for(0.02);
  }

  reload_requested_.set();
  reaper_stop_.set();
  flush_stop_.set();
  co_await cluster_.health.stop_and_wait();
  admin_.stop();
  for (const EndpointPtr& e : cluster_.endpoints()) e->pool.clear();
  co_await sleep_for(0.01);   // let the background loops see their stop events
  log::info("worker " + std::to_string(worker_id_) + " stopped");
}

Task<void> Worker::reload_loop() {
  for (;;) {
    co_await reload_requested_.wait();
    reload_requested_.clear();
    if (stopping_) co_return;
    std::pair<bool, std::string> result = reload();
    metrics::CONFIG_RELOADS.inc({result.first ? "ok" : "error"});
    if (result.first) {
      log::info("SIGHUP reload: " + result.second);
    } else {
      log::warning("SIGHUP reload failed: " + result.second);
    }
  }
}

Task<void> Worker::reaper_loop() {
  // Close idle upstream sockets that outlived their keep-alive window, or an
  // idle proxy discovers they are all dead at once when traffic returns.
  for (;;) {
    bool stop = co_await reaper_stop_.wait(monotonic() + std::max(1.0, cfg_.pool.idle_timeout / 2));
    if (stop) co_return;
    for (const EndpointPtr& e : cluster_.endpoints()) {
      e->pool.reap();
      metrics::POOL_IDLE.set({e->name}, static_cast<double>(e->pool.size()));
    }
  }
}

Task<void> Worker::flush_loop() {
  for (;;) {
    bool stop = co_await flush_stop_.wait(monotonic() + 0.2);
    log::flush();
    if (stop) co_return;
  }
}

std::pair<bool, std::string> Worker::reload() {
  if (cfg_.source_path.empty()) return {false, "no config file to reload"};
  Config fresh;
  try {
    fresh = Config::load(cfg_.source_path, false);
    overrides_.apply(fresh);   // command-line settings survive reloads
    fresh.validate();
  } catch (const ConfigError& e) {
    return {false, e.what()};
  }
  if (!(fresh.listen == cfg_.listen)) return {false, "listen settings cannot be changed without a restart"};
  if (!(fresh.admin == cfg_.admin)) return {false, "admin settings cannot be changed without a restart"};

  std::vector<std::string> before;
  for (const EndpointPtr& e : cluster_.endpoints()) before.push_back(e->name);
  std::string source = cfg_.source_path;
  cfg_ = std::move(fresh);
  cfg_.source_path = source;
  log::set_level(log::parse_level(cfg_.log.level));
  cluster_.apply_config(cfg_);

  std::vector<std::string> after, added, removed;
  for (const EndpointPtr& e : cluster_.endpoints()) after.push_back(e->name);
  for (const auto& n : after) {
    if (std::find(before.begin(), before.end(), n) == before.end()) added.push_back(n);
  }
  for (const auto& n : before) {
    if (std::find(after.begin(), after.end(), n) == after.end()) removed.push_back(n);
  }
  return {true, "policy=" + cfg_.policy + " endpoints=" + std::to_string(after.size()) +
                    " added=" + names_list(added) + " removed=" + names_list(removed)};
}

// ---------------------------------------------------------------------------
// Supervisor
// ---------------------------------------------------------------------------

int supervise(const Config& cfg, const CliOverrides& overrides) {
  int workers = cfg.listen.workers;
  if (workers > 1 && !cfg.listen.reuse_port) {
    log::warning("multi-worker needs listen.reuse_port; running 1 worker");
    workers = 1;
  }
  if (workers > 1 && cfg.listen.port == 0) {
    log::warning("multi-worker needs a fixed listen.port; running 1 worker");
    workers = 1;
  }

  // Blocked before any fork or thread, so every process inherits the mask and
  // signals arrive only where they are waited for.
  sigset_t set;
  block_signals({SIGTERM, SIGINT, SIGHUP, SIGCHLD}, &set);

  auto bind = [&]() {
    return listen_tcp(cfg.listen.host, cfg.listen.port, cfg.listen.backlog, cfg.listen.reuse_port);
  };

  if (workers == 1) {
    int fd;
    try {
      fd = bind();
    } catch (const std::exception& e) {
      log::error(e.what());
      return 1;
    }
    Worker worker(cfg, 0, overrides);
    return worker.run(fd);
  }

  // Probe the bind once here: a busy port should be one clear error, not N
  // children crash-looping on it.
  try {
    ::close(bind());
  } catch (const std::exception& e) {
    log::error(e.what());
    return 1;
  }

  auto start_worker = [&](int id) -> pid_t {
    log::flush();
    pid_t pid = ::fork();
    if (pid != 0) return pid;
    // Child: bind its *own* SO_REUSEPORT socket, so the kernel treats the
    // workers as independent accept queues.
    int code = 1;
    try {
      int fd = bind();
      Worker worker(cfg, id, overrides);
      code = worker.run(fd);
    } catch (const std::exception& e) {
      std::fprintf(stderr, "jdblance worker %d: %s\n", id, e.what());
    }
    log::flush();
    ::_exit(code);
  };

  std::vector<pid_t> pids(static_cast<size_t>(workers), 0);
  std::vector<double> started(static_cast<size_t>(workers), 0.0);
  for (int id = 0; id < workers; ++id) {
    pids[static_cast<size_t>(id)] = start_worker(id);
    started[static_cast<size_t>(id)] = monotonic();
  }
  std::string list;
  for (pid_t p : pids) list += (list.empty() ? "" : ", ") + std::to_string(p);
  log::info("supervisor pid " + std::to_string(::getpid()) + " with workers [" + list + "]");

  bool shutting_down = false;
  int exit_code = 0;
  for (;;) {
    timespec wait{0, 200 * 1000 * 1000};
    siginfo_t info;
    int sig = ::sigtimedwait(&set, &info, &wait);
    if (sig == SIGTERM || sig == SIGINT || sig == SIGHUP) {
      if (sig != SIGHUP) shutting_down = true;
      for (pid_t p : pids) {
        if (p > 0) ::kill(p, sig);
      }
    }
    int status = 0;
    pid_t done;
    while ((done = ::waitpid(-1, &status, WNOHANG)) > 0) {
      auto it = std::find(pids.begin(), pids.end(), done);
      if (it == pids.end()) continue;
      size_t id = static_cast<size_t>(it - pids.begin());
      *it = 0;
      std::string how;
      int code = 0;
      if (WIFEXITED(status)) {
        code = WEXITSTATUS(status);
        how = "exit " + std::to_string(code);
      } else if (WIFSIGNALED(status)) {
        code = 128 + WTERMSIG(status);
        how = std::string("signal ") + ::strsignal(WTERMSIG(status));
      }
      // A crash that was answered with a respawn is history; only how the
      // workers end during shutdown decides the supervisor's exit code.
      if (shutting_down && code != 0) exit_code = code;
      if (!shutting_down) {
        log::warning("worker " + std::to_string(id) + " (pid " + std::to_string(done) + ") died (" +
                     how + "); respawning");
      }
    }
    if (shutting_down) {
      if (std::all_of(pids.begin(), pids.end(), [](pid_t p) { return p == 0; })) break;
      continue;
    }
    for (size_t id = 0; id < pids.size(); ++id) {
      // At most one respawn per slot per second: a worker that dies on start
      // must not turn into a fork bomb.
      if (pids[id] == 0 && monotonic() - started[id] >= 1.0) {
        pids[id] = start_worker(static_cast<int>(id));
        started[id] = monotonic();
      }
    }
  }
  return exit_code;
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

int main_cli(int argc, char** argv) {
  const char* env_config = std::getenv("JDBLANCE_CONFIG");
  std::string config_path = env_config && *env_config ? env_config : "lb.json";
  CliOverrides overrides;
  bool check = false;

  try {
    for (int i = 1; i < argc; ++i) {
      std::string arg = argv[i];
      std::optional<std::string> inline_value;
      if (arg.rfind("--", 0) == 0) {
        size_t eq = arg.find('=');
        if (eq != std::string::npos) {
          inline_value = arg.substr(eq + 1);
          arg = arg.substr(0, eq);
        }
      }
      auto value = [&]() -> std::string {
        if (inline_value) return *inline_value;
        if (i + 1 >= argc) throw ConfigError(arg + " requires a value");
        return argv[++i];
      };
      if (arg == "-c" || arg == "--config") {
        config_path = value();
      } else if (arg == "--listen") {
        std::string v = value();
        size_t colon = v.rfind(':');
        int port = 0;
        if (colon == std::string::npos || !parse_int(std::string_view(v).substr(colon + 1), 0, 65535, &port)) {
          throw ConfigError("--listen expects HOST:PORT, got \"" + v + "\"");
        }
        std::string host = v.substr(0, colon);
        overrides.listen_host = host.empty() ? "0.0.0.0" : host;
        overrides.listen_port = port;
      } else if (arg == "--policy") {
        overrides.policy = value();
      } else if (arg == "--workers") {
        std::string v = value();
        int workers = 0;
        if (!parse_int(v, 1, 256, &workers)) throw ConfigError("--workers expects 1..256, got \"" + v + "\"");
        overrides.workers = workers;
      } else if (arg == "--endpoint") {
        overrides.endpoints.push_back(parse_endpoint_override(value()));
      } else if (arg == "--check") {
        check = true;
      } else if (arg == "--version") {
        std::printf("jdblance %s\n", kVersion);
        return 0;
      } else if (arg == "-h" || arg == "--help") {
        std::fputs(kUsage, stdout);
        return 0;
      } else {
        std::fprintf(stderr, "jdblance: unknown argument \"%s\"\n\n%s", arg.c_str(), kUsage);
        return 2;
      }
    }
  } catch (const ConfigError& e) {
    std::fprintf(stderr, "jdblance: %s\n", e.what());
    return 2;
  }

  Config cfg;
  try {
    cfg = Config::load(config_path, false);
    overrides.apply(cfg);
    cfg.validate();
  } catch (const ConfigError& e) {
    std::fprintf(stderr, "config error: %s\n", e.what());
    return 2;
  }

  if (check) {
    size_t endpoints = 0;
    std::vector<UpstreamConfig> pools = cfg.all_upstreams();
    for (const UpstreamConfig& up : pools) endpoints += up.endpoints.size();
    std::printf("config OK: %zu endpoints in %zu upstreams, %zu routes, policy=%s\n", endpoints, pools.size(),
                cfg.routes.size(), cfg.policy.c_str());
    return 0;
  }
  std::printf(kBanner, kVersion);
  std::fflush(stdout);
  long fds = raise_fd_limit();
  if (fds >= 0 && fds < 4096) {
    log::warning("open-file limit is " + std::to_string(fds) +
                 "; each proxied request holds two sockets -- raise it (ulimit -n)");
  }
  try {
    return supervise(cfg, overrides);
  } catch (const std::exception& e) {
    log::error(e.what());
    return 1;
  }
}

}  // namespace jdblance
