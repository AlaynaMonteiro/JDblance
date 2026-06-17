// jdblance-alertbot: alerting sidecar driven by the balancer's own view of the fleet.
//
// It reads GET /stats on the control plane instead of keeping a second list of
// backends, so the alert set is by definition the set being routed to.  It
// also sees what an HTTP-200 prober cannot: breaker trips, panic mode, and
// sustained shedding.  Alerts are edge-triggered: they fire on transitions.
//
//   jdblance-alertbot --admin http://localhost:8001
//   jdblance-alertbot --admin http://localhost:8001 --once      # for cron / CI
//
// WhatsApp delivery through Twilio is optional: set TWILIO_ACCOUNT_SID,
// TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_NUMBER and YOUR_WHATSAPP_NUMBER (or put
// them in ./.env).  The HTTPS call is made by `curl`, with the credentials fed
// on its stdin so they never appear in the process list.  Without
// credentials, alerts go to stdout.
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <optional>
#include <set>
#include <string>

#include "http1.hpp"
#include "json.hpp"
#include "loop.hpp"

using namespace jdblance;

namespace {

void load_dotenv(const char* path) {
  std::ifstream in(path);
  std::string line;
  while (std::getline(in, line)) {
    std::string_view s = http::trim(line);
    if (s.empty() || s[0] == '#') continue;
    size_t eq = s.find('=');
    if (eq == std::string_view::npos) continue;
    std::string key(http::trim(s.substr(0, eq)));
    std::string value(http::trim(s.substr(eq + 1)));
    if (value.size() >= 2 && (value.front() == '"' || value.front() == '\'') && value.back() == value.front()) {
      value = value.substr(1, value.size() - 2);
    }
    if (!key.empty()) ::setenv(key.c_str(), value.c_str(), 0);   // never override the environment
  }
}

std::string env(const char* name) {
  const char* v = std::getenv(name);
  return v ? v : "";
}

// A double-quoted curl config value.
std::string curl_quote(const std::string& s) {
  std::string out = "\"";
  for (char c : s) {
    if (c == '"' || c == '\\') out.push_back('\\');
    if (c == '\n') {
      out += "\\n";
      continue;
    }
    out.push_back(c);
  }
  return out + "\"";
}

class Notifier {
 public:
  Notifier()
      : sid_(env("TWILIO_ACCOUNT_SID")),
        token_(env("TWILIO_AUTH_TOKEN")),
        sender_(env("TWILIO_WHATSAPP_NUMBER")),
        recipient_(env("YOUR_WHATSAPP_NUMBER")),
        api_base_(env("TWILIO_API_BASE").empty() ? "https://api.twilio.com" : env("TWILIO_API_BASE")) {}

  bool enabled() const { return !sid_.empty() && !token_.empty() && !sender_.empty() && !recipient_.empty(); }

  void send(const std::string& message) {
    std::time_t now = std::time(nullptr);
    char stamp[16];
    std::strftime(stamp, sizeof stamp, "%H:%M:%S", std::localtime(&now));
    std::printf("[%s] %s\n", stamp, message.c_str());
    std::fflush(stdout);
    if (!enabled()) return;
    std::string config = "url = " + curl_quote(api_base_ + "/2010-04-01/Accounts/" + sid_ + "/Messages.json") +
                         "\nuser = " + curl_quote(sid_ + ":" + token_) +
                         "\ndata-urlencode = " + curl_quote("From=" + sender_) +
                         "\ndata-urlencode = " + curl_quote("To=" + recipient_) +
                         "\ndata-urlencode = " + curl_quote("Body=" + message) +
                         "\nmax-time = 10\nsilent\nshow-error\nfail\noutput = \"/dev/null\"\n";
    if (!run_curl(config)) std::fprintf(stderr, "  ! twilio delivery failed\n");
  }

 private:
  static bool run_curl(const std::string& config) {
    int fds[2];
    if (::pipe(fds) != 0) return false;
    pid_t pid = ::fork();
    if (pid < 0) {
      ::close(fds[0]);
      ::close(fds[1]);
      return false;
    }
    if (pid == 0) {
      ::dup2(fds[0], STDIN_FILENO);
      ::close(fds[0]);
      ::close(fds[1]);
      ::execlp("curl", "curl", "--config", "-", static_cast<char*>(nullptr));
      ::_exit(127);
    }
    ::close(fds[0]);
    const char* p = config.data();
    size_t left = config.size();
    while (left > 0) {
      ssize_t n = ::write(fds[1], p, left);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) break;
      p += n;
      left -= static_cast<size_t>(n);
    }
    ::close(fds[1]);
    int status = 0;
    while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {
    }
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
  }

  std::string sid_, token_, sender_, recipient_, api_base_;
};

std::optional<Json> fetch_stats(Loop& loop, const std::string& host, int port) {
  try {
    http::FetchResult r = loop.run_until_complete(http::fetch(host, port, "GET", "/stats", {}, "", 5.0));
    if (r.response.status != 200) return std::nullopt;
    return Json::parse(r.body);
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

std::string text(const Json* v, const char* fallback = "unknown") {
  if (v == nullptr || v->is_null()) return fallback;
  if (v->is_string()) return v->as_string();
  return v->dump();
}

class Watcher {
 public:
  Watcher(Notifier& notifier, int shed_threshold) : notifier_(notifier), shed_threshold_(shed_threshold) {}

  void poll(Loop& loop, const std::string& admin, const std::string& host, int port) {
    std::optional<Json> stats = fetch_stats(loop, host, port);
    if (!stats || !stats->is_object()) {
      if (!proxy_down_) {
        proxy_down_ = true;
        notifier_.send("CRITICAL: load balancer control plane unreachable at " + admin);
      }
      return;
    }
    if (proxy_down_) {
      proxy_down_ = false;
      notifier_.send("RECOVERY: load balancer control plane is answering again");
    }
    if (const Json* eps = stats->find("endpoints"); eps && eps->is_array()) {
      for (const Json& e : eps->as_array()) check_endpoint(e);
    }
    check_panic(*stats);
    check_shedding(*stats);
  }

 private:
  void check_endpoint(const Json& e) {
    if (!e.is_object()) return;
    std::string name = text(e.find("name"));
    std::string address = text(e.find("address"));
    const Json* h = e.find("healthy");
    const Json* en = e.find("enabled");
    bool healthy = h && h->is_bool() && h->as_bool() && en && en->is_bool() && en->as_bool();
    if (!healthy && !down_.count(name)) {
      down_.insert(name);
      notifier_.send("DOWN: " + name + " (" + address + ") failed health checks -- last error: " +
                     text(e.find("last_error")));
    } else if (healthy && down_.count(name)) {
      down_.erase(name);
      notifier_.send("RECOVERY: " + name + " (" + address + ") is back in rotation");
    }
    const Json* breaker = e.find("breaker");
    std::string state = breaker ? text(breaker->find("state"), "") : "";
    if (state == "open" && !tripped_.count(name)) {
      tripped_.insert(name);
      notifier_.send("BREAKER OPEN: " + name + " ejected after " + text(breaker->find("window_failures")) +
                     " failures in the last window; retrying in " + text(breaker->find("reopens_in")) + "s");
    } else if (state == "closed" && tripped_.count(name)) {
      tripped_.erase(name);
      notifier_.send("BREAKER CLOSED: " + name + " passed its probe and is serving again");
    }
  }

  void check_panic(const Json& stats) {
    const Json* p = stats.find("panic_mode");
    bool panic = p && p->is_bool() && p->as_bool();
    if (panic && !panic_) {
      panic_ = true;
      notifier_.send("PANIC MODE: only " + text(stats.find("healthy")) + "/" + text(stats.find("total")) +
                     " endpoints healthy; the balancer is ignoring health checks and spraying all backends");
    } else if (!panic && panic_) {
      panic_ = false;
      notifier_.send("PANIC MODE CLEARED: enough endpoints are healthy again");
    }
  }

  void check_shedding(const Json& stats) {
    const Json* totals = stats.find("totals");
    const Json* shed_v = totals ? totals->find("shed") : nullptr;
    double shed = shed_v && shed_v->is_number() ? shed_v->as_number() : 0;
    if (!last_shed_) {
      last_shed_ = shed;
      return;
    }
    double delta = shed - *last_shed_;
    last_shed_ = shed;
    if (delta >= shed_threshold_) {
      const Json* adm = stats.find("admission");
      notifier_.send("OVERLOAD: " + format_number(delta) + " requests shed since the last check (concurrency limit " +
                     text(adm ? adm->find("limit") : nullptr) + ", queue " +
                     text(adm ? adm->find("queued") : nullptr) + ")");
    }
  }

  Notifier& notifier_;
  int shed_threshold_;
  std::set<std::string> down_;
  std::set<std::string> tripped_;
  bool panic_ = false;
  bool proxy_down_ = false;
  std::optional<double> last_shed_;
};

volatile sig_atomic_t g_stop = 0;
void on_stop(int) { g_stop = 1; }

const char* kUsage =
    "usage: jdblance-alertbot [--admin URL] [--interval SECONDS] [--shed-threshold N] [--once]\n"
    "  --admin           control plane base URL (default $JDBLANCE_ADMIN or http://localhost:8001)\n";

}  // namespace

int main(int argc, char** argv) {
  std::string admin = env("JDBLANCE_ADMIN").empty() ? "http://localhost:8001" : env("JDBLANCE_ADMIN");
  double interval = 10;
  int shed_threshold = 50;
  bool once = false;
  try {
    for (int i = 1; i < argc; ++i) {
      std::string a = argv[i];
      auto value = [&]() -> std::string {
        if (i + 1 >= argc) throw std::invalid_argument(a + " requires a value");
        return argv[++i];
      };
      if (a == "--admin") admin = value();
      else if (a == "--interval") interval = std::stod(value());
      else if (a == "--shed-threshold") shed_threshold = std::stoi(value());
      else if (a == "--once") once = true;
      else if (a == "-h" || a == "--help") {
        std::fputs(kUsage, stdout);
        return 0;
      } else throw std::invalid_argument("unknown argument " + a);
    }
    if (interval <= 0) throw std::invalid_argument("--interval must be > 0");
  } catch (const std::exception& e) {
    std::fprintf(stderr, "jdblance-alertbot: %s\n%s", e.what(), kUsage);
    return 2;
  }

  // http://host[:port]
  std::string rest = admin.rfind("http://", 0) == 0 ? admin.substr(7) : "";
  while (!rest.empty() && rest.back() == '/') rest.pop_back();
  if (rest.empty() || rest.find('/') != std::string::npos) {
    std::fprintf(stderr, "jdblance-alertbot: --admin must look like http://host:port\n");
    return 2;
  }
  std::string host = rest;
  int port = 80;
  size_t colon = rest.rfind(':');
  if (colon != std::string::npos && rest.find(']', colon) == std::string::npos) {
    host = rest.substr(0, colon);
    port = std::atoi(rest.c_str() + colon + 1);
  }

  load_dotenv(".env");
  Notifier notifier;
  std::printf("watching %s every %gs (twilio %s)\n", admin.c_str(), interval,
              notifier.enabled() ? "enabled" : "disabled -- console only");
  std::fflush(stdout);

  ::signal(SIGINT, on_stop);
  ::signal(SIGTERM, on_stop);
  Loop loop;
  Watcher watcher(notifier, shed_threshold);
  watcher.poll(loop, admin, host, port);
  if (once) return 0;
  while (!g_stop) {
    timespec ts{static_cast<time_t>(interval), static_cast<long>((interval - static_cast<time_t>(interval)) * 1e9)};
    ::nanosleep(&ts, nullptr);   // EINTR on SIGTERM is exactly what we want
    if (g_stop) break;
    watcher.poll(loop, admin, host, port);
  }
  return 0;
}
