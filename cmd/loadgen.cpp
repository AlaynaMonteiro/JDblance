// jdblance-loadgen: open-loop HTTP load generator with coordinated-omission
// correction.
//
// `ab` and `wrk` default to a closed loop: each connection sends its next
// request only after the previous response arrives, so when the server stalls
// the client stops sending and the stall never shows up in the results.  This
// generator schedules requests from a Poisson arrival process at a fixed rate,
// independently of what has completed, and measures latency from the
// *intended* send time -- a 2 s stall at 1000 rps shows up as ~2000 requests
// with latencies from 2 s down to 0, which is what users experienced.
//
//   jdblance-loadgen http://localhost:8000/ --rate 2000 --duration 20
//   jdblance-loadgen http://localhost:8000/ --concurrency 64        # closed loop
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <map>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "balancer.hpp"
#include "http1.hpp"
#include "json.hpp"
#include "loop.hpp"

using namespace jdblance;

namespace {

struct Options {
  std::string url;
  double rate = 0;
  int concurrency = 0;
  int connections = 0;
  double duration = 10;
  std::string method = "GET";
  std::string body;
  double timeout = 10;
  bool uniform = false;
  bool json = false;
  double max_error_rate = -1;
};

// Exact percentiles: every sample is kept.  60 s at 20k rps is 1.2 M doubles,
// ~10 MB, and sorting that takes well under a second.  Exactness matters
// when the point of the tool is to argue about tails.
struct Recorder {
  std::vector<double> latencies;
  std::vector<double> service;
  std::map<int, uint64_t> statuses;
  std::map<std::string, uint64_t> errors;
  std::map<std::string, uint64_t> upstreams;
  std::map<std::string, uint64_t> attempts;
  std::map<long, uint64_t> per_second;
  std::map<long, std::vector<double>> per_second_latency;
  uint64_t bytes = 0;
  uint64_t hedged = 0;
  double wall = 1;
  uint64_t opened = 0;

  void record(double intended, double sent, double finished, int status, const std::string& upstream,
              uint64_t size, const std::string& attempt, bool was_hedged) {
    double latency = finished - intended;
    latencies.push_back(latency);
    service.push_back(finished - sent);
    ++statuses[status];
    ++upstreams[upstream.empty() ? "-" : upstream];
    ++attempts[attempt.empty() ? "1" : attempt];
    bytes += size;
    if (was_hedged) ++hedged;
    long second = static_cast<long>(finished);
    ++per_second[second];
    per_second_latency[second].push_back(latency);
  }
  void error(const std::string& kind) { ++errors[kind]; }
};

double percentile(const std::vector<double>& sorted, double q) {
  if (sorted.empty()) return std::nan("");
  long index = static_cast<long>(std::ceil(q * static_cast<double>(sorted.size()))) - 1;
  index = std::clamp<long>(index, 0, static_cast<long>(sorted.size()) - 1);
  return sorted[static_cast<size_t>(index)];
}

// Counting semaphore for coroutines.
class Gate {
 public:
  explicit Gate(int slots) : available_(slots) {}
  Task<void> acquire() {
    if (available_ > 0) {
      --available_;
      co_return;
    }
    auto ev = std::make_shared<Event>();
    waiters_.push_back(ev);
    co_await ev->wait();
  }
  void release() {
    if (!waiters_.empty()) {
      auto ev = waiters_.front();   // hand the slot straight over
      waiters_.pop_front();
      ev->set();
    } else {
      ++available_;
    }
  }

 private:
  int available_;
  std::deque<std::shared_ptr<Event>> waiters_;
};

// Keep-alive pool: one connection per concurrent request.  HTTP/1.1 has no
// multiplexing, and writing a second request down a busy socket is
// pipelining, so an open-loop generator needs a pool, capped by the gate.
// Time spent waiting at the gate is real client-side queueing; because
// latency is measured from the intended send time, it lands in the results.
class Client {
 public:
  Client(Address address, std::string head, std::string body, std::string method, double timeout,
         int max_connections, Recorder* recorder)
      : address_(address),
        wire_(std::move(head) + body),
        method_(std::move(method)),
        timeout_(timeout),
        gate_(max_connections),
        recorder_(recorder) {}

  Task<void> request(double intended) {
    co_await gate_.acquire();
    double sent = monotonic();
    std::unique_ptr<Conn> conn;
    std::string failure;
    try {
      conn = co_await acquire_conn();
      sent = monotonic();
      double deadline = sent + timeout_;
      co_await conn->socket->write_all(wire_, deadline);
      http::Response response = co_await http::read_response(*conn->reader, deadline);
      http::Framing framing = http::response_framing(method_, response);
      uint64_t size = co_await http::relay_body(*conn->reader, http::Sink{}, framing, std::nullopt, deadline);
      double finished = monotonic();
      recorder_->record(intended, sent, finished, response.status, response.get_or("x-upstream", "-"),
                        size, response.get_or("x-attempts", "1"), response.has("x-hedged"));
      if (!response.connection_has("close") && framing.kind != http::BodyKind::Eof &&
          conn->reader->buffered() == 0) {
        idle_.push_back(std::move(conn));
      }
    } catch (const TimeoutError&) {
      failure = "timeout";
    } catch (const http::ProtocolError& e) {
      failure = "protocol:" + e.message;
    } catch (const ConnectionClosed&) {
      failure = "closed";
    } catch (const IoError& e) {
      failure = std::strerror(e.code());
    }
    if (!failure.empty()) recorder_->error(failure);
    if (conn) conn->socket->close();
    gate_.release();
  }

  void close_all() {
    for (auto& c : idle_) c->socket->close();
    idle_.clear();
  }
  uint64_t opened() const { return opened_; }

 private:
  struct Conn {
    SocketPtr socket;
    std::unique_ptr<BufReader> reader;
  };
  Task<std::unique_ptr<Conn>> acquire_conn() {
    while (!idle_.empty()) {
      std::unique_ptr<Conn> c = std::move(idle_.back());
      idle_.pop_back();
      if (c->socket->idle_and_open()) co_return c;
      c->socket->close();
    }
    ++opened_;
    auto c = std::make_unique<Conn>();
    c->socket = co_await connect_tcp(address_, monotonic() + timeout_);
    c->reader = std::make_unique<BufReader>(c->socket);
    co_return c;
  }

  Address address_;
  std::string wire_;
  std::string method_;
  double timeout_;
  Gate gate_;
  Recorder* recorder_;
  std::vector<std::unique_ptr<Conn>> idle_;
  uint64_t opened_ = 0;
};

struct Pending {
  int outstanding = 0;
  Event drained;
};

Task<void> tracked(Client* client, double intended, Pending* pending) {
  co_await client->request(intended);
  if (--pending->outstanding == 0) pending->drained.set();
}

// Fire at `rate` regardless of whether earlier requests came back.  Poisson
// gaps by default: real traffic arrives in clumps, and evenly spaced arrivals
// flatter the server by never presenting a burst.
Task<void> open_loop(Client* client, double rate, double deadline, bool poisson, Pending* pending) {
  std::exponential_distribution<double> gap(rate);
  double next_at = monotonic();
  for (;;) {
    next_at += poisson ? gap(rng()) : 1.0 / rate;
    if (next_at > deadline) co_return;
    double now = monotonic();
    if (next_at > now) co_await sleep_for(next_at - now);
    ++pending->outstanding;
    pending->drained.clear();
    spawn(tracked(client, next_at, pending));
  }
}

Task<void> closed_loop(Client* client, double deadline, Pending* pending) {
  while (monotonic() < deadline) co_await client->request(monotonic());
  if (--pending->outstanding == 0) pending->drained.set();
}

Task<void> run(const Options& opt, Client* client) {
  double started = monotonic();
  double deadline = started + opt.duration;
  Pending pending;
  if (opt.rate > 0) {
    co_await open_loop(client, opt.rate, deadline, !opt.uniform, &pending);
  } else {
    for (int i = 0; i < opt.concurrency; ++i) {
      ++pending.outstanding;
      spawn(closed_loop(client, deadline, &pending));
    }
  }
  // Let the tail land, but do not wait forever on a server that stopped.
  if (pending.outstanding > 0) co_await pending.drained.wait(monotonic() + opt.timeout + 1);
  client->close_all();
}

std::string fmt(const char* format, double v) {
  char buf[64];
  std::snprintf(buf, sizeof buf, format, v);
  return buf;
}

std::string with_commas(uint64_t v) {
  std::string s = std::to_string(v);
  for (int i = static_cast<int>(s.size()) - 3; i > 0; i -= 3) s.insert(static_cast<size_t>(i), ",");
  return s;
}

Json summary_of(Recorder& r) {
  std::vector<double> lat = r.latencies;
  std::vector<double> svc = r.service;
  std::sort(lat.begin(), lat.end());
  std::sort(svc.begin(), svc.end());
  uint64_t errors = 0;
  for (auto& [_, c] : r.errors) errors += c;
  Json s = Json::object();
  s["requests"] = lat.size();
  s["errors"] = errors;
  s["duration_s"] = round_to(r.wall, 3);
  s["throughput_rps"] = r.wall > 0 ? round_to(static_cast<double>(lat.size()) / r.wall, 1) : 0.0;
  s["bytes"] = r.bytes;
  Json l = Json::object();
  const std::pair<const char*, double> qs[] = {{"p50", .5},    {"p75", .75},     {"p90", .9},
                                               {"p99", .99},   {"p99.9", .999}, {"p99.99", .9999},
                                               {"max", 1.0}};
  for (auto& [name, q] : qs) l[name] = round_to(percentile(lat, q) * 1000, 3);
  s["latency_ms"] = l;
  Json sv = Json::object();
  for (auto& [name, q] : {std::pair<const char*, double>{"p50", .5}, {"p99", .99}, {"max", 1.0}}) {
    sv[name] = round_to(percentile(svc, q) * 1000, 3);
  }
  s["service_ms"] = sv;
  double mean = lat.empty() ? 0 : std::accumulate(lat.begin(), lat.end(), 0.0) / static_cast<double>(lat.size());
  s["mean_ms"] = lat.empty() ? Json() : Json(round_to(mean * 1000, 3));
  Json st = Json::object();
  for (auto& [code, c] : r.statuses) st[std::to_string(code)] = c;
  s["statuses"] = st;
  Json ed = Json::object();
  for (auto& [k, c] : r.errors) ed[k] = c;
  s["errors_detail"] = ed;
  Json up = Json::object();
  for (auto& [k, c] : r.upstreams) up[k] = c;
  s["upstreams"] = up;
  Json at = Json::object();
  for (auto& [k, c] : r.attempts) at[k] = c;
  s["attempts"] = at;
  s["hedged"] = r.hedged;
  if (!r.upstreams.empty()) {
    std::vector<double> counts;
    for (auto& [_, c] : r.upstreams) counts.push_back(static_cast<double>(c));
    double lo = *std::min_element(counts.begin(), counts.end());
    double hi = *std::max_element(counts.begin(), counts.end());
    double m = std::accumulate(counts.begin(), counts.end(), 0.0) / static_cast<double>(counts.size());
    double var = 0;
    for (double c : counts) var += (c - m) * (c - m);
    var /= static_cast<double>(counts.size());
    Json b = Json::object();
    b["min"] = lo;
    b["max"] = hi;
    b["imbalance"] = round_to(hi / std::max(1.0, lo), 3);
    b["cv"] = counts.size() > 1 && m > 0 ? round_to(std::sqrt(var) / m, 4) : 0.0;
    s["balance"] = b;
  }
  return s;
}

void report(Recorder& r, const Json& s) {
  size_t total = r.latencies.size();
  uint64_t errors = static_cast<uint64_t>(s.find("errors")->as_number());
  uint64_t ok = 0;
  for (auto& [code, c] : r.statuses) {
    if (code < 400) ok += c;
  }
  std::printf("\n  requests   %s  (%s rps over %.1fs)\n", with_commas(total).c_str(),
              fmt("%.1f", s.find("throughput_rps")->as_number()).c_str(), r.wall);
  if (total) {
    std::printf("  success    %s  (%.2f%%)\n", with_commas(ok).c_str(), 100.0 * static_cast<double>(ok) / static_cast<double>(total));
  } else {
    std::printf("  success    0\n");
  }
  std::string detail;
  for (auto& [k, c] : r.errors) detail += (detail.empty() ? "" : ", ") + k + ": " + std::to_string(c);
  std::printf("  errors     %s  %s\n", with_commas(errors).c_str(), detail.empty() ? "" : ("{" + detail + "}").c_str());
  std::printf("  transfer   %.2f MB  (%.2f MB/s)\n", static_cast<double>(r.bytes) / 1e6,
              static_cast<double>(r.bytes) / r.wall / 1e6);
  std::printf("  sockets    %s opened (%.1f requests each)\n", with_commas(r.opened).c_str(),
              static_cast<double>(total) / static_cast<double>(std::max<uint64_t>(1, r.opened)));
  std::printf("\n  latency (corrected for coordinated omission)\n");
  const Json& l = *s.find("latency_ms");
  double mx = l.find("max")->as_number();
  for (const char* name : {"p50", "p75", "p90", "p99", "p99.9", "p99.99", "max"}) {
    double v = l.find(name)->as_number();
    int bars = mx > 0 ? std::min(48, static_cast<int>(v / mx * 48)) : 0;
    std::printf("    %-7s %10.3f ms  %s\n", name, v, std::string(static_cast<size_t>(std::max(0, bars)), '#').c_str());
  }
  const Json* mean = s.find("mean_ms");
  std::printf("    %-7s %10.3f ms\n", "mean", mean->is_null() ? 0.0 : mean->as_number());
  const Json& sv = *s.find("service_ms");
  std::printf("\n  service time (uncorrected, what ab/wrk would report)\n");
  std::printf("    p50 %.3f ms   p99 %.3f ms   max %.3f ms\n\n", sv.find("p50")->as_number(),
              sv.find("p99")->as_number(), sv.find("max")->as_number());
  std::printf("  statuses  ");
  for (auto& [code, c] : r.statuses) std::printf(" %d:%s", code, with_commas(c).c_str());
  std::printf("\n");
  if (r.attempts.size() > 1 || (r.attempts.size() == 1 && !r.attempts.count("1"))) {
    std::printf("  attempts  ");
    for (auto& [a, c] : r.attempts) std::printf(" %sx:%s", a.c_str(), with_commas(c).c_str());
    std::printf("\n");
  }
  if (r.hedged) std::printf("  hedged     %s\n", with_commas(r.hedged).c_str());

  if (r.upstreams.size() > 1) {
    std::printf("\n  distribution across upstreams\n");
    size_t widest = 0;
    uint64_t biggest = 0;
    for (auto& [n, c] : r.upstreams) {
      widest = std::max(widest, n.size());
      biggest = std::max(biggest, c);
    }
    for (auto& [n, c] : r.upstreams) {
      int bars = static_cast<int>(static_cast<double>(c) / static_cast<double>(biggest) * 40);
      std::printf("    %-*s  %8s  %5.2f%%  %s\n", static_cast<int>(widest), n.c_str(), with_commas(c).c_str(),
                  100.0 * static_cast<double>(c) / static_cast<double>(std::max<size_t>(1, total)),
                  std::string(static_cast<size_t>(bars), '#').c_str());
    }
    const Json& b = *s.find("balance");
    std::printf("    imbalance max/min = %s  cv = %s\n", format_number(b.find("imbalance")->as_number()).c_str(),
                format_number(b.find("cv")->as_number()).c_str());
  }

  if (r.per_second.size() > 2) {
    std::printf("\n  throughput / p99 per second\n");
    std::vector<long> seconds;
    for (auto& [sec, _] : r.per_second) seconds.push_back(sec);
    seconds.pop_back();   // the last bucket is partial
    uint64_t peak = 0;
    for (long sec : seconds) peak = std::max(peak, r.per_second[sec]);
    for (long sec : seconds) {
      std::vector<double> v = r.per_second_latency[sec];
      std::sort(v.begin(), v.end());
      uint64_t c = r.per_second[sec];
      int bars = static_cast<int>(static_cast<double>(c) / static_cast<double>(std::max<uint64_t>(1, peak)) * 30);
      std::printf("    +%3lds  %7s rps  p99 %9.2f ms  %s\n", sec - seconds.front(), with_commas(c).c_str(),
                  percentile(v, .99) * 1000, std::string(static_cast<size_t>(bars), '#').c_str());
    }
  }
  std::printf("\n");
}

const char* kUsage =
    "usage: jdblance-loadgen URL [-r RATE] [-c CONCURRENCY] [-n CONNECTIONS] [-d SECONDS]\n"
    "                     [-m METHOD] [--body TEXT] [--timeout S] [--uniform] [--json]\n"
    "                     [--max-error-rate FRACTION]\n"
    "  -r, --rate          open-loop arrival rate in req/s (0 = closed loop)\n"
    "  -c, --concurrency   closed-loop concurrency (ignored when --rate is set)\n"
    "  -n, --connections   TCP connections to spread the load over (default 64)\n"
    "  --max-error-rate    exit 1 if (errors + 5xx) / requests exceeds this\n";

}  // namespace

int main(int argc, char** argv) {
  Options opt;
  try {
    for (int i = 1; i < argc; ++i) {
      std::string a = argv[i];
      auto value = [&]() -> std::string {
        if (i + 1 >= argc) throw std::invalid_argument(a + " requires a value");
        return argv[++i];
      };
      if (a == "-h" || a == "--help") {
        std::fputs(kUsage, stdout);
        return 0;
      } else if (a == "-r" || a == "--rate") opt.rate = std::stod(value());
      else if (a == "-c" || a == "--concurrency") opt.concurrency = std::stoi(value());
      else if (a == "-n" || a == "--connections") opt.connections = std::stoi(value());
      else if (a == "-d" || a == "--duration") opt.duration = std::stod(value());
      else if (a == "-m" || a == "--method") opt.method = value();
      else if (a == "--body") opt.body = value();
      else if (a == "--timeout") opt.timeout = std::stod(value());
      else if (a == "--uniform") opt.uniform = true;
      else if (a == "--json") opt.json = true;
      else if (a == "--max-error-rate") opt.max_error_rate = std::stod(value());
      else if (!a.empty() && a[0] != '-' && opt.url.empty()) opt.url = a;
      else throw std::invalid_argument("unknown argument " + a);
    }
    if (opt.url.empty()) throw std::invalid_argument("URL is required");
    if (opt.rate < 0 || opt.duration <= 0 || opt.timeout <= 0) throw std::invalid_argument("bad numbers");
  } catch (const std::exception& e) {
    std::fprintf(stderr, "jdblance-loadgen: %s\n%s", e.what(), kUsage);
    return 2;
  }
  if (opt.rate == 0 && opt.concurrency == 0) opt.concurrency = 32;
  if (opt.rate == 0 && opt.connections == 0) opt.connections = opt.concurrency;
  int connections = opt.connections ? opt.connections : 64;

  // http://host[:port][/path][?query]
  std::string rest;
  if (opt.url.rfind("http://", 0) == 0) {
    rest = opt.url.substr(7);
  } else {
    std::fprintf(stderr, "jdblance-loadgen: only http:// URLs are supported\n");
    return 2;
  }
  size_t slash = rest.find('/');
  std::string authority = rest.substr(0, slash);
  std::string path = slash == std::string::npos ? "/" : rest.substr(slash);
  std::string host = authority;
  int port = 80;
  size_t colon = authority.rfind(':');
  if (colon != std::string::npos && authority.find(']', colon) == std::string::npos) {
    host = authority.substr(0, colon);
    port = std::atoi(authority.c_str() + colon + 1);
  }

  raise_fd_limit();
  Loop loop;
  Address address;
  try {
    address = resolve_blocking(host, port, false).front();
  } catch (const std::exception& e) {
    std::fprintf(stderr, "jdblance-loadgen: %s\n", e.what());
    return 2;
  }
  http::Headers headers = {
      {"Host", port == 80 ? host : authority},
      {"User-Agent", "jdblance-loadgen/1.0"},
      {"Accept", "*/*"},
      {"Connection", "keep-alive"},
  };
  if (!opt.body.empty()) {
    headers.emplace_back("Content-Length", std::to_string(opt.body.size()));
    headers.emplace_back("Content-Type", "application/octet-stream");
  }
  Recorder recorder;
  Client client(address, http::encode_request(opt.method, path, headers), opt.body, opt.method,
                opt.timeout, connections, &recorder);

  std::string mode = opt.rate > 0 ? "open-loop @ " + format_number(opt.rate) + " rps"
                                   : "closed-loop x" + std::to_string(opt.concurrency);
  // With --json, stdout carries only the document.
  std::fprintf(opt.json ? stderr : stdout, "-> %s %s  %s  max %d connections  %gs\n", opt.method.c_str(),
               opt.url.c_str(), mode.c_str(), connections, opt.duration);
  std::fflush(stdout);
  double started = monotonic();
  loop.run_until_complete(run(opt, &client));
  recorder.wall = monotonic() - started;
  recorder.opened = client.opened();

  Json summary = summary_of(recorder);
  if (opt.json) {
    std::printf("%s\n", summary.dump(2).c_str());
  } else {
    report(recorder, summary);
  }
  if (recorder.latencies.empty()) return 1;
  if (opt.max_error_rate >= 0) {
    uint64_t bad = static_cast<uint64_t>(summary.find("errors")->as_number());
    for (auto& [code, c] : recorder.statuses) {
      if (code >= 500) bad += c;
    }
    double total = static_cast<double>(recorder.latencies.size() + summary.find("errors")->as_number());
    double rate = static_cast<double>(bad) / total;
    if (rate > opt.max_error_rate) {
      std::fprintf(stderr, "jdblance-loadgen: error rate %.4f%% exceeds %.4f%%\n", rate * 100,
                   opt.max_error_rate * 100);
      return 1;
    }
  }
  return 0;
}
