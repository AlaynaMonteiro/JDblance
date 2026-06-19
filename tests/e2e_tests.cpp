// End-to-end: real sockets, real origins, real proxy, one event loop on
// ephemeral ports.  Nothing is mocked: these exercise the actual
// accept/parse/select/forward/relay path.
#include <algorithm>
#include <map>
#include <memory>
#include <set>

#include "admin.hpp"
#include "http1.hpp"
#include "json.hpp"
#include "loop.hpp"
#include "metrics.hpp"
#include "origin.hpp"
#include "proxy.hpp"
#include "testing.hpp"

using namespace jdblance;

namespace {

Task<void> accept_loop(Proxy* proxy, SocketPtr listener) {
  for (;;) {
    SocketPtr s;
    try {
      s = co_await accept_one(*listener);
    } catch (const std::exception&) {
      co_return;
    }
    spawn(proxy->handle_client(std::move(s)));
  }
}

// A raw upstream that does exactly what a test scripts: answer with fixed
// bytes, echo after a 101, or swallow the request and hang up.
struct Scripted {
  SocketPtr listener;
  int port = 0;
  int requests = 0;
  std::string response;        // empty: close without answering
  bool echo_after = false;     // after writing `response`, echo bytes until EOF
  std::optional<http::Request> last;
  std::set<Socket*> open;
};

Task<void> scripted_conn(std::shared_ptr<Scripted> st, SocketPtr c) {
  st->open.insert(c.get());
  BufReader r(c);
  try {
    http::Request req = co_await http::read_request(r, monotonic() + 5);
    http::Framing f = http::request_framing(req);
    co_await http::read_body_into_memory(r, f, 1 << 20, monotonic() + 5);
    ++st->requests;
    st->last = req;
    if (!st->response.empty()) co_await c->write_all(st->response, monotonic() + 5);
    if (st->echo_after) {
      char buf[4096];
      for (;;) {
        size_t n = co_await r.read_into(buf, sizeof buf, monotonic() + 5);
        if (n == 0) break;
        co_await c->write_all(std::string_view(buf, n), monotonic() + 5);
      }
    }
  } catch (const std::exception&) {
  }
  st->open.erase(c.get());
  c->close();
}

Task<void> scripted_accept(std::shared_ptr<Scripted> st) {
  for (;;) {
    SocketPtr c;
    try {
      c = co_await accept_one(*st->listener);
    } catch (const std::exception&) {
      co_return;
    }
    spawn(scripted_conn(st, std::move(c)));
  }
}

std::shared_ptr<Scripted> scripted(std::string response, bool echo_after = false) {
  auto st = std::make_shared<Scripted>();
  st->listener = std::make_shared<Socket>(listen_tcp("127.0.0.1", 0, 64, false));
  st->port = st->listener->local_port();
  st->response = std::move(response);
  st->echo_after = echo_after;
  spawn(scripted_accept(st));
  return st;
}

Task<void> settle(class Stack* s);

class Stack {
 public:
  // `origins` programmable origins named b0..bN-1, then `extra` endpoints
  // (name, port) appended in order; `overrides` is merged into the top level.
  struct Script {
    std::string name;
    std::string response;
    bool echo_after = false;
  };
  Stack(int origins, const std::string& overrides = "{}", std::vector<Script> raw = {}) {
    std::vector<std::pair<std::string, int>> extra;
    for (auto& r : raw) {
      scripts.push_back(scripted(r.response, r.echo_after));
      extra.emplace_back(r.name, scripts.back()->port);
    }
    for (int i = 0; i < origins; ++i) {
      auto o = std::make_shared<Origin>("b" + std::to_string(i));
      o->listen("127.0.0.1", 0);
      this->origins.push_back(o);
    }
    Json data = Json::parse(R"({"listen": {"host": "127.0.0.1", "port": 0},
                                "admin": {"enabled": false},
                                "health": {"enabled": false},
                                "log": {"access": false}})");
    Json endpoints = Json::array();
    for (auto& [name, port] : extra) {   // scripted upstreams first
      Json e = Json::object();
      e["name"] = name;
      e["address"] = "127.0.0.1:" + std::to_string(port);
      endpoints.push_back(e);
    }
    for (auto& o : this->origins) {
      Json e = Json::object();
      e["name"] = o->name;
      e["address"] = "127.0.0.1:" + std::to_string(o->port());
      endpoints.push_back(e);
    }
    data["endpoints"] = endpoints;
    Json extra_keys = Json::parse(overrides);   // named: a range-for would not extend its life
    for (auto& [k, v] : extra_keys.as_object()) data[k] = v;
    cfg = Config::from_json(data);
    cluster = std::make_unique<Cluster>(cfg);
    proxy = std::make_unique<Proxy>(cfg, *cluster);
    listener = std::make_shared<Socket>(listen_tcp("127.0.0.1", 0, 1024, false));
    port = listener->local_port();
    spawn(accept_loop(proxy.get(), listener));
  }

  ~Stack() {
    try {
      listener->close();
      proxy->close_all();
      for (auto& o : origins) o->close();
      for (auto& s : scripts) {
        s->listener->close();
      }
      loop.run_until_complete(settle(this));
    } catch (const std::exception& e) {
      std::fprintf(stderr, "teardown: %s\n", e.what());
    }
  }

  template <typename T>
  T run(Task<T> t) {
    return loop.run_until_complete(std::move(t));
  }

  Loop loop;
  std::vector<std::shared_ptr<Origin>> origins;
  std::vector<std::shared_ptr<Scripted>> scripts;
  Config cfg;
  std::unique_ptr<Cluster> cluster;
  std::unique_ptr<Proxy> proxy;
  SocketPtr listener;
  int port = 0;
};

Task<void> settle(Stack* s) {
  co_await s->cluster->health.stop_and_wait();
  for (const EndpointPtr& e : s->cluster->endpoints()) e->pool.clear();
  double deadline = monotonic() + 3.0;
  for (;;) {
    bool busy = s->proxy->connections() > 0;
    for (auto& o : s->origins) busy = busy || o->active_connections() > 0 || o->inflight > 0;
    for (auto& sc : s->scripts) {
      for (Socket* open : sc->open) open->cancel();
      busy = busy || !sc->open.empty();
    }
    if (!busy || monotonic() > deadline) break;
    co_await sleep_for(0.01);
  }
  co_await sleep_for(0.01);
}

// Header lists are always named variables here: GCC 13 ICEs on a braced
// initializer list inside a co_await'ed call.
Task<http::FetchResult> fetch_with(int port, std::string path, std::string method, std::string body,
                                   http::Headers headers) {
  co_return co_await http::fetch("127.0.0.1", port, std::move(method), std::move(path),
                                 std::move(headers), std::move(body), 5.0);
}

Task<http::FetchResult> fetch(int port, std::string path = "/", std::string method = "GET",
                              std::string body = "") {
  http::Headers none;
  co_return co_await fetch_with(port, std::move(path), std::move(method), std::move(body), none);
}

Task<http::FetchResult> fetch_raw(int port, std::string raw, std::string method = "GET") {
  co_return co_await http::fetch_raw("127.0.0.1", port, std::move(raw), std::move(method), 5.0);
}

// A persistent raw client connection.
struct RawConn {
  SocketPtr socket;
  std::unique_ptr<BufReader> reader;
};

Task<RawConn> open_conn(int port) {
  RawConn c;
  c.socket = co_await connect_tcp(*parse_numeric_address("127.0.0.1", port), monotonic() + 2);
  c.reader = std::make_unique<BufReader>(c.socket);
  co_return c;
}

Task<http::FetchResult> exchange(RawConn& c, std::string raw, std::string method = "GET") {
  co_await c.socket->write_all(raw, monotonic() + 5);
  http::FetchResult out;
  out.response = co_await http::read_response(*c.reader, monotonic() + 5);
  co_await http::relay_body(*c.reader, http::Sink{nullptr, &out.body},
                            http::response_framing(method, out.response), std::nullopt, monotonic() + 5,
                            true);
  co_return out;
}

std::string name_of(const http::FetchResult& r) {
  return Json::parse(r.body).find("name")->as_string();
}

}  // namespace

// ---------------------------------------------------------------------------
// Proxying
// ---------------------------------------------------------------------------

namespace {
Task<void> round_robin_scenario(Stack* s) {
  std::map<std::string, int> seen;
  for (int i = 0; i < 30; ++i) {
    http::FetchResult r = co_await fetch(s->port);
    CHECK_EQ(r.response.status, 200);
    ++seen[name_of(r)];
  }
  CHECK_EQ(seen.size(), 3u);
  for (auto& [_, c] : seen) CHECK_EQ(c, 10);
}

Task<void> headers_scenario(Stack* s) {
  http::FetchResult r = co_await fetch(s->port, "/echo-headers?x=1");
  std::string upstream = r.response.get_or("x-upstream");
  CHECK(upstream == "b0" || upstream == "b1" || upstream == "b2");
  CHECK(r.response.get_or("via").rfind("1.1 jdblance/", 0) == 0);
  CHECK(r.response.has("x-upstream-time"));
  CHECK_EQ(Json::parse(r.body).find("path")->as_string(), std::string("/echo-headers?x=1"));
}

Task<void> keepalive_scenario(Stack* s) {
  RawConn c = co_await open_conn(s->port);
  for (int i = 0; i < 5; ++i) {
    http::FetchResult r = co_await exchange(c, "GET / HTTP/1.1\r\nHost: t\r\n\r\n");
    CHECK_EQ(r.response.status, 200);
    CHECK_EQ(r.response.get_or("connection"), std::string("keep-alive"));
  }
  // HEAD: headers only, and the connection stays usable.
  http::FetchResult head = co_await exchange(c, "HEAD / HTTP/1.1\r\nHost: t\r\n\r\n", "HEAD");
  CHECK_EQ(head.response.status, 200);
  CHECK(head.body.empty());
  http::FetchResult after = co_await exchange(c, "GET / HTTP/1.1\r\nHost: t\r\n\r\n");
  CHECK_EQ(after.response.status, 200);
  CHECK_EQ(s->proxy->connections(), 1);
  c.socket->close();
}

Task<void> bodies_scenario(Stack* s) {
  http::FetchResult post = co_await fetch(s->port, "/echo", "POST", "payload-123");
  CHECK_EQ(post.response.status, 200);
  CHECK_EQ(post.body, std::string("payload-123"));

  http::FetchResult chunked = co_await fetch_raw(
      s->port,
      "POST /echo HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
      "5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n",
      "POST");
  CHECK_EQ(chunked.response.status, 200);
  CHECK_EQ(chunked.body, std::string("hello world"));

  // A body larger than the retry buffer streams through.
  std::string big(200 * 1024, 'z');
  http::FetchResult large = co_await fetch(s->port, "/echo", "POST", big);
  CHECK_EQ(large.body.size(), big.size());

  // Expect: 100-continue is answered only after admission.
  RawConn c = co_await open_conn(s->port);
  co_await c.socket->write_all(
      "POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n", monotonic() + 2);
  http::Response interim = co_await http::read_response(*c.reader, monotonic() + 2);
  CHECK_EQ(interim.status, 100);
  co_await c.socket->write_all("hello", monotonic() + 2);
  http::Response final_response = co_await http::read_response(*c.reader, monotonic() + 2);
  std::string body;
  co_await http::relay_body(*c.reader, http::Sink{nullptr, &body},
                            http::response_framing("POST", final_response), std::nullopt, monotonic() + 2);
  CHECK_EQ(final_response.status, 200);
  CHECK_EQ(body, std::string("hello"));
  c.socket->close();
}

Task<void> rejects_scenario(Stack* s) {
  http::FetchResult smuggle = co_await fetch_raw(
      s->port, "POST / HTTP/1.1\r\nHost: t\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n");
  CHECK_EQ(smuggle.response.status, 400);
  http::FetchResult connect = co_await fetch_raw(s->port, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n");
  CHECK_EQ(connect.response.status, 405);
  http::FetchResult garbage = co_await fetch_raw(s->port, "NOT HTTP\r\n\r\n");
  CHECK_EQ(garbage.response.status, 400);
}

Task<void> request_id_scenario(Stack* s) {
  http::Headers good = {{"X-Request-ID", "abc-123"}};
  http::FetchResult sane = co_await fetch_with(s->port, "/", "GET", "", good);
  CHECK_EQ(Json::parse(sane.body).find("request_id")->as_string(), std::string("abc-123"));
  http::Headers bad = {{"X-Request-ID", "a\"b"}};
  http::FetchResult evil = co_await fetch_with(s->port, "/", "GET", "", bad);
  std::string replaced = Json::parse(evil.body).find("request_id")->as_string();
  CHECK(replaced != "a\"b");
  CHECK_EQ(replaced.size(), 16u);
}
}  // namespace

TEST(e2e, round_robin_spreads_evenly) {
  Stack s(3, R"({"policy": "round_robin"})");
  s.run(round_robin_scenario(&s));
}

TEST(e2e, upstream_headers_and_path) {
  Stack s(3);
  s.run(headers_scenario(&s));
}

TEST(e2e, keep_alive_reuses_one_connection) {
  Stack s(3);
  s.run(keepalive_scenario(&s));
}

TEST(e2e, request_bodies) {
  Stack s(3);
  s.run(bodies_scenario(&s));
}

TEST(e2e, malformed_requests_are_rejected) {
  Stack s(2);
  s.run(rejects_scenario(&s));
}

TEST(e2e, request_id_is_propagated_only_if_safe) {
  Stack s(1);
  s.run(request_id_scenario(&s));
}

// ---------------------------------------------------------------------------
// Failure handling
// ---------------------------------------------------------------------------

namespace {
Task<void> dead_backend_scenario(Stack* s) {
  s->origins[0]->fail_rate = 1.0;
  for (int i = 0; i < 20; ++i) {
    http::FetchResult r = co_await fetch(s->port);
    CHECK_EQ(r.response.status, 200);
  }
}

Task<void> breaker_ejects_scenario(Stack* s) {
  s->origins[0]->fail_rate = 1.0;
  for (int i = 0; i < 30; ++i) co_await fetch(s->port);
  const auto& eps = s->cluster->endpoints();
  CHECK_EQ(std::string(eps[0]->breaker.state_name()), std::string("open"));
  CHECK_EQ(std::string(eps[1]->breaker.state_name()), std::string("closed"));
  CHECK_EQ(std::string(eps[2]->breaker.state_name()), std::string("closed"));
}

Task<void> breaker_recovers_scenario(Stack* s) {
  // origin0 fails requests but its /health stays green: only the breaker's
  // own cooldown can bring it back.
  s->origins[0]->fail_rate = 1.0;
  for (int i = 0; i < 12; ++i) co_await fetch(s->port);
  CHECK(s->cluster->endpoints()[0]->breaker.state() == BreakerState::Open);
  s->origins[0]->fail_rate = 0.0;
  co_await sleep_for(0.3);   // cooldown is 0.2 s
  std::set<std::string> seen;
  for (int i = 0; i < 12; ++i) {
    http::FetchResult r = co_await fetch(s->port);
    CHECK_EQ(r.response.status, 200);
    seen.insert(r.response.get_or("x-upstream"));
  }
  CHECK(seen.count("b0"));
  CHECK(s->cluster->endpoints()[0]->breaker.state() == BreakerState::Closed);
}

Task<void> all_down_scenario(Stack* s) {
  for (const EndpointPtr& e : s->cluster->endpoints()) e->admin_enabled = false;
  http::FetchResult r = co_await fetch(s->port);
  CHECK_EQ(r.response.status, 503);
  CHECK(r.body.find("no healthy upstream") != std::string::npos);
}

Task<void> connect_failure_scenario(Stack* s) {
  s->cluster->endpoints()[0]->port = 1;   // nothing listens there
  for (int i = 0; i < 9; ++i) {
    http::FetchResult r = co_await fetch(s->port);
    CHECK_EQ(r.response.status, 200);
  }
}

Task<void> no_replay_scenario(Stack* s, Scripted* hole) {
  // Round robin: the first pick is the black hole, which reads the request
  // and hangs up without answering.  The POST may have been processed, so it
  // must not be sent again; a GET may.
  http::FetchResult post = co_await fetch(s->port, "/echo", "POST", "charge-card");
  CHECK_EQ(post.response.status, 502);
  CHECK_EQ(hole->requests, 1);
  http::FetchResult first = co_await fetch(s->port);   // -> b0
  CHECK_EQ(first.response.status, 200);
  http::FetchResult get = co_await fetch(s->port);     // -> hole, then retried on b0
  CHECK_EQ(get.response.status, 200);
  CHECK_EQ(get.response.get_or("x-attempts"), std::string("2"));
  CHECK_EQ(hole->requests, 2);
}

Task<void> client_fault_scenario(Stack* s) {
  http::FetchResult bad = co_await fetch_raw(
      s->port,
      "POST /echo HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\nzz\r\nbody\r\n",
      "POST");
  CHECK_EQ(bad.response.status, 400);
  for (const EndpointPtr& e : s->cluster->endpoints()) {
    CHECK_EQ(e->total_failures, 0u);
    CHECK(e->breaker.state() == BreakerState::Closed);
  }
}
}  // namespace

TEST(e2e, dead_backend_is_retried_elsewhere) {
  Stack s(3, R"({"policy": "round_robin"})");
  s.run(dead_backend_scenario(&s));
}

TEST(e2e, breaker_ejects_a_persistently_failing_backend) {
  Stack s(3, R"({"policy": "round_robin",
                 "breaker": {"consecutive_failures": 3, "min_requests": 3, "error_threshold": 0.5, "cooldown": 30}})");
  s.run(breaker_ejects_scenario(&s));
}

TEST(e2e, breaker_recovers_without_a_health_check) {
  Stack s(3, R"({"policy": "round_robin",
                 "breaker": {"consecutive_failures": 3, "min_requests": 1000, "cooldown": 0.2, "max_cooldown": 1}})");
  s.run(breaker_recovers_scenario(&s));
}

TEST(e2e, all_backends_down_returns_503) {
  Stack s(3, R"({"policy": "round_robin", "retry": {"enabled": false}})");
  s.run(all_down_scenario(&s));
}

TEST(e2e, connect_failure_is_retried) {
  Stack s(3, R"({"policy": "round_robin"})");
  s.run(connect_failure_scenario(&s));
}

TEST(e2e, client_body_errors_are_not_blamed_on_the_backend) {
  Stack s(2, R"({"policy": "round_robin"})");
  s.run(client_fault_scenario(&s));
}

TEST(e2e, post_is_not_replayed_after_it_was_sent) {
  Stack s(1, R"({"policy": "round_robin"})", {{"hole", ""}});
  s.run(no_replay_scenario(&s, s.scripts[0].get()));
}

// ---------------------------------------------------------------------------
// Overload
// ---------------------------------------------------------------------------

namespace {
Task<void> fetch_into(int port, std::optional<int>* status) {
  try {
    http::FetchResult r = co_await fetch(port);
    *status = r.response.status;
  } catch (const std::exception&) {
    *status = -1;
  }
}

Task<void> concurrency_scenario(Stack* s) {
  for (auto& o : s->origins) o->base_latency = 0.3;
  std::vector<std::optional<int>> statuses(6);
  for (auto& st : statuses) spawn(fetch_into(s->port, &st));
  for (int i = 0; i < 300; ++i) {
    co_await sleep_for(0.01);
    if (std::all_of(statuses.begin(), statuses.end(), [](auto& v) { return v.has_value(); })) break;
  }
  std::multiset<int> got;
  for (auto& st : statuses) got.insert(st.value_or(-2));
  CHECK(got.count(503) > 0);
  CHECK(got.count(200) > 0);
}

Task<void> rate_limit_scenario(Stack* s) {
  std::vector<int> statuses;
  for (int i = 0; i < 6; ++i) {
    http::FetchResult r = co_await fetch(s->port);
    statuses.push_back(r.response.status);
    if (r.response.status == 429) CHECK_EQ(r.response.get_or("retry-after"), std::string("1"));
  }
  CHECK_EQ(statuses[0], 200);
  CHECK_EQ(statuses[1], 200);
  CHECK(std::count(statuses.begin(), statuses.end(), 429) > 0);
}

Task<void> body_limit_scenario(Stack* s) {
  http::FetchResult r = co_await fetch(s->port, "/echo", "POST", std::string(64, 'x'));
  CHECK_EQ(r.response.status, 413);
}

Task<void> max_connections_scenario(Stack* s) {
  RawConn hog = co_await open_conn(s->port);
  co_await sleep_for(0.05);   // let the proxy accept it
  http::FetchResult r = co_await fetch(s->port);
  CHECK_EQ(r.response.status, 503);
  CHECK(r.body.find("connection limit") != std::string::npos);
  hog.socket->close();
}
}  // namespace

TEST(e2e, concurrency_limit_sheds_with_503) {
  Stack s(3, R"({"limits": {"concurrency": {"enabled": false, "static_limit": 1, "max_queue": 0}}})");
  s.run(concurrency_scenario(&s));
}

TEST(e2e, rate_limit_returns_429) {
  Stack s(3, R"({"limits": {"rate_per_ip": 1, "rate_burst": 2}})");
  s.run(rate_limit_scenario(&s));
}

TEST(e2e, oversized_body_is_rejected) {
  Stack s(1, R"({"limits": {"max_body_bytes": 16}})");
  s.run(body_limit_scenario(&s));
}

TEST(e2e, connection_cap) {
  Stack s(1, R"({"limits": {"max_connections": 1}})");
  s.run(max_connections_scenario(&s));
}

// ---------------------------------------------------------------------------
// Control plane, drain, affinity, panic
// ---------------------------------------------------------------------------

namespace {
Task<int> status_of(int port, std::string path, std::string method = "GET") {
  http::FetchResult r = co_await fetch(port, std::move(path), std::move(method));
  co_return r.response.status;
}

Task<void> admin_scenario(Stack* s, AdminServer* admin) {
  co_await fetch(s->port);   // give the counters something to expose
  int ap = admin->port();
  http::FetchResult stats = co_await fetch(ap, "/stats");
  CHECK_EQ(stats.response.status, 200);
  Json j = Json::parse(stats.body);
  CHECK_EQ(j.find("total")->as_number(), 3.0);
  CHECK(j.find("latency_ms")->is_object());
  CHECK(j.find("status_classes")->find("2xx") != nullptr);

  int st = co_await status_of(ap, "/readyz");
  CHECK_EQ(st, 200);
  st = co_await status_of(ap, "/endpoints/b0/disable", "POST");
  CHECK_EQ(st, 200);
  CHECK(!s->cluster->endpoints()[0]->admin_enabled);
  st = co_await status_of(ap, "/endpoints/b0/enable", "POST");
  CHECK_EQ(st, 200);
  CHECK(s->cluster->endpoints()[0]->admin_enabled);
  st = co_await status_of(ap, "/endpoints/b1/weight?value=7", "POST");
  CHECK_EQ(st, 200);
  CHECK_EQ(s->cluster->endpoints()[1]->weight, 7);
  st = co_await status_of(ap, "/endpoints/b1/weight?value=0", "POST");
  CHECK_EQ(st, 400);
  st = co_await status_of(ap, "/endpoints/b2/breaker/open", "POST");
  CHECK_EQ(st, 200);
  CHECK(s->cluster->endpoints()[2]->breaker.state() == BreakerState::Open);
  st = co_await status_of(ap, "/endpoints/b2/breaker/close", "POST");
  CHECK_EQ(st, 200);

  http::FetchResult m = co_await fetch(ap, "/metrics");
  CHECK(m.body.find("jdblance_requests_total") != std::string::npos);
  CHECK(m.body.find("jdblance_endpoint_up{endpoint=\"b0\"} 1") != std::string::npos);
  CHECK(m.body.find("app_requests_total") == std::string::npos);   // origins keep their own registry
  // Rare-event counters exist at 0 before the first event, or increase()
  // never sees the first one (the outlier panel and alert stayed silent).
  CHECK(m.body.find("jdblance_outlier_ejections_total{endpoint=\"b1\"} 0") != std::string::npos);
  CHECK(m.body.find("jdblance_endpoint_ejections_total{endpoint=\"b1\"} 0") != std::string::npos);
  // Per-endpoint gauges are published at scrape time.
  CHECK(m.body.find("jdblance_endpoint_in_flight{endpoint=\"b0\"} 0") != std::string::npos);

  st = co_await status_of(ap, "/endpoints/nope/drain", "POST");
  CHECK_EQ(st, 404);
  st = co_await status_of(ap, "/nope");
  CHECK_EQ(st, 404);
  st = co_await status_of(ap, "/stats", "PUT");
  CHECK_EQ(st, 405);
  http::FetchResult head = co_await fetch(ap, "/healthz", "HEAD");
  CHECK_EQ(head.response.status, 200);
  CHECK(head.body.empty());
  http::FetchResult dash = co_await fetch(ap, "/");
  CHECK(dash.body.find("<title>JDblance") != std::string::npos);
  http::FetchResult config = co_await fetch(ap, "/config");
  CHECK_EQ(Json::parse(config.body).find("policy")->as_string(), std::string("p2c_ewma"));
  st = co_await status_of(ap, "/reload", "POST");
  CHECK_EQ(st, 501);

  st = co_await status_of(ap, "/drain", "POST");
  CHECK_EQ(st, 200);
  st = co_await status_of(ap, "/readyz");
  CHECK_EQ(st, 503);
}

Task<void> drained_endpoint_scenario(Stack* s) {
  s->cluster->endpoints()[0]->draining = true;
  std::set<std::string> seen;
  for (int i = 0; i < 20; ++i) {
    http::FetchResult r = co_await fetch(s->port);
    seen.insert(r.response.get_or("x-upstream"));
  }
  CHECK(seen == (std::set<std::string>{"b1", "b2"}));
}

Task<void> drain_closes_idle_scenario(Stack* s) {
  RawConn c = co_await open_conn(s->port);
  http::FetchResult r = co_await exchange(c, "GET / HTTP/1.1\r\nHost: t\r\n\r\n");
  CHECK_EQ(r.response.get_or("connection"), std::string("keep-alive"));
  double started = monotonic();
  s->proxy->start_drain();
  char byte;
  size_t n = co_await c.reader->read_into(&byte, 1, monotonic() + 2);
  CHECK_EQ(n, 0u);                       // closed by the proxy...
  CHECK(monotonic() - started < 0.5);    // ...now, not after client_idle
  // A new connection during drain is still served, once, with Connection: close.
  http::FetchResult late = co_await fetch_raw(s->port, "GET / HTTP/1.1\r\nHost: t\r\n\r\n");
  CHECK_EQ(late.response.status, 200);
  CHECK_EQ(late.response.get_or("connection"), std::string("close"));
  c.socket->close();
}

Task<void> affinity_scenario(Stack* s) {
  for (const char* session : {"alpha", "beta", "gamma", "delta"}) {
    std::set<std::string> landings;
    for (int i = 0; i < 6; ++i) {
      http::Headers headers = {{"X-Session", session}};
      http::FetchResult r = co_await fetch_with(s->port, "/", "GET", "", headers);
      landings.insert(r.response.get_or("x-upstream"));
    }
    CHECK_EQ(landings.size(), 1u);
  }
}

Task<void> panic_scenario(Stack* s, AdminServer* admin) {
  for (const EndpointPtr& e : s->cluster->endpoints()) e->healthy = false;
  std::pair<std::vector<EndpointPtr>, bool> c = s->cluster->candidates();
  CHECK(c.second);
  CHECK_EQ(c.first.size(), 3u);
  int st = co_await status_of(s->port, "/");
  CHECK_EQ(st, 200);
  // Serving in panic mode is not "ready": the orchestrator should know.
  st = co_await status_of(admin->port(), "/readyz");
  CHECK_EQ(st, 503);
  http::FetchResult stats = co_await fetch(admin->port(), "/stats");
  Json j = Json::parse(stats.body);
  CHECK(j.find("panic_mode")->as_bool());
  CHECK_EQ(j.find("healthy")->as_number(), 0.0);
}
}  // namespace

TEST(e2e, admin_surface) {
  Stack s(3);
  AdminServer admin(s.cfg, *s.cluster, *s.proxy);
  admin.start("127.0.0.1", 0);
  s.run(admin_scenario(&s, &admin));
  admin.stop();
}

TEST(e2e, drained_endpoint_receives_no_traffic) {
  Stack s(3, R"({"policy": "round_robin"})");
  s.run(drained_endpoint_scenario(&s));
}

TEST(e2e, drain_closes_idle_keepalive_connections) {
  Stack s(2);
  s.run(drain_closes_idle_scenario(&s));
}

TEST(e2e, same_key_lands_on_the_same_backend) {
  Stack s(3, R"({"policy": "maglev", "hash_key": {"source": "header", "name": "X-Session"}})");
  s.run(affinity_scenario(&s));
}

TEST(e2e, panic_mode_routes_to_unhealthy_endpoints_but_is_not_ready) {
  Stack s(3, R"({"policy": "round_robin", "panic_threshold": 0.5})");
  AdminServer admin(s.cfg, *s.cluster, *s.proxy);
  admin.start("127.0.0.1", 0);
  s.run(panic_scenario(&s, &admin));
  admin.stop();
}

// ---------------------------------------------------------------------------
// Protocol edges against scripted upstreams
// ---------------------------------------------------------------------------

namespace {
const char* kChunkedReply =
    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nX-Kind: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";

Task<void> http10_scenario(Stack* s) {
  http::FetchResult old = co_await fetch_raw(s->port, "GET / HTTP/1.0\r\n\r\n");
  CHECK_EQ(old.response.status, 200);
  CHECK(!old.response.has("transfer-encoding"));
  CHECK_EQ(old.response.get_or("connection"), std::string("close"));
  CHECK_EQ(old.body, std::string("hello"));

  http::FetchResult modern = co_await fetch(s->port);
  CHECK_EQ(modern.response.get_or("transfer-encoding"), std::string("chunked"));
  CHECK_EQ(modern.body, std::string("hello"));
}

Task<void> tunnel_scenario(Stack* s) {
  RawConn c = co_await open_conn(s->port);
  co_await c.socket->write_all("GET /ws HTTP/1.1\r\nHost: t\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n",
                               monotonic() + 2);
  http::Response r = co_await http::read_response(*c.reader, monotonic() + 2);
  CHECK_EQ(r.status, 101);
  CHECK_EQ(http::to_lower(r.get_or("upgrade")), std::string("websocket"));
  co_await c.socket->write_all("ping", monotonic() + 2);
  std::string echoed = co_await c.reader->read_exact(4, monotonic() + 2);
  CHECK_EQ(echoed, std::string("ping"));
  // Half-close propagates: our FIN reaches the upstream, which closes, which
  // reaches us as EOF.
  c.socket->shutdown_write();
  char byte;
  size_t n = co_await c.reader->read_into(&byte, 1, monotonic() + 2);
  CHECK_EQ(n, 0u);
  CHECK(s->scripts[0]->last.has_value());
  CHECK_EQ(http::to_lower(s->scripts[0]->last->get_or("upgrade")), std::string("websocket"));
  c.socket->close();
}

Task<void> unexpected_101_scenario(Stack* s) {
  int st = co_await status_of(s->port, "/");
  CHECK_EQ(st, 502);
}

Task<void> forwarding_headers_scenario(Stack* s) {
  http::Headers headers = {{"X-Forwarded-For", "1.2.3.4"}, {"Connection", "X-Secret"},
                           {"X-Secret", "leak"}, {"X-Request-ID", "rid-1"}};
  co_await fetch_with(s->port, "/", "GET", "", headers);
  const http::Request& seen = *s->scripts[0]->last;
  bool trusted = s->cfg.trust_forwarded_for;
  CHECK_EQ(seen.get_or("x-forwarded-for"), std::string(trusted ? "1.2.3.4, 127.0.0.1" : "127.0.0.1"));
  CHECK_EQ(seen.get_or("x-request-id"), std::string("rid-1"));
  CHECK_EQ(seen.get_or("x-forwarded-proto"), std::string("http"));
  CHECK(!seen.has("x-secret"));
  CHECK_EQ(seen.get_all("x-forwarded-for").size(), 1u);
}
}  // namespace

namespace {
Task<void> http10_upstream_scenario(Stack* s) {
  http::FetchResult r = co_await fetch(s->port);
  CHECK_EQ(r.response.status, 200);
  CHECK_EQ(r.body, std::string("ok"));
  // HTTP/1.0 without keep-alive closes after the response: never pool it.
  CHECK_EQ(s->cluster->endpoints()[0]->pool.size(), 0u);
}
}  // namespace

TEST(e2e, http10_upstream_connection_is_not_pooled) {
  Stack s(0, "{}", {{"old", "HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nok"}});
  s.run(http10_upstream_scenario(&s));
}

TEST(e2e, http10_client_gets_a_dechunked_body) {
  Stack s(0, "{}", {{"chunky", kChunkedReply}});
  s.run(http10_scenario(&s));
}

TEST(e2e, upgrade_tunnel_with_half_close) {
  Stack s(0, "{}", {{"ws", "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n", true}});
  s.run(tunnel_scenario(&s));
}

TEST(e2e, unsolicited_101_is_a_bad_gateway) {
  Stack s(0, R"({"retry": {"enabled": false}})",
          {{"liar", "HTTP/1.1 101 Switching Protocols\r\nUpgrade: x\r\n\r\n"}});
  s.run(unexpected_101_scenario(&s));
}

TEST(e2e, forwarding_headers_untrusted) {
  Stack s(0, "{}", {{"mirror", "HTTP/1.1 204 No Content\r\n\r\n"}});
  s.run(forwarding_headers_scenario(&s));
}

TEST(e2e, forwarding_headers_trusted_chain_appends_the_peer) {
  Stack s(0, R"({"trust_forwarded_for": true})", {{"mirror", "HTTP/1.1 204 No Content\r\n\r\n"}});
  s.run(forwarding_headers_scenario(&s));
}

// ---------------------------------------------------------------------------
// Hedging, health checking, reconfiguration, origin
// ---------------------------------------------------------------------------

namespace {
Task<void> hedge_scenario(Stack* s) {
  // Warm the cluster histogram so a p99 exists; the delay is clamped to
  // [min_delay, max_delay] whatever other tests left in it.
  for (int i = 0; i < 6; ++i) co_await fetch(s->port);
  s->origins[1]->base_latency = 0.5;
  double wins_before = metrics::HEDGE_WINS.value();
  bool saw_hedge = false;
  for (int i = 0; i < 4; ++i) {
    double started = monotonic();
    http::FetchResult r = co_await fetch(s->port);
    double took = monotonic() - started;
    CHECK_EQ(r.response.status, 200);
    if (r.response.has("x-hedged")) {
      saw_hedge = true;
      CHECK_EQ(r.response.get_or("x-upstream"), std::string("b0"));
      CHECK(took < 0.4);   // the slow primary would have taken 0.5 s
    }
  }
  CHECK(saw_hedge);
  CHECK(metrics::HEDGE_WINS.value() > wins_before);
  // The cancelled primary still taught the balancer it is slow (at least the
  // hedge delay), instead of looking idle and attractive.
  CHECK(s->cluster->endpoints()[1]->ewma() >= 0.02);
  // The losing primary was cancelled, not leaked: nothing stays in flight.
  co_await sleep_for(0.05);
  for (const EndpointPtr& e : s->cluster->endpoints()) CHECK_EQ(e->inflight, 0);
}

Task<void> health_scenario(Stack* s) {
  s->cluster->start_health();
  s->origins[0]->healthy = false;
  bool went_down = false;
  for (int i = 0; i < 100 && !went_down; ++i) {
    co_await sleep_for(0.02);
    went_down = !s->cluster->endpoints()[0]->healthy;
  }
  CHECK(went_down);
  CHECK(s->cluster->endpoints()[0]->last_error.find("503") != std::string::npos);
  s->origins[0]->healthy = true;
  bool came_back = false;
  for (int i = 0; i < 100 && !came_back; ++i) {
    co_await sleep_for(0.02);
    came_back = s->cluster->endpoints()[0]->healthy;
  }
  CHECK(came_back);
}

Task<void> reload_scenario(Stack* s) {
  for (int i = 0; i < 6; ++i) co_await fetch(s->port);
  EndpointPtr b0 = s->cluster->endpoints()[0];
  b0->breaker.force_open(60);
  Config next = s->cfg;
  next.policy = "maglev";
  next.endpoints.pop_back();   // drop b2
  EndpointConfig extra;
  extra.name = "b3";
  extra.address = s->cfg.endpoints[2].address;   // same origin, new name
  extra.weight = 2;
  next.endpoints.push_back(extra);
  next.endpoints[1].weight = 5;
  next.breaker.consecutive_failures = 2;
  s->cfg = next;
  s->cluster->apply_config(s->cfg);
  const auto& eps = s->cluster->endpoints();
  CHECK_EQ(eps.size(), 3u);
  CHECK(eps[0] == b0);                                   // survivor: same object...
  CHECK(eps[0]->breaker.state() == BreakerState::Open);  // ...live state carried over
  CHECK_EQ(eps[1]->weight, 5);
  CHECK_EQ(eps[2]->name, std::string("b3"));
  CHECK_EQ(std::string(s->cluster->balancer().name()), std::string("maglev"));
  for (int i = 0; i < 6; ++i) {
    int st = co_await status_of(s->port, "/");
    CHECK_EQ(st, 200);
  }
}

Task<void> chaos_scenario(Stack* s) {
  int op = s->origins[0]->port();
  http::Headers json = {{"Content-Type", "application/json"}};
  http::FetchResult r =
      co_await fetch_with(op, "/chaos", "POST", R"({"fail_rate": 1, "latency_ms": "5"})", json);
  CHECK_EQ(r.response.status, 200);
  CHECK_EQ(s->origins[0]->fail_rate, 1.0);
  CHECK_EQ(s->origins[0]->base_latency, 0.005);
  int st = co_await status_of(op, "/");
  CHECK_EQ(st, 500);
  co_await fetch(op, "/chaos?reset=1", "POST");
  CHECK_EQ(s->origins[0]->fail_rate, 0.0);
  http::Headers browser = {{"Accept", "text/html"}, {"X-Request-ID", "<x>"}};
  http::FetchResult page = co_await fetch_with(op, "/", "GET", "", browser);
  CHECK(page.body.find("&lt;x&gt;") != std::string::npos);
  CHECK(page.body.find("<x>") == std::string::npos);
  http::FetchResult m = co_await fetch(op, "/metrics");
  CHECK(m.body.find("app_requests_total{container=\"b0\"") != std::string::npos);
}
}  // namespace

TEST(e2e, hedge_beats_a_slow_primary) {
  Stack s(2, R"({"policy": "round_robin",
                 "hedge": {"enabled": true, "quantile": 0.99, "min_delay": 0.02, "max_delay": 0.05}})");
  s.run(hedge_scenario(&s));
}

TEST(e2e, health_checks_eject_and_restore) {
  Stack s(2, R"({"health": {"enabled": true, "interval": 0.05, "timeout": 0.5, "rise": 2, "fall": 2}})");
  s.run(health_scenario(&s));
}

TEST(e2e, live_reconfiguration_keeps_survivor_state) {
  Stack s(3, R"({"policy": "round_robin"})");
  s.run(reload_scenario(&s));
}

TEST(e2e, origin_chaos_api) {
  Stack s(1);
  s.run(chaos_scenario(&s));
}

// ---------------------------------------------------------------------------
// Routing, sticky cookies, caching, per-route limits, outliers, slow start
// ---------------------------------------------------------------------------

namespace {
// Moves the named endpoints out of the default pool into a new upstream, adds
// `routes`, and applies the result the way a reload would.
void reshape(Stack& s, const std::string& upstream, const std::vector<std::string>& names,
             const std::vector<RouteConfig>& routes, bool keep_default = true) {
  UpstreamConfig up;
  up.name = upstream;
  up.policy = "round_robin";
  std::vector<EndpointConfig> rest;
  for (const EndpointConfig& e : s.cfg.endpoints) {
    bool moving = std::find(names.begin(), names.end(), e.name) != names.end();
    (moving ? up.endpoints : rest).push_back(e);
  }
  s.cfg.endpoints = keep_default ? rest : std::vector<EndpointConfig>{};
  s.cfg.upstreams.push_back(up);
  s.cfg.routes = routes;
  s.cfg.validate();
  s.cluster->apply_config(s.cfg);
}

RouteConfig route_to(const std::string& name, const std::string& upstream, const std::string& prefix,
                     const std::string& host = "", bool strip = false) {
  RouteConfig r;
  r.name = name;
  r.upstream = upstream;
  r.prefix = prefix;
  r.host = host;
  r.strip_prefix = strip;
  return r;
}

std::string path_seen(const http::FetchResult& r) { return Json::parse(r.body).find("path")->as_string(); }

Task<void> path_routing_scenario(Stack* s) {
  for (int i = 0; i < 6; ++i) {
    http::FetchResult api = co_await fetch(s->port, "/api/users?id=7");
    CHECK_EQ(api.response.status, 200);
    CHECK_EQ(api.response.get_or("x-upstream"), std::string("b2"));
    CHECK_EQ(path_seen(api), std::string("/users?id=7"));   // prefix stripped
  }
  std::set<std::string> elsewhere;
  for (const char* path : {"/", "/apix", "/other/api", "/", "/apix", "/other"}) {
    http::FetchResult r = co_await fetch(s->port, path);
    CHECK_EQ(r.response.status, 200);
    CHECK_EQ(path_seen(r), std::string(path));               // default route: untouched
    elsewhere.insert(r.response.get_or("x-upstream"));
  }
  CHECK(elsewhere == (std::set<std::string>{"b0", "b1"}));
  CHECK(metrics::ROUTE_REQUESTS.value({"api"}) >= 6);
  CHECK(s->cluster->ready());
  s->cluster->find("b2")->healthy = false;   // the default pool is fine, the api pool is not
  CHECK(!s->cluster->ready());
}

Task<void> host_routing_scenario(Stack* s) {
  http::Headers shop = {{"Host", "Shop.Example.com:8080"}};
  http::FetchResult r = co_await fetch_with(s->port, "/", "GET", "", shop);
  CHECK_EQ(r.response.get_or("x-upstream"), std::string("b2"));
  http::Headers apex = {{"Host", "example.com"}};   // not a subdomain: default pool
  r = co_await fetch_with(s->port, "/", "GET", "", apex);
  CHECK(r.response.get_or("x-upstream") != "b2");
}

Task<void> no_default_scenario(Stack* s, AdminServer* admin) {
  http::FetchResult miss = co_await fetch(s->port, "/");
  CHECK_EQ(miss.response.status, 404);
  CHECK(miss.body.find("no route") != std::string::npos);
  http::FetchResult hit = co_await fetch(s->port, "/api");
  CHECK_EQ(hit.response.status, 200);
  int st = co_await status_of(admin->port(), "/readyz");
  CHECK_EQ(st, 200);
  // Readiness needs *every* upstream: lose the only one and it fails.
  for (const EndpointPtr& e : s->cluster->endpoints()) e->healthy = false;
  st = co_await status_of(admin->port(), "/readyz");
  CHECK_EQ(st, 503);
  Json stats = Json::parse((co_await fetch(admin->port(), "/stats")).body);
  CHECK_EQ(stats.find("upstreams")->as_array().size(), 1u);
  CHECK_EQ(stats.find("routes")->as_array()[0].find("upstream")->as_string(), std::string("api"));
}
}  // namespace

TEST(e2e, path_routing_with_prefix_strip) {
  Stack s(3, R"({"policy": "round_robin"})");
  reshape(s, "api", {"b2"}, {route_to("api", "api", "/api", "", true)});
  s.run(path_routing_scenario(&s));
}

TEST(e2e, host_routing_with_wildcard) {
  Stack s(3, R"({"policy": "round_robin"})");
  reshape(s, "shop", {"b2"}, {route_to("shop", "shop", "/", "*.example.com")});
  s.run(host_routing_scenario(&s));
}

TEST(e2e, unmatched_request_is_404_without_a_default_pool) {
  Stack s(2);
  reshape(s, "api", {"b0", "b1"}, {route_to("api", "api", "/api")}, false);
  AdminServer admin(s.cfg, *s.cluster, *s.proxy);
  admin.start("127.0.0.1", 0);
  s.run(no_default_scenario(&s, &admin));
  admin.stop();
}

namespace {
std::string cookie_from(const http::FetchResult& r, const std::string& name) {
  std::string set = r.response.get_or("set-cookie");
  if (set.rfind(name + "=", 0) != 0) return "";
  return set.substr(name.size() + 1, set.find(';') - name.size() - 1);
}

Task<void> sticky_scenario(Stack* s) {
  http::FetchResult first = co_await fetch(s->port);
  std::string token = cookie_from(first, "jdb");
  CHECK_EQ(token.size(), 16u);
  CHECK(first.response.get_or("set-cookie").find("Max-Age=60") != std::string::npos);
  CHECK(first.response.get_or("set-cookie").find("HttpOnly") != std::string::npos);
  std::string home = first.response.get_or("x-upstream");
  http::Headers with_cookie = {{"Cookie", "theme=dark; jdb=" + token}};
  for (int i = 0; i < 8; ++i) {   // round robin would rotate; the cookie pins it
    http::FetchResult r = co_await fetch_with(s->port, "/", "GET", "", with_cookie);
    CHECK_EQ(r.response.get_or("x-upstream"), home);
    CHECK(!r.response.has("set-cookie"));
  }
  s->cluster->find(home)->admin_enabled = false;   // its backend goes away
  http::FetchResult moved = co_await fetch_with(s->port, "/", "GET", "", with_cookie);
  CHECK_EQ(moved.response.status, 200);
  CHECK(moved.response.get_or("x-upstream") != home);
  std::string fresh = cookie_from(moved, "jdb");
  CHECK(!fresh.empty() && fresh != token);   // re-pinned to the new one

  // Pinned to a backend that is up but failing: the retry goes elsewhere and
  // the client is re-pinned to wherever it succeeded.
  std::string now_home = moved.response.get_or("x-upstream");
  for (auto& o : s->origins) {
    if (o->name == now_home) o->fail_rate = 1.0;
  }
  http::Headers repinned = {{"Cookie", "jdb=" + fresh}};
  http::FetchResult retried = co_await fetch_with(s->port, "/", "GET", "", repinned);
  CHECK_EQ(retried.response.status, 200);
  CHECK_EQ(retried.response.get_or("x-attempts"), std::string("2"));
  CHECK(retried.response.get_or("x-upstream") != now_home);
  CHECK(!cookie_from(retried, "jdb").empty() && cookie_from(retried, "jdb") != fresh);
}
}  // namespace

TEST(e2e, sticky_cookie_pins_and_repins) {
  Stack s(3, R"({"policy": "round_robin", "sticky": {"cookie": "jdb", "max_age": 60}})");
  s.run(sticky_scenario(&s));
}

namespace {
const char* kCacheable =
    "HTTP/1.1 200 OK\r\nCache-Control: public, max-age=60\r\nAge: 2\r\nContent-Type: text/plain\r\n"
    "Content-Length: 5\r\n\r\nhello";

Task<void> cache_scenario(Stack* s) {
  Scripted& origin = *s->scripts[0];
  double hits_before = metrics::CACHE.value({"hit"});
  http::FetchResult miss = co_await fetch(s->port, "/page");
  CHECK_EQ(miss.body, std::string("hello"));
  CHECK_EQ(miss.response.get_or("x-cache"), std::string("MISS"));
  CHECK_EQ(origin.requests, 1);

  http::FetchResult hit = co_await fetch(s->port, "/page");
  CHECK_EQ(hit.response.status, 200);
  CHECK_EQ(hit.body, std::string("hello"));
  CHECK_EQ(hit.response.get_or("x-cache"), std::string("HIT"));
  CHECK_EQ(hit.response.get_or("content-type"), std::string("text/plain"));
  CHECK(std::stoi(hit.response.get_or("age", "0")) >= 2);   // the origin's Age carries over
  CHECK_EQ(origin.requests, 1);                              // served without the backend

  http::FetchResult head = co_await fetch(s->port, "/page", "HEAD");
  CHECK_EQ(head.response.get_or("x-cache"), std::string("HIT"));
  CHECK_EQ(head.response.get_or("content-length"), std::string("5"));
  CHECK(head.body.empty());

  http::Headers no_cache = {{"Cache-Control", "no-cache"}};
  http::FetchResult bypass = co_await fetch_with(s->port, "/page", "GET", "", no_cache);
  CHECK(!bypass.response.has("x-cache"));
  CHECK_EQ(origin.requests, 2);

  co_await fetch(s->port, "/other");   // a different target is a different entry
  CHECK_EQ(origin.requests, 3);
  CHECK_EQ(s->cluster->cache.size(), 2u);
  CHECK_EQ(metrics::CACHE.value({"hit"}), hits_before + 2);
}

Task<void> uncacheable_scenario(Stack* s) {
  co_await fetch(s->port, "/x");
  http::FetchResult again = co_await fetch(s->port, "/x");
  CHECK_EQ(again.response.get_or("x-cache"), std::string("MISS"));
  CHECK_EQ(s->scripts[0]->requests, 2);
  CHECK_EQ(s->cluster->cache.size(), 0u);
}
}  // namespace

TEST(e2e, cache_serves_hits_without_the_backend) {
  Stack s(0, R"({"cache": {"enabled": true}})", {{"cached", kCacheable}});
  s.run(cache_scenario(&s));
}

TEST(e2e, cache_never_stores_what_it_must_not) {
  for (const char* response : {
           "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",   // no freshness information
           "HTTP/1.1 200 OK\r\nCache-Control: max-age=60\r\nSet-Cookie: s=1\r\nContent-Length: 2\r\n\r\nok",
           "HTTP/1.1 200 OK\r\nCache-Control: private, max-age=60\r\nContent-Length: 2\r\n\r\nok",
           "HTTP/1.1 200 OK\r\nCache-Control: max-age=60\r\nVary: Accept-Language\r\nContent-Length: 2\r\n\r\nok",
           "HTTP/1.1 500 Oops\r\nCache-Control: max-age=60\r\nContent-Length: 2\r\n\r\nno",
       }) {
    Stack s(0, R"({"cache": {"enabled": true}, "retry": {"enabled": false}})", {{"origin", response}});
    s.run(uncacheable_scenario(&s));
  }
}

TEST(e2e, cache_respects_a_route_opt_out) {
  Stack s(0, R"({"cache": {"enabled": true},
                 "routes": [{"name": "fresh", "prefix": "/live", "cache": false}]})",
          {{"cached", kCacheable}});
  auto scenario = [](Stack* st) -> Task<void> {
    co_await fetch(st->port, "/live/x");
    http::FetchResult r = co_await fetch(st->port, "/live/x");
    CHECK(!r.response.has("x-cache"));
    CHECK_EQ(st->scripts[0]->requests, 2);
  };
  s.run(scenario(&s));
}

namespace {
Task<void> route_limit_scenario(Stack* s) {
  http::Headers alice = {{"X-Api-Key", "alice"}};
  http::Headers bob = {{"X-Api-Key", "bob"}};
  int st = (co_await fetch_with(s->port, "/limited", "GET", "", alice)).response.status;
  CHECK_EQ(st, 200);
  http::FetchResult limited = co_await fetch_with(s->port, "/limited", "GET", "", alice);
  CHECK_EQ(limited.response.status, 429);
  CHECK_EQ(limited.response.get_or("retry-after"), std::string("1"));
  st = (co_await fetch_with(s->port, "/limited", "GET", "", bob)).response.status;
  CHECK_EQ(st, 200);   // its own bucket
  st = (co_await fetch(s->port, "/limited")).response.status;   // no key: the client IP's bucket
  CHECK_EQ(st, 200);
  st = (co_await fetch(s->port, "/limited")).response.status;
  CHECK_EQ(st, 429);
  for (int i = 0; i < 5; ++i) {   // other routes are not limited
    st = (co_await fetch(s->port, "/free")).response.status;
    CHECK_EQ(st, 200);
  }
  CHECK(metrics::SHED.value({"route_rate_limit"}) >= 2);
}
}  // namespace

TEST(e2e, per_route_rate_limit_by_header) {
  Stack s(2, R"({"routes": [{"name": "limited", "prefix": "/limited",
                             "rate_limit": {"rate": 0.5, "burst": 1, "key": "header:X-Api-Key"}}]})");
  s.run(route_limit_scenario(&s));
}

namespace {
Task<void> outlier_scenario(Stack* s) {
  s->origins[0]->fail_rate = 0.6;
  double before = metrics::OUTLIER_EJECTIONS.value({"b0"});
  // Paced traffic across three 0.2 s intervals (~25 requests each, ~6 of them
  // b0's), so every interval the lazy evaluation lands on has enough samples.
  // Paced, not a tight loop: thousands of short connections per run exhaust
  // the ephemeral ports and time out every test after it.
  for (int i = 0; i < 80; ++i) {
    co_await fetch(s->port);
    co_await sleep_for(0.008);
  }
  CHECK_EQ(metrics::OUTLIER_EJECTIONS.value({"b0"}), before + 1);   // once: it stays out for 30 s
  CHECK(s->cluster->find("b0")->breaker.state() == BreakerState::Open);
  for (int i = 0; i < 12; ++i) {
    http::FetchResult r = co_await fetch(s->port);
    CHECK_EQ(r.response.status, 200);
    CHECK(r.response.get_or("x-upstream") != "b0");
  }
}
}  // namespace

TEST(e2e, outlier_detection_ejects_a_degraded_backend) {
  // The breaker alone can never trip here (a 100% error-rate threshold and an
  // unreachable streak), so only the comparison with the peers can catch b0.
  Stack s(4, R"({"policy": "round_robin", "retry": {"enabled": false},
                 "breaker": {"error_threshold": 1.0, "consecutive_failures": 1000},
                 "outlier": {"enabled": true, "interval": 0.2, "min_requests": 3, "base_ejection": 30}})");
  s.run(outlier_scenario(&s));
}

namespace {
Task<void> slow_start_scenario(Stack* s, AdminServer* admin) {
  std::map<std::string, int> seen;
  for (int i = 0; i < 200; ++i) ++seen[(co_await fetch(s->port)).response.get_or("x-upstream")];
  CHECK(seen["b3"] < 30);   // ~5 expected while warming; ~50 at full weight
  // The others keep their full round-robin share and absorb b3's remainder,
  // spread by latency rather than by the rotation.
  for (const char* name : {"b0", "b1", "b2"}) CHECK(seen[name] >= 45);
  Json stats = Json::parse((co_await fetch(admin->port(), "/stats")).body);
  const Json& warming = *stats.find("upstreams")->as_array()[0].find("warming");
  CHECK_EQ(warming.as_array().size(), 1u);
  CHECK_EQ(warming.as_array()[0].as_string(), std::string("b3"));
}
}  // namespace

TEST(e2e, slow_start_ramps_an_endpoint_added_by_reload) {
  Stack s(4, R"({"policy": "round_robin", "slow_start": {"window": 60, "min_weight": 0.1}})");
  EndpointConfig b3 = s.cfg.endpoints.back();
  s.cfg.endpoints.pop_back();
  s.cluster->apply_config(s.cfg);   // b3 leaves...
  s.cfg.endpoints.push_back(b3);
  s.cluster->apply_config(s.cfg);   // ...and comes back as a new endpoint: it warms up
  AdminServer admin(s.cfg, *s.cluster, *s.proxy);
  admin.start("127.0.0.1", 0);
  s.run(slow_start_scenario(&s, &admin));
  admin.stop();
}
