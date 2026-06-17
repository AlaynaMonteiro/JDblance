// Unit tests: codec (weighted toward the smuggling surface), policies,
// resilience primitives, config, JSON, metrics.  Each subtle failure mode the
// README lists under "Edge cases handled" has a test here or in e2e_tests.
#include <sys/socket.h>
#include <unistd.h>

#include <cmath>
#include <cstdlib>
#include <map>
#include <set>

#include "balancer.hpp"
#include "config.hpp"
#include "endpoint.hpp"
#include "hashring.hpp"
#include "http1.hpp"
#include "json.hpp"
#include "limits.hpp"
#include "loop.hpp"
#include "metrics.hpp"
#include "origin.hpp"
#include "server.hpp"
#include "testing.hpp"

using namespace jdblance;

namespace {

template <typename T>
T run(Task<T> task) {
  Loop loop;
  return loop.run_until_complete(std::move(task));
}

http::Request parse_request(const std::string& raw) {
  BufReader reader(raw);
  return run(http::read_request(reader, kForever));
}

http::Response parse_response(const std::string& raw) {
  BufReader reader(raw);
  return run(http::read_response(reader, kForever));
}

int request_error(const std::string& raw) {
  try {
    http::Request req = parse_request(raw);
    http::request_framing(req);
  } catch (const http::ProtocolError& e) {
    return e.status;
  }
  return 0;
}

std::optional<std::string> buffer_body(const std::string& raw, http::Framing framing, size_t max) {
  BufReader reader(raw);
  return run(http::read_body_into_memory(reader, framing, max, kForever));
}

std::string relay(const std::string& raw, http::Framing framing, bool dechunk = false) {
  BufReader reader(raw);
  std::string out;
  run(http::relay_body(reader, http::Sink{nullptr, &out}, framing, std::nullopt, kForever, dechunk));
  return out;
}

const http::Framing kChunked{http::BodyKind::Chunked, 0, "chunked"};

}  // namespace

// ---------------------------------------------------------------------------
// HTTP codec
// ---------------------------------------------------------------------------

TEST(http, basic_request) {
  http::Request req = parse_request("GET /a?b=c HTTP/1.1\r\nHost: x\r\nX-A: 1\r\n\r\n");
  CHECK_EQ(req.method, std::string("GET"));
  CHECK_EQ(req.target, std::string("/a?b=c"));
  CHECK(req.path() == "/a");
  CHECK(req.query() == "b=c");
  CHECK_EQ(*req.get("x-a"), std::string("1"));
  CHECK(req.idempotent());
}

TEST(http, missing_and_duplicate_host_rejected) {
  CHECK_EQ(request_error("GET / HTTP/1.1\r\n\r\n"), 400);
  CHECK_EQ(request_error("GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n"), 400);
  CHECK_EQ(request_error("GET / HTTP/1.0\r\n\r\n"), 0);   // HTTP/1.0 may omit Host
}

TEST(http, obs_fold_and_space_before_colon_rejected) {
  CHECK_EQ(request_error("GET / HTTP/1.1\r\nHost: x\r\nX-A: 1\r\n  continued\r\n\r\n"), 400);
  CHECK_EQ(request_error("GET / HTTP/1.1\r\nHost: x\r\nX-A : 1\r\n\r\n"), 400);
}

TEST(http, bad_version_is_505) { CHECK_EQ(request_error("GET / HTTP/2.0\r\nHost: x\r\n\r\n"), 505); }

TEST(http, control_characters_rejected) {
  // A bare CR inside a value is a line terminator to some origins.
  CHECK_EQ(request_error(std::string("GET / HTTP/1.1\r\nHost: x\r\nX-A: a\rb\r\n\r\n")), 400);
  CHECK_EQ(request_error(std::string("GET / HTTP/1.1\r\nHost: x\r\nX-A: a\0b\r\n\r\n", 37)), 400);
  CHECK_EQ(request_error("GET / HTTP/1.1\r\nHost: x\r\nX(A): 1\r\n\r\n"), 400);   // not a token
  CHECK_EQ(request_error("GET /\x01 HTTP/1.1\r\nHost: x\r\n\r\n"), 400);
  CHECK_EQ(request_error("GET /\xc3\xa9 HTTP/1.1\r\nHost: x\r\n\r\n"), 400);
  CHECK_EQ(request_error("GET  / HTTP/1.1\r\nHost: x\r\n\r\n"), 400);   // double space
  CHECK_EQ(request_error("get / HTTP/1.1\r\nHost: x\r\n\r\n"), 400);
}

TEST(http, leading_crlf_before_request_line_is_tolerated) {
  // RFC 9112 2.2: ignore at least one empty line before the request line.
  http::Request req = parse_request("\r\nGET / HTTP/1.1\r\nHost: x\r\n\r\n");
  CHECK_EQ(req.method, std::string("GET"));
}

TEST(http, closed_before_any_byte_is_a_clean_close) {
  BufReader reader(std::string(""));
  CHECK_THROWS(run(http::read_request(reader, kForever)), ConnectionClosed);
}

TEST(http, head_size_limits) {
  CHECK_EQ(request_error("GET / HTTP/1.1\r\nHost: x\r\nX: " + std::string(70000, 'a') + "\r\n\r\n"), 431);
  std::string many = "GET / HTTP/1.1\r\nHost: x\r\n";
  for (int i = 0; i < 200; ++i) many += "X-" + std::to_string(i) + ": v\r\n";
  CHECK_EQ(request_error(many + "\r\n"), 431);
  CHECK_EQ(request_error("GET /" + std::string(9000, 'a') + " HTTP/1.1\r\nHost: x\r\n\r\n"), 414);
}

TEST(http, content_length_framing) {
  http::Framing f = http::request_framing(parse_request("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello"));
  CHECK(f.kind == http::BodyKind::Length);
  CHECK_EQ(f.length, 5u);
  CHECK(http::request_framing(parse_request("GET / HTTP/1.1\r\nHost: x\r\n\r\n")).empty());
  // Repeated identical values are one value; anything else is ambiguous.
  CHECK_EQ(request_error("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5, 5\r\n\r\n"), 0);
  CHECK_EQ(request_error("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n"), 400);
  CHECK_EQ(request_error("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: , 5\r\n\r\n"), 400);
  CHECK_EQ(request_error("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: +5\r\n\r\n"), 400);
  CHECK_EQ(request_error("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 99999999999999999999\r\n\r\n"), 400);
}

TEST(http, te_cl_conflict_rejected) {
  // The CL.TE smuggling primitive: refuse, never pick a winner.
  CHECK_EQ(request_error("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 6\r\n"
                         "Transfer-Encoding: chunked\r\n\r\n0\r\n\r\n"),
           400);
}

TEST(http, request_transfer_encoding_must_be_exactly_chunked) {
  CHECK_EQ(request_error("POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip\r\n\r\n"), 400);
  CHECK_EQ(request_error("POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip, chunked\r\n\r\n"), 400);
  CHECK_EQ(request_error("POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked, chunked\r\n\r\n"), 400);
  CHECK_EQ(request_error("POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: Chunked\r\n\r\n"), 0);
}

TEST(http, response_framing_rules) {
  http::Response r = parse_response("HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n");
  CHECK(http::response_framing("HEAD", r).kind == http::BodyKind::None);
  CHECK(http::response_framing("GET", r).kind == http::BodyKind::Length);
  CHECK(http::response_framing("GET", parse_response("HTTP/1.1 304 Not Modified\r\n\r\n")).kind ==
        http::BodyKind::None);
  CHECK(http::response_framing("GET", parse_response("HTTP/1.1 200 OK\r\n\r\n")).kind == http::BodyKind::Eof);
  // Codings before `chunked` are kept for the next hop, not silently dropped.
  http::Framing f = http::response_framing(
      "GET", parse_response("HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n"));
  CHECK(f.kind == http::BodyKind::Chunked);
  CHECK_EQ(f.codings, std::string("gzip, chunked"));
}

TEST(http, status_line_is_strict) {
  CHECK_EQ(parse_response("HTTP/1.1 200\r\n\r\n").status, 200);
  CHECK_EQ(parse_response("HTTP/1.0 503 Busy Now\r\n\r\n").reason, std::string("Busy Now"));
  for (const char* bad : {"HTTP/1.1 2000 OK\r\n\r\n", "HTTP/1.1 +20 OK\r\n\r\n", "HTTP/2 200 OK\r\n\r\n",
                          "HTTP/1.1 099 Low\r\n\r\n", "SIP/2.0 200 OK\r\n\r\n"}) {
    CHECK_THROWS(parse_response(bad), http::ProtocolError);
  }
}

TEST(http, chunked_body_is_dechunked_when_buffered) {
  CHECK_EQ(*buffer_body("5\r\nhello\r\n5\r\nworld\r\n0\r\n\r\n", kChunked, 4096), std::string("helloworld"));
}

TEST(http, chunked_over_limit_is_413) {
  try {
    buffer_body("a\r\n0123456789\r\n0\r\n\r\n", kChunked, 4);
    CHECK(false);
  } catch (const http::ProtocolError& e) {
    CHECK_EQ(e.status, 413);
  }
}

TEST(http, chunk_size_grammar_is_strict) {
  for (const char* bad : {"+5\r\nhello\r\n0\r\n\r\n", " 5\r\nhello\r\n0\r\n\r\n", "5 x\r\nhello\r\n0\r\n\r\n",
                          "0x5\r\nhello\r\n0\r\n\r\n", "11111111111111111\r\n", "5\r\nhelloXX0\r\n\r\n",
                          "5\r\nhel"}) {
    CHECK_THROWS(buffer_body(bad, kChunked, 1 << 20), http::ProtocolError);
  }
  // Extensions (with optional whitespace before ';') are legal -- and dropped.
  CHECK_EQ(*buffer_body("5 ;name=v\r\nhello\r\n0\r\n\r\n", kChunked, 4096), std::string("hello"));
}

TEST(http, length_body_over_buffer_limit_is_not_buffered) {
  CHECK(!buffer_body(std::string(100, 'x'), http::Framing{http::BodyKind::Length, 100, ""}, 10));
  CHECK_THROWS(buffer_body("abc", http::Framing{http::BodyKind::Length, 10, ""}, 100), http::ProtocolError);
}

TEST(http, relay_reframes_chunks) {
  CHECK_EQ(relay("3\r\nabc\r\n0\r\n\r\n", kChunked), std::string("3\r\nabc\r\n0\r\n\r\n"));
  // Extensions never reach the peer; trailers are normalised.
  CHECK_EQ(relay("A;ext=1\r\n0123456789\r\n0\r\nX-T:  v \r\n\r\n", kChunked),
           std::string("a\r\n0123456789\r\n0\r\nX-T: v\r\n\r\n"));
  // For an HTTP/1.0 peer: payload only.
  CHECK_EQ(relay("3\r\nabc\r\n2\r\nde\r\n0\r\nX-T: v\r\n\r\n", kChunked, true), std::string("abcde"));
}

TEST(http, relay_enforces_limits) {
  BufReader reader(std::string(50, 'x'));
  std::string out;
  CHECK_THROWS(run(http::relay_body(reader, http::Sink{nullptr, &out},
                                    http::Framing{http::BodyKind::Length, 50, ""}, 10, kForever)),
               http::ProtocolError);
  CHECK_THROWS(relay("abc", http::Framing{http::BodyKind::Length, 5, ""}), http::ProtocolError);
  CHECK_EQ(relay("until close", http::Framing{http::BodyKind::Eof, 0, ""}), std::string("until close"));
}

TEST(http, hop_by_hop_and_connection_named_headers_are_stripped) {
  http::Request req = parse_request(
      "GET / HTTP/1.1\r\nHost: x\r\nConnection: keep-alive, X-Secret\r\n"
      "X-Secret: leak\r\nKeep-Alive: 5\r\nProxy-Authorization: y\r\nX-Keep: yes\r\n\r\n");
  std::set<std::string> names;
  for (const auto& h : http::filtered_headers(req)) names.insert(http::to_lower(h.first));
  CHECK(!names.count("connection"));
  CHECK(!names.count("x-secret"));
  CHECK(!names.count("keep-alive"));
  CHECK(!names.count("proxy-authorization"));
  CHECK(names.count("x-keep"));
  CHECK(names.count("host"));
}

TEST(http, encode_roundtrip) {
  CHECK_EQ(http::encode_request("GET", "/x", {{"Host", "h"}, {"A", "b"}}),
           std::string("GET /x HTTP/1.1\r\nHost: h\r\nA: b\r\n\r\n"));
  CHECK_EQ(http::simple_response(404, "no"), std::string("HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; "
                                                         "charset=utf-8\r\nContent-Length: 2\r\nConnection: "
                                                         "close\r\n\r\nno"));
}

TEST(http, request_helpers) {
  http::Request req = parse_request(
      "GET /ws?x=1&value=7&flag HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: WebSocket\r\n"
      "Expect: 100-continue\r\n\r\n");
  CHECK_EQ(req.upgrade_target().value_or(""), std::string("websocket"));
  CHECK(req.expects_continue());
  CHECK_EQ(http::query_param(req.query(), "value").value_or(""), std::string("7"));
  CHECK_EQ(http::query_param(req.query(), "flag").value_or("missing"), std::string(""));
  CHECK(!http::query_param(req.query(), "nope"));
}

// ---------------------------------------------------------------------------
// Hashing and policies
// ---------------------------------------------------------------------------

namespace {
std::vector<EndpointPtr> pool_of(int count, std::vector<int> weights = {}) {
  std::vector<EndpointPtr> out;
  for (int i = 0; i < count; ++i) {
    int w = weights.empty() ? 1 : weights[static_cast<size_t>(i)];
    out.push_back(std::make_shared<Endpoint>("b" + std::to_string(i), "127.0.0.1", 9000 + i, w));
  }
  return out;
}

std::map<std::string, int> tally(Balancer& b, const std::vector<EndpointPtr>& pool, int n,
                                 const std::optional<std::string>& key = std::nullopt) {
  std::map<std::string, int> counts;
  for (int i = 0; i < n; ++i) ++counts[b.pick(pool, key)->name];
  return counts;
}
}  // namespace

TEST(hashing, blake2b_matches_reference_vectors) {
  // Reference values from an independent BLAKE2b implementation (8-byte
  // digest, salt = little-endian seed, digest read big-endian).  If these
  // change, every sticky session moves to a different backend.
  CHECK_EQ(hash64("", 0), 16476032584258269876ULL);
  CHECK_EQ(hash64("abc", 0), 15617099051652453721ULL);
  CHECK_EQ(hash64("abc", 1), 16256594602885300803ULL);
  CHECK_EQ(hash64("b0#1", 2), 5601430817169098125ULL);
  CHECK_EQ(hash64(std::string(300, 'x'), 0), 1348047167962039829ULL);
}

TEST(policy, round_robin_is_even) {
  auto pool = pool_of(4);
  RoundRobin rr;
  for (auto& [_, c] : tally(rr, pool, 400)) CHECK_EQ(c, 100);
}

TEST(policy, smooth_wrr_ratio_and_no_bursts) {
  auto pool = pool_of(3, {5, 1, 1});
  SmoothWeightedRoundRobin wrr;
  auto counts = tally(wrr, pool, 700);
  CHECK_EQ(counts["b0"], 500);
  CHECK_EQ(counts["b1"], 100);
  CHECK_EQ(counts["b2"], 100);

  auto two = pool_of(2, {3, 2});
  SmoothWeightedRoundRobin smooth;
  std::string prev;
  int run_length = 0, longest = 0;
  for (int i = 0; i < 10; ++i) {
    std::string name = smooth.pick(two, std::nullopt)->name;
    run_length = name == prev ? run_length + 1 : 1;
    longest = std::max(longest, run_length);
    prev = name;
  }
  CHECK(longest <= 2);   // ABABA, never AAABB
}

TEST(policy, least_conn_picks_idle_and_breaks_ties_randomly) {
  auto pool = pool_of(3);
  pool[0]->inflight = 10;
  pool[1]->inflight = 4;
  LeastConnections lc;
  CHECK_EQ(lc.pick(pool, std::nullopt)->name, std::string("b2"));
  pool[2]->inflight = 4;
  auto counts = tally(lc, pool, 400);
  CHECK(counts["b1"] > 100 && counts["b2"] > 100);
  CHECK_EQ(counts["b0"], 0);
}

TEST(policy, p2c_avoids_the_slow_backend) {
  auto pool = pool_of(2);
  pool[0]->observe_latency(0.500);
  pool[1]->observe_latency(0.001);
  PeakEwmaP2C p2c;
  auto counts = tally(p2c, pool, 500);
  CHECK(counts["b1"] > counts["b0"] * 20);
}

TEST(policy, p2c_in_flight_penalty) {
  auto pool = pool_of(2);
  for (auto& e : pool) e->observe_latency(0.010);
  pool[0]->inflight = 50;
  PeakEwmaP2C p2c;
  CHECK_EQ(tally(p2c, pool, 300)["b0"], 0);
}

TEST(policy, p2c_probes_an_unmeasured_endpoint) {
  // Scoring a never-measured endpoint at a fixed default latency starves it
  // on any fleet faster than the default: never picked, so never measured.
  auto pool = pool_of(3);
  pool[0]->observe_latency(0.002);
  pool[1]->observe_latency(0.002);
  PeakEwmaP2C p2c;
  CHECK(tally(p2c, pool, 200)["b2"] > 0);
  // ...but not while its first probe is still outstanding.
  pool[2]->inflight = 1;
  CHECK(pool[2]->load_score() > pool[0]->load_score());
}

TEST(policy, censored_latency_only_raises_the_estimate) {
  Endpoint e("a", "127.0.0.1", 1);
  double now = monotonic();
  e.observe_latency(0.010, now);
  e.observe_lower_bound(0.005, now);   // a lower bound below the estimate says nothing
  CHECK(std::fabs(e.ewma(now) - 0.010) < 1e-9);
  e.observe_lower_bound(0.300, now);   // "at least 300 ms" does
  CHECK(std::fabs(e.ewma(now) - 0.300) < 1e-9);
}

TEST(policy, consistent_hash_is_stable) {
  auto pool = pool_of(4);
  auto ring = build_balancer("consistent_hash");
  ring->notify_membership(pool);
  for (int i = 0; i < 200; ++i) {
    std::string key = "user-" + std::to_string(i);
    CHECK_EQ(ring->pick(pool, key)->name, ring->pick(pool, key)->name);
  }
}

TEST(policy, ring_moves_few_keys_when_a_member_leaves) {
  std::vector<std::string> members;
  for (int i = 0; i < 8; ++i) members.push_back("b" + std::to_string(i));
  ConsistentHashRing before(members, 160);
  ConsistentHashRing after(std::vector<std::string>(members.begin(), members.end() - 1), 160);
  int moved = 0;
  for (int i = 0; i < 5000; ++i) {
    std::string k = "key-" + std::to_string(i);
    moved += before.get(k) != after.get(k);
  }
  CHECK(moved / 5000.0 < 0.25);
}

TEST(policy, maglev_is_balanced_and_minimally_disruptive) {
  std::vector<std::string> seven;
  for (int i = 0; i < 7; ++i) seven.push_back("b" + std::to_string(i));
  MaglevTable table(seven, 65537);
  int lo = 1 << 30, hi = 0;
  for (auto& [_, c] : table.distribution()) {
    lo = std::min(lo, c);
    hi = std::max(hi, c);
  }
  CHECK(static_cast<double>(hi) / lo < 1.02);

  std::vector<std::string> eight = seven;
  eight.push_back("b7");
  MaglevTable before(eight, 65537), after(seven, 65537);
  int moved = 0;
  for (int i = 0; i < 5000; ++i) {
    std::string k = "key-" + std::to_string(i);
    moved += before.get(k) != after.get(k);
  }
  CHECK(moved / 5000.0 < 0.30);
}

TEST(policy, bounded_load_spills_hot_keys) {
  auto pool = pool_of(3);
  auto ring = build_balancer("consistent_hash", 160, 1.25);
  ring->notify_membership(pool);
  std::string owner = ring->pick(pool, std::string("hot"))->name;
  for (auto& e : pool) e->inflight = e->name == owner ? 50 : 1;
  CHECK(ring->pick(pool, std::string("hot"))->name != owner);
}

TEST(policy, unavailable_owner_falls_back_deterministically) {
  auto pool = pool_of(4);
  auto maglev = build_balancer("maglev");
  maglev->notify_membership(pool);
  std::string owner = maglev->pick(pool, std::string("session-1"))->name;
  std::vector<EndpointPtr> survivors;
  for (auto& e : pool) {
    if (e->name != owner) survivors.push_back(e);
  }
  std::set<std::string> picks;
  for (int i = 0; i < 20; ++i) picks.insert(maglev->pick(survivors, std::string("session-1"))->name);
  CHECK_EQ(picks.size(), 1u);
  CHECK(!picks.count(owner));
}

TEST(policy, every_policy_builds_and_picks) {
  auto pool = pool_of(3);
  for (std::string_view name : kPolicyNames) {
    auto b = build_balancer(std::string(name));
    b->notify_membership(pool);
    EndpointPtr chosen = b->pick(pool, std::string("key"));
    CHECK(std::find(pool.begin(), pool.end(), chosen) != pool.end());
    CHECK_EQ(std::string(b->name()), std::string(name));
  }
  CHECK_THROWS(build_balancer("magic"), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// Circuit breaker
// ---------------------------------------------------------------------------

TEST(breaker, trips_on_consecutive_failures) {
  CircuitBreaker b(0.5, 1000, 10, 5, 60, 3);
  b.record(false, 100);
  b.record(false, 100);
  CHECK(b.state() == BreakerState::Closed);
  CHECK(b.record(false, 100));
  CHECK(b.state() == BreakerState::Open);
  CHECK(!b.allow(100));
}

TEST(breaker, trips_on_error_rate) {
  CircuitBreaker b(0.5, 10, 10, 5, 60, 100);
  for (int i = 0; i < 10; ++i) b.record(i % 2 == 0, 0);
  CHECK(b.state() == BreakerState::Open);
}

TEST(breaker, half_open_probe_then_close) {
  CircuitBreaker b(0.5, 1000, 10, 5, 60, 1);
  b.record(false, 0);
  CHECK(!b.allow(1));
  CHECK(b.allow(6));   // cooldown elapsed: one probe
  CHECK(b.state() == BreakerState::HalfOpen);
  CHECK(!b.allow(6));  // only one at a time
  b.record(true, 6.1);
  CHECK(b.state() == BreakerState::Closed);
}

TEST(breaker, backoff_doubles_on_repeated_trips) {
  CircuitBreaker b(0.5, 1000, 10, 1, 60, 1);
  b.record(false, 0);
  CHECK(!b.allow(0.5));
  CHECK(b.allow(1.5));
  b.record(false, 1.6);   // probe failed
  CHECK(!b.allow(2.6));   // now 2 s, not 1 s
  CHECK(b.allow(3.7));
}

TEST(breaker, window_rolls_off_old_failures) {
  CircuitBreaker b(0.5, 4, 1.0, 5, 60, 100, 1, 10);
  for (int i = 0; i < 4; ++i) b.record(false, 0);
  CHECK(b.state() == BreakerState::Open);
  b.force_close();
  for (int i = 0; i < 4; ++i) b.record(false, i * 0.5);
  CHECK(b.state() == BreakerState::Closed);
}

TEST(breaker, open_breaker_becomes_selectable_after_cooldown) {
  // If selection skipped every OPEN endpoint, allow() -- the only thing that
  // moves OPEN to HALF_OPEN -- would never be called for it, and a backend
  // that fails requests but passes /health would stay ejected forever.
  CircuitBreaker b(0.5, 1000, 10, 5, 60, 1);
  b.record(false, 0);
  CHECK(!b.may_allow(1));
  CHECK(b.may_allow(6));
  CHECK(b.state() == BreakerState::Open);   // may_allow has no side effects
  CHECK(b.allow(6));
  CHECK(!b.may_allow(6));                   // probe outstanding
}

TEST(breaker, lost_probe_expires) {
  // A probe whose request was abandoned never reports back; it must not wedge
  // the breaker half-open forever.
  CircuitBreaker b(0.5, 1000, 10, 5, 60, 1);
  b.record(false, 0);
  CHECK(b.allow(6));
  CHECK(!b.allow(7));
  CHECK(b.allow(11.5));
}

TEST(breaker, long_idle_does_not_keep_wiping_the_window) {
  // Advancing the window by at most one window per call would make every
  // record() after an hour idle clear all buckets again, so the rate trip
  // could not fire for ~360 calls.
  CircuitBreaker b(0.5, 4, 1.0, 5, 60, 100, 1, 10);
  b.record(true, 0);
  b.record(false, 100.00);
  b.record(false, 100.01);
  b.record(false, 100.02);
  CHECK(b.state() == BreakerState::Closed);
  b.record(false, 100.03);
  CHECK(b.state() == BreakerState::Open);
}

// ---------------------------------------------------------------------------
// Rate limits, retry budget, gradient limiter
// ---------------------------------------------------------------------------

TEST(limits, token_bucket_refills) {
  TokenBucket bucket(10, 2.0);
  CHECK(bucket.allow(1, 0.0));
  CHECK(bucket.allow(1, 0.0));
  CHECK(!bucket.allow(1, 0.0));
  CHECK(bucket.allow(1, 0.2));
}

TEST(limits, keyed_limiter_is_per_key_and_bounded) {
  KeyedRateLimiter limiter(1, 1.0);
  CHECK(limiter.allow("a"));
  CHECK(!limiter.allow("a"));
  CHECK(limiter.allow("b"));
  KeyedRateLimiter small(1, 1.0, 4);
  for (int i = 0; i < 100; ++i) small.allow("ip-" + std::to_string(i));
  CHECK_EQ(small.size(), 4u);
  KeyedRateLimiter off(0);
  for (int i = 0; i < 1000; ++i) CHECK(off.allow("a"));
}

TEST(limits, retry_budget_caps_amplification) {
  RetryBudget budget(0.1, 0.0, 10.0);
  for (int i = 0; i < 100; ++i) budget.record_request(0.0);
  int allowed = 0;
  for (int i = 0; i < 50; ++i) allowed += budget.try_retry(0.0);
  CHECK_EQ(allowed, 10);

  RetryBudget floor(0.0, 1.0, 10.0);
  floor.record_request(0.0);
  allowed = 0;
  for (int i = 0; i < 20; ++i) allowed += floor.try_retry(0.0);
  CHECK_EQ(allowed, 10);
}

TEST(limits, gradient_limiter) {
  GradientLimiter limiter(20, 4, 4096, 0.5);
  for (int i = 0; i < 40; ++i) limiter.update(0.010, 1);
  double baseline = limiter.limit();
  for (int i = 0; i < 200; ++i) limiter.update(0.100, 1);
  CHECK(limiter.limit() < baseline);

  GradientLimiter drop(100, 4, 4096, 0.2, 500, 0.5);
  drop.update(0.01, 1, true);
  CHECK_EQ(drop.limit(), 50.0);

  GradientLimiter floor(10, 4, 4096, 0.2, 500, 0.5);
  for (int i = 0; i < 50; ++i) floor.update(0.01, 1, true);
  CHECK_EQ(floor.limit(), 4.0);
}

// ---------------------------------------------------------------------------
// Admission controller (needs a running loop)
// ---------------------------------------------------------------------------

namespace {
Task<void> acquire_into(AdmissionController* gate, std::optional<bool>* out) {
  bool admitted = co_await gate->acquire();
  *out = admitted;
}

Task<void> admission_basic() {
  AdmissionController gate(nullptr, 0.005, 0.1, 0, 2);
  bool a = co_await gate.acquire();
  bool b = co_await gate.acquire();
  bool c = co_await gate.acquire();   // queue size 0: shed
  CHECK(a && b && !c);
  gate.release();
  bool d = co_await gate.acquire();
  CHECK(d);
}

Task<void> admission_promotes() {
  AdmissionController gate(nullptr, 1.0, 0.1, 4, 1);
  bool first = co_await gate.acquire();
  CHECK(first);
  std::optional<bool> waiter;
  spawn(acquire_into(&gate, &waiter));
  co_await sleep_for(0.005);
  CHECK(!waiter);
  CHECK_EQ(gate.queued(), 1u);
  gate.release();
  co_await sleep_for(0.005);
  CHECK(waiter && *waiter);
  gate.release();
}

Task<void> admission_codel_sheds() {
  AdmissionController gate(nullptr, 0.001, 0.005, 64, 1);
  bool first = co_await gate.acquire();
  CHECK(first);
  std::vector<std::optional<bool>> results(20);
  for (auto& r : results) spawn(acquire_into(&gate, &r));
  co_await sleep_for(0.05);   // let the sojourn time stand above target
  gate.release();
  for (int i = 0; i < 200; ++i) {
    co_await sleep_for(0.005);
    if (gate.inflight() > 0) gate.release();
    bool all = std::all_of(results.begin(), results.end(), [](const auto& r) { return r.has_value(); });
    if (all) break;
  }
  bool shed = false, all_done = true;
  for (auto& r : results) {
    all_done = all_done && r.has_value();
    shed = shed || (r && !*r);
  }
  CHECK(all_done);
  CHECK(shed);   // CoDel never shed a standing queue otherwise
  while (gate.inflight() > 0) gate.release();
}
}  // namespace

TEST(admission, admits_up_to_the_limit) { run(admission_basic()); }
TEST(admission, queued_request_is_promoted_on_release) { run(admission_promotes()); }
TEST(admission, codel_sheds_a_standing_queue) { run(admission_codel_sheds()); }

// ---------------------------------------------------------------------------
// Connection pool (real sockets)
// ---------------------------------------------------------------------------

namespace {
std::pair<SocketPtr, SocketPtr> socket_pair() {
  int fds[2];
  if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) throw IoError(errno, "socketpair");
  return {std::make_shared<Socket>(fds[0]), std::make_shared<Socket>(fds[1])};
}
}  // namespace

TEST(pool, release_reuse_and_bounds) {
  Loop loop;
  ConnectionPool pool(2, 30.0);
  auto [a, a_peer] = socket_pair();
  CHECK(pool.release(a));
  CHECK_EQ(pool.size(), 1u);
  CHECK(pool.acquire() == a);
  CHECK_EQ(pool.size(), 0u);

  auto [closed, closed_peer] = socket_pair();
  closed->close();
  CHECK(!pool.release(closed));

  std::vector<std::pair<SocketPtr, SocketPtr>> pairs;
  for (int i = 0; i < 5; ++i) {
    pairs.push_back(socket_pair());
    pool.release(pairs.back().first);
  }
  CHECK_EQ(pool.size(), 2u);
  pool.clear();
}

TEST(pool, socket_closed_by_peer_is_not_reused) {
  Loop loop;
  ConnectionPool pool(4, 30.0);
  auto [mine, theirs] = socket_pair();
  CHECK(pool.release(mine));
  theirs->close();
  CHECK(pool.acquire() == nullptr);

  auto [chatty, peer] = socket_pair();
  CHECK(pool.release(chatty));
  CHECK(::write(peer->fd(), "x", 1) == 1);   // unsolicited bytes would corrupt the next response
  CHECK(pool.acquire() == nullptr);
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

namespace {
std::string config_error(const std::string& text) {
  try {
    Config::from_text(text);
  } catch (const ConfigError& e) {
    return e.what();
  }
  return "";
}
const char* kMinimal = R"({"endpoints": [{"name": "a", "address": "127.0.0.1:9001"}]})";
}  // namespace

TEST(config, shipped_lb_json_is_valid) {
  ::unsetenv("JDBLANCE_WORKERS");
  Config cfg = Config::load("lb.json");
  CHECK_EQ(cfg.endpoints.size(), 4u);
  CHECK_EQ(cfg.listen.workers, 1);
  CHECK_EQ(cfg.policy, std::string("p2c_ewma"));
  CHECK(cfg.hedge.enabled);
  CHECK(!cfg.source_path.empty());
}

TEST(config, defaults_and_minimal_config) {
  Config cfg = Config::from_text(kMinimal);
  CHECK_EQ(cfg.listen.port, 8000);
  CHECK_EQ(cfg.limits.concurrency.max_queue, 1024);
  CHECK(!cfg.hedge.enabled);
  CHECK_EQ(cfg.endpoints[0].host(), std::string("127.0.0.1"));
  CHECK_EQ(cfg.endpoints[0].port(), 9001);
}

TEST(config, comments_are_stripped_outside_strings_only) {
  Config cfg = Config::from_text(R"({
    // a comment
    "health": {"path": "/x//y#z"},  # another
    "endpoints": [{"name": "a", "address": "h:1"}]
  })");
  CHECK_EQ(cfg.health.path, std::string("/x//y#z"));
}

TEST(config, env_expansion) {
  ::setenv("JDBLANCE_TEST_PORT", "9123", 1);
  ::unsetenv("JDBLANCE_TEST_UNSET");
  Config cfg = Config::from_text(R"({"listen": {"port": ${JDBLANCE_TEST_PORT:-1}, "backlog": ${JDBLANCE_TEST_UNSET:-77}},
    "endpoints": [{"name": "a", "address": "h:1"}]})");
  CHECK_EQ(cfg.listen.port, 9123);
  CHECK_EQ(cfg.listen.backlog, 77);
}

TEST(config, errors_name_the_offending_key) {
  CHECK_EQ(config_error(R"({"limits": {"concurrency": {"bogus": 1}}, "endpoints": [{"name":"a","address":"h:1"}]})"),
           std::string("limits.concurrency: unknown key bogus"));
  CHECK_EQ(config_error(R"({"listen": {"port": "80"}, "endpoints": [{"name":"a","address":"h:1"}]})"),
           std::string("listen.port: expected an integer, got string"));
  CHECK_EQ(config_error(R"({"listen": {"port": 80.5}, "endpoints": [{"name":"a","address":"h:1"}]})"),
           std::string("listen.port: expected an integer, got 80.5"));
  CHECK(config_error(R"({"policy": "p2c_ewma", "policy": "maglev", "endpoints": []})").find("duplicate key") !=
        std::string::npos);
  CHECK(config_error("{nope}").find("invalid JSON") == 0);
}

TEST(config, validation) {
  auto with = [](const std::string& extra, const std::string& endpoints = R"([{"name":"a","address":"h:1"}])") {
    return config_error("{" + extra + (extra.empty() ? "" : ",") + "\"endpoints\": " + endpoints + "}");
  };
  CHECK(with("") == "");
  CHECK(with("", "[]").find("at least one endpoint") != std::string::npos);
  CHECK(with("", R"([{"name":"a","address":"h:1"},{"name":"a","address":"h:2"}])").find("unique") != std::string::npos);
  CHECK(with("", R"([{"name":"a","address":"h:99999"}])").find("bad port") != std::string::npos);
  CHECK(with("", R"([{"name":"a","address":"h"}])").find("host:port") != std::string::npos);
  CHECK(with("", R"([{"name":"a","address":"h:1","weight":0}])").find("weight") != std::string::npos);
  CHECK(with("", R"([{"name":"a b","address":"h:1"}])").find("name may only") != std::string::npos);
  CHECK(with("", R"([{"name":"a","address":"<script>:1"}])").find("invalid host") != std::string::npos);
  CHECK(with("", R"([{"address":"h:1"}])").find("name is required") != std::string::npos);
  CHECK(with(R"("policy": "magic")").find("unknown policy") != std::string::npos);
  CHECK(with(R"("hash_key": {"source": "header"})").find("requires hash_key.name") != std::string::npos);
  CHECK(with(R"("maglev_table_size": 65536)").find("prime") != std::string::npos);
  CHECK(with(R"("panic_threshold": 1.5)").find("panic_threshold") != std::string::npos);
  CHECK(with(R"("server_name": "evil\r\nX: y")").find("server_name") != std::string::npos);
  CHECK(with(R"("limits": {"concurrency": {"initial": 2}})").find("initial") != std::string::npos);
  CHECK(with(R"("endpoints6": 1)").find("unknown key endpoints6") != std::string::npos);
}

TEST(config, json_roundtrip) {
  Config cfg = Config::load("lb.json");
  Config again = Config::from_json(cfg.to_json());
  CHECK_EQ(again.to_json().dump(), cfg.to_json().dump());
}

TEST(config, cli_endpoint_overrides) {
  EndpointConfig a = parse_endpoint_override("a=10.0.0.1:8080");
  CHECK_EQ(a.address, std::string("10.0.0.1:8080"));
  CHECK_EQ(a.weight, 1);
  EndpointConfig b = parse_endpoint_override("b=host:80:3");
  CHECK_EQ(b.address, std::string("host:80"));
  CHECK_EQ(b.weight, 3);
  EndpointConfig c = parse_endpoint_override("c=[::1]:80:2");
  CHECK_EQ(c.address, std::string("[::1]:80"));
  CHECK_EQ(c.host(), std::string("[::1]"));
  CHECK_EQ(c.weight, 2);
  for (const char* bad : {"noequals", "=h:1", "a=h", "a=h:1:x", "a=[::1]80"}) {
    CHECK_THROWS(parse_endpoint_override(bad), ConfigError);
  }
  CliOverrides o;
  o.policy = "maglev";
  o.endpoints = {a};
  Config cfg = Config::load("lb.json");
  o.apply(cfg);
  CHECK_EQ(cfg.policy, std::string("maglev"));
  CHECK_EQ(cfg.endpoints.size(), 1u);
}

// ---------------------------------------------------------------------------
// JSON
// ---------------------------------------------------------------------------

TEST(json, parse_and_dump) {
  Json v = Json::parse(R"({"a": [1, 2.5, -3e2, true, false, null], "b": "x\"y\\z\n", "c": {}})");
  CHECK_EQ(v.find("a")->as_array().size(), 6u);
  CHECK_EQ(v.find("a")->as_array()[2].as_number(), -300.0);
  CHECK_EQ(v.find("b")->as_string(), std::string("x\"y\\z\n"));
  CHECK_EQ(v.dump(), std::string(R"({"a":[1,2.5,-300,true,false,null],"b":"x\"y\\z\n","c":{}})"));
  CHECK_EQ(Json::parse(v.dump(2)).dump(), v.dump());
}

TEST(json, unicode) {
  CHECK_EQ(Json::parse(R"("\u00e9\ud83d\ude00")").as_string(), std::string("\xc3\xa9\xf0\x9f\x98\x80"));
  CHECK_EQ(Json("\xc3\xa9").dump(), std::string("\"\xc3\xa9\""));   // valid UTF-8 passes through
  CHECK_EQ(Json(std::string("a\xff" "b")).dump(), std::string("\"a\\u00ffb\""));   // invalid bytes escaped
  CHECK_EQ(Json(std::string("\x01")).dump(), std::string("\"\\u0001\""));
}

TEST(json, rejects_invalid_documents) {
  for (const char* bad : {"[1,]", "{\"a\":1,}", "01", "1.", "-", "\"\\ud800\"", "\"a\nb\"", "[1] 2",
                          "{'a':1}", "nul", "\"\\x41\"", "1e999"}) {
    CHECK_THROWS(Json::parse(bad), JsonError);
  }
  std::string deep(1000, '[');
  CHECK_THROWS(Json::parse(deep), JsonError);
}

TEST(json, numbers) {
  CHECK_EQ(Json(0.1).dump(), std::string("0.1"));
  CHECK_EQ(Json(3.0).dump(), std::string("3"));
  CHECK_EQ(Json(std::nan("")).dump(), std::string("null"));
  CHECK_EQ(round_to(2.34567, 3), 2.346);
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

TEST(metrics, exposition_format) {
  metrics::Registry reg;
  metrics::Counter c("t_total", "Help\nwith newline", {"a"}, &reg);
  metrics::Gauge g("t_gauge", "g", {}, &reg);
  metrics::Histogram h("t_seconds", "h", {}, {0.1, 1.0}, &reg);
  c.inc({"x\"y"}, 2);
  g.set(1.5);
  h.observe(0.05);
  h.observe(0.5);
  h.observe(5);
  std::string out = reg.render();
  CHECK(out.find("# HELP t_total Help\\nwith newline\n") != std::string::npos);
  CHECK(out.find("t_total{a=\"x\\\"y\"} 2\n") != std::string::npos);
  CHECK(out.find("t_gauge 1.5\n") != std::string::npos);
  CHECK(out.find("t_seconds_bucket{le=\"0.1\"} 1\n") != std::string::npos);
  CHECK(out.find("t_seconds_bucket{le=\"1\"} 2\n") != std::string::npos);
  CHECK(out.find("t_seconds_bucket{le=\"+Inf\"} 3\n") != std::string::npos);
  CHECK(out.find("t_seconds_count 3\n") != std::string::npos);
  CHECK_THROWS(c.inc({}), std::logic_error);
}

TEST(metrics, histogram_quantile) {
  metrics::Registry reg;
  metrics::Histogram h("q_seconds", "h", {}, {0.01, 0.1, 1.0}, &reg);
  CHECK(!h.quantile(0.5));
  for (int i = 0; i < 90; ++i) h.observe(0.005);
  for (int i = 0; i < 10; ++i) h.observe(0.05);
  CHECK(std::fabs(*h.quantile(0.5) - 0.005555) < 1e-4);   // inside the first bucket
  CHECK(std::fabs(*h.quantile(0.95) - 0.055) < 1e-9);    // halfway through the second
  h.observe(50);
  CHECK_EQ(*h.quantile(1.0), 1.0);   // +Inf bucket: last finite bound
}

TEST(origin, html_escape) {
  CHECK_EQ(html_escape("<a href=\"x\">'&'</a>"), std::string("&lt;a href=&quot;x&quot;&gt;&#39;&amp;&#39;&lt;/a&gt;"));
}
