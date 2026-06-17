#include "origin.hpp"

#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <cstring>

#include "balancer.hpp"
#include "json.hpp"
#include "origin_page.hpp"

namespace jdblance {

std::string html_escape(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    switch (c) {
      case '&': out += "&amp;"; break;
      case '<': out += "&lt;"; break;
      case '>': out += "&gt;"; break;
      case '"': out += "&quot;"; break;
      case '\'': out += "&#39;"; break;
      default: out.push_back(c);
    }
  }
  return out;
}

namespace {
std::string hostname() {
  char buf[256] = "unknown";
  ::gethostname(buf, sizeof buf - 1);
  return buf;
}

std::string route_name(std::string_view path) {
  if (path == "/") return "home";
  static const char* names[] = {"health", "slow", "echo", "cpu", "stats", "info", "chaos", "metrics"};
  for (const char* n : names) {
    if (path.size() == std::strlen(n) + 1 && path.substr(1) == n) return n;
  }
  if (path.rfind("/static/", 0) == 0) return "static";
  return "unknown";
}

bool truthy(const Json& v) {
  if (v.is_bool()) return v.as_bool();
  if (v.is_number()) return v.as_number() != 0;
  if (v.is_null()) return false;
  std::string s = v.is_string() ? http::to_lower(v.as_string()) : "x";
  return !(s.empty() || s == "0" || s == "false" || s == "no" || s == "none");
}

double number_of(const Json& v) {
  if (v.is_number()) return v.as_number();
  if (v.is_string()) {
    const std::string& s = v.as_string();
    char* end = nullptr;
    double d = std::strtod(s.c_str(), &end);
    if (end != s.c_str() && *end == '\0' && std::isfinite(d)) return d;
  }
  throw http::ProtocolError("expected a number", 400);
}

std::string replace_all(std::string s, std::string_view from, std::string_view to) {
  for (size_t at = s.find(from); at != std::string::npos; at = s.find(from, at + to.size())) {
    s.replace(at, from.size(), to);
  }
  return s;
}
}  // namespace

Origin::Origin(std::string name_, double base_latency_, double jitter_, int capacity_,
               double fail_rate_)
    : name(std::move(name_)),
      base_latency(base_latency_),
      jitter(jitter_),
      capacity(capacity_),
      fail_rate(fail_rate_),
      started(wall_time()),
      requests_total_("app_requests_total", "Requests handled", {"container", "endpoint", "status"},
                      &registry),
      duration_("app_request_duration_seconds", "Handler latency", {"container", "endpoint"},
                {.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}, &registry),
      inflight_gauge_("app_requests_in_flight", "Concurrent requests", {"container"}, &registry),
      health_gauge_("app_health", "1 when the service reports healthy", {"container"}, &registry),
      chaos_gauge_("app_chaos", "Active fault injection", {"container", "kind"}, &registry) {
  health_gauge_.set({name}, 1);
  inflight_gauge_.set({name}, 0);
}

Origin::~Origin() {
  if (listener_) listener_->close();
}

void Origin::listen(const std::string& host, int port) {
  int fd = listen_tcp(host, port, 1024, false);
  listener_ = std::make_shared<Socket>(fd);
  port_ = listener_->local_port();
  spawn(accept_loop(shared_from_this(), listener_));
}

void Origin::close() {
  if (listener_) listener_->close();
  for (Socket* s : conns_) s->cancel();
}

double Origin::service_time() {
  double delay = base_latency + (jitter > 0 ? uniform(0.0, jitter) : 0.0);
  if (capacity > 0 && inflight > capacity) {
    // Every request past capacity waits behind the ones being served: linear
    // in the overflow, the honest first-order model of a saturated pool.
    delay += (base_latency > 0 ? base_latency : 0.001) * (inflight - capacity);
  }
  return delay;
}

Task<void> Origin::accept_loop(std::shared_ptr<Origin> self, SocketPtr listener) {
  for (;;) {
    SocketPtr socket;
    try {
      socket = co_await accept_one(*listener);
    } catch (const std::exception&) {
      co_return;
    }
    spawn(handle(self, std::move(socket)));
  }
}

void Origin::observe(const std::string& route, int status, double seconds) {
  requests_total_.inc({name, route, std::to_string(status)});
  duration_.observe({name, route}, seconds);
}

Task<void> Origin::handle(std::shared_ptr<Origin> self, SocketPtr socket) {
  self->conns_.insert(socket.get());
  BufReader reader(socket);
  try {
    for (;;) {
      http::Request req;
      std::optional<http::ProtocolError> bad;
      try {
        req = co_await http::read_request(reader, monotonic() + 65.0);
      } catch (const http::ProtocolError& e) {
        bad = e;
      }
      if (bad) {
        co_await socket->write_all(http::simple_response(bad->status, bad->message + "\n"),
                                   monotonic() + 5.0);
        break;
      }
      http::Framing framing = http::request_framing(req);
      std::string body;
      if (!framing.empty()) {
        std::optional<std::string> buffered =
            co_await http::read_body_into_memory(reader, framing, 16u << 20, monotonic() + 65.0);
        if (!buffered) {
          co_await socket->write_all(http::simple_response(413, "body too large\n"), monotonic() + 5.0);
          break;
        }
        body = std::move(*buffered);
      }

      bool keep_alive = req.version == "HTTP/1.0" ? req.connection_has("keep-alive")
                                                  : !req.connection_has("close");
      double began = monotonic();
      ++self->inflight;
      self->peak_inflight = std::max(self->peak_inflight, self->inflight);
      self->inflight_gauge_.set({self->name}, self->inflight);
      std::optional<Reply> reply;
      std::exception_ptr failure;
      try {
        reply = co_await self->route(req, body, reader);
      } catch (...) {
        failure = std::current_exception();
      }
      --self->inflight;
      self->inflight_gauge_.set({self->name}, self->inflight);
      if (failure) std::rethrow_exception(failure);
      if (!reply) break;   // stalled: the connection is intentionally abandoned

      ++self->requests;
      if (reply->status >= 500) ++self->failures;
      self->observe(route_name(req.path()), reply->status, monotonic() - began);
      http::Headers extra = {{"X-Origin", self->name}, {"X-Backend", self->name}};
      std::string wire = http::simple_response(reply->status, reply->body, reply->content_type,
                                               !keep_alive, extra);
      if (req.method == "HEAD") wire.resize(wire.size() - reply->body.size());
      co_await socket->write_all(wire, monotonic() + 30.0);
      if (!keep_alive) break;
    }
  } catch (const std::exception&) {
    // Peer went away, timed out, or the origin was closed under it.
  }
  self->conns_.erase(socket.get());
  socket->close();
}

Task<std::optional<Origin::Reply>> Origin::route(const http::Request& req, const std::string& body,
                                                 BufReader& reader) {
  std::string_view path = req.path();
  std::string_view query = req.query();
  const char* json = "application/json";

  if (path == "/chaos") {
    if (req.method != "POST") co_return Reply{405, "{\"error\":\"POST only\"}\n", json};
    co_return chaos(req, body);
  }
  if (path == "/health") {
    if (!healthy) co_return Reply{503, "{\"status\":\"unhealthy\",\"container\":" + json_quote(name) + "}\n", json};
    co_return Reply{200, "{\"status\":\"ok\",\"container\":" + json_quote(name) + "}\n", json};
  }
  if (path == "/stats" || path == "/info") {
    Json out = Json::object();
    out["name"] = name;
    out["container"] = name;
    out["pid"] = static_cast<long long>(::getpid());
    out["requests"] = requests;
    out["failures"] = failures;
    out["in_flight"] = inflight;
    out["peak_in_flight"] = peak_inflight;
    out["capacity"] = capacity;
    out["healthy"] = healthy;
    out["uptime"] = round_to(wall_time() - started, 1);
    Json state = Json::object();
    state["fail_rate"] = fail_rate;
    state["latency_ms"] = base_latency * 1000.0;
    state["jitter_ms"] = jitter * 1000.0;
    state["capacity"] = capacity;
    state["stall"] = stall;
    state["healthy"] = healthy;
    out["chaos"] = std::move(state);
    co_return Reply{200, out.dump() + "\n", json};
  }
  if (path == "/metrics") {
    co_return Reply{200, registry.render(), "text/plain; version=0.0.4; charset=utf-8"};
  }
  if (path == "/static/css/style.css") co_return Reply{200, origin_page::kStyleCss, "text/css; charset=utf-8"};
  if (path == "/static/js/script.js") {
    co_return Reply{200, origin_page::kScriptJs, "application/javascript; charset=utf-8"};
  }

  if (stall) {
    // Accept and never answer -- until the client gives up or we are closed.
    char sink[512];
    for (;;) {
      size_t n = co_await reader.read_into(sink, sizeof sink, monotonic() + 3600.0);
      if (n == 0) break;
    }
    co_return std::nullopt;
  }

  double delay = service_time();
  if (path == "/slow") {
    std::optional<std::string> ms = http::query_param(query, "ms");
    delay += (ms ? std::atof(ms->c_str()) : 100.0) / 1000.0;
  }
  if (delay > 0) co_await sleep_for(delay);

  if (path == "/cpu") {
    std::optional<std::string> ms = http::query_param(query, "ms");
    double budget = (ms ? std::atof(ms->c_str()) : 10.0) / 1000.0;
    double deadline = monotonic() + budget;
    uint64_t iterations = 0;
    while (monotonic() < deadline) ++iterations;   // genuinely burn the loop
    Json out = Json::object();
    out["name"] = name;
    out["iterations"] = iterations;
    co_return Reply{200, out.dump() + "\n", json};
  }

  if (fail_rate > 0 && uniform(0.0, 1.0) < fail_rate) {
    co_return Reply{500, "{\"name\":" + json_quote(name) + ",\"error\":\"injected\"}\n", json};
  }

  if (path == "/echo") {
    co_return Reply{200, body, req.get_or("content-type", "application/octet-stream")};
  }
  co_return home(req);
}

Origin::Reply Origin::home(const http::Request& req) {
  std::string accept = req.get_or("accept");
  if (accept.find("text/html") != std::string::npos) {
    std::string page = origin_page::kIndexHtml;
    page = replace_all(page, "@@CONTAINER@@", html_escape(name));
    page = replace_all(page, "@@REQUEST_ID@@", html_escape(req.get_or("x-request-id", "-")));
    page = replace_all(page, "@@FORWARDED_FOR@@", html_escape(req.get_or("x-forwarded-for", "-")));
    return {200, page, "text/html; charset=utf-8"};
  }
  Json out = Json::object();
  out["name"] = name;
  out["container"] = name;
  out["host"] = hostname();
  out["pid"] = static_cast<long long>(::getpid());
  out["path"] = req.target;
  out["in_flight"] = inflight;
  out["served"] = requests;
  out["request_id"] = req.get("x-request-id") ? Json(*req.get("x-request-id")) : Json();
  out["uptime"] = round_to(wall_time() - started, 1);
  out["ts"] = round_to(wall_time(), 3);
  return {200, out.dump() + "\n", "application/json"};
}

Origin::Reply Origin::chaos(const http::Request& req, const std::string& body) {
  // JSON body (what the docker stack and browser page send) or query string.
  Json payload = Json::object();
  bool from_json = false;
  if (!body.empty()) {
    try {
      Json parsed = Json::parse(body);
      if (parsed.is_object()) {
        payload = std::move(parsed);
        from_json = true;
      }
    } catch (const JsonError&) {
    }
  }
  if (!from_json) {
    std::string_view q = req.query();
    size_t start = 0;
    while (start < q.size()) {
      size_t amp = q.find('&', start);
      if (amp == std::string_view::npos) amp = q.size();
      std::string_view part = q.substr(start, amp - start);
      size_t eq = part.find('=');
      if (!part.empty()) {
        payload[part.substr(0, eq)] =
            std::string(eq == std::string_view::npos ? std::string_view() : part.substr(eq + 1));
      }
      start = amp + 1;
    }
  }

  try {
    if (const Json* v = payload.find("reset"); v && truthy(*v)) {
      fail_rate = 0;
      stall = false;
      healthy = true;
      base_latency = jitter = 0;
    }
    if (const Json* v = payload.find("fail_rate")) fail_rate = std::clamp(number_of(*v), 0.0, 1.0);
    if (const Json* v = payload.find("latency_ms")) base_latency = std::max(0.0, number_of(*v)) / 1000.0;
    if (const Json* v = payload.find("jitter_ms")) jitter = std::max(0.0, number_of(*v)) / 1000.0;
    if (const Json* v = payload.find("capacity")) capacity = std::max(0, static_cast<int>(number_of(*v)));
    if (const Json* v = payload.find("stall")) stall = truthy(*v);
    if (const Json* v = payload.find("healthy")) healthy = truthy(*v);
  } catch (const http::ProtocolError& e) {
    return {400, "{\"error\":" + json_quote(e.message) + "}\n", "application/json"};
  }

  health_gauge_.set({name}, healthy ? 1 : 0);
  chaos_gauge_.set({name, "fail_rate"}, fail_rate);
  chaos_gauge_.set({name, "latency_ms"}, base_latency * 1000.0);
  Json out = Json::object();
  out["name"] = name;
  out["container"] = name;
  out["fail_rate"] = fail_rate;
  out["latency_ms"] = base_latency * 1000.0;
  out["jitter_ms"] = jitter * 1000.0;
  out["capacity"] = capacity;
  out["stall"] = stall;
  out["healthy"] = healthy;
  return {200, out.dump() + "\n", "application/json"};
}

}  // namespace jdblance
