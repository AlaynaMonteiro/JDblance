#include "http1.hpp"

#include <algorithm>
#include <cstring>
#include <memory>

namespace jdblance::http {

// ---------------------------------------------------------------------------
// Small string helpers
// ---------------------------------------------------------------------------

namespace {
char lower_char(char c) noexcept { return (c >= 'A' && c <= 'Z') ? static_cast<char>(c + 32) : c; }

bool is_tchar(unsigned char c) noexcept {
  if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) return true;
  return std::strchr("!#$%&'*+-.^_`|~", c) != nullptr && c != 0;
}

// field-value = *( VCHAR / obs-text / SP / HTAB ); no CTLs, in particular no
// CR or NUL, which some origins treat as line terminators.
bool valid_field_value(std::string_view v) noexcept {
  for (unsigned char c : v) {
    if ((c < 0x20 && c != '\t') || c == 0x7F) return false;
  }
  return true;
}

const std::vector<std::string_view> kHopByHop = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "proxy-connection",
    "te",         "trailer",    "transfer-encoding",  "upgrade",
};

std::vector<std::string> split_list(std::string_view value) {
  std::vector<std::string> out;
  size_t start = 0;
  while (start <= value.size()) {
    size_t comma = value.find(',', start);
    if (comma == std::string_view::npos) comma = value.size();
    std::string_view item = trim(value.substr(start, comma - start));
    if (!item.empty()) out.push_back(to_lower(item));
    start = comma + 1;
  }
  return out;
}
}  // namespace

bool iequals(std::string_view a, std::string_view b) noexcept {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); ++i) {
    if (lower_char(a[i]) != lower_char(b[i])) return false;
  }
  return true;
}

std::string to_lower(std::string_view s) {
  std::string out(s);
  for (char& c : out) c = lower_char(c);
  return out;
}

std::string_view trim(std::string_view s) noexcept {
  while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.remove_prefix(1);
  while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) s.remove_suffix(1);
  return s;
}

bool is_token(std::string_view s) noexcept {
  if (s.empty()) return false;
  for (unsigned char c : s) {
    if (!is_tchar(c)) return false;
  }
  return true;
}

const char* status_text(int status) noexcept {
  switch (status) {
    case 100: return "Continue";
    case 101: return "Switching Protocols";
    case 200: return "OK";
    case 201: return "Created";
    case 202: return "Accepted";
    case 204: return "No Content";
    case 206: return "Partial Content";
    case 301: return "Moved Permanently";
    case 302: return "Found";
    case 304: return "Not Modified";
    case 400: return "Bad Request";
    case 401: return "Unauthorized";
    case 403: return "Forbidden";
    case 404: return "Not Found";
    case 405: return "Method Not Allowed";
    case 408: return "Request Timeout";
    case 411: return "Length Required";
    case 413: return "Content Too Large";
    case 414: return "URI Too Long";
    case 429: return "Too Many Requests";
    case 431: return "Request Header Fields Too Large";
    case 500: return "Internal Server Error";
    case 501: return "Not Implemented";
    case 502: return "Bad Gateway";
    case 503: return "Service Unavailable";
    case 504: return "Gateway Timeout";
    case 505: return "HTTP Version Not Supported";
    default: return "Unknown";
  }
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

const std::string* Message::get(std::string_view name) const noexcept {
  for (const auto& [k, v] : headers) {
    if (iequals(k, name)) return &v;
  }
  return nullptr;
}

std::string Message::get_or(std::string_view name, std::string fallback) const {
  const std::string* v = get(name);
  return v ? *v : std::move(fallback);
}

std::vector<std::string> Message::get_all(std::string_view name) const {
  std::vector<std::string> out;
  for (const auto& [k, v] : headers) {
    if (iequals(k, name)) out.push_back(v);
  }
  return out;
}

std::vector<std::string> Message::connection_tokens() const {
  std::vector<std::string> out;
  for (const auto& [k, v] : headers) {
    if (!iequals(k, "connection")) continue;
    for (auto& token : split_list(v)) out.push_back(std::move(token));
  }
  return out;
}

bool Message::connection_has(std::string_view token) const {
  // Asked several times per request: scan in place, no token vector.
  for (const auto& [k, v] : headers) {
    if (!iequals(k, "connection")) continue;
    std::string_view rest = v;
    for (;;) {
      size_t comma = rest.find(',');
      if (iequals(trim(rest.substr(0, comma)), token)) return true;
      if (comma == std::string_view::npos) break;
      rest.remove_prefix(comma + 1);
    }
  }
  return false;
}

std::string_view Request::path() const noexcept {
  std::string_view t = target;
  return t.substr(0, t.find('?'));
}

std::string_view Request::query() const noexcept {
  std::string_view t = target;
  size_t q = t.find('?');
  return q == std::string_view::npos ? std::string_view() : t.substr(q + 1);
}

bool Request::idempotent() const noexcept {
  return method == "GET" || method == "HEAD" || method == "OPTIONS" || method == "TRACE" ||
         method == "PUT" || method == "DELETE";
}

bool Request::expects_continue() const {
  for (const auto& v : get_all("expect")) {
    if (iequals(trim(v), "100-continue")) return true;
  }
  return false;
}

std::optional<std::string> Request::upgrade_target() const {
  if (!connection_has("upgrade")) return std::nullopt;
  const std::string* v = get("upgrade");
  if (v == nullptr) return std::nullopt;
  std::string out = to_lower(trim(*v));
  if (out.empty()) return std::nullopt;
  return out;
}

std::optional<std::string> query_param(std::string_view query, std::string_view name) {
  size_t start = 0;
  while (start <= query.size()) {
    size_t amp = query.find('&', start);
    if (amp == std::string_view::npos) amp = query.size();
    std::string_view part = query.substr(start, amp - start);
    size_t eq = part.find('=');
    std::string_view key = part.substr(0, eq);
    if (!part.empty() && key == name) {
      return std::string(eq == std::string_view::npos ? std::string_view() : part.substr(eq + 1));
    }
    start = amp + 1;
  }
  return std::nullopt;
}

// ---------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------

namespace {
Framing framing_from_headers(const Message& msg, bool is_response) {
  std::vector<std::string> te = msg.get_all("transfer-encoding");
  std::vector<std::string> cl = msg.get_all("content-length");

  if (!te.empty()) {
    if (!cl.empty()) throw ProtocolError("Content-Length with Transfer-Encoding", 400);
    std::vector<std::string> codings;
    for (const auto& v : te) {
      for (auto& c : split_list(v)) codings.push_back(std::move(c));
    }
    size_t chunked = static_cast<size_t>(std::count(codings.begin(), codings.end(), "chunked"));
    if (!is_response) {
      // Requests: exactly `chunked` or nothing.  Anything else either cannot
      // be framed (RFC 9112 6.3) or would be re-framed with its codings lost.
      if (codings.size() != 1 || chunked != 1) {
        throw ProtocolError("unsupported Transfer-Encoding", 400);
      }
      return Framing{BodyKind::Chunked, 0, "chunked"};
    }
    if (chunked > 1) throw ProtocolError("chunked applied more than once", 502);
    std::string joined;
    for (size_t i = 0; i < codings.size(); ++i) joined += (i ? ", " : "") + codings[i];
    if (codings.empty() || codings.back() != "chunked") {
      // Unknown final coding: the length is "until close" (RFC 9112 6.3).
      if (chunked != 0) throw ProtocolError("chunked is not the final coding", 502);
      return Framing{BodyKind::Eof, 0, joined};
    }
    return Framing{BodyKind::Chunked, 0, joined};
  }

  if (!cl.empty()) {
    int status = is_response ? 502 : 400;
    std::string value;
    bool first = true;
    for (const auto& header : cl) {
      std::string_view rest = header;
      for (;;) {
        size_t comma = rest.find(',');
        std::string_view item = trim(rest.substr(0, comma));
        if (item.empty()) throw ProtocolError("malformed Content-Length", status);
        if (first) {
          value = std::string(item);
          first = false;
        } else if (item != value) {
          throw ProtocolError("conflicting Content-Length", status);
        }
        if (comma == std::string_view::npos) break;
        rest.remove_prefix(comma + 1);
      }
    }
    if (value.size() > 18 ||
        !std::all_of(value.begin(), value.end(), [](char c) { return c >= '0' && c <= '9'; })) {
      throw ProtocolError("malformed Content-Length", status);
    }
    return Framing{BodyKind::Length, std::stoull(value), ""};
  }

  return is_response ? Framing{BodyKind::Eof, 0, ""} : Framing{BodyKind::Length, 0, ""};
}
}  // namespace

Framing request_framing(const Request& req) { return framing_from_headers(req, false); }

Framing response_framing(std::string_view request_method, const Response& resp) {
  // RFC 9112 6.3: these never carry a body, whatever the headers say.
  if (request_method == "HEAD" || resp.status == 204 || resp.status == 304 ||
      (resp.status >= 100 && resp.status < 200)) {
    return Framing{BodyKind::None, 0, ""};
  }
  if (request_method == "CONNECT" && resp.status >= 200 && resp.status < 300) {
    return Framing{BodyKind::None, 0, ""};
  }
  return framing_from_headers(resp, true);
}

// ---------------------------------------------------------------------------
// Head parsing
// ---------------------------------------------------------------------------

namespace {
std::string_view strip_eol(std::string_view line) noexcept {
  if (!line.empty() && line.back() == '\n') line.remove_suffix(1);
  if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
  return line;
}

Task<std::vector<std::string>> read_head_lines(BufReader& reader, double deadline,
                                               bool is_request, size_t* raw_size) {
  std::vector<std::string> lines;
  size_t total = 0;
  int leading_blanks = 0;
  for (;;) {
    std::string line;
    try {
      line = co_await reader.read_line(kMaxHeadBytes, deadline);
    } catch (const LineTooLong&) {
      throw ProtocolError("header line too long", 431);
    }
    if (line.empty()) {
      if (lines.empty()) throw ConnectionClosed();
      throw ProtocolError("truncated head", is_request ? 400 : 502);
    }
    total += line.size();
    if (total > kMaxHeadBytes) throw ProtocolError("head too large", 431);
    if (line.back() != '\n') throw ProtocolError("truncated head", is_request ? 400 : 502);
    std::string_view stripped = strip_eol(line);
    if (stripped.empty()) {
      if (lines.empty()) {
        // RFC 9112 2.2: tolerate a stray CRLF before the request line (old
        // clients append one to POST bodies).  Bounded, so it is not a way to
        // hold the connection open for free.
        if (is_request && ++leading_blanks <= 4) continue;
        throw ProtocolError("missing start line", is_request ? 400 : 502);
      }
      *raw_size = total;
      co_return lines;
    }
    if (lines.size() >= kMaxHeaders + 1) throw ProtocolError("too many header fields", 431);
    lines.emplace_back(stripped);
  }
}

void parse_field(std::string_view raw, Headers& out, int status) {
  if (raw.front() == ' ' || raw.front() == '\t') throw ProtocolError("obsolete line folding", status);
  size_t sep = raw.find(':');
  if (sep == std::string_view::npos || sep == 0) throw ProtocolError("malformed header field", status);
  std::string_view name = raw.substr(0, sep);
  if (!is_token(name)) {
    // RFC 9112 5.1: no whitespace before the colon.  Tolerating it is how
    // header names get desynchronised between two hops.
    bool ws = name.find_first_of(" \t") != std::string_view::npos;
    throw ProtocolError(ws ? "whitespace in header name" : "invalid header name", status);
  }
  std::string_view value = trim(raw.substr(sep + 1));
  if (!valid_field_value(value)) throw ProtocolError("invalid header value", status);
  out.emplace_back(std::string(name), std::string(value));
}

bool all_upper_alpha(std::string_view s) noexcept {
  return !s.empty() &&
         std::all_of(s.begin(), s.end(), [](char c) { return c >= 'A' && c <= 'Z'; });
}
}  // namespace

Task<Request> read_request(BufReader& reader, double deadline) {
  size_t size = 0;
  std::vector<std::string> lines = co_await read_head_lines(reader, deadline, true, &size);
  std::string_view start = lines[0];
  size_t sp1 = start.find(' ');
  size_t sp2 = sp1 == std::string_view::npos ? sp1 : start.find(' ', sp1 + 1);
  if (sp1 == std::string_view::npos || sp2 == std::string_view::npos ||
      start.find(' ', sp2 + 1) != std::string_view::npos) {
    throw ProtocolError("malformed request line", 400);
  }
  std::string_view method = start.substr(0, sp1);
  std::string_view target = start.substr(sp1 + 1, sp2 - sp1 - 1);
  std::string_view version = start.substr(sp2 + 1);
  if (!all_upper_alpha(method) || method.size() > 32) throw ProtocolError("invalid method", 400);
  if (target.size() > kMaxTarget) throw ProtocolError("request target too long", 414);
  if (target.empty() ||
      !std::all_of(target.begin(), target.end(),
                   [](char c) { return static_cast<unsigned char>(c) > 0x20 &&
                                       static_cast<unsigned char>(c) < 0x7F; })) {
    throw ProtocolError("invalid request target", 400);
  }
  if (version != "HTTP/1.1" && version != "HTTP/1.0") {
    throw ProtocolError("unsupported HTTP version", 505);
  }

  Request req;
  req.method = std::string(method);
  req.target = std::string(target);
  req.version = std::string(version);
  req.raw_size = size;
  req.headers.reserve(lines.size() - 1);
  for (size_t i = 1; i < lines.size(); ++i) parse_field(lines[i], req.headers, 400);

  size_t hosts = req.get_all("host").size();
  if (req.version == "HTTP/1.1" && hosts == 0) throw ProtocolError("missing Host", 400);
  if (hosts > 1) throw ProtocolError("duplicate Host", 400);
  co_return req;
}

Task<Response> read_response(BufReader& reader, double deadline) {
  size_t size = 0;
  std::vector<std::string> lines = co_await read_head_lines(reader, deadline, false, &size);
  std::string_view start = lines[0];
  // HTTP/1.x SP 3DIGIT [SP reason]
  if (start.size() < 12 || start.substr(0, 7) != "HTTP/1." || start[7] < '0' || start[7] > '9' ||
      start[8] != ' ') {
    throw ProtocolError(start.substr(0, 5) == "HTTP/" ? "unsupported upstream version"
                                                     : "malformed status line",
                        502);
  }
  std::string_view code = start.substr(9, 3);
  if (!std::all_of(code.begin(), code.end(), [](char c) { return c >= '0' && c <= '9'; }) ||
      (start.size() > 12 && start[12] != ' ')) {
    throw ProtocolError("malformed status line", 502);
  }
  Response resp;
  resp.status = (code[0] - '0') * 100 + (code[1] - '0') * 10 + (code[2] - '0');
  if (resp.status < 100 || resp.status > 599) throw ProtocolError("status out of range", 502);
  resp.version = std::string(start.substr(0, 8));
  if (start.size() > 13) resp.reason = std::string(start.substr(13));
  if (!valid_field_value(resp.reason)) throw ProtocolError("malformed status line", 502);
  resp.raw_size = size;
  resp.headers.reserve(lines.size() - 1);
  for (size_t i = 1; i < lines.size(); ++i) parse_field(lines[i], resp.headers, 502);
  co_return resp;
}

// ---------------------------------------------------------------------------
// Serialisation
// ---------------------------------------------------------------------------

Headers filtered_headers(const Message& msg, const std::vector<std::string_view>& drop) {
  std::vector<std::string> named = msg.connection_tokens();
  Headers out;
  out.reserve(msg.headers.size());
  for (const auto& h : msg.headers) {
    // Case-insensitive compare in place: lower-casing a copy of every header
    // name cost an allocation per header, twice per proxied request.
    auto in = [&](const auto& list) {
      for (const auto& name : list) {
        if (iequals(h.first, name)) return true;
      }
      return false;
    };
    if (in(kHopByHop) || in(named) || in(drop)) continue;
    out.push_back(h);
  }
  return out;
}

namespace {
void append_headers(std::string& out, const Headers& headers) {
  for (const auto& [name, value] : headers) {
    out += name;
    out += ": ";
    out += value;
    out += "\r\n";
  }
  out += "\r\n";
}
}  // namespace

std::string encode_request(std::string_view method, std::string_view target,
                           const Headers& headers) {
  std::string out;
  out.reserve(256);
  out.append(method);
  out += ' ';
  out.append(target);
  out += " HTTP/1.1\r\n";
  append_headers(out, headers);
  return out;
}

std::string encode_response(int status, const Headers& headers, std::string_view reason) {
  std::string out;
  out.reserve(256);
  out += "HTTP/1.1 ";
  out += std::to_string(status);
  out += ' ';
  if (reason.empty()) {
    out += status_text(status);
  } else {
    out.append(reason);
  }
  out += "\r\n";
  append_headers(out, headers);
  return out;
}

std::string simple_response(int status, std::string_view body, std::string_view content_type,
                            bool close, const Headers& extra) {
  Headers headers = {
      {"Content-Type", std::string(content_type)},
      {"Content-Length", std::to_string(body.size())},
      {"Connection", close ? "close" : "keep-alive"},
  };
  headers.insert(headers.end(), extra.begin(), extra.end());
  std::string out = encode_response(status, headers);
  out.append(body);
  return out;
}

// ---------------------------------------------------------------------------
// Bodies
// ---------------------------------------------------------------------------

namespace {
Task<void> sink_write(Sink sink, std::string_view data, double deadline) {
  if (sink.collect != nullptr) {
    sink.collect->append(data);
    co_return;
  }
  if (sink.socket == nullptr) co_return;
  try {
    co_await sink.socket->write_all(data, deadline);
  } catch (const TimeoutError&) {
    throw SinkError("write timeout", true);
  } catch (const IoError& e) {
    throw SinkError(e.what(), false);
  }
}

Task<std::string> read_body_line(BufReader& reader, size_t max, double deadline,
                                 const char* truncated) {
  std::string line;
  try {
    line = co_await reader.read_line(max, deadline);
  } catch (const LineTooLong&) {
    throw ProtocolError("malformed chunked body", 400);
  }
  if (line.empty() || line.back() != '\n') throw ProtocolError(truncated, 400);
  co_return line;
}

uint64_t parse_chunk_size(std::string_view line) {
  line = strip_eol(line);
  uint64_t value = 0;
  size_t i = 0;
  for (; i < line.size(); ++i) {
    char c = line[i];
    int digit;
    if (c >= '0' && c <= '9') digit = c - '0';
    else if (c >= 'a' && c <= 'f') digit = c - 'a' + 10;
    else if (c >= 'A' && c <= 'F') digit = c - 'A' + 10;
    else break;
    if (i >= 16) throw ProtocolError("chunk size out of range", 400);
    value = (value << 4) | static_cast<uint64_t>(digit);
  }
  if (i == 0) throw ProtocolError("malformed chunk size", 400);
  // chunk-ext = *( BWS ";" ... ): whitespace is only legal before a ';'.
  std::string_view rest = trim(line.substr(i));
  if (!rest.empty() && (rest.front() != ';' || !valid_field_value(rest))) {
    throw ProtocolError("malformed chunk size", 400);
  }
  if (value > kMaxChunkSize) throw ProtocolError("chunk size out of range", 400);
  return value;
}

std::string hex_size(uint64_t v) {
  static const char* digits = "0123456789abcdef";
  if (v == 0) return "0";
  std::string out;
  while (v) {
    out.insert(out.begin(), digits[v & 0xF]);
    v >>= 4;
  }
  return out;
}

Task<void> read_trailers(BufReader& reader, Sink sink, double deadline, bool forward = true) {
  size_t total = 0;
  for (;;) {
    std::string line = co_await read_body_line(reader, kMaxHeadBytes, deadline, "truncated trailers");
    total += line.size();
    if (total > kMaxHeadBytes) throw ProtocolError("trailer section too large", 400);
    std::string_view stripped = strip_eol(line);
    if (stripped.empty()) {
      if (forward) co_await sink_write(sink, "\r\n", deadline);
      co_return;
    }
    Headers scratch;
    parse_field(stripped, scratch, 400);
    if (!forward) continue;
    std::string normalised = scratch[0].first + ": " + scratch[0].second + "\r\n";
    co_await sink_write(sink, normalised, deadline);
  }
}

Task<uint64_t> relay_length(BufReader& reader, Sink sink, uint64_t remaining,
                            std::optional<uint64_t> limit, double deadline) {
  if (limit && remaining > *limit) throw ProtocolError("body exceeds configured limit", 413);
  size_t cap = static_cast<size_t>(std::min<uint64_t>(remaining, kRelayChunk));
  std::unique_ptr<char[]> buf(new char[cap]);
  uint64_t moved = 0;
  while (remaining > 0) {
    size_t want = static_cast<size_t>(std::min<uint64_t>(cap, remaining));
    size_t n = co_await reader.read_into(buf.get(), want, deadline);
    if (n == 0) throw ProtocolError("truncated body", 400);
    remaining -= n;
    moved += n;
    co_await sink_write(sink, std::string_view(buf.get(), n), deadline);
  }
  co_return moved;
}

Task<uint64_t> relay_eof(BufReader& reader, Sink sink, std::optional<uint64_t> limit,
                         double deadline) {
  std::unique_ptr<char[]> buf(new char[kRelayChunk]);
  uint64_t moved = 0;
  for (;;) {
    size_t n = co_await reader.read_into(buf.get(), kRelayChunk, deadline);
    if (n == 0) co_return moved;
    moved += n;
    if (limit && moved > *limit) throw ProtocolError("body exceeds configured limit", 413);
    co_await sink_write(sink, std::string_view(buf.get(), n), deadline);
  }
}

Task<uint64_t> relay_chunked(BufReader& reader, Sink sink, std::optional<uint64_t> limit,
                             double deadline, bool dechunk) {
  std::unique_ptr<char[]> buf(new char[kRelayChunk]);
  uint64_t moved = 0;
  for (;;) {
    std::string line =
        co_await read_body_line(reader, 4096, deadline, "truncated chunked body");
    uint64_t size = parse_chunk_size(line);
    // Re-framed rather than copied: chunk extensions never reach the peer.
    if (!dechunk) co_await sink_write(sink, hex_size(size) + "\r\n", deadline);
    if (size == 0) break;
    uint64_t remaining = size;
    while (remaining > 0) {
      size_t want = static_cast<size_t>(std::min<uint64_t>(kRelayChunk, remaining));
      size_t n = co_await reader.read_into(buf.get(), want, deadline);
      if (n == 0) throw ProtocolError("truncated chunk", 400);
      remaining -= n;
      moved += n;
      if (limit && moved > *limit) throw ProtocolError("body exceeds configured limit", 413);
      co_await sink_write(sink, std::string_view(buf.get(), n), deadline);
    }
    std::string terminator;
    try {
      terminator = co_await reader.read_exact(2, deadline);
    } catch (const IncompleteRead&) {
      throw ProtocolError("truncated chunk", 400);
    }
    if (terminator != "\r\n") throw ProtocolError("missing chunk terminator", 400);
    if (!dechunk) co_await sink_write(sink, "\r\n", deadline);
  }
  co_await read_trailers(reader, sink, deadline, !dechunk);
  co_return moved;
}
}  // namespace

Task<uint64_t> relay_body(BufReader& reader, Sink sink, Framing framing,
                          std::optional<uint64_t> limit, double deadline, bool dechunk) {
  switch (framing.kind) {
    case BodyKind::None: co_return 0;
    case BodyKind::Length: co_return co_await relay_length(reader, sink, framing.length, limit, deadline);
    case BodyKind::Chunked: co_return co_await relay_chunked(reader, sink, limit, deadline, dechunk);
    case BodyKind::Eof: co_return co_await relay_eof(reader, sink, limit, deadline);
  }
  co_return 0;
}

Task<std::optional<std::string>> read_body_into_memory(BufReader& reader, Framing framing,
                                                       size_t max_bytes, double deadline) {
  if (framing.empty()) co_return std::string();
  if (framing.kind == BodyKind::Eof) co_return std::nullopt;
  if (framing.kind == BodyKind::Length) {
    if (framing.length > max_bytes) co_return std::nullopt;
    std::string body;
    try {
      body = co_await reader.read_exact(static_cast<size_t>(framing.length), deadline);
    } catch (const IncompleteRead&) {
      throw ProtocolError("truncated body", 400);
    }
    co_return body;
  }
  std::string body;
  for (;;) {
    std::string line =
        co_await read_body_line(reader, 4096, deadline, "truncated chunked body");
    uint64_t size = parse_chunk_size(line);
    if (size == 0) break;
    if (body.size() + size > max_bytes) throw ProtocolError("body exceeds configured limit", 413);
    std::string chunk;
    std::string terminator;
    try {
      chunk = co_await reader.read_exact(static_cast<size_t>(size), deadline);
      terminator = co_await reader.read_exact(2, deadline);
    } catch (const IncompleteRead&) {
      throw ProtocolError("truncated chunk", 400);
    }
    if (terminator != "\r\n") throw ProtocolError("missing chunk terminator", 400);
    body += chunk;
  }
  co_await read_trailers(reader, Sink{}, deadline, false);
  co_return body;
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

Task<FetchResult> fetch_raw(std::string host, int port, std::string raw, std::string method,
                            double timeout) {
  double deadline = monotonic() + timeout;
  std::vector<Address> addrs = co_await resolve(host, port, deadline);
  SocketPtr sock = co_await connect_any(std::move(addrs), deadline);
  co_await sock->write_all(raw, deadline);
  BufReader reader(sock);
  FetchResult out;
  out.response = co_await read_response(reader, deadline);
  while (out.response.status >= 100 && out.response.status < 200 && out.response.status != 101) {
    out.response = co_await read_response(reader, deadline);
  }
  Framing framing = response_framing(method, out.response);
  // Callers want the payload, not the wire framing.
  co_await relay_body(reader, Sink{nullptr, &out.body}, framing, 256u << 20, deadline, true);
  co_return out;
}

Task<FetchResult> fetch(std::string host, int port, std::string method, std::string target,
                        Headers headers, std::string body, double timeout) {
  Headers fields;
  bool have_host = false;
  for (const auto& h : headers) have_host = have_host || iequals(h.first, "host");
  if (!have_host) fields.emplace_back("Host", host + ":" + std::to_string(port));
  fields.emplace_back("Connection", "close");
  if (!body.empty() || method == "POST" || method == "PUT" || method == "PATCH") {
    fields.emplace_back("Content-Length", std::to_string(body.size()));
  }
  fields.insert(fields.end(), headers.begin(), headers.end());
  std::string raw = encode_request(method, target, fields) + body;
  co_return co_await fetch_raw(std::move(host), port, std::move(raw), std::move(method), timeout);
}

}  // namespace jdblance::http
