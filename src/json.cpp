#include "json.hpp"

#include <charconv>
#include <cmath>
#include <cstring>

namespace jdblance {

const char* Json::type_name(Type t) noexcept {
  switch (t) {
    case Type::Null: return "null";
    case Type::Bool: return "boolean";
    case Type::Number: return "number";
    case Type::String: return "string";
    case Type::Array: return "array";
    case Type::Object: return "object";
  }
  return "?";
}

namespace {
[[noreturn]] void type_mismatch(Json::Type want, Json::Type got) {
  throw JsonError(std::string("expected ") + Json::type_name(want) + ", got " +
                  Json::type_name(got));
}
}  // namespace

bool Json::as_bool() const {
  if (!is_bool()) type_mismatch(Type::Bool, type());
  return std::get<bool>(v_);
}
double Json::as_number() const {
  if (!is_number()) type_mismatch(Type::Number, type());
  return std::get<double>(v_);
}
const std::string& Json::as_string() const {
  if (!is_string()) type_mismatch(Type::String, type());
  return std::get<std::string>(v_);
}
const Json::Array& Json::as_array() const {
  if (!is_array()) type_mismatch(Type::Array, type());
  return std::get<Array>(v_);
}
Json::Array& Json::as_array() {
  if (!is_array()) type_mismatch(Type::Array, type());
  return std::get<Array>(v_);
}
const Json::Object& Json::as_object() const {
  if (!is_object()) type_mismatch(Type::Object, type());
  return std::get<Object>(v_);
}
Json::Object& Json::as_object() {
  if (!is_object()) type_mismatch(Type::Object, type());
  return std::get<Object>(v_);
}

Json& Json::operator[](std::string_view key) {
  if (is_null()) v_ = Object{};
  Object& obj = as_object();
  for (auto& [k, v] : obj) {
    if (k == key) return v;
  }
  obj.emplace_back(std::string(key), Json());
  return obj.back().second;
}

const Json* Json::find(std::string_view key) const {
  if (!is_object()) return nullptr;
  for (const auto& [k, v] : std::get<Object>(v_)) {
    if (k == key) return &v;
  }
  return nullptr;
}

void Json::push_back(Json v) {
  if (is_null()) v_ = Array{};
  as_array().push_back(std::move(v));
}

// ---------------------------------------------------------------------------
// Serialisation
// ---------------------------------------------------------------------------

std::string format_number(double v) {
  if (!std::isfinite(v)) return "null";
  if (v == std::floor(v) && std::fabs(v) < 1e15) {
    char buf[32];
    auto res = std::to_chars(buf, buf + sizeof buf, static_cast<long long>(v));
    return std::string(buf, res.ptr);
  }
  char buf[64];
  auto res = std::to_chars(buf, buf + sizeof buf, v);
  return std::string(buf, res.ptr);
}

double round_to(double v, int digits) {
  if (!std::isfinite(v)) return v;
  double scale = std::pow(10.0, digits);
  return std::round(v * scale) / scale;
}

namespace {
// Length of a valid UTF-8 sequence starting at s[i], or 0 if invalid.
size_t utf8_sequence(std::string_view s, size_t i) {
  auto byte = [&](size_t k) { return static_cast<unsigned char>(s[k]); };
  unsigned char c = byte(i);
  size_t need;
  uint32_t cp;
  if (c < 0x80) return 1;
  if (c >= 0xC2 && c <= 0xDF) {
    need = 1;
    cp = c & 0x1F;
  } else if (c >= 0xE0 && c <= 0xEF) {
    need = 2;
    cp = c & 0x0F;
  } else if (c >= 0xF0 && c <= 0xF4) {
    need = 3;
    cp = c & 0x07;
  } else {
    return 0;
  }
  if (i + need >= s.size()) return 0;  // truncated sequence
  for (size_t k = 1; k <= need; ++k) {
    unsigned char cc = byte(i + k);
    if ((cc & 0xC0) != 0x80) return 0;
    cp = (cp << 6) | (cc & 0x3F);
  }
  if ((need == 2 && cp < 0x800) || (need == 3 && cp < 0x10000) || cp > 0x10FFFF ||
      (cp >= 0xD800 && cp <= 0xDFFF)) {
    return 0;
  }
  return need + 1;
}

void append_escaped(std::string& out, std::string_view s) {
  static const char* hex = "0123456789abcdef";
  out.push_back('"');
  for (size_t i = 0; i < s.size();) {
    unsigned char c = static_cast<unsigned char>(s[i]);
    switch (c) {
      case '"': out += "\\\""; ++i; continue;
      case '\\': out += "\\\\"; ++i; continue;
      case '\n': out += "\\n"; ++i; continue;
      case '\r': out += "\\r"; ++i; continue;
      case '\t': out += "\\t"; ++i; continue;
      case '\b': out += "\\b"; ++i; continue;
      case '\f': out += "\\f"; ++i; continue;
      default: break;
    }
    if (c < 0x20 || c == 0x7F) {
      out += "\\u00";
      out.push_back(hex[c >> 4]);
      out.push_back(hex[c & 0xF]);
      ++i;
      continue;
    }
    if (c < 0x80) {
      out.push_back(static_cast<char>(c));
      ++i;
      continue;
    }
    size_t n = utf8_sequence(s, i);
    if (n == 0) {
      // Not UTF-8 (e.g. a latin-1 header byte): emit it as the code point it
      // would be in latin-1, so the output is always valid JSON.
      out += "\\u00";
      out.push_back(hex[c >> 4]);
      out.push_back(hex[c & 0xF]);
      ++i;
      continue;
    }
    out.append(s.data() + i, n);
    i += n;
  }
  out.push_back('"');
}

void newline(std::string& out, int indent, int depth) {
  out.push_back('\n');
  out.append(static_cast<size_t>(indent * depth), ' ');
}
}  // namespace

std::string json_quote(std::string_view s) {
  std::string out;
  out.reserve(s.size() + 2);
  append_escaped(out, s);
  return out;
}

void Json::dump_to(std::string& out, int indent, int depth) const {
  switch (type()) {
    case Type::Null: out += "null"; return;
    case Type::Bool: out += std::get<bool>(v_) ? "true" : "false"; return;
    case Type::Number: out += format_number(std::get<double>(v_)); return;
    case Type::String: append_escaped(out, std::get<std::string>(v_)); return;
    case Type::Array: {
      const Array& arr = std::get<Array>(v_);
      out.push_back('[');
      for (size_t i = 0; i < arr.size(); ++i) {
        if (i) out.push_back(',');
        if (indent >= 0) newline(out, indent, depth + 1);
        arr[i].dump_to(out, indent, depth + 1);
      }
      if (indent >= 0 && !arr.empty()) newline(out, indent, depth);
      out.push_back(']');
      return;
    }
    case Type::Object: {
      const Object& obj = std::get<Object>(v_);
      out.push_back('{');
      for (size_t i = 0; i < obj.size(); ++i) {
        if (i) out.push_back(',');
        if (indent >= 0) newline(out, indent, depth + 1);
        append_escaped(out, obj[i].first);
        out += indent >= 0 ? ": " : ":";
        obj[i].second.dump_to(out, indent, depth + 1);
      }
      if (indent >= 0 && !obj.empty()) newline(out, indent, depth);
      out.push_back('}');
      return;
    }
  }
}

std::string Json::dump(int indent) const {
  std::string out;
  dump_to(out, indent, 0);
  return out;
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

namespace {
constexpr int kMaxDepth = 256;

class Parser {
 public:
  explicit Parser(std::string_view text) : s_(text) {}

  Json parse_document() {
    skip_ws();
    Json v = parse_value(0);
    skip_ws();
    if (i_ != s_.size()) fail("unexpected trailing data");
    return v;
  }

 private:
  [[noreturn]] void fail(const std::string& what) const {
    size_t line = 1, col = 1;
    for (size_t k = 0; k < i_ && k < s_.size(); ++k) {
      if (s_[k] == '\n') {
        ++line;
        col = 1;
      } else {
        ++col;
      }
    }
    throw JsonError(what + " at line " + std::to_string(line) + " column " + std::to_string(col));
  }

  void skip_ws() {
    while (i_ < s_.size() &&
           (s_[i_] == ' ' || s_[i_] == '\t' || s_[i_] == '\n' || s_[i_] == '\r')) {
      ++i_;
    }
  }

  bool consume(std::string_view lit) {
    if (s_.substr(i_, lit.size()) == lit) {
      i_ += lit.size();
      return true;
    }
    return false;
  }

  Json parse_value(int depth) {
    if (depth > kMaxDepth) fail("nesting too deep");
    if (i_ >= s_.size()) fail("unexpected end of input");
    char c = s_[i_];
    if (c == '{') return parse_object(depth);
    if (c == '[') return parse_array(depth);
    if (c == '"') return Json(parse_string());
    if (c == '-' || (c >= '0' && c <= '9')) return Json(parse_number());
    if (consume("true")) return Json(true);
    if (consume("false")) return Json(false);
    if (consume("null")) return Json(nullptr);
    fail(std::string("unexpected character '") + c + "'");
  }

  Json parse_object(int depth) {
    ++i_;  // {
    Json::Object obj;
    skip_ws();
    if (i_ < s_.size() && s_[i_] == '}') {
      ++i_;
      return Json(std::move(obj));
    }
    for (;;) {
      skip_ws();
      if (i_ >= s_.size() || s_[i_] != '"') fail("expected a string key");
      size_t key_at = i_;
      std::string key = parse_string();
      for (const auto& [k, _] : obj) {
        if (k == key) {
          i_ = key_at;
          fail("duplicate key \"" + key + "\"");
        }
      }
      skip_ws();
      if (i_ >= s_.size() || s_[i_] != ':') fail("expected ':'");
      ++i_;
      skip_ws();
      obj.emplace_back(std::move(key), parse_value(depth + 1));
      skip_ws();
      if (i_ < s_.size() && s_[i_] == ',') {
        ++i_;
        continue;
      }
      if (i_ < s_.size() && s_[i_] == '}') {
        ++i_;
        return Json(std::move(obj));
      }
      fail("expected ',' or '}'");
    }
  }

  Json parse_array(int depth) {
    ++i_;  // [
    Json::Array arr;
    skip_ws();
    if (i_ < s_.size() && s_[i_] == ']') {
      ++i_;
      return Json(std::move(arr));
    }
    for (;;) {
      skip_ws();
      arr.push_back(parse_value(depth + 1));
      skip_ws();
      if (i_ < s_.size() && s_[i_] == ',') {
        ++i_;
        continue;
      }
      if (i_ < s_.size() && s_[i_] == ']') {
        ++i_;
        return Json(std::move(arr));
      }
      fail("expected ',' or ']'");
    }
  }

  double parse_number() {
    size_t start = i_;
    if (s_[i_] == '-') ++i_;
    if (i_ >= s_.size()) fail("malformed number");
    if (s_[i_] == '0') {
      ++i_;
    } else if (s_[i_] >= '1' && s_[i_] <= '9') {
      while (i_ < s_.size() && s_[i_] >= '0' && s_[i_] <= '9') ++i_;
    } else {
      fail("malformed number");
    }
    if (i_ < s_.size() && s_[i_] == '.') {
      ++i_;
      if (i_ >= s_.size() || s_[i_] < '0' || s_[i_] > '9') fail("malformed number");
      while (i_ < s_.size() && s_[i_] >= '0' && s_[i_] <= '9') ++i_;
    }
    if (i_ < s_.size() && (s_[i_] == 'e' || s_[i_] == 'E')) {
      ++i_;
      if (i_ < s_.size() && (s_[i_] == '+' || s_[i_] == '-')) ++i_;
      if (i_ >= s_.size() || s_[i_] < '0' || s_[i_] > '9') fail("malformed number");
      while (i_ < s_.size() && s_[i_] >= '0' && s_[i_] <= '9') ++i_;
    }
    double value = 0;
    auto res = std::from_chars(s_.data() + start, s_.data() + i_, value);
    if (res.ec == std::errc::result_out_of_range) {
      i_ = start;
      fail("number out of range");
    }
    if (res.ec != std::errc() || res.ptr != s_.data() + i_) {
      i_ = start;
      fail("malformed number");
    }
    return value;
  }

  unsigned parse_hex4() {
    if (i_ + 4 > s_.size()) fail("truncated \\u escape");
    unsigned v = 0;
    for (int k = 0; k < 4; ++k) {
      char c = s_[i_++];
      v <<= 4;
      if (c >= '0' && c <= '9') v |= static_cast<unsigned>(c - '0');
      else if (c >= 'a' && c <= 'f') v |= static_cast<unsigned>(c - 'a' + 10);
      else if (c >= 'A' && c <= 'F') v |= static_cast<unsigned>(c - 'A' + 10);
      else fail("bad \\u escape");
    }
    return v;
  }

  static void put_utf8(std::string& out, uint32_t cp) {
    if (cp < 0x80) {
      out.push_back(static_cast<char>(cp));
    } else if (cp < 0x800) {
      out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
      out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else if (cp < 0x10000) {
      out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
      out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
      out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else {
      out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
      out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
      out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
      out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    }
  }

  std::string parse_string() {
    ++i_;  // opening quote
    std::string out;
    for (;;) {
      if (i_ >= s_.size()) fail("unterminated string");
      unsigned char c = static_cast<unsigned char>(s_[i_]);
      if (c == '"') {
        ++i_;
        return out;
      }
      if (c < 0x20) fail("control character in string");
      if (c != '\\') {
        out.push_back(static_cast<char>(c));
        ++i_;
        continue;
      }
      ++i_;
      if (i_ >= s_.size()) fail("unterminated escape");
      char e = s_[i_++];
      switch (e) {
        case '"': out.push_back('"'); break;
        case '\\': out.push_back('\\'); break;
        case '/': out.push_back('/'); break;
        case 'b': out.push_back('\b'); break;
        case 'f': out.push_back('\f'); break;
        case 'n': out.push_back('\n'); break;
        case 'r': out.push_back('\r'); break;
        case 't': out.push_back('\t'); break;
        case 'u': {
          uint32_t cp = parse_hex4();
          if (cp >= 0xD800 && cp <= 0xDBFF) {
            if (!consume("\\u")) fail("unpaired surrogate");
            uint32_t low = parse_hex4();
            if (low < 0xDC00 || low > 0xDFFF) fail("unpaired surrogate");
            cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
          } else if (cp >= 0xDC00 && cp <= 0xDFFF) {
            fail("unpaired surrogate");
          }
          put_utf8(out, cp);
          break;
        }
        default:
          --i_;
          fail("bad escape");
      }
    }
  }

  std::string_view s_;
  size_t i_ = 0;
};
}  // namespace

Json Json::parse(std::string_view text) { return Parser(text).parse_document(); }

}  // namespace jdblance
