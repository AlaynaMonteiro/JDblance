// Minimal JSON value: strict RFC 8259 parser and serializer.
//
// Enough for the config file, the control plane and the tools.  Objects keep
// insertion order (so /stats reads the way it is built) and the parser rejects
// duplicate keys -- in a config file a repeated key is always a mistake, and
// silently letting the last one win hides it.
#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace jdblance {

class JsonError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

class Json {
 public:
  using Array = std::vector<Json>;
  using Object = std::vector<std::pair<std::string, Json>>;
  enum class Type { Null, Bool, Number, String, Array, Object };

  Json() = default;
  Json(std::nullptr_t) {}
  Json(bool v) : v_(v) {}
  Json(double v) : v_(v) {}
  Json(int v) : v_(static_cast<double>(v)) {}
  Json(long v) : v_(static_cast<double>(v)) {}
  Json(long long v) : v_(static_cast<double>(v)) {}
  Json(unsigned v) : v_(static_cast<double>(v)) {}
  Json(unsigned long v) : v_(static_cast<double>(v)) {}
  Json(unsigned long long v) : v_(static_cast<double>(v)) {}
  Json(const char* v) : v_(std::string(v)) {}
  Json(std::string v) : v_(std::move(v)) {}
  Json(std::string_view v) : v_(std::string(v)) {}
  Json(Array v) : v_(std::move(v)) {}
  Json(Object v) : v_(std::move(v)) {}

  static Json object() { return Json(Object{}); }
  static Json array() { return Json(Array{}); }

  Type type() const noexcept { return static_cast<Type>(v_.index()); }
  bool is_null() const noexcept { return type() == Type::Null; }
  bool is_bool() const noexcept { return type() == Type::Bool; }
  bool is_number() const noexcept { return type() == Type::Number; }
  bool is_string() const noexcept { return type() == Type::String; }
  bool is_array() const noexcept { return type() == Type::Array; }
  bool is_object() const noexcept { return type() == Type::Object; }

  bool as_bool() const;
  double as_number() const;
  const std::string& as_string() const;
  const Array& as_array() const;
  Array& as_array();
  const Object& as_object() const;
  Object& as_object();

  // Object access.  operator[] inserts a null member if missing.
  Json& operator[](std::string_view key);
  const Json* find(std::string_view key) const;
  // Array append.
  void push_back(Json v);

  // indent < 0: compact.  Non-finite numbers serialise as null.
  std::string dump(int indent = -1) const;
  static Json parse(std::string_view text);

  static const char* type_name(Type t) noexcept;

 private:
  void dump_to(std::string& out, int indent, int depth) const;
  std::variant<std::nullptr_t, bool, double, std::string, Array, Object> v_;
};

// JSON string literal for `s`, quotes included.
std::string json_quote(std::string_view s);
// Shortest round-trip text for a double; integral values print without ".0".
std::string format_number(double v);
// Round to `digits` decimal places, for display.
double round_to(double v, int digits);

}  // namespace jdblance
