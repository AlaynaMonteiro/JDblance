// A deliberately tiny test harness: TEST registers a function, CHECK* throw on
// failure, the runner reports and sets the exit code.  No dependencies.
#pragma once

#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace testing {

struct Case {
  std::string suite;
  std::string name;
  std::function<void()> fn;
};

inline std::vector<Case>& cases() {
  static std::vector<Case> all;
  return all;
}

struct Register {
  Register(const char* suite, const char* name, std::function<void()> fn) {
    cases().push_back({suite, name, std::move(fn)});
  }
};

struct Failure : std::runtime_error {
  using std::runtime_error::runtime_error;
};

[[noreturn]] inline void fail(const char* file, int line, const std::string& what) {
  std::ostringstream out;
  out << file << ":" << line << ": " << what;
  throw Failure(out.str());
}

template <typename T, typename = void>
struct printable : std::false_type {};
template <typename T>
struct printable<T, std::void_t<decltype(std::declval<std::ostream&>() << std::declval<const T&>())>>
    : std::true_type {};

template <typename T>
std::string show(const T& v) {
  if constexpr (printable<T>::value) {
    std::ostringstream out;
    out << v;
    return out.str();
  } else {
    return "<unprintable>";
  }
}

}  // namespace testing

#define TEST_CAT2(a, b) a##b
#define TEST_CAT(a, b) TEST_CAT2(a, b)
#define TEST(suite, name)                                                                   \
  static void TEST_CAT(test_, TEST_CAT(suite, TEST_CAT(_, name)))();                        \
  static ::testing::Register TEST_CAT(reg_, TEST_CAT(suite, TEST_CAT(_, name)))(           \
      #suite, #name, TEST_CAT(test_, TEST_CAT(suite, TEST_CAT(_, name))));                  \
  static void TEST_CAT(test_, TEST_CAT(suite, TEST_CAT(_, name)))()

#define CHECK(cond)                                                       \
  do {                                                                    \
    if (!(cond)) ::testing::fail(__FILE__, __LINE__, "CHECK(" #cond ")"); \
  } while (0)

#define CHECK_EQ(a, b)                                                                     \
  do {                                                                                     \
    const auto check_a_ = (a); /* by value: (a) may refer into a temporary */            \
    const auto check_b_ = (b);                                                             \
    if (!(check_a_ == check_b_)) {                                                         \
      ::testing::fail(__FILE__, __LINE__,                                                  \
                      "CHECK_EQ(" #a ", " #b "): " + ::testing::show(check_a_) + " != " + \
                          ::testing::show(check_b_));                                      \
    }                                                                                      \
  } while (0)

#define CHECK_THROWS(expr, type)                                                       \
  do {                                                                                 \
    bool check_threw_ = false;                                                         \
    try {                                                                              \
      (void)(expr);                                                                    \
    } catch (const type&) {                                                            \
      check_threw_ = true;                                                             \
    }                                                                                  \
    if (!check_threw_) ::testing::fail(__FILE__, __LINE__, #expr " did not throw " #type); \
  } while (0)
