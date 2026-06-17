// Runs every registered test, or those whose "suite.name" contains argv[1].
#include <cstdio>
#include <exception>
#include <string>

#include "testing.hpp"

int main(int argc, char** argv) {
  std::string filter = argc > 1 ? argv[1] : "";
  int passed = 0, failed = 0;
  for (const auto& c : testing::cases()) {
    std::string id = c.suite + "." + c.name;
    if (!filter.empty() && id.find(filter) == std::string::npos) continue;
    try {
      c.fn();
      ++passed;
      std::printf("  ok    %s\n", id.c_str());
    } catch (const std::exception& e) {
      ++failed;
      std::printf("  FAIL  %s\n        %s\n", id.c_str(), e.what());
    }
    std::fflush(stdout);
  }
  std::printf("\n%d passed, %d failed\n", passed, failed);
  return failed == 0 && passed > 0 ? 0 : 1;
}
