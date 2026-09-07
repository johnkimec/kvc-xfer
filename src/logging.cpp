#include "kvc/logging.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <thread>

namespace kvc {

namespace {
std::atomic<int> g_level{static_cast<int>(LogLevel::kWarn)};
std::mutex g_sink_mu;
LogSink g_sink;

void default_sink(LogLevel level, std::string_view message) {
  const auto now = std::chrono::system_clock::now();
  const auto ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
  static std::mutex io_mu;
  std::lock_guard<std::mutex> lock(io_mu);
  std::fprintf(stderr, "[kvc %lld.%03lld %s] %.*s\n", static_cast<long long>(ms / 1000),
               static_cast<long long>(ms % 1000), log_level_name(level),
               static_cast<int>(message.size()), message.data());
}
}  // namespace

void set_log_level(LogLevel level) { g_level.store(static_cast<int>(level)); }
LogLevel log_level() { return static_cast<LogLevel>(g_level.load()); }

void set_log_sink(LogSink sink) {
  std::lock_guard<std::mutex> lock(g_sink_mu);
  g_sink = std::move(sink);
}

const char* log_level_name(LogLevel level) {
  switch (level) {
    case LogLevel::kTrace: return "TRACE";
    case LogLevel::kDebug: return "DEBUG";
    case LogLevel::kInfo: return "INFO";
    case LogLevel::kWarn: return "WARN";
    case LogLevel::kError: return "ERROR";
    case LogLevel::kOff: return "OFF";
  }
  return "?";
}

namespace detail {

bool log_enabled(LogLevel level) { return static_cast<int>(level) >= g_level.load(); }

LogLine::~LogLine() {
  LogSink sink;
  {
    std::lock_guard<std::mutex> lock(g_sink_mu);
    sink = g_sink;
  }
  const std::string msg = stream_.str();
  if (sink) {
    sink(level_, msg);
  } else {
    default_sink(level_, msg);
  }
}

}  // namespace detail
}  // namespace kvc
