#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <string>

#include "kvc/transfer.hpp"

namespace kvc {

struct MetricsSnapshot {
  uint64_t transfers_started = 0;
  uint64_t transfers_completed = 0;  // ok
  uint64_t transfers_failed = 0;
  uint64_t transfers_inflight = 0;
  uint64_t bytes_pushed = 0;
  uint64_t bytes_pulled = 0;
  uint64_t notifies_sent = 0;
  uint64_t notifies_received = 0;
  double latency_p50_us = 0;  // completed transfers only, log2-bucket estimate
  double latency_p99_us = 0;
  double latency_max_us = 0;

  std::string to_string() const;
};

// Lock-free counters plus a log2 latency histogram. Safe to update from any
// thread.
class Metrics {
 public:
  void record_start();
  void record_done(const TransferResult& result, bool push);
  void record_notify_sent();
  void record_notify_received();

  MetricsSnapshot snapshot() const;
  void reset();

 private:
  static constexpr size_t kBuckets = 40;  // up to ~2^39 us
  static size_t bucket_for_us(uint64_t us);
  static double bucket_upper_us(size_t bucket);

  std::atomic<uint64_t> started_{0};
  std::atomic<uint64_t> completed_{0};
  std::atomic<uint64_t> failed_{0};
  std::atomic<uint64_t> bytes_pushed_{0};
  std::atomic<uint64_t> bytes_pulled_{0};
  std::atomic<uint64_t> notifies_sent_{0};
  std::atomic<uint64_t> notifies_received_{0};
  std::atomic<uint64_t> latency_max_us_{0};
  std::array<std::atomic<uint64_t>, kBuckets> hist_{};
};

}  // namespace kvc
