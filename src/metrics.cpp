#include "kvc/metrics.hpp"

#include <algorithm>
#include <sstream>

namespace kvc {

std::string MetricsSnapshot::to_string() const {
  std::ostringstream os;
  os << "transfers{started=" << transfers_started << ", ok=" << transfers_completed
     << ", failed=" << transfers_failed << ", inflight=" << transfers_inflight << "} bytes{pushed="
     << bytes_pushed << ", pulled=" << bytes_pulled << "} notifies{sent=" << notifies_sent
     << ", received=" << notifies_received << "} latency_us{p50~" << latency_p50_us << ", p99~"
     << latency_p99_us << ", max=" << latency_max_us << "}";
  return os.str();
}

void Metrics::record_start() { started_.fetch_add(1, std::memory_order_relaxed); }

void Metrics::record_done(const TransferResult& result, bool push) {
  if (!result.ok()) {
    failed_.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  completed_.fetch_add(1, std::memory_order_relaxed);
  (push ? bytes_pushed_ : bytes_pulled_).fetch_add(result.bytes, std::memory_order_relaxed);
  const auto us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(result.elapsed).count());
  hist_[bucket_for_us(us)].fetch_add(1, std::memory_order_relaxed);
  uint64_t prev = latency_max_us_.load(std::memory_order_relaxed);
  while (us > prev && !latency_max_us_.compare_exchange_weak(prev, us)) {
  }
}

void Metrics::record_notify_sent() { notifies_sent_.fetch_add(1, std::memory_order_relaxed); }
void Metrics::record_notify_received() {
  notifies_received_.fetch_add(1, std::memory_order_relaxed);
}

size_t Metrics::bucket_for_us(uint64_t us) {
  size_t b = 0;
  while (us > 1 && b + 1 < kBuckets) {
    us >>= 1;
    ++b;
  }
  return b;
}

double Metrics::bucket_upper_us(size_t bucket) { return static_cast<double>(1ull << (bucket + 1)); }

MetricsSnapshot Metrics::snapshot() const {
  MetricsSnapshot s;
  s.transfers_started = started_.load();
  s.transfers_completed = completed_.load();
  s.transfers_failed = failed_.load();
  s.transfers_inflight = s.transfers_started - std::min(s.transfers_started,
                                                        s.transfers_completed + s.transfers_failed);
  s.bytes_pushed = bytes_pushed_.load();
  s.bytes_pulled = bytes_pulled_.load();
  s.notifies_sent = notifies_sent_.load();
  s.notifies_received = notifies_received_.load();
  s.latency_max_us = static_cast<double>(latency_max_us_.load());

  uint64_t total = 0;
  std::array<uint64_t, kBuckets> counts{};
  for (size_t i = 0; i < kBuckets; ++i) {
    counts[i] = hist_[i].load();
    total += counts[i];
  }
  if (total > 0) {
    auto percentile = [&](double p) {
      const uint64_t target = static_cast<uint64_t>(p * static_cast<double>(total - 1)) + 1;
      uint64_t seen = 0;
      for (size_t i = 0; i < kBuckets; ++i) {
        seen += counts[i];
        if (seen >= target) return bucket_upper_us(i);
      }
      return bucket_upper_us(kBuckets - 1);
    };
    s.latency_p50_us = percentile(0.50);
    s.latency_p99_us = percentile(0.99);
  }
  return s;
}

void Metrics::reset() {
  started_ = 0;
  completed_ = 0;
  failed_ = 0;
  bytes_pushed_ = 0;
  bytes_pulled_ = 0;
  notifies_sent_ = 0;
  notifies_received_ = 0;
  latency_max_us_ = 0;
  for (auto& b : hist_) b = 0;
}

}  // namespace kvc
