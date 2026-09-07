// Loopback throughput/latency benchmark for kvc-xfer.
//
//   kvc_bench [--transport tcp|local] [--layers N] [--heads N] [--head-dim N]
//             [--block-size N] [--num-blocks N] [--blocks N] [--iters N]
//             [--inflight N] [--arrangement layer|block] [--mode push|pull]
//             [--verbose]

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <numeric>
#include <string>
#include <vector>

#include "kvc/kvc.hpp"

using namespace kvc;

namespace {

struct Options {
  std::string transport = "tcp";
  uint32_t layers = 32;
  uint32_t heads = 8;
  uint32_t head_dim = 128;
  uint32_t block_size = 16;
  uint32_t num_blocks = 512;
  uint32_t blocks = 64;
  int iters = 50;
  int inflight = 4;
  KvArrangement arrangement = KvArrangement::kLayerMajor;
  bool pull = false;
  bool verbose = false;
};

void usage() {
  std::fprintf(stderr,
               "kvc_bench [--transport tcp|local] [--layers N] [--heads N] [--head-dim N]\n"
               "          [--block-size N] [--num-blocks N] [--blocks N] [--iters N]\n"
               "          [--inflight N] [--arrangement layer|block] [--mode push|pull]\n"
               "          [--verbose]\n");
}

bool parse(int argc, char** argv, Options& o) {
  auto need = [&](int& i) -> const char* {
    if (i + 1 >= argc) {
      usage();
      std::exit(2);
    }
    return argv[++i];
  };
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    if (a == "--transport") o.transport = need(i);
    else if (a == "--layers") o.layers = static_cast<uint32_t>(std::atoi(need(i)));
    else if (a == "--heads") o.heads = static_cast<uint32_t>(std::atoi(need(i)));
    else if (a == "--head-dim") o.head_dim = static_cast<uint32_t>(std::atoi(need(i)));
    else if (a == "--block-size") o.block_size = static_cast<uint32_t>(std::atoi(need(i)));
    else if (a == "--num-blocks") o.num_blocks = static_cast<uint32_t>(std::atoi(need(i)));
    else if (a == "--blocks") o.blocks = static_cast<uint32_t>(std::atoi(need(i)));
    else if (a == "--iters") o.iters = std::atoi(need(i));
    else if (a == "--inflight") o.inflight = std::max(1, std::atoi(need(i)));
    else if (a == "--arrangement") {
      const std::string v = need(i);
      o.arrangement = v == "block" ? KvArrangement::kBlockMajor : KvArrangement::kLayerMajor;
    } else if (a == "--mode") o.pull = std::string(need(i)) == "pull";
    else if (a == "--verbose") o.verbose = true;
    else {
      usage();
      return false;
    }
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  Options o;
  if (!parse(argc, argv, o)) return 2;
  set_log_level(o.verbose ? LogLevel::kInfo : LogLevel::kWarn);

  KvLayout layout;
  layout.num_layers = o.layers;
  layout.num_kv_heads = o.heads;
  layout.head_dim = o.head_dim;
  layout.block_size = o.block_size;
  layout.num_blocks = o.num_blocks;
  layout.dtype = DType::kF16;
  layout.arrangement = o.arrangement;
  if (auto s = layout.validate(); !s.ok()) {
    std::fprintf(stderr, "bad layout: %s\n", s.to_string().c_str());
    return 1;
  }
  if (o.blocks > o.num_blocks / 2) {
    std::fprintf(stderr, "--blocks must be <= num_blocks/2\n");
    return 1;
  }

  std::shared_ptr<Transport> ta, tb;
  auto fabric = std::make_shared<LocalFabric>();
  if (o.transport == "local") {
    ta = std::make_shared<LocalTransport>(LocalTransportConfig{"prefill", false}, fabric);
    tb = std::make_shared<LocalTransport>(LocalTransportConfig{"decode", false}, fabric);
  } else {
    TcpTransportConfig ca;
    ca.self = "prefill";
    ca.bind_host = "127.0.0.1";
    TcpTransportConfig cb = ca;
    cb.self = "decode";
    ta = std::make_shared<TcpTransport>(ca);
    tb = std::make_shared<TcpTransport>(cb);
  }
  TransferEngine prefill(EngineConfig{"prefill", layout, {}}, ta);
  TransferEngine decode(EngineConfig{"decode", layout, {}}, tb);
  if (!prefill.start().ok() || !decode.start().ok()) {
    std::fprintf(stderr, "failed to start engines\n");
    return 1;
  }
  std::vector<uint8_t> pmem(layout.total_bytes(), 0xAB), dmem(layout.total_bytes(), 0);
  const RegionId pr = *prefill.register_kv_cache(pmem.data(), pmem.size());
  const RegionId dr = *decode.register_kv_cache(dmem.data(), dmem.size());
  Endpoint ep;
  if (o.transport == "tcp") ep = std::static_pointer_cast<TcpTransport>(tb)->local_endpoint();
  if (auto s = prefill.connect("decode", ep); !s.ok()) {
    std::fprintf(stderr, "connect failed: %s\n", s.to_string().c_str());
    return 1;
  }

  std::vector<BlockId> src(o.blocks), dst(o.blocks);
  std::iota(src.begin(), src.end(), 0u);
  std::iota(dst.begin(), dst.end(), o.num_blocks / 2);
  const uint64_t bytes_per_transfer = o.blocks * layout.block_bytes();

  std::printf("%s\n", layout.to_string().c_str());
  std::printf("transport=%s mode=%s blocks/transfer=%u (%.2f MiB) iters=%d inflight=%d\n",
              o.transport.c_str(), o.pull ? "pull" : "push", o.blocks,
              static_cast<double>(bytes_per_transfer) / (1 << 20), o.iters, o.inflight);

  auto issue = [&]() -> TransferHandle {
    if (o.pull) return decode.pull_blocks("prefill", pr, src, dr, dst);
    return prefill.push_blocks("decode", pr, src, dr, dst);
  };

  // Warm-up.
  for (int i = 0; i < 3; ++i) {
    if (auto r = issue().wait(); !r.ok()) {
      std::fprintf(stderr, "transfer failed: %s\n", r.status.to_string().c_str());
      return 1;
    }
  }
  prefill.metrics().reset();
  decode.metrics().reset();

  std::vector<double> latencies_ms;
  latencies_ms.reserve(static_cast<size_t>(o.iters));
  const auto t0 = std::chrono::steady_clock::now();
  std::vector<TransferHandle> inflight;
  int issued = 0, done = 0;
  while (done < o.iters) {
    while (issued < o.iters && static_cast<int>(inflight.size()) < o.inflight) {
      inflight.push_back(issue());
      ++issued;
    }
    auto r = inflight.front().wait();
    inflight.erase(inflight.begin());
    if (!r.ok()) {
      std::fprintf(stderr, "transfer failed: %s\n", r.status.to_string().c_str());
      return 1;
    }
    latencies_ms.push_back(std::chrono::duration<double, std::milli>(r.elapsed).count());
    ++done;
  }
  const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  const double total_bytes = static_cast<double>(bytes_per_transfer) * o.iters;

  std::sort(latencies_ms.begin(), latencies_ms.end());
  auto pct = [&](double p) {
    return latencies_ms[std::min(latencies_ms.size() - 1,
                                 static_cast<size_t>(p * static_cast<double>(latencies_ms.size())))];
  };
  std::printf("throughput: %.2f GiB/s  (%.1f transfers/s)\n", total_bytes / secs / (1 << 30),
              o.iters / secs);
  std::printf("latency ms: p50=%.3f p90=%.3f p99=%.3f max=%.3f\n", pct(0.50), pct(0.90), pct(0.99),
              latencies_ms.back());
  std::printf("metrics: %s\n", (o.pull ? decode : prefill).metrics().snapshot().to_string().c_str());
  if (std::memcmp(pmem.data() + layout.offset(0, KvKind::kKey, src[0]),
                  dmem.data() + layout.offset(0, KvKind::kKey, dst[0]), layout.slice_bytes()) != 0) {
    std::fprintf(stderr, "data mismatch!\n");
    return 1;
  }
  return 0;
}
