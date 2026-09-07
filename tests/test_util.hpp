#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include "kvc/engine.hpp"
#include "kvc/tcp_transport.hpp"

namespace kvc::test {

inline bool wait_until(const std::function<bool()>& pred,
                       std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return pred();
}

// Deterministic byte pattern that differs per offset and per seed.
inline uint8_t pattern(uint64_t offset, uint64_t seed) {
  uint64_t x = offset * 0x9E3779B97F4A7C15ull ^ (seed + 1) * 0xD1B54A32D192ED03ull;
  x ^= x >> 29;
  x *= 0xBF58476D1CE4E5B9ull;
  x ^= x >> 32;
  return static_cast<uint8_t>(x);
}

inline void fill_pattern(std::vector<uint8_t>& buf, uint64_t seed) {
  for (uint64_t i = 0; i < buf.size(); ++i) buf[i] = pattern(i, seed);
}

inline KvLayout small_layout(KvArrangement arrangement = KvArrangement::kLayerMajor,
                             uint32_t num_blocks = 16) {
  KvLayout l;
  l.num_layers = 4;
  l.num_kv_heads = 2;
  l.head_dim = 8;
  l.block_size = 16;
  l.num_blocks = num_blocks;
  l.dtype = DType::kF16;
  l.arrangement = arrangement;
  return l;
}

// A TransferEngine over a TCP transport bound to an ephemeral loopback port,
// with its KV cache region allocated and registered.
struct TcpNode {
  PeerId name;
  std::shared_ptr<TcpTransport> transport;
  std::unique_ptr<TransferEngine> engine;
  std::vector<uint8_t> mem;
  RegionId region = kInvalidRegion;

  TcpNode(PeerId n, const KvLayout& layout) : name(std::move(n)) {
    TcpTransportConfig cfg;
    cfg.self = name;
    cfg.bind_host = "127.0.0.1";
    transport = std::make_shared<TcpTransport>(cfg);
    engine = std::make_unique<TransferEngine>(EngineConfig{name, layout, {}}, transport);
    if (!engine->start().ok()) return;
    mem.resize(layout.total_bytes());
    auto r = engine->register_kv_cache(mem.data(), mem.size());
    if (r.ok()) region = *r;
  }
  Status connect_to(const TcpNode& other) {
    return engine->connect(other.name, other.transport->local_endpoint());
  }
  Endpoint endpoint() const { return transport->local_endpoint(); }
};

// Returns true if block `b` of `layout` in `mem` holds `pattern(offset, seed)`
// computed with the offsets of `ref_layout` block `ref_b` (so a block copied
// between different arrangements can be verified).
inline bool block_matches(const std::vector<uint8_t>& mem, const KvLayout& layout, BlockId b,
                          const std::vector<uint8_t>& ref_mem, const KvLayout& ref_layout,
                          BlockId ref_b, LayerRange range = LayerRange::all()) {
  const LayerRange r = range.resolve(layout.num_layers);
  for (uint32_t layer = r.begin; layer < r.end; ++layer) {
    for (KvKind kind : {KvKind::kKey, KvKind::kValue}) {
      const uint64_t off = layout.offset(layer, kind, b);
      const uint64_t ref_off = ref_layout.offset(layer, kind, ref_b);
      for (uint64_t i = 0; i < layout.slice_bytes(); ++i) {
        if (mem[off + i] != ref_mem[ref_off + i]) return false;
      }
    }
  }
  return true;
}

}  // namespace kvc::test
