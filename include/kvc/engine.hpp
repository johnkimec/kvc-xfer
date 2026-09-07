#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <unordered_map>

#include "kvc/layout.hpp"
#include "kvc/memory.hpp"
#include "kvc/metrics.hpp"
#include "kvc/transport.hpp"

namespace kvc {

struct EngineConfig {
  PeerId self;
  KvLayout layout;
  // Applied to transfers whose options carry no timeout.
  std::chrono::milliseconds default_timeout{0};
};

// Block-level KV transfer API on top of a Transport. Knows the local cache
// layout (and optionally each peer's), turns block lists into coalesced copy
// descriptors, and records metrics.
class TransferEngine {
 public:
  TransferEngine(EngineConfig config, std::shared_ptr<Transport> transport);
  ~TransferEngine();

  TransferEngine(const TransferEngine&) = delete;
  TransferEngine& operator=(const TransferEngine&) = delete;

  Status start();
  void stop();

  const PeerId& self() const { return config_.self; }
  const KvLayout& layout() const { return config_.layout; }
  Transport& transport() { return *transport_; }
  Metrics& metrics() { return metrics_; }
  const Metrics& metrics() const { return metrics_; }

  // Registers the KV cache region; `length` must cover layout().total_bytes().
  Result<RegionId> register_kv_cache(void* base, uint64_t length,
                                     MemoryKind kind = MemoryKind::kHost, int device = -1);
  // Registers an arbitrary region (for push_raw/pull_raw).
  Result<RegionId> register_region(void* base, uint64_t length,
                                   MemoryKind kind = MemoryKind::kHost, int device = -1);
  Status unregister_region(RegionId id);
  std::optional<MemoryRegion> region(RegionId id) const;

  Status connect(const PeerId& peer, const Endpoint& endpoint);
  Status disconnect(const PeerId& peer);
  bool is_connected(const PeerId& peer) const;

  // Peers default to the local layout; override when a peer's num_blocks or
  // arrangement differs.
  void set_peer_layout(const PeerId& peer, KvLayout layout);
  KvLayout peer_layout(const PeerId& peer) const;

  // Copies local src_blocks[i] -> dst_peer's dst_blocks[i] for `range`.
  TransferHandle push_blocks(const PeerId& dst_peer, RegionId src_region,
                             std::span<const BlockId> src_blocks, RegionId dst_region,
                             std::span<const BlockId> dst_blocks,
                             LayerRange range = LayerRange::all(), TransferOptions opts = {});

  // Copies src_peer's src_blocks[i] -> local dst_blocks[i] for `range`.
  TransferHandle pull_blocks(const PeerId& src_peer, RegionId src_region,
                             std::span<const BlockId> src_blocks, RegionId dst_region,
                             std::span<const BlockId> dst_blocks,
                             LayerRange range = LayerRange::all(), TransferOptions opts = {});

  TransferHandle push_raw(const PeerId& dst_peer, std::vector<XferDesc> descs,
                          TransferOptions opts = {});
  TransferHandle pull_raw(const PeerId& src_peer, std::vector<XferDesc> descs,
                          TransferOptions opts = {});

  Status notify(const PeerId& peer, std::string_view payload);
  void set_notify_handler(NotifyHandler handler);
  void set_disconnect_handler(DisconnectHandler handler);

 private:
  TransferHandle track(TransferHandle handle, bool push);
  TransferOptions effective(TransferOptions opts) const;

  EngineConfig config_;
  std::shared_ptr<Transport> transport_;
  MemoryRegistry regions_;
  Metrics metrics_;

  mutable std::mutex mu_;
  std::unordered_map<PeerId, KvLayout> peer_layouts_;
  bool started_ = false;
};

}  // namespace kvc
