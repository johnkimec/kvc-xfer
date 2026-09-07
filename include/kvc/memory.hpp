#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <vector>

#include "kvc/status.hpp"
#include "kvc/types.hpp"

namespace kvc {

// A registered, addressable slab of memory. Peers refer to it by `id`.
struct MemoryRegion {
  RegionId id = kInvalidRegion;
  void* base = nullptr;
  uint64_t length = 0;
  MemoryKind kind = MemoryKind::kHost;
  int device = -1;

  bool contains(uint64_t offset, uint64_t len) const {
    return offset <= length && len <= length - offset;
  }
  std::byte* at(uint64_t offset) const { return static_cast<std::byte*>(base) + offset; }
};

// Thread-safe table of regions. Ids are stable for the life of the registry.
class MemoryRegistry {
 public:
  Result<RegionId> add(void* base, uint64_t length, MemoryKind kind = MemoryKind::kHost,
                       int device = -1);
  // Registers with a caller-chosen id (used to mirror a peer's registration).
  Status add(const MemoryRegion& region);
  Status remove(RegionId id);

  std::optional<MemoryRegion> get(RegionId id) const;
  std::vector<MemoryRegion> list() const;
  size_t size() const;

 private:
  mutable std::mutex mu_;
  std::map<RegionId, MemoryRegion> regions_;
  RegionId next_id_ = 1;
};

}  // namespace kvc
