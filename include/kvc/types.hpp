#pragma once

#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>

namespace kvc {

using BlockId = uint32_t;      // index of a KV block within a cache region
using RegionId = uint32_t;     // handle for a registered memory region
using TransferId = uint64_t;   // unique per transport instance
using PeerId = std::string;    // logical node name, e.g. "prefill-0"

constexpr RegionId kInvalidRegion = std::numeric_limits<RegionId>::max();
constexpr TransferId kInvalidTransfer = 0;

enum class MemoryKind : uint8_t {
  kHost = 0,        // pageable host memory
  kHostPinned = 1,  // page-locked host memory (cudaHostAlloc etc.)
  kDevice = 2,      // accelerator memory; requires a transport that can address it
};

const char* memory_kind_name(MemoryKind kind);

enum class KvKind : uint8_t { kKey = 0, kValue = 1 };

// Half-open range of transformer layers [begin, end). `end == kAllLayers`
// means "through the last layer of the layout"; resolve() pins it down.
struct LayerRange {
  static constexpr uint32_t kAllLayers = std::numeric_limits<uint32_t>::max();

  uint32_t begin = 0;
  uint32_t end = kAllLayers;

  static LayerRange all() { return {}; }
  static LayerRange of(uint32_t b, uint32_t e) { return {b, e}; }
  static LayerRange single(uint32_t layer) { return {layer, layer + 1}; }

  bool is_all() const { return begin == 0 && end == kAllLayers; }
  LayerRange resolve(uint32_t num_layers) const {
    return {begin, end == kAllLayers ? num_layers : end};
  }
  uint32_t count() const { return end > begin ? end - begin : 0; }

  bool operator==(const LayerRange& o) const { return begin == o.begin && end == o.end; }
  bool operator!=(const LayerRange& o) const { return !(*this == o); }
};

// A contiguous byte range inside one memory region.
struct Segment {
  uint64_t offset = 0;
  uint64_t length = 0;
  bool operator==(const Segment& o) const { return offset == o.offset && length == o.length; }
};

// One contiguous copy from (src_region, src_offset) to (dst_region, dst_offset).
// For Transport::write the src is local and dst is remote; for read it is the
// reverse. Region ids are always interpreted by the side that owns the memory.
struct XferDesc {
  RegionId src_region = kInvalidRegion;
  uint64_t src_offset = 0;
  RegionId dst_region = kInvalidRegion;
  uint64_t dst_offset = 0;
  uint64_t length = 0;

  bool operator==(const XferDesc& o) const {
    return src_region == o.src_region && src_offset == o.src_offset && dst_region == o.dst_region &&
           dst_offset == o.dst_offset && length == o.length;
  }
};

struct Endpoint {
  std::string host;
  uint16_t port = 0;

  std::string to_string() const;
  // Parses "host:port". IPv6 literals must be bracketed: "[::1]:9000".
  static std::optional<Endpoint> parse(std::string_view text);
};

struct TransferOptions {
  // Zero means no deadline; the transfer completes only on ack, failure,
  // cancellation, or disconnect.
  std::chrono::milliseconds timeout{0};
};

}  // namespace kvc
