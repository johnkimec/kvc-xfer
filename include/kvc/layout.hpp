#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "kvc/status.hpp"
#include "kvc/types.hpp"

namespace kvc {

enum class DType : uint8_t { kF16 = 0, kBF16 = 1, kF32 = 2, kF8E4M3 = 3, kF8E5M2 = 4, kI8 = 5 };

uint32_t dtype_bytes(DType dtype);
const char* dtype_name(DType dtype);

// How the KV cache is laid out inside a single contiguous region.
//
//   kLayerMajor: for each layer: K[num_blocks][block_size][heads][head_dim]
//                                then V[num_blocks][block_size][heads][head_dim]
//                (vLLM-style; one block spans 2*num_layers separate slices)
//   kBlockMajor: for each block: [layer][K|V][block_size][heads][head_dim]
//                (one block is a single contiguous run; ideal for transfer)
enum class KvArrangement : uint8_t { kLayerMajor = 0, kBlockMajor = 1 };

const char* arrangement_name(KvArrangement arrangement);

// Geometry of a paged KV cache. Both ends of a transfer must agree on
// everything except `num_blocks` and `arrangement`.
struct KvLayout {
  uint32_t num_layers = 0;
  uint32_t num_kv_heads = 0;
  uint32_t head_dim = 0;
  uint32_t block_size = 0;  // tokens per block
  uint32_t num_blocks = 0;
  DType dtype = DType::kF16;
  KvArrangement arrangement = KvArrangement::kLayerMajor;

  Status validate() const;

  // K (or V) bytes for one token in one layer.
  uint64_t token_kv_bytes() const;
  // K (or V) bytes for one block in one layer.
  uint64_t slice_bytes() const;
  // All K and V bytes of one block across every layer.
  uint64_t block_bytes() const;
  // Bytes of the whole cache region.
  uint64_t total_bytes() const;

  // Byte offset of one (layer, K|V, block) slice within the region.
  uint64_t offset(uint32_t layer, KvKind kind, BlockId block) const;

  // Contiguous segments that make up `block` over `range`. kBlockMajor yields
  // one segment; kLayerMajor yields two per layer.
  void block_segments(BlockId block, LayerRange range, std::vector<Segment>& out) const;
  std::vector<Segment> block_segments(BlockId block, LayerRange range = LayerRange::all()) const;

  // True if blocks can be copied between the two layouts slice-by-slice.
  bool transfer_compatible(const KvLayout& other) const;

  // Stable hash of every field that affects transfer compatibility plus
  // arrangement and num_blocks; exchanged between peers to catch mismatches.
  uint64_t fingerprint() const;

  std::string to_string() const;
};

// Builds coalesced copy descriptors moving src_blocks[i] -> dst_blocks[i] for
// every layer in `range`. Validates both block lists against their layouts.
Result<std::vector<XferDesc>> build_block_descriptors(const KvLayout& src, RegionId src_region,
                                                      std::span<const BlockId> src_blocks,
                                                      const KvLayout& dst, RegionId dst_region,
                                                      std::span<const BlockId> dst_blocks,
                                                      LayerRange range = LayerRange::all());

// Sorts descriptors and merges runs that are contiguous on both sides.
void coalesce_descriptors(std::vector<XferDesc>& descs);

uint64_t total_bytes(std::span<const XferDesc> descs);

}  // namespace kvc
