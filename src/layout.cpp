#include "kvc/layout.hpp"

#include <algorithm>
#include <sstream>

namespace kvc {

uint32_t dtype_bytes(DType dtype) {
  switch (dtype) {
    case DType::kF16:
    case DType::kBF16: return 2;
    case DType::kF32: return 4;
    case DType::kF8E4M3:
    case DType::kF8E5M2:
    case DType::kI8: return 1;
  }
  return 0;
}

const char* dtype_name(DType dtype) {
  switch (dtype) {
    case DType::kF16: return "f16";
    case DType::kBF16: return "bf16";
    case DType::kF32: return "f32";
    case DType::kF8E4M3: return "f8e4m3";
    case DType::kF8E5M2: return "f8e5m2";
    case DType::kI8: return "i8";
  }
  return "unknown";
}

const char* arrangement_name(KvArrangement arrangement) {
  switch (arrangement) {
    case KvArrangement::kLayerMajor: return "layer_major";
    case KvArrangement::kBlockMajor: return "block_major";
  }
  return "unknown";
}

Status KvLayout::validate() const {
  if (num_layers == 0) return Status::InvalidArgument("num_layers must be > 0");
  if (num_kv_heads == 0) return Status::InvalidArgument("num_kv_heads must be > 0");
  if (head_dim == 0) return Status::InvalidArgument("head_dim must be > 0");
  if (block_size == 0) return Status::InvalidArgument("block_size must be > 0");
  if (num_blocks == 0) return Status::InvalidArgument("num_blocks must be > 0");
  if (dtype_bytes(dtype) == 0) return Status::InvalidArgument("unknown dtype");
  // Guard against overflow in total_bytes().
  const long double total = static_cast<long double>(num_layers) * 2 * num_blocks * block_size *
                            num_kv_heads * head_dim * dtype_bytes(dtype);
  if (total > static_cast<long double>(1ull << 62)) {
    return Status::InvalidArgument("layout exceeds addressable size");
  }
  return Status::Ok();
}

uint64_t KvLayout::token_kv_bytes() const {
  return static_cast<uint64_t>(num_kv_heads) * head_dim * dtype_bytes(dtype);
}

uint64_t KvLayout::slice_bytes() const { return token_kv_bytes() * block_size; }

uint64_t KvLayout::block_bytes() const { return 2ull * num_layers * slice_bytes(); }

uint64_t KvLayout::total_bytes() const { return block_bytes() * num_blocks; }

uint64_t KvLayout::offset(uint32_t layer, KvKind kind, BlockId block) const {
  const uint64_t slice = slice_bytes();
  const uint64_t kv = kind == KvKind::kValue ? 1 : 0;
  switch (arrangement) {
    case KvArrangement::kLayerMajor:
      return (static_cast<uint64_t>(layer) * 2 + kv) * num_blocks * slice + block * slice;
    case KvArrangement::kBlockMajor:
      return block * block_bytes() + (static_cast<uint64_t>(layer) * 2 + kv) * slice;
  }
  return 0;
}

void KvLayout::block_segments(BlockId block, LayerRange range, std::vector<Segment>& out) const {
  const LayerRange r = range.resolve(num_layers);
  if (r.count() == 0) return;
  switch (arrangement) {
    case KvArrangement::kBlockMajor:
      out.push_back({offset(r.begin, KvKind::kKey, block), 2ull * r.count() * slice_bytes()});
      return;
    case KvArrangement::kLayerMajor:
      for (uint32_t layer = r.begin; layer < r.end; ++layer) {
        out.push_back({offset(layer, KvKind::kKey, block), slice_bytes()});
        out.push_back({offset(layer, KvKind::kValue, block), slice_bytes()});
      }
      return;
  }
}

std::vector<Segment> KvLayout::block_segments(BlockId block, LayerRange range) const {
  std::vector<Segment> out;
  block_segments(block, range, out);
  return out;
}

bool KvLayout::transfer_compatible(const KvLayout& o) const {
  return num_layers == o.num_layers && num_kv_heads == o.num_kv_heads && head_dim == o.head_dim &&
         block_size == o.block_size && dtype == o.dtype;
}

uint64_t KvLayout::fingerprint() const {
  uint64_t h = 0xcbf29ce484222325ull;  // FNV-1a
  auto mix = [&h](uint64_t v) {
    for (int i = 0; i < 8; ++i) {
      h ^= (v >> (i * 8)) & 0xff;
      h *= 0x100000001b3ull;
    }
  };
  mix(num_layers);
  mix(num_kv_heads);
  mix(head_dim);
  mix(block_size);
  mix(num_blocks);
  mix(static_cast<uint64_t>(dtype));
  mix(static_cast<uint64_t>(arrangement));
  return h;
}

std::string KvLayout::to_string() const {
  std::ostringstream os;
  os << "KvLayout{layers=" << num_layers << ", kv_heads=" << num_kv_heads
     << ", head_dim=" << head_dim << ", block_size=" << block_size << ", blocks=" << num_blocks
     << ", dtype=" << dtype_name(dtype) << ", arrangement=" << arrangement_name(arrangement)
     << ", block_bytes=" << block_bytes() << ", total_bytes=" << total_bytes() << "}";
  return os.str();
}

Result<std::vector<XferDesc>> build_block_descriptors(const KvLayout& src, RegionId src_region,
                                                      std::span<const BlockId> src_blocks,
                                                      const KvLayout& dst, RegionId dst_region,
                                                      std::span<const BlockId> dst_blocks,
                                                      LayerRange range) {
  KVC_RETURN_IF_ERROR(src.validate());
  KVC_RETURN_IF_ERROR(dst.validate());
  if (!src.transfer_compatible(dst)) {
    return Status::InvalidArgument("source and destination layouts are not transfer compatible: " +
                                   src.to_string() + " vs " + dst.to_string());
  }
  if (src_blocks.size() != dst_blocks.size()) {
    return Status::InvalidArgument("block count mismatch: " + std::to_string(src_blocks.size()) +
                                   " src vs " + std::to_string(dst_blocks.size()) + " dst");
  }
  if (src_blocks.empty()) return Status::InvalidArgument("no blocks to transfer");
  const LayerRange r = range.resolve(src.num_layers);
  if (r.begin >= r.end || r.end > src.num_layers) {
    return Status::InvalidArgument("layer range [" + std::to_string(range.begin) + ", " +
                                   std::to_string(range.end) + ") invalid for " +
                                   std::to_string(src.num_layers) + " layers");
  }
  for (BlockId b : src_blocks) {
    if (b >= src.num_blocks) {
      return Status::OutOfRange("source block " + std::to_string(b) + " >= num_blocks " +
                                std::to_string(src.num_blocks));
    }
  }
  for (BlockId b : dst_blocks) {
    if (b >= dst.num_blocks) {
      return Status::OutOfRange("destination block " + std::to_string(b) + " >= num_blocks " +
                                std::to_string(dst.num_blocks));
    }
  }

  std::vector<XferDesc> descs;
  descs.reserve(src_blocks.size() * r.count() * 2);
  const uint64_t slice = src.slice_bytes();
  for (size_t i = 0; i < src_blocks.size(); ++i) {
    for (uint32_t layer = r.begin; layer < r.end; ++layer) {
      for (KvKind kind : {KvKind::kKey, KvKind::kValue}) {
        descs.push_back({src_region, src.offset(layer, kind, src_blocks[i]), dst_region,
                         dst.offset(layer, kind, dst_blocks[i]), slice});
      }
    }
  }
  coalesce_descriptors(descs);
  return descs;
}

void coalesce_descriptors(std::vector<XferDesc>& descs) {
  if (descs.size() < 2) return;
  std::sort(descs.begin(), descs.end(), [](const XferDesc& a, const XferDesc& b) {
    if (a.src_region != b.src_region) return a.src_region < b.src_region;
    if (a.dst_region != b.dst_region) return a.dst_region < b.dst_region;
    if (a.src_offset != b.src_offset) return a.src_offset < b.src_offset;
    return a.dst_offset < b.dst_offset;
  });
  std::vector<XferDesc> merged;
  merged.reserve(descs.size());
  merged.push_back(descs[0]);
  for (size_t i = 1; i < descs.size(); ++i) {
    XferDesc& last = merged.back();
    const XferDesc& cur = descs[i];
    if (last.src_region == cur.src_region && last.dst_region == cur.dst_region &&
        last.src_offset + last.length == cur.src_offset &&
        last.dst_offset + last.length == cur.dst_offset) {
      last.length += cur.length;
    } else {
      merged.push_back(cur);
    }
  }
  descs.swap(merged);
}

uint64_t total_bytes(std::span<const XferDesc> descs) {
  uint64_t total = 0;
  for (const auto& d : descs) total += d.length;
  return total;
}

}  // namespace kvc
