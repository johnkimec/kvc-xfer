#include "kvc/layout.hpp"

#include <gtest/gtest.h>

#include "test_util.hpp"

using namespace kvc;

namespace {

KvLayout base(KvArrangement a) { return test::small_layout(a, 10); }

TEST(Layout, Sizes) {
  const KvLayout l = base(KvArrangement::kLayerMajor);
  ASSERT_TRUE(l.validate().ok());
  EXPECT_EQ(l.token_kv_bytes(), 2u * 8 * 2);  // heads*head_dim*f16
  EXPECT_EQ(l.slice_bytes(), 32u * 16);
  EXPECT_EQ(l.block_bytes(), 512u * 2 * 4);
  EXPECT_EQ(l.total_bytes(), 4096u * 10);
  EXPECT_EQ(dtype_bytes(DType::kF32), 4u);
  EXPECT_EQ(dtype_bytes(DType::kF8E4M3), 1u);
}

TEST(Layout, ValidateRejectsZeroFields) {
  KvLayout l = base(KvArrangement::kLayerMajor);
  l.num_layers = 0;
  EXPECT_EQ(l.validate().code(), StatusCode::kInvalidArgument);
  l = base(KvArrangement::kLayerMajor);
  l.block_size = 0;
  EXPECT_FALSE(l.validate().ok());
  l = base(KvArrangement::kLayerMajor);
  l.num_blocks = 0;
  EXPECT_FALSE(l.validate().ok());
}

TEST(Layout, LayerMajorOffsets) {
  const KvLayout l = base(KvArrangement::kLayerMajor);
  const uint64_t slice = l.slice_bytes();
  EXPECT_EQ(l.offset(0, KvKind::kKey, 0), 0u);
  EXPECT_EQ(l.offset(0, KvKind::kKey, 3), 3 * slice);
  EXPECT_EQ(l.offset(0, KvKind::kValue, 0), 10 * slice);
  EXPECT_EQ(l.offset(1, KvKind::kKey, 0), 20 * slice);
  EXPECT_EQ(l.offset(3, KvKind::kValue, 9), (3 * 20 + 10 + 9) * slice);
  EXPECT_EQ(l.offset(3, KvKind::kValue, 9) + slice, l.total_bytes());
}

TEST(Layout, BlockMajorOffsets) {
  const KvLayout l = base(KvArrangement::kBlockMajor);
  const uint64_t slice = l.slice_bytes();
  EXPECT_EQ(l.offset(0, KvKind::kKey, 0), 0u);
  EXPECT_EQ(l.offset(0, KvKind::kValue, 0), slice);
  EXPECT_EQ(l.offset(1, KvKind::kKey, 0), 2 * slice);
  EXPECT_EQ(l.offset(0, KvKind::kKey, 1), l.block_bytes());
  EXPECT_EQ(l.offset(3, KvKind::kValue, 9) + slice, l.total_bytes());
}

TEST(Layout, BlockSegments) {
  const KvLayout bm = base(KvArrangement::kBlockMajor);
  auto segs = bm.block_segments(4);
  ASSERT_EQ(segs.size(), 1u);
  EXPECT_EQ(segs[0], (Segment{4 * bm.block_bytes(), bm.block_bytes()}));

  segs = bm.block_segments(4, LayerRange::of(1, 3));
  ASSERT_EQ(segs.size(), 1u);
  EXPECT_EQ(segs[0], (Segment{bm.offset(1, KvKind::kKey, 4), 2 * 2 * bm.slice_bytes()}));

  const KvLayout lm = base(KvArrangement::kLayerMajor);
  segs = lm.block_segments(4);
  ASSERT_EQ(segs.size(), 8u);
  EXPECT_EQ(segs[0], (Segment{lm.offset(0, KvKind::kKey, 4), lm.slice_bytes()}));
  EXPECT_EQ(segs[1], (Segment{lm.offset(0, KvKind::kValue, 4), lm.slice_bytes()}));
  EXPECT_EQ(lm.block_segments(4, LayerRange::single(2)).size(), 2u);
  EXPECT_TRUE(lm.block_segments(4, LayerRange::of(2, 2)).empty());
}

TEST(Layout, DescriptorsLayerMajorSingleBlock) {
  const KvLayout l = base(KvArrangement::kLayerMajor);
  const BlockId src[] = {2};
  const BlockId dst[] = {7};
  auto r = build_block_descriptors(l, 1, src, l, 2, dst);
  ASSERT_TRUE(r.ok()) << r.status().to_string();
  EXPECT_EQ(r->size(), 8u);
  EXPECT_EQ(total_bytes(*r), l.block_bytes());
  for (const auto& d : *r) {
    EXPECT_EQ(d.src_region, 1u);
    EXPECT_EQ(d.dst_region, 2u);
    EXPECT_EQ(d.length, l.slice_bytes());
    // Same slice index on both sides differs by (dst-src)*slice.
    EXPECT_EQ(d.dst_offset - d.src_offset, 5 * l.slice_bytes());
  }
}

TEST(Layout, DescriptorsCoalesceAdjacentBlocks) {
  const KvLayout lm = base(KvArrangement::kLayerMajor);
  const BlockId src[] = {3, 4, 5};
  const BlockId dst[] = {6, 7, 8};
  auto r = build_block_descriptors(lm, 1, src, lm, 1, dst);
  ASSERT_TRUE(r.ok());
  EXPECT_EQ(r->size(), 8u);  // one run per (layer, K|V)
  for (const auto& d : *r) EXPECT_EQ(d.length, 3 * lm.slice_bytes());
  EXPECT_EQ(total_bytes(*r), 3 * lm.block_bytes());

  const KvLayout bm = base(KvArrangement::kBlockMajor);
  r = build_block_descriptors(bm, 1, src, bm, 1, dst);
  ASSERT_TRUE(r.ok());
  ASSERT_EQ(r->size(), 1u);
  EXPECT_EQ((*r)[0].length, 3 * bm.block_bytes());
  EXPECT_EQ((*r)[0].src_offset, 3 * bm.block_bytes());
  EXPECT_EQ((*r)[0].dst_offset, 6 * bm.block_bytes());
}

TEST(Layout, DescriptorsNonAdjacentBlocksStaySeparate) {
  const KvLayout bm = base(KvArrangement::kBlockMajor);
  const BlockId src[] = {0, 2};
  const BlockId dst[] = {5, 6};
  auto r = build_block_descriptors(bm, 1, src, bm, 1, dst);
  ASSERT_TRUE(r.ok());
  EXPECT_EQ(r->size(), 2u);  // src not contiguous even though dst is
}

TEST(Layout, DescriptorsAcrossArrangements) {
  const KvLayout lm = base(KvArrangement::kLayerMajor);
  const KvLayout bm = base(KvArrangement::kBlockMajor);
  const BlockId src[] = {1, 2};
  const BlockId dst[] = {1, 2};
  auto r = build_block_descriptors(lm, 1, src, bm, 1, dst);
  ASSERT_TRUE(r.ok());
  EXPECT_EQ(r->size(), 16u);  // slices contiguous on one side only
  EXPECT_EQ(total_bytes(*r), 2 * lm.block_bytes());
}

TEST(Layout, DescriptorsLayerRange) {
  const KvLayout lm = base(KvArrangement::kLayerMajor);
  const BlockId blocks[] = {1};
  auto r = build_block_descriptors(lm, 1, blocks, lm, 1, blocks, LayerRange::of(1, 3));
  ASSERT_TRUE(r.ok());
  EXPECT_EQ(r->size(), 4u);
  for (const auto& d : *r) {
    EXPECT_GE(d.src_offset, lm.offset(1, KvKind::kKey, 0));
    EXPECT_LT(d.src_offset, lm.offset(3, KvKind::kKey, 0));
  }
}

TEST(Layout, DescriptorErrors) {
  const KvLayout l = base(KvArrangement::kLayerMajor);
  const BlockId one[] = {1};
  const BlockId two[] = {1, 2};
  const BlockId bad[] = {10};
  EXPECT_EQ(build_block_descriptors(l, 1, one, l, 1, two).status().code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(build_block_descriptors(l, 1, bad, l, 1, one).status().code(), StatusCode::kOutOfRange);
  EXPECT_EQ(build_block_descriptors(l, 1, one, l, 1, bad).status().code(), StatusCode::kOutOfRange);
  EXPECT_EQ(build_block_descriptors(l, 1, {}, l, 1, {}).status().code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(build_block_descriptors(l, 1, one, l, 1, one, LayerRange::of(2, 9)).status().code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(build_block_descriptors(l, 1, one, l, 1, one, LayerRange::of(3, 3)).status().code(),
            StatusCode::kInvalidArgument);
  KvLayout other = l;
  other.head_dim = 16;
  EXPECT_EQ(build_block_descriptors(l, 1, one, other, 1, one).status().code(),
            StatusCode::kInvalidArgument);
  // Different num_blocks is fine.
  other = l;
  other.num_blocks = 100;
  EXPECT_TRUE(build_block_descriptors(l, 1, one, other, 1, one).ok());
}

TEST(Layout, CoalesceRequiresBothSidesContiguous) {
  std::vector<XferDesc> d = {{1, 0, 2, 0, 10}, {1, 10, 2, 10, 10}, {1, 20, 2, 40, 10}, {3, 30, 2, 50, 10}};
  coalesce_descriptors(d);
  ASSERT_EQ(d.size(), 3u);
  EXPECT_EQ(d[0], (XferDesc{1, 0, 2, 0, 20}));
  EXPECT_EQ(d[1], (XferDesc{1, 20, 2, 40, 10}));
  EXPECT_EQ(d[2], (XferDesc{3, 30, 2, 50, 10}));
}

TEST(Layout, Fingerprint) {
  const KvLayout a = base(KvArrangement::kLayerMajor);
  KvLayout b = a;
  EXPECT_EQ(a.fingerprint(), b.fingerprint());
  b.arrangement = KvArrangement::kBlockMajor;
  EXPECT_NE(a.fingerprint(), b.fingerprint());
  b = a;
  b.num_blocks += 1;
  EXPECT_NE(a.fingerprint(), b.fingerprint());
  EXPECT_TRUE(a.transfer_compatible(b));
}

TEST(Types, LayerRange) {
  EXPECT_TRUE(LayerRange::all().is_all());
  EXPECT_EQ(LayerRange::all().resolve(32), LayerRange::of(0, 32));
  EXPECT_EQ(LayerRange::of(4, 8).count(), 4u);
  EXPECT_EQ(LayerRange::of(8, 4).count(), 0u);
  EXPECT_EQ(LayerRange::single(5), LayerRange::of(5, 6));
}

TEST(Types, EndpointParse) {
  auto e = Endpoint::parse("10.0.0.1:9000");
  ASSERT_TRUE(e);
  EXPECT_EQ(e->host, "10.0.0.1");
  EXPECT_EQ(e->port, 9000);
  EXPECT_EQ(e->to_string(), "10.0.0.1:9000");
  e = Endpoint::parse("[::1]:80");
  ASSERT_TRUE(e);
  EXPECT_EQ(e->host, "::1");
  EXPECT_EQ(e->port, 80);
  EXPECT_EQ(e->to_string(), "[::1]:80");
  EXPECT_FALSE(Endpoint::parse("nohost"));
  EXPECT_FALSE(Endpoint::parse(":80"));
  EXPECT_FALSE(Endpoint::parse("h:99999"));
  EXPECT_FALSE(Endpoint::parse("h:abc"));
}

TEST(Types, StatusFormatting) {
  EXPECT_EQ(Status::Ok().to_string(), "OK");
  EXPECT_EQ(Status::NotFound("x").to_string(), "NOT_FOUND: x");
  Result<int> r = Status::Ok();  // misuse becomes Internal, not a silent success
  EXPECT_FALSE(r.ok());
  EXPECT_EQ(r.status().code(), StatusCode::kInternal);
  Result<int> v = 5;
  EXPECT_TRUE(v.ok());
  EXPECT_EQ(*v, 5);
}

}  // namespace
