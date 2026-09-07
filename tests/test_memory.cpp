#include "kvc/memory.hpp"

#include <gtest/gtest.h>

using namespace kvc;

TEST(Memory, RegistryAddGetRemove) {
  MemoryRegistry reg;
  std::vector<uint8_t> a(100), b(200);
  auto ia = reg.add(a.data(), a.size());
  auto ib = reg.add(b.data(), b.size(), MemoryKind::kHostPinned, 3);
  ASSERT_TRUE(ia.ok());
  ASSERT_TRUE(ib.ok());
  EXPECT_NE(*ia, *ib);
  EXPECT_EQ(reg.size(), 2u);
  auto ra = reg.get(*ia);
  ASSERT_TRUE(ra);
  EXPECT_EQ(ra->base, a.data());
  EXPECT_EQ(ra->length, 100u);
  auto rb = reg.get(*ib);
  EXPECT_EQ(rb->kind, MemoryKind::kHostPinned);
  EXPECT_EQ(rb->device, 3);
  EXPECT_TRUE(reg.remove(*ia).ok());
  EXPECT_FALSE(reg.get(*ia));
  EXPECT_EQ(reg.remove(*ia).code(), StatusCode::kNotFound);
  EXPECT_EQ(reg.list().size(), 1u);
}

TEST(Memory, RegistryRejectsBadInput) {
  MemoryRegistry reg;
  std::vector<uint8_t> a(10);
  EXPECT_FALSE(reg.add(nullptr, 10).ok());
  EXPECT_FALSE(reg.add(a.data(), 0).ok());
  MemoryRegion explicit_region{7, a.data(), 10, MemoryKind::kHost, -1};
  EXPECT_TRUE(reg.add(explicit_region).ok());
  EXPECT_EQ(reg.add(explicit_region).code(), StatusCode::kAlreadyExists);
  // Auto ids skip over explicitly used ids.
  for (int i = 0; i < 10; ++i) {
    auto id = reg.add(a.data(), 10);
    ASSERT_TRUE(id.ok());
    EXPECT_NE(*id, 7u);
  }
}

TEST(Memory, RegionContains) {
  std::vector<uint8_t> a(100);
  MemoryRegion r{1, a.data(), 100, MemoryKind::kHost, -1};
  EXPECT_TRUE(r.contains(0, 100));
  EXPECT_TRUE(r.contains(100, 0));
  EXPECT_TRUE(r.contains(50, 50));
  EXPECT_FALSE(r.contains(50, 51));
  EXPECT_FALSE(r.contains(101, 0));
  EXPECT_FALSE(r.contains(UINT64_MAX, 1));  // no overflow
  EXPECT_EQ(r.at(10), reinterpret_cast<std::byte*>(a.data() + 10));
}
