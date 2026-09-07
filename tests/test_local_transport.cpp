#include "kvc/local_transport.hpp"

#include <gtest/gtest.h>

#include "test_util.hpp"

using namespace kvc;

namespace {

struct LocalPair {
  std::shared_ptr<LocalFabric> fabric = std::make_shared<LocalFabric>();
  LocalTransport a{LocalTransportConfig{"a", false}, fabric};
  LocalTransport b{LocalTransportConfig{"b", false}, fabric};
  std::vector<uint8_t> amem = std::vector<uint8_t>(1024);
  std::vector<uint8_t> bmem = std::vector<uint8_t>(1024);

  LocalPair() {
    EXPECT_TRUE(a.start().ok());
    EXPECT_TRUE(b.start().ok());
    EXPECT_TRUE(a.connect("b", {}).ok());
    EXPECT_TRUE(b.connect("a", {}).ok());
    EXPECT_TRUE(a.register_memory({1, amem.data(), amem.size(), MemoryKind::kHost, -1}).ok());
    EXPECT_TRUE(b.register_memory({2, bmem.data(), bmem.size(), MemoryKind::kHost, -1}).ok());
    test::fill_pattern(amem, 1);
    test::fill_pattern(bmem, 2);
  }
};

TEST(LocalTransport, WriteAndRead) {
  LocalPair p;
  auto w = p.a.write("b", {{1, 100, 2, 200, 300}});
  auto r = w.wait();
  ASSERT_TRUE(r.ok()) << r.status.to_string();
  EXPECT_EQ(r.bytes, 300u);
  EXPECT_TRUE(std::equal(p.amem.begin() + 100, p.amem.begin() + 400, p.bmem.begin() + 200));
  EXPECT_EQ(p.bmem[199], test::pattern(199, 2));  // untouched neighbours
  EXPECT_EQ(p.bmem[500], test::pattern(500, 2));

  auto rd = p.a.read("b", {{2, 0, 1, 512, 64}, {2, 900, 1, 0, 100}});
  r = rd.wait();
  ASSERT_TRUE(r.ok()) << r.status.to_string();
  EXPECT_EQ(r.bytes, 164u);
  EXPECT_TRUE(std::equal(p.bmem.begin(), p.bmem.begin() + 64, p.amem.begin() + 512));
  EXPECT_TRUE(std::equal(p.bmem.begin() + 900, p.bmem.begin() + 1000, p.amem.begin()));
}

TEST(LocalTransport, Errors) {
  LocalPair p;
  EXPECT_EQ(p.a.write("b", {{1, 0, 9, 0, 10}}).wait().status.code(), StatusCode::kNotFound);
  EXPECT_EQ(p.a.write("b", {{1, 1000, 2, 0, 100}}).wait().status.code(), StatusCode::kOutOfRange);
  EXPECT_EQ(p.a.write("b", {{1, 0, 2, 1000, 100}}).wait().status.code(), StatusCode::kOutOfRange);
  EXPECT_EQ(p.a.write("zzz", {{1, 0, 2, 0, 1}}).wait().status.code(), StatusCode::kNotFound);
  EXPECT_EQ(p.a.write("b", {}).wait().status.code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(p.a.notify("zzz", "x").code(), StatusCode::kNotFound);
  EXPECT_EQ(p.a.connect("nobody", {}).code(), StatusCode::kUnavailable);
  // A partially failing batch reports the bytes that did land.
  auto r = p.a.write("b", {{1, 0, 2, 0, 10}, {1, 0, 2, 5000, 10}}).wait();
  EXPECT_FALSE(r.ok());
  EXPECT_EQ(r.bytes, 10u);
}

TEST(LocalTransport, Notify) {
  LocalPair p;
  std::mutex mu;
  std::vector<std::pair<PeerId, std::string>> got;
  p.b.set_notify_handler([&](const PeerId& from, std::string_view payload) {
    std::lock_guard<std::mutex> lock(mu);
    got.emplace_back(from, std::string(payload));
  });
  ASSERT_TRUE(p.a.notify("b", "hello").ok());
  ASSERT_TRUE(p.a.notify("b", std::string("\0bin", 4)).ok());
  ASSERT_TRUE(test::wait_until([&] {
    std::lock_guard<std::mutex> lock(mu);
    return got.size() == 2;
  }));
  EXPECT_EQ(got[0].first, "a");
  EXPECT_EQ(got[0].second, "hello");
  EXPECT_EQ(got[1].second, std::string("\0bin", 4));
}

TEST(LocalTransport, SynchronousModeCompletesInline) {
  auto fabric = std::make_shared<LocalFabric>();
  LocalTransport a{LocalTransportConfig{"a", true}, fabric};
  LocalTransport b{LocalTransportConfig{"b", true}, fabric};
  ASSERT_TRUE(a.start().ok());
  ASSERT_TRUE(b.start().ok());
  ASSERT_TRUE(a.connect("b", {}).ok());
  std::vector<uint8_t> am(16, 1), bm(16, 0);
  a.register_memory({1, am.data(), 16, MemoryKind::kHost, -1});
  b.register_memory({1, bm.data(), 16, MemoryKind::kHost, -1});
  auto h = a.write("b", {{1, 0, 1, 0, 16}});
  EXPECT_TRUE(h.done());
  EXPECT_EQ(bm[15], 1);
  EXPECT_TRUE(a.disconnect("b").ok());
  EXPECT_FALSE(a.is_connected("b"));
  b.stop();
  EXPECT_FALSE(a.connect("b", {}).ok());
}

}  // namespace
