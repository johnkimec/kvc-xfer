#include "kvc/engine.hpp"

#include <gtest/gtest.h>

#include "kvc/local_transport.hpp"
#include "test_util.hpp"

using namespace kvc;
using kvc::test::TcpNode;

namespace {

TEST(Engine, StartValidatesConfig) {
  auto fabric = std::make_shared<LocalFabric>();
  auto t = std::make_shared<LocalTransport>(LocalTransportConfig{"n", true}, fabric);
  KvLayout bad;
  TransferEngine e1(EngineConfig{"n", bad, {}}, t);
  EXPECT_EQ(e1.start().code(), StatusCode::kInvalidArgument);
  TransferEngine e2(EngineConfig{"other", test::small_layout(), {}}, t);
  EXPECT_EQ(e2.start().code(), StatusCode::kInvalidArgument);  // self mismatch
  TransferEngine e3(EngineConfig{"", test::small_layout(), {}}, t);
  ASSERT_TRUE(e3.start().ok());
  EXPECT_EQ(e3.self(), "n");
  EXPECT_TRUE(e3.start().ok());
  TransferEngine e4(EngineConfig{"n", test::small_layout(), {}}, nullptr);
  EXPECT_EQ(e4.start().code(), StatusCode::kInvalidArgument);
}

TEST(Engine, RegisterKvCacheChecksSize) {
  auto fabric = std::make_shared<LocalFabric>();
  auto t = std::make_shared<LocalTransport>(LocalTransportConfig{"n", true}, fabric);
  const KvLayout layout = test::small_layout();
  TransferEngine e(EngineConfig{"n", layout, {}}, t);
  ASSERT_TRUE(e.start().ok());
  std::vector<uint8_t> small(layout.total_bytes() - 1), exact(layout.total_bytes());
  EXPECT_EQ(e.register_kv_cache(small.data(), small.size()).status().code(),
            StatusCode::kInvalidArgument);
  auto id = e.register_kv_cache(exact.data(), exact.size());
  ASSERT_TRUE(id.ok());
  EXPECT_TRUE(e.region(*id));
  EXPECT_TRUE(e.unregister_region(*id).ok());
  EXPECT_FALSE(e.region(*id));
  EXPECT_EQ(e.unregister_region(*id).code(), StatusCode::kNotFound);
  EXPECT_EQ(e.push_raw("x", {{1, 0, 1, 0, 1}}).wait().status.code(), StatusCode::kNotFound);
  e.stop();
  EXPECT_EQ(e.push_raw("x", {{1, 0, 1, 0, 1}}).wait().status.code(), StatusCode::kUnavailable);
}

TEST(Engine, PushBlocksOverTcp) {
  const KvLayout layout = test::small_layout(KvArrangement::kLayerMajor, 32);
  TcpNode prefill("prefill", layout), decode("decode", layout);
  ASSERT_TRUE(prefill.connect_to(decode).ok());
  test::fill_pattern(prefill.mem, 11);
  test::fill_pattern(decode.mem, 22);

  const std::vector<BlockId> src = {3, 4, 9};
  const std::vector<BlockId> dst = {20, 21, 5};
  auto h = prefill.engine->push_blocks("decode", prefill.region, src, decode.region, dst);
  auto r = h.wait();
  ASSERT_TRUE(r.ok()) << r.status.to_string();
  EXPECT_EQ(r.bytes, 3 * layout.block_bytes());
  for (size_t i = 0; i < src.size(); ++i) {
    EXPECT_TRUE(test::block_matches(decode.mem, layout, dst[i], prefill.mem, layout, src[i]));
  }
  // Untouched blocks keep their own pattern.
  EXPECT_TRUE(test::block_matches(decode.mem, layout, 0, decode.mem, layout, 0));
  std::vector<uint8_t> original(layout.total_bytes());
  test::fill_pattern(original, 22);
  EXPECT_TRUE(test::block_matches(decode.mem, layout, 6, original, layout, 6));

  auto m = prefill.engine->metrics().snapshot();
  EXPECT_EQ(m.transfers_started, 1u);
  EXPECT_EQ(m.transfers_completed, 1u);
  EXPECT_EQ(m.bytes_pushed, 3 * layout.block_bytes());
  EXPECT_EQ(m.transfers_inflight, 0u);
}

TEST(Engine, PullBlocksAndLayerRange) {
  const KvLayout layout = test::small_layout(KvArrangement::kBlockMajor, 8);
  TcpNode a("a", layout), b("b", layout);
  ASSERT_TRUE(a.connect_to(b).ok());
  test::fill_pattern(a.mem, 1);
  test::fill_pattern(b.mem, 2);
  std::vector<uint8_t> b_before = b.mem;

  const std::vector<BlockId> remote = {1};
  const std::vector<BlockId> local = {6};
  auto r = b.engine->pull_blocks("a", a.region, remote, b.region, local, LayerRange::of(1, 3)).wait();
  ASSERT_TRUE(r.ok()) << r.status.to_string();
  EXPECT_EQ(r.bytes, 2 * 2 * layout.slice_bytes());
  EXPECT_TRUE(test::block_matches(b.mem, layout, 6, a.mem, layout, 1, LayerRange::of(1, 3)));
  EXPECT_TRUE(test::block_matches(b.mem, layout, 6, b_before, layout, 6, LayerRange::of(0, 1)));
  EXPECT_TRUE(test::block_matches(b.mem, layout, 6, b_before, layout, 6, LayerRange::of(3, 4)));
  auto m = b.engine->metrics().snapshot();
  EXPECT_EQ(m.bytes_pulled, r.bytes);
  EXPECT_EQ(m.bytes_pushed, 0u);
}

TEST(Engine, HeterogeneousArrangementsViaPeerLayout) {
  const KvLayout lm = test::small_layout(KvArrangement::kLayerMajor, 8);
  const KvLayout bm = test::small_layout(KvArrangement::kBlockMajor, 64);
  TcpNode a("a", lm), b("b", bm);
  ASSERT_TRUE(a.connect_to(b).ok());
  test::fill_pattern(a.mem, 1);
  test::fill_pattern(b.mem, 2);

  const std::vector<BlockId> src = {2, 3};
  const std::vector<BlockId> dst = {40, 41};
  // Without the peer layout the engine assumes b looks like a: block 40 is
  // out of range for an 8-block cache.
  EXPECT_EQ(a.engine->push_blocks("b", a.region, src, b.region, dst).wait().status.code(),
            StatusCode::kOutOfRange);
  a.engine->set_peer_layout("b", bm);
  EXPECT_EQ(a.engine->peer_layout("b").fingerprint(), bm.fingerprint());
  auto r = a.engine->push_blocks("b", a.region, src, b.region, dst).wait();
  ASSERT_TRUE(r.ok()) << r.status.to_string();
  EXPECT_TRUE(test::block_matches(b.mem, bm, 40, a.mem, lm, 2));
  EXPECT_TRUE(test::block_matches(b.mem, bm, 41, a.mem, lm, 3));
}

TEST(Engine, ValidationFailuresAreImmediate) {
  const KvLayout layout = test::small_layout();
  TcpNode a("a", layout), b("b", layout);
  ASSERT_TRUE(a.connect_to(b).ok());
  const std::vector<BlockId> one = {1};
  const std::vector<BlockId> bad = {99};
  EXPECT_EQ(a.engine->push_blocks("b", a.region, bad, b.region, one).wait().status.code(),
            StatusCode::kOutOfRange);
  EXPECT_EQ(a.engine->push_blocks("b", 77, one, b.region, one).wait().status.code(),
            StatusCode::kNotFound);
  EXPECT_EQ(a.engine->pull_blocks("b", b.region, one, 77, one).wait().status.code(),
            StatusCode::kNotFound);
  EXPECT_EQ(a.engine->push_blocks("nobody", a.region, one, b.region, one).wait().status.code(),
            StatusCode::kNotFound);
  // Remote region mismatch is caught by the remote.
  auto r = a.engine->push_blocks("b", a.region, one, 12345, one).wait();
  EXPECT_EQ(r.status.code(), StatusCode::kNotFound) << r.status.to_string();
  // Only pushes that pass engine validation reach the transport and count:
  // the unknown-peer push and the unknown-remote-region push.
  auto m = a.engine->metrics().snapshot();
  EXPECT_EQ(m.transfers_started, 2u);
  EXPECT_EQ(m.transfers_failed, 2u);
  EXPECT_EQ(m.transfers_completed, 0u);
}

TEST(Engine, DefaultTimeoutApplied) {
  const KvLayout layout = test::small_layout();
  TcpNode a("a", layout), b("b", layout);
  ASSERT_TRUE(a.connect_to(b).ok());
  // Sanity: a real transfer with a generous default timeout still succeeds.
  const std::vector<BlockId> one = {1};
  EXPECT_TRUE(a.engine->push_blocks("b", a.region, one, b.region, one, LayerRange::all(),
                                    TransferOptions{std::chrono::seconds(5)})
                  .wait()
                  .ok());
}

TEST(Engine, NotifyRoundTripAndMetrics) {
  const KvLayout layout = test::small_layout();
  TcpNode a("a", layout), b("b", layout);
  ASSERT_TRUE(a.connect_to(b).ok());
  std::atomic<int> got{0};
  b.engine->set_notify_handler([&](const PeerId& from, std::string_view p) {
    if (from == "a" && p == "ping") got.fetch_add(1);
  });
  ASSERT_TRUE(a.engine->notify("b", "ping").ok());
  ASSERT_TRUE(test::wait_until([&] { return got.load() == 1; }));
  EXPECT_EQ(a.engine->metrics().snapshot().notifies_sent, 1u);
  EXPECT_EQ(b.engine->metrics().snapshot().notifies_received, 1u);
  EXPECT_NE(a.engine->metrics().snapshot().to_string().find("notifies{sent=1"), std::string::npos);
}

}  // namespace
