#include "kvc/handoff.hpp"

#include <gtest/gtest.h>

#include <future>

#include "test_util.hpp"

using namespace kvc;
using kvc::test::TcpNode;

namespace {

struct ReadyRecorder {
  std::mutex mu;
  std::map<std::string, std::pair<Status, uint64_t>> results;
  KvHandoff::ReadyCallback cb() {
    return [this](const std::string& id, const Status& s, uint64_t bytes) {
      std::lock_guard<std::mutex> lock(mu);
      results[id] = {s, bytes};
    };
  }
  bool wait(const std::string& id, std::chrono::milliseconds t = std::chrono::seconds(5)) {
    return test::wait_until(
        [&] {
          std::lock_guard<std::mutex> lock(mu);
          return results.count(id) != 0;
        },
        t);
  }
  std::pair<Status, uint64_t> get(const std::string& id) {
    std::lock_guard<std::mutex> lock(mu);
    return results.at(id);
  }
};

struct PD {
  KvLayout layout;
  ReadyRecorder ready;  // declared first so it outlives the handoffs' callbacks
  TcpNode prefill;
  TcpNode decode;
  KvHandoff producer;
  KvHandoff consumer;

  explicit PD(HandoffConfig cfg = {}, KvLayout l = test::small_layout(KvArrangement::kLayerMajor, 32))
      : layout(l),
        prefill("prefill", layout),
        decode("decode", layout),
        producer(*prefill.engine, cfg),
        consumer(*decode.engine, cfg) {
    EXPECT_TRUE(prefill.connect_to(decode).ok());
    EXPECT_TRUE(test::wait_until([&] { return decode.engine->is_connected("prefill"); }));
    test::fill_pattern(prefill.mem, 100);
    test::fill_pattern(decode.mem, 200);
  }
  bool landed(const std::vector<BlockId>& src, const std::vector<BlockId>& dst,
              LayerRange range = LayerRange::all()) {
    for (size_t i = 0; i < src.size(); ++i) {
      if (!test::block_matches(decode.mem, layout, dst[i], prefill.mem, layout, src[i], range)) {
        return false;
      }
    }
    return true;
  }
};

TEST(Handoff, ExpectThenPublish) {
  PD pd;
  const std::vector<BlockId> dst = {10, 11, 12, 13};
  const std::vector<BlockId> src = {0, 1, 2, 3};
  ASSERT_TRUE(pd.consumer.expect("req-1", "prefill", pd.decode.region, dst, pd.ready.cb()).ok());
  ASSERT_TRUE(test::wait_until([&] { return pd.producer.has_prepare("req-1"); }));
  auto h = pd.producer.publish("req-1", "decode", pd.prefill.region, src);
  auto r = h.wait();
  ASSERT_TRUE(r.ok()) << r.status.to_string();
  EXPECT_EQ(r.bytes, 4 * pd.layout.block_bytes());
  ASSERT_TRUE(pd.ready.wait("req-1"));
  auto [st, bytes] = pd.ready.get("req-1");
  EXPECT_TRUE(st.ok()) << st.to_string();
  EXPECT_EQ(bytes, 4 * pd.layout.block_bytes());
  EXPECT_TRUE(pd.landed(src, dst));
  EXPECT_EQ(pd.producer.pending_prepares(), 0u);
  EXPECT_EQ(pd.consumer.pending_expects(), 0u);
}

TEST(Handoff, PublishBeforeExpectIsQueued) {
  PD pd;
  const std::vector<BlockId> dst = {5};
  const std::vector<BlockId> src = {7};
  auto h = pd.producer.publish("req-2", "decode", pd.prefill.region, src);
  EXPECT_FALSE(h.wait_for(std::chrono::milliseconds(50)));
  EXPECT_EQ(pd.producer.pending_publishes(), 1u);
  ASSERT_TRUE(pd.consumer.expect("req-2", "prefill", pd.decode.region, dst, pd.ready.cb()).ok());
  auto r = h.wait();
  ASSERT_TRUE(r.ok()) << r.status.to_string();
  ASSERT_TRUE(pd.ready.wait("req-2"));
  EXPECT_TRUE(pd.ready.get("req-2").first.ok());
  EXPECT_TRUE(pd.landed(src, dst));
  EXPECT_EQ(pd.producer.pending_publishes(), 0u);
  // A second publish for the same request without a new PREPARE just waits.
  auto dup = pd.producer.publish("req-2", "decode", pd.prefill.region, src);
  EXPECT_FALSE(dup.wait_for(std::chrono::milliseconds(20)));
  EXPECT_TRUE(pd.producer.abort("req-2", "test cleanup").ok());
  EXPECT_EQ(dup.wait().status.code(), StatusCode::kCancelled);
}

TEST(Handoff, LayerWisePipelining) {
  PD pd;
  const std::vector<BlockId> dst = {1, 2};
  const std::vector<BlockId> src = {3, 4};
  ASSERT_TRUE(pd.consumer.expect("req-3", "prefill", pd.decode.region, dst, pd.ready.cb()).ok());
  ASSERT_TRUE(test::wait_until([&] { return pd.producer.has_prepare("req-3"); }));
  auto h1 = pd.producer.publish("req-3", "decode", pd.prefill.region, src, LayerRange::of(0, 2));
  ASSERT_TRUE(h1.wait().ok());
  EXPECT_FALSE(pd.ready.wait("req-3", std::chrono::milliseconds(100)));  // half is not ready
  EXPECT_TRUE(pd.landed(src, dst, LayerRange::of(0, 2)));
  EXPECT_TRUE(pd.producer.has_prepare("req-3"));
  auto h2 = pd.producer.publish("req-3", "decode", pd.prefill.region, src, LayerRange::of(2, 4));
  ASSERT_TRUE(h2.wait().ok());
  ASSERT_TRUE(pd.ready.wait("req-3"));
  auto [st, bytes] = pd.ready.get("req-3");
  EXPECT_TRUE(st.ok());
  EXPECT_EQ(bytes, 2 * pd.layout.block_bytes());
  EXPECT_TRUE(pd.landed(src, dst));
  EXPECT_FALSE(pd.producer.has_prepare("req-3"));
}

TEST(Handoff, PublishValidationErrors) {
  PD pd;
  const std::vector<BlockId> dst = {1, 2};
  ASSERT_TRUE(pd.consumer.expect("req-4", "prefill", pd.decode.region, dst, pd.ready.cb()).ok());
  ASSERT_TRUE(test::wait_until([&] { return pd.producer.has_prepare("req-4"); }));
  // Block count mismatch.
  auto r = pd.producer.publish("req-4", "decode", pd.prefill.region, {1}).wait();
  EXPECT_EQ(r.status.code(), StatusCode::kInvalidArgument) << r.status.to_string();
  // Layer range outside what the consumer expects.
  r = pd.producer.publish("req-4", "decode", pd.prefill.region, {1, 2}, LayerRange::of(2, 9)).wait();
  EXPECT_EQ(r.status.code(), StatusCode::kInvalidArgument);
  // Wrong consumer name.
  r = pd.producer.publish("req-4", "someone-else", pd.prefill.region, {1, 2}).wait();
  EXPECT_EQ(r.status.code(), StatusCode::kInvalidArgument);
  // Bad local arguments.
  EXPECT_EQ(pd.producer.publish("", "decode", pd.prefill.region, {1, 2}).wait().status.code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(pd.producer.publish("req-4", "decode", 999, {1, 2}).wait().status.code(),
            StatusCode::kNotFound);
  EXPECT_EQ(pd.producer.publish("req-4", "decode", pd.prefill.region, {}).wait().status.code(),
            StatusCode::kInvalidArgument);
  // Consumer is still waiting; the valid publish goes through.
  EXPECT_TRUE(pd.producer.publish("req-4", "decode", pd.prefill.region, {1, 2}).wait().ok());
  ASSERT_TRUE(pd.ready.wait("req-4"));
  EXPECT_TRUE(pd.ready.get("req-4").first.ok());
}

TEST(Handoff, ExpectValidationErrors) {
  PD pd;
  EXPECT_EQ(pd.consumer.expect("", "prefill", pd.decode.region, {1}, pd.ready.cb()).code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(pd.consumer.expect("r", "prefill", pd.decode.region, {}, pd.ready.cb()).code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(pd.consumer.expect("r", "prefill", pd.decode.region, {999}, pd.ready.cb()).code(),
            StatusCode::kOutOfRange);
  EXPECT_EQ(pd.consumer.expect("r", "prefill", 999, {1}, pd.ready.cb()).code(),
            StatusCode::kNotFound);
  EXPECT_EQ(pd.consumer
                .expect("r", "prefill", pd.decode.region, {1}, pd.ready.cb(), LayerRange::of(0, 9))
                .code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(pd.consumer.expect("r", "nobody", pd.decode.region, {1}, pd.ready.cb()).code(),
            StatusCode::kNotFound);
  EXPECT_EQ(pd.consumer.pending_expects(), 0u);
  ASSERT_TRUE(pd.consumer.expect("r", "prefill", pd.decode.region, {1}, pd.ready.cb()).ok());
  EXPECT_EQ(pd.consumer.expect("r", "prefill", pd.decode.region, {1}, pd.ready.cb()).code(),
            StatusCode::kAlreadyExists);
}

TEST(Handoff, RemoteTransferFailureReportedToConsumer) {
  PD pd;
  // The consumer lies about its region id; the producer's push fails at the
  // consumer's transport and the failure is relayed via READY.
  ASSERT_TRUE(pd.consumer.expect("req-5", "prefill", pd.decode.region, {1}, pd.ready.cb()).ok());
  ASSERT_TRUE(test::wait_until([&] { return pd.producer.has_prepare("req-5"); }));
  ASSERT_TRUE(pd.decode.engine->unregister_region(pd.decode.region).ok());
  auto r = pd.producer.publish("req-5", "decode", pd.prefill.region, {1}).wait();
  EXPECT_EQ(r.status.code(), StatusCode::kNotFound) << r.status.to_string();
  ASSERT_TRUE(pd.ready.wait("req-5"));
  EXPECT_EQ(pd.ready.get("req-5").first.code(), StatusCode::kNotFound);
  EXPECT_EQ(pd.consumer.pending_expects(), 0u);
  EXPECT_TRUE(pd.producer.has_prepare("req-5"));  // producer may retry after fixing things
}

TEST(Handoff, AbortFromConsumer) {
  PD pd;
  ASSERT_TRUE(pd.consumer.expect("req-6", "prefill", pd.decode.region, {1}, pd.ready.cb()).ok());
  ASSERT_TRUE(test::wait_until([&] { return pd.producer.has_prepare("req-6"); }));
  ASSERT_TRUE(pd.consumer.abort("req-6", "scheduler preempted").ok());
  ASSERT_TRUE(pd.ready.wait("req-6"));
  EXPECT_EQ(pd.ready.get("req-6").first.code(), StatusCode::kCancelled);
  ASSERT_TRUE(test::wait_until([&] { return !pd.producer.has_prepare("req-6"); }));
  EXPECT_EQ(pd.consumer.abort("req-6", "again").code(), StatusCode::kNotFound);
  // Producer now queues rather than pushing to a vanished consumer.
  auto h = pd.producer.publish("req-6", "decode", pd.prefill.region, {1});
  EXPECT_FALSE(h.wait_for(std::chrono::milliseconds(20)));
  pd.producer.abort("req-6", "cleanup");
}

TEST(Handoff, AbortFromProducer) {
  PD pd;
  auto h = pd.producer.publish("req-7", "decode", pd.prefill.region, {1});
  ASSERT_TRUE(pd.consumer.expect("req-7", "prefill", pd.decode.region, {1}, pd.ready.cb()).ok());
  ASSERT_TRUE(h.wait().ok());
  ASSERT_TRUE(pd.ready.wait("req-7"));
  // Now a fresh request that the producer aborts while the consumer waits.
  ASSERT_TRUE(pd.consumer.expect("req-8", "prefill", pd.decode.region, {2}, pd.ready.cb()).ok());
  ASSERT_TRUE(test::wait_until([&] { return pd.producer.has_prepare("req-8"); }));
  ASSERT_TRUE(pd.producer.abort("req-8", "prefill failed").ok());
  ASSERT_TRUE(pd.ready.wait("req-8"));
  EXPECT_EQ(pd.ready.get("req-8").first.code(), StatusCode::kCancelled);
  EXPECT_NE(pd.ready.get("req-8").first.message().find("prefill failed"), std::string::npos);
}

TEST(Handoff, PrepareTimeout) {
  HandoffConfig cfg;
  cfg.prepare_wait_timeout = std::chrono::milliseconds(150);
  PD pd(cfg);
  auto h = pd.producer.publish("req-9", "decode", pd.prefill.region, {1});
  auto r = h.wait_for(std::chrono::seconds(5));
  ASSERT_TRUE(r);
  EXPECT_EQ(r->status.code(), StatusCode::kTimeout) << r->status.to_string();
  EXPECT_EQ(pd.producer.pending_publishes(), 0u);
}

TEST(Handoff, ReadyTimeoutAbortsProducerSide) {
  HandoffConfig cfg;
  cfg.ready_wait_timeout = std::chrono::milliseconds(150);
  PD pd(cfg);
  ASSERT_TRUE(pd.consumer.expect("req-10", "prefill", pd.decode.region, {1}, pd.ready.cb()).ok());
  ASSERT_TRUE(test::wait_until([&] { return pd.producer.has_prepare("req-10"); }));
  ASSERT_TRUE(pd.ready.wait("req-10"));
  EXPECT_EQ(pd.ready.get("req-10").first.code(), StatusCode::kTimeout);
  ASSERT_TRUE(test::wait_until([&] { return !pd.producer.has_prepare("req-10"); }));
}

TEST(Handoff, LayoutFingerprintMismatch) {
  const KvLayout lm = test::small_layout(KvArrangement::kLayerMajor, 32);
  const KvLayout bm = test::small_layout(KvArrangement::kBlockMajor, 64);
  TcpNode prefill("prefill", lm), decode("decode", bm);
  KvHandoff producer(*prefill.engine), consumer(*decode.engine);
  ASSERT_TRUE(prefill.connect_to(decode).ok());
  ASSERT_TRUE(test::wait_until([&] { return decode.engine->is_connected("prefill"); }));
  test::fill_pattern(prefill.mem, 1);
  ReadyRecorder ready;
  ASSERT_TRUE(consumer.expect("req-11", "prefill", decode.region, {50}, ready.cb()).ok());
  ASSERT_TRUE(test::wait_until([&] { return producer.has_prepare("req-11"); }));
  auto r = producer.publish("req-11", "decode", prefill.region, {3}).wait();
  EXPECT_EQ(r.status.code(), StatusCode::kInvalidArgument) << r.status.to_string();
  EXPECT_NE(r.status.message().find("fingerprint"), std::string::npos);
  prefill.engine->set_peer_layout("decode", bm);
  r = producer.publish("req-11", "decode", prefill.region, {3}).wait();
  ASSERT_TRUE(r.ok()) << r.status.to_string();
  ASSERT_TRUE(ready.wait("req-11"));
  EXPECT_TRUE(ready.get("req-11").first.ok());
  EXPECT_TRUE(test::block_matches(decode.mem, bm, 50, prefill.mem, lm, 3));
}

TEST(Handoff, DisconnectFailsOutstandingState) {
  PD pd;
  ASSERT_TRUE(pd.consumer.expect("req-12", "prefill", pd.decode.region, {1}, pd.ready.cb()).ok());
  auto h = pd.producer.publish("req-13", "decode", pd.prefill.region, {1});
  ASSERT_TRUE(test::wait_until([&] { return pd.producer.has_prepare("req-12"); }));
  std::atomic<int> user_disc{0};
  pd.consumer.set_disconnect_handler([&](const PeerId&) { user_disc.fetch_add(1); });
  ASSERT_TRUE(pd.prefill.engine->disconnect("decode").ok());
  ASSERT_TRUE(pd.ready.wait("req-12"));
  EXPECT_EQ(pd.ready.get("req-12").first.code(), StatusCode::kDisconnected);
  EXPECT_EQ(h.wait().status.code(), StatusCode::kDisconnected);
  EXPECT_EQ(pd.producer.pending_prepares(), 0u);
  EXPECT_EQ(user_disc.load(), 1);
}

TEST(Handoff, UserNotifiesPassThrough) {
  PD pd;
  std::promise<std::string> got;
  pd.consumer.set_notify_handler(
      [&](const PeerId&, std::string_view p) { got.set_value(std::string(p)); });
  ASSERT_TRUE(pd.prefill.engine->notify("decode", "app-level message").ok());
  auto f = got.get_future();
  ASSERT_EQ(f.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_EQ(f.get(), "app-level message");
}

TEST(Handoff, ManyRequestsConcurrently) {
  PD pd(HandoffConfig{}, test::small_layout(KvArrangement::kLayerMajor, 256));
  constexpr int kRequests = 40;
  std::vector<std::thread> threads;
  for (int i = 0; i < kRequests; ++i) {
    threads.emplace_back([&, i] {
      const std::string id = "r" + std::to_string(i);
      const std::vector<BlockId> src = {static_cast<BlockId>(i * 2), static_cast<BlockId>(i * 2 + 1)};
      const std::vector<BlockId> dst = {static_cast<BlockId>(100 + i * 2),
                                        static_cast<BlockId>(101 + i * 2)};
      if (i % 2 == 0) {
        pd.consumer.expect(id, "prefill", pd.decode.region, dst, pd.ready.cb());
        pd.producer.publish(id, "decode", pd.prefill.region, src).wait();
      } else {
        auto h = pd.producer.publish(id, "decode", pd.prefill.region, src);
        pd.consumer.expect(id, "prefill", pd.decode.region, dst, pd.ready.cb());
        h.wait();
      }
    });
  }
  for (auto& t : threads) t.join();
  for (int i = 0; i < kRequests; ++i) {
    const std::string id = "r" + std::to_string(i);
    ASSERT_TRUE(pd.ready.wait(id)) << id;
    EXPECT_TRUE(pd.ready.get(id).first.ok()) << pd.ready.get(id).first.to_string();
    EXPECT_TRUE(pd.landed({static_cast<BlockId>(i * 2), static_cast<BlockId>(i * 2 + 1)},
                          {static_cast<BlockId>(100 + i * 2), static_cast<BlockId>(101 + i * 2)}));
  }
  EXPECT_EQ(pd.producer.pending_prepares(), 0u);
  EXPECT_EQ(pd.producer.pending_publishes(), 0u);
  EXPECT_EQ(pd.consumer.pending_expects(), 0u);
}

}  // namespace
