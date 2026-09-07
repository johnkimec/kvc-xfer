// Minimal prefill/decode disaggregation demo on a single machine.
//
// Two engines run on loopback TCP. The decode node allocates blocks for a
// request and announces them; the prefill node "computes" the KV cache, pushes
// it layer-group by layer-group, and the decode node is told when everything
// has landed.

#include <cstdio>
#include <future>

#include "kvc/kvc.hpp"

using namespace kvc;

int main() {
  set_log_level(LogLevel::kInfo);

  KvLayout layout;
  layout.num_layers = 8;
  layout.num_kv_heads = 4;
  layout.head_dim = 64;
  layout.block_size = 16;
  layout.num_blocks = 128;
  layout.dtype = DType::kBF16;
  layout.arrangement = KvArrangement::kLayerMajor;

  TcpTransportConfig pc;
  pc.self = "prefill-0";
  pc.bind_host = "127.0.0.1";
  TcpTransportConfig dc = pc;
  dc.self = "decode-0";
  auto prefill_tx = std::make_shared<TcpTransport>(pc);
  auto decode_tx = std::make_shared<TcpTransport>(dc);

  TransferEngine prefill(EngineConfig{"prefill-0", layout, {}}, prefill_tx);
  TransferEngine decode(EngineConfig{"decode-0", layout, {}}, decode_tx);
  if (!prefill.start().ok() || !decode.start().ok()) return 1;

  // In a real server these would be the GPU KV-cache allocations; here they
  // are host buffers.
  std::vector<uint8_t> prefill_kv(layout.total_bytes()), decode_kv(layout.total_bytes());
  const RegionId prefill_region = *prefill.register_kv_cache(prefill_kv.data(), prefill_kv.size());
  const RegionId decode_region = *decode.register_kv_cache(decode_kv.data(), decode_kv.size());

  KvHandoff producer(prefill);
  KvHandoff consumer(decode);
  if (auto s = prefill.connect("decode-0", decode_tx->local_endpoint()); !s.ok()) {
    std::fprintf(stderr, "connect: %s\n", s.to_string().c_str());
    return 1;
  }

  // --- decode side: the scheduler admits request "req-42" with 6 blocks ---
  const std::string request_id = "req-42";
  const std::vector<BlockId> decode_blocks = {17, 18, 19, 20, 21, 22};
  std::promise<Status> ready;
  auto ready_future = ready.get_future();
  Status st = consumer.expect(request_id, "prefill-0", decode_region, decode_blocks,
                              [&](const std::string& id, const Status& s, uint64_t bytes) {
                                std::printf("[decode] %s ready: %s (%llu bytes)\n", id.c_str(),
                                            s.to_string().c_str(),
                                            static_cast<unsigned long long>(bytes));
                                ready.set_value(s);
                              });
  if (!st.ok()) {
    std::fprintf(stderr, "expect: %s\n", st.to_string().c_str());
    return 1;
  }

  // --- prefill side: run prefill, publishing as layer groups finish ---
  const std::vector<BlockId> prefill_blocks = {0, 1, 2, 3, 4, 5};
  for (uint32_t group = 0; group < layout.num_layers; group += 4) {
    const LayerRange range = LayerRange::of(group, group + 4);
    // "Compute" the KV for these layers: stamp a recognisable pattern.
    for (BlockId b : prefill_blocks) {
      for (const Segment& seg : layout.block_segments(b, range)) {
        for (uint64_t i = 0; i < seg.length; ++i) {
          prefill_kv[seg.offset + i] = static_cast<uint8_t>(b * 31 + group + (i & 7));
        }
      }
    }
    auto handle = producer.publish(request_id, "decode-0", prefill_region, prefill_blocks, range);
    auto result = handle.wait();
    std::printf("[prefill] layers [%u,%u) -> decode-0: %s, %llu bytes in %.3f ms\n", range.begin,
                range.end, result.status.to_string().c_str(),
                static_cast<unsigned long long>(result.bytes),
                std::chrono::duration<double, std::milli>(result.elapsed).count());
    if (!result.ok()) return 1;
  }

  if (ready_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
    std::fprintf(stderr, "decode never became ready\n");
    return 1;
  }
  if (!ready_future.get().ok()) return 1;

  // Verify the decode node sees exactly what prefill produced.
  bool ok = true;
  for (size_t i = 0; i < prefill_blocks.size(); ++i) {
    for (uint32_t layer = 0; layer < layout.num_layers; ++layer) {
      for (KvKind kind : {KvKind::kKey, KvKind::kValue}) {
        const uint64_t po = layout.offset(layer, kind, prefill_blocks[i]);
        const uint64_t dco = layout.offset(layer, kind, decode_blocks[i]);
        for (uint64_t j = 0; j < layout.slice_bytes(); ++j) {
          if (prefill_kv[po + j] != decode_kv[dco + j]) ok = false;
        }
      }
    }
  }
  std::printf("[demo] KV cache verified: %s\n", ok ? "match" : "MISMATCH");
  std::printf("[demo] prefill metrics: %s\n", prefill.metrics().snapshot().to_string().c_str());
  return ok ? 0 : 1;
}
