# Design

## Goals

1. Move KV blocks for a request from the node that ran prefill to the node
   that will run decode, with the least possible copying and a clear
   completion signal.
2. Keep the model-facing API in terms of **block ids and layer ranges**, not
   bytes, so a scheduler can hand over its block table directly.
3. Make the data plane pluggable. TCP works everywhere; RDMA, NVLink, or
   shared memory should slot in behind the same `Transport` interface.
4. Fail loudly and locally. Every operation returns a `Status` or a handle
   that completes with one; nothing hangs silently.

## Components

### KvLayout (`layout.hpp`)

Describes a paged KV cache living in one contiguous region:

```
token_kv_bytes = num_kv_heads * head_dim * dtype_bytes      (K or V, one token, one layer)
slice_bytes    = token_kv_bytes * block_size                 (K or V, one block, one layer)
block_bytes    = 2 * num_layers * slice_bytes                (all K and V of one block)
total_bytes    = block_bytes * num_blocks
```

Two arrangements are supported:

- `kLayerMajor` (vLLM style): `for layer: K[num_blocks][…] then V[num_blocks][…]`.
  One block is `2 * num_layers` separate slices.
- `kBlockMajor`: `for block: [layer][K|V][…]`. One block is a single run;
  a layer range of a block is also a single run.

`build_block_descriptors` pairs `src_blocks[i]` with `dst_blocks[i]`, emits
one descriptor per `(layer, K|V)` slice, then `coalesce_descriptors` sorts by
source offset and merges descriptors that are contiguous on **both** sides.
Adjacent blocks in a layer-major cache collapse to `2 * layers` descriptors
regardless of block count; block-major to block-major collapses to one.

The `fingerprint()` hashes every geometry field plus arrangement and
`num_blocks`; peers exchange it in `PREPARE` so a misconfigured pair fails
before any bytes move.

### Memory (`memory.hpp`)

A `MemoryRegion` is `(id, base, length, kind, device)`. Peers never see raw
pointers; every remote address is `(RegionId, offset)` and is bounds-checked
by the side that owns the memory. `MemoryRegistry` is a thread-safe table.

`MemoryKind` distinguishes pageable host, pinned host, and device memory so a
future transport can pick DMA paths; the TCP transport rejects `kDevice`.

### Transport (`transport.hpp`)

```
write(dst, descs)  -> TransferHandle   // local src regions -> remote dst regions
read(src, descs)   -> TransferHandle   // remote src regions -> local dst regions
notify(dst, bytes) -> Status           // small ordered control message
```

Completion semantics: a `write` completes when the remote has landed all bytes
(or reports which chunk failed and how many bytes did land). A `read`
completes when the local regions hold the data. `notify` is ordered after
earlier `write`s to the same peer from the same thread; the handoff protocol
relies on this so `READY` never overtakes its data.

#### TcpTransport

- One listening socket; one connection per peer pair. The connecting side
  sends `HELLO`; the accepting side registers the peer and replies `HELLO`.
  `connect()` blocks until the reply arrives (bounded by `connect_timeout`),
  so after it returns **either** side can transfer immediately.
- Per connection: a **reader thread** that parses frames and scatters payload
  straight into registered regions, and a **sender thread** draining a queue
  of `OutFrame`s with `sendmsg` (iovecs referencing user memory; no payload
  copy). Frames from different callers are serialized per connection.
- A **reaper thread** joins dead connections and expires transfers whose
  deadline passed (`TransferOptions::timeout`), completing them with
  `kTimeout`. A timed-out write may still land on the remote; the deadline
  only bounds how long the caller waits.
- Malformed input (bad magic, inconsistent lengths, `NOTIFY` before `HELLO`)
  closes the connection; the stream cannot be trusted afterwards. Semantic
  errors (unknown region, out of bounds) are answered in the ack and the
  connection stays healthy — the reader discards the offending bytes to stay
  in sync.
- On connection loss every pending transfer on that connection completes
  with `kDisconnected` and the disconnect handler fires once.

#### LocalTransport

memcpy between transports attached to the same `LocalFabric`. A worker thread
executes jobs so callbacks behave like the TCP case (asynchronously);
`synchronous = true` completes inline for deterministic unit tests. Useful for
co-located prefill/decode workers and for tests.

### TransferEngine (`engine.hpp`)

Owns the local `KvLayout`, a `MemoryRegistry`, per-peer layouts, and metrics.
`push_blocks`/`pull_blocks` validate block ids against the relevant layout,
build descriptors, apply the default timeout, and wrap the handle so metrics
are recorded on completion. `push_raw`/`pull_raw` bypass the layout for
arbitrary regions (e.g. per-layer tensors registered separately).

### KvHandoff (`handoff.hpp`)

```
consumer.expect(req, producer, region, blocks, cb, range)
     └─ PREPARE{req, region, range, fingerprint, blocks} ──▶ producer stores Prepare
producer.publish(req, consumer, region, blocks, range)
     ├─ if Prepare present: validate, push_blocks(range), then READY{req, range, status, bytes}
     └─ else: queue PendingPublish until PREPARE arrives or prepare_wait_timeout
consumer on READY: mark layers; when all layers of the expected range are done → cb(Ok)
                   on a non-OK READY → cb(error) immediately
abort(req) from either side → ABORT{req, reason}; both sides drop state,
                   pending handles complete with kCancelled
disconnect → expects from / publishes to that peer complete with kDisconnected
```

Handoff messages ride on `notify` with a 2-byte magic prefix; anything else
is forwarded to the handler set via `KvHandoff::set_notify_handler`, so
applications can share the channel.

## Threading model

| Thread | Runs |
|---|---|
| caller threads | `write/read/notify/publish/expect` — validate, build frames, enqueue; never block on the network |
| TCP reader (per connection) | frame parsing, scatter into regions, `notify` handlers, ack completion → **transfer callbacks** |
| TCP sender (per connection) | `sendmsg` of queued frames |
| TCP reaper | join dead connections, deadline expiry → transfer callbacks (`kTimeout`) |
| handoff reaper | `prepare_wait_timeout` / `ready_wait_timeout` expiry → callbacks |
| local worker | memcpy jobs and notify delivery |

Rules that follow from this:

- Callbacks and handlers run on transport threads. Keep them short. Calling
  back into the transport/engine/handoff from a handler is supported (the
  handoff does exactly that to answer `PREPARE` with a push).
- Do not call `stop()` or destroy a transport from inside one of its handlers
  (it would join its own thread).
- Setting a handler to `nullptr` blocks until in-flight invocations of the
  old handler have returned (`shared_mutex`), so `~KvHandoff` is safe while
  transports are still running.
- Memory referenced by an in-flight `write` must stay valid and unchanged
  until the handle completes; the sender reads it directly.

## Ownership and lifetimes

- `TransferHandle` is a shared reference to completion state; it may outlive
  the transport. Late `cancel()` calls after the transport is gone are no-ops
  (liveness token).
- `TransferEngine` holds a `shared_ptr<Transport>` and stops it on
  destruction. `KvHandoff` holds a reference to its engine and must be
  destroyed **before** the engine.
- Destroying a `KvHandoff` completes queued publish handles with `kCancelled`
  and drops pending expects without invoking their callbacks (the callback's
  captures are likely being destroyed too).

## Error model

`Status` codes and where they come from:

| code | meaning |
|---|---|
| `kInvalidArgument` | bad layout, block/range mismatch, empty descriptors, layout fingerprint mismatch |
| `kNotFound` | region not registered (local or remote), peer not connected, unknown request id |
| `kOutOfRange` | block id ≥ `num_blocks`, byte range exceeds region (checked by the owning side) |
| `kUnavailable` | transport not started / stopped, connect failures |
| `kTimeout` | transfer deadline, handshake, `prepare_wait_timeout`, `ready_wait_timeout` |
| `kCancelled` | `cancel()`, `abort()`, handoff destroyed |
| `kDisconnected` | connection closed with work outstanding |
| `kProtocolError` | malformed frame (the connection is closed) |
| `kUnimplemented` | device memory on a transport that cannot address it |

Partial results are reported: a write whose second chunk is out of range
completes with `kOutOfRange` and `bytes` equal to the first chunk.

## Extension points

**Adding a transport.** Implement `Transport`. The engine and handoff are
transport-agnostic. An RDMA implementation would: pin regions in
`register_memory` (and return `rkey`s to peers via a `HELLO`-style exchange),
map `write` to `RDMA_WRITE` with a completion-queue poller completing
`TransferState`s, and carry `notify` on a small send/recv queue pair. The
`(RegionId, offset)` addressing already matches the RDMA model.

**Device memory.** `MemoryKind::kDevice` plus `device` index are carried
through registration. A GPU-aware transport can use GPUDirect RDMA or a
staging copy; the TCP transport could add a bounce buffer for `kDevice` if
needed.

**Multiple connections per peer.** `TcpTransport` currently maps one
connection per peer. Striping large transfers across N sockets is a local
change in `write()` plus per-chunk connection selection; acks already carry
the transfer id so completion accounting is unchanged.

**Heterogeneous TP.** Re-sharding KV heads means each destination block slice
is a strided subset of the source. `XferDesc` can express it (many small
descriptors) but `build_block_descriptors` assumes equal `num_kv_heads`. A
head-range-aware descriptor builder is the natural extension.

**Layer-major caches split across allocations.** Register each layer's tensor
as its own region and use `push_raw` with descriptors from
`KvLayout::offset` computed per layer (or add a multi-region layout variant).

## Testing strategy

- Pure unit tests for layout math, coalescing, codec, registry, and the
  transfer state machine.
- Transport tests over real loopback sockets: round trips in both
  directions, multi-chunk scatter, a 48 MiB transfer (partial `sendmsg`
  handling), 400 concurrent transfers from 8 threads in both directions,
  remote error relay with the connection staying usable, notify ordering
  after writes, disconnect propagation, deadline expiry, cancel, garbage on
  the wire, and the handshake timeout.
- Engine tests including heterogeneous arrangements via `set_peer_layout`.
- Handoff tests for both call orders, layer-wise pipelining, every
  validation path, remote failure relay, abort from either side, both
  timeouts, fingerprint mismatch, disconnect, pass-through notifies, and 40
  concurrent requests with mixed orderings.
- The whole suite runs clean under ASan, UBSan, and TSan.
