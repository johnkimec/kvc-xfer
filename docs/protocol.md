# Wire protocol

All integers are little-endian. The codec (`kvc/protocol.hpp`) is shared by
network transports; `LocalTransport` does not serialize.

## Frame

```
offset  size  field
0       4     magic            0x3143564B ("KVC1" as bytes)
4       2     version          1
6       2     type             see below
8       8     transfer_id      0 for HELLO / NOTIFY
16      8     payload_length   bytes following this header
24      …     payload
```

| type | name | direction | payload |
|---|---|---|---|
| 1 | `HELLO` | connector first, acceptor replies | `u32 flags, str peer_id` |
| 2 | `WRITE` | pusher → owner of dst regions | `u32 nchunks, nchunks × { ChunkHeader, data }` |
| 3 | `WRITE_ACK` | owner → pusher | `u32 status, u64 bytes_landed, str message` |
| 4 | `READ_REQ` | puller → owner of src regions | `u32 nchunks, nchunks × ReadDesc` |
| 5 | `READ_RESP` | owner → puller | `u32 status, u32 nchunks, u32 msg_len, msg, nchunks × { ChunkHeader, data }` |
| 6 | `NOTIFY` | either | opaque bytes |

```
ChunkHeader (20 bytes):  u32 region | u64 offset | u64 length
ReadDesc    (32 bytes):  u32 src_region | u64 src_offset | u32 dst_region | u64 dst_offset | u64 length
str:                     u32 length | bytes
```

`status` values are `kvc::StatusCode` numerics (0 = OK).

### Handshake

The connecting side sends `HELLO` immediately. The accepting side does not
speak until it has received `HELLO`, registered the peer under that name,
and then replies with its own `HELLO`. `connect()` on the connecting side
returns only after the reply, bounded by `connect_timeout`. If the reply names
a different peer than the caller asked for, the connection is closed.

### WRITE

The receiver reads `nchunks`, then for each chunk reads the header and, if
`region` is registered and `[offset, offset+length)` fits, `recv`s the data
directly into place; otherwise it discards `length` bytes and records the
first error. When all chunks are consumed it sends `WRITE_ACK` with the
aggregate status and the number of bytes that landed. Length mismatches
between `payload_length` and the chunk sum are protocol errors and close the
connection.

### READ

`READ_REQ` carries full descriptors. The owner validates every `src` range
first; on any error it replies `READ_RESP{status, nchunks=0, msg}`. Otherwise
it replies with `nchunks` chunks whose `ChunkHeader` names the **requester's**
`dst_region`/`dst_offset`, streaming data straight from its own regions. The
requester scatters into its regions exactly like a `WRITE` receiver.

### Limits

- `payload_length` of control frames (`HELLO`, `WRITE_ACK`, `NOTIFY`, the
  `READ_RESP` message) is capped at 16 MiB; `nchunks` at 2^20. Violations are
  protocol errors.
- Data payloads are unbounded; they stream through fixed-size reads.

## Handoff messages (carried in NOTIFY)

Prefix: `0xC4 0x48` then a `u8 kind`. Payloads not starting with the prefix
are delivered to the application's notify handler.

| kind | name | fields |
|---|---|---|
| 1 | `PREPARE` | `str request_id, u32 dst_region, u32 layer_begin, u32 layer_end, u64 layout_fingerprint, u32 nblocks, nblocks × u32 block` |
| 2 | `READY` | `str request_id, u32 layer_begin, u32 layer_end, u32 status, str message, u64 bytes` |
| 3 | `ABORT` | `str request_id, str reason` |

### State machine

Consumer (decode):

```
expect() ──send PREPARE──▶ WAITING(layers_remaining = range.count())
WAITING ──READY(ok, [b,e))──▶ mark layers; if layers_remaining == 0 → cb(Ok, bytes) → DONE
WAITING ──READY(err)──▶ cb(err) → DONE
WAITING ──ABORT / disconnect / ready_wait_timeout──▶ cb(Cancelled|Disconnected|Timeout) → DONE
```

Producer (prefill):

```
PREPARE arrives ──▶ Prepare stored (replaces an earlier one for the same request)
publish() with Prepare ──▶ validate (consumer, block count, range ⊆ prepared range,
                           fingerprint == peer_layout(consumer).fingerprint())
                       ──▶ push_blocks(range) ──▶ READY{status, bytes}; handle completes
                           on success mark layers; when all prepared layers done → drop Prepare
publish() without Prepare ──▶ PendingPublish until PREPARE (then as above),
                              ABORT, disconnect, or prepare_wait_timeout
```

A `READY` with a non-OK status leaves the producer's `Prepare` in place so
the producer can retry after fixing the cause.
