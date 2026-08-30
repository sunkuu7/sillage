# Sillage on-disk format

Schema version **1**. Anything described here is stable: a change that breaks a
reader gets a version bump, not a silent redefinition.

A recording is a flat set of **chunks**. Each chunk is a slot range of one stream,
stored as three sibling objects sharing a base name:

```
<base>.zst         zstd-compressed payload
<base>.meta.json   metadata sidecar
<base>.idx         roaring-bitmap index
```

There are three streams: `tx`, `acct`, `block`. A chunk never mixes them.

## Payload — `.zst`

zstd-compressed. Decompressed, it is a flat sequence of length-prefixed frames:

```
[u32 LE length][payload bytes][u32 LE length][payload bytes]...
```

Each payload is an encoded Yellowstone `SubscribeUpdate` protobuf message, in the
order the writer received it from Geyser. There is no framing beyond the length
prefix and no padding. A truncated final frame is an error, not a tolerated tail.

## Metadata — `.meta.json`

Pretty-printed JSON. Field names and types are part of the format.

| field | type | meaning |
|---|---|---|
| `schema_version` | u32 | `1` |
| `stream` | string | `tx`, `acct`, or `block` |
| `start_slot` | u64 | first slot of the range, inclusive |
| `end_slot_exclusive` | u64 | end of the range, exclusive |
| `first_message_slot` | u64? | slot of the first message actually present |
| `last_message_slot` | u64? | slot of the last message actually present |
| `message_count` | u64 | frames in the payload |
| `uncompressed_bytes` | u64 | decompressed payload size |
| `compressed_bytes` | u64 | on-disk `.zst` size |
| `recv_ns_first` | u64? | wall-clock receipt of the first message, ns |
| `recv_ns_last` | u64? | wall-clock receipt of the last message, ns |
| `sealed_reason` | string | why the chunk was closed |
| `index_dimensions` | [string] | dimensions present in the `.idx` |

The slot-range fields describe the range the writer *intended* to cover; the
`first_message_slot` / `last_message_slot` pair describes what it actually
observed. They differ across gaps, and both are needed — the range determines
catalog placement, the observed pair determines what a reader can serve.

`recv_ns_first` and `recv_ns_last` are what make replay pacing possible. They are
the writer's receipt timestamps, not on-chain times.

## Index — `.idx` (SIDX)

Binary. Byte layout:

```
offset  size          contents
0       4             magic, ASCII "SIDX"
4       1             version, u8 = 1
5       4             header_len, u32 LE
9       header_len    header, MessagePack (named fields)
9+hl    to EOF        body: concatenated roaring bitmaps
```

The header deserializes to:

```
IdxHeader {
    stream:        String,
    start_slot:    u64,
    end_slot:      u64,
    message_count: u64,
    dimensions:    [ DimensionHeader ],
}

DimensionHeader {
    name:       String,
    value_type: Pubkey32 | Signature64 | U64 | Bool,
    entries:    [ DimEntryHeader ],
}

DimEntryHeader {
    value:  DimValue,
    offset: u64,   // into the body, relative to 9 + header_len
    length: u64,   // bytes of the serialized bitmap
}
```

Each entry's `(offset, length)` slice of the body is a roaring bitmap, in the
standard portable serialization, whose set bits are **frame ordinals** — indexes
into the payload's frame sequence, zero-based.

That indirection is the point of the format: a reader resolves a subscription
filter to a set of ordinals by intersecting bitmaps, and only then decompresses.
Data excluded by a filter is never decoded.

### Dimensions

| stream | dimensions |
|---|---|
| `tx` | `program_id` (Pubkey32), `account_key` (Pubkey32), `signature` (Signature64), `vote_flag` (Bool), `failed_flag` (Bool) |
| `acct` | `account_pubkey` (Pubkey32), `owner_program` (Pubkey32) |
| `block` | `slot` (U64), `parent_slot` (U64) |

Dimensions are per-stream and do not overlap. `DimValue` is encoded untagged:
`Bytes` for the two key types, `U64`, or `Bool`.

A dimension absent from `index_dimensions` was not built; a reader must fall back
to scanning rather than assume the empty set.

## Writing a compatible producer

Minimum to be readable by `sillage-reader`:

1. Emit frames as `[u32 LE length][SubscribeUpdate protobuf]`, zstd the result.
2. Write a `.meta.json` with `schema_version: 1` and honest slot bounds.
3. Write a `.idx` carrying the dimensions listed above for that stream, or omit
   the file entirely and accept that filters degrade to full scans.

The authoritative definitions live in `sillage-common/src/chunk.rs` and
`sillage-common/src/idx.rs`. Where this document and the code disagree, the code
is correct and this document is a bug.
