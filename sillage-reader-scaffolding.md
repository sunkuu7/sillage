# Sillage Reader — Scaffolding

> **Original planning document.** The phase numbering here is referenced by the Sillage grant proposal. Some items shipped differently or have been superseded since it was written — the code is authoritative where the two disagree.

The reader is a Rust service that hydrates compressed chunks from R2 onto local NVMe, parses customer `SubscribeRequest` filters into bitmap operations, and replays matching messages at original wall-clock pacing over the Yellowstone gRPC protocol.

Source of truth: R2. The reader is a cache. If the reader dies, a replacement box rebuilds itself from R2 in a few hours.

-----

## Phase 1 — Project skeleton & dependencies

- Cargo workspace structure (`sillage-reader` binary, `sillage-common` library shared with the writer)
- Core dependencies: `tokio`, `tonic`, `prost`, `yellowstone-grpc-proto`, `roaring`, `zstd`, `aws-sdk-s3` or `reqwest` for R2, `tracing` for logs
- Config loading from TOML + env vars (R2 credentials, bucket name, NVMe paths, listen address)
- Graceful shutdown handling (SIGTERM, in-flight customer drain)
- Basic structured logging via `tracing-subscriber`

-----

## Phase 2 — R2 sync loop

- Periodic poll of R2 bucket for new chunks (every 30-60s)
- Diff against local NVMe inventory → list of chunks to fetch
- Parallel downloads with concurrency cap
- Atomic move from `.partial` to final path on completion
- Inventory persistence (small SQLite or JSON file) tracking what’s local, slot ranges covered
- Lifecycle: delete chunks older than 24h to maintain the rolling window *(superseded: shipped as a configurable `local_retention_hours`, default 24 — a local cache eviction policy, not a fixed replay window)*
- Retry + backoff on R2 errors

-----

## Phase 3 — Local storage layer

- Directory layout: `/data/{tx|acct|block}/chunk-{start}-{end}.{zst,idx.*,meta.json}`
- Chunk metadata cache (in-memory map: slot range → file paths + sizes + summary)
- Block-level decoder: open `.zst` file, locate compressed block by offset, decompress just that block
- Page-cache friendly access (read sequential ranges, let the OS cache hot blocks)
- LRU cache for decoded blocks (bounded RAM, ~512MB-1GB)

-----

## Phase 4 — Index layer

- Roaring bitmap deserialization per chunk per stream type
- In-memory index cache keyed by chunk ID (most recent N chunks always loaded)
- Lazy load + LRU eviction for older chunks’ indexes
- Helper API: `filter_to_bitmap(chunk, SubscribeRequest) -> RoaringBitmap` returning surviving message offsets

-----

## Phase 5 — gRPC server

- Implement `Geyser` service from `yellowstone-grpc-proto`
- Accept `SubscribeRequest`, validate, parse into internal filter representation
- Reject unsupported features cleanly (return error, don’t silently degrade)
- Auth: simple bearer token check (x-token header) → maps to customer ID
- One tokio task per connected customer
- Backpressure via bounded channels to customer

-----

## Phase 6 — Replay engine (per customer)

- Resolve customer’s slot range (from explicit `from_slot` or default to “24h ago”) *(superseded: the default is the oldest slot available locally)*
- Walk chunks in slot order across the requested streams (tx, acct, block)
- For each chunk:
  - Apply filter → bitmap of message offsets
  - Fetch & decompress only the blocks containing those offsets
  - Decode messages, apply secondary filters (memcmp, datasize, signature)
- Pace output at wall-clock speed using stamped timestamps
- Merge streams by timestamp if customer subscribed to multiple types
- Handle disconnects, cleanup task on customer drop

-----

## Phase 7 — Wall-clock pacing

- Per-customer pacer task using `tokio::time::sleep_until` against captured timestamps
- Anchor: first message's `recv_ns` = customer's "T=0", subsequent messages offset from there
- Configurable speed multiplier (always 1x for M1, future-proofed) *(superseded: shipped in Phase 7 as the opt-in `x-replay-speed` header; unset still means 1×)*
- Backpressure handling: if customer is slow, hold messages; if they fall too far behind, drop the connection
- Track lag metric per customer for ops visibility

-----

## Phase 8 — Observability

- Per-customer metrics: messages sent, bytes sent, filter selectivity, lag
- Per-chunk metrics: cache hit rate, decompress time, filter resolution time
- R2 sync metrics: chunks fetched, fetch latency, bytes downloaded
- Prometheus exporter on a separate port
- Structured logs for every customer connect/disconnect, every chunk fetch

-----

## Phase 9 — Hardening

- Resource limits per customer (max concurrent decompressions, max RAM)
- Connection rate limiting per auth token
- Memory pressure handling: drop coldest cached chunks when above threshold
- Crash recovery: on startup, scan local NVMe, rebuild inventory, resume serving
- Integration tests against a local R2-compatible store (MinIO in CI)

-----

## Phase 10 — Operational tooling

- CLI subcommand to query reader state (`sillage-reader status`, `sillage-reader chunks`)
- CLI subcommand to force-resync a slot range from R2
- CLI subcommand to test a `SubscribeRequest` against the local chunks without serving
- Systemd unit file + deployment notes
- Backup/restore notes (writer is source of truth, so reader backup = re-sync from R2)

-----

## Out of M1 scope (deferred)

- Multi-region readers
- Per-stream box separation (Phase 2+, after measurement)
- ~~Replay speed multipliers other than 1x~~ — shipped in Phase 7 as `x-replay-speed`
- Real-time tail (live + replay combined)
- Customer self-serve credential management UI
- Per-stream rate limiting / quota enforcement

-----

## Open questions to revisit before starting Phase 6

- Filter representation: parse `SubscribeRequest` into a typed AST, or keep as-is and interpret directly?
- Stream merging: tightly synced across tx/acct/block, or allow each to drift slightly within tolerances?
- Customer ID model: just bearer token → ID, or token → quota + ID?
- Disconnect-resume semantics: does a customer reconnect “from where they left off” or restart from a specified slot?