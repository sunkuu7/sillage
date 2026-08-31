# Sillage Writer — Scaffolding

> **Original planning document.** The phase numbering here is referenced by the Sillage grant proposal. Some items shipped differently or have been superseded since it was written — the code is authoritative where the two disagree.

The writer is a Rust service that subscribes to a Solana validator's Yellowstone gRPC plugin, batches incoming `SubscribeUpdate` messages into slot-range chunks, indexes them with roaring bitmaps, compresses with zstd, and ships the resulting `.zst` + `.idx` + `.meta.json` triplets to R2.

The writer is the source of truth: whatever it doesn't capture is gone. R2 is the durable archive; the reader is a downstream cache. The writer's ingest path (Geyser → NVMe) and its upload path (NVMe → R2) are **decoupled** — sealed chunks land on local NVMe non-stop, and an independent uploader task ships them to R2 on its own pace. R2 outages produce uploader lag, never message loss.

Deployment: a single writer process per box. Internally the process runs three **independent task lanes** — one per stream (`tx`, `acct`, `block`) — each with its own Geyser subscription, chunker, NVMe path, and uploader. Lanes never share mutable state on the hot path; bounded per-lane channels enforce isolation so one slow stream cannot starve the others. The validator pushes to the writer box over the network via its Geyser plugin.

-----

## Phase 1 — Project skeleton & dependencies

- Add `sillage-writer` binary crate to the workspace (third member alongside `sillage-reader`, `sillage-common`)
- Reuse `sillage-common` for `Settings`, `ShutdownSignal`, tracing init
- New config sections in `sillage-common`:
  - `[geyser]` — endpoint URL, x-token, commitment level, max message size
  - `[writer]` — `slots_per_chunk` (default 1000), `out_of_order_tolerance_slots`, `max_open_chunks`, per-lane channel capacities
  - `[uploader]` — `scan_interval_secs`, `max_concurrent_uploads`, retry policy
- Single-process / three-lane structure: `main` spawns three independent supervisor tasks (one per stream type) and awaits all. Each lane owns its own Geyser subscription, chunker state, NVMe sub-directory, and uploader task. Lanes communicate with `main` only via the shared `ShutdownSignal`.
- NVMe path scoped per stream: `{nvme_path}/chunks/{tx|acct|block}/`
- Graceful shutdown: each lane stops accepting new Geyser messages, seals its open chunk if non-empty, lets its uploader finish the current cycle, then exits. `main` returns once all three lanes have finished.
- Structured logging via `tracing-subscriber` with a `stream` field on every event so log streams per lane are easy to filter

-----

## Phase 2 — Geyser subscriber

- Each lane opens its own Tonic client connection to the validator's Yellowstone gRPC endpoint — **three concurrent subscriptions** total, one per stream type. Per-lane connections give wire-level isolation: a stuck consumer on one stream cannot backpressure the others.
- Authenticate via `x-token` header from `[geyser]` config (same token used by all three lanes)
- Each lane builds a `SubscribeRequest` filter scoped to its stream type (the `tx` lane subscribes to all transactions; `acct` to all account updates; `block` to all block-level updates)
- Stamp each `SubscribeUpdate` with `recv_ns` (monotonic + wall-clock) on arrival in the lane's receive task
- Forward stamped messages to that lane's chunker via a **bounded per-lane** `tokio::sync::mpsc` channel; channel capacity is config-tunable. Hitting the cap is a signal that the lane's chunker is falling behind — log loudly, drop oldest with explicit gap accounting.
- Connection lifecycle: open on lane startup, hold open; per-lane reconnect logic deferred to Phase 9
- Track and expose per-lane: connection state, messages/sec, last-received slot, channel depth

-----

## Phase 3 — Chunker

- Slot-range bucketing: chunk index = `slot / slots_per_chunk`; chunk holds slots `[N * slots_per_chunk, (N+1) * slots_per_chunk)`
- Maintain a small map of "open" chunks (typically 1–2, may grow to `max_open_chunks` under out-of-order arrival)
- On message arrival:
  - Compute target chunk index from `slot`
  - If chunk is open, append message to its in-memory buffer
  - If chunk is older than the oldest open chunk minus `out_of_order_tolerance_slots`, **drop** (log gap)
- Seal trigger: when a message arrives for a chunk index *N+K* (`K >= 1`) and chunk *N-K_close* is the oldest still-open, seal *N-K_close*. (Exact triggering policy TBD in Phase 3 design.)
- Sealing writes three files atomically (write to `.partial`, fsync, rename):
  - `{start_slot:012}-{end_slot:012}.zst`     — zstd-compressed message stream
  - `{start_slot:012}-{end_slot:012}.idx`     — roaring bitmap indexes (Phase 4 defines schema)
  - `{start_slot:012}-{end_slot:012}.meta.json` — chunk metadata
- Local layout matches the R2 layout exactly: `{nvme_path}/chunks/{stream}/{start_slot:012}-{end_slot:012}.{zst,idx,meta.json}`

-----

## Phase 4 — Index construction

- For each sealed chunk, build one or more roaring bitmaps mapping filter values → message offsets within the chunk
- **`tx` stream dimensions:** program_id, account_key, signature, vote_flag, failed_flag
- **`acct` stream dimensions:** account_pubkey, owner_program
- **`block` stream dimensions:** slot, parent_slot (mostly identity; index for join compatibility)
- Serialization format for `.idx`: single file with named sections (e.g., msgpack-encoded header listing dimension name → byte offset → roaring bitmap)
- `.meta.json` schema:
  ```
  {
    "stream": "tx",
    "start_slot": 100000,
    "end_slot": 101000,
    "message_count": 84321,
    "byte_count": 12745112,
    "recv_ns_first": 1715900000000000000,
    "recv_ns_last":  1715900400000000000,
    "index_dimensions": ["program_id", "account_key", "signature", "vote_flag", "failed_flag"]
  }
  ```

-----

## Phase 5 — Uploader task

- Independent tokio task; **never** blocks the chunker or Geyser subscriber
- Periodic NVMe scan (every `scan_interval_secs`):
  - List sealed chunks (`.zst` exists) lacking a `.uploaded` marker
  - Sort by `start_slot` ascending (upload oldest first to minimize reader-side staleness)
- For each pending chunk, with bounded concurrency (`max_concurrent_uploads`):
  - PUT `.zst`, `.idx`, `.meta.json` to R2 at `chunks/{stream}/{start_slot:012}-{end_slot:012}.*`
  - On all three uploads succeeding, atomically create a zero-byte `.uploaded` marker locally
  - On any failure, log + leave the chunk for the next scan (transient → eventually consistent)
- Retry policy: 3 attempts per file with exponential backoff (1s, 2s, 4s); persistent failures log loudly + advance scan
- Shares the `ShutdownSignal` — exits cleanly between chunks, never mid-upload

-----

## Phase 6 — Crash recovery

- On startup, before subscribing to Geyser:
  - Sweep and delete any `*.partial` files under `{nvme_path}/chunks/{stream}/` (incomplete seals are unrecoverable)
  - Scan for sealed-but-not-marked-`.uploaded` chunks and queue them for the uploader
- Determine Geyser resume point:
  - If sealed chunks exist, request `commitment = processed` from `(latest_sealed_chunk.end_slot + 1)`
  - If no chunks exist, request from "tip"
- Log a **gap report** if there's a discontinuity between the resume point and the validator's earliest available slot (gap = data permanently lost; alerting concern)
- Chunker state is intentionally non-persistent: any in-flight buffers are discarded on crash; we accept a few seconds of message loss at the chunk boundary as the trade for not having a write-ahead log inside the writer

-----

## Phase 7 — Local lifecycle

- Periodic eviction task (default every 5 min):
  - Delete chunks (`.zst`, `.idx`, `.meta.json`, `.uploaded`) where `.uploaded` exists and mtime is older than `local_retention_hours` (default ~1h — once R2 confirms, NVMe is just scratch space)
- NVMe watermark monitoring:
  - Warn at 80% used
  - Alert at 95% used
  - Hard limit at 98%: refuse to accept new Geyser messages (Geyser channel `try_send` returns full, message dropped, gap logged) — failsafe only; should never trigger in steady state
- Optional: also evict the oldest **unuploaded** chunks if NVMe fills (last-resort data loss; alerting required); default policy is "alert and stop ingesting" rather than silently lose data

-----

## Phase 8 — Observability

- Per-stream metrics:
  - `geyser_messages_received_total` (counter)
  - `geyser_message_processing_seconds` (histogram)
  - `chunker_open_chunks` (gauge)
  - `chunker_chunks_sealed_total` (counter)
  - `chunker_seal_duration_seconds` (histogram)
  - `uploader_pending_chunks` (gauge)
  - `uploader_chunks_uploaded_total` (counter)
  - `uploader_upload_duration_seconds` (histogram)
  - `nvme_bytes_used` (gauge), `nvme_bytes_available` (gauge)
  - `gap_detected_total` (counter, with labels for cause: crash_recovery, nvme_full, out_of_order_drop)
- Prometheus exporter on a separate port (config: `[metrics] listen_addr`)
- Structured logs for every chunk seal, every upload, every gap

-----

## Phase 9 — Hardening

- Geyser connection: reconnect with exponential backoff (1s → 30s), preserve resume-from-slot across reconnects
- R2 client: retry + backoff layered on top of `aws-sdk-s3`'s built-in retries
- Resource limits: bounded channels everywhere; explicit caps on open chunks, in-flight uploads, in-memory message buffers
- Integration tests:
  - Mock Geyser plugin (small tonic server emitting deterministic `SubscribeUpdate` stream)
  - MinIO for the R2 side
  - End-to-end test: emit N known messages, assert that exactly the expected chunks land in MinIO with the expected layout, message counts, and bitmap contents
- Soak test: 24h continuous run against a real testnet validator with R2 outages injected

-----

## Phase 10 — Operational tooling

- CLI subcommands:
  - `sillage-writer status` — connection state, current chunker state, uploader queue depth
  - `sillage-writer chunks` — list local sealed chunks
  - `sillage-writer pending` — list local un-uploaded chunks
  - `sillage-writer validate-chunk <path>` — decompress, re-derive indexes, compare against stored `.idx`
  - `sillage-writer drop-partials` — manual cleanup of `*.partial` files
- Single systemd unit (`sillage-writer.service`) — one process per box covers all three streams via internal task lanes
- `journalctl -u sillage-writer | jq 'select(.stream == "tx")'` for per-stream log filtering (the `stream` tracing field set in Phase 1 makes this trivial)
- Deploy notes: NVMe sizing (rule of thumb: `peak_msgs_per_sec * avg_msg_bytes * retention_hours * 3600 * safety_factor`), network bandwidth to validator, R2 egress + storage cost projection

-----

## Out of M1 scope (deferred)

- Multi-validator ingestion (multiple Geyser sources per stream, redundancy)
- Snapshot-based bootstrap (rebuilding history from a validator snapshot)
- Cross-stream slot alignment guarantees (tx and acct chunks landing in lockstep)
- De-duplication across writer restarts (two writer processes briefly overlapping)
- Writer-side filtering for cost reduction (uploading only "interesting" messages)
- Live-tail mode (writer fanning out to subscribers in addition to archiving)

-----

## Open questions to revisit before starting Phase 4

- **Bitmap dimensions per stream**: is the list above complete, or are there filter axes we'll wish we'd indexed? Schema is hard to evolve once chunks are archived.
- **`.idx` file format**: single file with named sections (one open, atomic) vs one file per dimension (more handles, easier debugging, simpler partial reads). The reader's Phase 4 will care about this.
- **Out-of-order tolerance window**: how many slots back are we willing to accept late arrivals before dropping? Validators typically don't reorder much under `processed` commitment, but reorgs happen.
- **Cross-stream consistency**: do `tx` and `acct` chunks for the same slot range need to land in R2 atomically (reader sees a consistent cut), or is independent visibility acceptable?
- **Chunk-boundary policy**: exact rule for when to seal an open chunk. Options: "first message past the end_slot arrives," "all open chunks older than X by wall-clock time," or a watermark from Geyser. Affects how messages are buffered when slot ordering is imperfect.
