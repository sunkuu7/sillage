# Sillage

[![CI](https://github.com/sunkuu7/sillage/actions/workflows/ci.yml/badge.svg)](https://github.com/sunkuu7/sillage/actions/workflows/ci.yml)

Record Solana's Geyser stream. Replay it later through the same Yellowstone gRPC
protocol, paced to the original wall clock, with unmodified clients.

```
validator ──Geyser──▶ sillage-writer ──▶ object storage ──▶ sillage-reader ──gRPC──▶ your client
                      chunk + index                          hydrate + replay
```

## Why

If you need to test, debug, or benchmark a Geyser consumer against real mainnet
traffic, today you pick from three bad options:

- **Point it at live mainnet.** Not reproducible. The traffic differs every run,
  and you cannot re-run the incident you are trying to debug.
- **Run an archival validator.** Terabytes of storage and real operational load.
- **Buy a commercial stream.** Recurring cost, and still no replay of a *specific
  past window*.

Confirmed-ledger archives solve a different problem. They store finalized blocks
and transactions — from which the intermediate account-update notifications a
Geyser consumer actually receives, and their arrival order and timing, cannot be
reconstructed. Sillage records the stream as it was delivered.

## What it does

**`sillage-writer`** subscribes to a validator's Geyser plugin, batches updates
into slot-range chunks, compresses them with zstd, builds roaring-bitmap indexes,
and uploads to any S3-compatible object store.

**`sillage-reader`** hydrates chunks onto local NVMe, serves the Yellowstone gRPC
`Subscribe` API, resolves subscription filters through the bitmap indexes — so
excluded data is never decompressed — and paces emission to the original
inter-slot timing.

Object storage is the source of truth. Readers are disposable caches that rebuild
from it.

## Quickstart

```bash
cargo build --release
```

Point a reader at a bucket that already holds chunks:

```bash
export SILLAGE_R2__BUCKET=your-bucket
export SILLAGE_R2__ENDPOINT_URL=https://<account>.r2.cloudflarestorage.com
export SILLAGE_R2__ACCESS_KEY_ID=...
export SILLAGE_R2__SECRET_ACCESS_KEY=...
export SILLAGE_STORAGE__NVME_PATH=/var/lib/sillage
export SILLAGE_SERVER__LISTEN_ADDR=127.0.0.1:10000
export SILLAGE_READER__AUTH_TOKENS=your-token

./target/release/sillage-reader
```

Then subscribe with any Yellowstone client — see [`demo/yellowstone-client`](demo/yellowstone-client)
for a runnable example using the stock `@triton-one/yellowstone-grpc` package.
Nothing in it is sillage-specific.

Defaults live in [`config/default.toml`](config/default.toml); every field is
overridable by environment variable (`SILLAGE_<SECTION>__<FIELD>`), or point
`SILLAGE_CONFIG_PATH` at your own TOML.

## Replay speed

Speed is an opt-in extension carried in gRPC metadata as `x-replay-speed`, not in
`SubscribeRequest`. A client that never sets it gets `1` — original pacing.

`commitment` is accepted and ignored: archived chunks carry whatever commitment
the writer captured. Rejecting it would lock out every off-the-shelf client.

## On-disk format

Chunks are `.zst` payloads with a `.meta.json` sidecar and a `.idx` roaring-bitmap
index. The format is documented in [`docs/format.md`](docs/format.md) and is
stable at schema version 1 — you can write your own producer or reader against it.

## Self-hosting

Both binaries are single static-ish executables with no runtime dependencies
beyond a filesystem and an object store.

Run the reader behind a reverse proxy that terminates TLS. The reader speaks
prior-knowledge HTTP/2 (h2c) with no TLS of its own, so the proxy must be told to
use h2c upstream and to stream rather than buffer — buffering batches messages and
destroys the pacing. With Caddy:

```
replay.example.com {
	reverse_proxy 127.0.0.1:10000 {
		transport http {
			versions h2c 2
		}
		flush_interval -1
	}
}
```

Bind the reader to `127.0.0.1` when it sits behind a proxy. Left on `0.0.0.0`, the
plaintext port stays reachable and clients can bypass TLS, putting auth tokens on
the wire in the clear. The metrics listener (`/metrics`, `/health`) is separate and
should stay on loopback.

Account chunks decompress to hundreds of megabytes each. Size the box for the
streams you intend to serve, and note that per-client resource limits are not yet
implemented — see below.

## Status

Working and deployed, but young. Honest state:

- Reader: sync, storage, indexes, gRPC, replay engine, wall-clock pacing, and
  Prometheus observability are implemented and tested.
- Writer: ingest, chunking, indexing, upload, and crash recovery are implemented.
  Metrics instrumentation is not yet wired up.
- **Not implemented:** per-client resource limits. A client subscribing to account
  streams can drive memory hard. Do not expose a reader to untrusted callers yet.

323 passing tests, 2 ignored. `cargo test --workspace`.

## License

Apache-2.0. See [LICENSE](LICENSE).
