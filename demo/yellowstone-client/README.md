# Yellowstone client demo

Streams transaction signatures from a `sillage-reader` replay endpoint using
**the stock `@triton-one/yellowstone-grpc` client** — the same library people
point at a live validator. Nothing in `index.js` is sillage-specific, which is
the point: if this works, existing Yellowstone consumers work unmodified.

## Run

```bash
npm install
ENDPOINT=https://replay.sillage.sh X_TOKEN=<token> PRINT_EVERY=100 node index.js
```

## Options

| Env | Default | Notes |
|---|---|---|
| `ENDPOINT` | `http://127.0.0.1:10000` | Default targets a local reader over plaintext h2c. The public demo at `https://replay.sillage.sh` terminates TLS — use `https://` |
| `X_TOKEN` | *(required)* | Sent as the `x-token` header |
| `PROGRAM` | *(none)* | Restrict to txs touching a pubkey |
| `PRINT_EVERY` | `1` | Print every Nth signature |
| `SPEED` | `1` | `1` = original wall-clock pacing |
| `FROM_SLOT` | *(oldest retained)* | Below the retained floor returns `OUT_OF_RANGE` |

## Volume, measured

The recording holds 502 slots — 199 seconds of mainnet, 397ms per slot.

| | count | at 1x |
|---|---|---|
| all transactions | 545,688 | 2,741/s |
| **non-vote** (what this streams) | **163,795** | **823/s** |
| votes (excluded) | 381,893 | — |

Vote transactions are filtered out via `vote: false`, which the reader resolves
through the roaring-bitmap index — they are never decompressed, so excluding
them costs nothing.

823/s is still far past a readable terminal. **Set `PRINT_EVERY=100`** for about
eight lines a second; the summary counts every transaction regardless of what
gets printed.

## What to expect

At `SPEED=1` the replay is paced to the original chain wall-clock, so the run
takes about 3m20s and signatures arrive at the rate they originally did. That
pacing is the product claim; a fast run only proves the data is there.

When the replay is exhausted and no new chunks arrive, the reader **closes the
stream cleanly** after `follow_idle_timeout_secs`. That is the end-of-replay
signal, not an error — the script prints a summary and exits 0.

Do **not** subscribe to accounts against a small box. Account chunks decode to
~680MB each, and the demo host is sized for transactions and block metadata.

## Two things this demonstrates

`commitment` is set on every request here, and the client library sends it
unconditionally. The reader accepts and ignores it — archived chunks carry
whatever commitment the writer captured. Rejecting it, as an earlier version
did, locked out every off-the-shelf client.

`SPEED` rides in gRPC metadata (`x-replay-speed`) via an interceptor rather than
in `SubscribeRequest`, so it stays an opt-in extension. A client that knows
nothing about it gets 1x.
