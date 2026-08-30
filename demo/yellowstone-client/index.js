// Streams transaction signatures from a sillage-reader replay endpoint using
// the stock Yellowstone gRPC client — the same library people point at a live
// validator. Nothing here is sillage-specific: if this works, any existing
// Yellowstone consumer works.
//
//   ENDPOINT=https://replay.sillage.sh X_TOKEN=... node index.js
//
// Env:
//   ENDPOINT   reader address                  (default http://127.0.0.1:10000)
//   X_TOKEN    auth token                      (required)
//   PROGRAM     filter txs touching this pubkey (default: none — every
//               non-vote transaction)
//   PRINT_EVERY print only every Nth signature  (default 1; raise it when
//               streaming unfiltered, which is thousands per second)
//   SPEED       replay speed multiplier         (default 1 = original pacing)
//   FROM_SLOT   start slot                      (default: oldest slot retained)

// The package is CommonJS. Under ESM the default import yields the module
// object, whose own `.default` is the Client class; the enums come through as
// named exports.
import yellowstone from "@triton-one/yellowstone-grpc";
import { CommitmentLevel } from "@triton-one/yellowstone-grpc";
import { InterceptingCall } from "@grpc/grpc-js";
import bs58 from "bs58";

const Client = yellowstone.default ?? yellowstone;

const ENDPOINT = process.env.ENDPOINT ?? "http://127.0.0.1:10000";
const X_TOKEN = process.env.X_TOKEN;
const PROGRAM = process.env.PROGRAM ?? "";
const PRINT_EVERY = Math.max(1, Number(process.env.PRINT_EVERY ?? "1"));
const SPEED = process.env.SPEED ?? "1";
const FROM_SLOT = process.env.FROM_SLOT;

if (!X_TOKEN) {
  console.error("X_TOKEN is required");
  process.exit(1);
}

// Replay speed is a sillage extension carried in gRPC metadata rather than in
// SubscribeRequest, so it goes on the channel via an interceptor. A stock
// client that never sets it simply gets 1x — original wall-clock pacing.
const speedInterceptor = (options, nextCall) =>
  new InterceptingCall(nextCall(options), {
    start(metadata, listener, next) {
      metadata.add("x-replay-speed", String(SPEED));
      next(metadata, listener);
    },
  });

const client = new Client(ENDPOINT, X_TOKEN, {
  "grpc.max_receive_message_length": 64 * 1024 * 1024,
  "grpc.primary_user_agent": "sillage-demo",
  interceptors: [speedInterceptor],
});

const request = {
  accounts: {},
  slots: {},
  transactions: {
    demo: {
      // Skip vote transactions: they are the large majority of mainnet traffic
      // and never what a demo wants to show.
      vote: false,
      failed: false,
      accountInclude: PROGRAM ? [PROGRAM] : [],
      accountExclude: [],
      accountRequired: [],
    },
  },
  transactionsStatus: {},
  blocks: {},
  blocksMeta: {},
  entry: {},
  accountsDataSlice: [],
  // Accepted and ignored by the reader — archived chunks were captured at
  // whatever commitment the writer subscribed with. Sent anyway because every
  // real client sets it.
  commitment: CommitmentLevel.CONFIRMED,
  ...(FROM_SLOT ? { fromSlot: FROM_SLOT } : {}),
};

console.log(`endpoint   ${ENDPOINT}`);
console.log(`filter     ${PROGRAM || "(all non-vote transactions)"}`);
console.log(`speed      ${SPEED}x`);
console.log(`from_slot  ${FROM_SLOT ?? "(oldest retained)"}`);
if (PRINT_EVERY > 1) console.log(`printing   every ${PRINT_EVERY}th signature`);
console.log("");

const started = Date.now();
let count = 0;
let firstSlot = null;
let lastSlot = null;

const stream = await client.subscribe();

stream.on("data", (update) => {
  if (update.transaction) {
    const info = update.transaction.transaction;
    const slot = Number(update.transaction.slot);
    const signature = bs58.encode(info.signature);

    if (firstSlot === null) firstSlot = slot;
    lastSlot = slot;
    count += 1;

    if (count % PRINT_EVERY === 0) {
      const elapsed = ((Date.now() - started) / 1000).toFixed(1);
      console.log(`[${elapsed.padStart(6)}s] slot ${slot}  ${signature}`);
    }
  } else if (update.ping) {
    // The reader answers pings; keeping the stream alive is the client's job.
    stream.write({
      accounts: {},
      slots: {},
      transactions: {},
      transactionsStatus: {},
      blocks: {},
      blocksMeta: {},
      entry: {},
      accountsDataSlice: [],
      ping: { id: update.ping.id },
    });
  }
});

// The reader ends the stream cleanly once the replay is exhausted and no new
// chunks arrive — that is the end-of-replay signal, not a failure.
stream.on("end", () => finish("stream closed by server"));
stream.on("close", () => finish("connection closed"));
stream.on("error", (err) => {
  console.error(`\nstream error: ${err.message}`);
  process.exit(1);
});

let finished = false;
function finish(reason) {
  if (finished) return;
  finished = true;
  const elapsed = (Date.now() - started) / 1000;
  const slots = firstSlot === null ? 0 : lastSlot - firstSlot + 1;
  console.log(`\n--- ${reason} ---`);
  console.log(`transactions : ${count}`);
  console.log(`slots        : ${firstSlot ?? "-"} .. ${lastSlot ?? "-"} (${slots})`);
  console.log(`elapsed      : ${elapsed.toFixed(1)}s`);
  if (count > 0) {
    console.log(`rate         : ${(count / elapsed).toFixed(1)} tx/s`);
  }
  process.exit(0);
}

process.on("SIGINT", () => finish("interrupted"));

await new Promise((resolve, reject) => {
  stream.write(request, (err) => (err ? reject(err) : resolve()));
});
