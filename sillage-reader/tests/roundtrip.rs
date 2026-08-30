//! Format round trip: a producer written only from `docs/format.md`, consumed
//! through the reader's public API.
//!
//! Every other test in this workspace lives inside `src/` and reaches into
//! private internals. This one deliberately does not. It links against
//! `sillage-reader` the way an external consumer would, and it writes chunks
//! using nothing but the public `sillage-common` types and the byte layout
//! described in `docs/format.md` — no writer code, no crate-private helpers.
//!
//! That makes it the executable form of a claim the docs make: that a third
//! party can implement a compatible producer without reading our source. If
//! the spec is wrong, incomplete, or drifts from the reader, this fails.
//!
//! Scope limit worth stating plainly: `sillage-writer` is a binary-only crate,
//! so its chunker cannot be reached from here. This is a *spec* round trip, not
//! a writer/reader one. It proves the documented format is implementable and
//! that the reader honours it; it cannot catch the real writer drifting from
//! the same spec. Closing that gap needs a lib target on the writer.

use std::collections::BTreeMap;
use std::path::Path;

use prost::Message as _;
use roaring::RoaringBitmap;
use sillage_common::chunk::{write_len_prefixed, ChunkMeta, SCHEMA_VERSION};
use sillage_common::idx::{
    DimEntryHeader, DimValue, DimValueType, DimensionHeader, IdxHeader, DIM_ACCOUNT_KEY,
    DIM_FAILED_FLAG, DIM_PROGRAM_ID, DIM_SIGNATURE, DIM_VOTE_FLAG, IDX_MAGIC, IDX_VERSION,
};
use sillage_common::Stream;
use sillage_reader::filter::filter_tx;
use sillage_reader::index::{parse_chunk_index, IndexCache};
use sillage_reader::replay::{extract_recv_ns, plan_replay};
use sillage_reader::storage::{decode_chunk, ChunkCatalog};
use sillage_reader::subscription::parse_subscribe_request;
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterTransactions,
    SubscribeUpdate, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
};
use yellowstone_grpc_proto::solana::storage::confirmed_block::{
    CompiledInstruction, Message, Transaction, TransactionError, TransactionStatusMeta,
};

// ---------------------------------------------------------------------------
// A producer built from the spec alone.
// ---------------------------------------------------------------------------

/// Minimal `tx`-stream producer implementing `docs/format.md`.
///
/// Deliberately written against the document rather than against
/// `sillage-writer`, so that a divergence between the two shows up here.
struct SpecProducer {
    stream: Stream,
    start_slot: u64,
    end_slot_exclusive: u64,
    payload: Vec<u8>,
    ordinal: u32,
    dims: BTreeMap<&'static str, (DimValueType, BTreeMap<DimValue, RoaringBitmap>)>,
    first_message_slot: Option<u64>,
    last_message_slot: Option<u64>,
    recv_ns_first: Option<u64>,
    recv_ns_last: Option<u64>,
}

impl SpecProducer {
    fn new(start_slot: u64, end_slot_exclusive: u64) -> Self {
        // Dimension set for the `tx` stream, per the spec's per-stream table.
        let mut dims = BTreeMap::new();
        for (name, ty) in [
            (DIM_PROGRAM_ID, DimValueType::Pubkey32),
            (DIM_ACCOUNT_KEY, DimValueType::Pubkey32),
            (DIM_SIGNATURE, DimValueType::Signature64),
            (DIM_VOTE_FLAG, DimValueType::Bool),
            (DIM_FAILED_FLAG, DimValueType::Bool),
        ] {
            dims.insert(name, (ty, BTreeMap::new()));
        }
        Self {
            stream: Stream::Tx,
            start_slot,
            end_slot_exclusive,
            payload: Vec::new(),
            ordinal: 0,
            dims,
            first_message_slot: None,
            last_message_slot: None,
            recv_ns_first: None,
            recv_ns_last: None,
        }
    }

    fn mark(&mut self, dim: &'static str, value: DimValue) {
        let (_, map) = self.dims.get_mut(dim).expect("dimension registered");
        map.entry(value).or_default().insert(self.ordinal);
    }

    /// Append one message: frame it, then index it.
    fn push(&mut self, update: &SubscribeUpdate, slot: u64, recv_ns: u64) {
        write_len_prefixed(&mut self.payload, &update.encode_to_vec());

        if let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_ref() {
            if let Some(info) = tx.transaction.as_ref() {
                if info.signature.len() == 64 {
                    self.mark(DIM_SIGNATURE, DimValue::Bytes(info.signature.clone()));
                }
                if info.is_vote {
                    self.mark(DIM_VOTE_FLAG, DimValue::Bool(true));
                }
                if info.meta.as_ref().and_then(|m| m.err.as_ref()).is_some() {
                    self.mark(DIM_FAILED_FLAG, DimValue::Bool(true));
                }

                // Resolved keys: static account_keys, then loaded writable,
                // then loaded readonly — the order the program_id_index
                // addresses into.
                let mut resolved: Vec<Vec<u8>> = Vec::new();
                if let Some(msg) = info.transaction.as_ref().and_then(|t| t.message.as_ref()) {
                    resolved.extend(msg.account_keys.iter().cloned());
                }
                if let Some(meta) = info.meta.as_ref() {
                    resolved.extend(meta.loaded_writable_addresses.iter().cloned());
                    resolved.extend(meta.loaded_readonly_addresses.iter().cloned());
                }
                for key in resolved.iter().filter(|k| k.len() == 32) {
                    self.mark(DIM_ACCOUNT_KEY, DimValue::Bytes(key.clone()));
                }

                // Top-level instructions only; CPI is not indexed.
                if let Some(msg) = info.transaction.as_ref().and_then(|t| t.message.as_ref()) {
                    for ix in &msg.instructions {
                        if let Some(key) = resolved.get(ix.program_id_index as usize) {
                            if key.len() == 32 {
                                self.mark(DIM_PROGRAM_ID, DimValue::Bytes(key.clone()));
                            }
                        }
                    }
                }
            }
        }

        self.first_message_slot.get_or_insert(slot);
        self.last_message_slot = Some(slot);
        self.recv_ns_first.get_or_insert(recv_ns);
        self.recv_ns_last = Some(recv_ns);
        self.ordinal += 1;
    }

    /// Serialize the `.idx` exactly as the spec's byte layout describes.
    fn build_idx(&self) -> Vec<u8> {
        let mut body: Vec<u8> = Vec::new();
        let mut dimensions: Vec<DimensionHeader> = Vec::new();

        for (name, (value_type, values)) in &self.dims {
            let mut entries = Vec::new();
            for (value, bitmap) in values {
                let offset = body.len() as u64;
                bitmap.serialize_into(&mut body).expect("serialize bitmap");
                entries.push(DimEntryHeader {
                    value: value.clone(),
                    offset,
                    length: body.len() as u64 - offset,
                });
            }
            dimensions.push(DimensionHeader {
                name: (*name).to_string(),
                value_type: *value_type,
                entries,
            });
        }

        let header = IdxHeader {
            stream: self.stream.as_str().to_string(),
            start_slot: self.start_slot,
            end_slot: self.end_slot_exclusive,
            message_count: self.ordinal as u64,
            dimensions,
        };
        let header_bytes = rmp_serde::to_vec_named(&header).expect("msgpack header");

        let mut out = Vec::with_capacity(9 + header_bytes.len() + body.len());
        out.extend_from_slice(IDX_MAGIC);
        out.push(IDX_VERSION);
        out.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&header_bytes);
        out.extend_from_slice(&body);
        out
    }

    fn meta(&self, compressed_bytes: u64) -> ChunkMeta {
        ChunkMeta {
            schema_version: SCHEMA_VERSION,
            stream: self.stream.as_str().to_string(),
            start_slot: self.start_slot,
            end_slot_exclusive: self.end_slot_exclusive,
            first_message_slot: self.first_message_slot,
            last_message_slot: self.last_message_slot,
            message_count: self.ordinal as u64,
            uncompressed_bytes: self.payload.len() as u64,
            compressed_bytes,
            recv_ns_first: self.recv_ns_first,
            recv_ns_last: self.recv_ns_last,
            sealed_reason: "slot_range_complete".to_string(),
            index_dimensions: self.dims.keys().map(|k| (*k).to_string()).collect(),
        }
    }

    /// Write the trio under the documented key layout, sidecar last.
    fn write_to(&self, root: &Path) -> ChunkPaths {
        self.write_at(root, self.stream.as_str(), self.start_slot, true)
    }

    /// Escape hatch for the negative tests: allows a deliberately wrong
    /// directory, or omitting the sidecar.
    fn write_at(
        &self,
        root: &Path,
        stream_dir: &str,
        start_slot: u64,
        with_sidecar: bool,
    ) -> ChunkPaths {
        let dir = root.join("chunks").join(stream_dir);
        std::fs::create_dir_all(&dir).expect("create chunk dir");

        let stem = format!("{:012}-{:012}", start_slot, self.end_slot_exclusive);
        let zst = dir.join(format!("{stem}.zst"));
        let idx = dir.join(format!("{stem}.idx"));
        let meta = dir.join(format!("{stem}.meta.json"));

        let compressed = zstd::encode_all(self.payload.as_slice(), 3).expect("zstd encode");

        // Spec ordering: payload, then index, then the sidecar that makes the
        // chunk visible.
        std::fs::write(&zst, &compressed).expect("write zst");
        std::fs::write(&idx, self.build_idx()).expect("write idx");
        if with_sidecar {
            let json = serde_json::to_string_pretty(&self.meta(compressed.len() as u64))
                .expect("serialize meta");
            std::fs::write(&meta, json).expect("write meta");
        }

        ChunkPaths { zst, idx, meta }
    }
}

struct ChunkPaths {
    zst: std::path::PathBuf,
    idx: std::path::PathBuf,
    #[allow(dead_code)]
    meta: std::path::PathBuf,
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

const START_SLOT: u64 = 315_480_000;
const END_SLOT: u64 = 315_481_000;
const RECV_BASE: u64 = 1_700_000_000_000_000_000;

fn pk(n: u8) -> Vec<u8> {
    vec![n; 32]
}

fn sig(n: u8) -> Vec<u8> {
    vec![n; 64]
}

fn b58(bytes: &[u8]) -> String {
    bs58::encode(bytes).into_string()
}

fn tx_update(
    signature: Vec<u8>,
    is_vote: bool,
    failed: bool,
    account_keys: Vec<Vec<u8>>,
    loaded_writable: Vec<Vec<u8>>,
    program_id_index: u32,
) -> SubscribeUpdate {
    SubscribeUpdate {
        update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature,
                is_vote,
                transaction: Some(Transaction {
                    message: Some(Message {
                        account_keys,
                        instructions: vec![CompiledInstruction {
                            program_id_index,
                            accounts: vec![],
                            data: vec![],
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                meta: Some(TransactionStatusMeta {
                    err: failed.then(|| TransactionError { err: vec![1, 2, 3] }),
                    loaded_writable_addresses: loaded_writable,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })),
        ..Default::default()
    }
}

/// Six transactions with hand-computed dimension membership.
///
/// ord | sig | vote  | failed | account_keys      | program
/// ----|-----|-------|--------|-------------------|--------
///  0  |  1  | false | false  | pk1, pk2          | pk1
///  1  |  2  | true  | false  | pk3               | pk3
///  2  |  3  | false | true   | pk1, pk4          | pk4
///  3  |  4  | false | false  | pk2               | pk2
///  4  |  5  | true  | false  | pk1               | pk1
///  5  |  6  | false | false  | pk5 + loaded pk1  | pk5
fn fixture() -> Vec<(SubscribeUpdate, u64, u64)> {
    vec![
        (
            tx_update(sig(1), false, false, vec![pk(1), pk(2)], vec![], 0),
            START_SLOT,
            RECV_BASE,
        ),
        (
            tx_update(sig(2), true, false, vec![pk(3)], vec![], 0),
            START_SLOT,
            RECV_BASE + 100_000_000,
        ),
        (
            tx_update(sig(3), false, true, vec![pk(1), pk(4)], vec![], 1),
            START_SLOT + 1,
            RECV_BASE + 400_000_000,
        ),
        (
            tx_update(sig(4), false, false, vec![pk(2)], vec![], 0),
            START_SLOT + 1,
            RECV_BASE + 500_000_000,
        ),
        (
            tx_update(sig(5), true, false, vec![pk(1)], vec![], 0),
            START_SLOT + 2,
            RECV_BASE + 800_000_000,
        ),
        (
            tx_update(sig(6), false, false, vec![pk(5)], vec![pk(1)], 0),
            START_SLOT + 2,
            RECV_BASE + 900_000_000,
        ),
    ]
}

fn write_fixture(root: &Path) -> (Vec<SubscribeUpdate>, ChunkPaths) {
    let mut producer = SpecProducer::new(START_SLOT, END_SLOT);
    let items = fixture();
    for (update, slot, recv_ns) in &items {
        producer.push(update, *slot, *recv_ns);
    }
    let paths = producer.write_to(root);
    (items.into_iter().map(|(u, _, _)| u).collect(), paths)
}

fn ords(bitmap: &RoaringBitmap) -> Vec<u32> {
    bitmap.iter().collect()
}

// ---------------------------------------------------------------------------
// Catalog discovery
// ---------------------------------------------------------------------------

#[test]
fn catalog_discovers_a_chunk_written_to_the_documented_key_layout() {
    let tmp = tempfile::tempdir().unwrap();
    write_fixture(tmp.path());

    let catalog = ChunkCatalog::scan(tmp.path());
    let entry = catalog
        .get(Stream::Tx, START_SLOT)
        .expect("chunk written per docs/format.md must be catalogued");

    assert_eq!(entry.stream, Stream::Tx);
    assert_eq!(entry.start_slot, START_SLOT);
    assert_eq!(entry.end_slot_exclusive, END_SLOT);
    assert_eq!(entry.meta.schema_version, SCHEMA_VERSION);
    assert_eq!(entry.meta.message_count, 6);
    assert_eq!(entry.meta.first_message_slot, Some(START_SLOT));
    assert_eq!(entry.meta.last_message_slot, Some(START_SLOT + 2));
}

#[test]
fn a_chunk_outside_the_documented_prefix_is_invisible() {
    // The spec warns that the key layout is not free-form. Prove it: same
    // bytes, wrong directory, and the reader never sees it.
    let tmp = tempfile::tempdir().unwrap();
    let mut producer = SpecProducer::new(START_SLOT, END_SLOT);
    for (update, slot, recv_ns) in fixture() {
        producer.push(&update, slot, recv_ns);
    }
    producer.write_at(tmp.path(), "transactions", START_SLOT, true);

    let catalog = ChunkCatalog::scan(tmp.path());
    assert!(catalog.get(Stream::Tx, START_SLOT).is_none());
    assert_eq!(catalog.newest_end_slot(), None);
}

#[test]
fn a_chunk_missing_its_sidecar_is_not_catalogued() {
    // This is why the spec says to write the sidecar last: until it lands, the
    // chunk does not exist as far as the reader is concerned.
    let tmp = tempfile::tempdir().unwrap();
    let mut producer = SpecProducer::new(START_SLOT, END_SLOT);
    for (update, slot, recv_ns) in fixture() {
        producer.push(&update, slot, recv_ns);
    }
    producer.write_at(tmp.path(), Stream::Tx.as_str(), START_SLOT, false);

    let catalog = ChunkCatalog::scan(tmp.path());
    assert!(catalog.get(Stream::Tx, START_SLOT).is_none());
}

// ---------------------------------------------------------------------------
// Payload round trip
// ---------------------------------------------------------------------------

#[test]
fn frames_survive_the_round_trip_byte_for_byte() {
    let tmp = tempfile::tempdir().unwrap();
    let (originals, paths) = write_fixture(tmp.path());

    let decoded = decode_chunk(&paths.zst).expect("decode chunk");
    assert_eq!(decoded.len(), originals.len());

    for (ordinal, original) in originals.iter().enumerate() {
        let expected = original.encode_to_vec();
        let actual = decoded
            .frame(ordinal as u32)
            .expect("frame present at ordinal");
        assert_eq!(actual, expected.as_slice(), "frame {ordinal} differs");
    }

    assert!(decoded.frame(originals.len() as u32).is_none());
}

#[test]
fn messages_decode_back_to_the_original_protobufs_in_order() {
    let tmp = tempfile::tempdir().unwrap();
    let (originals, paths) = write_fixture(tmp.path());

    let decoded = decode_chunk(&paths.zst).expect("decode chunk");
    for (ordinal, original) in originals.iter().enumerate() {
        let msg = decoded
            .decode_message(ordinal as u32)
            .expect("decode SubscribeUpdate");
        assert_eq!(&msg, original, "message {ordinal} differs");
    }
}

// ---------------------------------------------------------------------------
// Index round trip
// ---------------------------------------------------------------------------

#[test]
fn index_header_survives_the_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let (_, paths) = write_fixture(tmp.path());

    let idx = parse_chunk_index(&paths.idx).expect("parse index written to spec");
    assert_eq!(idx.stream(), Stream::Tx.as_str());
    assert_eq!(idx.message_count(), 6);

    let mut dims: Vec<&str> = idx.dimensions().collect();
    dims.sort_unstable();
    assert_eq!(
        dims,
        vec![
            DIM_ACCOUNT_KEY,
            DIM_FAILED_FLAG,
            DIM_PROGRAM_ID,
            DIM_SIGNATURE,
            DIM_VOTE_FLAG
        ]
    );
}

#[test]
fn bitmaps_resolve_to_the_exact_ordinals_the_producer_indexed() {
    let tmp = tempfile::tempdir().unwrap();
    let (_, paths) = write_fixture(tmp.path());
    let idx = parse_chunk_index(&paths.idx).unwrap();

    let get = |dim: &str, value: DimValue| {
        idx.bitmap_for(dim, &value)
            .expect("bitmap lookup")
            .map(|bm| ords(&bm))
    };

    assert_eq!(get(DIM_VOTE_FLAG, DimValue::Bool(true)), Some(vec![1, 4]));
    assert_eq!(get(DIM_FAILED_FLAG, DimValue::Bool(true)), Some(vec![2]));

    // pk1 appears as a static key at 0, 2 and 4, and as a *loaded* address at
    // 5 — the resolved set is the union, not just account_keys.
    assert_eq!(
        get(DIM_ACCOUNT_KEY, DimValue::Bytes(pk(1))),
        Some(vec![0, 2, 4, 5])
    );
    assert_eq!(
        get(DIM_PROGRAM_ID, DimValue::Bytes(pk(1))),
        Some(vec![0, 4])
    );
    assert_eq!(get(DIM_SIGNATURE, DimValue::Bytes(sig(3))), Some(vec![2]));

    // A value that was never indexed is absent, not empty.
    assert_eq!(get(DIM_ACCOUNT_KEY, DimValue::Bytes(pk(99))), None);
    assert_eq!(get("no_such_dimension", DimValue::Bool(true)), None);
}

// ---------------------------------------------------------------------------
// Filter resolution — the point of the index
// ---------------------------------------------------------------------------

fn tx_filter() -> SubscribeRequestFilterTransactions {
    SubscribeRequestFilterTransactions::default()
}

#[test]
fn filters_resolve_through_the_index_to_the_right_ordinals() {
    let tmp = tempfile::tempdir().unwrap();
    let (_, paths) = write_fixture(tmp.path());
    let idx = parse_chunk_index(&paths.idx).unwrap();

    // No constraints: everything.
    assert_eq!(ords(&filter_tx(&idx, &tx_filter())), vec![0, 1, 2, 3, 4, 5]);

    // vote: false excludes the two vote transactions.
    let f = SubscribeRequestFilterTransactions {
        vote: Some(false),
        ..tx_filter()
    };
    assert_eq!(ords(&filter_tx(&idx, &f)), vec![0, 2, 3, 5]);

    // vote: true keeps only them.
    let f = SubscribeRequestFilterTransactions {
        vote: Some(true),
        ..tx_filter()
    };
    assert_eq!(ords(&filter_tx(&idx, &f)), vec![1, 4]);

    // failed: true is the single failed transaction.
    let f = SubscribeRequestFilterTransactions {
        failed: Some(true),
        ..tx_filter()
    };
    assert_eq!(ords(&filter_tx(&idx, &f)), vec![2]);

    // account_include is a union over the resolved key set.
    let f = SubscribeRequestFilterTransactions {
        account_include: vec![b58(&pk(1))],
        ..tx_filter()
    };
    assert_eq!(ords(&filter_tx(&idx, &f)), vec![0, 2, 4, 5]);

    // Constraints intersect: non-vote transactions touching pk1.
    let f = SubscribeRequestFilterTransactions {
        vote: Some(false),
        account_include: vec![b58(&pk(1))],
        ..tx_filter()
    };
    assert_eq!(ords(&filter_tx(&idx, &f)), vec![0, 2, 5]);

    // account_exclude subtracts.
    let f = SubscribeRequestFilterTransactions {
        account_include: vec![b58(&pk(1))],
        account_exclude: vec![b58(&pk(4))],
        ..tx_filter()
    };
    assert_eq!(ords(&filter_tx(&idx, &f)), vec![0, 4, 5]);

    // account_required is an AND across keys: only ordinal 2 has both.
    let f = SubscribeRequestFilterTransactions {
        account_required: vec![b58(&pk(1)), b58(&pk(4))],
        ..tx_filter()
    };
    assert_eq!(ords(&filter_tx(&idx, &f)), vec![2]);

    // A signature pins a single ordinal.
    let f = SubscribeRequestFilterTransactions {
        signature: Some(b58(&sig(3))),
        ..tx_filter()
    };
    assert_eq!(ords(&filter_tx(&idx, &f)), vec![2]);

    // A key nobody touched yields nothing.
    let f = SubscribeRequestFilterTransactions {
        account_include: vec![b58(&pk(99))],
        ..tx_filter()
    };
    assert!(filter_tx(&idx, &f).is_empty());
}

#[test]
fn filtered_ordinals_index_back_into_the_payload() {
    // The whole point of the format: resolve filters to ordinals against the
    // index, and only then touch the payload.
    let tmp = tempfile::tempdir().unwrap();
    let (originals, paths) = write_fixture(tmp.path());
    let idx = parse_chunk_index(&paths.idx).unwrap();

    let f = SubscribeRequestFilterTransactions {
        vote: Some(false),
        account_include: vec![b58(&pk(1))],
        ..tx_filter()
    };
    let matched = filter_tx(&idx, &f);
    assert_eq!(ords(&matched), vec![0, 2, 5]);

    let decoded = decode_chunk(&paths.zst).unwrap();
    for ordinal in matched.iter() {
        let msg = decoded.decode_message(ordinal).unwrap();
        assert_eq!(&msg, &originals[ordinal as usize]);

        let Some(UpdateOneof::Transaction(tx)) = msg.update_oneof.as_ref() else {
            panic!("ordinal {ordinal} is not a transaction");
        };
        let info = tx.transaction.as_ref().unwrap();
        assert!(!info.is_vote, "ordinal {ordinal} should not be a vote");
    }
}

// ---------------------------------------------------------------------------
// Planning and pacing
// ---------------------------------------------------------------------------

#[test]
fn replay_planning_accepts_a_spec_written_chunk() {
    let tmp = tempfile::tempdir().unwrap();
    write_fixture(tmp.path());

    let catalog = ChunkCatalog::scan(tmp.path());
    let request = SubscribeRequest {
        transactions: [("client".to_string(), tx_filter())].into_iter().collect(),
        ..Default::default()
    };
    let parsed = parse_subscribe_request(request).expect("filters accepted");
    let cache = IndexCache::new(1 << 20);

    let plan = plan_replay(&catalog, &parsed, &cache, None).expect("plan built");
    assert_eq!(plan.from_slot, START_SLOT);

    // Asking below the retained floor is an error, not a silent fast-forward.
    let err = plan_replay(&catalog, &parsed, &cache, Some(START_SLOT - 1));
    assert!(err.is_err());
}

#[test]
fn pacing_timestamps_come_back_from_the_sidecar() {
    // No `created_at` on these messages, so the reader interpolates across
    // recv_ns_first..recv_ns_last — the timing fidelity the format promises.
    let tmp = tempfile::tempdir().unwrap();
    write_fixture(tmp.path());

    let catalog = ChunkCatalog::scan(tmp.path());
    let entry = catalog.get(Stream::Tx, START_SLOT).unwrap();
    let decoded = decode_chunk(&entry.zst_path).unwrap();

    let first = decoded.decode_message(0).unwrap();
    let last = decoded.decode_message(5).unwrap();

    assert_eq!(extract_recv_ns(&first, &entry.meta, 0), Some(RECV_BASE));
    assert_eq!(
        extract_recv_ns(&last, &entry.meta, 5),
        Some(RECV_BASE + 900_000_000)
    );

    // Interpolation is monotonic across the chunk.
    let mut previous = 0u64;
    for ordinal in 0..6u32 {
        let msg = decoded.decode_message(ordinal).unwrap();
        let ns = extract_recv_ns(&msg, &entry.meta, ordinal).expect("timestamp");
        assert!(ns >= previous, "recv_ns went backwards at {ordinal}");
        previous = ns;
    }
}
