use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use anyhow::Context;
use prost::Message;
use sillage_common::chunk::{ChunkMeta, SCHEMA_VERSION};
use sillage_common::Stream;
use tracing::{debug, warn};

use crate::metrics;
use ::metrics::{counter, histogram};

#[derive(Debug, Clone)]
pub struct ChunkEntry {
    pub stream: Stream,
    pub start_slot: u64,
    pub end_slot_exclusive: u64,
    pub zst_path: PathBuf,
    pub idx_path: PathBuf,
    pub meta_path: PathBuf,
    pub zst_len: u64,
    pub meta: ChunkMeta,
}

/// Per-stream summary returned by [`ChunkCatalog::summary`].
pub struct CatalogSummary {
    pub per_stream: Vec<(Stream, usize, Option<u64>, Option<u64>)>,
}

/// In-memory index of all locally-present chunk trios, keyed by stream and
/// start-slot. Built by scanning the NVMe directory; never panics on IO.
pub struct ChunkCatalog {
    streams: HashMap<Stream, std::collections::BTreeMap<u64, ChunkEntry>>,
}

/// Shared, swappable handle to the current [`ChunkCatalog`].
///
/// The catalog is immutable once built. The syncer publishes a freshly scanned
/// one after any cycle that changed the on-disk chunk set; readers take a cheap
/// snapshot and hold it for the duration of a single operation — one replay
/// plan, one RPC. Holding a snapshot keeps an in-flight replay consistent even
/// while the syncer swaps a newer catalog in behind it.
///
/// The lock is held only long enough to clone an `Arc`, and the read path runs
/// once per subscribe rather than per message, so a plain `RwLock` is ample.
#[derive(Clone)]
pub struct SharedCatalog(Arc<RwLock<Arc<ChunkCatalog>>>);

impl SharedCatalog {
    pub fn new(catalog: ChunkCatalog) -> Self {
        Self(Arc::new(RwLock::new(Arc::new(catalog))))
    }

    /// Take a snapshot of the current catalog.
    ///
    /// A poisoned lock is recovered rather than propagated: the catalog is
    /// plain data behind the lock, so a panic elsewhere cannot have left it
    /// half-written, and serving a snapshot beats failing the request.
    pub fn snapshot(&self) -> Arc<ChunkCatalog> {
        self.0
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Publish a newly scanned catalog. Snapshots already handed out are
    /// unaffected and stay valid until their holders drop them.
    pub fn store(&self, catalog: ChunkCatalog) {
        let mut guard = self
            .0
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = Arc::new(catalog);
    }
}

impl ChunkCatalog {
    /// Walk `nvme_path/chunks/{stream}/*.meta.json` for every stream,
    /// parse metadata, verify sibling `.zst` and `.idx` exist, and
    /// populate the catalog. Incomplete trios or unparseable meta files
    /// are skipped with a log message.
    pub fn scan(nvme_path: &std::path::Path) -> Self {
        let mut streams: HashMap<Stream, std::collections::BTreeMap<u64, ChunkEntry>> =
            HashMap::new();

        for stream in Stream::all() {
            let dir = nvme_path.join("chunks").join(stream.as_str());
            if !dir.is_dir() {
                continue;
            }

            let entries = match std::fs::read_dir(&dir) {
                Ok(rd) => rd,
                Err(e) => {
                    warn!(path = %dir.display(), error = %e, "failed to read stream directory");
                    continue;
                }
            };

            for entry in entries.flatten() {
                let path = entry.path();
                let file_name = match path.file_name().and_then(|n| n.to_str()) {
                    Some(name) => name,
                    None => continue,
                };

                if !file_name.ends_with(".meta.json") {
                    continue;
                }

                let stem = match file_name.strip_suffix(".meta.json") {
                    Some(s) => s,
                    None => continue,
                };

                let mut parts = stem.splitn(2, '-');
                let start_str = parts.next().unwrap_or("");
                let end_str = parts.next().unwrap_or("");

                let start_slot = match start_str.parse::<u64>() {
                    Ok(s) => s,
                    Err(_) => {
                        debug!(file = file_name, "skipping unparseable start_slot");
                        continue;
                    }
                };
                let end_slot_exclusive = match end_str.parse::<u64>() {
                    Ok(s) => s,
                    Err(_) => {
                        debug!(file = file_name, "skipping unparseable end_slot");
                        continue;
                    }
                };

                let zst_path = dir.join(format!("{stem}.zst"));
                let idx_path = dir.join(format!("{stem}.idx"));

                if !zst_path.is_file() {
                    debug!(zst = %zst_path.display(), "skipping chunk trio: missing .zst");
                    continue;
                }
                if !idx_path.is_file() {
                    debug!(idx = %idx_path.display(), "skipping chunk trio: missing .idx");
                    continue;
                }

                let meta_bytes = match std::fs::read(&path) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(path = %path.display(), error = %e, "failed to read meta.json");
                        continue;
                    }
                };
                let meta: ChunkMeta = match serde_json::from_slice(&meta_bytes) {
                    Ok(m) => m,
                    Err(e) => {
                        warn!(path = %path.display(), error = %e, "failed to parse meta.json");
                        continue;
                    }
                };

                if meta.schema_version != SCHEMA_VERSION {
                    warn!(
                        path = %path.display(),
                        meta_schema_version = meta.schema_version,
                        expected = SCHEMA_VERSION,
                        "skipping chunk: meta schema_version mismatch"
                    );
                    continue;
                }

                let zst_len = match std::fs::metadata(&zst_path) {
                    Ok(m) => m.len(),
                    Err(e) => {
                        warn!(path = %zst_path.display(), error = %e, "failed to stat .zst");
                        continue;
                    }
                };

                streams.entry(stream).or_default().insert(
                    start_slot,
                    ChunkEntry {
                        stream,
                        start_slot,
                        end_slot_exclusive,
                        zst_path,
                        idx_path,
                        meta_path: path,
                        zst_len,
                        meta,
                    },
                );
            }
        }

        Self { streams }
    }

    /// Return entries for `stream` whose `[start_slot, end_slot_exclusive)`
    /// overlaps `[from_slot, to_slot_exclusive)`, in ascending order by
    /// `start_slot`.
    pub fn chunks_in_range(
        &self,
        stream: Stream,
        from_slot: u64,
        to_slot_exclusive: u64,
    ) -> Vec<&ChunkEntry> {
        let tree = match self.streams.get(&stream) {
            Some(t) => t,
            None => return Vec::new(),
        };

        // An entry [s, e) overlaps [from, to) iff s < to && e > from.
        // We use BTreeMap::range to narrow candidates: entries with
        // start_slot < to_slot_exclusive are the only ones that can overlap.
        // Then we filter for end_slot_exclusive > from_slot.
        tree.range(..to_slot_exclusive)
            .filter(|(_, entry)| entry.end_slot_exclusive > from_slot)
            .map(|(_, entry)| entry)
            .collect()
    }

    /// Look up a single chunk by stream and start_slot.
    pub fn get(&self, stream: Stream, start_slot: u64) -> Option<&ChunkEntry> {
        self.streams.get(&stream).and_then(|t| t.get(&start_slot))
    }

    /// Return the maximum `end_slot_exclusive` across all streams, or `None`
    /// if the catalog is empty.
    pub fn newest_end_slot(&self) -> Option<u64> {
        self.summary()
            .per_stream
            .iter()
            .filter_map(|(_, _, _, max_end)| *max_end)
            .max()
    }

    /// Per-stream summary: (stream, chunk_count, min_start_slot, max_end_slot_exclusive).
    pub fn summary(&self) -> CatalogSummary {
        let per_stream = Stream::all()
            .iter()
            .filter_map(|&stream| {
                let tree = self.streams.get(&stream)?;
                let count = tree.len();
                let min_start = tree.keys().next().copied();
                let max_end = tree.values().last().map(|e| e.end_slot_exclusive);
                Some((stream, count, min_start, max_end))
            })
            .collect();

        CatalogSummary { per_stream }
    }
}

/// A fully decompressed chunk with pre-computed frame byte-ranges.
///
/// `raw` holds the entire decompressed stream; `frames[i]` is the byte range
/// of the i-th length-prefixed message payload within `raw`.
/// Decoding individual messages is deferred to `decode_message()`.
pub struct DecodedChunk {
    raw: Vec<u8>,
    frames: Vec<std::ops::Range<usize>>,
}

/// Decompress a `.zst` chunk file and build a [`DecodedChunk`].
///
/// Reads the entire file, stream-decompresses with zstd, then walks the
/// length-prefixed frame layout to collect byte ranges. Returns `Err` on
/// IO failure, corrupt zstd data, or malformed frame layout.
pub fn decode_chunk(zst_path: &std::path::Path) -> anyhow::Result<DecodedChunk> {
    let start = Instant::now();
    let file = std::fs::File::open(zst_path)
        .with_context(|| format!("failed to open chunk file {}", zst_path.display()))?;
    let mut decoder = zstd::stream::read::Decoder::new(file)?;
    let mut raw = Vec::new();
    std::io::Read::read_to_end(&mut decoder, &mut raw)
        .with_context(|| format!("failed to decompress chunk {}", zst_path.display()))?;

    let mut frames = Vec::new();
    let mut offset = 0usize;
    for frame_result in sillage_common::chunk::iter_frames(&raw) {
        let payload = frame_result.with_context(|| {
            format!(
                "corrupt frame layout at offset {} in chunk {}",
                offset,
                zst_path.display()
            )
        })?;
        // The length prefix is 4 bytes before the payload.
        let payload_start = offset + 4;
        let payload_end = payload_start + payload.len();
        frames.push(payload_start..payload_end);
        offset = payload_end;
    }

    histogram!(metrics::CHUNK_DECODE_SECONDS).record(start.elapsed().as_secs_f64());
    Ok(DecodedChunk { raw, frames })
}

impl DecodedChunk {
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Raw payload bytes for the frame at `ordinal` (0-based), or `None` if
    /// out of range.
    pub fn frame(&self, ordinal: u32) -> Option<&[u8]> {
        let idx = ordinal as usize;
        self.frames.get(idx).map(|range| &self.raw[range.clone()])
    }

    /// Decode the frame at `ordinal` into a `SubscribeUpdate` proto message.
    /// Returns `Err` if the ordinal is out of range or the payload is not a
    /// valid `SubscribeUpdate`.
    pub fn decode_message(
        &self,
        ordinal: u32,
    ) -> anyhow::Result<yellowstone_grpc_proto::geyser::SubscribeUpdate> {
        let bytes = self.frame(ordinal).ok_or_else(|| {
            anyhow::anyhow!("ordinal {ordinal} out of range (max {})", self.len())
        })?;
        yellowstone_grpc_proto::geyser::SubscribeUpdate::decode(bytes).map_err(|e| {
            anyhow::anyhow!("failed to decode SubscribeUpdate at ordinal {ordinal}: {e}")
        })
    }

    /// Approximate heap allocation size.
    pub fn heap_bytes(&self) -> usize {
        self.raw.len() + self.frames.len() * std::mem::size_of::<std::ops::Range<usize>>()
    }
}

// ---------------------------------------------------------------------------
// ChunkCache — byte-budgeted hand-rolled LRU over DecodedChunk
// ---------------------------------------------------------------------------

/// Key for the decoded-chunk cache: (stream, start_slot).
#[derive(Hash, Eq, PartialEq, Clone)]
pub struct CacheKey {
    pub stream: Stream,
    pub start_slot: u64,
}

struct CacheInner {
    map: HashMap<CacheKey, std::sync::Arc<DecodedChunk>>,
    used: u64,
    tick: u64,
    last_used: HashMap<CacheKey, u64>,
}

/// Byte-budgeted LRU cache of decoded chunks.
///
/// Thread-safe via `Mutex` interior. On a cache miss, `get_or_decode`
/// decodes the chunk *outside* the lock so concurrent decodes of
/// different keys don't serialize. Oversized single chunks are cached
/// with a WARN log and never evicted into an infinite loop.
pub struct ChunkCache {
    inner: std::sync::Mutex<CacheInner>,
    budget_bytes: u64,
}

impl ChunkCache {
    pub fn new(budget_bytes: u64) -> Self {
        Self {
            inner: std::sync::Mutex::new(CacheInner {
                map: HashMap::new(),
                used: 0,
                tick: 0,
                last_used: HashMap::new(),
            }),
            budget_bytes,
        }
    }

    /// Return the number of cached chunks.
    pub fn len(&self) -> usize {
        match self.inner.lock() {
            Ok(guard) => guard.map.len(),
            Err(poisoned) => poisoned.into_inner().map.len(),
        }
    }

    /// Whether the cache currently holds no decoded chunks.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return total heap bytes currently used by cached chunks.
    pub fn used_bytes(&self) -> u64 {
        match self.inner.lock() {
            Ok(guard) => guard.used,
            Err(poisoned) => poisoned.into_inner().used,
        }
    }

    /// Get a decoded chunk from the cache, decoding from disk on miss.
    ///
    /// On a hit, bumps the LRU recency counter and returns a clone of
    /// the `Arc`. On a miss, drops the lock, decodes the file, re-locks,
    /// inserts, and evicts LRU entries until under budget (or only one
    /// entry remains, to handle oversized chunks).
    pub fn get_or_decode(
        &self,
        key: CacheKey,
        zst_path: &std::path::Path,
    ) -> anyhow::Result<std::sync::Arc<DecodedChunk>> {
        // --- Fast path: check cache under lock ---
        {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| anyhow::anyhow!("cache lock poisoned: {e}"))?;
            if let Some(arc) = inner.map.get(&key) {
                let cloned = std::sync::Arc::clone(arc);
                let tick = inner.tick;
                inner.tick = tick + 1;
                inner.last_used.insert(key, tick);
                counter!(metrics::CHUNK_CACHE_HITS_TOTAL).increment(1);
                return Ok(cloned);
            }
        }

        // --- Slow path: decode outside the lock ---
        counter!(metrics::CHUNK_CACHE_MISSES_TOTAL).increment(1);
        let decoded = decode_chunk(zst_path)?;
        let arc = std::sync::Arc::new(decoded);
        let chunk_bytes = arc.heap_bytes() as u64;

        // --- Re-acquire lock and insert ---
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| anyhow::anyhow!("cache lock poisoned: {e}"))?;

        // Another thread may have inserted the same key while we decoded.
        if let Some(existing) = inner.map.get(&key) {
            let cloned = std::sync::Arc::clone(existing);
            let tick = inner.tick;
            inner.tick = tick + 1;
            inner.last_used.insert(key, tick);
            counter!(metrics::CHUNK_CACHE_HITS_TOTAL).increment(1);
            return Ok(cloned);
        }

        // Insert the new entry.
        let tick = inner.tick;
        inner.tick = tick + 1;
        inner.last_used.insert(key.clone(), tick);
        inner.map.insert(key, std::sync::Arc::clone(&arc));
        inner.used += chunk_bytes;

        // --- Evict LRU entries until under budget ---
        while inner.used > self.budget_bytes && inner.map.len() > 1 {
            // Find the key with the smallest last_used tick.
            let evict_key = inner
                .last_used
                .iter()
                .min_by_key(|(_, &tick)| tick)
                .map(|(k, _)| k.clone());
            let evict_key = match evict_key {
                Some(k) => k,
                None => break,
            };

            let removed = inner.map.remove(&evict_key);
            inner.last_used.remove(&evict_key);
            if let Some(removed_chunk) = removed {
                inner.used -= removed_chunk.heap_bytes() as u64;
            }
        }

        // If a single oversized chunk exceeds the budget, log a warning
        // but do NOT evict it into an infinite loop.
        if inner.used > self.budget_bytes && inner.map.len() == 1 {
            warn!(
                budget_bytes = self.budget_bytes,
                chunk_bytes = inner.used,
                "oversized chunk exceeds cache budget; cached anyway"
            );
        }

        Ok(arc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// Helper: write a complete chunk trio (`.zst` + `.idx` + `.meta.json`)
    /// under `dir/chunks/{stream}/` with the given slot range.
    fn write_trio(
        dir: &std::path::Path,
        stream: Stream,
        start_slot: u64,
        end_slot_exclusive: u64,
        meta: &ChunkMeta,
    ) {
        let stream_dir = dir.join("chunks").join(stream.as_str());
        fs::create_dir_all(&stream_dir).unwrap();
        let stem = format!("{:012}-{:012}", start_slot, end_slot_exclusive);
        let zst_path = stream_dir.join(format!("{stem}.zst"));
        let idx_path = stream_dir.join(format!("{stem}.idx"));
        let meta_path = stream_dir.join(format!("{stem}.meta.json"));
        fs::write(&zst_path, b"compressed-data").unwrap();
        fs::write(&idx_path, b"index-data").unwrap();
        let json = serde_json::to_string(meta).unwrap();
        fs::write(&meta_path, json).unwrap();
    }

    fn make_meta(stream: &str, start: u64, end: u64) -> ChunkMeta {
        ChunkMeta {
            schema_version: sillage_common::chunk::SCHEMA_VERSION,
            stream: stream.to_string(),
            start_slot: start,
            end_slot_exclusive: end,
            first_message_slot: Some(start),
            last_message_slot: Some(end - 1),
            message_count: end - start,
            uncompressed_bytes: 4096,
            compressed_bytes: 1024,
            recv_ns_first: Some(1_000_000),
            recv_ns_last: Some(2_000_000),
            sealed_reason: "watermark".to_string(),
            index_dimensions: vec!["program_id".to_string()],
        }
    }

    #[test]
    fn catalog_scan_empty_dir_is_empty() {
        let tmp = TempDir::new().unwrap();
        let catalog = ChunkCatalog::scan(tmp.path());
        assert!(catalog.get(Stream::Tx, 0).is_none());
        assert!(catalog.get(Stream::Acct, 0).is_none());
        assert!(catalog.get(Stream::Block, 0).is_none());
        let summary = catalog.summary();
        assert!(summary.per_stream.is_empty());
    }

    #[test]
    fn catalog_scan_finds_entries_per_stream() {
        let tmp = TempDir::new().unwrap();
        // 2 tx chunks, 1 acct chunk
        write_trio(tmp.path(), Stream::Tx, 0, 1000, &make_meta("tx", 0, 1000));
        write_trio(
            tmp.path(),
            Stream::Tx,
            1000,
            2000,
            &make_meta("tx", 1000, 2000),
        );
        write_trio(tmp.path(), Stream::Acct, 0, 500, &make_meta("acct", 0, 500));

        let catalog = ChunkCatalog::scan(tmp.path());

        // Tx has 2 entries
        let tx0 = catalog.get(Stream::Tx, 0).unwrap();
        assert_eq!(tx0.start_slot, 0);
        assert_eq!(tx0.end_slot_exclusive, 1000);
        assert_eq!(tx0.stream, Stream::Tx);
        assert!(tx0.zst_path.is_file());
        assert!(tx0.idx_path.is_file());
        assert!(tx0.meta_path.is_file());
        assert!(tx0.zst_len > 0);

        let tx1 = catalog.get(Stream::Tx, 1000).unwrap();
        assert_eq!(tx1.start_slot, 1000);
        assert_eq!(tx1.end_slot_exclusive, 2000);

        // Acct has 1 entry
        let acct = catalog.get(Stream::Acct, 0).unwrap();
        assert_eq!(acct.end_slot_exclusive, 500);

        // Block has none
        assert!(catalog.get(Stream::Block, 0).is_none());

        // Summary
        let summary = catalog.summary();
        assert_eq!(summary.per_stream.len(), 2); // only streams with data
    }

    #[test]
    fn catalog_skips_incomplete_trio() {
        let tmp = TempDir::new().unwrap();
        // Write only meta.json, no .zst or .idx
        let stream_dir = tmp.path().join("chunks").join("tx");
        fs::create_dir_all(&stream_dir).unwrap();
        let meta = make_meta("tx", 0, 1000);
        let json = serde_json::to_string(&meta).unwrap();
        let meta_path = stream_dir.join("000000000000-0000001000.meta.json");
        fs::write(&meta_path, json).unwrap();
        // No .zst or .idx written

        let catalog = ChunkCatalog::scan(tmp.path());
        assert!(catalog.get(Stream::Tx, 0).is_none());
    }

    #[test]
    fn catalog_skips_mismatched_schema_version() {
        let tmp = TempDir::new().unwrap();
        let stream_dir = tmp.path().join("chunks").join("tx");
        fs::create_dir_all(&stream_dir).unwrap();
        let stem = "000000000000-000000001000";
        let zst_path = stream_dir.join(format!("{stem}.zst"));
        let idx_path = stream_dir.join(format!("{stem}.idx"));
        let meta_path = stream_dir.join(format!("{stem}.meta.json"));
        fs::write(&zst_path, b"data").unwrap();
        fs::write(&idx_path, b"idx").unwrap();
        // Write meta with schema_version bumped past the reader's expected value.
        let mut meta = make_meta("tx", 0, 1000);
        meta.schema_version = SCHEMA_VERSION + 999;
        fs::write(&meta_path, serde_json::to_string(&meta).unwrap()).unwrap();

        let catalog = ChunkCatalog::scan(tmp.path());
        assert!(catalog.get(Stream::Tx, 0).is_none());
    }

    #[test]
    fn catalog_skips_unparseable_meta() {
        let tmp = TempDir::new().unwrap();
        let stream_dir = tmp.path().join("chunks").join("tx");
        fs::create_dir_all(&stream_dir).unwrap();
        let stem = "000000000000-0000001000";
        let zst_path = stream_dir.join(format!("{stem}.zst"));
        let idx_path = stream_dir.join(format!("{stem}.idx"));
        let meta_path = stream_dir.join(format!("{stem}.meta.json"));
        fs::write(&zst_path, b"data").unwrap();
        fs::write(&idx_path, b"idx").unwrap();
        fs::write(&meta_path, b"NOT JSON{{{{").unwrap();

        let catalog = ChunkCatalog::scan(tmp.path());
        assert!(catalog.get(Stream::Tx, 0).is_none());
    }

    #[test]
    fn catalog_range_query_returns_overlapping_ascending() {
        let tmp = TempDir::new().unwrap();
        // Chunks: [0,1000), [1000,2000), [2000,3000), [3000,4000)
        for start in (0..4).map(|i| i * 1000) {
            write_trio(
                tmp.path(),
                Stream::Tx,
                start,
                start + 1000,
                &make_meta("tx", start, start + 1000),
            );
        }

        let catalog = ChunkCatalog::scan(tmp.path());

        // Query [1500, 3500) should overlap [1000,2000), [2000,3000), [3000,4000)
        let results = catalog.chunks_in_range(Stream::Tx, 1500, 3500);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].start_slot, 1000);
        assert_eq!(results[1].start_slot, 2000);
        assert_eq!(results[2].start_slot, 3000);
    }

    #[test]
    fn catalog_range_query_excludes_out_of_range() {
        let tmp = TempDir::new().unwrap();
        // Chunks: [0,1000), [1000,2000), [2000,3000)
        for start in (0..3).map(|i| i * 1000) {
            write_trio(
                tmp.path(),
                Stream::Tx,
                start,
                start + 1000,
                &make_meta("tx", start, start + 1000),
            );
        }

        let catalog = ChunkCatalog::scan(tmp.path());

        // Query [5000, 6000) — no overlap
        let results = catalog.chunks_in_range(Stream::Tx, 5000, 6000);
        assert!(results.is_empty());

        // Query [0, 0) — empty range, no overlap
        let results = catalog.chunks_in_range(Stream::Tx, 0, 0);
        assert!(results.is_empty());

        // Query [3000, 4000) — only [2000,3000) ends at 3000, but
        // [2000,3000) has end_slot_exclusive=3000 which is NOT > 3000,
        // so it does NOT overlap [3000,4000).
        let results = catalog.chunks_in_range(Stream::Tx, 3000, 4000);
        assert!(results.is_empty());
    }

    // --- DecodedChunk / decode_chunk tests ---

    use prost::Message;
    use sillage_common::chunk::write_len_prefixed;
    use yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateAccount,
    };

    fn make_subscribe_update(slot: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                slot,
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn encode_chunk_zst(messages: &[SubscribeUpdate]) -> Vec<u8> {
        let mut framed = Vec::new();
        for msg in messages {
            let mut buf = Vec::new();
            msg.encode(&mut buf).unwrap();
            write_len_prefixed(&mut framed, &buf);
        }
        let mut compressed = Vec::new();
        let mut encoder = zstd::stream::write::Encoder::new(&mut compressed, 3).unwrap();
        std::io::Write::write_all(&mut encoder, &framed).unwrap();
        encoder.finish().unwrap();
        compressed
    }

    fn write_zst_file(dir: &std::path::Path, name: &str, data: &[u8]) -> std::path::PathBuf {
        let path = dir.join(name);
        fs::write(&path, data).unwrap();
        path
    }

    #[test]
    fn decode_chunk_round_trips_message_payloads() {
        let tmp = TempDir::new().unwrap();
        let msgs: Vec<SubscribeUpdate> = (0..5).map(|i| make_subscribe_update(i * 100)).collect();
        let zst_data = encode_chunk_zst(&msgs);
        let zst_path = write_zst_file(tmp.path(), "test.zst", &zst_data);

        let chunk = decode_chunk(&zst_path).unwrap();
        assert_eq!(chunk.len(), 5);
        assert!(!chunk.is_empty());
        for i in 0..5u32 {
            let decoded = chunk.decode_message(i).unwrap();
            assert_eq!(decoded, msgs[i as usize]);
        }
    }

    #[test]
    fn decode_chunk_frame_bytes_match_input() {
        let tmp = TempDir::new().unwrap();
        let msgs: Vec<SubscribeUpdate> = (0..3).map(|i| make_subscribe_update(i * 50)).collect();
        let mut raw_payloads: Vec<Vec<u8>> = Vec::new();
        for msg in &msgs {
            let mut buf = Vec::new();
            msg.encode(&mut buf).unwrap();
            raw_payloads.push(buf);
        }
        let zst_data = encode_chunk_zst(&msgs);
        let zst_path = write_zst_file(tmp.path(), "test.zst", &zst_data);

        let chunk = decode_chunk(&zst_path).unwrap();
        assert_eq!(chunk.len(), 3);
        for i in 0..3u32 {
            assert_eq!(chunk.frame(i).unwrap(), raw_payloads[i as usize].as_slice());
        }
    }

    #[test]
    fn decode_chunk_empty_stream_has_zero_frames() {
        let tmp = TempDir::new().unwrap();
        let framed = Vec::<u8>::new();
        let mut compressed = Vec::new();
        let mut encoder = zstd::stream::write::Encoder::new(&mut compressed, 3).unwrap();
        std::io::Write::write_all(&mut encoder, &framed).unwrap();
        encoder.finish().unwrap();
        let zst_path = write_zst_file(tmp.path(), "empty.zst", &compressed);

        let chunk = decode_chunk(&zst_path).unwrap();
        assert_eq!(chunk.len(), 0);
        assert!(chunk.is_empty());
    }

    #[test]
    fn decode_chunk_errs_on_truncated_zstd() {
        let tmp = TempDir::new().unwrap();
        let msgs = vec![make_subscribe_update(42)];
        let zst_data = encode_chunk_zst(&msgs);
        // Truncate the compressed data mid-stream
        let truncated = &zst_data[..zst_data.len() / 2];
        let zst_path = write_zst_file(tmp.path(), "truncated.zst", truncated);

        let result = decode_chunk(&zst_path);
        assert!(result.is_err(), "expected error for truncated zstd data");
    }

    #[test]
    fn decode_chunk_errs_on_truncated_final_frame() {
        let tmp = TempDir::new().unwrap();
        // Build valid framed data, then append a length prefix that promises more
        // bytes than exist.
        let msgs = vec![make_subscribe_update(1)];
        let mut framed = Vec::new();
        for msg in &msgs {
            let mut buf = Vec::new();
            msg.encode(&mut buf).unwrap();
            write_len_prefixed(&mut framed, &buf);
        }
        // Append a truncated length prefix (4 bytes) claiming 9999 bytes, but
        // only 2 bytes follow.
        framed.extend_from_slice(&9999u32.to_le_bytes());
        framed.extend_from_slice(&[0xAB, 0xCD]);

        let mut compressed = Vec::new();
        let mut encoder = zstd::stream::write::Encoder::new(&mut compressed, 3).unwrap();
        std::io::Write::write_all(&mut encoder, &framed).unwrap();
        encoder.finish().unwrap();
        let zst_path = write_zst_file(tmp.path(), "bad_frame.zst", &compressed);

        let result = decode_chunk(&zst_path);
        assert!(result.is_err(), "expected error for truncated final frame");
    }

    #[test]
    fn decode_message_out_of_range_returns_err() {
        let tmp = TempDir::new().unwrap();
        let msgs = vec![make_subscribe_update(0)];
        let zst_data = encode_chunk_zst(&msgs);
        let zst_path = write_zst_file(tmp.path(), "test.zst", &zst_data);

        let chunk = decode_chunk(&zst_path).unwrap();
        assert_eq!(chunk.len(), 1);

        let err = chunk.decode_message(1).unwrap_err();
        assert!(err.to_string().contains("out of range"));

        let err = chunk.decode_message(99).unwrap_err();
        assert!(err.to_string().contains("out of range"));
    }

    // --- ChunkCache tests ---

    fn make_cache_key(stream: Stream, start_slot: u64) -> CacheKey {
        CacheKey { stream, start_slot }
    }

    fn write_zst_with_messages(
        dir: &std::path::Path,
        name: &str,
        msgs: &[SubscribeUpdate],
    ) -> std::path::PathBuf {
        let zst_data = encode_chunk_zst(msgs);
        write_zst_file(dir, name, &zst_data)
    }

    #[test]
    fn cache_hit_does_not_redecode() {
        let tmp = TempDir::new().unwrap();
        let msgs: Vec<SubscribeUpdate> = (0..5).map(|i| make_subscribe_update(i * 100)).collect();
        let zst_path = write_zst_with_messages(tmp.path(), "chunk.zst", &msgs);

        let cache = ChunkCache::new(10 * 1024 * 1024);
        let key = make_cache_key(Stream::Tx, 0);

        let arc1 = cache.get_or_decode(key.clone(), &zst_path).unwrap();
        assert_eq!(arc1.len(), 5);

        // Delete the source file — proves second call doesn't re-read from disk.
        fs::remove_file(&zst_path).unwrap();

        let arc2 = cache.get_or_decode(key, &zst_path).unwrap();
        assert_eq!(arc2.len(), 5);

        // Both Arcs point to the same allocation.
        assert!(std::sync::Arc::ptr_eq(&arc1, &arc2));
    }

    #[test]
    fn cache_evicts_lru_when_over_budget() {
        let tmp = TempDir::new().unwrap();
        let msg = make_subscribe_update(0);
        let zst_a = write_zst_with_messages(tmp.path(), "a.zst", std::slice::from_ref(&msg));
        let zst_b = write_zst_with_messages(tmp.path(), "b.zst", std::slice::from_ref(&msg));
        let zst_c = write_zst_with_messages(tmp.path(), "c.zst", std::slice::from_ref(&msg));

        // Budget: enough for 2 chunks but not 3.
        let one_chunk_bytes = decode_chunk(&zst_a).unwrap().heap_bytes() as u64;
        let budget = one_chunk_bytes * 2 + 1;

        let cache = ChunkCache::new(budget);
        let key_a = make_cache_key(Stream::Tx, 0);
        let key_b = make_cache_key(Stream::Tx, 1000);
        let key_c = make_cache_key(Stream::Tx, 2000);

        cache.get_or_decode(key_a.clone(), &zst_a).unwrap();
        cache.get_or_decode(key_b.clone(), &zst_b).unwrap();
        // Inserting C should evict A (LRU).
        cache.get_or_decode(key_c.clone(), &zst_c).unwrap();

        assert_eq!(cache.len(), 2);
        assert!(cache.used_bytes() <= budget);
    }

    #[test]
    fn cache_access_bumps_recency() {
        let tmp = TempDir::new().unwrap();
        let msg = make_subscribe_update(0);
        let zst_a = write_zst_with_messages(tmp.path(), "a.zst", std::slice::from_ref(&msg));
        let zst_b = write_zst_with_messages(tmp.path(), "b.zst", std::slice::from_ref(&msg));
        let zst_c = write_zst_with_messages(tmp.path(), "c.zst", std::slice::from_ref(&msg));

        let one_chunk_bytes = decode_chunk(&zst_a).unwrap().heap_bytes() as u64;
        let budget = one_chunk_bytes * 2 + 1;

        let cache = ChunkCache::new(budget);
        let key_a = make_cache_key(Stream::Tx, 0);
        let key_b = make_cache_key(Stream::Tx, 1000);
        let key_c = make_cache_key(Stream::Tx, 2000);

        cache.get_or_decode(key_a.clone(), &zst_a).unwrap();
        cache.get_or_decode(key_b.clone(), &zst_b).unwrap();

        // Access A again — bumps its recency above B.
        cache.get_or_decode(key_a.clone(), &zst_a).unwrap();

        // Insert C — should evict B (the now-least-recently-used).
        cache.get_or_decode(key_c.clone(), &zst_c).unwrap();

        assert_eq!(cache.len(), 2);
        // A and C should be present; B should be evicted.
        // Verify by checking that A is still a hit (file deleted).
        fs::remove_file(&zst_a).unwrap();
        cache.get_or_decode(key_a, &zst_a).unwrap();
    }

    #[test]
    fn cache_oversized_chunk_is_cached_without_loop() {
        let tmp = TempDir::new().unwrap();
        let msgs: Vec<SubscribeUpdate> = (0..100).map(make_subscribe_update).collect();
        let zst_path = write_zst_with_messages(tmp.path(), "big.zst", &msgs);

        let chunk_bytes = decode_chunk(&zst_path).unwrap().heap_bytes() as u64;
        // Budget is tiny — far smaller than one chunk.
        let budget = 16;
        assert!(chunk_bytes > budget);

        let cache = ChunkCache::new(budget);
        let key = make_cache_key(Stream::Tx, 0);

        let result = cache.get_or_decode(key, &zst_path);
        assert!(result.is_ok(), "oversized chunk should still be cached");
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn cache_concurrent_get_or_decode_same_key() {
        use std::sync::Arc;
        use std::thread;

        let tmp = TempDir::new().unwrap();
        let msgs: Vec<SubscribeUpdate> = (0..10).map(|i| make_subscribe_update(i * 50)).collect();
        let zst_path = Arc::new(write_zst_with_messages(tmp.path(), "concurrent.zst", &msgs));

        let cache = Arc::new(ChunkCache::new(10 * 1024 * 1024));
        let key = Arc::new(make_cache_key(Stream::Tx, 0));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let cache = Arc::clone(&cache);
            let key = Arc::clone(&key);
            let path = Arc::clone(&zst_path);
            handles.push(thread::spawn(move || {
                cache.get_or_decode((*key).clone(), &path).unwrap()
            }));
        }

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        // All threads should get a valid Arc with the same data.
        for arc in &results {
            assert_eq!(arc.len(), 10);
        }
        // At least the first and last should point to the same allocation.
        assert!(
            std::sync::Arc::ptr_eq(&results[0], &results[results.len() - 1]),
            "concurrent decodes of the same key should share the same Arc"
        );
        assert_eq!(cache.len(), 1);
    }
}
