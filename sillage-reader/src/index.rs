use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use roaring::RoaringBitmap;
use sillage_common::idx::{DimValue, IdxHeader, IDX_MAGIC, IDX_VERSION};
use sillage_common::Stream;
use tracing::warn;

use crate::metrics;
use ::metrics::counter;

/// A parsed `.idx` file kept in memory with lazy per-value bitmap access.
#[derive(Debug)]
pub struct ChunkIndex {
    raw: Vec<u8>,
    header: IdxHeader,
    // dim_name -> (value -> (body_offset, body_length))
    lookup: HashMap<String, HashMap<DimValue, (u64, u64)>>,
    body_start: usize,
}

/// Parse a `.idx` file from disk into a `ChunkIndex`.
///
/// Validates magic, version, and header length before parsing.
/// Builds an in-memory lookup table for lazy bitmap deserialization.
pub fn parse_chunk_index(idx_path: &Path) -> anyhow::Result<ChunkIndex> {
    let raw = std::fs::read(idx_path)
        .with_context(|| format!("failed to read index file {}", idx_path.display()))?;

    if raw.len() < 9 {
        anyhow::bail!(
            "index file {} is too short ({} bytes, need at least 9)",
            idx_path.display(),
            raw.len()
        );
    }

    if &raw[0..4] != IDX_MAGIC {
        anyhow::bail!(
            "index file {} has wrong magic: expected {:?}, got {:?}",
            idx_path.display(),
            IDX_MAGIC,
            &raw[0..4]
        );
    }

    if raw[4] != IDX_VERSION {
        anyhow::bail!(
            "index file {} has wrong version: expected {}, got {}",
            idx_path.display(),
            IDX_VERSION,
            raw[4]
        );
    }

    let header_len = u32::from_le_bytes(raw[5..9].try_into().unwrap()) as usize;
    if raw.len() < 9 + header_len {
        anyhow::bail!(
            "index file {} header length {} exceeds file size {}",
            idx_path.display(),
            header_len,
            raw.len()
        );
    }

    let header: IdxHeader = rmp_serde::from_slice(&raw[9..9 + header_len])
        .with_context(|| format!("failed to parse index header in {}", idx_path.display()))?;

    let body_start = 9 + header_len;

    let mut lookup: HashMap<String, HashMap<DimValue, (u64, u64)>> = HashMap::new();
    for dim in &header.dimensions {
        let mut value_map: HashMap<DimValue, (u64, u64)> = HashMap::new();
        for entry in &dim.entries {
            value_map.insert(entry.value.clone(), (entry.offset, entry.length));
        }
        lookup.insert(dim.name.clone(), value_map);
    }

    Ok(ChunkIndex {
        raw,
        header,
        lookup,
        body_start,
    })
}

impl ChunkIndex {
    /// Number of messages indexed in this chunk.
    pub fn message_count(&self) -> u64 {
        self.header.message_count
    }

    /// Stream name (e.g. "tx", "acct", "block").
    pub fn stream(&self) -> &str {
        &self.header.stream
    }

    /// Iterator over dimension names in header order.
    pub fn dimensions(&self) -> impl Iterator<Item = &str> {
        self.header.dimensions.iter().map(|d| d.name.as_str())
    }

    /// Return all values for a given dimension, if it exists.
    pub fn dim_values(&self, dim: &str) -> Option<Vec<&DimValue>> {
        self.lookup.get(dim).map(|m| m.keys().collect())
    }

    /// Look up the bitmap for a specific (dimension, value) pair.
    ///
    /// Returns `Ok(None)` if the dimension or value is not present.
    /// Deserializes the roaring bitmap from the raw body on demand.
    pub fn bitmap_for(&self, dim: &str, value: &DimValue) -> anyhow::Result<Option<RoaringBitmap>> {
        let dim_map = match self.lookup.get(dim) {
            Some(m) => m,
            None => return Ok(None),
        };
        let (offset, length) = match dim_map.get(value) {
            Some(v) => *v,
            None => return Ok(None),
        };

        let body = &self.raw[self.body_start..];
        let start = offset as usize;
        let end = start + length as usize;
        if end > body.len() {
            anyhow::bail!(
                "bitmap slice [{}..{}) exceeds body length {}",
                start,
                end,
                body.len()
            );
        }
        let blob = &body[start..end];
        RoaringBitmap::deserialize_from(blob)
            .map(Some)
            .with_context(|| format!("failed to deserialize bitmap for dim={dim} value={value:?}"))
    }

    /// Approximate heap bytes used by this parsed index.
    pub fn heap_bytes(&self) -> usize {
        self.raw.len()
            + self.lookup.capacity()
                * (std::mem::size_of::<String>()
                    + std::mem::size_of::<HashMap<DimValue, (u64, u64)>>())
            + self
                .lookup
                .values()
                .map(|m| {
                    m.capacity()
                        * (std::mem::size_of::<DimValue>() + std::mem::size_of::<(u64, u64)>())
                })
                .sum::<usize>()
    }
}

// ---------------------------------------------------------------------------
// IndexCache — byte-budgeted hand-rolled LRU over ChunkIndex
// ---------------------------------------------------------------------------

/// Key for the index cache: (stream, start_slot).
#[derive(Hash, Eq, PartialEq, Clone)]
pub struct IndexCacheKey {
    pub stream: Stream,
    pub start_slot: u64,
}

struct IndexCacheInner {
    map: HashMap<IndexCacheKey, Arc<ChunkIndex>>,
    used: u64,
    tick: u64,
    last_used: HashMap<IndexCacheKey, u64>,
}

/// Byte-budgeted LRU cache of parsed chunk indexes.
///
/// Thread-safe via `Mutex` interior. On a cache miss, `get_or_parse`
/// parses the index file *outside* the lock so concurrent parses of
/// different keys don't serialize. Oversized single entries are cached
/// with a WARN log and never evicted into an infinite loop.
pub struct IndexCache {
    inner: std::sync::Mutex<IndexCacheInner>,
    budget_bytes: u64,
}

impl IndexCache {
    pub fn new(budget_bytes: u64) -> Self {
        Self {
            inner: std::sync::Mutex::new(IndexCacheInner {
                map: HashMap::new(),
                used: 0,
                tick: 0,
                last_used: HashMap::new(),
            }),
            budget_bytes,
        }
    }

    pub fn len(&self) -> usize {
        match self.inner.lock() {
            Ok(guard) => guard.map.len(),
            Err(poisoned) => poisoned.into_inner().map.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn used_bytes(&self) -> u64 {
        match self.inner.lock() {
            Ok(guard) => guard.used,
            Err(poisoned) => poisoned.into_inner().used,
        }
    }

    pub fn get_or_parse(
        &self,
        key: IndexCacheKey,
        idx_path: &Path,
    ) -> anyhow::Result<Arc<ChunkIndex>> {
        // Fast path: check cache under lock
        {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| anyhow::anyhow!("index cache lock poisoned: {e}"))?;
            if let Some(arc) = inner.map.get(&key) {
                let cloned = Arc::clone(arc);
                let tick = inner.tick;
                inner.tick = tick + 1;
                inner.last_used.insert(key, tick);
                counter!(metrics::INDEX_CACHE_HITS_TOTAL).increment(1);
                return Ok(cloned);
            }
        }

        // Slow path: parse outside the lock
        counter!(metrics::INDEX_CACHE_MISSES_TOTAL).increment(1);
        let parsed = parse_chunk_index(idx_path)?;
        let arc = Arc::new(parsed);
        let index_bytes = arc.heap_bytes() as u64;

        // Re-acquire lock and insert
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| anyhow::anyhow!("index cache lock poisoned: {e}"))?;

        // Another thread may have inserted the same key while we parsed.
        if let Some(existing) = inner.map.get(&key) {
            let cloned = Arc::clone(existing);
            let tick = inner.tick;
            inner.tick = tick + 1;
            inner.last_used.insert(key, tick);
            counter!(metrics::INDEX_CACHE_HITS_TOTAL).increment(1);
            return Ok(cloned);
        }

        let tick = inner.tick;
        inner.tick = tick + 1;
        inner.last_used.insert(key.clone(), tick);
        inner.map.insert(key, Arc::clone(&arc));
        inner.used += index_bytes;

        // Evict LRU entries until under budget
        while inner.used > self.budget_bytes && inner.map.len() > 1 {
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
            if let Some(removed_index) = removed {
                inner.used -= removed_index.heap_bytes() as u64;
            }
        }

        // If a single oversized index exceeds the budget, log a warning
        // but do NOT evict it into an infinite loop.
        if inner.used > self.budget_bytes && inner.map.len() == 1 {
            warn!(
                budget_bytes = self.budget_bytes,
                index_bytes = inner.used,
                "oversized index exceeds cache budget; cached anyway"
            );
        }

        Ok(arc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sillage_common::idx::{
        DimEntryHeader, DimValueType, DimensionHeader, IDX_MAGIC, IDX_VERSION,
    };
    use tempfile::TempDir;

    /// Helper: build a complete `.idx` byte buffer from header fields + body.
    fn build_idx_bytes(
        stream: &str,
        message_count: u64,
        dimensions: Vec<DimensionHeader>,
        body: Vec<u8>,
    ) -> Vec<u8> {
        let header = IdxHeader {
            stream: stream.to_string(),
            start_slot: 0,
            end_slot: 100,
            message_count,
            dimensions,
        };
        let header_bytes = rmp_serde::to_vec_named(&header).unwrap();
        let header_len = header_bytes.len() as u32;
        let mut buffer = Vec::with_capacity(9 + header_bytes.len() + body.len());
        buffer.extend_from_slice(IDX_MAGIC);
        buffer.push(IDX_VERSION);
        buffer.extend_from_slice(&header_len.to_le_bytes());
        buffer.extend_from_slice(&header_bytes);
        buffer.extend_from_slice(&body);
        buffer
    }

    /// Helper: write bytes to a temp file and return the path.
    fn write_temp_idx(dir: &TempDir, name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let path = dir.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }

    #[test]
    fn parse_rejects_truncated_file() {
        let dir = TempDir::new().unwrap();
        let path = write_temp_idx(&dir, "truncated.idx", &[0u8; 4]);
        let result = parse_chunk_index(&path);
        assert!(result.is_err(), "expected error for truncated file");
    }

    #[test]
    fn parse_rejects_wrong_magic() {
        let dir = TempDir::new().unwrap();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"BADX");
        bytes.push(IDX_VERSION);
        bytes.extend_from_slice(&0u32.to_le_bytes());
        let path = write_temp_idx(&dir, "bad_magic.idx", &bytes);
        let result = parse_chunk_index(&path);
        assert!(result.is_err(), "expected error for wrong magic");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("wrong magic"),
            "error should mention wrong magic: {err}"
        );
    }

    #[test]
    fn parse_rejects_wrong_version() {
        let dir = TempDir::new().unwrap();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(IDX_MAGIC);
        bytes.push(2);
        bytes.extend_from_slice(&0u32.to_le_bytes());
        let path = write_temp_idx(&dir, "bad_version.idx", &bytes);
        let result = parse_chunk_index(&path);
        assert!(result.is_err(), "expected error for wrong version");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("wrong version"),
            "error should mention version mismatch: {err}"
        );
    }

    #[test]
    fn parse_accepts_valid_empty_index() {
        let dir = TempDir::new().unwrap();
        let bytes = build_idx_bytes("tx", 0, vec![], vec![]);
        let path = write_temp_idx(&dir, "empty.idx", &bytes);
        let idx = parse_chunk_index(&path).expect("should parse valid empty index");
        assert_eq!(idx.message_count(), 0);
        assert_eq!(idx.stream(), "tx");
        let dims: Vec<&str> = idx.dimensions().collect();
        assert!(dims.is_empty());
    }

    #[test]
    fn bitmap_for_returns_correct_bitmap() {
        let dir = TempDir::new().unwrap();

        // Build a roaring bitmap with ordinals 0 and 3
        let mut rb = RoaringBitmap::new();
        rb.insert(0);
        rb.insert(3);
        let mut body = Vec::new();
        rb.serialize_into(&mut body).unwrap();

        let offset = 0u64;
        let length = body.len() as u64;

        let key = DimValue::Bytes(vec![1, 2, 3, 4]);
        let dimensions = vec![DimensionHeader {
            name: "account_key".to_string(),
            value_type: DimValueType::Pubkey32,
            entries: vec![DimEntryHeader {
                value: key.clone(),
                offset,
                length,
            }],
        }];

        let bytes = build_idx_bytes("tx", 2, dimensions, body);
        let path = write_temp_idx(&dir, "bitmap.idx", &bytes);
        let idx = parse_chunk_index(&path).expect("should parse index with bitmap");

        let result = idx
            .bitmap_for("account_key", &key)
            .expect("bitmap_for should succeed")
            .expect("should find bitmap for known dim+value");

        assert!(result.contains(0), "bitmap should contain ordinal 0");
        assert!(result.contains(3), "bitmap should contain ordinal 3");
        assert!(!result.contains(1), "bitmap should not contain ordinal 1");
    }

    #[test]
    fn bitmap_for_unknown_dim_returns_none() {
        let dir = TempDir::new().unwrap();
        let bytes = build_idx_bytes("tx", 0, vec![], vec![]);
        let path = write_temp_idx(&dir, "empty_for_dim.idx", &bytes);
        let idx = parse_chunk_index(&path).expect("should parse");

        let result = idx
            .bitmap_for("nonexistent", &DimValue::U64(0))
            .expect("bitmap_for should succeed on unknown dim");
        assert!(result.is_none(), "unknown dim should return None");
    }

    #[test]
    fn bitmap_for_unknown_value_returns_none() {
        let dir = TempDir::new().unwrap();

        // Build an index with one dimension that has one entry
        let mut rb = RoaringBitmap::new();
        rb.insert(0);
        let mut body = Vec::new();
        rb.serialize_into(&mut body).unwrap();

        let known_key = DimValue::Bytes(vec![1, 2, 3, 4]);
        let dimensions = vec![DimensionHeader {
            name: "account_key".to_string(),
            value_type: DimValueType::Pubkey32,
            entries: vec![DimEntryHeader {
                value: known_key,
                offset: 0,
                length: body.len() as u64,
            }],
        }];

        let bytes = build_idx_bytes("tx", 1, dimensions, body);
        let path = write_temp_idx(&dir, "known_dim.idx", &bytes);
        let idx = parse_chunk_index(&path).expect("should parse");

        let wrong_value = DimValue::Bytes(vec![9, 9, 9, 9]);
        let result = idx
            .bitmap_for("account_key", &wrong_value)
            .expect("bitmap_for should succeed on unknown value");
        assert!(result.is_none(), "unknown value should return None");
    }

    // --- IndexCache tests ---

    fn make_index_cache_key(stream: Stream, start_slot: u64) -> IndexCacheKey {
        IndexCacheKey { stream, start_slot }
    }

    #[test]
    fn index_cache_hit_does_not_reparse() {
        let dir = TempDir::new().unwrap();
        let bytes = build_idx_bytes("tx", 5, vec![], vec![]);
        let path = write_temp_idx(&dir, "cache_hit.idx", &bytes);

        let cache = IndexCache::new(10 * 1024 * 1024);
        let key = make_index_cache_key(Stream::Tx, 0);

        let arc1 = cache.get_or_parse(key.clone(), &path).unwrap();
        assert_eq!(arc1.message_count(), 5);

        std::fs::remove_file(&path).unwrap();

        let arc2 = cache.get_or_parse(key, &path).unwrap();
        assert_eq!(arc2.message_count(), 5);
        assert!(Arc::ptr_eq(&arc1, &arc2));
    }

    #[test]
    fn index_cache_evicts_lru_when_over_budget() {
        let dir = TempDir::new().unwrap();
        let bytes_a = build_idx_bytes("tx", 1, vec![], vec![]);
        let bytes_b = build_idx_bytes("tx", 1, vec![], vec![]);
        let bytes_c = build_idx_bytes("tx", 1, vec![], vec![]);
        let path_a = write_temp_idx(&dir, "a.idx", &bytes_a);
        let path_b = write_temp_idx(&dir, "b.idx", &bytes_b);
        let path_c = write_temp_idx(&dir, "c.idx", &bytes_c);

        let one_index_bytes = parse_chunk_index(&path_a).unwrap().heap_bytes() as u64;
        let budget = one_index_bytes * 2 + 1;

        let cache = IndexCache::new(budget);
        let key_a = make_index_cache_key(Stream::Tx, 0);
        let key_b = make_index_cache_key(Stream::Tx, 1000);
        let key_c = make_index_cache_key(Stream::Tx, 2000);

        cache.get_or_parse(key_a, &path_a).unwrap();
        cache.get_or_parse(key_b, &path_b).unwrap();
        cache.get_or_parse(key_c, &path_c).unwrap();

        assert_eq!(cache.len(), 2);
        assert!(cache.used_bytes() <= budget);
    }

    #[test]
    fn index_cache_concurrent_get_or_parse_same_key() {
        use std::sync::Arc;
        use std::thread;

        let dir = TempDir::new().unwrap();
        let bytes = build_idx_bytes("tx", 10, vec![], vec![]);
        let path = Arc::new(write_temp_idx(&dir, "concurrent.idx", &bytes));

        let cache = Arc::new(IndexCache::new(10 * 1024 * 1024));
        let key = Arc::new(make_index_cache_key(Stream::Tx, 0));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let cache = Arc::clone(&cache);
            let key = Arc::clone(&key);
            let path = Arc::clone(&path);
            handles.push(thread::spawn(move || {
                cache.get_or_parse((*key).clone(), &path).unwrap()
            }));
        }

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        for arc in &results {
            assert_eq!(arc.message_count(), 10);
        }
        assert!(
            Arc::ptr_eq(&results[0], &results[results.len() - 1]),
            "concurrent parses of the same key should share the same Arc"
        );
        assert_eq!(cache.len(), 1);
    }
}
