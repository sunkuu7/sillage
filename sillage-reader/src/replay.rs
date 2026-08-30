use std::sync::Arc;

use crate::filter::{filter_acct, filter_block, filter_tx};
use crate::index::IndexCache;
use crate::metrics;
use crate::pacing::{LagAction, Pacer};
use crate::storage::{CacheKey, ChunkCache, ChunkCatalog, DecodedChunk};
use crate::subscription::SubscriptionFilters;
use ::metrics::{counter, histogram};
use prost::Message;
use roaring::RoaringBitmap;
use sillage_common::slot::extract_slot;
use sillage_common::Stream;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;

pub struct ChunkPlan {
    pub stream: Stream,
    pub entry: crate::storage::ChunkEntry,
    pub named_bitmaps: Vec<(String, RoaringBitmap)>,
    pub union: RoaringBitmap,
    /// Chunk-level fallback when `SubscribeUpdate.created_at` is `None`.
    /// Populated from `entry.meta.recv_ns_first` at plan time.
    pub chunk_recv_ns_first: Option<u64>,
}

pub struct ReplayPlan {
    pub from_slot: u64,
    pub to_slot_exclusive: u64,
    pub plans_per_stream: [Vec<ChunkPlan>; 3],
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReplayStats {
    pub sent: u64,
    pub bytes: u64,
    pub lag_max_ms: f64,
    /// Slot of the last message actually handed to the client. A follower
    /// resumes from `last_slot + 1`.
    pub last_slot: Option<u64>,
    /// True when the plan was consumed to the end. False means the replay
    /// stopped early — shutdown, a dropped subscriber, or a lag drop — in which
    /// case there is no point re-planning for more.
    pub drained: bool,
}

pub struct StreamCursor {
    plans: Vec<ChunkPlan>,
    plan_idx: usize,
    current_decoded: Option<Arc<DecodedChunk>>,
    current_ords: Option<roaring::bitmap::IntoIter>,
    peeked: Option<(u64, SubscribeUpdate, Vec<String>, Option<u64>)>,
}

impl StreamCursor {
    pub fn new(plans: Vec<ChunkPlan>) -> Self {
        Self {
            plans,
            plan_idx: 0,
            current_decoded: None,
            current_ords: None,
            peeked: None,
        }
    }

    pub async fn peek(
        &mut self,
        chunk_cache: &ChunkCache,
    ) -> anyhow::Result<Option<&(u64, SubscribeUpdate, Vec<String>, Option<u64>)>> {
        if self.peeked.is_some() {
            return Ok(self.peeked.as_ref());
        }

        loop {
            let needs_new_plan = match &self.current_ords {
                None => true,
                Some(ords) => ords.len() == 0,
            };

            if needs_new_plan {
                loop {
                    if self.plan_idx >= self.plans.len() {
                        return Ok(None);
                    }

                    let plan = &self.plans[self.plan_idx];
                    self.plan_idx += 1;

                    if plan.union.is_empty() {
                        continue;
                    }

                    let key = CacheKey {
                        stream: plan.stream,
                        start_slot: plan.entry.start_slot,
                    };
                    let decoded = chunk_cache.get_or_decode(key, &plan.entry.zst_path)?;
                    self.current_decoded = Some(decoded);
                    self.current_ords = Some(plan.union.clone().into_iter());
                    break;
                }
            }

            let ord = match self.current_ords.as_mut().and_then(|ords| ords.next()) {
                Some(ord) => ord,
                None => continue,
            };

            let decoded = self
                .current_decoded
                .as_ref()
                .expect("current_decoded must be set when current_ords is active");

            let msg = decoded.decode_message(ord)?;
            let slot = extract_slot(&msg).unwrap_or(0);

            let plan = &self.plans[self.plan_idx - 1];
            let name_matches: Vec<String> = plan
                .named_bitmaps
                .iter()
                .filter(|(_, b)| b.contains(ord))
                .map(|(n, _)| n.clone())
                .collect();

            let recv_ns = extract_recv_ns(&msg, &plan.entry.meta, ord);

            self.peeked = Some((slot, msg, name_matches, recv_ns));
            return Ok(self.peeked.as_ref());
        }
    }

    pub fn take_peeked(&mut self) -> Option<(u64, SubscribeUpdate, Vec<String>, Option<u64>)> {
        self.peeked.take()
    }
}

pub fn set_filters(mut update: SubscribeUpdate, names: Vec<String>) -> SubscribeUpdate {
    update.filters = names;
    update
}

pub fn extract_recv_ns(
    msg: &SubscribeUpdate,
    chunk_meta: &sillage_common::chunk::ChunkMeta,
    ord_in_chunk: u32,
) -> Option<u64> {
    if let Some(ts) = msg.created_at.as_ref() {
        if ts.seconds >= 0 {
            return Some((ts.seconds as u64) * 1_000_000_000 + (ts.nanos as u64));
        }
    }
    let first = chunk_meta.recv_ns_first?;
    let last = chunk_meta.recv_ns_last.unwrap_or(first);
    let count = chunk_meta.message_count;
    if count <= 1 || last == first {
        return Some(first);
    }
    let span = last - first;
    let frac_nanos = (ord_in_chunk as u128) * (span as u128) / ((count - 1) as u128);
    Some(first + frac_nanos as u64)
}

fn make_stats(
    emitted: u64,
    lag_max: std::time::Duration,
    bytes: u64,
    last_slot: Option<u64>,
    drained: bool,
) -> ReplayStats {
    ReplayStats {
        sent: emitted,
        lag_max_ms: lag_max.as_secs_f64() * 1000.0,
        bytes,
        last_slot,
        drained,
    }
}

pub async fn drive_replay(
    mut plan: ReplayPlan,
    chunk_cache: &ChunkCache,
    pacer: &mut Pacer,
    tx: tokio::sync::mpsc::Sender<Result<SubscribeUpdate, tonic::Status>>,
    shutdown: sillage_common::ShutdownSignal,
) -> anyhow::Result<ReplayStats> {
    // A chunk is planned when it *overlaps* the requested range, so the chunk
    // holding `from_slot` also holds earlier messages. Without this floor an
    // explicit from_slot replays the whole chunk it lands in, and a follower
    // resuming at `last_slot + 1` re-sends what it already delivered — forever.
    let from_slot = plan.from_slot;
    let mut cursors: [Option<StreamCursor>; 3] = [
        if plan.plans_per_stream[0].is_empty() {
            None
        } else {
            Some(StreamCursor::new(std::mem::take(
                &mut plan.plans_per_stream[0],
            )))
        },
        if plan.plans_per_stream[1].is_empty() {
            None
        } else {
            Some(StreamCursor::new(std::mem::take(
                &mut plan.plans_per_stream[1],
            )))
        },
        if plan.plans_per_stream[2].is_empty() {
            None
        } else {
            Some(StreamCursor::new(std::mem::take(
                &mut plan.plans_per_stream[2],
            )))
        },
    ];

    let mut emitted: u64 = 0;
    let mut bytes: u64 = 0;
    let mut last_slot: Option<u64> = None;
    let mut lag_max = std::time::Duration::ZERO;
    let mut lag_sum = std::time::Duration::ZERO;

    loop {
        let mut best: Option<(usize, u64)> = None;

        for (i, cursor_opt) in cursors.iter_mut().enumerate() {
            let cursor = match cursor_opt {
                Some(c) => c,
                None => continue,
            };

            match cursor.peek(chunk_cache).await {
                Ok(Some((slot, _, _, _))) => {
                    let candidate = (*slot, i);
                    if best.is_none() || candidate < (best.unwrap().1, best.unwrap().0) {
                        best = Some((i, *slot));
                    }
                }
                Ok(None) => continue,
                Err(e) => {
                    let _ = tx.send(Err(tonic::Status::internal(e.to_string()))).await;
                    return Err(e);
                }
            }
        }

        let (winner_idx, winner_slot) = match best {
            Some((idx, slot)) => (idx, slot),
            None => break,
        };

        let (_, update, names, recv_ns) = cursors[winner_idx]
            .as_mut()
            .expect("winner cursor must exist")
            .take_peeked()
            .expect("peeked value must exist after successful peek");

        // Below the requested start: consume it off the cursor and move on.
        if winner_slot < from_slot {
            continue;
        }

        let filtered = set_filters(update, names);

        let target = pacer.target(recv_ns);
        // Scheduling lag: how far past the target emit time we already are at
        // dequeue, before sleeping/sending. observe_emit() below measures the
        // post-send lag used for the warn/drop policy.
        let now = tokio::time::Instant::now();
        if now > target {
            histogram!(metrics::REPLAY_LAG_SECONDS).record((now - target).as_secs_f64());
        }
        tokio::select! {
            _ = tokio::time::sleep_until(target) => {}
            _ = shutdown.cancelled() => {
                if emitted > 0 {
                    let lag_avg_ms = lag_sum.as_secs_f64() * 1000.0 / emitted as f64;
                    tracing::info!(
                        emitted,
                        lag_max_ms = lag_max.as_secs_f64() * 1000.0,
                        lag_avg_ms,
                        "replay paced"
                    );
                }
                return Ok(make_stats(emitted, lag_max, bytes, last_slot, false));
            }
        }

        let msg_bytes = filtered.encoded_len() as u64;
        match tokio::time::timeout(pacer.lag_drop(), tx.send(Ok(filtered))).await {
            Err(_elapsed) => {
                tracing::warn!(emitted, "replay send timed out; dropping subscriber");
                counter!(metrics::REPLAY_DROPPED_TOTAL).increment(1);
                let _ = tx
                    .send(Err(tonic::Status::resource_exhausted(
                        "replay send timed out",
                    )))
                    .await;
                if emitted > 0 {
                    let lag_avg_ms = lag_sum.as_secs_f64() * 1000.0 / emitted as f64;
                    tracing::info!(
                        emitted,
                        lag_max_ms = lag_max.as_secs_f64() * 1000.0,
                        lag_avg_ms,
                        "replay paced"
                    );
                }
                return Ok(make_stats(emitted, lag_max, bytes, last_slot, false));
            }
            Ok(Err(_)) => {
                if emitted > 0 {
                    let lag_avg_ms = lag_sum.as_secs_f64() * 1000.0 / emitted as f64;
                    tracing::info!(
                        emitted,
                        lag_max_ms = lag_max.as_secs_f64() * 1000.0,
                        lag_avg_ms,
                        "replay paced"
                    );
                }
                return Ok(make_stats(emitted, lag_max, bytes, last_slot, false));
            }
            Ok(Ok(())) => {
                bytes += msg_bytes;
                last_slot = Some(winner_slot);
                counter!(metrics::MESSAGES_SENT_TOTAL).increment(1);
                counter!(metrics::BYTES_SENT_TOTAL).increment(msg_bytes);
            }
        }

        match pacer.observe_emit(target) {
            LagAction::Drop { lag } => {
                tracing::warn!(?lag, "replay lag exceeded drop threshold");
                counter!(metrics::REPLAY_DROPPED_TOTAL).increment(1);
                let _ = tx
                    .send(Err(tonic::Status::resource_exhausted(
                        "replay fell too far behind wall-clock",
                    )))
                    .await;
                if emitted > 0 {
                    let lag_avg_ms = lag_sum.as_secs_f64() * 1000.0 / emitted as f64;
                    tracing::info!(
                        emitted,
                        lag_max_ms = lag_max.as_secs_f64() * 1000.0,
                        lag_avg_ms,
                        "replay paced"
                    );
                }
                return Ok(make_stats(emitted, lag_max, bytes, last_slot, false));
            }
            LagAction::Warn { lag } => {
                lag_max = lag_max.max(lag);
                lag_sum += lag;
                tracing::warn!(?lag, "replay lagging behind wall-clock");
            }
            LagAction::Ok => {}
        }

        emitted += 1;
    }

    if emitted > 0 {
        let lag_avg_ms = lag_sum.as_secs_f64() * 1000.0 / emitted as f64;
        tracing::info!(
            emitted,
            lag_max_ms = lag_max.as_secs_f64() * 1000.0,
            lag_avg_ms,
            "replay paced"
        );
    }
    Ok(make_stats(emitted, lag_max, bytes, last_slot, true))
}

/// Why a replay could not be planned.
#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    /// The client asked to start before the oldest slot still held locally.
    /// Clients can discover the floor via the `SubscribeReplayInfo` RPC.
    #[error(
        "from_slot {requested} is older than the oldest available slot {first_available}; \
         query SubscribeReplayInfo for the current first_available"
    )]
    FromSlotTooOld {
        requested: u64,
        first_available: u64,
    },
    /// Nothing is held locally for the streams this subscription asked for —
    /// either the catalog is empty (a cold reader still hydrating from R2) or
    /// the requested streams have no chunks. Serving an empty stream instead
    /// would leave the client waiting on data that is never coming.
    #[error("no chunks available locally for the requested streams")]
    NoChunksAvailable,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Resolve the customer's slot range and build the per-stream chunk plans.
///
/// A slot number is an opaque id, not a duration: the replay window is defined
/// entirely by what the catalog currently holds. Omitting `from_slot` starts at
/// the oldest slot available for the subscribed streams — the syncer's
/// retention policy is what makes that "about 24h", and the replay path never
/// converts hours to slots itself. Asking for a slot below that floor is an
/// error rather than a silent fast-forward, and having nothing to serve at all
/// is an error rather than an empty stream that never ends.
pub fn plan_replay(
    catalog: &ChunkCatalog,
    parsed: &SubscriptionFilters,
    idx_cache: &IndexCache,
    explicit_from_slot: Option<u64>,
) -> Result<ReplayPlan, PlanError> {
    // Oldest slot held for the streams this subscription actually asked for.
    // `None` covers both an empty catalog and a catalog with chunks only for
    // streams the client did not subscribe to.
    let first_available = catalog
        .summary()
        .per_stream
        .iter()
        .filter(|(stream, _, _, _)| parsed.has_stream(*stream))
        .filter_map(|(_, _, min_start, _)| *min_start)
        .min();

    let first_available = match first_available {
        Some(floor) => floor,
        None => return Err(PlanError::NoChunksAvailable),
    };

    let to_slot_exclusive = catalog.newest_end_slot().unwrap_or(0);

    let from_slot = match explicit_from_slot {
        Some(requested) if requested < first_available => {
            return Err(PlanError::FromSlotTooOld {
                requested,
                first_available,
            })
        }
        Some(requested) => requested,
        None => first_available,
    };

    let streams = Stream::all();
    let mut plans_per_stream: [Vec<ChunkPlan>; 3] = [Vec::new(), Vec::new(), Vec::new()];

    for (i, &stream) in streams.iter().enumerate() {
        if !parsed.has_stream(stream) {
            continue;
        }

        let entries = catalog.chunks_in_range(stream, from_slot, to_slot_exclusive);
        for entry in entries {
            let key = crate::index::IndexCacheKey {
                stream,
                start_slot: entry.start_slot,
            };
            let idx = idx_cache.get_or_parse(key, &entry.idx_path)?;

            let named_bitmaps: Vec<(String, RoaringBitmap)> = match stream {
                Stream::Tx => parsed
                    .transactions
                    .iter()
                    .map(|(name, filter)| (name.clone(), filter_tx(&idx, filter)))
                    .collect(),
                Stream::Acct => parsed
                    .accounts
                    .iter()
                    .map(|(name, filter)| (name.clone(), filter_acct(&idx, filter)))
                    .collect(),
                Stream::Block => parsed
                    .blocks_meta
                    .iter()
                    .map(|(name, filter)| (name.clone(), filter_block(&idx, filter)))
                    .collect(),
            };

            let union = named_bitmaps
                .iter()
                .fold(RoaringBitmap::new(), |acc, (_, b)| acc | b);

            plans_per_stream[i].push(ChunkPlan {
                stream,
                entry: entry.clone(),
                named_bitmaps,
                union,
                chunk_recv_ns_first: entry.meta.recv_ns_first,
            });
        }
    }

    Ok(ReplayPlan {
        from_slot,
        to_slot_exclusive,
        plans_per_stream,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ChunkCatalog;
    use sillage_common::chunk::{ChunkMeta, SCHEMA_VERSION};
    use sillage_common::config::PacingConfig;
    use sillage_common::idx::{
        DimEntryHeader, DimValueType, DimensionHeader, IdxHeader, IDX_MAGIC, IDX_VERSION,
    };
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;
    use yellowstone_grpc_proto::geyser::{
        SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterTransactions,
    };
    use yellowstone_grpc_proto::prost_types::Timestamp;

    fn make_meta(stream: &str, start: u64, end: u64) -> ChunkMeta {
        ChunkMeta {
            schema_version: SCHEMA_VERSION,
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

    fn write_trio(
        dir: &Path,
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

    fn write_trio_with_idx(
        dir: &Path,
        stream: Stream,
        start_slot: u64,
        end_slot_exclusive: u64,
        idx_bytes: &[u8],
    ) {
        let stream_dir = dir.join("chunks").join(stream.as_str());
        fs::create_dir_all(&stream_dir).unwrap();
        let stem = format!("{:012}-{:012}", start_slot, end_slot_exclusive);
        let zst_path = stream_dir.join(format!("{stem}.zst"));
        let idx_path = stream_dir.join(format!("{stem}.idx"));
        let meta_path = stream_dir.join(format!("{stem}.meta.json"));
        fs::write(&zst_path, b"compressed-data").unwrap();
        fs::write(&idx_path, idx_bytes).unwrap();
        let meta = make_meta(stream.as_str(), start_slot, end_slot_exclusive);
        let json = serde_json::to_string(&meta).unwrap();
        fs::write(&meta_path, json).unwrap();
    }

    fn single_dim(
        dim_name: &str,
        value: sillage_common::idx::DimValue,
        bitmap: &RoaringBitmap,
        body: &mut Vec<u8>,
    ) -> DimensionHeader {
        let offset = body.len() as u64;
        bitmap.serialize_into(&mut *body).unwrap();
        let length = body.len() as u64 - offset;
        DimensionHeader {
            name: dim_name.to_string(),
            value_type: DimValueType::Pubkey32,
            entries: vec![DimEntryHeader {
                value,
                offset,
                length,
            }],
        }
    }

    fn pk_bytes(seed: u8) -> Vec<u8> {
        vec![seed; 32]
    }

    fn pk_base58(seed: u8) -> String {
        bs58::encode(pk_bytes(seed)).into_string()
    }

    fn make_subscription_filters_tx(
        filters: Vec<(String, SubscribeRequestFilterTransactions)>,
    ) -> SubscriptionFilters {
        SubscriptionFilters {
            transactions: filters,
            accounts: vec![],
            blocks_meta: vec![],
            from_slot: None,
        }
    }

    fn make_subscription_filters_acct(
        filters: Vec<(String, SubscribeRequestFilterAccounts)>,
    ) -> SubscriptionFilters {
        SubscriptionFilters {
            transactions: vec![],
            accounts: filters,
            blocks_meta: vec![],
            from_slot: None,
        }
    }

    fn make_subscription_filters_block(
        filters: Vec<(String, SubscribeRequestFilterBlocksMeta)>,
    ) -> SubscriptionFilters {
        SubscriptionFilters {
            transactions: vec![],
            accounts: vec![],
            blocks_meta: filters,
            from_slot: None,
        }
    }

    /// A cold reader must say so rather than hand back a stream that never
    /// produces a message.
    #[test]
    fn empty_catalog_is_rejected() {
        let tmp = TempDir::new().unwrap();
        let catalog = ChunkCatalog::scan(tmp.path());
        let idx_cache = IndexCache::new(10 * 1024 * 1024);
        let parsed = make_subscription_filters_tx(vec![(
            "sub1".to_string(),
            SubscribeRequestFilterTransactions::default(),
        )]);

        let err = plan_replay(&catalog, &parsed, &idx_cache, None)
            .err()
            .expect("empty catalog must be rejected");
        assert!(matches!(err, PlanError::NoChunksAvailable));
    }

    /// Chunks exist, but none for the stream this client subscribed to — same
    /// user-visible outcome as an empty catalog, so the same rejection.
    #[test]
    fn no_chunks_for_subscribed_stream_is_rejected() {
        let tmp = TempDir::new().unwrap();
        let acct_idx = build_idx_bytes("acct", 5, vec![], vec![]);
        write_trio_with_idx(tmp.path(), Stream::Acct, 1000, 2000, &acct_idx);

        let catalog = ChunkCatalog::scan(tmp.path());
        let idx_cache = IndexCache::new(10 * 1024 * 1024);
        let parsed = make_subscription_filters_tx(vec![(
            "sub1".to_string(),
            SubscribeRequestFilterTransactions::default(),
        )]);

        let err = plan_replay(&catalog, &parsed, &idx_cache, None)
            .err()
            .expect("tx subscription with only acct chunks must be rejected");
        assert!(matches!(err, PlanError::NoChunksAvailable));
    }

    /// A tx-only subscription leaves the acct and block lanes of the plan empty.
    #[test]
    fn unsubscribed_streams_get_empty_plans() {
        let tmp = TempDir::new().unwrap();
        let idx_bytes = build_idx_bytes("tx", 5, vec![], vec![]);
        write_trio_with_idx(tmp.path(), Stream::Tx, 0, 1000, &idx_bytes);
        let catalog = ChunkCatalog::scan(tmp.path());
        let idx_cache = IndexCache::new(10 * 1024 * 1024);
        let parsed = make_subscription_filters_tx(vec![(
            "sub1".to_string(),
            SubscribeRequestFilterTransactions::default(),
        )]);

        let plan = plan_replay(&catalog, &parsed, &idx_cache, None).unwrap();
        assert_eq!(plan.from_slot, 0);
        assert_eq!(plan.to_slot_exclusive, 1000);
        assert_eq!(plan.plans_per_stream[0].len(), 1);
        assert!(plan.plans_per_stream[1].is_empty());
        assert!(plan.plans_per_stream[2].is_empty());
    }

    #[test]
    fn explicit_from_slot_is_honored() {
        let tmp = TempDir::new().unwrap();
        let idx_bytes = build_idx_bytes("tx", 10, vec![], vec![]);
        write_trio_with_idx(tmp.path(), Stream::Tx, 0, 1000, &idx_bytes);
        write_trio_with_idx(tmp.path(), Stream::Tx, 1000, 2000, &idx_bytes);

        let catalog = ChunkCatalog::scan(tmp.path());
        let idx_cache = IndexCache::new(10 * 1024 * 1024);
        let parsed = make_subscription_filters_tx(vec![(
            "sub1".to_string(),
            SubscribeRequestFilterTransactions::default(),
        )]);

        let plan = plan_replay(&catalog, &parsed, &idx_cache, Some(1000)).unwrap();
        assert_eq!(plan.from_slot, 1000);
        assert_eq!(plan.to_slot_exclusive, 2000);
        assert_eq!(plan.plans_per_stream[0].len(), 1);
        assert_eq!(plan.plans_per_stream[0][0].entry.start_slot, 1000);
    }

    #[test]
    /// Omitting `from_slot` means "everything you have" — the oldest retained
    /// slot, not a duration-derived offset from the newest.
    fn default_from_slot_starts_at_oldest_available() {
        let tmp = TempDir::new().unwrap();
        let idx_bytes = build_idx_bytes("tx", 10, vec![], vec![]);
        write_trio_with_idx(tmp.path(), Stream::Tx, 1000, 2000, &idx_bytes);
        write_trio_with_idx(tmp.path(), Stream::Tx, 2000, 3000, &idx_bytes);
        write_trio_with_idx(tmp.path(), Stream::Tx, 3000, 4000, &idx_bytes);

        let catalog = ChunkCatalog::scan(tmp.path());
        let idx_cache = IndexCache::new(10 * 1024 * 1024);
        let parsed = make_subscription_filters_tx(vec![(
            "sub1".to_string(),
            SubscribeRequestFilterTransactions::default(),
        )]);

        let plan = plan_replay(&catalog, &parsed, &idx_cache, None).unwrap();
        assert_eq!(plan.from_slot, 1000, "should start at the oldest chunk");
        assert_eq!(plan.to_slot_exclusive, 4000);
        assert_eq!(plan.plans_per_stream[0].len(), 3, "all chunks replayed");
    }

    #[test]
    fn from_slot_below_retained_window_is_rejected() {
        let tmp = TempDir::new().unwrap();
        let idx_bytes = build_idx_bytes("tx", 10, vec![], vec![]);
        write_trio_with_idx(tmp.path(), Stream::Tx, 2000, 3000, &idx_bytes);

        let catalog = ChunkCatalog::scan(tmp.path());
        let idx_cache = IndexCache::new(10 * 1024 * 1024);
        let parsed = make_subscription_filters_tx(vec![(
            "sub1".to_string(),
            SubscribeRequestFilterTransactions::default(),
        )]);

        let result = plan_replay(&catalog, &parsed, &idx_cache, Some(1999));
        match result.err().expect("slot below the floor must be rejected") {
            PlanError::FromSlotTooOld {
                requested,
                first_available,
            } => {
                assert_eq!(requested, 1999);
                assert_eq!(first_available, 2000);
            }
            other => panic!("expected FromSlotTooOld, got {other:?}"),
        }
    }

    #[test]
    fn from_slot_at_the_floor_is_accepted() {
        let tmp = TempDir::new().unwrap();
        let idx_bytes = build_idx_bytes("tx", 10, vec![], vec![]);
        write_trio_with_idx(tmp.path(), Stream::Tx, 2000, 3000, &idx_bytes);

        let catalog = ChunkCatalog::scan(tmp.path());
        let idx_cache = IndexCache::new(10 * 1024 * 1024);
        let parsed = make_subscription_filters_tx(vec![(
            "sub1".to_string(),
            SubscribeRequestFilterTransactions::default(),
        )]);

        let plan = plan_replay(&catalog, &parsed, &idx_cache, Some(2000)).unwrap();
        assert_eq!(plan.from_slot, 2000);
        assert_eq!(plan.plans_per_stream[0].len(), 1);
    }

    /// The floor is per-subscription: a client asking only for `tx` must not be
    /// held to a floor set by an older `acct` chunk it never asked for.
    #[test]
    fn floor_is_scoped_to_subscribed_streams() {
        let tmp = TempDir::new().unwrap();
        let tx_idx = build_idx_bytes("tx", 10, vec![], vec![]);
        let acct_idx = build_idx_bytes("acct", 10, vec![], vec![]);
        write_trio_with_idx(tmp.path(), Stream::Acct, 1000, 2000, &acct_idx);
        write_trio_with_idx(tmp.path(), Stream::Tx, 3000, 4000, &tx_idx);

        let catalog = ChunkCatalog::scan(tmp.path());
        let idx_cache = IndexCache::new(10 * 1024 * 1024);
        let parsed = make_subscription_filters_tx(vec![(
            "sub1".to_string(),
            SubscribeRequestFilterTransactions::default(),
        )]);

        // tx-only subscription: floor is the tx chunk at 3000, not acct's 1000.
        let plan = plan_replay(&catalog, &parsed, &idx_cache, None).unwrap();
        assert_eq!(plan.from_slot, 3000);

        let err = plan_replay(&catalog, &parsed, &idx_cache, Some(1500))
            .err()
            .expect("1500 is below the tx floor even though acct holds it");
        assert!(matches!(
            err,
            PlanError::FromSlotTooOld {
                first_available: 3000,
                ..
            }
        ));
    }

    #[test]
    fn range_outside_catalog_yields_empty_plans() {
        let tmp = TempDir::new().unwrap();
        let idx_bytes = build_idx_bytes("tx", 10, vec![], vec![]);
        write_trio_with_idx(tmp.path(), Stream::Tx, 0, 1000, &idx_bytes);

        let catalog = ChunkCatalog::scan(tmp.path());
        let idx_cache = IndexCache::new(10 * 1024 * 1024);
        let parsed = make_subscription_filters_tx(vec![(
            "sub1".to_string(),
            SubscribeRequestFilterTransactions::default(),
        )]);

        let plan = plan_replay(&catalog, &parsed, &idx_cache, Some(5000)).unwrap();
        assert_eq!(plan.from_slot, 5000);
        assert_eq!(plan.to_slot_exclusive, 1000);
        assert!(plan.plans_per_stream[0].is_empty());
    }

    #[test]
    fn single_sub_bitmap() {
        let tmp = TempDir::new().unwrap();
        let mut body = Vec::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(0);
        bm.insert(2);
        let pk_val = sillage_common::idx::DimValue::Bytes(pk_bytes(0x01));
        let dim = single_dim(
            sillage_common::idx::DIM_ACCOUNT_KEY,
            pk_val.clone(),
            &bm,
            &mut body,
        );
        let idx_bytes = build_idx_bytes("tx", 5, vec![dim], body);
        write_trio_with_idx(tmp.path(), Stream::Tx, 0, 1000, &idx_bytes);

        let catalog = ChunkCatalog::scan(tmp.path());
        let idx_cache = IndexCache::new(10 * 1024 * 1024);
        let filter = SubscribeRequestFilterTransactions {
            account_include: vec![pk_base58(0x01)],
            ..Default::default()
        };
        let parsed = make_subscription_filters_tx(vec![("sub1".to_string(), filter)]);

        let plan = plan_replay(&catalog, &parsed, &idx_cache, None).unwrap();
        assert_eq!(plan.plans_per_stream[0].len(), 1);
        let chunk_plan = &plan.plans_per_stream[0][0];
        assert_eq!(chunk_plan.named_bitmaps.len(), 1);
        assert_eq!(chunk_plan.named_bitmaps[0].0, "sub1");
        assert_eq!(
            chunk_plan.named_bitmaps[0].1,
            RoaringBitmap::from_sorted_iter([0, 2]).unwrap()
        );
        assert_eq!(chunk_plan.union, chunk_plan.named_bitmaps[0].1);
    }

    #[test]
    fn multiple_subs_union() {
        let tmp = TempDir::new().unwrap();
        let mut body = Vec::new();

        let mut bm_a = RoaringBitmap::new();
        bm_a.insert(0);
        bm_a.insert(1);
        let offset_a = body.len() as u64;
        bm_a.serialize_into(&mut body).unwrap();
        let len_a = body.len() as u64 - offset_a;

        let mut bm_b = RoaringBitmap::new();
        bm_b.insert(2);
        bm_b.insert(3);
        let offset_b = body.len() as u64;
        bm_b.serialize_into(&mut body).unwrap();
        let len_b = body.len() as u64 - offset_b;

        let dim = DimensionHeader {
            name: sillage_common::idx::DIM_ACCOUNT_KEY.to_string(),
            value_type: DimValueType::Pubkey32,
            entries: vec![
                DimEntryHeader {
                    value: sillage_common::idx::DimValue::Bytes(pk_bytes(0x01)),
                    offset: offset_a,
                    length: len_a,
                },
                DimEntryHeader {
                    value: sillage_common::idx::DimValue::Bytes(pk_bytes(0x02)),
                    offset: offset_b,
                    length: len_b,
                },
            ],
        };

        let idx_bytes = build_idx_bytes("tx", 5, vec![dim], body);
        write_trio_with_idx(tmp.path(), Stream::Tx, 0, 1000, &idx_bytes);

        let catalog = ChunkCatalog::scan(tmp.path());
        let idx_cache = IndexCache::new(10 * 1024 * 1024);

        let filter_a = SubscribeRequestFilterTransactions {
            account_include: vec![pk_base58(0x01)],
            ..Default::default()
        };
        let filter_b = SubscribeRequestFilterTransactions {
            account_include: vec![pk_base58(0x02)],
            ..Default::default()
        };
        let parsed = make_subscription_filters_tx(vec![
            ("sub_a".to_string(), filter_a),
            ("sub_b".to_string(), filter_b),
        ]);

        let plan = plan_replay(&catalog, &parsed, &idx_cache, None).unwrap();
        assert_eq!(plan.plans_per_stream[0].len(), 1);
        let chunk_plan = &plan.plans_per_stream[0][0];
        assert_eq!(chunk_plan.named_bitmaps.len(), 2);
        assert_eq!(chunk_plan.named_bitmaps[0].0, "sub_a");
        assert_eq!(chunk_plan.named_bitmaps[1].0, "sub_b");
        assert_eq!(
            chunk_plan.union,
            RoaringBitmap::from_sorted_iter([0, 1, 2, 3]).unwrap()
        );
    }

    #[test]
    fn empty_subs_default_filter_match_all() {
        let tmp = TempDir::new().unwrap();
        let idx_bytes = build_idx_bytes("block", 10, vec![], vec![]);
        write_trio_with_idx(tmp.path(), Stream::Block, 0, 1000, &idx_bytes);

        let catalog = ChunkCatalog::scan(tmp.path());
        let idx_cache = IndexCache::new(10 * 1024 * 1024);
        let parsed = make_subscription_filters_block(vec![(
            "block_sub".to_string(),
            SubscribeRequestFilterBlocksMeta::default(),
        )]);

        let plan = plan_replay(&catalog, &parsed, &idx_cache, None).unwrap();
        assert_eq!(plan.plans_per_stream[2].len(), 1);
        let chunk_plan = &plan.plans_per_stream[2][0];
        assert_eq!(chunk_plan.named_bitmaps.len(), 1);
        assert_eq!(
            chunk_plan.named_bitmaps[0].1,
            RoaringBitmap::from_sorted_iter(0..10).unwrap()
        );
        assert_eq!(chunk_plan.union, chunk_plan.named_bitmaps[0].1);
    }

    #[test]
    fn skips_stream_not_in_subscription() {
        let tmp = TempDir::new().unwrap();
        let idx_bytes = build_idx_bytes("tx", 5, vec![], vec![]);
        write_trio_with_idx(tmp.path(), Stream::Tx, 0, 1000, &idx_bytes);
        write_trio_with_idx(tmp.path(), Stream::Acct, 0, 1000, &idx_bytes);

        let catalog = ChunkCatalog::scan(tmp.path());
        let idx_cache = IndexCache::new(10 * 1024 * 1024);
        let parsed = make_subscription_filters_acct(vec![(
            "acct_sub".to_string(),
            SubscribeRequestFilterAccounts::default(),
        )]);

        let plan = plan_replay(&catalog, &parsed, &idx_cache, None).unwrap();
        assert!(plan.plans_per_stream[0].is_empty());
        assert_eq!(plan.plans_per_stream[1].len(), 1);
        assert!(plan.plans_per_stream[2].is_empty());
    }

    #[test]
    fn multiple_chunks_across_range() {
        let tmp = TempDir::new().unwrap();
        let idx_bytes = build_idx_bytes("tx", 5, vec![], vec![]);
        write_trio_with_idx(tmp.path(), Stream::Tx, 0, 1000, &idx_bytes);
        write_trio_with_idx(tmp.path(), Stream::Tx, 1000, 2000, &idx_bytes);
        write_trio_with_idx(tmp.path(), Stream::Tx, 2000, 3000, &idx_bytes);

        let catalog = ChunkCatalog::scan(tmp.path());
        let idx_cache = IndexCache::new(10 * 1024 * 1024);
        let parsed = make_subscription_filters_tx(vec![(
            "sub1".to_string(),
            SubscribeRequestFilterTransactions::default(),
        )]);

        let plan = plan_replay(&catalog, &parsed, &idx_cache, None).unwrap();
        assert_eq!(plan.from_slot, 0);
        assert_eq!(plan.to_slot_exclusive, 3000);
        assert_eq!(plan.plans_per_stream[0].len(), 3);
        assert_eq!(plan.plans_per_stream[0][0].entry.start_slot, 0);
        assert_eq!(plan.plans_per_stream[0][1].entry.start_slot, 1000);
        assert_eq!(plan.plans_per_stream[0][2].entry.start_slot, 2000);
    }

    #[test]
    fn account_stream_filter() {
        let tmp = TempDir::new().unwrap();
        let mut body = Vec::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        let pk_val = sillage_common::idx::DimValue::Bytes(pk_bytes(0xAA));
        let dim = single_dim(
            sillage_common::idx::DIM_ACCOUNT_PUBKEY,
            pk_val.clone(),
            &bm,
            &mut body,
        );
        let idx_bytes = build_idx_bytes("acct", 3, vec![dim], body);
        write_trio_with_idx(tmp.path(), Stream::Acct, 0, 500, &idx_bytes);

        let catalog = ChunkCatalog::scan(tmp.path());
        let idx_cache = IndexCache::new(10 * 1024 * 1024);
        let filter = SubscribeRequestFilterAccounts {
            account: vec![pk_base58(0xAA)],
            ..Default::default()
        };
        let parsed = make_subscription_filters_acct(vec![("acct_sub".to_string(), filter)]);

        let plan = plan_replay(&catalog, &parsed, &idx_cache, None).unwrap();
        assert_eq!(plan.plans_per_stream[1].len(), 1);
        let chunk_plan = &plan.plans_per_stream[1][0];
        assert_eq!(chunk_plan.named_bitmaps.len(), 1);
        assert_eq!(
            chunk_plan.named_bitmaps[0].1,
            RoaringBitmap::from_sorted_iter([1]).unwrap()
        );
    }

    #[test]
    fn newest_end_slot_helper() {
        let tmp = TempDir::new().unwrap();
        assert!(ChunkCatalog::scan(tmp.path()).newest_end_slot().is_none());

        write_trio(tmp.path(), Stream::Tx, 0, 1000, &make_meta("tx", 0, 1000));
        write_trio(tmp.path(), Stream::Acct, 0, 500, &make_meta("acct", 0, 500));

        let catalog = ChunkCatalog::scan(tmp.path());
        assert_eq!(catalog.newest_end_slot(), Some(1000));
    }

    // --- StreamCursor tests ---

    use crate::storage::ChunkCache;
    use prost::Message;
    use sillage_common::chunk::write_len_prefixed;
    use yellowstone_grpc_proto::geyser::{subscribe_update::UpdateOneof, SubscribeUpdateAccount};

    fn make_update_with_slot(slot: u64) -> SubscribeUpdate {
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

    fn write_chunk_trio_with_zst(
        dir: &std::path::Path,
        stream: Stream,
        start_slot: u64,
        end_slot_exclusive: u64,
        zst_data: &[u8],
    ) -> crate::storage::ChunkEntry {
        let stream_dir = dir.join("chunks").join(stream.as_str());
        fs::create_dir_all(&stream_dir).unwrap();
        let stem = format!("{:012}-{:012}", start_slot, end_slot_exclusive);
        let zst_path = stream_dir.join(format!("{stem}.zst"));
        let idx_path = stream_dir.join(format!("{stem}.idx"));
        let meta_path = stream_dir.join(format!("{stem}.meta.json"));
        fs::write(&zst_path, zst_data).unwrap();
        fs::write(&idx_path, b"index-data").unwrap();
        let meta = make_meta(stream.as_str(), start_slot, end_slot_exclusive);
        let json = serde_json::to_string(&meta).unwrap();
        fs::write(&meta_path, json).unwrap();

        crate::storage::ChunkEntry {
            stream,
            start_slot,
            end_slot_exclusive,
            zst_path,
            idx_path,
            meta_path,
            zst_len: zst_data.len() as u64,
            meta,
        }
    }

    /// A plan starting mid-chunk must not replay the messages before its
    /// `from_slot`: chunks are planned on overlap, so the chunk containing the
    /// start point also holds earlier messages.
    #[tokio::test]
    async fn drive_replay_skips_messages_below_from_slot() {
        let tmp = TempDir::new().unwrap();
        let zst_data = encode_chunk_zst(&[
            make_update_with_slot(100),
            make_update_with_slot(200),
            make_update_with_slot(300),
        ]);
        let entry = write_chunk_trio_with_zst(tmp.path(), Stream::Tx, 0, 1000, &zst_data);

        let cache = ChunkCache::new(10 * 1024 * 1024);
        let mut union = RoaringBitmap::new();
        union.insert(0);
        union.insert(1);
        union.insert(2);

        let plan = ReplayPlan {
            from_slot: 250,
            to_slot_exclusive: 1000,
            plans_per_stream: [
                vec![ChunkPlan {
                    stream: Stream::Tx,
                    chunk_recv_ns_first: entry.meta.recv_ns_first,
                    entry,
                    union,
                    named_bitmaps: Vec::new(),
                }],
                Vec::new(),
                Vec::new(),
            ],
        };

        let (sender, mut rx) = tokio::sync::mpsc::channel(16);
        let mut pacer = Pacer::from_config(&PacingConfig {
            enabled: false,
            ..PacingConfig::default()
        });
        let stats = drive_replay(
            plan,
            &cache,
            &mut pacer,
            sender,
            sillage_common::ShutdownSignal::new(),
        )
        .await
        .unwrap();

        assert_eq!(stats.sent, 1, "only slot 300 is at or above from_slot 250");
        assert_eq!(stats.last_slot, Some(300));
        let msg = rx.recv().await.unwrap().unwrap();
        assert_eq!(extract_slot(&msg), Some(300));
    }

    #[tokio::test]
    async fn cursor_happy_path_two_messages() {
        let tmp = TempDir::new().unwrap();
        let msg0 = make_update_with_slot(100);
        let msg1 = make_update_with_slot(200);
        let zst_data = encode_chunk_zst(&[msg0.clone(), msg1.clone()]);
        let entry = write_chunk_trio_with_zst(tmp.path(), Stream::Tx, 0, 1000, &zst_data);

        let cache = ChunkCache::new(10 * 1024 * 1024);

        let mut union = RoaringBitmap::new();
        union.insert(0);
        union.insert(1);

        let plan = ChunkPlan {
            stream: Stream::Tx,
            entry,
            named_bitmaps: vec![
                ("sub_a".to_string(), {
                    let mut bm = RoaringBitmap::new();
                    bm.insert(0);
                    bm.insert(1);
                    bm
                }),
                ("sub_b".to_string(), {
                    let mut bm = RoaringBitmap::new();
                    bm.insert(1);
                    bm
                }),
            ],
            union,
            chunk_recv_ns_first: None,
        };

        let mut cursor = StreamCursor::new(vec![plan]);

        let peeked = cursor.peek(&cache).await.unwrap();
        assert!(peeked.is_some());
        let (slot, _, names, _) = peeked.unwrap();
        assert_eq!(*slot, 100);
        assert_eq!(*names, vec!["sub_a".to_string()]);

        cursor.take_peeked();

        let peeked = cursor.peek(&cache).await.unwrap();
        assert!(peeked.is_some());
        let (slot, _, names, _) = peeked.unwrap();
        assert_eq!(*slot, 200);
        let mut sorted_names = names.clone();
        sorted_names.sort();
        assert_eq!(sorted_names, vec!["sub_a", "sub_b"]);

        cursor.take_peeked();

        let peeked = cursor.peek(&cache).await.unwrap();
        assert!(peeked.is_none());
    }

    #[tokio::test]
    async fn cursor_empty_union_returns_none_without_decode() {
        let tmp = TempDir::new().unwrap();
        let msg = make_update_with_slot(42);
        let zst_data = encode_chunk_zst(&[msg]);
        let entry = write_chunk_trio_with_zst(tmp.path(), Stream::Tx, 0, 1000, &zst_data);

        let cache = ChunkCache::new(10 * 1024 * 1024);

        let plan = ChunkPlan {
            stream: Stream::Tx,
            entry,
            named_bitmaps: vec![],
            union: RoaringBitmap::new(),
            chunk_recv_ns_first: None,
        };

        let mut cursor = StreamCursor::new(vec![plan]);

        let peeked = cursor.peek(&cache).await.unwrap();
        assert!(peeked.is_none());

        assert_eq!(
            cache.len(),
            0,
            "empty union should not trigger chunk decode"
        );
    }

    #[tokio::test]
    async fn cursor_multiple_chunks_crosses_boundary() {
        let tmp = TempDir::new().unwrap();
        let msg0 = make_update_with_slot(100);
        let msg1 = make_update_with_slot(200);
        let msg2 = make_update_with_slot(300);

        let zst_a = encode_chunk_zst(std::slice::from_ref(&msg0));
        let zst_b = encode_chunk_zst(&[msg1.clone(), msg2.clone()]);

        let entry_a = write_chunk_trio_with_zst(tmp.path(), Stream::Tx, 0, 1000, &zst_a);
        let entry_b = write_chunk_trio_with_zst(tmp.path(), Stream::Tx, 1000, 2000, &zst_b);

        let cache = ChunkCache::new(10 * 1024 * 1024);

        let plan_a = ChunkPlan {
            stream: Stream::Tx,
            entry: entry_a,
            named_bitmaps: vec![("sub1".to_string(), {
                let mut bm = RoaringBitmap::new();
                bm.insert(0);
                bm
            })],
            union: {
                let mut u = RoaringBitmap::new();
                u.insert(0);
                u
            },
            chunk_recv_ns_first: None,
        };

        let plan_b = ChunkPlan {
            stream: Stream::Tx,
            entry: entry_b,
            named_bitmaps: vec![("sub1".to_string(), {
                let mut bm = RoaringBitmap::new();
                bm.insert(0);
                bm.insert(1);
                bm
            })],
            union: {
                let mut u = RoaringBitmap::new();
                u.insert(0);
                u.insert(1);
                u
            },
            chunk_recv_ns_first: None,
        };

        let mut cursor = StreamCursor::new(vec![plan_a, plan_b]);

        let peeked = cursor.peek(&cache).await.unwrap();
        assert!(peeked.is_some());
        assert_eq!(peeked.unwrap().0, 100);
        cursor.take_peeked();

        let peeked = cursor.peek(&cache).await.unwrap();
        assert!(peeked.is_some());
        assert_eq!(peeked.unwrap().0, 200);
        cursor.take_peeked();

        let peeked = cursor.peek(&cache).await.unwrap();
        assert!(peeked.is_some());
        assert_eq!(peeked.unwrap().0, 300);
        cursor.take_peeked();

        let peeked = cursor.peek(&cache).await.unwrap();
        assert!(peeked.is_none());
    }

    #[tokio::test]
    async fn cursor_peek_idempotent_without_take() {
        let tmp = TempDir::new().unwrap();
        let msg = make_update_with_slot(42);
        let zst_data = encode_chunk_zst(&[msg]);
        let entry = write_chunk_trio_with_zst(tmp.path(), Stream::Tx, 0, 1000, &zst_data);

        let cache = ChunkCache::new(10 * 1024 * 1024);

        let mut union = RoaringBitmap::new();
        union.insert(0);

        let plan = ChunkPlan {
            stream: Stream::Tx,
            entry,
            named_bitmaps: vec![("sub1".to_string(), union.clone())],
            union,
            chunk_recv_ns_first: None,
        };

        let mut cursor = StreamCursor::new(vec![plan]);

        let peeked1 = cursor.peek(&cache).await.unwrap();
        assert!(peeked1.is_some());
        let slot1 = peeked1.unwrap().0;

        let peeked2 = cursor.peek(&cache).await.unwrap();
        assert!(peeked2.is_some());
        let slot2 = peeked2.unwrap().0;

        assert_eq!(slot1, slot2);
    }

    #[tokio::test]
    async fn cursor_take_consumes_and_advances() {
        let tmp = TempDir::new().unwrap();
        let msg0 = make_update_with_slot(10);
        let msg1 = make_update_with_slot(20);
        let zst_data = encode_chunk_zst(&[msg0, msg1]);
        let entry = write_chunk_trio_with_zst(tmp.path(), Stream::Tx, 0, 1000, &zst_data);

        let cache = ChunkCache::new(10 * 1024 * 1024);

        let mut union = RoaringBitmap::new();
        union.insert(0);
        union.insert(1);

        let plan = ChunkPlan {
            stream: Stream::Tx,
            entry,
            named_bitmaps: vec![("sub1".to_string(), union.clone())],
            union,
            chunk_recv_ns_first: None,
        };

        let mut cursor = StreamCursor::new(vec![plan]);

        let peeked = cursor.peek(&cache).await.unwrap();
        assert!(peeked.is_some());
        assert_eq!(peeked.unwrap().0, 10);

        let taken = cursor.take_peeked();
        assert!(taken.is_some());
        let (slot, msg, names, _) = taken.unwrap();
        assert_eq!(slot, 10);
        assert_eq!(names, vec!["sub1"]);
        assert!(msg.update_oneof.is_some());

        let peeked = cursor.peek(&cache).await.unwrap();
        assert!(peeked.is_some());
        assert_eq!(peeked.unwrap().0, 20);

        let taken = cursor.take_peeked();
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().0, 20);

        let peeked = cursor.peek(&cache).await.unwrap();
        assert!(peeked.is_none());
    }

    #[test]
    fn set_filters_overrides_filters_field() {
        let update = SubscribeUpdate {
            filters: vec!["old_filter".to_string()],
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                slot: 42,
                ..Default::default()
            })),
            ..Default::default()
        };

        let result = set_filters(update, vec!["new_a".to_string(), "new_b".to_string()]);
        assert_eq!(result.filters, vec!["new_a", "new_b"]);
        assert_eq!(
            result.update_oneof,
            Some(UpdateOneof::Account(SubscribeUpdateAccount {
                slot: 42,
                ..Default::default()
            }))
        );
    }

    #[test]
    fn set_filters_with_empty_names() {
        let update = SubscribeUpdate {
            filters: vec!["old".to_string()],
            update_oneof: None,
            ..Default::default()
        };

        let result = set_filters(update, vec![]);
        assert!(result.filters.is_empty());
    }

    // --- drive_replay tests ---

    fn make_replay_plan(plans_per_stream: [Vec<ChunkPlan>; 3]) -> ReplayPlan {
        let from_slot = plans_per_stream
            .iter()
            .flat_map(|v| v.iter().map(|p| p.entry.start_slot))
            .min()
            .unwrap_or(0);
        let to_slot_exclusive = plans_per_stream
            .iter()
            .flat_map(|v| v.iter().map(|p| p.entry.end_slot_exclusive))
            .max()
            .unwrap_or(0);
        ReplayPlan {
            from_slot,
            to_slot_exclusive,
            plans_per_stream,
        }
    }

    fn make_chunk_plan(
        stream: Stream,
        entry: crate::storage::ChunkEntry,
        named_bitmaps: Vec<(&str, RoaringBitmap)>,
    ) -> ChunkPlan {
        let union = named_bitmaps
            .iter()
            .fold(RoaringBitmap::new(), |acc, (_, b)| acc | b);
        ChunkPlan {
            stream,
            entry,
            named_bitmaps: named_bitmaps
                .into_iter()
                .map(|(n, b)| (n.to_string(), b))
                .collect(),
            union,
            chunk_recv_ns_first: None,
        }
    }

    #[tokio::test]
    async fn drive_replay_happy_path_slot_order_and_stream_tiebreak() {
        let tmp = TempDir::new().unwrap();

        let msg_tx_0 = make_update_with_slot(100);
        let msg_tx_1 = make_update_with_slot(300);
        let zst_tx = encode_chunk_zst(&[msg_tx_0.clone(), msg_tx_1.clone()]);
        let entry_tx = write_chunk_trio_with_zst(tmp.path(), Stream::Tx, 0, 1000, &zst_tx);

        let msg_acct_0 = make_update_with_slot(200);
        let msg_acct_1 = make_update_with_slot(250);
        let zst_acct = encode_chunk_zst(&[msg_acct_0.clone(), msg_acct_1.clone()]);
        let entry_acct = write_chunk_trio_with_zst(tmp.path(), Stream::Acct, 0, 1000, &zst_acct);

        let cache = ChunkCache::new(10 * 1024 * 1024);

        let mut tx_bm = RoaringBitmap::new();
        tx_bm.insert(0);
        tx_bm.insert(1);

        let mut acct_bm = RoaringBitmap::new();
        acct_bm.insert(0);
        acct_bm.insert(1);

        let plan = make_replay_plan([
            vec![make_chunk_plan(
                Stream::Tx,
                entry_tx,
                vec![("tx_sub", tx_bm)],
            )],
            vec![make_chunk_plan(
                Stream::Acct,
                entry_acct,
                vec![("acct_sub", acct_bm)],
            )],
            vec![],
        ]);

        let (sender, mut receiver) = tokio::sync::mpsc::channel(100);
        let shutdown = sillage_common::ShutdownSignal::new();
        let mut pacer = Pacer::from_config(&PacingConfig {
            enabled: true,
            speed_multiplier: 1000.0,
            lag_warn_ms: 5_000,
            lag_drop_ms: 30_000,
        });

        let result = drive_replay(plan, &cache, &mut pacer, sender, shutdown)
            .await
            .unwrap();

        assert_eq!(result.sent, 4);

        let mut received: Vec<(u64, Vec<String>)> = Vec::new();
        while let Some(Ok(update)) = receiver.recv().await {
            let slot = extract_slot(&update).unwrap_or(0);
            received.push((slot, update.filters));
        }

        assert_eq!(received.len(), 4);
        assert_eq!(received[0].0, 100);
        assert_eq!(received[0].1, vec!["tx_sub"]);
        assert_eq!(received[1].0, 200);
        assert_eq!(received[1].1, vec!["acct_sub"]);
        assert_eq!(received[2].0, 250);
        assert_eq!(received[2].1, vec!["acct_sub"]);
        assert_eq!(received[3].0, 300);
        assert_eq!(received[3].1, vec!["tx_sub"]);
    }

    #[tokio::test]
    async fn drive_replay_same_slot_tiebreak_by_stream_index() {
        let tmp = TempDir::new().unwrap();

        let msg_tx = make_update_with_slot(100);
        let zst_tx = encode_chunk_zst(std::slice::from_ref(&msg_tx));
        let entry_tx = write_chunk_trio_with_zst(tmp.path(), Stream::Tx, 0, 1000, &zst_tx);

        let msg_acct = make_update_with_slot(100);
        let zst_acct = encode_chunk_zst(std::slice::from_ref(&msg_acct));
        let entry_acct = write_chunk_trio_with_zst(tmp.path(), Stream::Acct, 0, 1000, &zst_acct);

        let cache = ChunkCache::new(10 * 1024 * 1024);

        let mut tx_bm = RoaringBitmap::new();
        tx_bm.insert(0);
        let mut acct_bm = RoaringBitmap::new();
        acct_bm.insert(0);

        let plan = make_replay_plan([
            vec![make_chunk_plan(
                Stream::Tx,
                entry_tx,
                vec![("tx_sub", tx_bm)],
            )],
            vec![make_chunk_plan(
                Stream::Acct,
                entry_acct,
                vec![("acct_sub", acct_bm)],
            )],
            vec![],
        ]);

        let (sender, mut receiver) = tokio::sync::mpsc::channel(100);
        let shutdown = sillage_common::ShutdownSignal::new();
        let mut pacer = Pacer::from_config(&PacingConfig {
            enabled: true,
            speed_multiplier: 1000.0,
            lag_warn_ms: 5_000,
            lag_drop_ms: 30_000,
        });

        let result = drive_replay(plan, &cache, &mut pacer, sender, shutdown)
            .await
            .unwrap();

        assert_eq!(result.sent, 2);

        let mut received: Vec<(u64, Vec<String>)> = Vec::new();
        while let Some(Ok(update)) = receiver.recv().await {
            let slot = extract_slot(&update).unwrap_or(0);
            received.push((slot, update.filters));
        }

        assert_eq!(received.len(), 2);
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].1, vec!["tx_sub"]);
        assert_eq!(received[1].1, vec!["acct_sub"]);
    }

    #[tokio::test]
    async fn drive_replay_shutdown_mid_replay() {
        let tmp = TempDir::new().unwrap();

        let msg0 = make_update_with_slot(100);
        let msg1 = make_update_with_slot(200);
        let zst_data = encode_chunk_zst(&[msg0.clone(), msg1.clone()]);
        let entry = write_chunk_trio_with_zst(tmp.path(), Stream::Tx, 0, 1000, &zst_data);

        let cache = ChunkCache::new(10 * 1024 * 1024);

        let mut bm = RoaringBitmap::new();
        bm.insert(0);
        bm.insert(1);

        let plan = make_replay_plan([
            vec![make_chunk_plan(Stream::Tx, entry, vec![("sub1", bm)])],
            vec![],
            vec![],
        ]);

        let (sender, mut receiver) = tokio::sync::mpsc::channel(100);
        let shutdown = sillage_common::ShutdownSignal::new();

        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move {
            let mut pacer = Pacer::from_config(&PacingConfig {
                enabled: true,
                speed_multiplier: 1000.0,
                lag_warn_ms: 5_000,
                lag_drop_ms: 30_000,
            });
            drive_replay(plan, &cache, &mut pacer, sender, shutdown_clone).await
        });

        let first = receiver.recv().await.unwrap().unwrap();
        assert_eq!(extract_slot(&first).unwrap_or(0), 100);

        shutdown.cancel();

        let result = handle.await.unwrap();
        assert!(result.is_ok());
        let sent = result.unwrap().sent;
        assert!(sent >= 1);
    }

    #[tokio::test]
    async fn drive_replay_subscriber_drop() {
        let tmp = TempDir::new().unwrap();

        let msg0 = make_update_with_slot(100);
        let msg1 = make_update_with_slot(200);
        let zst_data = encode_chunk_zst(&[msg0.clone(), msg1.clone()]);
        let entry = write_chunk_trio_with_zst(tmp.path(), Stream::Tx, 0, 1000, &zst_data);

        let cache = ChunkCache::new(10 * 1024 * 1024);

        let mut bm = RoaringBitmap::new();
        bm.insert(0);
        bm.insert(1);

        let plan = make_replay_plan([
            vec![make_chunk_plan(Stream::Tx, entry, vec![("sub1", bm)])],
            vec![],
            vec![],
        ]);

        let (sender, receiver) = tokio::sync::mpsc::channel(100);
        let shutdown = sillage_common::ShutdownSignal::new();

        drop(receiver);

        let mut pacer = Pacer::from_config(&PacingConfig {
            enabled: true,
            speed_multiplier: 1000.0,
            lag_warn_ms: 5_000,
            lag_drop_ms: 30_000,
        });
        let result = drive_replay(plan, &cache, &mut pacer, sender, shutdown).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().sent, 0);
    }

    #[tokio::test]
    async fn drive_replay_single_stream() {
        let tmp = TempDir::new().unwrap();

        let msg0 = make_update_with_slot(50);
        let msg1 = make_update_with_slot(150);
        let zst_data = encode_chunk_zst(&[msg0.clone(), msg1.clone()]);
        let entry = write_chunk_trio_with_zst(tmp.path(), Stream::Block, 0, 1000, &zst_data);

        let cache = ChunkCache::new(10 * 1024 * 1024);

        let mut bm = RoaringBitmap::new();
        bm.insert(0);
        bm.insert(1);

        let plan = make_replay_plan([
            vec![],
            vec![],
            vec![make_chunk_plan(Stream::Block, entry, vec![("blk_sub", bm)])],
        ]);

        let (sender, mut receiver) = tokio::sync::mpsc::channel(100);
        let shutdown = sillage_common::ShutdownSignal::new();
        let mut pacer = Pacer::from_config(&PacingConfig {
            enabled: true,
            speed_multiplier: 1000.0,
            lag_warn_ms: 5_000,
            lag_drop_ms: 30_000,
        });

        let result = drive_replay(plan, &cache, &mut pacer, sender, shutdown)
            .await
            .unwrap();

        assert_eq!(result.sent, 2);

        let mut received: Vec<u64> = Vec::new();
        while let Some(Ok(update)) = receiver.recv().await {
            received.push(extract_slot(&update).unwrap_or(0));
        }

        assert_eq!(received, vec![50, 150]);
    }

    #[tokio::test]
    async fn drive_replay_empty_plan() {
        let cache = ChunkCache::new(10 * 1024 * 1024);

        let plan = make_replay_plan([vec![], vec![], vec![]]);

        let (sender, mut receiver) = tokio::sync::mpsc::channel(100);
        let shutdown = sillage_common::ShutdownSignal::new();
        let mut pacer = Pacer::from_config(&PacingConfig {
            enabled: true,
            speed_multiplier: 1000.0,
            lag_warn_ms: 5_000,
            lag_drop_ms: 30_000,
        });

        let result = drive_replay(plan, &cache, &mut pacer, sender, shutdown)
            .await
            .unwrap();

        assert_eq!(result.sent, 0);

        assert!(receiver.recv().await.is_none());
    }

    #[test]
    fn extract_recv_ns_created_at_present() {
        let msg = SubscribeUpdate {
            created_at: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 500_000_000,
            }),
            ..Default::default()
        };
        let meta = make_meta("tx", 0, 1000);
        let result = extract_recv_ns(&msg, &meta, 0);
        assert_eq!(result, Some(1_700_000_000_500_000_000));
    }

    #[test]
    fn extract_recv_ns_created_at_negative_seconds() {
        let msg = SubscribeUpdate {
            created_at: Some(Timestamp {
                seconds: -1,
                nanos: 0,
            }),
            ..Default::default()
        };
        let meta = ChunkMeta {
            recv_ns_first: Some(1_000_000),
            ..make_meta("tx", 0, 1000)
        };
        let result = extract_recv_ns(&msg, &meta, 0);
        assert_eq!(result, Some(1_000_000));
    }

    #[test]
    fn extract_recv_ns_chunk_linear_interp() {
        let msg = SubscribeUpdate {
            created_at: None,
            ..Default::default()
        };
        let meta = ChunkMeta {
            recv_ns_first: Some(1_000_000),
            recv_ns_last: Some(2_000_000),
            message_count: 3,
            ..make_meta("tx", 0, 1000)
        };
        let result = extract_recv_ns(&msg, &meta, 1);
        assert_eq!(result, Some(1_500_000));
    }

    #[test]
    fn extract_recv_ns_chunk_single_message() {
        let msg = SubscribeUpdate {
            created_at: None,
            ..Default::default()
        };
        let meta = ChunkMeta {
            message_count: 1,
            recv_ns_first: Some(5_000_000),
            ..make_meta("tx", 0, 1000)
        };
        let result = extract_recv_ns(&msg, &meta, 0);
        assert_eq!(result, Some(5_000_000));
    }

    #[test]
    fn extract_recv_ns_chunk_no_recv_ns() {
        let msg = SubscribeUpdate {
            created_at: None,
            ..Default::default()
        };
        let meta = ChunkMeta {
            recv_ns_first: None,
            recv_ns_last: None,
            ..make_meta("tx", 0, 1000)
        };
        let result = extract_recv_ns(&msg, &meta, 0);
        assert_eq!(result, None);
    }

    #[test]
    fn extract_recv_ns_chunk_first_only_no_last() {
        let msg = SubscribeUpdate {
            created_at: None,
            ..Default::default()
        };
        let meta = ChunkMeta {
            recv_ns_first: Some(3_000_000),
            recv_ns_last: None,
            ..make_meta("tx", 0, 1000)
        };
        let result = extract_recv_ns(&msg, &meta, 0);
        assert_eq!(result, Some(3_000_000));
    }

    #[tokio::test(start_paused = true)]
    async fn drive_replay_paced_wall_clock() {
        let tmp = TempDir::new().unwrap();

        let msg0 = SubscribeUpdate {
            created_at: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..Default::default()
        };
        let msg1 = SubscribeUpdate {
            created_at: Some(Timestamp {
                seconds: 1_700_001_000,
                nanos: 0,
            }),
            ..Default::default()
        };

        let zst_data = encode_chunk_zst(&[msg0.clone(), msg1.clone()]);
        let entry = write_chunk_trio_with_zst(tmp.path(), Stream::Tx, 0, 1000, &zst_data);

        let cache = ChunkCache::new(10 * 1024 * 1024);

        let mut bm = RoaringBitmap::new();
        bm.insert(0);
        bm.insert(1);

        let plan = make_replay_plan([
            vec![make_chunk_plan(Stream::Tx, entry, vec![("sub1", bm)])],
            vec![],
            vec![],
        ]);

        let (sender, mut receiver) = tokio::sync::mpsc::channel(100);
        let shutdown = sillage_common::ShutdownSignal::new();

        let before = tokio::time::Instant::now();
        let mut pacer = Pacer::from_config(&PacingConfig {
            enabled: true,
            speed_multiplier: 100.0,
            lag_warn_ms: 5_000,
            lag_drop_ms: 30_000,
        });
        let result = drive_replay(plan, &cache, &mut pacer, sender, shutdown)
            .await
            .unwrap();
        let elapsed = before.elapsed();

        assert_eq!(result.sent, 2);

        while receiver.recv().await.is_some() {}

        // speed=100.0, 1000s gap → 10s virtual wall-clock with paused time
        assert!(
            elapsed >= std::time::Duration::from_secs(9),
            "expected ≥9s elapsed, got {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn drive_replay_shutdown_during_sleep() {
        let tmp = TempDir::new().unwrap();

        let msg0 = make_update_with_slot(100);
        let msg1 = make_update_with_slot(200);
        let zst_data = encode_chunk_zst(&[msg0.clone(), msg1.clone()]);
        let entry = write_chunk_trio_with_zst(tmp.path(), Stream::Tx, 0, 1000, &zst_data);

        let cache = ChunkCache::new(10 * 1024 * 1024);

        let mut bm = RoaringBitmap::new();
        bm.insert(0);
        bm.insert(1);

        let plan = make_replay_plan([
            vec![make_chunk_plan(Stream::Tx, entry, vec![("sub1", bm)])],
            vec![],
            vec![],
        ]);

        let (sender, mut receiver) = tokio::sync::mpsc::channel(100);
        let shutdown = sillage_common::ShutdownSignal::new();

        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move {
            let mut pacer = Pacer::from_config(&PacingConfig {
                enabled: true,
                speed_multiplier: 1000.0,
                lag_warn_ms: 5_000,
                lag_drop_ms: 30_000,
            });
            drive_replay(plan, &cache, &mut pacer, sender, shutdown_clone).await
        });

        let first = receiver.recv().await.unwrap().unwrap();
        assert_eq!(extract_slot(&first).unwrap_or(0), 100);

        shutdown.cancel();

        let result = handle.await.unwrap();
        assert!(result.is_ok());
        let sent = result.unwrap().sent;
        assert!(sent >= 1);
    }

    #[tokio::test]
    async fn drive_replay_send_timeout() {
        let tmp = TempDir::new().unwrap();

        let msg0 = make_update_with_slot(100);
        let msg1 = make_update_with_slot(200);
        let zst_data = encode_chunk_zst(&[msg0.clone(), msg1.clone()]);
        let entry = write_chunk_trio_with_zst(tmp.path(), Stream::Tx, 0, 1000, &zst_data);

        let cache = ChunkCache::new(10 * 1024 * 1024);

        let mut bm = RoaringBitmap::new();
        bm.insert(0);
        bm.insert(1);

        let plan = make_replay_plan([
            vec![make_chunk_plan(Stream::Tx, entry, vec![("sub1", bm)])],
            vec![],
            vec![],
        ]);

        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let shutdown = sillage_common::ShutdownSignal::new();

        drop(_receiver);

        let mut pacer = Pacer::from_config(&PacingConfig {
            enabled: true,
            speed_multiplier: 1000.0,
            lag_warn_ms: 5_000,
            lag_drop_ms: 30_000,
        });
        let result = drive_replay(plan, &cache, &mut pacer, sender, shutdown).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().sent, 0);
    }
}
