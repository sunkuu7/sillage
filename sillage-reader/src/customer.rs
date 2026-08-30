use std::sync::Arc;
use std::time::{Duration, Instant};

use sillage_common::config::PacingConfig;
use sillage_common::shutdown::ShutdownSignal;
use tokio::sync::{mpsc, Mutex};
use tonic::{Status, Streaming};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, SubscribeRequest, SubscribeUpdate, SubscribeUpdatePong,
};

use ::metrics::{counter, gauge};
use sillage_reader::index::IndexCache;
use sillage_reader::metrics;
use sillage_reader::pacing::Pacer;
use sillage_reader::replay::{drive_replay, plan_replay, PlanError, ReplayPlan, ReplayStats};
use sillage_reader::storage::{ChunkCache, SharedCatalog};
use sillage_reader::subscription::SubscriptionFilters;

/// Stable, non-secret identity for the caller behind a connection.
///
/// Carries the *index* of the matching configured token, never the token
/// itself, so it is safe to log and to key connection limits on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ClientId {
    /// Auth is disabled (`reader.auth_tokens` empty) — all callers are one
    /// undifferentiated pool.
    Anonymous,
    Token(usize),
}

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientId::Anonymous => write!(f, "anonymous"),
            ClientId::Token(i) => write!(f, "token-{i}"),
        }
    }
}

/// Lightweight handle stored in the active-customers registry.
#[derive(Clone)]
pub(crate) struct CustomerHandle {
    pub(crate) customer_id: String,
    pub(crate) client: ClientId,
    pub(crate) connected_at: Instant,
    pub(crate) filter_summary: String,
}

/// Owns all context needed to serve a single gRPC subscription.
pub struct Customer {
    pub id: String,
    /// Registry entry, created and admitted by the gRPC layer before this task
    /// was spawned. Registration is the admission decision, so it cannot happen
    /// here.
    pub(crate) handle: CustomerHandle,
    pub parsed: SubscriptionFilters,
    pub tx: mpsc::Sender<Result<SubscribeUpdate, Status>>,
    pub shutdown: ShutdownSignal,
    /// The live handle, not a pinned snapshot: a follower takes a fresh
    /// snapshot for each replay pass so it picks up chunks the syncer landed
    /// after it connected. Each individual pass still plans against one
    /// consistent snapshot.
    pub catalog: SharedCatalog,
    pub chunk_cache: Arc<ChunkCache>,
    pub index_cache: Arc<IndexCache>,
    pub pacing: PacingConfig,
    /// How long to wait for new chunks once caught up before closing the
    /// stream.
    pub follow_idle_timeout: Duration,
    pub(crate) customers: Arc<Mutex<Vec<CustomerHandle>>>,
}

/// How often a caught-up follower re-checks the catalog for new chunks.
/// Re-planning an empty range is cheap — no chunks means no index parses.
const FOLLOW_POLL_INTERVAL: Duration = Duration::from_secs(2);

impl Customer {
    /// Run the customer connection lifecycle:
    /// 1. Plan replay from local chunks
    /// 2. Spawn a replay task that streams historical data
    /// 3. Handle inbound messages (ping/pong) until disconnect or shutdown
    /// 4. Clean up the active-customers registry on exit
    pub async fn run(self, mut inbound: Streaming<SubscribeRequest>) {
        let handle = self.handle.clone();

        tracing::info!(
            customer_id = %handle.customer_id,
            client = %handle.client,
            filters = %handle.filter_summary,
            "customer connected"
        );

        gauge!(metrics::ACTIVE_CONNECTIONS).increment(1.0);
        counter!(metrics::CONNECTIONS_TOTAL).increment(1);

        // The first plan is built inline so a bad request fails the
        // subscription immediately, before we commit to a follow loop.
        let first_plan = match plan_replay(
            &self.catalog.snapshot(),
            &self.parsed,
            &self.index_cache,
            self.parsed.from_slot,
        ) {
            Ok(plan) => plan,
            Err(e) => {
                self.reject(&handle, e).await;
                return;
            }
        };

        let replay_tx = self.tx.clone();
        // Dedicated stop signal for the replay task. Cancelling this (rather
        // than abort()ing the handle) lets drive_replay return Ok(stats) from
        // its shutdown select branch, so we recover accurate per-customer stats
        // even when the client disconnects mid-replay.
        let replay_stop = ShutdownSignal::new();
        let replay_cache = self.chunk_cache.clone();
        let replay_catalog = self.catalog.clone();
        let replay_index_cache = self.index_cache.clone();
        let replay_filters = self.parsed.clone();
        let pacing = self.pacing.clone();
        let idle_timeout = self.follow_idle_timeout;
        let replay_id = self.id.clone();
        let mut replay_handle = tokio::spawn({
            let replay_stop = replay_stop.clone();
            async move {
                follow_replay(
                    first_plan,
                    FollowContext {
                        id: replay_id,
                        catalog: replay_catalog,
                        filters: replay_filters,
                        index_cache: replay_index_cache,
                        chunk_cache: replay_cache,
                        pacing,
                        idle_timeout,
                    },
                    replay_tx,
                    replay_stop,
                )
                .await
            }
        });

        let mut replay_outcome: Option<ReplayStats> = None;
        let mut replay_finished = false;

        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => {
                    tracing::info!(customer_id = %self.id, "shutdown signal received");
                    break;
                }
                // The replay ran out of slots and gave up waiting. Closing the
                // stream here is the end-of-replay signal: the client sees a
                // clean close and can reconnect from its last slot.
                res = &mut replay_handle => {
                    replay_finished = true;
                    replay_outcome = match res {
                        Ok(Ok(stats)) => Some(stats),
                        _ => None,
                    };
                    tracing::info!(
                        customer_id = %self.id,
                        "replay exhausted; closing stream"
                    );
                    break;
                }
                msg = inbound.message() => match msg {
                    Ok(Some(req)) => {
                        if let Some(ping) = req.ping {
                            let update = SubscribeUpdate {
                                update_oneof: Some(UpdateOneof::Pong(SubscribeUpdatePong {
                                    id: ping.id,
                                })),
                                ..Default::default()
                            };
                            if self.tx.send(Ok(update)).await.is_err() {
                                tracing::info!(
                                    customer_id = %self.id,
                                    "subscriber dropped during pong"
                                );
                                break;
                            }
                        } else {
                            tracing::warn!(
                                customer_id = %self.id,
                                "filter updates not yet supported; ignoring"
                            );
                        }
                    }
                    Ok(None) => {
                        tracing::info!(customer_id = %self.id, "client closed inbound stream");
                        break;
                    }
                    Err(e) if is_client_hangup(&e) => {
                        // A client that closes its socket without a gRPC
                        // half-close is routine — plenty of clients simply
                        // stop reading and exit. Logging it as a warning
                        // buries the stream errors that are worth noticing.
                        tracing::info!(
                            customer_id = %self.id,
                            "client disconnected without closing the stream"
                        );
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(
                            customer_id = %self.id,
                            error = %e,
                            "inbound stream error"
                        );
                        break;
                    }
                }
            }
        }

        // Cooperative cancel + await (no abort) so drive_replay returns its
        // accumulated stats. On client disconnect the dropped receiver makes
        // tx.send fail fast; on shutdown replay_stop wakes the sleep — either
        // way the task returns promptly. If the replay already completed on its
        // own we keep the stats the select arm captured; awaiting again would
        // panic on a consumed JoinHandle.
        replay_stop.cancel();
        let stats: Option<ReplayStats> = if replay_finished {
            replay_outcome
        } else {
            match replay_handle.await {
                Ok(Ok(s)) => Some(s),
                _ => None,
            }
        };

        self.remove_from_registry(&handle).await;
        gauge!(metrics::ACTIVE_CONNECTIONS).decrement(1.0);

        let duration = handle.connected_at.elapsed().as_secs_f64();
        if let Some(stats) = stats {
            tracing::info!(
                customer_id = %handle.customer_id,
                duration_s = %duration,
                messages_sent = %stats.sent,
                bytes_sent = %stats.bytes,
                lag_max_ms = %stats.lag_max_ms,
                filters = %self.parsed.summary(),
                "customer disconnected"
            );
        } else {
            tracing::info!(
                customer_id = %handle.customer_id,
                duration_s = %duration,
                messages_sent = %0u64,
                bytes_sent = %0u64,
                lag_max_ms = %0.0f64,
                filters = %self.parsed.summary(),
                "customer disconnected"
            );
        }
    }

    /// Fail the subscription before any replay starts, mapping the planning
    /// error to the status code the client should act on.
    async fn reject(&self, handle: &CustomerHandle, e: PlanError) {
        let status = match &e {
            // Asking for a slot we no longer hold is a client-side mistake, not
            // a server fault: out_of_range tells the client to re-query
            // SubscribeReplayInfo rather than retry blindly.
            PlanError::FromSlotTooOld { .. } => {
                tracing::warn!(
                    customer_id = %self.id,
                    error = %e,
                    "rejecting subscription: from_slot below retained window"
                );
                Status::out_of_range(e.to_string())
            }
            // A cold reader still hydrating from R2, or streams we hold nothing
            // for. Unavailable tells the client to retry later rather than sit
            // on a stream that will never produce a message.
            PlanError::NoChunksAvailable => {
                tracing::warn!(
                    customer_id = %self.id,
                    error = %e,
                    "rejecting subscription: nothing available for requested streams"
                );
                Status::unavailable(e.to_string())
            }
            PlanError::Other(_) => {
                tracing::error!(customer_id = %self.id, error = %e, "replay planning failed");
                Status::internal(format!("replay planning failed: {e}"))
            }
        };
        let _ = self.tx.send(Err(status)).await;
        gauge!(metrics::ACTIVE_CONNECTIONS).decrement(1.0);
        self.remove_from_registry(handle).await;
    }

    async fn remove_from_registry(&self, handle: &CustomerHandle) {
        let customer_id = handle.customer_id.clone();
        self.customers
            .lock()
            .await
            .retain(|c| c.customer_id != customer_id);
    }
}

/// True when an inbound stream error is just the client going away.
///
/// A subscriber that stops reading and exits produces an h2 error whose root
/// cause is a broken pipe or reset connection, rather than a clean half-close.
/// That is normal client behaviour, not a fault worth warning about, so it is
/// separated from genuine stream failures.
///
/// Two strategies, because one is not enough. The cause chain is walked first,
/// which is exact — but for a real disconnect the chain runs
/// `Status -> transport::Error -> Status -> hyper::Error -> h2::Error` and
/// stops there: `h2::Error` keeps its `io::Error` in a private `Kind` and does
/// not return it from `source()`. The io kind is visible only in the rendered
/// chain, so that is checked as a fallback.
fn is_client_hangup(status: &Status) -> bool {
    if status.code() == tonic::Code::Cancelled {
        return true;
    }

    fn is_hangup_kind(kind: std::io::ErrorKind) -> bool {
        matches!(
            kind,
            std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::UnexpectedEof
        )
    }

    let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(status);
    while let Some(err) = source {
        if let Some(io) = err.downcast_ref::<std::io::Error>() {
            return is_hangup_kind(io.kind());
        }
        source = err.source();
    }

    // Fallback: the rendered chain carries the io kind that `source()` hides.
    let rendered = format!("{status}").to_ascii_lowercase();
    [
        "broken pipe",
        "brokenpipe",
        "connection reset",
        "connectionreset",
        "connection aborted",
        "connectionaborted",
        "unexpectedeof",
    ]
    .iter()
    .any(|needle| rendered.contains(needle))
}

/// Everything `follow_replay` needs to build each successive replay pass.
struct FollowContext {
    id: String,
    catalog: SharedCatalog,
    filters: SubscriptionFilters,
    index_cache: Arc<IndexCache>,
    chunk_cache: Arc<ChunkCache>,
    pacing: PacingConfig,
    idle_timeout: Duration,
}

/// Drive the initial plan, then keep following the catalog as the syncer lands
/// new chunks, resuming each pass from `last_slot + 1`.
///
/// Returns once the subscriber is gone, the reader is shutting down, or the
/// stream has been caught up with nothing new for `idle_timeout` — the last of
/// which is a normal end-of-stream, not an error.
async fn follow_replay(
    first_plan: ReplayPlan,
    ctx: FollowContext,
    tx: mpsc::Sender<Result<SubscribeUpdate, Status>>,
    stop: ShutdownSignal,
) -> anyhow::Result<ReplayStats> {
    // One pacer for the whole connection: its wall-clock anchor must survive
    // across passes, or every resume would restart pacing from zero.
    let mut pacer = Pacer::from_config(&ctx.pacing);

    let mut total = ReplayStats {
        sent: 0,
        bytes: 0,
        lag_max_ms: 0.0,
        last_slot: None,
        drained: true,
    };
    let mut plan = Some(first_plan);
    let mut idle_deadline = Instant::now() + ctx.idle_timeout;

    loop {
        if stop.is_cancelled() {
            return Ok(total);
        }

        let this_plan = match plan.take() {
            Some(p) => p,
            None => {
                let resume_from = total.last_slot.map(|s| s + 1).or(ctx.filters.from_slot);
                match plan_replay(
                    &ctx.catalog.snapshot(),
                    &ctx.filters,
                    &ctx.index_cache,
                    resume_from,
                ) {
                    Ok(p) => p,
                    // Retention swept past where this follower had reached, or
                    // the catalog lost the streams it wanted. Either way there
                    // is nothing further to serve.
                    Err(e) => {
                        tracing::warn!(
                            customer_id = %ctx.id,
                            error = %e,
                            "follow replay stopping: cannot plan from resume point"
                        );
                        let _ = tx.send(Err(Status::out_of_range(e.to_string()))).await;
                        total.drained = false;
                        return Ok(total);
                    }
                }
            }
        };

        let pass = drive_replay(
            this_plan,
            &ctx.chunk_cache,
            &mut pacer,
            tx.clone(),
            stop.clone(),
        )
        .await?;

        total.sent += pass.sent;
        total.bytes += pass.bytes;
        total.lag_max_ms = total.lag_max_ms.max(pass.lag_max_ms);
        if pass.last_slot.is_some() {
            total.last_slot = pass.last_slot;
        }

        // The pass ended early — subscriber gone, lag drop, or shutdown. No
        // point looking for more slots to send them.
        if !pass.drained {
            total.drained = false;
            return Ok(total);
        }

        if pass.sent > 0 {
            // Made progress; there may be more already waiting.
            idle_deadline = Instant::now() + ctx.idle_timeout;
            continue;
        }

        // Caught up. Wait for the syncer to land something new.
        if Instant::now() >= idle_deadline {
            tracing::info!(
                customer_id = %ctx.id,
                idle_timeout_s = ctx.idle_timeout.as_secs(),
                last_slot = ?total.last_slot,
                "no new slots within idle timeout; ending stream"
            );
            return Ok(total);
        }

        tokio::select! {
            _ = tokio::time::sleep(FOLLOW_POLL_INTERVAL) => {}
            _ = stop.cancelled() => return Ok(total),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status_from_io(kind: std::io::ErrorKind) -> Status {
        Status::from_error(Box::new(std::io::Error::new(kind, "test")))
    }

    /// A nested cause chain, as tonic → hyper → io produces in practice.
    #[derive(Debug)]
    struct Wrapper(std::io::Error);
    impl std::fmt::Display for Wrapper {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "h2 protocol error: error reading a body from connection")
        }
    }
    impl std::error::Error for Wrapper {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn hangup_detected_for_broken_pipe_and_reset() {
        for kind in [
            std::io::ErrorKind::BrokenPipe,
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::UnexpectedEof,
        ] {
            assert!(
                is_client_hangup(&status_from_io(kind)),
                "{kind:?} should read as a client hangup"
            );
        }
    }

    /// The real shape: the io error is two levels down, under an h2/hyper error.
    #[test]
    fn hangup_detected_through_a_nested_cause_chain() {
        let inner = std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "stream closed because of a broken pipe",
        );
        let status = Status::from_error(Box::new(Wrapper(inner)));
        assert!(is_client_hangup(&status));
    }

    /// The shape a real disconnect actually produces: the io kind survives only
    /// in the rendered chain, not through `source()`.
    #[test]
    fn hangup_detected_from_rendered_chain_when_source_hides_it() {
        #[derive(Debug)]
        struct Opaque;
        impl std::fmt::Display for Opaque {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "hyper::Error(Body, Error {{ kind: Io(Custom {{ kind: BrokenPipe, \
                     error: \"stream closed because of a broken pipe\" }}) }})"
                )
            }
        }
        impl std::error::Error for Opaque {}

        let status = Status::from_error(Box::new(Opaque));
        assert!(
            is_client_hangup(&status),
            "must fall back to the rendered chain"
        );
    }

    #[test]
    fn cancelled_is_a_hangup() {
        assert!(is_client_hangup(&Status::cancelled("client went away")));
    }

    /// Genuine faults must keep warning — this is the whole point of splitting
    /// the two cases apart.
    #[test]
    fn real_stream_errors_are_not_hangups() {
        assert!(!is_client_hangup(&Status::internal("decode blew up")));
        assert!(!is_client_hangup(&status_from_io(
            std::io::ErrorKind::InvalidData
        )));
    }

    use prost::Message as _;
    use sillage_common::chunk::{write_len_prefixed, ChunkMeta, SCHEMA_VERSION};
    use sillage_common::config::PacingConfig;
    use sillage_common::idx::{IdxHeader, IDX_MAGIC, IDX_VERSION};
    use sillage_common::Stream;
    use sillage_reader::storage::ChunkCatalog;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;
    use yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, SubscribeRequestFilterBlocksMeta, SubscribeUpdateBlockMeta,
    };

    /// A `blocks_meta` subscription matches every ordinal in a chunk, so the
    /// index needs only a message count — no dimension entries to construct.
    fn block_idx_bytes(message_count: u64) -> Vec<u8> {
        let header = IdxHeader {
            stream: "block".to_string(),
            start_slot: 0,
            end_slot: 0,
            message_count,
            dimensions: Vec::new(),
        };
        let header_bytes = rmp_serde::to_vec_named(&header).unwrap();
        let mut buffer = Vec::new();
        buffer.extend_from_slice(IDX_MAGIC);
        buffer.push(IDX_VERSION);
        buffer.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&header_bytes);
        buffer
    }

    fn encode_zst(slots: &[u64]) -> Vec<u8> {
        let mut framed = Vec::new();
        for &slot in slots {
            let msg = SubscribeUpdate {
                update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                    slot,
                    ..Default::default()
                })),
                ..Default::default()
            };
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

    fn write_block_chunk(dir: &Path, start_slot: u64, end_slot_exclusive: u64, slots: &[u64]) {
        let stream_dir = dir.join("chunks").join(Stream::Block.as_str());
        fs::create_dir_all(&stream_dir).unwrap();
        let stem = format!("{:012}-{:012}", start_slot, end_slot_exclusive);
        fs::write(stream_dir.join(format!("{stem}.zst")), encode_zst(slots)).unwrap();
        fs::write(
            stream_dir.join(format!("{stem}.idx")),
            block_idx_bytes(slots.len() as u64),
        )
        .unwrap();
        let meta = ChunkMeta {
            schema_version: SCHEMA_VERSION,
            stream: Stream::Block.as_str().to_string(),
            start_slot,
            end_slot_exclusive,
            first_message_slot: slots.first().copied(),
            last_message_slot: slots.last().copied(),
            message_count: slots.len() as u64,
            uncompressed_bytes: 0,
            compressed_bytes: 0,
            recv_ns_first: Some(0),
            recv_ns_last: Some(0),
            sealed_reason: "test".to_string(),
            index_dimensions: Vec::new(),
        };
        fs::write(
            stream_dir.join(format!("{stem}.meta.json")),
            serde_json::to_string(&meta).unwrap(),
        )
        .unwrap();
    }

    fn block_filters() -> SubscriptionFilters {
        SubscriptionFilters {
            transactions: vec![],
            accounts: vec![],
            blocks_meta: vec![(
                "sub".to_string(),
                SubscribeRequestFilterBlocksMeta::default(),
            )],
            from_slot: None,
        }
    }

    fn context(dir: &Path, catalog: SharedCatalog, idle: Duration) -> FollowContext {
        let _ = dir;
        FollowContext {
            id: "test-customer".to_string(),
            catalog,
            filters: block_filters(),
            index_cache: Arc::new(IndexCache::new(1 << 20)),
            chunk_cache: Arc::new(ChunkCache::new(1 << 20)),
            // Pacing off: these tests assert follow/termination logic, not
            // wall-clock behaviour.
            pacing: PacingConfig {
                enabled: false,
                ..PacingConfig::default()
            },
            idle_timeout: idle,
        }
    }

    /// The core of the follow contract: chunks that land after the first pass
    /// drains must still reach an already-connected client.
    #[tokio::test]
    async fn follower_picks_up_chunks_that_land_after_it_connected() {
        let tmp = TempDir::new().unwrap();
        write_block_chunk(tmp.path(), 0, 1000, &[10, 20]);

        let catalog = SharedCatalog::new(ChunkCatalog::scan(tmp.path()));
        let (tx, mut rx) = mpsc::channel(64);
        let stop = ShutdownSignal::new();

        let first_plan = plan_replay(
            &catalog.snapshot(),
            &block_filters(),
            &IndexCache::new(1 << 20),
            None,
        )
        .expect("initial plan");

        let ctx = context(tmp.path(), catalog.clone(), Duration::from_secs(30));
        let handle = tokio::spawn(follow_replay(first_plan, ctx, tx, stop.clone()));

        // Drain the first pass.
        assert!(rx.recv().await.is_some());
        assert!(rx.recv().await.is_some());

        // A new chunk lands and the syncer republishes, exactly as it would in
        // production.
        write_block_chunk(tmp.path(), 1000, 2000, &[1010, 1020]);
        catalog.store(ChunkCatalog::scan(tmp.path()));

        // The already-connected follower must receive it without reconnecting.
        let third = tokio::time::timeout(Duration::from_secs(20), rx.recv())
            .await
            .expect("follower should pick up the new chunk");
        assert!(third.is_some());

        stop.cancel();
        let stats = handle.await.unwrap().unwrap();
        assert!(
            stats.sent >= 3,
            "expected messages from both chunks, got {}",
            stats.sent
        );
    }

    /// Caught up with nothing new arriving: the stream ends rather than hanging
    /// open forever.
    #[tokio::test]
    async fn follower_ends_stream_after_idle_timeout() {
        let tmp = TempDir::new().unwrap();
        write_block_chunk(tmp.path(), 0, 1000, &[10, 20]);

        let catalog = SharedCatalog::new(ChunkCatalog::scan(tmp.path()));
        let (tx, mut rx) = mpsc::channel(64);
        let stop = ShutdownSignal::new();

        let first_plan = plan_replay(
            &catalog.snapshot(),
            &block_filters(),
            &IndexCache::new(1 << 20),
            None,
        )
        .expect("initial plan");

        // Idle timeout shorter than the poll interval so the very first
        // caught-up check gives up.
        let ctx = context(tmp.path(), catalog, Duration::from_millis(1));
        let started = Instant::now();
        let stats = follow_replay(first_plan, ctx, tx, stop)
            .await
            .expect("follow replay");

        assert_eq!(stats.sent, 2, "both messages delivered before giving up");
        assert_eq!(stats.last_slot, Some(20));
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "should end promptly once idle, took {:?}",
            started.elapsed()
        );

        // Sender dropped => stream closes cleanly rather than erroring.
        drop(rx.recv().await);
        while rx.recv().await.is_some() {}
    }
}
