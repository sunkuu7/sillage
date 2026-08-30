use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sillage_common::shutdown::ShutdownSignal;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status, Streaming};
use yellowstone_grpc_proto::geyser::{
    geyser_server::Geyser, GetBlockHeightRequest, GetBlockHeightResponse,
    GetLatestBlockhashRequest, GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse,
    GetVersionRequest, GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse,
    PingRequest, PongResponse, SubscribeDeshredRequest, SubscribeReplayInfoRequest,
    SubscribeReplayInfoResponse, SubscribeRequest, SubscribeUpdate, SubscribeUpdateDeshred,
};

use crate::customer::{ClientId, Customer, CustomerHandle};
use ::metrics::counter;
use sillage_common::config::PacingConfig;
use sillage_reader::index::IndexCache;
use sillage_reader::metrics;
use sillage_reader::storage::{ChunkCache, SharedCatalog};
use sillage_reader::subscription::parse_subscribe_request;

static CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_customer_id() -> String {
    let n = CONNECTION_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("customer-{n}")
}

pub(crate) struct ServiceConfig {
    pub auth_tokens: Vec<String>,
    pub subscription_channel_capacity: usize,
    pub follow_idle_timeout: Duration,
    pub limits: ConnectionLimits,
    pub pacing: PacingConfig,
}

/// Concurrency caps applied at admission time.
#[derive(Clone, Copy)]
pub(crate) struct ConnectionLimits {
    pub max_connections_total: usize,
    pub max_connections_per_token: usize,
}

#[derive(Clone)]
pub(crate) struct GeyserService {
    catalog: SharedCatalog,
    cache: Arc<ChunkCache>,
    index_cache: Arc<IndexCache>,
    auth_tokens: Vec<String>,
    subscription_channel_capacity: usize,
    follow_idle_timeout: Duration,
    limits: ConnectionLimits,
    pacing: PacingConfig,
    shutdown: ShutdownSignal,
    active_customers: Arc<Mutex<Vec<CustomerHandle>>>,
}

impl GeyserService {
    pub(crate) fn new(
        catalog: SharedCatalog,
        cache: Arc<ChunkCache>,
        index_cache: Arc<IndexCache>,
        cfg: ServiceConfig,
        shutdown: ShutdownSignal,
    ) -> Self {
        Self {
            catalog,
            cache,
            index_cache,
            auth_tokens: cfg.auth_tokens,
            subscription_channel_capacity: cfg.subscription_channel_capacity,
            follow_idle_timeout: cfg.follow_idle_timeout,
            limits: cfg.limits,
            pacing: cfg.pacing,
            shutdown,
            active_customers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(crate) fn catalog(&self) -> &SharedCatalog {
        &self.catalog
    }

    pub(crate) fn cache(&self) -> &ChunkCache {
        self.cache.as_ref()
    }

    pub(crate) fn index_cache(&self) -> &IndexCache {
        self.index_cache.as_ref()
    }

    /// Snapshot of currently-connected customer count. Uses try_lock so a
    /// concurrent subscribe() in flight doesn't block the caller; on
    /// contention, returns the previous value (acceptable for observability).
    pub(crate) fn active_connections(&self) -> usize {
        self.active_customers
            .try_lock()
            .map(|guard| guard.len())
            .unwrap_or(0)
    }

    fn latest_slot(&self) -> Option<u64> {
        self.catalog
            .snapshot()
            .summary()
            .per_stream
            .iter()
            .filter_map(|(_, _, _, max_end)| *max_end)
            .max()
    }

    fn first_available_slot(&self) -> Option<u64> {
        self.catalog
            .snapshot()
            .summary()
            .per_stream
            .iter()
            .filter_map(|(_, _, min_start, _)| *min_start)
            .min()
    }

    /// Verify the request carries a valid bearer token in `x-token` (text or
    /// binary metadata). Returns the caller's [`ClientId`] on success.
    ///
    /// Auth policy: if `auth_tokens` is empty, accept all connections (allows
    /// local development without configured secrets). When tokens are
    /// configured, missing token → Unauthenticated; mismatched token →
    /// PermissionDenied.
    fn validate_auth<T>(&self, request: &Request<T>) -> Result<ClientId, Status> {
        if self.auth_tokens.is_empty() {
            return Ok(ClientId::Anonymous);
        }

        let metadata = request.metadata();
        let token = metadata
            .get("x-token")
            .and_then(|v| v.to_str().ok().map(|s| s.to_string()))
            .or_else(|| {
                metadata
                    .get_bin("x-token-bin")
                    .and_then(|v| v.to_bytes().ok())
                    .and_then(|b| std::str::from_utf8(&b).ok().map(|s| s.to_string()))
            });

        let token = match token {
            Some(t) => t,
            None => {
                tracing::warn!("auth failed: missing x-token header");
                return Err(Status::unauthenticated("missing x-token header"));
            }
        };

        match self.auth_tokens.iter().position(|t| *t == token) {
            Some(idx) => Ok(ClientId::Token(idx)),
            None => {
                tracing::warn!("auth failed: invalid token");
                Err(Status::permission_denied("invalid token"))
            }
        }
    }

    /// Admit a connection if it fits within the configured concurrency caps,
    /// registering it atomically so two simultaneous subscribes cannot both
    /// slip past a limit they jointly exceed.
    ///
    /// Concurrent connections — not connection *rate* — is what this guards:
    /// each one costs a task, a cursor, and pressure on the shared decode
    /// caches. Rate limiting belongs at the proxy in front.
    async fn admit(&self, handle: CustomerHandle) -> Result<(), Status> {
        let mut customers = self.active_customers.lock().await;

        if customers.len() >= self.limits.max_connections_total {
            counter!(metrics::CONNECTIONS_REJECTED_TOTAL).increment(1);
            tracing::warn!(
                active = customers.len(),
                limit = self.limits.max_connections_total,
                "rejecting subscription: server connection limit reached"
            );
            return Err(Status::resource_exhausted(format!(
                "server is at its connection limit ({})",
                self.limits.max_connections_total
            )));
        }

        // Per-token caps are meaningless when auth is disabled: every caller is
        // Anonymous, so the total cap is the only meaningful bound.
        if handle.client != ClientId::Anonymous {
            let per_token = customers
                .iter()
                .filter(|c| c.client == handle.client)
                .count();
            if per_token >= self.limits.max_connections_per_token {
                counter!(metrics::CONNECTIONS_REJECTED_TOTAL).increment(1);
                tracing::warn!(
                    client = %handle.client,
                    active = per_token,
                    limit = self.limits.max_connections_per_token,
                    "rejecting subscription: per-token connection limit reached"
                );
                return Err(Status::resource_exhausted(format!(
                    "token is at its connection limit ({})",
                    self.limits.max_connections_per_token
                )));
            }
        }

        customers.push(handle);
        Ok(())
    }
}

/// Parse the `x-replay-speed` gRPC metadata header into a per-customer speed
/// multiplier. Returns `default` when the header is absent or unparseable.
/// Clamps the value to `[0.1, 1000.0]` and warns on clamp or parse failure.
fn parse_replay_speed(metadata: &MetadataMap, default: f64) -> f64 {
    const MIN_SPEED: f64 = 0.1;
    const MAX_SPEED: f64 = 1000.0;

    let raw = match metadata.get("x-replay-speed").and_then(|v| v.to_str().ok()) {
        Some(v) => v,
        None => return default,
    };

    match raw.parse::<f64>() {
        Ok(speed) => {
            if speed < MIN_SPEED {
                tracing::warn!(
                    raw = %raw,
                    clamped_to = MIN_SPEED,
                    "x-replay-speed below minimum, clamping"
                );
                MIN_SPEED
            } else if speed > MAX_SPEED {
                tracing::warn!(
                    raw = %raw,
                    clamped_to = MAX_SPEED,
                    "x-replay-speed above maximum, clamping"
                );
                MAX_SPEED
            } else {
                speed
            }
        }
        Err(_) => {
            tracing::warn!(
                raw = %raw,
                default,
                "x-replay-speed is not a valid number, using default"
            );
            default
        }
    }
}

#[tonic::async_trait]
impl Geyser for GeyserService {
    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, Status>> + Send + 'static>>;

    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let client = self.validate_auth(&request)?;

        let customer_id = next_customer_id();
        let metadata = request.metadata().clone();
        let mut inbound = request.into_inner();

        let first_msg = tokio::time::timeout(std::time::Duration::from_secs(5), inbound.message())
            .await
            .map_err(|_| {
                Status::deadline_exceeded("did not receive first SubscribeRequest within 5s")
            })?
            .map_err(|e| Status::internal(format!("inbound stream error: {e}")))?
            .ok_or_else(|| {
                Status::invalid_argument("client closed stream before sending first request")
            })?;

        let parsed = parse_subscribe_request(first_msg)?;

        let mut pacing = self.pacing.clone();
        let header_speed = parse_replay_speed(&metadata, self.pacing.speed_multiplier);
        if (header_speed - self.pacing.speed_multiplier).abs() > f64::EPSILON {
            pacing.speed_multiplier = header_speed;
            tracing::debug!(
                customer_id = %customer_id,
                speed_multiplier = header_speed,
                "x-replay-speed header applied"
            );
        }

        // Admission happens after parsing so a malformed request is reported as
        // such rather than consuming a connection slot, and before the channel
        // and task exist so a rejected caller costs nothing.
        let handle = CustomerHandle {
            customer_id: customer_id.clone(),
            client,
            connected_at: std::time::Instant::now(),
            filter_summary: parsed.summary(),
        };
        self.admit(handle.clone()).await?;

        let (tx, rx) =
            mpsc::channel::<Result<SubscribeUpdate, Status>>(self.subscription_channel_capacity);

        let customer = Customer {
            id: customer_id,
            handle,
            parsed,
            tx,
            shutdown: self.shutdown.clone(),
            // The live handle: a follower re-snapshots per replay pass so it
            // sees chunks the syncer lands after this subscribe.
            catalog: self.catalog.clone(),
            chunk_cache: self.cache.clone(),
            index_cache: self.index_cache.clone(),
            pacing,
            follow_idle_timeout: self.follow_idle_timeout,
            customers: self.active_customers.clone(),
        };

        tokio::spawn(async move {
            customer.run(inbound).await;
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as Self::SubscribeStream))
    }

    type SubscribeDeshredStream =
        Pin<Box<dyn Stream<Item = Result<SubscribeUpdateDeshred, Status>> + Send + 'static>>;

    /// Deshred replay is not supported: the writer does not index shreds,
    /// only BlockMeta. A faithful implementation would need a separate ingest
    /// pipeline.
    async fn subscribe_deshred(
        &self,
        _request: Request<Streaming<SubscribeDeshredRequest>>,
    ) -> Result<Response<Self::SubscribeDeshredStream>, Status> {
        Err(Status::unimplemented(
            "deshred replay is not supported; writer indexes BlockMeta only",
        ))
    }

    async fn subscribe_replay_info(
        &self,
        request: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        self.validate_auth(&request)?;
        Ok(Response::new(SubscribeReplayInfoResponse {
            first_available: self.first_available_slot(),
        }))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        self.validate_auth(&request)?;
        let req = request.into_inner();
        Ok(Response::new(PongResponse {
            count: req.count + 1,
        }))
    }

    /// Reader has no live chain state. Use a validator's RPC for current
    /// blockhash queries.
    async fn get_latest_blockhash(
        &self,
        _request: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        Err(Status::unimplemented(
            "reader has no live chain state; query a validator for blockhash",
        ))
    }

    async fn get_block_height(
        &self,
        request: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        self.validate_auth(&request)?;
        match self.latest_slot() {
            Some(slot) => Ok(Response::new(GetBlockHeightResponse { block_height: slot })),
            None => Err(Status::not_found("no chunks available")),
        }
    }

    async fn get_slot(
        &self,
        request: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        self.validate_auth(&request)?;
        match self.latest_slot() {
            Some(slot) => Ok(Response::new(GetSlotResponse { slot })),
            None => Err(Status::not_found("no chunks available")),
        }
    }

    /// Reader has no live chain state.
    async fn is_blockhash_valid(
        &self,
        _request: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        Err(Status::unimplemented(
            "reader has no live chain state; query a validator for blockhash validity",
        ))
    }

    async fn get_version(
        &self,
        request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        self.validate_auth(&request)?;
        Ok(Response::new(GetVersionResponse {
            version: format!(
                "{{\"name\":\"sillage-reader\",\"version\":\"{}\"}}",
                env!("CARGO_PKG_VERSION")
            ),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sillage_common::chunk::{ChunkMeta, SCHEMA_VERSION};
    use sillage_common::Stream;
    use sillage_reader::storage::ChunkCatalog;
    use std::fs;
    use tempfile::TempDir;
    use yellowstone_grpc_proto::geyser::geyser_server::GeyserServer;

    fn empty_service() -> GeyserService {
        empty_service_with_auth(vec![])
    }

    fn service_with_limits(tokens: Vec<String>, total: usize, per_token: usize) -> GeyserService {
        let tmp = TempDir::new().unwrap();
        let catalog = SharedCatalog::new(ChunkCatalog::scan(tmp.path()));
        GeyserService::new(
            catalog,
            Arc::new(ChunkCache::new(1024)),
            Arc::new(IndexCache::new(1024)),
            ServiceConfig {
                auth_tokens: tokens,
                subscription_channel_capacity: 1024,
                follow_idle_timeout: Duration::from_secs(900),
                limits: ConnectionLimits {
                    max_connections_total: total,
                    max_connections_per_token: per_token,
                },
                pacing: PacingConfig::default(),
            },
            ShutdownSignal::new(),
        )
    }

    fn handle_for(client: ClientId, n: usize) -> CustomerHandle {
        CustomerHandle {
            customer_id: format!("customer-{n}"),
            client,
            connected_at: std::time::Instant::now(),
            filter_summary: "tx=1".to_string(),
        }
    }

    #[tokio::test]
    async fn admit_rejects_past_the_server_total() {
        let svc = service_with_limits(vec![], 2, 16);
        for n in 0..2 {
            svc.admit(handle_for(ClientId::Anonymous, n))
                .await
                .expect("under the cap");
        }
        let err = svc
            .admit(handle_for(ClientId::Anonymous, 2))
            .await
            .expect_err("third connection exceeds the total");
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        assert!(
            err.message().contains("connection limit"),
            "{}",
            err.message()
        );
        assert_eq!(
            svc.active_connections(),
            2,
            "rejected caller was not registered"
        );
    }

    /// One noisy token must not consume the whole server budget.
    #[tokio::test]
    async fn admit_rejects_past_the_per_token_cap() {
        let svc = service_with_limits(vec!["a".into(), "b".into()], 100, 2);
        for n in 0..2 {
            svc.admit(handle_for(ClientId::Token(0), n)).await.unwrap();
        }
        let err = svc
            .admit(handle_for(ClientId::Token(0), 2))
            .await
            .expect_err("token 0 is at its cap");
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        assert!(err.message().contains("token"), "{}", err.message());

        // A different token has its own budget.
        svc.admit(handle_for(ClientId::Token(1), 3))
            .await
            .expect("token 1 is unaffected by token 0's usage");
    }

    /// With auth disabled every caller is Anonymous, so a per-token cap would
    /// otherwise throttle the whole server to one client's allowance.
    #[tokio::test]
    async fn per_token_cap_does_not_apply_when_auth_is_disabled() {
        let svc = service_with_limits(vec![], 100, 1);
        for n in 0..5 {
            svc.admit(handle_for(ClientId::Anonymous, n))
                .await
                .expect("anonymous callers are bounded only by the total");
        }
        assert_eq!(svc.active_connections(), 5);
    }

    #[tokio::test]
    async fn validate_auth_maps_tokens_to_stable_ids() {
        let svc = service_with_limits(vec!["first".into(), "second".into()], 10, 10);
        let mut req = Request::new(());
        req.metadata_mut()
            .insert("x-token", "second".parse().unwrap());
        assert_eq!(svc.validate_auth(&req).unwrap(), ClientId::Token(1));

        let anon = service_with_limits(vec![], 10, 10);
        assert_eq!(
            anon.validate_auth(&Request::new(())).unwrap(),
            ClientId::Anonymous
        );
    }

    /// The id is derived from position, never the secret itself.
    #[test]
    fn client_id_display_never_leaks_the_token() {
        assert_eq!(ClientId::Token(3).to_string(), "token-3");
        assert_eq!(ClientId::Anonymous.to_string(), "anonymous");
    }

    fn empty_service_with_auth(tokens: Vec<String>) -> GeyserService {
        let tmp = TempDir::new().unwrap();
        let catalog = SharedCatalog::new(ChunkCatalog::scan(tmp.path()));
        let cache = Arc::new(ChunkCache::new(1024));
        let index_cache = Arc::new(IndexCache::new(1024));
        GeyserService::new(
            catalog,
            cache,
            index_cache,
            ServiceConfig {
                auth_tokens: tokens,
                subscription_channel_capacity: 1024,
                follow_idle_timeout: Duration::from_secs(900),
                limits: ConnectionLimits {
                    max_connections_total: 256,
                    max_connections_per_token: 16,
                },
                pacing: PacingConfig::default(),
            },
            ShutdownSignal::new(),
        )
    }

    fn service_with_chunks(slots: &[(Stream, u64, u64)]) -> (GeyserService, TempDir) {
        let tmp = TempDir::new().unwrap();
        for &(stream, start, end) in slots {
            write_trio(tmp.path(), stream, start, end);
        }
        let catalog = SharedCatalog::new(ChunkCatalog::scan(tmp.path()));
        let cache = Arc::new(ChunkCache::new(1024));
        let index_cache = Arc::new(IndexCache::new(1024));
        let service = GeyserService::new(
            catalog,
            cache,
            index_cache,
            ServiceConfig {
                auth_tokens: vec![],
                subscription_channel_capacity: 1024,
                follow_idle_timeout: Duration::from_secs(900),
                limits: ConnectionLimits {
                    max_connections_total: 256,
                    max_connections_per_token: 16,
                },
                pacing: PacingConfig::default(),
            },
            ShutdownSignal::new(),
        );
        (service, tmp)
    }

    fn write_trio(dir: &std::path::Path, stream: Stream, start_slot: u64, end_slot_exclusive: u64) {
        let stream_dir = dir.join("chunks").join(stream.as_str());
        fs::create_dir_all(&stream_dir).unwrap();
        let stem = format!("{:012}-{:012}", start_slot, end_slot_exclusive);
        let zst_path = stream_dir.join(format!("{stem}.zst"));
        let idx_path = stream_dir.join(format!("{stem}.idx"));
        let meta_path = stream_dir.join(format!("{stem}.meta.json"));
        fs::write(&zst_path, b"compressed-data").unwrap();
        fs::write(&idx_path, b"index-data").unwrap();
        let meta = ChunkMeta {
            schema_version: SCHEMA_VERSION,
            stream: stream.as_str().to_string(),
            start_slot,
            end_slot_exclusive,
            first_message_slot: Some(start_slot),
            last_message_slot: Some(end_slot_exclusive - 1),
            message_count: end_slot_exclusive - start_slot,
            uncompressed_bytes: 4096,
            compressed_bytes: 1024,
            recv_ns_first: Some(1_000_000),
            recv_ns_last: Some(2_000_000),
            sealed_reason: "watermark".to_string(),
            index_dimensions: vec!["program_id".to_string()],
        };
        fs::write(&meta_path, serde_json::to_string(&meta).unwrap()).unwrap();
    }

    #[test]
    fn test_geyser_service_implements_trait() {
        let service = empty_service();
        let _server = GeyserServer::new(service);
    }

    #[test]
    fn test_geyser_service_is_cloneable() {
        let service = empty_service();
        let _cloned = service.clone();
    }

    #[tokio::test]
    async fn test_ping_increments_count() {
        let service = empty_service();
        let result = service.ping(Request::new(PingRequest { count: 5 })).await;
        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert_eq!(response.count, 6);
    }

    #[tokio::test]
    async fn test_get_version_returns_json() {
        let service = empty_service();
        let result = service
            .get_version(Request::new(GetVersionRequest {}))
            .await;
        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert!(response.version.contains("sillage-reader"));
        assert!(response.version.contains(env!("CARGO_PKG_VERSION")));
    }

    #[tokio::test]
    async fn test_get_slot_returns_latest_slot() {
        let (service, _tmp) = service_with_chunks(&[
            (Stream::Tx, 0, 1000),
            (Stream::Tx, 1000, 2500),
            (Stream::Acct, 0, 500),
        ]);
        let result = service
            .get_slot(Request::new(GetSlotRequest { commitment: None }))
            .await;
        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert_eq!(response.slot, 2500);
    }

    #[tokio::test]
    async fn test_get_block_height_returns_latest_slot() {
        let (service, _tmp) =
            service_with_chunks(&[(Stream::Tx, 0, 1000), (Stream::Block, 0, 3000)]);
        let result = service
            .get_block_height(Request::new(GetBlockHeightRequest { commitment: None }))
            .await;
        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert_eq!(response.block_height, 3000);
    }

    #[tokio::test]
    async fn test_get_slot_empty_catalog_returns_not_found() {
        let service = empty_service();
        let result = service
            .get_slot(Request::new(GetSlotRequest { commitment: None }))
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert!(err.message().contains("no chunks available"));
    }

    #[tokio::test]
    async fn test_get_block_height_empty_catalog_returns_not_found() {
        let service = empty_service();
        let result = service
            .get_block_height(Request::new(GetBlockHeightRequest { commitment: None }))
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert!(err.message().contains("no chunks available"));
    }

    #[tokio::test]
    async fn test_subscribe_replay_info_empty_catalog_returns_none() {
        let service = empty_service();
        let result = service
            .subscribe_replay_info(Request::new(SubscribeReplayInfoRequest {}))
            .await;
        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert!(response.first_available.is_none());
    }

    #[tokio::test]
    async fn test_subscribe_replay_info_returns_min_start_slot() {
        let (service, _tmp) = service_with_chunks(&[
            (Stream::Tx, 1000, 2000),
            (Stream::Acct, 500, 1500),
            (Stream::Block, 2000, 3000),
        ]);
        let result = service
            .subscribe_replay_info(Request::new(SubscribeReplayInfoRequest {}))
            .await;
        assert!(result.is_ok());
        let response = result.unwrap().into_inner();
        assert_eq!(response.first_available, Some(500));
    }

    #[tokio::test]
    async fn test_still_unimplemented_methods_remain_unimplemented() {
        let service = empty_service();

        let result = service
            .get_latest_blockhash(Request::new(GetLatestBlockhashRequest { commitment: None }))
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unimplemented);

        let result = service
            .is_blockhash_valid(Request::new(IsBlockhashValidRequest {
                blockhash: String::new(),
                commitment: None,
            }))
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unimplemented);
    }

    fn request_with_token<T>(req: T, token: &str) -> Request<T> {
        let mut request = Request::new(req);
        request
            .metadata_mut()
            .insert("x-token", token.parse().unwrap());
        request
    }

    #[tokio::test]
    async fn test_auth_no_config_allows_all() {
        let service = empty_service_with_auth(vec![]);
        let result = service.ping(Request::new(PingRequest { count: 1 })).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_auth_valid_token() {
        let service = empty_service_with_auth(vec!["valid-token".to_string()]);
        let request = request_with_token(PingRequest { count: 1 }, "valid-token");
        let result = service.ping(request).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_auth_missing_token() {
        let service = empty_service_with_auth(vec!["valid-token".to_string()]);
        let result = service.ping(Request::new(PingRequest { count: 1 })).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("missing x-token header"));
    }

    #[tokio::test]
    async fn test_auth_invalid_token() {
        let service = empty_service_with_auth(vec!["valid-token".to_string()]);
        let request = request_with_token(PingRequest { count: 1 }, "invalid-token");
        let result = service.ping(request).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(err.message().contains("invalid token"));
    }

    #[tokio::test]
    async fn test_auth_binary_token() {
        let service = empty_service_with_auth(vec!["binary-token".to_string()]);
        let mut request = Request::new(PingRequest { count: 1 });
        request.metadata_mut().insert_bin(
            "x-token-bin",
            tonic::metadata::MetadataValue::from_bytes("binary-token".as_bytes()),
        );
        let result = service.ping(request).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_active_connections_starts_at_zero() {
        let service = empty_service_with_auth(vec!["secret".to_string()]);
        assert_eq!(service.active_connections(), 0);
    }

    #[test]
    fn test_next_customer_id_is_unique() {
        let a = next_customer_id();
        let b = next_customer_id();
        let c = next_customer_id();
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert!(a.starts_with("customer-"));
    }

    #[test]
    fn test_parse_replay_speed_missing_header_returns_default() {
        let metadata = MetadataMap::new();
        assert_eq!(parse_replay_speed(&metadata, 1.0), 1.0);
        assert_eq!(parse_replay_speed(&metadata, 2.5), 2.5);
    }

    #[test]
    fn test_parse_replay_speed_valid_value() {
        let mut metadata = MetadataMap::new();
        metadata.insert("x-replay-speed", "3.5".parse().unwrap());
        assert_eq!(parse_replay_speed(&metadata, 1.0), 3.5);
    }

    #[test]
    fn test_parse_replay_speed_clamps_below_minimum() {
        let mut metadata = MetadataMap::new();
        metadata.insert("x-replay-speed", "0.01".parse().unwrap());
        assert_eq!(parse_replay_speed(&metadata, 1.0), 0.1);
    }

    #[test]
    fn test_parse_replay_speed_clamps_above_maximum() {
        let mut metadata = MetadataMap::new();
        metadata.insert("x-replay-speed", "5000.0".parse().unwrap());
        assert_eq!(parse_replay_speed(&metadata, 1.0), 1000.0);
    }

    #[test]
    fn test_parse_replay_speed_exact_minimum() {
        let mut metadata = MetadataMap::new();
        metadata.insert("x-replay-speed", "0.1".parse().unwrap());
        assert_eq!(parse_replay_speed(&metadata, 1.0), 0.1);
    }

    #[test]
    fn test_parse_replay_speed_exact_maximum() {
        let mut metadata = MetadataMap::new();
        metadata.insert("x-replay-speed", "1000.0".parse().unwrap());
        assert_eq!(parse_replay_speed(&metadata, 1.0), 1000.0);
    }

    #[test]
    fn test_parse_replay_speed_non_parseable_returns_default() {
        let mut metadata = MetadataMap::new();
        metadata.insert("x-replay-speed", "not-a-number".parse().unwrap());
        assert_eq!(parse_replay_speed(&metadata, 1.0), 1.0);
    }

    #[test]
    fn test_parse_replay_speed_negative_returns_minimum() {
        let mut metadata = MetadataMap::new();
        metadata.insert("x-replay-speed", "-5.0".parse().unwrap());
        assert_eq!(parse_replay_speed(&metadata, 1.0), 0.1);
    }
}
