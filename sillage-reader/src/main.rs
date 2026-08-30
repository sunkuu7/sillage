use anyhow::Result;
use sillage_common::config::Settings;
use sillage_common::logging::init_tracing;
use sillage_common::shutdown::{wait_for_shutdown, ShutdownSignal};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{error, info, warn};

mod customer;
mod grpc;
mod metrics_server;
mod r2;
mod server;
mod sync;

use grpc::GeyserService;
use r2::R2Client;
use sillage_reader::index::IndexCache;
use sillage_reader::metrics;
use sillage_reader::storage::{ChunkCache, ChunkCatalog, SharedCatalog};
use sync::Syncer;

#[tokio::main]
async fn main() -> Result<()> {
    let settings = match Settings::load() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to load configuration: {}", e);
            std::process::exit(1);
        }
    };

    init_tracing(&settings.log);
    let prometheus_handle = metrics_server::install_recorder();

    let addr: SocketAddr = match settings.server.listen_addr.parse() {
        Ok(a) => a,
        Err(e) => {
            error!(
                "Invalid listen address '{}': {}",
                settings.server.listen_addr, e
            );
            std::process::exit(1);
        }
    };

    info!(addr = %addr, "starting sillage-reader");

    // Two-phase shutdown:
    //   `server_shutdown` is cancelled by SIGTERM/SIGINT and propagated to the
    //   gRPC server so it drains gracefully.
    //   `sync_shutdown` is cancelled by main only AFTER the server has
    //   finished, so the syncer can finish in-flight downloads before exiting.
    let server_shutdown = ShutdownSignal::new();
    let sync_shutdown = ShutdownSignal::new();

    tokio::spawn({
        let server = server_shutdown.clone();
        async move {
            if let Err(e) = wait_for_shutdown(server.clone()).await {
                error!(error = %e, "signal handler failed; cancelling server");
                server.cancel();
            }
        }
    });

    let r2_client = R2Client::new(&settings.r2).ok();
    if r2_client.is_none() {
        warn!("R2 credentials missing; syncer will run in local-only mode");
    }

    // The catalog is shared: the server reads snapshots of it, and the syncer
    // publishes a freshly scanned one after any cycle that changed the chunks
    // on disk. Built before the syncer so both hold the same handle.
    let nvme_path = std::path::Path::new(&settings.storage.nvme_path);
    let catalog = SharedCatalog::new(ChunkCatalog::scan(nvme_path));

    let syncer = Syncer::new(
        r2_client,
        settings.reader.clone(),
        settings.storage.clone(),
        catalog.clone(),
    );
    let metrics_cfg = settings.reader.metrics.clone();
    let ready = Arc::new(AtomicBool::new(false));

    info!(
        scan_interval_s = settings.reader.scan_interval_secs,
        max_concurrent = settings.reader.max_concurrent_downloads,
        retention_h = settings.reader.local_retention_hours,
        "syncer configured"
    );

    let cache = Arc::new(ChunkCache::new(settings.reader.decoded_cache_bytes));
    let index_cache = Arc::new(IndexCache::new(settings.reader.index_cache_bytes));
    let auth_token_count = settings.reader.auth_tokens.len();
    if auth_token_count == 0 {
        warn!("reader.auth_tokens is empty; all gRPC connections will be accepted without auth");
    }
    let service = GeyserService::new(
        catalog,
        cache,
        index_cache,
        grpc::ServiceConfig {
            auth_tokens: settings.reader.auth_tokens.clone(),
            subscription_channel_capacity: settings.reader.subscription_channel_capacity,
            follow_idle_timeout: std::time::Duration::from_secs(
                settings.reader.follow_idle_timeout_secs,
            ),
            limits: grpc::ConnectionLimits {
                max_connections_total: settings.reader.max_connections_total,
                max_connections_per_token: settings.reader.max_connections_per_token,
            },
            pacing: settings.reader.pacing.clone(),
        },
        server_shutdown.clone(),
    );

    // Report startup inventory through the service — it owns the catalog +
    // cache the replay path (Phase 5/6) will read from.
    let summary = service.catalog().snapshot().summary();
    for (stream, count, min_start, max_end) in &summary.per_stream {
        info!(%stream, chunk_count = count, min_start_slot = min_start, max_end_slot = max_end, "catalog stream summary");
    }
    let total_chunks: usize = summary
        .per_stream
        .iter()
        .map(|(_, count, _, _)| *count)
        .sum();
    info!(
        total_chunks,
        decoded_cache_budget = settings.reader.decoded_cache_bytes,
        cached_now = service.cache().len(),
        "chunk catalog loaded"
    );
    info!(
        index_cache_budget = settings.reader.index_cache_bytes,
        cached_now = service.index_cache().len(),
        "index cache loaded"
    );
    info!(
        auth_token_count,
        subscription_channel_capacity = settings.reader.subscription_channel_capacity,
        active_connections = service.active_connections(),
        "gRPC service configured"
    );

    ::metrics::gauge!(metrics::READER_READY).set(1.0);
    ready.store(true, Ordering::SeqCst);

    let tls_cfg = settings.server.tls.clone();
    let server_handle = tokio::spawn({
        let shutdown = server_shutdown.clone();
        async move {
            match server::start_server(addr, service, &tls_cfg, shutdown).await {
                Ok(()) => true,
                Err(e) => {
                    error!("Server error: {:#}", e);
                    false
                }
            }
        }
    });

    if metrics_cfg.enabled {
        let metrics_addr: SocketAddr = match metrics_cfg.listen_addr.parse() {
            Ok(a) => a,
            Err(e) => {
                error!(
                    "Invalid metrics listen address '{}': {}",
                    metrics_cfg.listen_addr, e
                );
                std::process::exit(1);
            }
        };
        tokio::spawn({
            let shutdown = server_shutdown.clone();
            let handle = prometheus_handle.clone();
            let ready = ready.clone();
            async move {
                metrics_server::start_metrics_server(metrics_addr, handle, ready, shutdown).await;
            }
        });
        info!(addr = %metrics_cfg.listen_addr, "metrics server enabled");
    } else {
        warn!("metrics server disabled");
    }

    let syncer_handle = tokio::spawn({
        let shutdown = sync_shutdown.clone();
        async move {
            if let Err(e) = syncer.run(shutdown).await {
                error!("Syncer error: {}", e);
            }
        }
    });

    // Wait for server to finish (either from error or shutdown signal)
    let server_ok = match server_handle.await {
        Ok(ok) => ok,
        Err(e) => {
            error!(error = %e, "server task panicked");
            false
        }
    };

    // Server has drained; now cancel syncer shutdown so it can finish
    // in-flight downloads and exit cleanly.
    sync_shutdown.cancel();

    if let Err(e) = syncer_handle.await {
        error!(error = %e, "syncer task panicked");
    }

    // A listener that never came up — a bad TLS identity, an address already in
    // use — must not look like a clean exit, or a supervisor will treat the
    // process as having finished its work. Reported only after the syncer has
    // drained, so the two-phase shutdown still holds.
    if !server_ok {
        error!("shutting down after gRPC listener failure");
        std::process::exit(1);
    }

    info!("shutdown complete");
    Ok(())
}
