use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::{routing::get, Router};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use sillage_common::shutdown::ShutdownSignal;
use tracing::{info, warn};

use crate::metrics;

pub fn install_recorder() -> PrometheusHandle {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");
    metrics::describe();
    handle
}

pub async fn start_metrics_server(
    addr: SocketAddr,
    handle: PrometheusHandle,
    ready: Arc<AtomicBool>,
    shutdown: ShutdownSignal,
) {
    let app = Router::new()
        .route(
            "/metrics",
            get(move || {
                let h = handle.clone();
                async move { h.render() }
            }),
        )
        .route(
            "/health",
            get(move || {
                let r = ready.clone();
                async move {
                    if r.load(Ordering::Relaxed) {
                        (axum::http::StatusCode::OK, "OK")
                    } else {
                        (axum::http::StatusCode::SERVICE_UNAVAILABLE, "Not Ready")
                    }
                }
            }),
        );

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            warn!("failed to bind metrics server to {}: {}", addr, e);
            return;
        }
    };

    info!("metrics server listening on {}", addr);

    let cancel_token = shutdown.child_token();
    if let Err(e) = axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel_token.cancelled().await })
        .await
    {
        warn!("metrics server error: {}", e);
    }
}
