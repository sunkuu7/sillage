use std::time::Duration;

use anyhow::{Context, Result};
use futures::future::join_all;
use sillage_common::{
    config::Settings,
    logging::init_tracing,
    shutdown::{wait_for_shutdown, ShutdownSignal},
    Stream,
};
use tracing::{error, info, warn};

mod lane;
use lane::Lane;

mod stamp;

mod geyser;

mod chunker;

mod index;

mod r2;
use r2::R2Client;

mod recovery;

mod uploader;
use uploader::Uploader;

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::load().context("loading configuration")?;

    init_tracing(&settings.log);

    let geyser_config = settings
        .geyser
        .clone()
        .context("[geyser] section missing in config; writer cannot start")?;
    let writer_config = settings.writer.clone();
    let storage_config = settings.storage.clone();

    info!("starting sillage-writer");

    // Two-phase shutdown:
    //   `lane_shutdown` is cancelled by SIGTERM/SIGINT and propagated to the
    //   Geyser lanes so they drain and seal their final chunks.
    //   `uploader_shutdown` is cancelled by main only AFTER the lanes have
    //   joined, with a grace window so the uploader's periodic scan picks up
    //   chunks sealed during the lane drain phase.
    let lane_shutdown = ShutdownSignal::new();
    let uploader_shutdown = ShutdownSignal::new();

    tokio::spawn({
        let lane = lane_shutdown.clone();
        let upl = uploader_shutdown.clone();
        async move {
            if let Err(e) = wait_for_shutdown(lane.clone()).await {
                error!(error = %e, "signal handler failed; cancelling all");
                lane.cancel();
                upl.cancel();
            }
        }
    });

    info!(
        retention_h = settings.uploader.local_retention_hours,
        warn_pct = settings.uploader.disk_pressure_warn_pct,
        "uploader configured"
    );

    // Crash recovery: sweep orphan .partial files and discover per-stream
    // resume slots from existing sealed chunks. Must run before lanes spawn.
    let recovery = recovery::run_recovery(std::path::Path::new(&storage_config.nvme_path))
        .context("startup recovery sweep failed")?;
    info!(
        partials_removed = recovery.partials_removed,
        "startup recovery sweep complete"
    );
    let mut resume_slots: std::collections::HashMap<Stream, Option<u64>> =
        std::collections::HashMap::new();
    for s in &recovery.per_stream {
        info!(
            stream = %s.stream,
            resume_slot = ?s.resume_slot,
            unuploaded = s.unuploaded,
            "stream recovery state"
        );
        resume_slots.insert(s.stream, s.resume_slot);
    }

    let lanes: Vec<_> = Stream::all()
        .into_iter()
        .map(|stream| {
            let lane = Lane::new(stream, lane_shutdown.clone());
            let from_slot = resume_slots.get(&stream).copied().unwrap_or(None);
            tokio::spawn(lane.run(
                geyser_config.clone(),
                writer_config.clone(),
                storage_config.clone(),
                from_slot,
            ))
        })
        .collect();

    let r2_client = R2Client::new(&settings.r2).ok();
    if r2_client.is_none() {
        warn!("R2 credentials missing; uploader will run in local-only mode");
    }
    let uploader = Uploader::new(
        r2_client,
        settings.uploader.clone(),
        settings.storage.clone(),
    );
    let uploader_handle = tokio::spawn(uploader.run(uploader_shutdown.clone()));

    let results = join_all(lanes).await;
    for (stream, res) in Stream::all().into_iter().zip(results) {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!(stream = %stream, error = ?e, "lane exited with error");
                uploader_shutdown.cancel();
                std::process::exit(1);
            }
            Err(e) => {
                error!(stream = %stream, error = %e, "lane task panicked");
                uploader_shutdown.cancel();
                std::process::exit(1);
            }
        }
    }

    // Lanes drained. Grant the uploader a final scan window so it can pick up
    // chunks sealed during shutdown (reason="shutdown") before exiting.
    let grace = Duration::from_secs(settings.uploader.scan_interval_secs + 3);
    info!(
        grace_s = grace.as_secs(),
        "lanes drained; granting uploader final scan window"
    );
    tokio::time::sleep(grace).await;
    uploader_shutdown.cancel();

    match uploader_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => error!(error = ?e, "uploader exited with error"),
        Err(e) => error!(error = %e, "uploader task panicked"),
    }

    info!("shutdown complete");
    Ok(())
}
