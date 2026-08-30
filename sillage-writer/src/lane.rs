use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use sillage_common::config::{GeyserConfig, StorageConfig, WriterConfig};
use sillage_common::{ShutdownSignal, Stream};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tracing::{error, info, instrument, warn};

/// If the first observed slot is more than this many slots ahead of the
/// recovery `from_slot`, log a WARN — the provider likely did not honor
/// the from_slot field (or the validator's slot retention window expired).
/// 50 slots ≈ 20s on mainnet.
const GAP_WARN_THRESHOLD_SLOTS: u64 = 50;

/// Emit the per-lane "first slot observed" log, including any gap between
/// the resume request (`from_slot`) and what the provider actually sent.
/// Called exactly once per lane lifetime on the first message that has a
/// slot we can extract.
fn log_first_slot(stream: Stream, from_slot: Option<u64>, first_slot: u64) {
    match from_slot {
        None => {
            info!(stream = %stream, first_slot, "lane began at tip");
        }
        Some(resume) => {
            let gap = first_slot.saturating_sub(resume);
            if gap > GAP_WARN_THRESHOLD_SLOTS {
                warn!(
                    stream = %stream,
                    resume_slot = resume,
                    first_slot,
                    gap_slots = gap,
                    threshold = GAP_WARN_THRESHOLD_SLOTS,
                    "GAP detected: from_slot likely ignored by provider — data between resume_slot and first_slot is lost"
                );
            } else {
                info!(
                    stream = %stream,
                    resume_slot = resume,
                    first_slot,
                    gap_slots = gap,
                    "resume verified within threshold"
                );
            }
        }
    }
}
use yellowstone_grpc_proto::geyser::SubscribeUpdate;

use crate::chunker::Chunker;
use crate::geyser;
use crate::stamp::Stamped;
use sillage_common::slot::extract_slot;

pub(crate) struct Lane {
    stream: Stream,
    shutdown: ShutdownSignal,
}

impl Lane {
    pub fn new(stream: Stream, shutdown: ShutdownSignal) -> Self {
        Self { stream, shutdown }
    }

    #[instrument(skip(self, geyser, writer, storage), fields(stream = %self.stream))]
    pub async fn run(
        self,
        geyser: GeyserConfig,
        writer: WriterConfig,
        storage: StorageConfig,
        from_slot: Option<u64>,
    ) -> Result<()> {
        let geyser_stream = geyser::subscribe(&geyser, self.stream, from_slot).await?;
        info!(stream = %self.stream, from_slot = ?from_slot, "lane started");

        let (tx, mut rx) = mpsc::channel::<Stamped<SubscribeUpdate>>(writer.channel_capacity);
        let mut chunker = Chunker::new(
            self.stream,
            writer.clone(),
            std::path::Path::new(&storage.nvme_path),
        )?;
        let stream_label = self.stream;

        let consumer = tokio::spawn(async move {
            let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
            heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut count: u64 = 0;
            let mut first_slot_seen = false;
            loop {
                tokio::select! {
                    _ = heartbeat.tick() => {
                        info!(stream = %stream_label, count, "lane heartbeat");
                    }
                    _ = tick.tick() => {
                        if let Err(e) = chunker.tick() {
                            error!(stream = %stream_label, error = ?e, "chunker tick failed");
                            return Err::<u64, anyhow::Error>(e);
                        }
                    }
                    msg = rx.recv() => match msg {
                        Some(stamped) => {
                            if let Some(slot) = extract_slot(&stamped.inner) {
                                if !first_slot_seen {
                                    first_slot_seen = true;
                                    log_first_slot(stream_label, from_slot, slot);
                                }
                                if let Err(e) = chunker.ingest(stamped, slot) {
                                    error!(stream = %stream_label, error = ?e, "chunker ingest failed");
                                    return Err(e);
                                }
                                count += 1;
                                if count % 10_000 == 0 {
                                    info!(stream = %stream_label, count, "processed messages");
                                }
                            }
                        }
                        None => break,
                    }
                }
            }
            if let Err(e) = chunker.shutdown() {
                error!(stream = %stream_label, error = ?e, "chunker shutdown failed");
                return Err(e);
            }
            info!(stream = %stream_label, count, "consumer drained");
            Ok(count)
        });

        let mut pinned = std::pin::pin!(geyser_stream);
        loop {
            tokio::select! {
                result = pinned.next() => {
                    match result {
                        Some(Ok(stamped)) => {
                            tx.send(stamped).await.map_err(|_| anyhow::anyhow!("channel closed"))?;
                        }
                        Some(Err(e)) => {
                            error!(stream = %stream_label, error = %e, "geyser stream error");
                            return Err(e);
                        }
                        None => {
                            info!(stream = %stream_label, "geyser stream ended");
                            break;
                        }
                    }
                }
                _ = self.shutdown.cancelled() => {
                    info!(stream = %stream_label, "shutdown signal received");
                    break;
                }
            }
        }

        drop(tx);
        let total = consumer
            .await
            .map_err(|e| anyhow::anyhow!("consumer task panicked: {e}"))??;
        info!(stream = %stream_label, total, "lane finished");
        Ok(())
    }
}
