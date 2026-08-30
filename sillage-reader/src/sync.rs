use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use sillage_common::config::ReaderConfig;
use sillage_common::config::StorageConfig;
#[cfg(test)]
use sillage_common::config::{MetricsConfig, PacingConfig};
use sillage_common::shutdown::ShutdownSignal;
use sillage_common::Stream;
use tracing::{error, info, warn};

use ::metrics::{counter, histogram};
use sillage_reader::metrics;
use sillage_reader::storage::{ChunkCatalog, SharedCatalog};

use crate::r2::{R2Chunk, R2Client};

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub(crate) struct ChunkKey {
    pub stream: Stream,
    pub start_slot: u64,
    pub end_slot: u64,
}

/// Walk each stream directory under `nvme_path/chunks/`, looking for
/// `*.meta.json` filenames. Parses `(start_slot, end_slot)` from the
/// filename pattern `{start_slot:012}-{end_slot:012}.meta.json`.
///
/// Returns a `HashSet<ChunkKey>` of all locally-present chunks.
pub(crate) fn local_chunks(nvme_path: &Path) -> HashSet<ChunkKey> {
    let mut result = HashSet::new();

    for stream in Stream::all() {
        let dir = nvme_path.join("chunks").join(stream.as_str());
        if !dir.is_dir() {
            continue;
        }

        let entries = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(_) => continue,
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
                Err(_) => continue,
            };
            let end_slot = match end_str.parse::<u64>() {
                Ok(s) => s,
                Err(_) => continue,
            };

            result.insert(ChunkKey {
                stream,
                start_slot,
                end_slot,
            });
        }
    }

    result
}

/// Compute which remote chunks need to be synced down.
///
/// 1. Finds `newest_end_slot` = max of all remote `end_slot` values (0 if empty).
/// 2. Computes `retention_slots = retention_hours * 3600 * 5 / 2` (i.e. × 2.5 as u64).
/// 3. `cutoff = newest_end_slot.saturating_sub(retention_slots)`.
/// 4. Filters remote to chunks where `end_slot >= cutoff`.
/// 5. Removes chunks already present in `local`.
/// 6. Returns results sorted by `(stream.as_str(), start_slot)` ascending.
pub(crate) fn diff_for_sync(
    local: &HashSet<ChunkKey>,
    remote: &[R2Chunk],
    retention_hours: u64,
) -> Vec<R2Chunk> {
    let newest_end_slot = remote.iter().map(|c| c.end_slot).max().unwrap_or(0);
    let retention_slots = retention_hours * 3600 * 5 / 2;
    let cutoff = newest_end_slot.saturating_sub(retention_slots);

    let mut needed: Vec<R2Chunk> = remote
        .iter()
        .filter(|c| c.end_slot >= cutoff)
        .filter(|c| {
            !local.contains(&ChunkKey {
                stream: c.stream,
                start_slot: c.start_slot,
                end_slot: c.end_slot,
            })
        })
        .cloned()
        .collect();

    needed.sort_by(|a, b| {
        let key_a = (a.stream.as_str(), a.start_slot);
        let key_b = (b.stream.as_str(), b.start_slot);
        key_a.cmp(&key_b)
    });

    needed
}

/// Download a single file from R2 with exponential-backoff retry.
///
/// Retries up to `cfg.retry_attempts` times. On each failure before the last
/// attempt, logs a WARN and sleeps `cfg.retry_initial_delay_ms * 2^attempt` ms.
/// On the final failure, logs ERROR and returns the error.
pub(crate) async fn get_with_retry(
    r2: &R2Client,
    key: &str,
    dest: &Path,
    cfg: &ReaderConfig,
) -> Result<u64> {
    for attempt in 0..cfg.retry_attempts {
        let start = Instant::now();
        match r2.get_file(key, dest).await {
            Ok(bytes) => {
                histogram!(metrics::R2_FETCH_SECONDS).record(start.elapsed().as_secs_f64());
                counter!(metrics::R2_BYTES_DOWNLOADED_TOTAL).increment(bytes);
                return Ok(bytes);
            }
            Err(e) => {
                if attempt < cfg.retry_attempts - 1 {
                    warn!(
                        attempt = attempt + 1,
                        key, "download attempt failed, retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(
                        cfg.retry_initial_delay_ms * 2u64.pow(attempt),
                    ))
                    .await;
                } else {
                    error!(
                        attempt = attempt + 1,
                        key,
                        error = %e,
                        "download failed after all retries"
                    );
                    return Err(e);
                }
            }
        }
    }

    // Reached only when retry_attempts == 0
    anyhow::bail!("no retry attempts configured for key {key}")
}

/// Download all three files (.zst, .idx, .meta.json) for a chunk from R2.
///
/// Downloads each extension in order, writing to a `.partial` file first,
/// fsyncing, then atomically renaming to the final path. The `.meta.json`
/// file is always written last so its presence signals a complete local chunk.
pub(crate) async fn download_chunk(
    r2: &R2Client,
    chunk: &R2Chunk,
    dest_root: &Path,
    cfg: &ReaderConfig,
) -> Result<()> {
    for ext in ["zst", "idx", "meta.json"] {
        let key = format!(
            "chunks/{}/{:012}-{:012}.{}",
            chunk.stream.as_str(),
            chunk.start_slot,
            chunk.end_slot,
            ext
        );
        let final_path = dest_root
            .join("chunks")
            .join(chunk.stream.as_str())
            .join(format!(
                "{:012}-{:012}.{}",
                chunk.start_slot, chunk.end_slot, ext
            ));
        let partial_path_str = format!("{}.partial", final_path.display());
        let partial_path = Path::new(&partial_path_str);

        let Some(parent) = final_path.parent() else {
            return Err(anyhow::anyhow!("final_path has no parent"));
        };
        tokio::fs::create_dir_all(parent).await?;

        get_with_retry(r2, &key, partial_path, cfg).await?;
        std::fs::rename(partial_path, &final_path)?;
    }

    counter!(metrics::R2_CHUNKS_FETCHED_TOTAL).increment(1);
    Ok(())
}

#[cfg(test)]
fn path_mtime_ns(path: &Path) -> Option<u64> {
    std::fs::metadata(path)
        .ok()?
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos() as u64)
}

pub(crate) fn sweep_partials(nvme_path: &Path) -> Result<u32> {
    let mut deleted: u32 = 0;
    for stream in Stream::all() {
        let dir = nvme_path.join("chunks").join(stream.as_str());
        if !dir.is_dir() {
            continue;
        }
        let entries = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) => {
                warn!(stream = %stream, error = %e, "failed to read stream dir for partial sweep");
                continue;
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n,
                None => continue,
            };
            if !name.ends_with(".partial") {
                continue;
            }
            if let Err(e) = std::fs::remove_file(&path) {
                warn!(path = %path.display(), error = %e, "failed to delete partial file");
            } else {
                deleted += 1;
            }
        }
    }
    Ok(deleted)
}

/// Evict local chunks that fall outside the slot-based retention window.
///
/// Retention is anchored on the **newest chunk we currently hold locally**, not
/// on wall-clock time. We keep every chunk whose `end_slot` is at or above
/// `newest_local_end_slot - retention_slots` and delete the rest. This is the
/// same window definition `diff_for_sync` uses to decide what to download, so
/// the two can never disagree:
/// a chunk the sync loop would re-download is never swept, eliminating the
/// download→evict→re-download churn that a wall-clock anchor produces against a
/// stale bucket (where `recv_ns_last` reflects the *writer's* receipt time —
/// a different clock from the reader's).
///
/// Because the floor is newest-relative, a stalled upstream (no new chunks
/// arriving) simply freezes the cache at its current window rather than draining
/// it — serving the newest-available data beats serving nothing. Disk stays
/// bounded either way at `retention_slots` worth of chunks.
pub(crate) fn sweep_retention(nvme_path: &Path, retention_hours: u64) -> Result<u32> {
    let retention_slots = retention_hours * 3600 * 5 / 2;
    let local = local_chunks(nvme_path);
    let newest_end_slot = local.iter().map(|c| c.end_slot).max().unwrap_or(0);
    let cutoff = newest_end_slot.saturating_sub(retention_slots);

    let mut evicted: u32 = 0;
    for chunk in &local {
        if chunk.end_slot >= cutoff {
            continue;
        }
        let dir = nvme_path.join("chunks").join(chunk.stream.as_str());
        let stem = format!("{:012}-{:012}", chunk.start_slot, chunk.end_slot);
        for ext in ["zst", "idx", "meta.json"] {
            let file_path = dir.join(format!("{stem}.{ext}"));
            if let Err(e) = std::fs::remove_file(&file_path) {
                warn!(path = %file_path.display(), error = %e, "failed to delete file during retention sweep");
            }
        }
        evicted += 1;
    }
    Ok(evicted)
}

pub(crate) struct Syncer {
    r2: Option<R2Client>,
    cfg: ReaderConfig,
    storage_cfg: StorageConfig,
    catalog: SharedCatalog,
}

impl Syncer {
    pub fn new(
        r2: Option<R2Client>,
        cfg: ReaderConfig,
        storage_cfg: StorageConfig,
        catalog: SharedCatalog,
    ) -> Self {
        Self {
            r2,
            cfg,
            storage_cfg,
            catalog,
        }
    }

    /// Rescan the NVMe directory and publish the result as the new serving
    /// catalog. Called only after a cycle that actually changed the on-disk
    /// chunk set, so idle cycles cost nothing.
    ///
    /// `ChunkCatalog::scan` is blocking IO (a directory walk plus one JSON
    /// parse per chunk), so it runs on the blocking pool rather than stalling
    /// a runtime worker.
    async fn republish_catalog(&self, nvme_path: &Path) {
        let path = nvme_path.to_path_buf();
        match tokio::task::spawn_blocking(move || ChunkCatalog::scan(&path)).await {
            Ok(fresh) => {
                let chunks: usize = fresh
                    .summary()
                    .per_stream
                    .iter()
                    .map(|(_, count, _, _)| *count)
                    .sum();
                self.catalog.store(fresh);
                info!(chunks, "published refreshed chunk catalog");
            }
            Err(e) => {
                warn!(error = %e, "catalog rescan task panicked; serving previous catalog");
            }
        }
    }

    pub(crate) async fn run(self, shutdown: ShutdownSignal) -> Result<()> {
        let nvme_path = Path::new(&self.storage_cfg.nvme_path);
        let scan_interval = std::time::Duration::from_secs(self.cfg.scan_interval_secs);

        match sweep_partials(nvme_path) {
            Ok(count) => {
                if count > 0 {
                    info!(count, "startup partial sweep removed files");
                }
            }
            Err(e) => {
                warn!(error = %e, "startup partial sweep failed");
            }
        }

        let mut last_heartbeat = std::time::Instant::now();

        loop {
            if shutdown.is_cancelled() {
                info!("syncer shutting down");
                break;
            }

            let r2 = match &self.r2 {
                Some(r2) => r2.clone(),
                None => {
                    error!("R2 credentials missing, skipping scan");
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = tokio::time::sleep(scan_interval) => continue,
                    }
                }
            };

            let remote = match r2.list_chunks().await {
                Ok(chunks) => chunks,
                Err(e) => {
                    warn!(error = %e, "list_chunks failed, retrying next cycle");
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = tokio::time::sleep(scan_interval) => continue,
                    }
                }
            };

            let local = local_chunks(nvme_path);
            let to_download = diff_for_sync(&local, &remote, self.cfg.local_retention_hours);

            let remote_count = remote.len();
            let local_count = local.len();
            let semaphore = Arc::new(tokio::sync::Semaphore::new(
                self.cfg.max_concurrent_downloads,
            ));

            let mut handles = Vec::new();
            for chunk in &to_download {
                let permit = semaphore.clone().acquire_owned().await?;
                let r2 = r2.clone();
                let cfg = self.cfg.clone();
                let dest_root = nvme_path.to_path_buf();
                let chunk = chunk.clone();

                handles.push(tokio::spawn(async move {
                    let _permit = permit;
                    download_chunk(&r2, &chunk, &dest_root, &cfg).await
                }));
            }

            let results = futures::future::join_all(handles).await;
            let mut downloaded_this_scan: u32 = 0;
            for res in &results {
                match res {
                    Ok(Ok(())) => downloaded_this_scan += 1,
                    Ok(Err(e)) => {
                        warn!(error = %e, "chunk download failed");
                    }
                    Err(e) => {
                        warn!(error = %e, "chunk download task panicked");
                    }
                }
            }

            // Sweep after downloading, then publish: the catalog we hand to
            // the server then lists exactly what is on disk, never a chunk the
            // sweep is about to delete.
            let swept = match sweep_retention(nvme_path, self.cfg.local_retention_hours) {
                Ok(n) => {
                    if n > 0 {
                        info!(swept = n, "retention sweep evicted chunks");
                    }
                    n
                }
                Err(e) => {
                    warn!(error = %e, "retention sweep failed");
                    0
                }
            };

            if downloaded_this_scan > 0 || swept > 0 {
                self.republish_catalog(nvme_path).await;
            }

            if last_heartbeat.elapsed() >= std::time::Duration::from_secs(60) {
                info!(
                    remote_count,
                    local_count, downloaded_this_scan, swept, "syncer heartbeat"
                );
                last_heartbeat = std::time::Instant::now();
            }

            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("syncer shutting down");
                    break;
                }
                _ = tokio::time::sleep(scan_interval) => {}
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_chunk(stream: Stream, start: u64, end: u64) -> R2Chunk {
        R2Chunk {
            stream,
            start_slot: start,
            end_slot: end,
            key_prefix: format!("chunks/{}/{:012}-{:012}", stream.as_str(), start, end),
        }
    }

    #[test]
    fn local_chunks_returns_empty_when_no_dir() {
        let tmp = tempfile::tempdir().expect("create temp dir");
        let result = local_chunks(tmp.path());
        assert!(result.is_empty());
    }

    #[test]
    fn local_chunks_skips_files_without_meta_json() {
        let tmp = tempfile::tempdir().expect("create temp dir");
        let tx_dir = tmp.path().join("chunks").join("tx");
        std::fs::create_dir_all(&tx_dir).expect("create tx dir");

        std::fs::write(tx_dir.join("000000000100-000000000200.zst"), b"").expect("write zst");

        let result = local_chunks(tmp.path());
        assert!(result.is_empty());
    }

    #[test]
    fn local_chunks_finds_meta_json_per_stream() {
        let tmp = tempfile::tempdir().expect("create temp dir");

        for stream in Stream::all() {
            let dir = tmp.path().join("chunks").join(stream.as_str());
            std::fs::create_dir_all(&dir).expect("create stream dir");
            std::fs::write(dir.join("000000000100-000000000200.meta.json"), b"{}")
                .expect("write meta.json");
        }

        let result = local_chunks(tmp.path());
        assert_eq!(result.len(), 3);

        for stream in Stream::all() {
            assert!(
                result.contains(&ChunkKey {
                    stream,
                    start_slot: 100,
                    end_slot: 200,
                }),
                "missing chunk for stream {:?}",
                stream,
            );
        }
    }

    #[test]
    fn diff_returns_remote_minus_local() {
        let local: HashSet<ChunkKey> = [ChunkKey {
            stream: Stream::Tx,
            start_slot: 100,
            end_slot: 200,
        }]
        .into_iter()
        .collect();

        let remote = vec![
            make_chunk(Stream::Tx, 100, 200),
            make_chunk(Stream::Tx, 300, 400),
            make_chunk(Stream::Acct, 500, 600),
        ];

        let result = diff_for_sync(&local, &remote, 24);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].stream, Stream::Acct);
        assert_eq!(result[0].start_slot, 500);
        assert_eq!(result[1].stream, Stream::Tx);
        assert_eq!(result[1].start_slot, 300);
    }

    #[test]
    fn diff_filters_to_retention_window() {
        let local = HashSet::new();
        let remote = vec![
            make_chunk(Stream::Tx, 100, 10000),
            make_chunk(Stream::Tx, 11000, 12000),
            make_chunk(Stream::Block, 15000, 20000),
        ];

        let result = diff_for_sync(&local, &remote, 1);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].stream, Stream::Block);
        assert_eq!(result[0].start_slot, 15000);
        assert_eq!(result[1].stream, Stream::Tx);
        assert_eq!(result[1].start_slot, 11000);
    }

    #[test]
    fn diff_handles_empty_remote() {
        let local = HashSet::new();
        let remote: Vec<R2Chunk> = vec![];

        let result = diff_for_sync(&local, &remote, 24);
        assert!(result.is_empty());
    }

    #[test]
    fn diff_handles_empty_local() {
        let local = HashSet::new();
        let remote = vec![
            make_chunk(Stream::Tx, 100, 200),
            make_chunk(Stream::Acct, 300, 400),
        ];

        let result = diff_for_sync(&local, &remote, 24);

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn diff_sorts_by_stream_then_start_slot_ascending() {
        let local = HashSet::new();
        let remote = vec![
            make_chunk(Stream::Block, 500, 600),
            make_chunk(Stream::Tx, 100, 200),
            make_chunk(Stream::Acct, 300, 400),
            make_chunk(Stream::Tx, 50, 100),
        ];

        let result = diff_for_sync(&local, &remote, 24);

        // Sorted by (stream.as_str(), start_slot): acct < block < tx; within tx: 50 < 100
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].stream, Stream::Acct);
        assert_eq!(result[0].start_slot, 300);
        assert_eq!(result[1].stream, Stream::Block);
        assert_eq!(result[1].start_slot, 500);
        assert_eq!(result[2].stream, Stream::Tx);
        assert_eq!(result[2].start_slot, 50);
        assert_eq!(result[3].stream, Stream::Tx);
        assert_eq!(result[3].start_slot, 100);
    }

    mod download {
        use super::*;
        use aws_credential_types::provider::SharedCredentialsProvider;
        use aws_credential_types::Credentials;
        use aws_sdk_s3::config::Region;
        use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
        use aws_sdk_s3::{config::Config, Client};
        use aws_smithy_mocks::{create_mock_http_client, mock, MockResponseInterceptor, RuleMode};
        use aws_smithy_types::error::ErrorMetadata;
        use aws_smithy_types::retry::RetryConfig;
        use std::sync::{Arc, Mutex};

        fn build_mock_client(interceptor: MockResponseInterceptor) -> Client {
            let mock_http = create_mock_http_client();
            let creds = Credentials::new("test", "test", None, None, "test");
            let config = Config::builder()
                .region(Region::new("auto"))
                .endpoint_url("https://test.r2.cloudflarestorage.com")
                .credentials_provider(SharedCredentialsProvider::new(creds))
                .force_path_style(true)
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .retry_config(RetryConfig::disabled())
                .http_client(mock_http)
                .interceptor(interceptor)
                .build();
            Client::from_conf(config)
        }

        fn mock_r2(rules: &[&aws_smithy_mocks::Rule], rule_mode: RuleMode) -> R2Client {
            let mut interceptor = MockResponseInterceptor::new().rule_mode(rule_mode);
            for rule in rules {
                interceptor = interceptor.with_rule(rule);
            }
            let client = build_mock_client(interceptor);
            R2Client::from_client(client, "test-bucket".to_string())
        }

        fn test_reader_cfg() -> ReaderConfig {
            ReaderConfig {
                scan_interval_secs: 30,
                max_concurrent_downloads: 4,
                local_retention_hours: 24,
                retry_attempts: 3,
                retry_initial_delay_ms: 1,
                decoded_cache_bytes: 805_306_368,
                index_cache_bytes: 134_217_728,
                auth_tokens: Vec::new(),
                subscription_channel_capacity: 1024,
                follow_idle_timeout_secs: 900,
                max_connections_total: 256,
                max_connections_per_token: 16,
                pacing: PacingConfig::default(),
                metrics: MetricsConfig::default(),
            }
        }

        fn get_ok_rule() -> aws_smithy_mocks::Rule {
            mock!(aws_sdk_s3::Client::get_object).then_output(|| {
                GetObjectOutput::builder()
                    .body(b"test-data".to_vec().into())
                    .build()
            })
        }

        fn get_err_rule() -> aws_smithy_mocks::Rule {
            mock!(aws_sdk_s3::Client::get_object).then_error(|| {
                GetObjectError::generic(ErrorMetadata::builder().code("InternalError").build())
            })
        }

        #[tokio::test]
        async fn download_trio_writes_zst_idx_meta_in_order() {
            let recorded_keys: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            let rk = recorded_keys.clone();

            let rule = mock!(aws_sdk_s3::Client::get_object).then_compute_output(move |req| {
                if let Some(key) = req.key() {
                    rk.lock().unwrap().push(key.to_string());
                }
                GetObjectOutput::builder()
                    .body(b"data".to_vec().into())
                    .build()
            });

            let r2 = mock_r2(&[&rule], RuleMode::MatchAny);
            let dir = tempfile::tempdir().expect("create temp dir");
            let chunk = make_chunk(Stream::Tx, 100, 200);
            let cfg = test_reader_cfg();

            download_chunk(&r2, &chunk, dir.path(), &cfg).await.unwrap();

            let keys = recorded_keys.lock().unwrap();
            assert_eq!(keys.len(), 3);
            assert!(
                keys[0].ends_with(".zst"),
                "first key should be .zst, got {}",
                keys[0]
            );
            assert!(
                keys[1].ends_with(".idx"),
                "second key should be .idx, got {}",
                keys[1]
            );
            assert!(
                keys[2].ends_with(".meta.json"),
                "third key should be .meta.json, got {}",
                keys[2]
            );
        }

        #[tokio::test]
        async fn download_trio_meta_is_last_to_appear_locally() {
            let rule = get_ok_rule();
            let r2 = mock_r2(&[&rule], RuleMode::MatchAny);

            let dir = tempfile::tempdir().expect("create temp dir");
            let chunk = make_chunk(Stream::Tx, 100, 200);
            let cfg = test_reader_cfg();

            download_chunk(&r2, &chunk, dir.path(), &cfg).await.unwrap();

            let chunk_dir = dir.path().join("chunks").join("tx");
            let stem = format!("{:012}-{:012}", 100u64, 200u64);

            // All three files should exist at final paths
            assert!(chunk_dir.join(format!("{stem}.zst")).exists());
            assert!(chunk_dir.join(format!("{stem}.idx")).exists());
            assert!(chunk_dir.join(format!("{stem}.meta.json")).exists());

            // No .partial files should remain
            assert!(!chunk_dir.join(format!("{stem}.zst.partial")).exists());
            assert!(!chunk_dir.join(format!("{stem}.idx.partial")).exists());
            assert!(!chunk_dir.join(format!("{stem}.meta.json.partial")).exists());
        }

        #[tokio::test]
        async fn download_trio_partial_failure_no_meta() {
            // .zst succeeds, .idx fails all retries, .meta.json never attempted
            let rule = mock!(aws_sdk_s3::Client::get_object)
                .sequence()
                .output(|| {
                    GetObjectOutput::builder()
                        .body(b"zst-data".to_vec().into())
                        .build()
                })
                .error(|| {
                    GetObjectError::generic(ErrorMetadata::builder().code("InternalError").build())
                })
                .times(3) // 3 retry attempts for .idx
                .build();

            let r2 = mock_r2(&[&rule], RuleMode::Sequential);

            let dir = tempfile::tempdir().expect("create temp dir");
            let chunk = make_chunk(Stream::Tx, 100, 200);
            let cfg = test_reader_cfg();

            let result = download_chunk(&r2, &chunk, dir.path(), &cfg).await;
            assert!(
                result.is_err(),
                "download_chunk should fail when .idx fails all retries"
            );

            let chunk_dir = dir.path().join("chunks").join("tx");
            let stem = format!("{:012}-{:012}", 100u64, 200u64);

            // .zst should exist (it was downloaded and renamed)
            assert!(chunk_dir.join(format!("{stem}.zst")).exists());

            // .meta.json should NOT exist (never reached)
            assert!(!chunk_dir.join(format!("{stem}.meta.json")).exists());
        }

        #[tokio::test]
        async fn get_with_retry_succeeds_on_second_attempt() {
            let rule = mock!(aws_sdk_s3::Client::get_object)
                .sequence()
                .error(|| {
                    GetObjectError::generic(ErrorMetadata::builder().code("InternalError").build())
                })
                .output(|| {
                    GetObjectOutput::builder()
                        .body(b"data".to_vec().into())
                        .build()
                })
                .build();

            let r2 = mock_r2(&[&rule], RuleMode::Sequential);
            let dir = tempfile::tempdir().expect("create temp dir");
            let dest = dir.path().join("test-file.zst");
            let cfg = test_reader_cfg();

            let result = get_with_retry(&r2, "chunks/tx/test.zst", &dest, &cfg).await;
            assert!(result.is_ok());
            assert_eq!(rule.num_calls(), 2);
        }

        #[tokio::test]
        async fn get_with_retry_exhausts_and_errors() {
            let rule = get_err_rule();
            let r2 = mock_r2(&[&rule], RuleMode::MatchAny);

            let dir = tempfile::tempdir().expect("create temp dir");
            let dest = dir.path().join("test-file.zst");
            let cfg = test_reader_cfg();

            let result = get_with_retry(&r2, "chunks/tx/test.zst", &dest, &cfg).await;
            assert!(result.is_err());
            assert_eq!(rule.num_calls(), 3);
        }
    }

    mod sweep {
        use super::*;

        #[test]
        fn sweep_partials_removes_all_extensions() {
            let tmp = tempfile::tempdir().expect("create temp dir");
            for stream in Stream::all() {
                let dir = tmp.path().join("chunks").join(stream.as_str());
                std::fs::create_dir_all(&dir).expect("create stream dir");
                std::fs::write(dir.join("000000000100-000000000200.zst.partial"), b"")
                    .expect("write zst.partial");
                std::fs::write(dir.join("000000000100-000000000200.idx.partial"), b"")
                    .expect("write idx.partial");
                std::fs::write(dir.join("000000000100-000000000200.meta.json.partial"), b"")
                    .expect("write meta.json.partial");
            }
            let count = sweep_partials(tmp.path()).expect("sweep_partials should succeed");
            assert_eq!(count, 9);

            for stream in Stream::all() {
                let dir = tmp.path().join("chunks").join(stream.as_str());
                assert!(!dir.join("000000000100-000000000200.zst.partial").exists());
                assert!(!dir.join("000000000100-000000000200.idx.partial").exists());
                assert!(!dir
                    .join("000000000100-000000000200.meta.json.partial")
                    .exists());
            }
        }

        #[test]
        fn sweep_partials_returns_zero_when_empty() {
            let tmp = tempfile::tempdir().expect("create temp dir");
            let count = sweep_partials(tmp.path()).expect("sweep_partials should succeed");
            assert_eq!(count, 0);
        }

        #[test]
        fn sweep_retention_evicts_below_slot_floor() {
            // retention_slots(24h) = 24 * 3600 * 5 / 2 = 216_000.
            // newest_end_slot = 900_000 → floor = 684_000. The old chunk
            // (end 200) is far below the floor and must be evicted; the
            // newest one is kept. recv_ns_last content is irrelevant now.
            let tmp = tempfile::tempdir().expect("create temp dir");
            let dir = tmp.path().join("chunks").join(Stream::Tx.as_str());
            std::fs::create_dir_all(&dir).expect("create stream dir");

            let write_chunk = |stem: &str| {
                std::fs::write(dir.join(format!("{stem}.meta.json")), b"{}")
                    .expect("write meta.json");
                std::fs::write(dir.join(format!("{stem}.zst")), b"zst").expect("write zst");
                std::fs::write(dir.join(format!("{stem}.idx")), b"idx").expect("write idx");
            };
            let newest = "000000800001-000000900000";
            let old = "000000000100-000000000200";
            write_chunk(newest);
            write_chunk(old);

            let count = sweep_retention(tmp.path(), 24).expect("sweep_retention should succeed");
            assert_eq!(count, 1);
            assert!(!dir.join(format!("{old}.meta.json")).exists());
            assert!(!dir.join(format!("{old}.zst")).exists());
            assert!(!dir.join(format!("{old}.idx")).exists());
            assert!(dir.join(format!("{newest}.meta.json")).exists());
            assert!(dir.join(format!("{newest}.zst")).exists());
            assert!(dir.join(format!("{newest}.idx")).exists());
        }

        #[test]
        fn sweep_retention_keeps_chunks_within_slot_window() {
            // Two chunks both within retention_slots of the newest → keep both.
            let tmp = tempfile::tempdir().expect("create temp dir");
            let dir = tmp.path().join("chunks").join(Stream::Tx.as_str());
            std::fs::create_dir_all(&dir).expect("create stream dir");

            for stem in ["000000800001-000000900000", "000000700001-000000800000"] {
                std::fs::write(dir.join(format!("{stem}.meta.json")), b"{}")
                    .expect("write meta.json");
                std::fs::write(dir.join(format!("{stem}.zst")), b"zst").expect("write zst");
                std::fs::write(dir.join(format!("{stem}.idx")), b"idx").expect("write idx");
            }

            let count = sweep_retention(tmp.path(), 24).expect("sweep_retention should succeed");
            assert_eq!(count, 0);
            assert_eq!(local_chunks(tmp.path()).len(), 2);
        }

        #[test]
        fn sweep_retention_keeps_sole_newest_chunk() {
            // A lone chunk is always its own newest, so the floor sits below it
            // and it is never evicted — a stalled upstream freezes, not drains.
            let tmp = tempfile::tempdir().expect("create temp dir");
            let dir = tmp.path().join("chunks").join(Stream::Tx.as_str());
            std::fs::create_dir_all(&dir).expect("create stream dir");

            let stem = "000000000100-000000000200";
            std::fs::write(dir.join(format!("{stem}.meta.json")), b"{}").expect("write meta.json");
            std::fs::write(dir.join(format!("{stem}.zst")), b"zst").expect("write zst");
            std::fs::write(dir.join(format!("{stem}.idx")), b"idx").expect("write idx");

            let count = sweep_retention(tmp.path(), 24).expect("sweep_retention should succeed");
            assert_eq!(count, 0);
            assert!(dir.join(format!("{stem}.meta.json")).exists());
        }
    }

    mod e2e {
        use super::*;
        use aws_credential_types::provider::SharedCredentialsProvider;
        use aws_credential_types::Credentials;
        use aws_sdk_s3::config::Region;
        use aws_sdk_s3::operation::get_object::GetObjectOutput;
        use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
        use aws_sdk_s3::{config::Config, Client};
        use aws_smithy_mocks::{create_mock_http_client, mock, MockResponseInterceptor, RuleMode};
        use aws_smithy_types::retry::RetryConfig;
        use sillage_common::shutdown::ShutdownSignal;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        fn build_mock_client(interceptor: MockResponseInterceptor) -> Client {
            let mock_http = create_mock_http_client();
            let creds = Credentials::new("test", "test", None, None, "test");
            let config = Config::builder()
                .region(Region::new("auto"))
                .endpoint_url("https://test.r2.cloudflarestorage.com")
                .credentials_provider(SharedCredentialsProvider::new(creds))
                .force_path_style(true)
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .retry_config(RetryConfig::disabled())
                .http_client(mock_http)
                .interceptor(interceptor)
                .build();
            Client::from_conf(config)
        }

        fn mock_r2(rules: &[&aws_smithy_mocks::Rule], rule_mode: RuleMode) -> R2Client {
            let mut interceptor = MockResponseInterceptor::new().rule_mode(rule_mode);
            for rule in rules {
                interceptor = interceptor.with_rule(rule);
            }
            let client = build_mock_client(interceptor);
            R2Client::from_client(client, "test-bucket".to_string())
        }

        fn test_reader_cfg() -> ReaderConfig {
            ReaderConfig {
                scan_interval_secs: 1,
                max_concurrent_downloads: 2,
                local_retention_hours: 24,
                retry_attempts: 3,
                retry_initial_delay_ms: 1,
                decoded_cache_bytes: 805_306_368,
                index_cache_bytes: 134_217_728,
                auth_tokens: Vec::new(),
                subscription_channel_capacity: 1024,
                follow_idle_timeout_secs: 900,
                max_connections_total: 256,
                max_connections_per_token: 16,
                pacing: PacingConfig::default(),
                metrics: MetricsConfig::default(),
            }
        }

        fn test_storage_cfg(dir: &tempfile::TempDir) -> StorageConfig {
            StorageConfig {
                nvme_path: dir.path().to_str().unwrap().to_string(),
            }
        }

        #[tokio::test]
        async fn sync_downloads_in_window_chunks_sweeps_partials_and_skips_old() {
            let dir = tempfile::tempdir().expect("create temp dir");

            let tx_dir = dir.path().join("chunks").join("tx");
            std::fs::create_dir_all(&tx_dir).expect("create tx dir");
            std::fs::write(
                tx_dir.join("000000000050-000000000100.zst.partial"),
                b"stale",
            )
            .expect("write stale partial");

            // retention_slots = 24 * 3600 * 5 / 2 = 216000
            // newest_end_slot = 900000 → cutoff = 684000
            let in_window: Vec<(Stream, u64, u64)> = vec![
                (Stream::Tx, 600000, 700000),
                (Stream::Tx, 700001, 800000),
                (Stream::Tx, 800001, 900000),
            ];
            let out_of_window: (Stream, u64, u64) = (Stream::Tx, 100, 200);

            let in_window_clone = in_window.clone();
            let out_of_window_clone = out_of_window;
            let list_rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
                let mut objects = Vec::new();
                for (stream, start, end) in in_window_clone
                    .iter()
                    .chain(std::iter::once(&out_of_window_clone))
                {
                    for ext in ["zst", "idx", "meta.json"] {
                        objects.push(
                            aws_sdk_s3::types::Object::builder()
                                .key(format!(
                                    "chunks/{}/{:012}-{:012}.{}",
                                    stream.as_str(),
                                    start,
                                    end,
                                    ext
                                ))
                                .build(),
                        );
                    }
                }
                ListObjectsV2Output::builder()
                    .set_contents(Some(objects))
                    .is_truncated(false)
                    .build()
            });

            let recorded_keys: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            let rk = recorded_keys.clone();

            let get_rule = mock!(aws_sdk_s3::Client::get_object).then_compute_output(move |req| {
                let key = req.key().map(|k| k.to_string()).unwrap_or_default();
                rk.lock().unwrap().push(key.clone());

                let body = if key.ends_with(".meta.json") {
                    let now_ns = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64;
                    format!(r#"{{"recv_ns_last":{}}}"#, now_ns).into_bytes()
                } else {
                    b"data".to_vec()
                };
                GetObjectOutput::builder().body(body.into()).build()
            });

            let r2 = mock_r2(&[&list_rule, &get_rule], RuleMode::MatchAny);
            let storage_cfg = test_storage_cfg(&dir);
            let syncer = Syncer::new(
                Some(r2),
                test_reader_cfg(),
                storage_cfg,
                SharedCatalog::new(ChunkCatalog::scan(dir.path())),
            );

            let shutdown = ShutdownSignal::new();
            let shutdown_clone = shutdown.clone();
            let handle = tokio::spawn(async move { syncer.run(shutdown).await });

            tokio::time::sleep(Duration::from_secs(3)).await;
            shutdown_clone.cancel();

            let result = handle.await.expect("task join");
            assert!(result.is_ok(), "syncer should exit cleanly: {:?}", result);

            for (stream, start, end) in &in_window {
                let chunk_dir = dir.path().join("chunks").join(stream.as_str());
                let stem = format!("{:012}-{:012}", start, end);
                assert!(
                    chunk_dir.join(format!("{stem}.zst")).exists(),
                    "missing {}.zst for chunk ({}, {}, {})",
                    stem,
                    stream.as_str(),
                    start,
                    end
                );
                assert!(
                    chunk_dir.join(format!("{stem}.idx")).exists(),
                    "missing {}.idx for chunk ({}, {}, {})",
                    stem,
                    stream.as_str(),
                    start,
                    end
                );
                assert!(
                    chunk_dir.join(format!("{stem}.meta.json")).exists(),
                    "missing {}.meta.json for chunk ({}, {}, {})",
                    stem,
                    stream.as_str(),
                    start,
                    end
                );
            }

            let out_stem = format!("{:012}-{:012}", out_of_window.1, out_of_window.2);
            let out_dir = dir.path().join("chunks").join(out_of_window.0.as_str());
            assert!(
                !out_dir.join(format!("{out_stem}.meta.json")).exists(),
                "out-of-window chunk should not be downloaded"
            );

            for (stream, start, end) in &in_window {
                let chunk_dir = dir.path().join("chunks").join(stream.as_str());
                let stem = format!("{:012}-{:012}", start, end);
                let meta_mtime = path_mtime_ns(&chunk_dir.join(format!("{stem}.meta.json")))
                    .expect("meta.json mtime");
                let zst_mtime =
                    path_mtime_ns(&chunk_dir.join(format!("{stem}.zst"))).expect("zst mtime");
                let idx_mtime =
                    path_mtime_ns(&chunk_dir.join(format!("{stem}.idx"))).expect("idx mtime");
                assert!(
                    meta_mtime >= zst_mtime,
                    ".meta.json should be written after .zst for chunk ({}, {}, {})",
                    stream.as_str(),
                    start,
                    end
                );
                assert!(
                    meta_mtime >= idx_mtime,
                    ".meta.json should be written after .idx for chunk ({}, {}, {})",
                    stream.as_str(),
                    start,
                    end
                );
            }

            for stream in Stream::all() {
                let stream_dir = dir.path().join("chunks").join(stream.as_str());
                if !stream_dir.is_dir() {
                    continue;
                }
                for entry in std::fs::read_dir(&stream_dir).expect("read stream dir") {
                    let entry = entry.expect("dir entry");
                    let name = entry.file_name();
                    let name_str = name.to_string_lossy();
                    assert!(
                        !name_str.ends_with(".partial"),
                        "partial file should have been swept: {}",
                        name_str
                    );
                }
            }

            let keys = recorded_keys.lock().unwrap();
            for (stream, start, end) in &in_window {
                let prefix = format!("chunks/{}/{:012}-{:012}", stream.as_str(), start, end);
                let chunk_keys: Vec<&String> =
                    keys.iter().filter(|k| k.starts_with(&prefix)).collect();
                let zst_pos = chunk_keys.iter().position(|k| k.ends_with(".zst"));
                let idx_pos = chunk_keys.iter().position(|k| k.ends_with(".idx"));
                let meta_pos = chunk_keys.iter().position(|k| k.ends_with(".meta.json"));

                if let (Some(zi), Some(ii), Some(mi)) = (zst_pos, idx_pos, meta_pos) {
                    assert!(
                        mi > zi,
                        ".meta.json GET should come after .zst for chunk ({}, {}, {})",
                        stream.as_str(),
                        start,
                        end
                    );
                    assert!(
                        mi > ii,
                        ".meta.json GET should come after .idx for chunk ({}, {}, {})",
                        stream.as_str(),
                        start,
                        end
                    );
                }
            }
        }
    }

    mod loop_ {
        use super::*;
        use aws_credential_types::provider::SharedCredentialsProvider;
        use aws_credential_types::Credentials;
        use aws_sdk_s3::config::Region;
        use aws_sdk_s3::operation::get_object::GetObjectOutput;
        use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
        use aws_sdk_s3::{config::Config, Client};
        use aws_smithy_mocks::{create_mock_http_client, mock, MockResponseInterceptor, RuleMode};
        use aws_smithy_types::retry::RetryConfig;
        use sillage_common::shutdown::ShutdownSignal;
        use std::time::Duration;

        fn build_mock_client(interceptor: MockResponseInterceptor) -> Client {
            let mock_http = create_mock_http_client();
            let creds = Credentials::new("test", "test", None, None, "test");
            let config = Config::builder()
                .region(Region::new("auto"))
                .endpoint_url("https://test.r2.cloudflarestorage.com")
                .credentials_provider(SharedCredentialsProvider::new(creds))
                .force_path_style(true)
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .retry_config(RetryConfig::disabled())
                .http_client(mock_http)
                .interceptor(interceptor)
                .build();
            Client::from_conf(config)
        }

        fn mock_r2(rules: &[&aws_smithy_mocks::Rule], rule_mode: RuleMode) -> R2Client {
            let mut interceptor = MockResponseInterceptor::new().rule_mode(rule_mode);
            for rule in rules {
                interceptor = interceptor.with_rule(rule);
            }
            let client = build_mock_client(interceptor);
            R2Client::from_client(client, "test-bucket".to_string())
        }

        fn test_reader_cfg() -> ReaderConfig {
            ReaderConfig {
                scan_interval_secs: 1,
                max_concurrent_downloads: 4,
                local_retention_hours: 24,
                retry_attempts: 3,
                retry_initial_delay_ms: 1,
                decoded_cache_bytes: 805_306_368,
                index_cache_bytes: 134_217_728,
                auth_tokens: Vec::new(),
                subscription_channel_capacity: 1024,
                follow_idle_timeout_secs: 900,
                max_connections_total: 256,
                max_connections_per_token: 16,
                pacing: PacingConfig::default(),
                metrics: MetricsConfig::default(),
            }
        }

        fn test_storage_cfg(dir: &tempfile::TempDir) -> StorageConfig {
            StorageConfig {
                nvme_path: dir.path().to_str().unwrap().to_string(),
            }
        }

        fn make_r2_chunk(stream: Stream, start: u64, end: u64) -> R2Chunk {
            R2Chunk {
                stream,
                start_slot: start,
                end_slot: end,
                key_prefix: format!("chunks/{}/{:012}-{:012}", stream.as_str(), start, end),
            }
        }

        #[tokio::test]
        async fn syncer_skips_iteration_when_r2_none() {
            let dir = tempfile::tempdir().expect("create temp dir");
            let storage_cfg = test_storage_cfg(&dir);

            let syncer = Syncer::new(
                None,
                test_reader_cfg(),
                storage_cfg,
                SharedCatalog::new(ChunkCatalog::scan(dir.path())),
            );

            let shutdown = ShutdownSignal::new();
            let shutdown_clone = shutdown.clone();
            let handle = tokio::spawn(async move {
                tokio::select! {
                    result = syncer.run(shutdown) => result,
                    _ = tokio::time::sleep(Duration::from_secs(2)) => Ok(()),
                }
            });

            tokio::time::sleep(Duration::from_millis(500)).await;
            shutdown_clone.cancel();

            let result = handle.await.expect("task join");
            assert!(result.is_ok());
        }

        #[tokio::test]
        async fn syncer_downloads_pending_chunks_with_mock_r2() {
            let dir = tempfile::tempdir().expect("create temp dir");

            let chunks = vec![
                make_r2_chunk(Stream::Tx, 100, 200),
                make_r2_chunk(Stream::Tx, 300, 400),
                make_r2_chunk(Stream::Acct, 500, 600),
            ];

            let list_rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
                let mut objects = Vec::new();
                for chunk in &[
                    ("tx", 100u64, 200u64),
                    ("tx", 300u64, 400u64),
                    ("acct", 500u64, 600u64),
                ] {
                    for ext in ["zst", "idx", "meta.json"] {
                        objects.push(
                            aws_sdk_s3::types::Object::builder()
                                .key(format!(
                                    "chunks/{}/{:012}-{:012}.{}",
                                    chunk.0, chunk.1, chunk.2, ext
                                ))
                                .build(),
                        );
                    }
                }
                ListObjectsV2Output::builder()
                    .set_contents(Some(objects))
                    .is_truncated(false)
                    .build()
            });

            let get_rule = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
                GetObjectOutput::builder()
                    .body(b"data".to_vec().into())
                    .build()
            });

            let r2 = mock_r2(&[&list_rule, &get_rule], RuleMode::MatchAny);

            let storage_cfg = test_storage_cfg(&dir);
            let syncer = Syncer::new(
                Some(r2),
                test_reader_cfg(),
                storage_cfg,
                SharedCatalog::new(ChunkCatalog::scan(dir.path())),
            );

            let shutdown = ShutdownSignal::new();
            let shutdown_clone = shutdown.clone();

            let handle = tokio::spawn(async move { syncer.run(shutdown).await });

            tokio::time::sleep(Duration::from_secs(3)).await;
            shutdown_clone.cancel();

            let result = handle.await.expect("task join");
            assert!(result.is_ok());

            for chunk in &chunks {
                let chunk_dir = dir.path().join("chunks").join(chunk.stream.as_str());
                let stem = format!("{:012}-{:012}", chunk.start_slot, chunk.end_slot);
                assert!(
                    chunk_dir.join(format!("{stem}.zst")).exists(),
                    "missing {}.zst for chunk {:?}",
                    stem,
                    chunk
                );
                assert!(
                    chunk_dir.join(format!("{stem}.idx")).exists(),
                    "missing {}.idx for chunk {:?}",
                    stem,
                    chunk
                );
                assert!(
                    chunk_dir.join(format!("{stem}.meta.json")).exists(),
                    "missing {}.meta.json for chunk {:?}",
                    stem,
                    chunk
                );
            }
        }

        /// The syncer must make newly downloaded chunks visible to the serving
        /// catalog without a process restart. Before the catalog was made
        /// swappable, the server held a snapshot taken at boot and never saw
        /// anything the syncer fetched afterwards.
        #[tokio::test]
        async fn syncer_publishes_downloaded_chunks_to_catalog() {
            let dir = tempfile::tempdir().expect("create temp dir");

            let list_rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
                let mut objects = Vec::new();
                for ext in ["zst", "idx", "meta.json"] {
                    objects.push(
                        aws_sdk_s3::types::Object::builder()
                            .key(format!("chunks/tx/{:012}-{:012}.{}", 100, 200, ext))
                            .build(),
                    );
                }
                ListObjectsV2Output::builder()
                    .set_contents(Some(objects))
                    .is_truncated(false)
                    .build()
            });

            // Every file comes back as a valid ChunkMeta document. Only the
            // `.meta.json` is parsed by the catalog scan; `.zst` and `.idx`
            // just have to exist.
            let meta = sillage_common::chunk::ChunkMeta {
                schema_version: sillage_common::chunk::SCHEMA_VERSION,
                stream: "tx".to_string(),
                start_slot: 100,
                end_slot_exclusive: 200,
                first_message_slot: Some(100),
                last_message_slot: Some(199),
                message_count: 7,
                uncompressed_bytes: 128,
                compressed_bytes: 64,
                recv_ns_first: Some(1),
                recv_ns_last: Some(2),
                sealed_reason: "test".to_string(),
                index_dimensions: Vec::new(),
            };
            let meta_bytes = serde_json::to_vec(&meta).expect("serialize meta");
            let get_rule = mock!(aws_sdk_s3::Client::get_object).then_output(move || {
                GetObjectOutput::builder()
                    .body(meta_bytes.clone().into())
                    .build()
            });

            let r2 = mock_r2(&[&list_rule, &get_rule], RuleMode::MatchAny);
            let storage_cfg = test_storage_cfg(&dir);
            let catalog = SharedCatalog::new(ChunkCatalog::scan(dir.path()));

            // Nothing on disk yet, and a snapshot taken now must stay empty
            // even after the syncer publishes — snapshots are immutable.
            let before = catalog.snapshot();
            assert_eq!(before.newest_end_slot(), None, "catalog should start empty");

            let syncer = Syncer::new(Some(r2), test_reader_cfg(), storage_cfg, catalog.clone());

            let shutdown = ShutdownSignal::new();
            let shutdown_clone = shutdown.clone();
            let handle = tokio::spawn(async move { syncer.run(shutdown).await });

            tokio::time::sleep(Duration::from_secs(3)).await;
            shutdown_clone.cancel();
            handle.await.expect("task join").expect("syncer run");

            let after = catalog.snapshot();
            assert_eq!(
                after.newest_end_slot(),
                Some(200),
                "syncer should have published a catalog containing the downloaded chunk"
            );
            assert_eq!(
                before.newest_end_slot(),
                None,
                "the pre-existing snapshot must be unaffected by the swap"
            );
        }

        #[tokio::test]
        async fn syncer_exits_cleanly_on_shutdown_between_chunks() {
            let dir = tempfile::tempdir().expect("create temp dir");

            let list_rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
                ListObjectsV2Output::builder()
                    .set_contents(Some(vec![]))
                    .is_truncated(false)
                    .build()
            });

            let r2 = mock_r2(&[&list_rule], RuleMode::MatchAny);

            let storage_cfg = test_storage_cfg(&dir);
            let syncer = Syncer::new(
                Some(r2),
                test_reader_cfg(),
                storage_cfg,
                SharedCatalog::new(ChunkCatalog::scan(dir.path())),
            );

            let shutdown = ShutdownSignal::new();
            let shutdown_clone = shutdown.clone();

            let handle = tokio::spawn(async move { syncer.run(shutdown).await });

            tokio::time::sleep(Duration::from_millis(500)).await;
            shutdown_clone.cancel();

            let result = handle.await.expect("task join");
            assert!(result.is_ok());
        }

        #[tokio::test]
        async fn syncer_sweeps_partials_at_startup() {
            let dir = tempfile::tempdir().expect("create temp dir");

            for stream in Stream::all() {
                let stream_dir = dir.path().join("chunks").join(stream.as_str());
                std::fs::create_dir_all(&stream_dir).expect("create stream dir");
                std::fs::write(
                    stream_dir.join("000000000100-000000000200.zst.partial"),
                    b"",
                )
                .expect("write zst.partial");
                std::fs::write(
                    stream_dir.join("000000000100-000000000200.idx.partial"),
                    b"",
                )
                .expect("write idx.partial");
            }

            let list_rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
                ListObjectsV2Output::builder()
                    .set_contents(Some(vec![]))
                    .is_truncated(false)
                    .build()
            });

            let r2 = mock_r2(&[&list_rule], RuleMode::MatchAny);

            let storage_cfg = test_storage_cfg(&dir);
            let syncer = Syncer::new(
                Some(r2),
                test_reader_cfg(),
                storage_cfg,
                SharedCatalog::new(ChunkCatalog::scan(dir.path())),
            );

            let shutdown = ShutdownSignal::new();
            let shutdown_clone = shutdown.clone();

            let handle = tokio::spawn(async move { syncer.run(shutdown).await });

            tokio::time::sleep(Duration::from_secs(2)).await;
            shutdown_clone.cancel();

            let result = handle.await.expect("task join");
            assert!(result.is_ok());

            for stream in Stream::all() {
                let stream_dir = dir.path().join("chunks").join(stream.as_str());
                assert!(
                    !stream_dir
                        .join("000000000100-000000000200.zst.partial")
                        .exists(),
                    "partial .zst should be removed for stream {:?}",
                    stream
                );
                assert!(
                    !stream_dir
                        .join("000000000100-000000000200.idx.partial")
                        .exists(),
                    "partial .idx should be removed for stream {:?}",
                    stream
                );
            }
        }
    }
}
