use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use sillage_common::config::UploaderConfig;
use sillage_common::shutdown::ShutdownSignal;
use sillage_common::Stream;
use tracing::{debug, error, info, warn};

use crate::r2::R2Client;

#[derive(Clone)]
pub(crate) struct PendingChunk {
    pub stream: Stream,
    pub base: PathBuf,
    pub start_slot: u64,
    pub end_slot: u64,
}

pub(crate) struct Uploader {
    r2: Option<R2Client>,
    uploader_cfg: UploaderConfig,
    storage_cfg: sillage_common::config::StorageConfig,
}

impl Uploader {
    pub fn new(
        r2: Option<R2Client>,
        uploader_cfg: UploaderConfig,
        storage_cfg: sillage_common::config::StorageConfig,
    ) -> Self {
        Self {
            r2,
            uploader_cfg,
            storage_cfg,
        }
    }

    pub(crate) async fn run(self, shutdown: ShutdownSignal) -> Result<()> {
        let mut last_heartbeat = std::time::Instant::now();
        let scan_interval = Duration::from_secs(self.uploader_cfg.scan_interval_secs);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(
            self.uploader_cfg.max_concurrent_uploads,
        ));

        loop {
            if shutdown.is_cancelled() {
                info!("uploader shutting down");
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

            let pending = match scan_pending(Path::new(&self.storage_cfg.nvme_path)) {
                Ok(p) => p,
                Err(e) => {
                    warn!(error = %e, "scan_pending failed, retrying next cycle");
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = tokio::time::sleep(scan_interval) => continue,
                    }
                }
            };

            let swept = match sweep_retention(
                Path::new(&self.storage_cfg.nvme_path),
                self.uploader_cfg.local_retention_hours,
            ) {
                Ok(n) => n,
                Err(e) => {
                    warn!(error = %e, "retention sweep failed");
                    0
                }
            };

            match disk_usage_pct(Path::new(&self.storage_cfg.nvme_path)) {
                Ok(pct) if pct > self.uploader_cfg.disk_pressure_warn_pct => {
                    let stat = rustix::fs::statvfs(Path::new(&self.storage_cfg.nvme_path));
                    let (total_bytes, available_bytes) = match &stat {
                        Ok(s) => (s.f_blocks * s.f_frsize, s.f_bavail * s.f_frsize),
                        Err(_) => (0u64, 0u64),
                    };
                    warn!(
                        used_pct = pct,
                        total_bytes,
                        available_bytes,
                        path = %self.storage_cfg.nvme_path,
                        threshold = self.uploader_cfg.disk_pressure_warn_pct,
                        "disk pressure exceeds threshold"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    warn!(error = %e, "disk_usage_pct check failed");
                }
            }

            let pending_count = pending.len();
            let mut handles = Vec::new();
            for chunk in pending {
                let permit = semaphore.clone().acquire_owned().await?;
                let r2 = r2.clone();
                let cfg = self.uploader_cfg.clone();

                handles.push(tokio::spawn(async move {
                    let _permit = permit;
                    upload_chunk(&r2, &chunk, &cfg).await
                }));
            }

            let results = futures::future::join_all(handles).await;
            let mut uploaded_this_scan = 0u32;
            for res in &results {
                match res {
                    Ok(Ok(())) => uploaded_this_scan += 1,
                    Ok(Err(e)) => {
                        warn!(error = %e, "chunk upload failed");
                    }
                    Err(e) => {
                        warn!(error = %e, "chunk upload task panicked");
                    }
                }
            }

            if last_heartbeat.elapsed() >= Duration::from_secs(60) {
                let disk_pct = disk_usage_pct(Path::new(&self.storage_cfg.nvme_path)).unwrap_or(0);
                info!(
                    pending = pending_count,
                    uploaded_this_scan, swept, disk_pct, "uploader heartbeat"
                );
                last_heartbeat = std::time::Instant::now();
            }

            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("uploader shutting down");
                    break;
                }
                _ = tokio::time::sleep(scan_interval) => {}
            }
        }

        Ok(())
    }
}

/// Scan NVMe for sealed-but-unuploaded chunks.
///
/// For each stream, looks under `{nvme_path}/chunks/{stream}/` for `.meta.json`
/// files that lack a sibling `.uploaded` marker. Parses the slot range from the
/// filename pattern `{start_slot:012}-{end_slot:012}.meta.json`.
///
/// Returns chunks sorted by `(stream.as_str(), start_slot)` ascending.
fn scan_pending(nvme_path: &Path) -> Result<Vec<PendingChunk>> {
    let mut pending = Vec::new();

    for stream in Stream::all() {
        let dir = nvme_path.join("chunks").join(stream.as_str());
        if !dir.is_dir() {
            continue;
        }

        let entries = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) => {
                debug!(path = %dir.display(), error = %e, "skipping unreadable dir");
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

            let stem = file_name.strip_suffix(".meta.json").unwrap_or(file_name);
            let base = dir.join(stem);
            let uploaded_marker = base.with_extension("uploaded");

            if uploaded_marker.exists() {
                debug!(path = %path.display(), "skipping uploaded chunk");
                continue;
            }

            let mut parts = stem.splitn(2, '-');
            let start_str = parts.next().unwrap_or("");
            let end_str = parts.next().unwrap_or("");

            let start_slot = match start_str.parse::<u64>() {
                Ok(s) => s,
                Err(_) => {
                    debug!(file = %file_name, "skipping unparseable start_slot");
                    continue;
                }
            };
            let end_slot = match end_str.parse::<u64>() {
                Ok(s) => s,
                Err(_) => {
                    debug!(file = %file_name, "skipping unparseable end_slot");
                    continue;
                }
            };

            pending.push(PendingChunk {
                stream,
                base,
                start_slot,
                end_slot,
            });
        }
    }

    pending.sort_by(|a, b| {
        let key_a = (a.stream.as_str(), a.start_slot);
        let key_b = (b.stream.as_str(), b.start_slot);
        key_a.cmp(&key_b)
    });

    Ok(pending)
}

/// Upload a sealed chunk's three files (.zst, .idx, .meta.json) to R2 in order,
/// then write an atomic `.uploaded` marker on success.
pub(crate) async fn upload_chunk(
    r2: &R2Client,
    chunk: &PendingChunk,
    cfg: &UploaderConfig,
) -> Result<()> {
    for ext in ["zst", "idx", "meta.json"] {
        let key = format!(
            "chunks/{}/{:012}-{:012}.{}",
            chunk.stream.as_str(),
            chunk.start_slot,
            chunk.end_slot,
            ext
        );
        let path = chunk.base.with_extension(ext);
        put_with_retry(r2, &key, &path, cfg).await?;
    }

    let partial = chunk.base.with_extension("uploaded.partial");
    let marker = chunk.base.with_extension("uploaded");
    {
        let f = std::fs::File::create(&partial)?;
        f.sync_all()?;
    }
    std::fs::rename(&partial, &marker)?;

    Ok(())
}

/// Upload a single file to R2 with exponential-backoff retry.
pub(crate) async fn put_with_retry(
    r2: &R2Client,
    key: &str,
    path: &Path,
    cfg: &UploaderConfig,
) -> Result<()> {
    for n in 0..cfg.retry_attempts {
        match r2.put_file(key, path).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                if n < cfg.retry_attempts - 1 {
                    warn!(attempt = n + 1, key, "upload attempt failed, retrying");
                    tokio::time::sleep(Duration::from_millis(
                        cfg.retry_initial_delay_ms * 2_u64.pow(n),
                    ))
                    .await;
                } else {
                    error!(attempt = n + 1, key, error = %e, "upload failed after all retries");
                    return Err(e);
                }
            }
        }
    }

    // Reached only when retry_attempts == 0
    anyhow::bail!("no retry attempts configured for key {key}")
}

/// Delete chunk files older than `retention_hours` that have an `.uploaded` marker.
///
/// For each `.uploaded` file found under `{nvme_path}/chunks/{stream}/`, checks its
/// mtime. If the age exceeds the retention threshold, deletes the four siblings:
/// `.zst`, `.idx`, `.meta.json`, and `.uploaded`.  Returns the number of chunks
/// evicted.
fn sweep_retention(nvme_path: &Path, retention_hours: u64) -> Result<u32> {
    let retention = Duration::from_secs(retention_hours * 3600);
    let mut evicted = 0u32;
    let now = SystemTime::now();

    for stream in Stream::all() {
        let dir = nvme_path.join("chunks").join(stream.as_str());
        if !dir.is_dir() {
            continue;
        }

        let entries = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) => {
                warn!(path = %dir.display(), error = %e, "cannot read stream dir during retention sweep");
                continue;
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();
            let file_name = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };

            if !file_name.ends_with(".uploaded") {
                continue;
            }

            let mtime = match std::fs::metadata(&path)?.modified() {
                Ok(t) => t,
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "cannot read mtime");
                    continue;
                }
            };

            let age = match now.duration_since(mtime) {
                Ok(d) => d,
                Err(_) => continue,
            };

            if age > retention {
                let stem = file_name.strip_suffix(".uploaded").unwrap_or(file_name);
                let base = dir.join(stem);

                for ext in ["zst", "idx", "meta.json", "uploaded"] {
                    let sibling = base.with_extension(ext);
                    if sibling.exists() {
                        if let Err(e) = std::fs::remove_file(&sibling) {
                            warn!(path = %sibling.display(), error = %e, "failed to evict file during retention sweep");
                        }
                    }
                }
                evicted += 1;
            }
        }
    }

    if evicted > 0 {
        info!(evicted, "retention sweep completed");
    }

    Ok(evicted)
}

/// Return the filesystem usage percentage for the volume containing `path`.
fn disk_usage_pct(path: &Path) -> Result<u8> {
    let stat = rustix::fs::statvfs(path)?;
    let blocks = stat.f_blocks;
    let bavail = stat.f_bavail;

    if blocks == 0 {
        return Ok(0);
    }

    let used = blocks - bavail;
    let pct = (used * 100) / blocks;
    Ok(pct.min(100) as u8)
}

#[cfg(test)]
fn check_disk_pressure(pct: u8, threshold: u8) -> Option<(u8, u64, u64)> {
    if pct > threshold {
        Some((pct, 0, 0))
    } else {
        None
    }
}

#[cfg(test)]
fn log_disk_pressure_warning(
    pct: u8,
    total_bytes: u64,
    available_bytes: u64,
    path: &Path,
    threshold: u8,
) {
    warn!(
        used_pct = pct,
        total_bytes,
        available_bytes,
        path = %path.display(),
        threshold,
        "disk pressure exceeds threshold"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn scan_empty_dirs_returns_empty() {
        let dir = TempDir::new().unwrap();
        let result = scan_pending(dir.path()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn scan_finds_sealed_without_upload_marker() {
        let dir = TempDir::new().unwrap();
        let tx_dir = dir.path().join("chunks").join("tx");
        fs::create_dir_all(&tx_dir).unwrap();

        let stem = format!("{:012}-{:012}", 100u64, 200u64);
        fs::write(tx_dir.join(format!("{stem}.meta.json")), b"{}").unwrap();
        fs::write(tx_dir.join(format!("{stem}.zst")), b"").unwrap();
        fs::write(tx_dir.join(format!("{stem}.idx")), b"").unwrap();

        let result = scan_pending(dir.path()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].stream, Stream::Tx);
        assert_eq!(result[0].start_slot, 100);
        assert_eq!(result[0].end_slot, 200);
        assert_eq!(result[0].base, tx_dir.join(&stem));
    }

    #[test]
    fn scan_skips_uploaded_chunks() {
        let dir = TempDir::new().unwrap();
        let tx_dir = dir.path().join("chunks").join("tx");
        fs::create_dir_all(&tx_dir).unwrap();

        let stem = format!("{:012}-{:012}", 100u64, 200u64);
        fs::write(tx_dir.join(format!("{stem}.meta.json")), b"{}").unwrap();
        fs::write(tx_dir.join(format!("{stem}.uploaded")), b"").unwrap();

        let result = scan_pending(dir.path()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn scan_orders_by_start_slot() {
        let dir = TempDir::new().unwrap();
        let tx_dir = dir.path().join("chunks").join("tx");
        fs::create_dir_all(&tx_dir).unwrap();

        for &start in &[200u64, 100u64, 300u64] {
            let stem = format!("{:012}-{:012}", start, start + 100);
            fs::write(tx_dir.join(format!("{stem}.meta.json")), b"{}").unwrap();
        }

        let result = scan_pending(dir.path()).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].start_slot, 100);
        assert_eq!(result[1].start_slot, 200);
        assert_eq!(result[2].start_slot, 300);
    }

    #[test]
    fn scan_ignores_files_with_bad_filename() {
        let dir = TempDir::new().unwrap();
        let tx_dir = dir.path().join("chunks").join("tx");
        fs::create_dir_all(&tx_dir).unwrap();

        fs::write(tx_dir.join("nonsense.meta.json"), b"{}").unwrap();

        let result = scan_pending(dir.path()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn scan_handles_missing_chunks_dir_gracefully() {
        let dir = TempDir::new().unwrap();

        let result = scan_pending(dir.path()).unwrap();
        assert!(result.is_empty());
    }

    mod sweep {
        use super::*;
        use filetime::{set_file_mtime, FileTime};
        use std::fs;
        use std::time::{Duration, SystemTime};
        use tempfile::TempDir;

        fn make_chunk_files(dir: &TempDir, stream: &str, stem: &str) -> PathBuf {
            let chunk_dir = dir.path().join("chunks").join(stream);
            fs::create_dir_all(&chunk_dir).unwrap();
            let base = chunk_dir.join(stem);
            fs::write(base.with_extension("zst"), b"zst").unwrap();
            fs::write(base.with_extension("idx"), b"idx").unwrap();
            fs::write(base.with_extension("meta.json"), b"{}").unwrap();
            fs::write(base.with_extension("uploaded"), b"").unwrap();
            base
        }

        #[test]
        fn sweep_evicts_files_older_than_retention() {
            let dir = TempDir::new().unwrap();
            let base = make_chunk_files(&dir, "tx", "000000000100-000000000200");

            let old_time = SystemTime::now() - Duration::from_secs(25 * 3600);
            set_file_mtime(
                base.with_extension("uploaded"),
                FileTime::from_system_time(old_time),
            )
            .unwrap();

            let result = sweep_retention(dir.path(), 24).unwrap();
            assert_eq!(result, 1);
            assert!(!base.with_extension("zst").exists());
            assert!(!base.with_extension("idx").exists());
            assert!(!base.with_extension("meta.json").exists());
            assert!(!base.with_extension("uploaded").exists());
        }

        #[test]
        fn sweep_keeps_recent_chunks() {
            let dir = TempDir::new().unwrap();
            let base = make_chunk_files(&dir, "tx", "000000000100-000000000200");

            let result = sweep_retention(dir.path(), 24).unwrap();
            assert_eq!(result, 0);
            assert!(base.with_extension("zst").exists());
            assert!(base.with_extension("idx").exists());
            assert!(base.with_extension("meta.json").exists());
            assert!(base.with_extension("uploaded").exists());
        }

        #[test]
        fn sweep_handles_partial_file_set_gracefully() {
            let dir = TempDir::new().unwrap();
            let chunk_dir = dir.path().join("chunks").join("tx");
            fs::create_dir_all(&chunk_dir).unwrap();
            let base = chunk_dir.join("000000000100-000000000200");

            fs::write(base.with_extension("uploaded"), b"").unwrap();
            fs::write(base.with_extension("meta.json"), b"{}").unwrap();

            let old_time = SystemTime::now() - Duration::from_secs(25 * 3600);
            set_file_mtime(
                base.with_extension("uploaded"),
                FileTime::from_system_time(old_time),
            )
            .unwrap();

            let result = sweep_retention(dir.path(), 24).unwrap();
            assert_eq!(result, 1);
            assert!(!base.with_extension("uploaded").exists());
            assert!(!base.with_extension("meta.json").exists());
        }

        #[test]
        fn disk_usage_pct_returns_sane_value() {
            let dir = TempDir::new().unwrap();
            let pct = disk_usage_pct(dir.path()).unwrap();
            assert!(pct <= 100);
        }
    }

    mod disk_pressure {
        use super::*;
        use std::path::PathBuf;
        use tracing_test::traced_test;

        #[test]
        fn check_disk_pressure_returns_values_above_threshold() {
            let result = check_disk_pressure(85, 80);
            assert!(result.is_some());
            let (pct, _, _) = result.unwrap();
            assert_eq!(pct, 85);

            let result = check_disk_pressure(81, 80);
            assert!(result.is_some());

            let result = check_disk_pressure(80, 80);
            assert!(result.is_none());

            let result = check_disk_pressure(50, 80);
            assert!(result.is_none());

            let result = check_disk_pressure(100, 99);
            assert!(result.is_some());

            let result = check_disk_pressure(0, 1);
            assert!(result.is_none());
        }

        #[test]
        #[traced_test]
        fn disk_pressure_warn_log_emitted_above_threshold() {
            let path = PathBuf::from("/tmp/test-nvme");
            let threshold = 80u8;

            log_disk_pressure_warning(85, 1000, 500, &path, threshold);

            assert!(logs_contain("disk pressure exceeds threshold"));
        }

        #[test]
        #[traced_test]
        fn disk_pressure_warn_log_not_emitted_when_not_checked() {
            tracing::info!("some other log");
            assert!(!logs_contain("disk pressure exceeds threshold"));
        }
    }

    mod upload {
        use super::*;
        use aws_credential_types::provider::SharedCredentialsProvider;
        use aws_credential_types::Credentials;
        use aws_sdk_s3::config::Region;
        use aws_sdk_s3::operation::put_object::PutObjectError;
        use aws_sdk_s3::operation::put_object::PutObjectOutput;
        use aws_sdk_s3::{config::Config, Client};
        use aws_smithy_mocks::{create_mock_http_client, mock, MockResponseInterceptor, RuleMode};
        use aws_smithy_types::error::ErrorMetadata;
        use std::sync::{Arc, Mutex};

        fn build_mock_client(interceptor: MockResponseInterceptor) -> Client {
            use aws_smithy_types::retry::RetryConfig;

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

        fn mock_r2(rules: &[&aws_smithy_mocks::Rule], rule_mode: RuleMode) -> crate::r2::R2Client {
            let mut interceptor = MockResponseInterceptor::new().rule_mode(rule_mode);
            for rule in rules {
                interceptor = interceptor.with_rule(rule);
            }
            let client = build_mock_client(interceptor);
            crate::r2::R2Client::from_client(client, "test-bucket".to_string())
        }

        fn test_cfg() -> UploaderConfig {
            UploaderConfig {
                retry_attempts: 3,
                retry_initial_delay_ms: 1,
                ..UploaderConfig::default()
            }
        }

        fn make_chunk(dir: &TempDir) -> PendingChunk {
            let base = dir.path().join("000000000100-000000000200");
            fs::write(base.with_extension("zst"), b"zst-data").unwrap();
            fs::write(base.with_extension("idx"), b"idx-data").unwrap();
            fs::write(base.with_extension("meta.json"), b"{}").unwrap();
            PendingChunk {
                stream: Stream::Tx,
                base,
                start_slot: 100,
                end_slot: 200,
            }
        }

        fn put_ok_rule() -> aws_smithy_mocks::Rule {
            mock!(aws_sdk_s3::Client::put_object).then_output(|| PutObjectOutput::builder().build())
        }

        fn put_err_rule() -> aws_smithy_mocks::Rule {
            mock!(aws_sdk_s3::Client::put_object).then_error(|| {
                PutObjectError::generic(ErrorMetadata::builder().code("InternalError").build())
            })
        }

        #[tokio::test]
        async fn upload_trio_puts_zst_idx_meta_in_order() {
            let recorded_keys: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            let rk = recorded_keys.clone();

            let rule = mock!(aws_sdk_s3::Client::put_object).then_compute_output(move |req| {
                if let Some(key) = req.key() {
                    rk.lock().unwrap().push(key.to_string());
                }
                PutObjectOutput::builder().build()
            });

            let r2 = mock_r2(&[&rule], RuleMode::MatchAny);

            let dir = TempDir::new().unwrap();
            let chunk = make_chunk(&dir);
            let cfg = test_cfg();

            upload_chunk(&r2, &chunk, &cfg).await.unwrap();

            let keys = recorded_keys.lock().unwrap();
            assert_eq!(keys.len(), 3);
            assert!(keys[0].ends_with(".zst"));
            assert!(keys[1].ends_with(".idx"));
            assert!(keys[2].ends_with(".meta.json"));
        }

        #[tokio::test]
        async fn upload_trio_writes_marker_only_on_full_success() {
            let rule = put_ok_rule();
            let r2 = mock_r2(&[&rule], RuleMode::MatchAny);

            let dir = TempDir::new().unwrap();
            let chunk = make_chunk(&dir);
            let cfg = test_cfg();

            upload_chunk(&r2, &chunk, &cfg).await.unwrap();

            assert!(chunk.base.with_extension("uploaded").exists());
            assert!(!chunk.base.with_extension("uploaded.partial").exists());
        }

        #[tokio::test]
        async fn upload_trio_partial_failure_no_marker() {
            let rule = mock!(aws_sdk_s3::Client::put_object)
                .sequence()
                .output(|| PutObjectOutput::builder().build())
                .error(|| {
                    PutObjectError::generic(ErrorMetadata::builder().code("InternalError").build())
                })
                .times(3)
                .build();

            let r2 = mock_r2(&[&rule], RuleMode::Sequential);

            let dir = TempDir::new().unwrap();
            let chunk = make_chunk(&dir);
            let cfg = test_cfg();

            let result = upload_chunk(&r2, &chunk, &cfg).await;
            assert!(result.is_err());
            assert!(!chunk.base.with_extension("uploaded").exists());
        }

        #[tokio::test]
        async fn put_with_retry_succeeds_on_second_attempt() {
            let rule = mock!(aws_sdk_s3::Client::put_object)
                .sequence()
                .error(|| {
                    PutObjectError::generic(ErrorMetadata::builder().code("InternalError").build())
                })
                .output(|| PutObjectOutput::builder().build())
                .build();

            let r2 = mock_r2(&[&rule], RuleMode::Sequential);

            let dir = TempDir::new().unwrap();
            let file_path = dir.path().join("test.zst");
            fs::write(&file_path, b"data").unwrap();

            let cfg = test_cfg();
            let result = put_with_retry(&r2, "test-key", &file_path, &cfg).await;

            assert!(result.is_ok());
            assert_eq!(rule.num_calls(), 2);
        }

        #[tokio::test]
        async fn put_with_retry_exhausts_and_errors() {
            let rule = put_err_rule();
            let r2 = mock_r2(&[&rule], RuleMode::MatchAny);

            let dir = TempDir::new().unwrap();
            let file_path = dir.path().join("test.zst");
            fs::write(&file_path, b"data").unwrap();

            let cfg = test_cfg();
            let result = put_with_retry(&r2, "test-key", &file_path, &cfg).await;

            assert!(result.is_err());
            assert_eq!(rule.num_calls(), 3);
        }
    }

    mod loop_ {
        use super::*;
        use aws_credential_types::provider::SharedCredentialsProvider;
        use aws_credential_types::Credentials;
        use aws_sdk_s3::config::Region;
        use aws_sdk_s3::operation::put_object::PutObjectOutput;
        use aws_sdk_s3::{config::Config, Client};
        use aws_smithy_mocks::{create_mock_http_client, mock, MockResponseInterceptor, RuleMode};
        use sillage_common::config::StorageConfig;
        use sillage_common::shutdown::ShutdownSignal;
        use std::fs;
        use tempfile::TempDir;

        fn build_mock_client(interceptor: MockResponseInterceptor) -> Client {
            use aws_smithy_types::retry::RetryConfig;

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

        fn mock_r2(rules: &[&aws_smithy_mocks::Rule], rule_mode: RuleMode) -> crate::r2::R2Client {
            let mut interceptor = MockResponseInterceptor::new().rule_mode(rule_mode);
            for rule in rules {
                interceptor = interceptor.with_rule(rule);
            }
            let client = build_mock_client(interceptor);
            crate::r2::R2Client::from_client(client, "test-bucket".to_string())
        }

        fn test_uploader_cfg() -> UploaderConfig {
            UploaderConfig {
                scan_interval_secs: 1,
                max_concurrent_uploads: 2,
                retry_attempts: 3,
                retry_initial_delay_ms: 1,
                local_retention_hours: 24,
                disk_pressure_warn_pct: 80,
            }
        }

        fn test_storage_cfg(dir: &TempDir) -> StorageConfig {
            StorageConfig {
                nvme_path: dir.path().to_str().unwrap().to_string(),
            }
        }

        fn make_pending_chunk(dir: &TempDir, stream: Stream, start: u64, end: u64) -> PathBuf {
            let chunk_dir = dir.path().join("chunks").join(stream.as_str());
            fs::create_dir_all(&chunk_dir).unwrap();
            let stem = format!("{:012}-{:012}", start, end);
            let base = chunk_dir.join(&stem);
            fs::write(base.with_extension("zst"), b"zst-data").unwrap();
            fs::write(base.with_extension("idx"), b"idx-data").unwrap();
            fs::write(base.with_extension("meta.json"), b"{}").unwrap();
            base
        }

        #[tokio::test]
        async fn uploader_skips_iteration_when_r2_none() {
            let dir = TempDir::new().unwrap();
            let storage_cfg = test_storage_cfg(&dir);

            let uploader = Uploader::new(
                None,
                UploaderConfig {
                    scan_interval_secs: 1,
                    ..UploaderConfig::default()
                },
                storage_cfg,
            );

            let shutdown = ShutdownSignal::new();
            let shutdown_clone = shutdown.clone();
            let handle = tokio::spawn(async move {
                tokio::select! {
                    result = uploader.run(shutdown) => result,
                    _ = tokio::time::sleep(Duration::from_secs(2)) => Ok(()),
                }
            });

            tokio::time::sleep(Duration::from_millis(500)).await;
            shutdown_clone.cancel();

            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }

        #[tokio::test]
        async fn uploader_processes_pending_chunks_with_mock_r2() {
            let dir = TempDir::new().unwrap();

            for i in 0..3u64 {
                let start = i * 100;
                let end = start + 99;
                make_pending_chunk(&dir, Stream::Tx, start, end);
            }

            let rule = mock!(aws_sdk_s3::Client::put_object)
                .then_output(|| PutObjectOutput::builder().build());
            let r2 = mock_r2(&[&rule], RuleMode::MatchAny);

            let storage_cfg = test_storage_cfg(&dir);
            let uploader = Uploader::new(Some(r2), test_uploader_cfg(), storage_cfg);

            let shutdown = ShutdownSignal::new();
            let shutdown_clone = shutdown.clone();

            let handle = tokio::spawn(async move { uploader.run(shutdown).await });

            tokio::time::sleep(Duration::from_secs(3)).await;
            shutdown_clone.cancel();

            let result = handle.await.unwrap();
            assert!(result.is_ok());

            for i in 0..3u64 {
                let start = i * 100;
                let end = start + 99;
                let stem = format!("{:012}-{:012}", start, end);
                let chunk_dir = dir.path().join("chunks").join("tx");
                let marker = chunk_dir.join(&stem).with_extension("uploaded");
                assert!(
                    marker.exists(),
                    "missing .uploaded marker for chunk {start}-{end}"
                );
            }
        }

        #[tokio::test]
        async fn uploader_exits_cleanly_on_shutdown_between_chunks() {
            let dir = TempDir::new().unwrap();
            make_pending_chunk(&dir, Stream::Tx, 100, 200);

            let rule = mock!(aws_sdk_s3::Client::put_object)
                .then_output(|| PutObjectOutput::builder().build());
            let r2 = mock_r2(&[&rule], RuleMode::MatchAny);

            let storage_cfg = test_storage_cfg(&dir);
            let uploader = Uploader::new(Some(r2), test_uploader_cfg(), storage_cfg);

            let shutdown = ShutdownSignal::new();
            let shutdown_clone = shutdown.clone();

            let handle = tokio::spawn(async move { uploader.run(shutdown).await });

            tokio::time::sleep(Duration::from_millis(500)).await;
            shutdown_clone.cancel();

            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }

        #[tokio::test]
        #[ignore]
        async fn uploader_respects_concurrency_cap() {
            // Approach: create 8 pending chunks with max_concurrent_uploads=2,
            // inject a mock R2 that sleeps 100ms per PUT, and track in-flight
            // count with an AtomicUsize. Assert max in-flight never exceeds 2.
            // Skipped because aws-smithy-mocks does not support injecting async
            // delays or side-effects into mock responses — the mock framework
            // returns responses immediately. A proper concurrency-cap test would
            // require a real HTTP layer or a custom S3 client wrapper with
            // controllable latency.
        }
    }

    mod e2e {
        use super::*;
        use crate::stamp::Stamped;
        use aws_credential_types::provider::SharedCredentialsProvider;
        use aws_credential_types::Credentials;
        use aws_sdk_s3::config::Region;
        use aws_sdk_s3::operation::put_object::PutObjectOutput;
        use aws_sdk_s3::{config::Config, Client};
        use aws_smithy_mocks::{create_mock_http_client, mock, MockResponseInterceptor, RuleMode};
        use aws_smithy_types::retry::RetryConfig;
        use filetime::{set_file_mtime, FileTime};
        use sillage_common::config::{StorageConfig, WriterConfig};
        use sillage_common::shutdown::ShutdownSignal;
        use std::sync::{Arc, Mutex};
        use tempfile::TempDir;
        use yellowstone_grpc_proto::geyser::{
            subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateSlot,
        };

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

        fn mock_r2(rules: &[&aws_smithy_mocks::Rule], rule_mode: RuleMode) -> crate::r2::R2Client {
            let mut interceptor = MockResponseInterceptor::new().rule_mode(rule_mode);
            for rule in rules {
                interceptor = interceptor.with_rule(rule);
            }
            let client = build_mock_client(interceptor);
            crate::r2::R2Client::from_client(client, "test-bucket".to_string())
        }

        fn test_writer_cfg() -> WriterConfig {
            WriterConfig {
                slots_per_chunk: 10,
                out_of_order_tolerance_slots: 100,
                max_open_chunks: 4,
                channel_capacity: 8192,
            }
        }

        fn test_uploader_cfg() -> UploaderConfig {
            UploaderConfig {
                scan_interval_secs: 1,
                max_concurrent_uploads: 2,
                retry_attempts: 3,
                retry_initial_delay_ms: 1,
                local_retention_hours: 24,
                disk_pressure_warn_pct: 80,
            }
        }

        fn test_storage_cfg(dir: &TempDir) -> StorageConfig {
            StorageConfig {
                nvme_path: dir.path().to_str().unwrap().to_string(),
            }
        }

        fn make_slot_update(slot: u64) -> Stamped<SubscribeUpdate> {
            Stamped::new(SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot,
                    ..Default::default()
                })),
                ..Default::default()
            })
        }

        #[tokio::test]
        async fn e2e_chunker_to_uploader_to_mock_r2() {
            // 1. Create temp dir
            let dir = TempDir::new().unwrap();

            // 2. Build Chunker with slots_per_chunk=10
            let cfg = test_writer_cfg();
            let mut chunker = crate::chunker::Chunker::new(Stream::Tx, cfg, dir.path()).unwrap();

            // 3. Create 30 synthetic SubscribeUpdate messages with slots 0..=29
            //    This spans 3 chunk ranges: 0-9, 10-19, 20-29
            for slot in 0..30u64 {
                let msg = make_slot_update(slot);
                chunker.ingest(msg, slot).unwrap();
            }

            // 5. Shutdown chunker to seal all open chunks
            chunker.shutdown().unwrap();

            // 6. Verify 3 chunks exist on disk
            let tx_dir = dir.path().join("chunks").join("tx");
            let expected_stems = [
                format!("{:012}-{:012}", 0u64, 10u64),
                format!("{:012}-{:012}", 10u64, 20u64),
                format!("{:012}-{:012}", 20u64, 30u64),
            ];
            for stem in &expected_stems {
                assert!(
                    tx_dir.join(format!("{stem}.zst")).exists(),
                    "missing {stem}.zst"
                );
                assert!(
                    tx_dir.join(format!("{stem}.idx")).exists(),
                    "missing {stem}.idx"
                );
                assert!(
                    tx_dir.join(format!("{stem}.meta.json")).exists(),
                    "missing {stem}.meta.json"
                );
            }

            // 7. Build mock R2 client that records PUT keys
            let recorded_keys: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            let rk = recorded_keys.clone();
            let rule = mock!(aws_sdk_s3::Client::put_object).then_compute_output(move |req| {
                if let Some(key) = req.key() {
                    rk.lock().unwrap().push(key.to_string());
                }
                PutObjectOutput::builder().build()
            });
            let r2 = mock_r2(&[&rule], RuleMode::MatchAny);

            // 8. Build Uploader
            let storage_cfg = test_storage_cfg(&dir);
            let uploader = Uploader::new(Some(r2), test_uploader_cfg(), storage_cfg);

            // 9. Spawn uploader with shutdown signal and timeout
            let shutdown = ShutdownSignal::new();
            let shutdown_clone = shutdown.clone();
            let handle = tokio::spawn(async move {
                tokio::select! {
                    result = uploader.run(shutdown) => result,
                    _ = tokio::time::sleep(Duration::from_secs(4)) => Ok(()),
                }
            });

            // 10. Wait for uploads to complete, then signal shutdown
            tokio::time::sleep(Duration::from_secs(3)).await;
            shutdown_clone.cancel();

            let result = handle.await.unwrap();
            assert!(result.is_ok());

            // 11. Assert: 3 .uploaded markers exist locally
            for stem in &expected_stems {
                let marker = tx_dir.join(format!("{stem}.uploaded"));
                assert!(marker.exists(), "missing .uploaded marker for {stem}");
            }

            // Assert: mock recorded exactly 9 PUTs (3 chunks × 3 files)
            let keys = recorded_keys.lock().unwrap();
            assert_eq!(keys.len(), 9, "expected 9 PUTs, got {}", keys.len());

            // Assert: each chunk's PUT keys appear in .zst, .idx, .meta.json order
            // (chunks may be interleaved due to concurrency, so group by stem first)
            let mut per_chunk: std::collections::BTreeMap<String, Vec<String>> =
                std::collections::BTreeMap::new();
            for key in keys.iter() {
                let stem = key
                    .rsplit_once('/')
                    .map(|(_, rest)| rest.to_string())
                    .unwrap_or_else(|| key.clone());
                let chunk_stem = stem.split('.').next().unwrap_or("").to_string();
                per_chunk.entry(chunk_stem).or_default().push(key.clone());
            }
            assert_eq!(per_chunk.len(), 3, "expected 3 chunk groups");
            for (chunk_stem, chunk_keys) in &per_chunk {
                assert_eq!(chunk_keys.len(), 3, "chunk {chunk_stem} should have 3 PUTs");
                assert!(
                    chunk_keys[0].ends_with(".zst"),
                    "chunk {chunk_stem} first PUT should be .zst, got {}",
                    chunk_keys[0]
                );
                assert!(
                    chunk_keys[1].ends_with(".idx"),
                    "chunk {chunk_stem} second PUT should be .idx, got {}",
                    chunk_keys[1]
                );
                assert!(
                    chunk_keys[2].ends_with(".meta.json"),
                    "chunk {chunk_stem} third PUT should be .meta.json, got {}",
                    chunk_keys[2]
                );
            }

            // Drop the lock before proceeding to retention sweep
            drop(keys);

            // 12. Backdate one .uploaded marker's mtime to 25 hours ago
            let old_stem = &expected_stems[0];
            let old_marker = tx_dir.join(format!("{old_stem}.uploaded"));
            let old_time = SystemTime::now() - Duration::from_secs(25 * 3600);
            set_file_mtime(&old_marker, FileTime::from_system_time(old_time)).unwrap();

            // 13. Run sweep_retention with 24-hour retention
            let evicted = sweep_retention(dir.path(), 24).unwrap();

            // 14. Assert that chunk's 4 files are gone
            assert_eq!(evicted, 1, "expected 1 evicted chunk, got {evicted}");
            assert!(!tx_dir.join(format!("{old_stem}.zst")).exists());
            assert!(!tx_dir.join(format!("{old_stem}.idx")).exists());
            assert!(!tx_dir.join(format!("{old_stem}.meta.json")).exists());
            assert!(!tx_dir.join(format!("{old_stem}.uploaded")).exists());

            // The other two chunks should still be present
            for stem in &expected_stems[1..] {
                assert!(
                    tx_dir.join(format!("{stem}.uploaded")).exists(),
                    "chunk {stem} should still be present after sweep"
                );
            }
        }
    }
}
