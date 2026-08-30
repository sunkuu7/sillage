//! Startup crash-recovery: sweep `*.partial` files left behind by a previous
//! crash mid-seal, then report per-stream resume state (last sealed slot,
//! pending uploads) so lanes can resume Geyser from the right point and ops
//! can see the gap.
//!
//! Chunker state is intentionally non-persistent: a crash mid-chunk discards
//! the in-memory zstd buffer + index. We accept a few seconds of message loss
//! at the chunk boundary as the trade for not having a write-ahead log.

use std::path::Path;

use anyhow::Result;
use sillage_common::Stream;
use tracing::warn;

#[derive(Debug, Clone)]
pub(crate) struct StreamRecovery {
    pub stream: Stream,
    /// Exclusive end slot of the most recent sealed chunk for this stream,
    /// i.e. the next slot the lane should request from Geyser.
    pub resume_slot: Option<u64>,
    /// Sealed chunks (`.meta.json` exists) that lack a `.uploaded` marker.
    /// Already discovered by the uploader's periodic scan; surfaced here for
    /// startup visibility.
    pub unuploaded: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct RecoveryReport {
    pub partials_removed: u32,
    pub per_stream: Vec<StreamRecovery>,
}

/// Run the full startup recovery: sweep partials, compute per-stream resume
/// state. Idempotent — safe to call any number of times.
pub(crate) fn run_recovery(nvme_path: &Path) -> Result<RecoveryReport> {
    let partials_removed = sweep_partials(nvme_path)?;
    let mut per_stream = Vec::with_capacity(Stream::all().len());
    for stream in Stream::all() {
        per_stream.push(StreamRecovery {
            stream,
            resume_slot: latest_sealed_end_slot(nvme_path, stream)?,
            unuploaded: count_unuploaded(nvme_path, stream)?,
        });
    }
    Ok(RecoveryReport {
        partials_removed,
        per_stream,
    })
}

/// Remove every `*.partial` file under `{nvme_path}/chunks/{stream}/`. These
/// are remnants from a crash mid-seal and are unrecoverable.
fn sweep_partials(nvme_path: &Path) -> Result<u32> {
    let mut count = 0u32;
    for stream in Stream::all() {
        let dir = nvme_path.join("chunks").join(stream.as_str());
        if !dir.is_dir() {
            continue;
        }
        let entries = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) => {
                warn!(path = %dir.display(), error = %e, "cannot read stream dir during partial sweep");
                continue;
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            // Path::extension returns the trailing extension only; .zst.partial,
            // .idx.partial, .meta.json.partial, .uploaded.partial all end in
            // ".partial" so the last component is "partial".
            if path.extension().and_then(|e| e.to_str()) == Some("partial") {
                match std::fs::remove_file(&path) {
                    Ok(_) => {
                        warn!(path = %path.display(), "removed orphan .partial from previous crash");
                        count += 1;
                    }
                    Err(e) => {
                        warn!(path = %path.display(), error = %e, "could not remove .partial");
                    }
                }
            }
        }
    }
    Ok(count)
}

/// Return the maximum `end_slot_exclusive` across all sealed chunks for a
/// stream, parsed from `.meta.json` filenames. `None` if no sealed chunks
/// exist (fresh start).
fn latest_sealed_end_slot(nvme_path: &Path, stream: Stream) -> Result<Option<u64>> {
    let dir = nvme_path.join("chunks").join(stream.as_str());
    if !dir.is_dir() {
        return Ok(None);
    }
    let mut max_end: Option<u64> = None;
    for entry in std::fs::read_dir(&dir)?.flatten() {
        let name_owned = entry.file_name();
        let name = match name_owned.to_str() {
            Some(s) => s,
            None => continue,
        };
        // Sealed chunks are identified by .meta.json (the seal-complete marker).
        let stem = match name.strip_suffix(".meta.json") {
            Some(s) => s,
            None => continue,
        };
        // Filename: {start:012}-{end:012}
        let (_, end_str) = match stem.split_once('-') {
            Some(p) => p,
            None => continue,
        };
        if let Ok(end) = end_str.parse::<u64>() {
            max_end = Some(max_end.map_or(end, |m| m.max(end)));
        }
    }
    Ok(max_end)
}

/// Count `.meta.json` files lacking a sibling `.uploaded` marker.
fn count_unuploaded(nvme_path: &Path, stream: Stream) -> Result<u32> {
    let dir = nvme_path.join("chunks").join(stream.as_str());
    if !dir.is_dir() {
        return Ok(0);
    }
    let mut count = 0u32;
    for entry in std::fs::read_dir(&dir)?.flatten() {
        let path = entry.path();
        let name_owned = path.file_name().map(|n| n.to_owned());
        let name = match name_owned.as_ref().and_then(|n| n.to_str()) {
            Some(s) => s,
            None => continue,
        };
        let stem = match name.strip_suffix(".meta.json") {
            Some(s) => s,
            None => continue,
        };
        let uploaded = dir.join(format!("{stem}.uploaded"));
        if !uploaded.exists() {
            count += 1;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn touch(path: &Path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, b"").unwrap();
    }

    #[test]
    fn sweep_partials_removes_all_extensions() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        let tx_dir = root.join("chunks").join("tx");
        touch(&tx_dir.join("000000000000-000000000100.zst.partial"));
        touch(&tx_dir.join("000000000000-000000000100.idx.partial"));
        touch(&tx_dir.join("000000000000-000000000100.meta.json.partial"));
        touch(&tx_dir.join("000000000000-000000000100.uploaded.partial"));
        // A real sealed chunk that must NOT be touched.
        touch(&tx_dir.join("000000000100-000000000200.zst"));

        let removed = sweep_partials(root).unwrap();
        assert_eq!(removed, 4);
        assert!(tx_dir.join("000000000100-000000000200.zst").exists());
        assert!(!tx_dir
            .join("000000000000-000000000100.zst.partial")
            .exists());
    }

    #[test]
    fn sweep_partials_returns_zero_when_no_partials() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        touch(&root.join("chunks/tx/000000000000-000000000100.zst"));
        let removed = sweep_partials(root).unwrap();
        assert_eq!(removed, 0);
    }

    #[test]
    fn sweep_partials_handles_missing_dirs() {
        let dir = TempDir::new().unwrap();
        let removed = sweep_partials(dir.path()).unwrap();
        assert_eq!(removed, 0);
    }

    #[test]
    fn latest_sealed_end_slot_returns_max() {
        let dir = TempDir::new().unwrap();
        let tx_dir = dir.path().join("chunks").join("tx");
        touch(&tx_dir.join("000000000000-000000000100.meta.json"));
        touch(&tx_dir.join("000000000100-000000000200.meta.json"));
        touch(&tx_dir.join("000000000200-000000000350.meta.json"));
        let got = latest_sealed_end_slot(dir.path(), Stream::Tx).unwrap();
        assert_eq!(got, Some(350));
    }

    #[test]
    fn latest_sealed_end_slot_returns_none_when_no_sealed() {
        let dir = TempDir::new().unwrap();
        // .zst alone (no .meta.json) means seal was incomplete — must not count.
        touch(&dir.path().join("chunks/tx/000000000000-000000000100.zst"));
        let got = latest_sealed_end_slot(dir.path(), Stream::Tx).unwrap();
        assert_eq!(got, None);
    }

    #[test]
    fn latest_sealed_end_slot_ignores_partials_and_bad_names() {
        let dir = TempDir::new().unwrap();
        let tx_dir = dir.path().join("chunks").join("tx");
        touch(&tx_dir.join("000000000000-000000000100.meta.json.partial"));
        touch(&tx_dir.join("nonsense.meta.json"));
        touch(&tx_dir.join("000000000100-000000000200.meta.json"));
        let got = latest_sealed_end_slot(dir.path(), Stream::Tx).unwrap();
        assert_eq!(got, Some(200));
    }

    #[test]
    fn count_unuploaded_counts_meta_without_marker() {
        let dir = TempDir::new().unwrap();
        let tx_dir = dir.path().join("chunks").join("tx");
        touch(&tx_dir.join("000000000000-000000000100.meta.json"));
        touch(&tx_dir.join("000000000000-000000000100.uploaded"));
        touch(&tx_dir.join("000000000100-000000000200.meta.json"));
        // chunk 100-200 has no .uploaded marker → counts
        touch(&tx_dir.join("000000000200-000000000300.meta.json"));
        // chunk 200-300 has no .uploaded marker → counts
        let count = count_unuploaded(dir.path(), Stream::Tx).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn run_recovery_reports_per_stream() {
        let dir = TempDir::new().unwrap();
        touch(
            &dir.path()
                .join("chunks/tx/000000000000-000000000100.zst.partial"),
        );
        touch(
            &dir.path()
                .join("chunks/tx/000000000100-000000000200.meta.json"),
        );
        touch(
            &dir.path()
                .join("chunks/acct/000000000050-000000000150.meta.json"),
        );
        let report = run_recovery(dir.path()).unwrap();
        assert_eq!(report.partials_removed, 1);
        let tx = report
            .per_stream
            .iter()
            .find(|s| s.stream == Stream::Tx)
            .unwrap();
        assert_eq!(tx.resume_slot, Some(200));
        assert_eq!(tx.unuploaded, 1);
        let acct = report
            .per_stream
            .iter()
            .find(|s| s.stream == Stream::Acct)
            .unwrap();
        assert_eq!(acct.resume_slot, Some(150));
        let block = report
            .per_stream
            .iter()
            .find(|s| s.stream == Stream::Block)
            .unwrap();
        assert_eq!(block.resume_slot, None);
    }
}
