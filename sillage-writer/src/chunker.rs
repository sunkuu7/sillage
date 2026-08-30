use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use prost::Message;
use sillage_common::chunk::{write_len_prefixed, ChunkMeta, SCHEMA_VERSION};
use sillage_common::config::WriterConfig;
use sillage_common::Stream;
use tracing::{info, warn};
use yellowstone_grpc_proto::geyser::SubscribeUpdate;
use zstd::stream::write::Encoder;

use crate::index::IndexBuilder;
use crate::stamp::Stamped;

const ZSTD_LEVEL: i32 = 3;
const SEAL_LOOKAHEAD_CHUNKS: u64 = 2;
const IDLE_SEAL_THRESHOLD: Duration = Duration::from_secs(300);

struct OpenChunk {
    start_slot: u64,
    end_slot_exclusive: u64,
    partial_zst: PathBuf,
    partial_meta: PathBuf,
    final_zst: PathBuf,
    final_meta: PathBuf,
    partial_idx: PathBuf,
    final_idx: PathBuf,
    encoder: Option<Encoder<'static, BufWriter<File>>>,
    message_count: u64,
    uncompressed_bytes: u64,
    first_message_slot: Option<u64>,
    last_message_slot: Option<u64>,
    recv_ns_first: Option<u64>,
    recv_ns_last: Option<u64>,
    last_activity: Instant,
    index: IndexBuilder,
}

pub(crate) struct Chunker {
    stream: Stream,
    cfg: WriterConfig,
    base_dir: PathBuf,
    open: BTreeMap<u64, OpenChunk>,
    watermark_slot: Option<u64>,
}

impl Chunker {
    pub fn new(stream: Stream, cfg: WriterConfig, nvme_path: &Path) -> Result<Self> {
        let base_dir = nvme_path.join("chunks").join(stream.as_str());
        fs::create_dir_all(&base_dir)
            .with_context(|| format!("creating chunk dir {}", base_dir.display()))?;
        Ok(Self {
            stream,
            cfg,
            base_dir,
            open: BTreeMap::new(),
            watermark_slot: None,
        })
    }

    pub fn ingest(&mut self, msg: Stamped<SubscribeUpdate>, slot: u64) -> Result<()> {
        self.watermark_slot = Some(self.watermark_slot.map_or(slot, |w| w.max(slot)));
        let watermark = self.watermark_slot.unwrap();
        let watermark_chunk = watermark / self.cfg.slots_per_chunk;
        let chunk_idx = slot / self.cfg.slots_per_chunk;

        if slot + self.cfg.out_of_order_tolerance_slots < watermark {
            warn!(stream = %self.stream, slot, watermark,
                  "message older than tolerance, dropping (gap)");
            return Ok(());
        }

        if !self.open.contains_key(&chunk_idx) {
            while self.open.len() >= self.cfg.max_open_chunks {
                let oldest = *self.open.keys().next().expect("open is non-empty");
                if oldest == chunk_idx {
                    break;
                }
                self.seal_chunk(oldest, "cap-reached")?;
            }
            self.open_chunk(chunk_idx)?;
        }

        if let Some(chunk) = self.open.get_mut(&chunk_idx) {
            chunk.append(&msg, slot)?;
        }

        let to_seal: Vec<u64> = self
            .open
            .keys()
            .copied()
            .filter(|&idx| watermark_chunk >= idx + SEAL_LOOKAHEAD_CHUNKS)
            .collect();
        for idx in to_seal {
            self.seal_chunk(idx, "watermark")?;
        }

        Ok(())
    }

    pub fn tick(&mut self) -> Result<()> {
        let now = Instant::now();
        let stale: Vec<u64> = self
            .open
            .iter()
            .filter(|(_, c)| now.duration_since(c.last_activity) > IDLE_SEAL_THRESHOLD)
            .map(|(idx, _)| *idx)
            .collect();
        for idx in stale {
            self.seal_chunk(idx, "shutdown")?;
        }
        Ok(())
    }

    pub fn shutdown(&mut self) -> Result<()> {
        let indices: Vec<u64> = self.open.keys().copied().collect();
        for idx in indices {
            self.seal_chunk(idx, "shutdown")?;
        }
        Ok(())
    }

    fn open_chunk(&mut self, chunk_idx: u64) -> Result<()> {
        if self.open.contains_key(&chunk_idx) {
            return Ok(());
        }

        let start_slot = chunk_idx * self.cfg.slots_per_chunk;
        let end_slot_exclusive = start_slot + self.cfg.slots_per_chunk;
        let stem = format!("{:012}-{:012}", start_slot, end_slot_exclusive);
        let final_zst = self.base_dir.join(format!("{stem}.zst"));
        let final_meta = self.base_dir.join(format!("{stem}.meta.json"));
        let final_idx = self.base_dir.join(format!("{stem}.idx"));

        if final_zst.exists() || final_meta.exists() {
            warn!(stream = %self.stream, chunk_idx, path = %final_zst.display(),
                  "chunk already sealed on disk; dropping messages for this range");
            return Ok(());
        }

        let partial_zst = self.base_dir.join(format!("{stem}.zst.partial"));
        let partial_meta = self.base_dir.join(format!("{stem}.meta.json.partial"));
        let partial_idx = self.base_dir.join(format!("{stem}.idx.partial"));

        let file = File::create(&partial_zst)
            .with_context(|| format!("creating {}", partial_zst.display()))?;
        let encoder =
            Encoder::new(BufWriter::new(file), ZSTD_LEVEL).context("initializing zstd encoder")?;

        info!(stream = %self.stream, chunk_idx, start_slot, end_slot_exclusive, "opened chunk");

        self.open.insert(
            chunk_idx,
            OpenChunk {
                start_slot,
                end_slot_exclusive,
                partial_zst,
                partial_meta,
                partial_idx,
                final_zst,
                final_meta,
                final_idx,
                encoder: Some(encoder),
                message_count: 0,
                uncompressed_bytes: 0,
                first_message_slot: None,
                last_message_slot: None,
                recv_ns_first: None,
                recv_ns_last: None,
                last_activity: Instant::now(),
                index: IndexBuilder::for_stream(self.stream),
            },
        );
        Ok(())
    }

    fn seal_chunk(&mut self, chunk_idx: u64, reason: &'static str) -> Result<()> {
        let chunk = self
            .open
            .remove(&chunk_idx)
            .context("seal_chunk on missing idx")?;
        let OpenChunk {
            mut encoder,
            partial_zst,
            partial_meta,
            partial_idx,
            final_zst,
            final_meta,
            final_idx,
            start_slot,
            end_slot_exclusive,
            message_count,
            uncompressed_bytes,
            first_message_slot,
            last_message_slot,
            recv_ns_first,
            recv_ns_last,
            index,
            last_activity: _,
        } = chunk;

        let encoder = encoder.take().expect("encoder present");
        let buf_writer = encoder.finish().context("finishing zstd encoder")?;
        let file = buf_writer.into_inner().context("flushing BufWriter")?;
        let compressed_bytes = file.metadata().map(|m| m.len()).unwrap_or(0);
        file.sync_all().context("fsync chunk.zst.partial")?;
        drop(file);

        let meta = ChunkMeta {
            schema_version: SCHEMA_VERSION,
            stream: self.stream.as_str().to_string(),
            start_slot,
            end_slot_exclusive,
            first_message_slot,
            last_message_slot,
            message_count,
            uncompressed_bytes,
            compressed_bytes,
            recv_ns_first,
            recv_ns_last,
            sealed_reason: reason.to_string(),
            index_dimensions: index.dimension_names(),
        };
        let meta_json = serde_json::to_vec_pretty(&meta).context("serializing meta.json")?;
        let mut meta_file = File::create(&partial_meta)
            .with_context(|| format!("creating {}", partial_meta.display()))?;
        meta_file.write_all(&meta_json)?;
        meta_file.sync_all()?;
        drop(meta_file);

        fs::rename(&partial_zst, &final_zst).with_context(|| {
            format!(
                "renaming {} → {}",
                partial_zst.display(),
                final_zst.display()
            )
        })?;

        let idx_bytes = index
            .serialize(start_slot, end_slot_exclusive, message_count)
            .with_context(|| format!("serializing index for chunk {chunk_idx}"))?;
        let mut idx_file = File::create(&partial_idx)
            .with_context(|| format!("creating {}", partial_idx.display()))?;
        idx_file.write_all(&idx_bytes)?;
        idx_file.sync_all().context("fsync chunk.idx.partial")?;
        drop(idx_file);

        fs::rename(&partial_idx, &final_idx).with_context(|| {
            format!(
                "renaming {} → {}",
                partial_idx.display(),
                final_idx.display()
            )
        })?;

        fs::rename(&partial_meta, &final_meta).with_context(|| {
            format!(
                "renaming {} → {}",
                partial_meta.display(),
                final_meta.display()
            )
        })?;

        info!(stream = %self.stream, chunk_idx, start_slot, end_slot_exclusive,
              message_count, compressed_bytes, uncompressed_bytes, reason, "sealed chunk");
        Ok(())
    }
}

impl OpenChunk {
    fn append(&mut self, msg: &Stamped<SubscribeUpdate>, slot: u64) -> Result<()> {
        let encoder = self.encoder.as_mut().context("encoder gone")?;
        let mut buf = Vec::with_capacity(msg.inner.encoded_len() + 4);
        msg.inner.encode(&mut buf).context("prost encode")?;
        let mut framed = Vec::with_capacity(buf.len() + 4);
        write_len_prefixed(&mut framed, &buf);
        encoder.write_all(&framed).context("encoder write")?;

        self.index.observe(self.message_count as u32, &msg.inner);
        self.message_count += 1;
        self.uncompressed_bytes += framed.len() as u64;
        self.first_message_slot = Some(self.first_message_slot.map_or(slot, |s| s.min(slot)));
        self.last_message_slot = Some(self.last_message_slot.map_or(slot, |s| s.max(slot)));
        self.recv_ns_first = Some(
            self.recv_ns_first
                .map_or(msg.recv.wall_ns, |t| t.min(msg.recv.wall_ns)),
        );
        self.recv_ns_last = Some(
            self.recv_ns_last
                .map_or(msg.recv.wall_ns, |t| t.max(msg.recv.wall_ns)),
        );
        self.last_activity = Instant::now();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn test_cfg(slots_per_chunk: u64, tolerance: u64, max_open_chunks: usize) -> WriterConfig {
        WriterConfig {
            slots_per_chunk,
            out_of_order_tolerance_slots: tolerance,
            max_open_chunks,
            channel_capacity: 8192,
        }
    }

    fn stamped_msg(wall_ns: u64) -> Stamped<SubscribeUpdate> {
        Stamped {
            recv: crate::stamp::RecvStamp {
                mono: Instant::now(),
                wall_ns,
            },
            inner: SubscribeUpdate::default(),
        }
    }

    #[test]
    fn bucketing_seals_completed_chunk_on_watermark_advance() {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_cfg(1000, 100, 10);
        let mut chunker = Chunker::new(Stream::Tx, cfg, dir.path()).unwrap();

        for slot in 100..1100u64 {
            let msg = stamped_msg(1_000_000);
            chunker.ingest(msg, slot).unwrap();
        }

        chunker.shutdown().unwrap();

        let tx_dir = dir.path().join("chunks").join("tx");
        let entries: Vec<_> = fs::read_dir(&tx_dir).unwrap().collect();
        let zsts: Vec<_> = entries
            .iter()
            .filter(|e| {
                let path = e.as_ref().unwrap().path();
                path.extension().is_some_and(|ext| ext == "zst")
            })
            .collect();

        assert!(!zsts.is_empty(), "at least one .zst should exist");

        for zst_entry in &zsts {
            let zst_path = zst_entry.as_ref().unwrap().path();
            let stem = zst_path.file_stem().unwrap().to_str().unwrap();
            let idx_path = tx_dir.join(format!("{stem}.idx"));
            let meta_path = tx_dir.join(format!("{stem}.meta.json"));
            assert!(idx_path.exists(), "{stem}.idx should exist");
            assert!(meta_path.exists(), "{stem}.meta.json should exist");
        }

        let partials: Vec<_> = entries
            .iter()
            .filter(|e| {
                let path = e.as_ref().unwrap().path();
                path.to_string_lossy().ends_with(".partial")
            })
            .collect();
        assert!(partials.is_empty(), "no .partial files should remain");

        let meta_path = tx_dir.join(format!("{:012}-{:012}.meta.json", 0, 100));
        if meta_path.exists() {
            let meta: serde_json::Value =
                serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
            let dims = meta["index_dimensions"].as_array().unwrap();
            assert_eq!(dims.len(), 5);
            assert_eq!(dims[0], "program_id");
            assert_eq!(dims[1], "account_key");
            assert_eq!(dims[2], "signature");
            assert_eq!(dims[3], "vote_flag");
            assert_eq!(dims[4], "failed_flag");
        }
    }

    #[test]
    fn out_of_order_within_tolerance_lands_in_right_chunk() {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_cfg(1000, 100, 10);
        let mut chunker = Chunker::new(Stream::Tx, cfg, dir.path()).unwrap();

        for &slot in &[100u64, 102, 105, 99] {
            let msg = stamped_msg(1_000_000);
            chunker.ingest(msg, slot).unwrap();
        }

        let chunk = chunker.open.get(&0u64).unwrap();
        assert_eq!(chunk.message_count, 4);
    }

    #[test]
    fn out_of_order_beyond_tolerance_drops_with_warn() {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_cfg(1000, 100, 10);
        let mut chunker = Chunker::new(Stream::Tx, cfg, dir.path()).unwrap();

        let msg = stamped_msg(1_000_000);
        chunker.ingest(msg, 100_000).unwrap();

        let msg = stamped_msg(1_000_000);
        chunker.ingest(msg, 50_000).unwrap();

        let total: u64 = chunker.open.values().map(|c| c.message_count).sum();
        assert_eq!(total, 1);
    }

    #[test]
    fn cap_reached_force_seals_oldest() {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_cfg(1000, 100, 2);
        let mut chunker = Chunker::new(Stream::Tx, cfg, dir.path()).unwrap();

        let msg = stamped_msg(1_000_000);
        chunker.ingest(msg, 100).unwrap();
        assert!(chunker.open.contains_key(&0u64));

        let msg = stamped_msg(1_000_000);
        chunker.ingest(msg, 1100).unwrap();
        assert!(chunker.open.contains_key(&1u64));

        let msg = stamped_msg(1_000_000);
        chunker.ingest(msg, 2100).unwrap();

        assert!(!chunker.open.contains_key(&0u64));
        assert!(chunker.open.contains_key(&1u64));
        assert!(chunker.open.contains_key(&2u64));

        let stem = format!("{:012}-{:012}", 0, 1000);
        let meta_path = dir
            .path()
            .join("chunks")
            .join("tx")
            .join(format!("{stem}.meta.json"));
        let meta: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
        assert_eq!(meta["sealed_reason"], "cap-reached");
    }

    #[test]
    fn shutdown_seals_all_open_chunks_with_shutdown_reason() {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_cfg(1000, 100, 10);
        let mut chunker = Chunker::new(Stream::Tx, cfg, dir.path()).unwrap();

        let msg = stamped_msg(1_000_000);
        chunker.ingest(msg, 100).unwrap();
        let msg = stamped_msg(1_000_000);
        chunker.ingest(msg, 1100).unwrap();

        assert!(chunker.open.contains_key(&0u64));
        assert!(chunker.open.contains_key(&1u64));

        chunker.shutdown().unwrap();
        assert!(chunker.open.is_empty());

        for &start in &[0u64, 1000u64] {
            let stem = format!("{:012}-{:012}", start, start + 1000);
            let meta_path = dir
                .path()
                .join("chunks")
                .join("tx")
                .join(format!("{stem}.meta.json"));
            let meta: serde_json::Value =
                serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
            assert_eq!(meta["sealed_reason"], "shutdown");
        }
    }

    #[test]
    fn sealed_chunk_meta_matches_disk_zst() {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_cfg(1000, 100, 10);
        let mut chunker = Chunker::new(Stream::Tx, cfg, dir.path()).unwrap();

        for slot in 0..100u64 {
            let msg = stamped_msg(1_000_000 + slot);
            chunker.ingest(msg, slot).unwrap();
        }

        let msg = stamped_msg(2_000_000);
        chunker.ingest(msg, 3000).unwrap();

        let stem = format!("{:012}-{:012}", 0, 1000);
        let zst_path = dir
            .path()
            .join("chunks")
            .join("tx")
            .join(format!("{stem}.zst"));
        let compressed = fs::read(&zst_path).unwrap();

        let cursor = std::io::Cursor::new(&compressed);
        let mut decoder = zstd::stream::read::Decoder::new(cursor).unwrap();
        let mut decompressed = Vec::new();
        std::io::Read::read_to_end(&mut decoder, &mut decompressed).unwrap();

        let mut count = 0u64;
        let mut cursor = &decompressed[..];
        while !cursor.is_empty() {
            let len = u32::from_le_bytes(cursor[..4].try_into().unwrap()) as usize;
            cursor = &cursor[4..];
            let _msg = SubscribeUpdate::decode(&cursor[..len]).unwrap();
            cursor = &cursor[len..];
            count += 1;
        }
        assert_eq!(count, 100);

        let meta_path = dir
            .path()
            .join("chunks")
            .join("tx")
            .join(format!("{stem}.meta.json"));
        let meta: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
        assert_eq!(meta["message_count"], 100);
        assert_eq!(meta["start_slot"], 0);
        assert_eq!(meta["end_slot_exclusive"], 1000);
        assert_eq!(meta["first_message_slot"], 0);
        assert_eq!(meta["last_message_slot"], 99);
    }

    #[test]
    fn conflict_guard_skips_existing_chunk() {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_cfg(1000, 100, 10);
        let stream_dir = dir.path().join("chunks").join("tx");
        fs::create_dir_all(&stream_dir).unwrap();

        let stem = format!("{:012}-{:012}", 0, 1000);
        let zst_path = stream_dir.join(format!("{stem}.zst"));
        fs::write(&zst_path, b"fake").unwrap();

        let mut chunker = Chunker::new(Stream::Tx, cfg, dir.path()).unwrap();

        let msg = stamped_msg(1_000_000);
        chunker.ingest(msg, 500).unwrap();

        let partial_path = stream_dir.join(format!("{stem}.zst.partial"));
        assert!(!partial_path.exists());
        assert!(!chunker.open.contains_key(&0u64));
    }

    #[test]
    fn ingest_observes_into_index() {
        use crate::index::DIM_ACCOUNT_KEY;
        use yellowstone_grpc_proto::geyser::{
            subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateTransaction,
            SubscribeUpdateTransactionInfo,
        };
        use yellowstone_grpc_proto::solana::storage::confirmed_block::{
            Message, Transaction, TransactionStatusMeta,
        };

        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_cfg(1000, 100, 10);
        let mut chunker = Chunker::new(Stream::Tx, cfg, dir.path()).unwrap();

        for slot in 100..103u64 {
            let mut msg = stamped_msg(1_000_000);
            msg.inner = SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    transaction: Some(SubscribeUpdateTransactionInfo {
                        signature: vec![0u8; 64],
                        is_vote: false,
                        transaction: Some(Transaction {
                            message: Some(Message {
                                account_keys: vec![vec![slot as u8; 32]],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        meta: Some(TransactionStatusMeta {
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            };
            chunker.ingest(msg, slot).unwrap();
        }

        let chunk = chunker.open.get(&0u64).unwrap();
        let account_dim = chunk.index.dims.get(DIM_ACCOUNT_KEY).unwrap();
        assert_eq!(account_dim.len(), 3);
        for (_, bitmap) in account_dim.iter() {
            assert_eq!(bitmap.len(), 1);
        }
    }

    #[test]
    fn seal_writes_zst_and_idx_and_meta() {
        use yellowstone_grpc_proto::geyser::{
            subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateTransaction,
            SubscribeUpdateTransactionInfo,
        };
        use yellowstone_grpc_proto::solana::storage::confirmed_block::{
            Message, Transaction, TransactionStatusMeta,
        };

        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_cfg(100, 10, 10);
        let mut chunker = Chunker::new(Stream::Tx, cfg, dir.path()).unwrap();

        for slot in 0..250u64 {
            let mut msg = stamped_msg(1_000_000 + slot);
            msg.inner = SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    transaction: Some(SubscribeUpdateTransactionInfo {
                        signature: vec![0u8; 64],
                        is_vote: false,
                        transaction: Some(Transaction {
                            message: Some(Message {
                                account_keys: vec![vec![slot as u8; 32]],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        meta: Some(TransactionStatusMeta {
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            };
            chunker.ingest(msg, slot).unwrap();
        }

        chunker.shutdown().unwrap();

        let tx_dir = dir.path().join("chunks").join("tx");
        let entries: Vec<_> = fs::read_dir(&tx_dir).unwrap().collect();
        let zsts: Vec<_> = entries
            .iter()
            .filter(|e| {
                let path = e.as_ref().unwrap().path();
                path.extension().is_some_and(|ext| ext == "zst")
            })
            .collect();

        assert!(!zsts.is_empty(), "at least one .zst should exist");

        for zst_entry in &zsts {
            let zst_path = zst_entry.as_ref().unwrap().path();
            let stem = zst_path.file_stem().unwrap().to_str().unwrap();
            let idx_path = tx_dir.join(format!("{stem}.idx"));
            let meta_path = tx_dir.join(format!("{stem}.meta.json"));
            assert!(idx_path.exists(), "{stem}.idx should exist");
            assert!(meta_path.exists(), "{stem}.meta.json should exist");
        }

        let partials: Vec<_> = entries
            .iter()
            .filter(|e| {
                let path = e.as_ref().unwrap().path();
                path.to_string_lossy().ends_with(".partial")
            })
            .collect();
        assert!(partials.is_empty(), "no .partial files should remain");

        let meta_path = tx_dir.join(format!("{:012}-{:012}.meta.json", 0, 100));
        if meta_path.exists() {
            let meta: serde_json::Value =
                serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
            let dims = meta["index_dimensions"].as_array().unwrap();
            assert_eq!(dims.len(), 5);
            assert_eq!(dims[0], "program_id");
            assert_eq!(dims[1], "account_key");
            assert_eq!(dims[2], "signature");
            assert_eq!(dims[3], "vote_flag");
            assert_eq!(dims[4], "failed_flag");
        }
    }

    #[test]
    fn end_to_end_seal_and_index_query() {
        use crate::index::{DimValue, IdxHeader, IDX_MAGIC, IDX_VERSION};
        use yellowstone_grpc_proto::geyser::{
            subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateTransaction,
            SubscribeUpdateTransactionInfo,
        };
        use yellowstone_grpc_proto::solana::storage::confirmed_block::{
            Message, Transaction, TransactionStatusMeta,
        };

        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_cfg(10, 5, 10);
        let mut chunker = Chunker::new(Stream::Tx, cfg, dir.path()).unwrap();

        for slot in 0..30u64 {
            let mut msg = stamped_msg(1_000_000 + slot);
            let pubkey = vec![(slot % 3) as u8; 32];
            msg.inner = SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    transaction: Some(SubscribeUpdateTransactionInfo {
                        signature: vec![0u8; 64],
                        is_vote: false,
                        transaction: Some(Transaction {
                            message: Some(Message {
                                account_keys: vec![pubkey.clone()],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        meta: Some(TransactionStatusMeta {
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            };
            chunker.ingest(msg, slot).unwrap();
        }

        chunker.shutdown().unwrap();

        let tx_dir = dir.path().join("chunks").join("tx");
        let mut zst_files: Vec<_> = fs::read_dir(&tx_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "zst"))
            .collect();
        zst_files.sort_by_key(|e| e.file_name());
        assert_eq!(zst_files.len(), 3, "should have 3 sealed chunks");

        for zst_entry in &zst_files {
            let path = zst_entry.path();
            let stem = path.file_stem().unwrap().to_str().unwrap();
            let idx_path = tx_dir.join(format!("{stem}.idx"));
            let meta_path = tx_dir.join(format!("{stem}.meta.json"));
            assert!(idx_path.exists(), "{stem}.idx should exist");
            assert!(meta_path.exists(), "{stem}.meta.json should exist");
        }

        let partials: Vec<_> = fs::read_dir(&tx_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().to_string_lossy().ends_with(".partial"))
            .collect();
        assert!(partials.is_empty(), "no .partial files should remain");

        let chunk0_path = zst_files[0].path();
        let chunk0_stem = chunk0_path.file_stem().unwrap().to_str().unwrap();
        let idx_path = tx_dir.join(format!("{chunk0_stem}.idx"));
        let idx_bytes = fs::read(&idx_path).unwrap();

        assert_eq!(&idx_bytes[0..4], IDX_MAGIC);
        assert_eq!(idx_bytes[4], IDX_VERSION);
        let header_len = u32::from_le_bytes(idx_bytes[5..9].try_into().unwrap());
        let header: IdxHeader =
            rmp_serde::from_slice(&idx_bytes[9..9 + header_len as usize]).unwrap();

        assert_eq!(header.stream, "tx");
        assert_eq!(header.message_count, 10);

        let meta_path = tx_dir.join(format!("{chunk0_stem}.meta.json"));
        let meta: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
        let dims = meta["index_dimensions"].as_array().unwrap();
        assert_eq!(dims.len(), 5);
        assert_eq!(dims[0], "program_id");
        assert_eq!(dims[1], "account_key");
        assert_eq!(dims[2], "signature");
        assert_eq!(dims[3], "vote_flag");
        assert_eq!(dims[4], "failed_flag");

        let account_dim = header
            .dimensions
            .iter()
            .find(|d| d.name == "account_key")
            .unwrap();
        assert_eq!(
            account_dim.entries.len(),
            3,
            "account_key should have 3 distinct pubkeys"
        );

        let entry = account_dim
            .entries
            .iter()
            .find(|e| e.value == DimValue::Bytes(vec![0u8; 32]))
            .expect("should find entry for pubkey [0;32]");

        let body = &idx_bytes[9 + header_len as usize..];
        let bitmap = roaring::RoaringBitmap::deserialize_from(
            &body[entry.offset as usize..entry.offset as usize + entry.length as usize],
        )
        .unwrap();

        assert_eq!(bitmap.len(), 4);
        assert!(bitmap.contains(0));
        assert!(bitmap.contains(3));
        assert!(bitmap.contains(6));
        assert!(bitmap.contains(9));
    }

    /// QA Evidence Test — Phase 4 Final QA
    /// Verifies that .idx files are queryable by parsing the header,
    /// finding a dimension, deserializing a roaring bitmap, and
    /// asserting expected message offsets.
    #[test]
    fn qa_evidence_idx_queryable() {
        use crate::index::{DimValue, IdxHeader, IDX_MAGIC, IDX_VERSION};
        use std::io::Write;
        use yellowstone_grpc_proto::geyser::{
            subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateTransaction,
            SubscribeUpdateTransactionInfo,
        };
        use yellowstone_grpc_proto::solana::storage::confirmed_block::{
            Message, Transaction, TransactionStatusMeta,
        };

        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_cfg(100, 10, 10);
        let mut chunker = Chunker::new(Stream::Tx, cfg, dir.path()).unwrap();

        // Feed 10 synthetic tx messages with 3 known pubkeys
        for slot in 0..10u64 {
            let mut msg = stamped_msg(1_000_000 + slot);
            let pubkey = vec![(slot % 3) as u8; 32];
            msg.inner = SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    transaction: Some(SubscribeUpdateTransactionInfo {
                        signature: vec![0u8; 64],
                        is_vote: false,
                        transaction: Some(Transaction {
                            message: Some(Message {
                                account_keys: vec![pubkey],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        meta: Some(TransactionStatusMeta {
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            };
            chunker.ingest(msg, slot).unwrap();
        }

        chunker.shutdown().unwrap();

        let tx_dir = dir.path().join("chunks").join("tx");
        let idx_path = tx_dir.join(format!("{:012}-{:012}.idx", 0, 100));
        assert!(idx_path.exists(), ".idx file should exist after shutdown");

        let idx_bytes = fs::read(&idx_path).unwrap();

        // Parse header
        assert_eq!(&idx_bytes[0..4], IDX_MAGIC, "magic should be SIDX");
        assert_eq!(idx_bytes[4], IDX_VERSION, "version should be 1");
        let header_len = u32::from_le_bytes(idx_bytes[5..9].try_into().unwrap());
        let header: IdxHeader =
            rmp_serde::from_slice(&idx_bytes[9..9 + header_len as usize]).unwrap();

        assert_eq!(header.stream, "tx");
        assert_eq!(header.message_count, 10);

        // Find account_key dimension
        let account_dim = header
            .dimensions
            .iter()
            .find(|d| d.name == "account_key")
            .expect("account_key dimension should exist");
        assert_eq!(
            account_dim.entries.len(),
            3,
            "should have 3 distinct account_key values"
        );

        // Find entry for pubkey [0;32] and deserialize bitmap
        let entry = account_dim
            .entries
            .iter()
            .find(|e| e.value == DimValue::Bytes(vec![0u8; 32]))
            .expect("should find entry for pubkey [0;32]");

        let body = &idx_bytes[9 + header_len as usize..];
        let bitmap = roaring::RoaringBitmap::deserialize_from(
            &body[entry.offset as usize..entry.offset as usize + entry.length as usize],
        )
        .expect("bitmap should deserialize");

        // Assert expected offsets (messages 0, 3, 6, 9 have pubkey [0;32])
        assert_eq!(bitmap.len(), 4, "bitmap should contain 4 messages");
        assert!(bitmap.contains(0), "should contain message 0");
        assert!(bitmap.contains(3), "should contain message 3");
        assert!(bitmap.contains(6), "should contain message 6");
        assert!(bitmap.contains(9), "should contain message 9");

        // Verify meta.json.index_dimensions
        let meta_path = tx_dir.join(format!("{:012}-{:012}.meta.json", 0, 100));
        let meta: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
        let dims = meta["index_dimensions"].as_array().unwrap();
        assert_eq!(dims.len(), 5, "meta should list 5 index dimensions");
        assert_eq!(dims[0], "program_id");
        assert_eq!(dims[1], "account_key");
        assert_eq!(dims[2], "signature");
        assert_eq!(dims[3], "vote_flag");
        assert_eq!(dims[4], "failed_flag");

        // Write evidence (workspace root is one level up from crate root in tests)
        let evidence_dir = std::env::current_dir()
            .unwrap()
            .parent()
            .unwrap()
            .join(".sisyphus")
            .join("evidence")
            .join("phase4-final-qa");
        fs::create_dir_all(&evidence_dir).ok();
        let evidence_path = evidence_dir.join("idx-queryable.txt");
        let mut file = File::create(&evidence_path).unwrap();
        writeln!(file, "=== Phase 4 Final QA Evidence ===").unwrap();
        writeln!(file, "Test: qa_evidence_idx_queryable").unwrap();
        writeln!(file, "Date: {:?}", std::time::SystemTime::now()).unwrap();
        writeln!(file).unwrap();
        writeln!(file, "IDX file parsed: {}", idx_path.display()).unwrap();
        writeln!(file, "  Magic: SIDX  ✓").unwrap();
        writeln!(file, "  Version: {}  ✓", IDX_VERSION).unwrap();
        writeln!(file, "  Stream: {}  ✓", header.stream).unwrap();
        writeln!(file, "  Message count: {}  ✓", header.message_count).unwrap();
        writeln!(file, "  Dimensions: {}", header.dimensions.len()).unwrap();
        for dim in &header.dimensions {
            writeln!(file, "    - {} ({} entries)", dim.name, dim.entries.len()).unwrap();
        }
        writeln!(file).unwrap();
        writeln!(file, "account_key dimension query:").unwrap();
        writeln!(file, "  Distinct values: {}", account_dim.entries.len()).unwrap();
        writeln!(file, "  Bitmap for [0;32] pubkey:").unwrap();
        writeln!(
            file,
            "    Contains offsets: {:?}",
            bitmap.iter().collect::<Vec<_>>()
        )
        .unwrap();
        writeln!(file, "    Expected: [0, 3, 6, 9]").unwrap();
        writeln!(
            file,
            "    Match: {}",
            bitmap.iter().collect::<Vec<_>>() == vec![0, 3, 6, 9]
        )
        .unwrap();
        writeln!(file).unwrap();
        writeln!(file, "meta.json.index_dimensions:").unwrap();
        for dim in dims {
            writeln!(file, "  - {}", dim.as_str().unwrap()).unwrap();
        }
        writeln!(file).unwrap();
        writeln!(file, "VERDICT: IDX_QUERYABLE = YES").unwrap();
        writeln!(file, "VERDICT: META_DIMENSIONS = VALID").unwrap();
    }
}
