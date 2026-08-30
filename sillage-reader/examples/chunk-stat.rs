use std::env;

use anyhow::Result;
use sillage_common::Settings;
use sillage_reader::storage::{decode_chunk, ChunkCatalog};

fn main() -> Result<()> {
    let nvme_path = match env::var("SILLAGE_STORAGE__NVME_PATH") {
        Ok(p) => p,
        Err(_) => {
            let settings = Settings::load()?;
            settings.storage.nvme_path
        }
    };

    println!("nvme_path: {nvme_path}");

    let catalog = ChunkCatalog::scan(std::path::Path::new(&nvme_path));
    let summary = catalog.summary();

    if summary.per_stream.is_empty() {
        println!("No chunks found.");
        return Ok(());
    }

    let mut total_chunks = 0usize;
    let mut total_zst_bytes = 0u64;
    let mut newest_entry = None;

    for (stream, count, min_start, max_end) in &summary.per_stream {
        let stream_zst_bytes: u64 = catalog
            .chunks_in_range(*stream, 0, u64::MAX)
            .iter()
            .map(|e| e.zst_len)
            .sum();

        total_chunks += count;
        total_zst_bytes += stream_zst_bytes;

        println!(
            "stream={stream:?} chunks={count} slots=[{min_start:?}, {max_end:?}) zst_bytes={stream_zst_bytes}",
        );

        for entry in catalog.chunks_in_range(*stream, 0, u64::MAX) {
            if newest_entry
                .as_ref()
                .map(|e: &&sillage_reader::storage::ChunkEntry| {
                    entry.end_slot_exclusive > e.end_slot_exclusive
                })
                .unwrap_or(true)
            {
                newest_entry = Some(entry);
            }
        }
    }

    println!("total_chunks={total_chunks} total_zst_bytes={total_zst_bytes}");

    if let Some(entry) = newest_entry {
        println!(
            "\ndecoding newest chunk: stream={:?} end_slot={} path={}",
            entry.stream,
            entry.end_slot_exclusive,
            entry.zst_path.display()
        );

        let decoded = decode_chunk(&entry.zst_path)?;
        let frame_count = decoded.len();
        let decompressed_bytes = decoded.heap_bytes();
        let meta_message_count = entry.meta.message_count;

        println!("meta_message_count={meta_message_count} decoded_frames={frame_count} decompressed_bytes={decompressed_bytes}");

        if meta_message_count as usize == frame_count {
            println!("OK: message_count matches decoded frame count");
        } else {
            println!(
                "MISMATCH: message_count={meta_message_count} != decoded_frames={frame_count}"
            );
        }
    }

    Ok(())
}
