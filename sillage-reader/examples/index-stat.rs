use sillage_common::idx::{DimValue, DIM_PROGRAM_ID};
use sillage_reader::index::parse_chunk_index;
use std::path::Path;

fn main() {
    let nvme_path =
        std::env::var("SILLAGE_STORAGE__NVME_PATH").unwrap_or_else(|_| "/data".to_string());

    let tx_chunks_dir = Path::new(&nvme_path).join("chunks/tx");

    let mut idx_files: Vec<(u64, std::path::PathBuf)> = match std::fs::read_dir(&tx_chunks_dir) {
        Ok(entries) => entries
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.extension()? == "idx" {
                    let stem = path.file_stem()?.to_str()?;
                    let parts: Vec<&str> = stem.split('-').collect();
                    if parts.len() == 2 {
                        let start_slot = parts[0].parse::<u64>().ok()?;
                        Some((start_slot, path))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect(),
        Err(e) => {
            eprintln!(
                "Failed to read tx chunks dir {}: {}",
                tx_chunks_dir.display(),
                e
            );
            std::process::exit(1);
        }
    };

    if idx_files.is_empty() {
        println!("No tx chunks found");
        std::process::exit(1);
    }

    idx_files.sort_by_key(|(start_slot, _)| *start_slot);
    let newest_path = &idx_files.last().unwrap().1;

    let idx = match parse_chunk_index(newest_path) {
        Ok(idx) => idx,
        Err(e) => {
            eprintln!("Failed to parse index {}: {}", newest_path.display(), e);
            std::process::exit(1);
        }
    };

    let values = match idx.dim_values(DIM_PROGRAM_ID) {
        Some(v) => v,
        None => {
            println!("No program_id dimension");
            std::process::exit(1);
        }
    };

    if values.is_empty() {
        println!("No program_id entries");
        std::process::exit(1);
    }

    let first_value = values[0];
    let bitmap = match idx.bitmap_for(DIM_PROGRAM_ID, first_value) {
        Ok(Some(bm)) => bm,
        Ok(None) => {
            eprintln!("Bitmap not found for first program_id value");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Failed to get bitmap: {}", e);
            std::process::exit(1);
        }
    };

    let n = bitmap.len();
    let m = idx.message_count();

    let hex_string = match first_value {
        DimValue::Bytes(b) => b
            .iter()
            .map(|byte| format!("{:02x}", byte))
            .collect::<String>(),
        _ => {
            eprintln!("Unexpected DimValue variant for program_id");
            std::process::exit(1);
        }
    };

    println!(
        "OK: program_id={} bitmap_len={} message_count={} (N <= M, N > 0)",
        hex_string, n, m
    );

    assert!(n > 0, "bitmap_len must be > 0, got {}", n);
    assert!(
        n <= m,
        "bitmap_len ({}) must be <= message_count ({})",
        n,
        m
    );
}
