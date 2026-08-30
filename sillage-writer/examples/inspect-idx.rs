//! Dump per-dimension cardinality + total offsets from a `.idx` file.
//!
//! Usage: cargo run --release --example inspect-idx -- <path-to.idx>

use std::env;
use std::fs;
use std::io::Cursor;

use anyhow::{anyhow, bail, Result};
use roaring::RoaringBitmap;
use serde::Deserialize;

const IDX_MAGIC: &[u8; 4] = b"SIDX";
const IDX_VERSION: u8 = 1;

#[derive(Debug, Deserialize)]
struct IdxHeader {
    stream: String,
    start_slot: u64,
    end_slot: u64,
    message_count: u64,
    dimensions: Vec<DimensionHeader>,
}

#[derive(Debug, Deserialize)]
struct DimensionHeader {
    name: String,
    #[allow(dead_code)]
    value_type: serde::de::IgnoredAny,
    entries: Vec<DimEntryHeader>,
}

#[derive(Debug, Deserialize)]
struct DimEntryHeader {
    #[allow(dead_code)]
    value: serde::de::IgnoredAny,
    offset: u64,
    length: u64,
}

fn main() -> Result<()> {
    let path = env::args()
        .nth(1)
        .ok_or_else(|| anyhow!("usage: inspect-idx <path>"))?;
    let bytes = fs::read(&path)?;

    if bytes.len() < 9 || &bytes[0..4] != IDX_MAGIC {
        bail!("not a SIDX file: {}", path);
    }
    if bytes[4] != IDX_VERSION {
        bail!("unsupported version: {}", bytes[4]);
    }
    let header_len = u32::from_le_bytes(bytes[5..9].try_into()?) as usize;
    let header_end = 9 + header_len;
    if bytes.len() < header_end {
        bail!("truncated header");
    }
    let header: IdxHeader = rmp_serde::from_slice(&bytes[9..header_end])?;
    let body = &bytes[header_end..];

    println!("{}", path);
    println!(
        "  stream={}  slots={}..{}  message_count={}",
        header.stream, header.start_slot, header.end_slot, header.message_count
    );

    for dim in &header.dimensions {
        let distinct = dim.entries.len();
        let mut total: u64 = 0;
        for entry in &dim.entries {
            let start = entry.offset as usize;
            let end = start + entry.length as usize;
            if end > body.len() {
                bail!("entry out of bounds in dim {}", dim.name);
            }
            let bm = RoaringBitmap::deserialize_from(Cursor::new(&body[start..end]))?;
            total += bm.len();
        }
        println!(
            "  {:<16} distinct={:<8} total_offsets={}",
            dim.name, distinct, total
        );
    }

    Ok(())
}
