//! End-to-end Phase 4 demo on real data.
//!
//! Picks the newest tx chunk, picks the first indexed `account_key`, builds a
//! synthetic `SubscribeRequestFilterTransactions { account_include: [that_pk] }`,
//! runs `filter_tx` to get a bitmap, then uses `ChunkCache` to decode the chunk
//! and decodes the matched messages. Prints summary + verifies the resolved
//! account_keys of each matched message actually contain the requested pubkey.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use sillage_common::idx::{DimValue, DIM_ACCOUNT_KEY};
use sillage_common::Settings;
use sillage_common::Stream;
use sillage_reader::filter::filter_tx;
use sillage_reader::index::{parse_chunk_index, IndexCache};
use sillage_reader::storage::{CacheKey, ChunkCache, ChunkCatalog};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, SubscribeRequestFilterTransactions,
};

fn main() -> Result<()> {
    let nvme_path = match std::env::var("SILLAGE_STORAGE__NVME_PATH") {
        Ok(p) => p,
        Err(_) => Settings::load()?.storage.nvme_path,
    };
    println!("nvme_path: {nvme_path}");

    let catalog = ChunkCatalog::scan(Path::new(&nvme_path));
    let tx_chunks = catalog.chunks_in_range(Stream::Tx, 0, u64::MAX);
    let newest = tx_chunks
        .iter()
        .max_by_key(|e| e.end_slot_exclusive)
        .copied()
        .context("no tx chunks found in catalog")?;

    println!(
        "newest tx chunk: start={} end={} zst={} idx={}",
        newest.start_slot,
        newest.end_slot_exclusive,
        newest.zst_path.display(),
        newest.idx_path.display(),
    );

    // --- Step 1: pick an indexed account_key from the chunk's .idx ---
    let idx = parse_chunk_index(&newest.idx_path)?;
    let account_values = idx
        .dim_values(DIM_ACCOUNT_KEY)
        .context("chunk has no account_key dimension")?;

    let target_bytes = account_values
        .iter()
        .find_map(|v| match v {
            DimValue::Bytes(b) if b.len() == 32 => Some(b.clone()),
            _ => None,
        })
        .context("no 32-byte account_key entries in index")?;

    let target_b58 = bs58::encode(&target_bytes).into_string();
    let target_hex: String = target_bytes.iter().map(|b| format!("{b:02x}")).collect();
    println!("target account: {target_b58} (hex={target_hex})");

    // --- Step 2: build the synthetic filter ---
    let f = SubscribeRequestFilterTransactions {
        account_include: vec![target_b58.clone()],
        ..Default::default()
    };

    // --- Step 3: filter_tx → RoaringBitmap of matching ordinals ---
    let bitmap = filter_tx(&idx, &f);
    let matched = bitmap.len();
    let total = idx.message_count();
    println!(
        "filter_tx: matched {matched} / {total} messages (selectivity {:.4}%)",
        100.0 * matched as f64 / total as f64,
    );
    if matched == 0 {
        anyhow::bail!("bitmap is empty — index lied about this account_key");
    }

    // --- Step 4: decode the chunk and pull the matched messages ---
    let chunk_cache = Arc::new(ChunkCache::new(768 * 1024 * 1024));
    let _index_cache = Arc::new(IndexCache::new(128 * 1024 * 1024));

    let chunk_key = CacheKey {
        stream: Stream::Tx,
        start_slot: newest.start_slot,
    };
    let decoded = chunk_cache.get_or_decode(chunk_key, &newest.zst_path)?;
    println!(
        "decoded chunk: {} frames, ~{} MiB heap",
        decoded.len(),
        decoded.heap_bytes() / (1024 * 1024),
    );

    // --- Step 5: verify the first few matches actually contain the pubkey ---
    let mut verified = 0u64;
    let mut sample_slots = Vec::new();
    for ord in bitmap.iter().take(matched.min(10) as usize) {
        let msg = decoded.decode_message(ord)?;
        let tx_info = match msg.update_oneof {
            Some(UpdateOneof::Transaction(t)) => t.transaction,
            _ => continue,
        };
        let Some(info) = tx_info else { continue };
        sample_slots.push(ord);

        // Mirror writer's resolved-keys logic: static + loaded_writable + loaded_readonly
        let mut resolved: Vec<&Vec<u8>> = Vec::new();
        if let Some(tx) = info.transaction.as_ref() {
            if let Some(msg) = tx.message.as_ref() {
                for k in &msg.account_keys {
                    resolved.push(k);
                }
            }
        }
        if let Some(meta) = info.meta.as_ref() {
            for a in &meta.loaded_writable_addresses {
                resolved.push(a);
            }
            for a in &meta.loaded_readonly_addresses {
                resolved.push(a);
            }
        }
        if resolved
            .iter()
            .any(|k| k.as_slice() == target_bytes.as_slice())
        {
            verified += 1;
        } else {
            anyhow::bail!(
                "consistency check failed: ord={ord} did NOT contain the target pubkey in its resolved keys"
            );
        }
    }

    println!(
        "verified {verified}/{} sampled matches contain the target pubkey; first ordinals: {:?}",
        verified, sample_slots
    );

    // --- Step 6: cache-hit proof ---
    let chunk_key2 = CacheKey {
        stream: Stream::Tx,
        start_slot: newest.start_slot,
    };
    let decoded2 = chunk_cache.get_or_decode(chunk_key2, &newest.zst_path)?;
    if Arc::ptr_eq(&decoded, &decoded2) {
        println!("OK: second get_or_decode hit the cache (Arc::ptr_eq)");
    } else {
        anyhow::bail!("cache miss on second call — LRU is misbehaving");
    }

    println!("OK: full Phase 4 pipeline — index → filter → bitmap → decode → verify");
    Ok(())
}
