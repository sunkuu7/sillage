//! End-to-end replay engine demo on real local data.
//!
//! Picks the newest tx chunk, finds the first indexed `account_key`,
//! builds a `SubscriptionFilters` with that key, runs `plan_replay` then
//! `drive_replay`, and asserts slot-monotonic ordering + correct filter names.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use sillage_common::config::PacingConfig;
use sillage_common::idx::{DimValue, DIM_ACCOUNT_KEY};
use sillage_common::slot::extract_slot;
use sillage_common::Settings;
use sillage_common::Stream;
use sillage_reader::index::{parse_chunk_index, IndexCache};
use sillage_reader::pacing::Pacer;
use sillage_reader::replay::{drive_replay, plan_replay};
use sillage_reader::storage::ChunkCache;
use sillage_reader::subscription::SubscriptionFilters;
use yellowstone_grpc_proto::geyser::SubscribeRequestFilterTransactions;

fn main() -> Result<()> {
    let mut speed = 1000.0;
    let args: Vec<String> = std::env::args().collect();
    for i in 1..args.len() {
        if args[i] == "--speed" && i + 1 < args.len() {
            speed = args[i + 1].parse::<f64>().unwrap_or(1000.0);
        }
    }

    let nvme_path = match std::env::var("SILLAGE_STORAGE__NVME_PATH") {
        Ok(p) => p,
        Err(_) => Settings::load()?.storage.nvme_path,
    };
    println!("nvme_path: {nvme_path}");

    let catalog = sillage_reader::storage::ChunkCatalog::scan(Path::new(&nvme_path));
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
    println!("target account: {target_b58}");

    // --- Step 2: build SubscriptionFilters ---
    let filters = SubscriptionFilters {
        transactions: vec![(
            "demo".to_string(),
            SubscribeRequestFilterTransactions {
                account_include: vec![target_b58],
                ..Default::default()
            },
        )],
        accounts: vec![],
        blocks_meta: vec![],
        from_slot: None,
    };

    // --- Step 3: plan_replay ---
    let idx_cache = Arc::new(IndexCache::new(128 * 1024 * 1024));
    let chunk_cache = Arc::new(ChunkCache::new(768 * 1024 * 1024));

    let plan = plan_replay(&catalog, &filters, &idx_cache, None)?;
    println!(
        "plan: from_slot={} to_slot_exclusive={} tx_plans={} acct_plans={} block_plans={}",
        plan.from_slot,
        plan.to_slot_exclusive,
        plan.plans_per_stream[0].len(),
        plan.plans_per_stream[1].len(),
        plan.plans_per_stream[2].len(),
    );

    if plan.plans_per_stream[0].is_empty() {
        eprintln!("no tx plans in replay plan — nothing to replay");
        std::process::exit(1);
    }

    // --- Step 4: drive_replay on a channel ---
    let rt = tokio::runtime::Runtime::new()?;
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let shutdown = sillage_common::ShutdownSignal::new();
    let pacing_cfg = PacingConfig {
        enabled: true,
        speed_multiplier: speed,
        ..PacingConfig::default()
    };
    let mut pacer = Pacer::from_config(&pacing_cfg);

    let start = Instant::now();
    let stats = rt.block_on(drive_replay(plan, &chunk_cache, &mut pacer, tx, shutdown))?;
    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;

    println!(
        "paced: speed={speed} emitted={} wall_ms={elapsed_ms:.1} lag_max_ms={:.1}",
        stats.sent, stats.lag_max_ms
    );

    // --- Step 5: drain and collect up to 100 updates ---
    let mut updates: Vec<(u64, Vec<String>)> = Vec::new();
    while let Some(Ok(update)) = rx.blocking_recv() {
        let slot = extract_slot(&update).unwrap_or(0);
        updates.push((slot, update.filters));
        if updates.len() >= 100 {
            break;
        }
    }

    let n = updates.len();
    let first_slot = updates.first().map(|(s, _)| *s).unwrap_or(0);
    let last_slot = updates.last().map(|(s, _)| *s).unwrap_or(0);
    let filter_names: Vec<&str> = updates
        .iter()
        .flat_map(|(_, names)| names.iter().map(|n| n.as_str()))
        .collect();

    println!(
        "received {n} updates: first_slot={first_slot} last_slot={last_slot} filter_names={:?}",
        filter_names
    );

    // --- Step 6: assertions ---
    assert!(
        stats.sent >= 1,
        "expected at least 1 update, got {}",
        stats.sent
    );
    if speed >= 100.0 {
        assert!(
            elapsed < Duration::from_secs(5),
            "fast-forward should complete quickly: elapsed={elapsed:?}"
        );
    }

    let mut prev_slot = 0u64;
    for (i, (slot, _)) in updates.iter().enumerate() {
        assert!(
            *slot >= prev_slot || i == 0,
            "slot not monotonic at index {i}: slot={slot} prev={prev_slot}"
        );
        if i > 0 {
            assert!(
                *slot >= prev_slot,
                "slot not monotonic at index {i}: slot={slot} prev={prev_slot}"
            );
        }
        prev_slot = *slot;
    }

    for (i, (_, names)) in updates.iter().enumerate() {
        assert!(
            names == &["demo".to_string()],
            "update {i} has unexpected filter names: {:?}",
            names
        );
    }

    println!(
        "OK: replay engine — plan → drive → {n} updates, slots monotonic, all filters=[\"demo\"]"
    );
    Ok(())
}
