use crate::index::ChunkIndex;
use roaring::RoaringBitmap;
use sillage_common::idx::{
    DimValue, DIM_ACCOUNT_KEY, DIM_ACCOUNT_PUBKEY, DIM_FAILED_FLAG, DIM_OWNER_PROGRAM,
    DIM_SIGNATURE, DIM_VOTE_FLAG,
};
use yellowstone_grpc_proto::geyser::{
    SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterTransactions,
};

/// Build a bitmap containing all ordinals in `0..message_count`.
fn match_all(message_count: u64) -> RoaringBitmap {
    RoaringBitmap::from_sorted_iter(0..message_count as u32).unwrap()
}

/// Decode a base58 string to a 32-byte Vec<u8>, or None if invalid.
fn decode_pubkey(s: &str) -> Option<Vec<u8>> {
    bs58::decode(s).into_vec().ok().filter(|v| v.len() == 32)
}

/// Decode a base58 string to a 64-byte Vec<u8>, or None if invalid.
fn decode_signature(s: &str) -> Option<Vec<u8>> {
    bs58::decode(s).into_vec().ok().filter(|v| v.len() == 64)
}

/// Filter transactions: given a chunk index and a single subscription's
/// transaction filter, return the bitmap of message ordinals that match.
///
/// Predicate semantics:
/// - `vote`, `failed`: positive-only index; `Some(false)` is computed as
///   the complement against `match_all`.
/// - `signature`: strict — an undecodable / wrong-length base58 string
///   yields an empty result (unsatisfiable).
/// - `account_include` (OR): lenient — invalid base58 entries are
///   silently dropped; the union is taken over the decodable subset.
///   All-invalid drops the constraint entirely.
/// - `account_required` (AND): strict — any undecodable required pubkey
///   yields an empty result (a key we cannot identify cannot match any
///   real chain message).
/// - `account_exclude`: lenient — invalid base58 entries are dropped;
///   only decodable pubkeys' bitmaps are subtracted.
pub fn filter_tx(idx: &ChunkIndex, f: &SubscribeRequestFilterTransactions) -> RoaringBitmap {
    let mut result = match_all(idx.message_count());

    if let Some(vote) = f.vote {
        let vote_bitmap = idx
            .bitmap_for(DIM_VOTE_FLAG, &DimValue::Bool(true))
            .unwrap_or_default()
            .unwrap_or_default();
        if vote {
            result &= vote_bitmap;
        } else {
            let complement = &match_all(idx.message_count()) - &vote_bitmap;
            result &= complement;
        }
    }

    if let Some(failed) = f.failed {
        let failed_bitmap = idx
            .bitmap_for(DIM_FAILED_FLAG, &DimValue::Bool(true))
            .unwrap_or_default()
            .unwrap_or_default();
        if failed {
            result &= failed_bitmap;
        } else {
            let complement = &match_all(idx.message_count()) - &failed_bitmap;
            result &= complement;
        }
    }

    if let Some(ref sig_str) = f.signature {
        if let Some(sig_bytes) = decode_signature(sig_str) {
            let sig_bitmap = idx
                .bitmap_for(DIM_SIGNATURE, &DimValue::Bytes(sig_bytes))
                .unwrap_or_default()
                .unwrap_or_default();
            result &= sig_bitmap;
        } else {
            // Invalid signature decode → empty result
            return RoaringBitmap::new();
        }
    }

    if !f.account_include.is_empty() {
        let mut union = RoaringBitmap::new();
        let mut any_valid = false;
        for pk_str in &f.account_include {
            if let Some(pk_bytes) = decode_pubkey(pk_str) {
                any_valid = true;
                if let Ok(Some(bm)) = idx.bitmap_for(DIM_ACCOUNT_KEY, &DimValue::Bytes(pk_bytes)) {
                    union |= bm;
                }
            }
        }
        if any_valid {
            result &= union;
        }
    }

    if !f.account_required.is_empty() {
        for pk_str in &f.account_required {
            let Some(pk_bytes) = decode_pubkey(pk_str) else {
                return RoaringBitmap::new();
            };
            let bm = idx
                .bitmap_for(DIM_ACCOUNT_KEY, &DimValue::Bytes(pk_bytes))
                .unwrap_or_default()
                .unwrap_or_default();
            result &= bm;
        }
    }

    for pk_str in &f.account_exclude {
        if let Some(pk_bytes) = decode_pubkey(pk_str) {
            if let Ok(Some(bm)) = idx.bitmap_for(DIM_ACCOUNT_KEY, &DimValue::Bytes(pk_bytes)) {
                result -= bm;
            }
        }
    }

    result
}

/// Filter accounts: given a chunk index and a single subscription's
/// account filter, return the bitmap of message ordinals that match.
///
/// Only `account` and `owner` (OR-lists, invalid base58 silently
/// dropped) participate in the bitmap intersection here. The `filters`
/// field (memcmp / datasize / token_account_state / lamports) and
/// `nonempty_txn_signature` are not supported at the reader and are
/// **rejected upstream** by `sillage-reader::subscription::
/// parse_subscribe_request` with `InvalidArgument`; if you somehow get
/// a request past that gate, this function simply ignores those fields.
pub fn filter_acct(idx: &ChunkIndex, f: &SubscribeRequestFilterAccounts) -> RoaringBitmap {
    let mut result = match_all(idx.message_count());

    if !f.account.is_empty() {
        let mut union = RoaringBitmap::new();
        let mut any_valid = false;
        for pk_str in &f.account {
            if let Some(pk_bytes) = decode_pubkey(pk_str) {
                any_valid = true;
                if let Ok(Some(bm)) = idx.bitmap_for(DIM_ACCOUNT_PUBKEY, &DimValue::Bytes(pk_bytes))
                {
                    union |= bm;
                }
            }
        }
        if any_valid {
            result &= union;
        }
    }

    if !f.owner.is_empty() {
        let mut union = RoaringBitmap::new();
        let mut any_valid = false;
        for pk_str in &f.owner {
            if let Some(pk_bytes) = decode_pubkey(pk_str) {
                any_valid = true;
                if let Ok(Some(bm)) = idx.bitmap_for(DIM_OWNER_PROGRAM, &DimValue::Bytes(pk_bytes))
                {
                    union |= bm;
                }
            }
        }
        if any_valid {
            result &= union;
        }
    }

    // filters (memcmp/datasize) and nonempty_txn_signature are unindexed —
    // Phase 6 post-filters during scan.

    result
}

/// Filter blocks: `SubscribeRequestFilterBlocksMeta` has no indexable predicates.
pub fn filter_block(idx: &ChunkIndex, _f: &SubscribeRequestFilterBlocksMeta) -> RoaringBitmap {
    match_all(idx.message_count())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::parse_chunk_index;
    use sillage_common::idx::{
        DimEntryHeader, DimValueType, DimensionHeader, IDX_MAGIC, IDX_VERSION,
    };
    use tempfile::TempDir;

    /// Helper: build a complete `.idx` byte buffer from header fields + body.
    fn build_idx_bytes(
        stream: &str,
        message_count: u64,
        dimensions: Vec<DimensionHeader>,
        body: Vec<u8>,
    ) -> Vec<u8> {
        let header = sillage_common::idx::IdxHeader {
            stream: stream.to_string(),
            start_slot: 0,
            end_slot: 100,
            message_count,
            dimensions,
        };
        let header_bytes = rmp_serde::to_vec_named(&header).unwrap();
        let header_len = header_bytes.len() as u32;
        let mut buffer = Vec::with_capacity(9 + header_bytes.len() + body.len());
        buffer.extend_from_slice(IDX_MAGIC);
        buffer.push(IDX_VERSION);
        buffer.extend_from_slice(&header_len.to_le_bytes());
        buffer.extend_from_slice(&header_bytes);
        buffer.extend_from_slice(&body);
        buffer
    }

    /// Helper: write bytes to a temp file and return the path.
    fn write_temp_idx(dir: &TempDir, name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let path = dir.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }

    /// Helper: build a dimension with a single roaring bitmap entry.
    fn single_dim(
        dim_name: &str,
        value: DimValue,
        bitmap: &RoaringBitmap,
        body: &mut Vec<u8>,
    ) -> DimensionHeader {
        let offset = body.len() as u64;
        bitmap.serialize_into(&mut *body).unwrap();
        let length = body.len() as u64 - offset;
        DimensionHeader {
            name: dim_name.to_string(),
            value_type: DimValueType::Pubkey32,
            entries: vec![DimEntryHeader {
                value,
                offset,
                length,
            }],
        }
    }

    fn sig_bytes(seed: u8) -> Vec<u8> {
        vec![seed; 64]
    }

    fn sig_base58(seed: u8) -> String {
        bs58::encode(sig_bytes(seed)).into_string()
    }

    fn pk_bytes(seed: u8) -> Vec<u8> {
        vec![seed; 32]
    }

    fn pk_base58(seed: u8) -> String {
        bs58::encode(pk_bytes(seed)).into_string()
    }

    #[test]
    fn tx_empty_filter_matches_all() {
        let dir = TempDir::new().unwrap();
        let bytes = build_idx_bytes("tx", 5, vec![], vec![]);
        let path = write_temp_idx(&dir, "empty.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterTransactions::default();
        let result = filter_tx(&idx, &f);
        assert_eq!(result, RoaringBitmap::from_sorted_iter(0..5).unwrap());
    }

    #[test]
    fn tx_vote_true() {
        let dir = TempDir::new().unwrap();
        let mut body = Vec::new();
        let mut vote_bm = RoaringBitmap::new();
        vote_bm.insert(1);
        let dim = single_dim(DIM_VOTE_FLAG, DimValue::Bool(true), &vote_bm, &mut body);

        let dim = DimensionHeader {
            value_type: DimValueType::Bool,
            ..dim
        };

        let bytes = build_idx_bytes("tx", 5, vec![dim], body);
        let path = write_temp_idx(&dir, "vote.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterTransactions {
            vote: Some(true),
            ..Default::default()
        };
        let result = filter_tx(&idx, &f);
        assert_eq!(result, RoaringBitmap::from_sorted_iter([1]).unwrap());
    }

    #[test]
    fn tx_vote_false() {
        let dir = TempDir::new().unwrap();
        let mut body = Vec::new();
        let mut vote_bm = RoaringBitmap::new();
        vote_bm.insert(1);
        let dim = single_dim(DIM_VOTE_FLAG, DimValue::Bool(true), &vote_bm, &mut body);
        let dim = DimensionHeader {
            value_type: DimValueType::Bool,
            ..dim
        };

        let bytes = build_idx_bytes("tx", 5, vec![dim], body);
        let path = write_temp_idx(&dir, "vote_false.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterTransactions {
            vote: Some(false),
            ..Default::default()
        };
        let result = filter_tx(&idx, &f);
        let expected: RoaringBitmap = RoaringBitmap::from_sorted_iter([0, 2, 3, 4]).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn tx_failed_true() {
        let dir = TempDir::new().unwrap();
        let mut body = Vec::new();
        let mut failed_bm = RoaringBitmap::new();
        failed_bm.insert(2);
        let dim = single_dim(DIM_FAILED_FLAG, DimValue::Bool(true), &failed_bm, &mut body);
        let dim = DimensionHeader {
            value_type: DimValueType::Bool,
            ..dim
        };

        let bytes = build_idx_bytes("tx", 5, vec![dim], body);
        let path = write_temp_idx(&dir, "failed.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterTransactions {
            failed: Some(true),
            ..Default::default()
        };
        let result = filter_tx(&idx, &f);
        assert_eq!(result, RoaringBitmap::from_sorted_iter([2]).unwrap());
    }

    #[test]
    fn tx_signature_hit() {
        let dir = TempDir::new().unwrap();
        let mut body = Vec::new();
        let mut sig_bm = RoaringBitmap::new();
        sig_bm.insert(0);
        let sig_val = DimValue::Bytes(sig_bytes(0xAB));
        let dim = single_dim(DIM_SIGNATURE, sig_val.clone(), &sig_bm, &mut body);
        let dim = DimensionHeader {
            value_type: DimValueType::Signature64,
            ..dim
        };

        let bytes = build_idx_bytes("tx", 3, vec![dim], body);
        let path = write_temp_idx(&dir, "sig.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterTransactions {
            signature: Some(sig_base58(0xAB)),
            ..Default::default()
        };
        let result = filter_tx(&idx, &f);
        assert_eq!(result, RoaringBitmap::from_sorted_iter([0]).unwrap());
    }

    #[test]
    fn tx_signature_miss() {
        let dir = TempDir::new().unwrap();
        let mut body = Vec::new();
        let mut sig_bm = RoaringBitmap::new();
        sig_bm.insert(0);
        let sig_val = DimValue::Bytes(sig_bytes(0xAB));
        let dim = single_dim(DIM_SIGNATURE, sig_val.clone(), &sig_bm, &mut body);
        let dim = DimensionHeader {
            value_type: DimValueType::Signature64,
            ..dim
        };

        let bytes = build_idx_bytes("tx", 3, vec![dim], body);
        let path = write_temp_idx(&dir, "sig_miss.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterTransactions {
            signature: Some(sig_base58(0xCD)),
            ..Default::default()
        };
        let result = filter_tx(&idx, &f);
        assert!(
            result.is_empty(),
            "non-matching signature should yield empty bitmap"
        );
    }

    #[test]
    fn tx_signature_invalid_base58() {
        let dir = TempDir::new().unwrap();
        let bytes = build_idx_bytes("tx", 3, vec![], vec![]);
        let path = write_temp_idx(&dir, "sig_invalid.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterTransactions {
            signature: Some("not_valid_base58!!!".to_string()),
            ..Default::default()
        };
        let result = filter_tx(&idx, &f);
        assert!(
            result.is_empty(),
            "invalid base58 signature should yield empty bitmap"
        );
    }

    #[test]
    fn tx_account_include_single() {
        let dir = TempDir::new().unwrap();
        let mut body = Vec::new();
        let mut acct_bm = RoaringBitmap::new();
        acct_bm.insert(0);
        acct_bm.insert(1);
        let pk_val = DimValue::Bytes(pk_bytes(0x01));
        let dim = single_dim(DIM_ACCOUNT_KEY, pk_val.clone(), &acct_bm, &mut body);

        let bytes = build_idx_bytes("tx", 3, vec![dim], body);
        let path = write_temp_idx(&dir, "acct_include.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterTransactions {
            account_include: vec![pk_base58(0x01)],
            ..Default::default()
        };
        let result = filter_tx(&idx, &f);
        assert_eq!(result, RoaringBitmap::from_sorted_iter([0, 1]).unwrap());
    }

    #[test]
    fn tx_account_include_multiple_or() {
        let dir = TempDir::new().unwrap();
        let mut body = Vec::new();

        // pkA at {0}
        let mut bm_a = RoaringBitmap::new();
        bm_a.insert(0);
        let offset_a = body.len() as u64;
        bm_a.serialize_into(&mut body).unwrap();
        let len_a = body.len() as u64 - offset_a;

        // pkB at {1}
        let mut bm_b = RoaringBitmap::new();
        bm_b.insert(1);
        let offset_b = body.len() as u64;
        bm_b.serialize_into(&mut body).unwrap();
        let len_b = body.len() as u64 - offset_b;

        let dim = DimensionHeader {
            name: DIM_ACCOUNT_KEY.to_string(),
            value_type: DimValueType::Pubkey32,
            entries: vec![
                DimEntryHeader {
                    value: DimValue::Bytes(pk_bytes(0x01)),
                    offset: offset_a,
                    length: len_a,
                },
                DimEntryHeader {
                    value: DimValue::Bytes(pk_bytes(0x02)),
                    offset: offset_b,
                    length: len_b,
                },
            ],
        };

        let bytes = build_idx_bytes("tx", 3, vec![dim], body);
        let path = write_temp_idx(&dir, "acct_include_or.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterTransactions {
            account_include: vec![pk_base58(0x01), pk_base58(0x02)],
            ..Default::default()
        };
        let result = filter_tx(&idx, &f);
        assert_eq!(result, RoaringBitmap::from_sorted_iter([0, 1]).unwrap());
    }

    #[test]
    fn tx_account_required_and() {
        let dir = TempDir::new().unwrap();
        let mut body = Vec::new();

        // pkA at {0, 1}
        let mut bm_a = RoaringBitmap::new();
        bm_a.insert(0);
        bm_a.insert(1);
        let offset_a = body.len() as u64;
        bm_a.serialize_into(&mut body).unwrap();
        let len_a = body.len() as u64 - offset_a;

        // pkB at {1, 2}
        let mut bm_b = RoaringBitmap::new();
        bm_b.insert(1);
        bm_b.insert(2);
        let offset_b = body.len() as u64;
        bm_b.serialize_into(&mut body).unwrap();
        let len_b = body.len() as u64 - offset_b;

        let dim = DimensionHeader {
            name: DIM_ACCOUNT_KEY.to_string(),
            value_type: DimValueType::Pubkey32,
            entries: vec![
                DimEntryHeader {
                    value: DimValue::Bytes(pk_bytes(0x01)),
                    offset: offset_a,
                    length: len_a,
                },
                DimEntryHeader {
                    value: DimValue::Bytes(pk_bytes(0x02)),
                    offset: offset_b,
                    length: len_b,
                },
            ],
        };

        let bytes = build_idx_bytes("tx", 3, vec![dim], body);
        let path = write_temp_idx(&dir, "acct_required.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterTransactions {
            account_required: vec![pk_base58(0x01), pk_base58(0x02)],
            ..Default::default()
        };
        let result = filter_tx(&idx, &f);
        assert_eq!(result, RoaringBitmap::from_sorted_iter([1]).unwrap());
    }

    #[test]
    fn tx_account_required_invalid_base58_yields_empty() {
        let dir = TempDir::new().unwrap();
        let mut body = Vec::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(0);
        bm.insert(1);
        let pk_val = DimValue::Bytes(pk_bytes(0x01));
        let dim = single_dim(DIM_ACCOUNT_KEY, pk_val, &bm, &mut body);

        let bytes = build_idx_bytes("tx", 3, vec![dim], body);
        let path = write_temp_idx(&dir, "acct_required_invalid.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterTransactions {
            account_required: vec![pk_base58(0x01), "not_valid_base58!!!".to_string()],
            ..Default::default()
        };
        let result = filter_tx(&idx, &f);
        assert!(
            result.is_empty(),
            "undecodable required pubkey must yield empty bitmap (unsatisfiable)"
        );
    }

    #[test]
    fn tx_account_exclude() {
        let dir = TempDir::new().unwrap();
        let mut body = Vec::new();
        let mut acct_bm = RoaringBitmap::new();
        acct_bm.insert(0);
        acct_bm.insert(1);
        let pk_val = DimValue::Bytes(pk_bytes(0x01));
        let dim = single_dim(DIM_ACCOUNT_KEY, pk_val.clone(), &acct_bm, &mut body);

        let bytes = build_idx_bytes("tx", 4, vec![dim], body);
        let path = write_temp_idx(&dir, "acct_exclude.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterTransactions {
            account_exclude: vec![pk_base58(0x01)],
            ..Default::default()
        };
        let result = filter_tx(&idx, &f);
        let expected = RoaringBitmap::from_sorted_iter([2, 3]).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn tx_combined_vote_and_account() {
        let dir = TempDir::new().unwrap();
        let mut body = Vec::new();

        // vote_flag=true at {1, 2}
        let mut vote_bm = RoaringBitmap::new();
        vote_bm.insert(1);
        vote_bm.insert(2);
        let offset_vote = body.len() as u64;
        vote_bm.serialize_into(&mut body).unwrap();
        let len_vote = body.len() as u64 - offset_vote;

        // account_key pkA at {0, 1}
        let mut acct_bm = RoaringBitmap::new();
        acct_bm.insert(0);
        acct_bm.insert(1);
        let offset_acct = body.len() as u64;
        acct_bm.serialize_into(&mut body).unwrap();
        let len_acct = body.len() as u64 - offset_acct;

        let dims = vec![
            DimensionHeader {
                name: DIM_VOTE_FLAG.to_string(),
                value_type: DimValueType::Bool,
                entries: vec![DimEntryHeader {
                    value: DimValue::Bool(true),
                    offset: offset_vote,
                    length: len_vote,
                }],
            },
            DimensionHeader {
                name: DIM_ACCOUNT_KEY.to_string(),
                value_type: DimValueType::Pubkey32,
                entries: vec![DimEntryHeader {
                    value: DimValue::Bytes(pk_bytes(0x01)),
                    offset: offset_acct,
                    length: len_acct,
                }],
            },
        ];

        let bytes = build_idx_bytes("tx", 4, dims, body);
        let path = write_temp_idx(&dir, "combined.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterTransactions {
            vote: Some(true),
            account_include: vec![pk_base58(0x01)],
            ..Default::default()
        };
        let result = filter_tx(&idx, &f);
        // vote=true: {1,2}, account_include pkA: {0,1} → intersection: {1}
        assert_eq!(result, RoaringBitmap::from_sorted_iter([1]).unwrap());
    }

    #[test]
    fn acct_empty_filter_matches_all() {
        let dir = TempDir::new().unwrap();
        let bytes = build_idx_bytes("acct", 5, vec![], vec![]);
        let path = write_temp_idx(&dir, "acct_empty.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterAccounts::default();
        let result = filter_acct(&idx, &f);
        assert_eq!(result, RoaringBitmap::from_sorted_iter(0..5).unwrap());
    }

    #[test]
    fn acct_account_filter() {
        let dir = TempDir::new().unwrap();
        let mut body = Vec::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(0);
        let pk_val = DimValue::Bytes(pk_bytes(0xAA));
        let dim = single_dim(DIM_ACCOUNT_PUBKEY, pk_val.clone(), &bm, &mut body);

        let bytes = build_idx_bytes("acct", 3, vec![dim], body);
        let path = write_temp_idx(&dir, "acct_acct.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterAccounts {
            account: vec![pk_base58(0xAA)],
            ..Default::default()
        };
        let result = filter_acct(&idx, &f);
        assert_eq!(result, RoaringBitmap::from_sorted_iter([0]).unwrap());
    }

    #[test]
    fn acct_owner_filter() {
        let dir = TempDir::new().unwrap();
        let mut body = Vec::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        let pk_val = DimValue::Bytes(pk_bytes(0xBB));
        let dim = single_dim(DIM_OWNER_PROGRAM, pk_val.clone(), &bm, &mut body);

        let bytes = build_idx_bytes("acct", 3, vec![dim], body);
        let path = write_temp_idx(&dir, "acct_owner.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterAccounts {
            owner: vec![pk_base58(0xBB)],
            ..Default::default()
        };
        let result = filter_acct(&idx, &f);
        assert_eq!(result, RoaringBitmap::from_sorted_iter([1]).unwrap());
    }

    #[test]
    fn block_always_match_all() {
        let dir = TempDir::new().unwrap();
        let bytes = build_idx_bytes("block", 10, vec![], vec![]);
        let path = write_temp_idx(&dir, "block.idx", &bytes);
        let idx = parse_chunk_index(&path).unwrap();

        let f = SubscribeRequestFilterBlocksMeta::default();
        let result = filter_block(&idx, &f);
        assert_eq!(result, RoaringBitmap::from_sorted_iter(0..10).unwrap());
    }
}
