use std::collections::{BTreeMap, HashMap};

use anyhow::Context as _;
use roaring::RoaringBitmap;
pub use sillage_common::idx::*;
use sillage_common::Stream;
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateAccount,
    SubscribeUpdateBlockMeta, SubscribeUpdateTransaction,
};

pub(crate) struct IndexBuilder {
    pub(crate) stream: Stream,
    pub(crate) dims: HashMap<&'static str, BTreeMap<DimValue, RoaringBitmap>>,
    pub(crate) dim_types: HashMap<&'static str, DimValueType>,
    order: Vec<&'static str>,
}

impl IndexBuilder {
    pub(crate) fn for_stream(stream: Stream) -> Self {
        let mut dims = HashMap::new();
        let mut dim_types = HashMap::new();
        let mut order = Vec::new();

        match stream {
            Stream::Tx => {
                for (name, ty) in [
                    (DIM_PROGRAM_ID, DimValueType::Pubkey32),
                    (DIM_ACCOUNT_KEY, DimValueType::Pubkey32),
                    (DIM_SIGNATURE, DimValueType::Signature64),
                    (DIM_VOTE_FLAG, DimValueType::Bool),
                    (DIM_FAILED_FLAG, DimValueType::Bool),
                ] {
                    dims.insert(name, BTreeMap::new());
                    dim_types.insert(name, ty);
                    order.push(name);
                }
            }
            Stream::Acct => {
                for (name, ty) in [
                    (DIM_ACCOUNT_PUBKEY, DimValueType::Pubkey32),
                    (DIM_OWNER_PROGRAM, DimValueType::Pubkey32),
                ] {
                    dims.insert(name, BTreeMap::new());
                    dim_types.insert(name, ty);
                    order.push(name);
                }
            }
            Stream::Block => {
                for (name, ty) in [
                    (DIM_SLOT, DimValueType::U64),
                    (DIM_PARENT_SLOT, DimValueType::U64),
                ] {
                    dims.insert(name, BTreeMap::new());
                    dim_types.insert(name, ty);
                    order.push(name);
                }
            }
        }

        Self {
            stream,
            dims,
            dim_types,
            order,
        }
    }

    pub(crate) fn dimension_names(&self) -> Vec<String> {
        self.order.iter().map(|&s| s.to_string()).collect()
    }

    fn insert(&mut self, dim: &'static str, value: DimValue, offset: u32) {
        if let Some(map) = self.dims.get_mut(dim) {
            map.entry(value)
                .or_insert_with(RoaringBitmap::new)
                .insert(offset);
        }
    }

    fn observe_tx(&mut self, offset: u32, update: &SubscribeUpdateTransaction) {
        let Some(info) = update.transaction.as_ref() else {
            return;
        };

        if info.signature.len() == 64 {
            self.insert(
                DIM_SIGNATURE,
                DimValue::Bytes(info.signature.clone()),
                offset,
            );
        }

        if info.is_vote {
            self.insert(DIM_VOTE_FLAG, DimValue::Bool(true), offset);
        }

        if info.meta.as_ref().and_then(|m| m.err.as_ref()).is_some() {
            self.insert(DIM_FAILED_FLAG, DimValue::Bool(true), offset);
        }

        // resolved = static account_keys + loaded_writable + loaded_readonly
        let mut resolved: Vec<Vec<u8>> = Vec::new();
        if let Some(tx) = info.transaction.as_ref() {
            if let Some(msg) = tx.message.as_ref() {
                for key in &msg.account_keys {
                    resolved.push(key.clone());
                }
            }
        }
        if let Some(meta) = info.meta.as_ref() {
            for addr in &meta.loaded_writable_addresses {
                resolved.push(addr.clone());
            }
            for addr in &meta.loaded_readonly_addresses {
                resolved.push(addr.clone());
            }
        }

        for key in &resolved {
            if key.len() == 32 {
                self.insert(DIM_ACCOUNT_KEY, DimValue::Bytes(key.clone()), offset);
            }
        }

        // top-level instructions only (no CPI)
        if let Some(tx) = info.transaction.as_ref() {
            if let Some(msg) = tx.message.as_ref() {
                for ix in &msg.instructions {
                    let idx = ix.program_id_index as usize;
                    if let Some(key) = resolved.get(idx) {
                        if key.len() == 32 {
                            self.insert(DIM_PROGRAM_ID, DimValue::Bytes(key.clone()), offset);
                        }
                    }
                }
            }
        }
    }

    fn observe_acct(&mut self, offset: u32, update: &SubscribeUpdateAccount) {
        let Some(info) = update.account.as_ref() else {
            return;
        };

        if info.pubkey.len() == 32 {
            self.insert(
                DIM_ACCOUNT_PUBKEY,
                DimValue::Bytes(info.pubkey.clone()),
                offset,
            );
        }

        if info.owner.len() == 32 {
            self.insert(
                DIM_OWNER_PROGRAM,
                DimValue::Bytes(info.owner.clone()),
                offset,
            );
        }
    }

    fn observe_block(&mut self, offset: u32, update: &SubscribeUpdateBlockMeta) {
        self.insert(DIM_SLOT, DimValue::U64(update.slot), offset);
        self.insert(DIM_PARENT_SLOT, DimValue::U64(update.parent_slot), offset);
    }

    pub(crate) fn observe(&mut self, offset: u32, update: &SubscribeUpdate) {
        match (update.update_oneof.as_ref(), self.stream) {
            (Some(UpdateOneof::Transaction(t)), Stream::Tx) => self.observe_tx(offset, t),
            (Some(UpdateOneof::Account(a)), Stream::Acct) => self.observe_acct(offset, a),
            (Some(UpdateOneof::BlockMeta(b)), Stream::Block) => self.observe_block(offset, b),
            _ => {}
        }
    }

    pub(crate) fn serialize(
        &self,
        start_slot: u64,
        end_slot: u64,
        message_count: u64,
    ) -> anyhow::Result<Vec<u8>> {
        let mut body: Vec<u8> = Vec::new();
        let mut dimensions: Vec<DimensionHeader> = Vec::new();

        for dim_name in &self.order {
            let map = self.dims.get(dim_name).expect("dim registered");
            let value_type = *self.dim_types.get(dim_name).expect("dim type registered");
            let mut entries: Vec<DimEntryHeader> = Vec::new();

            for (value, bitmap) in map.iter() {
                let offset = body.len() as u64;
                bitmap
                    .serialize_into(&mut body)
                    .with_context(|| format!("serializing bitmap for {dim_name}"))?;
                let length = body.len() as u64 - offset;
                entries.push(DimEntryHeader {
                    value: value.clone(),
                    offset,
                    length,
                });
            }

            dimensions.push(DimensionHeader {
                name: dim_name.to_string(),
                value_type,
                entries,
            });
        }

        let header = IdxHeader {
            stream: self.stream.as_str().to_string(),
            start_slot,
            end_slot,
            message_count,
            dimensions,
        };

        let header_bytes =
            rmp_serde::to_vec_named(&header).context("msgpack-encoding index header")?;
        let header_len = header_bytes.len() as u32;

        let mut buffer = Vec::with_capacity(9 + header_bytes.len() + body.len());
        buffer.extend_from_slice(IDX_MAGIC);
        buffer.push(IDX_VERSION);
        buffer.extend_from_slice(&header_len.to_le_bytes());
        buffer.extend_from_slice(&header_bytes);
        buffer.extend_from_slice(&body);

        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateAccount,
        SubscribeUpdateAccountInfo, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
    };
    use yellowstone_grpc_proto::solana::storage::confirmed_block::{
        CompiledInstruction, Message, Transaction, TransactionError, TransactionStatusMeta,
    };

    fn pk(n: u8) -> Vec<u8> {
        vec![n; 32]
    }

    fn sig(n: u8) -> Vec<u8> {
        vec![n; 64]
    }

    fn make_tx_update(info: SubscribeUpdateTransactionInfo) -> SubscribeUpdate {
        SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                transaction: Some(info),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[test]
    fn tx_extractor_indexes_program_and_account_keys() {
        let mut builder = IndexBuilder::for_stream(Stream::Tx);
        let update = make_tx_update(SubscribeUpdateTransactionInfo {
            signature: sig(0xFF),
            is_vote: false,
            transaction: Some(Transaction {
                message: Some(Message {
                    account_keys: vec![pk(1), pk(2), pk(3)],
                    instructions: vec![
                        CompiledInstruction {
                            program_id_index: 0,
                            accounts: vec![],
                            data: vec![],
                        },
                        CompiledInstruction {
                            program_id_index: 2,
                            accounts: vec![],
                            data: vec![],
                        },
                    ],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            meta: Some(TransactionStatusMeta {
                err: None,
                ..Default::default()
            }),
            ..Default::default()
        });

        builder.observe(0, &update);

        let program_dim = builder.dims.get(DIM_PROGRAM_ID).unwrap();
        assert_eq!(program_dim.len(), 2);
        assert_eq!(
            program_dim.get(&DimValue::Bytes(pk(1))).unwrap(),
            &RoaringBitmap::from([0])
        );
        assert_eq!(
            program_dim.get(&DimValue::Bytes(pk(3))).unwrap(),
            &RoaringBitmap::from([0])
        );

        let account_dim = builder.dims.get(DIM_ACCOUNT_KEY).unwrap();
        assert_eq!(account_dim.len(), 3);
        for n in [1u8, 2, 3] {
            assert_eq!(
                account_dim.get(&DimValue::Bytes(pk(n))).unwrap(),
                &RoaringBitmap::from([0])
            );
        }
    }

    #[test]
    fn tx_resolves_program_id_via_loaded_addresses() {
        let mut builder = IndexBuilder::for_stream(Stream::Tx);
        let update = make_tx_update(SubscribeUpdateTransactionInfo {
            signature: sig(0xAA),
            is_vote: false,
            transaction: Some(Transaction {
                message: Some(Message {
                    account_keys: vec![pk(1)],
                    instructions: vec![CompiledInstruction {
                        program_id_index: 2,
                        accounts: vec![],
                        data: vec![],
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            meta: Some(TransactionStatusMeta {
                loaded_writable_addresses: vec![pk(2)],
                loaded_readonly_addresses: vec![pk(3)],
                ..Default::default()
            }),
            ..Default::default()
        });

        builder.observe(0, &update);

        let program_dim = builder.dims.get(DIM_PROGRAM_ID).unwrap();
        assert_eq!(program_dim.len(), 1);
        assert_eq!(
            program_dim.get(&DimValue::Bytes(pk(3))).unwrap(),
            &RoaringBitmap::from([0])
        );
    }

    #[test]
    fn tx_signature_indexed() {
        let mut builder = IndexBuilder::for_stream(Stream::Tx);
        let signature = sig(0xBB);
        let update = make_tx_update(SubscribeUpdateTransactionInfo {
            signature: signature.clone(),
            is_vote: false,
            transaction: Some(Transaction {
                message: Some(Message {
                    account_keys: vec![pk(1)],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            meta: Some(TransactionStatusMeta {
                ..Default::default()
            }),
            ..Default::default()
        });

        builder.observe(0, &update);

        let sig_dim = builder.dims.get(DIM_SIGNATURE).unwrap();
        assert_eq!(sig_dim.len(), 1);
        assert_eq!(
            sig_dim.get(&DimValue::Bytes(signature)).unwrap(),
            &RoaringBitmap::from([0])
        );
    }

    #[test]
    fn tx_vote_flag_only_present_when_true() {
        let mut builder = IndexBuilder::for_stream(Stream::Tx);

        let update_vote = make_tx_update(SubscribeUpdateTransactionInfo {
            signature: sig(1),
            is_vote: true,
            transaction: Some(Transaction {
                message: Some(Message {
                    account_keys: vec![pk(1)],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            meta: Some(TransactionStatusMeta {
                ..Default::default()
            }),
            ..Default::default()
        });
        builder.observe(0, &update_vote);

        assert_eq!(builder.dims.get(DIM_VOTE_FLAG).unwrap().len(), 1);
        assert_eq!(
            builder
                .dims
                .get(DIM_VOTE_FLAG)
                .unwrap()
                .get(&DimValue::Bool(true))
                .unwrap(),
            &RoaringBitmap::from([0])
        );

        let update_not_vote = make_tx_update(SubscribeUpdateTransactionInfo {
            signature: sig(2),
            is_vote: false,
            transaction: Some(Transaction {
                message: Some(Message {
                    account_keys: vec![pk(2)],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            meta: Some(TransactionStatusMeta {
                ..Default::default()
            }),
            ..Default::default()
        });
        builder.observe(1, &update_not_vote);

        assert_eq!(builder.dims.get(DIM_VOTE_FLAG).unwrap().len(), 1);
    }

    #[test]
    fn tx_failed_flag_only_present_when_err_is_some() {
        let mut builder = IndexBuilder::for_stream(Stream::Tx);

        let update_failed = make_tx_update(SubscribeUpdateTransactionInfo {
            signature: sig(1),
            is_vote: false,
            transaction: Some(Transaction {
                message: Some(Message {
                    account_keys: vec![pk(1)],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            meta: Some(TransactionStatusMeta {
                err: Some(TransactionError { err: vec![1] }),
                ..Default::default()
            }),
            ..Default::default()
        });
        builder.observe(0, &update_failed);

        assert_eq!(builder.dims.get(DIM_FAILED_FLAG).unwrap().len(), 1);
        assert_eq!(
            builder
                .dims
                .get(DIM_FAILED_FLAG)
                .unwrap()
                .get(&DimValue::Bool(true))
                .unwrap(),
            &RoaringBitmap::from([0])
        );

        let update_ok = make_tx_update(SubscribeUpdateTransactionInfo {
            signature: sig(2),
            is_vote: false,
            transaction: Some(Transaction {
                message: Some(Message {
                    account_keys: vec![pk(2)],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            meta: Some(TransactionStatusMeta {
                err: None,
                ..Default::default()
            }),
            ..Default::default()
        });
        builder.observe(1, &update_ok);

        assert_eq!(builder.dims.get(DIM_FAILED_FLAG).unwrap().len(), 1);
    }

    #[test]
    fn tx_skips_malformed_pubkey_lengths() {
        let mut builder = IndexBuilder::for_stream(Stream::Tx);
        let update = make_tx_update(SubscribeUpdateTransactionInfo {
            signature: sig(0xCC),
            is_vote: false,
            transaction: Some(Transaction {
                message: Some(Message {
                    account_keys: vec![vec![1, 2, 3]],
                    instructions: vec![CompiledInstruction {
                        program_id_index: 0,
                        accounts: vec![],
                        data: vec![],
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            meta: Some(TransactionStatusMeta {
                ..Default::default()
            }),
            ..Default::default()
        });

        builder.observe(0, &update);

        let account_dim = builder.dims.get(DIM_ACCOUNT_KEY).unwrap();
        assert!(account_dim.is_empty());
    }

    #[test]
    fn test_dimension_names_tx() {
        let builder = IndexBuilder::for_stream(Stream::Tx);
        let names = builder.dimension_names();
        assert_eq!(names.len(), 5);
        assert_eq!(
            names,
            vec![
                "program_id",
                "account_key",
                "signature",
                "vote_flag",
                "failed_flag",
            ]
        );
    }

    #[test]
    fn test_dimension_names_acct() {
        let builder = IndexBuilder::for_stream(Stream::Acct);
        let names = builder.dimension_names();
        assert_eq!(names.len(), 2);
        assert_eq!(names, vec!["account_pubkey", "owner_program"]);
    }

    #[test]
    fn test_dimension_names_block() {
        let builder = IndexBuilder::for_stream(Stream::Block);
        let names = builder.dimension_names();
        assert_eq!(names.len(), 2);
        assert_eq!(names, vec!["slot", "parent_slot"]);
    }

    #[test]
    fn block_extractor_handles_slot_and_parent_slot() {
        let mut builder = IndexBuilder::for_stream(Stream::Block);
        let update = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot: 100,
                parent_slot: 99,
                ..Default::default()
            })),
            ..Default::default()
        };

        builder.observe(0, &update);

        let slot_dim = builder.dims.get(DIM_SLOT).unwrap();
        assert_eq!(slot_dim.len(), 1);
        assert_eq!(
            slot_dim.get(&DimValue::U64(100)).unwrap(),
            &RoaringBitmap::from([0])
        );

        let parent_slot_dim = builder.dims.get(DIM_PARENT_SLOT).unwrap();
        assert_eq!(parent_slot_dim.len(), 1);
        assert_eq!(
            parent_slot_dim.get(&DimValue::U64(99)).unwrap(),
            &RoaringBitmap::from([0])
        );
    }

    #[test]
    fn acct_extractor_round_trip() {
        let mut builder = IndexBuilder::for_stream(Stream::Acct);
        let update = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: pk(9),
                    owner: pk(10),
                    ..Default::default()
                }),
                ..Default::default()
            })),
            ..Default::default()
        };

        builder.observe(42, &update);

        let pubkey_dim = builder.dims.get(DIM_ACCOUNT_PUBKEY).unwrap();
        assert_eq!(pubkey_dim.len(), 1);
        assert_eq!(
            pubkey_dim.get(&DimValue::Bytes(pk(9))).unwrap(),
            &RoaringBitmap::from([42])
        );

        let owner_dim = builder.dims.get(DIM_OWNER_PROGRAM).unwrap();
        assert_eq!(owner_dim.len(), 1);
        assert_eq!(
            owner_dim.get(&DimValue::Bytes(pk(10))).unwrap(),
            &RoaringBitmap::from([42])
        );
    }

    #[test]
    fn acct_skips_malformed_pubkey_lengths() {
        let mut builder = IndexBuilder::for_stream(Stream::Acct);
        let update = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: vec![1, 2, 3],
                    owner: pk(10),
                    ..Default::default()
                }),
                ..Default::default()
            })),
            ..Default::default()
        };

        builder.observe(0, &update);

        let pubkey_dim = builder.dims.get(DIM_ACCOUNT_PUBKEY).unwrap();
        assert!(pubkey_dim.is_empty());

        let owner_dim = builder.dims.get(DIM_OWNER_PROGRAM).unwrap();
        assert_eq!(owner_dim.len(), 1);
        assert_eq!(
            owner_dim.get(&DimValue::Bytes(pk(10))).unwrap(),
            &RoaringBitmap::from([0])
        );
    }

    #[test]
    fn serialize_round_trip() {
        let mut builder = IndexBuilder::for_stream(Stream::Tx);

        let update0 = make_tx_update(SubscribeUpdateTransactionInfo {
            signature: sig(0xAA),
            is_vote: false,
            transaction: Some(Transaction {
                message: Some(Message {
                    account_keys: vec![pk(1), pk(2)],
                    instructions: vec![CompiledInstruction {
                        program_id_index: 0,
                        accounts: vec![],
                        data: vec![],
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            meta: Some(TransactionStatusMeta {
                ..Default::default()
            }),
            ..Default::default()
        });
        builder.observe(0, &update0);

        let update1 = make_tx_update(SubscribeUpdateTransactionInfo {
            signature: sig(0xBB),
            is_vote: false,
            transaction: Some(Transaction {
                message: Some(Message {
                    account_keys: vec![pk(2), pk(3)],
                    instructions: vec![CompiledInstruction {
                        program_id_index: 1,
                        accounts: vec![],
                        data: vec![],
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            meta: Some(TransactionStatusMeta {
                ..Default::default()
            }),
            ..Default::default()
        });
        builder.observe(1, &update1);

        let bytes = builder.serialize(100, 200, 2).unwrap();

        assert_eq!(&bytes[0..4], b"SIDX");
        assert_eq!(bytes[4], 1);

        let header_len = u32::from_le_bytes(bytes[5..9].try_into().unwrap());

        let header: IdxHeader = rmp_serde::from_slice(&bytes[9..9 + header_len as usize]).unwrap();

        assert_eq!(header.stream, "tx");
        assert_eq!(header.start_slot, 100);
        assert_eq!(header.end_slot, 200);
        assert_eq!(header.message_count, 2);

        let account_dim = header
            .dimensions
            .iter()
            .find(|d| d.name == "account_key")
            .unwrap();

        let entry = account_dim
            .entries
            .iter()
            .find(|e| e.value == DimValue::Bytes(pk(2)))
            .unwrap();

        let body = &bytes[9 + header_len as usize..];
        let bitmap = RoaringBitmap::deserialize_from(
            &body[entry.offset as usize..entry.offset as usize + entry.length as usize],
        )
        .unwrap();

        assert_eq!(bitmap, RoaringBitmap::from([0, 1]));
    }

    #[test]
    fn empty_index_serializes_with_empty_entries() {
        let builder = IndexBuilder::for_stream(Stream::Tx);
        let bytes = builder.serialize(0, 0, 0).unwrap();

        assert_eq!(&bytes[0..4], b"SIDX");
        assert_eq!(bytes[4], 1);

        let header_len = u32::from_le_bytes(bytes[5..9].try_into().unwrap());
        let header: IdxHeader = rmp_serde::from_slice(&bytes[9..9 + header_len as usize]).unwrap();

        assert_eq!(header.dimensions.len(), 5);
        for dim in &header.dimensions {
            assert!(dim.entries.is_empty());
        }
    }

    #[test]
    fn serialize_orders_dimensions_in_registration_order() {
        let mut builder = IndexBuilder::for_stream(Stream::Tx);
        let update = make_tx_update(SubscribeUpdateTransactionInfo {
            signature: sig(0xDD),
            is_vote: false,
            transaction: Some(Transaction {
                message: Some(Message {
                    account_keys: vec![pk(1)],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            meta: Some(TransactionStatusMeta {
                ..Default::default()
            }),
            ..Default::default()
        });
        builder.observe(0, &update);

        let bytes = builder.serialize(0, 0, 1).unwrap();
        let header_len = u32::from_le_bytes(bytes[5..9].try_into().unwrap());
        let header: IdxHeader = rmp_serde::from_slice(&bytes[9..9 + header_len as usize]).unwrap();

        let dim_names: Vec<&str> = header.dimensions.iter().map(|d| d.name.as_str()).collect();
        let expected: Vec<String> = builder.dimension_names();
        assert_eq!(
            dim_names,
            expected.iter().map(|s| s.as_str()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn serialize_entries_are_deterministic() {
        let build_index = || {
            let mut builder = IndexBuilder::for_stream(Stream::Tx);
            let update0 = make_tx_update(SubscribeUpdateTransactionInfo {
                signature: sig(0xEE),
                is_vote: false,
                transaction: Some(Transaction {
                    message: Some(Message {
                        account_keys: vec![pk(1), pk(2)],
                        instructions: vec![CompiledInstruction {
                            program_id_index: 0,
                            accounts: vec![],
                            data: vec![],
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                meta: Some(TransactionStatusMeta {
                    ..Default::default()
                }),
                ..Default::default()
            });
            builder.observe(0, &update0);

            let update1 = make_tx_update(SubscribeUpdateTransactionInfo {
                signature: sig(0xFF),
                is_vote: true,
                transaction: Some(Transaction {
                    message: Some(Message {
                        account_keys: vec![pk(2), pk(3)],
                        instructions: vec![CompiledInstruction {
                            program_id_index: 1,
                            accounts: vec![],
                            data: vec![],
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                meta: Some(TransactionStatusMeta {
                    ..Default::default()
                }),
                ..Default::default()
            });
            builder.observe(1, &update1);

            builder.serialize(10, 20, 2).unwrap()
        };

        let bytes1 = build_index();
        let bytes2 = build_index();
        assert_eq!(bytes1, bytes2);
    }

    #[test]
    fn serialize_round_trip_repeated_pubkeys() {
        let mut builder = IndexBuilder::for_stream(Stream::Tx);

        for slot in 0..10u64 {
            let pubkey = vec![(slot % 3) as u8; 32];
            let update = make_tx_update(SubscribeUpdateTransactionInfo {
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
            });
            builder.observe(slot as u32, &update);
        }

        let bytes = builder.serialize(0, 10, 10).unwrap();

        assert_eq!(&bytes[0..4], IDX_MAGIC);
        assert_eq!(bytes[4], IDX_VERSION);
        let header_len = u32::from_le_bytes(bytes[5..9].try_into().unwrap());
        let header: IdxHeader = rmp_serde::from_slice(&bytes[9..9 + header_len as usize]).unwrap();

        assert_eq!(header.stream, "tx");
        assert_eq!(header.message_count, 10);

        let account_dim = header
            .dimensions
            .iter()
            .find(|d| d.name == "account_key")
            .unwrap();
        assert_eq!(account_dim.entries.len(), 3);

        let body = &bytes[9 + header_len as usize..];

        let entry_zero = account_dim
            .entries
            .iter()
            .find(|e| e.value == DimValue::Bytes(vec![0u8; 32]))
            .unwrap();
        let bitmap_zero = RoaringBitmap::deserialize_from(
            &body[entry_zero.offset as usize
                ..entry_zero.offset as usize + entry_zero.length as usize],
        )
        .unwrap();
        assert_eq!(bitmap_zero.len(), 4);
        assert!(bitmap_zero.contains(0));
        assert!(bitmap_zero.contains(3));
        assert!(bitmap_zero.contains(6));
        assert!(bitmap_zero.contains(9));
    }

    #[test]
    fn serialize_file_round_trip_repeated_pubkeys() {
        let mut builder = IndexBuilder::for_stream(Stream::Tx);

        for slot in 0..10u64 {
            let pubkey = vec![(slot % 3) as u8; 32];
            let update = make_tx_update(SubscribeUpdateTransactionInfo {
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
            });
            builder.observe(slot as u32, &update);
        }

        let bytes = builder.serialize(0, 10, 10).unwrap();

        let dir = tempfile::TempDir::new().unwrap();
        let idx_path = dir.path().join("test.idx");
        std::fs::write(&idx_path, &bytes).unwrap();

        let read_bytes = std::fs::read(&idx_path).unwrap();
        assert_eq!(bytes, read_bytes, "file round-trip should preserve bytes");

        let header_len = u32::from_le_bytes(read_bytes[5..9].try_into().unwrap());
        let header: IdxHeader =
            rmp_serde::from_slice(&read_bytes[9..9 + header_len as usize]).unwrap();

        let account_dim = header
            .dimensions
            .iter()
            .find(|d| d.name == "account_key")
            .unwrap();
        let body = &read_bytes[9 + header_len as usize..];

        let entry_zero = account_dim
            .entries
            .iter()
            .find(|e| e.value == DimValue::Bytes(vec![0u8; 32]))
            .unwrap();
        let bitmap_zero = RoaringBitmap::deserialize_from(
            &body[entry_zero.offset as usize
                ..entry_zero.offset as usize + entry_zero.length as usize],
        )
        .unwrap();
        assert_eq!(bitmap_zero.len(), 4);
        assert!(bitmap_zero.contains(0));
        assert!(bitmap_zero.contains(3));
        assert!(bitmap_zero.contains(6));
        assert!(bitmap_zero.contains(9));
    }
}
