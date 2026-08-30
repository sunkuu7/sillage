use prost::Message;
use tracing::warn;
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateAccount,
    SubscribeUpdateBlockMeta, SubscribeUpdateTransaction,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UpdateKind {
    Tx,
    Account,
    BlockMeta,
    Ping,
    Pong,
    Slot,
    Other,
}

impl std::fmt::Display for UpdateKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateKind::Tx => write!(f, "tx"),
            UpdateKind::Account => write!(f, "account"),
            UpdateKind::BlockMeta => write!(f, "block_meta"),
            UpdateKind::Ping => write!(f, "ping"),
            UpdateKind::Pong => write!(f, "pong"),
            UpdateKind::Slot => write!(f, "slot"),
            UpdateKind::Other => write!(f, "other"),
        }
    }
}

pub fn summarize(update: &SubscribeUpdate) -> (UpdateKind, u64, Option<u64>, String) {
    let bytes = update.encoded_len() as u64;

    match &update.update_oneof {
        Some(UpdateOneof::Transaction(tx)) => summarize_tx(tx, bytes),
        Some(UpdateOneof::Account(acct)) => summarize_account(acct, bytes),
        Some(UpdateOneof::BlockMeta(bm)) => summarize_block_meta(bm, bytes),
        Some(UpdateOneof::Ping(_)) => (UpdateKind::Ping, bytes, None, "ping".to_string()),
        Some(UpdateOneof::Pong(_)) => (UpdateKind::Pong, bytes, None, "pong".to_string()),
        Some(UpdateOneof::Slot(slot)) => {
            warn!(slot = slot.slot, "received unexpected slot update");
            (UpdateKind::Slot, bytes, Some(slot.slot), format!("slot={}", slot.slot))
        }
        Some(UpdateOneof::TransactionStatus(ts)) => {
            let sig = bs58::encode(&ts.signature).into_string();
            (
                UpdateKind::Other,
                bytes,
                Some(ts.slot),
                format!("transaction_status slot={} sig={sig}", ts.slot),
            )
        }
        Some(UpdateOneof::Block(_)) => (UpdateKind::Other, bytes, None, "block".to_string()),
        Some(UpdateOneof::Entry(_)) => (UpdateKind::Other, bytes, None, "entry".to_string()),
        None => (UpdateKind::Other, bytes, None, "empty".to_string()),
    }
}

fn summarize_tx(tx: &SubscribeUpdateTransaction, bytes: u64) -> (UpdateKind, u64, Option<u64>, String) {
    let slot = tx.slot;
    let sig = tx
        .transaction
        .as_ref()
        .map(|info| bs58::encode(&info.signature).into_string())
        .unwrap_or_else(|| "?".to_string());
    let summary = format!("tx slot={slot} sig={sig}");
    (UpdateKind::Tx, bytes, Some(slot), summary)
}

fn summarize_account(
    acct: &SubscribeUpdateAccount,
    bytes: u64,
) -> (UpdateKind, u64, Option<u64>, String) {
    let slot = acct.slot;
    let (pk, owner, data_len) = acct
        .account
        .as_ref()
        .map(|info| {
            let pk = bs58::encode(&info.pubkey).into_string();
            let owner = bs58::encode(&info.owner).into_string();
            let data_len = info.data.len();
            (pk, owner, data_len)
        })
        .unwrap_or_else(|| ("?".to_string(), "?".to_string(), 0));
    let summary = format!("account slot={slot} pk={pk} owner={owner} data_len={data_len}");
    (UpdateKind::Account, bytes, Some(slot), summary)
}

fn summarize_block_meta(
    bm: &SubscribeUpdateBlockMeta,
    bytes: u64,
) -> (UpdateKind, u64, Option<u64>, String) {
    let slot = bm.slot;
    let blockhash = &bm.blockhash;
    let summary = format!("block_meta slot={slot} blockhash={blockhash}");
    (UpdateKind::BlockMeta, bytes, Some(slot), summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use yellowstone_grpc_proto::geyser::{
        SubscribeUpdateBlockMeta, SubscribeUpdatePing, SubscribeUpdatePong, SubscribeUpdateSlot,
    };

    #[test]
    fn summarize_ping() {
        let update = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
            ..Default::default()
        };
        let (kind, _bytes, slot, summary) = summarize(&update);
        assert_eq!(kind, UpdateKind::Ping);
        assert_eq!(slot, None);
        assert_eq!(summary, "ping");
    }

    #[test]
    fn summarize_pong() {
        let update = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Pong(SubscribeUpdatePong { id: 42 })),
            ..Default::default()
        };
        let (kind, _bytes, slot, summary) = summarize(&update);
        assert_eq!(kind, UpdateKind::Pong);
        assert_eq!(slot, None);
        assert_eq!(summary, "pong");
    }

    #[test]
    fn summarize_slot_warns_and_returns_slot() {
        let update = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 123456789,
                ..Default::default()
            })),
            ..Default::default()
        };
        let (kind, _bytes, slot, summary) = summarize(&update);
        assert_eq!(kind, UpdateKind::Slot);
        assert_eq!(slot, Some(123456789));
        assert_eq!(summary, "slot=123456789");
    }

    #[test]
    fn summarize_block_meta() {
        let update = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot: 500,
                blockhash: "abc123".to_string(),
                ..Default::default()
            })),
            ..Default::default()
        };
        let (kind, _bytes, slot, summary) = summarize(&update);
        assert_eq!(kind, UpdateKind::BlockMeta);
        assert_eq!(slot, Some(500));
        assert_eq!(summary, "block_meta slot=500 blockhash=abc123");
    }

    #[test]
    fn summarize_empty_is_other() {
        let update = SubscribeUpdate::default();
        let (kind, _bytes, slot, summary) = summarize(&update);
        assert_eq!(kind, UpdateKind::Other);
        assert_eq!(slot, None);
        assert_eq!(summary, "empty");
    }
}
