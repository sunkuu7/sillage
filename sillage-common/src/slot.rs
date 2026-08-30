use yellowstone_grpc_proto::geyser::{subscribe_update::UpdateOneof, SubscribeUpdate};

pub fn extract_slot(update: &SubscribeUpdate) -> Option<u64> {
    match update.update_oneof.as_ref() {
        Some(UpdateOneof::Transaction(t)) => Some(t.slot),
        Some(UpdateOneof::Account(a)) => Some(a.slot),
        Some(UpdateOneof::BlockMeta(b)) => Some(b.slot),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yellowstone_grpc_proto::geyser::{
        SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateBlockMeta,
        SubscribeUpdateTransaction,
    };

    #[test]
    fn test_extract_slot_tx() {
        let update = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                slot: 42,
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(extract_slot(&update), Some(42));
    }

    #[test]
    fn test_extract_slot_acct() {
        let update = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                slot: 42,
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(extract_slot(&update), Some(42));
    }

    #[test]
    fn test_extract_slot_block() {
        let update = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot: 42,
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(extract_slot(&update), Some(42));
    }

    #[test]
    fn test_extract_slot_unsupported_variant() {
        let update = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Ping(Default::default())),
            ..Default::default()
        };
        assert_eq!(extract_slot(&update), None);
    }

    #[test]
    fn test_extract_slot_empty_update_oneof() {
        let update = SubscribeUpdate {
            update_oneof: None,
            ..Default::default()
        };
        assert_eq!(extract_slot(&update), None);
    }
}
