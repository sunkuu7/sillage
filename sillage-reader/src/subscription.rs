use sillage_common::Stream;
use tonic::Status;
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterTransactions,
};

#[derive(Clone)]
pub struct SubscriptionFilters {
    pub transactions: Vec<(String, SubscribeRequestFilterTransactions)>,
    pub accounts: Vec<(String, SubscribeRequestFilterAccounts)>,
    pub blocks_meta: Vec<(String, SubscribeRequestFilterBlocksMeta)>,
    pub from_slot: Option<u64>,
}

impl std::fmt::Debug for SubscriptionFilters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionFilters")
            .field("transactions", &self.transactions.len())
            .field("accounts", &self.accounts.len())
            .field("blocks_meta", &self.blocks_meta.len())
            .field("from_slot", &self.from_slot)
            .finish()
    }
}

pub fn parse_subscribe_request(req: SubscribeRequest) -> Result<SubscriptionFilters, Status> {
    // `slots` is tolerated rather than rejected: the stock `GeyserGrpcClient`
    // wrapper injects one unconditionally, so rejecting it locks out every
    // off-the-shelf client. We do not serve slot updates, and a request that
    // asks for *only* slots still fails the "at least one filter" check below
    // with a message naming what is unsupported.
    if !req.slots.is_empty() {
        tracing::warn!(
            count = req.slots.len(),
            "ignoring slots subscription: slot updates are not served"
        );
    }
    if !req.blocks.is_empty() {
        return Err(Status::invalid_argument(
            "blocks subscription is not supported",
        ));
    }
    if !req.entry.is_empty() {
        return Err(Status::invalid_argument(
            "entry subscription is not supported",
        ));
    }
    if !req.transactions_status.is_empty() {
        return Err(Status::invalid_argument(
            "transactions_status subscription is not supported",
        ));
    }
    if !req.accounts_data_slice.is_empty() {
        return Err(Status::invalid_argument(
            "accounts_data_slice is not supported",
        ));
    }
    // Commitment is accepted and ignored. Archived chunks were captured at
    // whatever commitment the writer subscribed with; the reader cannot
    // re-derive a different one after the fact, and every standard Yellowstone
    // client sets this field. Rejecting it would block them all.
    if let Some(commitment) = req.commitment {
        tracing::debug!(
            commitment,
            "ignoring requested commitment: replay serves the level the writer captured"
        );
    }

    let from_slot = req.from_slot;

    let transactions: Vec<_> = req.transactions.into_iter().collect();
    let accounts: Vec<_> = req.accounts.into_iter().collect();
    let blocks_meta: Vec<_> = req.blocks_meta.into_iter().collect();

    if transactions.is_empty() && accounts.is_empty() && blocks_meta.is_empty() {
        return Err(Status::invalid_argument(
            "subscription must specify at least one of transactions, accounts, or blocks_meta \
             (slots and entry updates are not served)",
        ));
    }

    for (_name, filter) in &accounts {
        if !filter.filters.is_empty() {
            return Err(Status::invalid_argument(
                "account memcmp/datasize filters are not yet supported",
            ));
        }
        if filter.nonempty_txn_signature.is_some() {
            return Err(Status::invalid_argument(
                "nonempty_txn_signature filter is not yet supported",
            ));
        }
    }

    Ok(SubscriptionFilters {
        transactions,
        accounts,
        blocks_meta,
        from_slot,
    })
}

impl SubscriptionFilters {
    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty() && self.accounts.is_empty() && self.blocks_meta.is_empty()
    }

    pub fn has_stream(&self, stream: Stream) -> bool {
        match stream {
            Stream::Tx => !self.transactions.is_empty(),
            Stream::Acct => !self.accounts.is_empty(),
            Stream::Block => !self.blocks_meta.is_empty(),
        }
    }

    pub fn summary(&self) -> String {
        format!(
            "tx={} acct={} block={} from_slot={:?}",
            self.transactions.len(),
            self.accounts.len(),
            self.blocks_meta.len(),
            self.from_slot,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yellowstone_grpc_proto::geyser::{
        SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
        SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterTransactions,
    };

    #[test]
    fn test_parse_empty_rejected() {
        let req = SubscribeRequest::default();
        let err = parse_subscribe_request(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("at least one of transactions"));
    }

    #[test]
    fn test_parse_tx_only() {
        let mut req = SubscribeRequest::default();
        req.transactions.insert(
            "tx1".to_string(),
            SubscribeRequestFilterTransactions::default(),
        );
        let parsed = parse_subscribe_request(req).unwrap();
        assert_eq!(parsed.transactions.len(), 1);
        assert!(parsed.accounts.is_empty());
        assert!(parsed.blocks_meta.is_empty());
        assert!(parsed.has_stream(Stream::Tx));
        assert!(!parsed.has_stream(Stream::Acct));
        assert!(!parsed.has_stream(Stream::Block));
    }

    #[test]
    fn test_parse_acct_only() {
        let mut req = SubscribeRequest::default();
        req.accounts.insert(
            "acct1".to_string(),
            SubscribeRequestFilterAccounts::default(),
        );
        let parsed = parse_subscribe_request(req).unwrap();
        assert!(parsed.transactions.is_empty());
        assert_eq!(parsed.accounts.len(), 1);
        assert!(parsed.blocks_meta.is_empty());
        assert!(!parsed.has_stream(Stream::Tx));
        assert!(parsed.has_stream(Stream::Acct));
        assert!(!parsed.has_stream(Stream::Block));
    }

    #[test]
    fn test_parse_block_only() {
        let mut req = SubscribeRequest::default();
        req.blocks_meta.insert(
            "block1".to_string(),
            SubscribeRequestFilterBlocksMeta::default(),
        );
        let parsed = parse_subscribe_request(req).unwrap();
        assert!(parsed.transactions.is_empty());
        assert!(parsed.accounts.is_empty());
        assert_eq!(parsed.blocks_meta.len(), 1);
        assert!(!parsed.has_stream(Stream::Tx));
        assert!(!parsed.has_stream(Stream::Acct));
        assert!(parsed.has_stream(Stream::Block));
    }

    #[test]
    fn test_parse_multiple_streams() {
        let mut req = SubscribeRequest::default();
        req.transactions.insert(
            "tx1".to_string(),
            SubscribeRequestFilterTransactions::default(),
        );
        req.accounts.insert(
            "acct1".to_string(),
            SubscribeRequestFilterAccounts::default(),
        );
        req.blocks_meta.insert(
            "block1".to_string(),
            SubscribeRequestFilterBlocksMeta::default(),
        );
        let parsed = parse_subscribe_request(req).unwrap();
        assert_eq!(parsed.transactions.len(), 1);
        assert_eq!(parsed.accounts.len(), 1);
        assert_eq!(parsed.blocks_meta.len(), 1);
        assert!(parsed.has_stream(Stream::Tx));
        assert!(parsed.has_stream(Stream::Acct));
        assert!(parsed.has_stream(Stream::Block));
        assert!(!parsed.is_empty());
    }

    /// The stock `GeyserGrpcClient` injects a slots filter on every
    /// subscription. Tolerating it is what lets off-the-shelf clients connect.
    #[test]
    fn test_parse_ignores_injected_slots_alongside_real_filters() {
        let mut req = SubscribeRequest::default();
        req.slots.insert("slot1".to_string(), Default::default());
        req.transactions.insert(
            "tx1".to_string(),
            SubscribeRequestFilterTransactions::default(),
        );
        let parsed =
            parse_subscribe_request(req).expect("injected slots must not fail the request");
        assert_eq!(parsed.transactions.len(), 1);
        assert!(!parsed.has_stream(Stream::Acct));
    }

    /// Tolerating slots must not become silent degradation: a request asking
    /// for nothing but slots still fails, and says why.
    #[test]
    fn test_parse_rejects_slots_only_subscription() {
        let mut req = SubscribeRequest::default();
        req.slots.insert("slot1".to_string(), Default::default());
        let err = parse_subscribe_request(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("slots"));
    }

    #[test]
    fn test_parse_rejects_blocks() {
        let mut req = SubscribeRequest::default();
        req.blocks.insert("block1".to_string(), Default::default());
        let err = parse_subscribe_request(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("blocks subscription"));
    }

    #[test]
    fn test_parse_rejects_entry() {
        let mut req = SubscribeRequest::default();
        req.entry.insert("entry1".to_string(), Default::default());
        let err = parse_subscribe_request(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("entry subscription"));
    }

    /// Every standard Yellowstone client sets commitment. The reader serves
    /// whatever the writer captured, so the field is accepted and ignored
    /// rather than rejected.
    #[test]
    fn test_parse_accepts_and_ignores_commitment() {
        for commitment in [0i32, 1, 2] {
            let mut req = SubscribeRequest {
                commitment: Some(commitment),
                ..Default::default()
            };
            req.transactions.insert(
                "tx1".to_string(),
                SubscribeRequestFilterTransactions::default(),
            );
            let parsed = parse_subscribe_request(req)
                .unwrap_or_else(|e| panic!("commitment {commitment} should be accepted: {e}"));
            assert_eq!(parsed.transactions.len(), 1);
        }
    }

    #[test]
    fn test_parse_rejects_memcmp() {
        let mut req = SubscribeRequest::default();
        let mut acct_filter = SubscribeRequestFilterAccounts::default();
        acct_filter
            .filters
            .push(SubscribeRequestFilterAccountsFilter::default());
        req.accounts.insert("acct1".to_string(), acct_filter);
        let err = parse_subscribe_request(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("memcmp/datasize"));
    }

    #[test]
    fn test_parse_rejects_nonempty_txn_signature() {
        let mut req = SubscribeRequest::default();
        let acct_filter = SubscribeRequestFilterAccounts {
            nonempty_txn_signature: Some(true),
            ..Default::default()
        };
        req.accounts.insert("acct1".to_string(), acct_filter);
        let err = parse_subscribe_request(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("nonempty_txn_signature"));
    }

    #[test]
    fn test_parse_rejects_transactions_status() {
        let mut req = SubscribeRequest::default();
        req.transactions_status
            .insert("ts1".to_string(), Default::default());
        let err = parse_subscribe_request(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("transactions_status"));
    }

    #[test]
    fn test_parse_rejects_accounts_data_slice() {
        use yellowstone_grpc_proto::geyser::SubscribeRequestAccountsDataSlice;
        let mut req = SubscribeRequest::default();
        req.accounts_data_slice
            .push(SubscribeRequestAccountsDataSlice {
                offset: 0,
                length: 32,
            });
        req.transactions.insert(
            "tx1".to_string(),
            SubscribeRequestFilterTransactions::default(),
        );
        let err = parse_subscribe_request(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("accounts_data_slice"));
    }

    #[test]
    fn test_parse_accepts_empty_account_filters() {
        let mut req = SubscribeRequest::default();
        req.accounts.insert(
            "acct1".to_string(),
            SubscribeRequestFilterAccounts::default(),
        );
        let parsed = parse_subscribe_request(req).unwrap();
        assert_eq!(parsed.accounts.len(), 1);
    }

    #[test]
    fn test_parse_from_slot_default_is_none() {
        let mut req = SubscribeRequest::default();
        req.transactions.insert(
            "tx1".to_string(),
            SubscribeRequestFilterTransactions::default(),
        );
        let parsed = parse_subscribe_request(req).unwrap();
        assert_eq!(parsed.from_slot, None);
    }

    #[test]
    fn test_parse_from_slot_explicit() {
        let mut req = SubscribeRequest {
            from_slot: Some(500_000),
            ..Default::default()
        };
        req.transactions.insert(
            "tx1".to_string(),
            SubscribeRequestFilterTransactions::default(),
        );
        let parsed = parse_subscribe_request(req).unwrap();
        assert_eq!(parsed.from_slot, Some(500_000));
    }
}
