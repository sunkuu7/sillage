use anyhow::Result;
use sillage_common::{config::GeyserConfig, Stream};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterTransactions, SubscribeUpdate,
};

use crate::stamp::Stamped;

/// Build a [`SubscribeRequest`] with exactly one filter entry for the given stream type
/// and the specified commitment level. If `from_slot` is `Some`, the request asks the
/// validator to resume from that slot (used on crash recovery; not honored by every
/// Yellowstone provider — silently falls back to tip if unsupported).
pub(crate) fn filter_for(
    stream: Stream,
    commitment: CommitmentLevel,
    from_slot: Option<u64>,
) -> SubscribeRequest {
    let mut req = SubscribeRequest::default();
    match stream {
        Stream::Tx => {
            req.transactions.insert(
                "all".to_string(),
                SubscribeRequestFilterTransactions::default(),
            );
        }
        Stream::Acct => {
            req.accounts
                .insert("all".to_string(), SubscribeRequestFilterAccounts::default());
        }
        Stream::Block => {
            req.blocks_meta.insert(
                "all".to_string(),
                SubscribeRequestFilterBlocksMeta::default(),
            );
        }
    }
    req.commitment = Some(commitment as i32);
    req.from_slot = from_slot;
    req
}

/// Map our config [`Commitment`](sillage_common::config::Commitment) to the
/// protobuf [`CommitmentLevel`].
pub(crate) fn proto_commitment(c: sillage_common::config::Commitment) -> CommitmentLevel {
    match c {
        sillage_common::config::Commitment::Confirmed => CommitmentLevel::Confirmed,
        sillage_common::config::Commitment::Finalized => CommitmentLevel::Finalized,
    }
}

/// Connect to the Geyser endpoint and subscribe to a single stream.
///
/// Returns a stream of [`Stamped<SubscribeUpdate>`] — each message is
/// timestamped with a monotonic receive marker.
pub(crate) async fn subscribe(
    config: &GeyserConfig,
    stream: Stream,
    from_slot: Option<u64>,
) -> Result<impl futures::Stream<Item = Result<Stamped<SubscribeUpdate>>>> {
    let commitment = proto_commitment(config.commitment);
    let request = filter_for(stream, commitment, from_slot);

    let mut builder = GeyserGrpcClient::build_from_shared(config.endpoint.clone())?;

    if config.endpoint.starts_with("https://") {
        builder = builder.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }

    if !config.x_token.is_empty() {
        builder = builder.x_token(Some(config.x_token.as_str()))?;
    }

    builder = builder.max_decoding_message_size(config.max_message_size_bytes);

    let mut client = builder.connect().await?;
    let geyser_stream = client.subscribe_once(request).await?;

    let adapted = futures::StreamExt::map(geyser_stream, move |result| {
        result.map(Stamped::new).map_err(|e| anyhow::anyhow!("{e}"))
    });

    Ok(adapted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::time::Instant;

    #[test]
    fn filter_for_tx_has_only_transactions_filter() {
        let req = filter_for(Stream::Tx, CommitmentLevel::Confirmed, None);
        assert!(
            !req.transactions.is_empty(),
            "transactions map should not be empty"
        );
        assert!(
            req.accounts.is_empty(),
            "accounts map should be empty for Tx stream"
        );
        assert!(
            req.blocks_meta.is_empty(),
            "blocks_meta map should be empty for Tx stream"
        );
        assert!(
            req.slots.is_empty(),
            "slots map should be empty for Tx stream"
        );
        assert!(
            req.blocks.is_empty(),
            "blocks map should be empty for Tx stream"
        );
    }

    #[test]
    fn filter_for_acct_has_only_accounts_filter() {
        let req = filter_for(Stream::Acct, CommitmentLevel::Confirmed, None);
        assert!(!req.accounts.is_empty(), "accounts map should not be empty");
        assert!(
            req.transactions.is_empty(),
            "transactions map should be empty for Acct stream"
        );
        assert!(
            req.blocks_meta.is_empty(),
            "blocks_meta map should be empty for Acct stream"
        );
        assert!(
            req.slots.is_empty(),
            "slots map should be empty for Acct stream"
        );
        assert!(
            req.blocks.is_empty(),
            "blocks map should be empty for Acct stream"
        );
    }

    #[test]
    fn filter_for_block_has_only_blocks_meta_filter() {
        let req = filter_for(Stream::Block, CommitmentLevel::Confirmed, None);
        assert!(
            !req.blocks_meta.is_empty(),
            "blocks_meta map should not be empty"
        );
        assert!(
            req.transactions.is_empty(),
            "transactions map should be empty for Block stream"
        );
        assert!(
            req.accounts.is_empty(),
            "accounts map should be empty for Block stream"
        );
        assert!(
            req.slots.is_empty(),
            "slots map should be empty for Block stream"
        );
        assert!(
            req.blocks.is_empty(),
            "blocks map should be empty for Block stream"
        );
    }

    #[test]
    fn filter_carries_commitment() {
        for c in [CommitmentLevel::Confirmed, CommitmentLevel::Finalized] {
            for stream in Stream::all() {
                let req = filter_for(stream, c, None);
                assert_eq!(
                    req.commitment,
                    Some(c as i32),
                    "commitment mismatch for stream={stream:?}, commitment={c:?}"
                );
            }
        }
    }

    #[test]
    fn filter_passes_from_slot_through_when_set() {
        let req = filter_for(Stream::Tx, CommitmentLevel::Confirmed, Some(421000000));
        assert_eq!(req.from_slot, Some(421000000));
    }

    #[test]
    fn filter_omits_from_slot_when_none() {
        let req = filter_for(Stream::Tx, CommitmentLevel::Confirmed, None);
        assert_eq!(req.from_slot, None);
    }

    #[test]
    fn proto_commitment_round_trip() {
        assert_eq!(
            proto_commitment(sillage_common::config::Commitment::Confirmed),
            CommitmentLevel::Confirmed
        );
        assert_eq!(
            proto_commitment(sillage_common::config::Commitment::Finalized),
            CommitmentLevel::Finalized
        );
    }

    #[test]
    fn stamps_each_message_with_monotonic_increase() {
        use futures::stream;

        let updates: Vec<Result<SubscribeUpdate, tonic::Status>> = vec![
            Ok(SubscribeUpdate::default()),
            Ok(SubscribeUpdate::default()),
            Ok(SubscribeUpdate::default()),
        ];

        let stamped = stream::iter(updates)
            .map(|result| result.map(Stamped::new).map_err(|e| anyhow::anyhow!("{e}")));

        let collected: Vec<Result<Stamped<SubscribeUpdate>>> =
            futures::executor::block_on_stream(stamped).collect();

        let mut prev_mono: Option<Instant> = None;
        for item in &collected {
            let stamped = item.as_ref().expect("should not be an error");
            if let Some(prev) = prev_mono {
                assert!(
                    stamped.recv.mono >= prev,
                    "monotonic timestamps must not decrease"
                );
            }
            prev_mono = Some(stamped.recv.mono);
        }
        assert_eq!(collected.len(), 3, "should have three stamped messages");
    }
}
