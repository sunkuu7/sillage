use anyhow::{bail, Result};
use clap::Parser;
use tokio::time::{interval, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{ClientTlsConfig, Endpoint};
use tonic::{Request, Status};
use tracing::{info, Level};
use yellowstone_grpc_proto::geyser::geyser_client::GeyserClient;
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterTransactions, SubscribeRequestPing,
};

mod decode;
mod stats;

#[derive(Parser, Debug)]
#[command(name = "sillage-probe")]
#[command(about = "Yellowstone gRPC test client")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:10099")]
    endpoint: String,

    #[arg(long, env = "SILLAGE_PROBE_TOKEN")]
    x_token: Option<String>,

    #[arg(long)]
    speed: Option<f64>,

    #[arg(long)]
    tx_account: Vec<String>,

    #[arg(long)]
    tx_vote: Option<bool>,

    #[arg(long)]
    tx_failed: Option<bool>,

    #[arg(long)]
    account: Vec<String>,

    #[arg(long)]
    account_owner: Vec<String>,

    #[arg(long)]
    blocks_meta: bool,

    #[arg(long)]
    from_slot: Option<u64>,

    #[arg(long)]
    max_messages: Option<usize>,

    #[arg(long)]
    duration: Option<u64>,

    #[arg(long, default_value = "1000")]
    progress_every: usize,

    #[arg(long)]
    print_updates: bool,

    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let log_level = if cli.verbose { Level::DEBUG } else { Level::INFO };
    tracing_subscriber::fmt().with_max_level(log_level).init();

    let req = build_subscribe_request(&cli)?;

    info!("endpoint={}", cli.endpoint);
    if let Some(token) = &cli.x_token {
        info!("x-token={}", mask_token(token));
    }
    if let Some(speed) = cli.speed {
        info!("speed={speed}");
    }
    if let Some(max) = cli.max_messages {
        info!("max_messages={max}");
    }
    if let Some(secs) = cli.duration {
        info!("duration={secs}s");
    }
    info!("progress_every={}", cli.progress_every);
    info!("print_updates={}", cli.print_updates);

    info!("SubscribeRequest: {:?}", req);

    let channel = build_channel(&cli.endpoint).await?;

    // We use the raw proto-level GeyserClient with a custom interceptor instead of the
    // high-level GeyserGrpcClient wrapper because the wrapper silently injects a `slots`
    // subscription into every SubscribeRequest, which the sillage-reader rejects.
    let interceptor = build_interceptor(&cli);
    let mut client = GeyserClient::with_interceptor(channel, interceptor);

    let (tx, rx) = tokio::sync::mpsc::channel::<SubscribeRequest>(1);
    let request_stream = ReceiverStream::new(rx);

    tx.send(req).await?;

    let mut stream = client.subscribe(request_stream).await?.into_inner();

    let mut stats = stats::Stats::new();
    let mut ping_interval = interval(Duration::from_secs(30));

    loop {
        tokio::select! {
            biased;

            maybe_update = stream.message() => {
                match maybe_update {
                    Ok(Some(update)) => {
                        let (kind, bytes, slot, summary) = decode::summarize(&update);
                        stats.observe(kind, bytes, slot);

                        if cli.print_updates {
                            println!("{summary}");
                        }

                        if stats.total_msgs % cli.progress_every as u64 == 0 {
                            info!("received {} messages", stats.total_msgs);
                        }

                        if let Some(max) = cli.max_messages {
                            if stats.total_msgs >= max as u64 {
                                info!("max_messages={max} reached, stopping");
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        info!("stream ended by server");
                        break;
                    }
                    Err(e) => {
                        eprintln!("stream error: {} - {}", e.code(), e.message());
                        std::process::exit(1);
                    }
                }
            }

            _ = ping_interval.tick() => {
                let ping_req = SubscribeRequest {
                    ping: Some(SubscribeRequestPing { id: 0 }),
                    ..Default::default()
                };
                if tx.send(ping_req).await.is_err() {
                    info!("ping send failed (stream closed)");
                    break;
                }
            }

            _ = tokio::signal::ctrl_c() => {
                info!("Ctrl-C received, stopping");
                break;
            }

            _ = async {
                if let Some(secs) = cli.duration {
                    tokio::time::sleep(Duration::from_secs(secs)).await;
                } else {
                    std::future::pending().await
                }
            } => {
                if cli.duration.is_some() {
                    info!("duration reached, stopping");
                }
                break;
            }
        }
    }

    println!("{}", stats.render(cli.speed));
    Ok(())
}

async fn build_channel(endpoint: &str) -> Result<tonic::transport::Channel> {
    let mut endpoint = Endpoint::from_shared(endpoint.to_string())?;

    if endpoint.uri().scheme_str() == Some("https") {
        endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }

    let channel = endpoint.connect().await?;
    Ok(channel)
}

fn build_interceptor(cli: &Cli) -> impl tonic::service::Interceptor + use<'_> {
    move |mut req: Request<()>| {
        if let Some(token) = &cli.x_token {
            let value = MetadataValue::try_from(token.as_str())
                .map_err(|e| Status::invalid_argument(format!("invalid x-token: {e}")))?;
            req.metadata_mut().insert("x-token", value);
        }

        if let Some(speed) = cli.speed {
            let speed_str = format!("{speed}");
            let value = MetadataValue::try_from(speed_str.as_str())
                .map_err(|e| Status::invalid_argument(format!("invalid x-replay-speed: {e}")))?;
            req.metadata_mut().insert("x-replay-speed", value);
        }

        Ok(req)
    }
}

fn build_subscribe_request(cli: &Cli) -> Result<SubscribeRequest> {
    let mut req = SubscribeRequest::default();

    let has_tx = !cli.tx_account.is_empty() || cli.tx_vote.is_some() || cli.tx_failed.is_some();
    let has_acct = !cli.account.is_empty() || !cli.account_owner.is_empty();
    let has_block = cli.blocks_meta;

    if !has_tx && !has_acct && !has_block {
        bail!("at least one filter group is required (--tx-account, --account, or --blocks-meta)");
    }

    if has_tx {
        let mut tx_filter = SubscribeRequestFilterTransactions::default();
        if !cli.tx_account.is_empty() {
            tx_filter.account_include = cli.tx_account.clone();
        }
        if let Some(vote) = cli.tx_vote {
            tx_filter.vote = Some(vote);
        }
        if let Some(failed) = cli.tx_failed {
            tx_filter.failed = Some(failed);
        }
        req.transactions.insert("probe".to_string(), tx_filter);
    }

    if has_acct {
        let mut acct_filter = SubscribeRequestFilterAccounts::default();
        if !cli.account.is_empty() {
            acct_filter.account = cli.account.clone();
        }
        if !cli.account_owner.is_empty() {
            acct_filter.owner = cli.account_owner.clone();
        }
        req.accounts.insert("probe".to_string(), acct_filter);
    }

    if has_block {
        req.blocks_meta
            .insert("probe".to_string(), SubscribeRequestFilterBlocksMeta::default());
    }

    req.from_slot = cli.from_slot;

    Ok(req)
}

fn mask_token(token: &str) -> String {
    if token.len() <= 8 {
        "***".to_string()
    } else {
        format!("{}...{}", &token[..4], &token[token.len() - 4..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli_with_tx_account(pk: &str) -> Cli {
        Cli {
            endpoint: "http://127.0.0.1:10099".to_string(),
            x_token: None,
            speed: None,
            tx_account: vec![pk.to_string()],
            tx_vote: None,
            tx_failed: None,
            account: vec![],
            account_owner: vec![],
            blocks_meta: false,
            from_slot: None,
            max_messages: None,
            duration: None,
            progress_every: 1000,
            print_updates: false,
            verbose: false,
        }
    }

    fn empty_cli() -> Cli {
        Cli {
            endpoint: "http://127.0.0.1:10099".to_string(),
            x_token: None,
            speed: None,
            tx_account: vec![],
            tx_vote: None,
            tx_failed: None,
            account: vec![],
            account_owner: vec![],
            blocks_meta: false,
            from_slot: None,
            max_messages: None,
            duration: None,
            progress_every: 1000,
            print_updates: false,
            verbose: false,
        }
    }

    #[test]
    fn build_subscribe_request_tx_filter() {
        let cli = cli_with_tx_account("11111111111111111111111111111111");
        let req = build_subscribe_request(&cli).unwrap();
        assert!(!req.transactions.is_empty());
        assert!(req.accounts.is_empty());
        assert!(req.blocks_meta.is_empty());
        assert_eq!(req.commitment, None);
        assert!(req.slots.is_empty());
        assert!(req.entry.is_empty());
        assert!(req.transactions_status.is_empty());
        assert!(req.accounts_data_slice.is_empty());
    }

    #[test]
    fn build_subscribe_request_no_filters_errors() {
        let cli = empty_cli();
        let err = build_subscribe_request(&cli).unwrap_err();
        assert!(err.to_string().contains("at least one filter group"));
    }

    #[test]
    fn build_subscribe_request_blocks_meta() {
        let cli = Cli {
            blocks_meta: true,
            ..empty_cli()
        };
        let req = build_subscribe_request(&cli).unwrap();
        assert!(!req.blocks_meta.is_empty());
        assert!(req.transactions.is_empty());
        assert!(req.accounts.is_empty());
    }

    #[test]
    fn build_subscribe_request_from_slot() {
        let cli = Cli {
            tx_account: vec!["11111111111111111111111111111111".to_string()],
            from_slot: Some(300_000_000),
            ..empty_cli()
        };
        let req = build_subscribe_request(&cli).unwrap();
        assert_eq!(req.from_slot, Some(300_000_000));
    }

    #[test]
    fn build_subscribe_request_account_filter() {
        let cli = Cli {
            account: vec!["11111111111111111111111111111111".to_string()],
            account_owner: vec!["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string()],
            ..empty_cli()
        };
        let req = build_subscribe_request(&cli).unwrap();
        assert!(!req.accounts.is_empty());
        let filter = req.accounts.get("probe").unwrap();
        assert_eq!(filter.account, vec!["11111111111111111111111111111111"]);
        assert_eq!(filter.owner, vec!["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"]);
        assert!(filter.filters.is_empty());
        assert_eq!(filter.nonempty_txn_signature, None);
    }
}
