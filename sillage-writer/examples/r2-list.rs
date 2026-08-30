//! List objects under a prefix in R2 and print one line per object:
//! `<key>\t<last_modified_unix_secs>\t<size_bytes>`.
//!
//! Reads R2 config from env vars:
//!   R2_ENDPOINT, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
//!   R2_PREFIX (optional, default: "chunks/")
//!   R2_REGION (optional, default: "auto")
//!
//! Used by scripts/smoke-phase5.sh to cross-reference local .uploaded markers
//! against actual R2 contents and prove .meta.json was uploaded last per chunk.

use std::env;

use anyhow::{anyhow, Result};
use aws_credential_types::{provider::SharedCredentialsProvider, Credentials};
use aws_sdk_s3::{config::Region, Client, Config};

fn must_env(key: &str) -> Result<String> {
    env::var(key).map_err(|_| anyhow!("env var {key} not set"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let endpoint = must_env("R2_ENDPOINT")?;
    let bucket = must_env("R2_BUCKET")?;
    let access = must_env("R2_ACCESS_KEY_ID")?;
    let secret = must_env("R2_SECRET_ACCESS_KEY")?;
    let prefix = env::var("R2_PREFIX").unwrap_or_else(|_| "chunks/".to_string());
    let region = env::var("R2_REGION").unwrap_or_else(|_| "auto".to_string());

    let creds = Credentials::new(access, secret, None, None, "smoke-r2-list");
    let cfg = Config::builder()
        .region(Region::new(region))
        .endpoint_url(endpoint)
        .credentials_provider(SharedCredentialsProvider::new(creds))
        .force_path_style(true)
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .build();
    let client = Client::from_conf(cfg);

    let mut cont_token: Option<String> = None;
    loop {
        let mut req = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix(&prefix)
            .max_keys(1000);
        if let Some(t) = &cont_token {
            req = req.continuation_token(t);
        }
        let out = req.send().await?;
        for obj in out.contents() {
            let key = obj.key().unwrap_or("");
            let lm = obj.last_modified().map(|t| t.secs()).unwrap_or(0);
            let size = obj.size().unwrap_or(0);
            println!("{key}\t{lm}\t{size}");
        }
        if out.is_truncated().unwrap_or(false) {
            cont_token = out.next_continuation_token().map(str::to_string);
        } else {
            break;
        }
    }

    Ok(())
}
