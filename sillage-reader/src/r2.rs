use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_s3::{config::Region, Client, Config};
use sillage_common::config::R2Config;
use sillage_common::Stream;
use tracing::debug;

#[derive(Clone)]
pub(crate) struct R2Client {
    client: Client,
    bucket: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct R2Chunk {
    pub stream: Stream,
    pub start_slot: u64,
    pub end_slot: u64,
    pub key_prefix: String,
}

impl R2Client {
    pub fn new(cfg: &R2Config) -> Result<Self> {
        if cfg.bucket.is_empty() || cfg.access_key_id.is_empty() || cfg.secret_access_key.is_empty()
        {
            anyhow::bail!(
                "R2 credentials missing: bucket, access_key_id, or secret_access_key is empty"
            );
        }

        let creds = Credentials::new(
            &cfg.access_key_id,
            &cfg.secret_access_key,
            None,
            None,
            "sillage-reader",
        );

        let config = Config::builder()
            .region(Region::new(cfg.region.clone()))
            .endpoint_url(&cfg.endpoint_url)
            .credentials_provider(SharedCredentialsProvider::new(creds))
            .force_path_style(true)
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .build();

        let client = Client::from_conf(config);

        Ok(Self {
            client,
            bucket: cfg.bucket.clone(),
        })
    }

    /// List all complete chunks in R2 under the "chunks/" prefix.
    ///
    /// Paginates through `list_objects_v2` results, groups keys by their stem
    /// (stream, start_slot, end_slot), and only returns chunks where all three
    /// files (.zst, .idx, .meta.json) are present.
    ///
    /// Unknown stream strings are skipped with a DEBUG log.
    pub async fn list_chunks(&self) -> Result<Vec<R2Chunk>> {
        let prefix = "chunks/";
        let mut continuation_token: Option<String> = None;
        let mut keys: Vec<String> = Vec::new();

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);

            if let Some(ref token) = continuation_token {
                req = req.continuation_token(token.as_str());
            }

            let resp = req.send().await.context("list_objects_v2 to R2")?;

            if let Some(objects) = resp.contents.as_ref() {
                for obj in objects {
                    if let Some(key) = obj.key() {
                        keys.push(key.to_string());
                    }
                }
            }

            if resp.is_truncated().unwrap_or(false) {
                continuation_token = resp.next_continuation_token().map(|t| t.to_string());
            } else {
                break;
            }
        }

        let mut groups: BTreeMap<(String, u64, u64), Vec<String>> = BTreeMap::new();

        for key in &keys {
            let stripped = match key.strip_prefix("chunks/") {
                Some(s) => s,
                None => continue,
            };

            let (stream_str, filename) = match stripped.split_once('/') {
                Some(pair) => pair,
                None => continue,
            };

            let (stem, ext) = match filename.rsplit_once('.') {
                Some(pair) => pair,
                None => continue,
            };

            // .meta.json has a two-part extension
            let ext = if ext == "json" && stem.ends_with(".meta") {
                let new_stem = stem.strip_suffix(".meta").unwrap_or(stem);
                (new_stem, "meta.json")
            } else {
                (stem, ext)
            };

            let (start_str, end_str) = match ext.0.split_once('-') {
                Some(pair) => pair,
                None => continue,
            };

            let start_slot = match start_str.parse::<u64>() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let end_slot = match end_str.parse::<u64>() {
                Ok(v) => v,
                Err(_) => continue,
            };

            groups
                .entry((stream_str.to_string(), start_slot, end_slot))
                .or_default()
                .push(ext.1.to_string());
        }

        let mut chunks = Vec::new();

        for ((stream_str, start_slot, end_slot), exts) in groups {
            let stream = match Stream::all().iter().find(|s| s.as_str() == stream_str) {
                Some(s) => *s,
                None => {
                    debug!(stream = %stream_str, "skipping unknown stream in R2 listing");
                    continue;
                }
            };

            let has_zst = exts.iter().any(|e| e == "zst");
            let has_idx = exts.iter().any(|e| e == "idx");
            let has_meta = exts.iter().any(|e| e == "meta.json");

            if has_zst && has_idx && has_meta {
                let key_prefix =
                    format!("chunks/{}/{:012}-{:012}", stream_str, start_slot, end_slot);
                chunks.push(R2Chunk {
                    stream,
                    start_slot,
                    end_slot,
                    key_prefix,
                });
            }
        }

        Ok(chunks)
    }

    /// Download a file from R2 to a local path.
    ///
    /// Writes the full body to `dest` and fsyncs before returning so the caller's
    /// subsequent `rename` produces a durable, fully-written file. No retry logic
    /// — retries live in the syncer.
    pub async fn get_file(&self, key: &str, dest: &Path) -> Result<u64> {
        use std::io::Write;

        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .context("get_object from R2")?;

        let data = resp
            .body
            .collect()
            .await
            .context("reading get_object body")?
            .into_bytes();

        let bytes = data.len() as u64;

        let mut file =
            std::fs::File::create(dest).with_context(|| format!("creating {}", dest.display()))?;
        file.write_all(&data)
            .with_context(|| format!("writing {} to {}", key, dest.display()))?;
        file.sync_all()
            .with_context(|| format!("fsyncing {}", dest.display()))?;

        Ok(bytes)
    }
}

#[cfg(test)]
impl R2Client {
    pub(crate) fn from_client(client: Client, bucket: String) -> Self {
        Self { client, bucket }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::config::Region;
    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
    use aws_sdk_s3::{config::Config, Client as S3Client};
    use aws_smithy_mocks::{create_mock_http_client, mock, MockResponseInterceptor, RuleMode};
    use aws_smithy_types::retry::RetryConfig;

    fn valid_r2_config() -> R2Config {
        R2Config {
            bucket: "test-bucket".to_string(),
            region: "auto".to_string(),
            endpoint_url: "https://r2.example.com".to_string(),
            access_key_id: "key123".to_string(),
            secret_access_key: "secret456".to_string(),
        }
    }

    fn build_mock_client(interceptor: MockResponseInterceptor) -> S3Client {
        let mock_http = create_mock_http_client();
        let creds = Credentials::new("test", "test", None, None, "test");
        let config = Config::builder()
            .region(Region::new("auto"))
            .endpoint_url("https://test.r2.cloudflarestorage.com")
            .credentials_provider(SharedCredentialsProvider::new(creds))
            .force_path_style(true)
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .retry_config(RetryConfig::disabled())
            .http_client(mock_http)
            .interceptor(interceptor)
            .build();
        S3Client::from_conf(config)
    }

    fn mock_r2(rules: &[&aws_smithy_mocks::Rule], rule_mode: RuleMode) -> R2Client {
        let mut interceptor = MockResponseInterceptor::new().rule_mode(rule_mode);
        for rule in rules {
            interceptor = interceptor.with_rule(rule);
        }
        let client = build_mock_client(interceptor);
        R2Client::from_client(client, "test-bucket".to_string())
    }

    #[test]
    fn r2_client_rejects_empty_credentials() {
        let cfg = R2Config {
            bucket: String::new(),
            region: "auto".to_string(),
            endpoint_url: "https://r2.example.com".to_string(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
        };
        assert!(R2Client::new(&cfg).is_err());
    }

    #[test]
    fn r2_client_accepts_valid_credentials() {
        let cfg = valid_r2_config();
        assert!(R2Client::new(&cfg).is_ok());
    }

    #[tokio::test]
    async fn list_chunks_groups_keys_by_stem_and_filters_incomplete() {
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![
                    aws_sdk_s3::types::Object::builder()
                        .key("chunks/tx/000000000100-000000000200.zst")
                        .build(),
                    aws_sdk_s3::types::Object::builder()
                        .key("chunks/tx/000000000100-000000000200.idx")
                        .build(),
                    aws_sdk_s3::types::Object::builder()
                        .key("chunks/tx/000000000100-000000000200.meta.json")
                        .build(),
                    aws_sdk_s3::types::Object::builder()
                        .key("chunks/tx/000000000300-000000000400.zst")
                        .build(),
                    aws_sdk_s3::types::Object::builder()
                        .key("chunks/tx/000000000300-000000000400.meta.json")
                        .build(),
                    aws_sdk_s3::types::Object::builder()
                        .key("chunks/acct/000000000500-000000000600.zst")
                        .build(),
                    aws_sdk_s3::types::Object::builder()
                        .key("chunks/acct/000000000500-000000000600.idx")
                        .build(),
                    aws_sdk_s3::types::Object::builder()
                        .key("chunks/acct/000000000500-000000000600.meta.json")
                        .build(),
                ]))
                .is_truncated(false)
                .build()
        });

        let r2 = mock_r2(&[&rule], RuleMode::MatchAny);
        let chunks = r2.list_chunks().await.unwrap();

        assert_eq!(chunks.len(), 2);

        let tx_chunk = chunks.iter().find(|c| c.stream == Stream::Tx).unwrap();
        assert_eq!(tx_chunk.start_slot, 100);
        assert_eq!(tx_chunk.end_slot, 200);
        assert_eq!(tx_chunk.key_prefix, "chunks/tx/000000000100-000000000200");

        let acct_chunk = chunks.iter().find(|c| c.stream == Stream::Acct).unwrap();
        assert_eq!(acct_chunk.start_slot, 500);
        assert_eq!(acct_chunk.end_slot, 600);
        assert_eq!(
            acct_chunk.key_prefix,
            "chunks/acct/000000000500-000000000600"
        );
    }

    #[tokio::test]
    async fn list_chunks_paginates() {
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2)
            .sequence()
            .output(|| {
                ListObjectsV2Output::builder()
                    .set_contents(Some(vec![
                        aws_sdk_s3::types::Object::builder()
                            .key("chunks/tx/000000000100-000000000200.zst")
                            .build(),
                        aws_sdk_s3::types::Object::builder()
                            .key("chunks/tx/000000000100-000000000200.idx")
                            .build(),
                        aws_sdk_s3::types::Object::builder()
                            .key("chunks/tx/000000000100-000000000200.meta.json")
                            .build(),
                    ]))
                    .is_truncated(true)
                    .next_continuation_token("token-page2")
                    .build()
            })
            .output(|| {
                ListObjectsV2Output::builder()
                    .set_contents(Some(vec![
                        aws_sdk_s3::types::Object::builder()
                            .key("chunks/block/000000000300-000000000400.zst")
                            .build(),
                        aws_sdk_s3::types::Object::builder()
                            .key("chunks/block/000000000300-000000000400.idx")
                            .build(),
                        aws_sdk_s3::types::Object::builder()
                            .key("chunks/block/000000000300-000000000400.meta.json")
                            .build(),
                    ]))
                    .is_truncated(false)
                    .build()
            })
            .build();

        let r2 = mock_r2(&[&rule], RuleMode::Sequential);
        let chunks = r2.list_chunks().await.unwrap();

        assert_eq!(chunks.len(), 2);

        let tx_chunk = chunks.iter().find(|c| c.stream == Stream::Tx).unwrap();
        assert_eq!(tx_chunk.start_slot, 100);
        assert_eq!(tx_chunk.end_slot, 200);

        let block_chunk = chunks.iter().find(|c| c.stream == Stream::Block).unwrap();
        assert_eq!(block_chunk.start_slot, 300);
        assert_eq!(block_chunk.end_slot, 400);
    }
}
