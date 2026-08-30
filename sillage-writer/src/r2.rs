use std::path::Path;

use anyhow::{Context, Result};
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_s3::{config::Region, primitives::ByteStream, Client, Config};

use sillage_common::config::R2Config;

#[derive(Clone)]
pub(crate) struct R2Client {
    client: Client,
    bucket: String,
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
            "sillage-writer",
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

    pub async fn put_file(&self, key: &str, path: &Path) -> Result<()> {
        let body = ByteStream::from_path(path)
            .await
            .context("reading file for upload")?;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .context("put_object to R2")?;

        Ok(())
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

    fn valid_r2_config() -> R2Config {
        R2Config {
            bucket: "test-bucket".to_string(),
            region: "auto".to_string(),
            endpoint_url: "https://r2.example.com".to_string(),
            access_key_id: "key123".to_string(),
            secret_access_key: "secret456".to_string(),
        }
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
}
