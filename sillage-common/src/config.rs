use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Commitment {
    #[default]
    Confirmed,
    Finalized,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    #[serde(default)]
    pub server: ServerConfig,
    pub r2: R2Config,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub log: LogConfig,
    #[serde(default)]
    pub geyser: Option<GeyserConfig>,
    #[serde(default)]
    pub writer: WriterConfig,
    #[serde(default)]
    pub uploader: UploaderConfig,
    #[serde(default)]
    pub reader: ReaderConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    #[serde(default)]
    pub tls: TlsConfig,
}

/// TLS identity for the gRPC listener. Disabled by default: a reader behind a
/// TLS-terminating proxy wants plaintext h2c, and local development should not
/// need certificates. Clients dialing `https://` directly need this on.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,
    /// PEM-encoded certificate chain, leaf first.
    #[serde(default)]
    pub cert_path: String,
    /// PEM-encoded private key for `cert_path`.
    #[serde(default)]
    pub key_path: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            tls: TlsConfig::default(),
        }
    }
}

fn default_listen_addr() -> String {
    "0.0.0.0:10000".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct R2Config {
    pub bucket: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default = "default_endpoint")]
    pub endpoint_url: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

fn default_region() -> String {
    "auto".to_string()
}

fn default_endpoint() -> String {
    "https://example.r2.cloudflarestorage.com".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_nvme_path")]
    pub nvme_path: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            nvme_path: default_nvme_path(),
        }
    }
}

fn default_nvme_path() -> String {
    "/data".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_log_format")]
    pub format: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "json".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeyserConfig {
    pub endpoint: String,
    #[serde(default)]
    pub x_token: String,
    #[serde(default)]
    pub commitment: Commitment,
    #[serde(default = "default_max_message_size_bytes")]
    pub max_message_size_bytes: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WriterConfig {
    #[serde(default = "default_slots_per_chunk")]
    pub slots_per_chunk: u64,
    #[serde(default = "default_out_of_order_tolerance_slots")]
    pub out_of_order_tolerance_slots: u64,
    #[serde(default = "default_max_open_chunks")]
    pub max_open_chunks: usize,
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UploaderConfig {
    #[serde(default = "default_uploader_scan_interval_secs")]
    pub scan_interval_secs: u64,
    #[serde(default = "default_max_concurrent_uploads")]
    pub max_concurrent_uploads: usize,
    #[serde(default = "default_retry_attempts")]
    pub retry_attempts: u32,
    #[serde(default = "default_retry_initial_delay_ms")]
    pub retry_initial_delay_ms: u64,
    #[serde(default = "default_local_retention_hours")]
    pub local_retention_hours: u64,
    #[serde(default = "default_disk_pressure_warn_pct")]
    pub disk_pressure_warn_pct: u8,
}

fn default_max_message_size_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_slots_per_chunk() -> u64 {
    1000
}

fn default_out_of_order_tolerance_slots() -> u64 {
    100
}

fn default_max_open_chunks() -> usize {
    4
}

fn default_channel_capacity() -> usize {
    8192
}

fn default_uploader_scan_interval_secs() -> u64 {
    5
}

fn default_max_concurrent_uploads() -> usize {
    4
}

fn default_retry_attempts() -> u32 {
    3
}

fn default_retry_initial_delay_ms() -> u64 {
    1000
}

fn default_local_retention_hours() -> u64 {
    24
}

fn default_disk_pressure_warn_pct() -> u8 {
    80
}

#[derive(Debug, Clone, Deserialize)]
pub struct PacingConfig {
    #[serde(default = "default_pacing_enabled")]
    pub enabled: bool,
    #[serde(default = "default_pacing_speed_multiplier")]
    pub speed_multiplier: f64,
    #[serde(default = "default_pacing_lag_warn_ms")]
    pub lag_warn_ms: u64,
    #[serde(default = "default_pacing_lag_drop_ms")]
    pub lag_drop_ms: u64,
}

fn default_pacing_enabled() -> bool {
    true
}

fn default_pacing_speed_multiplier() -> f64 {
    1.0
}

fn default_pacing_lag_warn_ms() -> u64 {
    5_000
}

fn default_pacing_lag_drop_ms() -> u64 {
    30_000
}

impl Default for PacingConfig {
    fn default() -> Self {
        Self {
            enabled: default_pacing_enabled(),
            speed_multiplier: default_pacing_speed_multiplier(),
            lag_warn_ms: default_pacing_lag_warn_ms(),
            lag_drop_ms: default_pacing_lag_drop_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_metrics_enabled")]
    pub enabled: bool,
    #[serde(default = "default_metrics_listen_addr")]
    pub listen_addr: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_metrics_enabled(),
            listen_addr: default_metrics_listen_addr(),
        }
    }
}

fn default_metrics_enabled() -> bool {
    true
}

fn default_metrics_listen_addr() -> String {
    "0.0.0.0:10001".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReaderConfig {
    #[serde(default = "default_reader_scan_interval_secs")]
    pub scan_interval_secs: u64,
    #[serde(default = "default_max_concurrent_downloads")]
    pub max_concurrent_downloads: usize,
    #[serde(default = "default_reader_local_retention_hours")]
    pub local_retention_hours: u64,
    #[serde(default = "default_reader_retry_attempts")]
    pub retry_attempts: u32,
    #[serde(default = "default_reader_retry_initial_delay_ms")]
    pub retry_initial_delay_ms: u64,
    #[serde(default = "default_decoded_cache_bytes")]
    pub decoded_cache_bytes: u64,
    #[serde(default = "default_index_cache_bytes")]
    pub index_cache_bytes: u64,
    #[serde(default, deserialize_with = "deserialize_comma_separated_vec")]
    pub auth_tokens: Vec<String>,
    #[serde(default = "default_subscription_channel_capacity")]
    pub subscription_channel_capacity: usize,
    /// How long a caught-up subscriber waits for new chunks before the reader
    /// closes its stream. Must exceed the writer's chunk cadence, or healthy
    /// followers get disconnected between seals.
    #[serde(default = "default_follow_idle_timeout_secs")]
    pub follow_idle_timeout_secs: u64,
    /// Ceiling on concurrently connected subscribers. Each one costs a task, a
    /// replay cursor, and pressure on the shared decode caches.
    #[serde(default = "default_max_connections_total")]
    pub max_connections_total: usize,
    /// Ceiling per authenticated token, so one client cannot consume the whole
    /// server budget. Ignored when `auth_tokens` is empty.
    #[serde(default = "default_max_connections_per_token")]
    pub max_connections_per_token: usize,
    #[serde(default)]
    pub pacing: PacingConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
}

fn default_reader_scan_interval_secs() -> u64 {
    30
}

fn default_max_concurrent_downloads() -> usize {
    4
}

fn default_reader_local_retention_hours() -> u64 {
    24
}

fn default_reader_retry_attempts() -> u32 {
    3
}

fn default_reader_retry_initial_delay_ms() -> u64 {
    1000
}

fn default_decoded_cache_bytes() -> u64 {
    768 * 1024 * 1024
}

fn default_index_cache_bytes() -> u64 {
    134_217_728 // 128 MiB
}

fn deserialize_comma_separated_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct CommaSeparatedVec;

    impl<'de> serde::de::Visitor<'de> for CommaSeparatedVec {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a sequence of strings or a comma-separated string")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut vec = Vec::new();
            while let Some(item) = seq.next_element::<String>()? {
                vec.push(item);
            }
            Ok(vec)
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect())
        }
    }

    deserializer.deserialize_any(CommaSeparatedVec)
}

/// Roughly 2x the seal cadence of a 1000-slot chunk at ~400ms/slot, so a
/// follower riding the head is not dropped while waiting for the next chunk.
fn default_follow_idle_timeout_secs() -> u64 {
    900
}

fn default_max_connections_total() -> usize {
    256
}

fn default_max_connections_per_token() -> usize {
    16
}

fn default_subscription_channel_capacity() -> usize {
    1024
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            scan_interval_secs: default_reader_scan_interval_secs(),
            max_concurrent_downloads: default_max_concurrent_downloads(),
            local_retention_hours: default_reader_local_retention_hours(),
            retry_attempts: default_reader_retry_attempts(),
            retry_initial_delay_ms: default_reader_retry_initial_delay_ms(),
            decoded_cache_bytes: default_decoded_cache_bytes(),
            index_cache_bytes: default_index_cache_bytes(),
            auth_tokens: Vec::new(),
            subscription_channel_capacity: default_subscription_channel_capacity(),
            follow_idle_timeout_secs: default_follow_idle_timeout_secs(),
            max_connections_total: default_max_connections_total(),
            max_connections_per_token: default_max_connections_per_token(),
            pacing: PacingConfig::default(),
            metrics: MetricsConfig::default(),
        }
    }
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            slots_per_chunk: default_slots_per_chunk(),
            out_of_order_tolerance_slots: default_out_of_order_tolerance_slots(),
            max_open_chunks: default_max_open_chunks(),
            channel_capacity: default_channel_capacity(),
        }
    }
}

impl Default for UploaderConfig {
    fn default() -> Self {
        Self {
            scan_interval_secs: default_uploader_scan_interval_secs(),
            max_concurrent_uploads: default_max_concurrent_uploads(),
            retry_attempts: default_retry_attempts(),
            retry_initial_delay_ms: default_retry_initial_delay_ms(),
            local_retention_hours: default_local_retention_hours(),
            disk_pressure_warn_pct: default_disk_pressure_warn_pct(),
        }
    }
}

impl Settings {
    pub fn load() -> Result<Self, ConfigError> {
        let config_path =
            std::env::var("SILLAGE_CONFIG_PATH").unwrap_or_else(|_| "config/default".to_string());
        let settings: Self = Config::builder()
            .add_source(File::with_name(&config_path).required(false))
            .add_source(
                Environment::with_prefix("SILLAGE")
                    .prefix_separator("_")
                    .separator("__"),
            )
            .build()?
            .try_deserialize()?;
        settings.validate()?;
        Ok(settings)
    }

    pub fn load_from_path(path: &str) -> Result<Self, ConfigError> {
        let settings: Self = Config::builder()
            .add_source(File::with_name(path).required(true))
            .add_source(
                Environment::with_prefix("SILLAGE")
                    .prefix_separator("_")
                    .separator("__"),
            )
            .build()?
            .try_deserialize()?;
        settings.validate()?;
        Ok(settings)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let m = self.reader.pacing.speed_multiplier;
        if !m.is_finite() || m <= 0.0 {
            return Err(ConfigError::Message(format!(
                "reader.pacing.speed_multiplier must be a finite positive number, got {m}"
            )));
        }
        if self.server.tls.enabled {
            if self.server.tls.cert_path.is_empty() {
                return Err(ConfigError::Message(
                    "server.tls.enabled is true but server.tls.cert_path is empty".to_string(),
                ));
            }
            if self.server.tls.key_path.is_empty() {
                return Err(ConfigError::Message(
                    "server.tls.enabled is true but server.tls.key_path is empty".to_string(),
                ));
            }
        }
        if self.reader.metrics.enabled {
            self.reader
                .metrics
                .listen_addr
                .parse::<std::net::SocketAddr>()
                .map_err(|e| ConfigError::Message(format!("invalid metrics listen_addr: {e}")))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempConfig {
        _dir: tempfile::TempDir,
        path_no_ext: String,
    }

    fn write_temp_toml(content: &str) -> TempConfig {
        let dir = tempfile::TempDir::new().expect("create temp dir");
        let file_path = dir.path().join("test.toml");
        std::fs::write(&file_path, content).expect("write temp file");
        let path_no_ext = dir
            .path()
            .join("test")
            .to_str()
            .expect("path to string")
            .to_string();
        TempConfig {
            _dir: dir,
            path_no_ext,
        }
    }

    fn load_from_file_only(path: &str) -> Result<Settings, ConfigError> {
        Config::builder()
            .add_source(File::with_name(path).required(true))
            .build()?
            .try_deserialize()
    }

    #[test]
    fn test_load_valid_config() {
        let tc = write_temp_toml(
            r#"
[server]
listen_addr = "127.0.0.1:9090"

[r2]
bucket = "my-bucket"
region = "us-east-1"
endpoint_url = "https://r2.example.com"
access_key_id = "key123"
secret_access_key = "secret456"

[storage]
nvme_path = "/mnt/nvme"

[log]
level = "debug"
format = "plain"
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load valid config");

        assert_eq!(settings.server.listen_addr, "127.0.0.1:9090");
        assert_eq!(settings.r2.bucket, "my-bucket");
        assert_eq!(settings.r2.region, "us-east-1");
        assert_eq!(settings.r2.endpoint_url, "https://r2.example.com");
        assert_eq!(settings.r2.access_key_id, "key123");
        assert_eq!(settings.r2.secret_access_key, "secret456");
        assert_eq!(settings.storage.nvme_path, "/mnt/nvme");
        assert_eq!(settings.log.level, "debug");
        assert_eq!(settings.log.format, "plain");
    }

    #[test]
    fn test_env_override() {
        let tc = write_temp_toml(
            r#"
[server]
listen_addr = "127.0.0.1:9090"

[r2]
bucket = "my-bucket"
region = "us-east-1"
endpoint_url = "https://r2.example.com"
access_key_id = "key123"
secret_access_key = "secret456"

[storage]
nvme_path = "/mnt/nvme"

[log]
level = "debug"
format = "plain"
"#,
        );
        std::env::set_var("SILLAGE_SERVER__LISTEN_ADDR", "0.0.0.0:8080");

        let settings =
            Settings::load_from_path(&tc.path_no_ext).expect("load config with env override");
        assert_eq!(settings.server.listen_addr, "0.0.0.0:8080");

        std::env::remove_var("SILLAGE_SERVER__LISTEN_ADDR");
    }

    #[test]
    fn test_missing_required_field() {
        let tc = write_temp_toml(
            r#"
[server]
listen_addr = "127.0.0.1:9090"

[r2]
region = "us-east-1"
"#,
        );
        let result = load_from_file_only(&tc.path_no_ext);
        assert!(result.is_err(), "should fail with missing required field");

        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("bucket") || err_msg.contains("r2"),
            "error should mention missing field, got: {err_msg}"
        );
    }

    #[test]
    fn test_invalid_toml() {
        let tc = write_temp_toml(
            r#"
[server
listen_addr = broken
"#,
        );
        let result = load_from_file_only(&tc.path_no_ext);
        assert!(result.is_err(), "should fail with invalid TOML, not panic");
    }

    #[test]
    fn test_default_values() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load minimal config");

        assert_eq!(settings.server.listen_addr, "0.0.0.0:10000");
        assert_eq!(settings.r2.region, "auto");
        assert_eq!(
            settings.r2.endpoint_url,
            "https://example.r2.cloudflarestorage.com"
        );
        assert_eq!(settings.storage.nvme_path, "/data");
        assert_eq!(settings.log.level, "info");
        assert_eq!(settings.log.format, "json");
    }

    #[test]
    fn test_env_prefix_separator() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        std::env::set_var("SILLAGE_STORAGE__NVME_PATH", "/custom/nvme");

        let settings =
            Settings::load_from_path(&tc.path_no_ext).expect("load config with env override");
        assert_eq!(settings.storage.nvme_path, "/custom/nvme");

        std::env::remove_var("SILLAGE_STORAGE__NVME_PATH");
    }

    #[test]
    fn test_config_path_env_override() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "env-override-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        std::env::set_var("SILLAGE_CONFIG_PATH", &tc.path_no_ext);

        let settings = Settings::load().expect("load config from env var path");
        assert_eq!(settings.r2.bucket, "env-override-bucket");

        std::env::remove_var("SILLAGE_CONFIG_PATH");
    }

    #[test]
    fn test_writer_defaults_when_section_omitted() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        assert_eq!(settings.writer.slots_per_chunk, 1000);
        assert_eq!(settings.writer.out_of_order_tolerance_slots, 100);
        assert_eq!(settings.writer.max_open_chunks, 4);
        assert_eq!(settings.writer.channel_capacity, 8192);
    }

    #[test]
    fn test_uploader_defaults_when_section_omitted() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        assert_eq!(settings.uploader.scan_interval_secs, 5);
        assert_eq!(settings.uploader.max_concurrent_uploads, 4);
        assert_eq!(settings.uploader.retry_attempts, 3);
        assert_eq!(settings.uploader.retry_initial_delay_ms, 1000);
        assert_eq!(settings.uploader.local_retention_hours, 24);
        assert_eq!(settings.uploader.disk_pressure_warn_pct, 80);
    }

    #[test]
    fn test_uploader_config_with_explicit_retention_and_pressure() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"

[uploader]
scan_interval_secs = 10
max_concurrent_uploads = 8
retry_attempts = 5
retry_initial_delay_ms = 500
local_retention_hours = 48
disk_pressure_warn_pct = 90
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        assert_eq!(settings.uploader.local_retention_hours, 48);
        assert_eq!(settings.uploader.disk_pressure_warn_pct, 90);
    }

    #[test]
    fn test_uploader_config_defaults_retention_and_pressure() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        assert_eq!(settings.uploader.local_retention_hours, 24);
        assert_eq!(settings.uploader.disk_pressure_warn_pct, 80);
    }

    #[test]
    fn test_uploader_env_override_retention() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        std::env::set_var("SILLAGE_UPLOADER__LOCAL_RETENTION_HOURS", "12");

        let settings =
            Settings::load_from_path(&tc.path_no_ext).expect("load config with env override");
        assert_eq!(settings.uploader.local_retention_hours, 12);

        std::env::remove_var("SILLAGE_UPLOADER__LOCAL_RETENTION_HOURS");
    }

    #[test]
    fn test_geyser_parses_when_present() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"

[geyser]
endpoint = "http://validator:10000"
x_token = "token123"
commitment = "confirmed"
max_message_size_bytes = 33554432
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        let geyser = settings.geyser.expect("geyser should be present");
        assert_eq!(geyser.endpoint, "http://validator:10000");
        assert_eq!(geyser.x_token, "token123");
        assert_eq!(geyser.commitment, Commitment::Confirmed);
        assert_eq!(geyser.max_message_size_bytes, 33554432);
    }

    #[test]
    fn test_geyser_none_when_absent() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        assert!(settings.geyser.is_none());
    }

    #[test]
    fn test_geyser_rejects_processed_commitment() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"

[geyser]
endpoint = "http://validator:10000"
commitment = "processed"
"#,
        );
        let result = load_from_file_only(&tc.path_no_ext);
        assert!(
            result.is_err(),
            "should fail with invalid commitment value 'processed'"
        );
    }

    #[test]
    fn test_writer_env_override() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        std::env::set_var("SILLAGE_WRITER__SLOTS_PER_CHUNK", "500");

        let settings =
            Settings::load_from_path(&tc.path_no_ext).expect("load config with env override");
        assert_eq!(settings.writer.slots_per_chunk, 500);

        std::env::remove_var("SILLAGE_WRITER__SLOTS_PER_CHUNK");
    }

    #[test]
    fn test_reader_defaults() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        assert_eq!(settings.reader.scan_interval_secs, 30);
        assert_eq!(settings.reader.max_concurrent_downloads, 4);
        assert_eq!(settings.reader.local_retention_hours, 24);
        assert_eq!(settings.reader.retry_attempts, 3);
        assert_eq!(settings.reader.retry_initial_delay_ms, 1000);
        assert_eq!(settings.reader.decoded_cache_bytes, 805_306_368);
    }

    #[test]
    fn test_reader_explicit() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"

[reader]
scan_interval_secs = 60
max_concurrent_downloads = 8
local_retention_hours = 48
retry_attempts = 5
retry_initial_delay_ms = 2000
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        assert_eq!(settings.reader.scan_interval_secs, 60);
        assert_eq!(settings.reader.max_concurrent_downloads, 8);
        assert_eq!(settings.reader.local_retention_hours, 48);
        assert_eq!(settings.reader.retry_attempts, 5);
        assert_eq!(settings.reader.retry_initial_delay_ms, 2000);
    }

    #[test]
    fn test_reader_decoded_cache_bytes_explicit() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"

[reader]
decoded_cache_bytes = 1073741824
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        assert_eq!(settings.reader.decoded_cache_bytes, 1_073_741_824);
    }

    #[test]
    fn test_reader_decoded_cache_bytes_env_override() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        std::env::set_var("SILLAGE_READER__DECODED_CACHE_BYTES", "1073741824");

        let settings =
            Settings::load_from_path(&tc.path_no_ext).expect("load config with env override");
        assert_eq!(settings.reader.decoded_cache_bytes, 1_073_741_824);

        std::env::remove_var("SILLAGE_READER__DECODED_CACHE_BYTES");
    }

    #[test]
    fn test_reader_index_cache_bytes_default() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        assert_eq!(settings.reader.index_cache_bytes, 134_217_728);
    }

    #[test]
    fn test_reader_index_cache_bytes_explicit() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"

[reader]
index_cache_bytes = 268435456
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        assert_eq!(settings.reader.index_cache_bytes, 268_435_456);
    }

    #[test]
    fn test_reader_index_cache_bytes_env_override() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        std::env::set_var("SILLAGE_READER__INDEX_CACHE_BYTES", "268435456");

        let settings =
            Settings::load_from_path(&tc.path_no_ext).expect("load config with env override");
        assert_eq!(settings.reader.index_cache_bytes, 268_435_456);

        std::env::remove_var("SILLAGE_READER__INDEX_CACHE_BYTES");
    }

    #[test]
    fn test_reader_env_override() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        std::env::set_var("SILLAGE_READER__MAX_CONCURRENT_DOWNLOADS", "8");

        let settings =
            Settings::load_from_path(&tc.path_no_ext).expect("load config with env override");
        assert_eq!(settings.reader.max_concurrent_downloads, 8);

        std::env::remove_var("SILLAGE_READER__MAX_CONCURRENT_DOWNLOADS");
    }

    #[test]
    fn test_reader_auth_tokens_default() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        assert!(settings.reader.auth_tokens.is_empty());
    }

    #[test]
    fn test_reader_auth_tokens_explicit() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"

[reader]
auth_tokens = ["token1", "token2"]
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        assert_eq!(settings.reader.auth_tokens, vec!["token1", "token2"]);
    }

    #[test]
    fn test_tls_disabled_by_default() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        assert!(!settings.server.tls.enabled);
        assert!(settings.server.tls.cert_path.is_empty());
    }

    #[test]
    fn test_tls_enabled_without_cert_path_is_rejected() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"

[server.tls]
enabled = true
key_path = "/etc/sillage/tls.key"
"#,
        );
        // load_from_file_only deserializes without validating, so call the
        // validation step directly.
        let settings = load_from_file_only(&tc.path_no_ext).expect("deserialize");
        let err = settings.validate().expect_err("cert_path is required");
        assert!(err.to_string().contains("cert_path"), "got: {err}");
    }

    #[test]
    fn test_tls_enabled_without_key_path_is_rejected() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"

[server.tls]
enabled = true
cert_path = "/etc/sillage/tls.crt"
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("deserialize");
        let err = settings.validate().expect_err("key_path is required");
        assert!(err.to_string().contains("key_path"), "got: {err}");
    }

    #[test]
    fn test_tls_paths_parse_when_enabled() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"

[server.tls]
enabled = true
cert_path = "/etc/sillage/tls.crt"
key_path = "/etc/sillage/tls.key"
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        settings.validate().expect("complete TLS config is valid");
        assert!(settings.server.tls.enabled);
        assert_eq!(settings.server.tls.cert_path, "/etc/sillage/tls.crt");
        assert_eq!(settings.server.tls.key_path, "/etc/sillage/tls.key");
    }

    #[test]
    fn test_reader_subscription_channel_capacity_default() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        assert_eq!(settings.reader.subscription_channel_capacity, 1024);
    }

    #[test]
    fn test_reader_subscription_channel_capacity_explicit() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"

[reader]
subscription_channel_capacity = 2048
"#,
        );
        let settings = load_from_file_only(&tc.path_no_ext).expect("load config");
        assert_eq!(settings.reader.subscription_channel_capacity, 2048);
    }

    #[test]
    fn test_reader_auth_tokens_env_override() {
        let tc = write_temp_toml(
            r#"
[r2]
bucket = "my-bucket"
access_key_id = "key123"
secret_access_key = "secret456"
"#,
        );
        std::env::set_var("SILLAGE_READER__AUTH_TOKENS", "token1,token2");

        let settings =
            Settings::load_from_path(&tc.path_no_ext).expect("load config with env override");
        assert_eq!(settings.reader.auth_tokens, vec!["token1", "token2"]);

        std::env::remove_var("SILLAGE_READER__AUTH_TOKENS");
    }
}
