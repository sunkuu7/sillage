use crate::config::LogConfig;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

/// Initialize the global tracing subscriber.
///
/// # Panics
/// Panics if called more than once (tracing subscriber can only be set once per process).
pub fn init_tracing(config: &LogConfig) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.level));

    let fmt_layer = match config.format.as_str() {
        "json" => fmt::layer()
            .json()
            .with_target(true)
            .with_line_number(true)
            .boxed(),
        _ => fmt::layer()
            .pretty()
            .with_target(true)
            .with_line_number(true)
            .boxed(),
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Once;

    static INIT_JSON: Once = Once::new();
    static INIT_PRETTY: Once = Once::new();

    #[test]
    fn test_init_tracing_json() {
        INIT_JSON.call_once(|| {
            let config = LogConfig {
                level: "info".to_string(),
                format: "json".to_string(),
            };
            init_tracing(&config);
        });
        tracing::info!("test_json_format");
    }

    #[test]
    #[ignore = "Cannot run multiple tracing init tests in same process"]
    fn test_init_tracing_pretty() {
        INIT_PRETTY.call_once(|| {
            let config = LogConfig {
                level: "debug".to_string(),
                format: "pretty".to_string(),
            };
            init_tracing(&config);
        });
        tracing::debug!("test_pretty_format");
    }
}
