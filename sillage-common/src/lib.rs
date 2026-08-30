pub mod chunk;
pub mod config;
pub mod idx;
pub mod logging;
pub mod shutdown;
pub mod slot;
pub mod stream;

pub use config::{LogConfig, Settings};
pub use logging::init_tracing;
pub use shutdown::ShutdownSignal;
pub use stream::Stream;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_public_api_accessible() {
        let _ = std::any::TypeId::of::<Settings>();
        let _ = std::any::TypeId::of::<ShutdownSignal>();
    }
}
