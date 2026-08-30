use tokio_util::sync::CancellationToken;

/// A cloneable shutdown signal wrapper around CancellationToken.
#[derive(Clone, Debug)]
pub struct ShutdownSignal {
    token: CancellationToken,
}

impl ShutdownSignal {
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }

    pub fn cancel(&self) {
        self.token.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }

    /// Returns a child token that will be cancelled when this signal is cancelled.
    pub fn child_token(&self) -> CancellationToken {
        self.token.child_token()
    }
}

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}

/// Wait for SIGTERM or SIGINT, cancel the provided signal, and return.
pub async fn wait_for_shutdown(signal: ShutdownSignal) -> std::io::Result<()> {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sig = signal(SignalKind::terminate())?;
        sig.recv().await;
        std::io::Result::Ok(())
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<std::io::Result<()>>();

    tokio::select! {
        result = ctrl_c => { result?; },
        result = terminate => { result?; },
    }

    signal.cancel();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ShutdownSignal;

    #[test]
    fn test_shutdown_signal_new_not_cancelled() {
        let signal = ShutdownSignal::new();
        assert!(!signal.is_cancelled());
    }

    #[test]
    fn test_shutdown_signal_cancel() {
        let signal = ShutdownSignal::new();
        assert!(!signal.is_cancelled());
        signal.cancel();
        assert!(signal.is_cancelled());
    }

    #[tokio::test]
    async fn test_shutdown_signal_cancelled_future() {
        let signal = ShutdownSignal::new();
        signal.cancel();
        signal.cancelled().await;
    }

    #[tokio::test]
    async fn test_shutdown_signal_clone() {
        let signal = ShutdownSignal::new();
        let cloned = signal.clone();
        signal.cancel();
        assert!(cloned.is_cancelled());
    }

    #[tokio::test]
    async fn test_multiple_listeners() {
        let signal = ShutdownSignal::new();
        let listeners: Vec<ShutdownSignal> = (0..5).map(|_| signal.clone()).collect();
        signal.cancel();
        for listener in &listeners {
            assert!(listener.is_cancelled());
        }
    }
}
