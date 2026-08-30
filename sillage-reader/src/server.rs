use crate::grpc::GeyserService;
use anyhow::{Context, Result};
use sillage_common::config::TlsConfig;
use sillage_common::shutdown::ShutdownSignal;
use std::net::SocketAddr;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::{info, warn};
use yellowstone_grpc_proto::geyser::geyser_server::GeyserServer;

pub(crate) async fn start_server(
    addr: SocketAddr,
    service: GeyserService,
    tls: &TlsConfig,
    shutdown: ShutdownSignal,
) -> Result<()> {
    let mut builder = Server::builder();

    if tls.enabled {
        builder = builder
            .tls_config(ServerTlsConfig::new().identity(load_identity(tls)?))
            .context("configuring TLS for the gRPC listener")?;
        info!(
            addr = %addr,
            cert_path = %tls.cert_path,
            "gRPC listener serving TLS"
        );
    } else {
        warn!(
            addr = %addr,
            "gRPC listener serving plaintext h2c; terminate TLS at a proxy or set server.tls"
        );
    }

    builder
        .add_service(GeyserServer::new(service))
        .serve_with_shutdown(addr, async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}

/// Load the PEM certificate chain and private key named by `tls`.
///
/// Both files are read at startup so a missing or unreadable one fails the
/// process with the offending path in the message, rather than surfacing later
/// as an opaque handshake failure on the first client connection.
fn load_identity(tls: &TlsConfig) -> Result<Identity> {
    let cert = std::fs::read(&tls.cert_path)
        .with_context(|| format!("reading TLS certificate {}", tls.cert_path))?;
    let key = std::fs::read(&tls.key_path)
        .with_context(|| format!("reading TLS private key {}", tls.key_path))?;
    Ok(Identity::from_pem(cert, key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tls_cfg(cert: &std::path::Path, key: &std::path::Path) -> TlsConfig {
        TlsConfig {
            enabled: true,
            cert_path: cert.display().to_string(),
            key_path: key.display().to_string(),
        }
    }

    #[test]
    fn load_identity_reads_both_files() {
        let tmp = TempDir::new().unwrap();
        let cert = tmp.path().join("tls.crt");
        let key = tmp.path().join("tls.key");
        std::fs::write(&cert, b"-----BEGIN CERTIFICATE-----\n").unwrap();
        std::fs::write(&key, b"-----BEGIN PRIVATE KEY-----\n").unwrap();

        assert!(load_identity(&tls_cfg(&cert, &key)).is_ok());
    }

    /// A missing certificate must fail startup with the path in the message,
    /// not at handshake time on someone's first connection.
    #[test]
    fn load_identity_names_the_missing_certificate() {
        let tmp = TempDir::new().unwrap();
        let cert = tmp.path().join("absent.crt");
        let key = tmp.path().join("tls.key");
        std::fs::write(&key, b"-----BEGIN PRIVATE KEY-----\n").unwrap();

        let err = load_identity(&tls_cfg(&cert, &key)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("absent.crt"), "path missing from error: {msg}");
    }

    #[test]
    fn load_identity_names_the_missing_key() {
        let tmp = TempDir::new().unwrap();
        let cert = tmp.path().join("tls.crt");
        let key = tmp.path().join("absent.key");
        std::fs::write(&cert, b"-----BEGIN CERTIFICATE-----\n").unwrap();

        let err = load_identity(&tls_cfg(&cert, &key)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("absent.key"), "path missing from error: {msg}");
    }
}
