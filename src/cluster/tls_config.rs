// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Shared certificate loading for authenticated storage transports.
use std::io;
use std::path::PathBuf;
#[cfg(feature = "peer-tls")]
use std::{io::Read, path::Path};

#[derive(Clone, Debug)]
pub struct TlsOptions {
    pub ca_certificate: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

impl TlsOptions {
    /// Read an all-or-none CA/certificate/key path set for an environment prefix.
    pub fn from_env(prefix: &str) -> io::Result<Option<Self>> {
        let paths: Vec<_> = ["TLS_CA_CERT", "TLS_CERT", "TLS_KEY"]
            .iter()
            .map(|suffix| std::env::var_os(format!("{prefix}_{suffix}")))
            .collect();
        Self::from_paths(paths)
    }
    pub(crate) fn from_paths(paths: Vec<Option<std::ffi::OsString>>) -> io::Result<Option<Self>> {
        if paths.len() != 3 {
            return Err(invalid("peer TLS requires three path settings"));
        }
        if paths.iter().all(Option::is_none) {
            return Ok(None);
        }
        if paths
            .iter()
            .any(|path| path.as_ref().map_or(true, |p| p.is_empty()))
        {
            return Err(invalid(
                "peer TLS requires CA certificate, certificate and key paths",
            ));
        }
        Ok(Some(Self {
            ca_certificate: paths[0].clone().unwrap().into(),
            certificate: paths[1].clone().unwrap().into(),
            private_key: paths[2].clone().unwrap().into(),
        }))
    }
}

fn invalid(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(feature = "peer-tls")]
pub(super) struct TlsConfig {
    pub(super) client: std::sync::Arc<rustls::ClientConfig>,
    pub(super) server: std::sync::Arc<rustls::ServerConfig>,
}

#[cfg(feature = "peer-tls")]
impl TlsConfig {
    pub(super) fn load(options: &TlsOptions, protocol: &[u8]) -> io::Result<Self> {
        use std::sync::Arc;
        fn bounded_file(path: &Path) -> io::Result<Vec<u8>> {
            let mut bytes = Vec::new();
            std::fs::File::open(path)?
                .take(1024 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            if bytes.is_empty() || bytes.len() > 1024 * 1024 {
                return Err(invalid("peer TLS file must contain 1..1048576 bytes"));
            }
            Ok(bytes)
        }
        let ca = bounded_file(&options.ca_certificate)?;
        let cert = bounded_file(&options.certificate)?;
        let key = bounded_file(&options.private_key)?;
        let mut roots = rustls::RootCertStore::empty();
        for certificate in rustls_pemfile::certs(&mut ca.as_slice()) {
            roots.add(certificate?).map_err(invalid)?;
        }
        if roots.is_empty() {
            return Err(invalid("peer TLS CA file contains no certificates"));
        }
        let certs = rustls_pemfile::certs(&mut cert.as_slice()).collect::<io::Result<Vec<_>>>()?;
        let key = rustls_pemfile::private_key(&mut key.as_slice())?
            .ok_or_else(|| invalid("peer TLS key file contains no private key"))?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots.clone()),
            Arc::clone(&provider),
        )
        .build()
        .map_err(invalid)?;
        let mut server = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(invalid)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs.clone(), key.clone_key())
            .map_err(invalid)?;
        server.send_tls13_tickets = 0;
        server.max_early_data_size = 0;
        let mut client = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(invalid)?
            .with_root_certificates(roots)
            .with_client_auth_cert(certs, key)
            .map_err(invalid)?;
        client.enable_early_data = false;
        client.resumption = rustls::client::Resumption::disabled();
        client.alpn_protocols = vec![protocol.to_vec()];
        server.alpn_protocols = client.alpn_protocols.clone();
        Ok(Self {
            client: Arc::new(client),
            server: Arc::new(server),
        })
    }
}

#[cfg(all(test, feature = "peer-tls", target_os = "linux"))]
#[path = "../../tests/node_regime/tls.rs"]
pub(super) mod tls_test_material;

#[cfg(all(test, feature = "peer-tls", target_os = "linux"))]
mod tests {
    use super::*;

    fn handshake(client: &TlsConfig, server: &TlsConfig) -> Result<(), rustls::Error> {
        let mut client = rustls::ClientConnection::new(
            client.client.clone(),
            rustls::pki_types::ServerName::IpAddress(
                "127.0.0.1".parse::<std::net::IpAddr>().unwrap().into(),
            ),
        )
        .unwrap();
        let mut server = rustls::ServerConnection::new(server.server.clone()).unwrap();
        for _ in 0..16 {
            while client.wants_write() {
                let mut bytes = Vec::new();
                client.write_tls(&mut bytes).unwrap();
                server.read_tls(&mut bytes.as_slice()).unwrap();
                server.process_new_packets()?;
            }
            while server.wants_write() {
                let mut bytes = Vec::new();
                server.write_tls(&mut bytes).unwrap();
                client.read_tls(&mut bytes.as_slice()).unwrap();
                client.process_new_packets()?;
            }
            if !client.is_handshaking() && !server.is_handshaking() {
                assert_eq!(client.alpn_protocol(), server.alpn_protocol());
                assert!(client.peer_certificates().is_some());
                assert!(server.peer_certificates().is_some());
                return Ok(());
            }
        }
        panic!("TLS handshake did not finish within the exchange bound");
    }

    #[test]
    fn shared_credentials_do_not_allow_cross_protocol_handshakes() {
        let files = tls_test_material::TlsFiles::new();
        let options = TlsOptions {
            ca_certificate: files.path("ca.pem"),
            certificate: files.path("peer.pem"),
            private_key: files.path("peer.key"),
        };
        let peer = TlsConfig::load(&options, b"matrixobject-raft-v1").unwrap();
        let api = TlsConfig::load(&options, b"matrixobject-api-v1").unwrap();
        handshake(&peer, &peer).unwrap();
        handshake(&api, &api).unwrap();
        assert!(handshake(&peer, &api).is_err());
        assert!(handshake(&api, &peer).is_err());
    }
}
