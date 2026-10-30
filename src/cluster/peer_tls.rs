// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Optional mutual TLS for the bounded Raft peer transport.
use std::io::{self, Read};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct PeerTlsOptions {
    pub ca_certificate: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

impl PeerTlsOptions {
    /// Read an all-or-none CA/certificate/key path set for a Raft environment prefix.
    pub fn from_env(prefix: &str) -> io::Result<Option<Self>> {
        let paths: Vec<_> = ["TLS_CA_CERT", "TLS_CERT", "TLS_KEY"]
            .iter()
            .map(|suffix| std::env::var_os(format!("{prefix}_{suffix}")))
            .collect();
        Self::from_paths(paths)
    }
    fn from_paths(paths: Vec<Option<std::ffi::OsString>>) -> io::Result<Option<Self>> {
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

#[derive(Clone, Default)]
pub(super) enum PeerTransport {
    #[default]
    Plain,
    #[cfg(feature = "peer-tls")]
    Tls(std::sync::Arc<TlsConfig>),
}

#[cfg(feature = "peer-tls")]
pub(super) struct TlsConfig {
    client: std::sync::Arc<rustls::ClientConfig>,
    server: std::sync::Arc<rustls::ServerConfig>,
}

impl PeerTransport {
    pub(super) fn load(options: Option<&PeerTlsOptions>) -> io::Result<Self> {
        let Some(options) = options else {
            return Ok(Self::Plain);
        };
        #[cfg(not(feature = "peer-tls"))]
        {
            let _ = options;
            Err(invalid(
                "peer TLS configuration requires the peer-tls feature",
            ))
        }
        #[cfg(feature = "peer-tls")]
        {
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
            let certs =
                rustls_pemfile::certs(&mut cert.as_slice()).collect::<io::Result<Vec<_>>>()?;
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
            client.alpn_protocols = vec![b"matrixobject-raft-v1".to_vec()];
            server.alpn_protocols = client.alpn_protocols.clone();
            Ok(Self::Tls(Arc::new(TlsConfig {
                client: Arc::new(client),
                server: Arc::new(server),
            })))
        }
    }

    /// Once this directory has used TLS, missing TLS configuration cannot downgrade it.
    pub(super) fn pin_directory(&self, directory: &Path) -> io::Result<()> {
        let path = directory.join("peer.transport");
        let secure = !matches!(self, Self::Plain);
        match std::fs::read(&path) {
            Ok(bytes) if bytes == b"tls-v1\n" && secure => Ok(()),
            Ok(_) => Err(invalid("Raft state requires valid peer TLS configuration")),
            Err(error) if error.kind() == io::ErrorKind::NotFound && !secure => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                use std::io::Write;
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)?;
                file.write_all(b"tls-v1\n")?;
                file.sync_all()?;
                #[cfg(unix)]
                std::fs::File::open(directory)?.sync_all()?;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn send(
        &self,
        stream: &mut TcpStream,
        address: &SocketAddr,
        bytes: &[u8],
        deadline: Instant,
    ) -> io::Result<()> {
        #[cfg(not(feature = "peer-tls"))]
        let _ = address;
        match self {
            Self::Plain => {
                super::peer_delivery::write_until(
                    stream,
                    &(bytes.len() as u32).to_be_bytes(),
                    deadline,
                )?;
                super::peer_delivery::write_until(stream, bytes, deadline)
            }
            #[cfg(feature = "peer-tls")]
            Self::Tls(config) => send_tls(config, stream, address, bytes, deadline),
        }
    }
}

#[cfg(feature = "peer-tls")]
fn send_tls(
    config: &TlsConfig,
    stream: &mut TcpStream,
    address: &SocketAddr,
    bytes: &[u8],
    deadline: Instant,
) -> io::Result<()> {
    use std::io::Write;
    use super::peer_delivery::remaining;
    let name = rustls::pki_types::ServerName::IpAddress(address.ip().into());
    let mut connection =
        rustls::ClientConnection::new(config.client.clone(), name).map_err(invalid)?;
    connection.set_buffer_limit(Some(64 * 1024));
    fn flush(
        connection: &mut rustls::ClientConnection,
        stream: &mut TcpStream,
        deadline: Instant,
    ) -> io::Result<()> {
        while connection.wants_write() {
            stream.set_write_timeout(Some(remaining(deadline)?))?;
            match connection.write_tls(stream) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
    while connection.is_handshaking() {
        flush(&mut connection, stream, deadline)?;
        if !connection.is_handshaking() {
            break;
        }
        stream.set_read_timeout(Some(remaining(deadline)?))?;
        match connection.read_tls(stream) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(_) => {
                connection.process_new_packets().map_err(invalid)?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    flush(&mut connection, stream, deadline)?;
    if connection.alpn_protocol() != Some(&b"matrixobject-raft-v1"[..]) {
        return Err(invalid("peer TLS application protocol was not negotiated"));
    }
    for part in [&(bytes.len() as u32).to_be_bytes()[..], bytes] {
        for block in part.chunks(16 * 1024) {
            remaining(deadline)?;
            connection.writer().write_all(block)?;
            flush(&mut connection, stream, deadline)?;
        }
    }
    connection.send_close_notify();
    flush(&mut connection, stream, deadline)
}

pub(super) struct PeerSocket {
    pub(super) socket: TcpStream,
    #[cfg(feature = "peer-tls")]
    tls: Option<rustls::ServerConnection>,
}
impl From<TcpStream> for PeerSocket {
    fn from(socket: TcpStream) -> Self {
        Self {
            socket,
            #[cfg(feature = "peer-tls")]
            tls: None,
        }
    }
}
impl PeerSocket {
    pub(super) fn new(socket: TcpStream, transport: &PeerTransport) -> io::Result<Self> {
        #[allow(unused_mut)]
        let mut result = Self::from(socket);
        #[cfg(not(feature = "peer-tls"))]
        let _ = transport;
        #[cfg(feature = "peer-tls")]
        if let PeerTransport::Tls(config) = transport {
            let mut connection =
                rustls::ServerConnection::new(config.server.clone()).map_err(invalid)?;
            connection.set_buffer_limit(Some(64 * 1024));
            result.tls = Some(connection);
        }
        Ok(result)
    }
    pub(super) fn wants_write(&self) -> bool {
        #[cfg(feature = "peer-tls")]
        if let Some(connection) = &self.tls {
            return connection.wants_write();
        }
        false
    }
}
impl Read for PeerSocket {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        #[cfg(feature = "peer-tls")]
        if let Some(connection) = &mut self.tls {
            // Flush at most one bounded TLS buffer per sweep, retaining socket readiness.
            if connection.wants_write() {
                if connection.write_tls(&mut self.socket)? == 0 {
                    return Err(io::ErrorKind::WriteZero.into());
                }
                if connection.wants_write() {
                    return Err(io::ErrorKind::WouldBlock.into());
                }
            }
            if !connection.is_handshaking()
                && connection.alpn_protocol() != Some(&b"matrixobject-raft-v1"[..])
            {
                return Err(invalid("peer TLS application protocol was not negotiated"));
            }
            match connection.reader().read(bytes) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                result => return result,
            }
            if connection.read_tls(&mut (&mut self.socket).take(64 * 1024))? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            connection.process_new_packets().map_err(invalid)?;
            if !connection.is_handshaking()
                && connection.alpn_protocol() != Some(&b"matrixobject-raft-v1"[..])
            {
                return Err(invalid("peer TLS application protocol was not negotiated"));
            }
            // Application bytes cannot be delivered until client authentication completes.
            connection.reader().read(bytes)
        } else {
            self.socket.read(bytes)
        }
        #[cfg(not(feature = "peer-tls"))]
        self.socket.read(bytes)
    }
}

#[cfg(all(test, feature = "peer-tls", target_os = "linux"))]
#[path = "../../tests/node_regime/tls.rs"]
mod tls_test_material;

#[cfg(all(test, feature = "peer-tls", target_os = "linux"))]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;
    use std::sync::{mpsc, Arc};
    use std::time::Duration;
    use tls_test_material::TlsFiles;

    fn options(files: &TlsFiles, name: &str) -> PeerTlsOptions {
        PeerTlsOptions {
            ca_certificate: files.path("ca.pem"),
            certificate: files.path(format!("{name}.pem")),
            private_key: files.path(format!("{name}.key")),
        }
    }
    fn receiver(
        transport: PeerTransport,
    ) -> (
        SocketAddr,
        mpsc::Receiver<Vec<u8>>,
        Arc<super::super::shutdown::ServerShutdown>,
        std::thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(super::super::shutdown::ServerShutdown::default());
        let worker_stop = stop.clone();
        let (tx, rx) = mpsc::sync_channel(8);
        let worker = std::thread::spawn(move || {
            super::super::peer_receiver::receive_with_transport(
                listener,
                tx,
                &worker_stop,
                Duration::from_secs(2),
                None,
                &transport,
            )
        });
        (address, rx, stop, worker)
    }
    fn deliver(transport: &PeerTransport, address: &SocketAddr, bytes: &[u8]) -> io::Result<()> {
        super::super::peer_delivery::PeerDelivery::new(bytes.to_vec(), Duration::from_secs(2))
            .send_with_transport(address, transport)
    }
    #[test]
    fn peer_tls_requires_trusted_client_certificates_and_preserves_frames() {
        let files = TlsFiles::new();
        let good = PeerTransport::load(Some(&options(&files, "peer"))).unwrap();
        let (address, received, stop, worker) = receiver(good.clone());
        let bytes: Vec<_> = (0..512 * 1024).map(|n| (n % 251) as u8).collect();
        deliver(&good, &address, &bytes).unwrap();
        assert_eq!(
            received.recv_timeout(Duration::from_secs(3)).unwrap(),
            bytes
        );
        // A plaintext frame cannot enter an authenticated receiver.
        deliver(&PeerTransport::Plain, &address, b"plain").unwrap();
        assert!(received.recv_timeout(Duration::from_millis(200)).is_err());
        // Trust the server CA, but present a client identity signed by another CA.
        let stranger = TlsFiles::new();
        let mut foreign = options(&stranger, "peer");
        foreign.ca_certificate = files.path("ca.pem");
        let foreign = PeerTransport::load(Some(&foreign)).unwrap();
        let _ = deliver(&foreign, &address, b"foreign-client");
        assert!(received.recv_timeout(Duration::from_millis(200)).is_err());
        // A client with no identity must also fail authentication.
        let mut roots = rustls::RootCertStore::empty();
        for cert in
            rustls_pemfile::certs(&mut std::fs::read(files.path("ca.pem")).unwrap().as_slice())
        {
            roots.add(cert.unwrap()).unwrap();
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut anonymous = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        anonymous.alpn_protocols = vec![b"matrixobject-raft-v1".to_vec()];
        let mut connection = rustls::ClientConnection::new(
            Arc::new(anonymous),
            rustls::pki_types::ServerName::IpAddress(address.ip().into()),
        )
        .unwrap();
        let mut socket = TcpStream::connect(address).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let _ = rustls::Stream::new(&mut connection, &mut socket).write_all(b"\0\0\0\x04anon");
        drop(socket);
        assert!(received.recv_timeout(Duration::from_millis(200)).is_err());
        // A trusted identity without the peer application protocol is insufficient.
        let PeerTransport::Tls(config) = &good else {
            unreachable!()
        };
        let mut no_protocol = (*config.client).clone();
        no_protocol.alpn_protocols.clear();
        let mut connection = rustls::ClientConnection::new(
            Arc::new(no_protocol),
            rustls::pki_types::ServerName::IpAddress(address.ip().into()),
        )
        .unwrap();
        let mut socket = TcpStream::connect(address).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let _ = rustls::Stream::new(&mut connection, &mut socket).write_all(b"\0\0\0\x04alpn");
        drop(socket);
        assert!(received.recv_timeout(Duration::from_millis(200)).is_err());
        deliver(&good, &address, b"after-refusals").unwrap();
        assert_eq!(
            received.recv_timeout(Duration::from_secs(3)).unwrap(),
            b"after-refusals"
        );
        stop.stop();
        worker.join().unwrap();
    }
    #[test]
    fn peer_tls_checks_server_address_trust_and_prevents_plaintext_downgrade() {
        let files = TlsFiles::new();
        files.issue("wrong-address", "192.0.2.1");
        let good = PeerTransport::load(Some(&options(&files, "peer"))).unwrap();
        let wrong_address = PeerTransport::load(Some(&options(&files, "wrong-address"))).unwrap();
        let (address, received, stop, worker) = receiver(wrong_address);
        assert!(deliver(&good, &address, b"wrong-name").is_err());
        assert!(received.recv_timeout(Duration::from_millis(200)).is_err());
        stop.stop();
        worker.join().unwrap();
        let stranger = TlsFiles::new();
        let foreign = PeerTransport::load(Some(&options(&stranger, "peer"))).unwrap();
        let (address, received, stop, worker) = receiver(foreign);
        assert!(deliver(&good, &address, b"wrong-ca").is_err());
        assert!(received.recv_timeout(Duration::from_millis(200)).is_err());
        stop.stop();
        worker.join().unwrap();
        PeerTransport::Plain.pin_directory(&files.root).unwrap();
        good.pin_directory(&files.root).unwrap();
        good.pin_directory(&files.root).unwrap();
        assert!(PeerTransport::Plain.pin_directory(&files.root).is_err());
        assert_eq!(
            std::fs::read(files.path("peer.transport")).unwrap(),
            b"tls-v1\n"
        );
        std::fs::write(files.path("peer.transport"), b"").unwrap();
        assert!(good.pin_directory(&files.root).is_err());
        let mut missing = options(&files, "peer");
        missing.private_key = files.path("absent.key");
        assert!(PeerTransport::load(Some(&missing)).is_err());
    }
    #[test]
    fn peer_tls_stalled_handshakes_keep_delivery_and_shutdown_bounded() {
        let files = TlsFiles::new();
        let good = PeerTransport::load(Some(&options(&files, "peer"))).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let (release, released) = mpsc::channel();
        let stalled = std::thread::spawn(move || {
            let until = Instant::now() + Duration::from_secs(3);
            let socket = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error)
                        if error.kind() == io::ErrorKind::WouldBlock && Instant::now() < until =>
                    {
                        std::thread::sleep(Duration::from_millis(1))
                    }
                    other => panic!("stalled TLS listener: {other:?}"),
                }
            };
            released.recv_timeout(Duration::from_secs(3)).unwrap();
            drop(socket);
        });
        let began = Instant::now();
        let error = super::super::peer_delivery::PeerDelivery::new(
            b"deadline".to_vec(),
            Duration::from_millis(100),
        )
        .send_with_transport(&address, &good)
        .unwrap_err();
        assert!(
            matches!(
                error.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            ),
            "{error}"
        );
        assert!(began.elapsed() < Duration::from_secs(2));
        release.send(()).unwrap();
        stalled.join().unwrap();
        let (address, received, stop, worker) = receiver(good);
        let mut stalled = TcpStream::connect(address).unwrap();
        stalled.write_all(&[22, 3]).unwrap();
        let began = Instant::now();
        stop.stop();
        worker.join().unwrap();
        assert!(began.elapsed() < Duration::from_secs(1));
        assert!(received.try_recv().is_err());
    }
}

#[cfg(test)]
mod configuration_tests {
    use super::*;
    #[test]
    fn peer_tls_requires_complete_settings_and_a_supported_build() {
        assert!(PeerTlsOptions::from_paths(vec![None, None, None])
            .unwrap()
            .is_none());
        for paths in [
            vec![None, None],
            vec![Some("ca.pem".into()), None, None],
            vec![
                Some("ca.pem".into()),
                Some("cert.pem".into()),
                Some("".into()),
            ],
        ] {
            assert!(PeerTlsOptions::from_paths(paths).is_err());
        }
        let missing = std::env::temp_dir().join(format!(
            "absent-peer-tls-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        assert!(!missing.exists());
        let options = PeerTlsOptions {
            ca_certificate: missing.clone(),
            certificate: missing.clone(),
            private_key: missing,
        };
        let error = PeerTransport::load(Some(&options))
            .err()
            .expect("configuration must fail");
        if cfg!(feature = "peer-tls") {
            assert_eq!(error.kind(), io::ErrorKind::NotFound);
        } else {
            assert_eq!(
                error.to_string(),
                "peer TLS configuration requires the peer-tls feature"
            );
        }
    }
}
