// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Mutual TLS for bounded storage API connections.
pub use super::tls_config::TlsOptions as ApiTlsOptions;
use std::cell::Cell;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
#[cfg(feature = "peer-tls")]
use std::sync::Mutex;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

#[cfg(feature = "peer-tls")]
const PROTOCOL: &[u8] = b"matrixobject-api-v1";

#[derive(Clone)]
pub struct ApiTlsConfig {
    _private: (),
    #[cfg(feature = "peer-tls")]
    config: Arc<super::tls_config::TlsConfig>,
}
impl std::fmt::Debug for ApiTlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ApiTlsConfig { authenticated: true }")
    }
}
impl ApiTlsConfig {
    pub fn load(options: &ApiTlsOptions) -> io::Result<Self> {
        #[cfg(feature = "peer-tls")]
        {
            Ok(Self {
                _private: (),
                config: Arc::new(super::tls_config::TlsConfig::load(options, PROTOCOL)?),
            })
        }
        #[cfg(not(feature = "peer-tls"))]
        {
            let _ = options;
            Err(invalid("API TLS requires the api-tls or peer-tls feature"))
        }
    }
    pub fn from_env(prefix: &str) -> io::Result<Option<Self>> {
        ApiTlsOptions::from_env(prefix)?
            .as_ref()
            .map(Self::load)
            .transpose()
    }
}
pub(super) fn pin_file(path: &std::path::Path, secure: bool) -> io::Result<()> {
    match std::fs::File::open(path) {
        Ok(file) => {
            let mut bytes = Vec::new();
            file.take(16).read_to_end(&mut bytes)?;
            if secure && bytes == b"tls-v1\n" {
                Ok(())
            } else {
                Err(invalid(
                    "stored API transport requires valid TLS configuration",
                ))
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound && !secure => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?;
            file.write_all(b"tls-v1\n")?;
            file.sync_all()?;
            #[cfg(unix)]
            std::fs::File::open(
                path.parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or_else(|| std::path::Path::new(".")),
            )?
            .sync_all()?;
            Ok(())
        }
        Err(error) => Err(error),
    }
}
pub(super) fn journal_pin(path: &std::path::Path) -> std::path::PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".api.transport");
    name.into()
}

fn invalid(message: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}
#[cfg(feature = "peer-tls")]
fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|left| !left.is_zero())
        .ok_or_else(|| io::ErrorKind::TimedOut.into())
}

pub(super) struct ApiSocket {
    socket: TcpStream,
    nonblocking: Arc<AtomicBool>,
    read_deadline: Cell<Option<Instant>>,
    write_deadline: Cell<Option<Instant>>,
    #[cfg(feature = "peer-tls")]
    tls: Option<Arc<Mutex<rustls::Connection>>>,
}
impl std::fmt::Debug for ApiSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiSocket")
            .field("peer", &self.socket.peer_addr())
            .finish()
    }
}
impl ApiSocket {
    fn plain(socket: TcpStream, nonblocking: bool) -> Self {
        Self {
            socket,
            nonblocking: Arc::new(AtomicBool::new(nonblocking)),
            read_deadline: Cell::new(None),
            write_deadline: Cell::new(None),
            #[cfg(feature = "peer-tls")]
            tls: None,
        }
    }
    pub(super) fn client(
        socket: TcpStream,
        address: SocketAddr,
        config: Option<&ApiTlsConfig>,
        deadline: Instant,
    ) -> io::Result<Self> {
        #[allow(unused_mut)]
        let mut result = Self::plain(socket, false);
        let Some(config) = config else {
            return Ok(result);
        };
        #[cfg(not(feature = "peer-tls"))]
        {
            let _ = (config, address, deadline);
            Err(invalid("API TLS is unavailable"))
        }
        #[cfg(feature = "peer-tls")]
        {
            let mut connection = rustls::ClientConnection::new(
                config.config.client.clone(),
                rustls::pki_types::ServerName::IpAddress(address.ip().into()),
            )
            .map_err(invalid)?;
            connection.set_buffer_limit(Some(64 * 1024));
            while connection.is_handshaking() || connection.wants_write() {
                while connection.wants_write() {
                    result
                        .socket
                        .set_write_timeout(Some(remaining(deadline)?))?;
                    if connection.write_tls(&mut result.socket)? == 0 {
                        return Err(io::ErrorKind::WriteZero.into());
                    }
                }
                if !connection.is_handshaking() {
                    break;
                }
                result.socket.set_read_timeout(Some(remaining(deadline)?))?;
                if connection.read_tls(&mut result.socket)? == 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                connection.process_new_packets().map_err(invalid)?;
            }
            if connection.alpn_protocol() != Some(PROTOCOL) {
                return Err(invalid("API TLS protocol was not negotiated"));
            }
            result.tls = Some(Arc::new(Mutex::new(rustls::Connection::Client(connection))));
            Ok(result)
        }
    }
    pub(super) fn server(socket: TcpStream, config: Option<&ApiTlsConfig>) -> io::Result<Self> {
        #[allow(unused_mut)]
        let mut result = Self::plain(socket, true);
        if let Some(config) = config {
            #[cfg(not(feature = "peer-tls"))]
            {
                let _ = config;
                return Err(invalid("API TLS is unavailable"));
            }
            #[cfg(feature = "peer-tls")]
            {
                let mut connection =
                    rustls::ServerConnection::new(config.config.server.clone()).map_err(invalid)?;
                connection.set_buffer_limit(Some(64 * 1024));
                result.tls = Some(Arc::new(Mutex::new(rustls::Connection::Server(connection))));
            }
        }
        Ok(result)
    }
    pub(super) fn raw(&self) -> &TcpStream {
        &self.socket
    }
    pub(super) fn wants_write(&self) -> bool {
        #[cfg(feature = "peer-tls")]
        if let Some(tls) = &self.tls {
            return tls.lock().unwrap_or_else(|e| e.into_inner()).wants_write();
        }
        false
    }
    pub(super) fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            socket: self.socket.try_clone()?,
            nonblocking: self.nonblocking.clone(),
            read_deadline: Cell::new(self.read_deadline.get()),
            write_deadline: Cell::new(self.write_deadline.get()),
            #[cfg(feature = "peer-tls")]
            tls: self.tls.clone(),
        })
    }
    pub(super) fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.socket.set_read_timeout(timeout)?;
        self.read_deadline
            .set(timeout.and_then(|value| Instant::now().checked_add(value)));
        Ok(())
    }
    pub(super) fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.socket.set_write_timeout(timeout)?;
        self.write_deadline
            .set(timeout.and_then(|value| Instant::now().checked_add(value)));
        Ok(())
    }
    pub(super) fn set_nonblocking(&self, enabled: bool) -> io::Result<()> {
        self.socket.set_nonblocking(enabled)?;
        self.nonblocking.store(enabled, Ordering::Release);
        Ok(())
    }
    pub(super) fn set_nodelay(&self, enabled: bool) -> io::Result<()> {
        self.socket.set_nodelay(enabled)
    }
    pub(super) fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.socket.shutdown(how)
    }
    pub(super) fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
    pub(super) fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.socket.peer_addr()
    }
    #[cfg(feature = "peer-tls")]
    fn write_budget(&self, deadline: Option<Instant>) -> io::Result<()> {
        if !self.nonblocking.load(Ordering::Acquire) {
            if let Some(deadline) = deadline {
                self.socket.set_write_timeout(Some(remaining(deadline)?))?;
            }
        }
        Ok(())
    }
}
impl Read for ApiSocket {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        #[cfg(feature = "peer-tls")]
        if let Some(tls) = &self.tls {
            let mut connection = tls.lock().map_err(|_| invalid("API TLS state poisoned"))?;
            if connection.wants_write() {
                self.write_budget(self.read_deadline.get())?;
                if connection.write_tls(&mut self.socket)? == 0 {
                    return Err(io::ErrorKind::WriteZero.into());
                }
                if connection.wants_write() {
                    return Err(io::ErrorKind::WouldBlock.into());
                }
            }
            if !connection.is_handshaking() && connection.alpn_protocol() != Some(PROTOCOL) {
                return Err(invalid("API TLS protocol was not negotiated"));
            }
            match connection.reader().read(bytes) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                result => return result,
            }
            if !self.nonblocking.load(Ordering::Acquire) {
                if let Some(deadline) = self.read_deadline.get() {
                    self.socket.set_read_timeout(Some(remaining(deadline)?))?;
                }
            }
            if connection.read_tls(&mut (&mut self.socket).take(64 * 1024))? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            connection.process_new_packets().map_err(invalid)?;
            if !connection.is_handshaking() && connection.alpn_protocol() != Some(PROTOCOL) {
                return Err(invalid("API TLS protocol was not negotiated"));
            }
            connection.reader().read(bytes)
        } else {
            self.socket.read(bytes)
        }
        #[cfg(not(feature = "peer-tls"))]
        {
            self.socket.read(bytes)
        }
    }
}
impl Write for ApiSocket {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.flush()?;
        #[cfg(feature = "peer-tls")]
        if let Some(tls) = &self.tls {
            let mut connection = tls.lock().map_err(|_| invalid("API TLS state poisoned"))?;
            if connection.is_handshaking() || connection.alpn_protocol() != Some(PROTOCOL) {
                return Err(invalid("API TLS is not authenticated"));
            }
            return connection
                .writer()
                .write(&bytes[..bytes.len().min(64 * 1024)]);
        }
        self.socket.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        #[cfg(feature = "peer-tls")]
        if let Some(tls) = &self.tls {
            let mut connection = tls.lock().map_err(|_| invalid("API TLS state poisoned"))?;
            if connection.wants_write() {
                self.write_budget(self.write_deadline.get())?;
                if connection.write_tls(&mut self.socket)? == 0 {
                    return Err(io::ErrorKind::WriteZero.into());
                }
                if connection.wants_write() {
                    return Err(io::ErrorKind::WouldBlock.into());
                }
            }
        }
        Ok(())
    }
}

#[cfg(all(
    test,
    feature = "peer-tls",
    feature = "replica-server",
    feature = "meta-server",
    target_os = "linux"
))]
mod tests {
    use super::super::tls_config::tls_test_material::TlsFiles;
    use super::*;
    use crate::cluster::*;
    struct Running {
        stop: Box<dyn Fn()>,
        worker: Option<std::thread::JoinHandle<io::Result<()>>>,
    }
    impl Drop for Running {
        fn drop(&mut self) {
            (self.stop)();
            if let Some(worker) = self.worker.take() {
                worker.join().unwrap().unwrap();
            }
        }
    }
    fn config(files: &TlsFiles) -> ApiTlsConfig {
        ApiTlsConfig::load(&ApiTlsOptions {
            ca_certificate: files.path("ca.pem"),
            certificate: files.path("peer.pem"),
            private_key: files.path("peer.key"),
        })
        .unwrap()
    }
    fn replica(tls: &ApiTlsConfig) -> (String, Running) {
        let server = ReplicaNodeServer::bind(
            "127.0.0.1:0",
            ReplicaNodeOptions {
                tls: Some(tls.clone()),
                worker_threads: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let address = server.local_addr().unwrap().to_string();
        let stop = server.shutdown_handle();
        (
            address,
            Running {
                stop: Box::new(move || stop.stop()),
                worker: Some(std::thread::spawn(move || server.serve())),
            },
        )
    }
    fn options(tls: &ApiTlsConfig) -> ReplicaClientOptions {
        ReplicaClientOptions {
            tls: Some(tls.clone()),
            connect_timeout: Duration::from_secs(2),
            read_timeout: Duration::from_secs(2),
            write_timeout: Duration::from_secs(2),
            ..Default::default()
        }
    }
    #[test]
    fn api_tls_serves_persistent_metadata_and_star_data_connections() {
        let files = TlsFiles::new();
        let tls = config(&files);
        let nodes: Vec<_> = (0..3).map(|_| replica(&tls)).collect();
        let endpoints: Vec<_> = nodes.iter().map(|(endpoint, _)| endpoint.clone()).collect();
        let options = options(&tls);
        let mut set = RemoteReplicaSet::connect(&endpoints, 2, 2, &options).unwrap();
        let page = crate::PageId {
            chunk_id: crate::ChunkId::new(1, 1).unwrap(),
            page_index: 0,
        };
        let bytes: Vec<_> = (0..512 * 1024).map(|index| (index % 251) as u8).collect();
        assert_eq!(set.write_chunk(&page, 0, &bytes).unwrap(), 3);
        assert_eq!(
            set.read_chunk(&page, 0, bytes.len() as u64).unwrap().data,
            bytes
        );
        assert_eq!(set.write_chunk(&page, 0, b"changed").unwrap(), 3);
        assert_eq!(set.read_chunk(&page, 0, 7).unwrap().data, b"changed");
        let server = MetaNodeServer::bind(
            "127.0.0.1:0",
            MetaNodeOptions {
                tls: Some(tls.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        server.seed_data_nodes(1, 4096).unwrap();
        let endpoint = server.local_addr().unwrap().to_string();
        let stop = server.shutdown_handle();
        let _running = Running {
            stop: Box::new(move || stop.stop()),
            worker: Some(std::thread::spawn(move || server.serve())),
        };
        let mut client = RemoteMetaClient::connect(
            &endpoint,
            &MetaClientOptions {
                tls: Some(tls),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(client.list_placement_nodes().unwrap().len(), 1);
        assert!(matches!(
            client.read_manifest("bucket", "missing"),
            Err(crate::ObjectError::NotFound(_))
        ));
        assert_eq!(client.list_placement_nodes().unwrap().len(), 1);
    }

    #[test]
    fn api_tls_rejects_plaintext_and_foreign_clients_before_mutations() {
        let files = TlsFiles::new();
        let tls = config(&files);
        let (endpoint, _running) = replica(&tls);
        let page = crate::PageId {
            chunk_id: crate::ChunkId::new(1, 1).unwrap(),
            page_index: 0,
        };
        let plain = ReplicaClientOptions {
            read_timeout: Duration::from_millis(300),
            ..Default::default()
        };
        assert!(RemoteReplicaClient::connect(&endpoint, &plain)
            .and_then(|mut client| client.write_chunk(page.clone(), 0, b"plain"))
            .is_err());
        let foreign = TlsFiles::new();
        let stranger = ApiTlsConfig::load(&ApiTlsOptions {
            ca_certificate: files.path("ca.pem"),
            certificate: foreign.path("peer.pem"),
            private_key: foreign.path("peer.key"),
        })
        .unwrap();
        assert!(RemoteReplicaClient::connect(&endpoint, &options(&stranger))
            .and_then(|mut client| client.write_chunk(page, 0, b"foreign"))
            .is_err());
        let mut client = RemoteReplicaClient::connect(&endpoint, &options(&tls)).unwrap();
        assert_eq!(client.ping().unwrap(), 0);
    }

    #[test]
    fn api_tls_checks_server_identity_and_requires_client_certificates() {
        let files = TlsFiles::new();
        let tls = config(&files);
        let (endpoint, _running) = replica(&tls);
        let foreign = TlsFiles::new();
        let wrong_trust = ApiTlsConfig::load(&ApiTlsOptions {
            ca_certificate: foreign.path("ca.pem"),
            certificate: files.path("peer.pem"),
            private_key: files.path("peer.key"),
        })
        .unwrap();
        assert!(RemoteReplicaClient::connect(&endpoint, &options(&wrong_trust)).is_err());

        files.issue("other-address", "127.0.0.2");
        let wrong_address = ApiTlsConfig::load(&ApiTlsOptions {
            ca_certificate: files.path("ca.pem"),
            certificate: files.path("other-address.pem"),
            private_key: files.path("other-address.key"),
        })
        .unwrap();
        let (other_endpoint, _other_running) = replica(&wrong_address);
        assert!(RemoteReplicaClient::connect(&other_endpoint, &options(&tls)).is_err());

        let mut roots = rustls::RootCertStore::empty();
        let ca = std::fs::read(files.path("ca.pem")).unwrap();
        for certificate in rustls_pemfile::certs(&mut ca.as_slice()) {
            roots.add(certificate.unwrap()).unwrap();
        }
        let mut anonymous = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        anonymous.alpn_protocols = vec![PROTOCOL.to_vec()];
        let mut connection = rustls::ClientConnection::new(
            Arc::new(anonymous),
            rustls::pki_types::ServerName::IpAddress(
                "127.0.0.1".parse::<std::net::IpAddr>().unwrap().into(),
            ),
        )
        .unwrap();
        let mut socket = TcpStream::connect(&endpoint).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut stream = rustls::Stream::new(&mut connection, &mut socket);
        // A client can finish its half of TLS before seeing the server's refusal.
        // Either the write or the following read must fail without an API reply.
        let denied = stream.write_all(b"unauthenticated").is_err()
            || stream.read(&mut [0; 1]).map_or(true, |count| count == 0);
        assert!(denied, "anonymous client received application bytes");
        let mut client = RemoteReplicaClient::connect(&endpoint, &options(&tls)).unwrap();
        assert_eq!(client.ping().unwrap(), 0);
    }

    #[test]
    fn api_tls_bounds_stalled_handshakes_and_server_shutdown() {
        let files = TlsFiles::new();
        let tls = config(&files);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let began = Instant::now();
        assert!(RemoteReplicaClient::connect(
            &endpoint,
            &ReplicaClientOptions {
                connect_timeout: Duration::from_millis(100),
                ..options(&tls)
            }
        )
        .is_err());
        assert!(began.elapsed() < Duration::from_secs(1));
        let (endpoint, running) = replica(&tls);
        let mut socket = TcpStream::connect(endpoint).unwrap();
        socket.write_all(&[22, 3]).unwrap();
        let began = Instant::now();
        drop(running);
        assert!(began.elapsed() < Duration::from_secs(1));
    }
}
