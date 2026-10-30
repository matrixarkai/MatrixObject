// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! A single delivery deadline includes queueing, connection and frame writes.
use std::io::{self, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

pub(super) struct PeerDelivery {
    bytes: Vec<u8>,
    deadline: Instant,
}
impl PeerDelivery {
    pub(super) fn new(bytes: Vec<u8>, timeout: Duration) -> Self {
        Self {
            bytes,
            deadline: Instant::now() + timeout,
        }
    }
    pub(super) fn send(self, address: &SocketAddr) -> io::Result<()> {
        let mut stream = TcpStream::connect_timeout(address, remaining(self.deadline)?)?;
        stream.set_nodelay(true)?;
        write_until(
            &mut stream,
            &(self.bytes.len() as u32).to_be_bytes(),
            self.deadline,
        )?;
        write_until(&mut stream, &self.bytes, self.deadline)
    }
}
fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "peer packet deadline"))
}
fn write_until(stream: &mut TcpStream, mut bytes: &[u8], deadline: Instant) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.set_write_timeout(Some(remaining(deadline)?))?;
        match stream.write(bytes) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "incomplete peer packet",
                ))
            }
            Ok(n) => bytes = &bytes[n..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpListener;
    use std::sync::mpsc;

    #[test]
    fn queued_peer_delivery_expires_before_connect_and_preserves_fresh_frames() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::sync_channel(8);
        // Queue age is part of the budget; dequeuing must not renew it.
        let old = PeerDelivery {
            bytes: b"old".to_vec(),
            deadline: Instant::now() - Duration::from_secs(1),
        };
        tx.send(old).unwrap();
        assert_eq!(
            rx.recv().unwrap().send(&address).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        tx.send(PeerDelivery::new(b"fresh".to_vec(), Duration::from_secs(2)))
            .unwrap();
        rx.recv().unwrap().send(&address).unwrap();
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut frame = Vec::new();
        stream.read_to_end(&mut frame).unwrap();
        assert_eq!(frame, [0, 0, 0, 5, b'f', b'r', b'e', b's', b'h']);
        // A connection obtained earlier also cannot renew an expired write budget.
        assert_eq!(
            write_until(
                &mut stream,
                b"late",
                Instant::now() - Duration::from_secs(1)
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::TimedOut
        );
    }
}
