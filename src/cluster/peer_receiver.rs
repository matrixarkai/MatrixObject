// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::shutdown::ServerShutdown;
use std::io::{self, Read};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::SyncSender;
use std::time::{Duration, Instant};

const PACKET_LIMIT: usize = 8 * 1024 * 1024;
const BUFFER_LIMIT: usize = 2 * PACKET_LIMIT;
const CONNECTION_LIMIT: usize = 16;
const READ_BUDGET: usize = 64 * 1024;

struct Frame {
    stream: TcpStream,
    deadline: Instant,
    header: [u8; 4],
    header_read: usize,
    body: Vec<u8>,
    body_read: usize,
}
impl Frame {
    fn advance(&mut self, reserved: &mut usize) -> io::Result<bool> {
        if Instant::now() >= self.deadline {
            return Err(io::ErrorKind::TimedOut.into());
        }
        if self.header_read < 4 {
            let n = self.stream.read(&mut self.header[self.header_read..])?;
            if n == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            self.header_read += n;
            if self.header_read < 4 {
                return Ok(false);
            }
            let length = u32::from_be_bytes(self.header) as usize;
            if length == 0 || length > PACKET_LIMIT || length > BUFFER_LIMIT - *reserved {
                return Err(io::ErrorKind::InvalidData.into());
            }
            self.body
                .try_reserve_exact(length)
                .map_err(|_| io::Error::other("peer buffer allocation failed"))?;
            self.body.resize(length, 0);
            *reserved += length;
        }
        let end = self.body.len().min(self.body_read + READ_BUDGET);
        let n = self.stream.read(&mut self.body[self.body_read..end])?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        self.body_read += n;
        Ok(self.body_read == self.body.len())
    }
}

// One bounded sweep per connection prevents a fragmented sender from holding
// the listener. Each connection still carries exactly one existing wire frame.
pub(super) fn receive(
    listener: TcpListener,
    incoming: SyncSender<Vec<u8>>,
    stop: &ServerShutdown,
    timeout: Duration,
) {
    let mut frames: Vec<Frame> = Vec::new();
    let mut reserved = 0;
    while !stop.is_stopped() {
        for _ in frames.len()..CONNECTION_LIMIT {
            match listener.accept() {
                Ok((stream, _)) => {
                    if stream.set_nonblocking(true).is_err() {
                        continue;
                    }
                    frames.push(Frame {
                        stream,
                        deadline: Instant::now() + timeout,
                        header: [0; 4],
                        header_read: 0,
                        body: Vec::new(),
                        body_read: 0,
                    });
                }
                Err(_) => break,
            }
        }
        let mut index = 0;
        let mut progressed = false;
        while index < frames.len() {
            let before = frames[index].header_read + frames[index].body_read;
            let result = frames[index].advance(&mut reserved);
            progressed |= before != frames[index].header_read + frames[index].body_read;
            match result {
                Ok(false) => index += 1,
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) =>
                {
                    index += 1
                }
                result => {
                    let frame = frames.swap_remove(index);
                    reserved -= frame.body.len();
                    if matches!(result, Ok(true)) {
                        let _ = incoming.try_send(frame.body);
                    }
                    progressed = true;
                }
            }
        }
        if !progressed {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{mpsc, Arc};

    #[test]
    fn incomplete_peer_frames_obey_total_buffer_and_absolute_deadline_limits() {
        fn pair() -> (TcpStream, Frame) {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let (stream, _) = listener.accept().unwrap();
            stream.set_nonblocking(true).unwrap();
            (
                client,
                Frame {
                    stream,
                    deadline: Instant::now() + Duration::from_secs(5),
                    header: [0; 4],
                    header_read: 0,
                    body: Vec::new(),
                    body_read: 0,
                },
            )
        }
        let mut reserved = 0;
        let mut held = Vec::new();
        for _ in 0..2 {
            let (mut client, mut frame) = pair();
            client
                .write_all(&(PACKET_LIMIT as u32).to_be_bytes())
                .unwrap();
            assert_eq!(
                frame.advance(&mut reserved).unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
            held.push((client, frame));
        }
        assert_eq!(reserved, BUFFER_LIMIT);
        let (mut client, mut refused) = pair();
        client.write_all(&1u32.to_be_bytes()).unwrap();
        assert_eq!(
            refused.advance(&mut reserved).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(refused.body.is_empty());
        assert_eq!(reserved, BUFFER_LIMIT);
        let (_, expired) = &mut held[0];
        expired.deadline = Instant::now();
        assert_eq!(
            expired.advance(&mut reserved).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        // The receive loop releases exactly the removed frame's reservation.
        reserved -= held.remove(0).1.body.len();
        let (mut client, mut admitted) = pair();
        client.write_all(&[0, 0, 0, 3, b'n', b'e', b'w']).unwrap();
        assert!(admitted.advance(&mut reserved).unwrap());
        assert_eq!(admitted.body, b"new");
        assert_eq!(reserved, PACKET_LIMIT + 3);
    }

    #[test]
    fn slow_peer_frames_do_not_block_healthy_packets_or_shutdown() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(ServerShutdown::default());
        let worker_stop = Arc::clone(&stop);
        let (tx, rx) = mpsc::sync_channel(8);
        let worker =
            std::thread::spawn(move || receive(listener, tx, &worker_stop, Duration::from_secs(2)));
        let result = std::panic::catch_unwind(|| {
            let mut slow = TcpStream::connect(address).unwrap();
            slow.write_all(&[0, 0]).unwrap();
            let mut partial = TcpStream::connect(address).unwrap();
            partial.write_all(&1024u32.to_be_bytes()).unwrap();
            partial.write_all(b"partial").unwrap();
            let mut valid = TcpStream::connect(address).unwrap();
            valid.write_all(&5u32.to_be_bytes()).unwrap();
            valid.write_all(b"ready").unwrap();
            assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), b"ready");
            // Zero, oversized and truncated frames must never become peer packets.
            for length in [0, PACKET_LIMIT as u32 + 1, 10] {
                let mut invalid = TcpStream::connect(address).unwrap();
                invalid.write_all(&length.to_be_bytes()).unwrap();
            }
            assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
            (slow, partial)
        });
        let begin = Instant::now();
        stop.stop();
        worker.join().unwrap();
        assert!(begin.elapsed() < Duration::from_secs(1));
        if let Err(error) = result {
            std::panic::resume_unwind(error);
        }
    }
}
