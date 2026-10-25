// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A replica in another process, reached over a socket.
//!
//! # WHAT THIS MODULE IS FOR
//!
//! Replication in this store was, until this module, entirely inside one address space. A
//! "replica" was an element of `replicas: Vec<MatrixReplicaService>`, built by
//! `(0..replica_count).map(|_| MatrixReplicaService::new())`. The fan-out was a `for` loop
//! over `self.replicas.iter_mut()`. A write quorum was a count of loop iterations that
//! returned `Ok`, and a fault was a `bool` on a struct at an index.
//!
//! That arrangement pays three times for a payload and buys nothing a second copy is for.
//! One process ending takes all three copies with it, because all three are on its heap.
//!
//! This module is the other arrangement, as a vertical slice: a
//! [`crate::cluster::server::ReplicaNodeServer`] owns one replica and nothing else can reach it except
//! through a socket; a [`crate::cluster::client::RemoteReplicaClient`] makes the same four calls remotely;
//! a [`crate::cluster::replica_set::RemoteReplicaSet`] fans out over several of them under the same quorum
//! rule; and [`crate::cluster::placement::ReplicaPlacement`] is the one place that chooses between the two,
//! defaulting to the in-process arrangement so that nothing existing changes.
//!
//! # WHAT 3.00x MEANS AFTER THIS, AND WHAT IT MEANT BEFORE
//!
//! The payload amplification figure this store publishes for replication is 3.00x, and the
//! number is unchanged by this module: three copies are three copies. What changed is what
//! the three are. Under [`crate::cluster::placement::ReplicaPlacement::InProcess`] they are three
//! `BTreeMap`s on one heap, and the figure is the price of a guarantee the arrangement cannot
//! provide. Under [`crate::cluster::placement::ReplicaPlacement::Remote`] they are three copies in three
//! address spaces, and the same 3.00x is the price of surviving the loss of one of them --
//! which the suite demonstrates by killing a replica process and reading every acknowledged
//! byte back from the survivors.
//!
//! # WHAT IS NOT REMOTE, AND THE MEASURED SIZE OF IT
//!
//! Four calls cross the socket: `write_chunk`, `read_chunk`, `head_chunk`, `discard_chunk`.
//! Everything else a replica can do is in-process only, and `MatrixChunkService` -- the
//! service that holds `replicas` and runs the historic fan-out -- is UNCHANGED by this
//! module. It does not route through [`crate::cluster::placement::ReplicaFanout`].
//!
//! That is a deliberate stop, and the size of what it stops short of was measured rather than
//! guessed. Replacing `MatrixChunkService`'s `replicas: Vec<MatrixReplicaService>` with
//! anything that could be remote was tried, by giving the field a type with no methods and
//! asking the compiler what broke. At the commit this module was written against, it broke
//! in 298 places:
//!
//! | what the compiler found | count |
//! |---|---:|
//! | distinct replica METHODS called through the field | 82 |
//! | call sites for them | 198 |
//! | distinct PRIVATE FIELDS of the replica read directly through it | 13 |
//! | sites reading those fields | 35 |
//! | files involved | 57 |
//!
//! The 35 direct field reads are the part that is a redesign and not a port. `replica.pages`
//! alone is read at 12 sites: the whole chunk map, borrowed, by callers that walk it. There
//! is no socket call behind that -- answering it over a wire means shipping the replica's
//! entire state to its caller, which is not a remote method but a snapshot transfer with a
//! different name. The same is true of `write_request_receipts`, `batch_write_request_receipts`
//! and the four `physical_*` counters: each is a borrow of the replica's own bookkeeping by
//! code that assumes it is local.
//!
//! The port is therefore two pieces of work, not one: (1) turn those 35 field reads into
//! methods, which is mechanical but has to decide for each one whether the answer is a value
//! or a stream; (2) only then can the 82 methods go behind a dispatch. Until (1) is done,
//! a `Vec` of something-that-might-be-remote cannot compile, so there is no half-measure
//! available and no shim that would make one look available.
//!
//! There is one more difference that no amount of mechanical work removes: a remote read
//! takes `&mut self` because sending a request frame is a write, while the local read takes
//! `&self`. See [`crate::cluster::client`] for why that is recorded rather than smoothed over.
//!
//! # IT IS UNAUTHENTICATED
//!
//! No signing, no token, no TLS, the same as [`crate::net`]. Anything that can open a socket
//! to a replica node's port can read and overwrite every page it holds, so the shipped
//! binary binds loopback unless told otherwise.

pub mod client;
pub mod message;
pub mod placement;
pub mod replica_set;
pub mod server;
pub mod wire;

pub use client::{ReplicaClientOptions, RemoteReplicaClient};
pub use message::{ReplicaReply, ReplicaRequest};
pub use placement::{InProcessReplicaSet, ReplicaFanout, ReplicaPlacement};
pub use replica_set::RemoteReplicaSet;
pub use server::{ReplicaNodeOptions, ReplicaNodeServer, ReplicaNodeShutdown};
pub use wire::{RequestCode, StatusCode, MAX_PAYLOAD_BYTES, PROTOCOL_STAMP};
