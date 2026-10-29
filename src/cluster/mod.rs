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
//! The data calls cross the socket: `write_chunk`, `read_chunk`, `head_chunk`, `discard_chunk`.
//! `page_inventory` also returns bounded batches of page identities and write sequences,
//! without copying payloads. Its exclusive cursor resumes in key order. A traversal is live,
//! not a snapshot; writers must be fenced before it can certify replica readmission.
//! Everything else a replica can do is in-process only, and `MatrixChunkService` -- the
//! service that holds `replicas` and runs the historic fan-out -- is UNCHANGED by this
//! module. It does not route through [`crate::cluster::placement::ReplicaFanout`].
//!
//! That is a deliberate stop, and the size of what it stops short of is measured rather than
//! guessed -- and measured by a script, `tools/replica_surface.sh`, rather than recorded by
//! hand. The figures that used to be in this paragraph were written down once and every one
//! of them went stale, which is what a number nothing re-derives does.
//!
//! The experiment: give the ELEMENT type of `MatrixChunkService.replicas` no methods and no
//! fields, compile with every feature, and read what the compiler says is missing. As of
//! #226:
//!
//! | what the compiler found | count |
//! |---|---:|
//! | distinct replica METHODS the layer above calls | 127 |
//! | sites for them | 379 |
//! | of those, production sites | 247, in 53 files |
//! | of those, library-suite sites | 132 |
//! | **direct reads of a replica FIELD** | **0** |
//! | methods with exactly one production site | 60 |
//!
//! The zero is the point of #226. A field read cannot cross a process boundary and a method
//! call can, so closing the sixteen fields the layer above reached for is what turned this
//! from a redesign into a dispatch problem.
//!
//! # WHICH OF THE 127 HAS TO CROSS, AND HOW THAT WAS DECIDED
//!
//! Three rules, applied to every method:
//!
//! 1. it **crosses** when its answer depends on state only the node has;
//! 2. it **folds into a batched call** when it is consulted at the same moment as others, so
//!    that a request does not become a round trip per question;
//! 3. it **stays local** when it hands back the node's state instead of an answer -- that is
//!    a snapshot transfer with a different name, not a remote method.
//!
//! Rule 2 does most of the work, and the call it folds into already exists.
//! `ReplicaServiceControlProof` has **76 fields**, and **25 of the 127 methods are already
//! answered by one of them** -- `is_frozen` (18 production sites), `is_draining`,
//! `accepting_writes`, `capacity_bytes`, the four `physical_*` counters, the `trim_flow_*`
//! family, the read-cache counters. Those 25 methods account for **63 of the 247 production
//! sites**, and `control_proof` already carries all of their answers in one value. So the
//! status half of this surface is one call, not twenty-five.
//!
//! What that costs, said rather than hidden: `control_proof` computes its checksum from
//! `export_snapshot()`, so it is O(state) in the pages the replica holds -- and it is the
//! most-called method on the replica, at 29 production sites. Remotely that cost lands on the
//! node and only a small struct crosses, which is the right side for it to land on; locally
//! it is already paid at every one of those sites.
//!
//! The rest divides cleanly. **Crosses:** the data verbs and their idempotent and borrowed
//! variants; the page-index queries (`page_count`, `page_ids`, `page_write_sequence`,
//! `page_write_sequences`, `page_logical_length`, `holds_any_page`); the fence
//! (`check_expected_control_checksum`, 18 sites); repair and handoff; the snapshot export and
//! install, which are already shaped as a stream. **Crosses as a command rather than a read:**
//! the `configure_*` and `set_*` family, which is administrative and infrequent. **Stays
//! local:** `pages_local` and the `*_local` receipt accessors, by construction -- see
//! `src/services/chunk/page_access.rs` for why those two whole-state traversals, the footprint
//! audit and the batch-mutation fingerprints, need a different call rather than a wire.
//!
//! # TWO WAYS THIS MEASUREMENT CAN LIE, BOTH MET
//!
//! Recorded because both produced a confident wrong table before being caught, and the script
//! now guards against each:
//!
//! * **Replacing the whole `Vec` measures the wrong thing.** It surfaces the collection's own
//!   methods -- `iter`, `get`, `len`, `get_mut`, `clone`, `iter_mut` -- and never reaches the
//!   element's. The first run of this experiment reported **6** distinct methods for that
//!   reason. A collection's API is not what crosses a socket.
//! * **A stand-in inserted above the struct steals its `#[derive]`.** Put between
//!   `#[derive(Clone, Debug, Eq, PartialEq)]` and `pub struct MatrixChunkService`, the derive
//!   attaches to the stand-in and `MatrixChunkService` silently loses `Clone` -- which
//!   produced **24 cascade `clone` failures** that read exactly like a real part of the
//!   surface. The script appends the stand-in and refuses to score if `E0119` or a
//!   `MatrixChunkService: Clone` bound appears at all.
//!
//! And one way a needle lies: a method passed as a function path,
//! `.map(MatrixReplicaService::mutation_qos_config)`, is a third spelling that "no method
//! named" never matches. **25 sites** use it, so the script counts `E0631` as well as `E0599`.
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

// THE FRAMING IS SHARED BY BOTH HALVES, so it is behind neither feature. `wire` holds the
// six-byte header, the stamp, the payload bound, the status codes and the cursor that refuses
// to read past the end of a payload; `message` holds `check_stamp`, `encode_failure` and
// `decode_failure` beside the replica messages. A build with only the control plane needs
// those five items and would otherwise need a second copy of them.
mod conditional_requests;
pub mod message;
pub mod wire;

// The data plane: a replica node, a client, and the quorum over several of them.
#[cfg(feature = "replica-server")]
pub mod client;
#[cfg(feature = "replica-server")]
pub mod placement;
#[cfg(feature = "replica-server")]
pub mod replica_set;
#[cfg(feature = "replica-server")]
pub mod server;

// The control plane: a metaserver node, a client, and the directory that chooses.
#[cfg(feature = "meta-server")]
pub mod directory;
#[cfg(feature = "meta-server")]
pub mod meta_client;
#[cfg(feature = "meta-server")]
mod meta_identity;
#[cfg(feature = "meta-server")]
pub mod meta_message;
#[cfg(feature = "meta-server")]
pub mod meta_server;
#[cfg(feature = "meta-server")]
pub mod meta_wire;
#[cfg(feature = "meta-server")]
pub use meta_identity::MetaGroupIdentity;

pub use message::{ChunkGroupIdentity, ReplicaReply, ReplicaRequest};
pub use wire::{RequestCode, StatusCode, MAX_PAYLOAD_BYTES, PROTOCOL_STAMP};

#[cfg(feature = "replica-server")]
pub use client::{RemoteReplicaClient, ReplicaClientOptions};
#[cfg(feature = "replica-server")]
pub use placement::{
    check_quorums, majority, InProcessReplicaSet, ReplicaFanout, ReplicaPlacement,
};
#[cfg(feature = "replica-server")]
pub use replica_set::RemoteReplicaSet;
#[cfg(all(feature = "replica-server", feature = "meta-server"))]
pub use server::ReplicaHeartbeatOptions;
#[cfg(feature = "replica-server")]
pub use server::{ReplicaNodeOptions, ReplicaNodeServer, ReplicaNodeShutdown};

#[cfg(feature = "meta-server")]
pub use directory::{ControlPlaneStatus, MetaDirectory};
#[cfg(feature = "meta-server")]
pub use meta_client::{MetaCallOutcome, MetaClientOptions, RemoteMetaClient};
#[cfg(feature = "meta-server")]
pub use meta_message::{
    CommittedPlacement, MetaReply, MetaRequest, PlacementAssignment, PlacementNodeSummary,
};
#[cfg(feature = "meta-server")]
pub use meta_server::{DataNodeSeed, MetaNodeOptions, MetaNodeServer, MetaNodeShutdown};
#[cfg(feature = "meta-server")]
pub use meta_wire::{MetaRequestCode, META_CODE_BASE};

#[cfg(feature = "replica-server")]
mod replica_journal;

mod connections;
#[cfg(any(feature = "replica-server", feature = "meta-server"))]
mod shutdown;

#[cfg(feature = "meta-server")]
mod meta_snapshot_file;
#[cfg(feature = "meta-server")]
mod placement_journal;

#[cfg(feature = "metadata-consensus")]
mod meta_consensus;
#[cfg(feature = "metadata-consensus")]
pub use meta_consensus::MetaRaftOptions;

#[cfg(feature = "chunk-consensus")]
pub use server::ChunkRaftOptions;

#[cfg(feature = "meta-server")]
mod chunk_groups;
#[cfg(feature = "meta-server")]
pub use chunk_groups::{ChunkGroupCatalog, ChunkGroupConfig};

#[cfg(any(feature = "metadata-consensus", feature = "chunk-consensus"))]
mod consensus_checkpoint;

#[cfg(feature = "chunk-consensus")]
mod chunk_checkpoint;

#[cfg(feature = "metadata-consensus")]
mod meta_checkpoint;

#[cfg(all(feature = "meta-server", feature = "replica-server"))]
mod object_client;
#[cfg(all(feature = "meta-server", feature = "replica-server"))]
pub use object_client::{ObjectWriteFailure, ObjectWriteOptions};

#[cfg(any(feature = "metadata-consensus", feature = "chunk-consensus"))]
mod peer_receiver;
