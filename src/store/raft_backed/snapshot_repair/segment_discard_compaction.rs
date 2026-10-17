// Slab lifecycle operations are grouped by mutation and Raft phase.
include!("segment_discard_prepare.rs");
include!("segment_discard_propose.rs");
include!("segment_compaction_prepare.rs");
include!("segment_compaction_propose.rs");
