// Slab lifecycle operations are grouped by mutation and Raft phase.
include!("slab_discard_prepare.rs");
include!("slab_discard_propose.rs");
include!("slab_compaction_prepare.rs");
include!("slab_compaction_propose.rs");
