# Unified LOG writer outcomes

Placement outcome version 17 carries one writer lease transition and either a
chunk-catalog publication or the complete expected manifest of an existing open
tail. It binds both tables to the same placement index. A stale lease, stale
manifest, mismatched index or already-published manifest half is refused before
either table changes. Committed consumers enforce structural bounds, while the
leader alone applies its configurable admission limit.

A grant adds one empty LOG tail whose chunk identity derives from the outcome
index. Taking over an open tail changes only the lease. A switch seals the old
tail without shortening it and adds the next empty tail. Neither operation can
change an existing object's storage kind or attributes. Lease-only grants still
carry the expected manifest, because independent catalog publications do not
advance the placement index.

Reader capability bit 6 identifies support for this outcome and metadata snapshot
version 24. Declaring support does not activate it. Activation follows the existing
feature command's complete reader-set checks; this change does not raise the bit
by default. The replicated activation is part of the prior-state binding for a
version 17 entry, alongside the index and the expected lease and catalog entry.

Once activated, snapshots use version 24 even when the chunk catalog is empty.
This keeps the reader floor after all objects are removed and immediately after
activation. Older snapshot formats cannot carry the active bit. Readers accept
prior snapshot formats when the new semantics are inactive; unraised declarations
retain their prior snapshot encoding.

This change adds readers and atomic application through the common placement
outcome path used by metadata journal replay and consensus. Public lease commands
still use the earlier manifest catalog, and no command emits version 17 yet. The
unified command preparation, draft rollback, default activation and gateway LOG
routing must be integrated before this becomes the ordinary write path.
