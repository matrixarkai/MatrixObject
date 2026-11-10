# Distributed manifest listing

`RemoteMetaClient::list_manifests(bucket, after, limit)` returns at most 32
summaries in ascending key order. `after` is an exclusive key cursor; start with
`None` and continue with the final returned key while `has_more` is true.
Each summary carries the key, generation, logical length and deletion flag.
Tombstones are included. An unknown bucket returns an empty page.

Metadata opcode 91 uses canonical binary protobuf described in
`proto/manifest_listing.proto`. Replies are bounded at 256 KiB, including the
maximum supported key lengths. The catalog seeks directly past the cursor,
visits at most `limit + 1` records and never clones extent lists or payloads.

Every page requires a current-term metadata quorum confirmation. Listing does
not append an outcome. Pages are independent reads: concurrent insertion,
deletion or overwrite can affect later pages, so a scan is not a consistent
cluster snapshot. Enumeration alone does not authorize page reclamation;
reclamation also needs protection against concurrent publication and readers.

WSL process validation continues after metadata leader loss, restores tombstones
after total restart and refuses listing from a lone surviving voter.

## Revision-checked continuation

`list_manifests_at_revision(bucket, after, limit, expected_revision)` opts into
version 2. Start with `None`, retain the returned 16-byte opaque revision, and
pass it to every later page. A manifest publication anywhere in this catalog
invalidates the old revision. Allocation, heartbeat and membership activity do
not change it. The server compares the revision while holding the same state
guard used to construct the page. Old servers reject version 2; version-1
requests and replies retain their existing encoding.

The revision is the 128-bit sum of retained manifest generations. Tombstones
retain generation history, and successful publications advance exactly one
generation, making the sum monotonic within one metadata history. Duplicate
outcome application leaves it unchanged. Checkpoint restore reconstructs the
sum without changing the durable checkpoint format. Removing committed catalog
entries would require a new revision scheme; tombstones remain retained.

Tokens belong to one metadata history. Use a pinned metadata catalog when
continuing across endpoints. A successful scan does not block subsequent
publication or protect older readers, so its token alone still cannot authorize
data reclamation.
