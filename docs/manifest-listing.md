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
