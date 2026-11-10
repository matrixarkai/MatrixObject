# Authenticated object reads

New object writes publish a version 2 binary manifest. Each extent carries a
SHA-256 digest of its page bytes alongside its length and write sequence. The
metadata quorum publishes the manifest only after the data write quorum has
acknowledged the pages.

A routed star client connects a read quorum. Write and read quorums must
overlap, so at least one connected member held each acknowledged page when
the manifest was published. A returned page is accepted only when its length,
sequence, and SHA-256 digest match the committed extent. Missing, stale, or
damaged replies cause the client to try another member. A slow member gets a
bounded first attempt before the client tries the next member. Healthy reads
transfer one page copy instead of the two copies required by payload voting.

Version 1 manifests continue to use exact quorum-voted reads. The on-disk
version 1 encoding is unchanged. Upgrade every metaserver voter to a version
that understands version 2 manifests before using clients that publish them:
older metadata binaries reject the new manifest encoding. Chunk servers can
continue to serve scalar reads when they do not support the wider batch.
