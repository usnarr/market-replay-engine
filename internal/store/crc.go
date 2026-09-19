package store

import "hash/crc32"

// castagnoli backs every checksum in this format: the header checksum,
// the per-block record checksums, each blob's own checksum, and the
// default canonical hash. CRC-32C has a hardware instruction on amd64
// and arm64, so an always-on checksum does not become the throughput
// bottleneck. See docs/format.md.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)
