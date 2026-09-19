package nohashmaphash

// hash/crc32 is the project's actual choice for the emit stage's running
// hash (see docs/format.md); it is not banned.
import "hash/crc32"

var _ = crc32.ChecksumIEEE
