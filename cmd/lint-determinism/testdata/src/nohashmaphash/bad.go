// Package nohashmaphash is analysistest fixture data for the
// no-hash-maphash rule.
package nohashmaphash

import "hash/maphash" // want `no-hash-maphash`

var _ = maphash.Bytes
