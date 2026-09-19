// Package nocontainerheap is analysistest fixture data for the
// no-container-heap rule.
package nocontainerheap

import "container/heap" // want `no-container-heap`

var _ = heap.Init
