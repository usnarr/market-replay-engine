// Package nosyncmap is analysistest fixture data for the no-sync-map rule.
package nosyncmap

import "sync"

var cache sync.Map // want `no-sync-map`

func newCache() *sync.Map { // want `no-sync-map`
	return &sync.Map{} // want `no-sync-map`
}
