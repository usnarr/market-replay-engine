package nosyncmap

import "sync"

// A sync.Mutex guarding a plain map is not sync.Map; the "sync" import
// itself is not what this rule bans.
type guarded struct {
	mu sync.Mutex
	m  map[string]int
}

func (g *guarded) get(k string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.m[k]
}
