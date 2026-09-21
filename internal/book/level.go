package book

import (
	"sort"

	"replay/internal/store"
)

// side is one side of a book — bid or ask — held as a slice sorted
// ascending by price. Never a map: CLAUDE.md bans a map on any path whose
// output depends on iteration order, and a sorted slice also gives
// ordered traversal for free, which a top-N-levels view or a spread
// calculation needs. priceLess is the one comparator both sides share;
// which end of the slice counts as "best" is a convention Book's
// accessors document, not something side itself knows.
type side struct {
	levels []store.Level
}

// priceLess reports whether a sorts before b by price alone.
func priceLess(a, b store.Level) bool {
	return a.Price < b.Price
}

// search returns the index where price belongs in s, and whether a level
// at that price already exists there.
func (s *side) search(price int64) (int, bool) {
	lo, hi := 0, len(s.levels)
	for lo < hi {
		mid := (lo + hi) / 2
		if s.levels[mid].Price < price {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, lo < len(s.levels) && s.levels[lo].Price == price
}

// upsert sets price's size in s: it inserts a new level, updates one in
// place, or — when size is zero — removes it. Search-then-splice is one
// operation, so it can never leave two levels at the same price. Removing
// a price that is not present is ErrLevelNotFound: docs/book.md defines
// this as a divergence between the book and the venue, not a no-op.
func (s *side) upsert(price, size int64) error {
	i, found := s.search(price)

	switch {
	case size == 0 && !found:
		return ErrLevelNotFound
	case size == 0:
		s.levels = append(s.levels[:i], s.levels[i+1:]...)
		return nil
	case found:
		s.levels[i].Size = size
		return nil
	default:
		s.levels = append(s.levels, store.Level{})
		copy(s.levels[i+1:], s.levels[i:])
		s.levels[i] = store.Level{Price: price, Size: size}
		return nil
	}
}

// reset replaces s's contents with levels, sorted ascending by price. A
// snapshot blob is not required to arrive pre-sorted. Two levels sharing a
// price is ErrDuplicateLevel: that is a malformed snapshot payload, not a
// valid book state for this side to hold.
func (s *side) reset(levels []store.Level) error {
	s.levels = append(s.levels[:0], levels...)
	sort.Slice(s.levels, func(i, j int) bool { return priceLess(s.levels[i], s.levels[j]) })

	for i := 1; i < len(s.levels); i++ {
		if s.levels[i].Price == s.levels[i-1].Price {
			return ErrDuplicateLevel
		}
	}
	return nil
}

// snapshot returns a copy of s's levels, ascending by price.
func (s *side) snapshot() []store.Level {
	out := make([]store.Level, len(s.levels))
	copy(out, s.levels)
	return out
}
