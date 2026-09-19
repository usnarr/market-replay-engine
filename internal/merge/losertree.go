package merge

// LoserTree is a fixed-size tournament tree over k cursors. It names the
// cursor holding the smallest key in log2(k) comparisons per pop, half
// what a binary min-heap costs — but the property the determinism
// guarantee rests on is a different one: the tree's shape is settled by
// k at construction and never changes, however the data is distributed
// and however many cursors run out. A heap re-shapes itself on every
// pop. See docs/determinism.md.
//
// Keys are cached here, one per cursor, so a comparison reads two
// 24-byte values and never calls back into a store reader to decode a
// record again.
type LoserTree struct {
	// keys holds one key per leaf. Leaves from k up are padding that
	// stays at SentinelKey forever, which rounds the tree to a perfect
	// binary shape without making the node arithmetic depend on k.
	keys []Key
	// tree[0] is the winning cursor. tree[1:] holds, at each internal
	// node, the cursor that lost its match there.
	tree []int32
	k    int
}

// NewLoserTree returns a tree over k cursors, each starting exhausted.
// Cache each cursor's first key with SetKey, then call Init.
func NewLoserTree(k int) *LoserTree {
	if k < 0 {
		panic("merge: negative cursor count")
	}

	m := 1
	for m < k {
		m *= 2
	}
	t := &LoserTree{keys: make([]Key, m), tree: make([]int32, m), k: k}
	for i := range t.keys {
		t.keys[i] = SentinelKey
	}
	return t
}

// SetKey caches cursor i's first key. Call it before Init. A cursor left
// unset starts exhausted, which is how an empty venue partition stays in
// the tree rather than being left out of it.
func (t *LoserTree) SetKey(i int, key Key) {
	if i < 0 || i >= t.k {
		panic("merge: SetKey cursor out of range")
	}
	t.keys[i] = key
}

// Init builds the tree from the cached keys. It plays a full tournament
// once, keeping the loser of each match at the node it lost at, and
// costs one temporary slice that no later operation needs.
func (t *LoserTree) Init() {
	m := len(t.keys)
	if m == 1 {
		t.tree[0] = 0
		return
	}

	win := make([]int32, 2*m)
	for i := 0; i < m; i++ {
		win[m+i] = int32(i)
	}
	for node := m - 1; node >= 1; node-- {
		winner, loser := win[2*node], win[2*node+1]
		if t.less(loser, winner) {
			winner, loser = loser, winner
		}
		win[node], t.tree[node] = winner, loser
	}
	t.tree[0] = win[1]
}

// Winner returns the cursor holding the smallest cached key, and that
// key. SentinelKey means every cursor is exhausted and the stream has
// ended; the returned index means nothing in that case.
func (t *LoserTree) Winner() (int, Key) {
	w := t.tree[0]
	return int(w), t.keys[w]
}

// Advance replaces the current winner's cached key with key and replays
// that cursor's matches to the root. Pass SentinelKey when the cursor
// has no more records. Calling Advance once Winner reports SentinelKey
// is a caller error.
//
// It also checks the ordering invariant the whole merge rests on: the
// key it retires must sort strictly before the key that takes the root.
// A tie is ErrDuplicateKey and a decrease is ErrOutOfOrder, both hard
// stops. The check is one comparison, because both keys are already
// known — and it covers the whole stream, so a duplicate inside one
// venue and a duplicate across two are caught by the same line.
func (t *LoserTree) Advance(key Key) error {
	w := t.tree[0]
	retired := t.keys[w]
	t.keys[w] = key
	t.fix(w)

	next := t.keys[t.tree[0]]
	if next == SentinelKey {
		// Every cursor is exhausted. They all hold the same sentinel, so
		// checking it against the retired key would read the end of the
		// stream as a duplicate.
		return nil
	}
	switch c := compareKey(retired, next); {
	case c == 0:
		return ErrDuplicateKey
	case c > 0:
		return ErrOutOfOrder
	}
	return nil
}

// fix replays cursor i's matches from its leaf to the root. The path is
// the same every time for a given i, because the tree's shape never
// changes, so this is exactly log2(k) comparisons against the cursors i
// lost to before.
func (t *LoserTree) fix(i int32) {
	m := int32(len(t.keys))
	winner := i
	for node := (m + i) / 2; node >= 1; node /= 2 {
		if t.less(t.tree[node], winner) {
			t.tree[node], winner = winner, t.tree[node]
		}
	}
	t.tree[0] = winner
}

// less reports whether cursor a's cached key sorts before cursor b's.
// Equal keys report false, so the cursor already holding a node keeps
// it. Only sentinels can be equal here — two live cursors differ in
// venue_id by construction — and which sentinel holds a node is never
// observable, because the merge stops as soon as the winner is one.
func (t *LoserTree) less(a, b int32) bool {
	return compareKey(t.keys[a], t.keys[b]) < 0
}
