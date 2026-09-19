package merge

import (
	"errors"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// mk builds a key from its four fields, written in key order.
func mk(ts int64, venue uint16, seq uint64, inst uint32) Key {
	return Key{ExchangeTs: ts, VenueID: venue, SequenceNumber: seq, InstrumentID: inst}
}

// drainTree drives a tree over per-cursor key streams and returns the
// winners in the order it chose them. It fails the test if the tree
// rejects a key; use tryDrainTree for input meant to be rejected.
func drainTree(t *testing.T, streams [][]Key) []Key {
	t.Helper()

	got, err := tryDrainTree(t, streams)
	if err != nil {
		t.Fatalf("Advance() error = %v, want nil", err)
	}
	return got
}

// tryDrainTree is the whole merge loop, with plain slices standing in
// for cursors. It stops at the tree's first rejected key.
func tryDrainTree(t *testing.T, streams [][]Key) ([]Key, error) {
	t.Helper()

	total := 0
	for _, s := range streams {
		total += len(s)
	}

	tree := NewLoserTree(len(streams))
	pos := make([]int, len(streams))
	for i, s := range streams {
		if len(s) > 0 {
			tree.SetKey(i, s[0])
			pos[i] = 1
		}
	}
	tree.Init()

	got := make([]Key, 0, total)
	for {
		i, key := tree.Winner()
		if key == SentinelKey {
			return got, nil
		}
		if len(got) >= total {
			t.Fatalf("tree produced more than the %d keys it was given; it is not draining", total)
		}
		got = append(got, key)

		next := SentinelKey
		if pos[i] < len(streams[i]) {
			next = streams[i][pos[i]]
			pos[i]++
		}
		if err := tree.Advance(next); err != nil {
			return got, err
		}
	}
}

func TestLoserTreeMergeOrder(t *testing.T) {
	tests := []struct {
		name    string
		streams [][]Key
		want    []Key
	}{
		{
			name:    "no_cursors_at_all",
			streams: [][]Key{},
			want:    []Key{},
		},
		{
			name:    "one_cursor_passes_its_stream_through",
			streams: [][]Key{{mk(10, 1, 0, 7), mk(20, 1, 1, 7), mk(30, 1, 2, 7)}},
			want:    []Key{mk(10, 1, 0, 7), mk(20, 1, 1, 7), mk(30, 1, 2, 7)},
		},
		{
			name: "two_cursors_interleave_by_timestamp",
			streams: [][]Key{
				{mk(10, 1, 0, 7), mk(30, 1, 1, 7)},
				{mk(20, 2, 0, 7), mk(40, 2, 1, 7)},
			},
			want: []Key{mk(10, 1, 0, 7), mk(20, 2, 0, 7), mk(30, 1, 1, 7), mk(40, 2, 1, 7)},
		},
		{
			name: "a_cursor_count_that_is_not_a_power_of_two",
			streams: [][]Key{
				{mk(10, 1, 0, 7), mk(50, 1, 1, 7)},
				{mk(20, 2, 0, 7)},
				{mk(30, 3, 0, 7), mk(60, 3, 1, 7)},
			},
			want: []Key{
				mk(10, 1, 0, 7), mk(20, 2, 0, 7), mk(30, 3, 0, 7),
				mk(50, 1, 1, 7), mk(60, 3, 1, 7),
			},
		},
		{
			name: "an_empty_cursor_never_wins",
			streams: [][]Key{
				{mk(10, 1, 0, 7), mk(20, 1, 1, 7)},
				{},
				{mk(15, 3, 0, 7)},
			},
			want: []Key{mk(10, 1, 0, 7), mk(15, 3, 0, 7), mk(20, 1, 1, 7)},
		},
		{
			name: "a_cursor_that_ends_early_stops_winning",
			streams: [][]Key{
				{mk(10, 1, 0, 7)},
				{mk(20, 2, 0, 7), mk(30, 2, 1, 7), mk(40, 2, 2, 7)},
			},
			want: []Key{mk(10, 1, 0, 7), mk(20, 2, 0, 7), mk(30, 2, 1, 7), mk(40, 2, 2, 7)},
		},
		{
			name:    "every_cursor_empty_yields_nothing",
			streams: [][]Key{{}, {}, {}},
			want:    []Key{},
		},
		{
			name: "equal_timestamps_break_on_venue_id",
			streams: [][]Key{
				{mk(10, 5, 0, 7)},
				{mk(10, 2, 0, 7)},
				{mk(10, 9, 0, 7)},
			},
			want: []Key{mk(10, 2, 0, 7), mk(10, 5, 0, 7), mk(10, 9, 0, 7)},
		},
		{
			name: "one_venue_ahead_of_every_other_drains_last",
			streams: [][]Key{
				{mk(100, 1, 0, 7), mk(110, 1, 1, 7)},
				{mk(10, 2, 0, 7), mk(20, 2, 1, 7)},
			},
			want: []Key{
				mk(10, 2, 0, 7), mk(20, 2, 1, 7),
				mk(100, 1, 0, 7), mk(110, 1, 1, 7),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := drainTree(t, tt.streams)

			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("merged keys mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLoserTreeShapeDependsOnlyOnCursorCount(t *testing.T) {
	// The tree's shape is the structural basis for the claim that merged
	// output does not depend on the worker count. Assert it directly
	// rather than inferring it from output: for one k, the shape must be
	// the same whatever the data is, and must not change while draining.
	sizes := []struct {
		k    int
		want int
	}{
		{k: 0, want: 1},
		{k: 1, want: 1},
		{k: 2, want: 2},
		{k: 3, want: 4},
		{k: 4, want: 4},
		{k: 5, want: 8},
		{k: 8, want: 8},
		{k: 9, want: 16},
	}

	for _, s := range sizes {
		t.Run("k_"+strconv.Itoa(s.k), func(t *testing.T) {
			tree := NewLoserTree(s.k)

			if len(tree.tree) != s.want || len(tree.keys) != s.want {
				t.Fatalf("tree over %d cursors has %d nodes and %d keys, want %d of each",
					s.k, len(tree.tree), len(tree.keys), s.want)
			}
		})
	}

	t.Run("a_tie_keeps_the_cursor_already_holding_the_node", func(t *testing.T) {
		// Every cursor here holds the sentinel, so every match is a tie.
		// The rule is that the incumbent keeps the node, which leaves the
		// lowest-indexed cursor as the winner. Nothing observable depends
		// on which exhausted cursor wins, but the rule must still be
		// pinned: the same comparison decides which record goes first if
		// a corrupt artifact ever puts two equal keys in the tree at once.
		tree := NewLoserTree(3)
		tree.Init()

		i, key := tree.Winner()

		if key != SentinelKey {
			t.Fatalf("Winner() key = %v, want the sentinel", key)
		}
		if i != 0 {
			t.Errorf("Winner() = %d, want 0 — a tie keeps the incumbent", i)
		}
	})

	t.Run("an_exhausted_cursor_is_not_removed_from_the_tree", func(t *testing.T) {
		streams := [][]Key{
			{mk(10, 1, 0, 7)},
			{mk(20, 2, 0, 7), mk(30, 2, 1, 7)},
			{},
		}
		tree := NewLoserTree(len(streams))
		tree.SetKey(0, streams[0][0])
		tree.SetKey(1, streams[1][0])
		tree.Init()
		before := len(tree.tree)

		for _, key := range []Key{SentinelKey, streams[1][1], SentinelKey} {
			if err := tree.Advance(key); err != nil {
				t.Fatalf("Advance(%v) error = %v, want nil", key, err)
			}
		}

		if len(tree.tree) != before {
			t.Errorf("tree has %d nodes after draining, want %d", len(tree.tree), before)
		}
		if _, key := tree.Winner(); key != SentinelKey {
			t.Errorf("Winner() key = %v, want the sentinel once every cursor is exhausted", key)
		}
	})
}

func TestLoserTreeRejectsKeysThatDoNotIncrease(t *testing.T) {
	// The check costs one comparison, because the tree already knows the
	// key it is retiring and the key that replaces it at the root. A
	// duplicate is unreachable on a checksum-validated artifact written
	// by this project's own writer, so reaching it means the file is
	// corrupt or the writer's validation was bypassed.
	tests := []struct {
		name    string
		streams [][]Key
		want    error
		wantN   int
	}{
		{
			name:    "one_cursor_repeats_a_key",
			streams: [][]Key{{mk(10, 1, 0, 7), mk(10, 1, 0, 7)}},
			want:    ErrDuplicateKey,
			wantN:   1,
		},
		{
			name:    "one_cursor_repeats_a_key_behind_another_cursor",
			streams: [][]Key{{mk(10, 1, 0, 7), mk(10, 1, 0, 7)}, {mk(50, 2, 0, 7)}},
			want:    ErrDuplicateKey,
			wantN:   1,
		},
		{
			name:    "two_cursors_hold_the_same_key",
			streams: [][]Key{{mk(10, 1, 0, 7)}, {mk(10, 1, 0, 7)}},
			want:    ErrDuplicateKey,
			wantN:   1,
		},
		{
			name:    "one_cursor_goes_backwards",
			streams: [][]Key{{mk(20, 1, 1, 7), mk(10, 1, 0, 7)}},
			want:    ErrOutOfOrder,
			wantN:   1,
		},
		{
			name:    "one_cursor_goes_backwards_after_another_cursor_wins",
			streams: [][]Key{{mk(10, 1, 0, 7), mk(30, 1, 1, 7)}, {mk(20, 2, 0, 7), mk(15, 2, 1, 7)}},
			want:    ErrOutOfOrder,
			wantN:   2,
		},
		{
			name:    "a_key_that_only_repeats_its_instrument_id",
			streams: [][]Key{{mk(10, 1, 0, 7), mk(10, 1, 1, 7), mk(10, 1, 1, 7)}},
			want:    ErrDuplicateKey,
			wantN:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tryDrainTree(t, tt.streams)

			if !errors.Is(err, tt.want) {
				t.Fatalf("Advance() error = %v, want %v", err, tt.want)
			}
			if len(got) != tt.wantN {
				t.Errorf("emitted %d keys before the error, want %d — the offending key must not be emitted",
					len(got), tt.wantN)
			}
		})
	}
}

func TestLoserTreeAcceptsEveryCursorReachingTheSentinelAtOnce(t *testing.T) {
	// Every exhausted cursor holds the same sentinel key, so the
	// ordering check must not read the end of the stream as a duplicate.
	streams := [][]Key{
		{mk(10, 1, 0, 7)},
		{mk(20, 2, 0, 7)},
		{},
		{mk(30, 3, 0, 7)},
	}

	got, err := tryDrainTree(t, streams)

	if err != nil {
		t.Fatalf("Advance() error = %v, want nil", err)
	}
	if len(got) != 3 {
		t.Errorf("emitted %d keys, want 3", len(got))
	}
}

func TestLoserTreeCursorOrderDoesNotChangeOutput(t *testing.T) {
	// Which cursor index a venue lands on is an arbitrary choice made
	// when partitions are discovered. It must not reach the output.
	streams := [][]Key{
		{mk(10, 1, 0, 7), mk(40, 1, 1, 7), mk(70, 1, 2, 7)},
		{mk(20, 2, 0, 7), mk(50, 2, 1, 7)},
		{mk(30, 3, 0, 7), mk(60, 3, 1, 7), mk(80, 3, 2, 7)},
		{},
	}
	want := drainTree(t, streams)

	rotations := [][][]Key{
		{streams[1], streams[2], streams[3], streams[0]},
		{streams[2], streams[3], streams[0], streams[1]},
		{streams[3], streams[0], streams[1], streams[2]},
	}
	for i, rotated := range rotations {
		t.Run("rotation_"+strconv.Itoa(i+1), func(t *testing.T) {
			got := drainTree(t, rotated)

			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("merged keys mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
