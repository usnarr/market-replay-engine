package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	"replay/api"
	"replay/internal/merge"
	"replay/internal/store"
	"replay/internal/synth"
)

// This test is deliberately not called TestDeterminism*: `make
// determinism` runs -run TestDeterminism, and the transport is not
// part of that suite. The determinism boundary is the in-process
// fan-out interface, and internal/merge's and internal/fanout's own
// suites never open a connection. This test catches a different class
// of bug -- truncation, reordering, a codec or schema mistake -- and
// keeping the two apart is what stops "the determinism test" from
// quietly coming to mean something weaker. See docs/determinism.md.

// inProcessHash replays cfg's dataset straight through the merge, with
// no ring and no transport, and returns its canonical hash and record
// count. This is the ground truth the client's own hash is checked
// against.
func inProcessHash(t *testing.T, cfg Config) (string, int) {
	t.Helper()

	cursors, err := openCursors(cfg)
	if err != nil {
		t.Fatalf("openCursors() error = %v, want nil", err)
	}
	m, err := merge.NewMerger(cursors)
	if err != nil {
		closeCursors(cursors)
		t.Fatalf("NewMerger() error = %v, want nil", err)
	}
	defer func() {
		if err := m.Close(); err != nil {
			t.Errorf("Merger.Close() error = %v, want nil", err)
		}
	}()

	h := store.NewCanonicalHasher(store.CanonicalCRC32C)
	count := 0
	for {
		ev, ok, err := m.Next()
		if err != nil {
			t.Fatalf("Next() error = %v, want nil", err)
		}
		if !ok {
			break
		}
		h.Write(ev.Record, ev.Blob)
		count++
	}
	return hex.EncodeToString(h.Sum(nil)), count
}

// appendSnapshotPayload rebuilds a snapshot's blob payload from the
// decoded wire message: the bid count, the ask count, then every
// level. It is the test's own encoding of that layout, written without
// reusing internal/store's decoder on purpose — if both ends shared
// one helper, a bug inside it would cancel out and the two hashes
// would still agree.
func appendSnapshotPayload(dst []byte, rec *api.Record) []byte {
	var header [4]byte
	binary.LittleEndian.PutUint16(header[0:], uint16(rec.GetBidCount()))
	binary.LittleEndian.PutUint16(header[2:], uint16(rec.GetLevelCount()-rec.GetBidCount()))
	dst = append(dst, header[:]...)

	var level [16]byte
	for _, l := range rec.GetLevels() {
		binary.LittleEndian.PutUint64(level[0:], uint64(l.GetPrice()))
		binary.LittleEndian.PutUint64(level[8:], uint64(l.GetSize()))
		dst = append(dst, level[:]...)
	}
	return dst
}

// clientHash computes the canonical hash from received messages alone,
// exactly as a client outside this repository would: decode each
// message back into a record, rebuild a snapshot's payload from the
// decoded levels, and feed both through the same projection.
func clientHash(responses []*api.SubscribeResponse) string {
	h := store.NewCanonicalHasher(store.CanonicalCRC32C)
	var payload []byte
	for _, resp := range responses {
		msg := resp.GetRecord()
		rec := store.Record{
			ExchangeTs:     msg.GetExchangeTs(),
			SequenceNumber: msg.GetSequenceNumber(),
			InstrumentID:   msg.GetInstrumentId(),
			VenueID:        uint16(msg.GetVenueId()),
			RecordType:     uint8(msg.GetRecordType()),
			SideFlags:      uint8(msg.GetSideFlags()),
			Price:          msg.GetPrice(),
			Size:           msg.GetSize(),
			LevelCount:     uint16(msg.GetLevelCount()),
		}
		payload = payload[:0]
		if rec.RecordType == store.RecordTypeSnapshotPointer {
			payload = appendSnapshotPayload(payload, msg)
		}
		h.Write(rec, payload)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func TestIntegrationGRPCHashMatchesInProcess(t *testing.T) {
	dir := t.TempDir()
	ds := synth.Standard(t, dir)
	cfg := testConfig(t, ds)
	wantHash, wantCount := inProcessHash(t, cfg)

	srv := newTestServer(t, cfg)
	client := startTestServer(t, srv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.Subscribe(ctx, blockRequest())
	if err != nil {
		t.Fatalf("Subscribe() error = %v, want nil", err)
	}
	var responses []*api.SubscribeResponse
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv() error = %v, want nil", err)
		}
		responses = append(responses, resp)
	}

	if len(responses) != wantCount {
		t.Fatalf("the client received %d records, want %d", len(responses), wantCount)
	}
	if got := clientHash(responses); got != wantHash {
		t.Errorf("the client's canonical hash = %s, want %s (the in-process merged stream's own hash)", got, wantHash)
	}
	snapshots := 0
	for _, resp := range responses {
		if resp.GetRecord().GetRecordType() == uint32(store.RecordTypeSnapshotPointer) {
			snapshots++
		}
	}
	if snapshots == 0 {
		t.Error("no snapshot pointer crossed the wire; the hash proves nothing about decoded levels")
	}
}
