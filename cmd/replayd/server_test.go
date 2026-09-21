package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"replay/api"
	"replay/internal/fanout"
	"replay/internal/store"
	"replay/internal/synth"
)

// fakeStream is a grpc.ServerStreamingServer that collects what the
// handler sends, with no transport in the loop. It embeds the
// ServerStream interface as a nil value on purpose: a handler that
// reaches for any method beyond Context and Send panics here rather
// than passing quietly. The real transport is covered by
// TestIntegrationGRPCHashMatchesInProcess.
type fakeStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent []*api.SubscribeResponse
}

func (f *fakeStream) Context() context.Context { return f.ctx }

func (f *fakeStream) Send(resp *api.SubscribeResponse) error {
	f.sent = append(f.sent, resp)
	return nil
}

func newFakeStream(t *testing.T) *fakeStream {
	t.Helper()

	// Cancelled when the subtest ends, so the handler's stream-context
	// watcher goroutine always finishes, exactly as grpc-go's own
	// cancellation on handler return would make it.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &fakeStream{ctx: ctx}
}

// testConfig is a Config over the standard synthetic dataset, with the
// ring large enough that a Block subscriber never engages the barrier.
func testConfig(t *testing.T, ds *synth.Dataset) Config {
	t.Helper()

	var files []string
	for _, venue := range ds.Files {
		files = append(files, venue...)
	}
	return Config{
		Files:        files,
		Workers:      4,
		Capacity:     8192,
		MaxBlobBytes: 4096,
	}
}

func newTestServer(t *testing.T, cfg Config) *Server {
	t.Helper()

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer() error = %v, want nil", err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
	})
	return srv
}

// blockRequest is a valid request: Block, from the beginning, at real
// time.
func blockRequest() *api.SubscribeRequest {
	return &api.SubscribeRequest{
		Mode:     api.Mode_MODE_BLOCK,
		Start:    &api.SubscribeRequest_Beginning{Beginning: &api.StartAtBeginning{}},
		SpeedNum: 1,
		SpeedDen: 1,
	}
}

func TestOpenCursors(t *testing.T) {
	dir := t.TempDir()
	ds := synth.Standard(t, dir)
	cfg := testConfig(t, ds)
	slices.Reverse(cfg.Files)

	cursors, err := openCursors(cfg)
	if err != nil {
		t.Fatalf("openCursors() error = %v, want nil", err)
	}
	defer closeCursors(cursors)

	var got []uint16
	for _, c := range cursors {
		got = append(got, c.VenueID())
	}
	// synth.Shape's last venue has no files at all, so it has no cursor
	// here: a venue with nothing on disk is not part of this run.
	want := []uint16{1, 2, 3, 4}
	if !slices.Equal(got, want) {
		t.Errorf("openCursors(files in reverse order) venues = %v, want %v", got, want)
	}
}

func TestServerSubscribe(t *testing.T) {
	t.Run("a_block_subscriber_receives_every_record_in_canonical_order", func(t *testing.T) {
		dir := t.TempDir()
		ds := synth.Standard(t, dir)
		srv := newTestServer(t, testConfig(t, ds))
		stream := newFakeStream(t)

		if err := srv.Subscribe(blockRequest(), stream); err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		if len(stream.sent) != ds.RecordCount() {
			t.Fatalf("Subscribe() sent %d records, want %d", len(stream.sent), ds.RecordCount())
		}
		snapshots := 0
		var prev *api.Record
		for i, resp := range stream.sent {
			if resp.GetHasGap() {
				t.Fatalf("response %d reported gap %d-%d; Block promises no loss",
					i, resp.GetFirstMissedIndex(), resp.GetLastMissedIndex())
			}
			rec := resp.GetRecord()
			if rec == nil {
				t.Fatalf("response %d carries no record", i)
			}
			if prev != nil && !inCanonicalOrder(prev, rec) {
				t.Fatalf("response %d is out of canonical order: %v then %v", i, prev, rec)
			}
			if rec.GetRecordType() == uint32(store.RecordTypeSnapshotPointer) {
				snapshots++
				if got, want := len(rec.GetLevels()), int(rec.GetLevelCount()); got != want {
					t.Fatalf("response %d carries %d levels, want level_count = %d", i, got, want)
				}
				if rec.GetBidCount() > rec.GetLevelCount() {
					t.Fatalf("response %d has bid_count %d above level_count %d", i, rec.GetBidCount(), rec.GetLevelCount())
				}
			}
			prev = rec
		}
		if snapshots == 0 {
			t.Error("the run carried no snapshot pointer; this test proves nothing about decoded levels")
		}
	})

	t.Run("a_drop_subscriber_accounts_for_every_record_it_missed", func(t *testing.T) {
		dir := t.TempDir()
		ds := synth.Standard(t, dir)
		cfg := testConfig(t, ds)
		// Small enough that the writer laps this subscriber: the gap
		// fields have to carry what it missed.
		cfg.Capacity = 2
		srv := newTestServer(t, cfg)
		stream := newFakeStream(t)

		req := blockRequest()
		req.Mode = api.Mode_MODE_DROP
		if err := srv.Subscribe(req, stream); err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		var missed uint64
		for i, resp := range stream.sent {
			if !resp.GetHasGap() {
				continue
			}
			want := resp.GetLastMissedIndex() - resp.GetFirstMissedIndex() + 1
			if resp.GetMissedCount() != want {
				t.Errorf("response %d reports missed_count %d, want %d", i, resp.GetMissedCount(), want)
			}
			missed += resp.GetMissedCount()
		}
		if got := uint64(len(stream.sent)) + missed; got != uint64(ds.RecordCount()) {
			t.Errorf("received %d + missed %d = %d, want %d (the whole merged stream)",
				len(stream.sent), missed, got, ds.RecordCount())
		}
	})

	t.Run("a_request_that_leaves_something_implicit_is_rejected", func(t *testing.T) {
		dir := t.TempDir()
		ds := synth.Standard(t, dir)
		srv := newTestServer(t, testConfig(t, ds))

		noMode := blockRequest()
		noMode.Mode = api.Mode_MODE_UNSPECIFIED
		noStart := blockRequest()
		noStart.Start = nil
		zeroSpeed := blockRequest()
		zeroSpeed.SpeedNum, zeroSpeed.SpeedDen = 0, 0
		negativeSpeed := blockRequest()
		negativeSpeed.SpeedNum = -1
		unreachableSpeed := blockRequest()
		unreachableSpeed.SpeedNum, unreachableSpeed.SpeedDen = 1<<40, 1

		tests := []struct {
			name string
			req  *api.SubscribeRequest
			want codes.Code
		}{
			{name: "an_unspecified_backpressure_mode_is_never_defaulted", req: noMode, want: codes.InvalidArgument},
			{name: "an_unset_start_position_is_never_defaulted", req: noStart, want: codes.InvalidArgument},
			{name: "a_zero_speed_is_not_a_speed", req: zeroSpeed, want: codes.InvalidArgument},
			{name: "a_negative_speed_is_not_a_speed", req: negativeSpeed, want: codes.InvalidArgument},
			{name: "a_speed_outside_the_supported_range_is_rejected", req: unreachableSpeed, want: codes.InvalidArgument},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := srv.Subscribe(tt.req, newFakeStream(t))

				if got := status.Code(err); got != tt.want {
					t.Errorf("Subscribe(%v) code = %v, want %v", tt.req, got, tt.want)
				}
			})
		}
	})

	// The two speed subtests go through attach, not Subscribe: there is
	// one paced emit loop per server, so the second subscriber has to
	// arrive while the first is still attached, and a Subscribe call
	// only returns once its whole stream has drained. Drop subscribers,
	// so an undrained one never holds the writer at the barrier.
	t.Run("a_second_subscriber_naming_a_different_speed_is_rejected", func(t *testing.T) {
		dir := t.TempDir()
		ds := synth.Standard(t, dir)
		srv := newTestServer(t, testConfig(t, ds))
		if _, err := srv.attach(fanout.ModeDrop, fanout.Beginning(), testSpeed(t, 1, 1)); err != nil {
			t.Fatalf("attach() error = %v, want nil", err)
		}

		_, err := srv.attach(fanout.ModeDrop, fanout.Live(), testSpeed(t, 1, 2))

		if !errors.Is(err, errSpeedMismatch) {
			t.Errorf("attach() at 1/2 after 1/1 error = %v, want errSpeedMismatch", err)
		}
		if got := status.Code(subscribeStatus(err)); got != codes.FailedPrecondition {
			t.Errorf("subscribeStatus(errSpeedMismatch) code = %v, want %v", got, codes.FailedPrecondition)
		}
	})

	t.Run("an_equivalent_speed_written_differently_is_not_a_mismatch", func(t *testing.T) {
		dir := t.TempDir()
		ds := synth.Standard(t, dir)
		srv := newTestServer(t, testConfig(t, ds))
		if _, err := srv.attach(fanout.ModeDrop, fanout.Beginning(), testSpeed(t, 1, 1)); err != nil {
			t.Fatalf("attach() error = %v, want nil", err)
		}

		_, err := srv.attach(fanout.ModeDrop, fanout.Live(), testSpeed(t, 7, 7))

		// The run may already have ended by now, which is its own
		// rejection; what must never happen is 7/7 being read as a
		// different speed from 1/1.
		if errors.Is(err, errSpeedMismatch) {
			t.Errorf("attach() at 7/7 after 1/1 error = %v, want anything but errSpeedMismatch: NewSpeed reduces to lowest terms", err)
		}
	})
}

// bufconnBufferSize is the in-memory listener's own buffer. It is not
// a flow-control window; it only has to be large enough not to become
// the narrower limit of the two.
const bufconnBufferSize = 1 << 20

// startTestServer serves srv over an in-memory bufconn listener with
// the pinned server options, and returns a client dialed with the
// matching pinned client options. bufconn keeps the real grpc-go
// client, server, codec, framing and flow control in the test, and
// leaves only the operating system's sockets out.
func startTestServer(t *testing.T, srv *Server) api.ReplayServiceClient {
	t.Helper()

	lis := bufconn.Listen(bufconnBufferSize)
	gs := grpc.NewServer(grpcServerOptions()...)
	api.RegisterReplayServiceServer(gs, srv)

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		if err := gs.Serve(lis); err != nil {
			t.Errorf("Serve() error = %v, want nil", err)
		}
	}()

	opts := append(grpcDialOptions(),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	conn, err := grpc.NewClient("passthrough:///bufconn", opts...)
	if err != nil {
		t.Fatalf("NewClient() error = %v, want nil", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("conn.Close() error = %v, want nil", err)
		}
		gs.GracefulStop()
		<-serveDone
	})
	return api.NewReplayServiceClient(conn)
}

func TestGRPCFlowControlWindowsArePinned(t *testing.T) {
	t.Run("both_initial_windows_clear_the_size_grpc_go_would_ignore", func(t *testing.T) {
		tests := []struct {
			name string
			got  int
		}{
			{name: "the_stream_window_is_pinned", got: initialWindowSize},
			{name: "the_connection_window_is_pinned", got: initialConnWindowSize},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.got < bdpDisableThreshold {
					t.Errorf("window size = %d, want at least %d: grpc-go ignores a smaller one and keeps estimating",
						tt.got, bdpDisableThreshold)
				}
			})
		}
	})

	t.Run("a_pinned_client_and_server_stream_a_whole_run", func(t *testing.T) {
		dir := t.TempDir()
		ds := synth.Standard(t, dir)
		srv := newTestServer(t, testConfig(t, ds))
		client := startTestServer(t, srv)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stream, err := client.Subscribe(ctx, blockRequest())
		if err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		received := 0
		for {
			_, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("Recv() error = %v, want nil", err)
			}
			received++
		}

		if received != ds.RecordCount() {
			t.Errorf("received %d records over a pinned connection, want %d", received, ds.RecordCount())
		}
	})
}

func TestMetrics(t *testing.T) {
	t.Run("a_block_subscribers_run_counts_every_record_and_no_gap", func(t *testing.T) {
		dir := t.TempDir()
		ds := synth.Standard(t, dir)
		srv := newTestServer(t, testConfig(t, ds))

		if err := srv.Subscribe(blockRequest(), newFakeStream(t)); err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		tests := []struct {
			name string
			got  float64
			want float64
		}{
			{name: "every_record_is_counted", got: testutil.ToFloat64(srv.metrics.recordsEmitted), want: float64(ds.RecordCount())},
			{name: "a_block_subscriber_reports_no_gap", got: testutil.ToFloat64(srv.metrics.gapsDetected), want: 0},
			{name: "a_block_subscriber_misses_no_record", got: testutil.ToFloat64(srv.metrics.recordsMissed), want: 0},
			{name: "the_subscriber_gauge_returns_to_zero", got: testutil.ToFloat64(srv.metrics.subscribers), want: 0},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.got != tt.want {
					t.Errorf("metric = %v, want %v", tt.got, tt.want)
				}
			})
		}
		if got := testutil.ToFloat64(srv.metrics.bytesEmitted); got < float64(store.RecordSize*ds.RecordCount()) {
			t.Errorf("bytes emitted = %v, want at least %v (one fixed-stride record each)",
				got, store.RecordSize*ds.RecordCount())
		}
	})

	t.Run("a_lapped_drop_subscribers_counters_add_up_to_the_whole_run", func(t *testing.T) {
		dir := t.TempDir()
		ds := synth.Standard(t, dir)
		cfg := testConfig(t, ds)
		cfg.Capacity = 2
		srv := newTestServer(t, cfg)
		req := blockRequest()
		req.Mode = api.Mode_MODE_DROP

		if err := srv.Subscribe(req, newFakeStream(t)); err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}

		emitted := testutil.ToFloat64(srv.metrics.recordsEmitted)
		missed := testutil.ToFloat64(srv.metrics.recordsMissed)
		if emitted+missed != float64(ds.RecordCount()) {
			t.Errorf("emitted %v + missed %v = %v, want %d (the whole merged stream)",
				emitted, missed, emitted+missed, ds.RecordCount())
		}
		if missed > 0 && testutil.ToFloat64(srv.metrics.gapsDetected) == 0 {
			t.Errorf("records were missed but no gap was counted")
		}
	})

	t.Run("the_handler_serves_the_runs_own_registry", func(t *testing.T) {
		dir := t.TempDir()
		ds := synth.Standard(t, dir)
		srv := newTestServer(t, testConfig(t, ds))
		rec := httptest.NewRecorder()

		srv.MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

		if rec.Code != http.StatusOK {
			t.Fatalf("GET /metrics status = %d, want %d", rec.Code, http.StatusOK)
		}
		for _, name := range []string{"replay_records_emitted_total", "replay_emit_index", "replay_pacing_slip_nanoseconds", "replay_subscribers"} {
			if !strings.Contains(rec.Body.String(), name) {
				t.Errorf("GET /metrics body does not mention %s", name)
			}
		}
	})
}

func testSpeed(t *testing.T, num, den int64) fanout.Speed {
	t.Helper()

	s, err := fanout.NewSpeed(num, den)
	if err != nil {
		t.Fatalf("NewSpeed(%d, %d) error = %v, want nil", num, den, err)
	}
	return s
}

// inCanonicalOrder reports whether b follows a under the total ordering
// key (exchange_ts, venue_id, sequence_number, instrument_id).
func inCanonicalOrder(a, b *api.Record) bool {
	if a.GetExchangeTs() != b.GetExchangeTs() {
		return a.GetExchangeTs() < b.GetExchangeTs()
	}
	if a.GetVenueId() != b.GetVenueId() {
		return a.GetVenueId() < b.GetVenueId()
	}
	if a.GetSequenceNumber() != b.GetSequenceNumber() {
		return a.GetSequenceNumber() < b.GetSequenceNumber()
	}
	return a.GetInstrumentId() < b.GetInstrumentId()
}
