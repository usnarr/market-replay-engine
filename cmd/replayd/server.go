package main

import (
	"cmp"
	"context"
	"errors"
	"math"
	"net/http"
	"slices"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"replay/api"
	"replay/internal/clock"
	"replay/internal/fanout"
	"replay/internal/merge"
	"replay/internal/store"
)

// Errors this server produces itself, as opposed to the ones it
// translates out of internal/*.
var (
	errModeUnspecified = errors.New("replayd: mode must be MODE_BLOCK or MODE_DROP, never left unspecified")
	errStartUnset      = errors.New("replayd: start position must be set")
	errSpeedMismatch   = errors.New("replayd: the run is already paced at a different speed")
)

// gRPC flow-control window sizes, pinned explicitly at both ends.
//
// Left unset, grpc-go sizes the HTTP/2 flow-control window with a BDP
// estimator that times its own ping round trips — a real-time read
// inside the transport, on the path that decides when a Block
// subscriber's backpressure actually engages. Passing an explicit
// initial window sets grpc-go's StaticWindowSize, which is what turns
// that estimator off for the connection's whole life
// (google.golang.org/grpc/internal/transport: the estimator is
// constructed only when StaticWindowSize is false).
//
// grpc-go ignores an initial window below 64 KiB, so the value has to
// clear bdpDisableThreshold to mean anything. 1 MiB windows and 512 KiB
// transport buffers are ordinary values for a high-throughput stream;
// nothing here depends on the exact number, only on it being pinned
// rather than estimated. See docs/determinism.md.
const (
	initialWindowSize     = 1 << 20
	initialConnWindowSize = 1 << 20
	readBufferSize        = 1 << 19
	writeBufferSize       = 1 << 19

	// bdpDisableThreshold is grpc-go's own default window size, below
	// which it ignores a configured initial window.
	bdpDisableThreshold = 65535
)

// grpcServerOptions returns the options every replayd gRPC server is
// constructed with. See the window-size constants above.
func grpcServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.InitialWindowSize(initialWindowSize),
		grpc.InitialConnWindowSize(initialConnWindowSize),
		grpc.ReadBufferSize(readBufferSize),
		grpc.WriteBufferSize(writeBufferSize),
	}
}

// grpcDialOptions is the client half of the same pinning. A client that
// dials without it leaves its own receive window estimated, which puts
// the real-time read back on the other end of the same stream.
func grpcDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithInitialWindowSize(initialWindowSize),
		grpc.WithInitialConnWindowSize(initialConnWindowSize),
		grpc.WithReadBufferSize(readBufferSize),
		grpc.WithWriteBufferSize(writeBufferSize),
	}
}

// Config is one replay run, fixed before the server serves anything.
type Config struct {
	// Files are the hot-tier files to replay, in any order. They are
	// grouped into one cursor per venue by the venue each file's own
	// header declares, never by their order on the command line.
	Files []string

	// Workers is the merge's decode concurrency. It changes decode and
	// I/O concurrency only, never the merged stream.
	Workers int

	// Capacity and MaxBlobBytes size the fan-out ring.
	Capacity     int
	MaxBlobBytes int

	// SeekTs, when HasSeek is set, trims every venue to the records at
	// or after it before the merge ever sees them. It is a property of
	// the run; a subscriber's own start position is the StartAt in its
	// Subscribe request, which is a different thing resolved against
	// the ring.
	SeekTs  int64
	HasSeek bool

	// WatchdogTimeout is fanout.Config.WatchdogTimeout: how long, in
	// nanoseconds, a stalled Block subscriber may hold the writer back
	// before it is evicted. Zero disables it.
	WatchdogTimeout int64

	// ManifestPath is where the run manifest is written when the run
	// ends. Empty writes none. See manifest.go.
	ManifestPath string
}

// metrics is the server's instrument set. Every field is a concrete
// prometheus.Counter or Gauge, resolved once at construction and never
// a *CounterVec looked up by label: WithLabelValues does a map lookup
// and builds a label slice on every call, which is both an allocation
// and a data-dependent cost on a path this project keeps free of both.
// Counters and gauges only, never a histogram: a histogram observation
// needs a clock read to bucket by, and a clock read on the replay path
// is banned.
type metrics struct {
	recordsEmitted prometheus.Counter
	bytesEmitted   prometheus.Counter
	gapsDetected   prometheus.Counter
	recordsMissed  prometheus.Counter
	subscribers    prometheus.Gauge
}

// newMetrics registers the instrument set against reg. The emit index
// and the pacing slip are GaugeFuncs over the ring's own atomics, read
// when Prometheus scrapes rather than written per record, so neither
// costs the send loop anything at all.
func newMetrics(reg prometheus.Registerer, r *fanout.Ring) (*metrics, error) {
	m := &metrics{
		recordsEmitted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "replay_records_emitted_total",
			Help: "Records delivered to subscribers.",
		}),
		bytesEmitted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "replay_bytes_emitted_total",
			Help: "Record and snapshot payload bytes delivered to subscribers, before gRPC's own framing.",
		}),
		gapsDetected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "replay_gaps_detected_total",
			Help: "Gaps reported to Drop subscribers.",
		}),
		recordsMissed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "replay_records_missed_total",
			Help: "Records inside reported gaps, summed over every gap.",
		}),
		subscribers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "replay_subscribers",
			Help: "Subscribers currently attached.",
		}),
	}

	collectors := []prometheus.Collector{
		m.recordsEmitted, m.bytesEmitted, m.gapsDetected, m.recordsMissed, m.subscribers,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "replay_emit_index",
			Help: "The next emit index the writer will assign.",
		}, func() float64 { return float64(r.WriteIndex()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "replay_pacing_slip_nanoseconds",
			Help: "Cumulative nanoseconds the writer has spent parked at the Block barrier.",
		}, func() float64 { return float64(r.PacingSlipNanos()) }),
	}
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// Server assembles internal/store, internal/merge, internal/clock and
// internal/fanout behind the gRPC service. It holds no replay logic of
// its own: everything below the wire translation lives in internal/*,
// so the core packages stay testable with no gRPC server in the loop.
type Server struct {
	api.UnimplementedReplayServiceServer

	cfg     Config
	ring    *fanout.Ring
	merger  *merge.Merger
	reg     *prometheus.Registry
	metrics *metrics
	dataset []DatasetFile

	// clk is the one real clock this command constructs, kept so the
	// manifest's timing summary is read through the Clock interface
	// like everything else rather than from a second time.Now.
	clk clock.Clock

	mu        sync.Mutex
	started   bool
	speed     fanout.Speed
	pacer     *fanout.Pacer
	startedAt int64
	mix       SubscriberMix
	runDone   chan struct{}
	runErr    error
}

// NewServer opens cfg's files, builds one cursor per venue, and wires
// the merge and the ring. It does not start emitting: the first
// accepted Subscribe does that, so a subscriber asking for the
// beginning of the stream actually receives it.
func NewServer(cfg Config) (*Server, error) {
	cursors, err := openCursors(cfg)
	if err != nil {
		return nil, err
	}

	m, err := merge.NewConcurrentMerger(cursors, cfg.Workers)
	if err != nil {
		closeCursors(cursors)
		return nil, err
	}

	r, err := fanout.NewRing(fanout.Config{
		Capacity:     cfg.Capacity,
		MaxBlobBytes: cfg.MaxBlobBytes,
		// The one real clock this command constructs. Everything
		// downstream of here takes the clock.Clock interface.
		Clock:           clock.RealClock{},
		WatchdogTimeout: cfg.WatchdogTimeout,
	})
	if err != nil {
		_ = m.Close()
		return nil, err
	}

	reg := prometheus.NewRegistry()
	mx, err := newMetrics(reg, r)
	if err != nil {
		_ = m.Close()
		return nil, err
	}

	dataset, err := datasetIdentity(cfg.Files)
	if err != nil {
		_ = m.Close()
		return nil, err
	}

	return &Server{
		cfg:     cfg,
		ring:    r,
		merger:  m,
		reg:     reg,
		metrics: mx,
		dataset: dataset,
		clk:     clock.RealClock{},
	}, nil
}

// MetricsHandler serves this run's metrics in Prometheus' own text
// format. Each server keeps its own registry rather than the package
// default, so two servers in one process never collide.
func (s *Server) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{})
}

// openCursors opens every configured file and groups them into one
// cursor per venue. Venues are ordered by id and each venue's files by
// first exchange timestamp, with the path as the tie-break, so the same
// set of files always produces the same cursor list however the command
// line happened to order them. A sorted slice, never a map: this is
// CLAUDE.md's rule for a set whose order can reach output.
func openCursors(cfg Config) ([]*merge.Cursor, error) {
	type openFile struct {
		path  string
		first int64
		r     *store.Reader
	}

	files := make([]openFile, 0, len(cfg.Files))
	closeAll := func() {
		for _, f := range files {
			_ = f.r.Close()
		}
	}
	for _, path := range cfg.Files {
		r, err := store.Open(path)
		if err != nil {
			closeAll()
			return nil, err
		}
		files = append(files, openFile{path: path, first: firstExchangeTs(r), r: r})
	}
	slices.SortFunc(files, func(a, b openFile) int {
		if c := cmp.Compare(a.r.VenueID(), b.r.VenueID()); c != 0 {
			return c
		}
		if c := cmp.Compare(a.first, b.first); c != 0 {
			return c
		}
		return cmp.Compare(a.path, b.path)
	})

	var cursors []*merge.Cursor
	for i := 0; i < len(files); {
		venue := files[i].r.VenueID()
		j := i
		readers := make([]*store.Reader, 0, len(files)-i)
		for ; j < len(files) && files[j].r.VenueID() == venue; j++ {
			readers = append(readers, files[j].r)
		}
		i = j

		c, err := merge.NewCursor(venue, readers)
		if err != nil {
			closeAll()
			return nil, err
		}
		if cfg.HasSeek {
			// Before the cursor's first Next, which is what SeekTime
			// requires and what makes the merge's own seek-suffix
			// property hold for the whole run.
			if err := c.SeekTime(cfg.SeekTs); err != nil {
				closeAll()
				return nil, err
			}
		}
		cursors = append(cursors, c)
	}
	return cursors, nil
}

// firstExchangeTs returns r's first record's exchange timestamp, or
// math.MinInt64 for an empty file, which sorts it ahead of every file
// that holds something and contributes nothing either way.
func firstExchangeTs(r *store.Reader) int64 {
	if r.Len() == 0 {
		return math.MinInt64
	}
	return r.RecordAt(0).ExchangeTs
}

func closeCursors(cursors []*merge.Cursor) {
	for _, c := range cursors {
		_ = c.Close()
	}
}

// Subscribe registers one subscriber and streams its deliveries. The
// request fixes the backpressure mode, the start position and the run's
// speed; none of the three has a default this method will pick.
func (s *Server) Subscribe(req *api.SubscribeRequest, stream grpc.ServerStreamingServer[api.SubscribeResponse]) error {
	mode, err := modeFromProto(req.GetMode())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	start, startKind, err := startFromProto(req)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	speed, err := fanout.NewSpeed(req.GetSpeedNum(), req.GetSpeedDen())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	sub, err := s.attach(mode, start, speed)
	if err != nil {
		return subscribeStatus(err)
	}
	s.metrics.subscribers.Inc()
	s.countSubscriber(mode, startKind)
	defer func() {
		s.metrics.subscribers.Dec()
		_ = s.ring.Unsubscribe(sub)
	}()

	// grpc-go's stream context is the one place this server observes
	// "the client went away", and it is the transport's own concern,
	// never a clock read on the replay path. All it can do is stop this
	// one subscriber, which internal/fanout defines as out of band:
	// cancelling one subscriber cannot change what any other one
	// receives. A single-clause receive, never a select — the handler's
	// own return cancels this context, so this goroutine always ends.
	ctx := stream.Context()
	go func() {
		<-ctx.Done()
		_ = s.ring.Unsubscribe(sub)
	}()

	var levels []store.Level
	for {
		d, ok, err := sub.Next()
		if err != nil {
			return deliveryStatus(ctx, err)
		}
		if !ok {
			return nil
		}

		var resp *api.SubscribeResponse
		resp, levels, err = deliveryToProto(d, levels)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		if err := stream.Send(resp); err != nil {
			return err
		}

		// Concrete counters, resolved at construction: no label lookup,
		// no allocation, and every Add takes an integral value, so the
		// totals stay exactly comparable between two runs of the same
		// dataset. The byte count is the record and its payload, not
		// the framed wire size, which depends on gRPC's own codec and
		// would cost a second pass over every message to measure.
		s.metrics.recordsEmitted.Inc()
		s.metrics.bytesEmitted.Add(float64(store.RecordSize + len(d.Blob)))
		if d.HasGap {
			s.metrics.gapsDetected.Inc()
			s.metrics.recordsMissed.Add(float64(d.Gap.Count))
		}
	}
}

// attach registers a subscriber, starting the run itself if this is the
// first one. The run's speed is the first accepted request's speed:
// there is one paced emit loop per server, so a later request naming a
// different speed is rejected rather than silently ignored. The
// subscriber is registered before the emit loop starts, so the first
// subscriber really does see the first record.
func (s *Server) attach(mode fanout.BackpressureMode, start fanout.StartAt, speed fanout.Speed) (*fanout.Subscriber, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started && speed != s.speed {
		return nil, errSpeedMismatch
	}

	sub, err := s.ring.Subscribe(mode, start)
	if err != nil {
		return nil, err
	}
	if !s.started {
		s.started = true
		s.speed = speed
		s.startedAt = s.clk.Now()
		s.pacer = fanout.NewPacer(s.clk, speed)
		s.runDone = make(chan struct{})
		go s.run(s.pacer)
	}
	return sub, nil
}

// countSubscriber records one accepted subscription for the run
// manifest. The start kind comes from the request, not from
// StartAt.Kind: that method's own constants are unexported, so a
// caller outside internal/fanout cannot compare against them.
func (s *Server) countSubscriber(mode fanout.BackpressureMode, startKind string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if mode == fanout.ModeBlock {
		s.mix.Block++
	} else {
		s.mix.Drop++
	}
	switch startKind {
	case startKindBeginning:
		s.mix.StartBeginning++
	case startKindExchangeTs:
		s.mix.StartExchangeTs++
	case startKindEmitIndex:
		s.mix.StartEmitIndex++
	case startKindLive:
		s.mix.StartLive++
	}
}

// run drains the merge into the ring until the stream ends, then
// releases everything the run held.
//
// RunPaced deliberately does not call SetEnd when it fails: an aborted
// run has no valid prefix to hand subscribers. That leaves every
// subscriber parked in Next with nothing to wake it, so this is where
// the ring is ended on that path. The error is still reported, through
// Close.
func (s *Server) run(p *fanout.Pacer) {
	defer close(s.runDone)

	err := s.ring.RunPaced(s.merger, p)
	if err != nil {
		s.ring.SetEnd(s.ring.WriteIndex())
	}
	// RunPaced never closes the merger; its caller owns it.
	if cerr := s.merger.Close(); cerr != nil && err == nil {
		err = cerr
	}
	// The manifest records a failed run too: "this run aborted here" is
	// exactly what a catalogue needs.
	if merr := s.writeRunManifest(err); merr != nil && err == nil {
		err = merr
	}
	s.runErr = err
}

// writeRunManifest writes the run manifest, if one was configured. It
// runs on the emit goroutine once the stream has ended, which is the
// one moment "as observed at run end" actually names.
func (s *Server) writeRunManifest(runErr error) error {
	if s.cfg.ManifestPath == "" {
		return nil
	}

	s.mu.Lock()
	startedAt, speed, mix := s.startedAt, s.speed, s.mix
	s.mu.Unlock()

	ended := s.clk.Now()
	m := Manifest{
		Dataset: s.dataset,
		Config: RunConfig{
			SpeedNum:             speed.Num(),
			SpeedDen:             speed.Den(),
			Workers:              s.cfg.Workers,
			RingCapacity:         s.cfg.Capacity,
			MaxBlobBytes:         s.cfg.MaxBlobBytes,
			WatchdogTimeoutNanos: s.cfg.WatchdogTimeout,
		},
		Subscribers: mix,
		Result: RunResult{
			EmitIndexAtEnd:  s.ring.WriteIndex(),
			PacingSlipNanos: s.ring.PacingSlipNanos(),
		},
		Timing: Timing{
			StartedUnixNano: startedAt,
			EndedUnixNano:   ended,
			ElapsedNanos:    ended - startedAt,
		},
	}
	if s.cfg.HasSeek {
		seek := s.cfg.SeekTs
		m.Config.SeekExchangeTs = &seek
	}
	if runErr != nil {
		m.Result.Error = runErr.Error()
	}
	return writeManifest(s.cfg.ManifestPath, m)
}

// Close stops the run and releases the files it holds. It returns the
// run's own error, if it had one. It is not safe to call concurrently
// with itself.
func (s *Server) Close() error {
	s.mu.Lock()
	started, pacer, done := s.started, s.pacer, s.runDone
	s.mu.Unlock()

	if !started {
		return s.merger.Close()
	}
	pacer.Stop()
	<-done
	return s.runErr
}

// modeFromProto translates the wire enum. MODE_UNSPECIFIED is rejected,
// never defaulted: proto3 gives an unset field the zero value, so a
// zero that meant Block or Drop would make "backpressure mode is
// explicit at subscribe time" false for every client that forgot it.
func modeFromProto(m api.Mode) (fanout.BackpressureMode, error) {
	switch m {
	case api.Mode_MODE_BLOCK:
		return fanout.ModeBlock, nil
	case api.Mode_MODE_DROP:
		return fanout.ModeDrop, nil
	default:
		return fanout.ModeUnset, errModeUnspecified
	}
}

// Start-kind names, as the run manifest records them.
const (
	startKindBeginning  = "beginning"
	startKindExchangeTs = "exchange_ts"
	startKindEmitIndex  = "emit_index"
	startKindLive       = "live"
)

// startFromProto translates the start oneof and names which case it
// was. The four cases mirror internal/fanout's four StartAt
// constructors exactly.
func startFromProto(req *api.SubscribeRequest) (fanout.StartAt, string, error) {
	switch st := req.GetStart().(type) {
	case *api.SubscribeRequest_Beginning:
		return fanout.Beginning(), startKindBeginning, nil
	case *api.SubscribeRequest_ExchangeTs:
		return fanout.ExchangeTs(st.ExchangeTs), startKindExchangeTs, nil
	case *api.SubscribeRequest_EmitIndex:
		return fanout.EmitIndex(st.EmitIndex), startKindEmitIndex, nil
	case *api.SubscribeRequest_Live:
		return fanout.Live(), startKindLive, nil
	default:
		return fanout.StartAt{}, "", errStartUnset
	}
}

// deliveryToProto translates one delivery into its wire form, reusing
// levels as scratch space for a snapshot's decoded levels.
//
// Delivery.Blob aliases the subscriber's own buffer and is invalid
// after that subscriber's next call to Next, so every byte of it is
// copied out here, before the send loop goes round again.
func deliveryToProto(d fanout.Delivery, levels []store.Level) (*api.SubscribeResponse, []store.Level, error) {
	rec := &api.Record{
		ExchangeTs:     d.Record.ExchangeTs,
		SequenceNumber: d.Record.SequenceNumber,
		InstrumentId:   d.Record.InstrumentID,
		VenueId:        uint32(d.Record.VenueID),
		RecordType:     uint32(d.Record.RecordType),
		SideFlags:      uint32(d.Record.SideFlags),
		Price:          d.Record.Price,
		Size:           d.Record.Size,
		LevelCount:     uint32(d.Record.LevelCount),
	}

	if d.Record.RecordType == store.RecordTypeSnapshotPointer {
		var (
			bids int
			err  error
		)
		levels, bids, err = store.AppendLevelsFromPayload(levels[:0], d.Blob)
		if err != nil {
			return nil, levels, err
		}
		rec.BidCount = uint32(bids)
		rec.Levels = make([]*api.Level, len(levels))
		for i, l := range levels {
			rec.Levels[i] = &api.Level{Price: l.Price, Size: l.Size}
		}
	}

	resp := &api.SubscribeResponse{Record: rec, HasGap: d.HasGap}
	if d.HasGap {
		resp.FirstMissedIndex = d.Gap.FirstMissedIndex
		resp.LastMissedIndex = d.Gap.LastMissedIndex
		resp.MissedCount = d.Gap.Count
	}
	return resp, levels, nil
}

// subscribeStatus maps a registration failure onto a gRPC code. Each
// one says something different about what the client got wrong, so none
// of them collapses into Internal.
func subscribeStatus(err error) error {
	switch {
	case errors.Is(err, fanout.ErrModeUnset), errors.Is(err, fanout.ErrStartUnset):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, fanout.ErrStartLapped):
		return status.Error(codes.OutOfRange, err.Error())
	case errors.Is(err, fanout.ErrClosed), errors.Is(err, errSpeedMismatch):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// deliveryStatus maps a Next failure onto a gRPC code. A cancellation
// this server did not ask for is the client's own disconnect, and is
// reported as the context's own status rather than as a server fault.
func deliveryStatus(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, fanout.ErrCanceled):
		if cerr := ctx.Err(); cerr != nil {
			return status.FromContextError(cerr).Err()
		}
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, fanout.ErrEvicted):
		return status.Error(codes.Aborted, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
