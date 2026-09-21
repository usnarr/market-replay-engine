// Command replayd serves one deterministic replay run over gRPC.
//
// It is an assembly, not a place for new replay logic: it opens the
// hot-tier files, builds the merge and the fan-out ring from
// internal/*, and translates between those types and the wire schema in
// api/replay.proto. clock.RealClock is constructed here and nowhere
// else in this command; everything downstream takes the clock.Clock
// interface.
//
// "time.Now appears exactly once" is a first-party rule. grpc-go and
// the Go runtime read real time internally and are outside this
// repository's control; see docs/determinism.md for the exact scope of
// that exception.
package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"

	"replay/api"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "replayd:", err)
		os.Exit(1)
	}
}

// fileList is a repeatable -file flag. A repeated flag, not one
// comma-separated string: a path may legally contain a comma, and the
// server sorts the files itself anyway.
type fileList []string

func (f *fileList) String() string { return strings.Join(*f, ",") }

func (f *fileList) Set(v string) error {
	*f = append(*f, v)
	return nil
}

func run(args []string) error {
	fs := flag.NewFlagSet("replayd", flag.ContinueOnError)

	var files fileList
	fs.Var(&files, "file", "hot-tier file to replay; repeat once per file")
	listen := fs.String("listen", "127.0.0.1:0", "address to serve gRPC on")
	metricsListen := fs.String("metrics-listen", "", "address to serve Prometheus metrics on; empty disables them")
	workers := fs.Int("workers", 4, "merge decode worker count; changes concurrency only, never the merged stream")
	capacity := fs.Int("capacity", 1024, "fan-out ring slot count; must be a power of two")
	maxBlob := fs.Int("max-blob-bytes", 4096, "longest snapshot blob payload the ring will carry")
	seekTs := fs.Int64("from-ts", 0, "replay only the records at or after this exchange timestamp")
	watchdog := fs.Int64("watchdog-ns", 0, "evict a Block subscriber that holds the writer this long with no progress; 0 disables")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no -file given: there is nothing to replay")
	}

	// A seek to timestamp 0 is a real request, so "was it set" cannot be
	// read off the value.
	hasSeek := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "from-ts" {
			hasSeek = true
		}
	})

	srv, err := NewServer(Config{
		Files:           files,
		Workers:         *workers,
		Capacity:        *capacity,
		MaxBlobBytes:    *maxBlob,
		SeekTs:          *seekTs,
		HasSeek:         hasSeek,
		WatchdogTimeout: *watchdog,
	})
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		_ = srv.Close()
		return err
	}
	fmt.Fprintln(os.Stdout, "replayd: listening on", lis.Addr().String())

	gs := grpc.NewServer(grpcServerOptions()...)
	api.RegisterReplayServiceServer(gs, srv)

	var metricsSrv *http.Server
	if *metricsListen != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", srv.MetricsHandler())
		metricsSrv = &http.Server{Addr: *metricsListen, Handler: mux}
		go func() {
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				fmt.Fprintln(os.Stderr, "replayd: metrics:", err)
			}
		}()
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		gs.GracefulStop()
	}()

	serveErr := gs.Serve(lis)
	if metricsSrv != nil {
		// Close, not Shutdown: Shutdown waits for in-flight scrapes
		// with a deadline, and a deadline here would need a real clock
		// read this command does not get to make.
		_ = metricsSrv.Close()
	}
	closeErr := srv.Close()
	if serveErr != nil {
		return serveErr
	}
	return closeErr
}
