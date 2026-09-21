package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"replay/api"
	"replay/internal/synth"
)

// readManifest runs a server to the end of its stream and returns the
// manifest it wrote.
func readManifest(t *testing.T, srv *Server, path string) Manifest {
	t.Helper()

	if err := srv.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v, want nil", path, err)
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("the manifest is not valid JSON: %v", err)
	}
	return m
}

func TestRunManifest(t *testing.T) {
	t.Run("a_finished_run_writes_every_documented_field", func(t *testing.T) {
		dir := t.TempDir()
		ds := synth.Standard(t, dir)
		cfg := testConfig(t, ds)
		cfg.ManifestPath = filepath.Join(dir, "run.json")
		cfg.SeekTs, cfg.HasSeek = 0, false
		srv := newTestServer(t, cfg)

		req := blockRequest()
		req.SpeedNum, req.SpeedDen = 10000, 5000
		if err := srv.Subscribe(req, newFakeStream(t)); err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		m := readManifest(t, srv, cfg.ManifestPath)

		if len(m.Dataset) != len(cfg.Files) {
			t.Errorf("manifest names %d files, want %d", len(m.Dataset), len(cfg.Files))
		}
		if !slices.IsSortedFunc(m.Dataset, func(a, b DatasetFile) int { return cmpDatasetFile(a, b) }) {
			t.Errorf("manifest dataset is not sorted by path: %v", m.Dataset)
		}
		for _, f := range m.Dataset {
			if f.Path == "" || !filepath.IsAbs(f.Path) {
				t.Errorf("dataset file path %q is not absolute", f.Path)
			}
			if f.Bytes <= 0 {
				t.Errorf("dataset file %q has size %d, want a positive size", f.Path, f.Bytes)
			}
		}
		// 10000/5000 reduces to 2/1: two requests for the same speed
		// written two different ways must produce the same manifest.
		if m.Config.SpeedNum != 2 || m.Config.SpeedDen != 1 {
			t.Errorf("manifest speed = %d/%d, want 2/1 (reduced)", m.Config.SpeedNum, m.Config.SpeedDen)
		}
		if m.Config.Workers != cfg.Workers || m.Config.RingCapacity != cfg.Capacity || m.Config.MaxBlobBytes != cfg.MaxBlobBytes {
			t.Errorf("manifest config = %+v, want workers %d, capacity %d, max blob %d",
				m.Config, cfg.Workers, cfg.Capacity, cfg.MaxBlobBytes)
		}
		if m.Config.SeekExchangeTs != nil {
			t.Errorf("manifest seek = %v, want null: this run seeked nowhere", *m.Config.SeekExchangeTs)
		}
		if m.Subscribers.Block != 1 || m.Subscribers.Drop != 0 || m.Subscribers.StartBeginning != 1 {
			t.Errorf("manifest subscriber mix = %+v, want one Block subscriber starting at the beginning", m.Subscribers)
		}
		if got, want := m.Result.EmitIndexAtEnd, uint64(ds.RecordCount()); got != want {
			t.Errorf("manifest emit index at end = %d, want %d", got, want)
		}
		if m.Result.Error != "" {
			t.Errorf("manifest records error %q, want none", m.Result.Error)
		}
		if m.Timing.EndedUnixNano < m.Timing.StartedUnixNano {
			t.Errorf("manifest timing ends (%d) before it starts (%d)", m.Timing.EndedUnixNano, m.Timing.StartedUnixNano)
		}
		if got := m.Timing.EndedUnixNano - m.Timing.StartedUnixNano; m.Timing.ElapsedNanos != got {
			t.Errorf("manifest elapsed = %d, want %d (ended - started)", m.Timing.ElapsedNanos, got)
		}
	})

	t.Run("a_seek_is_recorded_even_when_it_is_zero", func(t *testing.T) {
		dir := t.TempDir()
		ds := synth.Standard(t, dir)
		cfg := testConfig(t, ds)
		cfg.ManifestPath = filepath.Join(dir, "run.json")
		cfg.SeekTs, cfg.HasSeek = 0, true
		srv := newTestServer(t, cfg)

		req := blockRequest()
		req.Mode = api.Mode_MODE_DROP
		req.Start = &api.SubscribeRequest_EmitIndex{EmitIndex: 0}
		if err := srv.Subscribe(req, newFakeStream(t)); err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		m := readManifest(t, srv, cfg.ManifestPath)

		if m.Config.SeekExchangeTs == nil {
			t.Fatal("manifest seek = null, want 0: a seek to timestamp zero is a real seek")
		}
		if *m.Config.SeekExchangeTs != 0 {
			t.Errorf("manifest seek = %d, want 0", *m.Config.SeekExchangeTs)
		}
		if m.Subscribers.Drop != 1 || m.Subscribers.StartEmitIndex != 1 {
			t.Errorf("manifest subscriber mix = %+v, want one Drop subscriber starting at an emit index", m.Subscribers)
		}
	})

	t.Run("a_run_with_no_manifest_path_writes_nothing", func(t *testing.T) {
		dir := t.TempDir()
		ds := synth.Standard(t, dir)
		cfg := testConfig(t, ds)
		srv := newTestServer(t, cfg)

		if err := srv.Subscribe(blockRequest(), newFakeStream(t)); err != nil {
			t.Fatalf("Subscribe() error = %v, want nil", err)
		}
		if err := srv.Close(); err != nil {
			t.Fatalf("Close() error = %v, want nil", err)
		}

		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir() error = %v, want nil", err)
		}
		for _, e := range entries {
			if filepath.Ext(e.Name()) == ".json" {
				t.Errorf("an unconfigured run wrote %s", e.Name())
			}
		}
	})
}

func cmpDatasetFile(a, b DatasetFile) int {
	switch {
	case a.Path < b.Path:
		return -1
	case a.Path > b.Path:
		return 1
	}
	return 0
}
