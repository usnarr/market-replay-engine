# See CLAUDE.md for the invariants these targets enforce.
# See plans/01-repo-and-toolchain.md (not committed) for the design behind this file.

.PHONY: test bench determinism profile lint fuzz

# Full suite with -race. Covers internal/... and cmd/replayd.
# cmd/convert and cmd/catalogue are separate modules; run their own
# `go test ./...` from inside each directory, or extend this target
# once those modules have code worth testing.
test:
	go test -race ./...

# go test -bench -benchmem, never combined with -race: the race
# detector's own instrumentation allocates and would fail the
# zero-allocation gate regardless of the code under test.
# Writes machine-readable output to bench/results/ for the
# regression comparison described in plans/13-bench-and-profiles.md.
bench:
	mkdir -p bench/results
	go test -run=^$$ -bench=. -benchmem ./... | tee bench/results/latest.txt

# Replays the same dataset at 1, 4, 16, and 64 workers and asserts
# an identical output hash, at the merged stream and at every Block
# subscriber. This is the project's core assertion. See
# CLAUDE.md's Project invariants section.
determinism:
	go test -run TestDeterminism -v ./internal/merge/... ./internal/fanout/...

# CPU and memory profile, writes profiles/. Generate a committed SVG
# per version with: go tool pprof -svg <profile> > profiles/<name>.svg
profile:
	mkdir -p profiles
	go test -run=^$$ -bench=. -cpuprofile=profiles/cpu.prof -memprofile=profiles/mem.prof ./...

# go vet, staticcheck, and the project's own determinism analyzer.
lint:
	go vet ./...
	staticcheck ./...
	go run ./cmd/lint-determinism ./...

# Fuzzes the binary record and header decoder. Short duration,
# suitable for routine CI use; run a longer session periodically,
# not per-commit.
fuzz:
	go test -fuzz=FuzzDecode -fuzztime=30s ./internal/store/...
