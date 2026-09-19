# See CLAUDE.md for the invariants these targets enforce.
# See plans/01-repo-and-toolchain.md (not committed) for the design behind this file.

.PHONY: test bench determinism profile lint fuzz

# Full suite with -race. Covers internal/... and cmd/replayd, plus
# cmd/lint-determinism (a separate module, run from inside its own
# directory -- see plans/01-repo-and-toolchain.md). cmd/convert and
# cmd/catalogue are separate modules too, still with no code worth
# testing; extend this target once they have some.
test:
	go test -race ./...
	cd cmd/lint-determinism && go test -race ./...

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

# go vet, staticcheck, and the project's own determinism analyzer,
# run over ./... from the repo root. cmd/lint-determinism is a
# separate module (see plans/01-repo-and-toolchain.md), so it needs
# its own go vet / staticcheck pass from inside its own directory --
# not the determinism analyzer itself: its testdata/ fixtures contain
# deliberate violations, so running the analyzer against its own
# module would fail by design.
#
# vet's unsafeptr check is disabled for internal/store alone. That
# package turns the address MapViewOfFile returns into a byte slice,
# which is the one place in this repository that needs unsafe and is
# exactly what unsafeptr is built to flag. The check stays on
# everywhere else. Only a Windows build compiles that file, so this
# would otherwise be red on a developer's machine and invisible in CI,
# where the lint job runs on Linux.
lint:
	go vet $(shell go list ./... | grep -v '^replay/internal/store$$')
	go vet -unsafeptr=false ./internal/store/...
	staticcheck ./...
	go run ./cmd/lint-determinism ./...
	cd cmd/lint-determinism && go vet ./... && staticcheck ./...

# Fuzzes the binary record and header decoder. Short duration,
# suitable for routine CI use; run a longer session periodically,
# not per-commit.
fuzz:
	go test -fuzz=FuzzDecode -fuzztime=30s ./internal/store/...
