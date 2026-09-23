# See CLAUDE.md for the invariants these targets enforce.
# See plans/01-repo-and-toolchain.md (not committed) for the design behind this file.

.PHONY: test bench determinism profile lint fuzz proto

# Full suite with -race. Covers internal/... and cmd/replayd, plus
# cmd/lint-determinism, cmd/convert and cmd/catalogue (separate modules,
# each run from inside its own directory -- see
# plans/01-repo-and-toolchain.md).
test:
	go test -race ./...
	cd cmd/lint-determinism && go test -race ./...
	cd cmd/convert && go test -race ./...
	cd cmd/catalogue && go test -race ./...

# go test -bench -benchmem, never combined with -race: the race
# detector's own instrumentation allocates and would fail the
# zero-allocation gate regardless of the code under test.
# Writes machine-readable output to bench/results/ for the
# regression comparison described in plans/13-bench-and-profiles.md.
#
# The exit code is checked explicitly, not piped through tee: make
# runs recipes under /bin/sh, and a POSIX pipeline's exit status is its
# last command's -- `go test ... | tee file` exits 0 even when go test
# itself failed, silently turning off the allocation gate from make
# bench's own point of view. BENCHCOUNT lets CI request several samples
# (for plans/13-bench-and-profiles.md's regression comparison) without
# reaching for a second target; the local default stays 1.
BENCHCOUNT ?= 1

bench:
	mkdir -p bench/results
	go test -run=^$$ -bench=. -benchmem -count=$(BENCHCOUNT) ./... > bench/results/latest.txt || { cat bench/results/latest.txt; exit 1; }
	cat bench/results/latest.txt

# Replays the same dataset at 1, 4, 16, and 64 workers and asserts
# an identical output hash, at the merged stream and at every Block
# subscriber. This is the project's core assertion. See
# CLAUDE.md's Project invariants section.
determinism:
	go test -run TestDeterminism -v ./internal/merge/... ./internal/fanout/...

# CPU and memory profile, one pair of .prof files per package in
# PROFILE_PKGS, then one committed SVG per profile: go test -bench refuses
# -cpuprofile/-memprofile against more than one package in a single
# invocation, so profiling the whole hot path needs one go test call per
# package, not one call over ./....
#
# The SVG is the durable artifact the specification asks for -- one
# committed pprof SVG per version -- so it is named with the current short
# commit hash and lives beside the others in profiles/. The raw .prof is a
# regenerable intermediate and stays gitignored.
#
# go tool pprof -svg shells out to Graphviz's `dot`, so this target needs
# dot on PATH. The .prof files are already written by the time the SVG step
# runs, so a missing dot costs the profiles nothing: install Graphviz and
# re-run.
#
# -cpuprofile's own sampling goroutine used to make internal/allocgate's
# byte pass report a false allocation, since that pass reads process-wide
# runtime.MemStats.TotalAlloc. AssertZero now skips its own check under
# -cpuprofile instead of failing (see its doc comment), so this target
# checks the benchmark run's exit status normally, the same as make bench.
#
# go test also leaves the compiled test binary (<pkg>.test, .exe on
# Windows) in the repo root when a profiling flag is set -- pprof's own
# symbolization needs it. Gitignored (*.test.exe); safe to delete
# afterward.
PROFILE_PKGS ?= internal/store internal/merge internal/fanout

profile:
	mkdir -p profiles
	sha=$$(git rev-parse --short HEAD); \
	for pkg in $(PROFILE_PKGS); do \
		name=$$(basename $$pkg); \
		go test -run=^$$ -bench=. -cpuprofile=profiles/$$name-cpu.prof -memprofile=profiles/$$name-mem.prof ./$$pkg/... || exit 1; \
		for kind in cpu mem; do \
			go tool pprof -svg profiles/$$name-$$kind.prof > profiles/$$name-$$kind-$$sha.svg || exit 1; \
		done; \
	done

# go vet, staticcheck, and the project's own determinism analyzer,
# run over ./... from the repo root. cmd/lint-determinism is a
# separate module (see plans/01-repo-and-toolchain.md), so it needs
# its own go vet / staticcheck pass from inside its own directory --
# not the determinism analyzer itself: its testdata/ fixtures contain
# deliberate violations, so running the analyzer against its own
# module would fail by design.
#
# The determinism analyzer also covers cmd/convert, which is in
# orderedPath's scope (see cmd/lint-determinism/scope.go). ./... from
# the repo root does not cross into a separate module, so that package
# is named explicitly. The workspace go.work makes it resolvable from
# here, so this still needs only one analyzer run.
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
	go run ./cmd/lint-determinism ./... replay/cmd/convert/...
	cd cmd/lint-determinism && go vet ./... && staticcheck ./...
	cd cmd/convert && go vet ./... && staticcheck ./...
	cd cmd/catalogue && go vet ./... && staticcheck ./...

# Fuzzes the binary record and header decoder. Short duration,
# suitable for routine CI use; run a longer session periodically,
# not per-commit.
fuzz:
	go test -fuzz=FuzzDecode -fuzztime=30s ./internal/store/...

# Regenerates api/replay.pb.go and api/replay_grpc.pb.go from
# api/replay.proto, which is the source of truth for both. The
# generated files are committed, so this target only has to run when
# the schema changes -- but it must run then, and its output must be
# committed in the same commit as the .proto change.
#
# buf, not protoc: buf bundles its own compiler as a pure Go binary, so
# generating needs no C++ protoc and no system package manager. The
# three tools install into $(go env GOPATH)/bin, which must be on PATH.
# See buf.yaml and buf.gen.yaml for the module layout and the plugin
# configuration. The plugin versions are pinned, not @latest: a floating
# generator would rewrite committed files on an unrelated commit.
PROTOC_GEN_GO_VERSION      ?= v1.36.12
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1

proto:
	go install github.com/bufbuild/buf/cmd/buf@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	buf lint
	buf generate
