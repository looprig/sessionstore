SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

.PHONY: test fuzz fmt fmt-check vet staticcheck gosec vuln secure check build

GOFILES = GOWORK=off go run ./internal/modfiles/cmd/modfiles -root .

test:
	GOWORK=off go test -race ./...

fmt:
	$(GOFILES) | xargs -0 gofmt -w

fmt-check:
	@output="$$(mktemp)"; trap 'rm -f "$$output"' EXIT; \
		if ! $(GOFILES) | xargs -0 gofmt -l >"$$output"; then exit 1; fi; \
		if [[ -s "$$output" ]]; then cat "$$output"; exit 1; fi

vet:
	GOWORK=off go vet ./...

staticcheck:
	GOWORK=off go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...

CHECK_GO_DIRS = $(shell GOWORK=off go list -f '{{.Dir}}' ./...)

gosec:
	GOWORK=off go run github.com/securego/gosec/v2/cmd/gosec@v2.28.0 -quiet $(CHECK_GO_DIRS)

vuln:
	GOWORK=off go mod verify
	GOWORK=off go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...

# Fuzz targets are shaped correctly but only ever executed their seed corpus
# under `go test`, so a reachable panic could sit behind a green check. Run each
# target for a bounded time as part of check; a crasher is written to
# testdata/fuzz and is committable.
FUZZTIME ?= 30s
FUZZ_TARGETS = $(shell GOWORK=off go test -list '^Fuzz' . | grep '^Fuzz')

# A failing fuzz target has three shapes and only one of them is a bug here.
# Go writes a reproducer under testdata/fuzz for every failure it can attribute
# to an INPUT. The other two write nothing and are told apart only by the
# message text: a worker killed by a signal ("terminated by unexpected signal")
# and one that stopped answering the coordinator ("hung or terminated
# unexpectedly", "communicating with fuzzing process") are environmental, not
# findings. That text is the whole diagnosis, and it is exactly what a scrolled
# or piped run loses — which cost one investigation already.
#
# Each target's output is therefore streamed AND kept, and a failure ends by
# saying which shape it was and where the full text is.
#
# The environmental check runs FIRST for a reason that is not obvious and was
# established by killing a worker mid-run: a run interrupted while MINIMIZING
# leaves a file under testdata/fuzz even though it recorded no crash, and that
# file replays clean. A reproducer check ordered first would call it a finding.
FUZZLOGS ?= .fuzzlogs

# `set -o pipefail` is stated HERE rather than inherited from .SHELLFLAGS, and
# that is not belt-and-braces: .SHELLFLAGS arrived in GNU make 3.82, and the
# make that ships with macOS is 3.81, which ignores it silently. Every other
# recipe in this file is safe either way because none of them pipe; this one
# streams a fuzz run through tee and would otherwise report the exit status of
# tee, turning a failed fuzz target into a green check.
fuzz:
	@mkdir -p "$(FUZZLOGS)"
	@set -o pipefail; for target in $(FUZZ_TARGETS); do \
		log="$(FUZZLOGS)/$$target.log"; \
		echo "GOWORK=off go test -run ^$$target$$ -fuzz ^$$target$$ -fuzztime $(FUZZTIME) . | tee $$log"; \
		marker="$(FUZZLOGS)/$$target.started"; : >"$$marker"; \
		if ! GOWORK=off go test -run "^$$target$$" -fuzz "^$$target$$" -fuzztime $(FUZZTIME) . 2>&1 | tee "$$log"; then \
			echo "--- $$target failed; full output in $$log"; \
			if grep -qE 'unexpected signal|hung or terminated unexpectedly|communicating with fuzzing process' "$$log"; then \
				echo "--- ENVIRONMENTAL: the worker died or stopped answering, so nothing was attributed to an input."; \
				echo "--- re-run this target alone before investigating the package."; \
			elif grep -q 'failure while testing seed corpus entry' "$$log"; then \
				echo "--- SEED CORPUS: an entry already committed under testdata/fuzz fails, so no new reproducer is written."; \
				echo "--- the offending entry is named in $$log."; \
			elif [ -n "$$(find testdata/fuzz/$$target -type f -newer "$$marker" 2>/dev/null)" ]; then \
				echo "--- REPRODUCER: a new entry was written under testdata/fuzz/$$target; commit it and fix the target."; \
			else \
				echo "--- UNCLASSIFIED: no new reproducer and no known message; read $$log in full."; \
			fi; \
			exit 1; \
		fi; \
	done

secure: fmt-check vet staticcheck gosec vuln

build:
	GOWORK=off go build ./...

check: fmt-check vet staticcheck gosec vuln test fuzz build
