SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

.PHONY: test fuzz fmt fmt-check vet staticcheck gosec vuln secure check build

GOFILES = GOWORK=off go run ./internal/modfiles/cmd/modfiles -root .

# PIPEFAIL is stated in each piping recipe rather than inherited from
# .SHELLFLAGS above, and that is not belt-and-braces: .SHELLFLAGS arrived in GNU
# make 3.82 and the make that ships with macOS is 3.81, which ignores it
# SILENTLY. Without it a pipeline reports the status of its LAST stage, so a
# recipe whose real work is upstream of a pipe passes when that work fails.
#
# Every piping recipe in this file must use it, and all three did have the bug:
# `fmt` and `fmt-check` pipe the file enumerator into gofmt, so a crashing
# enumerator formatted nothing and checked nothing while exiting 0, and `fuzz`
# pipes a fuzz run through tee, so a failed target reported tee's success. A
# recipe added later that pipes needs this too; one that does not, does not.
PIPEFAIL := set -o pipefail;

test:
	GOWORK=off go test -race ./...

fmt:
	$(PIPEFAIL) $(GOFILES) | xargs -0 gofmt -w

fmt-check:
	@$(PIPEFAIL) output="$$(mktemp)"; trap 'rm -f "$$output"' EXIT; \
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
FUZZLOGS ?= .fuzzlogs

# Classifying a fuzz failure: what the ARTIFACT does, and what nobody can know.
#
# A failure ends by saying which shape it was and where the full text is. The
# output is streamed AND kept because that text is part of the diagnosis and it
# is exactly what a scrolled or piped run loses — which cost one investigation.
#
# Three answers were tried here and the first two were wrong in the same way, so
# they are recorded rather than left to be re-derived:
#
#   - "Only an input-attributed failure writes under testdata/fuzz" is FALSE. A
#     run interrupted while MINIMIZING writes a file too. Presence of an
#     artifact does not make a finding.
#   - "The message text separates them" is FALSE and dangerous. A target that
#     dies of a runtime fatal error — concurrent map writes, a stack overflow —
#     kills its worker, so it prints the same "hung or terminated unexpectedly"
#     a killed worker prints, AND writes a real reproducer. No ordering of text
#     patterns fixes that: nothing in the text distinguishes the two.
#   - "An artifact that replays clean was left by an interrupted run" is ALSO
#     FALSE, and it is the converse of a true observation, which is why it looks
#     safe. What is true is that an interrupted run leaves an artifact that
#     replays clean. The converse fails for every finding that is not
#     reproducible from ONE input in a FRESH process: accumulation, resource
#     exhaustion, cold start, first-write, ordering between inputs. Those are
#     ordinary classes. Acting on the converse deleted a real reproducer and
#     exited zero.
#
# So the classifier asserts only what it can establish. A REPLAY THAT FAILS is a
# finding, whatever the run printed — and the replay runs -count=5, because a
# repetition-dependent failure is cheap to convert into a definite finding and
# expensive to misfile. A REPLAY THAT PASSES establishes that the input alone in
# a fresh process is not enough, and NOTHING MORE: it is reported as
# UNREPRODUCIBLE, a name that is true of an interrupted run and of a
# multi-input finding alike, and it FAILS THE BUILD.
#
# The artifact is QUARANTINED, never deleted. Moving it out of testdata/fuzz
# solves the corpus poisoning completely — an unreproducible entry left there
# fails every later warmup from inside the corpus instead of from the fuzzer —
# while keeping the one thing that cannot be reconstructed. .fuzzlogs is
# gitignored, so a quarantined input is out of the corpus without being out of
# reach. There is no case in which deleting beats moving.
#
# A replay that exits zero having matched NO SUBTEST is not a clean replay, it
# is no replay at all — an empty -run filter exits zero — so the replay runs -v
# and a PASS line for the exact subtest is required before its success is
# believed. Every artifact the run wrote is examined, not the first: a second
# one left behind poisons the corpus just as well.
#
# ONE branch retries, and the split is exactly "premise proven" against "premise
# assumed". A failure that wrote NO artifact and printed a worker-death message
# is a case where nothing was attributed to an input — an input-attributed
# failure always writes an artifact, runtime fatal errors included — so a class
# that by construction has no input cannot be a masked flaky target. That one is
# retried once, because `make check` gates every task in this program and Go's
# coordinator shutdown race is not this package's to fix. Every branch that has
# an artifact fails the build.
fuzz:
	@$(PIPEFAIL) \
	targets="$(FUZZ_TARGETS)"; \
	if [ -z "$$targets" ]; then \
		echo "--- no fuzz targets were listed, so this stage would pass without running anything."; \
		echo "--- the package most likely does not compile: run 'GOWORK=off go build ./...'"; \
		exit 1; \
	fi; \
	mkdir -p "$(FUZZLOGS)"; \
	for target in $$targets; do \
		attempt=1; \
		while :; do \
			log="$(FUZZLOGS)/$$target.log"; \
			if [ "$$attempt" -gt 1 ]; then log="$(FUZZLOGS)/$$target.retry$$attempt.log"; fi; \
			marker="$$(mktemp)"; \
			echo "GOWORK=off go test -run ^$$target$$ -fuzz ^$$target$$ -fuzztime $(FUZZTIME) . | tee $$log"; \
			if GOWORK=off go test -run "^$$target$$" -fuzz "^$$target$$" -fuzztime $(FUZZTIME) . 2>&1 | tee "$$log"; then \
				rm -f "$$marker"; break; \
			fi; \
			artifacts="$$(mktemp)"; \
			find "testdata/fuzz/$$target" -type f -newer "$$marker" >"$$artifacts" 2>/dev/null || true; \
			rm -f "$$marker"; \
			echo "--- $$target failed; full output in $$log"; \
			examined=0; class=finding; \
			while IFS= read -r artifact; do \
				[ -n "$$artifact" ] || continue; \
				examined=1; \
				name="$$(basename "$$artifact")"; \
				replay="$(FUZZLOGS)/$$target.$$name.replay.log"; \
				if ! GOWORK=off go test -run "^$$target$$/^$$name$$" -count=5 -v . >"$$replay" 2>&1; then \
					echo "--- REPRODUCER: $$artifact REPLAYS AS A FAILURE (replay in $$replay)."; \
					echo "--- this is a finding whatever the run printed. Commit it and fix the target."; \
				elif ! grep -q -- "--- PASS: $$target/$$name" "$$replay"; then \
					echo "--- UNCLASSIFIED: replaying $$artifact matched no subtest, so its exit status"; \
					echo "--- says nothing about the input. Left in place; read $$replay and $$log."; \
				else \
					quarantine="$(FUZZLOGS)/$$target.$$name.unreplayable"; \
					mv "$$artifact" "$$quarantine"; \
					rmdir "$$(dirname "$$artifact")" 2>/dev/null || true; \
					echo "--- UNREPRODUCIBLE: the run failed and wrote $$name, which does not replay from"; \
					echo "--- that input alone in a fresh process. That is either an interrupted run or a"; \
					echo "--- finding needing more than one input: accumulation, cold start, a race."; \
					echo "--- Quarantined to $$quarantine (kept, and out of the seed corpus); read $$log."; \
				fi; \
			done <"$$artifacts"; \
			rm -f "$$artifacts"; \
			if [ "$$examined" = 0 ]; then \
				if grep -qE 'unexpected signal|hung or terminated unexpectedly|communicating with fuzzing process|waiting for fuzzing process|context deadline exceeded' "$$log"; then \
					class=environmental; \
					echo "--- ENVIRONMENTAL: the coordinator lost a worker and no input was attributed."; \
				elif grep -q 'failure while testing seed corpus entry' "$$log"; then \
					echo "--- SEED CORPUS: an entry already committed under testdata/fuzz fails, so no new"; \
					echo "--- reproducer is written. The offending entry is named in $$log."; \
				else \
					echo "--- UNCLASSIFIED: no artifact and no known message; read $$log in full."; \
				fi; \
			fi; \
			if [ "$$class" = environmental ] && [ "$$attempt" -lt 2 ]; then \
				echo "--- retrying $$target once: nothing was attributed to an input, so this failure"; \
				echo "--- is not this package's. A recurrence fails the build and both logs are kept."; \
				attempt=$$((attempt + 1)); \
				continue; \
			fi; \
			exit 1; \
		done; \
	done

secure: fmt-check vet staticcheck gosec vuln

build:
	GOWORK=off go build ./...

check: fmt-check vet staticcheck gosec vuln test fuzz build
