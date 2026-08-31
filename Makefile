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

# Classifying a fuzz failure: REPLAY, not message text.
#
# A failing target has three shapes and only one of them is a finding, so a
# failure ends by saying which shape it was and where the full text is. The
# output is streamed AND kept because that text is the whole diagnosis and it is
# exactly what a scrolled or piped run loses — which cost one investigation.
#
# The discriminator is the ARTIFACT REPLAYING, and getting there took two wrong
# answers worth recording so they are not re-derived:
#
#   - "Only an input-attributed failure writes under testdata/fuzz" is FALSE. A
#     run interrupted while MINIMIZING writes a file too, and that file replays
#     clean. So presence of an artifact does not make a finding.
#   - "The message text separates them" is also FALSE, and dangerously so. A
#     target that dies of a runtime fatal error — `concurrent map writes`, a
#     stack overflow — kills its worker, so it prints the same "hung or
#     terminated unexpectedly" a killed worker prints, AND writes a real
#     reproducer. Classifying on text sends an operator away from a genuine
#     crash. No ordering of text patterns can fix that, because nothing in the
#     text distinguishes the two cases.
#
# What does distinguish them is what the artifact DOES: replay it, and let its
# exit status decide. A failing replay is the reproducer, whatever message the
# run printed. A clean replay means the file is debris, and the debris is
# REMOVED rather than reported: this run created it, it proves nothing, and Go
# reads testdata/fuzz as the seed corpus, so leaving it converts an interrupted
# run into a permanent warmup failure on every run afterwards. Message text is
# consulted only when no artifact was written at all, where it separates a dead
# worker from a committed seed corpus entry that no longer passes.
#
# ENVIRONMENTAL failures are RETRIED ONCE, and only they. The class is now
# provable rather than guessed — the artifact replayed clean, or no artifact
# exists and the message says the coordinator lost a worker — and by definition
# it is not attributable to this package. `make check` is the gate every task in
# this program runs, so a stage that fails once in a few dozen runs for reasons
# outside the package is a tax on every future task. A failure that recurs on
# the retry is reported and fails the build; both logs are kept.
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
			artifact="$$(find testdata/fuzz/$$target -type f -newer "$$marker" 2>/dev/null | head -1)"; \
			rm -f "$$marker"; \
			echo "--- $$target failed; full output in $$log"; \
			class=finding; \
			if [ -n "$$artifact" ]; then \
				if GOWORK=off go test -run "^$$target$$/$$(basename "$$artifact")$$" . >"$$log.replay" 2>&1; then \
					class=environmental; \
					echo "--- ENVIRONMENTAL: $$artifact was written but REPLAYS CLEAN, so the run was"; \
					echo "--- interrupted rather than finding anything. Removing it: this run created it,"; \
					echo "--- it proves nothing, and left in place it joins the seed corpus and fails"; \
					echo "--- every later run from inside the corpus instead of from the fuzzer."; \
					rm -f "$$artifact"; \
					rmdir "$$(dirname "$$artifact")" 2>/dev/null || true; \
				else \
					echo "--- REPRODUCER: $$artifact REPLAYS AS A FAILURE (replay output in $$log.replay)."; \
					echo "--- this is a finding whatever the run printed. Commit it and fix the target."; \
				fi; \
			elif grep -qE 'unexpected signal|hung or terminated unexpectedly|communicating with fuzzing process|waiting for fuzzing process|context deadline exceeded' "$$log"; then \
				class=environmental; \
				echo "--- ENVIRONMENTAL: the coordinator lost a worker and no input was attributed."; \
			elif grep -q 'failure while testing seed corpus entry' "$$log"; then \
				echo "--- SEED CORPUS: an entry already committed under testdata/fuzz fails, so no new"; \
				echo "--- reproducer is written. The offending entry is named in $$log."; \
			else \
				echo "--- UNCLASSIFIED: no artifact and no known message; read $$log in full."; \
			fi; \
			if [ "$$class" = environmental ] && [ "$$attempt" -lt 2 ]; then \
				echo "--- retrying $$target once: an environmental failure is not this package's."; \
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
