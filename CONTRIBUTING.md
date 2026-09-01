# Contributing to looprig/sessionstore

Thanks for contributing. `sessionstore` owns Looprig's transport-neutral durable
session aggregate over released Core wire records and Storage primitives.

## Before writing code

Read [`CLAUDE.md`](CLAUDE.md). Open an issue for non-trivial public API or persistence
changes so compatibility and migration behavior can be reviewed first.

Production imports are limited to the standard library, `github.com/looprig/core`,
and `github.com/looprig/storage`. Do not add local `replace` directives, vendored
dependencies, concrete providers, Harness, Factory, Host, or transport libraries.

## Build and test

Run these before pushing:

```sh
GOWORK=off go mod tidy
GOWORK=off go test -race ./...
GOWORK=off make check
```

Add tests before implementation, keep errors typed, and mutation-test dependency,
fencing, ordering, and tenant-boundary assertions. Keep one logical change per PR and
call out public API or persisted-format changes explicitly.

## Known follow-ups

These are known, deliberately left out of the `v0.1.0` release, and recorded here
so they are picked up by intent rather than rediscovered. Items 1 and 2 were
raised in review and judged non-blocking by an independent reviewer; item 3 is a
testing gap noted during release review.

1. **The sweep-cursor reconciliation is held by redundancy, not by structure.**
   `assertSweepCursorKindRolesAreUnderstood` is called from two cursor guards
   (`shards_test.go` and `host_targets_test.go`). Deleting either call leaves the
   suite green, because the other call site still catches the escape. The
   realistic future failure is a THIRD guard that reads cursor literals and
   forgets to call it. The fix is about three lines: have the reconciliation
   RETURN the file list both guards then walk, so removing the call stops
   compiling instead of silently weakening the guard.

2. **A bare uncalled generic instantiation is a known false positive.** A
   declaration of the shape `var f = someFunc[sweepCursorKind]` is an occurrence
   of the identifier that the role reconciliation does not account for, so it
   would fail the guard although it introduces no composite literal to attribute.
   A position-aware fix (roughly fourteen lines: record composite-literal type
   nodes and exclude them; `ast.Inspect` is pre-order, so the `CompositeLit` arm
   records its `Type` before the child is visited) was built and verified during
   review, and review then recommended NOT taking it: it closes a far more
   obscure shape than the `(*T)(nil)` conversion already handled, and it adds
   state to the most intricate walk in the file.

3. **Three claims that hold but nothing pins.** None is a correctness fault
   today; each is a place where a regression would be silent.
   - A stale Host incarnation that re-publishes over a row a newer incarnation
     withdrew is handled by `hostGenerationFence`, but nothing drives that
     interleaving. Impact if it regressed is liveness — capacity advertised for
     a Host that has drained — not correctness.
   - `TestUnmarkedBackendDoesNotProbeLegacyData` holds the OUTCOME (an unmarked
     backend holding `sessions/<uuid>` data opens as `tenant-v1`) rather than
     the absence of a probe. Sufficient for the guarantee as stated, but a probe
     that read legacy data and then ignored it would still pass. Asserting zero
     provider reads before the marker write would close it.
   - README documents that `ReadPublicJournalRequest.FromSeq` is the one route
     past a journal record that fails to decode. That was verified by hand
     against a corrupt frame — a walk from the start returns
     `JournalErrorIntegrity`, and a walk positioned above the bad sequence
     succeeds — but no committed test pins it, so the documented escape hatch
     could be removed without a failure.

## Code of conduct

Be excellent to each other. Discussions stay technical and respectful; harassment
and personal attacks are not welcome.

## License

Contributions are licensed under Apache License 2.0 as described in
[`LICENSE`](LICENSE).
