# CLAUDE.md — sessionstore

`sessionstore` is the transport-neutral durable session aggregate. Its production
code may import only the Go standard library, `github.com/looprig/core`, and
`github.com/looprig/storage`. Concrete providers and service/runtime packages belong
at composition boundaries, never here.

## Dependency boundary

- Use exact released Core and Storage versions.
- Do not add `replace` directives or vendor dependencies.
- Do not import Harness, Factory, Host, inference, Centrifuge, or concrete storage
  providers from production code.
- Tests may use `github.com/looprig/storage/memstore`; cross-provider behavior tests
  belong in the integration repository.

## Backend requirements

- `Open` requires the backend's `Blobs` to implement Storage's optional
  `BlobReaderLifecycle` with a positive close bound. Storage's `memstore` and
  `natsstore` satisfy it; `fsstore` deliberately does not and is refused with
  `*InvalidBackendError{Component: "BlobReaderLifecycle"}` before layout or any
  other provider I/O. Do not weaken this into a best-effort filesystem path.
- The backend layout and the control shard count are persisted in an immutable
  layout marker at the first `Open` and compared on every later one. Neither is a
  runtime setting; changing either for a populated backend is an offline
  migration.
- Do not add a cache in front of any read here: every read is a direct provider
  read or a bounded provider query, and a cached revision defeats the
  compare-and-swap fences.

## Not implemented, deliberately

- **Gate continuation.** Multiple gate projections per session are durable and
  readable and their deadlines are indexed, but nothing here decides what
  happens when one expires: `ListDueGates` is a read, `ResolveGate` starts no
  continuation, and the active-continuation pointer role is a `Set`/`Get`/`Clear`
  triple over an object reference that nothing in this package reads to decide
  anything. Do not document or imply otherwise.
- **Object reclamation.** A verified object whose reference never committed is
  left behind as an orphan. The list and delete helpers are unexported; there is
  no caller-facing garbage collector.
- **Retention and reaping.** Nothing is reclaimed for age.
  `ReconcileHostTargets` withdraws lapsed capacity rows, but that is a liveness
  sweep and the row is retained and reused. The only safe reaper is one that
  removes a session's whole scope at once, because the registry and pointer rows
  ARE that session's fences.

## Code and security

- Keep public contracts transport-neutral and use Core's `sessionwire/v1` records.
- Return typed errors from package-level APIs and preserve causes with `Unwrap`.
- Scope every session-domain operation by authenticated tenant and session identity.
- Keep I/O bounded and context-aware. Never implement ordered queries with global
  key scans.
- Persist objects before records that reference them, and fail closed on ambiguity.

## Testing and build

- Follow red-green-refactor. Use table-driven parallel subtests where cases share a
  shape, and mutation-test important guards.
- Run every Go command standalone with `GOWORK=off`.
- Before committing, run `GOWORK=off go test -race ./...`, `GOWORK=off make check`,
  `git diff --check`, and inspect `git status --short`.
