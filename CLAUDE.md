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
