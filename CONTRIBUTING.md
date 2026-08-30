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

## Code of conduct

Be excellent to each other. Discussions stay technical and respectful; harassment
and personal attacks are not welcome.

## License

Contributions are licensed under Apache License 2.0 as described in
[`LICENSE`](LICENSE).
