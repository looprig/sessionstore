# Durable public create implementation plan

**Goal:** Tenant-global create identity with resumable preparation and admission,
acknowledging only an exact catalog and disposition inbox winner.

**Architecture:** Create-only reservation, public-create catalog version 3,
optional existing verified payload upload, then explicit marked disposition inbox
admission. Reuse catalog and inbox create helpers; add no transition or execution
state machine. Root approved the staged design before production edits.

**Tech stack:** Go, Storage OrderedIndex/KV/Blobs, real memstore fault fixtures.

## Approved contract

`PreparePublicCreate` validates a complete immutable identity and proposed
runtime mapping/times before provider I/O. It binds existing protocol/collision
witnesses, creates a tenant-scoped command reservation, and creates the session
catalog from that reservation's winner. It returns a preparation, never acceptance.
Existing generic catalog rows cannot be adopted. Reservation conflict checks bind
session, launch target (existing HostTargetKey), storage binding, opaque command
kind and payload digest/size. Winning runtime ID, times and initial workload survive
all retries. Current desired fields never participate in immutable identity checks.

`PutCommandPayload` supplies large bytes only after preparation establishes the
actual catalog. `AdmitPublicCreate` rereads and matches reservation/catalog, checks
supplied content and immutable object index using existing admission helpers, and
acknowledges only the exact marked pending inbox winner. Generic disposition
admission cannot use the catalog's create CommandID or emit the explicit create
marker. Command kind remains opaque; no string inference determines create status.

The reservation uses a separate version-1 namespace with tenant ordering scope and
opaque CommandID stable key. Catalog version 3 retains immutable create data; v1/v2
remain unchanged. An explicit marker in v2 inbox descriptors fails older strict
canonical decoding. All reads are fixed exact lookups, with no scan or recovery
worker. Errors and cancellations return no accepted result, including ambiguous
committed writes. A retry may need to supply bytes again. Orphan reservations and
uploads are retained; no reclamation or reconstruction is promised.

## Implementation sequence

1. Add failing public-create API and behavior tests in `public_create_test.go`.
   Run `GOWORK=off go test -run '^TestPublicCreate' .` and inspect failure.
2. Add `public_create.go` for strict reservation codec, validation and exact
   preparation/read operations. Extract the shared create portion in `catalog.go`;
   preserve existing generic behavior except refusal to adopt public-create rows.
3. Add catalog v3 encoding/decoding, immutable identity validation and preservation.
   Test old decoders fail closed and desired updates preserve retries.
4. Extract reusable disposition admission helper in `disposition_inbox.go`, add
   explicit public-create marker, and add final admission with exact agreement.
5. Add real restart, cancellation and commit-then-error tests after reservation,
   catalog, payload/index and inbox writes. Race 12 independent replicas, competing
   commands/sessions, and independently uploaded equivalent oversized content.
6. Add corruption, cross-identity, legacy/generic collisions, opaque ID, pre-I/O
   validation, bounded provider count and protocol fencing tests. Add codec fuzz
   coverage and isolated snapshot mutants for decisive authority checks.
7. Run full native `make check`, standalone race suite and default fuzz duration.
   Never edit source during native checks. Record exact evidence and commit only
   reviewed repository-local files on local main. No push, tag or dependency edits.

This is a SessionStore prerequisite; it does not accept Factory A3.1 or enable
Host dispatch, claims, application, settlement or migration.

## API boundaries for consumers

Only successful `AdmitPublicCreate` verifies all three authoritative records and
acknowledges a public create. Generic `GetDispositionCommand` and due pages retain
their catalog-binding checks; their pending records and `PublicCreate` marker are
not independently verified public-create acceptance proof. Future dispatch/claim
code must not treat the marker alone as public-create provenance.

The final context checks are specific to `PreparePublicCreate` and
`AdmitPublicCreate`. Generic `AdmitDispositionCommand` retains its previous
semantics: if a provider commits, cancels the context and nevertheless returns a
successful response, generic admission may return success. The dedicated public
create APIs return an error and no preparation/ACK in that case.

Preparation may leave a mode/collision witness and losing reservation behind.
An error never promises rollback. There is no automated reconciliation, payload
reconstruction, orphan cleanup or catalog recreation after deletion. Large-payload
admission verifies the immutable metadata index, not current body existence after
external deletion; consumption must still verify the full object stream.

## Verification evidence (2026-09-05)

Implemented above parent `1c162e9818a3d1ecd5b89a1a7a78e960bc1d614f`.
Initial API test failed on the absent preparation/admission methods. Subsequent
red tests exposed acceptance of duplicate immutable catalog members, an invalid
empty-content reservation, and canceled successful provider responses; the
corresponding checks now pass.

`make check` exited 0: formatting, vet, staticcheck, gosec, module verification,
govulncheck (no vulnerabilities), full standalone race suite, all **20 fuzz
targets at the native 30-second duration**, and build. No fuzz failure or retry
occurred. The native run used its existing default worker setting; subsequent
standalone verification used `GOMAXPROCS=8 GOWORK=off go test ./...` and passed.
Production and test files remained byte-identical to the frozen baseline through
the native run. Native log and snapshots are retained locally under
`/private/tmp/sessionstore-public-create-check.OKQFHo/`.

Five independent snapshot mutations compiled and failed on behavioral assertions:

- Reservation equality removed: conflicting identity was accepted.
- Catalog agreement removed: competing commands returned different winners.
- Explicit marker comparison removed: unmarked create was acknowledged and a
  generic retry adopted a marked second command.
- Winning runtime/timestamp comparison removed: mismatched runtime ID, accepted
  timestamp and deadline were acknowledged.
- Payload content comparison removed: wrong content was admitted.

Real memstore tests cover twelve Store replicas, reopen after failure at each
durable stage, before/after-commit faults, canceled successful responses,
independent oversized uploads and winning references, mutable desired changes,
losing reservations, deleted catalogs, legacy/generic collisions and fences,
cross-tenant opaque IDs, pre-I/O validation, exact provider-call bounds and scan
refusal. The frozen old descriptor accepts ordinary v2 bytes but rejects the
marked v2 record; old catalog versions reject public-create v3.

This evidence is for the Store prerequisite only. Independent root acceptance,
release, Factory adoption and the broader ownership/settlement integration gate
remain separate.
