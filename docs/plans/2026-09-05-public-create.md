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

## Quality-review correction checkpoint (2026-09-05, unpublished)

The next agent inherits uncommitted edits on local main above
`9d07c0cd0710cd0fb8169a77f93ad1b54763298a`. Published v0.4.0 remains
`5170b531`; admission/public-create formats are still unpublished. Root approved
correcting these formats before their first release; no migration or change to
released catalog v1/v2 is intended.

Private tagged DTOs now separate public-create reservation identity, target and
proposal bytes from the public Go domain fields. Reservation v1 and catalog v3
provenance share those conversions. Target members are `agent_id`,
`runtime_compatibility_id`, and `placement`. Existing `desiredWorkloadWire` is
reused unchanged: `initial_workload` is always present, with
`{"payload_version":"","payload":null}` for empty workload. `payload_size`
remains present at zero. Full literal golden bytes pin reservation and catalog
v3 for empty and nonempty workload (binary bytes `00 ff 01`, base64 `AP8B`) and
opaque IDs with case, slash and colon. Both codecs reject omitted workload,
empty-object workload and omitted zero payload-size alternatives. Public Go
domain fields/tags remain unchanged.

`public_create_quality_test.go` also surgically changes only top-level catalog
tenant/session/agent/binding/created-at while preserving embedded provenance
and canonical JSON order. Codec and actual `GetCatalogEntry` checks require
the provenance identity error. A reservation filing matrix checks namespace,
stable key, ordering/ranking scopes, nonzero Due, zero order, zero/rewritten
revision, and deletion through the winner helper and `AdmitPublicCreate`,
requiring typed errors and no ACK. Revision is documented as defense in depth,
not an independently necessary admission invariant. Exported marker and generic
getter Godoc now state that catalog-binding reads do not verify reservation
proof or authorize future create dispatch; only dedicated admission checks all
three records. No generic cancellation or dispatch behavior changed.

Verification so far: golden tests failed on the original uppercase domain JSON
member names before production edits. After DTO conversion and correcting the
golden's existing catalog idempotency member, `GOWORK=off GOMAXPROCS=8 go test
-run '^TestPublicCreate' .` passed (0.410s). `git diff --check` passed.

Isolated copies for the two formerly surviving mutations are under
`/private/tmp/sessionstore-quality.3Rk1e2/catalog-mutant/` (catalog/provenance
comparison removed) and `filing-mutant/` (reservation `checkFiledScope` call
removed). Shared main and module-cache sources were never mutated. Initial
attempts failed at setup on sandbox cache permissions and are not mutation
evidence. Retries with `GOCACHE=/private/tmp/sessionstore-quality.3Rk1e2/go-cache`
compiled and were both killed by assertions: catalog mutant (0.284s) accepted
all five codec mismatches and agent/binding/created-at mismatches through the
real getter; tenant/session still hit the separate filed-record identity guard.
Filing mutant (0.342s) accepted ranking-scope and Due mismatches through both
the helper and actual `AdmitPublicCreate`, returning a created marked inbox ACK.
Command for each was `GOWORK=off GOMAXPROCS=8 GOCACHE=... go test -count=1
-run '^TestPublicCreate(CatalogProvenanceIdentity|ReservationFilingValidation)$' .`.
Both processes have exited; no verification process remains running.

Work paused for the user's requested agent handoff, then resumed by root.
Root inspected the full diff and froze these sources unchanged.

Root verification (2026-09-05, sources frozen at the diff above over
`9d07c0cd`): `GOWORK=off GOMAXPROCS=8 make check` PASSED, exit 0 — vet,
staticcheck v0.8.1, gosec v2.28.0, `go mod verify` (all modules verified),
govulncheck v1.6.0 (no vulnerabilities), the `-race` suite, **20** default
30-second fuzz targets with no crashers and no new interesting inputs, and
build. Standalone `GOWORK=off go test ./...` passed (3.540s). `GOWORK=off
go mod tidy` produced no `go.mod` or `go.sum` diff. `git diff --check` clean.
No `replace` directives and no vendor directory. The two mutant copies stayed
under `/private/tmp/sessionstore-quality.3Rk1e2/`; shared sources and the
module cache were never mutated.

These changes are now one separate repository-local commit on local `main`.
Still owed before any release: independent spec and code-quality rechecks of
the committed revision. Do not push, tag, update dependencies/root docs,
release, accept Factory A3.1, or lift the Host hold.

## Disposition inbox wire pinning (2026-09-05, over `289819c4`)

The independent quality recheck of `289819c4` passed with advisory A1: the third
record `AdmitPublicCreate` writes — the disposition inbox row — was still
marshalled straight off the exported `DispositionInboxRecord` and
`DispositionCommandDescriptor`, with no private DTO and no golden byte literal.
Its probe renaming `json:"public_create"` to `json:"pc"` survived the whole
suite. Reproduced first as RED here: on a `cp -R` copy under
`/private/tmp/mut-red`, that rename left `GOWORK=off go test -count=1 ./...`
green on all four packages.

`disposition_inbox.go` now carries `dispositionInboxRecordWire` and
`dispositionDescriptorWire`, with `dispositionInboxToWire` and
`(dispositionInboxRecordWire).record()` written out member by member in both
directions: 4 of 4 record members and 11 of 11 descriptor members each way, no
unmapped member on either side. `dispositionInboxWire` embeds the private record
DTO instead of the exported one; encode and decode route through the
conversions. Stored bytes are unchanged — the golden literals were written
against the pre-DTO encoder and still pass byte-for-byte after it. staticcheck
S1016 wants both literals replaced by a struct conversion; that is refused with a
`//lint:ignore` and a stated reason, because a conversion only compiles while the
DTO's field names match the exported struct's, which reintroduces the coupling
being removed. This is the repository's first lint suppression.

`TestDispositionInboxWireGolden` pins full encoded bytes for three cases:
marker present with an inline payload, marker absent with the same payload
(`public_create` is `omitempty`, so both spellings are pinned), and marker
present with a `payload_object` instead. Opaque IDs carry case, a slash and a
colon (`Tenant/A:B`, `Session/A:B`, `Create/A:B`, `Kind/A:B`). Each case also
decodes back to the canonical record and rejects noncanonical spellings: the
`pc` rename, `descriptor` renamed to `command`, an omitted `payload_size` and an
explicit `"public_create":false`. Dropping the marker outright is deliberately
NOT a codec error — those bytes are the valid unmarked record — so the test says
so and leaves that to the winner comparison in `admitDispositionCommand`.
`desiredWorkloadWire`'s doc comment (advisory A2) now describes both spellings:
absent through the catalog's pointer, explicitly present as
`{"payload_version":"","payload":null}` in the by-value reservation embedding.
`DispositionCommandDescriptor`'s Godoc gained one sentence stating its tags are
not the durable spelling.

Verification, all `GOWORK=off` in `/Users/ipotter/code/looprig/sessionstore`:
`go build ./... && go vet ./...` clean (9.2s). `go test -race -count=1 ./...`
passed all four packages (12.7s package, 30.2s wall). `go test -count=1 -run
'^TestPublicCreate|^TestDisposition' .` passed with 34 top-level tests matched
(0.554s). `GOWORK=off GOMAXPROCS=8 make check` ran to completion, exit 0, in
12:27.5 — vet, staticcheck v0.8.1, gosec v2.28.0, `go mod verify` (all modules
verified), govulncheck v1.6.0 (no vulnerabilities), the `-race` suite, 20 default
30-second fuzz targets with no crashers, and build. `go mod tidy` produced no
`go.mod` or `go.sum` diff; `git diff --check` clean; pins remain `core v0.7.0`
and `storage v0.6.0` with no `replace` and no vendor directory.

Mutation probes, each on a `cp -R` copy under `/private/tmp`, shared sources and
the module cache untouched. The formerly surviving marker rename is now KILLED by
`TestDispositionInboxWireGolden` (`marked_inline`, `marked_object`). Dropping
`Payload` from the encode conversion is KILLED by the disposition suite
(`TestDispositionUnreadablePageContinues` and others); dropping it from the
decode conversion is KILLED likewise. Renaming the DTO's `descriptor` to
`command` is KILLED by the golden and `TestPublicCreateCodecsFailClosed`;
renaming the DTO's `payload_object` to `object` is KILLED by the golden's
`marked_object` case. Renaming the tag on the EXPORTED descriptor alone survives,
which is the intended result and the decoupling proof: stored bytes no longer
follow the exported struct's tags.

Released formats re-verified, not assumed: a throwaway probe encoding a
representative catalog v1 record and a v2 bound record through
`encodeCatalogRecord` was run in a detached worktree at published v0.4.0
`5170b531` and against these sources; the emitted bytes are BYTE-IDENTICAL. `go
doc -all .` shows zero removed lines against `5170b531` (225 added) and, against
`289819c4`, only the one added `DispositionCommandDescriptor` doc sentence. No
settlement, no new mode claim, nothing that would lift the Host hold.

This is one further repository-local commit on local `main`. Nothing is pushed,
tagged, released or accepted; the release decision, version bump and tag remain
root's.

## Conversion-totality guard and the corrected S1016 reason (2026-09-05, over `7aeec70e`)

The independent quality gate of `7aeec70e` passed with no blocking findings and
two advisories, both about the hand-written DTO conversions. This commit closes
both. No production encoding behaviour changes: the whole diff is one comment
rewrite, one comment sentence, and one new test file.

Advisory A1 was that the S1016 suppression's stated reason is factually wrong.
It claimed a struct conversion would reintroduce the coupling being removed —
that the durable spelling would follow the exported struct's tags. It would not:
**Go ignores struct tags in conversions**, and the gate proved it by building the
conversion variant and finding it compiles, passes, emits byte-identical bytes,
and still survives the probe that renames all 15 exported JSON tags at once. What
a conversion couples is Go field names, order and types. The comment above
`dispositionInboxToWire` now says that, and states the justification that is
actually valid and was already the writer's second one: a conversion is a single
statement, so the drop-a-field mutation probes that prove these conversions
exhaustive become inexpressible and their totality would rest on the compiler
alone. The `//lint:ignore` reason on the forward direction now reads "written out
so each member drop stays a killable mutation". The suppression itself is
unchanged and still load-bearing: deleting both directives on a `cp -R` copy
leaves `staticcheck v0.8.1` reporting S1016 at `disposition_inbox.go:333` and
`:345` and exiting 1.

Advisory A2 was the real cost of that choice, and it is now closed rather than
merely documented. RED first: on a `cp -R` copy under `/private/tmp/ss-red`, a
new member added to BOTH `DispositionCommandDescriptor` and
`dispositionDescriptorWire` and mapped in NEITHER conversion built clean and left
`GOWORK=off go test -count=1 ./...` green on all four packages, exit 0, with the
member silently dropped on encode and decode. The hazard is real.

`wire_dto_test.go` adds two guards. `TestWireDTOsMirrorExportedRecords` compares
member names between each exported record and its private DTO, and
`TestWireConversionsCarryEveryMember` fills every member of a record, including
every member of every nested record, with a distinguishable non-zero value and
requires the conversion pair to return it unchanged. Both fail as vacuous rather
than passing on nothing: zero pairs, a memberless struct, or a member the filler
left zero is a `t.Fatal`, and a member of a kind the filler does not handle is a
failure asking for it to be extended, never a silent skip. Against the same
`/private/tmp/ss-red` probe the guard FAILS, naming the dropped member.

The guard covers all six hand-converted pairs, not only the two the inbox
commit added: `DispositionInboxRecord`/`dispositionInboxRecordWire`,
`DispositionCommandDescriptor`/`dispositionDescriptorWire`,
`PublicCreateReservation`/`publicCreateReservationWire`,
`PublicCreateIdentity`/`publicCreateIdentityWire`,
`HostTargetKey`/`publicCreateTargetWire` and
`DesiredWorkload`/`desiredWorkloadWire`. The `289819c4` pairs have the identical
hazard and the same shape of conversion, so excluding them would have left the
gap open where more of it lives; `desiredWorkloadWire` is included because
`publicCreateToWire` maps it by hand too. The four nested pairs are reached
through the two top-level round trips, so both directions of all six conversions
are exercised. Four further probes on `cp -R` copies, each killed by the full
suite: a member added to `PublicCreateIdentity` alone (KILLED by both guards),
dropping `PayloadSize` from `publicCreateToWire`, dropping `Placement` from the
`publicCreateTargetWire` mapping, and dropping `PayloadVersion` from the
`DesiredWorkload` decode (KILLED by the round-trip guard and the public-create
golden). `public_create.go`'s DTO comment gained one sentence pointing a future
author at the two guards.

Verification, all `GOWORK=off` in `/Users/ipotter/code/looprig/sessionstore`:
`go build ./... && go vet ./...` clean, exit 0 (9s). `go test -race -count=1
./...` passed all four packages, exit 0 (30s wall). `go test -count=1 -run
'^TestPublicCreate|^TestDisposition' .` exit 0 (2s). `make staticcheck` clean,
exit 0, with no unused-directive complaint. `gofmt -l .` empty; `go mod verify`
all modules verified; `go mod tidy` produced no `go.mod` or `go.sum` diff; `git
diff --check` clean. Pins remain `core v0.7.0` and `storage v0.6.0`, no
`replace`, no vendor. `GOWORK=off GOMAXPROCS=8 make check` ran to completion, exit 0, in 754s (12:34) —
vet, staticcheck v0.8.1, gosec v2.28.0, `go mod verify`, govulncheck v1.6.0 (no
vulnerabilities), the `-race` suite, 20 default 30-second fuzz targets with no
crashers, and build.

Byte identity re-proved rather than assumed: a throwaway probe encoding the three
golden inbox shapes through `encodeDispositionInboxRecord` was run in a detached
worktree at `7aeec70e` and against these sources, and the bytes are
BYTE-IDENTICAL. `go doc -all .` against `7aeec70e` is identical — zero removed
and zero added lines — so the public Go API is untouched and remains purely
additive against published v0.4.0. `testdata` is unchanged: one tracked fuzz
corpus seed, no untracked files.

This is one further repository-local commit on local `main`. Nothing is pushed,
tagged, released or accepted; the release decision, version bump and tag remain
root's.
