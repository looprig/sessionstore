# Disposition settlement (step 2) implementation checkpoint

**Goal:** Make one dispatch of a disposition command durably authorized before
it happens, and settle it terminally only from evidence the store obtained
itself, without SessionStore learning what a runtime is.

**Architecture:** Two revision-checked edges over the record
`disposition_inbox.go` already admits — `claimed -> applying` and
`applying -> applied | rejected` — through the same ordered-index operations
every other transition uses, plus a narrow injected evidence reader over
committed journal records. No second state machine, no journal I/O, no agent
execution.

**Tech stack:** Go, Storage OrderedIndex/KV, real memstore fault fixtures.

## What this provides

`BeginDispositionAttempt` moves a claimed command to `applying` and records the
complete attempt: the caller-chosen attempt identity, the journal grant the
runtime returned, the Host residency grant the dispatch is authorized under and
the caller's own start instant. Runtime dispatch is forbidden without that
successful compare-and-swap, so a losing writer cannot produce a runtime call
and a concurrent rejection and dispatch have one winner. Once `applying`
commits, the attempt identity and both grant identities are immutable.

`SettleDispositionCommand` settles one applying command. It reads the store's
own catalog authority and the current inbox record, holds the caller to the
revision it decided on, derives the evidence question entirely from that record,
obtains evidence through the configured `DispositionEvidenceReader`, verifies it
and only then compare-and-swaps that exact revision to terminal. A settlement
request names a command, a revision and the claimant's residency and nothing
else: it cannot supply an outcome, an absence or any caller-authored proof
struct. The original holder and a successor settle by the same route; the
difference between them is in the evidence, never in the caller's assertion.

Two ownership domains are carried side by side and never meet. `JournalEpoch`
and `ResidencyEpoch` are distinct named types, held as separate members of
`DispositionAttempt`, and no expression in the settlement path mixes them.

Evidence rules, all verified before the terminal write: the evidence must name
this attempt and the grant this attempt selected; an application (`applied` or a
successful no-op) is authored by that same grant; an `applied` disposition names
the public event committed in its own envelope, so the event's sequence is the
disposition's; a no-op names no event, because a private disposition envelope
fabricates none; a recovery closure is authored by a strictly later grant and
names that author's verified opening fence, which necessarily precedes the
closure. A no-op settles as `applied` because a no-op is an application;
`not_applied` is the tombstone and settles as `rejected`. Corrupt, incomplete,
mismatched or unavailable evidence fails closed and leaves the record unsettled.
A reader error or cancellation propagates as itself and is never a skip, an
absence or permission to settle.

The settling residency is recorded in the outcome as settlement context, beside
the attempt and never over it, so a successor's epochs never overwrite the
grants the dispatch was actually authorized under.

Meeting a terminal record at its own revision is an idempotent result: it is
returned as it stands with `settled=false`, reads no evidence and writes
nothing. Losing the compare-and-swap returns `InboxErrorConflict` carrying the
current revision, which is a reread instruction and never permission to repeat
execution.

## What it does not provide

- **No claim edge.** `pending -> claimed` has no entry point. No exported API
  produces a claimed record; the only writer is a test helper inside the
  package, so no claimed record can exist in any store. `BeginDispositionAttempt`
  and `SettleDispositionCommand` are therefore unreachable in production, and
  `DispositionClaim`'s durable shape is still provisional. A release note for
  whatever version carries this must say so; consumers must not pin behaviour
  on it.
- **No production `DispositionEvidenceReader`.** Only a test fake exists. A
  store opened without `WithDispositionEvidence` refuses to settle rather than
  settling on record state.
- **No reject-before-dispatch entry point.** Its record SHAPE is storable as of
  this round (below); nothing writes one.
- **No runtime application, no journal read or write, no new-mode object
  writes.** The journal is reached only through the injected reader interface.
- **No exactly-once claim** about any external side effect, no undo of one, no
  termination of a remote tool.
- **No reaping, retention or gate continuation**, and no new due horizon beyond
  a settled record leaving the due view. There is deliberately no apply-deadline
  check and no new expiry on either edge: an applying record is closed by
  evidence, not by a timer, so a command whose evidence is unavailable stays
  observably unresolved instead of being given a clock-shaped conclusion.
- **No Harness or Host change**, and cross-store command settlement — the
  protocol Host's independent agent-store adoption is sequenced behind — is not
  implemented.

## What steps 3 and 4 still owe

- **The claim edge.** Step 3 must implement `pending -> claimed` and a
  production `DispositionEvidenceReader` over the real journal. Until both
  exist, `BeginDispositionAttempt` and `SettleDispositionCommand` remain
  unreachable in production regardless of what version they ship in.
- **The evidence-reader contract from ambiguity 2 below.** SessionStore cannot
  verify the mapping itself, so step 3's reader implementation must (a) report
  the public event's identity, not the envelope's, so a runtime-control
  envelope yields an empty `EventID`; (b) report `DispositionSeq` and
  `EventSeq` as the one journal sequence of the shared envelope; and (c) report
  `AuthorJournalEpoch` as the `LeaseEpoch` of the opening fence at
  `AuthorFenceSeq`. It fails closed if violated, so it is not a hazard today,
  but it is an unratified contract until a reader exists to honour it.
- **The reject-before-dispatch entry point.** The record shape is storable as
  of this fix round; nothing writes one. A later step must add the transition
  that produces it, or explicitly decide not to and remove the shape.
- **The residency-fencing question (A8) — CLOSED, and the cost stated here was
  wrong.** This item recorded settlement as unfenced and claimed that fencing it
  "would need a lease read this path does not do". Root ruled on it and the fence
  is implemented. No lease read was needed: "residency high-water" for a command
  means the record's own committed mark, which is its CLAIM's residency, and that
  value is already in hand at the compare-and-swap — one comparison, no `Leaser`.
  `SettleDispositionCommand` now refuses a settling residency strictly BELOW the
  claim's with `InboxErrorEpoch` on `residency_epoch`, before the evidence read,
  matching the released legacy `CompleteCommand`. Only the superseded arm applies:
  a successor holds a HIGHER residency by lease monotonicity and settles
  unobstructed, which is what keeps "evidence is the authority" true.
  `Outcome.SettlingResidencyEpoch` is documented as "who asked, and that they were
  not already superseded" — still NOT "who validly settled this", because no lease
  is read and a residency equal to the claim's may since have lapsed.
- **Cross-store command settlement.** Host's independent agent-store adoption
  is sequenced behind this protocol and is not implemented by step 2, 3 or this
  checkpoint.
- **Gate continuation, object reclamation, retention/reaping.** Deliberately
  out of scope for every step so far; still owed by whichever step AGENTS.md
  ultimately assigns them to.

## Release conditions

- **Purely additive — a minor bump.** `go doc -all` shows zero removed lines
  against published v0.4.0 across the whole branch, this fix round included.
  Whatever version carries this work takes the next minor version, never a
  patch and never a major.
- **Do not release ahead of `v0.5.0`.** SessionStore `main` is held at
  `f2b5f42e` for a pending, gated, CI-green `v0.5.0` tag. This branch must not
  be released, tagged or pushed until `v0.5.0` is tagged at that tip and this
  branch is rebased onto it.
- **The release note must state both new entry points are unreachable in
  production.** There is no `pending -> claimed` producer anywhere in the
  repository and no production `DispositionEvidenceReader` implementation —
  only a test fake (`fakeEvidence` in `disposition_settlement_test.go`).
  `BeginDispositionAttempt` and `SettleDispositionCommand` therefore cannot be
  driven by any real caller yet, and `DispositionClaim`'s durable shape is
  correspondingly still provisional. Consumers must not pin behaviour on it
  until step 3 lands a real claim producer and a real evidence reader.

## The six resolved ambiguities

1. **`claimed` has no producer.** Accepted rather than inventing the claim edge,
   which would have front-run this step's scope and shipped a residency-guarded
   producer with no design review. The shape is not left loose:
   `validateDispositionState` makes the claim mandatory for `claimed`,
   `applying` and a settled terminal record, and golden literals pin its bytes.
   Because no claimed record can exist in any store, a later producer that needs
   another member breaks nothing — the condition is documentary, not technical.
2. **A selected journal grant's identity is its epoch alone.** The lease
   namespace and session binding that make an epoch meaningful are already in
   the immutable descriptor, and the evidence request carries them to the
   reader. A second namespace member would have duplicated an immutable field
   and added a second thing that could disagree with it. Note for later steps:
   adding a member to `DispositionAttempt` is NOT byte-compatible, because
   decoding re-encodes and requires `bytes.Equal`; once records exist in the
   field, extension needs a `record_version` bump and offline conversion.
3. **`EventSeq == DispositionSeq` for `applied`.** One envelope has one
   `EventID` and two body slots, so one envelope is one journal record and
   therefore one sequence: requiring equality IS the "same envelope" predicate,
   not an approximation. The same schema ratifies the no-op rule, since a
   runtime-control envelope has no `EventID` field at all. **This is where the
   cross-repo obligation lives.** SessionStore cannot verify the mapping itself,
   so step 3's reader must (a) report the public event's identity, not the
   envelope's, so a runtime-control envelope yields an empty `EventID`; (b)
   report `DispositionSeq` and `EventSeq` as the one journal sequence of the
   shared envelope; and (c) report `AuthorJournalEpoch` as the `LeaseEpoch` of
   the opening fence at `AuthorFenceSeq`. It fails closed, so it is not a
   hazard, but it is an unratified contract today.
4. **A no-op settles as `applied`; `not_applied` is the tombstone.** A no-op is
   an application, and `Outcome.Kind` keeps the distinction the design demands
   between reject-before-dispatch and an applied no-op.
5. **`SettledAt` comes from the store clock.** The whole point of
   `SettleDispositionCommandRequest` is that the caller supplies nothing an
   outcome is built from; accepting a caller timestamp would have been the one
   caller-authored value in a record whose entire purpose is that it has none.
   `StartedAt` stays caller-supplied because the attempt request is the caller's
   own statement.
6. **Idempotency versus conflict.** A stale revision is a reread instruction
   carrying the current revision. Meeting the terminal record at its own
   revision returns it with `settled=false`, which is what distinguishes "I
   settled it" from "it was already settled".

## Fix round over `6918f505` (2026-09-06)

The independent gate of `6918f505` returned PASS with no blocking defect, four
advisories and three findings the original writer had not flagged. This round
closes them on the branch. No encoder changed.

**The untested terminal refusal.** Deleting `BeginDispositionAttempt`'s terminal
refusal passed the entire suite: the record fell through to the not-claimed
check and answered `InboxErrorState`. Both are refusals, so there was no
correctness hole, but the codes mean different things to a Host — terminal means
stop, someone settled this; state means reread and reconsider — and the doc
comment promised the distinction. The refusal ORDER is now pinned step by step
by `TestBeginDispositionAttemptRefusalOrder`, with each case constructed so it
also satisfies a LATER refusal: a settled record is also not claimed; an
applying record carries a claim the residency fence could have answered on; a
lapsed claim is met by both a superseded residency and one that never claimed
the command. Each case asserts it is not vacuous. The order is terminal ->
not-claimed -> superseded residency (`InboxErrorEpoch`, carrying the committed
epoch) -> a residency that never claimed it (`InboxErrorClaimLost`) -> a lapsed
claim on the store clock, mirroring `commandClaimFence`.

**The protocol-mode fence is a write, and the write claim needed narrowing.**
`bindProtocolMode` on the transition read path was untested — the catalog's own
binding check carried the only test that looked — and it is not a read: it PUTs
the create-only witness when it finds none. So `6918f505`'s "one read and one
revision CAS" is inaccurate; the real sequence is a catalog read, a witness
read, a possible witness PUT, an inbox read and the inbox compare-and-swap.
Reachability turned out narrower than the gate stated. The fence has three
branches, and two of them are easy to conflate: a witness pinning the other
mode is refused, a witness MISSING beside a live collision witness is ALSO
refused (a bound scope with no mode pin is conservatively legacy and is never
silently repinned), and only a session with no witness at all gets one
installed. Reaching that last branch from a disposition transition additionally
requires the collision witness to disappear between the catalog read and the
fence, because the catalog read requires it. `TestDispositionTransitionsFence-
TheProtocolMode` asserts all three branches for both entry points and opens
exactly that window with a KV fixture, rather than claiming the write is
ordinarily reachable. The documentation now says all of this.

**Reject-before-dispatch is now representable.** The design allows a `pending`
or `claimed` command to be rejected only while no dispatch attempt was durably
authorized, but the terminal validator demanded a claim AND an attempt, so such
a record could not be stored at all. `validateDispositionState` is relaxed:
only a rejection may lack an attempt, it then carries no outcome because every
outcome here is keyed by the attempt it settles, and `applied` still always
carries the attempt that applied it. Both new shapes — rejected before any
claim, and rejected after a claim before dispatch — are pinned by full golden
byte literals and by the state/member table. This is the record shape and NOT
the entry point: nothing writes one, and admission still hardcodes pending. A
validator that accepts more never invalidates a stored record, which is why
this was free before the shape has a producer and would have been noisy after a
tag. The cost is stated in source: bytes whose state member alone reads
`"rejected"` are now a valid tombstone rather than a decode failure, which is
one fewer accidental-corruption tripwire on a state no producer writes yet. A
discriminating member would have front-run the rejection authority that belongs
to a later step.

**Two documentation claims were false of the code beside them.**
`DispositionOutcome.SettledAt`'s store-clock origin is a deliberate deviation
and was undocumented, while the same file asserted one screen away that a
stored instant is "the caller's own clock reading, as every stored instant in
this package is". Both are fixed: the deviation and its reason are documented
where the field is defined, and the contradicting sentence is qualified to
caller-supplied instants. `DispositionInboxRecordVersion`'s doc still described
a pending-only codec that "refuses" terminal states, which step 2 had already
made false; it now describes what v2 actually carries and restates that a
further durable member still needs a version bump.

**The fuzz target got the new states.** `FuzzDispositionInboxCodec` seeded only
pending records, so the three new durable members were never fuzzed. Every
decodable state is now seeded, including the rejection that carries none of
them. The "codec grants progress" assertion is generalised rather than dropped:
the decoded state must be one the INPUT BYTES spell, which is exactly what
"pending only" meant while pending was the only decodable state, and a decoder
that invents any state still fails.

**Two smaller advisories.** The zero settling-residency request check was
unpinned because a zero value is caught a layer down by the stored outcome's own
rules — different code, same refusal; the new
`TestSettleDispositionValidatesItsRequestBeforeAnyRead` asserts the FIELD as
well as the code, so the request-level check is what answers. And
`validateDispositionState`'s defensive copy is unreachable from outside the
package, since no exported API accepts a `DispositionInboxRecord`; the comment
claimed a guarantee to a caller who cannot exist and now says what the copy
actually buys.

**One advisory was left open for root, and root has since closed it.** As
written, this section said settlement does not fence the settling residency and
that fencing it "would need a lease read this path does not do". The second
clause was false and is corrected here rather than left for a later step to
rediscover as a contradiction: the mark is the record's own claim residency,
already read at the compare-and-swap, so the fence is one comparison and no
`Leaser` is involved. A superseded settling residency is now refused with
`InboxErrorEpoch` on `residency_epoch` before the evidence read; equal and higher
residencies proceed exactly as before, so a successor still settles and evidence
remains the authority. What survives unchanged from the original advisory is the
limit: `Outcome.SettlingResidencyEpoch` is still only "who asked, and that they
were not already superseded", never "who validly held the session when this
settled", because the fence is a high-water check and not a liveness check.

### Verification evidence (2026-09-06)

Branch `feat/settlement-step2`, over `6918f505b0b09065dc53b7a1a22903ae6dcf3d6c`.
SessionStore `main` was not touched and remains held at `f2b5f42e` for a
pending `v0.5.0` tag. Nothing was pushed, tagged, rebased or accepted.

RED first for the representability change: the two new golden cases and the two
new state/member rows failed on the real validator with
`sessionstore: inbox invalid (state)` before `validateDispositionState` was
relaxed. The pinning tests for existing behaviour cannot have a RED phase
against correct code, so each is held to the gate's standard instead and names
the mutation that kills it.

Nine mutation probes, each on a `cp -R` copy under `/private/tmp/ss-mut`,
whole-package suite each, shared sources and the module cache untouched.

| Probe | Mutation | Result | Killed by |
|---|---|---|---|
| G3 | `currentDispositionEntry`'s terminal refusal deleted | **KILLED** | `...RefusalOrder/terminal_before_not_claimed` |
| G10 | `bindProtocolMode` removed from the transition read path | **KILLED** | `...FenceTheProtocolMode`, all three branches, both entry points |
| G4 | `SettleDispositionCommandRequest.ResidencyEpoch == 0` check removed | **KILLED** | `...ValidatesItsRequestBeforeAnyRead/zero_residency` |
| M11 | residency fence moved before the not-claimed check | **KILLED** | `...RefusalOrder/not_claimed_before_the_residency_fence` |
| M12 | lapsed-claim check moved before the residency fence | **KILLED** | `...RefusalOrder/superseded_residency...`, `/claim_ownership...` |
| M13 | terminal validator tightened back to requiring an attempt | **KILLED** | both new golden cases, both new member rows, and the fuzz seeds |
| M14 | relaxation widened to admit `applied` with no attempt | **KILLED** | `...RequireTheirMembers/applied_before_any_claim`, `/applied_with_a_claim_but_no_attempt` |
| M16 | fuzz assertion restored to its pending-only spelling | **KILLED** | `FuzzDispositionInboxCodec` seeds 4-10 — the proof the new seeds reach the new states |
| G2 | `validateDispositionState`'s defensive copy dropped | **SURVIVED** | equivalent at the public API; answered with a truer comment, not a test |

Format safety re-proved rather than assumed. Four trees were extracted and the
SAME probe file compiled into each, so the encoders themselves produced the
bytes: published v0.4.0 `5170b531`, the pending-tag tip `f2b5f42e`, step 2
`6918f50` and these sources. The catalog v1 record and the catalog v2 bound
record are **one unique byte string across all four**; the disposition pending
record and `dispositionInboxDue(pending)` are **one unique byte string across
the three trees where the type exists**. `go doc -all .` against published
v0.4.0: **0 removed lines, 502 added**. Against `6918f50`: 14 removed and 36
added, every removed line a prose line deliberately rewritten here — no
declaration was removed or changed.

### Independent re-verification at `1d4ada02` (2026-09-06)

Reproduced from a fresh worktree rather than taken on trust, over the sources
above unchanged (no code edits made in this pass; only this checkpoint doc's
"what steps 3 and 4 still owe" and "release conditions" sections were added).

Byte-identity was re-derived independently, not copied from the table above:
three detached worktrees at `5170b531` (v0.4.0), `f2b5f42e` (base) and
`1d4ada02` (head), with the same probe compiled into each. The catalog v1 and
v2 records are one identical hex string across all three. The disposition
pending record and `dispositionInboxDue(pending)` are one identical hex
string / `{State:1 UnixMillis:1788093000000}` across the two trees where the
type exists (base and head). `go doc -all .` at v0.4.0 vs. head:
**0 removed, 502 added** — reproduces the figure above exactly. This is more
added than the independent gate's **480** at `6918f50`: expected, not a
discrepancy, because `1d4ada02` is `6918f50` plus this fix round's own
exported documentation (the corrected transition-sequence comment, the
`SettledAt`/`StartedAt` doc, the widened `DispositionInboxRecordVersion` doc,
and so on) — 22 more lines of doc prose, zero more removed. What the gate's
number and this number agree on, and what actually matters for release safety,
is that **removed lines are zero in both measurements**. Standalone
`GOWORK=off`: `go build ./...` and `go vet ./...` both exit 0; `go test -race
-count=1 ./...` passes all four packages; `gofmt -l .` empty; `go mod verify`
all modules verified; `go mod tidy` produces no `go.mod`/`go.sum` diff;
`git diff --check` clean. `GOWORK=off GOMAXPROCS=8 make check` was run to
completion under heavy concurrent host load; see the task's own report for its
exit code, since this doc is not the record of that run.


## Root verification of the fix round (2026-09-06)

Root waited on the full native check directly after the assigned agent stalled
three times on background-watcher notifications.

`GOWORK=off GOMAXPROCS=8 make check` at `1d4ada0` completed every stage: vet,
staticcheck v0.8.1, gosec v2.28.0, `go mod verify`, govulncheck, the `-race`
suite, **24 fuzz targets** and the final build. **Zero FAIL, zero crashers, zero
panics** across the whole log, and root independently re-ran the terminating
build stage to `build_exit=0`.

Note the fuzz count: 24 targets, not the 20 seen on earlier runs. The fix round's
seeding of the `Claimed`, `Applying`, `Applied` and `Rejected` states is what
added them, which is the intended effect of closing that gate finding — the three
new durable members previously had zero fuzz coverage.

Root did not capture `make`'s own exit status, because the process was launched by
the assigned agent rather than by root; the evidence above is the stage-by-stage
log plus the independently re-run terminating stage. Stated precisely rather than
reported as a bare exit code root did not observe.
