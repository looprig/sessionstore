# sessionstore

`sessionstore` is Looprig's transport-neutral durable session aggregate. It composes
the primitives from [`github.com/looprig/storage`](https://github.com/looprig/storage)
with canonical records from
[`github.com/looprig/core/sessionwire/v1`](https://github.com/looprig/core) so durable
session state can be shared without coupling Factory, Host, or Harness to one another.

The module will own journal fencing and replay, catalog and gate projections, durable
command admission, host and placement records, fenced pointers, reconciliation
claims, and session-scoped object references. Concrete storage providers are selected
by the product composition root.

Production imports are intentionally limited to the Go standard library, Core, and
Storage. Published module files use exact released versions and contain no local
`replace` directives or vendor tree.

## Provider compatibility

`Open` requires the backend's `Blobs` primitive to implement Storage's optional
`BlobReaderLifecycle` capability, be a concrete non-nil implementation, and
advertise a positive close bound. This lets Store shutdown stop an outstanding
object read before an explicitly owned provider is closed. The reader close bound
and `WithShutdownTimeout` cover separate shutdown phases and are not compared.

Storage v0.6.0's memory backend and natsstore v0.5.1 satisfy the requirement.
fsstore v0.5.1 intentionally does not claim bounded reader shutdown and is
rejected with `*InvalidBackendError` naming `BlobReaderLifecycle`. Any other
provider is compatible only after it implements the capability and its
provider-specific blocked-I/O proof. Capability rejection happens before layout
marker or other provider I/O; a provider passed to a failed `Open` remains
caller-owned.

## Layout compatibility

An unmarked backend is atomically initialized as the tenant-scoped `tenant-v1`
layout. The historical `sessions/<uuid>` layout is available only through
`WithLegacySingleTenant`, which persists and enforces the exact configured tenant.
SessionStore never probes for old data, auto-migrates, or dual-writes layouts.

Migration must be performed offline with SessionStore stopped, into a new backend
already initialized for `tenant-v1`. Validate the migrated data before switching
the composition root; do not rewrite a live backend's immutable layout marker.

Provider ownership options take effect only after `Open` has successfully validated
and bound the backend layout. If `Open` fails, the provider remains caller-owned and
SessionStore does not close it.

## Object orphans

`PutObject` mints an object's identity before writing it and returns a reference
only after re-reading the persisted bytes and verifying them against the declared
length and digest. A provider failure after the blob has committed therefore
returns an error and no reference while leaving a verified blob behind — an
orphan.

That is deliberate. Deleting on a post-commit failure would issue a delete
against a provider that has just proved unreliable, and every object key is
content- and generation-addressed, so an orphan can never be confused with, or
served as, another object. Reclaiming orphans is the store operator's
responsibility, over the tenant- and session-scoped blob prefix; SessionStore's
only enumeration path is internal and unexported, so no caller-facing garbage
collector exists yet.

## Journal ownership: no rebasing after a fence conflict

`OpenJournal` acquires the session lease, reads the ledger tip exactly once, and
appends an opening fence at precisely that tip stamped with the grant's epoch. If
that CAS conflicts, the grant is spent: the lease is released and a typed
`*JournalError` with code `fenced` is returned. The caller may acquire a fresh,
strictly higher epoch and reopen.

It deliberately does not refresh the tip and retry. A retry loop lets a writer
reorder itself behind records it never observed, under an epoch that a
predecessor may still believe it holds; failing the grant instead makes epoch
order and ledger order agree.

After a successful open the writer tracks only its own committed sequence and
CASes every later append on it. It never re-reads the tip, so a successor's
opening fence permanently fails it. An append whose outcome could not be
resolved — an unresolved ambiguous ack, or a contested record that could not be
read back — is equally terminal (code `unknown`): the writer latches the failure
rather than rebasing onto whatever is now durable. A *definite* backend failure
is not terminal, because it left the tracked tip untouched and the same record
can simply be offered again.

An over-threshold body is uploaded and verified as an immutable object before its
reference is appended. If the append then fails, the verified object is left
behind as an orphan for the same reason `PutObject` leaves one.

## Public journal reads

`ReadPublicJournal` returns only a public event's stored canonical public body
and its Core metadata. Runtime control records, ownership fences, and application
prefixes are withheld entirely: they contribute nothing to a page except an
advance of `covered_through`, the authenticated watermark that lets a client
close a sequence gap without learning the kind or bytes of what filled it. A
public body held in an object is resolved through the ordinary verified object
path; a private runtime object is never fetched by a public read.

Page cursors are opaque and bound to the projection they were issued for, to the
exact tenant and session, and to the tip captured by the first page — a runtime
cursor cannot be replayed into a public read, a cursor cannot be moved between
sessions, and an inflated captured tip cannot widen the snapshot a walk covers.
`ReadRuntimeJournal` is the privileged counterpart and returns every record as
stored, leaving object-backed bodies unresolved for the caller to fetch.

## Catalog ownership: a Host epoch and a Factory revision

The session catalog is one authoritative `OrderedIndex` record per session,
ordered and ranked by the tenant's namespace and ranked by
`LastActiveAt.UnixNano()`, so a recent-first tenant page stays a bounded
provider query and a status read stays a direct get.

Its fields have two owners and two different guards, and the difference is
structural rather than advisory. `UpdateCatalogHostState` carries the writing
lease epoch and is refused if that epoch is below the record's committed
high-water mark; an equal epoch is admitted, because one grant legitimately
writes many times. `UpdateCatalogDesiredState` carries an expected revision and
a retry-stable idempotency key and has no lease-epoch member at all, so a
Factory cannot spell a claim on a lease it does not hold. The idempotency key is
compared before the revision: a retry of an already-applied desired-state write
carries an expected revision that its own success invalidated, so comparing the
revision first would reject exactly the requests idempotency exists to absorb.

Both paths close their read-compare-write with the same revision
compare-and-swap, which is what makes the epoch a fence rather than advice: a
writer that observed a stale high-water mark loses the swap and, on re-reading,
meets the successor's epoch.

## Recent-first pages are one ranked query

`ListSessions` returns a Core `SessionPage` from a single `ListRanked` call. The
tenant is the ranking scope, so the tenant restriction and the recency order are
both inside the provider query and the limit applies to a result that is already
restricted and already ordered. Nothing enumerates a key prefix, sorts a
catalog, or narrows a wider page afterwards; those cost work proportional to a
tenant's history rather than to the page, and a picker renders on every visit.

Unlike a direct get, a listing does not verify the tenant's collision witness:
it names no session, and a tenant that has never created one has no binding to
prove, so requiring one would answer "this tenant is empty" with a failure.
Separation instead comes from holding every returned record to the tenant it
itself claims, so a scope two tenants somehow shared fails the page closed
rather than disclosing a row.

A page cursor is a versioned SessionStore envelope wrapping the provider's own
opaque token. The envelope binds the token to the tenant it was issued for and
tags it as a catalog page, so it can be moved neither between tenants nor into a
journal read even when the provider underneath does not bind its own cursors,
and a caller retains a SessionStore token rather than a provider one. Pagination
resumes from the provider's frozen `(rank, stable_key, ordering_scope)` position
rather than from a snapshot, so a session whose recency changes mid-walk may
repeat or be skipped; a sweep that must see every session once reconciles by
identity, not by page.

## Open gates: one projection, one deadline index

The catalog record is the only store of open gates. `ReadGates` returns Core's
`GatePage` from that one record, in the record's canonical
`(opened_seq, gate_id)` order, with no cursor and no limit: a session's open
gates are bounded by `MaxCatalogOpenGates`, so the whole answer is one bounded
read rather than a walk.

`OpenGate` and `ResolveGate` add and remove one gate at a time and also maintain
a deadline INTENT: one ordered record per open gate whose due state is the
gate's absolute deadline, carrying its identity and opening event and nothing
else. It is an index, not a second copy of the projection — the catalog cannot
answer "what is due" without reading every session, and the ordered index's due
view can, deployment-wide, in pages proportional to what is due.

The two writes are ordered, and the order is the durability argument. An open
makes the intent durable before it commits the projection; a resolve clears the
projection before it retires the intent. So the only state an interrupted
operation can leave is an intent with no matching open gate, never a gate open
with no durable deadline. `ListDueGates` is the reader that closes that: it
validates every due intent against the session's durable open projection and
drops the ones that match nothing. It is a bounded read that takes no action —
what a Host does about an expired gate is gate continuation, which this package
does not yet implement.

Its missing continuation is a liveness hazard rather than an ergonomic gap, and
a caller has to know it. A remnant intent is dropped from the page but never
retired: `OpenGate` writes the intent before the projection, so an intent with
no matching open gate cannot be told apart from a gate being opened right now,
and a reader that retired what it drops would race a live open. Because the due
view is deadline-ordered and this call has no resume position, `Limit` remnants
at the head of the order mask every live gate behind them indefinitely — and a
Host that re-projects wholesale produces one remnant per gate it drops, so a few
hundred ordinary re-projections can silently switch expiry off deployment-wide.

`DueGatePage` therefore reports `Examined` and the effective `Limit` beside its
gates: `Examined == Limit` with no gates is head-of-line blocking rather than an
empty answer, and it is the only signal available until a continuation exists. A
later task adds that continuation and, with it, whatever retires remnants
safely; until then no caller may treat this as a sweep.

Retiring an intent is a tombstone rather than an erasure: the record stays
readable for audit, its identity can never be reused to reopen the same gate,
and a tombstone is not due by the provider's own contract, so it leaves the due
pages without this package maintaining a flag.

`UpdateCatalogHostState` still replaces the whole open-gate projection and
deliberately leaves intents alone: it is the Host's re-projection path, not an
incremental gate edit. A gate projected only that way is readable but has no
deadline index, and a gate dropped that way leaves a remnant intent the due
reader discards.

## Command admission: one create, one immutable acceptance order

`AdmitCommand` makes one client command durable and reports whether this call is
the one that accepted it. It is exactly one `OrderedIndex.Create`, filed in the
inbox namespace under `(session ordering scope, raw CommandID)`, with the apply
deadline as the record's due state and no rank.

Identity is `(TenantID, SessionID, CommandID)`. The session is part of it, so
the same client command id in another session — or another tenant — is a
different command, and a duplicate within one session is a retry rather than a
new acceptance. Because `Create` is atomically idempotent by identity, the
duplicate case needs no read of its own: a loser receives the winner's canonical
stored record.

The runtime mapping is allocated once. A caller PROPOSES a `RuntimeCommandID`
and must then use the one the returned record carries: racing replicas
legitimately propose different values, and only the winner's is stored, returned
and used.

A duplicate whose command CONTENT differs — kind, inline payload, or referenced
payload object — fails closed with `InboxErrorCommandMismatch`, because silently
returning the first command's record would tell a caller its command was
accepted when nothing of the kind happened. Everything else is deliberately
excluded from that comparison. The proposed runtime id is excluded because
disagreeing about it is the expected outcome of a race. The accepted instant and
apply deadline are excluded because a retry carries a fresh clock reading, so
comparing them would turn every real retry into a mismatch. The state, claim,
result and rejection are excluded because by the time a retry arrives the
command may already be applied or rejected, and that progress is not evidence
that this retry differs.

For the same reason there is no "the deadline must be in the future" check: a
retry of an unknown outcome may arrive after the original deadline has passed
and must still be able to learn the mapping that was durably accepted. The
deadline is validated as an instant and nothing more.

`InboxErrorCommandMismatch` is deliberately not called `InboxErrorConflict`. This package spells
"conflict" two ways already — `CatalogErrorConflict` is a lost revision CAS
(recoverable, retry after a re-read) and `ObjectErrorConflict` is a key holding
different content — so the spelling is kept for the revision-CAS meaning the
command transition machine uses when it compare-and-swaps this record, and the
caller-caused case takes a name that cannot be mistaken for either.

`InboxEntry.AcceptedOrder` is the provider's immutable acceptance order, and it
is exposed here where `CatalogEntry`'s deliberately is not: consumers sort a
session's bounded ordered page by it, and a retry must receive it unchanged as
evidence that it is the same acceptance. It is an OPAQUE COMPARISON KEY.
It is strictly increasing within one session's order scope, but it is not
contiguous, not one-based, and not comparable across sessions: a provider may
allocate it from a JetStream stream sequence or a shared SQL sequence, so a
session's first command can be order 5000 and its second 9000. Nothing may
derive a count, a position, or "the next" order from it.

`created == true` additionally holds the provider's reply to the bytes THIS
call sent, which is the one claim the identity, scope, due and order checks
cannot make: they hold a reply to the record's own bytes, and a substituted
record satisfies them exactly as well as the real one. On a duplicate the
stored bytes are the winner's and only the content is comparable, so the exact
comparison is deliberately made on the created path alone.

Deleting a command is not reclamation: its identity and acceptance order can
never be reused, so a tombstone is a permanent answer to any caller still
retrying it. Whoever adds retention or compaction must bound terminal-command
retention below by the client retry window.

## Command transitions: one read, one revision CAS

`ClaimCommand`, `BeginApplyingCommand`, `CompleteCommand` and `RejectCommand`
drive `pending -> claimed(epoch, expiry) -> applying(epoch, expiry) -> applied |
rejected`, and `GetCommand` reads the record a caller re-decides against. Each
transition is one `Get` and one `Update` of the same authoritative record at the
revision the caller named, and it touches no other aggregate.

The epoch on a claim is the SESSION LEASE epoch the claimer acts under, not a
number this aggregate allocates, which is what makes it meaningful to a Host and
to a Factory replica reconciling the same command. An epoch BELOW the record's
claim epoch is refused as `InboxErrorEpoch` carrying the high-water mark, which
is `hostEpochFence`'s rule deliberately. Where the two fences differ is the
equal case: the catalog fences a record one lease owns, so one grant writing
many times is normal, while a claim fences a work item two writers under one
epoch may both reach for and the record carries no claimant identity to tell
them apart. So an equal epoch may not take a LIVE claim, only a lapsed one, and
a strictly greater epoch may take either — its predecessor is provably fenced
out of the journal, and stalling every claimed command for a claim TTL on every
failover buys nothing.

A claim cannot be RENEWED. A claimer that needs more time must enter `applying`
before its claim lapses; re-claiming under the same epoch is refused while the
claim is live and, once it lapses, is open to every writer at that epoch or
above. `MaxCommandClaimTTL` bounds how far ahead of the store's clock a claim may
expire — a ceiling on caller error and clock skew, not a policy TTL. It exists
because a claim may legitimately outlive the apply deadline while `inboxDue` caps
the due horizon AT the deadline, so an over-long claim leaves the row due and
settleable by nobody for the claim's whole life; unbounded, that is centuries.

`applying` may be entered only by the holder of a live claim, and it has no
deadline check: an unexpired claim wins the deadline race, which is what stops a
reconciler's clock from cancelling work about to commit. `CompleteCommand`
requires the claim's epoch but NOT a live claim, because it records an effect
that is already in the journal and refusing it would strand a committed
application behind a lapsed TTL. `RejectCommand` keeps the live-claim
requirement, because it decides something that has not happened; its lease epoch
is optional, and a caller that names none is the deadline reconciler, which may
settle a `pending` or lapsed-`claimed` command and nothing else.

## Recovering an application from the journal

`FindCommandApplication` correlates one command's record with its session's
journal and reports what the journal PROVES about its application, which is what
lets an `applying` record whose claim has lapsed be settled at all. Correlation
is against the identities in the durable record, never against identities a
caller supplies, and all three of them must agree: the public `CommandID`, the
`RuntimeCommandID` the inbox mapped it to (compared as a decoded UUID value, not
as text), and the command kind. A prefix that names the command under a
different runtime identity or kind is `conflicted`, not absent — treating a
broken mapping as absence is what would let a command whose effect committed be
rejected.

Two things make a negative answer safe, and both are durable state rather than
an assertion by the caller asking for the settlement. ADJACENCY: a prefix
belongs immediately before its effect, so the record at `prefix+1` is the whole
question — a public event means `committed`, an opening fence above the prefix's
epoch means `abandoned`, and anything else, including nothing yet, means
`unresolved` and refuses both settlements. FENCING: journal grants are strictly
increasing and an opening fence is committed by CAS at the tip, so a fence above
an epoch proves that lease can never append again.

`CompleteCommand` therefore admits a strictly greater epoch on an `applying`
record when the correlation is `committed` and the result it records is that
correlated effect. The apply deadline takes no part in it: finishing a durable
application is continuation, not a new claim, and `ClaimCommand` still refuses
at or after the deadline. `RejectCommand` admits only a correlation that proves
no effect committed, for EVERY caller and state — the claim holder standing on
its own committed effect is refused exactly as a late reconciler is — and an
expired `applying` record additionally needs a lease epoch above the claim's AND
a journal fence above that same epoch. The correlation walks the session's
stream from its first record on every rejection; that cost is deliberate and
unconditional, because a check the slow path performs and the fast path skips is
how a committed effect gets overwritten.

That narrows the head-of-line hazard the previous section used to leave open: an
expired `applying` record was settleable by nobody, forever, and a conforming
one is now settled by the next lease holder — whose own `OpenJournal` writes the
fence that makes its evidence conclusive, so the row clears when the session is
next attached. The permanent hazard was RELOCATED rather than eliminated. A
due-command reader should size its examined-versus-returned signal for three
sources: a live claim outliving the deadline, bounded by `MaxCommandClaimTTL`;
an `unresolved` correlation in the crash window between a prefix and its effect,
bounded by re-attachment; and an `unresolved` correlation caused by a
writer-contract violation, bounded by nothing at all, because the record at
`prefix+1` is already durable and will never become the effect or the fence.

Three obligations fall on a Host and none of them can be checked here, so
`inbox_recovery.go` states them where an author will look. A prefix is committed
immediately before its effect, one application at a time — so a prefix is a
COMMITMENT to append the effect next. If that append fails, the prefix is
already durable and the command reads `unresolved`, so it can no longer be
rejected under the claim that wrote it; the only route out is to drop the
journal grant, reopen (which fences the prefix into `abandoned`), wait out the
applying claim's own expiry, and reject at the higher epoch. Do NOT complete
over it: nothing checks a same-epoch result against the journal, so a fabricated
event id is accepted and becomes the command's durable outcome. Every runtime-visible
effect gets a prefix — an effect without one reads as `absent` and can be
rejected over, which is the one part of the writer contract adjacency cannot
enforce. And a Host must not append a prefix for a command whose claim epoch is
below its current grant: losing the lease abandons in-flight applications, and
re-attaching does not resume one. The fence proves a GRANT is dead, not that a
Host stopped writing, so a Host that re-attaches and carries on applying commits
the very fence that makes its own unfinished work look settleable.

A command's due state is derived from the record rather than from the operation
writing it. A non-terminal command is due at the earliest instant something must
look at it again — its apply deadline, or a claim expiry that lapses first, so a
crashed writer costs the command a claim TTL rather than a rejection at its
deadline — bounded above by the deadline, so the reconciler's page can never
miss a command whose deadline has passed. A terminal command is not due at all
and stays directly readable by its stable key. `validateInboxState` states, on
the record, which members each state must and must not carry, which is what makes
`applied` and `rejected` structurally exclusive rather than merely sequenced.

`InboxEntry.CommandStatus` projects the durable record onto core's public
`CommandStatus`. The five durable states map onto four public ones by treating
an UNCLAIMED `pending` command as `accepted` — core's own definition, "the inbox
commit succeeded", is exactly what this store knows about a command nobody has
picked up — and `claimed`/`applying` as `pending`. Claim liveness deliberately
does not enter into it: a public caller cannot act on it, and it changes with a
clock rather than with the command.

## The Host registry: an expiring route over a permanent fence

`PutHostRegistration` publishes where one session is currently running:
`(host_id, host_generation, agent_id, runtime_compatibility_id, placement,
internal_endpoint, residency, accepting)` together with the writing lease epoch,
the Host's observation instant, and the instant the observation lapses. It is
one `OrderedIndex` record per session, filed in the session's own namespace,
unranked and never due, and it is read and written directly rather than listed.
`HostRegistration.Observation` projects it into Core's
`HostLinkRegistryObservation`, which is also the record's validator: Core owns
what a Host route means, so this package does not restate the endpoint,
placement and residency rules and cannot drift from the peer that applies them.

The record has two halves with opposite lifetimes. The ROUTE expires, and a
reader past `ExpiresAt` is refused it: the Host that published it may have died
at any instant since, and nothing will tell the record. The LEASE EPOCH never
expires — it is the high-water mark that refuses a superseded Host's write — and
that is why an expired or released registration is retained rather than deleted.
Dropping the row would drop the fence.

**These rows are permanent.** One per session that has ever been registered,
kept forever, with no expiry sweep and no deletion path anywhere in this
package. That is affordable rather than a debt: a registration is never listed,
never ranked, never due, and never read except by name, so a session that ran
once and stopped costs a few hundred stored bytes and nothing at all to every
reader. It is also not optional — the row IS the session's fence, so the
retention is what makes the epoch mean anything.

The consequence for anyone adding retention later, stated on `HostRegistration`
as a carry-forward contract and repeated here because this is the document a
sweep's author reads first: **the only safe reaper is one that removes a
session's whole scope at once** — this record, its catalog record, its journal,
its commands, and its collision witnesses. Deleting this row alone destroys the
fence while leaving the session registrable, which is the exact state the
retention exists to prevent, and a sweep that walks record kinds independently
and reclaims the cheapest first will reach this one first. There is no partial
version of this that is safe.

So the two reads are deliberately different functions.
`GetHostRegistration` reports `not_found`, `expired`, or `released` and carries
no tuple in any of the three: a router cannot bind to a Host this store will not
vouch for. `expired` and `released` do carry the retained lease epoch on the
error, because those two codes are the whole public account of a session with no
route — they are what a retention decision is made from, and the fence is the
one durable fact left to check that decision against. Every write instead reads the RAW record, expired and released ones
included, because a writer that believed the public reader would create a fresh
record over a row it could not see — and creating a fresh record is exactly how
a fencing high-water mark gets reset to whatever a superseded lease named. A
stored record that cannot be decoded is likewise a failure and never an absence,
for the same reason.

`ClearHostRegistration` releases a session by writing a tombstone under the same
fence, never by deleting the row. A tombstone is a registration with no route
at all, which is one nil rather than an enumeration of cleared members, so a
released record cannot be routed to however its timestamps read or whatever
clock the reader is configured with. Cleanup is idempotent under one grant and
returns the stored tombstone without writing; a LATER grant releasing the same
session is not a repeat and rewrites the tombstone, because otherwise the fence
would stay at the older epoch and every lease granted in between could still
write. A session that was never registered is `not_found`: cleanup is idempotent
with respect to its own tombstone, not with respect to nothing.

Three members of this record also appear on the catalog record, and in each case
the catalog holds Factory-authored DESIRED state or a durable status projection
while the registry holds the Host's OBSERVED answer: desired placement against
the admission model the session is actually running under, the last known
residency against the routable residency that disappears with the route, and the
desired runtime against the runtime the running Host actually loaded. The lease
epoch appears on both because each record carries the epoch its OWN writes are
fenced at; neither is derived from the other and they advance independently.

## The Host target directory: derived capacity that rows leave

`PublishHostTarget` advertises what one Host can currently take for one target:
`(agent_id, runtime_compatibility_id, placement)` names the target, `host_id`
completes the row's identity, and the offer itself is
`(internal_endpoint, isolation_class, accepting, available_capacity)` plus the
Host's observation instant and the instant it promises to heartbeat by. One Host
serving several targets publishes one row per target; that is derived capacity,
not a competing catalogue of what agents or runtimes exist.
`HostTarget.Report` projects a row into Core's `HostLinkCapacityReport`, which
is also the record's validator, exactly as the registry delegates to Core.

**It is the opposite of the registry in almost every way, and deliberately so.**
The registry is one permanent, unranked, never-due row per SESSION whose lease
epoch is that session's ownership fence. This is one row per (target, host),
ranked by free capacity, due at its own heartbeat expiry, and REMOVABLE — rows
here must actually leave, or a crashed Host permanently occupies the front of
every placement page.

**A row never proves session ownership.** There is nowhere in it to say so: the
record names no tenant, no session and no lease epoch, and neither does the
projection a placement page publishes. `HostGeneration` is a write-ordering
high-water mark over one row — `host_id` is part of the identity, so the only
writers it can ever compare are incarnations of ONE Host — and what it prevents
is a dead incarnation overwriting live capacity, which is a liveness fault. The
error code for it is `generation` rather than `epoch` for exactly that reason.
`TestHostTargetsCannotSpellSessionOwnership` reads the source and fails if any
type in this record's family grows a tenant, session, or lease-epoch member.

Both views are functions of the RECORD, in one function each. A row is ranked by
`available_capacity` when its Host is accepting, and is UNRANKED when the Host
has stopped accepting or when the row is withdrawn; it is due at its expiry when
it is advertised, and NOT DUE when it is withdrawn. Withdrawal is one nil
pointer rather than a set of cleared members, so "withdrawn" leaving both views
is a property of the record's shape rather than a rule a writer has to remember.

Three things remove a row from the placement page, and nothing else does:

- **A graceful drain.** `DrainHostTarget` writes the withdrawn record, which
  leaves both views in one compare-and-swap, at the instant a Host decides to
  stop rather than when its promise runs out.
- **The due reconciler.** `ReconcileHostTargets` is a SERVICE operation — it
  names no tenant and no target and sweeps the whole directory's deadline view —
  and it is the only thing that removes a CRASHED Host's row. A deployment that
  never calls it accumulates ranked capacity that no longer exists.
- Nothing else. In particular `ListCompatibleHosts` does not: it declines to
  publish a lapsed row and counts it in `LapsedSkipped`, so a caller is never
  handed an endpoint this store will not vouch for, but the row stays ranked. A
  nonzero count means the directory is owed a sweep.

**No single row can fail a bounded page, anywhere in this package.** Every
per-row refusal is counted and stepped over — lapsed ones in `LapsedSkipped`,
undecodable and misfiled ones in `UnreadableSkipped` — and that is the strongest
rule in this record rather than leniency. Nothing here ever rewrites a row it
cannot read, because a newer writer may have produced it; so a reader that
failed the whole page on one would take *every* Host serving that target out of
service for as long as the row existed, which is forever, with no recovery path
anywhere in the system. Skipping leaves the newer writer's row untouched and
starts publishing it the instant a reader that understands it asks. A failure
returned from `ListCompatibleHosts` is therefore always about the query — a bad
limit, a foreign cursor, a provider that could not answer — and never about one
row.

`ListDueGates` and `ListSessions` obey the same rule, and they were changed to.
Both used to fail the whole page on one row, and both were reachable states with
no way out: a single undecodable gate intent sits at the head of an ascending
deadline view that has no cursor at all, so it disabled gate expiry for *every
tenant* permanently; a single undecodable catalog row made a tenant unlistable
and, because a failed page issues no continuation, took every session ranked
behind it too. They report `DueGatePage.Unreadable` and
`SessionPage.UnreadableSkipped`. `SessionPage` is this package's own type
embedding Core's, added for exactly that count.

Nothing here calls the provider's `Delete`, and it cannot: the ordered index
promises an identity is never reusable after a tombstone, while a Host that
drains at shutdown and advertises again at startup reuses this identity as a
matter of course. A withdrawn row is therefore retained and REUSED, which is
what makes a restart work.

The reconciler revalidates each row's own stored expiry before writing anything
and compare-and-swaps onto the revision the due page reported. The due view is
weakly consistent, so a Host may have heartbeated since the page was read; the
two checks together mean such a Host either presents an unlapsed expiry, in
which case the sweep leaves it alone, or has already advanced the revision, in
which case the write loses. A sweep that trusted the page alone would withdraw
the capacity of a Host that is alive, and `StillLive` counts the rows the
revalidation saved.

A sweep queries at one due bound for its whole walk, because a due cursor binds
to the exact bound that issued it; the bound therefore travels in the sweep's
own continuation, while the instant each row is revalidated against is read
fresh. The two are deliberately allowed to differ: the bound decides only which
rows a page contains, the revalidation decides whether any of them may be
withdrawn, and a fresh reading is monotonically at or after the bound, which is
the safe direction. Carrying the bound therefore costs nothing in safety — a
bound this store never issued can at worst make the sweep look at rows it then
declines to touch.

A row the sweep cannot decode stays due, so it heads every later ascending due
page. It is counted in `Unreadable` and deliberately NOT rewritten: it may be a
newer writer's row, and un-ranking that during a rolling upgrade would take live
capacity out of service on every pass. **The page budget alone does not make
that survivable, and believing it did is how this record nearly shipped with the
head-of-line failure its deadline view exists not to have.** A budget bounds the
work one pass does; it says nothing about progress, because every pass restarts
at the head of the same view and nothing removes an unreadable row, so that
population only grows — once it reaches `MaxPages × Limit`, every later pass
spends its whole budget on those rows and withdraws nothing, forever. What
supplies progress is the CONTINUATION on the result: a sweep that runs out of
budget reports where it stopped, and a caller that pages until `Exhausted`
reaches every row however many unreadable ones precede them.

**There is no in-band repair for a genuinely corrupt row.** A row that is not a
newer writer's — one whose bytes are damaged rather than merely unfamiliar — is
permanent: it cannot be withdrawn, because a withdrawal is built from the
record's own decoded identity; it cannot be republished, because a Host names a
target and a host rather than a row; it cannot be swept, for the reason above;
and `Delete` is correctly unusable here, because it would retire that Host's
identity for the target forever. So a persistently nonzero `Unreadable` or
`UnreadableSkipped` is not something a retry, a sweep, or an operator command
resolves — it needs a build that understands the row, or direct provider-level
intervention outside this package. The counts exist so that state is visible
rather than silent; they are not a queue that drains.

`ListCompatibleHosts` is one ranked provider query per page: the target is the
ranking scope, so the restriction and the capacity order are both inside the
query. It deliberately does not verify the target's collision witness, which the
writes bind — a listing names no row, and a target nothing has ever advertised
has no witness to prove, so requiring one would answer "no capacity" with a
failure. Cross-target safety comes from below instead: every row a page returns
is held to the target its own bytes claim.
