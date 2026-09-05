package sessionstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// FIXED CONTROL SHARDS: WHY THIS IS A SEPARATE FILE FROM reconcile.go
//
// The runbook offered this file or an extension of reconcile.go. It is separate
// because reconcile.go carries a structural guard that a shared file would
// silently destroy. TestNothingInThisPackageReadsAClaimToDecideAWrite derives
// every top-level name reconcile.go declares and requires that NO other
// production file use any of them, which is what makes "a reconciliation claim
// licenses nothing" a property of the package rather than a comment. Shard
// placement has the opposite requirement: inbox.go and gates.go MUST call it,
// because a record's shard is part of where it is filed. Declaring
// shardNamespace in reconcile.go would fail that guard on its first use, and
// the only way to keep both would be to weaken the guard to an allowlist —
// which would leave the claim's isolation resting on whoever maintains the
// list.
//
// The guard globs *.go, so this file is inspected by it automatically and the
// guard grows strictly stronger for being written here.
//
// WHAT A SHARD IS. Reconciliation is a cross-tenant, service-plane sweep: it
// asks "what work is due anywhere?", which is a question about wall-clock time
// rather than about a tenant. The ordered index answers exactly that with
// ListDue, which is NAMESPACE-WIDE and takes no scope — so the unit a sweep can
// address is a namespace, and a shard is therefore a namespace suffix rather
// than a scope, a rank, or a filter applied after a page.
//
// A record's ordering scope is UNCHANGED by sharding: it stays the session's
// physical namespace, so identity remains (namespace, ordering_scope,
// stable_key) and two sessions that hash to one shard cannot collide. What the
// shard buys is that a sweep can be spread across replicas and paged in a fixed
// number of streams, instead of every replica walking one deployment-wide view.
//
// WHY A HASH RATHER THAN A REGISTRY. The shard is a pure function of
// (TenantID, SessionID), so a writer and a sweeper compute the same answer with
// no lookup, no cache to invalidate, and nothing to keep in step. That is also
// why the count cannot be a runtime knob: it is an input to that function, so
// changing it moves every existing record's address without moving the record.

const (
	// DefaultControlShards is the shard count a store adopts when its backend
	// is first initialized and no count was named.
	DefaultControlShards = 16

	// MaxControlShards bounds the count. It is a ceiling on a REPLICA'S SWEEP
	// COST rather than on the provider: a sweep visits every shard round-robin,
	// so the count is a per-pass floor on the number of provider queries even
	// when nothing at all is due. It is also bounded below by the marker's
	// two-byte field and by controlShardToken's fixed width.
	MaxControlShards = 4096

	// MinControlShards is one — an unsharded deployment, which is a legitimate
	// configuration and the one a single-replica local Factory wants.
	MinControlShards = 1
)

// controlShardToken renders a shard as the fixed-width namespace segment that
// names it.
//
// Four lowercase hex digits spell every shard MaxControlShards permits, and a
// hex digit is a legal first byte of a storage name segment, whose grammar
// requires [a-z0-9].
//
// FIXED WIDTH IS NOT LOAD-BEARING FOR SAFETY, and it is worth saying so rather
// than implying otherwise. This is the only producer of a shard segment, so
// there is one spelling per shard whatever the width; a variable-width
// rendering would place records differently but would not make two shards
// collide. What the fixed width buys is that shard segments sort in shard
// order and are recognizable by length, which is what lets a test say "this
// namespace is one of base's shards" without parsing. That is pinned by
// TestShardNamespacesAreDistinctOverTheWholeCrossProduct rather than left
// here.
func controlShardToken(shard uint32) string {
	if shard >= MaxControlShards {
		panic("sessionstore: internal control shard bound invariant")
	}
	return fmt.Sprintf("%04x", shard)
}

// shardNamespace is the ONE spelling of a sharded namespace, and every
// OrderedID that names one is required to be written as a direct call to it
// with a package namespace constant as its base.
//
// That is not a style rule. TestOrderedNamespacesAreDistinct reads the
// Namespace of every OrderedID this package files from source and proves the
// set is pairwise distinct; a namespace computed some other way would be
// invisible to it, and a record kind sharing a partition with another is
// exactly the failure that guard exists to catch. Requiring this one spelling
// is what lets the guard recover the BASE from the syntax and then prove
// distinctness over the whole (base, shard) cross product rather than over the
// bases alone.
//
// The separator makes the shard its OWN SEGMENT rather than a suffix glued to
// the base's last one. That keeps the base recoverable from the result — the
// namespace is the base plus one segment, nothing more — which is the form the
// distinctness guard's reasoning is stated in, and it keeps a base's own
// segments intact instead of silently lengthening the final one.
func shardNamespace(base string, shard uint32) string {
	return base + "/" + controlShardToken(shard)
}

// controlShardOf places one session in a control shard.
//
// The digest is domain-separated from every other use of this keyspace's hash —
// a session's namespace token, its witness, a cursor scope — so a shard number
// can never be derived from a stored name and a stored name can never be
// derived from a shard number. digestFrame length-prefixes each input, so
// ("ab", "c") and ("a", "bc") are different frames and no pair of identities
// can be reassociated into another pair's shard.
//
// The reduction is the low 64 bits of the digest modulo the count. Modulo of a
// uniform 64-bit value is biased toward the low residues by at most
// count / 2^64, which for the 4096-shard ceiling is one part in 2^52 — smaller
// than the sampling noise of any deployment that could exist, and
// TestControlShardsSpreadEvenlyOverALargeFixture measures the distribution
// rather than leaving this paragraph to stand for it.
//
// It takes the count as an ARGUMENT rather than reading k.shards, so the
// function is total over counts and a test can drive placement at a count the
// store was not opened with. deriveSessionScope is the only production
// function that calls it — once per layout branch — and both branches pass the
// store's persisted count.
//
// It uses the keyspace's injected digest for the same reason every other
// derived name does: a deployment gets ONE hash, so a test that substitutes a
// colliding digest sees a consistently colliding store rather than one whose
// names collide and whose shards do not.
func (k keyspace) controlShardOf(tenant sessionwire.TenantID, session sessionwire.SessionID, count uint32) uint32 {
	if count < MinControlShards || count > MaxControlShards {
		panic("sessionstore: internal control shard count invariant")
	}
	sum := k.digest(digestFrame("looprig/sessionstore/shard/v1", []byte(tenant), []byte(session)))
	// #nosec G115 -- the modulus is count, which the bound above holds to MaxControlShards
	return uint32(binary.BigEndian.Uint64(sum[:8]) % uint64(count))
}

// ControlShards reports the shard count this store's backend is committed to.
//
// It is a READ of a persisted decision, not a setting. A sweeper needs it to
// know how many shards to visit, and it must come from the store rather than
// from the sweeper's own configuration: a sweeper that visited a different
// number would silently never look at some of them.
func (s *Store) ControlShards() int { return int(s.keys.shards) }

// SERVICE-ONLY, AND WHAT THAT DOES AND DOES NOT MEAN HERE.
//
// The two sweeps below are control-plane queries. They are cross-tenant by
// construction: a shard holds whichever tenants hash into it, so a page can mix
// them, and a tenant principal must never be able to reach one.
//
// THERE IS NO CAPABILITY GATE IN THIS PACKAGE, and there is not one here
// either. SessionStore takes identities as data and authorizes nothing; a
// caller authorizes before it calls. What this package can offer, and does, is
// a STRUCTURAL HINT: a sweep request names no tenant and no session, so there
// is no tenant identity for a service to forward from a tenant's request and
// nothing a tenant-scoped handler could naturally build one from. That is the
// same guarantee ReconcileHostTargets carries, stated the same way, and it is
// weaker than an enforcement. It is written plainly rather than implied because
// an implied enforcement is worse than none.
//
// RetireGateDeadlineIntent cannot carry even that hint: it must name the
// session whose intent it retires. Its protection is elsewhere — the store
// revalidates the row against the durable projection and refuses anything a
// tenant could aim it at productively — and the service-only rule for it is
// prose alone.

// A SWEEP CONTINUATION, STATED ONCE FOR BOTH KINDS.
//
// Its payload is the due bound this sweep is querying at, followed by the
// provider's own due token carried verbatim.
//
// THE BOUND IS IN THE TOKEN because it has to be: the ordered index binds a due
// cursor to the exact bound that issued it, so a resumed call that recomputed
// the bound from a fresh request would present a token for a different query
// and be refused.
//
// Carrying it costs nothing in safety. The bound only selects WHICH rows a page
// contains; neither sweep WRITES, so the worst a caller presenting a bound this
// store never issued can do is look at rows that are not due — every one of
// which is still held to its own stored filing before it is reported. The
// retirement a remnant enables is a SEPARATE call that revalidates from scratch
// against its own clock reading and its own read of the projection, so no part
// of it rests on the bound this token carries.
//
// WHY ONE TYPE AND NOT TWO COPIES. The two kinds differ in a magic, a digest
// domain, and an error vocabulary, and in nothing else; written out, their
// encoders, decoders and position resolvers were byte-identical but for those
// three. This package has hoisted that exact shape twice already — epochFence
// found its fourth copy only after three were merged, and checkFiledScope
// records that "what each copy was free to do was drift" — and the drift here
// has a specific shape: one copy quietly stops enforcing the ceiling on ISSUE,
// or starts accepting a position with no provider bytes, on a path that only
// runs when something is already wrong and where a weakened check looks exactly
// like a passing one.
type sweepCursorKind struct {
	// magic is the envelope's kind tag. cursor.go states what it buys, and
	// TestCursorMagicsAreDistinct derives the set from source.
	magic   string
	version byte

	// domain separates this kind's scope digest from every other derivation in
	// this keyspace; TestCursorScopeDomainsAreDistinct derives that set too.
	domain string

	// invalid and backend are the kind's own error vocabulary. They are
	// FUNCTIONS rather than codes because the two vocabularies are different
	// types, and a shared type would have forced one of the two families to
	// borrow the other's — which is the drift this hoist exists to prevent,
	// reintroduced at the error boundary.
	cursorErr  func() error
	invalidErr func(field string) error
	backendErr func(field string) error
}

const (
	// sweepCursorBoundBytes is the big-endian due bound every sweep
	// continuation carries ahead of the provider's own token.
	sweepCursorBoundBytes = 8

	// maxSweepCursorBytes bounds a decoded continuation and
	// maxSweepCursorPayload the bound-plus-provider-token inside it. The
	// ceiling is enforced on ISSUE as well as on presentation, so a token this
	// store hands out is always one it will accept back — and for a sweep that
	// is sharper than usual, because a continuation it cannot reissue is a
	// sweep that silently reverts to making no progress at the head of the
	// view.
	maxSweepCursorBytes   = 4 << 10
	maxSweepCursorPayload = maxSweepCursorBytes - cursorPayloadAt
)

// The two kinds. Declaring them here rather than beside their callers is what
// makes "these differ in exactly three things" checkable by reading one screen.
var (
	dueCommandCursor = sweepCursorKind{
		magic:      "LRDC",
		version:    1,
		domain:     "looprig/sessionstore/duecommand/cursor/v1",
		cursorErr:  func() error { return inboxErr(InboxErrorCursor, "cursor", nil) },
		invalidErr: func(field string) error { return inboxErr(InboxErrorInvalid, field, nil) },
		backendErr: func(field string) error { return inboxErr(InboxErrorBackend, field, nil) },
	}

	dueGateCursor = sweepCursorKind{
		magic:      "LRDG",
		version:    1,
		domain:     "looprig/sessionstore/duegate/cursor/v1",
		cursorErr:  func() error { return catalogErr(CatalogErrorCursor, "cursor", nil) },
		invalidErr: func(field string) error { return catalogErr(CatalogErrorInvalid, field, nil) },
		backendErr: func(field string) error { return catalogErr(CatalogErrorBackend, field, nil) },
	}
)

// scope binds a continuation to this cursor KIND and to the SHARD it was issued
// for, and to nothing else.
//
// The shard is in the scope rather than in the payload because it is an
// identity the token is FOR, not a value the sweep carries forward.
//
// IT IS DEFENCE IN DEPTH, NOT THE ONLY BARRIER, and an earlier version of this
// comment claimed more than the contract supports. It said a mis-sharded token
// would SILENTLY skip rows. It would not: storage's DueCursor contract binds a
// provider token to the exact namespace and bound that issued it, and a shard
// IS a namespace here, so ListDue refuses one for another shard with a typed
// invalid-cursor error. What this binding adds is that the refusal happens
// HERE, before a provider round trip, and reaches the caller labelled as its
// own cursor rather than as a failure of the query it was presented to — which
// are different facts and, for a sweeper, different responses.
//
// A sweep names no tenant and no session, so there is nothing else to bind it
// to; inventing an identity would suggest a scoping this operation does not
// have.
func (k sweepCursorKind) scope(s *Store, shard uint32) [cursorScopeBytes]byte {
	return s.keys.digest(digestFrame(k.domain, binary.BigEndian.AppendUint32(nil, shard)))
}

func (k sweepCursorKind) encode(s *Store, shard uint32, bound int64, after storage.DueCursor) (sessionwire.Cursor, error) {
	payload := make([]byte, sweepCursorBoundBytes, sweepCursorBoundBytes+len(after))
	binary.BigEndian.PutUint64(payload, uint64(bound)) // #nosec G115 -- a signed bound round-trips through the same width
	payload = append(payload, after...)
	if len(payload) > maxSweepCursorPayload {
		return "", k.backendErr("next_cursor")
	}
	return sessionwire.Cursor(encodeCursorEnvelope(k.magic, k.version, k.scope(s, shard), payload)), nil
}

// decode unwraps a continuation this store issued for this kind and this shard.
// One a sweep issued always carries at least one provider byte beyond the
// bound, because an exhausted view returns no cursor at all.
func (k sweepCursorKind) decode(s *Store, shard uint32, cursor sessionwire.Cursor) (int64, storage.DueCursor, error) {
	payload, ok := decodeCursorEnvelope(
		k.magic, k.version, k.scope(s, shard),
		string(cursor), sweepCursorBoundBytes+1, maxSweepCursorPayload)
	if !ok {
		return 0, "", k.cursorErr()
	}
	bound := int64(binary.BigEndian.Uint64(payload[:sweepCursorBoundBytes])) // #nosec G115 -- the inverse of the encode above
	return bound, storage.DueCursor(payload[sweepCursorBoundBytes:]), nil
}

// position resolves one request to the (bound, provider position) a page is
// read at, refusing a request that states the bound twice.
//
// A request may not carry both a continuation and a bound. Preferring either
// silently is how a resumed sweep starts querying a bound it was never bound to
// — the provider would refuse the token, and the sweep would restart at the
// head of the view every time, which looks like liveness and is starvation.
func (k sweepCursorKind) position(
	s *Store,
	shard uint32,
	cursor sessionwire.Cursor,
	dueAtOrBefore time.Time,
) (int64, storage.DueCursor, error) {
	if cursor == "" {
		// rankableTime already refuses the zero Time, which is earlier than
		// every representable instant; a separate IsZero check would be a
		// second statement of one rule.
		if !rankableTime(dueAtOrBefore) {
			return 0, "", k.invalidErr("due_at_or_before")
		}
		return dueAtOrBefore.UnixMilli(), "", nil
	}
	if !dueAtOrBefore.IsZero() {
		return 0, "", k.invalidErr("due_at_or_before")
	}
	return k.decode(s, shard, cursor)
}

// ListDueCommandsRequest positions one bounded page of one shard's outstanding
// commands.
//
// Shard names the control shard to read and must be below the store's
// ControlShards. A caller sweeps by visiting every shard round-robin; the store
// deliberately does not do that for it, because a replica that swept every
// shard in one call would hold the whole deployment's reconciliation in one
// request's latency and one caller's failure.
//
// DueAtOrBefore is the inclusive wall-clock bound and is the FIRST page's
// query. A continuation carries its own bound, so a resumed request must leave
// this zero: presenting both would be two answers to one question, and silently
// preferring either is how a resumed sweep starts querying a bound it was never
// bound to.
type ListDueCommandsRequest struct {
	Shard         int
	DueAtOrBefore time.Time
	Limit         int
	Cursor        sessionwire.Cursor
}

// DueCommand is one outstanding command. The identities are read from the
// RECORD rather than restated beside it — a sweep learns them from the row —
// and Entry is the same value a named read of that command returns, held to the
// same filing checks.
//
// It wraps a single member rather than being one, for the reason DueGatePage is
// a type rather than a second return value: what a reconciler needs BESIDE the
// record is not settled yet, and adding a field is a smaller change to make
// than changing the element type of a public slice.
type DueCommand struct {
	Entry InboxEntry
}

// DueCommandPage is one bounded page of outstanding commands together with what
// producing it cost. It is where the three cost members both sweeps carry are
// defined; DueGatePage refers here rather than restating them.
//
// EXAMINED is the number of rows the provider returned, and LIMIT is the
// EFFECTIVE limit after a zero request limit has been resolved to the store's
// page size, so the comparison is available to a caller that named no limit.
// Together they answer a question the results alone cannot: Examined == Limit
// with nothing reported means this page was full and none of it said anything,
// which is a different state from "nothing is due".
//
// UNREADABLE counts rows this reader could not decode, that disagreed with the
// filing they were found under, or that belong in a different shard. Each is
// SKIPPED rather than failing the page, and that is the strongest rule here
// rather than leniency: this view is ascending by an instant that never moves
// and nothing rewrites such a row, so a reader that failed on one would switch
// reconciliation off for every tenant in the shard until someone repaired the
// row by hand.
//
// LOCATING AN UNREADABLE ROW IS OUT OF BAND, and that is a real limitation
// rather than an oversight to be discovered. Unreadable is a count; this
// package has no logger and no channel to report a row's identity through, and
// adding one is public surface a later task should design rather than something
// to bolt on here. What an operator has instead is the SHARD and the DUE BOUND
// the page was read at, which narrow the row to one namespace and one prefix of
// an ordered view. The row's stable key and ordering scope are in hand at both
// skip sites, so a reporting channel is cheap to add when something exists to
// receive it.
//
// NEXTCURSOR is what keeps that skipping from becoming starvation. An
// unreadable row is stepped over by the provider's own continuation, which
// resumes from the tuple the page ended on, so a row left in place is passed
// rather than met again on the next page.
type DueCommandPage struct {
	Commands   []DueCommand
	Examined   int
	Unreadable int
	Limit      int
	NextCursor sessionwire.Cursor
}

// ListDueCommands returns one bounded page of one shard's commands whose
// horizon has passed.
//
// It is a READ. It claims nothing, expires nothing, and writes nothing: what a
// reconciler does about an outstanding command is the reconciler's business,
// and every write it then performs is a compare-and-swap against the revision
// this page reported.
//
// ITS COST IS THE PAGE, AND THAT IS THE WHOLE POINT. The rows come from the
// ordered index's due view of one namespace, so a terminal command — which
// inboxDue files NOT DUE — is not in the view at all, a historical session
// contributes nothing, and the deployment's tenant count does not appear in the
// cost. Nothing here reads the catalog, and nothing enumerates a session's
// inbox.
//
// IT DOES NOT VERIFY EACH ROW'S SESSION WITNESSES, and that is a departure from
// every NAMED read here worth stating rather than leaving to be noticed. Those
// paths verify because they DERIVE a record name from identities and must not
// trust it on its own. This one derives nothing: the provider supplies the row,
// and the row is held to its own bytes — its filing, its scope, its shard. A
// collision would put two sessions in one ordering scope, and a row would still
// report the identities its own bytes carry; what a reconciler then DOES with
// it goes through a named write, which verifies the witnesses and refuses. The
// alternative costs one KV read per distinct session per page for a check that
// decides nothing this page reports.
func (s *Store) ListDueCommands(ctx context.Context, req ListDueCommandsRequest) (DueCommandPage, error) {
	shard, err := s.validateShard(req.Shard, inboxInvalid)
	if err != nil {
		return DueCommandPage{}, err
	}
	limit, ok := s.pageLimit(req.Limit)
	if !ok {
		return DueCommandPage{}, inboxErr(InboxErrorInvalid, "limit", nil)
	}
	bound, after, err := dueCommandCursor.position(s, shard, req.Cursor, req.DueAtOrBefore)
	if err != nil {
		return DueCommandPage{}, err
	}

	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return DueCommandPage{}, err
	}
	defer release()

	provider, err := s.backend.OrderedIndex.ListDue(
		opCtx, shardNamespace(inboxNamespace, shard), bound, after, limit)
	if err != nil {
		return DueCommandPage{}, classifyInboxOrderedError(err, "due_commands")
	}
	page := DueCommandPage{
		Commands: make([]DueCommand, 0, len(provider.Records)),
		Examined: len(provider.Records),
		Limit:    limit,
	}
	for _, stored := range provider.Records {
		entry, ok := s.dueCommandFor(stored, shard)
		if !ok {
			page.Unreadable++
			continue
		}
		page.Commands = append(page.Commands, DueCommand{Entry: entry})
	}
	if provider.NextCursor != "" {
		if page.NextCursor, err = dueCommandCursor.encode(s, shard, bound, provider.NextCursor); err != nil {
			return DueCommandPage{}, err
		}
	}
	return page, nil
}

// dueCommandFor reads one row of a due page, reporting whether this store can
// vouch for it.
//
// The row is decoded here and again inside inboxEntryFor, and that is
// deliberate, for the reason the host target sweep states: this reader has no
// request to hold the row to, so it derives the session scope from the row's
// OWN bytes and then asks the same filing question a named read asks. Saving
// the second decode would mean a second, weaker check that only the sweep uses,
// and a shortcut that skips a check the slow path performs is exactly how a
// sweep starts reporting rows a named read would have refused.
//
// It reports a boolean rather than an error because every failure here has ONE
// response — count it and step over it. There is no failure that is about the
// store rather than the row: nothing on this path touches the provider.
func (s *Store) dueCommandFor(stored storage.OrderedRecord, shard uint32) (InboxEntry, bool) {
	record, err := decodeInboxRecord(stored.Value)
	if err != nil {
		return InboxEntry{}, false
	}
	scope, err := s.deriveSessionScope(record.TenantID, record.SessionID)
	if err != nil {
		return InboxEntry{}, false
	}
	// THE SHARD IS PART OF THE FILING, so it is verified like every other part
	// of it. The provider chose this row's namespace, this reader did not name
	// the row, and the shard is a function of the record's own identities — so
	// a row whose bytes hash elsewhere is a row filed where it does not belong,
	// and reporting it would mean one sweep pass answering for a shard it was
	// not asked about while the shard that owns the row never sees it.
	if scope.ControlShard != shard {
		return InboxEntry{}, false
	}
	entry, err := inboxEntryFor(stored, scope, record.TenantID, record.SessionID, record.CommandID)
	if err != nil {
		return InboxEntry{}, false
	}
	return entry, true
}

// validateShard bounds a caller-supplied shard against the count this backend
// is committed to, at the entry point, before anything derived from it is
// built. An out-of-range shard is a caller mistake rather than an empty page:
// returning nothing for shard 99 of a 16-shard store would let a sweeper
// misconfigured against the wrong count report a clean sweep forever.
func (s *Store) validateShard(shard int, invalid func(string, error) error) (uint32, error) {
	if shard < 0 || shard >= int(s.keys.shards) {
		return 0, invalid("shard", nil)
	}
	return uint32(shard), nil // #nosec G115 -- bounded above by s.keys.shards, itself at most MaxControlShards
}

// MinGateIntentRemnantAge is how long a gate deadline intent must have been
// durable before this store will retire it as a remnant.
//
// IT IS A CEILING ON HOW LONG ONE OpenGate CALL CAN TAKE, not a policy delay.
// OpenGate writes the intent, then commits the open projection, and between
// those two writes the intent looks exactly like a remnant. Nothing in the two
// records distinguishes "the open crashed" from "the open is in flight", so a
// retirement inside that window can tombstone the deadline of a gate that is
// about to become publicly open — leaving it waiting with nothing to expire it,
// under an identity that can never be reused because this package's tombstones
// are permanent.
//
// Five minutes is chosen against the cost of being wrong in each direction, and
// the two costs are not symmetric. Waiting too long leaves a remnant row in a
// due page for longer; the continuation steps past it, so the cost is a row per
// page, not a stalled sweep. Waiting too little destroys a live gate's
// deadline. So the window is set far above any plausible span of the interval it
// actually covers, rather than close to it.
//
// THE INTERVAL IS THE CLOCK READING TO THE PROJECTION COMMIT, not "between two
// writes". OpenGate reads the clock at the top, before it is admitted and
// before it reads the catalog, because this package reads the clock once ahead
// of any provider work. So the exposed span is a mutex acquisition, the
// session-scope verification and catalog read, the intent write and the
// projection write — several round trips rather than the gap between two of
// them. Leaving the reading where it is remains right: moving it after the
// catalog read would buy a shorter interval by breaking the rule that keeps
// every operation's decisions evaluated at one instant.
//
// WHICH CLOCK, AND WHAT THAT DOES NOT BUY. RecordedAt is stamped by the store
// that opened the gate and the age is evaluated by the store that sweeps —
// different processes, each with its own injected clock, with no shared time
// available (see WithClock). The comparison is therefore skew-relative: a fast
// sweeper reaches the window early by the skew, a slow one late. That is
// affordable only because the window is minutes and the skew a deployment
// tolerates is seconds; it would not be affordable for a window of seconds, and
// shrinking this constant without a real clock is the way to make it unsafe.
const MinGateIntentRemnantAge = 5 * time.Minute

// RetireGateDeadlineIntentRequest tombstones one gate's deadline intent.
//
// Revision is the revision the caller observed on the row, in a
// RemnantGateIntent from ListDueGates. The write is a compare-and-swap onto it,
// so a row that moved between the page and this call is refused rather than
// retired on stale evidence.
//
// It is the counterpart of RemnantGateIntent, which is what a sweep reports and
// what a caller builds this from; the two are deliberately separate types for
// the reason stated there.
//
// It names a tenant and a session because it must: the intent is filed under
// the session's scope and there is no way to reach it without them. So this
// operation cannot carry the structural hint the sweeps carry — a request that
// names no tenant — and its service-only status is prose alone. What protects
// it is that there is nothing a tenant could aim it at productively: it refuses
// any gate the session's own durable record still projects as open.
type RetireGateDeadlineIntentRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	GateID    sessionwire.GateID
	Revision  uint64
}

// RetireGateDeadlineIntent removes one remnant gate deadline intent.
//
// A REMNANT IS THE ONLY THING IT REMOVES, and "remnant" is decided here, from
// durable state, rather than accepted from the caller. ListDueGates reports
// candidates; this operation independently re-reads the session's projection
// and the row, at its own clock reading, and refuses everything else.
//
// WHAT EACH ABSENT ANSWER LICENSES, enumerated, because this is a path where a
// value that reads as absent removes work:
//
//   - The INTENT ROW IS ABSENT: refused, NotFound. Absence is not "already
//     retired" — this package never erases, so a retired intent has a durable
//     spelling and it is a tombstone. An absent row means the caller is
//     retiring something this store has never held, and answering success would
//     tell a sweeper it had handled a row it never touched.
//   - The INTENT ROW IS A TOMBSTONE: success, and nothing is written. This is
//     the repeat case, and a caller cannot tell a lost reply from a failure, so
//     a second retirement is ordinary rather than mistaken. The tombstone is
//     this store's own record that the work is done.
//   - The SESSION HAS NO DURABLE EXISTENCE — no catalog record, a tombstoned
//     one, or an unbound session witness: retirement is PERMITTED. A session
//     that does not durably exist cannot durably project an open gate, so every
//     intent it carries is a remnant. This is exactly noSuchSession's set and
//     deliberately not one entry wider: a collision, an unreadable record, or a
//     provider fault says nothing about whether the gate is open, and each of
//     those STOPS the operation instead.
//   - The GATE IS NOT IN AN EXISTING RECORD'S OpenGates: retirement is
//     permitted, subject to the age below. This is the ordinary remnant.
//   - The GATE IS OPEN: refused as a conflict. Retiring a live gate's deadline
//     is the one outcome this operation must never produce.
//
// And the age: an intent younger than MinGateIntentRemnantAge is refused with
// TooSoon, because inside that window "remnant" and "in flight" are the same
// bytes. TooSoon is its own code rather than a conflict for a reason a caller
// acts on — it means retry later, whereas a conflict means re-read.
func (s *Store) RetireGateDeadlineIntent(ctx context.Context, req RetireGateDeadlineIntentRequest) error {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return err
	}
	if err := req.GateID.Validate(); err != nil {
		return catalogErr(CatalogErrorInvalid, "gate_id", err)
	}
	// The clock is read before any provider read, as every other operation here
	// does: a reading taken afterwards could be later than the instant the row
	// was read at, so the age answer would be about a moment the row was never
	// evaluated in.
	now := s.clock.Now()

	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return err
	}
	defer release()

	// The session's collision witnesses are verified BEFORE the intent is read,
	// so a derived record name is never trusted on its own — the discipline
	// every read path in this package applies.
	//
	// An UNBOUND witness is relaxed here and nowhere else, because on this path
	// it is an ANSWER rather than a failure: it is one of the three ways a
	// session has no durable existence, and a session that does not durably
	// exist cannot durably project an open gate. Nothing is lost by proceeding
	// — the derived name cannot have been taken by another identity, since the
	// case where it HAS is a bound witness that disagrees, which is a hash
	// collision and is still refused here.
	if err := s.verifySessionScope(opCtx, scope); err != nil && !noSuchSession(err) {
		return err
	}
	stored, err := s.backend.OrderedIndex.Get(opCtx, gateIntentID(scope, req.GateID))
	if err != nil {
		return classifyCatalogOrderedError(err, "gate_intent")
	}
	if stored.Deleted {
		return nil
	}
	intent, err := gateIntentFor(stored)
	if err != nil {
		return err
	}
	if intent.TenantID != req.TenantID || intent.SessionID != req.SessionID {
		return catalogErr(CatalogErrorIdentity, "gate_intent", nil)
	}
	if err := verifyGateIntentFiling(stored, intent, scope); err != nil {
		return err
	}
	// The age is checked BEFORE the projection is read, so an in-flight open is
	// refused without a second round trip and without this operation's answer
	// depending on which of two racing reads landed first.
	if now.Sub(intent.RecordedAt) < MinGateIntentRemnantAge {
		return catalogErr(CatalogErrorTooSoon, "recorded_at", nil)
	}
	if err := s.refuseIfGateIsStillOpen(opCtx, scope, req); err != nil {
		return err
	}
	// Retirement mutates legacy gate state even when the catalog is absent.
	// Reserve/check the legacy protocol without requiring collision witnesses
	// or a catalog, preserving retirement of orphaned crash remnants.
	if err := s.bindProtocolMode(opCtx, scope, ProtocolModeLegacy); err != nil {
		return err
	}
	if _, err := s.backend.OrderedIndex.Delete(opCtx, gateIntentID(scope, req.GateID), req.Revision); err != nil {
		return classifyCatalogOrderedError(err, "gate_intent")
	}
	return nil
}

// refuseIfGateIsStillOpen reads the session's durable record and reports a
// conflict when it projects the named gate as open.
//
// It is separated from its caller so the three-way classification of the read's
// failure is stated once and reads as the decision it is: a session that does
// not durably exist permits the retirement, a row-local failure and a provider
// failure both stop it, and only a record that really does project the gate
// refuses it.
func (s *Store) refuseIfGateIsStillOpen(
	ctx context.Context,
	scope sessionScope,
	req RetireGateDeadlineIntentRequest,
) error {
	entry, err := s.readCatalogEntry(ctx, scope, req.TenantID, req.SessionID)
	if err != nil {
		if noSuchSession(err) {
			return nil
		}
		return err
	}
	for _, gate := range entry.Record.OpenGates {
		if gate.GateID == req.GateID {
			return &CatalogError{Code: CatalogErrorConflict, Field: "gate_id", Revision: entry.Revision}
		}
	}
	return nil
}
