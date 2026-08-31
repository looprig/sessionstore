package sessionstore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// This file adds the durable read side of public gates. It deliberately stops
// there. There is no suspension object, no resume handler, no timeout
// reconciler, and nothing that activates a suspend policy: gate CONTINUATION —
// what a Host does when a gate is answered or expires — is a later task, and a
// deadline recorded here drives nothing on its own.
//
// How this relates to the catalog's OpenGates
//
// The catalog record remains the ONE store of open gates. A gate's public
// content — its prompt, its opening event, its answerability — lives in
// CatalogRecord.OpenGates and nowhere else, which is why ReadGates is a single
// direct record read and why this file introduces no second copy of a
// projection to keep in step with it.
//
// A deadline intent is not that second copy. It is an INDEX: one ordered record
// per open gate whose due state is the gate's absolute deadline, carrying only
// the identity and the opening event needed to find and validate the gate it
// points at. The catalog cannot serve that query — it is ranked by recency, and
// a deadline sweep over it would have to read every session in the deployment —
// and the ordered index's due view can, across every tenant, in pages
// proportional to what is actually due.
//
// The join between them is directional and its one intermediate state is
// deliberate:
//
//   - OpenGate writes the intent BEFORE it commits the open projection, so an
//     interrupted open leaves an intent whose gate is not projected open.
//   - ResolveGate clears the projection BEFORE it retires the intent, so an
//     interrupted resolve leaves the same state, not the opposite one.
//
// So "an intent with no matching open gate" is the only crash remnant either
// path can produce, and it is exactly what ListDueGates validates away by
// checking each intent against the durable open projection before returning it.
// The opposite state — a gate publicly open with no durable record of its
// deadline — is never produced by these two operations.
//
// UpdateCatalogHostState still replaces OpenGates wholesale and deliberately
// does not touch intents: it is the Host's re-projection path, not an
// incremental gate edit. A gate projected only that way is readable but has no
// deadline index, and a gate dropped that way leaves a remnant intent that
// ListDueGates discards. Both are consistent with the rule above, which is why
// the two paths can coexist without either one having to know about the other.

// gateNamespace is the one OrderedIndex namespace holding gate deadline
// intents. Like the catalog it is a single namespace for the whole deployment,
// with tenants separated by the ordering scope rather than by the namespace: a
// namespace is a provider's physical partition, so one per tenant would make a
// provider allocate an unbounded number of streams or tables.
//
// Keeping it single is also what makes the due view usable at all. ListDue is
// namespace-wide by contract — it takes no scope — so one namespace is what
// lets a deployment-wide reader page through what is due without iterating
// tenants. That is deliberate: expiry is a property of wall-clock time, not of
// a tenant, and a per-tenant due query would have to be run for every tenant
// that has ever existed.
const gateNamespace = "sessionstore/gates"

const (
	// GateIntentRecordVersion is the independent version of the stored gate
	// deadline intent. A reader fails closed on any other version rather than
	// guessing which members a future encoder meant.
	GateIntentRecordVersion uint8 = 1

	// MaxGateIntentBytes bounds an encoded deadline intent. An intent holds
	// identities and one timestamp, never a prompt, so this is far above what a
	// legitimate record needs and far below the provider's own value bound.
	MaxGateIntentBytes = 8 << 10
)

// The same relationship the catalog states between its record bound and the
// provider's, for the same reason: an intent this package accepts always fits
// in the provider, leaving no state that can be written but not rewritten.
const _ = uint(storage.MaxOrderedValueBytes - MaxGateIntentBytes)

// maxGateIntentEncodedBytes is the largest an encoded intent can be. Unlike a
// catalog record, whose gate prompts are unbounded caller text, an intent holds
// four validated identities, one integer, and one timestamp, so its size has an
// arithmetic ceiling.
//
// The measurement, not an estimate: four ids at sessionwire.MaxIDBytes, each at
// its worst JSON escaping — a sessionwire id accepts control bytes, and
// encoding/json spells those as six-character \u00XX escapes — is 6144 bytes.
// The scaffolding around them, member names and punctuation, the widest uint64,
// and the two widest RFC 3339 instants, measures 230 bytes. That is 6374
// against the 6656 below, so the +512 term is 230 bytes of real content and 282
// bytes of headroom. It is not slack to spend: it does not hold one more id.
//
// The +256 term this constant used to carry was re-measured, not widened on
// suspicion, when RecordedAt was added: one more instant is 52 bytes of member
// name, punctuation and value, which left 26 bytes under the old ceiling. That
// is inside the noise of a future member, which is why the term moved.
//
// A NEW MEMBER INVALIDATES THIS ARITHMETIC — re-measure it rather than assuming
// the headroom absorbs one. The constant cannot check that itself, because it
// is not a function of the wire struct; what does check it is
// TestGateIntentSizeCeilingCoversEveryMember, which builds the widest possible
// value of every member by reflection and so grows a new one automatically.
//
// That is also why encodeGateIntent has no size check. A runtime branch there
// could not be reached by any intent that passes validation, so it could never
// be tested and would sit in the file as an untested claim.
//
// The decode side keeps its bound and needs it: stored bytes are not this
// package's own output, and a bound before a decoder allocates is a
// precondition rather than a restatement of this arithmetic.
const maxGateIntentEncodedBytes = 4*6*sessionwire.MaxIDBytes + 512

const _ = uint(MaxGateIntentBytes - maxGateIntentEncodedBytes)

// OpenGateRequest projects one gate as publicly open and records its absolute
// deadline. LeaseEpoch is the writing Host's grant epoch, compared against the
// record's committed high-water mark exactly as UpdateCatalogHostState's is:
// open gates are Host-owned state.
//
// Gate.OpenedJournalSeq must name an event at or below the record's durable
// journal tip. A gate whose opening event is not yet durable is refused rather
// than stored, because a reader would otherwise be handed a page claiming an
// event its own tip says does not exist.
type OpenGateRequest struct {
	TenantID   sessionwire.TenantID
	SessionID  sessionwire.SessionID
	LeaseEpoch uint64
	Gate       sessionwire.GateProjection
}

// ResolveGateRequest retires one open gate. It records only that the gate is no
// longer open and awaiting an answer: it carries no response, decides nothing
// about what the session does next, and starts no continuation.
//
// It is idempotent, and it must be: a resolve interrupted between clearing the
// projection and retiring the intent is completed by repeating it.
type ResolveGateRequest struct {
	TenantID   sessionwire.TenantID
	SessionID  sessionwire.SessionID
	LeaseEpoch uint64
	GateID     sessionwire.GateID
}

// ReadGatesRequest reads one session's open public gates.
//
// It has no cursor and no limit. The catalog holds at most
// MaxCatalogOpenGates gates for a session, so the whole answer is bounded by
// construction and is read from one record; a continuation would be a token
// that could never be issued. A future API that pages a larger open-gate set is
// what GatePage.OpenGateCount is reserved for.
type ReadGatesRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
}

// ListDueGatesRequest positions one bounded page of one control shard's gates
// whose deadline has passed. It is cross-tenant rather than tenant-scoped,
// because the due view is: see gateNamespace, and see shards.go for what a
// shard is and for what "service-only" does and does not mean here.
type ListDueGatesRequest struct {
	// Shard names the control shard to read and must be below the store's
	// ControlShards. A caller sweeps by visiting every shard round-robin.
	Shard int

	// DueAtOrBefore is the inclusive wall-clock bound and is the FIRST page's
	// query. A continuation carries its own bound, so a resumed request must
	// leave this zero; see dueGatePosition.
	DueAtOrBefore time.Time

	// Limit is the page's record ceiling. Zero means the store's configured
	// page size.
	Limit int

	// Cursor resumes a sweep of this shard from the position a previous page
	// ended at. It is opaque and is bound to this cursor kind and this shard.
	Cursor sessionwire.Cursor
}

// RemnantGateIntent is one deadline intent whose gate the session's durable
// record does not project as open, together with the revision a retirement
// names.
//
// It is reported rather than acted on, and rather than merely dropped, because
// this reader cannot decide the question a retirement has to answer: whether
// the open that wrote the intent crashed or is still in flight. Only elapsed
// time can, and the service that sweeps is the one holding the clock the
// retirement is evaluated against. See RetireGateDeadlineIntent.
//
// The revision travels with it so the retirement is a compare-and-swap onto the
// row this page actually saw. Without it a sweeper would have to re-read, and a
// re-read is a second decision point at which a gate could have been reopened.
type RemnantGateIntent struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	GateID    sessionwire.GateID
	Revision  uint64
}

// DueGate is one gate whose deadline has passed, together with the session it
// belongs to. Gate is the projection read back from that session's durable
// catalog record, not from the intent: the intent is an index into the
// projection and never a second copy of it.
type DueGate struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	Gate      sessionwire.GateProjection
}

// DueGatePage is one bounded page of due gates together with what producing it
// cost.
//
// Examined and Limit exist because Gates alone cannot be read. A page whose
// rows were all remnant intents returns no gates and no error, which is
// indistinguishable from a deployment where nothing is due. Examined == Limit
// with no gates says this page was full and none of it reported a live gate —
// which the continuation now makes transient rather than permanent, but which
// a caller still wants to see, because it is the difference between "nothing is
// due" and "this shard is accumulating rows that report nothing".
//
// Limit is the EFFECTIVE limit, after a zero request limit has been resolved to
// the store's page size, so the comparison is available to a caller that did
// not name one.
//
// Unreadable counts rows this reader could not decode, that disagreed with the
// filing they were found under, or that belong in a different shard. It is a
// DIFFERENT signal from a remnant, and the difference is what a caller can DO:
// a remnant is a row that was read, understood, and can be retired, and it is
// reported in Remnants for exactly that; an unreadable row is one nothing in
// this package can vouch for, so it is counted and left alone.
//
// Such a row is skipped rather than failing the page, and that is the strongest
// rule here rather than leniency. This view is ascending by deadline, an
// unreadable row's deadline is in the past and never changes, and nothing in
// this package rewrites it — so a reader that failed the page on one would
// switch gate expiry off for every tenant in the shard until someone repaired
// the row by hand. The continuation steps past it; failing would not.
type DueGatePage struct {
	Gates      []DueGate
	Remnants   []RemnantGateIntent
	Examined   int
	Unreadable int
	Limit      int

	// NextCursor resumes this shard's sweep after the position this page ended
	// at. It is empty when the view is exhausted.
	//
	// It is what turns the head-of-line hazard this reader used to have from
	// permanent into transient. A row that reports nothing — a remnant, or one
	// this reader could not read at all — is stepped over by the provider's own
	// continuation, which resumes from the frozen (due_at, stable_key,
	// ordering_scope) tuple the page ended on. So a blocking row is PASSED
	// rather than met again at the head of every page, and a gate behind it is
	// reached on the next page instead of never.
	//
	// A still-open gate past its deadline is deliberately NOT stepped over
	// permanently: it stays in the view, so every fresh pass reports it again,
	// because it is current due work that nothing has dealt with. It does not
	// block, because the continuation moves past it within a pass.
	NextCursor sessionwire.Cursor
}

// OpenGate records a gate's deadline and then projects it as publicly open.
//
// Every rejection below precedes both writes, and the two writes are ordered:
// see this file's header for why the intent is durable first.
func (s *Store) OpenGate(ctx context.Context, req OpenGateRequest) (CatalogEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return CatalogEntry{}, err
	}
	if req.LeaseEpoch == 0 {
		return CatalogEntry{}, catalogErr(CatalogErrorInvalid, "lease_epoch", nil)
	}
	// The gate is validated by the same canonicalizer the stored record uses,
	// on a one-element list, so a gate accepted here is a gate the record can
	// hold and this path cannot grow its own second opinion about what a valid
	// projection is. It also returns the canonical spelling, so the deadline
	// the intent records and the deadline the projection stores are the same
	// UTC instant.
	canonical, err := canonicalGates([]sessionwire.GateProjection{req.Gate})
	if err != nil {
		return CatalogEntry{}, err
	}
	gate := canonical[0]
	// A deadline outside the representable range would wrap rather than sort
	// late, which in a due view means firing at the wrong end of time.
	if !rankableTime(gate.Deadline) {
		return CatalogEntry{}, catalogErr(CatalogErrorInvalid, "gate.deadline", nil)
	}
	// The clock is read once, before any provider work, as every other
	// operation here reads it. The instant is the STORE'S, never the caller's:
	// a caller-supplied one could be placed far enough in the past to make its
	// own in-flight open immediately retireable, which is precisely the window
	// RecordedAt exists to hold open.
	intent, err := encodeGateIntent(gateIntent{
		TenantID:         req.TenantID,
		SessionID:        req.SessionID,
		GateID:           gate.GateID,
		OpenedEventID:    gate.OpenedEventID,
		OpenedJournalSeq: gate.OpenedJournalSeq,
		Deadline:         gate.Deadline,
		RecordedAt:       s.clock.Now(),
	})
	if err != nil {
		return CatalogEntry{}, err
	}

	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return CatalogEntry{}, err
	}
	defer release()

	current, err := s.readCatalogEntry(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return CatalogEntry{}, err
	}
	if err := hostEpochFence(current.Record, req.LeaseEpoch); err != nil {
		return CatalogEntry{}, err
	}
	// The gate must name an event the journal has durably committed. This is
	// the write-side half of "a reader validates its matching durable open
	// event": a page core would refuse to publish is refused before it is
	// stored.
	if gate.OpenedJournalSeq > current.Record.LastJournalSeq {
		return CatalogEntry{}, catalogErr(CatalogErrorSequence, "gate.opened_journal_seq", nil)
	}
	for _, open := range current.Record.OpenGates {
		if open.GateID == gate.GateID {
			// A repeat of the same open is the same intent and returns the
			// stored record unchanged; a DIFFERENT gate under an identity that
			// is already open is a conflict, not a replacement, because the
			// projection a reader already published would silently change
			// underneath it.
			if !sameOpenGate(open, gate) {
				return CatalogEntry{}, &CatalogError{Code: CatalogErrorConflict, Field: "gate_id", Revision: current.Revision}
			}
			// The projection is not evidence that the deadline is indexed.
			// UpdateCatalogHostState projects gates wholesale and deliberately
			// leaves intents alone, so this branch is reached for gates that
			// have no intent at all — and returning success without writing one
			// would make the operation whose entire job is making a deadline
			// durable silently do nothing.
			//
			// This is the one place an intent legitimately follows a
			// projection. It cannot produce the state this file's header rules
			// out — a gate open with no durable deadline — because that state
			// already exists when this branch is entered, and this is what
			// repairs it. commitGateIntent is idempotent by identity and still
			// refuses an identity held by a different or a resolved gate.
			if err := s.commitGateIntent(opCtx, scope, gate, intent); err != nil {
				return CatalogEntry{}, err
			}
			return current, nil
		}
		// Two gates opened by one event cannot be ordered against each other,
		// and a page that carries both violates the deterministic order its own
		// contract states. Refusing the second one keeps every stored
		// projection readable.
		if open.OpenedJournalSeq == gate.OpenedJournalSeq {
			return CatalogEntry{}, catalogErr(CatalogErrorSequence, "gate.opened_journal_seq", nil)
		}
	}
	// Refused here rather than by the record canonicalizer, which reports the
	// same overflow as an invalid record: this is a distinguishable outcome for
	// the caller, and refusing before the intent is written is what keeps a
	// rejected open from leaving a deadline behind.
	if len(current.Record.OpenGates)+1 > MaxCatalogOpenGates {
		return CatalogEntry{}, catalogErr(CatalogErrorTooLarge, "open_gates", nil)
	}

	if err := s.commitGateIntent(opCtx, scope, gate, intent); err != nil {
		return CatalogEntry{}, err
	}
	next := current.Record
	next.LeaseEpoch = req.LeaseEpoch
	next.OpenGates = append(append([]sessionwire.GateProjection(nil), current.Record.OpenGates...), gate)
	return s.writeCatalogRecord(opCtx, scope, next, current.Revision)
}

// ResolveGate clears one gate from the open projection and then retires its
// deadline intent.
//
// Retiring is a tombstone rather than an erasure: the intent's bytes remain
// readable for audit, its identity can never be reused to reopen the same gate,
// and a tombstone is unranked and not due, so it leaves the due pages by the
// provider's own contract rather than by a flag this package would have to
// maintain.
func (s *Store) ResolveGate(ctx context.Context, req ResolveGateRequest) (CatalogEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return CatalogEntry{}, err
	}
	if req.LeaseEpoch == 0 {
		return CatalogEntry{}, catalogErr(CatalogErrorInvalid, "lease_epoch", nil)
	}
	if err := req.GateID.Validate(); err != nil {
		return CatalogEntry{}, catalogErr(CatalogErrorInvalid, "gate_id", err)
	}

	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return CatalogEntry{}, err
	}
	defer release()

	current, err := s.readCatalogEntry(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return CatalogEntry{}, err
	}
	if err := hostEpochFence(current.Record, req.LeaseEpoch); err != nil {
		return CatalogEntry{}, err
	}

	entry := current
	if remaining, found := withoutGate(current.Record.OpenGates, req.GateID); found {
		next := current.Record
		next.LeaseEpoch = req.LeaseEpoch
		next.OpenGates = remaining
		if entry, err = s.writeCatalogRecord(opCtx, scope, next, current.Revision); err != nil {
			return CatalogEntry{}, err
		}
	}
	// Not conditional on the gate having been projected. A resolve interrupted
	// after the projection was cleared must still be able to retire the intent,
	// and that retry arrives with nothing left in the projection to find.
	if err := s.retireGateIntent(opCtx, scope, req.GateID); err != nil {
		return CatalogEntry{}, err
	}
	return entry, nil
}

// ReadGates returns one session's open public gates as core's bounded gate
// page, in the record's canonical (opened_seq, gate_id) order.
//
// It is one direct record read. The gates are already canonical when the record
// decodes, so nothing here re-sorts them: the comparator lives in
// canonicalGates and a second one here is precisely what would later disagree
// with the stored order.
func (s *Store) ReadGates(ctx context.Context, req ReadGatesRequest) (sessionwire.GatePage, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return sessionwire.GatePage{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return sessionwire.GatePage{}, err
	}
	defer release()

	entry, err := s.readCatalogEntry(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return sessionwire.GatePage{}, err
	}
	page := sessionwire.GatePage{
		JournalTip:    entry.Record.LastJournalSeq,
		OpenGateCount: uint64(len(entry.Record.OpenGates)),
		Gates:         append(make([]sessionwire.GateProjection, 0, len(entry.Record.OpenGates)), entry.Record.OpenGates...),
	}
	// Core owns what a public gate page means, so the page is verified by
	// asking core rather than by restating its rules here.
	//
	// Only one of those rules is reachable. Per-gate validity, uniqueness, and
	// the ascending order are all established by canonicalGates before a record
	// is ever stored; what no write path can establish for a record replaced
	// wholesale through UpdateCatalogHostState is that each gate's opening
	// event is at or below the record's own durable tip. That is a stored
	// record disagreeing with itself about which events exist, so it is
	// reported as a sequence failure rather than as a caller mistake.
	if err := page.Validate(); err != nil {
		return sessionwire.GatePage{}, catalogErr(CatalogErrorSequence, "gates", err)
	}
	return page, nil
}

// ListDueGates returns one bounded page of gates whose absolute deadline has
// passed, deployment-wide, each one validated against the durable open
// projection it names.
//
// It is a READ. It takes no action, cancels nothing, suspends nothing, and
// schedules nothing: what a Host does about an expired gate is gate
// continuation, which this task deliberately does not implement. It exists
// because the ordering contract in this file's header creates exactly one crash
// remnant — an intent whose open event never committed — and something has to
// be the reader that validates it away rather than acting on it.
//
// # It has a continuation, and that is what stops it being starved
//
// A remnant intent — one whose gate the session's durable record does not
// project as open — is REPORTED rather than acted on, and it is not retired
// here. It cannot be: OpenGate makes an intent durable before it commits the
// projection, so an intent with no matching open gate is indistinguishable, in
// its bytes, from a gate being opened right now. Retirement is a separate call
// that waits out a window no single open can outlive; see
// RetireGateDeadlineIntent and gateIntent.RecordedAt.
//
// Without a resume position that would be permanent head-of-line blocking: the
// view is ordered by deadline ASCENDING, a remnant's deadline is in the past
// and never changes, and a Host that re-projects its open gates wholesale
// through UpdateCatalogHostState — a normal path, documented as such above —
// produces one remnant per gate it drops. Once Limit of them accumulate ahead
// of the live gates, every page from the head consists entirely of them.
//
// NextCursor is the fix, and bounding the pass would not have been: a page
// budget bounds what one pass costs, but nothing about it moves the row that is
// blocking, so the blocked rows stay blocked. The continuation steps PAST a row
// that reported nothing, so the sweep reaches what is behind it on the next
// page. A caller that pages a shard to exhaustion sees every due row in it.
//
// The page still reports Examined and Unreadable, because they answer a
// different question: whether a full page reported nothing, and whether rows
// were skipped because they could not be read at all. See DueGatePage.
func (s *Store) ListDueGates(ctx context.Context, req ListDueGatesRequest) (DueGatePage, error) {
	shard, err := s.validateShard(req.Shard, catalogInvalid)
	if err != nil {
		return DueGatePage{}, err
	}
	limit, ok := s.pageLimit(req.Limit)
	if !ok {
		return DueGatePage{}, catalogErr(CatalogErrorInvalid, "limit", nil)
	}
	bound, after, err := s.dueGatePosition(shard, req)
	if err != nil {
		return DueGatePage{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return DueGatePage{}, err
	}
	defer release()

	page, err := s.backend.OrderedIndex.ListDue(
		opCtx, shardNamespace(gateNamespace, shard), bound, after, limit)
	if err != nil {
		return DueGatePage{}, classifyCatalogOrderedError(err, "due_gates")
	}
	due := DueGatePage{
		Gates:    make([]DueGate, 0, len(page.Records)),
		Examined: len(page.Records),
		Limit:    limit,
	}
	// Rows are ordered by deadline, so one session's gates do NOT arrive
	// adjacently: they interleave with every other session's. What the map
	// delivers is that a session's record is read at most once per page
	// whatever order its rows arrive in, which is why it is a map and not a
	// one-entry last-seen cache — that would reintroduce a re-read per row.
	//
	// It retains a scope and the session's open-gate projections rather than
	// whole catalog records, so a page holds at most its own row count times
	// MaxCatalogOpenGates projections instead of that many 256 KiB records.
	sessions := make(map[dueSessionKey]dueSession, len(page.Records))
	for _, stored := range page.Records {
		intent, err := gateIntentFor(stored)
		if err != nil {
			due.Unreadable++
			continue
		}
		key := dueSessionKey{tenant: intent.TenantID, session: intent.SessionID}
		session, cached := sessions[key]
		if !cached {
			// Deriving a scope is pure, so a row's filing is settled before any
			// provider work is done on its behalf: a misfiled row costs no
			// round trip.
			if session.scope, err = s.deriveSessionScope(intent.TenantID, intent.SessionID); err != nil {
				due.Unreadable++
				continue
			}
		}
		// THE SHARD IS PART OF THE FILING. This reader did not name the row; it
		// learned the row's identity from the row, and the shard is a function
		// of that identity — so a row whose bytes hash elsewhere is filed where
		// it does not belong. Reporting it would let one shard's sweep answer
		// for a shard it was not asked about while the shard that owns the row
		// never sees it, and reporting it as a REMNANT would aim a retirement
		// at a row a correct sweep is still responsible for.
		if session.scope.ControlShard != shard {
			due.Unreadable++
			continue
		}
		// The intent must be FILED as its own bytes say it should be. It is
		// checked for every row rather than once per session: what is being
		// verified belongs to the ROW, and a cached session would otherwise let
		// a misfiled intent through behind a well-filed one.
		if err := verifyGateIntentFiling(stored, intent, session.scope); err != nil {
			due.Unreadable++
			continue
		}
		if !cached {
			entry, err := s.readCatalogEntry(opCtx, session.scope, intent.TenantID, intent.SessionID)
			if err != nil {
				// A session that has no durable existence cannot have a
				// durably open gate, so its remnant intents are reported as
				// remnants rather than failing the whole page. That is a narrow
				// list on purpose: an absent record, a tombstoned one, and an
				// unbound session witness each mean "there is no such session",
				// while every other failure — a collision, a corrupt record, a
				// provider error — is a reason to stop rather than to conclude
				// anything about this gate.
				if !noSuchSession(err) {
					if !rowLocalCatalogFailure(err) {
						// Reported WITHOUT a row position, deliberately. This
						// branch has just decided the failure is not about this
						// row — it is a provider fault or an ambiguous outcome
						// — so labelling it "due_gates[3]" would point an
						// operator at a row that is very likely fine. Every
						// failure that IS about a row is counted above and
						// never returned at all, which is what left this the
						// only path out and made the label wrong.
						return DueGatePage{}, err
					}
					// The session's own record is unreadable, which is a fact
					// about THIS row's session rather than about the view. It
					// is counted and stepped over for the reason Unreadable
					// documents; a provider failure is not, because it says
					// nothing about any row and continuing would turn an
					// outage into a page of silent zeroes.
					due.Unreadable++
					continue
				}
			} else {
				session.exists = true
				session.gates = entry.Record.OpenGates
			}
			sessions[key] = session
		}
		// The validation this reader exists for: an intent is only reported as
		// a due GATE when the session's durable record really does project that
		// gate as open, with the same opening event and the same deadline. An
		// intent written for an open that never committed matches nothing.
		matched := false
		for _, gate := range session.gates {
			if intent.matches(gate) {
				due.Gates = append(due.Gates, DueGate{TenantID: intent.TenantID, SessionID: intent.SessionID, Gate: gate})
				matched = true
				break
			}
		}
		if !matched {
			due.Remnants = append(due.Remnants, RemnantGateIntent{
				TenantID: intent.TenantID, SessionID: intent.SessionID,
				GateID: intent.GateID, Revision: stored.Revision})
		}
	}
	if page.NextCursor != "" {
		if due.NextCursor, err = s.encodeDueGateCursor(shard, bound, page.NextCursor); err != nil {
			return DueGatePage{}, err
		}
	}
	return due, nil
}

// The due gates continuation. Its payload is the due bound this sweep is
// querying at, followed by the provider's own due token carried verbatim.
//
// THE BOUND IS IN THE TOKEN because the ordered index binds a due cursor to the
// exact bound that issued it, so a resumed call that recomputed the bound would
// present a token for a different query and be refused. Carrying it costs
// nothing in safety: ListDueGates WRITES NOTHING, and every row it reports is
// held to its own stored filing and to the session's durable projection before
// it is reported at all — so a caller presenting a bound this store never
// issued can at worst make the reader look at rows that are not due.
//
// The retirement a remnant enables is a SEPARATE call that revalidates from
// scratch against its own clock reading and its own read of the projection, so
// no part of it rests on the bound this token carries.
const (
	dueGateCursorMagic        = "LRDG"
	dueGateCursorVersion byte = 1

	dueGateBoundBytes = 8

	// The ceiling is enforced on ISSUE as well as on presentation, so a token
	// this store hands out is always one it will accept back — and a
	// continuation it could not reissue is a sweep that silently reverts to
	// making no progress at the head of the view.
	maxDueGateCursorBytes   = 4 << 10
	maxDueGateCursorPayload = maxDueGateCursorBytes - cursorPayloadAt
)

// dueGateCursorScope binds a continuation to this cursor KIND and to the SHARD
// it was issued for, and to nothing else; see dueCommandCursorScope for why the
// shard is in the scope rather than in the payload, and cursor.go for why the
// scope is a binding tag and not a MAC.
func (s *Store) dueGateCursorScope(shard uint32) [cursorScopeBytes]byte {
	return s.keys.digest(digestFrame(
		"looprig/sessionstore/duegate/cursor/v1", binary.BigEndian.AppendUint32(nil, shard)))
}

func (s *Store) encodeDueGateCursor(shard uint32, bound int64, after storage.DueCursor) (sessionwire.Cursor, error) {
	payload := make([]byte, dueGateBoundBytes, dueGateBoundBytes+len(after))
	binary.BigEndian.PutUint64(payload, uint64(bound)) // #nosec G115 -- a signed bound round-trips through the same width
	payload = append(payload, after...)
	if len(payload) > maxDueGateCursorPayload {
		return "", catalogErr(CatalogErrorBackend, "next_cursor", nil)
	}
	return sessionwire.Cursor(encodeCursorEnvelope(
		dueGateCursorMagic, dueGateCursorVersion, s.dueGateCursorScope(shard), payload)), nil
}

// decodeDueGateCursor unwraps a continuation this store issued for this shard.
// One this reader issued always carries at least one provider byte beyond the
// bound, because an exhausted view returns no cursor at all.
func (s *Store) decodeDueGateCursor(shard uint32, cursor sessionwire.Cursor) (int64, storage.DueCursor, error) {
	payload, ok := decodeCursorEnvelope(
		dueGateCursorMagic, dueGateCursorVersion, s.dueGateCursorScope(shard),
		string(cursor), dueGateBoundBytes+1, maxDueGateCursorPayload)
	if !ok {
		return 0, "", catalogErr(CatalogErrorCursor, "cursor", nil)
	}
	bound := int64(binary.BigEndian.Uint64(payload[:dueGateBoundBytes])) // #nosec G115 -- the inverse of the encode above
	return bound, storage.DueCursor(payload[dueGateBoundBytes:]), nil
}

// dueGatePosition resolves one request to the (bound, provider position) the
// page is read at, refusing a request that states the bound twice.
//
// A request may not carry both a continuation and a bound. Preferring either
// silently is how a resumed sweep starts querying a bound it was never bound to
// — the provider would refuse the token, and the sweep would restart at the
// head of the view every time, which looks like liveness and is starvation.
func (s *Store) dueGatePosition(shard uint32, req ListDueGatesRequest) (int64, storage.DueCursor, error) {
	if req.Cursor == "" {
		// rankableTime already refuses the zero Time, which is earlier than
		// every representable instant; a separate IsZero check would be a
		// second statement of one rule.
		if !rankableTime(req.DueAtOrBefore) {
			return 0, "", catalogErr(CatalogErrorInvalid, "due_at_or_before", nil)
		}
		return req.DueAtOrBefore.UnixMilli(), "", nil
	}
	if !req.DueAtOrBefore.IsZero() {
		return 0, "", catalogErr(CatalogErrorInvalid, "due_at_or_before", nil)
	}
	return s.decodeDueGateCursor(shard, req.Cursor)
}

// verifyGateIntentFiling holds every provider-supplied key component of one
// stored intent to what the record's own bytes say it should be.
//
// A due page is provider-driven: unlike a direct get, the reader does not name
// the row it is about to read, it learns the row's identity FROM the row. So
// each component the provider chose has to be reconciled with the record it
// files, and the components are enumerated here rather than checked wherever
// each one happens to be used, because the interesting failure is the one
// nobody thought to check.
//
// The enumeration, and why each entry is or is not here:
//
//   - StableKey — the gate id. Checked, in gateIntentFor, where the value is
//     decoded; a provider that hashes the key stores the original for exactly
//     this comparison.
//   - OrderingScope, RankingScope and Due — the triad every session-scoped
//     record files identically, checked through checkFiledScope, which states
//     the rule and why each of the three is worth stating. The due state is the
//     reason this function exists at all: a due page selects rows BY that
//     field, so a reader that trusted it would report a gate as expired because
//     the index said so while the record's own deadline was still a day away.
//   - Deleted is not record-derived, but it needs no counterpart: ListDue
//     returns current nondeleted records by contract, and a violation of that
//     one means reporting a RETIRED gate as due, so it is asserted directly.
//   - Namespace is not record-derived either: it echoes the query this reader
//     itself issued, and the bytes carry no counterpart to compare it against.
//   - Rank is written as unranked and nothing ranks or reads gate intents, so a
//     check would guard a view with no consumer.
//   - Revision and Order are provider state with no meaning in the record.
func verifyGateIntentFiling(stored storage.OrderedRecord, intent gateIntent, scope sessionScope) error {
	if stored.Deleted {
		return catalogErr(CatalogErrorDeleted, "gate_intent", nil)
	}
	return checkFiledScope(stored, scope.SessionNamespace, gateDue(intent.Deadline), catalogIdentity)
}

// dueSessionKey is the identity a due page caches a catalog record under. It is
// the full (tenant, session) pair rather than the session alone: a session id is
// unique WITHIN a tenant, so caching by session id would let one tenant's record
// answer for another tenant's identically-named session.
type dueSessionKey struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
}

// dueSession is what a due page remembers about one session it has already
// resolved: the scope every row filed under that session must match, whether
// the session durably exists, and its open-gate projections.
//
// It holds the projections rather than the CatalogEntry so a page retains the
// gates it actually matches against instead of whole catalog records, whose
// bound is three orders of magnitude larger. Caching the "does not exist" case
// matters as much as caching a record: a page full of remnant intents for one
// deleted session would otherwise re-read it once per row.
type dueSession struct {
	scope  sessionScope
	exists bool
	gates  []sessionwire.GateProjection
}

// noSuchSession reports whether err means the named session has no durable
// existence at all, as opposed to being unreadable for some other reason. It is
// stated once because the due reader's decision to DROP a row rather than fail
// the page rests entirely on it, and a looser spelling of it would turn a
// provider outage into silently empty results.
func noSuchSession(err error) bool {
	var catalog *CatalogError
	if errors.As(err, &catalog) {
		return catalog.Code == CatalogErrorNotFound || catalog.Code == CatalogErrorDeleted
	}
	var keyspace *KeyspaceError
	return errors.As(err, &keyspace) && keyspace.Code == KeyspaceBindingNotFound
}

// commitGateIntent makes one gate's deadline durable. Create is idempotent by
// identity, so a retry of an interrupted open finds its own intent; what it
// must not do is silently adopt a DIFFERENT gate's intent under the same
// identity, or reopen a gate that has already been resolved.
func (s *Store) commitGateIntent(
	ctx context.Context,
	scope sessionScope,
	gate sessionwire.GateProjection,
	value []byte,
) error {
	stored, created, err := s.backend.OrderedIndex.Create(
		ctx, gateIntentID(scope, gate.GateID), scope.SessionNamespace, value, storage.Rank{}, gateDue(gate.Deadline))
	if err != nil {
		return classifyCatalogOrderedError(err, "gate_intent")
	}
	if created {
		return nil
	}
	if stored.Deleted {
		return catalogErr(CatalogErrorDeleted, "gate_intent", nil)
	}
	existing, err := gateIntentFor(stored)
	if err != nil {
		return err
	}
	if !existing.matches(gate) {
		return catalogErr(CatalogErrorConflict, "gate_intent", nil)
	}
	return nil
}

// retireGateIntent tombstones one gate's deadline intent. An intent that never
// existed or is already a tombstone is already retired: both make this
// idempotent, which is what lets an interrupted resolve be completed by a
// repeat.
func (s *Store) retireGateIntent(ctx context.Context, scope sessionScope, gate sessionwire.GateID) error {
	id := gateIntentID(scope, gate)
	stored, err := s.backend.OrderedIndex.Get(ctx, id)
	if err != nil {
		classified := classifyCatalogOrderedError(err, "gate_intent")
		var catalog *CatalogError
		if errors.As(classified, &catalog) && catalog.Code == CatalogErrorNotFound {
			return nil
		}
		return classified
	}
	if stored.Deleted {
		return nil
	}
	if _, err := s.backend.OrderedIndex.Delete(ctx, id, stored.Revision); err != nil {
		return classifyCatalogOrderedError(err, "gate_intent")
	}
	return nil
}

// gateIntentID names the one ordered record per open gate. The ordering scope
// is the session's physical namespace, so one session's gates share an order
// scope and are adjacent in a due page, and the stable key is the raw GateID:
// an opaque provider-verified value, not a name.
func gateIntentID(scope sessionScope, gate sessionwire.GateID) storage.OrderedID {
	return storage.OrderedID{
		Namespace:     shardNamespace(gateNamespace, scope.ControlShard),
		OrderingScope: scope.SessionNamespace,
		StableKey:     storage.StableKey(gate),
	}
}

// gateDue is the single definition of a deadline's due state. Every writer and
// every comparison goes through it, so no path can disagree about how an
// instant becomes an absolute due time.
//
// UnixMilli truncates toward the epoch, so a deadline with sub-millisecond
// precision becomes due at most one millisecond early. The due view is a
// wall-clock index whose consumers reconcile by sweeping, not a scheduler with
// a tighter promise, so that is the right end to lose precision at.
func gateDue(deadline time.Time) storage.Due {
	return storage.Due{State: storage.DueAt, UnixMillis: deadline.UnixMilli()}
}

// sameOpenGate reports whether two projections are the same open gate: the same
// identity opened by the same durable event with the same deadline. Prompt text
// is deliberately not compared — a Host may re-render a prompt — but nothing
// that a reader's position depends on may differ.
func sameOpenGate(a, b sessionwire.GateProjection) bool {
	return a.GateID == b.GateID &&
		a.OpenedEventID == b.OpenedEventID &&
		a.OpenedJournalSeq == b.OpenedJournalSeq &&
		a.Deadline.Equal(b.Deadline)
}

// withoutGate returns the projection with one gate removed, reporting whether
// it was there. The result keeps the canonical order because a subsequence of
// an ordered list is ordered.
func withoutGate(open []sessionwire.GateProjection, gate sessionwire.GateID) ([]sessionwire.GateProjection, bool) {
	remaining := make([]sessionwire.GateProjection, 0, len(open))
	found := false
	for _, candidate := range open {
		if candidate.GateID == gate {
			found = true
			continue
		}
		remaining = append(remaining, candidate)
	}
	return remaining, found
}

// gateIntent is the durable deadline index entry for one open gate. It holds
// identity, the opening event, and the deadline — never a prompt, an answer, or
// anything else a reader could mistake for the gate itself.
type gateIntent struct {
	TenantID         sessionwire.TenantID
	SessionID        sessionwire.SessionID
	GateID           sessionwire.GateID
	OpenedEventID    sessionwire.EventID
	OpenedJournalSeq uint64
	Deadline         time.Time

	// RecordedAt is the STORE'S OWN clock reading at the moment this intent
	// first became durable. It is not part of the gate and matches nothing in
	// the projection; it exists for exactly one consumer, and it is the only
	// thing that can serve that consumer.
	//
	// WHY THE RECORD HAS TO CARRY IT. OpenGate makes the intent durable BEFORE
	// it commits the open projection, deliberately, so that no gate is ever
	// publicly open without a deadline. The price is that "an intent with no
	// matching open gate" has two causes that are identical in every stored
	// byte: an open that crashed, and an open that is happening right now. A
	// retirement that could not tell them apart would eventually tombstone a
	// live open's intent, leaving the gate open with no deadline and an
	// identity that can never be reused — the one state this file's header
	// promises is never produced.
	//
	// Nothing derivable from the two records distinguishes them. The deadline
	// is caller-supplied and may already be past; the opening sequence is at or
	// below the tip before the intent is written, so it is equally true in both
	// cases. ELAPSED TIME IS THE ONLY DISCRIMINATOR, and an elapsed time needs
	// a start, which is this member. See MinGateIntentRemnantAge for the window
	// and for what a skewed clock costs.
	//
	// It is stamped ONCE, when the intent is created. commitGateIntent is
	// idempotent by identity and compares with matches, which ignores this
	// member, so a repeat of an interrupted open finds the ORIGINAL instant
	// rather than restarting the window — which is what makes the window an age
	// rather than a rate limit on retries.
	RecordedAt time.Time
}

// matches reports whether a projection is the open gate this intent indexes.
func (i gateIntent) matches(gate sessionwire.GateProjection) bool {
	return i.GateID == gate.GateID &&
		i.OpenedEventID == gate.OpenedEventID &&
		i.OpenedJournalSeq == gate.OpenedJournalSeq &&
		i.Deadline.Equal(gate.Deadline)
}

type gateIntentWire struct {
	RecordVersion    uint8                 `json:"record_version"`
	TenantID         sessionwire.TenantID  `json:"tenant_id"`
	SessionID        sessionwire.SessionID `json:"session_id"`
	GateID           sessionwire.GateID    `json:"gate_id"`
	OpenedEventID    sessionwire.EventID   `json:"opened_event_id"`
	OpenedJournalSeq uint64                `json:"opened_journal_seq"`
	Deadline         time.Time             `json:"deadline"`
	RecordedAt       time.Time             `json:"recorded_at"`
}

// encodeGateIntent validates and encodes one deadline intent.
func encodeGateIntent(intent gateIntent) ([]byte, error) {
	intent, err := canonicalGateIntent(intent)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(gateIntentWire{
		RecordVersion:    GateIntentRecordVersion,
		TenantID:         intent.TenantID,
		SessionID:        intent.SessionID,
		GateID:           intent.GateID,
		OpenedEventID:    intent.OpenedEventID,
		OpenedJournalSeq: intent.OpenedJournalSeq,
		Deadline:         intent.Deadline,
		RecordedAt:       intent.RecordedAt,
	})
	if err != nil {
		return nil, catalogErr(CatalogErrorInvalid, "gate_intent", err)
	}
	return encoded, nil
}

// decodeGateIntent strictly decodes one stored deadline intent and re-validates
// it, so an intent corrupted in place cannot be handed to a reader.
func decodeGateIntent(value []byte) (gateIntent, error) {
	wire, err := decodeVersionedRecord[gateIntentWire](
		value, MaxGateIntentBytes, GateIntentRecordVersion,
		versionedRecordFields{Record: "gate_intent", Version: "gate_intent.record_version"}, catalogRecordFailure)
	if err != nil {
		return gateIntent{}, err
	}
	return canonicalGateIntent(gateIntent{
		TenantID:         wire.TenantID,
		SessionID:        wire.SessionID,
		GateID:           wire.GateID,
		OpenedEventID:    wire.OpenedEventID,
		OpenedJournalSeq: wire.OpenedJournalSeq,
		Deadline:         wire.Deadline,
		RecordedAt:       wire.RecordedAt,
	})
}

// canonicalGateIntent validates an intent and returns its one canonical
// spelling. Encoding and decoding both end here, so an intent read back is
// byte-identical to the intent written.
func canonicalGateIntent(intent gateIntent) (gateIntent, error) {
	if err := intent.TenantID.Validate(); err != nil {
		return gateIntent{}, catalogErr(CatalogErrorInvalid, "gate_intent.tenant_id", err)
	}
	if err := intent.SessionID.Validate(); err != nil {
		return gateIntent{}, catalogErr(CatalogErrorInvalid, "gate_intent.session_id", err)
	}
	if err := intent.GateID.Validate(); err != nil {
		return gateIntent{}, catalogErr(CatalogErrorInvalid, "gate_intent.gate_id", err)
	}
	if err := intent.OpenedEventID.Validate(); err != nil {
		return gateIntent{}, catalogErr(CatalogErrorInvalid, "gate_intent.opened_event_id", err)
	}
	// Zero is not a journal sequence, and an intent that named one would match
	// no projection core would accept.
	if intent.OpenedJournalSeq == 0 {
		return gateIntent{}, catalogErr(CatalogErrorInvalid, "gate_intent.opened_journal_seq", nil)
	}
	// rankableTime already refuses the zero Time; see ListDueGates.
	if !rankableTime(intent.Deadline) {
		return gateIntent{}, catalogErr(CatalogErrorInvalid, "gate_intent.deadline", nil)
	}
	// The recorded instant is bounded but NOT compared against the deadline.
	// The two are independent facts — when this store wrote the row, and when
	// the gate expires — and a legitimate intent may be recorded after its own
	// deadline, because OpenGate accepts a deadline that has already passed.
	// Relating them here would make those intents undecodable.
	if !rankableTime(intent.RecordedAt) {
		return gateIntent{}, catalogErr(CatalogErrorInvalid, "gate_intent.recorded_at", nil)
	}
	intent.Deadline = intent.Deadline.UTC()
	intent.RecordedAt = intent.RecordedAt.UTC()
	return intent, nil
}

// gateIntentFor decodes one stored ordered record and holds the decoded intent
// to the identity the provider filed it under. A provider that hashes the
// stable key stores the original bytes for exactly this verification.
func gateIntentFor(stored storage.OrderedRecord) (gateIntent, error) {
	intent, err := decodeGateIntent(stored.Value)
	if err != nil {
		return gateIntent{}, err
	}
	if storage.StableKey(intent.GateID) != stored.ID.StableKey {
		return gateIntent{}, catalogErr(CatalogErrorIdentity, "gate_intent", nil)
	}
	return intent, nil
}

// rowLocalCatalogFailure reports whether a catalog failure is a fact about ONE
// record rather than about the store or the provider.
//
// The distinction is what lets a bounded reader step over a row without turning
// an outage into a page of silent zeroes. A record that cannot be decoded, that
// disagrees with the identity it was filed under, or whose session's collision
// witness has been taken by another identity is a durable fact about that row:
// retrying reports it again, no other row is implicated, and failing the whole
// page on it disables the reader for every caller. A backend error, an
// ambiguous mutation, or a caller mistake is not about the row at all.
//
// It is deliberately a CLOSED list of the row-local codes rather than an open
// list of the others, so a code added later is treated as page-failing until
// someone decides it is row-local. The safe default is to stop.
func rowLocalCatalogFailure(err error) bool {
	var keyspace *KeyspaceError
	if errors.As(err, &keyspace) {
		return keyspace.Code == KeyspaceHashCollision
	}
	var failure *CatalogError
	if !errors.As(err, &failure) {
		return false
	}
	switch failure.Code {
	case CatalogErrorIdentity, CatalogErrorMalformed, CatalogErrorVersion, CatalogErrorTooLarge:
		return true
	default:
		return false
	}
}
