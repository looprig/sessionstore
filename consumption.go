package sessionstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// A SESSION'S COMMANDS ARE CONSUMED IN THE ORDER THEY WERE ACCEPTED, AND HOW
// FAR A CONSUMER HAS GOT IS DURABLE. This file is those two things and nothing
// else: one per-session ascending read of the disposition inbox, and one small
// permanent row per session recording a position in it.
//
// WHY THE DISPOSITION FAMILY, WHICH IS THE FIRST QUESTION TO ANSWER. There are
// two command families here, and they are mutually exclusive per session by
// construction: a session's protocol mode is a create-only immutable pin
// (session_binding.go), the legacy inbox binds ProtocolModeLegacy on admission,
// and the disposition inbox binds ProtocolModeDisposition. A CONSUMER cannot
// choose — AcquireResidency (residency.go) REFUSES any session whose catalog
// binding is not ProtocolModeDisposition, so every session a Host can hold a
// residency over, and therefore every session anything can consume commands
// for, is disposition-bound. A per-session listing and cursor over the LEGACY
// inbox would be correct code that no consumer could ever call: the cursor's
// first write would try to pin legacy on a scope already pinned disposition and
// be refused with a catalog conflict, permanently and unfixably by retry. That
// is not a hypothetical — it is what the first version of this file did, and it
// is why both operations here take the disposition family's authority check.
//
// WHY THE LISTING IS NOT THE DUE VIEW, which is the second. ListDueDispositionCommands
// answers "what in this SHARD needs attention by this instant". It is
// cross-session by its request type, it is ordered by a deadline, and a settled
// command leaves it altogether because dispositionInboxDue files a terminal
// record NOT DUE. Every one of those is right for a reconciler and wrong for a
// consumer: a consumer reads ONE session, in the order the commands were
// accepted, and must still see a command it has already settled — that is what
// makes a cursor meaningful. The two are therefore two provider views of the
// same rows rather than one view with a filter over it, and neither is
// derivable from the other. The rows, the namespace and the filing checks are
// shared; the view is not.
//
// WHY THE CURSOR IS NOT A PAGE TOKEN. Every sessionwire.Cursor this package
// issues is a position inside one query's result, valid until the query ends
// and durable nowhere. This cursor is the opposite: it outlives every query,
// every process and every residency, and it is the thing a Host reads at
// startup to learn where the last Host stopped. They share a word and nothing
// else.

const (
	// dispositionCursorNamespace holds one consumption cursor per session.
	//
	// It is UNSHARDED, unlike the inbox it points into, and the reason is the
	// one sessionPointerDue gives: a shard exists to bound a DUE SWEEP, these
	// rows are never due, never ranked and never listed, and they are only ever
	// read by name. A sharded namespace would buy a partition for a sweep that
	// does not exist and cannot be added without answering what removes a row
	// from it.
	//
	// IT NAMES THE DISPOSITION FAMILY IN ITS OWN NAME, deliberately. An
	// acceptance order from the legacy inbox and one from the disposition inbox
	// are positions in two different provider streams with unrelated numbering,
	// so a row that recorded one and was later read as the other would be a
	// silently wrong position rather than a decode failure. Should a legacy
	// cursor ever be owed, it gets its own namespace and the two can never be
	// confused for each other.
	dispositionCursorNamespace = "sessionstore/disposition-command-cursors"

	// dispositionCursorStableKey separates cursor ROLES within one session, of
	// which there is currently one. It is a package constant rather than the
	// session identity for the reason sessionPointerID's stable key is the
	// pointer kind: the ordering scope already names the session, so the stable
	// key's whole job is to keep this session's roles apart, and no
	// caller-supplied text reaches this name.
	dispositionCursorStableKey = "disposition-command-consumption"

	// DispositionCommandCursorRecordVersion is the independent version of the
	// stored cursor. A reader fails closed on any other version rather than
	// guessing which members a future encoder meant.
	DispositionCommandCursorRecordVersion uint8 = 1

	// MaxDispositionCommandCursorRecordBytes bounds an encoded cursor. The
	// record is two identities, two integers and an instant, so this is
	// generous by more than an order of magnitude; what it is for is that an
	// oversized record is refused HERE rather than by the provider, so a record
	// this package accepted can always be rewritten.
	MaxDispositionCommandCursorRecordBytes = 4 << 10
)

// Stated as an unsigned constant for the reason the other records state theirs:
// prose cannot enforce the relationship between this bound and the provider's.
const _ = uint(storage.MaxOrderedValueBytes - MaxDispositionCommandCursorRecordBytes)

// ---------------------------------------------------------------------------
// The per-session ordered listing
// ---------------------------------------------------------------------------

// ListSessionDispositionCommandsRequest positions one bounded page of ONE
// session's disposition inbox in immutable acceptance order.
//
// AfterOrder is an EXCLUSIVE lower bound and is the caller's. Zero starts at
// the head of the session's stream, which is the provider's own spelling and is
// unambiguous because no provider allocates order zero. A caller resumes by
// passing the previous page's NextAfterOrder, or its own durable cursor.
//
// THE BOUND IS THE CALLER'S AND THE ORDERING IS THE STORE'S. A consumer that
// sorted a page for itself would be inferring an order rather than reading one,
// and the order it inferred would be its own opinion about rows the provider
// had already ranked. Nothing in this request selects an order, a direction or
// a sort key, because there is exactly one and it is the provider's immutable
// acceptance order.
//
// There is no Cursor member, deliberately. A page token would be a second way
// to say the one thing AfterOrder says, and the two would have to be reconciled
// on every continuation — which is the failure ListDueDispositionCommandsRequest
// documents at length for a query whose bound genuinely cannot be restated.
// This one's can: the bound IS a row's order, and a caller that has the row has
// the bound.
type ListSessionDispositionCommandsRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	AfterOrder uint64
	Limit      int
}

// SessionDispositionCommandPage is one bounded ascending page of one session's
// disposition inbox.
//
// Commands are in STRICTLY increasing AcceptedOrder, every one of them strictly
// above the request's bound. They are the SAME DispositionInboxEntry values a
// named read of each command returns, held to the same filing checks and to the
// same catalog binding — there is no weaker sweep-shaped variant, because a
// consumer acts on these rows and every write it then makes is a
// compare-and-swap against the revision reported here.
//
// LIMIT is the EFFECTIVE limit after a zero request limit has been resolved to
// the store's page size, so a caller that named no limit can still tell a full
// page from a short one.
//
// NEXTAFTERORDER is the last row's order, and is zero exactly when the page is
// empty. An empty page means the stream is exhausted at this bound: unlike a
// due page, nothing here is skipped, so there is no "empty but not finished"
// state to distinguish. A caller that receives one keeps the bound it asked
// with.
//
// There is no Unreadable count, and its absence is the contract rather than an
// omission — see ListSessionDispositionCommands.
type SessionDispositionCommandPage struct {
	Commands       []DispositionInboxEntry
	Limit          int
	NextAfterOrder uint64
}

// ListSessionDispositionCommands returns one bounded page of one session's
// disposition commands in ascending immutable acceptance order, strictly after
// the caller's bound.
//
// It is a READ. It claims nothing, settles nothing and writes nothing; it does
// not move the consumption cursor, which is a separate durable decision a
// caller makes with SaveDispositionCommandCursor after it has acted.
//
// ITS COST IS THE PAGE PLUS ONE CATALOG READ. One ListOrdered against one
// (namespace, ordering scope) pair returns at most Limit rows, and the catalog
// is read ONCE for the whole page rather than once per row — which is the one
// place this listing is cheaper than ListDueDispositionCommands rather than
// merely different, and it is cheaper for a reason rather than by luck: a due
// sweep is handed rows from many sessions and must ask the binding question per
// row, while every row here belongs to the one session the caller named.
// Nothing here reads the journal, an object or any other session. The
// deployment's tenant count, the shard's population and the session's settled
// history above the bound do not appear in the cost. The session's settled
// history BELOW the bound does not either, which is the point of the bound.
//
// IT VERIFIES THE SESSION'S WITNESSES AND ITS CATALOG BINDING, unlike
// ListDueDispositionCommands' per-row skipping, and the difference is the same
// one that decides every other named read in this package: a due sweep is
// HANDED its rows by the provider and derives no name, while this call DERIVES
// the ordering scope from caller-supplied identities and must not trust a
// derived name on its own. dispositionCatalog makes both checks — readCatalogEntry
// verifies the collision witnesses, and the binding must be
// ProtocolModeDisposition — before the provider is asked for a single row.
//
// IT FAILS CLOSED ON A ROW IT CANNOT VOUCH FOR, which is the deliberate
// divergence from the due sweep, and it is worth stating as a decision rather
// than discovering as a difference. The due view counts an unreadable row and
// steps over it, because failing would switch reconciliation off for every
// tenant in the shard and because its continuation passes the row rather than
// meeting it again. Neither argument holds here. This is a CONSUMPTION stream:
// a consumer that was handed a page with a row quietly missing would act on
// what it received and then advance its durable cursor past the row, so the
// command would never be applied and nothing would ever look at it again. And
// the blast radius of failing is one session rather than one shard. So a row
// that does not decode, that disagrees with its filing, or that carries another
// session's identities or another binding fails the whole page, with the same
// typed error a named read of that command would return. A PROVIDER TOMBSTONE
// fails it too, and for the strongest reason of the set: nothing in this
// package deletes a disposition command row, so a tombstone in this stream is a
// record destroyed by something outside it, and a consumer stepping over one
// would step over a command whose own bytes it can no longer see.
//
// WHAT FAILING CLOSED COSTS, stated plainly: one unreadable row stops that
// session's consumer at that row, permanently. There is no skip, no quarantine
// and no reporting channel — this package has neither a logger nor anywhere in
// this page to record a skip, which is the same limitation
// DispositionDueCommandPage states about locating an unreadable row. AND THERE
// IS NO REPAIR OPERATION EITHER: this package offers no way to rewrite a
// corrupt command row, so "until the row is repaired" would name a remedy that
// does not exist here. What an operator has is the failure's field, the
// session's identities, and the fact that the bound the page was read at
// narrows the row to the first one above it; the repair itself is a
// provider-level act outside this module.
func (s *Store) ListSessionDispositionCommands(
	ctx context.Context, req ListSessionDispositionCommandsRequest,
) (SessionDispositionCommandPage, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return SessionDispositionCommandPage{}, err
	}
	limit, ok := s.pageLimit(req.Limit)
	if !ok {
		return SessionDispositionCommandPage{}, inboxInvalid("limit", nil)
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return SessionDispositionCommandPage{}, err
	}
	defer release()
	// The binding is the session's authority AND its witness verification, in
	// one read, and it is read BEFORE any inbox row: a page of commands from a
	// session whose catalog says another protocol is a page this store must not
	// vouch for at all, and finding that out per row would be both slower and
	// weaker.
	binding, err := s.dispositionCatalog(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return SessionDispositionCommandPage{}, err
	}
	provider, err := s.backend.OrderedIndex.ListOrdered(
		opCtx, shardNamespace(dispositionInboxNamespace, scope.ControlShard),
		scope.SessionNamespace, req.AfterOrder, limit)
	if err != nil {
		return SessionDispositionCommandPage{}, classifyInboxOrderedError(err, "session_commands")
	}
	// The provider's page is held to the request before any row is decoded. A
	// reply longer than the limit is a backend failure rather than a bonus:
	// this call's whole cost argument is that the caller's limit bounds the
	// work, and a caller that asked for one row and received a thousand has
	// been handed an unbounded page by something it cannot see.
	if len(provider.Records) > limit {
		return SessionDispositionCommandPage{}, inboxErr(InboxErrorBackend, "limit", nil)
	}
	page := SessionDispositionCommandPage{
		Commands: make([]DispositionInboxEntry, 0, len(provider.Records)),
		Limit:    limit,
	}
	// SEEDED WITH THE CALLER'S BOUND, not with zero, and that single assignment
	// carries the word "strictly" in this method's contract. It makes the loop
	// below enforce TWO things with one comparison: that the provider's rows
	// ascend, and that the FIRST of them is strictly above the bound the caller
	// gave. A provider that answered from the head of the stream, or handed
	// back the row sitting exactly at the exclusive bound, is caught here and
	// nowhere else.
	previous := req.AfterOrder
	for _, stored := range provider.Records {
		if err := opCtx.Err(); err != nil {
			return SessionDispositionCommandPage{}, err
		}
		// THE ORDER IS HELD TO THE CLAIM THIS CALL MAKES ABOUT IT, row by row,
		// and this is the one check that cannot be delegated to
		// dispositionInboxEntryFor. Every other check asks whether a row is
		// what it says it is; this one asks whether the SEQUENCE the provider
		// returned is the ascending, strictly-bounded one this method's
		// documentation promises. A caller is forbidden from sorting the page
		// for itself, so if the store does not verify the ordering nothing
		// does, and "ascending acceptance order" becomes a sentence with no
		// probe behind it.
		//
		// THE COMPARISON IS `<=` RATHER THAN `<`, and the difference is not
		// pedantry. `<` would admit two rows carrying the SAME acceptance
		// order — which for a consumption stream means the same command handed
		// to a consumer twice in one page — and would admit a first row sitting
		// exactly AT the caller's exclusive bound, which is the one row the
		// caller has already consumed. Both are driven.
		if stored.Order <= previous {
			return SessionDispositionCommandPage{}, inboxIdentity("order", nil)
		}
		previous = stored.Order
		// The row is decoded here and again inside dispositionInboxEntryFor,
		// which is the same deliberate double decode dueDispositionFor performs
		// and for a related reason: this reader has no command identity to hold
		// the row to, so it learns one from the row and then asks
		// dispositionInboxEntryFor the same filing question a named read asks.
		// Saving the second decode would mean a second, weaker check that only
		// this listing uses.
		//
		// NOTHING ELSE IS READ FROM record HERE, and a redundant identity
		// comparison that once stood at this point was removed rather than
		// kept: dispositionInboxEntryFor already holds the row's own TenantID
		// and SessionID to the two identities passed to it, so the check was a
		// second copy of a rule that lives there, and mutation testing
		// confirmed no test could tell the two copies apart. A guard that
		// cannot fail independently of the guard beside it is not a second
		// guard.
		record, err := decodeDispositionInboxRecord(stored.Value)
		if err != nil {
			return SessionDispositionCommandPage{}, err
		}
		entry, err := dispositionInboxEntryFor(
			stored, scope, req.TenantID, req.SessionID, record.Descriptor.CommandID, binding)
		if err != nil {
			return SessionDispositionCommandPage{}, err
		}
		page.Commands = append(page.Commands, entry)
	}
	if len(page.Commands) == 0 {
		// An exhausted stream reports no continuation. The provider says the
		// same thing, and this restates it rather than forwarding it so that a
		// provider answering an empty page with a position cannot hand a caller
		// a bound it was never given rows for.
		if provider.NextAfterOrder != 0 {
			return SessionDispositionCommandPage{}, inboxIdentity("next_after_order", nil)
		}
		return page, nil
	}
	if provider.NextAfterOrder != previous {
		return SessionDispositionCommandPage{}, inboxIdentity("next_after_order", nil)
	}
	page.NextAfterOrder = provider.NextAfterOrder
	return page, nil
}

// ---------------------------------------------------------------------------
// The durable consumption cursor
// ---------------------------------------------------------------------------

// DispositionCommandCursor is the durable record of how far one session's
// disposition command consumer has got, and of the residency epoch that last
// said so.
//
// WHAT IT IS. ConsumedOrder is an immutable acceptance order from THIS
// session's DISPOSITION inbox, and the record's meaning is exactly: SOME
// CONSUMER ASSERTED IT WAS DONE WITH EVERY COMMAND AT OR BELOW THIS POSITION.
// LeaseEpoch is the epoch of the grant that last wrote the record and is its
// fencing high-water mark; it never falls, which is why this record is never
// deleted.
//
// WHAT IT IS NOT, and this half matters more, because the obvious reading of a
// durable cursor is far stronger than what this row can support. It follows the
// rule SettlingResidencyEpoch states for the settlement record: the stored
// epoch is CONTEXT, NOT AUTHORITY.
//
//  1. IT IS NOT PROOF THAT ANY COMMAND WAS APPLIED. It is not a summary of the
//     inbox and it is not derived from one. A consumer that read ten commands,
//     rejected three, failed at the eleventh and saved ten writes exactly the
//     same row as one that applied all ten. The authoritative state of a
//     command is its own record's State, reachable by name, and nothing here
//     substitutes for reading it.
//
//  2. IT IS NOT PROOF THAT THE SAVER HELD A LIVE RESIDENCY. The fence
//     establishes that LeaseEpoch is at or above the greatest epoch previously
//     committed to this row; it does not establish that the grant was still
//     held at the compare-and-swap, and NO PATH IN THIS PACKAGE READS A LIVE
//     LEASE on this record. Read the field as "who asked, at or above the
//     mark", never as "who validly consumed".
//
//  3. IT AUTHORIZES NOTHING. It licenses no claim, no dispatch, no settlement
//     and no read. Every one of those goes through its own operation, with its
//     own preconditions, and none of them consults this row.
//
//  4. IT SAYS NOTHING ABOUT COMMANDS ABOVE IT. In particular a command above
//     the cursor may be applied, settled, or claimed by someone else; the
//     cursor is a consumer's position, not a watermark the inbox respects.
//
//  5. ZERO IS NOT PROOF THAT NOTHING WAS CONSUMED. It is proof that nothing was
//     RECORDED. A consumer that processed a hundred commands and crashed before
//     its first save leaves this row absent, and a successor that trusted zero
//     as history rather than as a starting position would re-present all
//     hundred. Re-presentation is safe — command application is idempotent by
//     identity — but a caller must know that is what it is relying on.
//
//  6. IT IS NOT COMPARABLE ACROSS SESSIONS OR PROTOCOLS, for the reason
//     DispositionInboxEntry states about AcceptedOrder itself: the number is a
//     position in one session's disposition stream and means nothing in any
//     other.
//
// UpdatedAt is the STORE's clock, read when the request is validated and before
// any provider call, exactly as SessionPointer's is and for the same reason:
// nothing decides on it, so it is an audit line rather than an input, and
// paying a round trip to make it truer would buy nothing.
//
// The accumulation is one small permanent row per session that has ever had a
// consumer: never listed, never ranked, never due, never read except by name.
// The registry's carry-forward contract about retention applies word for word —
// the only safe reaper is one that removes the session's whole scope at once,
// because deleting this row alone destroys a fence while leaving the session
// writable.
type DispositionCommandCursor struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	LeaseEpoch    uint64
	ConsumedOrder uint64

	UpdatedAt time.Time
}

// DispositionCommandCursorEntry is a cursor together with the revision a later
// compare-and-swap names.
//
// The ZERO ENTRY is the answer for a session that has never recorded one, and
// it is a whole-value answer rather than a found flag: ConsumedOrder is zero
// exactly when nothing is recorded, because a zero order is refused on the way
// in and no provider allocates one. A caller may therefore test the entry, the
// cursor, or the order and get the same answer from all three.
type DispositionCommandCursorEntry struct {
	Cursor   DispositionCommandCursor
	Revision uint64
}

// LoadDispositionCommandCursorRequest reads one session's consumption cursor.
type LoadDispositionCommandCursorRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
}

// SaveDispositionCommandCursorRequest records one session's consumption cursor.
//
// LeaseEpoch is the consumer's residency epoch and is compared against the
// record's committed high-water mark. ConsumedOrder is the acceptance order the
// consumer is done through and is compared against the record's committed
// order; both are high-water marks and neither ever falls.
//
// There is no expected revision and no timestamp, for the reason
// SetSessionPointerRequest carries neither: a cursor is not a decision a caller
// makes about a record it has read — it is the current truth about how far a
// consumer has got — so the write is closed against the revision this store
// reads for itself.
type SaveDispositionCommandCursorRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	LeaseEpoch    uint64
	ConsumedOrder uint64
}

// LoadDispositionCommandCursor returns one session's consumption cursor, or the
// zero entry when none has been recorded.
//
// A MISSING CURSOR IS NOT AN ERROR, and that is the requirement rather than a
// convenience. "Nothing has been consumed" is a complete, true and actionable
// answer — it is precisely the starting position of a fresh consumer — so
// reporting it as a failure would make every caller translate a not-found code
// back into the zero it already means, and one caller would get it wrong.
//
// IT IS NOT WIDER THAN THAT, in two directions a caller must know about.
//
// First, a stored row this reader cannot decode is NOT an absent cursor; it is
// a fencing high-water mark that cannot be evaluated, and it is reported as the
// typed failure it is. The hazard is entirely in that direction: absence
// licenses SaveDispositionCommandCursor to create a fresh record at whatever
// epoch and order the caller named, so a reader that reported an undecodable
// row as absence would let any caller reset the fence.
//
// Second, this is a NAMED READ of the disposition family and takes that
// family's authority check: it reads the catalog first, which verifies the
// session's collision witnesses and requires ProtocolModeDisposition, exactly
// as GetDispositionCommand does. A session with no catalog record, or one bound
// to another protocol, is REFUSED rather than answered "zero". That refusal is
// a *CatalogError and a caller must not translate it into "no commands"; the
// zero entry is the answer for a session that EXISTS and has no cursor.
func (s *Store) LoadDispositionCommandCursor(
	ctx context.Context, req LoadDispositionCommandCursorRequest,
) (DispositionCommandCursorEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return DispositionCommandCursorEntry{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return DispositionCommandCursorEntry{}, err
	}
	defer release()
	if _, err := s.dispositionCatalog(opCtx, scope, req.TenantID, req.SessionID); err != nil {
		return DispositionCommandCursorEntry{}, err
	}
	entry, _, err := s.readDispositionCursor(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return DispositionCommandCursorEntry{}, err
	}
	return entry, nil
}

// SaveDispositionCommandCursor records how far a session's consumer has got,
// under the caller's residency epoch.
//
// THE CONCURRENCY CONTRACT, stated in full because a consumer designs against
// it. Three rules, in the order they are applied:
//
//  1. THE EPOCH FENCE RUNS FIRST, and it answers a question about the CALLER:
//     may you write this session at all. A strictly lower epoch than the
//     committed one is refused with InboxErrorEpoch carrying the committed
//     mark. An equal one is admitted, because one grant saves many times.
//
//  2. THE ORDER FENCE RUNS SECOND, and it answers a question about the caller's
//     DATA: is the position you hold at least as far as the one stored. A
//     strictly lower order is refused with InboxErrorOrder carrying the
//     committed position. An equal one is admitted, so a save retried after an
//     ambiguous outcome succeeds rather than reporting a regression.
//
//     THE ORDER OF THE TWO IS LOAD-BEARING, AND THE CASE THAT DECIDES IT IS A
//     CALLER BELOW *BOTH* MARKS. That is worth stating precisely, because the
//     obvious candidate is the wrong one: a caller with a LOW EPOCH and a HIGH
//     POSITION passes the order fence and is then refused by the epoch fence,
//     so it receives InboxErrorEpoch under EITHER ordering and tells you
//     nothing about which ran first. The caller whose answer actually changes
//     is the one below both — a superseded lease that also holds a stale
//     position. Epoch-first tells it "you have lost the session", which is
//     terminal and correct; order-first would tell it "your position is stale",
//     which invites it to fetch newer data and retry forever against a session
//     it no longer owns. TestSaveDispositionCommandCursorFencesTheEpochBeforeTheOrder's
//     `earlier_epoch, earlier_order` row is that probe, and it is the ONLY row
//     that changes answer when the two calls are swapped.
//
//  3. THE WRITE IS A COMPARE-AND-SWAP on the revision this call just read. Two
//     savers under the SAME epoch are therefore ordered by the provider, and
//     the loser receives InboxErrorConflict carrying the actual revision rather
//     than overwriting the winner. A LOST CREATE RACE IS THE SAME ANSWER: a
//     create that finds the identity already present reports conflict, not
//     success and not a corrupt-record failure, because the caller's correct
//     response is identical — re-read and meet both fences.
//
// So the contract is: MANY CONCURRENT SAVERS ARE SAFE, the stored position is
// non-decreasing under every interleaving, and every CONCURRENCY refusal — that
// is, every refusal of a well-formed request against a readable row — is one of
// exactly three typed answers: you have lost the session (InboxErrorEpoch),
// your position is stale (InboxErrorOrder), or you raced (InboxErrorConflict).
// A MALFORMED REQUEST OR AN UNREADABLE ROW IS NOT ONE OF THOSE THREE and is not
// claimed to be: an invalid request is InboxErrorInvalid, an undecodable row is
// InboxErrorMalformed, a provider tombstone is InboxErrorDeleted, a session
// bound to another protocol is a *CatalogError, and a provider failure is
// InboxErrorBackend. A consumer's error classification must cover those too.
//
// What this is NOT is a lock: this call never waits, never retries for a
// caller, and never blocks a second writer.
//
// WHAT A SUCCESSFUL SAVE PROVES is only what DispositionCommandCursor says it
// does. In particular it is not evidence that the saver held a live residency
// at the swap; the fence establishes an ordering against what is stored, and
// nothing in this package reads a live lease here.
func (s *Store) SaveDispositionCommandCursor(
	ctx context.Context, req SaveDispositionCommandCursorRequest,
) (DispositionCommandCursorEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return DispositionCommandCursorEntry{}, err
	}
	// Encoding validates, so an invalid request is refused before any provider
	// work — including before the catalog is read. It returns the CANONICAL
	// record over the request-shaped one built here, so nothing below can reach
	// a form the stored bytes are not in.
	value, cursor, err := encodeDispositionCursor(DispositionCommandCursor{
		TenantID:      req.TenantID,
		SessionID:     req.SessionID,
		LeaseEpoch:    req.LeaseEpoch,
		ConsumedOrder: req.ConsumedOrder,
		UpdatedAt:     s.clock.Now(),
	})
	if err != nil {
		return DispositionCommandCursorEntry{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return DispositionCommandCursorEntry{}, err
	}
	defer release()
	// THE CATALOG IS THE AUTHORITY AND IT IS READ BEFORE ANYTHING IS WRITTEN.
	// A cursor is a position in the disposition inbox, so a session whose
	// catalog is not disposition-bound has no such positions and must not
	// acquire a row claiming otherwise. This also verifies the collision
	// witnesses, so a derived name is never trusted on its own.
	if _, err := s.dispositionCatalog(opCtx, scope, req.TenantID, req.SessionID); err != nil {
		return DispositionCommandCursorEntry{}, err
	}
	// The protocol witness is bound as every disposition write binds it. On
	// this path the catalog read above has already established the mode, so
	// this is idempotent rather than decisive — it is here so that the set of
	// disposition writes that bind the witness has no exceptions somebody has
	// to remember.
	if err := s.bindSessionScopeMode(opCtx, scope, ProtocolModeDisposition); err != nil {
		return DispositionCommandCursorEntry{}, err
	}
	current, found, err := s.readDispositionCursor(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return DispositionCommandCursorEntry{}, err
	}
	if !found {
		return s.createDispositionCursor(opCtx, scope, cursor, value)
	}
	if err := dispositionCursorEpochFence(current.Cursor, req.LeaseEpoch); err != nil {
		return DispositionCommandCursorEntry{}, err
	}
	if err := dispositionCursorOrderFence(current.Cursor, req.ConsumedOrder); err != nil {
		return DispositionCommandCursorEntry{}, err
	}
	return s.writeDispositionCursor(opCtx, scope, cursor, value, current.Revision)
}

// dispositionCursorEpochFence admits a write against the cursor's committed
// high-water epoch. It is epochFence in the inbox's vocabulary, for the reason
// pointerEpochFence is in the pointer's: the rule is shared, the NAME of a
// violation belongs to the record.
//
// The refusal carries both marks, because a caller that has to raise its epoch
// will have to satisfy the position too and one round trip is enough to learn
// both.
//
// The zero check is deliberately NOT here: encodeDispositionCursor makes it
// before the read, so an epochless request is refused as the caller mistake it
// is rather than being reported as whatever the read happened to find.
func dispositionCursorEpochFence(current DispositionCommandCursor, epoch uint64) error {
	return epochFence(current.LeaseEpoch, epoch, func(committed uint64) error {
		return &InboxError{
			Code:  InboxErrorEpoch,
			Field: "lease_epoch",
			Epoch: committed,
			Order: current.ConsumedOrder,
		}
	})
}

// dispositionCursorOrderFence admits a write whose position is at least as far
// as the one already recorded.
//
// It is the SAME SHAPE as the epoch fence and a different fact, which is why it
// is a second function rather than a second call to the first: an equal order
// is admitted, because a save retried after an ambiguous outcome must be able
// to succeed, and only a strictly lower one is a regression.
func dispositionCursorOrderFence(current DispositionCommandCursor, order uint64) error {
	if order < current.ConsumedOrder {
		return &InboxError{
			Code:  InboxErrorOrder,
			Field: "consumed_order",
			Epoch: current.LeaseEpoch,
			Order: current.ConsumedOrder,
		}
	}
	return nil
}

// readDispositionCursor reads the RAW stored cursor under an already-derived
// scope whose catalog its caller has already checked.
//
// It reports absence as a boolean rather than as an error because its two
// callers give absence different meanings — a load answers zero, a save creates
// — and an error would push that decision into a comparison against a code one
// of them would get wrong.
//
// Everything else IS an error. A stored record this reader cannot decode is not
// an absent record: it is a fencing high-water mark that cannot be evaluated,
// and reporting it as absence would let a save create straight over it at any
// epoch and any position.
func (s *Store) readDispositionCursor(
	ctx context.Context,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (DispositionCommandCursorEntry, bool, error) {
	stored, err := s.backend.OrderedIndex.Get(ctx, dispositionCursorID(scope))
	if err != nil {
		if errors.As(err, new(*storage.OrderedRecordNotFoundError)) {
			return DispositionCommandCursorEntry{}, false, nil
		}
		return DispositionCommandCursorEntry{}, false, classifyInboxOrderedError(err, "get")
	}
	entry, err := dispositionCursorEntryFor(stored, scope, tenant, session)
	if err != nil {
		return DispositionCommandCursorEntry{}, false, err
	}
	return entry, true, nil
}

// createDispositionCursor creates the first cursor a session has ever had.
//
// A create that finds the identity already there is a LOST RACE and is reported
// as InboxErrorConflict carrying the current revision, rather than being turned
// into an update here. Two reasons, and the second is the one that makes this
// branch worth its own test.
//
// The first is the fences: the record that arrived while this call was in
// flight carries two high-water marks this request has never been compared
// against, and evaluating them on this path would put a second copy of both in
// the file. A caller retries and meets them on the ordinary path.
//
// The second is the ANSWER. Without this branch the call would fall through to
// verifyCommandCursorBytes, which does catch the substitution — the winner's
// bytes are not this caller's — but reports InboxErrorIdentity, "your record is
// corrupt". That is the wrong instruction: a racing caller must retry, and
// identity says do not. The SAFETY property would survive removing this branch;
// the "you raced" arm of the three-answer contract would not. Two Hosts booting
// on a session that has never had a cursor is exactly where this races.
func (s *Store) createDispositionCursor(
	ctx context.Context,
	scope sessionScope,
	cursor DispositionCommandCursor,
	value []byte,
) (DispositionCommandCursorEntry, error) {
	stored, created, err := s.backend.OrderedIndex.Create(
		ctx, dispositionCursorID(scope), scope.SessionNamespace, value,
		storage.Rank{}, dispositionCursorDue(cursor))
	if err != nil {
		return DispositionCommandCursorEntry{}, classifyInboxOrderedError(err, "create")
	}
	if !created {
		// The revision is read from the provider's own reply rather than from a
		// decode of it: a racing winner's row may be anything, including a row
		// this reader would refuse, and a caller that lost a race needs to be
		// told it lost regardless of what the winner wrote.
		return DispositionCommandCursorEntry{}, &InboxError{
			Code: InboxErrorConflict, Field: "create", Revision: stored.Revision}
	}
	entry, err := dispositionCursorEntryFor(stored, scope, cursor.TenantID, cursor.SessionID)
	if err != nil {
		return DispositionCommandCursorEntry{}, err
	}
	return entry, verifyDispositionCursorBytes(stored, value)
}

// writeDispositionCursor compare-and-swaps one cursor onto the revision its
// caller read.
func (s *Store) writeDispositionCursor(
	ctx context.Context,
	scope sessionScope,
	cursor DispositionCommandCursor,
	value []byte,
	expectedRevision uint64,
) (DispositionCommandCursorEntry, error) {
	stored, err := s.backend.OrderedIndex.Update(
		ctx, dispositionCursorID(scope), expectedRevision, value,
		storage.Rank{}, dispositionCursorDue(cursor))
	if err != nil {
		return DispositionCommandCursorEntry{}, classifyInboxOrderedError(err, "update")
	}
	entry, err := dispositionCursorEntryFor(stored, scope, cursor.TenantID, cursor.SessionID)
	if err != nil {
		return DispositionCommandCursorEntry{}, err
	}
	return entry, verifyDispositionCursorBytes(stored, value)
}

// verifyDispositionCursorBytes holds a write's reply to the bytes the write
// handed the provider.
//
// Every other check on this path holds the reply to the RECORD'S OWN bytes,
// which a substituted record satisfies exactly as well as the real one. On a
// path where this package wrote the value, the provider is claiming something
// stronger — that it stored THESE bytes — and a reply naming another lease's
// epoch or another position would otherwise be returned to the caller as its
// own successful write and be used as the fence a later write is measured
// against.
//
// The comparison is exact because canonicalization is a fixed point: the bytes
// were produced by encodeDispositionCursor from a record that decodes and
// re-encodes to them.
func verifyDispositionCursorBytes(stored storage.OrderedRecord, value []byte) error {
	if !bytes.Equal(stored.Value, value) {
		return inboxIdentity("value", nil)
	}
	return nil
}

// dispositionCursorID names the one ordered record per session per cursor role.
func dispositionCursorID(scope sessionScope) storage.OrderedID {
	return storage.OrderedID{
		Namespace:     dispositionCursorNamespace,
		OrderingScope: scope.SessionNamespace,
		StableKey:     dispositionCursorStableKey,
	}
}

// dispositionCursorDue is the single definition of a cursor's due state, and
// like its siblings it is a function of the RECORD rather than of the operation
// writing it. Every write path calls it and the filing check compares against
// it, so no path can file a due state a reader cannot rebuild from the bytes.
//
// It is constant, and the constant is NOT DUE, for the reason
// sessionPointerDue's is: a cursor has no deadline, nothing sweeps it, and it
// stops being current only when a later save replaces it. The rows are also
// permanent, because they hold a fence, so a due state here would put one entry
// per session into a deadline page that nothing may ever remove from.
//
// CARRY-FORWARD CONTRACT: a later task that needs a cursor reconciler must
// derive its horizon HERE, from members of the record, never from the operation
// — the filing check compares the stored due against this derivation on every
// read, so an operation-derived due makes every concurrent reader fail with an
// identity error no retry can fix. It must also answer what removes a row from
// that page, because nothing here does.
func dispositionCursorDue(DispositionCommandCursor) storage.Due { return storage.Due{} }

// dispositionCursorEntryFor decodes one stored cursor and holds every
// provider-supplied component of its filing to what the record's own bytes say
// it should be, plus the identity the caller asked for.
//
// The enumeration, and why each entry is or is not here:
//
//   - Deleted — asserted first, and it is a fail-closed condition rather than a
//     lifecycle state. This package never deletes a cursor; there is no clear
//     operation and no tombstone, precisely so the fence survives. A provider
//     tombstone means the fence has been physically destroyed by something
//     outside this package, and the only safe answer is to refuse every read
//     and write of that identity rather than let the next save create a fresh
//     record.
//   - The record's OWN TenantID and SessionID — held to the request. This is
//     NOT a restatement of checkFiledScope below, and the difference is the
//     whole reason both exist: that check reads the PROVIDER-SUPPLIED FILING,
//     this one reads the RECORD'S OWN BYTES, and the two come from different
//     sources. A provider that filed a row correctly and answered with another
//     session's bytes would pass the filing check and hand this caller another
//     session's consumption position as its own.
//   - StableKey — held to this package's constant. It asks whether the provider
//     FILED the row where it said it did, which is a different question again.
//   - OrderingScope, RankingScope and Due — the triad every session-scoped
//     record files identically, through checkFiledScope.
//   - Rank — compared as a WHOLE VALUE against what this file files. This
//     record's views are fully determined here: it is written unranked and
//     not-due on every path.
//   - Namespace is excluded for the reason every other record excludes it: a
//     package constant with no counterpart in the record, so comparing against
//     it could only restate that this file's constant equals itself. What keeps
//     the record kinds apart is that each owns one, which
//     TestOrderedNamespacesAreDistinct pins.
//   - Order is excluded. The provider's acceptance order for THIS row is the
//     position of a cursor write in the cursor namespace's stream and has
//     nothing to do with the inbox position the record carries; conflating the
//     two is exactly the mistake this exclusion prevents. The entry does not
//     expose it and nothing lists this namespace.
//   - Revision is provider state with no meaning in the record; it is returned
//     for a later compare-and-swap rather than verified.
func dispositionCursorEntryFor(
	stored storage.OrderedRecord,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (DispositionCommandCursorEntry, error) {
	if stored.Deleted {
		return DispositionCommandCursorEntry{}, inboxErr(InboxErrorDeleted, "record", nil)
	}
	cursor, err := decodeDispositionCursor(stored.Value)
	if err != nil {
		return DispositionCommandCursorEntry{}, err
	}
	if cursor.TenantID != tenant || cursor.SessionID != session {
		return DispositionCommandCursorEntry{}, inboxIdentity("record", nil)
	}
	if stored.ID.StableKey != dispositionCursorStableKey {
		return DispositionCommandCursorEntry{}, inboxIdentity("stable_key", nil)
	}
	if err := checkFiledScope(stored, scope.SessionNamespace, dispositionCursorDue(cursor), inboxIdentity); err != nil {
		return DispositionCommandCursorEntry{}, err
	}
	if stored.Rank != (storage.Rank{}) {
		return DispositionCommandCursorEntry{}, inboxIdentity("rank", nil)
	}
	return DispositionCommandCursorEntry{Cursor: cursor, Revision: stored.Revision}, nil
}

// dispositionCursorWire is the stored JSON shape.
//
// It is a PRIVATE DTO for the reason the disposition record's is: it fixes this
// record's durable member names independently of the exported struct's Go field
// names, so renaming an exported field cannot silently rewrite a stored record.
// The exported DispositionCommandCursor deliberately carries NO JSON tags at
// all, so there is nothing decorative for a reader to mistake for the durable
// spelling. TestDispositionCommandCursorWireGolden pins that spelling with a
// byte literal.
type dispositionCursorWire struct {
	RecordVersion uint8                 `json:"record_version"`
	TenantID      sessionwire.TenantID  `json:"tenant_id"`
	SessionID     sessionwire.SessionID `json:"session_id"`
	LeaseEpoch    uint64                `json:"lease_epoch"`
	ConsumedOrder uint64                `json:"consumed_order"`
	UpdatedAt     time.Time             `json:"updated_at"`
}

// encodeDispositionCursor validates and encodes one cursor, returning the
// CANONICAL record beside the bytes.
//
// A caller must carry that record forward rather than the request-shaped value
// it passed in, for the reason encodeSessionPointer states: the due state is
// derived from the record, and deriving one from a record the stored bytes are
// not in is exactly the divergence that makes every concurrent reader fail.
func encodeDispositionCursor(cursor DispositionCommandCursor) ([]byte, DispositionCommandCursor, error) {
	cursor, err := canonicalDispositionCursor(cursor)
	if err != nil {
		return nil, DispositionCommandCursor{}, err
	}
	encoded, err := json.Marshal(dispositionCursorWire{
		RecordVersion: DispositionCommandCursorRecordVersion,
		TenantID:      cursor.TenantID,
		SessionID:     cursor.SessionID,
		LeaseEpoch:    cursor.LeaseEpoch,
		ConsumedOrder: cursor.ConsumedOrder,
		UpdatedAt:     cursor.UpdatedAt,
	})
	if err != nil {
		return nil, DispositionCommandCursor{}, inboxInvalid("record", err)
	}
	if len(encoded) > MaxDispositionCommandCursorRecordBytes {
		return nil, DispositionCommandCursor{}, inboxErr(InboxErrorTooLarge, "record", nil)
	}
	return encoded, cursor, nil
}

// decodeDispositionCursor strictly decodes one stored cursor and re-validates
// it, so a record corrupted in place cannot be handed to a caller or, worse, be
// used as a fence a later write is measured against.
func decodeDispositionCursor(value []byte) (DispositionCommandCursor, error) {
	wire, err := decodeVersionedRecord[dispositionCursorWire](
		value, MaxDispositionCommandCursorRecordBytes, DispositionCommandCursorRecordVersion,
		versionedRecordFields{Record: "record", Version: "record_version"}, inboxRecordFailure)
	if err != nil {
		return DispositionCommandCursor{}, err
	}
	return canonicalDispositionCursor(DispositionCommandCursor{
		TenantID:      wire.TenantID,
		SessionID:     wire.SessionID,
		LeaseEpoch:    wire.LeaseEpoch,
		ConsumedOrder: wire.ConsumedOrder,
		UpdatedAt:     wire.UpdatedAt,
	})
}

// canonicalDispositionCursor validates a cursor and returns its one canonical
// spelling. Encoding and decoding both end here, so a record read back is
// byte-identical to the record written and two encoders cannot disagree.
//
// A ZERO CONSUMED ORDER IS REFUSED, and that refusal is what makes the zero
// entry a complete answer. No provider allocates order zero —
// dispositionInboxEntryFor rejects a row that claims one — so zero is not a
// position; it is this record's spelling of "nothing recorded". Admitting a
// stored zero would make an absent cursor and a recorded one indistinguishable
// to LoadDispositionCommandCursor, and the zero-means-none contract would
// quietly stop being decidable.
//
// A ZERO LEASE EPOCH IS REFUSED for the reason canonicalSessionPointer refuses
// one: the epoch is a fence, and a fence minted at zero admits every writer.
//
// THE INSTANT IS NORMALIZED TO UTC, and that line is load-bearing even though
// nothing decides on UpdatedAt. Production's clock is time.Now(), so without it
// two stores in different zones would write different durable spellings of the
// same instant — and this record's "one canonical spelling, byte-identical on
// read-back" property is exactly what verifyDispositionCursorBytes compares.
//
// The instant bounds are this package's own, as they are for every other
// record: a year Go's JSON encoder cannot spell would otherwise be refused at
// Marshal with an untyped failure rather than here.
func canonicalDispositionCursor(cursor DispositionCommandCursor) (DispositionCommandCursor, error) {
	if err := cursor.TenantID.Validate(); err != nil {
		return DispositionCommandCursor{}, inboxInvalid("tenant_id", err)
	}
	if err := cursor.SessionID.Validate(); err != nil {
		return DispositionCommandCursor{}, inboxInvalid("session_id", err)
	}
	if cursor.LeaseEpoch == 0 {
		return DispositionCommandCursor{}, inboxInvalid("lease_epoch", nil)
	}
	if cursor.ConsumedOrder == 0 {
		return DispositionCommandCursor{}, inboxInvalid("consumed_order", nil)
	}
	if !rankableTime(cursor.UpdatedAt) {
		return DispositionCommandCursor{}, inboxInvalid("updated_at", nil)
	}
	cursor.UpdatedAt = cursor.UpdatedAt.UTC()
	return cursor, nil
}
