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
// else: one per-session ascending read of the inbox, and one small permanent
// row per session recording a position in it.
//
// WHY THE LISTING IS NOT THE DUE VIEW, which is the question a reader of
// shards.go will arrive with. ListDueCommands answers "what in this SHARD needs
// attention by this instant". It is cross-session by its request type, it is
// ordered by a deadline, and a terminal command leaves it altogether because
// inboxDue files one NOT DUE. Every one of those is right for a reconciler and
// wrong for a consumer: a consumer reads ONE session, in the order the commands
// were accepted, and must still see a command it has already settled — that is
// what makes a cursor meaningful. The two are therefore two provider views of
// the same rows rather than one view with a filter over it, and neither is
// derivable from the other. The rows, the namespace and the filing checks are
// shared; the view is not.
//
// WHY THE CURSOR IS NOT A PAGE TOKEN. Every sessionwire.Cursor this package
// issues is a position inside one query's result, valid until the query ends
// and durable nowhere. This cursor is the opposite: it outlives every query,
// every process and every lease, and it is the thing a Host reads at startup to
// learn where the last Host stopped. They share a word and nothing else.

const (
	// commandCursorNamespace holds one consumption cursor per session.
	//
	// It is UNSHARDED, unlike the inbox it points into, and the reason is the
	// one sessionPointerDue gives: a shard exists to bound a DUE SWEEP, these
	// rows are never due, never ranked and never listed, and they are only ever
	// read by name. A sharded namespace would buy a partition for a sweep that
	// does not exist and cannot be added without answering what removes a row
	// from it.
	commandCursorNamespace = "sessionstore/command-cursors"

	// commandCursorStableKey separates cursor ROLES within one session, of
	// which there is currently one. It is a package constant rather than the
	// session identity for the reason sessionPointerID's stable key is the
	// pointer kind: the ordering scope already names the session, so the stable
	// key's whole job is to keep this session's roles apart, and no
	// caller-supplied text reaches this name.
	commandCursorStableKey = "command-consumption"

	// CommandCursorRecordVersion is the independent version of the stored
	// cursor. A reader fails closed on any other version rather than guessing
	// which members a future encoder meant.
	CommandCursorRecordVersion uint8 = 1

	// MaxCommandCursorRecordBytes bounds an encoded cursor. The record is two
	// identities, two integers and an instant, so this is generous by more than
	// an order of magnitude; what it is for is that an oversized record is
	// refused HERE rather than by the provider, so a record this package
	// accepted can always be rewritten.
	MaxCommandCursorRecordBytes = 4 << 10
)

// Stated as an unsigned constant for the reason the other records state theirs:
// prose cannot enforce the relationship between this bound and the provider's.
const _ = uint(storage.MaxOrderedValueBytes - MaxCommandCursorRecordBytes)

// ---------------------------------------------------------------------------
// The per-session ordered listing
// ---------------------------------------------------------------------------

// ListSessionCommandsRequest positions one bounded page of ONE session's inbox
// in immutable acceptance order.
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
// on every continuation — which is the failure ListDueCommandsRequest documents
// at length for a query whose bound genuinely cannot be restated. This one's
// can: the bound IS a row's order, and a caller that has the row has the bound.
type ListSessionCommandsRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	AfterOrder uint64
	Limit      int
}

// SessionCommandPage is one bounded ascending page of one session's inbox.
//
// Commands are in strictly increasing AcceptedOrder, every one of them above
// the request's bound. They are the SAME InboxEntry values a named read of each
// command returns, held to the same filing checks — there is no weaker
// sweep-shaped variant, because a consumer acts on these rows and every write
// it then makes is a compare-and-swap against the revision reported here.
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
// omission — see ListSessionCommands.
type SessionCommandPage struct {
	Commands       []InboxEntry
	Limit          int
	NextAfterOrder uint64
}

// ListSessionCommands returns one bounded page of one session's commands in
// ascending immutable acceptance order, strictly after the caller's bound.
//
// It is a READ. It claims nothing, settles nothing and writes nothing; it does
// not move the consumption cursor, which is a separate durable decision a
// caller makes with SaveCommandCursor after it has acted.
//
// ITS COST IS THE PAGE. One ListOrdered against one (namespace, ordering scope)
// pair returns at most Limit rows, and nothing here reads the catalog, the
// journal, an object or any other session. The deployment's tenant count, the
// shard's population and the session's terminal history above the bound do not
// appear in it. The session's terminal history BELOW the bound does not either,
// which is the point of the bound.
//
// IT VERIFIES THE SESSION'S WITNESSES, unlike ListDueCommands, and the
// difference is the same one that decides every other read in this package: a
// due sweep is HANDED its rows by the provider and derives no name, while this
// call DERIVES the ordering scope from caller-supplied identities and must not
// trust a derived name on its own. readInboxCommands makes that check before
// the provider is asked for anything.
//
// IT FAILS CLOSED ON A ROW IT CANNOT VOUCH FOR, which is the other deliberate
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
// session's identities fails the whole page, with the same typed error a named
// read of that command would return. A PROVIDER TOMBSTONE fails it too, and for
// the strongest reason of the set: nothing in this package deletes a command
// row, so a tombstone in this stream is a record destroyed by something outside
// it, and a consumer stepping over one would step over a command whose own
// bytes it can no longer see.
//
// WHAT FAILING CLOSED COSTS, stated plainly: one unreadable row stops that
// session's consumer at that row, permanently, until the row is repaired. There
// is no skip, no quarantine and no reporting channel — this package has neither
// a logger nor anywhere in this page to record a skip, which is the same
// limitation DueCommandPage states about locating an unreadable row. What an
// operator has is the failure's field, the session's identities, and the fact
// that the bound the page was read at narrows the row to the first one above
// it.
func (s *Store) ListSessionCommands(ctx context.Context, req ListSessionCommandsRequest) (SessionCommandPage, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return SessionCommandPage{}, err
	}
	limit, ok := s.pageLimit(req.Limit)
	if !ok {
		return SessionCommandPage{}, inboxInvalid("limit", nil)
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return SessionCommandPage{}, err
	}
	defer release()
	if err := s.verifySessionScope(opCtx, scope); err != nil {
		return SessionCommandPage{}, err
	}
	provider, err := s.backend.OrderedIndex.ListOrdered(
		opCtx, shardNamespace(inboxNamespace, scope.ControlShard), scope.SessionNamespace, req.AfterOrder, limit)
	if err != nil {
		return SessionCommandPage{}, classifyInboxOrderedError(err, "session_commands")
	}
	// The provider's page is held to the request before any row is decoded. A
	// reply longer than the limit is a backend failure rather than a bonus:
	// this call's whole cost argument is that the caller's limit bounds the
	// work, and a caller that asked for one row and received a thousand has
	// been handed an unbounded page by something it cannot see.
	if len(provider.Records) > limit {
		return SessionCommandPage{}, inboxErr(InboxErrorBackend, "limit", nil)
	}
	page := SessionCommandPage{Commands: make([]InboxEntry, 0, len(provider.Records)), Limit: limit}
	previous := req.AfterOrder
	for _, stored := range provider.Records {
		if err := opCtx.Err(); err != nil {
			return SessionCommandPage{}, err
		}
		// THE ORDER IS HELD TO THE CLAIM THIS CALL MAKES ABOUT IT, row by row,
		// and this is the one check that cannot be delegated to inboxEntryFor.
		// Every other check asks whether a row is what it says it is; this one
		// asks whether the SEQUENCE the provider returned is the ascending,
		// strictly-bounded one this method's documentation promises. A caller
		// is forbidden from sorting the page for itself, so if the store does
		// not verify the ordering nothing does, and "ascending acceptance
		// order" becomes a sentence with no probe behind it.
		if stored.Order <= previous {
			return SessionCommandPage{}, inboxIdentity("order", nil)
		}
		previous = stored.Order
		// The row is decoded here and again inside inboxEntryFor, which is the
		// same deliberate double decode dueCommandFor performs and for a
		// related reason: this reader has no command identity to hold the row
		// to, so it learns one from the row and then asks inboxEntryFor the
		// same filing question a named read asks. Saving the second decode
		// would mean a second, weaker check that only this listing uses.
		//
		// NOTHING ELSE IS READ FROM record HERE, and the redundant identity
		// comparison that used to stand at this point was removed rather than
		// kept: inboxEntryFor already holds the row's own TenantID and
		// SessionID to the two identities passed to it, so the check was a
		// second copy of a rule that lives there, and mutation testing
		// confirmed no test could tell the two copies apart. A guard that
		// cannot fail independently of the guard beside it is not a second
		// guard.
		record, err := decodeInboxRecord(stored.Value)
		if err != nil {
			return SessionCommandPage{}, err
		}
		entry, err := inboxEntryFor(stored, scope, req.TenantID, req.SessionID, record.CommandID)
		if err != nil {
			return SessionCommandPage{}, err
		}
		page.Commands = append(page.Commands, entry)
	}
	if len(page.Commands) == 0 {
		// An exhausted stream reports no continuation. The provider says the
		// same thing, and this restates it rather than forwarding it so that a
		// provider answering an empty page with a position cannot hand a caller
		// a bound it was never given rows for.
		if provider.NextAfterOrder != 0 {
			return SessionCommandPage{}, inboxIdentity("next_after_order", nil)
		}
		return page, nil
	}
	if provider.NextAfterOrder != previous {
		return SessionCommandPage{}, inboxIdentity("next_after_order", nil)
	}
	page.NextAfterOrder = provider.NextAfterOrder
	return page, nil
}

// ---------------------------------------------------------------------------
// The durable consumption cursor
// ---------------------------------------------------------------------------

// CommandCursor is the durable record of how far one session's command consumer
// has got, and of the lease epoch that last said so.
//
// WHAT IT IS. ConsumedOrder is an immutable acceptance order from this
// session's inbox, and the record's meaning is exactly: SOME CONSUMER ASSERTED
// IT WAS DONE WITH EVERY COMMAND AT OR BELOW THIS POSITION. LeaseEpoch is the
// epoch of the grant that last wrote the record and is its fencing high-water
// mark; it never falls, which is why this record is never deleted.
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
//  2. IT IS NOT PROOF THAT THE SAVER HELD A LIVE LEASE. The fence establishes
//     that LeaseEpoch is at or above the greatest epoch previously committed to
//     this row; it does not establish that the grant was still held at the
//     compare-and-swap, and NO PATH IN THIS PACKAGE READS A LIVE LEASE on this
//     record. Read the field as "who asked, at or above the mark", never as
//     "who validly consumed".
//
//  3. IT AUTHORIZES NOTHING. It licenses no claim, no dispatch, no settlement
//     and no read. Every one of those goes through its own operation, with its
//     own preconditions, and none of them consults this row.
//
//  4. IT SAYS NOTHING ABOUT COMMANDS ABOVE IT. In particular a command above
//     the cursor may be applied, terminal, or claimed by someone else; the
//     cursor is a consumer's position, not a watermark the inbox respects.
//
//  5. ZERO IS NOT PROOF THAT NOTHING WAS CONSUMED. It is proof that nothing was
//     RECORDED. A consumer that processed a hundred commands and crashed before
//     its first save leaves this row absent, and a successor that trusted zero
//     as history rather than as a starting position would re-present all
//     hundred. Re-presentation is safe — command application is idempotent by
//     identity — but a caller must know that is what it is relying on.
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
type CommandCursor struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	LeaseEpoch    uint64
	ConsumedOrder uint64

	UpdatedAt time.Time
}

// CommandCursorEntry is a cursor together with the revision a later
// compare-and-swap names.
//
// The ZERO ENTRY is the answer for a session that has never recorded one, and
// it is a whole-value answer rather than a found flag: ConsumedOrder is zero
// exactly when nothing is recorded, because a zero order is refused on the way
// in and no provider allocates one. A caller may therefore test the entry, the
// cursor, or the order and get the same answer from all three.
type CommandCursorEntry struct {
	Cursor   CommandCursor
	Revision uint64
}

// LoadCommandCursorRequest reads one session's consumption cursor.
type LoadCommandCursorRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
}

// SaveCommandCursorRequest records one session's consumption cursor.
//
// LeaseEpoch is the grant's epoch and is compared against the record's
// committed high-water mark. ConsumedOrder is the acceptance order the consumer
// is done through and is compared against the record's committed order; both
// are high-water marks and neither ever falls.
//
// There is no expected revision and no timestamp, for the reason
// SetSessionPointerRequest carries neither: a cursor is not a decision a caller
// makes about a record it has read — it is the current truth about how far a
// consumer has got — so the write is closed against the revision this store
// reads for itself.
type SaveCommandCursorRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	LeaseEpoch    uint64
	ConsumedOrder uint64
}

// LoadCommandCursor returns one session's consumption cursor, or the zero entry
// when none has been recorded.
//
// A MISSING CURSOR IS NOT AN ERROR, and that is the requirement rather than a
// convenience. "Nothing has been consumed" is a complete, true and actionable
// answer — it is precisely the starting position of a fresh consumer — so
// reporting it as a failure would make every caller translate a not-found code
// back into the zero it already means, and one caller would get it wrong.
//
// IT IS NOT WIDER THAN THAT. A stored row this reader cannot decode is NOT an
// absent cursor; it is a fencing high-water mark that cannot be evaluated, and
// it is reported as the typed failure it is. The hazard is entirely in that
// direction: absence licenses SaveCommandCursor to create a fresh record at
// whatever epoch and order the caller named, so a reader that reported an
// unreadable row as absence would let any caller reset the fence.
//
// It verifies the session's collision witnesses before any provider read, so a
// derived name is never trusted on its own.
func (s *Store) LoadCommandCursor(ctx context.Context, req LoadCommandCursorRequest) (CommandCursorEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return CommandCursorEntry{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return CommandCursorEntry{}, err
	}
	defer release()
	entry, _, err := s.readCommandCursor(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return CommandCursorEntry{}, err
	}
	return entry, nil
}

// SaveCommandCursor records how far a session's consumer has got, under the
// caller's lease epoch.
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
//     THE ORDER OF THE TWO IS LOAD-BEARING and is the same argument setPointer
//     makes: a superseded lease carrying a newer position must be told it has
//     lost the session, not told to fetch newer data and retry, and a live
//     lease carrying an older position must be told its position is stale, not
//     that its authority is in doubt. The two failures ask for opposite
//     responses, and reversing the checks would give a superseded Host an
//     InboxErrorOrder it would retry forever.
//
//  3. THE WRITE IS A COMPARE-AND-SWAP on the revision this call just read. Two
//     savers under the SAME epoch are therefore ordered by the provider, and
//     the loser receives InboxErrorConflict carrying the actual revision rather
//     than overwriting the winner. There is no lost update and no silent
//     regression: a caller that retries re-reads, meets both fences again, and
//     either advances the position or is told why it may not.
//
// So the contract is: MANY CONCURRENT SAVERS ARE SAFE, the stored position is
// non-decreasing under every interleaving, and every refusal is one of exactly
// three typed answers — you have lost the session, your position is stale, or
// you raced and should retry. What it is NOT is a lock: this call never waits,
// never retries for a caller, and never blocks a second writer.
//
// WHAT A SUCCESSFUL SAVE PROVES is only what CommandCursor says it does. In
// particular it is not evidence that the saver held a live lease at the swap;
// the fence establishes an ordering against what is stored, and nothing in this
// package reads a live lease here.
func (s *Store) SaveCommandCursor(ctx context.Context, req SaveCommandCursorRequest) (CommandCursorEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return CommandCursorEntry{}, err
	}
	// Encoding validates, so an invalid request is refused before any provider
	// work — including before the session's witnesses are bound. It returns the
	// CANONICAL record over the request-shaped one built here, so nothing below
	// can reach a form the stored bytes are not in.
	value, cursor, err := encodeCommandCursor(CommandCursor{
		TenantID:      req.TenantID,
		SessionID:     req.SessionID,
		LeaseEpoch:    req.LeaseEpoch,
		ConsumedOrder: req.ConsumedOrder,
		UpdatedAt:     s.clock.Now(),
	})
	if err != nil {
		return CommandCursorEntry{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return CommandCursorEntry{}, err
	}
	defer release()
	// A cursor is durable session data and may be the first record a session
	// has — a Host can be handed a session whose commands are all still to come
	// — so writing one binds the session's collision witnesses exactly as
	// admitting a command does. The binding is create-only and idempotent.
	if err := s.bindSessionScope(opCtx, scope); err != nil {
		return CommandCursorEntry{}, err
	}
	current, found, err := s.readCommandCursor(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return CommandCursorEntry{}, err
	}
	if !found {
		return s.createCommandCursor(opCtx, scope, cursor, value)
	}
	if err := commandCursorEpochFence(current.Cursor, req.LeaseEpoch); err != nil {
		return CommandCursorEntry{}, err
	}
	if err := commandCursorOrderFence(current.Cursor, req.ConsumedOrder); err != nil {
		return CommandCursorEntry{}, err
	}
	return s.writeCommandCursor(opCtx, scope, cursor, value, current.Revision)
}

// commandCursorEpochFence admits a write against the cursor's committed
// high-water epoch. It is epochFence in the inbox's vocabulary, for the reason
// pointerEpochFence is in the pointer's: the rule is shared, the NAME of a
// violation belongs to the record.
//
// The refusal carries both marks, because a caller that has to raise its epoch
// will have to satisfy the order too and one round trip is enough to learn
// both.
//
// The zero check is deliberately NOT here: encodeCommandCursor makes it before
// the read, so an epochless request is refused as the caller mistake it is
// rather than being reported as whatever the read happened to find.
func commandCursorEpochFence(current CommandCursor, epoch uint64) error {
	return epochFence(current.LeaseEpoch, epoch, func(committed uint64) error {
		return &InboxError{
			Code:  InboxErrorEpoch,
			Field: "lease_epoch",
			Epoch: committed,
			Order: current.ConsumedOrder,
		}
	})
}

// commandCursorOrderFence admits a write whose position is at least as far as
// the one already recorded.
//
// It is the SAME SHAPE as the epoch fence and a different fact, which is why it
// is a second function rather than a second call to the first: an equal order
// is admitted, because a save retried after an ambiguous outcome must be able
// to succeed, and only a strictly lower one is a regression.
func commandCursorOrderFence(current CommandCursor, order uint64) error {
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

// readCommandCursor reads the RAW stored cursor under an already-derived scope.
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
func (s *Store) readCommandCursor(
	ctx context.Context,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (CommandCursorEntry, bool, error) {
	if err := s.verifySessionScope(ctx, scope); err != nil {
		return CommandCursorEntry{}, false, err
	}
	stored, err := s.backend.OrderedIndex.Get(ctx, commandCursorID(scope))
	if err != nil {
		if errors.As(err, new(*storage.OrderedRecordNotFoundError)) {
			return CommandCursorEntry{}, false, nil
		}
		return CommandCursorEntry{}, false, classifyInboxOrderedError(err, "get")
	}
	entry, err := commandCursorEntryFor(stored, scope, tenant, session)
	if err != nil {
		return CommandCursorEntry{}, false, err
	}
	return entry, true, nil
}

// createCommandCursor creates the first cursor a session has ever had.
//
// A create that finds the identity already there is a lost race and is reported
// as a conflict carrying the current revision, rather than being turned into an
// update here. The reason is the fences: the record that arrived while this
// call was in flight carries two high-water marks this request has never been
// compared against, and evaluating them on this path would put a second copy of
// both in the file. A caller retries and meets them on the ordinary path.
func (s *Store) createCommandCursor(
	ctx context.Context,
	scope sessionScope,
	cursor CommandCursor,
	value []byte,
) (CommandCursorEntry, error) {
	stored, created, err := s.backend.OrderedIndex.Create(
		ctx, commandCursorID(scope), scope.SessionNamespace, value, storage.Rank{}, commandCursorDue(cursor))
	if err != nil {
		return CommandCursorEntry{}, classifyInboxOrderedError(err, "create")
	}
	entry, err := commandCursorEntryFor(stored, scope, cursor.TenantID, cursor.SessionID)
	if err != nil {
		return CommandCursorEntry{}, err
	}
	if !created {
		return CommandCursorEntry{}, &InboxError{Code: InboxErrorConflict, Field: "create", Revision: entry.Revision}
	}
	return entry, verifyCommandCursorBytes(stored, value)
}

// writeCommandCursor compare-and-swaps one cursor onto the revision its caller
// read.
func (s *Store) writeCommandCursor(
	ctx context.Context,
	scope sessionScope,
	cursor CommandCursor,
	value []byte,
	expectedRevision uint64,
) (CommandCursorEntry, error) {
	stored, err := s.backend.OrderedIndex.Update(
		ctx, commandCursorID(scope), expectedRevision, value, storage.Rank{}, commandCursorDue(cursor))
	if err != nil {
		return CommandCursorEntry{}, classifyInboxOrderedError(err, "update")
	}
	entry, err := commandCursorEntryFor(stored, scope, cursor.TenantID, cursor.SessionID)
	if err != nil {
		return CommandCursorEntry{}, err
	}
	return entry, verifyCommandCursorBytes(stored, value)
}

// verifyCommandCursorBytes holds a write's reply to the bytes the write handed
// the provider.
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
// were produced by encodeCommandCursor from a record that decodes and
// re-encodes to them.
func verifyCommandCursorBytes(stored storage.OrderedRecord, value []byte) error {
	if !bytes.Equal(stored.Value, value) {
		return inboxIdentity("value", nil)
	}
	return nil
}

// commandCursorID names the one ordered record per session per cursor role.
func commandCursorID(scope sessionScope) storage.OrderedID {
	return storage.OrderedID{
		Namespace:     commandCursorNamespace,
		OrderingScope: scope.SessionNamespace,
		StableKey:     commandCursorStableKey,
	}
}

// commandCursorDue is the single definition of a cursor's due state, and like
// its siblings it is a function of the RECORD rather than of the operation
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
func commandCursorDue(CommandCursor) storage.Due { return storage.Due{} }

// commandCursorEntryFor decodes one stored cursor and holds every
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
//   - The record's own TenantID and SessionID — held to the request, so a
//     provider returning another session's row cannot hand a caller a position
//     into someone else's inbox.
//   - StableKey — held to this package's constant. It asks whether the provider
//     FILED the row where it said it did, which is a different question from
//     whether the bytes are this session's.
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
func commandCursorEntryFor(
	stored storage.OrderedRecord,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (CommandCursorEntry, error) {
	if stored.Deleted {
		return CommandCursorEntry{}, inboxErr(InboxErrorDeleted, "record", nil)
	}
	cursor, err := decodeCommandCursor(stored.Value)
	if err != nil {
		return CommandCursorEntry{}, err
	}
	if cursor.TenantID != tenant || cursor.SessionID != session {
		return CommandCursorEntry{}, inboxIdentity("record", nil)
	}
	if stored.ID.StableKey != commandCursorStableKey {
		return CommandCursorEntry{}, inboxIdentity("stable_key", nil)
	}
	if err := checkFiledScope(stored, scope.SessionNamespace, commandCursorDue(cursor), inboxIdentity); err != nil {
		return CommandCursorEntry{}, err
	}
	if stored.Rank != (storage.Rank{}) {
		return CommandCursorEntry{}, inboxIdentity("rank", nil)
	}
	return CommandCursorEntry{Cursor: cursor, Revision: stored.Revision}, nil
}

// commandCursorWire is the stored JSON shape.
//
// It is a PRIVATE DTO for the reason the disposition record's is: it fixes this
// record's durable member names independently of the exported struct's Go field
// names, so renaming an exported field cannot silently rewrite a stored record.
// TestCommandCursorWireGolden pins the spelling with a byte literal.
type commandCursorWire struct {
	RecordVersion uint8                 `json:"record_version"`
	TenantID      sessionwire.TenantID  `json:"tenant_id"`
	SessionID     sessionwire.SessionID `json:"session_id"`
	LeaseEpoch    uint64                `json:"lease_epoch"`
	ConsumedOrder uint64                `json:"consumed_order"`
	UpdatedAt     time.Time             `json:"updated_at"`
}

// encodeCommandCursor validates and encodes one cursor, returning the CANONICAL
// record beside the bytes.
//
// A caller must carry that record forward rather than the request-shaped value
// it passed in, for the reason encodeSessionPointer states: the due state is
// derived from the record, and deriving one from a record the stored bytes are
// not in is exactly the divergence that makes every concurrent reader fail.
func encodeCommandCursor(cursor CommandCursor) ([]byte, CommandCursor, error) {
	cursor, err := canonicalCommandCursor(cursor)
	if err != nil {
		return nil, CommandCursor{}, err
	}
	encoded, err := json.Marshal(commandCursorWire{
		RecordVersion: CommandCursorRecordVersion,
		TenantID:      cursor.TenantID,
		SessionID:     cursor.SessionID,
		LeaseEpoch:    cursor.LeaseEpoch,
		ConsumedOrder: cursor.ConsumedOrder,
		UpdatedAt:     cursor.UpdatedAt,
	})
	if err != nil {
		return nil, CommandCursor{}, inboxInvalid("record", err)
	}
	if len(encoded) > MaxCommandCursorRecordBytes {
		return nil, CommandCursor{}, inboxErr(InboxErrorTooLarge, "record", nil)
	}
	return encoded, cursor, nil
}

// decodeCommandCursor strictly decodes one stored cursor and re-validates it,
// so a record corrupted in place cannot be handed to a caller or, worse, be
// used as a fence a later write is measured against.
func decodeCommandCursor(value []byte) (CommandCursor, error) {
	wire, err := decodeVersionedRecord[commandCursorWire](
		value, MaxCommandCursorRecordBytes, CommandCursorRecordVersion,
		versionedRecordFields{Record: "record", Version: "record_version"}, inboxRecordFailure)
	if err != nil {
		return CommandCursor{}, err
	}
	return canonicalCommandCursor(CommandCursor{
		TenantID:      wire.TenantID,
		SessionID:     wire.SessionID,
		LeaseEpoch:    wire.LeaseEpoch,
		ConsumedOrder: wire.ConsumedOrder,
		UpdatedAt:     wire.UpdatedAt,
	})
}

// canonicalCommandCursor validates a cursor and returns its one canonical
// spelling. Encoding and decoding both end here, so a record read back is
// byte-identical to the record written and two encoders cannot disagree.
//
// A ZERO CONSUMED ORDER IS REFUSED, and that refusal is what makes the zero
// entry a complete answer. No provider allocates order zero — inboxEntryFor
// rejects a row that claims one — so zero is not a position; it is this
// record's spelling of "nothing recorded". Admitting a stored zero would make
// an absent cursor and a recorded one indistinguishable to LoadCommandCursor,
// and the zero-means-none contract would quietly stop being decidable.
//
// A ZERO LEASE EPOCH IS REFUSED for the reason canonicalSessionPointer refuses
// one: the epoch is a fence, and a fence minted at zero admits every writer.
//
// The instant bounds are this package's own, as they are for every other
// record: a year Go's JSON encoder cannot spell would otherwise be refused at
// Marshal with an untyped failure rather than here.
func canonicalCommandCursor(cursor CommandCursor) (CommandCursor, error) {
	if err := cursor.TenantID.Validate(); err != nil {
		return CommandCursor{}, inboxInvalid("tenant_id", err)
	}
	if err := cursor.SessionID.Validate(); err != nil {
		return CommandCursor{}, inboxInvalid("session_id", err)
	}
	if cursor.LeaseEpoch == 0 {
		return CommandCursor{}, inboxInvalid("lease_epoch", nil)
	}
	if cursor.ConsumedOrder == 0 {
		return CommandCursor{}, inboxInvalid("consumed_order", nil)
	}
	if !rankableTime(cursor.UpdatedAt) {
		return CommandCursor{}, inboxInvalid("updated_at", nil)
	}
	cursor.UpdatedAt = cursor.UpdatedAt.UTC()
	return cursor, nil
}
