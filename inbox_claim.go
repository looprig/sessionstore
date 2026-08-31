package sessionstore

import (
	"context"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// This file is the durable command transition machine:
//
//	pending -> claimed(epoch, expiry) -> applying(epoch, expiry) -> applied | rejected
//
// Every edge is one read and one revision compare-and-swap of the same
// authoritative record inbox.go admitted. Nothing here reads or writes any
// other aggregate: a command's progress is a property of its own record.
//
// # What a claim epoch is, and what "stale" means for one
//
// The epoch on a claim is the SESSION LEASE epoch the claimer is acting under —
// the same monotonic grant the journal and the catalog fence on, not a number
// this aggregate allocates. That is what makes it meaningful across writers: a
// Host holds it because it holds the session, and a Factory replica reconciling
// the same command reads it rather than inventing one.
//
// Stale therefore means one thing precisely: an epoch BELOW the epoch the
// record's current claim was taken under. Such a caller has provably been
// superseded — a later grant exists, so its own lease is gone — and no retry
// under that epoch can ever succeed, which is why it is refused as
// InboxErrorEpoch carrying the high-water mark rather than as a race. This is
// hostEpochFence's rule, deliberately: the two fences answer the same question
// about the same lease, and a second, subtly different idea of when a Host has
// been superseded is exactly the drift that fence exists to prevent.
//
// The claim's high-water mark never falls. A claim is replaced only by a claim
// at an epoch at least as high, and a terminal record keeps the claim that
// applied it, so the epoch a superseded writer is measured against is the
// highest one that ever held the command.
//
// Where the two fences differ is what an EQUAL epoch means, and the difference
// is not a divergence but a consequence of what is being protected. The catalog
// fences a RECORD that one lease owns, so one grant writing many times is
// normal and equal is admitted. A claim fences a WORK ITEM that two writers
// under the same lease epoch — an owning Host and a reconciling Factory replica
// — may both reach for, and the record carries no claimant identity to tell
// them apart, so an equal epoch cannot prove "this is my own claim". An equal
// epoch therefore may not take a LIVE claim; it may take an expired one, which
// is the same writer resuming or another one recovering, and either is correct.
//
// A STRICTLY GREATER epoch may take a live claim. Its holder's lease has been
// superseded, so it can no longer commit anything to the journal, and making
// the successor wait out a claim TTL it can already prove is dead would stall
// every claimed command in the session on every failover for no safety.
//
// # What this file deliberately does not do
//
// It never inspects journal application evidence, and it refuses every
// transition out of an applying record whose claim has lapsed. Recovering one
// is continuation of an existing application rather than a new claim: it turns
// on a correlated application prefix, which is the next task's, and a machine
// that let a reconciler reject an expired applying record on state alone would
// be able to overwrite a command whose effect had already committed. Refusing
// is the fail-closed half of that rule and the extension point for the half
// that reads the evidence.
//
// # What a caller's clock does and does not decide
//
// Claim liveness and the apply deadline are evaluated against the STORE's clock
// (Clock, WithClock), read exactly once per operation, while the instants a
// caller wants STORED — a claim's expiry, an application's completion — are the
// caller's own readings, as every other timestamp this package stores is. A
// guard read twice within one operation could disagree with itself, and a guard
// supplied by the request would let the caller whose behaviour it bounds choose
// the answer.

// ClaimCommandRequest takes a short-lived claim on one accepted command.
//
// ExpectedRevision is the revision the caller decided on, which is the revision
// the compare-and-swap names. It is required: a transition is a decision about
// a record the caller has read, and a claim that named no revision would be a
// blind write dressed as a compare-and-swap.
//
// LeaseEpoch is the session lease epoch the claimer is acting under, and
// ClaimExpiresAt is the caller's own reading of when the claim lapses. The
// claim's expiry may fall after the command's apply deadline — that is what
// lets an unexpired claim win the deadline race — but it may not fall in the
// past.
type ClaimCommandRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID

	ExpectedRevision uint64
	LeaseEpoch       uint64
	ClaimExpiresAt   time.Time
}

// BeginApplyingCommandRequest moves a claimed command into applying. Its
// members mean what ClaimCommandRequest's mean; ClaimExpiresAt replaces the
// claim's expiry, because the bound that mattered while the claimer was
// preparing is not the bound that matters while it is applying.
type BeginApplyingCommandRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID

	ExpectedRevision uint64
	LeaseEpoch       uint64
	ClaimExpiresAt   time.Time
}

// CompleteCommandRequest records the terminal application of a command.
//
// Result names the durable journal event that carried the command's effect. It
// is required, because "applied" with no event is a claim that something
// happened with nothing to point at.
type CompleteCommandRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID

	ExpectedRevision uint64
	LeaseEpoch       uint64
	Result           CommandResult
}

// RejectCommandRequest records the terminal typed rejection of a command.
//
// LeaseEpoch is OPTIONAL here and required everywhere else in this file, and
// the asymmetry is the deadline reconciler's. Claiming, applying, and completing
// are things a session's lease holder does; rejecting is also what a Factory
// replica does to a command that has run out of deadline, and such a replica may
// be reconciling a session that has never had a lease at all. A zero epoch is
// therefore the honest statement "I am not acting under a session lease", and it
// buys exactly the authority the state machine grants that caller: it may settle
// a command that nobody is working on, and nothing else.
//
// Rejection is a value rather than a pointer because a rejection without a
// reason is not a state this record has. It is validated as a public projection,
// so it carries a stable typed cause rather than a provider or runtime message.
type RejectCommandRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID

	ExpectedRevision uint64
	LeaseEpoch       uint64
	Rejection        sessionwire.ErrorDetail
}

// GetCommandRequest reads one accepted command by its public identity.
type GetCommandRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID
}

// GetCommand returns one command's authoritative record, its current revision,
// and its immutable acceptance order.
//
// It is the read half of every compare-and-swap in this file: a caller that
// loses a race, or that meets a state it has no edge out of, learns what to do
// next by reading the record rather than by decoding the failure. A terminal
// command stays readable here forever — its terminal write leaves it out of the
// due view but not out of the store — which is what lets a caller report an
// outcome it did not itself commit.
func (s *Store) GetCommand(ctx context.Context, req GetCommandRequest) (InboxEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return InboxEntry{}, err
	}
	if err := req.CommandID.Validate(); err != nil {
		return InboxEntry{}, inboxErr(InboxErrorInvalid, "command_id", err)
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return InboxEntry{}, err
	}
	defer release()

	return s.readInboxEntry(opCtx, scope, req.TenantID, req.SessionID, req.CommandID)
}

// ClaimCommand takes a short-lived claim on a command so one writer works on it
// at a time.
//
// The order of its refusals is the order in which the answers become permanent,
// so a caller meeting two of them at once is told the one that will still be
// true after it retries:
//
//  1. A terminal command is settled. Nothing about it can be claimed, and the
//     answer is to read its outcome.
//  2. An applying command is not claimable at any epoch. See this file's header:
//     resuming one is continuation, not a claim.
//  3. A superseded epoch can never succeed again under that epoch.
//  4. The apply deadline has passed, so no NEW claim may start — permanently,
//     for this command, for every caller. It is deliberately checked before the
//     claim is examined, because "you are too late" stays true when the live
//     claim that would otherwise be reported expires.
//  5. A live claim at this epoch belongs to someone else and will expire.
//
// A claim taken over an EXPIRED claim is an ordinary claim, not a special
// reclaim: the record's members carry the new claim exactly as the first one
// did, and the reclaim horizon a reader derives from them follows. There is no
// second due state to file and no operation-shaped due state anywhere in this
// file — see inboxDue, which is the only definition there is.
func (s *Store) ClaimCommand(ctx context.Context, req ClaimCommandRequest) (InboxEntry, error) {
	scope, now, err := s.beginInboxTransition(req.TenantID, req.SessionID, req.CommandID, req.ExpectedRevision, req.LeaseEpoch, true)
	if err != nil {
		return InboxEntry{}, err
	}
	claim, err := requestedClaim(req.LeaseEpoch, req.ClaimExpiresAt, now)
	if err != nil {
		return InboxEntry{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return InboxEntry{}, err
	}
	defer release()

	current, err := s.currentInboxEntry(opCtx, scope, req.TenantID, req.SessionID, req.CommandID, req.ExpectedRevision)
	if err != nil {
		return InboxEntry{}, err
	}
	if current.Record.State == InboxStateApplying {
		return InboxEntry{}, inboxErr(InboxErrorState, "state", nil)
	}
	if err := commandEpochFence(current.Record, req.LeaseEpoch); err != nil {
		return InboxEntry{}, err
	}
	if !now.Before(current.Record.ApplyDeadline) {
		return InboxEntry{}, inboxErr(InboxErrorDeadline, "apply_deadline", nil)
	}
	if claimLive(current.Record, now) && req.LeaseEpoch == current.Record.Claim.LeaseEpoch {
		return InboxEntry{}, inboxErr(InboxErrorClaimHeld, "claim", nil)
	}
	return s.commitInboxTransition(opCtx, scope, current, inboxTransition{State: InboxStateClaimed, Claim: claim})
}

// BeginApplyingCommand moves a claimed command into applying, which is the
// statement that application is starting now rather than that capacity has been
// reserved. Only the holder of a live claim may make it: an epoch that is not
// the claim's has not claimed this command, and an expired claim is no longer a
// claim, so both are told the claim is lost and may claim again if the deadline
// still allows one.
//
// There is no apply-deadline check, and its absence is the deadline race the
// spec settles in the claimer's favour: a writer holding a live claim may begin
// applying even past the deadline, which is precisely what stops a reconciler's
// clock from cancelling work that is about to commit.
func (s *Store) BeginApplyingCommand(ctx context.Context, req BeginApplyingCommandRequest) (InboxEntry, error) {
	scope, now, err := s.beginInboxTransition(req.TenantID, req.SessionID, req.CommandID, req.ExpectedRevision, req.LeaseEpoch, true)
	if err != nil {
		return InboxEntry{}, err
	}
	claim, err := requestedClaim(req.LeaseEpoch, req.ClaimExpiresAt, now)
	if err != nil {
		return InboxEntry{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return InboxEntry{}, err
	}
	defer release()

	current, err := s.currentInboxEntry(opCtx, scope, req.TenantID, req.SessionID, req.CommandID, req.ExpectedRevision)
	if err != nil {
		return InboxEntry{}, err
	}
	if current.Record.State != InboxStateClaimed {
		return InboxEntry{}, inboxErr(InboxErrorState, "state", nil)
	}
	if err := commandEpochFence(current.Record, req.LeaseEpoch); err != nil {
		return InboxEntry{}, err
	}
	if req.LeaseEpoch != current.Record.Claim.LeaseEpoch {
		return InboxEntry{}, inboxErr(InboxErrorClaimLost, "lease_epoch", nil)
	}
	if !claimLive(current.Record, now) {
		return InboxEntry{}, inboxErr(InboxErrorClaimLost, "claim", nil)
	}
	return s.commitInboxTransition(opCtx, scope, current, inboxTransition{State: InboxStateApplying, Claim: claim})
}

// CompleteCommand records that an applying command's effect committed.
//
// It requires the caller to be the epoch the claim was taken under and does NOT
// require that claim to still be live, and the asymmetry with rejection is
// deliberate. Completing RECORDS SOMETHING THAT ALREADY HAPPENED: the result
// names a journal event that is already durable, which the caller could only
// have committed while it held the session lease. Refusing to record it because
// a claim TTL lapsed in the meantime would leave a command whose effect is
// visible in the journal sitting in applying, waiting for a recovery pass to
// discover what the writer standing right there already knew. Rejecting, by
// contrast, DECIDES something that has not happened, so it keeps the live-claim
// requirement.
//
// A successor lease finishing an application it did not start is the other half
// of this, and it is not here: it turns on the correlated application prefix
// this file does not read, so a greater epoch is refused as a lost claim rather
// than admitted on state alone.
func (s *Store) CompleteCommand(ctx context.Context, req CompleteCommandRequest) (InboxEntry, error) {
	scope, _, err := s.beginInboxTransition(req.TenantID, req.SessionID, req.CommandID, req.ExpectedRevision, req.LeaseEpoch, true)
	if err != nil {
		return InboxEntry{}, err
	}
	if req.Result.isZero() {
		return InboxEntry{}, inboxErr(InboxErrorInvalid, "result", nil)
	}
	if err := validateCommandResult(req.Result); err != nil {
		return InboxEntry{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return InboxEntry{}, err
	}
	defer release()

	current, err := s.currentInboxEntry(opCtx, scope, req.TenantID, req.SessionID, req.CommandID, req.ExpectedRevision)
	if err != nil {
		return InboxEntry{}, err
	}
	if current.Record.State != InboxStateApplying {
		return InboxEntry{}, inboxErr(InboxErrorState, "state", nil)
	}
	if err := commandEpochFence(current.Record, req.LeaseEpoch); err != nil {
		return InboxEntry{}, err
	}
	if req.LeaseEpoch != current.Record.Claim.LeaseEpoch {
		return InboxEntry{}, inboxErr(InboxErrorClaimLost, "lease_epoch", nil)
	}
	// The claim that applied the command is kept: it is the durable record of
	// which lease did so, and validateInboxState requires an applied command to
	// have one.
	return s.commitInboxTransition(opCtx, scope, current,
		inboxTransition{State: InboxStateApplied, Claim: current.Record.Claim, Result: req.Result})
}

// RejectCommand settles a command with a durable typed reason.
//
// It has two callers with different authority, and one rule that serves both:
//
//   - The holder of a live claim may reject the command it is working on, from
//     claimed or from applying. It has revalidated the command and found it
//     cannot be applied, and that answer is as durable as an application.
//   - A reconciler may settle a command NOBODY is working on: pending, or
//     claimed under a claim that has lapsed. It needs no lease epoch, and if it
//     names one it is still held to the record's high-water mark, because a
//     caller that asserts an epoch is asserting a view of the session that may
//     be stale.
//
// An unexpired claim therefore wins the deadline race outright: while the claim
// is live the only caller who may reject is its holder, whatever the clock says.
// So does an applying record, which additionally cannot be reclaimed at all, so
// the two-step of superseding the claim and then rejecting is closed as well.
//
// An APPLYING record whose claim has lapsed is refused, and that refusal is the
// one place this file is deliberately incomplete: whether such a command should
// be finished or rejected depends on whether its application prefix committed,
// which is evidence this file does not read. Rejecting it on state alone could
// overwrite a command whose effect is already in the journal — the exact
// overwrite the terminal states exist to prevent — so it fails closed here and
// waits for the reader that can tell the two apart.
//
// CARRY-FORWARD CONTRACT for whoever adds that reader: AN EXPIRED APPLYING
// COMMAND IS A HEAD-OF-LINE HAZARD IN THE DUE VIEW, not merely an unfinished
// case. Such a record stays non-terminal, so it stays due, so it occupies a
// place in every due page from its deadline onward, and nothing in this file can
// settle it. One crashed applier therefore parks a row in the deadline view
// permanently. Nothing is starved TODAY, because this package exposes no due
// command reader for anything to be starved out of; the hazard arrives with the
// reader. ListDueGates met the same shape and answered it by reporting what a
// page EXAMINED alongside what it returned, so a page that is full of rows it
// could not act on is distinguishable from a deployment with nothing to do.
func (s *Store) RejectCommand(ctx context.Context, req RejectCommandRequest) (InboxEntry, error) {
	scope, now, err := s.beginInboxTransition(req.TenantID, req.SessionID, req.CommandID, req.ExpectedRevision, req.LeaseEpoch, false)
	if err != nil {
		return InboxEntry{}, err
	}
	rejection := req.Rejection
	if err := validateCommandRejection(&rejection); err != nil {
		return InboxEntry{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return InboxEntry{}, err
	}
	defer release()

	current, err := s.currentInboxEntry(opCtx, scope, req.TenantID, req.SessionID, req.CommandID, req.ExpectedRevision)
	if err != nil {
		return InboxEntry{}, err
	}
	// A caller that names no epoch makes no claim about the session's leases and
	// is not measured against them; it is confined instead by the claim rules
	// below, which is the whole of a reconciler's authority.
	if req.LeaseEpoch != 0 {
		if err := commandEpochFence(current.Record, req.LeaseEpoch); err != nil {
			return InboxEntry{}, err
		}
	}
	switch {
	case claimLive(current.Record, now):
		if req.LeaseEpoch != current.Record.Claim.LeaseEpoch {
			return InboxEntry{}, inboxErr(InboxErrorClaimHeld, "claim", nil)
		}
	case current.Record.State == InboxStateApplying:
		return InboxEntry{}, inboxErr(InboxErrorClaimLost, "claim", nil)
	}
	return s.commitInboxTransition(opCtx, scope, current,
		inboxTransition{State: InboxStateRejected, Claim: current.Record.Claim, Rejection: &rejection})
}

// CommandStatus projects the durable record onto core's public command status.
//
// The durable machine has five states and the public vocabulary has four, so the
// projection makes one semantic choice, and it is this: an UNCLAIMED pending
// command is accepted, while claimed and applying are pending.
//
// Core's own words settle it. "Accepted means the inbox commit succeeded; it
// does not promise that a Host has already applied the command" — which is
// exactly and only what this store knows about a command nobody has picked up.
// Once a writer has claimed it, something more than the commit is true: the
// command is being worked on, and the public state that says so is pending. The
// alternative — reporting every non-terminal command as accepted — would make
// the public status say nothing that the acknowledgement of the original request
// had not already said, for the whole life of the command.
//
// The claim's LIVENESS deliberately does not enter into it. A command whose
// claim has lapsed is still a command someone started; the public caller cannot
// act on the difference, and reporting it would leak a scheduling detail that
// changes with a clock rather than with the command.
//
// It lives here rather than beside the record because the mapping is a statement
// about the machine, and it is offered here rather than left to each consumer
// because Factory, Host and any later reader answering "what happened to my
// command" must not each invent their own answer.
func (e InboxEntry) CommandStatus() (sessionwire.CommandStatus, error) {
	status := sessionwire.CommandStatus{
		CommandID:     e.Record.CommandID,
		AcceptedOrder: e.AcceptedOrder,
	}
	switch e.Record.State {
	case InboxStatePending:
		status.State = sessionwire.CommandStateAccepted
	case InboxStateClaimed, InboxStateApplying:
		status.State = sessionwire.CommandStatePending
	case InboxStateApplied:
		status.State = sessionwire.CommandStateApplied
	case InboxStateRejected:
		status.State = sessionwire.CommandStateRejected
		status.Error = e.Record.Rejection
	default:
		return sessionwire.CommandStatus{}, inboxErr(InboxErrorInvalid, "state", nil)
	}
	// Core owns what a public status means, so the projection is verified by
	// asking core rather than by restating its rules here — the shape ReadGates
	// already uses. Nothing a stored record can produce reaches it, because
	// validateInboxState has already refused a rejected record with no reason;
	// an entry a caller assembled by hand has not been through that.
	if err := status.Validate(); err != nil {
		return sessionwire.CommandStatus{}, inboxErr(InboxErrorInvalid, "status", err)
	}
	return status, nil
}

// beginInboxTransition performs the part of every transition that must happen
// BEFORE the store admits the operation: it validates the caller's identities
// and its compare-and-swap and lease members, and reads the clock once.
//
// The ordering is the package's, and it is not cosmetic: an invalid request
// against a closing store must be told what the caller got wrong rather than
// that the store is closing, because the caller's mistake is the durable fact
// and the store's state is not.
//
// leaseRequired distinguishes the transitions a session's lease holder makes
// from rejection, which a reconciler with no lease may also make. See
// RejectCommandRequest.
func (s *Store) beginInboxTransition(
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
	expectedRevision uint64,
	leaseEpoch uint64,
	leaseRequired bool,
) (sessionScope, time.Time, error) {
	scope, err := s.deriveSessionScope(tenant, session)
	if err != nil {
		return sessionScope{}, time.Time{}, err
	}
	if err := command.Validate(); err != nil {
		return sessionScope{}, time.Time{}, inboxErr(InboxErrorInvalid, "command_id", err)
	}
	// Zero is not a revision any provider assigns, so it cannot be a revision a
	// caller read. Admitting it would make the compare-and-swap unconditional.
	if expectedRevision == 0 {
		return sessionScope{}, time.Time{}, inboxErr(InboxErrorInvalid, "expected_revision", nil)
	}
	if leaseRequired && leaseEpoch == 0 {
		return sessionScope{}, time.Time{}, inboxErr(InboxErrorInvalid, "lease_epoch", nil)
	}
	return scope, s.clock.Now(), nil
}

// requestedClaim validates a caller's claim members and returns the claim to
// store. The well-formedness rule is the record's own, called rather than
// restated; what is added here is the one thing a stored record cannot express,
// which is that a claim must lapse in the FUTURE. A claim born expired is
// indistinguishable from no claim to every guard in this file, so accepting one
// would let a caller write a state it can never act on and hand the command
// straight back to the reclaim horizon.
func requestedClaim(epoch uint64, expiresAt time.Time, now time.Time) (CommandClaim, error) {
	claim := CommandClaim{LeaseEpoch: epoch, ExpiresAt: expiresAt}
	if err := validateCommandClaim(claim); err != nil {
		return CommandClaim{}, err
	}
	if !now.Before(expiresAt) {
		return CommandClaim{}, inboxErr(InboxErrorInvalid, "claim_expires_at", nil)
	}
	return claim, nil
}

// claimLive reports whether the record's claim still holds at now. The interval
// is half-open — a claim held up to but not including its expiry — which is the
// same convention the apply deadline uses, so an instant is never simultaneously
// inside one bound and outside the other.
//
// The zero-claim conjunct states the intent and decides nothing: an absent claim
// has the zero instant for an expiry and no clock reading this package will
// accept precedes it, so the comparison alone would already answer false. It
// stays because a reader should not have to reconstruct that argument to see
// that an unclaimed command is unclaimed.
func claimLive(record InboxRecord, now time.Time) bool {
	return !record.Claim.isZero() && now.Before(record.Claim.ExpiresAt)
}

// commandEpochFence admits a claiming write against the epoch the record's
// current claim was taken under. See this file's header for why it is
// hostEpochFence's rule and where the two differ.
//
// It has no zero-epoch arm, and deliberately so: the one caller that may name no
// epoch is RejectCommand, which does not call this at all for that caller.
// Stating "unless the epoch is zero" here as well would put the reconciler's
// exemption in two places, and the copy that was not the one being read would be
// the one that was wrong.
func commandEpochFence(record InboxRecord, epoch uint64) error {
	if epoch < record.Claim.LeaseEpoch {
		return &InboxError{Code: InboxErrorEpoch, Field: "lease_epoch", Epoch: record.Claim.LeaseEpoch}
	}
	return nil
}

// readInboxEntry reads one command's current record. Every path that reads a
// command shares it, so the witness check and the stored-identity check are
// stated exactly once, as readCatalogEntry does for the catalog.
func (s *Store) readInboxEntry(
	ctx context.Context,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
) (InboxEntry, error) {
	if err := s.verifySessionScope(ctx, scope); err != nil {
		return InboxEntry{}, err
	}
	stored, err := s.backend.OrderedIndex.Get(ctx, inboxID(scope, command))
	if err != nil {
		return InboxEntry{}, classifyInboxOrderedError(err, "get")
	}
	return inboxEntryFor(stored, scope, tenant, session, command)
}

// currentInboxEntry reads the record a transition is about and holds it to the
// two preconditions every transition shares: the caller's expected revision,
// and the fact that a settled command has no transitions left.
//
// The revision is compared here rather than only by the provider so that the
// guards below it are evaluated against a record the caller actually decided on.
// Reading at one revision, deciding, and then compare-and-swapping a DIFFERENT
// revision the caller named would be a decision about one record enforced
// against another. The provider's own comparison still runs — a record can move
// between this read and the write — and reports the same code.
func (s *Store) currentInboxEntry(
	ctx context.Context,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
	expectedRevision uint64,
) (InboxEntry, error) {
	current, err := s.readInboxEntry(ctx, scope, tenant, session, command)
	if err != nil {
		return InboxEntry{}, err
	}
	if current.Revision != expectedRevision {
		return InboxEntry{}, &InboxError{Code: InboxErrorConflict, Field: "expected_revision", Revision: current.Revision}
	}
	if current.Record.State.terminal() {
		return InboxEntry{}, inboxErr(InboxErrorTerminal, "state", nil)
	}
	return current, nil
}

// inboxTransition is the COMPLETE set of members a transition may change.
//
// It exists so that the members admission fixed cannot be changed by any
// transition — not because a transition is trusted not to, but because it has
// no way to say so. That is the carry-forward contract sameCommandAs states:
// Kind, Payload and PayloadRef are compared against a retry of the original
// command, so a transition that cleared an applied command's inline body — the
// obvious housekeeping, on a record this package itself calls a control record
// rather than a blob store — would turn every later retry into a permanent
// mismatch, telling the caller it had reused a command id for a different
// command. The identity, the runtime mapping and the accepted instant are fixed
// for the same kind of reason.
//
// Result and Rejection are absent from a non-terminal transition and a terminal
// one supplies exactly one of them, which validateInboxState then holds the
// whole record to.
type inboxTransition struct {
	State     InboxState
	Claim     CommandClaim
	Result    CommandResult
	Rejection *sessionwire.ErrorDetail
}

// commitInboxTransition applies one transition to the record it was decided
// against and compare-and-swaps it.
//
// It is the single writer in this file, which is what makes the record's filing
// impossible to get wrong in one path and right in another: the due state is
// derived from the record being written by inboxDue and nowhere else, the rank
// stays the unranked state admission wrote, and the revision named is the one
// the decision was made against. Encoding validates, so a transition that
// produced an incoherent record is refused before the provider sees it, and the
// stored reply is put back through inboxEntryFor so the write is held to every
// component of its filing exactly as a read is.
func (s *Store) commitInboxTransition(
	ctx context.Context,
	scope sessionScope,
	current InboxEntry,
	next inboxTransition,
) (InboxEntry, error) {
	record := current.Record
	record.State = next.State
	record.Claim = next.Claim
	record.Result = next.Result
	record.Rejection = next.Rejection
	value, record, err := encodeInboxRecord(record)
	if err != nil {
		return InboxEntry{}, err
	}
	stored, err := s.backend.OrderedIndex.Update(
		ctx, inboxID(scope, record.CommandID), current.Revision, value, storage.Rank{}, inboxDue(record))
	if err != nil {
		return InboxEntry{}, classifyInboxOrderedError(err, "update")
	}
	return inboxEntryFor(stored, scope, record.TenantID, record.SessionID, record.CommandID)
}
