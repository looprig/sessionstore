package sessionstore

import (
	"context"
	"reflect"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// This file is the disposition protocol's dispatch authorization and its
// terminal settlement, and it is deliberately NOT a second copy of
// inbox_claim.go's state machine. It adds two edges to the record admitted by
// disposition_inbox.go —
//
//	claimed -> applying (an attempt)   applying -> applied | rejected (settlement)
//
// — each one read plus one revision compare-and-swap of that same record,
// through the same ordered-index operations every other transition uses.
//
// # The two ownership domains never meet
//
// A ResidencyEpoch is the Host's orchestration grant and a JournalEpoch is the
// runtime's grant in the bound agent journal. They are separate types here
// because they are separate authorities: residency guards the inbox claim this
// file's attempt consumes, and the journal grant guards application and its
// persisted outcomes. NOTHING in this file compares one with the other, and no
// value of one is derived from the other — the attempt records both, side by
// side, exactly as its caller supplied them.
//
// # Why an attempt is written BEFORE any dispatch
//
// The attempt is the durable statement that ONE dispatch of this command is
// authorized. Runtime dispatch is forbidden without it, so a losing
// compare-and-swap cannot produce a runtime call; a concurrent rejection and a
// dispatch therefore have one winner rather than two. Once applying commits,
// the attempt identity and BOTH original grant identities are immutable: a
// successor settling this command records its own residency as settlement
// context in the outcome and never rewrites the attempt to make its epochs look
// current.
//
// # What settlement is allowed to believe
//
// Only the store's own record and evidence a configured reader obtained from
// the bound journal. A settlement request names a command, the revision the
// caller decided on and the claimant's residency; it cannot supply an outcome,
// an absence, or any caller-authored proof. The expected descriptor the
// evidence is held to is DERIVED from the current inbox record, which is why
// the reader's request carries the pinned binding rather than anything the
// caller said.
//
// Evidence is verified BEFORE the inbox compare-and-swap, and it stays valid
// while that write runs because a terminal runtime disposition cannot change.
// That is what closes the negative-read/late-dispatch race without a
// transaction spanning the two stores. Corrupt, incomplete, mismatched or
// unavailable evidence fails closed and leaves the record unsettled — a
// provider failure or a cancellation propagates as itself and is never a skip,
// an absence, or permission to settle.
//
// # What this file does not do
//
// It does not claim a command (pending -> claimed has no entry point yet), does
// not read or write a journal, does not apply anything, and makes no
// exactly-once claim about any external effect. It cannot undo a side effect or
// terminate a remote tool. Reaping, retention and gate continuation are not
// implemented.

// JournalEpoch is a runtime grant in a session's bound agent journal. It is
// meaningful only with that journal's lease namespace and the session binding
// that resolves it, and it must never be compared with, or assigned from, a
// ResidencyEpoch: the two are different authorities over different stores. The
// runtime returns this value from its own journal ownership capability; a Host
// may not copy its residency epoch into it.
type JournalEpoch uint64

// DispositionAttemptID is the immutable identity of one authorized dispatch. It
// is an opaque caller-chosen name, bounded like every other identity here, and
// it is what the runtime's durable disposition must name for the evidence to be
// about this attempt.
type DispositionAttemptID string

// DispositionClaim is the residency-guarded claim a command is worked on under.
//
// The claim EDGE is not implemented by this step: nothing here moves a pending
// command into claimed, and this type exists because the attempt below consumes
// such a record and must say precisely what it requires of one. ExpiresAt is
// the claimant's own reading of when the claim lapses, evaluated against the
// STORE's clock exactly as every other liveness guard in this package is.
type DispositionClaim struct {
	ResidencyEpoch ResidencyEpoch `json:"residency_epoch"`
	ExpiresAt      time.Time      `json:"expires_at"`
}

// DispositionAttempt is the complete record of one authorized dispatch, written
// before the dispatch and immutable afterwards.
//
// JournalEpoch is the grant the runtime returned for the bound journal and
// ResidencyEpoch is the Host grant the dispatch was authorized under. They are
// stored as two members of two types on purpose: an attempt that kept one
// number could not tell a successor which authority had actually been held.
//
// These struct tags are NOT the durable spelling; see dispositionAttemptWire.
type DispositionAttempt struct {
	AttemptID      DispositionAttemptID `json:"attempt_id"`
	JournalEpoch   JournalEpoch         `json:"journal_epoch"`
	ResidencyEpoch ResidencyEpoch       `json:"residency_epoch"`
	StartedAt      time.Time            `json:"started_at"`
}

// DispositionOutcomeKind is the closed vocabulary of terminal runtime
// dispositions. Each value is a different durable statement and each carries
// different evidence, so a value outside this set is refused rather than given
// a default.
type DispositionOutcomeKind string

const (
	// DispositionApplied means the runtime durably accepted the specific
	// control and committed its effect in the same journal envelope. It does
	// NOT mean the model work or the external tools that control introduced
	// have finished.
	DispositionApplied DispositionOutcomeKind = "applied"

	// DispositionNoOp is an explicit successful application with no effect —
	// an interrupt of an idle session. It is a success, and it fabricates no
	// public event: reject-before-dispatch and an applied no-op are different
	// outcomes and are never merged.
	DispositionNoOp DispositionOutcomeKind = "no_op"

	// DispositionNotApplied is a successor runtime's recovery closure: it
	// proves, under a strictly later journal grant whose opening fence it
	// names, that no accepted transition occurred for this attempt. It is the
	// tombstone late dispatch must consult. An empty read is not one.
	DispositionNotApplied DispositionOutcomeKind = "not_applied"
)

func (k DispositionOutcomeKind) valid() bool {
	return k == DispositionApplied || k == DispositionNoOp || k == DispositionNotApplied
}

// terminalState is the inbox state a disposition kind settles a command into. A
// successful no-op is an APPLICATION, so it settles as applied.
func (k DispositionOutcomeKind) terminalState() InboxState {
	if k == DispositionNotApplied {
		return InboxStateRejected
	}
	return InboxStateApplied
}

// DispositionEvidence is one committed runtime disposition, as read from the
// bound journal by the configured reader. Every member describes a record that
// is already durable in that journal; nothing here is a conclusion about a
// caller and nothing here may be supplied by one.
//
// AuthorJournalEpoch is the grant that AUTHORED the disposition, which is the
// attempt's own grant for an application and a strictly later one for a
// recovery closure. AuthorFenceSeq names the verified opening fence of that
// later author grant and is set only for a closure. EventID and EventSeq name
// the public event carried in the same envelope as an applied disposition.
type DispositionEvidence struct {
	AttemptID           DispositionAttemptID
	Kind                DispositionOutcomeKind
	AttemptJournalEpoch JournalEpoch
	AuthorJournalEpoch  JournalEpoch
	DispositionSeq      uint64
	AuthorFenceSeq      uint64
	EventID             sessionwire.EventID
	EventSeq            uint64
}

// DispositionEvidenceRequest is the question the store asks its reader. It is
// DERIVED from the store's current inbox record — the pinned binding, the
// durable runtime mapping and the immutable attempt — and never from a
// settlement caller, which is what makes the reader's scope the session's own
// bound journal rather than anywhere a caller could point it.
type DispositionEvidenceRequest struct {
	TenantID         sessionwire.TenantID
	SessionID        sessionwire.SessionID
	CommandID        sessionwire.CommandID
	Kind             CommandKind
	RuntimeCommandID RuntimeCommandID
	Binding          SessionBinding
	Attempt          DispositionAttempt
}

// DispositionEvidenceReader is the narrow, trusted boundary over committed
// journal records. An implementation resolves ONLY the immutable binding in the
// request, reads committed dispositions from that journal, and returns what it
// found. It must not execute an agent, and this package neither imports nor
// knows anything about what does.
//
// Returning an error is the correct answer for an unavailable, cancelled,
// incomplete or unreadable journal: the error propagates to the caller and the
// command is left unsettled. A reader must NEVER report a missing or unreadable
// record as an empty DispositionEvidence — absence is not a disposition, and
// the zero value is refused here precisely so that a reader which did so
// settles nothing.
type DispositionEvidenceReader interface {
	ReadDispositionEvidence(ctx context.Context, req DispositionEvidenceRequest) (DispositionEvidence, error)
}

// WithDispositionEvidence configures the settlement evidence boundary. Without
// it SettleDispositionCommand refuses: a store with no reader has no way to
// verify anything and settling on record state alone is the overwrite the
// protocol exists to prevent.
func WithDispositionEvidence(reader DispositionEvidenceReader) Option {
	return func(cfg *config) error {
		if reader == nil || isNilDynamic(reflect.ValueOf(reader)) {
			return &InvalidOptionError{Field: "DispositionEvidence"}
		}
		cfg.evidence = reader
		return nil
	}
}

// DispositionOutcome is the durable terminal result of one command, keyed by
// the attempt that produced it and by the journal record that proves it.
//
// SettlingResidencyEpoch is the residency the SETTLEMENT was requested under,
// which is separate settlement context and may be a successor's: it is recorded
// beside the attempt rather than over it, so the record keeps naming the grants
// the dispatch was actually authorized under.
//
// These struct tags are NOT the durable spelling; see dispositionOutcomeWire.
type DispositionOutcome struct {
	Kind                   DispositionOutcomeKind `json:"kind"`
	AttemptID              DispositionAttemptID   `json:"attempt_id"`
	AttemptJournalEpoch    JournalEpoch           `json:"attempt_journal_epoch"`
	AuthorJournalEpoch     JournalEpoch           `json:"author_journal_epoch"`
	DispositionSeq         uint64                 `json:"disposition_seq"`
	AuthorFenceSeq         uint64                 `json:"author_fence_seq"`
	EventID                sessionwire.EventID    `json:"event_id"`
	EventSeq               uint64                 `json:"event_seq"`
	SettlingResidencyEpoch ResidencyEpoch         `json:"settling_residency_epoch"`
	SettledAt              time.Time              `json:"settled_at"`
}

// BeginDispositionAttemptRequest authorizes exactly one dispatch of one
// claimed command.
//
// ExpectedRevision is the revision of the record the caller read and decided
// on, as every compare-and-swap in this package requires. JournalEpoch is the
// grant the runtime's own construction returned; ResidencyEpoch is the Host
// grant the caller holds, and it must be the claim's own — a lower one has been
// superseded permanently, and a higher one has not claimed this command.
//
// StartedAt is the caller's own clock reading of when the attempt began, as
// every stored instant in this package is. It is recorded, not used as a guard.
type BeginDispositionAttemptRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID

	ExpectedRevision uint64
	AttemptID        DispositionAttemptID
	JournalEpoch     JournalEpoch
	ResidencyEpoch   ResidencyEpoch
	StartedAt        time.Time
}

// SettleDispositionCommandRequest asks the store to settle one command from
// durable evidence.
//
// It names a command, the revision the caller decided on, and the residency the
// claimant holds — and nothing else. It CANNOT supply an outcome, an absence,
// or any proof structure: the store derives what it expects from its own record
// and obtains the evidence itself, which is the whole of why a settlement
// cannot be talked into a state the journal does not support.
//
// ResidencyEpoch is settlement context, recorded in the outcome. It is not
// compared with any journal epoch, and it does not authorize the settlement —
// the evidence does. It may be a successor's, which is what lets the original
// holder AND a successor settle the same command by the same route.
type SettleDispositionCommandRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID

	ExpectedRevision uint64
	ResidencyEpoch   ResidencyEpoch
}

// BeginDispositionAttempt moves a claimed disposition command into applying and
// records the complete attempt.
//
// Its refusals are ordered so that a caller meeting two at once is told the one
// that stays true: a settled command has no transitions left; a record that is
// not claimed has no attempt edge; a residency below the claim's is superseded
// permanently; a residency that is not the claim's has not claimed this
// command; and a lapsed claim is no claim at all.
//
// There is no apply-deadline check and no new expiry, both deliberately. A
// writer holding a live claim may begin applying past the deadline — that is
// the deadline race settled in the dispatcher's favour — and an applying record
// is closed by EVIDENCE rather than by a timer, so inventing an expiry here
// would create a clock-shaped route to a conclusion the journal has not
// supported. A command whose evidence is unavailable stays observably
// unresolved instead.
func (s *Store) BeginDispositionAttempt(ctx context.Context, req BeginDispositionAttemptRequest) (DispositionInboxEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return DispositionInboxEntry{}, err
	}
	if err := req.CommandID.Validate(); err != nil {
		return DispositionInboxEntry{}, inboxInvalid("command_id", err)
	}
	if req.ExpectedRevision == 0 {
		return DispositionInboxEntry{}, inboxInvalid("expected_revision", nil)
	}
	attempt := DispositionAttempt{AttemptID: req.AttemptID, JournalEpoch: req.JournalEpoch, ResidencyEpoch: req.ResidencyEpoch, StartedAt: req.StartedAt}
	if err := validateDispositionAttempt(&attempt); err != nil {
		return DispositionInboxEntry{}, err
	}
	now := s.clock.Now()
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return DispositionInboxEntry{}, err
	}
	defer release()

	current, err := s.currentDispositionEntry(opCtx, scope, req.TenantID, req.SessionID, req.CommandID, req.ExpectedRevision)
	if err != nil {
		return DispositionInboxEntry{}, err
	}
	if current.Record.State != InboxStateClaimed || current.Record.Claim == nil {
		return DispositionInboxEntry{}, inboxErr(InboxErrorState, "state", nil)
	}
	claim := *current.Record.Claim
	if err := dispositionResidencyFence(claim, req.ResidencyEpoch); err != nil {
		return DispositionInboxEntry{}, err
	}
	if !now.Before(claim.ExpiresAt) {
		return DispositionInboxEntry{}, inboxErr(InboxErrorClaimLost, "claim", nil)
	}
	next := current.Record
	next.State = InboxStateApplying
	next.Attempt = &attempt
	return s.commitDispositionTransition(opCtx, scope, current, next)
}

// SettleDispositionCommand settles one applying command from verified evidence.
//
// The sequence is fixed and each step exists for a stated reason:
//
//  1. Read the store's OWN authority — the immutable catalog binding — and the
//     current inbox record, and hold the caller to the revision it decided on.
//     Losing that comparison returns the current revision, because the answer
//     to a lost compare-and-swap is to reread the inbox and never to repeat
//     execution.
//  2. A record that is already terminal at that revision is an IDEMPOTENT
//     result and is returned as it stands, with settled=false and with no
//     evidence read: a terminal runtime disposition cannot change, so there is
//     nothing to re-verify and nothing to write.
//  3. A record with no durably authorized attempt has nothing to settle. The
//     terminal outcome is keyed by an attempt, so evidence about a command that
//     never had one would be evidence about nothing.
//  4. Obtain the evidence through the configured reader, using a request the
//     store derived from its own record, and VERIFY it — before the write.
//  5. Compare-and-swap that exact revision to terminal.
//
// It does this for the original holder AND for a successor; the difference
// between them is in the evidence, not in the caller's assertion. It makes no
// claim that any external side effect happened exactly once.
func (s *Store) SettleDispositionCommand(ctx context.Context, req SettleDispositionCommandRequest) (DispositionInboxEntry, bool, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	if err := req.CommandID.Validate(); err != nil {
		return DispositionInboxEntry{}, false, inboxInvalid("command_id", err)
	}
	if req.ExpectedRevision == 0 {
		return DispositionInboxEntry{}, false, inboxInvalid("expected_revision", nil)
	}
	if req.ResidencyEpoch == 0 {
		return DispositionInboxEntry{}, false, inboxInvalid("residency_epoch", nil)
	}
	now := s.clock.Now()
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	defer release()

	current, err := s.dispositionEntryAtRevision(opCtx, scope, req.TenantID, req.SessionID, req.CommandID, req.ExpectedRevision)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	if current.Record.State.terminal() {
		return current, false, nil
	}
	if current.Record.State != InboxStateApplying || current.Record.Attempt == nil {
		return DispositionInboxEntry{}, false, inboxErr(InboxErrorState, "state", nil)
	}
	if s.evidence == nil {
		return DispositionInboxEntry{}, false, inboxErr(InboxErrorEvidence, "reader", nil)
	}
	d := current.Record.Descriptor
	attempt := *current.Record.Attempt
	evidence, err := s.evidence.ReadDispositionEvidence(opCtx, DispositionEvidenceRequest{
		TenantID:         d.TenantID,
		SessionID:        d.SessionID,
		CommandID:        d.CommandID,
		Kind:             d.Kind,
		RuntimeCommandID: d.RuntimeCommandID,
		Binding:          d.Binding,
		Attempt:          attempt,
	})
	// A reader's failure is returned as ITSELF rather than folded into an inbox
	// code: an unavailable journal and a cancelled read are facts about the
	// evidence boundary that a caller separates by type, and neither is a
	// finding about the command.
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	outcome, err := verifiedDispositionOutcome(attempt, evidence, req.ResidencyEpoch, now)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	next := current.Record
	next.State = outcome.Kind.terminalState()
	next.Outcome = &outcome
	settled, err := s.commitDispositionTransition(opCtx, scope, current, next)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	return settled, true, nil
}

// verifiedDispositionOutcome is the whole of what the store believes about a
// runtime disposition, and every comparison in it is against the store's own
// immutable attempt.
//
// The rules are the protocol's, stated once:
//
//   - The evidence must be about THIS attempt and the grant that attempt
//     selected. A disposition naming another attempt identity, or the same one
//     under a different journal grant, is evidence about something else.
//   - An application (applied or a successful no-op) is authored BY the
//     authorized attempt grant, so its author and attempt journal epochs are
//     equal. An applied disposition names the public event committed in the
//     same envelope, which is why the event's sequence is the disposition's; a
//     no-op names no event at all, because a private disposition envelope
//     fabricates none.
//   - A recovery closure is authored by a STRICTLY LATER grant and names that
//     author's verified opening fence, which necessarily precedes the closure
//     it authored.
//
// No comparison here involves a ResidencyEpoch, and there is no route by which
// one could: the residency is carried through untouched into the outcome as
// settlement context.
func verifiedDispositionOutcome(attempt DispositionAttempt, e DispositionEvidence, residency ResidencyEpoch, now time.Time) (DispositionOutcome, error) {
	if !e.Kind.valid() {
		return DispositionOutcome{}, inboxErr(InboxErrorEvidence, "kind", nil)
	}
	if e.AttemptID != attempt.AttemptID {
		return DispositionOutcome{}, inboxErr(InboxErrorEvidence, "attempt_id", nil)
	}
	if e.AttemptJournalEpoch != attempt.JournalEpoch {
		return DispositionOutcome{}, inboxErr(InboxErrorEvidence, "attempt_journal_epoch", nil)
	}
	if e.DispositionSeq == 0 {
		return DispositionOutcome{}, inboxErr(InboxErrorEvidence, "disposition_seq", nil)
	}
	switch e.Kind {
	case DispositionNotApplied:
		if e.AuthorJournalEpoch <= e.AttemptJournalEpoch {
			return DispositionOutcome{}, inboxErr(InboxErrorEvidence, "author_journal_epoch", nil)
		}
		if e.AuthorFenceSeq == 0 || e.AuthorFenceSeq >= e.DispositionSeq {
			return DispositionOutcome{}, inboxErr(InboxErrorEvidence, "author_fence_seq", nil)
		}
		if e.EventID != "" || e.EventSeq != 0 {
			return DispositionOutcome{}, inboxErr(InboxErrorEvidence, "event", nil)
		}
	default:
		if e.AuthorJournalEpoch != e.AttemptJournalEpoch {
			return DispositionOutcome{}, inboxErr(InboxErrorEvidence, "author_journal_epoch", nil)
		}
		if e.AuthorFenceSeq != 0 {
			return DispositionOutcome{}, inboxErr(InboxErrorEvidence, "author_fence_seq", nil)
		}
		if e.Kind == DispositionApplied {
			if err := e.EventID.Validate(); err != nil {
				return DispositionOutcome{}, inboxErr(InboxErrorEvidence, "event_id", err)
			}
			if e.EventSeq != e.DispositionSeq {
				return DispositionOutcome{}, inboxErr(InboxErrorEvidence, "event_seq", nil)
			}
		} else if e.EventID != "" || e.EventSeq != 0 {
			return DispositionOutcome{}, inboxErr(InboxErrorEvidence, "event", nil)
		}
	}
	return DispositionOutcome{
		Kind:                   e.Kind,
		AttemptID:              e.AttemptID,
		AttemptJournalEpoch:    e.AttemptJournalEpoch,
		AuthorJournalEpoch:     e.AuthorJournalEpoch,
		DispositionSeq:         e.DispositionSeq,
		AuthorFenceSeq:         e.AuthorFenceSeq,
		EventID:                e.EventID,
		EventSeq:               e.EventSeq,
		SettlingResidencyEpoch: residency,
		SettledAt:              now,
	}, nil
}

// dispositionResidencyFence admits a write that must come from the claim's own
// residency, in the order commandClaimFence uses and for its reasons: a
// residency below the claim's high-water mark is superseded permanently and
// must be told so, while one above it merely has not claimed this command.
func dispositionResidencyFence(claim DispositionClaim, residency ResidencyEpoch) error {
	if err := epochFence(uint64(claim.ResidencyEpoch), uint64(residency), func(committed uint64) error {
		return &InboxError{Code: InboxErrorEpoch, Field: "residency_epoch", Epoch: committed}
	}); err != nil {
		return err
	}
	if residency != claim.ResidencyEpoch {
		return inboxErr(InboxErrorClaimLost, "residency_epoch", nil)
	}
	return nil
}

// dispositionEntryAtRevision reads the record a transition is about, under the
// store's own catalog authority, and holds the caller to the revision it
// decided on. The provider's comparison still runs at the write; this one
// exists so the guards above run against the record the caller actually read.
func (s *Store) dispositionEntryAtRevision(
	ctx context.Context,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
	expectedRevision uint64,
) (DispositionInboxEntry, error) {
	binding, err := s.dispositionCatalog(ctx, scope, tenant, session)
	if err != nil {
		return DispositionInboxEntry{}, err
	}
	if err := s.bindProtocolMode(ctx, scope, ProtocolModeDisposition); err != nil {
		return DispositionInboxEntry{}, err
	}
	stored, err := s.backend.OrderedIndex.Get(ctx, dispositionInboxID(scope, command))
	if err != nil {
		return DispositionInboxEntry{}, classifyInboxOrderedError(err, "get")
	}
	current, err := dispositionInboxEntryFor(stored, scope, tenant, session, command, binding)
	if err != nil {
		return DispositionInboxEntry{}, err
	}
	if current.Revision != expectedRevision {
		return DispositionInboxEntry{}, &InboxError{Code: InboxErrorConflict, Field: "expected_revision", Revision: current.Revision}
	}
	return current, nil
}

// currentDispositionEntry additionally refuses a settled command, which is what
// every NON-terminal transition needs and settlement does not: settlement meets
// a terminal record as its own idempotent answer.
func (s *Store) currentDispositionEntry(
	ctx context.Context,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
	expectedRevision uint64,
) (DispositionInboxEntry, error) {
	current, err := s.dispositionEntryAtRevision(ctx, scope, tenant, session, command, expectedRevision)
	if err != nil {
		return DispositionInboxEntry{}, err
	}
	if current.Record.State.terminal() {
		return DispositionInboxEntry{}, inboxErr(InboxErrorTerminal, "state", nil)
	}
	return current, nil
}

// commitDispositionTransition is the single writer for both edges. The due
// state is derived from the record being written by dispositionInboxDue and
// nowhere else, the rank stays the unranked state admission wrote, the revision
// named is the one the decision was made against, and the stored reply is put
// back through dispositionInboxEntryFor so a write is held to every component
// of its filing exactly as a read is.
func (s *Store) commitDispositionTransition(
	ctx context.Context,
	scope sessionScope,
	current DispositionInboxEntry,
	next DispositionInboxRecord,
) (DispositionInboxEntry, error) {
	value, record, err := encodeDispositionInboxRecord(next)
	if err != nil {
		return DispositionInboxEntry{}, err
	}
	d := record.Descriptor
	stored, err := s.backend.OrderedIndex.Update(ctx, dispositionInboxID(scope, d.CommandID), current.Revision, value, storage.Rank{}, dispositionInboxDue(record))
	if err != nil {
		return DispositionInboxEntry{}, classifyInboxOrderedError(err, "update")
	}
	return dispositionInboxEntryFor(stored, scope, d.TenantID, d.SessionID, d.CommandID, d.Binding)
}
