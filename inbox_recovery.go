package sessionstore

import (
	"context"
	"errors"
	"io"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
)

// This file correlates a command's inbox record with the journal evidence of
// its application, and it is the reader inbox_claim.go fails closed for.
//
// # Why an aggregate that touches no other aggregate suddenly reads a second one
//
// Every transition in inbox_claim.go is a decision about one record made from
// that record alone, deliberately. Two of them cannot be: settling a command
// whose applier is gone is a decision about whether an EFFECT COMMITTED, and
// that fact lives in the journal. A machine that guessed it from state would be
// able to reject a command whose GateResolved or tool result is already durable
// and visible to a client — the one overwrite the terminal states exist to
// prevent. So this file adds exactly one thing to that machine: a durable
// answer to "did this command's effect commit", and two settlements gated on
// it.
//
// # The correlation, and why it is two identities and a kind
//
// A Host commits an application prefix — a PRIVATE runtime record, withheld
// from every public reader — immediately before the effect it is about to
// apply. It carries the public CommandID, the RuntimeCommandID the inbox
// durably mapped that command to, the command kind, and the lease epoch the
// writer stamps.
//
// Correlation requires ALL THREE of the record's own facts to agree, and a
// disagreement is not absence. A prefix naming this CommandID under a different
// runtime identity is either a Host that derived one identity from the other —
// the thing the spec forbids by name — or a durable mapping that has been
// broken; either way the store cannot tell which command that effect belongs
// to, so the command becomes UNSETTLEABLE rather than settled the convenient
// way. Treating a mismatch as absence would be the dangerous reading: it would
// let a command whose effect committed under a mismatched identity be rejected.
//
// The runtime identity is compared as a VALUE, not as text. The inbox stores an
// opaque string because it does not own the runtime's grammar, while the
// envelope stores a UUID, so the comparison parses the stored mapping and
// compares the decoded value — which makes an upper-case stored spelling
// correlate with the same UUID rather than silently failing to. A stored
// mapping that is not a UUID at all cannot correlate with any prefix, and that
// is reported as a conflict rather than as absence for the same reason.
//
// # What proves an effect did NOT commit
//
// Absence of evidence is the hard half, because a scan reports what is durable
// NOW while the applier may be about to append. Two things make a negative
// answer safe, and both are properties of durable state rather than assertions
// by the caller asking for the settlement:
//
//   - ADJACENCY. A prefix belongs immediately before its effect, so the record
//     at prefix+1 is the whole question. If it is a public event the effect
//     committed. If it is an OPENING FENCE at a strictly greater epoch, the
//     applier's very next append slot was taken by its successor, so it never
//     wrote anything after the prefix and never will. Anything else — another
//     prefix, a control record, or nothing at all yet — is UNRESOLVED, and both
//     settlements refuse. Two thirds of the writer contract are enforced by
//     that rather than assumed: a Host that interleaves two applications in one
//     stream, or that separates a prefix from its effect, parks both commands
//     instead of letting one adopt the other's event.
//
//     OMISSION is the third and cannot be enforced here. An effect committed
//     with NO prefix in front of it reads as ABSENT, which is settleable as
//     rejected — over a durable effect. No reading of the stream could catch
//     it: most public events are not command applications, so a store that
//     treated an uncorrelated event as evidence would have to attribute every
//     event in the session to some command. The prefix is the only thing that
//     makes an event attributable, so a Host that omits one has withheld the
//     evidence this file exists to read.
//
//   - FENCING. A journal grant's epochs are strictly increasing per name and an
//     opening fence is committed by compare-and-swap at the tip, so a fence at
//     epoch F proves every GRANT below F has lost the stream permanently: its
//     next append targets the sequence the fence took. An applying record can
//     therefore be settled as unfinished only once the journal carries a fence
//     above the epoch that claimed it. Until then the applier may still commit
//     the effect this settlement would orphan, and no epoch the CALLER names
//     can rule that out — a superseding epoch is a claim about the world, and
//     the fence is durable state.
//
//     Be precise about WHAT it binds, because the obvious over-reading is the
//     dangerous one: it kills the GRANT the record's claim epoch names, not the
//     Host process that held it. A Host that loses its grant, re-attaches under
//     a higher one, and carries on applying writes — with its own OpenJournal —
//     the very fence that makes its own unfinished application look settleable.
//     Nothing in this package can stop that, which is why it appears as a
//     stated obligation below rather than as a guarantee here.
//
// The two are separate questions and are asked separately: adjacency is about
// the prefix's own writer, fencing is about the lease that holds the record's
// claim, and those are not always the same epoch.
//
// # WRITER OBLIGATIONS
//
// A Host is held to three things this package cannot check, each of which costs
// a command its recoverability silently rather than failing anything:
//
//  1. An application prefix is committed IMMEDIATELY BEFORE its effect, with no
//     record of any kind between them, and one application at a time per
//     session. Violating this makes the command UNRESOLVED permanently: the
//     record at prefix+1 is durable and will never become the effect or the
//     fence the correlation needs.
//  2. Every runtime-visible effect of a command gets a prefix. An effect
//     without one is invisible here and can be rejected over.
//  3. A HOST MUST NOT APPEND AN APPLICATION PREFIX FOR A COMMAND WHOSE CLAIM
//     EPOCH IS BELOW ITS CURRENT GRANT. Losing the lease ABANDONS every
//     in-flight application; re-attaching does not resume one. That is not a
//     stylistic rule. The store admits no route back: an applying record is not
//     re-enterable at ANY epoch (BeginApplyingCommand takes only a claimed
//     one), and a claimed record admits only the claim's own epoch, so a
//     re-attached Host cannot legally resume. But nothing gates the PREFIX
//     WRITE itself, and this
//     file's new "a later epoch finishes an application" rule reads invitingly
//     like "re-attach and carry on". It is not. The two moves a re-attached
//     Host has for a command its predecessor was applying are the two below:
//     finish it if the evidence says it committed, reject it if the evidence
//     says it did not.
//
// # What this costs, and the accumulation it settles
//
// Correlation walks the session's journal from its first record. It is a cold
// path — a reconciler settling a command that ran out of deadline, or a Host
// recovering a session it has just attached to — and it is deliberately walked
// in full rather than sampled or cached: a shortcut here is a check the slow
// path performs and the fast path does not, on the one question where being
// wrong overwrites a committed effect.
//
// The cost is real and does not decay. It is the whole stream, decoded frame by
// frame, once per rejection, on a journal that only grows, and it is paid again
// by a caller that then loses the compare-and-swap.
//
// CARRY-FORWARD CONTRACT for whoever pays it down: BOUND THE WALK, NEVER MAKE
// IT CONDITIONAL. Two bounds are legitimate. A per-command journal index is the
// general answer. Cheaper and available today: a prefix cannot precede its
// command's admission, so recording the journal tip in the inbox record AT
// ADMISSION bounds every later walk to the records written since — a durable
// member of the record, derived by the store, which is why it does not reopen
// FindCommandApplicationRequest's refusal of caller-supplied positioning. What
// is NOT legitimate is skipping the walk for states that "cannot" have
// evidence: that is the check the slow path performs and the fast path does
// not, and the state it would trust is the state the evidence exists to
// second-guess.
//
// It repays that by closing the head-of-line hazard RejectCommand documents. An
// expired applying record used to be settleable by nobody, forever. It is now
// settled by the next lease holder — and the fence that makes its evidence
// conclusive is written by that same holder's OpenJournal, so the row clears
// exactly when a session is next attached rather than never.

// CommandApplicationOutcome is what a session's journal proves about one
// command's application. It is a closed vocabulary because each value unlocks a
// different settlement, and a caller that met an unlisted one would have no
// safe default.
type CommandApplicationOutcome string

const (
	// CommandApplicationAbsent means no record in the journal names this
	// command. Nothing has been applied under it.
	CommandApplicationAbsent CommandApplicationOutcome = "absent"

	// CommandApplicationCommitted means a correlated prefix is immediately
	// followed by the public event that carried its effect. The command has
	// been applied, whichever lease did it.
	CommandApplicationCommitted CommandApplicationOutcome = "committed"

	// CommandApplicationAbandoned means a correlated prefix is immediately
	// followed by an opening fence above its own epoch: the application started
	// and its writer lost the stream before committing anything more.
	CommandApplicationAbandoned CommandApplicationOutcome = "abandoned"

	// CommandApplicationUnresolved means a correlated prefix exists whose
	// outcome cannot be read: it is still at the tip with its writer possibly
	// alive, or the record after it is neither its effect nor its writer's
	// fence. Both settlements refuse; the answer may become readable later.
	CommandApplicationUnresolved CommandApplicationOutcome = "unresolved"

	// CommandApplicationConflicted means a prefix names this command's public
	// identity with a different runtime identity or kind. The durable mapping
	// is broken, so no settlement is safe and an operator has to look.
	CommandApplicationConflicted CommandApplicationOutcome = "conflicted"
)

// precedence orders the outcomes when a journal carries more than one prefix
// for one command — a retry after an abandoned attempt, or an anomaly.
//
// It is a total order rather than a chain of special cases, and its shape is
// "the least settleable wins, except that a durable effect is a fact". A broken
// correlation outranks everything: it is the one finding that says the store
// cannot tell which effect belongs to this command, and a later clean prefix
// does not repair an earlier contradiction. A committed effect outranks an
// unresolved attempt because the command HAS been applied and recording that is
// correct however many attempts followed it. Abandonment is the weakest
// positive finding: it is only interesting when nothing better was seen.
func (o CommandApplicationOutcome) precedence() int {
	switch o {
	case CommandApplicationConflicted:
		return 4
	case CommandApplicationCommitted:
		return 3
	case CommandApplicationUnresolved:
		return 2
	case CommandApplicationAbandoned:
		return 1
	default:
		return 0
	}
}

// CommandApplication is what one session's journal proves about one command.
//
// Every member describes DURABLE STATE this store read, never a conclusion
// about a caller. The sequences are journal sequences in the session's own
// stream; the epochs are session lease epochs.
//
// CapturedTip is the tip the correlation was taken at, so a caller can say what
// its answer was true of. An UNRESOLVED answer taken at one tip may be readable
// at a later one; ABSENT, COMMITTED and ABANDONED do not change, because the
// journal does not rewrite and the fence that settled them cannot be undone.
type CommandApplication struct {
	CommandID        sessionwire.CommandID
	RuntimeCommandID RuntimeCommandID

	Outcome CommandApplicationOutcome

	// PrefixSeq and PrefixEpoch locate the prefix the outcome is about, and are
	// zero when the outcome is Absent.
	PrefixSeq   uint64
	PrefixEpoch uint64

	// EffectSeq and EffectEventID name the public event that carried the
	// effect, and are zero unless the outcome is Committed.
	EffectSeq     uint64
	EffectEventID sessionwire.EventID

	// SupersedingEpoch is the highest opening-fence epoch in the journal, which
	// is the highest lease that has ever owned the stream. Every writer below
	// it is provably fenced out.
	SupersedingEpoch uint64

	CapturedTip uint64
}

// namesEffect reports whether a caller's terminal result is the effect this
// correlation found. It is the whole of what a recovering successor is held to,
// stated once so that the completion path cannot drift from the query: the
// outcome must be Committed, and the event and sequence must be the correlated
// ones rather than any event the caller preferred.
//
// CompletedAt is deliberately not compared. It is the caller's own clock
// reading of when it recorded the outcome, as every stored instant in this
// package is; the journal has no counterpart to hold it to.
//
// The outcome test is the load-bearing one and the two comparisons cannot fail
// on their own today: observe clears the effect members whenever a finding is
// displaced, so a non-committed correlation carries no event to match and a
// result naming an empty event is refused as malformed long before it reaches
// here. It is stated anyway rather than left implied, because the alternative
// is a rule that reads "an effect is whatever is in these two fields" and is
// correct only as long as a helper three functions away keeps clearing them.
// This is the one guard in this file no mutation of it can make observable.
func (a CommandApplication) namesEffect(result CommandResult) bool {
	return a.Outcome == CommandApplicationCommitted &&
		result.EventID == a.EffectEventID &&
		result.JournalSeq == a.EffectSeq
}

// provesNoEffect reports whether the journal establishes that no effect
// committed under this command. Only two outcomes do: one where nothing names
// the command at all, and one where an application started and its writer was
// fenced before it could commit anything else.
func (a CommandApplication) provesNoEffect() bool {
	return a.Outcome == CommandApplicationAbsent || a.Outcome == CommandApplicationAbandoned
}

// fences reports whether the journal proves a writer holding the given lease
// epoch can no longer append to this session. See this file's header: a
// committed opening fence above an epoch is that proof, and it is the only
// thing that makes a negative answer about an in-flight applier safe.
func (a CommandApplication) fences(epoch uint64) bool {
	return a.SupersedingEpoch > epoch
}

// FindCommandApplicationRequest names one accepted command whose journal
// evidence a caller wants. It carries no positioning members: the correlation
// is a question about the whole of a session's history, and a caller that could
// bound the walk could bound away the evidence.
type FindCommandApplicationRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID
}

// FindCommandApplication reports what a session's journal proves about one
// command's application.
//
// It reads the command's authoritative record FIRST and correlates against the
// identities stored there, never against identities a caller supplied. That is
// the point of the durable mapping: a caller that could name the runtime
// identity to correlate on could ask about a mapping that was never accepted,
// and the answer would be evidence about nothing.
//
// It is offered publicly because a recovering Host has to DECIDE between
// finishing and rejecting, and because the result a finished application
// records is the correlated effect — which the caller has no other way to name.
// A journal fault is reported in the journal's own vocabulary: it is a fault of
// the stream rather than of the command record, and a caller separates them by
// type exactly as it already must.
func (s *Store) FindCommandApplication(ctx context.Context, req FindCommandApplicationRequest) (CommandApplication, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return CommandApplication{}, err
	}
	if err := req.CommandID.Validate(); err != nil {
		return CommandApplication{}, inboxErr(InboxErrorInvalid, "command_id", err)
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return CommandApplication{}, err
	}
	defer release()

	current, err := s.readInboxEntry(opCtx, scope, req.TenantID, req.SessionID, req.CommandID)
	if err != nil {
		return CommandApplication{}, err
	}
	return s.scanCommandApplication(opCtx, scope, current.Record)
}

// correlatedPrefix is one application prefix that matched the record on both
// identities and the kind, held until the NEXT record says what became of it.
type correlatedPrefix struct {
	seq   uint64
	epoch uint64
}

// scanCommandApplication walks the session's journal once and reports what it
// proves about one command.
//
// It takes the RECORD rather than an identity because correlation is against
// the durable mapping, and it takes an already-admitted context and a derived
// scope because its two transition callers have read the record they are about
// to write; re-admitting or re-verifying here would state their preconditions a
// second time.
//
// The walk is a three-record state machine and nothing more: the record before
// (was it a prefix?), the prefix awaiting resolution, and the record in hand.
// prefixBefore tracks ANY writer's prefix rather than only this command's,
// because a prefix stacked behind another is exactly the interleaving that
// makes adjacency meaningless, and the stacked one must not be allowed to adopt
// the record after it.
func (s *Store) scanCommandApplication(
	ctx context.Context,
	scope sessionScope,
	record InboxRecord,
) (CommandApplication, error) {
	app := CommandApplication{
		CommandID:        record.CommandID,
		RuntimeCommandID: record.RuntimeCommandID,
		Outcome:          CommandApplicationAbsent,
	}
	// A stored mapping that is not a UUID decodes to the ZERO UUID, and the
	// comparison below then refuses every prefix — which is the right answer
	// and needs no arm of its own. DecodeEnvelope runs validateEnvelope, which
	// refuses an application prefix carrying a zero runtime identity, so no
	// decoded prefix can ever equal it. An explicit "if the mapping did not
	// parse" branch would restate that refusal in a second place, where it
	// could not be reached and so could not be seen to rot.
	//
	// The parse failure is not returned, either. Whether a command's mapping is
	// a UUID is a property of a stored record rather than of this query, and a
	// query that refused outright would turn a command that is merely
	// uncorrelatable into one that cannot be asked about.
	runtime, _ := uuid.Parse(string(record.RuntimeCommandID))

	tip, err := s.backend.Ledger.Tip(ctx, scope.JournalName)
	if err != nil {
		return CommandApplication{}, journalErr(JournalErrorBackend, "tip", err)
	}
	app.CapturedTip = tip
	// A session that has never been written has no evidence to read. This is a
	// pure OPTIMISATION and is documented as one rather than as a precondition:
	// storage v0.6.0 states that an absent ledger behaves as empty and that any
	// read beyond the tip yields a drained cursor, so removing it would cost a
	// round trip and change no answer.
	if tip == 0 {
		return app, nil
	}
	cursor, err := s.backend.Ledger.Read(ctx, scope.JournalName, 1)
	if err != nil {
		return CommandApplication{}, journalErr(JournalErrorBackend, "read", err)
	}
	defer cursor.Close()

	var pending *correlatedPrefix
	prefixBefore := false
	for {
		stored, err := cursor.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return CommandApplication{}, journalErr(JournalErrorBackend, "read", err)
		}
		// The walk stops at the tip it captured, so a record committed while
		// it was walking is not evidence: the answer is true of a snapshot the
		// caller is told the boundary of. Without it a correlation could report
		// an effect that landed after the decision it is feeding was made.
		if stored.Seq > tip {
			break
		}
		env, err := DecodeEnvelope(stored.Payload)
		if err != nil {
			return CommandApplication{}, journalErr(JournalErrorIntegrity, "record", err)
		}
		if env.Kind == EnvelopeKindOpeningFence && env.LeaseEpoch > app.SupersedingEpoch {
			app.SupersedingEpoch = env.LeaseEpoch
		}
		if pending != nil {
			app.resolve(*pending, stored.Seq, env)
			pending = nil
		}
		if env.Kind == EnvelopeKindApplicationPrefix && env.CommandID == record.CommandID {
			held := correlatedPrefix{seq: stored.Seq, epoch: env.LeaseEpoch}
			switch {
			case env.RuntimeCommandID != runtime || env.CommandKind != string(record.Kind):
				app.observe(CommandApplicationConflicted, held, commandEffect{})
			case prefixBefore:
				// Stacked behind another prefix: the record after this one may
				// be the earlier application's effect, so nothing this prefix
				// is followed by can be attributed to it.
				app.observe(CommandApplicationUnresolved, held, commandEffect{})
			default:
				pending = &held
			}
		}
		prefixBefore = env.Kind == EnvelopeKindApplicationPrefix
	}
	// A prefix at the tip has no record after it to say what became of it, and
	// its writer may be about to append one.
	if pending != nil {
		app.observe(CommandApplicationUnresolved, *pending, commandEffect{})
	}
	return app, nil
}

// resolve reads the single record that follows a correlated prefix. See this
// file's header for why exactly two shapes conclude anything and everything
// else is unresolved.
func (a *CommandApplication) resolve(prefix correlatedPrefix, seq uint64, env Envelope) {
	switch {
	case env.Kind == EnvelopeKindPublicEvent:
		a.observe(CommandApplicationCommitted, prefix, commandEffect{seq: seq, eventID: env.EventID})
	case env.Kind == EnvelopeKindOpeningFence && env.LeaseEpoch > prefix.epoch:
		a.observe(CommandApplicationAbandoned, prefix, commandEffect{})
	default:
		a.observe(CommandApplicationUnresolved, prefix, commandEffect{})
	}
}

// commandEffect is the public event a prefix was followed by, and is empty for
// every finding except a committed one.
type commandEffect struct {
	seq     uint64
	eventID sessionwire.EventID
}

// observe folds one prefix's finding into the correlation, keeping the finding
// with the highest precedence and, among equals, the latest.
//
// The effect travels WITH the finding rather than being written by the caller
// afterwards, which is what makes the two impossible to disagree: a finding
// that displaces a committed one cannot leave that one's event behind to be
// named by a completion the new finding does not support, and a committed
// finding cannot be recorded without the event that made it committed.
func (a *CommandApplication) observe(outcome CommandApplicationOutcome, prefix correlatedPrefix, effect commandEffect) {
	if outcome.precedence() < a.Outcome.precedence() {
		return
	}
	a.Outcome = outcome
	a.PrefixSeq = prefix.seq
	a.PrefixEpoch = prefix.epoch
	a.EffectSeq = effect.seq
	a.EffectEventID = effect.eventID
}
