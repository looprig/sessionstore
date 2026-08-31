// Correlating a command's inbox record with the journal evidence of its
// application, and the two settlements that evidence unlocks.
package sessionstore

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// loggingOrdered records the ordered-index calls one operation makes into the
// SAME log the ledger records into. Two logs could not answer the question this
// file asks — whether the journal was consulted before or after the record was
// written — because interleaving two independent sequences is exactly what a
// single log is for.
type loggingOrdered struct {
	storage.OrderedIndex
	log *callLog
}

func (o *loggingOrdered) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	o.log.add("ordered:get")
	return o.OrderedIndex.Get(ctx, id)
}

func (o *loggingOrdered) Update(
	ctx context.Context,
	id storage.OrderedID,
	expectedRevision uint64,
	value []byte,
	rank storage.Rank,
	due storage.Due,
) (storage.OrderedRecord, error) {
	o.log.add("ordered:update")
	return o.OrderedIndex.Update(ctx, id, expectedRevision, value, rank, due)
}

var _ storage.OrderedIndex = (*loggingOrdered)(nil)

// --- fixtures --------------------------------------------------------------

// A second command and a second runtime identity, so a correlation can be shown
// to reject the ones that are not this command's.
const (
	otherCommand = sessionwire.CommandID("command-b")
	otherRuntime = RuntimeCommandID("2f1c7d1e-0f3a-4c5b-9f21-000000000002")
	otherKind    = CommandKind("interrupt")
)

// applicationPrefix is the private journal record a Host commits immediately
// before a command's effect. LeaseEpoch is deliberately absent: the writer
// stamps it, and a caller that names one is refused.
func applicationPrefix(command sessionwire.CommandID, runtime RuntimeCommandID, kind CommandKind) Envelope {
	return Envelope{
		Kind:             EnvelopeKindApplicationPrefix,
		CommandID:        command,
		RuntimeCommandID: uuid.MustParse(string(runtime)),
		CommandKind:      string(kind),
	}
}

func openRecoveryJournal(t *testing.T, store *Store) *JournalWriter {
	t.Helper()
	writer, err := store.OpenJournal(context.Background(), OpenJournalRequest{
		TenantID:  catalogTenant,
		SessionID: catalogSession,
	})
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close(context.Background()) })
	return writer
}

func mustAppend(t *testing.T, writer *JournalWriter, env Envelope) uint64 {
	t.Helper()
	seq, err := writer.Append(context.Background(), env)
	if err != nil {
		t.Fatalf("Append(%d): %v", env.Kind, err)
	}
	return seq
}

func mustClose(t *testing.T, writer *JournalWriter) {
	t.Helper()
	if err := writer.Close(context.Background()); err != nil {
		t.Fatalf("Close journal: %v", err)
	}
}

func mustFindApplication(t *testing.T, store *Store, entry InboxEntry) CommandApplication {
	t.Helper()
	app, err := store.FindCommandApplication(context.Background(), FindCommandApplicationRequest{
		TenantID:  entry.Record.TenantID,
		SessionID: entry.Record.SessionID,
		CommandID: entry.Record.CommandID,
	})
	if err != nil {
		t.Fatalf("FindCommandApplication: %v", err)
	}
	return app
}

// applyingCommand is the shape every case in this file starts from: one command
// in applying under the epoch of a journal writer that really holds the
// session's stream, because the claim epoch and the journal grant epoch are the
// same lease.
type applyingCommand struct {
	store   *Store
	clock   *movableClock
	entry   InboxEntry
	writer  *JournalWriter
	epoch   uint64
	backend *storage.Composite
}

func startApplying(t *testing.T, backend *storage.Composite) *applyingCommand {
	t.Helper()
	store, clock, admitted := inboxFixture(t, backend)
	writer := openRecoveryJournal(t, store)
	applying := mustBeginApplying(t, store, mustClaim(t, store, admitted, writer.Epoch()), writer.Epoch())
	return &applyingCommand{
		store:   store,
		clock:   clock,
		entry:   applying,
		writer:  writer,
		epoch:   writer.Epoch(),
		backend: backend,
	}
}

// supersede closes the applying writer and opens the next one, which commits an
// opening fence at a strictly greater epoch. That fence is the durable proof
// the predecessor can never append again.
func (a *applyingCommand) supersede(t *testing.T) uint64 {
	t.Helper()
	mustClose(t, a.writer)
	next := openRecoveryJournal(t, a.store)
	a.writer = next
	return next.Epoch()
}

func (a *applyingCommand) completeAs(epoch uint64, result CommandResult) (InboxEntry, error) {
	request := testCompleteRequest(a.entry, epoch)
	request.Result = result
	return a.store.CompleteCommand(context.Background(), request)
}

func (a *applyingCommand) rejectAs(epoch uint64) (InboxEntry, error) {
	return a.store.RejectCommand(context.Background(), testRejectRequest(a.entry, epoch))
}

// correlatedResult is the completion a successor lease derives from the
// evidence: the caller supplies its own completion instant, and the store holds
// the rest to the journal.
func correlatedResult(app CommandApplication) CommandResult {
	return CommandResult{CompletedAt: inboxClaimLapsed, EventID: app.EffectEventID, JournalSeq: app.EffectSeq}
}

// offeredResult is what a recovering caller actually sends. When the journal
// correlates an effect it is that effect; when it does not, it is a
// well-formed result the caller believes in and the journal does not support,
// which is the only way to reach the evidence rule at all — a result with no
// event is refused as malformed long before it, so a test that sent one would
// pass for the wrong reason.
func offeredResult(app CommandApplication) CommandResult {
	if app.EffectEventID == "" {
		return CommandResult{CompletedAt: inboxClaimLapsed, EventID: "event-42", JournalSeq: 42}
	}
	return correlatedResult(app)
}

// --- what a prefix correlates ---------------------------------------------

// TestApplicationPrefixCarriesBothIdentitiesAndTheLease is step one: the record
// the correlation reads carries the PUBLIC CommandID, the runtime identity the
// inbox mapped it to, the command kind, and the lease epoch that applied it —
// and the reader reports the position of each half of the application.
func TestApplicationPrefixCarriesBothIdentitiesAndTheLease(t *testing.T) {
	t.Parallel()

	fixture := startApplying(t, memstore.New())
	prefixSeq := mustAppend(t, fixture.writer, applicationPrefix(inboxCommand, inboxRuntime, inboxKind))
	effectSeq := mustAppend(t, fixture.writer, publicEvent("event-effect", `{"applied":true}`))

	app := mustFindApplication(t, fixture.store, fixture.entry)
	if app.Outcome != CommandApplicationCommitted {
		t.Fatalf("outcome = %q, want %q", app.Outcome, CommandApplicationCommitted)
	}
	if app.CommandID != inboxCommand || app.RuntimeCommandID != inboxRuntime {
		t.Fatalf("correlated identities = %q/%q", app.CommandID, app.RuntimeCommandID)
	}
	if app.PrefixSeq != prefixSeq || app.PrefixEpoch != fixture.epoch {
		t.Fatalf("prefix = seq %d epoch %d, want seq %d epoch %d", app.PrefixSeq, app.PrefixEpoch, prefixSeq, fixture.epoch)
	}
	if app.EffectSeq != effectSeq || app.EffectEventID != "event-effect" {
		t.Fatalf("effect = seq %d id %q, want seq %d id %q", app.EffectSeq, app.EffectEventID, effectSeq, "event-effect")
	}
}

// TestAPrefixIsWithheldFromEveryPublicReader pins the record's privacy at the
// place this task creates a reason to read it: correlation runs on the runtime
// projection, and a public caller learns only that the sequence is covered.
func TestAPrefixIsWithheldFromEveryPublicReader(t *testing.T) {
	t.Parallel()

	fixture := startApplying(t, memstore.New())
	prefixSeq := mustAppend(t, fixture.writer, applicationPrefix(inboxCommand, inboxRuntime, inboxKind))

	page, err := fixture.store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{
		TenantID:  catalogTenant,
		SessionID: catalogSession,
	})
	if err != nil {
		t.Fatalf("ReadPublicJournal: %v", err)
	}
	if len(page.Events) != 0 {
		t.Fatalf("a public page returned %d events over a journal of private records", len(page.Events))
	}
	if page.CoveredThrough != prefixSeq {
		t.Fatalf("covered_through = %d, want %d", page.CoveredThrough, prefixSeq)
	}
}

// --- the four crash windows ------------------------------------------------

// TestEvidenceAcrossTheCrashWindows walks the application's timeline one record
// at a time and states what the journal proves at each point. The windows are
// the cases the recovery exists for: a writer that died before its prefix,
// after it, after its effect, and one that is still alive.
func TestEvidenceAcrossTheCrashWindows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		write       func(t *testing.T, fixture *applyingCommand)
		supersede   bool
		want        CommandApplicationOutcome
		wantEffect  bool
		completable bool
		rejectable  bool
	}{
		{
			name:       "crashed before the prefix",
			write:      func(*testing.T, *applyingCommand) {},
			supersede:  true,
			want:       CommandApplicationAbsent,
			rejectable: true,
		},
		{
			name: "crashed after the prefix and before the effect",
			write: func(t *testing.T, fixture *applyingCommand) {
				mustAppend(t, fixture.writer, applicationPrefix(inboxCommand, inboxRuntime, inboxKind))
			},
			supersede:  true,
			want:       CommandApplicationAbandoned,
			rejectable: true,
		},
		{
			name: "crashed after the effect and before the terminal CAS",
			write: func(t *testing.T, fixture *applyingCommand) {
				mustAppend(t, fixture.writer, applicationPrefix(inboxCommand, inboxRuntime, inboxKind))
				mustAppend(t, fixture.writer, publicEvent("event-effect", `{"applied":true}`))
			},
			supersede:   true,
			want:        CommandApplicationCommitted,
			wantEffect:  true,
			completable: true,
		},
		{
			// Nothing here is a crash: the applier still holds the stream, so
			// the prefix at the tip proves only that an application STARTED.
			name: "still applying, prefix at the tip",
			write: func(t *testing.T, fixture *applyingCommand) {
				mustAppend(t, fixture.writer, applicationPrefix(inboxCommand, inboxRuntime, inboxKind))
			},
			want: CommandApplicationUnresolved,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fixture := startApplying(t, memstore.New())
			test.write(t, fixture)
			successor := fixture.epoch + 1
			if test.supersede {
				successor = fixture.supersede(t)
			}
			// Every window is evaluated after the claim has lapsed, which is
			// the only state in which recovery has anything to settle.
			fixture.clock.set(inboxClaimLapsed)

			app := mustFindApplication(t, fixture.store, fixture.entry)
			if app.Outcome != test.want {
				t.Fatalf("outcome = %q, want %q", app.Outcome, test.want)
			}
			if (app.EffectSeq != 0) != test.wantEffect {
				t.Fatalf("effect seq = %d, want present = %v", app.EffectSeq, test.wantEffect)
			}

			_, completeErr := fixture.completeAs(successor, offeredResult(app))
			if test.completable {
				if completeErr != nil {
					t.Fatalf("a successor could not finish a committed application: %v", completeErr)
				}
			} else {
				assertInboxCode(t, completeErr, InboxErrorEvidence)
				assertInboxUnchanged(t, fixture.store, fixture.entry)
			}

			rejectFixture := startApplying(t, memstore.New())
			test.write(t, rejectFixture)
			rejectEpoch := rejectFixture.epoch + 1
			if test.supersede {
				rejectEpoch = rejectFixture.supersede(t)
			}
			rejectFixture.clock.set(inboxClaimLapsed)
			_, rejectErr := rejectFixture.rejectAs(rejectEpoch)
			if test.rejectable {
				if rejectErr != nil {
					t.Fatalf("a successor could not settle an unfinished application: %v", rejectErr)
				}
			} else {
				assertInboxCode(t, rejectErr, InboxErrorEvidence)
				assertInboxUnchanged(t, rejectFixture.store, rejectFixture.entry)
			}
		})
	}
}

// --- the successor lease's authority ---------------------------------------

// TestASuccessorFinishesAnApplicationAfterTheDeadline is the rule the whole
// task turns on: finishing an application that is already durable is
// CONTINUATION, so the apply deadline does not stop it — while the deadline
// still stops the same successor from taking a NEW claim on the same command.
func TestASuccessorFinishesAnApplicationAfterTheDeadline(t *testing.T) {
	t.Parallel()

	fixture := startApplying(t, memstore.New())
	mustAppend(t, fixture.writer, applicationPrefix(inboxCommand, inboxRuntime, inboxKind))
	mustAppend(t, fixture.writer, publicEvent("event-effect", `{"applied":true}`))
	successor := fixture.supersede(t)
	fixture.clock.set(inboxAfterDue)

	app := mustFindApplication(t, fixture.store, fixture.entry)
	applied, err := fixture.completeAs(successor, correlatedResult(app))
	if err != nil {
		t.Fatalf("a successor could not finish an application after the deadline: %v", err)
	}
	if applied.Record.State != InboxStateApplied {
		t.Fatalf("state = %q, want %q", applied.Record.State, InboxStateApplied)
	}
	// The claim that APPLIED the command is kept: the successor recorded the
	// outcome, it did not produce it.
	if applied.Record.Claim.LeaseEpoch != fixture.epoch {
		t.Fatalf("claim epoch = %d, want the applying lease %d", applied.Record.Claim.LeaseEpoch, fixture.epoch)
	}

	// The successor's recovery authority stops exactly there. The same lease
	// may not start a NEW claim on a command whose deadline has passed, which
	// is what keeps "finish what exists" from becoming "start whatever you
	// like, late". The command is left in claimed with a lapsed claim so that
	// nothing but the deadline can refuse it.
	fresh, freshClock, admitted := inboxFixture(t, memstore.New())
	freshWriter := openRecoveryJournal(t, fresh)
	claimed := mustClaim(t, fresh, admitted, freshWriter.Epoch())
	mustClose(t, freshWriter)
	successorLease := openRecoveryJournal(t, fresh).Epoch()
	freshClock.set(inboxAfterDue)

	request := testClaimRequest(claimed, successorLease)
	request.ClaimExpiresAt = inboxAfterDue.Add(time.Minute)
	_, err = fresh.ClaimCommand(context.Background(), request)
	assertInboxCode(t, err, InboxErrorDeadline)
}

// TestASuccessorMustNameTheCorrelatedEffect holds the recovered completion to
// the journal rather than to the caller's word: the result a successor records
// is the effect the prefix is correlated with, not one it chose.
func TestASuccessorMustNameTheCorrelatedEffect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		spoil func(app CommandApplication) CommandResult
	}{
		{
			name: "another event at the correlated sequence",
			spoil: func(app CommandApplication) CommandResult {
				result := correlatedResult(app)
				result.EventID = "event-somebody-elses"
				return result
			},
		},
		{
			name: "the correlated event at another sequence",
			spoil: func(app CommandApplication) CommandResult {
				result := correlatedResult(app)
				result.JournalSeq = app.EffectSeq + 1
				return result
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fixture := startApplying(t, memstore.New())
			mustAppend(t, fixture.writer, applicationPrefix(inboxCommand, inboxRuntime, inboxKind))
			mustAppend(t, fixture.writer, publicEvent("event-effect", `{"applied":true}`))
			successor := fixture.supersede(t)
			fixture.clock.set(inboxClaimLapsed)

			app := mustFindApplication(t, fixture.store, fixture.entry)
			_, err := fixture.completeAs(successor, test.spoil(app))
			assertInboxCode(t, err, InboxErrorEvidence)
			assertInboxUnchanged(t, fixture.store, fixture.entry)
		})
	}
}

// TestTheApplyingLeaseStillNeedsNoEvidence keeps S4.2's completion contract
// intact: the lease that took the claim records its own application without the
// store reading the journal at all, because it is standing at the effect it
// just committed.
func TestTheApplyingLeaseStillNeedsNoEvidence(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	log := &callLog{}
	base.Ledger = &scriptedLedger{Ledger: base.Ledger, log: log}
	fixture := startApplying(t, base)
	fixture.clock.set(inboxClaimLapsed)

	before := log.count("read")
	if _, err := fixture.completeAs(fixture.epoch, testCompleteRequest(fixture.entry, fixture.epoch).Result); err != nil {
		t.Fatalf("the applying lease could not complete: %v", err)
	}
	if got := log.count("read") - before; got != 0 {
		t.Fatalf("completing under the claim's own epoch read the journal %d times", got)
	}
}

// TestRecoveringRejectionNeedsASuccessorEpoch confines the settlement of an
// expired applying record to the lease that replaced the applier. A caller at
// the applier's own epoch — or none at all — cannot prove the applier is gone.
func TestRecoveringRejectionNeedsASuccessorEpoch(t *testing.T) {
	t.Parallel()

	for _, epochName := range []string{"the applier's own epoch", "no epoch at all"} {
		t.Run(epochName, func(t *testing.T) {
			t.Parallel()

			fixture := startApplying(t, memstore.New())
			fixture.supersede(t)
			fixture.clock.set(inboxClaimLapsed)

			epoch := fixture.epoch
			if epochName == "no epoch at all" {
				epoch = 0
			}
			_, err := fixture.rejectAs(epoch)
			assertInboxCode(t, err, InboxErrorClaimLost)
			assertInboxUnchanged(t, fixture.store, fixture.entry)
		})
	}
}

// TestRecoveringRejectionNeedsTheApplierFenced is the race the fence closes. A
// successor epoch alone is the caller's own assertion; until the journal
// carries a fence above the applying lease, that lease can still commit the
// effect this rejection would orphan.
func TestRecoveringRejectionNeedsTheApplierFenced(t *testing.T) {
	t.Parallel()

	fixture := startApplying(t, memstore.New())
	fixture.clock.set(inboxClaimLapsed)

	_, err := fixture.rejectAs(fixture.epoch + 1)
	if got := assertInboxCode(t, err, InboxErrorEvidence); got.Field != "applying_lease" {
		t.Fatalf("field = %q, want %q", got.Field, "applying_lease")
	}
	assertInboxUnchanged(t, fixture.store, fixture.entry)
}

// --- what a committed effect forbids ---------------------------------------

// TestNoCallerRejectsACommittedEffect is the overwrite the terminal states
// exist to prevent, closed for every caller and every state rather than for the
// reconciler alone. The subject is deliberately the CLAIM HOLDER, whose
// authority nothing else in the machine questions: a rule that only stopped the
// late reconciler would leave the writer best placed to commit the overwrite
// free to do it. The reconciler's half of the same rule is
// TestAPendingCommandIsCheckedForEvidenceToo.
func TestNoCallerRejectsACommittedEffect(t *testing.T) {
	t.Parallel()

	fixture := startApplying(t, memstore.New())
	mustAppend(t, fixture.writer, applicationPrefix(inboxCommand, inboxRuntime, inboxKind))
	mustAppend(t, fixture.writer, publicEvent("event-effect", `{"applied":true}`))

	// The claim is LIVE and the caller is its holder, so every authority rule
	// in the machine admits this rejection. Only the journal refuses it.
	_, err := fixture.rejectAs(fixture.epoch)
	if got := assertInboxCode(t, err, InboxErrorEvidence); got.Field != "application" {
		t.Fatalf("field = %q, want %q", got.Field, "application")
	}
	assertInboxUnchanged(t, fixture.store, fixture.entry)
}

// TestAPendingCommandIsCheckedForEvidenceToo is the reconciler's rule from the
// spec, and it is not decoration: the check is a property of the RECORD's
// journal, so a command whose effect committed cannot be swept away by the
// deadline path either.
func TestAPendingCommandIsCheckedForEvidenceToo(t *testing.T) {
	t.Parallel()

	store, clock, admitted := inboxFixture(t, memstore.New())
	writer := openRecoveryJournal(t, store)
	mustAppend(t, writer, applicationPrefix(inboxCommand, inboxRuntime, inboxKind))
	mustAppend(t, writer, publicEvent("event-effect", `{"applied":true}`))
	clock.set(inboxAfterDue)

	_, err := store.RejectCommand(context.Background(), testRejectRequest(admitted, 0))
	assertInboxCode(t, err, InboxErrorEvidence)
	assertInboxUnchanged(t, store, admitted)
}

// --- correlation refuses what is not this command's ------------------------

// TestCorrelationValidatesBothIdentities is the heart of step three. A prefix
// that names this command's PUBLIC identity but a different runtime identity —
// or a different kind — is not this command's application, and it is not
// treated as absence either: a broken durable mapping is refused, not settled.
func TestCorrelationValidatesBothIdentities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix Envelope
		want   CommandApplicationOutcome
	}{
		{
			name:   "another command entirely",
			prefix: applicationPrefix(otherCommand, otherRuntime, otherKind),
			want:   CommandApplicationAbsent,
		},
		{
			name:   "this command under another runtime identity",
			prefix: applicationPrefix(inboxCommand, otherRuntime, inboxKind),
			want:   CommandApplicationConflicted,
		},
		{
			name:   "this command under another kind",
			prefix: applicationPrefix(inboxCommand, inboxRuntime, otherKind),
			want:   CommandApplicationConflicted,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fixture := startApplying(t, memstore.New())
			mustAppend(t, fixture.writer, test.prefix)
			mustAppend(t, fixture.writer, publicEvent("event-effect", `{"applied":true}`))
			successor := fixture.supersede(t)
			fixture.clock.set(inboxClaimLapsed)

			app := mustFindApplication(t, fixture.store, fixture.entry)
			if app.Outcome != test.want {
				t.Fatalf("outcome = %q, want %q", app.Outcome, test.want)
			}
			_, err := fixture.completeAs(successor, offeredResult(app))
			assertInboxCode(t, err, InboxErrorEvidence)

			// Absence of any correlated prefix is settleable; a broken
			// correlation is not, because the store cannot tell which command
			// the effect belongs to.
			_, rejectErr := fixture.rejectAs(successor)
			if test.want == CommandApplicationAbsent {
				if rejectErr != nil {
					t.Fatalf("a foreign prefix blocked an unrelated command's settlement: %v", rejectErr)
				}
				return
			}
			assertInboxCode(t, rejectErr, InboxErrorEvidence)
		})
	}
}

// TestStackedPrefixesAreNotCorrelatable states the writer contract the effect
// correlation rests on, by proving what happens when it is broken. A prefix
// belongs immediately before its effect; two applications interleaved in one
// stream make the adjacency meaningless, so BOTH become unsettleable rather
// than one of them acquiring the other's effect.
func TestStackedPrefixesAreNotCorrelatable(t *testing.T) {
	t.Parallel()

	// Both positions in the stack are driven. The SECOND one is the dangerous
	// half: the record after it is the FIRST application's effect, so a
	// correlation that only looked at adjacency would hand this command an
	// event that is not its own.
	for _, ours := range []string{"first in the stack", "second in the stack"} {
		t.Run(ours, func(t *testing.T) {
			t.Parallel()

			fixture := startApplying(t, memstore.New())
			mine := applicationPrefix(inboxCommand, inboxRuntime, inboxKind)
			theirs := applicationPrefix(otherCommand, otherRuntime, otherKind)
			if ours == "first in the stack" {
				mustAppend(t, fixture.writer, mine)
				mustAppend(t, fixture.writer, theirs)
			} else {
				mustAppend(t, fixture.writer, theirs)
				mustAppend(t, fixture.writer, mine)
			}
			mustAppend(t, fixture.writer, publicEvent("event-effect", `{"applied":true}`))
			successor := fixture.supersede(t)
			fixture.clock.set(inboxClaimLapsed)

			app := mustFindApplication(t, fixture.store, fixture.entry)
			if app.Outcome != CommandApplicationUnresolved {
				t.Fatalf("outcome = %q, want %q", app.Outcome, CommandApplicationUnresolved)
			}
			if app.EffectSeq != 0 || app.EffectEventID != "" {
				t.Fatalf("a stacked prefix took the following event as its effect: seq %d id %q", app.EffectSeq, app.EffectEventID)
			}
			if _, err := fixture.completeAs(successor, offeredResult(app)); err == nil {
				t.Fatal("a stacked prefix was allowed to claim the following event as its effect")
			}
			_, err := fixture.rejectAs(successor)
			assertInboxCode(t, err, InboxErrorEvidence)
		})
	}
}

// TestASeparatedPrefixIsNotCorrelatable is the other half of the adjacency
// contract: a record between a prefix and its effect breaks the correlation
// rather than being skipped over, because skipping is what would let an
// unrelated later event be adopted as this command's.
//
// The fixture supersedes the writer before asking, which is the load-bearing
// part: re-attachment is what resolves the CONFORMING crash window, and it does
// not resolve this one. The record at prefix+1 is already durable and will
// never become either the effect or a fence, so a contract violation is an
// unsettleable command FOREVER. That is the permanent head-of-line row
// RejectCommand's carry-forward note now attributes to writers rather than to
// crashes, and TestStackedPrefixesAreNotCorrelatable supersedes for the same
// reason.
func TestASeparatedPrefixIsNotCorrelatable(t *testing.T) {
	t.Parallel()

	fixture := startApplying(t, memstore.New())
	mustAppend(t, fixture.writer, applicationPrefix(inboxCommand, inboxRuntime, inboxKind))
	mustAppend(t, fixture.writer, runtimeControl("control-1", `{"note":"between"}`))
	mustAppend(t, fixture.writer, publicEvent("event-effect", `{"applied":true}`))
	fixture.supersede(t)
	fixture.clock.set(inboxClaimLapsed)

	app := mustFindApplication(t, fixture.store, fixture.entry)
	if app.Outcome != CommandApplicationUnresolved {
		t.Fatalf("outcome = %q, want %q", app.Outcome, CommandApplicationUnresolved)
	}
}

// TestALaterApplicationSupersedesAnAbandonedOne pins the precedence across two
// attempts. An application that was abandoned and then retried under a later
// lease is APPLIED: the effect is durable, whichever attempt produced it.
func TestALaterApplicationSupersedesAnAbandonedOne(t *testing.T) {
	t.Parallel()

	fixture := startApplying(t, memstore.New())
	mustAppend(t, fixture.writer, applicationPrefix(inboxCommand, inboxRuntime, inboxKind))
	successor := fixture.supersede(t)
	mustAppend(t, fixture.writer, applicationPrefix(inboxCommand, inboxRuntime, inboxKind))
	effectSeq := mustAppend(t, fixture.writer, publicEvent("event-effect", `{"applied":true}`))
	fixture.clock.set(inboxClaimLapsed)

	app := mustFindApplication(t, fixture.store, fixture.entry)
	if app.Outcome != CommandApplicationCommitted || app.EffectSeq != effectSeq {
		t.Fatalf("outcome = %q effect = %d, want %q at %d", app.Outcome, app.EffectSeq, CommandApplicationCommitted, effectSeq)
	}
	if _, err := fixture.completeAs(successor, correlatedResult(app)); err != nil {
		t.Fatalf("a retried application could not be finished: %v", err)
	}
}

// craftJournal appends raw frames to a session's stream, bypassing the writer.
//
// It exists for the shapes a conforming writer cannot produce — a caller-minted
// opening fence, a prefix stamped with an epoch its writer never held — which
// are exactly the shapes the correlation's conjuncts are there to refuse. A
// guard that only ever meets well-formed history is a guard nothing tests.
func craftJournal(t *testing.T, store *Store, envelopes ...Envelope) {
	t.Helper()
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	tip, err := store.backend.Ledger.Tip(context.Background(), scope.JournalName)
	if err != nil {
		t.Fatalf("Tip: %v", err)
	}
	for _, env := range envelopes {
		frame, err := EncodeEnvelope(env)
		if err != nil {
			t.Fatalf("EncodeEnvelope: %v", err)
		}
		if err := store.backend.Ledger.Append(context.Background(), scope.JournalName, tip, frame); err != nil {
			t.Fatalf("Append: %v", err)
		}
		tip++
	}
}

func openingFence(epoch uint64) Envelope {
	return Envelope{Kind: EnvelopeKindOpeningFence, LeaseEpoch: epoch}
}

func stampedPrefix(epoch uint64, command sessionwire.CommandID, runtime RuntimeCommandID, kind CommandKind) Envelope {
	env := applicationPrefix(command, runtime, kind)
	env.LeaseEpoch = epoch
	return env
}

// TestOnlyAHIGHERFenceEndsAnApplication pins the epoch comparison that makes a
// fence proof rather than decoration. A fence at or below the prefix's own
// epoch is not the successor taking the stream — it cannot be, in a conforming
// journal — and a correlation that accepted one would call an application
// abandoned on the strength of a record its own writer could have produced.
func TestOnlyAHIGHERFenceEndsAnApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		fenceEpoch uint64
		want       CommandApplicationOutcome
	}{
		{name: "below the applying epoch", fenceEpoch: 4, want: CommandApplicationUnresolved},
		{name: "at the applying epoch", fenceEpoch: 5, want: CommandApplicationUnresolved},
		{name: "above the applying epoch", fenceEpoch: 6, want: CommandApplicationAbandoned},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store, _, admitted := inboxFixture(t, memstore.New())
			craftJournal(t, store,
				openingFence(5),
				stampedPrefix(5, inboxCommand, inboxRuntime, inboxKind),
				openingFence(test.fenceEpoch),
			)
			app := mustFindApplication(t, store, admitted)
			if app.Outcome != test.want {
				t.Fatalf("outcome = %q, want %q", app.Outcome, test.want)
			}
		})
	}
}

// TestAMappingThatIsNotARuntimeUUIDCorrelatesWithNothing covers the identity
// the inbox deliberately does not impose a grammar on. A stored mapping that no
// prefix can carry is a broken correlation rather than an absent one: reporting
// absence would make a command whose effect committed under some other spelling
// look settleable.
func TestAMappingThatIsNotARuntimeUUIDCorrelatesWithNothing(t *testing.T) {
	t.Parallel()

	store, _, _ := inboxFixture(t, memstore.New())
	request := testAdmitRequest()
	request.CommandID = otherCommand
	request.ProposedRuntimeCommandID = "not-a-uuid"
	admitted := mustAdmit(t, store, request)

	craftJournal(t, store,
		openingFence(5),
		stampedPrefix(5, otherCommand, inboxRuntime, inboxKind),
		openingFence(6),
	)
	app := mustFindApplication(t, store, admitted)
	if app.Outcome != CommandApplicationConflicted {
		t.Fatalf("outcome = %q, want %q", app.Outcome, CommandApplicationConflicted)
	}
}

// TestASuccessorCannotResumeAnAbandonedApplication pins the mitigation the
// writer obligation rests on: the store offers a re-attached Host no route back
// into an application its predecessor started. It may finish one on evidence or
// settle one on evidence, and it may not resume one — so a prefix written after
// re-attachment is a Host acting outside every transition this package admits.
func TestASuccessorCannotResumeAnAbandonedApplication(t *testing.T) {
	t.Parallel()

	fixture := startApplying(t, memstore.New())
	successor := fixture.supersede(t)
	fixture.clock.set(inboxClaimLapsed)

	// Applying is not re-enterable at any epoch, so the successor cannot get
	// back in through the front door...
	request := testBeginApplyingRequest(fixture.entry, successor)
	request.ClaimExpiresAt = inboxDeadline
	_, err := fixture.store.BeginApplyingCommand(context.Background(), request)
	assertInboxCode(t, err, InboxErrorState)

	// ...nor through a fresh claim, which applying refuses as well.
	claim := testClaimRequest(fixture.entry, successor)
	claim.ClaimExpiresAt = inboxDeadline
	_, err = fixture.store.ClaimCommand(context.Background(), claim)
	assertInboxCode(t, err, InboxErrorState)
	assertInboxUnchanged(t, fixture.store, fixture.entry)
}

// TestARuntimeMappingCorrelatesByVALUENotBySpelling is the assertion that keeps
// the decoded comparison from being a comment.
//
// The inbox stores the runtime identity as the opaque string it was given,
// because it does not own that grammar; the envelope stores a UUID. Comparing
// the two as TEXT passes every other case in this file and is wrong in exactly
// one: a mapping accepted in upper case would correlate with nothing, so a
// command whose effect is durably in the journal would report a broken mapping
// and become permanently unsettleable — neither finishable nor rejectable.
// Parsing the stored mapping and comparing VALUES is what makes the spelling
// the caller happened to send irrelevant to the evidence.
func TestARuntimeMappingCorrelatesByVALUENotBySpelling(t *testing.T) {
	t.Parallel()

	store, _, _ := inboxFixture(t, memstore.New())
	request := testAdmitRequest()
	request.CommandID = otherCommand
	request.ProposedRuntimeCommandID = RuntimeCommandID(strings.ToUpper(string(inboxRuntime)))
	admitted := mustAdmit(t, store, request)
	if admitted.Record.RuntimeCommandID == inboxRuntime {
		t.Fatal("the store normalized the mapping, so this case no longer distinguishes the two comparisons")
	}

	// The prefix carries the same identity in the canonical lower-case form a
	// writer emits.
	craftJournal(t, store,
		openingFence(5),
		stampedPrefix(5, otherCommand, inboxRuntime, inboxKind),
		publicEvent("event-effect", `{"applied":true}`),
	)

	app := mustFindApplication(t, store, admitted)
	if app.Outcome != CommandApplicationCommitted {
		t.Fatalf("outcome = %q, want %q: the correlation compared spellings rather than values", app.Outcome, CommandApplicationCommitted)
	}
	if app.EffectEventID != "event-effect" {
		t.Fatalf("effect = %q, want %q", app.EffectEventID, "event-effect")
	}
}

// TestCorrelationFailsClosedOnAnUnreadableJournal keeps the evidence rule
// honest about what it does not know: a record the reader cannot decode makes
// the whole correlation unavailable rather than making it report absence, which
// would turn a damaged stream into a licence to settle.
func TestCorrelationFailsClosedOnAnUnreadableJournal(t *testing.T) {
	t.Parallel()

	store, _, admitted := inboxFixture(t, memstore.New())
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	if err := store.backend.Ledger.Append(context.Background(), scope.JournalName, 0, []byte("not a frame")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	_, err = store.FindCommandApplication(context.Background(), FindCommandApplicationRequest{
		TenantID:  admitted.Record.TenantID,
		SessionID: admitted.Record.SessionID,
		CommandID: admitted.Record.CommandID,
	})
	var journal *JournalError
	if !errors.As(err, &journal) || journal.Code != JournalErrorIntegrity {
		t.Fatalf("FindCommandApplication over a damaged stream = %v, want a journal integrity failure", err)
	}
	// And the settlement it gates fails with it rather than proceeding.
	_, err = store.RejectCommand(context.Background(), testRejectRequest(admitted, 0))
	if !errors.As(err, &journal) || journal.Code != JournalErrorIntegrity {
		t.Fatalf("RejectCommand over a damaged stream = %v, want a journal integrity failure", err)
	}
	assertInboxUnchanged(t, store, admitted)
}

// TestPrecedenceAcrossSeveralPrefixes states what the correlation does when one
// command has more than one prefix in the stream. The cases are ordered pairs
// of findings, because precedence is only observable when two disagree.
func TestPrecedenceAcrossSeveralPrefixes(t *testing.T) {
	t.Parallel()

	prefix := stampedPrefix(5, inboxCommand, inboxRuntime, inboxKind)
	broken := stampedPrefix(5, inboxCommand, otherRuntime, inboxKind)
	effect := publicEvent("event-effect", `{"applied":true}`)

	tests := []struct {
		name    string
		records []Envelope
		want    CommandApplicationOutcome
	}{
		{
			// A durable effect is a fact, and a later attempt that is still
			// in flight does not unmake it.
			name:    "committed then still applying",
			records: []Envelope{openingFence(5), prefix, effect, prefix},
			want:    CommandApplicationCommitted,
		},
		{
			// A broken correlation is never repaired by a clean prefix
			// elsewhere in the stream, in either order.
			name:    "committed then broken",
			records: []Envelope{openingFence(5), prefix, effect, broken, effect},
			want:    CommandApplicationConflicted,
		},
		{
			name:    "broken then committed",
			records: []Envelope{openingFence(5), broken, effect, prefix, effect},
			want:    CommandApplicationConflicted,
		},
		{
			// Abandonment is the weakest finding: an attempt that failed and
			// was retried to completion is applied.
			name:    "abandoned then committed",
			records: []Envelope{openingFence(5), prefix, openingFence(6), prefix, effect},
			want:    CommandApplicationCommitted,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store, _, admitted := inboxFixture(t, memstore.New())
			craftJournal(t, store, test.records...)
			app := mustFindApplication(t, store, admitted)
			if app.Outcome != test.want {
				t.Fatalf("outcome = %q, want %q", app.Outcome, test.want)
			}
			// A finding that displaces a committed one must take its effect
			// with it. An event left behind by a superseded finding is an
			// event a completion could name while the correlation that
			// justified it no longer stands.
			if hasEffect := app.EffectSeq != 0 || app.EffectEventID != ""; hasEffect != (test.want == CommandApplicationCommitted) {
				t.Fatalf("outcome %q reported effect seq %d id %q", app.Outcome, app.EffectSeq, app.EffectEventID)
			}
		})
	}
}

// TestTheLatestCommittedEffectIsTheCorrelatedOne pins which effect a journal
// carrying two completed applications of one command reports. Both are durable,
// so both are true; the correlation reports the LATEST, because that is the
// state of the session as it stands and a caller reading a record and then
// recording an older event would be describing a session it no longer has.
func TestTheLatestCommittedEffectIsTheCorrelatedOne(t *testing.T) {
	t.Parallel()

	store, _, admitted := inboxFixture(t, memstore.New())
	prefix := stampedPrefix(5, inboxCommand, inboxRuntime, inboxKind)
	craftJournal(t, store,
		openingFence(5),
		prefix, publicEvent("event-first", `{"applied":1}`),
		prefix, publicEvent("event-last", `{"applied":2}`),
	)

	app := mustFindApplication(t, store, admitted)
	if app.Outcome != CommandApplicationCommitted {
		t.Fatalf("outcome = %q, want %q", app.Outcome, CommandApplicationCommitted)
	}
	if app.EffectEventID != "event-last" || app.EffectSeq != 5 {
		t.Fatalf("effect = %q at %d, want %q at 5", app.EffectEventID, app.EffectSeq, "event-last")
	}
}

// TestCorrelationDoesNotReadAnEmptyStream holds the walk to the Ledger contract
// rather than to what one provider tolerates. A session that has never been
// written has no records to read from, and a reader that issued the read anyway
// would be relying on a behaviour storage does not promise — the same reason
// walkJournal declines it.
func TestCorrelationDoesNotReadAnEmptyStream(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	log := &callLog{}
	base.Ledger = &scriptedLedger{Ledger: base.Ledger, log: log}
	store, _, admitted := inboxFixture(t, base)

	app := mustFindApplication(t, store, admitted)
	if app.Outcome != CommandApplicationAbsent || app.CapturedTip != 0 {
		t.Fatalf("correlation over an empty stream = %+v", app)
	}
	if log.count("tip") == 0 {
		t.Fatal("the correlation never asked for the tip")
	}
	if got := log.count("read"); got != 0 {
		t.Fatalf("the correlation read an empty stream %d times", got)
	}
}

// TestCorrelationStopsAtTheTipItCaptured is the snapshot boundary. A record
// committed while the walk is in progress is not evidence for a decision that
// was already being made, and CapturedTip is what tells a caller which journal
// its answer is true of.
func TestCorrelationStopsAtTheTipItCaptured(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	scripted := &scriptedLedger{Ledger: base.Ledger}
	base.Ledger = scripted
	store, _, admitted := inboxFixture(t, base)
	writer := openRecoveryJournal(t, store)
	fenceTip := writer.Sequence()

	// The effect lands after the tip has been read and before the walk reaches
	// it, which is the only ordering in which the bound is observable.
	var once sync.Once
	scripted.onTip = func(string) {
		once.Do(func() {
			mustAppend(t, writer, applicationPrefix(inboxCommand, inboxRuntime, inboxKind))
			mustAppend(t, writer, publicEvent("event-effect", `{"applied":true}`))
		})
	}

	app := mustFindApplication(t, store, admitted)
	if app.Outcome != CommandApplicationAbsent {
		t.Fatalf("outcome = %q, want %q: the walk read past the tip it captured", app.Outcome, CommandApplicationAbsent)
	}
	if app.CapturedTip != fenceTip {
		t.Fatalf("captured tip = %d, want %d", app.CapturedTip, fenceTip)
	}
}

// --- ordering and races ----------------------------------------------------

// TestRecoveryReadsTheRecordThenTheJournalThenWrites proves the order the
// evidence rule depends on rather than assuming it. Evidence read AFTER the
// compare-and-swap would settle the command against a journal state that had
// not been consulted when the decision was made.
func TestRecoveryReadsTheRecordThenTheJournalThenWrites(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	log := &callLog{}
	base.Ledger = &scriptedLedger{Ledger: base.Ledger, log: log}
	base.OrderedIndex = &loggingOrdered{OrderedIndex: base.OrderedIndex, log: log}

	fixture := startApplying(t, base)
	mustAppend(t, fixture.writer, applicationPrefix(inboxCommand, inboxRuntime, inboxKind))
	mustAppend(t, fixture.writer, publicEvent("event-effect", `{"applied":true}`))
	successor := fixture.supersede(t)
	fixture.clock.set(inboxClaimLapsed)
	app := mustFindApplication(t, fixture.store, fixture.entry)

	log.reset()
	if _, err := fixture.completeAs(successor, correlatedResult(app)); err != nil {
		t.Fatalf("CompleteCommand: %v", err)
	}
	want := []string{"ordered:get", "tip", "read", "ordered:update"}
	if got := log.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("call order = %v, want %v", got, want)
	}
}

// TestOneSettlementWinsAgainstAConcurrentRecovery is the property that survives
// over time: a successor finishing an application and a reconciler settling the
// same record race on ONE revision, so exactly one of them writes and the loser
// is told to re-read.
func TestOneSettlementWinsAgainstAConcurrentRecovery(t *testing.T) {
	t.Parallel()

	fixture := startApplying(t, memstore.New())
	mustAppend(t, fixture.writer, applicationPrefix(inboxCommand, inboxRuntime, inboxKind))
	mustAppend(t, fixture.writer, publicEvent("event-effect", `{"applied":true}`))
	successor := fixture.supersede(t)
	fixture.clock.set(inboxClaimLapsed)
	app := mustFindApplication(t, fixture.store, fixture.entry)

	var wait sync.WaitGroup
	results := make([]error, 2)
	start := make(chan struct{})
	for i := range results {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, results[i] = fixture.completeAs(successor, correlatedResult(app))
		}()
	}
	close(start)
	wait.Wait()

	winners := 0
	for _, err := range results {
		switch {
		case err == nil:
			winners++
		default:
			var inbox *InboxError
			if !errors.As(err, &inbox) || (inbox.Code != InboxErrorConflict && inbox.Code != InboxErrorTerminal) {
				t.Fatalf("loser = %v, want a conflict or a terminal record", err)
			}
		}
	}
	if winners != 1 {
		t.Fatalf("%d writers won one revision", winners)
	}
}

// --- the public surface ----------------------------------------------------

var _ func(*Store, context.Context, FindCommandApplicationRequest) (CommandApplication, error) = (*Store).FindCommandApplication

func TestInboxRecoveryOperationsRefuseAfterClose(t *testing.T) {
	store, _, admitted := inboxFixture(t, memstore.New())
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	operations := map[string]func() error{
		"FindCommandApplication": func() error {
			_, err := store.FindCommandApplication(context.Background(), FindCommandApplicationRequest{
				TenantID:  admitted.Record.TenantID,
				SessionID: admitted.Record.SessionID,
				CommandID: admitted.Record.CommandID,
			})
			return err
		},
	}

	declared := declaredStoreOperations(t, "inbox_recovery.go")
	for name := range declared {
		if operations[name] == nil {
			t.Errorf("inbox_recovery.go declares the public operation %s and this test does not exercise it", name)
		}
	}
	for name := range operations {
		if !declared[name] {
			t.Errorf("this test exercises %s, which inbox_recovery.go no longer declares", name)
		}
	}
	for name, call := range operations {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.As(err, new(*StoreClosedError)) {
				t.Fatalf("%s after Close = %T %v, want *StoreClosedError", name, err, err)
			}
		})
	}
}
