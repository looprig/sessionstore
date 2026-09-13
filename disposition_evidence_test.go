// The PRODUCTION settlement evidence reader: the one implementation of
// DispositionEvidenceReader in this module, over the session's own bound
// journal. Until this file existed the only implementation anywhere was
// disposition_settlement_test.go's fake.
package sessionstore

import (
	"context"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// appendRawFrame appends one encoded envelope straight to the ledger, which is
// the shape a runtime actually writes: harness calls EncodeEnvelope and appends
// the bytes itself rather than going through this package's JournalWriter, so
// stampWriterOwnedFields never runs and every writer-owned field in the record
// is a CLAIM. The fixtures must be able to make a false one.
func appendRawFrame(t *testing.T, s *Store, env Envelope) uint64 {
	t.Helper()
	return appendRawFrameTo(t, s, catalogTenant, catalogSession, env)
}

func appendRawFrameTo(t *testing.T, s *Store, tenant sessionwire.TenantID, session sessionwire.SessionID, env Envelope) uint64 {
	t.Helper()
	scope, err := s.deriveSessionScope(tenant, session)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := EncodeEnvelope(env)
	if err != nil {
		t.Fatalf("EncodeEnvelope: %v", err)
	}
	ctx := context.Background()
	tip, err := s.backend.Ledger.Tip(ctx, scope.JournalName)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.AppendDefinite(ctx, s.backend.Ledger, scope.JournalName, tip, frame); err != nil {
		t.Fatal(err)
	}
	return tip + 1
}

// journalEvidenceFixture leaves one command applying under settlementAttempt,
// with the store reading its own journal for evidence.
func journalEvidenceFixture(t *testing.T, opts ...Option) (*Store, DispositionInboxEntry) {
	t.Helper()
	opts = append([]Option{WithClock(newMovableClock(settlementNow)), WithJournalDispositionEvidence()}, opts...)
	s := openStore(t, memstore.New(), opts...)
	createDispositionCatalog(t, s)
	admitted, _, err := s.AdmitDispositionCommand(context.Background(), dispositionRequest())
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	claimed := fileDispositionClaim(t, s, admitted, DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: settlementExpiry})
	applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	return s, applying
}

// dispositionFrameFor is a disposition record that correlates with the store's
// own record on every identity, so a test that changes ONE of them is asserting
// about that identity alone.
func dispositionFrameFor(entry DispositionInboxEntry, kind DispositionOutcomeKind, leaseEpoch uint64) Envelope {
	d := entry.Record.Descriptor
	runtime, _ := uuid.Parse(string(d.RuntimeCommandID))
	return Envelope{
		Kind:                EnvelopeKindCommandDisposition,
		CommandID:           d.CommandID,
		LeaseEpoch:          leaseEpoch,
		RuntimeCommandID:    runtime,
		CommandKind:         string(d.Kind),
		AttemptID:           string(settlementAttempt),
		AttemptJournalEpoch: uint64(settlementJournal),
		DispositionKind:     string(kind),
	}
}

func readEvidence(t *testing.T, s *Store, entry DispositionInboxEntry) (DispositionEvidence, error) {
	t.Helper()
	d := entry.Record.Descriptor
	return s.ReadDispositionEvidence(context.Background(), DispositionEvidenceRequest{
		TenantID:         d.TenantID,
		SessionID:        d.SessionID,
		CommandID:        d.CommandID,
		Kind:             d.Kind,
		RuntimeCommandID: d.RuntimeCommandID,
		Binding:          d.Binding,
		Attempt:          *entry.Record.Attempt,
	})
}

// An application is authored by the attempt's own grant, so the author epoch is
// the nearest preceding opening fence's and there is no author fence to name.
func TestJournalDispositionEvidenceReadsAnApplication(t *testing.T) {
	s, applying := journalEvidenceFixture(t)
	appendRawFrame(t, s, openingFence(uint64(settlementJournal)))
	seq := appendRawFrame(t, s, dispositionFrameFor(applying, DispositionApplied, uint64(settlementJournal)))

	evidence, err := readEvidence(t, s, applying)
	if err != nil {
		t.Fatalf("ReadDispositionEvidence: %v", err)
	}
	want := DispositionEvidence{
		AttemptID:           settlementAttempt,
		Kind:                DispositionApplied,
		AttemptJournalEpoch: settlementJournal,
		AuthorJournalEpoch:  settlementJournal,
		DispositionSeq:      seq,
	}
	if evidence != want {
		t.Fatalf("evidence = %+v, want %+v", evidence, want)
	}
	// The store settles on it end to end, which is the property that matters:
	// the reader and the verifier agree about the same record.
	settled, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
	if err != nil || !ok || settled.Record.State != InboxStateApplied {
		t.Fatalf("settlement: %+v %v %v", settled, ok, err)
	}
}

// A refusal is authored by the same grant and reaches the same terminal
// vocabulary, but settles as rejected. It is driven through the production
// reader because the arm exists for a LIVE runtime, which is precisely the
// journal shape a fake cannot vouch for.
func TestJournalDispositionEvidenceReadsARefusal(t *testing.T) {
	s, applying := journalEvidenceFixture(t)
	appendRawFrame(t, s, openingFence(uint64(settlementJournal)))
	appendRawFrame(t, s, dispositionFrameFor(applying, DispositionRefused, uint64(settlementJournal)))

	settled, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
	if err != nil || !ok {
		t.Fatalf("settlement: %+v %v %v", settled, ok, err)
	}
	if settled.Record.State != InboxStateRejected || settled.Record.Outcome.Kind != DispositionRefused {
		t.Fatalf("refusal settled as %q/%+v", settled.Record.State, settled.Record.Outcome)
	}
	if settled.Record.Outcome.AuthorFenceSeq != 0 {
		t.Fatalf("a refusal named an author fence: %+v", settled.Record.Outcome)
	}
}

// A recovery closure is authored by a strictly later grant, and the fence the
// evidence names is that author's own opening fence — the nearest one PRECEDING
// the closure rather than the FIRST in the journal.
//
// This case probes only that half. The other half — nearest preceding rather
// than LAST — cannot be seen here, because nothing is written after the
// disposition, and a walk that resolved the grant from the final fence state
// would agree with this fixture at every point.
// TestJournalDispositionEvidenceIgnoresFencesAfterTheDisposition is what
// separates them.
func TestJournalDispositionEvidenceClosureNamesItsOwnFence(t *testing.T) {
	s, applying := journalEvidenceFixture(t)
	appendRawFrame(t, s, openingFence(uint64(settlementJournal)))
	appendRawFrame(t, s, publicEvent("event-1", `{"n":1}`))
	fenceSeq := appendRawFrame(t, s, openingFence(uint64(settlementJournal)+1))
	seq := appendRawFrame(t, s, dispositionFrameFor(applying, DispositionNotApplied, uint64(settlementJournal)+1))

	evidence, err := readEvidence(t, s, applying)
	if err != nil {
		t.Fatalf("ReadDispositionEvidence: %v", err)
	}
	if evidence.AuthorJournalEpoch != settlementJournal+1 || evidence.AuthorFenceSeq != fenceSeq || evidence.DispositionSeq != seq {
		t.Fatalf("evidence = %+v, want author %d fenced at %d", evidence, settlementJournal+1, fenceSeq)
	}
	if evidence.AttemptJournalEpoch != settlementJournal {
		t.Fatalf("the closure lost the attempt grant it is about: %+v", evidence)
	}
	settled, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc+1))
	if err != nil || !ok || settled.Record.State != InboxStateRejected {
		t.Fatalf("settlement: %+v %v %v", settled, ok, err)
	}
}

// Absence is NOT evidence. A reader that returned an empty DispositionEvidence
// here would settle nothing — the verifier refuses the zero value — but it would
// also report a journal fault as a finding about the command, so the refusal is
// asserted at the reader rather than left to the layer above.
func TestJournalDispositionEvidenceRefusesAbsence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(*testing.T, *Store, DispositionInboxEntry)
	}{
		{"an empty journal", func(*testing.T, *Store, DispositionInboxEntry) {}},
		{"a journal with no disposition at all", func(t *testing.T, s *Store, _ DispositionInboxEntry) {
			appendRawFrame(t, s, openingFence(uint64(settlementJournal)))
			appendRawFrame(t, s, publicEvent("event-1", `{"n":1}`))
		}},
		{"a disposition about another attempt", func(t *testing.T, s *Store, applying DispositionInboxEntry) {
			appendRawFrame(t, s, openingFence(uint64(settlementJournal)))
			other := dispositionFrameFor(applying, DispositionApplied, uint64(settlementJournal))
			other.AttemptID = "attempt/other"
			appendRawFrame(t, s, other)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, applying := journalEvidenceFixture(t)
			tc.build(t, s, applying)
			evidence, err := readEvidence(t, s, applying)
			inboxError := assertInboxCode(t, err, InboxErrorEvidence)
			if inboxError.Field != "disposition" {
				t.Fatalf("Field = %q, want disposition", inboxError.Field)
			}
			if evidence != (DispositionEvidence{}) {
				t.Fatalf("a refusal returned evidence: %+v", evidence)
			}
			_, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
			if ok || err == nil {
				t.Fatalf("absence settled the command: %v %v", ok, err)
			}
			got, err := s.GetDispositionCommand(context.Background(), GetDispositionCommandRequest{TenantID: applying.Record.Descriptor.TenantID, SessionID: applying.Record.Descriptor.SessionID, CommandID: applying.Record.Descriptor.CommandID})
			if err != nil || got.Record.State != InboxStateApplying {
				t.Fatalf("record moved: %+v %v", got, err)
			}
		})
	}
}

// Two records naming one attempt are two answers, and the reader has no rule
// for choosing between them. They cannot be duplicates of each other either:
// they sit at different sequences, so they are necessarily distinct records.
func TestJournalDispositionEvidenceRefusesMoreThanOneRecord(t *testing.T) {
	for _, tc := range []struct {
		name   string
		second DispositionOutcomeKind
	}{
		{"contradicting each other", DispositionNotApplied},
		{"agreeing with each other", DispositionApplied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, applying := journalEvidenceFixture(t)
			appendRawFrame(t, s, openingFence(uint64(settlementJournal)))
			appendRawFrame(t, s, dispositionFrameFor(applying, DispositionApplied, uint64(settlementJournal)))
			appendRawFrame(t, s, dispositionFrameFor(applying, tc.second, uint64(settlementJournal)))

			_, err := readEvidence(t, s, applying)
			inboxError := assertInboxCode(t, err, InboxErrorEvidence)
			if inboxError.Field != "disposition" {
				t.Fatalf("Field = %q, want disposition", inboxError.Field)
			}
		})
	}

	// Both rows above put the two records on ONE page, so neither can see a
	// walk that stops as soon as it has a match. The refusal is advertised in
	// the reader's own doc comment, and an early exit is exactly the shape a
	// later "obvious" optimisation takes — it would quietly convert "more than
	// one is refused" into "the first one wins" with the whole suite green.
	t.Run("separated by a page boundary", func(t *testing.T) {
		s, applying := journalEvidenceFixture(t, WithLimits(Limits{MaxPageSize: 2}))
		appendRawFrame(t, s, openingFence(uint64(settlementJournal)))
		first := appendRawFrame(t, s, dispositionFrameFor(applying, DispositionApplied, uint64(settlementJournal)))
		appendRawFrame(t, s, publicEvent("event-a", `{"n":1}`))
		appendRawFrame(t, s, publicEvent("event-b", `{"n":2}`))
		second := appendRawFrame(t, s, dispositionFrameFor(applying, DispositionApplied, uint64(settlementJournal)))

		// Non-vacuity, asserted against the reader's OWN pagination rather than
		// arithmetic on the page size: the second record must not be reachable
		// on the first page, or this row is the single-page case again.
		d := applying.Record.Descriptor
		page, err := s.ReadRuntimeJournal(context.Background(), ReadRuntimeJournalRequest{TenantID: d.TenantID, SessionID: d.SessionID})
		if err != nil {
			t.Fatal(err)
		}
		if page.NextCursor == "" {
			t.Fatal("vacuous: the whole journal fits in one page")
		}
		for _, record := range page.Records {
			if record.Seq == second {
				t.Fatalf("vacuous: both dispositions (%d, %d) are on the first page", first, second)
			}
		}

		_, err = readEvidence(t, s, applying)
		inboxError := assertInboxCode(t, err, InboxErrorEvidence)
		if inboxError.Field != "disposition" {
			t.Fatalf("Field = %q, want disposition", inboxError.Field)
		}
	})
}

// A record that names this attempt but disagrees about which COMMAND it is
// about is a conflict, never an absence: stepping over it would report "no
// disposition" for a journal that plainly holds one, and a Host would then take
// a recovery path on a command a runtime may have accepted.
func TestJournalDispositionEvidenceCrossChecksIdentitiesAsConflict(t *testing.T) {
	for _, tc := range []struct {
		name    string
		corrupt func(*Envelope)
		field   string
	}{
		{name: "another command id", corrupt: func(e *Envelope) { e.CommandID = "public/command:other" }, field: "command_id"},
		{name: "another runtime command id", corrupt: func(e *Envelope) { e.RuntimeCommandID = uuid.UUID{9} }, field: "runtime_command_id"},
		{name: "another command kind", corrupt: func(e *Envelope) { e.CommandKind = "other_kind" }, field: "command_kind"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, applying := journalEvidenceFixture(t)
			appendRawFrame(t, s, openingFence(uint64(settlementJournal)))
			frame := dispositionFrameFor(applying, DispositionApplied, uint64(settlementJournal))
			tc.corrupt(&frame)
			appendRawFrame(t, s, frame)

			_, err := readEvidence(t, s, applying)
			inboxError := assertInboxCode(t, err, InboxErrorEvidence)
			if inboxError.Field != tc.field {
				t.Fatalf("Field = %q, want %q — a conflicting record was read as an absence", inboxError.Field, tc.field)
			}
		})
	}
}

// The author grant is DERIVED from the nearest preceding opening fence and the
// record's own LeaseEpoch is only checked against it.
//
// This is required rather than defensive. Harness bypasses this package's
// JournalWriter — it encodes an envelope and appends the bytes itself — so
// stampWriterOwnedFields never runs and the stored LeaseEpoch is a claim by the
// writer. Believing it would let a record under grant 9 settle as a recovery
// closure authored by grant 10.
func TestJournalDispositionEvidenceDerivesTheAuthorGrantFromTheFence(t *testing.T) {
	t.Run("a claimed epoch above its fence is refused", func(t *testing.T) {
		s, applying := journalEvidenceFixture(t)
		appendRawFrame(t, s, openingFence(uint64(settlementJournal)))
		appendRawFrame(t, s, dispositionFrameFor(applying, DispositionNotApplied, uint64(settlementJournal)+1))

		_, err := readEvidence(t, s, applying)
		inboxError := assertInboxCode(t, err, InboxErrorEvidence)
		if inboxError.Field != "lease_epoch" {
			t.Fatalf("Field = %q, want lease_epoch", inboxError.Field)
		}
	})

	t.Run("a claimed epoch below its fence is refused", func(t *testing.T) {
		s, applying := journalEvidenceFixture(t)
		appendRawFrame(t, s, openingFence(uint64(settlementJournal)))
		appendRawFrame(t, s, openingFence(uint64(settlementJournal)+1))
		appendRawFrame(t, s, dispositionFrameFor(applying, DispositionApplied, uint64(settlementJournal)))

		_, err := readEvidence(t, s, applying)
		inboxError := assertInboxCode(t, err, InboxErrorEvidence)
		if inboxError.Field != "lease_epoch" {
			t.Fatalf("Field = %q, want lease_epoch", inboxError.Field)
		}
	})

	t.Run("a disposition before any fence is refused", func(t *testing.T) {
		s, applying := journalEvidenceFixture(t)
		appendRawFrame(t, s, dispositionFrameFor(applying, DispositionApplied, uint64(settlementJournal)))

		_, err := readEvidence(t, s, applying)
		inboxError := assertInboxCode(t, err, InboxErrorEvidence)
		if inboxError.Field != "opening_fence" {
			t.Fatalf("Field = %q, want opening_fence", inboxError.Field)
		}
	})
}

// The walk is not bounded by one page. A journal longer than the store's page
// size must still be searched to its captured tip, and the fence tracking must
// survive the page boundary — the fence and the disposition it authored land on
// different pages here.
func TestJournalDispositionEvidenceSpansPages(t *testing.T) {
	s, applying := journalEvidenceFixture(t, WithLimits(Limits{MaxPageSize: 2}))
	appendRawFrame(t, s, openingFence(uint64(settlementJournal)))
	for i := 0; i < 7; i++ {
		appendRawFrame(t, s, publicEvent("event-"+string(rune('a'+i)), `{"n":1}`))
	}
	seq := appendRawFrame(t, s, dispositionFrameFor(applying, DispositionApplied, uint64(settlementJournal)))
	if seq <= 2 {
		t.Fatalf("vacuous: the disposition landed at seq %d, inside the first page", seq)
	}

	evidence, err := readEvidence(t, s, applying)
	if err != nil {
		t.Fatalf("ReadDispositionEvidence: %v", err)
	}
	if evidence.DispositionSeq != seq || evidence.AuthorJournalEpoch != settlementJournal {
		t.Fatalf("evidence = %+v, want seq %d", evidence, seq)
	}
}

// The store is not its own evidence reader unless it was asked to be, and the
// two ways of asking do not stack: a store with both configured has two answers
// to the same question and no rule for choosing.
func TestJournalDispositionEvidenceOptionIsExclusiveAndOptIn(t *testing.T) {
	t.Run("unconfigured refuses to settle", func(t *testing.T) {
		s := openStore(t, memstore.New(), WithClock(newMovableClock(settlementNow)))
		createDispositionCatalog(t, s)
		admitted, _, err := s.AdmitDispositionCommand(context.Background(), dispositionRequest())
		if err != nil {
			t.Fatal(err)
		}
		claimed := fileDispositionClaim(t, s, admitted, DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: settlementExpiry})
		applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
		if err != nil {
			t.Fatal(err)
		}
		_, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
		if ok {
			t.Fatal("an unconfigured store settled a command")
		}
		inboxError := assertInboxCode(t, err, InboxErrorEvidence)
		if inboxError.Field != "reader" {
			t.Fatalf("Field = %q, want reader", inboxError.Field)
		}
	})

	for _, tc := range []struct {
		name string
		opts []Option
	}{
		{"journal then injected", []Option{WithJournalDispositionEvidence(), WithDispositionEvidence(&fakeEvidence{})}},
		{"injected then journal", []Option{WithDispositionEvidence(&fakeEvidence{}), WithJournalDispositionEvidence()}},
		{"journal twice", []Option{WithJournalDispositionEvidence(), WithJournalDispositionEvidence()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Open(context.Background(), memstore.New(), tc.opts...)
			var invalid *InvalidOptionError
			if !errors.As(err, &invalid) || invalid.Field != "DispositionEvidence" {
				t.Fatalf("Open error = %v, want an InvalidOptionError on DispositionEvidence", err)
			}
		})
	}
}

// The reader's scope is the REQUEST's own session, and a disposition sitting in
// another session's journal is not evidence about this command.
//
// This is the fixture-constant gap the sweep found: every other case in this
// file runs on one tenant and one session, so nothing else in the table could
// tell a reader that resolved the right session from one that resolved any
// session at all. The frames written here are byte-identical to the ones that
// settle the command in TestJournalDispositionEvidenceReadsAnApplication; only
// the journal they land in differs.
func TestJournalDispositionEvidenceIsScopedToTheRequestSession(t *testing.T) {
	const otherSession = sessionwire.SessionID("session-b")

	s, applying := journalEvidenceFixture(t)
	appendRawFrameTo(t, s, catalogTenant, otherSession, openingFence(uint64(settlementJournal)))
	appendRawFrameTo(t, s, catalogTenant, otherSession, dispositionFrameFor(applying, DispositionApplied, uint64(settlementJournal)))
	// Non-vacuity: the same frames in this session's OWN journal do settle it,
	// so the refusal below is about the scope and not about the frames.
	if _, err := readEvidence(t, s, applying); err == nil {
		t.Fatal("another session's disposition was read as evidence")
	}
	appendRawFrame(t, s, openingFence(uint64(settlementJournal)))
	appendRawFrame(t, s, dispositionFrameFor(applying, DispositionApplied, uint64(settlementJournal)))
	if _, err := readEvidence(t, s, applying); err != nil {
		t.Fatalf("vacuous: the same frames in this session are not evidence either: %v", err)
	}
}

// A fence written AFTER the disposition is not the disposition's fence.
//
// This is the other half of "nearest preceding", and it is the direction that
// was unprobed: every other fixture in this file writes the disposition LAST,
// so a reader that resolved the author grant from the journal's FINAL fence
// state would have agreed with all of them. It does not agree here.
//
// The uncovered direction is also the NORMAL one rather than an edge case. A
// successor settling a closure has already acquired residency and opened its
// own journal, which mints a fence; any ordinary traffic after a disposition
// puts records — lease-turnover fences included — behind it. Resolving from the
// last fence would refuse an ordinary settlement with lease_epoch, which is the
// same error a forged epoch earns, so the failure would read as an attack.
func TestJournalDispositionEvidenceIgnoresFencesAfterTheDisposition(t *testing.T) {
	t.Run("an application, then a lease turnover", func(t *testing.T) {
		s, applying := journalEvidenceFixture(t)
		appendRawFrame(t, s, openingFence(uint64(settlementJournal)))
		seq := appendRawFrame(t, s, dispositionFrameFor(applying, DispositionApplied, uint64(settlementJournal)))
		later := appendRawFrame(t, s, openingFence(uint64(settlementJournal)+1))
		if later <= seq {
			t.Fatalf("vacuous: the later fence landed at %d, not after the disposition at %d", later, seq)
		}

		evidence, err := readEvidence(t, s, applying)
		if err != nil {
			t.Fatalf("a fence after the disposition refused it: %v", err)
		}
		want := DispositionEvidence{
			AttemptID:           settlementAttempt,
			Kind:                DispositionApplied,
			AttemptJournalEpoch: settlementJournal,
			AuthorJournalEpoch:  settlementJournal,
			DispositionSeq:      seq,
		}
		if evidence != want {
			t.Fatalf("evidence = %+v, want %+v", evidence, want)
		}
		settled, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
		if err != nil || !ok || settled.Record.State != InboxStateApplied {
			t.Fatalf("settlement: %+v %v %v", settled, ok, err)
		}
	})

	// The closure arm additionally pins the SEQUENCE the evidence names, which
	// the application arm cannot: an application names no author fence at all,
	// so a reader reading the wrong fence's sequence would be invisible there.
	t.Run("a closure, then a further lease turnover", func(t *testing.T) {
		s, applying := journalEvidenceFixture(t)
		appendRawFrame(t, s, openingFence(uint64(settlementJournal)))
		authorFence := appendRawFrame(t, s, openingFence(uint64(settlementJournal)+1))
		seq := appendRawFrame(t, s, dispositionFrameFor(applying, DispositionNotApplied, uint64(settlementJournal)+1))
		later := appendRawFrame(t, s, openingFence(uint64(settlementJournal)+2))
		if later <= seq {
			t.Fatalf("vacuous: the later fence landed at %d, not after the disposition at %d", later, seq)
		}

		evidence, err := readEvidence(t, s, applying)
		if err != nil {
			t.Fatalf("a fence after the closure refused it: %v", err)
		}
		if evidence.AuthorJournalEpoch != settlementJournal+1 {
			t.Fatalf("author grant = %d, want the closure's own %d", evidence.AuthorJournalEpoch, settlementJournal+1)
		}
		if evidence.AuthorFenceSeq != authorFence {
			t.Fatalf("author fence = %d, want the fence that authored it at %d", evidence.AuthorFenceSeq, authorFence)
		}
		settled, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc+1))
		if err != nil || !ok || settled.Record.State != InboxStateRejected {
			t.Fatalf("settlement: %+v %v %v", settled, ok, err)
		}
	})
}

// The attempt grant the evidence carries is the RECORD's claim, never the
// store's own attempt.
//
// The two are equal in every ordinary case, which is exactly why this needs its
// own fixture: a reader that substituted the store's grant would agree with
// every other test in this file and would silently disable the verifier's
// foreign-grant check, since e.AttemptJournalEpoch != attempt.JournalEpoch would
// have become a comparison of a value with itself. The rule the reader's own doc
// sells — "a disposition naming the same attempt under a DIFFERENT journal grant
// is evidence about something else" — would then hold of nothing.
//
// The reader does not judge the claim; it reports it and the verifier refuses
// it. Both halves are asserted, because the reader passing a foreign grant
// through is only safe if something downstream still refuses it.
func TestJournalDispositionEvidenceCarriesTheRecordsOwnAttemptGrant(t *testing.T) {
	foreign := settlementJournal + 5

	s, applying := journalEvidenceFixture(t)
	appendRawFrame(t, s, openingFence(uint64(settlementJournal)))
	frame := dispositionFrameFor(applying, DispositionApplied, uint64(settlementJournal))
	frame.AttemptJournalEpoch = uint64(foreign)
	seq := appendRawFrame(t, s, frame)
	if applying.Record.Attempt.JournalEpoch == foreign {
		t.Fatal("vacuous: the foreign grant is the store's own attempt grant")
	}

	evidence, err := readEvidence(t, s, applying)
	if err != nil {
		t.Fatalf("ReadDispositionEvidence: %v", err)
	}
	if evidence.AttemptJournalEpoch != foreign {
		t.Fatalf("attempt grant = %d, want the record's own claim %d: the reader substituted a grant", evidence.AttemptJournalEpoch, foreign)
	}
	if evidence.DispositionSeq != seq || evidence.AuthorJournalEpoch != settlementJournal {
		t.Fatalf("evidence = %+v", evidence)
	}

	_, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
	if ok {
		t.Fatal("a disposition authored under a foreign attempt grant settled the command")
	}
	inboxError := assertInboxCode(t, err, InboxErrorEvidence)
	if inboxError.Field != "attempt_journal_epoch" {
		t.Fatalf("Field = %q, want attempt_journal_epoch", inboxError.Field)
	}
}
