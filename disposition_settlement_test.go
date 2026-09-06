package sessionstore

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// The disposition protocol's claim edge (pending -> claimed) is deliberately not
// implemented by this step, so no exported API can put a record into the state
// this step's compare-and-swap starts from. Tests file one directly through the
// package's own encoder and the same ordered-index update the transition uses,
// which is the smallest thing that can stand in for the missing producer: it
// writes bytes the codec accepts and nothing else.
func fileDispositionClaim(t *testing.T, s *Store, entry DispositionInboxEntry, claim DispositionClaim) DispositionInboxEntry {
	t.Helper()
	record := entry.Record
	record.State = InboxStateClaimed
	record.Claim = &claim
	value, record, err := encodeDispositionInboxRecord(record)
	if err != nil {
		t.Fatalf("encode claimed record: %v", err)
	}
	scope, err := s.deriveSessionScope(record.Descriptor.TenantID, record.Descriptor.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := s.backend.OrderedIndex.Update(context.Background(), dispositionInboxID(scope, record.Descriptor.CommandID), entry.Revision, value, storage.Rank{}, dispositionInboxDue(record))
	if err != nil {
		t.Fatalf("file claimed record: %v", err)
	}
	filed, err := dispositionInboxEntryFor(stored, scope, record.Descriptor.TenantID, record.Descriptor.SessionID, record.Descriptor.CommandID, record.Descriptor.Binding)
	if err != nil {
		t.Fatalf("filed claimed record: %v", err)
	}
	return filed
}

var (
	settlementNow      = time.Date(2026, 8, 30, 11, 40, 0, 0, time.UTC)
	settlementExpiry   = time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	settlementAttempt  = DispositionAttemptID("attempt/A:B")
	settlementResidenc = ResidencyEpoch(4)
	settlementJournal  = JournalEpoch(9)
)

// settlementFixture admits one disposition command and leaves it claimed under
// settlementResidenc, which is the state a dispatch attempt starts from.
func settlementFixture(t *testing.T, reader DispositionEvidenceReader, opts ...Option) (*Store, *movableClock, DispositionInboxEntry) {
	t.Helper()
	clock := newMovableClock(settlementNow)
	opts = append([]Option{WithClock(clock)}, opts...)
	if reader != nil {
		opts = append(opts, WithDispositionEvidence(reader))
	}
	s := openStore(t, memstore.New(), opts...)
	createDispositionCatalog(t, s)
	entry, _, err := s.AdmitDispositionCommand(context.Background(), dispositionRequest())
	if err != nil {
		t.Fatalf("AdmitDispositionCommand: %v", err)
	}
	return s, clock, fileDispositionClaim(t, s, entry, DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: settlementExpiry})
}

func beginRequest(entry DispositionInboxEntry) BeginDispositionAttemptRequest {
	d := entry.Record.Descriptor
	return BeginDispositionAttemptRequest{
		TenantID:         d.TenantID,
		SessionID:        d.SessionID,
		CommandID:        d.CommandID,
		ExpectedRevision: entry.Revision,
		AttemptID:        settlementAttempt,
		JournalEpoch:     settlementJournal,
		ResidencyEpoch:   settlementResidenc,
		StartedAt:        settlementNow,
	}
}

// fakeEvidence is the injected reader. It records the request the store derived
// so a test can prove the store never asked a question a caller supplied.
type fakeEvidence struct {
	evidence DispositionEvidence
	err      error
	seen     []DispositionEvidenceRequest
}

func (f *fakeEvidence) ReadDispositionEvidence(_ context.Context, req DispositionEvidenceRequest) (DispositionEvidence, error) {
	f.seen = append(f.seen, req)
	if f.err != nil {
		return DispositionEvidence{}, f.err
	}
	return f.evidence, nil
}

func appliedEvidence() DispositionEvidence {
	return DispositionEvidence{
		AttemptID:           settlementAttempt,
		Kind:                DispositionApplied,
		AttemptJournalEpoch: settlementJournal,
		AuthorJournalEpoch:  settlementJournal,
		DispositionSeq:      12,
		EventID:             "01J0000000000000000000EVNT",
		EventSeq:            12,
	}
}

func noOpEvidence() DispositionEvidence {
	return DispositionEvidence{
		AttemptID:           settlementAttempt,
		Kind:                DispositionNoOp,
		AttemptJournalEpoch: settlementJournal,
		AuthorJournalEpoch:  settlementJournal,
		DispositionSeq:      12,
	}
}

func notAppliedEvidence() DispositionEvidence {
	return DispositionEvidence{
		AttemptID:           settlementAttempt,
		Kind:                DispositionNotApplied,
		AttemptJournalEpoch: settlementJournal,
		AuthorJournalEpoch:  settlementJournal + 1,
		DispositionSeq:      31,
		AuthorFenceSeq:      30,
	}
}

func settleRequest(entry DispositionInboxEntry, residency ResidencyEpoch) SettleDispositionCommandRequest {
	d := entry.Record.Descriptor
	return SettleDispositionCommandRequest{
		TenantID:         d.TenantID,
		SessionID:        d.SessionID,
		CommandID:        d.CommandID,
		ExpectedRevision: entry.Revision,
		ResidencyEpoch:   residency,
	}
}

// TestBeginDispositionAttemptRecordsBothGrants is the whole of what an attempt
// is: a claimed record moves to applying and durably carries the attempt
// identity, the journal grant the runtime returned and the Host residency
// grant, as three separate members. Neither epoch is derived from the other.
func TestBeginDispositionAttemptRecordsBothGrants(t *testing.T) {
	s, _, claimed := settlementFixture(t, nil)
	applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
	if err != nil {
		t.Fatalf("BeginDispositionAttempt: %v", err)
	}
	if applying.Record.State != InboxStateApplying {
		t.Fatalf("state = %q, want applying", applying.Record.State)
	}
	attempt := applying.Record.Attempt
	if attempt == nil {
		t.Fatal("applying record carries no attempt")
	}
	if attempt.AttemptID != settlementAttempt || attempt.JournalEpoch != settlementJournal || attempt.ResidencyEpoch != settlementResidenc || !attempt.StartedAt.Equal(settlementNow) {
		t.Fatalf("attempt = %+v", attempt)
	}
	if applying.Record.Claim == nil || applying.Record.Claim.ResidencyEpoch != settlementResidenc {
		t.Fatalf("claim not preserved: %+v", applying.Record.Claim)
	}
	if applying.Revision == claimed.Revision {
		t.Fatal("revision did not move")
	}
	got, err := s.GetDispositionCommand(context.Background(), GetDispositionCommandRequest{TenantID: claimed.Record.Descriptor.TenantID, SessionID: claimed.Record.Descriptor.SessionID, CommandID: claimed.Record.Descriptor.CommandID})
	if err != nil || got.Revision != applying.Revision || got.Record.Attempt == nil || *got.Record.Attempt != *attempt {
		t.Fatalf("durable readback: %+v %v", got, err)
	}
}

// A losing compare-and-swap cannot produce a runtime call, so every refusal
// below must leave the record exactly as it was.
func TestBeginDispositionAttemptRefusals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*BeginDispositionAttemptRequest)
		code   InboxErrorCode
	}{
		{"stale revision", func(r *BeginDispositionAttemptRequest) { r.ExpectedRevision++ }, InboxErrorConflict},
		{"zero revision", func(r *BeginDispositionAttemptRequest) { r.ExpectedRevision = 0 }, InboxErrorInvalid},
		{"empty attempt id", func(r *BeginDispositionAttemptRequest) { r.AttemptID = "" }, InboxErrorInvalid},
		{"zero journal epoch", func(r *BeginDispositionAttemptRequest) { r.JournalEpoch = 0 }, InboxErrorInvalid},
		{"zero residency epoch", func(r *BeginDispositionAttemptRequest) { r.ResidencyEpoch = 0 }, InboxErrorInvalid},
		{"unset start", func(r *BeginDispositionAttemptRequest) { r.StartedAt = time.Time{} }, InboxErrorInvalid},
		{"superseded residency", func(r *BeginDispositionAttemptRequest) { r.ResidencyEpoch = settlementResidenc - 1 }, InboxErrorEpoch},
		{"later residency never claimed this", func(r *BeginDispositionAttemptRequest) { r.ResidencyEpoch = settlementResidenc + 1 }, InboxErrorClaimLost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, claimed := settlementFixture(t, nil)
			req := beginRequest(claimed)
			tc.mutate(&req)
			_, err := s.BeginDispositionAttempt(context.Background(), req)
			assertInboxCode(t, err, tc.code)
			got, err := s.GetDispositionCommand(context.Background(), GetDispositionCommandRequest{TenantID: req.TenantID, SessionID: req.SessionID, CommandID: req.CommandID})
			if err != nil || got.Revision != claimed.Revision || got.Record.State != InboxStateClaimed || got.Record.Attempt != nil {
				t.Fatalf("refused attempt still wrote: %+v %v", got, err)
			}
		})
	}
}

// The refusal ORDER is the contract, not just the set of refusals: a caller
// meeting two conditions at once must be told the one that stays true, because
// the codes mean different things to a Host. Each case below deliberately
// satisfies a later refusal as well as the one it asserts, so the assertion
// fails if the two are ever reordered.
func TestBeginDispositionAttemptRefusalOrder(t *testing.T) {
	// A settled command is terminal AND is not claimed. Terminal means stop,
	// someone else settled this; state means reread and reconsider. Deleting
	// the terminal refusal lets this record fall through to the not-claimed
	// check and answer the weaker of the two.
	t.Run("terminal before not claimed", func(t *testing.T) {
		reader := &fakeEvidence{evidence: appliedEvidence()}
		s, _, claimed := settlementFixture(t, reader)
		applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
		if err != nil {
			t.Fatal(err)
		}
		settled, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
		if err != nil || !ok || !settled.Record.State.terminal() {
			t.Fatalf("settle: %+v %v %v", settled, ok, err)
		}
		if settled.Record.State == InboxStateClaimed {
			t.Fatal("vacuous: a settled record is claimed, so it meets only one refusal")
		}
		req := beginRequest(settled)
		req.AttemptID = "attempt/after-settlement"
		_, err = s.BeginDispositionAttempt(context.Background(), req)
		assertInboxCode(t, err, InboxErrorTerminal)
		got, err := s.GetDispositionCommand(context.Background(), GetDispositionCommandRequest{TenantID: req.TenantID, SessionID: req.SessionID, CommandID: req.CommandID})
		if err != nil || got.Revision != settled.Revision || *got.Record.Outcome != *settled.Record.Outcome {
			t.Fatalf("refused attempt disturbed a settled record: %+v %v", got, err)
		}
	})

	// An applying record carries a claim, so the residency fence would have
	// something to run against and would answer InboxErrorEpoch for a
	// superseded residency. It must not: the record has already authorized a
	// dispatch, and that is the fact that stays true.
	t.Run("not claimed before the residency fence", func(t *testing.T) {
		s, _, claimed := settlementFixture(t, nil)
		applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
		if err != nil {
			t.Fatal(err)
		}
		if applying.Record.Claim == nil {
			t.Fatal("vacuous: an applying record has no claim for the fence to consult")
		}
		req := beginRequest(applying)
		req.AttemptID = "attempt/second"
		req.ResidencyEpoch = settlementResidenc - 1
		_, err = s.BeginDispositionAttempt(context.Background(), req)
		assertInboxCode(t, err, InboxErrorState)
	})

	// A pending command is the other not-claimed record, and it reaches that
	// refusal with no claim at all.
	t.Run("pending is not claimed", func(t *testing.T) {
		s := openStore(t, memstore.New(), WithClock(newMovableClock(settlementNow)))
		createDispositionCatalog(t, s)
		admitted, _, err := s.AdmitDispositionCommand(context.Background(), dispositionRequest())
		if err != nil {
			t.Fatal(err)
		}
		if admitted.Record.State != InboxStatePending || admitted.Record.Claim != nil {
			t.Fatalf("vacuous fixture: %+v", admitted.Record)
		}
		_, err = s.BeginDispositionAttempt(context.Background(), beginRequest(admitted))
		assertInboxCode(t, err, InboxErrorState)
	})

	// A superseded residency meeting a claim that has ALSO lapsed must be told
	// it was superseded: that is permanent, while a lapsed claim is a fact
	// about a claim this caller no longer has any standing to hold anyway.
	t.Run("superseded residency before a lapsed claim", func(t *testing.T) {
		s, clock, claimed := settlementFixture(t, nil)
		clock.set(settlementExpiry)
		req := beginRequest(claimed)
		req.ResidencyEpoch = settlementResidenc - 1
		_, err := s.BeginDispositionAttempt(context.Background(), req)
		epoch := assertInboxCode(t, err, InboxErrorEpoch)
		if epoch.Epoch != uint64(settlementResidenc) {
			t.Fatalf("epoch = %d, want the committed claim's %d", epoch.Epoch, settlementResidenc)
		}
	})

	// A residency that never claimed this command, meeting the same lapsed
	// claim, is told the same thing it is told on a live claim.
	t.Run("claim ownership before a lapsed claim", func(t *testing.T) {
		s, clock, claimed := settlementFixture(t, nil)
		clock.set(settlementExpiry)
		req := beginRequest(claimed)
		req.ResidencyEpoch = settlementResidenc + 1
		_, err := s.BeginDispositionAttempt(context.Background(), req)
		claimLost := assertInboxCode(t, err, InboxErrorClaimLost)
		if claimLost.Field != "residency_epoch" {
			t.Fatalf("field = %q, want residency_epoch: a lapsed claim answered for the fence", claimLost.Field)
		}
	})
}

// An expired claim is no longer a claim: the dispatch it would authorize may no
// longer be the only one, so the transition refuses on the store's clock.
func TestBeginDispositionAttemptRefusesLapsedClaim(t *testing.T) {
	s, clock, claimed := settlementFixture(t, nil)
	clock.set(settlementExpiry)
	_, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
	assertInboxCode(t, err, InboxErrorClaimLost)
}

// A pending record has no claim, and an applying one has already authorized a
// dispatch: neither is an edge this transition has.
func TestBeginDispositionAttemptRefusesWrongState(t *testing.T) {
	s, _, claimed := settlementFixture(t, nil)
	applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
	if err != nil {
		t.Fatal(err)
	}
	req := beginRequest(applying)
	req.AttemptID = "attempt/second"
	_, err = s.BeginDispositionAttempt(context.Background(), req)
	assertInboxCode(t, err, InboxErrorState)
}

// The retry of an ambiguous attempt write learns the durable winner by being
// told the current revision, and the winner's grants are unchanged.
func TestBeginDispositionAttemptAmbiguousWriteRetryFindsWinner(t *testing.T) {
	s, _, claimed := settlementFixture(t, nil)
	req := beginRequest(claimed)
	applying, err := s.BeginDispositionAttempt(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	retry := req
	retry.AttemptID = "attempt/other"
	retry.JournalEpoch = settlementJournal + 5
	_, err = s.BeginDispositionAttempt(context.Background(), retry)
	conflict := assertInboxCode(t, err, InboxErrorConflict)
	if conflict.Revision != applying.Revision {
		t.Fatalf("conflict revision = %d, want %d", conflict.Revision, applying.Revision)
	}
	got, err := s.GetDispositionCommand(context.Background(), GetDispositionCommandRequest{TenantID: req.TenantID, SessionID: req.SessionID, CommandID: req.CommandID})
	if err != nil || got.Record.Attempt.AttemptID != settlementAttempt || got.Record.Attempt.JournalEpoch != settlementJournal {
		t.Fatalf("attempt was overwritten: %+v %v", got, err)
	}
}

// The evidence reader is asked a question the store derived from its own
// record, never one the caller supplied, and the settlement it authorizes is
// the terminal compare-and-swap of that exact revision.
func TestSettleDispositionAppliedUsesDerivedEvidenceRequest(t *testing.T) {
	reader := &fakeEvidence{evidence: appliedEvidence()}
	s, _, claimed := settlementFixture(t, reader)
	applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
	if err != nil {
		t.Fatal(err)
	}
	settled, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
	if err != nil || !ok {
		t.Fatalf("SettleDispositionCommand: %+v %v %v", settled, ok, err)
	}
	if len(reader.seen) != 1 {
		t.Fatalf("evidence reads = %d, want 1", len(reader.seen))
	}
	d := applying.Record.Descriptor
	want := DispositionEvidenceRequest{TenantID: d.TenantID, SessionID: d.SessionID, CommandID: d.CommandID, Kind: d.Kind, RuntimeCommandID: d.RuntimeCommandID, Binding: d.Binding, Attempt: *applying.Record.Attempt}
	if reader.seen[0] != want {
		t.Fatalf("derived evidence request = %+v, want %+v", reader.seen[0], want)
	}
	if settled.Record.State != InboxStateApplied {
		t.Fatalf("state = %q", settled.Record.State)
	}
	outcome := settled.Record.Outcome
	if outcome == nil || outcome.Kind != DispositionApplied || outcome.DispositionSeq != 12 || outcome.EventSeq != 12 || outcome.EventID != "01J0000000000000000000EVNT" {
		t.Fatalf("outcome = %+v", outcome)
	}
	if outcome.AttemptID != settlementAttempt || outcome.AttemptJournalEpoch != settlementJournal || outcome.AuthorJournalEpoch != settlementJournal {
		t.Fatalf("outcome identities = %+v", outcome)
	}
	if *settled.Record.Attempt != *applying.Record.Attempt {
		t.Fatalf("attempt overwritten by settlement: %+v", settled.Record.Attempt)
	}
}

// A successful no-op is an application, and it fabricates no public event.
func TestSettleDispositionNoOpIsAnApplication(t *testing.T) {
	reader := &fakeEvidence{evidence: noOpEvidence()}
	s, _, claimed := settlementFixture(t, reader)
	applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
	if err != nil {
		t.Fatal(err)
	}
	settled, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
	if err != nil || !ok || settled.Record.State != InboxStateApplied {
		t.Fatalf("no-op settlement: %+v %v %v", settled, ok, err)
	}
	if settled.Record.Outcome.Kind != DispositionNoOp || settled.Record.Outcome.EventID != "" || settled.Record.Outcome.EventSeq != 0 {
		t.Fatalf("no-op invented an event: %+v", settled.Record.Outcome)
	}
}

// Recovery closure is authored by a strictly later grant and names that
// author's verified opening fence. A successor settles it, and its own
// residency epoch is recorded as settlement context beside — never over — the
// original attempt's two grants.
func TestSettleDispositionNotAppliedBySuccessor(t *testing.T) {
	reader := &fakeEvidence{evidence: notAppliedEvidence()}
	s, _, claimed := settlementFixture(t, reader)
	applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
	if err != nil {
		t.Fatal(err)
	}
	settled, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc+3))
	if err != nil || !ok || settled.Record.State != InboxStateRejected {
		t.Fatalf("successor settlement: %+v %v %v", settled, ok, err)
	}
	outcome := settled.Record.Outcome
	if outcome.Kind != DispositionNotApplied || outcome.AuthorJournalEpoch != settlementJournal+1 || outcome.AuthorFenceSeq != 30 {
		t.Fatalf("outcome = %+v", outcome)
	}
	if outcome.SettlingResidencyEpoch != settlementResidenc+3 {
		t.Fatalf("settling residency = %d", outcome.SettlingResidencyEpoch)
	}
	if settled.Record.Attempt.ResidencyEpoch != settlementResidenc || settled.Record.Attempt.JournalEpoch != settlementJournal {
		t.Fatalf("successor overwrote the original grants: %+v", settled.Record.Attempt)
	}
}

// Every evidence defect fails closed and leaves the inbox unsettled. Each case
// is one thing the design requires the store to validate before the inbox CAS.
func TestSettleDispositionRejectsUnverifiedEvidence(t *testing.T) {
	for _, tc := range []struct {
		name     string
		evidence DispositionEvidence
	}{
		{"another attempt", func() DispositionEvidence { e := appliedEvidence(); e.AttemptID = "attempt/other"; return e }()},
		{"another attempt grant", func() DispositionEvidence {
			e := appliedEvidence()
			e.AttemptJournalEpoch = settlementJournal + 1
			return e
		}()},
		{"applied by a later grant", func() DispositionEvidence {
			e := appliedEvidence()
			e.AuthorJournalEpoch = settlementJournal + 1
			return e
		}()},
		{"applied with no event", func() DispositionEvidence { e := appliedEvidence(); e.EventID, e.EventSeq = "", 0; return e }()},
		{"applied naming an event outside its envelope", func() DispositionEvidence { e := appliedEvidence(); e.EventSeq = 13; return e }()},
		{"no-op with a fabricated event", func() DispositionEvidence {
			e := noOpEvidence()
			e.EventID, e.EventSeq = "01J0000000000000000000EVNT", 12
			return e
		}()},
		{"not applied by the attempt's own grant", func() DispositionEvidence {
			e := notAppliedEvidence()
			e.AuthorJournalEpoch = settlementJournal
			return e
		}()},
		{"not applied by an earlier grant", func() DispositionEvidence {
			e := notAppliedEvidence()
			e.AuthorJournalEpoch = settlementJournal - 1
			return e
		}()},
		{"not applied with no author fence", func() DispositionEvidence { e := notAppliedEvidence(); e.AuthorFenceSeq = 0; return e }()},
		{"not applied fenced after its own disposition", func() DispositionEvidence {
			e := notAppliedEvidence()
			e.AuthorFenceSeq = e.DispositionSeq + 1
			return e
		}()},
		{"unsequenced disposition", func() DispositionEvidence { e := appliedEvidence(); e.DispositionSeq, e.EventSeq = 0, 0; return e }()},
		{"unknown kind", func() DispositionEvidence { e := appliedEvidence(); e.Kind = "settled"; return e }()},
		{"empty evidence", DispositionEvidence{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &fakeEvidence{evidence: tc.evidence}
			s, _, claimed := settlementFixture(t, reader)
			applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
			if err != nil {
				t.Fatal(err)
			}
			_, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
			assertInboxCode(t, err, InboxErrorEvidence)
			if ok {
				t.Fatal("unverified evidence settled the command")
			}
			got, err := s.GetDispositionCommand(context.Background(), GetDispositionCommandRequest{TenantID: applying.Record.Descriptor.TenantID, SessionID: applying.Record.Descriptor.SessionID, CommandID: applying.Record.Descriptor.CommandID})
			if err != nil || got.Revision != applying.Revision || got.Record.State != InboxStateApplying || got.Record.Outcome != nil {
				t.Fatalf("record moved: %+v %v", got, err)
			}
		})
	}
}

// An unavailable or canceled evidence read propagates as itself. It is not a
// skip, an absence, or permission to settle.
func TestSettleDispositionPropagatesEvidenceFailure(t *testing.T) {
	unavailable := errors.New("journal unavailable")
	reader := &fakeEvidence{err: unavailable}
	s, _, claimed := settlementFixture(t, reader)
	applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
	if err != nil {
		t.Fatal(err)
	}
	_, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
	if !errors.Is(err, unavailable) || ok {
		t.Fatalf("evidence failure = %v %v, want the provider's own error", err, ok)
	}
	reader.err = context.Canceled
	if _, _, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
	got, err := s.GetDispositionCommand(context.Background(), GetDispositionCommandRequest{TenantID: applying.Record.Descriptor.TenantID, SessionID: applying.Record.Descriptor.SessionID, CommandID: applying.Record.Descriptor.CommandID})
	if err != nil || got.Record.State != InboxStateApplying {
		t.Fatalf("record moved: %+v %v", got, err)
	}
}

// With no configured reader there is no evidence boundary at all, so settlement
// refuses rather than settling on the record's own state.
func TestSettleDispositionRequiresAConfiguredReader(t *testing.T) {
	s, _, claimed := settlementFixture(t, nil)
	applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
	if err != nil {
		t.Fatal(err)
	}
	_, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
	assertInboxCode(t, err, InboxErrorEvidence)
	if ok {
		t.Fatal("settled with no evidence reader")
	}
}

// A settlement request is validated before any provider I/O, and each refusal
// names the request member that was wrong. The zero settling residency is
// asserted by FIELD as well as by code, because a zero one is also caught a
// layer down by the stored outcome's own rules — a test that looked only at the
// code could not tell which of the two answered.
func TestSettleDispositionValidatesItsRequestBeforeAnyRead(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*SettleDispositionCommandRequest)
		field  string
	}{
		{"zero revision", func(r *SettleDispositionCommandRequest) { r.ExpectedRevision = 0 }, "expected_revision"},
		{"zero residency", func(r *SettleDispositionCommandRequest) { r.ResidencyEpoch = 0 }, "residency_epoch"},
		{"empty command", func(r *SettleDispositionCommandRequest) { r.CommandID = "" }, "command_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &fakeEvidence{evidence: appliedEvidence()}
			s, _, claimed := settlementFixture(t, reader)
			applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
			if err != nil {
				t.Fatal(err)
			}
			req := settleRequest(applying, settlementResidenc)
			tc.mutate(&req)
			_, ok, err := s.SettleDispositionCommand(context.Background(), req)
			invalid := assertInboxCode(t, err, InboxErrorInvalid)
			if ok || invalid.Field != tc.field {
				t.Fatalf("field = %q, want %q (ok=%v)", invalid.Field, tc.field, ok)
			}
			if len(reader.seen) != 0 {
				t.Fatalf("an invalid request reached the evidence reader: %d", len(reader.seen))
			}
		})
	}
}

// A record with no durably authorized attempt has nothing to settle: the
// terminal outcome is keyed by an attempt, and a pending or claimed record has
// none. Reading evidence for one would be evidence about nothing.
func TestSettleDispositionRefusesWithoutAnAttempt(t *testing.T) {
	reader := &fakeEvidence{evidence: appliedEvidence()}
	s, _, claimed := settlementFixture(t, reader)
	_, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(claimed, settlementResidenc))
	assertInboxCode(t, err, InboxErrorState)
	if ok || len(reader.seen) != 0 {
		t.Fatalf("evidence read for an unattempted command: %v %d", ok, len(reader.seen))
	}
}

// Losing the terminal CAS is a reread instruction carrying the current
// revision; meeting the settled state again at that revision is idempotent, and
// a repeat settlement neither reads evidence again nor writes.
func TestSettleDispositionIsIdempotentAndConflictsOnStaleRevision(t *testing.T) {
	reader := &fakeEvidence{evidence: appliedEvidence()}
	s, _, claimed := settlementFixture(t, reader)
	applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
	if err != nil {
		t.Fatal(err)
	}
	settled, _, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
	if err != nil {
		t.Fatal(err)
	}
	_, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
	conflict := assertInboxCode(t, err, InboxErrorConflict)
	if ok || conflict.Revision != settled.Revision {
		t.Fatalf("stale settlement = %v, revision %d want %d", ok, conflict.Revision, settled.Revision)
	}
	again, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(settled, settlementResidenc))
	if err != nil || ok || again.Revision != settled.Revision || *again.Record.Outcome != *settled.Record.Outcome {
		t.Fatalf("idempotent settlement: %+v %v %v", again, ok, err)
	}
	if len(reader.seen) != 1 {
		t.Fatalf("evidence reads = %d, want 1: a settled command re-reads nothing", len(reader.seen))
	}
}

// Settlement authority is the store's own catalog binding, exactly as every
// other disposition read is, and a terminal record leaves the due view.
func TestSettleDispositionChecksCatalogAuthorityAndClearsTheDueView(t *testing.T) {
	reader := &fakeEvidence{evidence: appliedEvidence()}
	clock := newMovableClock(settlementNow)
	s := openStore(t, memstore.New(), WithClock(clock), WithControlShards(1), WithDispositionEvidence(reader))
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
	page, err := s.ListDueDispositionCommands(context.Background(), ListDueDispositionCommandsRequest{Shard: 0, DueAtOrBefore: inboxDeadline, Limit: 10})
	if err != nil || len(page.Commands) != 1 {
		t.Fatalf("applying command is not due: %+v %v", page, err)
	}
	if _, _, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc)); err != nil {
		t.Fatal(err)
	}
	page, err = s.ListDueDispositionCommands(context.Background(), ListDueDispositionCommandsRequest{Shard: 0, DueAtOrBefore: inboxDeadline, Limit: 10})
	if err != nil || len(page.Commands) != 0 || page.Examined != 0 {
		t.Fatalf("settled command still due: %+v %v", page, err)
	}
}

// dispositionWitnessRace removes both session witnesses at the instant the
// protocol fence reads for them, which is the only window in which that fence's
// create-only PUT is reachable from a disposition transition: the catalog read
// that runs first already requires the collision witness, so an external
// deletion has to land between the two reads.
type dispositionWitnessRace struct {
	storage.KV
	arm  bool
	keys []string
}

func (r *dispositionWitnessRace) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	if r.arm && key == r.keys[0] {
		r.arm = false
		for _, k := range r.keys {
			if err := r.KV.Delete(ctx, k); err != nil {
				return nil, 0, err
			}
		}
	}
	return r.KV.Get(ctx, key)
}

// Neither transition is "one read and one compare-and-swap", and the difference
// matters to anyone reasoning about failure modes. Each reads the catalog, then
// re-fences the session's protocol mode through bindProtocolMode — which is a
// potential WRITE — and only then reads and compare-and-swaps the inbox record.
// All three branches of that fence are asserted, because the catalog's own
// binding check answers the mode question in the ordinary case and would carry
// a test that looked only at the transition's outcome.
func TestDispositionTransitionsFenceTheProtocolMode(t *testing.T) {
	setup := func(t *testing.T, race *dispositionWitnessRace) (*storage.Composite, *Store, DispositionInboxEntry, sessionScope, string, []byte) {
		t.Helper()
		backend := memstore.New()
		if race != nil {
			race.KV = backend.KV
			backend.KV = race
		}
		s := openStore(t, backend, WithClock(newMovableClock(settlementNow)), WithDispositionEvidence(&fakeEvidence{evidence: appliedEvidence()}))
		createDispositionCatalog(t, s)
		admitted, _, err := s.AdmitDispositionCommand(context.Background(), dispositionRequest())
		if err != nil {
			t.Fatal(err)
		}
		claimed := fileDispositionClaim(t, s, admitted, DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: settlementExpiry})
		d := claimed.Record.Descriptor
		scope, err := s.deriveSessionScope(d.TenantID, d.SessionID)
		if err != nil {
			t.Fatal(err)
		}
		key := scope.SessionNamespace + "/protocol"
		want := encodeWitness(1, scope.sessionWitness, []byte(ProtocolModeDisposition))
		got, _, err := backend.KV.Get(context.Background(), key)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("vacuous fixture: the create did not install the witness this test manipulates: %v", err)
		}
		if race != nil {
			race.keys = []string{key, scope.sessionWitnessKey}
		}
		return backend, s, claimed, scope, key, want
	}

	// A mode witness pinned to the other protocol refuses both transitions even
	// though the catalog binding still says disposition. This is the fence
	// itself answering, not the catalog: they are two independent records, and
	// this is the case in which they disagree.
	t.Run("a legacy witness refuses both transitions", func(t *testing.T) {
		for _, entry := range []string{"attempt", "settlement"} {
			t.Run(entry, func(t *testing.T) {
				backend, s, claimed, scope, key, _ := setup(t, nil)
				_, rev, err := backend.KV.Get(context.Background(), key)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := backend.KV.Put(context.Background(), key, rev, encodeWitness(1, scope.sessionWitness, []byte(ProtocolModeLegacy))); err != nil {
					t.Fatal(err)
				}
				if entry == "attempt" {
					_, err = s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
				} else {
					_, _, err = s.SettleDispositionCommand(context.Background(), settleRequest(claimed, settlementResidenc))
				}
				assertCatalogCode(t, err, CatalogErrorConflict)
				stored, err := backend.OrderedIndex.Get(context.Background(), dispositionInboxID(scope, claimed.Record.Descriptor.CommandID))
				if err != nil || stored.Revision != claimed.Revision {
					t.Fatalf("a refused transition wrote: %+v %v", stored, err)
				}
			})
		}
	})

	// A mode witness missing BESIDE a live collision witness is the
	// conservatively-legacy case: a bound scope with no mode pin is read as
	// legacy and the transition is refused, never silently repinned.
	t.Run("a missing witness beside a bound scope is a conflict", func(t *testing.T) {
		backend, s, claimed, _, key, _ := setup(t, nil)
		if err := backend.KV.Delete(context.Background(), key); err != nil {
			t.Fatal(err)
		}
		_, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
		assertCatalogCode(t, err, CatalogErrorConflict)
		if _, _, err := backend.KV.Get(context.Background(), key); err == nil {
			t.Fatal("a refused transition repinned the mode")
		}
	})

	// With NO witness at all the fence installs one, and that PUT is the write
	// the documentation has to account for. It is reachable from here only in
	// the window this fixture opens, because the catalog read that runs first
	// needs the collision witness — which is exactly why the transition is
	// documented as a potential write rather than a read.
	t.Run("the fence installs a witness when the session has none", func(t *testing.T) {
		for _, entry := range []string{"attempt", "settlement"} {
			t.Run(entry, func(t *testing.T) {
				race := &dispositionWitnessRace{}
				backend, s, claimed, _, key, want := setup(t, race)
				current := claimed
				if entry == "settlement" {
					applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
					if err != nil {
						t.Fatal(err)
					}
					current = applying
				}
				race.arm = true
				var err error
				if entry == "attempt" {
					_, err = s.BeginDispositionAttempt(context.Background(), beginRequest(current))
				} else {
					var ok bool
					_, ok, err = s.SettleDispositionCommand(context.Background(), settleRequest(current, settlementResidenc))
					if err == nil && !ok {
						t.Fatal("settlement did not settle")
					}
				}
				if err != nil {
					t.Fatalf("%s over an unwitnessed session: %v", entry, err)
				}
				if race.arm {
					t.Fatal("vacuous: the fence never read the mode witness")
				}
				got, _, getErr := backend.KV.Get(context.Background(), key)
				if getErr != nil || !bytes.Equal(got, want) {
					t.Fatalf("the %s did not install the protocol witness: %x %v", entry, got, getErr)
				}
			})
		}
	})
}

// Legacy commands are a different protocol with different epoch assumptions.
// Neither new entry point may touch one, and the released legacy transitions
// may not consume a disposition record.
func TestSettlementEntryPointsAreProtocolSeparated(t *testing.T) {
	reader := &fakeEvidence{evidence: appliedEvidence()}
	s := openStore(t, memstore.New(), WithClock(newMovableClock(settlementNow)), WithDispositionEvidence(reader))
	legacy := mustAdmit(t, s, testAdmitRequest())
	_, err := s.BeginDispositionAttempt(context.Background(), BeginDispositionAttemptRequest{
		TenantID: legacy.Record.TenantID, SessionID: legacy.Record.SessionID, CommandID: legacy.Record.CommandID,
		ExpectedRevision: legacy.Revision, AttemptID: settlementAttempt, JournalEpoch: settlementJournal,
		ResidencyEpoch: settlementResidenc, StartedAt: settlementNow,
	})
	if err == nil {
		t.Fatal("a legacy command accepted a disposition attempt")
	}
	_, _, err = s.SettleDispositionCommand(context.Background(), SettleDispositionCommandRequest{
		TenantID: legacy.Record.TenantID, SessionID: legacy.Record.SessionID, CommandID: legacy.Record.CommandID,
		ExpectedRevision: legacy.Revision, ResidencyEpoch: settlementResidenc,
	})
	if err == nil {
		t.Fatal("a legacy command was settled by the disposition protocol")
	}
	if len(reader.seen) != 0 {
		t.Fatalf("evidence read for a legacy command: %d", len(reader.seen))
	}
}

// A residency epoch is not a journal epoch, and settlement never compares them.
// Deliberately unequal domains, in both directions, change no outcome.
func TestSettlementNeverComparesResidencyWithJournalEpochs(t *testing.T) {
	for _, residency := range []ResidencyEpoch{1, ResidencyEpoch(settlementJournal), ResidencyEpoch(settlementJournal) + 1000} {
		reader := &fakeEvidence{evidence: appliedEvidence()}
		clock := newMovableClock(settlementNow)
		s := openStore(t, memstore.New(), WithClock(clock), WithDispositionEvidence(reader))
		createDispositionCatalog(t, s)
		admitted, _, err := s.AdmitDispositionCommand(context.Background(), dispositionRequest())
		if err != nil {
			t.Fatal(err)
		}
		claimed := fileDispositionClaim(t, s, admitted, DispositionClaim{ResidencyEpoch: residency, ExpiresAt: settlementExpiry})
		begin := beginRequest(claimed)
		begin.ResidencyEpoch = residency
		applying, err := s.BeginDispositionAttempt(context.Background(), begin)
		if err != nil {
			t.Fatalf("residency %d: %v", residency, err)
		}
		settled, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, residency))
		if err != nil || !ok || settled.Record.State != InboxStateApplied {
			t.Fatalf("residency %d: %+v %v %v", residency, settled, ok, err)
		}
	}
}

// A durable record survives the store that wrote it. Each stage is written,
// the store is closed, a new one is opened over the same backend, and the
// operation is retried: the winner is the one that committed.
func TestSettlementSurvivesReopenAtEveryDurableStage(t *testing.T) {
	backend := memstore.New()
	reader := &fakeEvidence{evidence: appliedEvidence()}
	open := func() *Store {
		return openStore(t, backend, WithClock(newMovableClock(settlementNow)), WithDispositionEvidence(reader))
	}
	first := open()
	createDispositionCatalog(t, first)
	admitted, _, err := first.AdmitDispositionCommand(context.Background(), dispositionRequest())
	if err != nil {
		t.Fatal(err)
	}
	claimed := fileDispositionClaim(t, first, admitted, DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: settlementExpiry})
	applying, err := first.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	second := open()
	retry := beginRequest(claimed)
	retry.AttemptID = "attempt/after-restart"
	if _, err := second.BeginDispositionAttempt(context.Background(), retry); err == nil {
		t.Fatal("a restarted store re-authorized a dispatch")
	}
	settled, ok, err := second.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
	if err != nil || !ok {
		t.Fatalf("settlement after restart: %v %v", ok, err)
	}
	if err := second.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	third := open()
	again, ok, err := third.SettleDispositionCommand(context.Background(), settleRequest(settled, settlementResidenc))
	if err != nil || ok || again.Record.State != InboxStateApplied || *again.Record.Outcome != *settled.Record.Outcome {
		t.Fatalf("terminal outcome did not survive: %+v %v %v", again, ok, err)
	}
	if again.Record.Attempt.AttemptID != settlementAttempt {
		t.Fatalf("attempt did not survive: %+v", again.Record.Attempt)
	}
}

// The store must not be able to reach a terminal state the codec cannot hold,
// and a stored record whose members contradict its state is not decodable.
func TestDispositionRecordStatesRequireTheirMembers(t *testing.T) {
	base := func() DispositionInboxRecord {
		d, err := dispositionDescriptor(dispositionRequest())
		if err != nil {
			t.Fatal(err)
		}
		return DispositionInboxRecord{Descriptor: d, AcceptedAt: inboxAcceptedAt, ApplyDeadline: inboxDeadline, State: InboxStatePending}
	}
	claim := &DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: settlementExpiry}
	attempt := &DispositionAttempt{AttemptID: settlementAttempt, JournalEpoch: settlementJournal, ResidencyEpoch: settlementResidenc, StartedAt: settlementNow}
	outcome := &DispositionOutcome{Kind: DispositionApplied, AttemptID: settlementAttempt, AttemptJournalEpoch: settlementJournal, AuthorJournalEpoch: settlementJournal, DispositionSeq: 12, EventID: "01J0000000000000000000EVNT", EventSeq: 12, SettlingResidencyEpoch: settlementResidenc, SettledAt: settlementNow}
	for _, tc := range []struct {
		name  string
		build func(*DispositionInboxRecord)
		ok    bool
	}{
		{"pending", func(*DispositionInboxRecord) {}, true},
		{"pending with a claim", func(r *DispositionInboxRecord) { r.Claim = claim }, false},
		{"pending with an attempt", func(r *DispositionInboxRecord) { r.Attempt = attempt }, false},
		{"claimed", func(r *DispositionInboxRecord) { r.State, r.Claim = InboxStateClaimed, claim }, true},
		{"claimed with no claim", func(r *DispositionInboxRecord) { r.State = InboxStateClaimed }, false},
		{"claimed with an attempt", func(r *DispositionInboxRecord) {
			r.State, r.Claim, r.Attempt = InboxStateClaimed, claim, attempt
		}, false},
		{"applying", func(r *DispositionInboxRecord) {
			r.State, r.Claim, r.Attempt = InboxStateApplying, claim, attempt
		}, true},
		{"applying with no attempt", func(r *DispositionInboxRecord) { r.State, r.Claim = InboxStateApplying, claim }, false},
		{"applying with an outcome", func(r *DispositionInboxRecord) {
			r.State, r.Claim, r.Attempt, r.Outcome = InboxStateApplying, claim, attempt, outcome
		}, false},
		{"applied", func(r *DispositionInboxRecord) {
			r.State, r.Claim, r.Attempt, r.Outcome = InboxStateApplied, claim, attempt, outcome
		}, true},
		{"applied with no outcome", func(r *DispositionInboxRecord) {
			r.State, r.Claim, r.Attempt = InboxStateApplied, claim, attempt
		}, false},
		{"applied with no attempt", func(r *DispositionInboxRecord) {
			r.State, r.Claim, r.Outcome = InboxStateApplied, claim, outcome
		}, false},
		{"applied carrying a rejection outcome", func(r *DispositionInboxRecord) {
			settled := *outcome
			settled.Kind, settled.AuthorJournalEpoch, settled.AuthorFenceSeq, settled.EventID, settled.EventSeq = DispositionNotApplied, settlementJournal+1, 11, "", 0
			r.State, r.Claim, r.Attempt, r.Outcome = InboxStateApplied, claim, attempt, &settled
		}, false},
		{"rejected", func(r *DispositionInboxRecord) {
			settled := *outcome
			settled.Kind, settled.AuthorJournalEpoch, settled.AuthorFenceSeq, settled.EventID, settled.EventSeq = DispositionNotApplied, settlementJournal+1, 11, "", 0
			r.State, r.Claim, r.Attempt, r.Outcome = InboxStateRejected, claim, attempt, &settled
		}, true},
		{"rejected carrying an applied outcome", func(r *DispositionInboxRecord) {
			r.State, r.Claim, r.Attempt, r.Outcome = InboxStateRejected, claim, attempt, outcome
		}, false},
		// Reject-before-dispatch: the design's negative outcome for a command
		// no dispatch was ever authorized for. It has no attempt by definition,
		// and therefore no evidence-keyed outcome, and it may or may not have
		// reached a claim first. Both shapes must be storable.
		{"rejected before any claim", func(r *DispositionInboxRecord) { r.State = InboxStateRejected }, true},
		{"rejected after a claim before dispatch", func(r *DispositionInboxRecord) {
			r.State, r.Claim = InboxStateRejected, claim
		}, true},
		// The relaxation is exactly that wide and no wider: only a rejection
		// may lack an attempt, it may not then carry an outcome keyed by one,
		// and nothing may reach applied without the attempt that applied it.
		{"rejected before dispatch carrying an outcome", func(r *DispositionInboxRecord) {
			r.State, r.Outcome = InboxStateRejected, outcome
		}, false},
		{"rejected before dispatch with a claim and an outcome", func(r *DispositionInboxRecord) {
			r.State, r.Claim, r.Outcome = InboxStateRejected, claim, outcome
		}, false},
		{"applied before any claim", func(r *DispositionInboxRecord) { r.State = InboxStateApplied }, false},
		{"applied with a claim but no attempt", func(r *DispositionInboxRecord) {
			r.State, r.Claim = InboxStateApplied, claim
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := base()
			tc.build(&record)
			value, canonical, err := encodeDispositionInboxRecord(record)
			if tc.ok != (err == nil) {
				t.Fatalf("encode = %v, want ok=%v", err, tc.ok)
			}
			if !tc.ok {
				return
			}
			decoded, err := decodeDispositionInboxRecord(value)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if decoded.State != canonical.State {
				t.Fatalf("decoded state = %q", decoded.State)
			}
		})
	}
}

// The three post-admission members are durable records too, so their stored
// member names are pinned here by full byte literals exactly as the pending
// record's are. Renaming any of them in the private DTO changes these bytes;
// renaming one on the exported struct changes nothing, which is the whole point
// of the DTO. Each case also decodes back to the canonical record and refuses a
// noncanonical spelling of its own members.
func TestDispositionSettlementWireGolden(t *testing.T) {
	const head = `{"record_version":2,"descriptor":{`
	const identity = `"tenant_id":"Tenant/A:B","session_id":"Session/A:B","command_id":"Create/A:B","binding":{"storage_binding_id":"agent-pool/east","binding_version":"config-2026-09","runtime_session_id":"runtime/session-a","protocol_mode":"disposition"},"runtime_command_id":"2f1c7d1e-0f3a-4c5b-9f21-000000000001","kind":"Kind/A:B","payload_digest":"47ffa3ea45a70b8a41c2c0825df323c00a8b7a01c1ea06083cc41dddcc001123","payload_size":3,"payload":"AP8B"},"accepted_at":"2026-08-30T11:30:00Z","apply_deadline":"2026-08-30T12:30:00Z","state":"`
	const claimWire = `"claim":{"residency_epoch":4,"expires_at":"2026-08-30T12:00:00Z"}`
	const attemptWire = `"attempt":{"attempt_id":"attempt/A:B","journal_epoch":9,"residency_epoch":4,"started_at":"2026-08-30T11:40:00Z"}`
	claim := &DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: settlementExpiry}
	attempt := &DispositionAttempt{AttemptID: settlementAttempt, JournalEpoch: settlementJournal, ResidencyEpoch: settlementResidenc, StartedAt: settlementNow}
	applied := &DispositionOutcome{Kind: DispositionApplied, AttemptID: settlementAttempt, AttemptJournalEpoch: settlementJournal, AuthorJournalEpoch: settlementJournal, DispositionSeq: 12, EventID: "01J0000000000000000000EVNT", EventSeq: 12, SettlingResidencyEpoch: settlementResidenc, SettledAt: settlementNow}
	closure := &DispositionOutcome{Kind: DispositionNotApplied, AttemptID: settlementAttempt, AttemptJournalEpoch: settlementJournal, AuthorJournalEpoch: settlementJournal + 1, DispositionSeq: 31, AuthorFenceSeq: 30, SettlingResidencyEpoch: settlementResidenc + 3, SettledAt: settlementNow}
	for _, tc := range []struct {
		name  string
		state InboxState
		build func(*DispositionInboxRecord)
		want  string
		bad   []string
	}{
		{
			"claimed", InboxStateClaimed,
			func(r *DispositionInboxRecord) { r.Claim = claim },
			identity + `claimed",` + claimWire + `}`,
			[]string{`"residency_epoch"`, `"expires_at"`, `"claim"`},
		},
		{
			"applying", InboxStateApplying,
			func(r *DispositionInboxRecord) { r.Claim, r.Attempt = claim, attempt },
			identity + `applying",` + claimWire + `,` + attemptWire + `}`,
			[]string{`"attempt_id"`, `"journal_epoch"`, `"started_at"`, `"attempt"`},
		},
		{
			"applied", InboxStateApplied,
			func(r *DispositionInboxRecord) { r.Claim, r.Attempt, r.Outcome = claim, attempt, applied },
			identity + `applied",` + claimWire + `,` + attemptWire + `,"outcome":{"kind":"applied","attempt_id":"attempt/A:B","attempt_journal_epoch":9,"author_journal_epoch":9,"disposition_seq":12,"author_fence_seq":0,"event_id":"01J0000000000000000000EVNT","event_seq":12,"settling_residency_epoch":4,"settled_at":"2026-08-30T11:40:00Z"}}`,
			[]string{`"kind"`, `"attempt_journal_epoch"`, `"author_journal_epoch"`, `"disposition_seq"`, `"author_fence_seq"`, `"event_id"`, `"event_seq"`, `"settling_residency_epoch"`, `"settled_at"`, `"outcome"`},
		},
		{
			"rejected", InboxStateRejected,
			func(r *DispositionInboxRecord) { r.Claim, r.Attempt, r.Outcome = claim, attempt, closure },
			identity + `rejected",` + claimWire + `,` + attemptWire + `,"outcome":{"kind":"not_applied","attempt_id":"attempt/A:B","attempt_journal_epoch":9,"author_journal_epoch":10,"disposition_seq":31,"author_fence_seq":30,"event_id":"","event_seq":0,"settling_residency_epoch":7,"settled_at":"2026-08-30T11:40:00Z"}}`,
			[]string{`"event_id"`, `"event_seq"`, `"author_fence_seq"`},
		},
		{
			"rejected before any claim", InboxStateRejected,
			func(*DispositionInboxRecord) {},
			identity + `rejected"}`,
			[]string{`"state"`},
		},
		{
			"rejected after a claim before dispatch", InboxStateRejected,
			func(r *DispositionInboxRecord) { r.Claim = claim },
			identity + `rejected",` + claimWire + `}`,
			[]string{`"claim"`, `"expires_at"`},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := dispositionDescriptor(dispositionRequest())
			if err != nil {
				t.Fatal(err)
			}
			d.TenantID, d.SessionID, d.CommandID, d.Kind = "Tenant/A:B", "Session/A:B", "Create/A:B", "Kind/A:B"
			d.Payload, d.PayloadSize = []byte{0, 255, 1}, 3
			d.PayloadDigest = "47ffa3ea45a70b8a41c2c0825df323c00a8b7a01c1ea06083cc41dddcc001123"
			record := DispositionInboxRecord{Descriptor: d, AcceptedAt: inboxAcceptedAt, ApplyDeadline: inboxDeadline, State: tc.state}
			tc.build(&record)
			want := head + tc.want
			got, canonical, err := encodeDispositionInboxRecord(record)
			if err != nil || string(got) != want {
				t.Fatalf("golden:\n got: %s\n err: %v\nwant: %s", got, err, want)
			}
			decoded, err := decodeDispositionInboxRecord([]byte(want))
			if err != nil || !reflect.DeepEqual(decoded, canonical) {
				t.Fatalf("decode: %+v %v", decoded, err)
			}
			for _, member := range tc.bad {
				renamed := []byte(strings.Replace(want, member, `"x"`, 1))
				if bytes.Equal(renamed, []byte(want)) {
					t.Fatalf("vacuous: %s is not in the golden bytes", member)
				}
				if _, err := decodeDispositionInboxRecord(renamed); err == nil {
					t.Fatalf("noncanonical spelling of %s accepted", member)
				}
			}
		})
	}
}

// dispositionCommitThenFail commits an ordered update and then reports failure,
// which is the ambiguous acknowledgement every durable write can meet. A caller
// that retries must learn the durable winner rather than write a second one.
type dispositionCommitThenFail struct {
	storage.OrderedIndex
	arm bool
}

func (f *dispositionCommitThenFail) Update(ctx context.Context, id storage.OrderedID, revision uint64, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, error) {
	stored, err := f.OrderedIndex.Update(ctx, id, revision, value, rank, due)
	if err == nil && f.arm {
		f.arm = false
		return storage.OrderedRecord{}, errors.New("ambiguous acknowledgement")
	}
	return stored, err
}

// Both edges are compare-and-swaps of one record, so an ambiguous
// acknowledgement is resolved the same way for both: the caller retries, is
// told the current revision, rereads, and finds the write that committed.
func TestDispositionAmbiguousAcknowledgementResolvesToTheDurableWinner(t *testing.T) {
	backend := memstore.New()
	fault := &dispositionCommitThenFail{OrderedIndex: backend.OrderedIndex}
	backend.OrderedIndex = fault
	reader := &fakeEvidence{evidence: appliedEvidence()}
	s := openStore(t, backend, WithClock(newMovableClock(settlementNow)), WithDispositionEvidence(reader))
	createDispositionCatalog(t, s)
	admitted, _, err := s.AdmitDispositionCommand(context.Background(), dispositionRequest())
	if err != nil {
		t.Fatal(err)
	}
	claimed := fileDispositionClaim(t, s, admitted, DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: settlementExpiry})

	fault.arm = true
	if _, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed)); err == nil {
		t.Fatal("ambiguous write reported success")
	}
	get := GetDispositionCommandRequest{TenantID: claimed.Record.Descriptor.TenantID, SessionID: claimed.Record.Descriptor.SessionID, CommandID: claimed.Record.Descriptor.CommandID}
	applying, err := s.GetDispositionCommand(context.Background(), get)
	if err != nil || applying.Record.State != InboxStateApplying || applying.Record.Attempt.AttemptID != settlementAttempt {
		t.Fatalf("the ambiguous attempt did not commit: %+v %v", applying, err)
	}
	retry := beginRequest(claimed)
	retry.AttemptID = "attempt/retry"
	_, retryErr := s.BeginDispositionAttempt(context.Background(), retry)
	conflict := assertInboxCode(t, retryErr, InboxErrorConflict)
	if conflict.Revision != applying.Revision {
		t.Fatalf("conflict revision = %d, want %d", conflict.Revision, applying.Revision)
	}

	fault.arm = true
	if _, _, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc)); err == nil {
		t.Fatal("ambiguous settlement reported success")
	}
	settled, err := s.GetDispositionCommand(context.Background(), get)
	if err != nil || settled.Record.State != InboxStateApplied || settled.Record.Outcome.Kind != DispositionApplied {
		t.Fatalf("the ambiguous settlement did not commit: %+v %v", settled, err)
	}
	again, ok, err := s.SettleDispositionCommand(context.Background(), settleRequest(settled, settlementResidenc))
	if err != nil || ok || *again.Record.Outcome != *settled.Record.Outcome {
		t.Fatalf("retry after an ambiguous settlement: %+v %v %v", again, ok, err)
	}
	if *settled.Record.Attempt != *applying.Record.Attempt {
		t.Fatalf("attempt changed: %+v", settled.Record.Attempt)
	}
}
