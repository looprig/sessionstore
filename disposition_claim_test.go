package sessionstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// Every fixture in this file drives a DISPOSITION session and takes its
// residency the way a consumer takes one — through AcquireResidency, which is
// the only route that yields an epoch the claim edge will accept. That is not
// ceremony: v0.7.0 shipped twenty green tests over the legacy protocol mode, and
// a claim test that manufactured its own epoch would repeat the mistake in the
// other direction by never proving the two ends fit together.

// claimFixture admits one PENDING command on a real provider and returns a LIVE
// residency grant over the same session.
func claimFixture(t *testing.T, opts ...Option) (*Store, *movableClock, DispositionInboxEntry, *ResidencyGrant) {
	t.Helper()
	clock := newMovableClock(settlementNow)
	s := openStore(t, memstore.New(), append([]Option{WithClock(clock)}, opts...)...)
	return withClaimFixture(t, s, clock)
}

func withClaimFixture(t *testing.T, s *Store, clock *movableClock) (*Store, *movableClock, DispositionInboxEntry, *ResidencyGrant) {
	t.Helper()
	createDispositionCatalog(t, s)
	admitted, created, err := s.AdmitDispositionCommand(context.Background(), dispositionRequest())
	if err != nil || !created {
		t.Fatalf("AdmitDispositionCommand: %v %v", created, err)
	}
	if admitted.Record.State != InboxStatePending || admitted.Record.Claim != nil {
		t.Fatalf("vacuous fixture: admission did not produce a pending unclaimed record: %+v", admitted.Record)
	}
	// BURN ONE RESIDENCY EPOCH. The provider issues epoch 1 first, and a grant
	// at epoch 1 has no strictly lower epoch beneath it — so every "a superseded
	// residency is refused" case below would in fact be asserting the zero-epoch
	// request refusal, which is a different rule. Acquiring and releasing once
	// makes the mark this file fences against expressible at all.
	burn, err := s.AcquireResidency(context.Background(), AcquireResidencyRequest{TenantID: catalogTenant, SessionID: catalogSession})
	if err != nil {
		t.Fatalf("AcquireResidency: %v", err)
	}
	if err := burn.Release(context.Background()); err != nil {
		t.Fatalf("release burned residency: %v", err)
	}
	grant := acquireTestResidency(t, s)
	if grant.Epoch() < 2 {
		t.Fatalf("vacuous: residency epoch %d leaves no strictly lower epoch for a fence to refuse", grant.Epoch())
	}
	return s, clock, admitted, grant
}

func acquireTestResidency(t *testing.T, s *Store) *ResidencyGrant {
	t.Helper()
	grant, err := s.AcquireResidency(context.Background(), AcquireResidencyRequest{TenantID: catalogTenant, SessionID: catalogSession})
	if err != nil {
		t.Fatalf("AcquireResidency: %v", err)
	}
	t.Cleanup(func() {
		if err := grant.Release(context.Background()); err != nil {
			t.Errorf("release residency: %v", err)
		}
	})
	if grant.Epoch() == 0 {
		t.Fatal("vacuous: the provider issued residency epoch 0, which no fence can measure")
	}
	return grant
}

// mustClaimDisposition takes a claim and fails unless the store actually wrote one, so a
// downstream assertion can never be reached with a zero entry.
func mustClaimDisposition(t *testing.T, s *Store, entry DispositionInboxEntry, residency ResidencyEpoch, expires time.Time) DispositionInboxEntry {
	t.Helper()
	claimed, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(entry, residency, expires))
	if err != nil || !ok {
		t.Fatalf("ClaimDispositionCommand: %v %v", ok, err)
	}
	if claimed.Record.State != InboxStateClaimed || claimed.Record.Claim == nil {
		t.Fatalf("claim produced %+v, want a claimed record carrying a claim", claimed.Record)
	}
	return claimed
}

// claimPastDeadline is a claim expiry that outlives the command's apply deadline
// and still falls inside MaxCommandClaimTTL of settlementNow. It is what makes
// the deadline race expressible: a live claim past the deadline.
var claimPastDeadline = settlementNow.Add(55 * time.Minute)

func claimRequest(entry DispositionInboxEntry, residency ResidencyEpoch, expires time.Time) ClaimDispositionCommandRequest {
	d := entry.Record.Descriptor
	return ClaimDispositionCommandRequest{
		TenantID:         d.TenantID,
		SessionID:        d.SessionID,
		CommandID:        d.CommandID,
		ExpectedRevision: entry.Revision,
		ResidencyEpoch:   residency,
		ClaimExpiresAt:   expires,
	}
}

func rejectRequest(entry DispositionInboxEntry, residency ResidencyEpoch) RejectDispositionCommandRequest {
	d := entry.Record.Descriptor
	return RejectDispositionCommandRequest{
		TenantID:         d.TenantID,
		SessionID:        d.SessionID,
		CommandID:        d.CommandID,
		ExpectedRevision: entry.Revision,
		ResidencyEpoch:   residency,
	}
}

func getDisposition(t *testing.T, s *Store, entry DispositionInboxEntry) DispositionInboxEntry {
	t.Helper()
	d := entry.Record.Descriptor
	got, err := s.GetDispositionCommand(context.Background(), GetDispositionCommandRequest{TenantID: d.TenantID, SessionID: d.SessionID, CommandID: d.CommandID})
	if err != nil {
		t.Fatalf("GetDispositionCommand: %v", err)
	}
	return got
}

func assertDispositionUnchanged(t *testing.T, s *Store, want DispositionInboxEntry) {
	t.Helper()
	got := getDisposition(t, s, want)
	if got.Revision != want.Revision || got.Record.State != want.Record.State {
		t.Fatalf("a refused transition wrote: %+v, want revision %d state %q", got, want.Revision, want.Record.State)
	}
}

// The claim edge is the missing entry point of the disposition lifecycle: it
// moves a pending command into claimed under the caller's residency, and the
// record it produces is exactly what BeginDispositionAttempt's existing
// precondition consumes.
func TestClaimDispositionCommandMovesPendingToClaimed(t *testing.T) {
	s, _, admitted, grant := claimFixture(t)
	claimed, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(admitted, grant.Epoch(), settlementExpiry))
	if err != nil || !ok {
		t.Fatalf("ClaimDispositionCommand: %v %v", ok, err)
	}
	if claimed.Record.State != InboxStateClaimed {
		t.Fatalf("state = %q, want %q", claimed.Record.State, InboxStateClaimed)
	}
	if claimed.Record.Claim == nil {
		t.Fatal("claimed record carries no claim")
	}
	if claimed.Record.Claim.ResidencyEpoch != grant.Epoch() || !claimed.Record.Claim.ExpiresAt.Equal(settlementExpiry) {
		t.Fatalf("claim = %+v, want residency %d expiring %v", *claimed.Record.Claim, grant.Epoch(), settlementExpiry)
	}
	if claimed.Record.Attempt != nil || claimed.Record.Outcome != nil {
		t.Fatalf("claim invented a post-claim member: %+v", claimed.Record)
	}
	if claimed.Revision <= admitted.Revision {
		t.Fatalf("revision %d did not advance past %d", claimed.Revision, admitted.Revision)
	}
	if claimed.AcceptedOrder != admitted.AcceptedOrder {
		t.Fatalf("acceptance order moved %d -> %d", admitted.AcceptedOrder, claimed.AcceptedOrder)
	}
	// It composes with the existing precondition WITHOUT loosening it: the
	// attempt edge still requires State == claimed and the claim's own
	// residency, and this is the first record produced by the package itself
	// that satisfies both.
	req := beginRequest(claimed)
	req.ResidencyEpoch = grant.Epoch()
	applying, err := s.BeginDispositionAttempt(context.Background(), req)
	if err != nil {
		t.Fatalf("a real claim did not satisfy BeginDispositionAttempt: %v", err)
	}
	if applying.Record.State != InboxStateApplying || applying.Record.Attempt.ResidencyEpoch != grant.Epoch() {
		t.Fatalf("attempt over a real claim: %+v", applying.Record)
	}
	if !getDisposition(t, s, applying).Record.Claim.ExpiresAt.Equal(settlementExpiry) {
		t.Fatal("the attempt rewrote the claim it consumed")
	}
}

// A replay of the SAME request is idempotent and writes nothing. The replay is
// over the reread, not over the lost response: a stale revision is still a
// conflict carrying the current one, which is this package's standing answer to
// a compare-and-swap whose outcome the caller did not learn.
func TestClaimDispositionCommandIsIdempotentOnReplay(t *testing.T) {
	s, _, admitted, grant := claimFixture(t)
	claimed, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(admitted, grant.Epoch(), settlementExpiry))
	if err != nil || !ok {
		t.Fatalf("first claim: %v %v", ok, err)
	}
	again, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant.Epoch(), settlementExpiry))
	if err != nil || ok {
		t.Fatalf("replayed claim: %+v %v %v", again, ok, err)
	}
	if again.Revision != claimed.Revision || *again.Record.Claim != *claimed.Record.Claim {
		t.Fatalf("replay wrote: %+v, want %+v", again, claimed)
	}
	stale, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(admitted, grant.Epoch(), settlementExpiry))
	conflict := assertInboxCode(t, err, InboxErrorConflict)
	if ok || conflict.Revision != claimed.Revision {
		t.Fatalf("stale replay = %+v %v, conflict revision %d, want %d", stale, ok, conflict.Revision, claimed.Revision)
	}
}

// A claim CANNOT BE RENEWED, for legacy ClaimCommand's reason: renewal would let
// one writer hold a command indefinitely, and the state that legitimately spans
// a long application is applying. A second call at the same residency naming a
// DIFFERENT expiry is therefore a renewal request and is refused — which is also
// what keeps the idempotent arm above from being a renewal in disguise.
func TestClaimDispositionCommandRefusesRenewal(t *testing.T) {
	s, _, admitted, grant := claimFixture(t)
	claimed := mustClaimDisposition(t, s, admitted, grant.Epoch(), settlementExpiry)
	later := settlementExpiry.Add(5 * time.Minute)
	if later.After(inboxDeadline) {
		t.Fatal("vacuous: the renewal this test refuses would also be refused by the apply deadline")
	}
	_, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant.Epoch(), later))
	assertInboxCode(t, err, InboxErrorClaimHeld)
	if ok {
		t.Fatal("renewal accepted")
	}
	if !getDisposition(t, s, claimed).Record.Claim.ExpiresAt.Equal(settlementExpiry) {
		t.Fatal("a refused renewal extended the claim")
	}
}

// A lapsed claim is no claim: re-claiming over one is an ordinary claim at the
// same residency, and a STRICTLY HIGHER residency may take over even a LIVE one,
// which is what makes failover a claim rather than a special reclaim.
func TestClaimDispositionCommandAdmitsALapsedClaimAndASuccessor(t *testing.T) {
	t.Run("lapsed claim at the same residency", func(t *testing.T) {
		s, clock, admitted, grant := claimFixture(t)
		claimed := mustClaimDisposition(t, s, admitted, grant.Epoch(), settlementExpiry)
		clock.set(settlementExpiry)
		renewed, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant.Epoch(), settlementExpiry.Add(10*time.Minute)))
		if err != nil || !ok {
			t.Fatalf("re-claim over a lapsed claim: %v %v", ok, err)
		}
		if !renewed.Record.Claim.ExpiresAt.Equal(settlementExpiry.Add(10 * time.Minute)) {
			t.Fatalf("re-claim kept the lapsed expiry: %+v", *renewed.Record.Claim)
		}
	})

	t.Run("successor residency over a live claim", func(t *testing.T) {
		s, _, admitted, first := claimFixture(t)
		claimed := mustClaimDisposition(t, s, admitted, first.Epoch(), settlementExpiry)
		if err := first.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
		second := acquireTestResidency(t, s)
		if second.Epoch() <= first.Epoch() {
			t.Fatalf("vacuous: successor residency %d is not above %d, so this is not a successor at all", second.Epoch(), first.Epoch())
		}
		stolen, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, second.Epoch(), settlementExpiry))
		if err != nil || !ok {
			t.Fatalf("successor claim over a live claim: %v %v", ok, err)
		}
		if stolen.Record.Claim.ResidencyEpoch != second.Epoch() {
			t.Fatalf("claim residency = %d, want the successor's %d", stolen.Record.Claim.ResidencyEpoch, second.Epoch())
		}
	})
}

// The refusal ORDER is the contract. Each case satisfies a LATER refusal too, so
// the assertion fails if two are ever reordered.
func TestClaimDispositionCommandRefusalOrder(t *testing.T) {
	// An applying record has already authorized a dispatch. That fact stays
	// true, so it answers ahead of the residency fence — which would otherwise
	// have a claim to measure a superseded caller against.
	t.Run("an authorized attempt before the residency fence", func(t *testing.T) {
		s, _, admitted, grant := claimFixture(t)
		claimed := mustClaimDisposition(t, s, admitted, grant.Epoch(), settlementExpiry)
		req := beginRequest(claimed)
		req.ResidencyEpoch = grant.Epoch()
		applying, err := s.BeginDispositionAttempt(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if applying.Record.Claim == nil {
			t.Fatal("vacuous: an applying record has no claim for the fence to consult")
		}
		_, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(applying, grant.Epoch()-1, settlementExpiry))
		state := assertInboxCode(t, err, InboxErrorState)
		if ok || state.Field != "attempt" {
			t.Fatalf("field = %q, want attempt (ok=%v)", state.Field, ok)
		}
	})

	// A superseded residency meeting a claim that has ALSO lapsed, on a command
	// whose apply deadline has ALSO passed, is told it was superseded: that is
	// the answer that is permanent.
	t.Run("superseded residency before the deadline and a lapsed claim", func(t *testing.T) {
		s, clock, admitted, grant := claimFixture(t)
		claimed := mustClaimDisposition(t, s, admitted, grant.Epoch(), settlementExpiry)
		clock.set(inboxDeadline)
		_, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant.Epoch()-1, inboxDeadline.Add(time.Minute)))
		epoch := assertInboxCode(t, err, InboxErrorEpoch)
		if ok || epoch.Field != "residency_epoch" || epoch.Epoch != uint64(grant.Epoch()) {
			t.Fatalf("epoch refusal = %+v (ok=%v), want field residency_epoch at %d", epoch, ok, grant.Epoch())
		}
	})

	// The apply deadline closes NEW claims permanently, for every caller, and it
	// answers ahead of the held-claim refusal, which merely expires.
	t.Run("the deadline before a held claim", func(t *testing.T) {
		s, clock, admitted, grant := claimFixture(t)
		claimed := mustClaimDisposition(t, s, admitted, grant.Epoch(), claimPastDeadline)
		clock.set(inboxDeadline)
		if !clock.Now().Before(claimed.Record.Claim.ExpiresAt) {
			t.Fatal("vacuous: the claim this case requires to still be live has lapsed")
		}
		_, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant.Epoch(), claimPastDeadline.Add(time.Minute)))
		deadline := assertInboxCode(t, err, InboxErrorDeadline)
		if ok || deadline.Field != "apply_deadline" {
			t.Fatalf("field = %q, want apply_deadline (ok=%v)", deadline.Field, ok)
		}
	})

	// The idempotent replay answers ahead of the deadline: a replay is not a NEW
	// claim, and the claim it replays is already durable.
	t.Run("an exact replay before the deadline", func(t *testing.T) {
		s, clock, admitted, grant := claimFixture(t)
		expires := claimPastDeadline
		claimed := mustClaimDisposition(t, s, admitted, grant.Epoch(), expires)
		clock.set(inboxDeadline)
		again, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant.Epoch(), expires))
		if err != nil || ok || again.Revision != claimed.Revision {
			t.Fatalf("replay past the deadline = %+v %v %v", again, ok, err)
		}
	})
}

// countingOrderedIndex counts the reads a transition performs so a test can
// prove a refusal happened BEFORE any of them.
type countingOrderedIndex struct {
	storage.OrderedIndex
	gets int
}

func (c *countingOrderedIndex) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	c.gets++
	return c.OrderedIndex.Get(ctx, id)
}

// A malformed request is refused on its own terms, before the store reads the
// catalog, the mode witness or the record — which is what makes an invalid
// request incapable of installing the protocol-mode witness the fence may PUT.
func TestDispositionClaimEdgesValidateBeforeAnyRead(t *testing.T) {
	for _, tc := range []struct {
		name  string
		claim func(*ClaimDispositionCommandRequest)
		rejec func(*RejectDispositionCommandRequest)
		field string
	}{
		{"zero revision", func(r *ClaimDispositionCommandRequest) { r.ExpectedRevision = 0 }, func(r *RejectDispositionCommandRequest) { r.ExpectedRevision = 0 }, "expected_revision"},
		{"empty command", func(r *ClaimDispositionCommandRequest) { r.CommandID = "" }, func(r *RejectDispositionCommandRequest) { r.CommandID = "" }, "command_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := memstore.New()
			counter := &countingOrderedIndex{OrderedIndex: backend.OrderedIndex}
			backend.OrderedIndex = counter
			clock := newMovableClock(settlementNow)
			s := openStore(t, backend, WithClock(clock))
			_, _, admitted, grant := withClaimFixture(t, s, clock)

			claim := claimRequest(admitted, grant.Epoch(), settlementExpiry)
			tc.claim(&claim)
			counter.gets = 0
			_, ok, err := s.ClaimDispositionCommand(context.Background(), claim)
			invalid := assertInboxCode(t, err, InboxErrorInvalid)
			if ok || invalid.Field != tc.field || counter.gets != 0 {
				t.Fatalf("claim: field %q ok %v reads %d, want %q false 0", invalid.Field, ok, counter.gets, tc.field)
			}

			reject := rejectRequest(admitted, grant.Epoch())
			tc.rejec(&reject)
			counter.gets = 0
			_, ok, err = s.RejectDispositionCommand(context.Background(), reject)
			invalid = assertInboxCode(t, err, InboxErrorInvalid)
			if ok || invalid.Field != tc.field || counter.gets != 0 {
				t.Fatalf("reject: field %q ok %v reads %d, want %q false 0", invalid.Field, ok, counter.gets, tc.field)
			}
		})
	}
}

// The claim edge's own request members are validated too, and the expiry bound
// is the legacy claim's: a claim must lapse in the FUTURE and within
// MaxCommandClaimTTL, because a claim born expired is indistinguishable from no
// claim and an unbounded one parks a row in the due view for the deployment's
// life.
func TestClaimDispositionCommandValidatesItsClaim(t *testing.T) {
	for _, tc := range []struct {
		name    string
		expires time.Time
		epoch   func(ResidencyEpoch) ResidencyEpoch
		field   string
	}{
		{"zero residency", settlementExpiry, func(ResidencyEpoch) ResidencyEpoch { return 0 }, "residency_epoch"},
		{"unset expiry", time.Time{}, func(e ResidencyEpoch) ResidencyEpoch { return e }, "claim_expires_at"},
		{"expiry in the past", settlementNow.Add(-time.Second), func(e ResidencyEpoch) ResidencyEpoch { return e }, "claim_expires_at"},
		{"expiry at now", settlementNow, func(e ResidencyEpoch) ResidencyEpoch { return e }, "claim_expires_at"},
		{"expiry beyond the TTL", settlementNow.Add(MaxCommandClaimTTL + time.Second), func(e ResidencyEpoch) ResidencyEpoch { return e }, "claim_expires_at"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, admitted, grant := claimFixture(t)
			req := claimRequest(admitted, tc.epoch(grant.Epoch()), tc.expires)
			_, ok, err := s.ClaimDispositionCommand(context.Background(), req)
			invalid := assertInboxCode(t, err, InboxErrorInvalid)
			if ok || invalid.Field != tc.field {
				t.Fatalf("field = %q, want %q (ok=%v)", invalid.Field, tc.field, ok)
			}
			assertDispositionUnchanged(t, s, admitted)
		})
	}
	// The control: the boundary value one tick inside the TTL is ACCEPTED, so
	// the refusals above are the bound rather than a blanket rejection.
	t.Run("the TTL boundary is inclusive", func(t *testing.T) {
		s, _, admitted, grant := claimFixture(t)
		if _, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(admitted, grant.Epoch(), settlementNow.Add(MaxCommandClaimTTL))); err != nil || !ok {
			t.Fatalf("the TTL bound itself was refused: %v %v", ok, err)
		}
	})
}

// The pre-dispatch reject edge produces the terminal rejection shape the codec
// has always accepted and nothing has ever written: rejected, with no attempt
// and no outcome, from pending.
func TestRejectDispositionCommandRejectsAPendingCommand(t *testing.T) {
	s, _, admitted, grant := claimFixture(t, WithControlShards(1))
	page, err := s.ListDueDispositionCommands(context.Background(), ListDueDispositionCommandsRequest{Shard: 0, DueAtOrBefore: inboxDeadline, Limit: 10})
	if err != nil || len(page.Commands) != 1 {
		t.Fatalf("POSITIVE CONTROL: a pending command is not due in this fixture: %+v %v", page, err)
	}
	rejected, ok, err := s.RejectDispositionCommand(context.Background(), rejectRequest(admitted, grant.Epoch()))
	if err != nil || !ok {
		t.Fatalf("RejectDispositionCommand: %v %v", ok, err)
	}
	if rejected.Record.State != InboxStateRejected {
		t.Fatalf("state = %q, want %q", rejected.Record.State, InboxStateRejected)
	}
	if rejected.Record.Claim != nil || rejected.Record.Attempt != nil || rejected.Record.Outcome != nil {
		t.Fatalf("a pre-dispatch rejection of a PENDING command invented a member: %+v", rejected.Record)
	}
	page, err = s.ListDueDispositionCommands(context.Background(), ListDueDispositionCommandsRequest{Shard: 0, DueAtOrBefore: inboxDeadline, Limit: 10})
	if err != nil || len(page.Commands) != 0 || page.Examined != 0 {
		t.Fatalf("a rejected command is still due: %+v %v", page, err)
	}
}

// Rejecting a CLAIMED command keeps the claim: it is the durable record of who
// was working on the command when it was refused, and the codec admits it
// precisely because an attemptless rejection may have one.
func TestRejectDispositionCommandKeepsTheClaimItRefuses(t *testing.T) {
	s, _, admitted, grant := claimFixture(t)
	claimed := mustClaimDisposition(t, s, admitted, grant.Epoch(), settlementExpiry)
	rejected, ok, err := s.RejectDispositionCommand(context.Background(), rejectRequest(claimed, grant.Epoch()))
	if err != nil || !ok {
		t.Fatalf("reject a claimed command: %v %v", ok, err)
	}
	if rejected.Record.State != InboxStateRejected || rejected.Record.Claim == nil {
		t.Fatalf("rejection dropped the claim: %+v", rejected.Record)
	}
	if *rejected.Record.Claim != *claimed.Record.Claim {
		t.Fatalf("claim = %+v, want %+v", *rejected.Record.Claim, *claimed.Record.Claim)
	}
	if rejected.Record.Attempt != nil || rejected.Record.Outcome != nil {
		t.Fatalf("rejection invented a post-claim member: %+v", rejected.Record)
	}
}

// THIS IS THE EDGE'S DEFINING REFUSAL. The pre-dispatch reject must never become
// a caller-authored rejection AFTER an attempt: once applying commits, the only
// thing that may close the command is verified evidence, and a runtime error
// with no durable disposition leaves it applying until a SUCCESSOR runtime
// writes not_applied under a strictly later journal grant.
func TestRejectDispositionCommandRefusesACommandWithAnAttempt(t *testing.T) {
	s, _, admitted, grant := claimFixture(t, WithDispositionEvidence(&fakeEvidence{evidence: appliedEvidence()}))
	claimed := mustClaimDisposition(t, s, admitted, grant.Epoch(), settlementExpiry)
	req := beginRequest(claimed)
	req.ResidencyEpoch = grant.Epoch()
	applying, err := s.BeginDispositionAttempt(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if applying.Record.Attempt == nil {
		t.Fatal("vacuous fixture: no attempt to refuse")
	}
	// Every residency a caller could name, including the claim's own, the
	// successor's, and none at all.
	for _, residency := range []ResidencyEpoch{0, grant.Epoch(), grant.Epoch() + 1} {
		_, ok, err := s.RejectDispositionCommand(context.Background(), rejectRequest(applying, residency))
		state := assertInboxCode(t, err, InboxErrorState)
		if ok || state.Field != "attempt" {
			t.Fatalf("residency %d: field %q ok %v, want attempt false", residency, state.Field, ok)
		}
		assertDispositionUnchanged(t, s, applying)
	}
}

// A replayed pre-dispatch rejection is idempotent and writes nothing.
func TestRejectDispositionCommandIsIdempotentForAPreDispatchRejection(t *testing.T) {
	s, _, admitted, grant := claimFixture(t)
	rejected, ok, err := s.RejectDispositionCommand(context.Background(), rejectRequest(admitted, grant.Epoch()))
	if err != nil || !ok {
		t.Fatalf("first rejection: %v %v", ok, err)
	}
	again, ok, err := s.RejectDispositionCommand(context.Background(), rejectRequest(rejected, grant.Epoch()))
	if err != nil || ok || again.Revision != rejected.Revision || again.Record.State != InboxStateRejected {
		t.Fatalf("replayed rejection: %+v %v %v", again, ok, err)
	}
	// A reconciler that never held a residency gets the same idempotent answer.
	again, ok, err = s.RejectDispositionCommand(context.Background(), rejectRequest(rejected, 0))
	if err != nil || ok || again.Revision != rejected.Revision {
		t.Fatalf("reconciler replay: %+v %v %v", again, ok, err)
	}
}

// A RECONCILER names no residency and is not measured against any lease. What
// it buys is exactly the authority the record grants: it may settle a command
// nobody is working on, and nothing else.
func TestRejectDispositionCommandNeedsNoResidency(t *testing.T) {
	t.Run("a pending command", func(t *testing.T) {
		s, _, admitted, _ := claimFixture(t)
		if _, ok, err := s.RejectDispositionCommand(context.Background(), rejectRequest(admitted, 0)); err != nil || !ok {
			t.Fatalf("reconciler rejection of a pending command: %v %v", ok, err)
		}
	})
	t.Run("a lapsed claim", func(t *testing.T) {
		s, clock, admitted, grant := claimFixture(t)
		claimed := mustClaimDisposition(t, s, admitted, grant.Epoch(), settlementExpiry)
		clock.set(settlementExpiry)
		if _, ok, err := s.RejectDispositionCommand(context.Background(), rejectRequest(claimed, 0)); err != nil || !ok {
			t.Fatalf("reconciler rejection over a lapsed claim: %v %v", ok, err)
		}
	})
	t.Run("a live claim is not the reconciler's to settle", func(t *testing.T) {
		s, _, admitted, grant := claimFixture(t)
		claimed := mustClaimDisposition(t, s, admitted, grant.Epoch(), settlementExpiry)
		_, ok, err := s.RejectDispositionCommand(context.Background(), rejectRequest(claimed, 0))
		held := assertInboxCode(t, err, InboxErrorClaimHeld)
		if ok || held.Field != "claim" {
			t.Fatalf("field = %q, want claim (ok=%v)", held.Field, ok)
		}
		assertDispositionUnchanged(t, s, claimed)
	})
}

// A caller that VOLUNTEERS a residency is asserting a view of the session, and a
// view below the record's high-water mark is stale. The fence is narrower than
// an authority boundary — nothing forces a caller to name an epoch — and is
// still worth running, exactly as legacy RejectCommand states.
func TestRejectDispositionCommandFencesAVolunteeredResidency(t *testing.T) {
	s, clock, admitted, grant := claimFixture(t)
	claimed := mustClaimDisposition(t, s, admitted, grant.Epoch(), settlementExpiry)
	clock.set(settlementExpiry)
	_, ok, err := s.RejectDispositionCommand(context.Background(), rejectRequest(claimed, grant.Epoch()-1))
	epoch := assertInboxCode(t, err, InboxErrorEpoch)
	if ok || epoch.Field != "residency_epoch" || epoch.Epoch != uint64(grant.Epoch()) {
		t.Fatalf("epoch refusal = %+v (ok=%v)", epoch, ok)
	}
	// The control: the SAME lapsed-claim record is settleable by the caller that
	// names no epoch at all, so the refusal above is the fence and not the state.
	if _, ok, err := s.RejectDispositionCommand(context.Background(), rejectRequest(claimed, 0)); err != nil || !ok {
		t.Fatalf("CONTROL: the same record refused a reconciler too: %v %v", ok, err)
	}
}

// Both edges are ordinary disposition transitions: they read the immutable
// catalog binding and re-fence the session's protocol mode, so a legacy session
// is refused with a CatalogError about the binding rather than an InboxError
// about the command.
func TestDispositionClaimEdgesRefuseALegacySession(t *testing.T) {
	s := openStore(t, memstore.New(), WithClock(newMovableClock(settlementNow)))
	if _, _, err := s.CreateCatalogEntry(context.Background(), testCreateRequest()); err != nil {
		t.Fatal(err)
	}
	entry := DispositionInboxEntry{Record: DispositionInboxRecord{Descriptor: DispositionCommandDescriptor{TenantID: catalogTenant, SessionID: catalogSession, CommandID: "public/command:1"}}, Revision: 1}
	_, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(entry, 1, settlementExpiry))
	catalog := assertCatalogCode(t, err, CatalogErrorInvalid)
	if ok || catalog.Field != "binding.protocol_mode" {
		t.Fatalf("claim: field %q ok %v", catalog.Field, ok)
	}
	_, ok, err = s.RejectDispositionCommand(context.Background(), rejectRequest(entry, 1))
	catalog = assertCatalogCode(t, err, CatalogErrorInvalid)
	if ok || catalog.Field != "binding.protocol_mode" {
		t.Fatalf("reject: field %q ok %v", catalog.Field, ok)
	}
}

// Both edges are durable: what they wrote is what a reopened store reads.
func TestDispositionClaimEdgesSurviveReopen(t *testing.T) {
	backend := memstore.New()
	clock := newMovableClock(settlementNow)
	first := openStore(t, backend, WithClock(clock))
	_, _, admitted, grant := withClaimFixture(t, first, clock)
	claimed, _, err := first.ClaimDispositionCommand(context.Background(), claimRequest(admitted, grant.Epoch(), settlementExpiry))
	if err != nil {
		t.Fatal(err)
	}
	rejected, _, err := first.RejectDispositionCommand(context.Background(), rejectRequest(claimed, grant.Epoch()))
	if err != nil {
		t.Fatal(err)
	}
	second := openStore(t, backend, WithClock(clock))
	got := getDisposition(t, second, rejected)
	if got.Revision != rejected.Revision || got.Record.State != InboxStateRejected || got.Record.Attempt != nil || got.Record.Outcome != nil {
		t.Fatalf("reopened record = %+v, want the pre-dispatch rejection at revision %d", got, rejected.Revision)
	}
	if got.Record.Claim == nil || *got.Record.Claim != *claimed.Record.Claim {
		t.Fatalf("reopened claim = %+v, want %+v", got.Record.Claim, claimed.Record.Claim)
	}
}

// FACTORY'S AUTHORITY, checked rather than asserted. Factory holds a tenant and
// a session id and nothing else: it cannot bind a protocol mode, because it has
// no source for the four binding members and a mode is immutable after create.
//
// The reject edge needs NO residency at all, so Factory satisfies it with what
// it already has. The claim edge needs a residency EPOCH, and the only producer
// of one is AcquireResidency — whose whole request is a tenant and a session id
// over a catalog that is ALREADY disposition-bound. So Factory can satisfy that
// too, without supplying a binding member; what it cannot do is hold that lease
// concurrently with a Host, which is a deployment question and not an API one.
func TestFactoryAuthorityForBothDispositionEdges(t *testing.T) {
	clock := newMovableClock(settlementNow)
	s := openStore(t, memstore.New(), WithClock(clock))
	createDispositionCatalog(t, s)
	admit := func(t *testing.T, id string) DispositionInboxEntry {
		t.Helper()
		req := dispositionRequest()
		req.CommandID = sessionwire.CommandID("public/command:" + id)
		entry, _, err := s.AdmitDispositionCommand(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		return entry
	}

	// Everything Factory has.
	tenant, session := catalogTenant, catalogSession

	forReject := admit(t, "reject")
	if _, ok, err := s.RejectDispositionCommand(context.Background(), RejectDispositionCommandRequest{
		TenantID: tenant, SessionID: session, CommandID: forReject.Record.Descriptor.CommandID, ExpectedRevision: forReject.Revision,
	}); err != nil || !ok {
		t.Fatalf("Factory cannot reject with tenant and session alone: %v %v", ok, err)
	}

	grant, err := s.AcquireResidency(context.Background(), AcquireResidencyRequest{TenantID: tenant, SessionID: session})
	if err != nil {
		t.Fatalf("Factory cannot acquire residency with tenant and session alone: %v", err)
	}
	defer func() {
		if err := grant.Release(context.Background()); err != nil {
			t.Errorf("release: %v", err)
		}
	}()
	forClaim := admit(t, "claim")
	if _, ok, err := s.ClaimDispositionCommand(context.Background(), ClaimDispositionCommandRequest{
		TenantID: tenant, SessionID: session, CommandID: forClaim.Record.Descriptor.CommandID, ExpectedRevision: forClaim.Revision,
		ResidencyEpoch: grant.Epoch(), ClaimExpiresAt: settlementExpiry,
	}); err != nil || !ok {
		t.Fatalf("Factory cannot claim with a residency it acquired itself: %v %v", ok, err)
	}
}

// A terminal command has no transitions left, and the sharp case is THIS
// EDGE'S OWN prior result: a pre-dispatch rejection has no attempt, so every
// later guard in the claim edge would wave it through. Only the terminal check
// refuses it, and a command that could be re-claimed after being rejected would
// be re-dispatchable after being refused.
func TestClaimDispositionCommandRefusesAPreDispatchRejection(t *testing.T) {
	s, _, admitted, grant := claimFixture(t)
	rejected, ok, err := s.RejectDispositionCommand(context.Background(), rejectRequest(admitted, grant.Epoch()))
	if err != nil || !ok {
		t.Fatalf("reject: %v %v", ok, err)
	}
	if rejected.Record.Attempt != nil || rejected.Record.Claim != nil {
		t.Fatalf("vacuous: this record carries a member a later guard would refuse it on: %+v", rejected.Record)
	}
	_, ok, err = s.ClaimDispositionCommand(context.Background(), claimRequest(rejected, grant.Epoch(), settlementExpiry))
	terminal := assertInboxCode(t, err, InboxErrorTerminal)
	if ok || terminal.Field != "state" {
		t.Fatalf("field = %q, want state (ok=%v)", terminal.Field, ok)
	}
	assertDispositionUnchanged(t, s, rejected)
}

// Both edges in disposition_claim.go answer "has a dispatch been authorized?"
// with ONE check — Attempt != nil — and their doc comments assert that a second
// comparison against InboxStateApplying would be an EQUIVALENT restatement
// rather than a second guard. An equivalence is the strongest claim a comment
// can make and carries no evidence, so it is discharged here over the codec's
// WHOLE domain rather than at the states a fixture happens to reach.
//
// The domain is every combination of every DECLARED state with the presence and
// absence of each of the three post-admission members. For every combination
// that the record validator ADMITS and that is not terminal, the two predicates
// must agree.
func TestANonTerminalDispositionRecordHasAnAttemptExactlyWhenApplying(t *testing.T) {
	t.Parallel()
	states := everyDispositionState(t)
	claim := &DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: settlementExpiry}
	attempt := &DispositionAttempt{AttemptID: settlementAttempt, JournalEpoch: settlementJournal, ResidencyEpoch: settlementResidenc, StartedAt: settlementNow}
	outcome := &DispositionOutcome{
		Kind: DispositionApplied, AttemptID: settlementAttempt, AttemptJournalEpoch: settlementJournal,
		AuthorJournalEpoch: settlementJournal, DispositionSeq: 12, EventID: "01J0000000000000000000EVNT", EventSeq: 12,
		SettlingResidencyEpoch: settlementResidenc, SettledAt: settlementNow,
	}
	admitted, checked := 0, 0
	for _, state := range states {
		for _, c := range []*DispositionClaim{nil, claim} {
			for _, a := range []*DispositionAttempt{nil, attempt} {
				for _, o := range []*DispositionOutcome{nil, outcome} {
					checked++
					r := DispositionInboxRecord{State: state, Claim: c, Attempt: a, Outcome: o}
					if validateDispositionState(&r) != nil {
						continue
					}
					admitted++
					if state.terminal() {
						continue
					}
					if (r.Attempt != nil) != (state == InboxStateApplying) {
						t.Errorf("state %q with attempt=%v is admitted and breaks the equivalence the two edges rely on", state, r.Attempt != nil)
					}
				}
			}
		}
	}
	if checked != len(states)*8 {
		t.Fatalf("the domain is %d combinations, want %d", checked, len(states)*8)
	}
	// Non-vacuity in BOTH directions: the validator must admit some records and
	// refuse some, or "for every admitted record" is a sentence about nothing.
	if admitted == 0 || admitted == checked {
		t.Fatalf("vacuous: %d of %d combinations admitted", admitted, checked)
	}
}

// A claim's two well-formedness rules are the RECORD's, so they must hold on
// every route into a stored record and not only on the edge that writes one.
// The expiry bound in particular is UNREACHABLE from ClaimDispositionCommand —
// validateBoundedExpiry refuses anything before now and anything more than
// MaxCommandClaimTTL after it, which is strictly inside the rankable range — so
// a probe driven only through the edge would leave it unmeasured. This drives
// the codec directly, which is the only caller that can reach it.
// emptyPayloadDigest is the identity an empty inline payload carries, which a
// record assembled by hand must supply because the codec derives nothing.
func emptyPayloadDigest() string {
	sum := sha256.Sum256(nil)
	return hex.EncodeToString(sum[:])
}

func TestADecodedDispositionClaimIsHeldToItsOwnBounds(t *testing.T) {
	t.Parallel()
	base := DispositionInboxRecord{
		Descriptor:    DispositionCommandDescriptor{TenantID: catalogTenant, SessionID: catalogSession, CommandID: "public/command:1", Binding: testSessionBinding(), RuntimeCommandID: inboxRuntime, Kind: inboxKind, PayloadDigest: emptyPayloadDigest()},
		AcceptedAt:    inboxAcceptedAt,
		ApplyDeadline: inboxDeadline,
		State:         InboxStateClaimed,
	}
	for _, tc := range []struct {
		name  string
		claim DispositionClaim
		field string
	}{
		{"zero residency", DispositionClaim{ResidencyEpoch: 0, ExpiresAt: settlementExpiry}, "residency_epoch"},
		{"expiry above the rankable range", DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: maxRankableTime.Add(time.Nanosecond)}, "claim_expires_at"},
		{"expiry below the rankable range", DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: minRankableTime.Add(-time.Nanosecond)}, "claim_expires_at"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := base
			claim := tc.claim
			r.Claim = &claim
			_, _, err := encodeDispositionInboxRecord(r)
			invalid := assertInboxCode(t, err, InboxErrorInvalid)
			if invalid.Field != tc.field {
				t.Fatalf("field = %q, want %q", invalid.Field, tc.field)
			}
		})
	}
	// The control: the SAME record with a well-formed claim encodes, so the
	// refusals above are the claim's bounds and not the surrounding record.
	r := base
	r.Claim = &DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: settlementExpiry}
	if _, _, err := encodeDispositionInboxRecord(r); err != nil {
		t.Fatalf("CONTROL: the well-formed record does not encode either: %v", err)
	}
}

// A replay names the SAME INSTANT, not the same time.Time struct. Every other
// fixture in this file is UTC and carries no monotonic reading, which is exactly
// the shape struct equality gets right — so this is the only case that can see
// the difference, and without it the idempotent arm is asserted only for callers
// that were never going to expose it.
//
// Both representations here are the real ones a consumer produces: a claim
// expiry computed in a non-UTC zone, and one carrying a monotonic reading from
// time.Now.
func TestClaimDispositionCommandReplaysAcrossTimeRepresentations(t *testing.T) {
	zone := time.FixedZone("Test/Plus0530", 5*3600+1800)
	for _, tc := range []struct {
		name  string
		first time.Time
		again time.Time
	}{
		{"a non-UTC zone", settlementExpiry.In(zone), settlementExpiry.In(zone)},
		{"UTC then the same instant elsewhere", settlementExpiry, settlementExpiry.In(zone)},
		{"a monotonic reading against a stored wall clock", settlementExpiry, settlementExpiry.Round(0).Add(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, admitted, grant := claimFixture(t)
			claimed := mustClaimDisposition(t, s, admitted, grant.Epoch(), tc.first)
			if !claimed.Record.Claim.ExpiresAt.Equal(tc.again) {
				t.Fatalf("vacuous: the stored expiry %v is not the instant being replayed %v", claimed.Record.Claim.ExpiresAt, tc.again)
			}
			again, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant.Epoch(), tc.again))
			if err != nil || ok || again.Revision != claimed.Revision {
				t.Fatalf("a replay naming the same instant was not idempotent: %+v %v %v", again, ok, err)
			}
		})
	}
	// The control: a DIFFERENT instant at the same residency is still a renewal
	// and is still refused, so the comparison above was loosened to the instant
	// and not to nothing.
	t.Run("a different instant is still a renewal", func(t *testing.T) {
		s, _, admitted, grant := claimFixture(t)
		claimed := mustClaimDisposition(t, s, admitted, grant.Epoch(), settlementExpiry.In(zone))
		_, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant.Epoch(), settlementExpiry.In(zone).Add(time.Second)))
		assertInboxCode(t, err, InboxErrorClaimHeld)
		if ok {
			t.Fatal("CONTROL: a renewal one second later was accepted as a replay")
		}
	})
}
