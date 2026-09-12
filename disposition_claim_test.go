package sessionstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
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
func mustClaimDisposition(t *testing.T, s *Store, entry DispositionInboxEntry, grant *ResidencyGrant, expires time.Time) DispositionInboxEntry {
	t.Helper()
	claimed, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(entry, grant, expires))
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

func claimRequest(entry DispositionInboxEntry, grant *ResidencyGrant, expires time.Time) ClaimDispositionCommandRequest {
	d := entry.Record.Descriptor
	return ClaimDispositionCommandRequest{
		TenantID:         d.TenantID,
		SessionID:        d.SessionID,
		CommandID:        d.CommandID,
		ExpectedRevision: entry.Revision,
		Residency:        grant,
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

// Claim comparisons in this file go through sameDispositionClaim rather than
// struct equality. Both operands always come back through the codec today, so
// `==` would agree — but this package has just paid for learning that struct
// equality over a time.Time is unsafe, and an assertion that later compares a
// codec value against a caller-constructed one would regress silently in exactly
// that class.
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
	claimed, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(admitted, grant, settlementExpiry))
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
	claimed, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(admitted, grant, settlementExpiry))
	if err != nil || !ok {
		t.Fatalf("first claim: %v %v", ok, err)
	}
	again, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant, settlementExpiry))
	if err != nil || ok {
		t.Fatalf("replayed claim: %+v %v %v", again, ok, err)
	}
	if again.Revision != claimed.Revision || !sameDispositionClaim(*again.Record.Claim, *claimed.Record.Claim) {
		t.Fatalf("replay wrote: %+v, want %+v", again, claimed)
	}
	stale, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(admitted, grant, settlementExpiry))
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
	claimed := mustClaimDisposition(t, s, admitted, grant, settlementExpiry)
	later := settlementExpiry.Add(5 * time.Minute)
	if later.After(inboxDeadline) {
		t.Fatal("vacuous: the renewal this test refuses would also be refused by the apply deadline")
	}
	_, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant, later))
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
		claimed := mustClaimDisposition(t, s, admitted, grant, settlementExpiry)
		clock.set(settlementExpiry)
		renewed, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant, settlementExpiry.Add(10*time.Minute)))
		if err != nil || !ok {
			t.Fatalf("re-claim over a lapsed claim: %v %v", ok, err)
		}
		if !renewed.Record.Claim.ExpiresAt.Equal(settlementExpiry.Add(10 * time.Minute)) {
			t.Fatalf("re-claim kept the lapsed expiry: %+v", *renewed.Record.Claim)
		}
	})

	t.Run("successor residency over a live claim", func(t *testing.T) {
		s, _, admitted, first := claimFixture(t)
		claimed := mustClaimDisposition(t, s, admitted, first, settlementExpiry)
		if err := first.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
		second := acquireTestResidency(t, s)
		if second.Epoch() <= first.Epoch() {
			t.Fatalf("vacuous: successor residency %d is not above %d, so this is not a successor at all", second.Epoch(), first.Epoch())
		}
		stolen, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, second, settlementExpiry))
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
	//
	// The record is filed through the codec at a residency ABOVE the grant's, so
	// the caller below is genuinely superseded and the fence has something to
	// answer with. A grant cannot express that — which is the point of the
	// capability — so fileDispositionClaim is the only way to reach it, exactly
	// as its own doc says.
	t.Run("an authorized attempt before the residency fence", func(t *testing.T) {
		s, _, admitted, grant := claimFixture(t)
		claimed := fileDispositionClaim(t, s, admitted, DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: settlementExpiry})
		applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
		if err != nil {
			t.Fatal(err)
		}
		if applying.Record.Claim == nil {
			t.Fatal("vacuous: an applying record has no claim for the fence to consult")
		}
		if grant.Epoch() >= applying.Record.Claim.ResidencyEpoch {
			t.Fatalf("vacuous: the grant's epoch %d is not below the record's mark %d, so the fence would not fire anyway", grant.Epoch(), applying.Record.Claim.ResidencyEpoch)
		}
		_, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(applying, grant, settlementExpiry))
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
		claimed := fileDispositionClaim(t, s, admitted, DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: settlementExpiry})
		if grant.Epoch() >= settlementResidenc {
			t.Fatalf("vacuous: the grant's epoch %d is not below the mark %d", grant.Epoch(), settlementResidenc)
		}
		clock.set(inboxDeadline)
		if clock.Now().Before(claimed.Record.Claim.ExpiresAt) || clock.Now().Before(claimed.Record.ApplyDeadline) {
			t.Fatal("vacuous: this case requires the claim to have lapsed AND the deadline to have passed")
		}
		_, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant, inboxDeadline.Add(time.Minute)))
		epoch := assertInboxCode(t, err, InboxErrorEpoch)
		if ok || epoch.Field != "residency_epoch" || epoch.Epoch != uint64(settlementResidenc) {
			t.Fatalf("epoch refusal = %+v (ok=%v), want field residency_epoch at %d", epoch, ok, settlementResidenc)
		}
	})

	// The apply deadline closes NEW claims permanently, for every caller, and it
	// answers ahead of the held-claim refusal, which merely expires.
	t.Run("the deadline before a held claim", func(t *testing.T) {
		s, clock, admitted, grant := claimFixture(t)
		claimed := mustClaimDisposition(t, s, admitted, grant, claimPastDeadline)
		clock.set(inboxDeadline)
		if !clock.Now().Before(claimed.Record.Claim.ExpiresAt) {
			t.Fatal("vacuous: the claim this case requires to still be live has lapsed")
		}
		_, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant, claimPastDeadline.Add(time.Minute)))
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
		claimed := mustClaimDisposition(t, s, admitted, grant, expires)
		clock.set(inboxDeadline)
		again, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant, expires))
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

			claim := claimRequest(admitted, grant, settlementExpiry)
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
		field   string
	}{
		{"unset expiry", time.Time{}, "claim_expires_at"},
		{"expiry in the past", settlementNow.Add(-time.Second), "claim_expires_at"},
		{"expiry at now", settlementNow, "claim_expires_at"},
		{"expiry beyond the TTL", settlementNow.Add(MaxCommandClaimTTL + time.Second), "claim_expires_at"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, admitted, grant := claimFixture(t)
			req := claimRequest(admitted, grant, tc.expires)
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
		if _, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(admitted, grant, settlementNow.Add(MaxCommandClaimTTL))); err != nil || !ok {
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
	claimed := mustClaimDisposition(t, s, admitted, grant, settlementExpiry)
	rejected, ok, err := s.RejectDispositionCommand(context.Background(), rejectRequest(claimed, grant.Epoch()))
	if err != nil || !ok {
		t.Fatalf("reject a claimed command: %v %v", ok, err)
	}
	if rejected.Record.State != InboxStateRejected || rejected.Record.Claim == nil {
		t.Fatalf("rejection dropped the claim: %+v", rejected.Record)
	}
	if !sameDispositionClaim(*rejected.Record.Claim, *claimed.Record.Claim) {
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
	claimed := mustClaimDisposition(t, s, admitted, grant, settlementExpiry)
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
		claimed := mustClaimDisposition(t, s, admitted, grant, settlementExpiry)
		clock.set(settlementExpiry)
		if _, ok, err := s.RejectDispositionCommand(context.Background(), rejectRequest(claimed, 0)); err != nil || !ok {
			t.Fatalf("reconciler rejection over a lapsed claim: %v %v", ok, err)
		}
	})
	t.Run("a live claim is not the reconciler's to settle", func(t *testing.T) {
		s, _, admitted, grant := claimFixture(t)
		claimed := mustClaimDisposition(t, s, admitted, grant, settlementExpiry)
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
	claimed := mustClaimDisposition(t, s, admitted, grant, settlementExpiry)
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

// A legacy session is refused by both edges, and they refuse it DIFFERENTLY now
// that the claim edge takes a grant. The reject edge takes a bare epoch, reaches
// the catalog and answers with a CatalogError about the binding, as every other
// disposition transition does. The claim edge cannot be reached for a legacy
// session at all: AcquireResidency refuses to issue a grant over one, so there
// is no value of the request's Residency member that names it.
//
// That is a real consequence of the capability and is asserted rather than
// assumed — a reader should not expect a CatalogError from the claim edge here.
func TestDispositionClaimEdgesRefuseALegacySession(t *testing.T) {
	s := openStore(t, memstore.New(), WithClock(newMovableClock(settlementNow)))
	if _, _, err := s.CreateCatalogEntry(context.Background(), testCreateRequest()); err != nil {
		t.Fatal(err)
	}
	entry := DispositionInboxEntry{Record: DispositionInboxRecord{Descriptor: DispositionCommandDescriptor{TenantID: catalogTenant, SessionID: catalogSession, CommandID: "public/command:1"}}, Revision: 1}

	_, ok, err := s.RejectDispositionCommand(context.Background(), rejectRequest(entry, 1))
	catalog := assertCatalogCode(t, err, CatalogErrorInvalid)
	if ok || catalog.Field != "binding.protocol_mode" {
		t.Fatalf("reject: field %q ok %v", catalog.Field, ok)
	}

	// No grant is obtainable for this session, so the claim edge has no reachable
	// request naming it.
	if _, err := s.AcquireResidency(context.Background(), AcquireResidencyRequest{TenantID: catalogTenant, SessionID: catalogSession}); err == nil {
		t.Fatal("AcquireResidency issued a grant over a LEGACY session")
	} else {
		catalog = assertCatalogCode(t, err, CatalogErrorInvalid)
		if catalog.Field != "binding.protocol_mode" {
			t.Fatalf("acquire: field %q", catalog.Field)
		}
	}
	// And a grant issued for ANOTHER session cannot be pointed at this one.
	other := openStore(t, memstore.New(), WithClock(newMovableClock(settlementNow)))
	createDispositionCatalog(t, other)
	foreign := acquireTestResidency(t, other)
	_, ok, err = s.ClaimDispositionCommand(context.Background(), claimRequest(entry, foreign, settlementExpiry))
	invalid := assertInboxCode(t, err, InboxErrorInvalid)
	if ok || invalid.Field != "residency" {
		t.Fatalf("claim with a foreign grant: field %q ok %v", invalid.Field, ok)
	}
}

// Both edges are durable: what they wrote is what a reopened store reads.
func TestDispositionClaimEdgesSurviveReopen(t *testing.T) {
	backend := memstore.New()
	clock := newMovableClock(settlementNow)
	first := openStore(t, backend, WithClock(clock))
	_, _, admitted, grant := withClaimFixture(t, first, clock)
	claimed, _, err := first.ClaimDispositionCommand(context.Background(), claimRequest(admitted, grant, settlementExpiry))
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
	if got.Record.Claim == nil || !sameDispositionClaim(*got.Record.Claim, *claimed.Record.Claim) {
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
		Residency: grant, ClaimExpiresAt: settlementExpiry,
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
	_, ok, err = s.ClaimDispositionCommand(context.Background(), claimRequest(rejected, grant, settlementExpiry))
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
// The MONOTONIC half of the same defect needs a clock this file's fixtures
// cannot provide and lives in its own test below; a row here claiming to cover
// it would be a no-op, because settlementExpiry is a constructed time.Date and
// Round(0) on such a value is the identity.
func TestClaimDispositionCommandReplaysAcrossTimeRepresentations(t *testing.T) {
	zone := time.FixedZone("Test/Plus0530", 5*3600+1800)
	for _, tc := range []struct {
		name  string
		first time.Time
		again time.Time
	}{
		{"a non-UTC zone", settlementExpiry.In(zone), settlementExpiry.In(zone)},
		{"UTC then the same instant elsewhere", settlementExpiry, settlementExpiry.In(zone)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, admitted, grant := claimFixture(t)
			claimed := mustClaimDisposition(t, s, admitted, grant, tc.first)
			if !claimed.Record.Claim.ExpiresAt.Equal(tc.again) {
				t.Fatalf("vacuous: the stored expiry %v is not the instant being replayed %v", claimed.Record.Claim.ExpiresAt, tc.again)
			}
			again, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant, tc.again))
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
		claimed := mustClaimDisposition(t, s, admitted, grant, settlementExpiry.In(zone))
		_, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant, settlementExpiry.In(zone).Add(time.Second)))
		assertInboxCode(t, err, InboxErrorClaimHeld)
		if ok {
			t.Fatal("CONTROL: a renewal one second later was accepted as a replay")
		}
	})
}

// F1. THE CLAIM EDGE IS THE ONLY PRODUCER OF THE RECORD'S HIGH-WATER MARK, and
// that mark only ever rises, so an epoch no provider issued would supersede every
// real Host for that command permanently. The capability is what makes such an
// epoch INEXPRESSIBLE rather than merely refused: the request carries a
// *ResidencyGrant, and the epoch is read off it.
//
// What can still be attempted is handing the edge a grant it should not honour,
// and each conjunct of residencyFor is driven here.
func TestClaimDispositionCommandRefusesAResidencyItDidNotIssue(t *testing.T) {
	t.Run("no grant at all", func(t *testing.T) {
		s, _, admitted, _ := claimFixture(t)
		_, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(admitted, nil, settlementExpiry))
		invalid := assertInboxCode(t, err, InboxErrorInvalid)
		if ok || invalid.Field != "residency" {
			t.Fatalf("field = %q, want residency (ok=%v)", invalid.Field, ok)
		}
		assertDispositionUnchanged(t, s, admitted)
	})

	t.Run("a released grant", func(t *testing.T) {
		s, _, admitted, grant := claimFixture(t)
		if err := grant.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
		_, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(admitted, grant, settlementExpiry))
		invalid := assertInboxCode(t, err, InboxErrorInvalid)
		if ok || invalid.Field != "residency" {
			t.Fatalf("field = %q, want residency (ok=%v)", invalid.Field, ok)
		}
		assertDispositionUnchanged(t, s, admitted)
	})

	t.Run("a grant for another session", func(t *testing.T) {
		backend := memstore.New()
		clock := newMovableClock(settlementNow)
		s := openStore(t, backend, WithClock(clock))
		createDispositionCatalog(t, s)
		admitted, _, err := s.AdmitDispositionCommand(context.Background(), dispositionRequest())
		if err != nil {
			t.Fatal(err)
		}
		// A second disposition session in the SAME store, whose residency lease is
		// a different namespace and whose epochs are therefore incomparable.
		req := testCreateRequest()
		req.SessionID = "session-b"
		req.Binding = testSessionBinding()
		if _, _, err := s.CreateCatalogEntry(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		foreign, err := s.AcquireResidency(context.Background(), AcquireResidencyRequest{TenantID: catalogTenant, SessionID: "session-b"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = foreign.Release(context.Background()) })
		_, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(admitted, foreign, settlementExpiry))
		invalid := assertInboxCode(t, err, InboxErrorInvalid)
		if ok || invalid.Field != "residency" {
			t.Fatalf("field = %q, want residency (ok=%v)", invalid.Field, ok)
		}
		assertDispositionUnchanged(t, s, admitted)
	})

	t.Run("a grant from another store", func(t *testing.T) {
		s, _, admitted, _ := claimFixture(t)
		other := openStore(t, memstore.New(), WithClock(newMovableClock(settlementNow)))
		createDispositionCatalog(t, other)
		foreign := acquireTestResidency(t, other)
		_, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(admitted, foreign, settlementExpiry))
		invalid := assertInboxCode(t, err, InboxErrorInvalid)
		if ok || invalid.Field != "residency" {
			t.Fatalf("field = %q, want residency (ok=%v)", invalid.Field, ok)
		}
		assertDispositionUnchanged(t, s, admitted)
	})

	// The control. The SAME store, session and command with the store's OWN live
	// grant is accepted, and the epoch it stores is the grant's — so the refusals
	// above are the grant check and not a blanket refusal, and the stored mark
	// demonstrably comes from the provider rather than from the caller.
	t.Run("the store's own grant is accepted and supplies the epoch", func(t *testing.T) {
		s, _, admitted, grant := claimFixture(t)
		claimed := mustClaimDisposition(t, s, admitted, grant, settlementExpiry)
		if claimed.Record.Claim.ResidencyEpoch != grant.Epoch() {
			t.Fatalf("stored mark = %d, want the grant's %d", claimed.Record.Claim.ResidencyEpoch, grant.Epoch())
		}
	})
}

// F1's structural half. The capability only bounds the mark while the claim edge
// stays the ONLY thing that writes a DispositionClaim, so that is checked against
// the sources rather than asserted in prose.
//
// It is a spelling lock, like TestEveryDispositionTerminalCallSiteIsDriven, and
// the trade is the same: a new writer fails here loudly instead of widening the
// mark's provenance silently. A writer that legitimately needs to exist adds
// itself to the allowed set, which is a one-line change and a decision someone
// has to make on purpose.
func TestOnlyTheClaimEdgeAndTheCodecConstructADispositionClaim(t *testing.T) {
	t.Parallel()
	// Each entry is a decision, not a label. Two of the three cannot carry a
	// caller's number at all, which is why they are harmless:
	allowed := map[string]string{
		"ClaimDispositionCommand":    "THE PRODUCER. Gated on a *ResidencyGrant, so its epoch is the provider's",
		"claim":                      "the wire decoder: rebuilds a claim from bytes already stored",
		"dispositionRecordHighWater": "a ZERO claim, used only as the absent-mark reader; carries no epoch",
	}
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	found, scanned := map[string]int{}, 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		scanned++
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		var fn string
		note := func(site string) {
			found[fn]++
			if _, ok := allowed[fn]; !ok {
				t.Errorf("%s constructs a DispositionClaim (%s) in %s. Only the claim edge may produce the record's high-water mark, and it is gated on a grant so the epoch cannot be a caller's number. Route this through ClaimDispositionCommand, or add it to the allowed set deliberately", fn, site, name)
			}
		}
		isClaim := func(e ast.Expr) bool {
			id, ok := e.(*ast.Ident)
			return ok && id.Name == "DispositionClaim"
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.FuncDecl:
				fn = node.Name.Name
			case *ast.CompositeLit:
				if isClaim(node.Type) {
					note("composite literal")
				}
			case *ast.ValueSpec:
				// `var claim DispositionClaim` is a construction too, and it is
				// the shape dispositionRecordHighWater uses; a scan that saw only
				// literals would report that site as absent and go vacuous.
				if node.Type != nil && isClaim(node.Type) {
					note("zero-value declaration")
				}
			}
			return true
		})
	}
	if scanned == 0 {
		t.Fatal("vacuous: no production files were scanned")
	}
	// Non-vacuity both ways: the producer must actually have been seen, and a
	// stale allowance is an error too, or the set grows without limit.
	if found["ClaimDispositionCommand"] == 0 {
		t.Error("vacuous: ClaimDispositionCommand no longer constructs a DispositionClaim, so this guard is watching nothing")
	}
	for fn, why := range allowed {
		if found[fn] == 0 {
			t.Errorf("%q is allowed to construct a DispositionClaim (%s) and no longer does; remove the allowance", fn, why)
		}
	}
}

// F2. THE MONOTONIC HALF, driven for real.
//
// `time.Time` carries three things struct equality compares: the wall clock, a
// *Location pointer, and a monotonic reading. The test above covers the zone;
// this covers the monotonic reading, and it needs its own fixture because every
// time in this package is a constructed `time.Date` literal — on which `Round(0)`
// is the IDENTITY, so a row built from one measures nothing at all.
//
// A value with a real monotonic reading can only come from time.Now, and
// validateBoundedExpiry measures the expiry against the STORE's clock — so the
// store's clock has to track real time for such an expiry to be inside
// MaxCommandClaimTTL at all. That is the whole awkwardness, and it is why this is
// a separate fixture rather than a table row.
//
// Both non-vacuity guards are load-bearing: without them this test would pass
// under struct equality exactly as the row it replaces did.
func TestClaimDispositionCommandReplaysAnExpiryCarryingAMonotonicReading(t *testing.T) {
	base := time.Now()
	s := openStore(t, memstore.New(), WithClock(newMovableClock(base)))
	createDispositionCatalog(t, s)
	req := dispositionRequest()
	req.AcceptedAt = base.Round(0).UTC()
	req.ApplyDeadline = base.Round(0).UTC().Add(2 * time.Hour)
	admitted, created, err := s.AdmitDispositionCommand(context.Background(), req)
	if err != nil || !created {
		t.Fatalf("admit: %v %v", created, err)
	}
	grant := acquireTestResidency(t, s)

	expires := time.Now().Add(30 * time.Minute)
	if expires.Round(0) == expires {
		t.Fatal("VACUOUS: the requested expiry carries no monotonic reading, so struct equality would match it and this test would prove nothing")
	}
	claimed := mustClaimDisposition(t, s, admitted, grant, expires)
	stored := claimed.Record.Claim.ExpiresAt
	if stored.Round(0) != stored {
		t.Fatal("VACUOUS: the STORED expiry kept a monotonic reading, so struct equality would match it too")
	}
	if !stored.Equal(expires) {
		t.Fatalf("the codec did not preserve the instant: stored %v (%d ns), requested %v (%d ns)", stored, stored.Nanosecond(), expires, expires.Nanosecond())
	}
	again, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant, expires))
	if err != nil || ok || again.Revision != claimed.Revision {
		t.Fatalf("a replay whose expiry carries a monotonic reading was not idempotent: %+v %v %v", again, ok, err)
	}
}

// F4. THE LESSON GENERALISED, and the reason this test exists.
//
// The UTC blind spot was not "we forgot a zone". It was: EVERY FIXTURE SHARED A
// VALUE, so a property that only differs when that value differs cannot be
// measured — by a test or by a mutant. Mutation testing cannot find such a gap,
// because the mutant and the original agree on every input the suite can build.
//
// Asking "what else do all the fixtures share?" answers it. They share a zero
// SUB-SECOND component: settlementNow, settlementExpiry, inboxAcceptedAt and
// inboxDeadline are all time.Date(..., 0, 0, time.UTC) and every derived value
// adds whole minutes. So nothing pinned the replay comparison finer than one
// second, and truncating it to a second survived the whole suite — while every
// real caller using time.Now().Add(ttl) lands on a sub-second boundary.
func TestClaimDispositionCommandDistinguishesClaimsInsideOneSecond(t *testing.T) {
	s, _, admitted, grant := claimFixture(t)
	first := settlementExpiry.Add(250 * time.Millisecond)
	near := settlementExpiry.Add(750 * time.Millisecond)
	if first.Truncate(time.Second) != near.Truncate(time.Second) {
		t.Fatal("VACUOUS: the two instants are not inside the same second, so a second-resolution comparison would already tell them apart")
	}
	if first.Equal(near) {
		t.Fatal("VACUOUS: the two instants are equal")
	}
	claimed := mustClaimDisposition(t, s, admitted, grant, first)
	_, ok, err := s.ClaimDispositionCommand(context.Background(), claimRequest(claimed, grant, near))
	held := assertInboxCode(t, err, InboxErrorClaimHeld)
	if ok || held.Field != "claim" {
		t.Fatalf("a renewal 500ms later was accepted as a replay: field %q ok %v", held.Field, ok)
	}
	if !getDisposition(t, s, claimed).Record.Claim.ExpiresAt.Equal(first) {
		t.Fatal("the sub-second renewal moved the stored expiry")
	}
}

// F3. A LIVE CLAIM ADMITS ONLY ITS OWN HOLDER, at EVERY residency — and the
// successor is the one that could distinguish the rule from a weaker one. The
// two residencies the other tests drive (zero and the holder's own) are both
// refused by `Claim.ResidencyEpoch > req.ResidencyEpoch` as well, so without this
// case that weakening is invisible.
//
// It also pins THE ASYMMETRY BETWEEN THE TWO EDGES, which is real and worth
// stating: a successor may CLAIM a live claim away from its predecessor
// (failover), but may not REJECT the command under it. Rejecting is a terminal
// decision about work someone may be in the middle of, so it is the claim
// holder's alone; taking the claim first is the successor's route, and it makes
// the successor the holder before it decides anything terminal.
func TestRejectDispositionCommandRefusesASuccessorOverALiveClaim(t *testing.T) {
	s, _, admitted, first := claimFixture(t)
	claimed := mustClaimDisposition(t, s, admitted, first, settlementExpiry)
	if err := first.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := acquireTestResidency(t, s)
	if second.Epoch() <= first.Epoch() {
		t.Fatalf("vacuous: %d is not a successor of %d", second.Epoch(), first.Epoch())
	}
	if !dispositionClaimLive(getDisposition(t, s, claimed).Record, settlementNow) {
		t.Fatal("vacuous: the claim this case requires to be live has lapsed")
	}
	_, ok, err := s.RejectDispositionCommand(context.Background(), rejectRequest(claimed, second.Epoch()))
	held := assertInboxCode(t, err, InboxErrorClaimHeld)
	if ok || held.Field != "claim" {
		t.Fatalf("a successor rejected a live claim it does not hold: field %q ok %v", held.Field, ok)
	}
	assertDispositionUnchanged(t, s, claimed)

	// The other half of the asymmetry, in the same fixture so the two cannot
	// drift apart: the SAME successor may take the claim, and having taken it may
	// then reject.
	stolen := mustClaimDisposition(t, s, claimed, second, settlementExpiry)
	if stolen.Record.Claim.ResidencyEpoch != second.Epoch() {
		t.Fatalf("claim residency = %d, want the successor's %d", stolen.Record.Claim.ResidencyEpoch, second.Epoch())
	}
	if _, ok, err := s.RejectDispositionCommand(context.Background(), rejectRequest(stolen, second.Epoch())); err != nil || !ok {
		t.Fatalf("the successor could not reject the claim it had taken: %v %v", ok, err)
	}
}
