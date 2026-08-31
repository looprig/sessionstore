package sessionstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// --- fixtures and assertions ----------------------------------------------

// The one timeline every case in this file places itself on. A command is
// accepted at 11:30 with an apply deadline of 12:30 (inbox_test.go's fixtures);
// claims are taken at 11:40 and expire at 12:00, which leaves a reclaim window
// before the deadline and a stretch after the deadline in which no new claim
// may start.
var (
	inboxClaimStart  = time.Date(2026, 8, 30, 11, 40, 0, 0, time.UTC)
	inboxClaimExpiry = time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	inboxClaimLapsed = time.Date(2026, 8, 30, 12, 5, 0, 0, time.UTC)
	inboxAfterDue    = time.Date(2026, 8, 30, 12, 40, 0, 0, time.UTC)
)

const (
	// The lease epoch a fixture claims under, one that has provably lost the
	// session, and one that supersedes it.
	inboxEpoch      = uint64(5)
	inboxStaleEpoch = uint64(3)
	inboxNextEpoch  = uint64(7)
)

// movableClock is a Clock a test advances. fixedClock cannot serve here: every
// case has to build its fixture through the real operations at one instant and
// then evaluate a guard at another.
type movableClock struct {
	mu  sync.Mutex
	now time.Time
}

func newMovableClock(now time.Time) *movableClock { return &movableClock{now: now} }

func (c *movableClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *movableClock) set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

// inboxFixture opens a store whose clock starts at inboxClaimStart with one
// admitted command in it.
func inboxFixture(t *testing.T, backend *storage.Composite) (*Store, *movableClock, InboxEntry) {
	t.Helper()
	clock := newMovableClock(inboxClaimStart)
	store := openStore(t, backend, WithClock(clock))
	return store, clock, mustAdmit(t, store, testAdmitRequest())
}

func testClaimRequest(entry InboxEntry, epoch uint64) ClaimCommandRequest {
	return ClaimCommandRequest{
		TenantID:         entry.Record.TenantID,
		SessionID:        entry.Record.SessionID,
		CommandID:        entry.Record.CommandID,
		ExpectedRevision: entry.Revision,
		LeaseEpoch:       epoch,
		ClaimExpiresAt:   inboxClaimExpiry,
	}
}

func testBeginApplyingRequest(entry InboxEntry, epoch uint64) BeginApplyingCommandRequest {
	return BeginApplyingCommandRequest{
		TenantID:         entry.Record.TenantID,
		SessionID:        entry.Record.SessionID,
		CommandID:        entry.Record.CommandID,
		ExpectedRevision: entry.Revision,
		LeaseEpoch:       epoch,
		ClaimExpiresAt:   inboxClaimExpiry,
	}
}

func testCompleteRequest(entry InboxEntry, epoch uint64) CompleteCommandRequest {
	return CompleteCommandRequest{
		TenantID:         entry.Record.TenantID,
		SessionID:        entry.Record.SessionID,
		CommandID:        entry.Record.CommandID,
		ExpectedRevision: entry.Revision,
		LeaseEpoch:       epoch,
		Result: CommandResult{
			CompletedAt: inboxClaimStart,
			EventID:     "event-42",
			JournalSeq:  42,
		},
	}
}

func testRejectRequest(entry InboxEntry, epoch uint64) RejectCommandRequest {
	return RejectCommandRequest{
		TenantID:         entry.Record.TenantID,
		SessionID:        entry.Record.SessionID,
		CommandID:        entry.Record.CommandID,
		ExpectedRevision: entry.Revision,
		LeaseEpoch:       epoch,
		Rejection: sessionwire.ErrorDetail{
			Code:      sessionwire.ErrorCodeRuntimeUnavailable,
			Message:   "no compatible runtime",
			Retryable: true,
		},
	}
}

func mustClaim(t *testing.T, store *Store, entry InboxEntry, epoch uint64) InboxEntry {
	t.Helper()
	claimed, err := store.ClaimCommand(context.Background(), testClaimRequest(entry, epoch))
	if err != nil {
		t.Fatalf("ClaimCommand: %v", err)
	}
	return claimed
}

func mustBeginApplying(t *testing.T, store *Store, entry InboxEntry, epoch uint64) InboxEntry {
	t.Helper()
	applying, err := store.BeginApplyingCommand(context.Background(), testBeginApplyingRequest(entry, epoch))
	if err != nil {
		t.Fatalf("BeginApplyingCommand: %v", err)
	}
	return applying
}

// inboxStates builds one admitted command into each durable state, through the
// real operations rather than by writing bytes: a fixture assembled by hand
// would be evidence that the machine accepts a state, not that it produces one.
var inboxStates = map[string]func(t *testing.T, store *Store, entry InboxEntry) InboxEntry{
	"pending": func(_ *testing.T, _ *Store, entry InboxEntry) InboxEntry {
		return entry
	},
	"claimed": func(t *testing.T, store *Store, entry InboxEntry) InboxEntry {
		return mustClaim(t, store, entry, inboxEpoch)
	},
	"applying": func(t *testing.T, store *Store, entry InboxEntry) InboxEntry {
		return mustBeginApplying(t, store, mustClaim(t, store, entry, inboxEpoch), inboxEpoch)
	},
	"applied": func(t *testing.T, store *Store, entry InboxEntry) InboxEntry {
		applying := mustBeginApplying(t, store, mustClaim(t, store, entry, inboxEpoch), inboxEpoch)
		applied, err := store.CompleteCommand(context.Background(), testCompleteRequest(applying, inboxEpoch))
		if err != nil {
			t.Fatalf("CompleteCommand: %v", err)
		}
		return applied
	},
	"rejected": func(t *testing.T, store *Store, entry InboxEntry) InboxEntry {
		rejected, err := store.RejectCommand(context.Background(), testRejectRequest(entry, inboxEpoch))
		if err != nil {
			t.Fatalf("RejectCommand: %v", err)
		}
		return rejected
	},
}

// inboxOperations is every transition the machine offers, each one addressed at
// the entry the fixture left behind so its expected revision is current.
var inboxOperations = map[string]func(store *Store, entry InboxEntry, epoch uint64) (InboxEntry, error){
	"claim": func(store *Store, entry InboxEntry, epoch uint64) (InboxEntry, error) {
		return store.ClaimCommand(context.Background(), testClaimRequest(entry, epoch))
	},
	"begin_applying": func(store *Store, entry InboxEntry, epoch uint64) (InboxEntry, error) {
		return store.BeginApplyingCommand(context.Background(), testBeginApplyingRequest(entry, epoch))
	},
	"complete": func(store *Store, entry InboxEntry, epoch uint64) (InboxEntry, error) {
		return store.CompleteCommand(context.Background(), testCompleteRequest(entry, epoch))
	},
	"reject": func(store *Store, entry InboxEntry, epoch uint64) (InboxEntry, error) {
		return store.RejectCommand(context.Background(), testRejectRequest(entry, epoch))
	},
}

// assertInboxUnchanged fails unless the command's stored row is byte-for-byte
// the one want names, at the same revision. It reads the provider directly for
// the reason assertNoInboxRecord does: the question is what was WRITTEN, and
// asking the store API would route the answer through the same decode path the
// operation under test just used.
func assertInboxUnchanged(t *testing.T, store *Store, want InboxEntry) {
	t.Helper()
	scope, err := store.deriveSessionScope(want.Record.TenantID, want.Record.SessionID)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	stored, err := store.backend.OrderedIndex.Get(context.Background(), inboxID(scope, want.Record.CommandID))
	if err != nil {
		t.Fatalf("a refused transition made the record unreadable: %v", err)
	}
	if stored.Revision != want.Revision {
		t.Fatalf("a refused transition advanced the revision %d -> %d", want.Revision, stored.Revision)
	}
	wantBytes, _, err := encodeInboxRecord(want.Record)
	if err != nil {
		t.Fatalf("encode expected record: %v", err)
	}
	if !bytes.Equal(stored.Value, wantBytes) {
		t.Fatalf("a refused transition changed the record:\nwant %s\ngot  %s", wantBytes, stored.Value)
	}
}

// --- the state machine ----------------------------------------------------

// transitionExpectation is what one edge of the machine does. An empty want
// means the edge is allowed and wantState is what it produces.
type transitionExpectation struct {
	want      InboxErrorCode
	wantState InboxState
}

// commandStateMachine is the COMPLETE edge table: every durable state against
// every transition, under a lease epoch that is the record's own so that each
// cell carries exactly ONE fault. A cell that was both in the wrong state and
// under the wrong epoch would pass whichever guard fired first and would keep
// passing if the other were deleted; the epoch rules are driven separately.
//
// It is a table of the CROSS PRODUCT and the test proves that, rather than the
// table happening to contain one entry per pair today. A hand-written list of
// twenty cells looks identical to this until a sixth state or a fifth operation
// arrives, at which point up to nine edges go untested in silence — the same
// failure declaredStoreOperations exists to prevent one layer up in this file,
// applied to the edges instead of to the operations.
var commandStateMachine = map[string]map[string]transitionExpectation{
	"pending": {
		"claim":          {wantState: InboxStateClaimed},
		"begin_applying": {want: InboxErrorState},
		"complete":       {want: InboxErrorState},
		"reject":         {wantState: InboxStateRejected},
	},
	"claimed": {
		// A claim under the epoch that already holds a LIVE claim is held off;
		// that this cell is not a state failure is the whole of the equal-epoch
		// rule, and the cross product is what forces it to be stated.
		"claim":          {want: InboxErrorClaimHeld},
		"begin_applying": {wantState: InboxStateApplying},
		"complete":       {want: InboxErrorState},
		"reject":         {wantState: InboxStateRejected},
	},
	"applying": {
		"claim":          {want: InboxErrorState},
		"begin_applying": {want: InboxErrorState},
		"complete":       {wantState: InboxStateApplied},
		"reject":         {wantState: InboxStateRejected},
	},
	// Both terminal states refuse every transition, which is what makes them
	// mutually exclusive rather than merely written at different times.
	"applied": {
		"claim":          {want: InboxErrorTerminal},
		"begin_applying": {want: InboxErrorTerminal},
		"complete":       {want: InboxErrorTerminal},
		"reject":         {want: InboxErrorTerminal},
	},
	"rejected": {
		"claim":          {want: InboxErrorTerminal},
		"begin_applying": {want: InboxErrorTerminal},
		"complete":       {want: InboxErrorTerminal},
		"reject":         {want: InboxErrorTerminal},
	},
}

// TestCommandStateMachineAdmitsExactlyItsTransitions drives every operation
// against every durable state and holds each refusal to leaving the record
// untouched.
//
// The edges come from iterating the state and operation registries rather than
// from the table, so an unlisted pair fails rather than being skipped, and a
// listed pair that no longer exists fails too.
func TestCommandStateMachineAdmitsExactlyItsTransitions(t *testing.T) {
	t.Parallel()

	for state := range inboxStates {
		for operation := range inboxOperations {
			expect, listed := commandStateMachine[state][operation]
			if !listed {
				t.Errorf("the machine has a %s x %s edge and the table does not say what it does", state, operation)
				continue
			}
			t.Run(state+"/"+operation, func(t *testing.T) {
				t.Parallel()

				store, _, admitted := inboxFixture(t, memstore.New())
				entry := inboxStates[state](t, store, admitted)

				got, err := inboxOperations[operation](store, entry, inboxEpoch)
				if expect.want != "" {
					assertInboxCode(t, err, expect.want)
					assertInboxUnchanged(t, store, entry)
					return
				}
				if err != nil {
					t.Fatalf("%s from %s: %v", operation, state, err)
				}
				if got.Record.State != expect.wantState {
					t.Fatalf("state = %q, want %q", got.Record.State, expect.wantState)
				}
				if got.Revision == entry.Revision {
					t.Fatalf("an accepted transition did not advance the revision %d", entry.Revision)
				}
				if got.AcceptedOrder != entry.AcceptedOrder {
					t.Fatalf("acceptance order moved %d -> %d", entry.AcceptedOrder, got.AcceptedOrder)
				}
			})
		}
	}

	for state, operations := range commandStateMachine {
		if inboxStates[state] == nil {
			t.Errorf("the table names the state %s, which the machine no longer builds", state)
		}
		for operation := range operations {
			if inboxOperations[operation] == nil {
				t.Errorf("the table names the operation %s, which the machine no longer offers", operation)
			}
		}
	}
}

// TestReconcilerRejectsWithoutALease is the one edge outside the cross product:
// rejection is the only transition a caller may make while naming no lease
// epoch at all, so it is driven here rather than given a second epoch column in
// a table whose whole point is one fault per cell.
func TestReconcilerRejectsWithoutALease(t *testing.T) {
	t.Parallel()

	store, _, admitted := inboxFixture(t, memstore.New())
	rejected, err := store.RejectCommand(context.Background(), testRejectRequest(admitted, 0))
	if err != nil {
		t.Fatalf("a reconciler with no lease could not settle a pending command: %v", err)
	}
	if rejected.Record.State != InboxStateRejected {
		t.Fatalf("state = %q, want %q", rejected.Record.State, InboxStateRejected)
	}
}

// --- epochs ----------------------------------------------------------------

// TestClaimEpochRefusesASupersededLease holds every transition to the record's
// claim high-water mark and to reporting it, so a superseded writer learns the
// epoch it has to beat rather than merely that it failed.
func TestClaimEpochRefusesASupersededLease(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		state     string
		operation string
	}{
		{name: "claim", state: "claimed", operation: "claim"},
		{name: "begin applying", state: "claimed", operation: "begin_applying"},
		{name: "complete", state: "applying", operation: "complete"},
		{name: "reject", state: "claimed", operation: "reject"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store, _, admitted := inboxFixture(t, memstore.New())
			entry := inboxStates[test.state](t, store, admitted)

			_, err := inboxOperations[test.operation](store, entry, inboxStaleEpoch)
			failure := assertInboxCode(t, err, InboxErrorEpoch)
			if failure.Epoch != inboxEpoch {
				t.Fatalf("reported epoch = %d, want the high-water %d", failure.Epoch, inboxEpoch)
			}
			if failure.Field != "lease_epoch" {
				t.Fatalf("field = %q, want %q", failure.Field, "lease_epoch")
			}
			assertInboxUnchanged(t, store, entry)
		})
	}
}

// TestALiveClaimIsHeldAgainstItsOwnEpochAndTakenByAGreaterOne pins the one
// place this fence differs from the catalog's, and the reason: a claim cannot
// tell two writers under one lease epoch apart, while a strictly greater epoch
// is provably the successor of the writer holding the claim.
func TestALiveClaimIsHeldAgainstItsOwnEpochAndTakenByAGreaterOne(t *testing.T) {
	t.Parallel()

	t.Run("equal epoch is held off", func(t *testing.T) {
		t.Parallel()

		store, _, admitted := inboxFixture(t, memstore.New())
		claimed := inboxStates["claimed"](t, store, admitted)

		_, err := store.ClaimCommand(context.Background(), testClaimRequest(claimed, inboxEpoch))
		if got := assertInboxCode(t, err, InboxErrorClaimHeld); got.Field != "claim" {
			t.Fatalf("field = %q, want %q", got.Field, "claim")
		}
		assertInboxUnchanged(t, store, claimed)
	})

	t.Run("a greater epoch supersedes", func(t *testing.T) {
		t.Parallel()

		store, _, admitted := inboxFixture(t, memstore.New())
		claimed := inboxStates["claimed"](t, store, admitted)

		taken, err := store.ClaimCommand(context.Background(), testClaimRequest(claimed, inboxNextEpoch))
		if err != nil {
			t.Fatalf("a superseding lease could not take a live claim: %v", err)
		}
		if taken.Record.Claim.LeaseEpoch != inboxNextEpoch {
			t.Fatalf("claim epoch = %d, want %d", taken.Record.Claim.LeaseEpoch, inboxNextEpoch)
		}
		// The high-water mark moved with it, so the superseded holder is now
		// refused as the stale writer it is rather than merely losing a race.
		_, err = store.BeginApplyingCommand(context.Background(), testBeginApplyingRequest(taken, inboxEpoch))
		if got := assertInboxCode(t, err, InboxErrorEpoch); got.Epoch != inboxNextEpoch {
			t.Fatalf("reported epoch = %d, want %d", got.Epoch, inboxNextEpoch)
		}
	})
}

// --- claim expiry and the apply deadline ----------------------------------

// TestAnExpiredClaimIsReclaimable proves the reclaim window is real: the same
// lease resuming and a later lease recovering may both take a claim that has
// lapsed, so a crashed writer costs the command a claim TTL rather than its
// whole deadline.
func TestAnExpiredClaimIsReclaimable(t *testing.T) {
	t.Parallel()

	for _, epoch := range []uint64{inboxEpoch, inboxNextEpoch} {
		t.Run(time.Duration(epoch).String(), func(t *testing.T) {
			t.Parallel()

			store, clock, admitted := inboxFixture(t, memstore.New())
			claimed := inboxStates["claimed"](t, store, admitted)
			clock.set(inboxClaimLapsed)

			reclaimed, err := store.ClaimCommand(context.Background(), ClaimCommandRequest{
				TenantID:         claimed.Record.TenantID,
				SessionID:        claimed.Record.SessionID,
				CommandID:        claimed.Record.CommandID,
				ExpectedRevision: claimed.Revision,
				LeaseEpoch:       epoch,
				ClaimExpiresAt:   inboxDeadline,
			})
			if err != nil {
				t.Fatalf("reclaim of an expired claim: %v", err)
			}
			if reclaimed.Record.State != InboxStateClaimed || reclaimed.Record.Claim.LeaseEpoch != epoch {
				t.Fatalf("reclaimed record = %+v", reclaimed.Record)
			}
			if !reclaimed.Record.Claim.ExpiresAt.Equal(inboxDeadline) {
				t.Fatalf("claim expiry = %s, want %s", reclaimed.Record.Claim.ExpiresAt, inboxDeadline)
			}
		})
	}
}

// TestAnExpiredClaimCannotApply proves the other half: a lapsed claim is not a
// claim, so its holder must take a new one before it may begin applying.
func TestAnExpiredClaimCannotApply(t *testing.T) {
	t.Parallel()

	store, clock, admitted := inboxFixture(t, memstore.New())
	claimed := inboxStates["claimed"](t, store, admitted)
	clock.set(inboxClaimLapsed)

	// The claim the request would WRITE is live; the only lapsed claim in the
	// case is the one on the record, so nothing but that can refuse it.
	request := testBeginApplyingRequest(claimed, inboxEpoch)
	request.ClaimExpiresAt = inboxDeadline
	_, err := store.BeginApplyingCommand(context.Background(), request)
	if got := assertInboxCode(t, err, InboxErrorClaimLost); got.Field != "claim" {
		t.Fatalf("field = %q, want %q", got.Field, "claim")
	}
	assertInboxUnchanged(t, store, claimed)
}

// TestAnUnclaimedEpochCannotApply separates the other route to a lost claim: a
// writer at a legitimate epoch that simply never claimed this command.
func TestAnUnclaimedEpochCannotApply(t *testing.T) {
	t.Parallel()

	store, _, admitted := inboxFixture(t, memstore.New())
	claimed := inboxStates["claimed"](t, store, admitted)

	_, err := store.BeginApplyingCommand(context.Background(), testBeginApplyingRequest(claimed, inboxNextEpoch))
	if got := assertInboxCode(t, err, InboxErrorClaimLost); got.Field != "lease_epoch" {
		t.Fatalf("field = %q, want %q", got.Field, "lease_epoch")
	}
	assertInboxUnchanged(t, store, claimed)
}

// TestNoNewClaimStartsAtOrAfterTheApplyDeadline pins the half-open bound at the
// instant itself, which is the only place an off-by-one is observable.
func TestNoNewClaimStartsAtOrAfterTheApplyDeadline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		at   time.Time
		want InboxErrorCode
	}{
		{name: "before the deadline", at: inboxClaimStart},
		{name: "at the deadline", at: inboxDeadline, want: InboxErrorDeadline},
		{name: "after the deadline", at: inboxAfterDue, want: InboxErrorDeadline},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store, clock, admitted := inboxFixture(t, memstore.New())
			clock.set(test.at)

			// The claim is taken relative to the instant the case runs at, so
			// it is well formed and within MaxCommandClaimTTL wherever the
			// clock is, and at and after the deadline it outlives the deadline.
			// Only the deadline rule can refuse any of them.
			claim := testClaimRequest(admitted, inboxEpoch)
			claim.ClaimExpiresAt = test.at.Add(30 * time.Minute)
			_, err := store.ClaimCommand(context.Background(), claim)
			if test.want == "" {
				if err != nil {
					t.Fatalf("ClaimCommand: %v", err)
				}
				return
			}
			if got := assertInboxCode(t, err, test.want); got.Field != "apply_deadline" {
				t.Fatalf("field = %q, want %q", got.Field, "apply_deadline")
			}
			assertInboxUnchanged(t, store, admitted)
		})
	}
}

// TestAnUnexpiredClaimWinsTheDeadlineRace drives the race the spec settles: a
// claim that outlives the apply deadline keeps the reconciler out until it
// lapses, and an applying record keeps it out whether the claim has lapsed or
// not.
func TestAnUnexpiredClaimWinsTheDeadlineRace(t *testing.T) {
	t.Parallel()

	// A claim taken before the deadline that expires after it, within
	// MaxCommandClaimTTL of when it is taken. Past the deadline the reconciler
	// is running and the claim is still live.
	longClaim := inboxDeadline.Add(5 * time.Minute)
	pastDeadline := inboxDeadline.Add(time.Minute)
	afterLongClaim := longClaim.Add(time.Minute)

	claimLongInto := func(t *testing.T, store *Store, entry InboxEntry) InboxEntry {
		t.Helper()
		request := testClaimRequest(entry, inboxEpoch)
		request.ClaimExpiresAt = longClaim
		claimed, err := store.ClaimCommand(context.Background(), request)
		if err != nil {
			t.Fatalf("ClaimCommand: %v", err)
		}
		return claimed
	}

	t.Run("a live claim keeps the reconciler out", func(t *testing.T) {
		t.Parallel()

		store, clock, admitted := inboxFixture(t, memstore.New())
		claimed := claimLongInto(t, store, admitted)
		clock.set(pastDeadline)

		_, err := store.RejectCommand(context.Background(), testRejectRequest(claimed, 0))
		assertInboxCode(t, err, InboxErrorClaimHeld)
		assertInboxUnchanged(t, store, claimed)

		// Its holder may still settle it, and so may nobody else — including a
		// lease that would otherwise supersede the claim, because rejecting is
		// not claiming.
		_, err = store.RejectCommand(context.Background(), testRejectRequest(claimed, inboxNextEpoch))
		assertInboxCode(t, err, InboxErrorClaimHeld)

		rejected, err := store.RejectCommand(context.Background(), testRejectRequest(claimed, inboxEpoch))
		if err != nil {
			t.Fatalf("the claim holder could not reject its own command: %v", err)
		}
		if rejected.Record.State != InboxStateRejected {
			t.Fatalf("state = %q", rejected.Record.State)
		}
	})

	t.Run("a lapsed claim lets the reconciler in", func(t *testing.T) {
		t.Parallel()

		store, clock, admitted := inboxFixture(t, memstore.New())
		claimed := claimLongInto(t, store, admitted)
		clock.set(afterLongClaim)

		rejected, err := store.RejectCommand(context.Background(), testRejectRequest(claimed, 0))
		if err != nil {
			t.Fatalf("the reconciler could not settle an abandoned command: %v", err)
		}
		if rejected.Record.State != InboxStateRejected || rejected.Record.Rejection == nil {
			t.Fatalf("rejected record = %+v", rejected.Record)
		}
		if !rejected.Record.Result.isZero() {
			t.Fatal("a rejection recorded an application result")
		}
	})

	t.Run("applying keeps the reconciler out however the clock runs", func(t *testing.T) {
		t.Parallel()

		// Live and lapsed report different codes and mean different things —
		// "someone is on it" and "this one needs the evidence a later task
		// reads" — but neither is a rejection.
		for _, test := range []struct {
			at   time.Time
			want InboxErrorCode
		}{
			{at: pastDeadline, want: InboxErrorClaimHeld},
			{at: afterLongClaim, want: InboxErrorClaimLost},
		} {
			store, clock, admitted := inboxFixture(t, memstore.New())
			claimed := claimLongInto(t, store, admitted)
			request := testBeginApplyingRequest(claimed, inboxEpoch)
			request.ClaimExpiresAt = longClaim
			applying, err := store.BeginApplyingCommand(context.Background(), request)
			if err != nil {
				t.Fatalf("BeginApplyingCommand: %v", err)
			}
			clock.set(test.at)

			_, err = store.RejectCommand(context.Background(), testRejectRequest(applying, 0))
			assertInboxCode(t, err, test.want)
			assertInboxUnchanged(t, store, applying)

			// Nor may a successor lease take it, which is what closes the
			// two-step of superseding the claim and rejecting as its holder.
			successor := testClaimRequest(applying, inboxNextEpoch)
			successor.ClaimExpiresAt = test.at.Add(30 * time.Minute)
			_, err = store.ClaimCommand(context.Background(), successor)
			assertInboxCode(t, err, InboxErrorState)
		}
	})
}

// --- what a transition files ----------------------------------------------

// storedInboxRecord returns the provider's row for a command, unmediated by the
// package's own reader.
func storedInboxRecord(t *testing.T, store *Store, entry InboxEntry) storage.OrderedRecord {
	t.Helper()
	scope, err := store.deriveSessionScope(entry.Record.TenantID, entry.Record.SessionID)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	stored, err := store.backend.OrderedIndex.Get(context.Background(), inboxID(scope, entry.Record.CommandID))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	return stored
}

// TestTransitionsFileTheDueStateTheRecordDerives is the carry-forward contract
// inboxDue states, driven rather than asserted about.
//
// The failure it exists to catch is not a wrong due time. It is a due state
// filed as a function of the OPERATION — the natural shape for a reclaim, which
// wants the command due at the claim it just wrote — because inboxEntryFor
// compares every read against the derivation, so such a record answers every
// concurrent RETRY of the command with an identity failure no caller can fix.
// The retry is therefore part of the test rather than a separate one.
func TestTransitionsFileTheDueStateTheRecordDerives(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expires time.Time
		want    time.Time
	}{
		{name: "a claim lapsing first is the horizon", expires: inboxClaimExpiry, want: inboxClaimExpiry},
		{name: "the deadline bounds a longer claim", expires: inboxAfterDue, want: inboxDeadline},
		{name: "a claim to the deadline is the deadline", expires: inboxDeadline, want: inboxDeadline},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store, _, admitted := inboxFixture(t, memstore.New())
			if got := storedInboxRecord(t, store, admitted).Due; got.UnixMillis != inboxDeadline.UnixMilli() {
				t.Fatalf("an unclaimed command is due at %d, want its deadline %d", got.UnixMillis, inboxDeadline.UnixMilli())
			}

			request := testClaimRequest(admitted, inboxEpoch)
			request.ClaimExpiresAt = test.expires
			claimed, err := store.ClaimCommand(context.Background(), request)
			if err != nil {
				t.Fatalf("ClaimCommand: %v", err)
			}
			stored := storedInboxRecord(t, store, claimed)
			if stored.Due.State != storage.DueAt || stored.Due.UnixMillis != test.want.UnixMilli() {
				t.Fatalf("due = %+v, want %d", stored.Due, test.want.UnixMilli())
			}
			if stored.Due.UnixMillis > claimed.Record.ApplyDeadline.UnixMilli() {
				t.Fatal("a claimed command became due only after its own apply deadline")
			}
			if stored.Rank != (storage.Rank{}) {
				t.Fatalf("a transition ranked the command: %+v", stored.Rank)
			}

			// The retry the misfiling would blind. It reaches the same nine
			// filing checks a reader does, so a due state derived from the
			// operation fails here and nowhere else.
			retry, created, err := store.AdmitCommand(context.Background(), testAdmitRequest())
			if err != nil {
				t.Fatalf("a retry of a claimed command: %v", err)
			}
			if created || retry.Record.State != InboxStateClaimed {
				t.Fatalf("retry returned created=%v state=%q", created, retry.Record.State)
			}
		})
	}
}

// TestTerminalTransitionsLeaveTheCommandNotDueAndDirectlyGettable pins the two
// halves of a terminal CAS: it leaves the due view in the same write that
// settles the outcome, and it stays readable by its stable key forever, which
// is what lets a caller learn an outcome it did not commit.
func TestTerminalTransitionsLeaveTheCommandNotDueAndDirectlyGettable(t *testing.T) {
	t.Parallel()

	// The state each terminal transition is made FROM, so the recorder can be
	// reset immediately before the settling write and see only that write.
	tests := []struct {
		state  string
		from   string
		settle func(store *Store, entry InboxEntry) (InboxEntry, error)
	}{
		{state: "applied", from: "applying", settle: func(store *Store, entry InboxEntry) (InboxEntry, error) {
			return store.CompleteCommand(context.Background(), testCompleteRequest(entry, inboxEpoch))
		}},
		{state: "rejected", from: "pending", settle: func(store *Store, entry InboxEntry) (InboxEntry, error) {
			return store.RejectCommand(context.Background(), testRejectRequest(entry, inboxEpoch))
		}},
	}

	for _, test := range tests {
		t.Run(test.state, func(t *testing.T) {
			t.Parallel()

			base := memstore.New()
			ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
			base.OrderedIndex = ordered
			store, _, admitted := inboxFixture(t, base)
			before := inboxStates[test.from](t, store, admitted)

			ordered.reset()
			terminal, err := test.settle(store, before)
			if err != nil {
				t.Fatalf("settle from %s: %v", test.from, err)
			}

			// ATOMICALLY, asserted where it is claimed. The settled outcome and
			// the departure from the due view are one provider write, so there
			// is no instant at which a reader can see a command that is
			// terminal and still due, or due and already settled. A revision
			// comparison alone would not say this: a path that wrote the
			// outcome and then un-filed the due state in a second
			// compare-and-swap would still return a record whose revision
			// matches what is stored, having passed through exactly the state
			// this rules out.
			var ops []string
			for _, call := range ordered.snapshot() {
				ops = append(ops, call.op)
			}
			if len(ops) != 2 || ops[0] != "get" || ops[1] != "update" {
				t.Fatalf("settling issued %v, want one get then one update", ops)
			}
			update, _ := ordered.lastOf("update")
			if update.due != (storage.Due{}) {
				t.Fatalf("the settling write filed due %+v, want not due", update.due)
			}
			if update.expectedRevision != before.Revision {
				t.Fatalf("the settling write named revision %d, want %d", update.expectedRevision, before.Revision)
			}

			stored := storedInboxRecord(t, store, terminal)
			if stored.Due != (storage.Due{}) {
				t.Fatalf("a terminal command is still due: %+v", stored.Due)
			}
			if stored.Deleted {
				t.Fatal("a terminal command was tombstoned rather than settled")
			}
			// The record the transition returned is the record that is stored,
			// so what the assertions above hold that write to is what a later
			// reader meets.
			if stored.Revision != terminal.Revision {
				t.Fatalf("the stored revision %d is not the one the transition returned %d", stored.Revision, terminal.Revision)
			}

			page, err := store.backend.OrderedIndex.ListDue(
				context.Background(), inboxNamespace, inboxAfterDue.Add(24*time.Hour).UnixMilli(), "", 10)
			if err != nil {
				t.Fatalf("ListDue: %v", err)
			}
			if len(page.Records) != 0 {
				t.Fatalf("a terminal command still appears in the due view: %d records", len(page.Records))
			}

			got, err := store.GetCommand(context.Background(), GetCommandRequest{
				TenantID:  terminal.Record.TenantID,
				SessionID: terminal.Record.SessionID,
				CommandID: terminal.Record.CommandID,
			})
			if err != nil {
				t.Fatalf("a terminal command is no longer gettable: %v", err)
			}
			if got.Record.State != terminal.Record.State || got.AcceptedOrder != admitted.AcceptedOrder {
				t.Fatalf("got %+v, want the terminal record at order %d", got, admitted.AcceptedOrder)
			}
		})
	}
}

// TestTransitionsPreserveTheAdmittedCommand is the carry-forward contract
// sameCommandAs states, driven the whole length of the machine.
//
// The retry at the end is the consequence rather than a courtesy: the compared
// members are compared against a retry of the ORIGINAL command, so a transition
// that rewrote one — emptying an applied command's inline body being the
// obvious housekeeping — would make every later retry a permanent mismatch.
func TestTransitionsPreserveTheAdmittedCommand(t *testing.T) {
	t.Parallel()

	store, _, admitted := inboxFixture(t, memstore.New())

	assertSameCommand := func(t *testing.T, stage string, entry InboxEntry) {
		t.Helper()
		if !entry.Record.sameCommandAs(admitted.Record) {
			t.Fatalf("%s changed the command's content: %q %q", stage, entry.Record.Kind, entry.Record.Payload)
		}
		if entry.Record.RuntimeCommandID != admitted.Record.RuntimeCommandID {
			t.Fatalf("%s changed the runtime mapping", stage)
		}
		if entry.Record.CommandID != admitted.Record.CommandID || entry.AcceptedOrder != admitted.AcceptedOrder {
			t.Fatalf("%s changed the command's identity or acceptance order", stage)
		}
		if !entry.Record.AcceptedAt.Equal(admitted.Record.AcceptedAt) ||
			!entry.Record.ApplyDeadline.Equal(admitted.Record.ApplyDeadline) {
			t.Fatalf("%s moved an admitted instant", stage)
		}
	}

	claimed := mustClaim(t, store, admitted, inboxEpoch)
	assertSameCommand(t, "claim", claimed)
	applying := mustBeginApplying(t, store, claimed, inboxEpoch)
	assertSameCommand(t, "begin applying", applying)
	applied, err := store.CompleteCommand(context.Background(), testCompleteRequest(applying, inboxEpoch))
	if err != nil {
		t.Fatalf("CompleteCommand: %v", err)
	}
	assertSameCommand(t, "complete", applied)
	if len(applied.Record.Payload) == 0 {
		t.Fatal("the applied command's inline body was reclaimed, which no later retry can survive")
	}

	retry, created, err := store.AdmitCommand(context.Background(), testAdmitRequest())
	if err != nil {
		t.Fatalf("a retry of the applied command: %v", err)
	}
	if created {
		t.Fatal("a retry reported a fresh acceptance")
	}
	if retry.Record.RuntimeCommandID != admitted.Record.RuntimeCommandID || retry.AcceptedOrder != admitted.AcceptedOrder {
		t.Fatalf("the retry did not receive the original mapping and order: %+v", retry)
	}
}

// --- the compare-and-swap -------------------------------------------------

// TestTransitionIsOneGetAndOneUpdate holds every transition to reading the
// record it decides against and writing it once, at the revision it read. A
// listing here would be a scan of a namespace that spans the deployment.
func TestTransitionIsOneGetAndOneUpdate(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	store, _, admitted := inboxFixture(t, base)

	ordered.reset()
	claimed := mustClaim(t, store, admitted, inboxEpoch)

	var ops []string
	for _, call := range ordered.snapshot() {
		ops = append(ops, call.op)
	}
	if len(ops) != 2 || ops[0] != "get" || ops[1] != "update" {
		t.Fatalf("a claim issued %v, want one get then one update", ops)
	}
	update, _ := ordered.lastOf("update")
	if update.expectedRevision != admitted.Revision {
		t.Fatalf("the compare-and-swap named revision %d, want the revision read %d", update.expectedRevision, admitted.Revision)
	}
	if update.due != inboxDue(claimed.Record) || update.rank != (storage.Rank{}) {
		t.Fatalf("the update filed due %+v rank %+v", update.due, update.rank)
	}
}

// TestTransitionRefusesAStaleRevision covers both halves of the compare-and-
// swap: the caller's own expectation, checked against the record the store
// reads, and the provider's, checked against a writer that got in between.
func TestTransitionRefusesAStaleRevision(t *testing.T) {
	t.Parallel()

	t.Run("the caller decided against an older record", func(t *testing.T) {
		t.Parallel()

		store, _, admitted := inboxFixture(t, memstore.New())
		claimed := mustClaim(t, store, admitted, inboxEpoch)

		// admitted names the revision before the claim, which is exactly what a
		// caller holding a stale read has.
		_, err := store.BeginApplyingCommand(context.Background(), testBeginApplyingRequest(admitted, inboxEpoch))
		got := assertInboxCode(t, err, InboxErrorConflict)
		if got.Revision != claimed.Revision {
			t.Fatalf("reported revision = %d, want %d", got.Revision, claimed.Revision)
		}
		if got.Field != "expected_revision" {
			t.Fatalf("field = %q", got.Field)
		}
		assertInboxUnchanged(t, store, claimed)
	})

	t.Run("a writer got in between the read and the write", func(t *testing.T) {
		t.Parallel()

		base := memstore.New()
		ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
		base.OrderedIndex = ordered
		store, _, admitted := inboxFixture(t, base)

		// The hook re-enters the store, so it disarms itself before recursing.
		var interposing atomic.Bool
		ordered.beforeUpdate = func() {
			if !interposing.CompareAndSwap(false, true) {
				return
			}
			if _, err := store.ClaimCommand(context.Background(), testClaimRequest(admitted, inboxNextEpoch)); err != nil {
				t.Errorf("interposed claim: %v", err)
			}
		}
		_, err := store.ClaimCommand(context.Background(), testClaimRequest(admitted, inboxEpoch))
		assertInboxCode(t, err, InboxErrorConflict)
	})
}

// TestConcurrentClaimsElectOneWinner is the property the whole compare-and-swap
// exists for, and it is stated over racing goroutines rather than interleaved
// hooks because the failure it guards against — two writers believing they hold
// one command — is a race by nature.
func TestConcurrentClaimsElectOneWinner(t *testing.T) {
	t.Parallel()

	store, _, admitted := inboxFixture(t, memstore.New())

	const claimers = 8
	var (
		start   sync.WaitGroup
		done    sync.WaitGroup
		mu      sync.Mutex
		winners int
	)
	start.Add(1)
	for range claimers {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			_, err := store.ClaimCommand(context.Background(), testClaimRequest(admitted, inboxEpoch))
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				winners++
				return
			}
			var failure *InboxError
			if !errors.As(err, &failure) {
				t.Errorf("loser got %T %v, want *InboxError", err, err)
				return
			}
			if failure.Code != InboxErrorConflict && failure.Code != InboxErrorClaimHeld {
				t.Errorf("loser got %q, want a lost race or a held claim", failure.Code)
			}
		}()
	}
	start.Done()
	done.Wait()

	if winners != 1 {
		t.Fatalf("%d of %d claimers believed they held the command", winners, claimers)
	}
}

// --- the request, before anything is admitted ------------------------------

// TestTransitionRefusesAnInvalidRequestBeforeAdmission holds every request
// member to being validated before the store admits the operation and before
// the provider is touched.
//
// Each case is asserted twice, and the pair is the point. Against a CLOSED
// store the caller must still be told what IT got wrong: the caller's mistake
// is the durable fact and the store's lifecycle is not, so a bad request
// answered with "the store is closing" would send a caller to look at the wrong
// system. Against an open store the record must be untouched.
func TestTransitionRefusesAnInvalidRequestBeforeAdmission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func(store *Store, entry InboxEntry) error
		// operation is the public Store method the case drives, which is what
		// makes the completeness check below a cross product rather than a
		// hand-written list.
		operation string
		want      InboxErrorCode
		field     string
	}{
		{
			name: "claim without a revision",
			call: func(store *Store, entry InboxEntry) error {
				request := testClaimRequest(entry, inboxEpoch)
				request.ExpectedRevision = 0
				_, err := store.ClaimCommand(context.Background(), request)
				return err
			},
			operation: "ClaimCommand",
			want:      InboxErrorInvalid, field: "expected_revision",
		},
		{
			name: "claim without a lease epoch",
			call: func(store *Store, entry InboxEntry) error {
				_, err := store.ClaimCommand(context.Background(), testClaimRequest(entry, 0))
				return err
			},
			operation: "ClaimCommand",
			want:      InboxErrorInvalid, field: "lease_epoch",
		},
		{
			name: "claim with an unstorable expiry",
			call: func(store *Store, entry InboxEntry) error {
				request := testClaimRequest(entry, inboxEpoch)
				request.ClaimExpiresAt = maxRankableTime.Add(time.Hour)
				_, err := store.ClaimCommand(context.Background(), request)
				return err
			},
			operation: "ClaimCommand",
			want:      InboxErrorInvalid, field: "claim",
		},
		{
			name: "claim that has already lapsed",
			call: func(store *Store, entry InboxEntry) error {
				request := testClaimRequest(entry, inboxEpoch)
				request.ClaimExpiresAt = inboxAcceptedAt
				_, err := store.ClaimCommand(context.Background(), request)
				return err
			},
			operation: "ClaimCommand",
			want:      InboxErrorInvalid, field: "claim_expires_at",
		},
		{
			name: "claim of an unnamed command",
			call: func(store *Store, entry InboxEntry) error {
				request := testClaimRequest(entry, inboxEpoch)
				request.CommandID = ""
				_, err := store.ClaimCommand(context.Background(), request)
				return err
			},
			operation: "ClaimCommand",
			want:      InboxErrorInvalid, field: "command_id",
		},
		{
			name: "begin applying without a lease epoch",
			call: func(store *Store, entry InboxEntry) error {
				_, err := store.BeginApplyingCommand(context.Background(), testBeginApplyingRequest(entry, 0))
				return err
			},
			operation: "BeginApplyingCommand",
			want:      InboxErrorInvalid, field: "lease_epoch",
		},
		{
			name: "begin applying with a lapsed claim",
			call: func(store *Store, entry InboxEntry) error {
				request := testBeginApplyingRequest(entry, inboxEpoch)
				request.ClaimExpiresAt = inboxAcceptedAt
				_, err := store.BeginApplyingCommand(context.Background(), request)
				return err
			},
			operation: "BeginApplyingCommand",
			want:      InboxErrorInvalid, field: "claim_expires_at",
		},
		{
			name: "complete with no result",
			call: func(store *Store, entry InboxEntry) error {
				request := testCompleteRequest(entry, inboxEpoch)
				request.Result = CommandResult{}
				_, err := store.CompleteCommand(context.Background(), request)
				return err
			},
			operation: "CompleteCommand",
			want:      InboxErrorInvalid, field: "result",
		},
		{
			name: "complete with a result naming no event",
			call: func(store *Store, entry InboxEntry) error {
				request := testCompleteRequest(entry, inboxEpoch)
				request.Result.EventID = ""
				_, err := store.CompleteCommand(context.Background(), request)
				return err
			},
			operation: "CompleteCommand",
			want:      InboxErrorInvalid, field: "result",
		},
		{
			name: "complete without a lease epoch",
			call: func(store *Store, entry InboxEntry) error {
				_, err := store.CompleteCommand(context.Background(), testCompleteRequest(entry, 0))
				return err
			},
			operation: "CompleteCommand",
			want:      InboxErrorInvalid, field: "lease_epoch",
		},
		{
			name: "reject with no reason",
			call: func(store *Store, entry InboxEntry) error {
				request := testRejectRequest(entry, inboxEpoch)
				request.Rejection = sessionwire.ErrorDetail{}
				_, err := store.RejectCommand(context.Background(), request)
				return err
			},
			operation: "RejectCommand",
			want:      InboxErrorInvalid, field: "rejection",
		},
		{
			name: "reject with reason text that is not text",
			call: func(store *Store, entry InboxEntry) error {
				request := testRejectRequest(entry, inboxEpoch)
				request.Rejection.Message = "no \xff runtime"
				_, err := store.RejectCommand(context.Background(), request)
				return err
			},
			operation: "RejectCommand",
			want:      InboxErrorInvalid, field: "rejection.message",
		},
		{
			name: "reject without a revision",
			call: func(store *Store, entry InboxEntry) error {
				request := testRejectRequest(entry, inboxEpoch)
				request.ExpectedRevision = 0
				_, err := store.RejectCommand(context.Background(), request)
				return err
			},
			operation: "RejectCommand",
			want:      InboxErrorInvalid, field: "expected_revision",
		},
		{
			name: "get an unnamed command",
			call: func(store *Store, entry InboxEntry) error {
				_, err := store.GetCommand(context.Background(), GetCommandRequest{
					TenantID:  entry.Record.TenantID,
					SessionID: entry.Record.SessionID,
				})
				return err
			},
			operation: "GetCommand",
			want:      InboxErrorInvalid, field: "command_id",
		},
		{
			name: "correlate an unnamed command",
			call: func(store *Store, entry InboxEntry) error {
				_, err := store.FindCommandApplication(context.Background(), FindCommandApplicationRequest{
					TenantID:  entry.Record.TenantID,
					SessionID: entry.Record.SessionID,
				})
				return err
			},
			operation: "FindCommandApplication",
			want:      InboxErrorInvalid, field: "command_id",
		},
	}

	// The table is held to covering every public operation the two inbox
	// transition files declare, for the reason the Close tables are: an
	// operation added without a row here silently stops being held to
	// validating its request before the store admits it, and the ordering that
	// costs — an invalid request answered with "the store is closing" — is
	// invisible until a caller meets it.
	declared := map[string]bool{}
	for _, file := range []string{"inbox_claim.go", "inbox_recovery.go"} {
		for name := range declaredStoreOperations(t, file) {
			declared[name] = true
		}
	}
	covered := map[string]bool{}
	for _, test := range tests {
		if test.operation == "" {
			t.Fatalf("the case %q names no operation, so it cannot be counted towards coverage", test.name)
		}
		if !declared[test.operation] {
			t.Errorf("the case %q drives %s, which no inbox transition file declares", test.name, test.operation)
		}
		covered[test.operation] = true
	}
	for name := range declared {
		if !covered[name] {
			t.Errorf("%s takes a request and no case here gives it a malformed one", name)
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store, _, admitted := inboxFixture(t, memstore.New())
			got := assertInboxCode(t, test.call(store, admitted), test.want)
			if got.Field != test.field {
				t.Fatalf("field = %q, want %q", got.Field, test.field)
			}
			assertInboxUnchanged(t, store, admitted)

			// The same request against a store that is closing.
			closing, err := Open(context.Background(), memstore.New(), WithClock(newMovableClock(inboxClaimStart)))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := closing.Close(context.Background()); err != nil {
				t.Fatalf("Close: %v", err)
			}
			err = test.call(closing, admitted)
			if errors.As(err, new(*StoreClosedError)) {
				t.Fatalf("an invalid request on a closing store reported the store's state: %v", err)
			}
			assertInboxCode(t, err, test.want)
		})
	}
}

// TestTransitionRefusesAnUnknownIdentity covers the identities the shared scope
// derivation owns, which report their own type rather than the inbox's.
func TestTransitionRefusesAnUnknownIdentity(t *testing.T) {
	t.Parallel()

	store, _, admitted := inboxFixture(t, memstore.New())

	request := testClaimRequest(admitted, inboxEpoch)
	request.SessionID = ""
	if _, err := store.ClaimCommand(context.Background(), request); !errors.As(err, new(*InvalidIdentityError)) {
		t.Fatalf("error = %T %v, want *InvalidIdentityError", err, err)
	}
	assertInboxUnchanged(t, store, admitted)
}

// --- reading, and what a missing record says -------------------------------

func TestGetCommandReportsWhatIsNotThere(t *testing.T) {
	t.Parallel()

	t.Run("never admitted", func(t *testing.T) {
		t.Parallel()

		store, _, admitted := inboxFixture(t, memstore.New())
		_, err := store.GetCommand(context.Background(), GetCommandRequest{
			TenantID:  admitted.Record.TenantID,
			SessionID: admitted.Record.SessionID,
			CommandID: "command-elsewhere",
		})
		if got := assertInboxCode(t, err, InboxErrorNotFound); got.Field != "get" {
			t.Fatalf("field = %q, want %q", got.Field, "get")
		}
	})

	t.Run("tombstoned", func(t *testing.T) {
		t.Parallel()

		store, _, admitted := inboxFixture(t, memstore.New())
		scope, err := store.deriveSessionScope(admitted.Record.TenantID, admitted.Record.SessionID)
		if err != nil {
			t.Fatalf("deriveSessionScope: %v", err)
		}
		if _, err := store.backend.OrderedIndex.Delete(
			context.Background(), inboxID(scope, admitted.Record.CommandID), admitted.Revision); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, err = store.GetCommand(context.Background(), GetCommandRequest{
			TenantID:  admitted.Record.TenantID,
			SessionID: admitted.Record.SessionID,
			CommandID: admitted.Record.CommandID,
		})
		assertInboxCode(t, err, InboxErrorDeleted)
	})
}

// TestTransitionMeetsATombstoneBetweenTheReadAndTheWrite reaches the one arm of
// the update classification a conforming provider produces only under a race:
// the record was live when the transition decided and a tombstone by the time
// it wrote.
func TestTransitionMeetsATombstoneBetweenTheReadAndTheWrite(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	store, _, admitted := inboxFixture(t, base)

	scope, err := store.deriveSessionScope(admitted.Record.TenantID, admitted.Record.SessionID)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	var deleting atomic.Bool
	ordered.beforeUpdate = func() {
		if !deleting.CompareAndSwap(false, true) {
			return
		}
		if _, err := base.OrderedIndex.Delete(
			context.Background(), inboxID(scope, admitted.Record.CommandID), admitted.Revision); err != nil {
			t.Errorf("interposed delete: %v", err)
		}
	}
	_, err = store.ClaimCommand(context.Background(), testClaimRequest(admitted, inboxEpoch))
	if got := assertInboxCode(t, err, InboxErrorDeleted); got.Field != "update" {
		t.Fatalf("field = %q, want %q", got.Field, "update")
	}
}

func TestClassifyInboxOrderedErrorCoversTheUpdatePath(t *testing.T) {
	t.Parallel()

	id := storage.OrderedID{Namespace: inboxNamespace, OrderingScope: "scope", StableKey: "key"}
	tests := []struct {
		name string
		err  error
		want InboxErrorCode
	}{
		{name: "not found", err: &storage.OrderedRecordNotFoundError{ID: id}, want: InboxErrorNotFound},
		{name: "deleted", err: &storage.OrderedDeletedError{ID: id}, want: InboxErrorDeleted},
		{name: "conflict", err: &storage.OrderedRevisionConflictError{ID: id, ExpectedRevision: 1, ActualRevision: 4}, want: InboxErrorConflict},
		{name: "exhausted", err: &storage.OrderedRevisionExhaustedError{ID: id}, want: InboxErrorBackend},
		{name: "ambiguous", err: &storage.OrderedAmbiguousError{}, want: InboxErrorUnknown},
		{name: "other", err: errors.New("provider is unwell"), want: InboxErrorBackend},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := assertInboxCode(t, classifyInboxOrderedError(test.err, "update"), test.want)
			if !errors.Is(got, test.err) {
				t.Fatalf("the provider cause was not preserved: %v", got)
			}
			if got.Field != "update" {
				t.Fatalf("field = %q", got.Field)
			}
		})
	}

	// The revision a lost race reports is the provider's, so a caller can
	// re-read to it rather than guess.
	conflict := classifyInboxOrderedError(
		&storage.OrderedRevisionConflictError{ID: id, ExpectedRevision: 1, ActualRevision: 4}, "update")
	if got := assertInboxCode(t, conflict, InboxErrorConflict); got.Revision != 4 {
		t.Fatalf("reported revision = %d, want 4", got.Revision)
	}
}

// --- the public projection -------------------------------------------------

// TestCommandStatusProjectsTheDurableState pins the one semantic choice the
// projection makes: a command nobody has picked up is accepted, and one that
// has been picked up is pending, whether or not the claim that picked it up is
// still live.
func TestCommandStatusProjectsTheDurableState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		state string
		at    time.Time
		want  sessionwire.CommandState
	}{
		{name: "unclaimed", state: "pending", want: sessionwire.CommandStateAccepted},
		{name: "claimed", state: "claimed", want: sessionwire.CommandStatePending},
		{name: "claim lapsed", state: "claimed", at: inboxClaimLapsed, want: sessionwire.CommandStatePending},
		{name: "applying", state: "applying", want: sessionwire.CommandStatePending},
		{name: "applied", state: "applied", want: sessionwire.CommandStateApplied},
		{name: "rejected", state: "rejected", want: sessionwire.CommandStateRejected},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store, clock, admitted := inboxFixture(t, memstore.New())
			entry := inboxStates[test.state](t, store, admitted)
			if !test.at.IsZero() {
				clock.set(test.at)
			}

			status, err := entry.CommandStatus()
			if err != nil {
				t.Fatalf("CommandStatus: %v", err)
			}
			if status.State != test.want {
				t.Fatalf("state = %q, want %q", status.State, test.want)
			}
			if status.CommandID != admitted.Record.CommandID || status.AcceptedOrder != admitted.AcceptedOrder {
				t.Fatalf("status = %+v, want the command at order %d", status, admitted.AcceptedOrder)
			}
			// The public projection carries the typed reason for a rejection
			// and nothing for any other state, which is core's own rule.
			if (status.Error != nil) != (test.want == sessionwire.CommandStateRejected) {
				t.Fatalf("status carried error %+v in state %q", status.Error, status.State)
			}
			// It is verified by core rather than by this test restating what a
			// valid status is.
			if err := status.Validate(); err != nil {
				t.Fatalf("core refused the projection: %v", err)
			}
		})
	}
}

func TestCommandStatusRefusesAnEntryTheStoreCannotProduce(t *testing.T) {
	t.Parallel()

	// A hand-assembled entry has not been through the record's own validation,
	// which is the only way to reach either refusal.
	unknown := InboxEntry{Record: InboxRecord{CommandID: inboxCommand, State: "surprise"}, AcceptedOrder: 1}
	if _, err := unknown.CommandStatus(); assertInboxCode(t, err, InboxErrorInvalid).Field != "state" {
		t.Fatal("an unknown state was projected")
	}
	reasonless := InboxEntry{Record: InboxRecord{CommandID: inboxCommand, State: InboxStateRejected}, AcceptedOrder: 1}
	if _, err := reasonless.CommandStatus(); assertInboxCode(t, err, InboxErrorInvalid).Field != "status" {
		t.Fatal("a rejection with no reason was projected")
	}
}

// --- what each state may carry --------------------------------------------

// TestInboxRecordRefusesAnIncoherentState drives validateInboxState from both
// sides: a record a caller could assemble, and a record arriving as BYTES,
// which is the case that matters because no transition in this package can
// produce one and a corrupted row can.
func TestInboxRecordRefusesAnIncoherentState(t *testing.T) {
	t.Parallel()

	claim := CommandClaim{LeaseEpoch: inboxEpoch, ExpiresAt: inboxClaimExpiry}
	result := CommandResult{CompletedAt: inboxAcceptedAt, EventID: "event-1", JournalSeq: 1}
	rejection := &sessionwire.ErrorDetail{Code: sessionwire.ErrorCodeCommandRejected, Message: "no"}

	tests := []struct {
		name   string
		mutate func(*InboxRecord)
		field  string
	}{
		{name: "pending holds a claim", field: "claim", mutate: func(r *InboxRecord) {
			r.State, r.Claim, r.Result = InboxStatePending, claim, CommandResult{}
		}},
		{name: "pending holds a result", field: "result", mutate: func(r *InboxRecord) {
			r.State, r.Claim = InboxStatePending, CommandClaim{}
		}},
		{name: "pending holds a rejection", field: "rejection", mutate: func(r *InboxRecord) {
			r.State, r.Claim, r.Result, r.Rejection = InboxStatePending, CommandClaim{}, CommandResult{}, rejection
		}},
		{name: "claimed holds no claim", field: "claim", mutate: func(r *InboxRecord) {
			r.State, r.Claim, r.Result = InboxStateClaimed, CommandClaim{}, CommandResult{}
		}},
		{name: "claimed holds a result", field: "result", mutate: func(r *InboxRecord) {
			r.State = InboxStateClaimed
		}},
		{name: "applying holds no claim", field: "claim", mutate: func(r *InboxRecord) {
			r.State, r.Claim, r.Result = InboxStateApplying, CommandClaim{}, CommandResult{}
		}},
		{name: "applying holds a rejection", field: "rejection", mutate: func(r *InboxRecord) {
			r.State, r.Result, r.Rejection = InboxStateApplying, CommandResult{}, rejection
		}},
		{name: "applied holds no result", field: "result", mutate: func(r *InboxRecord) {
			r.Result = CommandResult{}
		}},
		{name: "applied holds no claim", field: "claim", mutate: func(r *InboxRecord) {
			r.Claim = CommandClaim{}
		}},
		{name: "applied is also rejected", field: "rejection", mutate: func(r *InboxRecord) {
			r.Rejection = rejection
		}},
		{name: "rejected holds no rejection", field: "rejection", mutate: func(r *InboxRecord) {
			r.State, r.Result = InboxStateRejected, CommandResult{}
		}},
		{name: "rejected is also applied", field: "result", mutate: func(r *InboxRecord) {
			r.State, r.Rejection = InboxStateRejected, rejection
		}},
		{name: "the state is not a state", field: "state", mutate: func(r *InboxRecord) {
			r.State = "surprise"
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			record := testInboxRecord()
			record.Claim, record.Result = claim, result
			test.mutate(&record)

			_, _, err := encodeInboxRecord(record)
			if got := assertInboxCode(t, err, InboxErrorInvalid); got.Field != test.field {
				t.Fatalf("encode reported field %q, want %q", got.Field, test.field)
			}

			// The same record as stored bytes. It is built by encoding a
			// coherent record and substituting the offending members, because a
			// decoder that rejected it for being unparsable would prove nothing
			// about the rule under test.
			coherent := testInboxRecord()
			coherent.Claim, coherent.Result = claim, result
			value, _, err := encodeInboxRecord(coherent)
			if err != nil {
				t.Fatalf("encode the coherent record: %v", err)
			}
			var members map[string]json.RawMessage
			if err := json.Unmarshal(value, &members); err != nil {
				t.Fatalf("the coherent record is not JSON: %v", err)
			}
			mutated := testInboxRecord()
			mutated.Claim, mutated.Result = claim, result
			test.mutate(&mutated)
			members["state"] = mustJSON(t, mutated.State)
			setOrDelete(members, "claim", mutated.Claim.isZero(), mustJSON(t, commandClaimWire{
				LeaseEpoch: mutated.Claim.LeaseEpoch, ExpiresAt: mutated.Claim.ExpiresAt,
			}))
			setOrDelete(members, "result", mutated.Result.isZero(), mustJSON(t, commandResultWire{
				CompletedAt: mutated.Result.CompletedAt, EventID: mutated.Result.EventID, JournalSeq: mutated.Result.JournalSeq,
			}))
			setOrDelete(members, "rejection", mutated.Rejection == nil, mustJSON(t, mutated.Rejection))
			encoded, err := json.Marshal(members)
			if err != nil {
				t.Fatalf("marshal the mutated record: %v", err)
			}
			if _, err := decodeInboxRecord(encoded); assertInboxCode(t, err, InboxErrorInvalid).Field != test.field {
				t.Fatalf("decode reported %v, want invalid %q", err, test.field)
			}
		})
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return encoded
}

func setOrDelete(members map[string]json.RawMessage, name string, absent bool, value json.RawMessage) {
	if absent {
		delete(members, name)
		return
	}
	members[name] = value
}

// --- the public surface and its lifecycle ---------------------------------

var (
	_ func(*Store, context.Context, GetCommandRequest) (InboxEntry, error)           = (*Store).GetCommand
	_ func(*Store, context.Context, ClaimCommandRequest) (InboxEntry, error)         = (*Store).ClaimCommand
	_ func(*Store, context.Context, BeginApplyingCommandRequest) (InboxEntry, error) = (*Store).BeginApplyingCommand
	_ func(*Store, context.Context, CompleteCommandRequest) (InboxEntry, error)      = (*Store).CompleteCommand
	_ func(*Store, context.Context, RejectCommandRequest) (InboxEntry, error)        = (*Store).RejectCommand
	_ func(InboxEntry) (sessionwire.CommandStatus, error)                            = InboxEntry.CommandStatus
)

// declaredStoreOperations returns the name of every public Store operation
// declared in one source file, parsed from the source rather than listed by
// hand, so an operation cannot be added without a close test noticing.
//
// It takes the file name because each subject's close test enumerates the file
// that DECLARES its operations, and this file adds a second inbox file. One
// parameterized enumerator beats a copy per file for the ordinary reason: the
// copies would drift, and the drift would be silent.
func declaredStoreOperations(t *testing.T, filename string) map[string]bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", filename, err)
	}
	operations := map[string]bool{}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv == nil || !function.Name.IsExported() {
			continue
		}
		receiver, ok := function.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		if name, ok := receiver.X.(*ast.Ident); ok && name.Name == "Store" {
			operations[function.Name.Name] = true
		}
	}
	if len(operations) == 0 {
		t.Fatalf("no public Store operations were found in %s; the enumerator is not reaching the declarations", filename)
	}
	return operations
}

func TestInboxClaimOperationsRefuseAfterClose(t *testing.T) {
	store, _, admitted := inboxFixture(t, memstore.New())
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	operations := map[string]func() error{
		"GetCommand": func() error {
			_, err := store.GetCommand(context.Background(), GetCommandRequest{
				TenantID:  admitted.Record.TenantID,
				SessionID: admitted.Record.SessionID,
				CommandID: admitted.Record.CommandID,
			})
			return err
		},
		"ClaimCommand": func() error {
			_, err := store.ClaimCommand(context.Background(), testClaimRequest(admitted, inboxEpoch))
			return err
		},
		"BeginApplyingCommand": func() error {
			_, err := store.BeginApplyingCommand(context.Background(), testBeginApplyingRequest(admitted, inboxEpoch))
			return err
		},
		"CompleteCommand": func() error {
			_, err := store.CompleteCommand(context.Background(), testCompleteRequest(admitted, inboxEpoch))
			return err
		},
		"RejectCommand": func() error {
			_, err := store.RejectCommand(context.Background(), testRejectRequest(admitted, inboxEpoch))
			return err
		},
	}

	declared := declaredStoreOperations(t, "inbox_claim.go")
	for name := range declared {
		if operations[name] == nil {
			t.Errorf("inbox_claim.go declares the public operation %s and this test does not exercise it", name)
		}
	}
	for name := range operations {
		if !declared[name] {
			t.Errorf("this test exercises %s, which inbox_claim.go no longer declares (was it moved to another file?)", name)
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

// --- boundaries -----------------------------------------------------------

// TestClaimLivenessIsHalfOpen pins the claim's expiry instant itself, which is
// the only place an off-by-one in the interval is observable and the place the
// convention has to agree with the apply deadline's.
func TestClaimLivenessIsHalfOpen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		at   time.Time
		live bool
	}{
		{name: "a millisecond before expiry", at: inboxClaimExpiry.Add(-time.Millisecond), live: true},
		{name: "at expiry", at: inboxClaimExpiry},
		{name: "after expiry", at: inboxClaimExpiry.Add(time.Millisecond)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// Each probe gets its own fixture: whichever of them the instant
			// admits advances the revision, and the other would then be
			// answered by the compare-and-swap rather than by the interval.
			claimedIn := func() (*Store, InboxEntry) {
				store, clock, admitted := inboxFixture(t, memstore.New())
				claimed := inboxStates["claimed"](t, store, admitted)
				clock.set(test.at)
				return store, claimed
			}

			// A live claim holds off an equal epoch; a lapsed one is
			// reclaimable.
			store, claimed := claimedIn()
			request := testClaimRequest(claimed, inboxEpoch)
			request.ClaimExpiresAt = inboxDeadline
			_, claimErr := store.ClaimCommand(context.Background(), request)

			// A live claim may begin applying; a lapsed one may not.
			store, claimed = claimedIn()
			applying := testBeginApplyingRequest(claimed, inboxEpoch)
			applying.ClaimExpiresAt = inboxDeadline
			_, applyErr := store.BeginApplyingCommand(context.Background(), applying)

			if test.live {
				assertInboxCode(t, claimErr, InboxErrorClaimHeld)
				if applyErr != nil {
					t.Fatalf("a live claim could not begin applying: %v", applyErr)
				}
				return
			}
			if claimErr != nil {
				t.Fatalf("a lapsed claim was not reclaimable: %v", claimErr)
			}
			assertInboxCode(t, applyErr, InboxErrorClaimLost)
		})
	}
}

// TestCompleteRecordsAnApplicationWhoseClaimLapsed is the asymmetry
// CompleteCommand documents, made observable: an effect that is already in the
// journal is recorded by the lease that committed it even if its claim TTL ran
// out first, while the same lapse still stops that lease from DECIDING to
// reject.
func TestCompleteRecordsAnApplicationWhoseClaimLapsed(t *testing.T) {
	t.Parallel()

	store, clock, admitted := inboxFixture(t, memstore.New())
	applying := inboxStates["applying"](t, store, admitted)
	clock.set(inboxClaimLapsed)

	_, err := store.RejectCommand(context.Background(), testRejectRequest(applying, inboxEpoch))
	assertInboxCode(t, err, InboxErrorClaimLost)

	applied, err := store.CompleteCommand(context.Background(), testCompleteRequest(applying, inboxEpoch))
	if err != nil {
		t.Fatalf("a committed application could not be recorded after its claim lapsed: %v", err)
	}
	if applied.Record.State != InboxStateApplied || applied.Record.Result.JournalSeq != 42 {
		t.Fatalf("applied record = %+v", applied.Record)
	}
	// The claim that applied it is kept as the durable record of which lease
	// did so.
	if applied.Record.Claim.LeaseEpoch != inboxEpoch {
		t.Fatalf("the applying claim was not preserved: %+v", applied.Record.Claim)
	}
}

// TestCompleteRefusesALeaseThatDidNotApply closes the other half of the
// completion rule: dropping the claim TTL requirement does not mean dropping
// the claim. A successor lease may not record an application it did not start
// on its own word — it is sent to the journal, and a session with no
// application evidence in it refuses.
//
// This is where the two halves of the file meet, so the failure it asserts is
// deliberately the EVIDENCE one rather than a lost claim: a successor is no
// longer refused for being a successor, it is refused for having nothing to
// point at. The evidence it would need is inbox_recovery_test.go's subject.
func TestCompleteRefusesALeaseThatDidNotApply(t *testing.T) {
	t.Parallel()

	store, _, admitted := inboxFixture(t, memstore.New())
	applying := inboxStates["applying"](t, store, admitted)

	_, err := store.CompleteCommand(context.Background(), testCompleteRequest(applying, inboxNextEpoch))
	if got := assertInboxCode(t, err, InboxErrorEvidence); got.Field != "result" {
		t.Fatalf("field = %q, want %q", got.Field, "result")
	}
	assertInboxUnchanged(t, store, applying)
}

// TestInboxReadsVerifyTheSessionBindingBeforeTheProvider is the inbox's
// counterpart to the catalog's binding test: a command is session-scoped data,
// so a session whose collision witnesses are not bound has no readable commands
// and the provider is never asked.
func TestInboxReadsVerifyTheSessionBindingBeforeTheProvider(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	store := openStore(t, base, WithClock(newMovableClock(inboxClaimStart)))

	unbound := GetCommandRequest{TenantID: catalogTenant, SessionID: catalogSession, CommandID: inboxCommand}
	if _, err := store.GetCommand(context.Background(), unbound); err == nil {
		t.Fatal("a command in an unbound session was readable")
	} else {
		assertKeyspaceCode(t, err, KeyspaceBindingNotFound)
	}
	claim := ClaimCommandRequest{
		TenantID: catalogTenant, SessionID: catalogSession, CommandID: inboxCommand,
		ExpectedRevision: 1, LeaseEpoch: inboxEpoch, ClaimExpiresAt: inboxClaimExpiry,
	}
	if _, err := store.ClaimCommand(context.Background(), claim); err == nil {
		t.Fatal("a command in an unbound session was claimable")
	} else {
		assertKeyspaceCode(t, err, KeyspaceBindingNotFound)
	}
	if got := ordered.countOf("get"); got != 0 {
		t.Fatalf("an unbound session reached the OrderedIndex %d times", got)
	}
}

// TestClaimTTLIsBoundedAbove pins the ceiling on how far ahead of the store's
// clock a claim may lapse, at the instant itself.
//
// The bound is not tidiness. A claim may legitimately outlive the apply deadline
// — that is what lets it win the deadline race — and inboxDue caps the due
// horizon at the deadline, so an over-long claim leaves the row DUE and
// unactionable for the claim's whole life: it is the head-of-line occupancy
// RejectCommand documents, reached by one caller with a skewed clock rather than
// by a crash.
func TestClaimTTLIsBoundedAbove(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expires time.Duration
		want    InboxErrorCode
	}{
		{name: "at the ceiling", expires: MaxCommandClaimTTL},
		{name: "past the ceiling", expires: MaxCommandClaimTTL + time.Millisecond, want: InboxErrorInvalid},
		{name: "far past the ceiling", expires: 100 * 365 * 24 * time.Hour, want: InboxErrorInvalid},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store, _, admitted := inboxFixture(t, memstore.New())
			request := testClaimRequest(admitted, inboxEpoch)
			request.ClaimExpiresAt = inboxClaimStart.Add(test.expires)

			claimed, err := store.ClaimCommand(context.Background(), request)
			if test.want == "" {
				if err != nil {
					t.Fatalf("a claim at the ceiling was refused: %v", err)
				}
				if !claimed.Record.Claim.ExpiresAt.Equal(request.ClaimExpiresAt) {
					t.Fatalf("stored expiry = %s, want %s", claimed.Record.Claim.ExpiresAt, request.ClaimExpiresAt)
				}
				return
			}
			if got := assertInboxCode(t, err, test.want); got.Field != "claim_expires_at" {
				t.Fatalf("field = %q, want %q", got.Field, "claim_expires_at")
			}
			assertInboxUnchanged(t, store, admitted)
		})
	}
}

// declaredInboxStates enumerates the InboxState constants from inbox.go's
// source, so the state machine's product is closed over what the PACKAGE
// declares rather than over what this file remembered to register.
//
// It is the states half of the discipline declaredStoreOperations applies to
// the operations, and it closes the last hop: without it a sixth InboxState
// constant reaches the record's validator and the durable machine while
// inboxStates — a hand-maintained map — says nothing, and the cross product
// stays green over five states because five is all it was ever shown.
func declaredInboxStates(t *testing.T) map[string]bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "inbox.go", nil, 0)
	if err != nil {
		t.Fatalf("parse inbox.go: %v", err)
	}
	states := map[string]bool{}
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, spec := range general.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			// The type is named on the spec, so a constant block that also
			// declares something else cannot smuggle a non-state in.
			if name, ok := value.Type.(*ast.Ident); !ok || name.Name != "InboxState" {
				continue
			}
			for _, literal := range value.Values {
				text, ok := literal.(*ast.BasicLit)
				if !ok || text.Kind != token.STRING {
					continue
				}
				unquoted, err := strconv.Unquote(text.Value)
				if err != nil {
					t.Fatalf("unquote %s: %v", text.Value, err)
				}
				states[unquoted] = true
			}
		}
	}
	if len(states) == 0 {
		t.Fatal("no InboxState constants were found in inbox.go; the enumerator is not reaching the declarations")
	}
	return states
}

func TestCommandStateMachineCoversEveryDeclaredState(t *testing.T) {
	t.Parallel()

	declared := declaredInboxStates(t)
	for state := range declared {
		if inboxStates[state] == nil {
			t.Errorf("inbox.go declares the state %q and the machine's tests cannot build a record in it", state)
		}
	}
	for state := range inboxStates {
		if !declared[state] {
			t.Errorf("the tests build a record in state %q, which inbox.go no longer declares", state)
		}
	}
}
