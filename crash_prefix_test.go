package sessionstore

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// These tests stop after each durable prefix a process can leave behind, then
// drive the next operation that is allowed to act on what remains. The second
// assertion is the important half: a prefix is safe only when its read-side
// meaning licenses the recovery action and no stronger one.

func TestCrashPrefixObjectUploadWithoutReferenceReadsAsDeletableOrphan(t *testing.T) {
	backend := memstore.New()
	baseLedger := backend.Ledger
	ledger := &scriptedLedger{Ledger: baseLedger}
	backend.Ledger = ledger
	store := openJournalStore(t, backend)
	store.overflowThreshold = 8
	writer := openTestJournal(t, store)
	name := journalName(t, store)

	ledger.appendFn = func(expected uint64, _ []byte) (bool, error) {
		return true, &storage.ConflictError{Name: name, Expected: expected}
	}
	_, err := writer.Append(context.Background(), publicEvent(
		"event-crashed", `{"body":"`+strings.Repeat("x", 40)+`"}`))
	requireJournalCode(t, err, JournalErrorFenced)

	if records := readLedger(t, backend, name); len(records) != 1 || records[0].Kind != EnvelopeKindOpeningFence {
		t.Fatalf("truncated journal = %+v, want only the opening fence and no reference", records)
	}
	orphans, err := store.listObjectReferences(context.Background(), testTenant, testSession, ObjectKindJournalPublic)
	if err != nil {
		t.Fatalf("listObjectReferences: %v", err)
	}
	if len(orphans) != 1 {
		t.Fatalf("uploaded objects = %+v, want the one unreferenced upload", orphans)
	}
	if err := store.deleteObject(context.Background(), testTenant, testSession, ObjectKindJournalPublic, orphans[0]); err != nil {
		t.Fatalf("delete orphan: %v", err)
	}
	if remaining, err := store.listObjectReferences(context.Background(), testTenant, testSession, ObjectKindJournalPublic); err != nil || len(remaining) != 0 {
		t.Fatalf("objects after orphan cleanup = %+v, %v", remaining, err)
	}
}

func TestCrashPrefixOpeningFenceLicensesOnlyAFreshHigherEpoch(t *testing.T) {
	backend := memstore.New()
	backend.Leaser = newPermissiveLeaser()
	store := openJournalStore(t, backend)
	first := openTestJournal(t, store)

	second := openTestJournal(t, store)
	records := readLedger(t, backend, journalName(t, store))
	if len(records) != 2 || records[0].Kind != EnvelopeKindOpeningFence || records[0].LeaseEpoch != first.Epoch() ||
		records[1].Kind != EnvelopeKindOpeningFence || records[1].LeaseEpoch != second.Epoch() {
		t.Fatalf("truncated opens = %+v, want consecutive ownership fences", records)
	}
	if _, err := first.Append(context.Background(), runtimeControl("stale", `{}`)); err == nil {
		t.Fatal("the durable successor fence licensed an append by the stale writer")
	} else {
		requireJournalCode(t, err, JournalErrorFenced)
	}
	if _, err := second.Append(context.Background(), runtimeControl("successor", `{}`)); err != nil {
		t.Fatalf("fresh higher-epoch writer could not append: %v", err)
	}
}

func TestCrashPrefixInboxCreateReturnsTheOriginalAdmission(t *testing.T) {
	store := openStore(t, memstore.New())
	first := mustAdmit(t, store, testAdmitRequest())

	// The retry PROPOSES A DIFFERENT RUNTIME ID, and that is what makes the
	// assertion below able to fail at all. Retrying with testAdmitRequest()'s
	// fixed proposal meant "returned the winner's stored mapping" and "handed
	// the caller back its own proposal" were the same value, so a duplicate
	// path that did the second would have passed unchallenged — the row this
	// test underwrites claims the ORIGINAL identity is returned, and an
	// identical proposal cannot witness that.
	retryRequest := testAdmitRequest()
	retryRequest.ProposedRuntimeCommandID = inboxRetryRuntime
	if first.Record.RuntimeCommandID == retryRequest.ProposedRuntimeCommandID {
		t.Fatal("the retry proposes the admitted runtime id, so the carry-forward assertion cannot fail")
	}
	retry, created, err := store.AdmitCommand(context.Background(), retryRequest)
	if err != nil {
		t.Fatalf("AdmitCommand retry: %v", err)
	}
	if created || retry.Revision != first.Revision || retry.AcceptedOrder != first.AcceptedOrder ||
		retry.Record.RuntimeCommandID != first.Record.RuntimeCommandID || retry.Record.State != InboxStatePending {
		t.Fatalf("retry = %+v created=%v, want the original pending admission %+v", retry, created, first)
	}
	got, err := store.GetCommand(context.Background(), GetCommandRequest{
		TenantID: first.Record.TenantID, SessionID: first.Record.SessionID, CommandID: first.Record.CommandID,
	})
	if err != nil || got.Revision != first.Revision {
		t.Fatalf("GetCommand after lost create reply = %+v, %v", got, err)
	}
}

func TestCrashPrefixesClaimApplyAndTerminalLicenseOnlyTheirNextTransitions(t *testing.T) {
	store, clock, admitted := inboxFixture(t, memstore.New())
	claimed := mustClaim(t, store, admitted, inboxEpoch)

	got, err := store.GetCommand(context.Background(), GetCommandRequest{
		TenantID: admitted.Record.TenantID, SessionID: admitted.Record.SessionID, CommandID: admitted.Record.CommandID,
	})
	if err != nil || got.Record.State != InboxStateClaimed {
		t.Fatalf("claim prefix reads as %+v, %v; want claimed", got, err)
	}
	if _, err := store.ClaimCommand(context.Background(), testClaimRequest(claimed, inboxEpoch)); err == nil {
		t.Fatal("a live claim licensed another writer under the same epoch")
	} else {
		assertInboxCode(t, err, InboxErrorClaimHeld)
	}

	reclaimed, err := store.ClaimCommand(context.Background(), testClaimRequest(claimed, inboxNextEpoch))
	if err != nil {
		t.Fatalf("live claim did not license its higher-epoch successor: %v", err)
	}
	begin := testBeginApplyingRequest(reclaimed, inboxNextEpoch)
	applying, err := store.BeginApplyingCommand(context.Background(), begin)
	if err != nil {
		t.Fatalf("claimed prefix did not license begin-applying: %v", err)
	}
	// "Do not start a new claim" is DRIVEN rather than asserted in prose. The
	// attempt is made BEFORE the clock is advanced, and that placement is the
	// point: at this instant the deadline is open and the epoch is the claim's
	// own, so state is the ONLY thing that can refuse it. Made after the
	// advance, the deadline would refuse it too and the state gate could be
	// removed without this test noticing.
	if _, err := store.ClaimCommand(context.Background(), testClaimRequest(applying, inboxNextEpoch)); err == nil {
		t.Fatal("an applying record licensed a new claim")
	} else {
		assertInboxCode(t, err, InboxErrorState)
	}
	clock.set(inboxAfterDue)
	completed, err := store.CompleteCommand(context.Background(), testCompleteRequest(applying, inboxNextEpoch))
	if err != nil {
		t.Fatalf("applying prefix did not license completion after deadline: %v", err)
	}
	if completed.Record.State != InboxStateApplied {
		t.Fatalf("terminal prefix = %+v, want applied", completed.Record)
	}

	retry, created, err := store.AdmitCommand(context.Background(), testAdmitRequest())
	if err != nil || created || retry.Record.State != InboxStateApplied || retry.Revision != completed.Revision {
		t.Fatalf("admission after lost terminal reply = %+v created=%v err=%v", retry, created, err)
	}
}

func TestCrashPrefixRegistryCleanupReadsAsReleasedAndRetainsItsFence(t *testing.T) {
	store, _ := registryFixture(t, memstore.New())
	mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
	cleared, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryEpoch))
	if err != nil {
		t.Fatalf("ClearHostRegistration: %v", err)
	}

	_, err = store.GetHostRegistration(context.Background(), GetHostRegistrationRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	})
	if got := assertRegistryCode(t, err, RegistryErrorReleased); got.Epoch != registryEpoch {
		t.Fatalf("released read carries epoch %d, want %d", got.Epoch, registryEpoch)
	}
	retry, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryEpoch))
	if err != nil || retry.Revision != cleared.Revision {
		t.Fatalf("cleanup retry = %+v, %v; want the durable tombstone", retry, err)
	}
	if _, err := store.PutHostRegistration(context.Background(), testPutRegistrationRequest(registryStaleEpoch)); err == nil {
		t.Fatal("released state licensed a stale Host to reset the registry fence")
	} else {
		assertRegistryCode(t, err, RegistryErrorEpoch)
	}
}

func TestCrashPrefixesPointerReplaceAndClearRetainBothHighWaters(t *testing.T) {
	for _, ops := range pointerOperationMatrix() {
		t.Run(ops.Noun, func(t *testing.T) {
			store, _ := pointerFixture(t, memstore.New())
			ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch, pointerSequence, 1))
			replacement := ops.testSetRequest(t, pointerEpoch, pointerSequence+1, 2)
			replaced := ops.mustSet(t, store, replacement)
			if got := ops.mustGet(t, store); got.Revision != replaced.Revision || *got.Pointer.Target != replacement.Target {
				t.Fatalf("lost replace reply reads as %+v, want %+v", got, replaced)
			}

			cleared, err := ops.Clear(store, context.Background(), testClearPointerRequest(pointerEpoch))
			if err != nil {
				t.Fatalf("Clear%s: %v", ops.Noun, err)
			}
			_, err = ops.Get(store, context.Background(), testGetPointerRequest())
			if got := assertPointerCode(t, err, PointerErrorCleared); got.Epoch != pointerEpoch || got.Sequence != pointerSequence+1 {
				t.Fatalf("cleared read carries epoch=%d sequence=%d", got.Epoch, got.Sequence)
			}
			if _, err := ops.Set(store, context.Background(), ops.testSetRequest(t, pointerEpoch-1, pointerSequence+2, 3)); err == nil {
				t.Fatal("clear licensed a stale epoch to replace the pointer")
			} else {
				assertPointerField(PointerErrorEpoch, "lease_epoch")(t, err)
			}
			// The OTHER half of what the tombstone's two high-waters license,
			// which the epoch attempt above cannot witness: a LIVE lease
			// carrying an older capture. It is driven one below the retained
			// sequence rather than far below it, so an off-by-one in the fence
			// — admitting the sequence it must refuse — fails here.
			if _, err := ops.Set(store, context.Background(), ops.testSetRequest(t, pointerEpoch, pointerSequence, 3)); err == nil {
				t.Fatal("clear licensed an older sequence to replace the pointer")
			} else {
				got := assertPointerCode(t, err, PointerErrorSequence)
				if got.Field != "sequence" || got.Epoch != pointerEpoch || got.Sequence != pointerSequence+1 {
					t.Fatalf("older-sequence refusal = %+v, want the retained epoch and sequence high-waters", got)
				}
			}
			retry, err := ops.Clear(store, context.Background(), testClearPointerRequest(pointerEpoch))
			if err != nil || retry.Revision != cleared.Revision {
				t.Fatalf("clear retry = %+v, %v; want the durable tombstone", retry, err)
			}
		})
	}
}

func TestCrashPrefixesGateIntentAndOpenProjectionLicenseDifferentSweepActions(t *testing.T) {
	t.Run("intent without projection is an age-gated remnant", func(t *testing.T) {
		base := memstore.New()
		ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
		base.OrderedIndex = ordered
		clock := newMovableClock(catalogActiveAt)
		store := openStore(t, base, WithControlShards(1), WithClock(clock))
		mustPrepareSession(t, store, catalogTenant, catalogSession, 10)
		gate := gateWithDeadline(testGate("gate-crashed", 5), catalogDeadline)
		interruptOpenAfterItsIntent(t, store, ordered, gate, 10)

		// WHICH OPERATION HOLDS THE AGE. ListDueGates has no age filter and
		// reads no clock: it reports the remnant as soon as the intent is
		// durable, and it is asserted here at the intent's OWN instant to say
		// so. An earlier account of this prefix read "after
		// MinGateIntentRemnantAge, the due query reports one remnant", which
		// credited the reporting operation with a fence it does not have and
		// left the operation that DOES have it — RetireGateDeadlineIntent —
		// described as unconditional.
		listed := func(t *testing.T) DueGatePage {
			t.Helper()
			page, err := store.ListDueGates(context.Background(), ListDueGatesRequest{
				Shard: 0, DueAtOrBefore: catalogDeadline, Limit: 10,
			})
			if err != nil || len(page.Remnants) != 1 || len(page.Gates) != 0 {
				t.Fatalf("intent-only prefix reads as %+v, %v; want one remnant", page, err)
			}
			return page
		}
		page := listed(t)

		// The age gate is driven AT THE THRESHOLD, from BOTH sides, one
		// nanosecond apart. MinGateIntentRemnantAge is half-open — an intent
		// exactly that old is retirable and one nanosecond younger is not — and
		// a pair of attempts further apart than that leaves the comparison's
		// boundary untested in both directions, which is how an off-by-one on a
		// fence survives a suite that looks thorough.
		clock.set(catalogActiveAt.Add(MinGateIntentRemnantAge - time.Nanosecond))
		if younger := listed(t); len(younger.Remnants) != 1 {
			t.Fatalf("the due query withheld the remnant inside the retirement window: %+v", younger)
		}
		assertCatalogField(t,
			store.RetireGateDeadlineIntent(context.Background(), RetireGateDeadlineIntentRequest(page.Remnants[0])),
			CatalogErrorTooSoon, "recorded_at")

		clock.set(catalogActiveAt.Add(MinGateIntentRemnantAge))
		if err := store.RetireGateDeadlineIntent(context.Background(), RetireGateDeadlineIntentRequest(page.Remnants[0])); err != nil {
			t.Fatalf("aged remnant did not license retirement: %v", err)
		}
	})

	t.Run("intent with projection is current due work", func(t *testing.T) {
		store, clock := retireFixture(t)
		gate := gateWithDeadline(testGate("gate-open", 5), catalogDeadline)
		mustOpenGate(t, store, 1, gate)
		clock.set(catalogActiveAt.Add(24 * time.Hour))
		page, err := store.ListDueGates(context.Background(), ListDueGatesRequest{
			Shard: 0, DueAtOrBefore: catalogDeadline, Limit: 10,
		})
		if err != nil || len(page.Gates) != 1 || len(page.Remnants) != 0 {
			t.Fatalf("open prefix reads as %+v, %v; want current due work", page, err)
		}
		revision := storedGateIntent(t, store, gate.GateID).Revision
		err = store.RetireGateDeadlineIntent(context.Background(), retireRequest(gate.GateID, revision))
		assertCatalogField(t, err, CatalogErrorConflict, "gate_id")
	})
}
