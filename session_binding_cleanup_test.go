package sessionstore

import (
	"context"
	"testing"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func TestSessionBindingRefusesReconciliationReleaseOnWitnessDisagreement(t *testing.T) {
	s, _ := reconcileFixture(t, memstore.New())
	ctx := context.Background()
	req := testCreateRequest()
	req.Binding = testSessionBinding()
	if _, _, err := s.CreateCatalogEntry(ctx, req); err != nil {
		t.Fatal(err)
	}
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	value, claim, err := encodeReconciliationClaim(testReconciliationClaim())
	if err != nil {
		t.Fatal(err)
	}
	id := reconciliationClaimID(scope, req.SessionID)
	before, _, err := s.backend.OrderedIndex.Create(ctx, id, scope.SessionNamespace, value, storage.Rank{}, reconciliationClaimDue(claim))
	if err != nil {
		t.Fatal(err)
	}
	// A claim has no protocol of its own; refusal is justified only when the
	// catalog and immutable witness disagree, not by a disposition catalog.
	key := scope.SessionNamespace + "/protocol"
	_, revision, err := s.keys.kv.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.keys.kv.Put(ctx, key, revision, encodeWitness(1, scope.sessionWitness, []byte(ProtocolModeLegacy))); err != nil {
		t.Fatal(err)
	}
	_, err = s.ReleaseReconciliationClaim(ctx, testReleaseRequest(reconcileHolder))
	assertCatalogCode(t, err, CatalogErrorConflict)
	after, err := s.backend.OrderedIndex.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision != before.Revision {
		t.Fatal("legacy cleanup advanced disposition claim revision")
	}
}

// Until v0.13.0 this pinned a REFUSAL: retirement reserved the legacy protocol,
// so a disposition session's remnant could never be retired. Since host v0.4.0
// every Host-published gate lives on a disposition session, so the refusal
// left every such remnant in the due view for good (tests lane, I1.3). What
// still holds is the property the refusal was protecting: retirement never
// runs the LEGACY mutation on a disposition session — it pins no legacy mode
// and deletes nothing; it retires the row in place.
func TestSessionBindingRetiresDispositionGateIntentInPlace(t *testing.T) {
	clock := newMovableClock(catalogActiveAt)
	s := openStore(t, memstore.New(), WithClock(clock))
	ctx := context.Background()
	req := testCreateRequest()
	req.Binding = testSessionBinding()
	if _, _, err := s.CreateCatalogEntry(ctx, req); err != nil {
		t.Fatal(err)
	}
	revision := seedRemnantOfACrashBeforeGateOpened(t, s, "gate-old")
	clock.set(catalogActiveAt.Add(MinGateIntentRemnantAge))
	if err := s.RetireGateDeadlineIntent(ctx, retireRequest("gate-old", revision)); err != nil {
		t.Fatalf("RetireGateDeadlineIntent: %v", err)
	}
	after := storedGateIntent(t, s, "gate-old")
	if after.Deleted {
		t.Fatal("a disposition remnant was tombstoned, so no successor could re-publish its gate")
	}
	assertIntentRetired(t, s, "gate-old")
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if mode, bound, err := s.boundProtocolMode(ctx, scope); err != nil || !bound || mode != ProtocolModeDisposition {
		t.Fatalf("protocol witness = %q bound=%v err=%v, want disposition", mode, bound, err)
	}
}
