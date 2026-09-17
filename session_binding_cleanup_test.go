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

func TestSessionBindingRefusesLegacyGateIntentRetirement(t *testing.T) {
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
	err := s.RetireGateDeadlineIntent(ctx, retireRequest("gate-old", revision))
	assertCatalogCode(t, err, CatalogErrorConflict)
	after := storedGateIntent(t, s, "gate-old")
	if after.Deleted || after.Revision != revision {
		t.Fatal("legacy cleanup retired disposition gate intent")
	}
}
