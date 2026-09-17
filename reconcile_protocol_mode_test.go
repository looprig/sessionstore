package sessionstore

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/looprig/storage/memstore"
)

func TestReconciliationClaimsServeCatalogProtocolThroughLifecycle(t *testing.T) {
	for _, mode := range []ProtocolMode{ProtocolModeLegacy, ProtocolModeDisposition} {
		t.Run(string(mode), func(t *testing.T) {
			store, clock := reconcileFixture(t, memstore.New())
			if mode == ProtocolModeDisposition {
				createDispositionCatalog(t, store)
			} else {
				if _, _, err := store.CreateCatalogEntry(context.Background(), testCreateRequest()); err != nil {
					t.Fatal(err)
				}
			}
			before, found := protocolWitnessOf(t, store)
			if !found {
				t.Fatal("missing protocol witness")
			}
			mustAcquireClaim(t, store, testAcquireRequest(reconcileHolder))
			mustAcquireClaim(t, store, testAcquireRequest(reconcileHolder))
			_, err := store.AcquireReconciliationClaim(context.Background(), testAcquireRequest(reconcileOtherHolder))
			assertReconcileCode(t, err, ReconcileErrorHeld)
			if _, err := store.ReleaseReconciliationClaim(context.Background(), testReleaseRequest(reconcileHolder)); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReleaseReconciliationClaim(context.Background(), testReleaseRequest(reconcileHolder)); err != nil {
				t.Fatal(err)
			}
			mustAcquireClaim(t, store, testAcquireRequest(reconcileOtherHolder))
			clock.set(reconcileLapsedAt)
			_, err = store.GetReconciliationClaim(context.Background(), testGetClaimRequest())
			assertReconcileCode(t, err, ReconcileErrorLapsed)
			next := testAcquireRequest(reconcileHolder)
			next.ExpiresAt = reconcileLapsedAt.Add(30 * time.Second)
			mustAcquireClaim(t, store, next)
			after, _ := protocolWitnessOf(t, store)
			if !bytes.Equal(before, after) {
				t.Fatal("claim changed protocol witness")
			}
		})
	}
}

func TestReconciliationClaimRefusesCatalogWitnessDisagreement(t *testing.T) {
	for _, mode := range []ProtocolMode{ProtocolModeLegacy, ProtocolModeDisposition} {
		t.Run(string(mode), func(t *testing.T) {
			store, _ := reconcileFixture(t, memstore.New())
			if mode == ProtocolModeDisposition {
				createDispositionCatalog(t, store)
			} else {
				if _, _, err := store.CreateCatalogEntry(context.Background(), testCreateRequest()); err != nil {
					t.Fatal(err)
				}
			}
			mustAcquireClaim(t, store, testAcquireRequest(reconcileHolder))
			scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
			if err != nil {
				t.Fatal(err)
			}
			key := scope.SessionNamespace + "/protocol"
			_, revision, err := store.keys.kv.Get(context.Background(), key)
			if err != nil {
				t.Fatal(err)
			}
			other := ProtocolModeDisposition
			if mode == ProtocolModeDisposition {
				other = ProtocolModeLegacy
			}
			wrong := encodeWitness(1, scope.sessionWitness, []byte(other))
			if _, err := store.keys.kv.Put(context.Background(), key, revision, wrong); err != nil {
				t.Fatal(err)
			}
			before, _, err := store.readReconciliationClaim(context.Background(), scope, catalogTenant, catalogSession)
			if err != nil {
				t.Fatal(err)
			}
			for _, operation := range []func() error{
				func() error {
					_, err := store.AcquireReconciliationClaim(context.Background(), testAcquireRequest(reconcileHolder))
					return err
				},
				func() error {
					_, err := store.ReleaseReconciliationClaim(context.Background(), testReleaseRequest(reconcileHolder))
					return err
				},
			} {
				var conflict *CatalogError
				if err := operation(); !errors.As(err, &conflict) || conflict.Code != CatalogErrorConflict || conflict.Field != "binding.protocol_mode" {
					t.Fatalf("refusal = %v", err)
				}
			}
			after, _, err := store.readReconciliationClaim(context.Background(), scope, catalogTenant, catalogSession)
			if err != nil || after.Revision != before.Revision {
				t.Fatalf("refusal changed claim: %+v, %v", after, err)
			}
			witness, _ := protocolWitnessOf(t, store)
			if !bytes.Equal(witness, wrong) {
				t.Fatal("refusal repaired immutable witness")
			}
		})
	}
}

func TestReconciliationClaimWithoutCatalogPreservesLegacyFirstWrite(t *testing.T) {
	for _, mode := range []ProtocolMode{ProtocolModeLegacy, ProtocolModeDisposition} {
		t.Run(string(mode), func(t *testing.T) {
			store, _ := reconcileFixture(t, memstore.New())
			scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.bindSessionScopeMode(context.Background(), scope, mode); err != nil {
				t.Fatal(err)
			}
			before, _ := protocolWitnessOf(t, store)
			_, err = store.AcquireReconciliationClaim(context.Background(), testAcquireRequest(reconcileHolder))
			if mode == ProtocolModeLegacy {
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.ReleaseReconciliationClaim(context.Background(), testReleaseRequest(reconcileHolder)); err != nil {
					t.Fatal(err)
				}
			} else {
				var conflict *CatalogError
				if !errors.As(err, &conflict) || conflict.Code != CatalogErrorConflict {
					t.Fatalf("refusal = %v", err)
				}
				if _, found, err := store.readReconciliationClaim(context.Background(), scope, catalogTenant, catalogSession); err != nil || found {
					t.Fatalf("claim written: %v, %v", found, err)
				}
				// A raw remnant cannot turn a missing disposition catalog into
				// release authority either.
				value, claim, err := encodeReconciliationClaim(testReconciliationClaim())
				if err != nil {
					t.Fatal(err)
				}
				before, err := store.createReconciliationClaim(context.Background(), scope, claim, value)
				if err != nil {
					t.Fatal(err)
				}
				_, err = store.ReleaseReconciliationClaim(context.Background(), testReleaseRequest(reconcileHolder))
				if !errors.As(err, &conflict) || conflict.Code != CatalogErrorConflict {
					t.Fatalf("release refusal = %v", err)
				}
				after, _, err := store.readReconciliationClaim(context.Background(), scope, catalogTenant, catalogSession)
				if err != nil || after.Revision != before.Revision {
					t.Fatalf("refusal changed claim: %+v, %v", after, err)
				}
			}
			after, _ := protocolWitnessOf(t, store)
			if !bytes.Equal(before, after) {
				t.Fatal("protocol witness changed")
			}
		})
	}
}
