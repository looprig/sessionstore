package sessionstore

import (
	"context"
	"testing"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// This file pins v0.13.0's grant-authorized route publish.
//
// A registration's LeaseEpoch is a monotonic bound on every later writer of the
// row, unbounded above, that outlives the route that set it — the case this
// package's rule says must be store-issued. Until v0.13.0 PutHostRegistration
// took it only as a bare caller-named number. PutHostRegistrationRequest now
// carries an additive Residency *ResidencyGrant; the bare path is kept, because
// every released Host (≤ v0.5.0) publishes through it.

type registryGrantFixture struct {
	store        *Store
	older, newer *ResidencyGrant
}

func newRegistryGrantFixture(t *testing.T) registryGrantFixture {
	t.Helper()
	backend := memstore.New()
	backend.Leaser = newPermissiveLeaser()
	store := openStore(t, backend, WithClock(newMovableClock(registryObservedAt)))
	createDispositionCatalog(t, store)
	older := acquireTestResidency(t, store)
	newer := acquireTestResidency(t, store)
	if older.Epoch() >= newer.Epoch() {
		t.Fatalf("vacuous: grants %d and %d", older.Epoch(), newer.Epoch())
	}
	return registryGrantFixture{store: store, older: older, newer: newer}
}

func grantRegistrationRequest(grant *ResidencyGrant) PutHostRegistrationRequest {
	req := testPutRegistrationRequest(0)
	req.Residency = grant
	return req
}

func TestPutHostRegistrationUnderAGrantStoresTheGrantsEpoch(t *testing.T) {
	f := newRegistryGrantFixture(t)
	created, err := f.store.PutHostRegistration(context.Background(), grantRegistrationRequest(f.newer))
	if err != nil {
		t.Fatalf("PutHostRegistration under a grant: %v", err)
	}
	if created.Registration.LeaseEpoch != uint64(f.newer.Epoch()) {
		t.Fatalf("stored epoch %d, want the grant's %d", created.Registration.LeaseEpoch, f.newer.Epoch())
	}
	// One grant heartbeats many times.
	heartbeat, err := f.store.PutHostRegistration(context.Background(), grantRegistrationRequest(f.newer))
	if err != nil {
		t.Fatalf("heartbeat under the same grant: %v", err)
	}
	if heartbeat.Revision <= created.Revision {
		t.Fatalf("heartbeat wrote nothing: revision %d -> %d", created.Revision, heartbeat.Revision)
	}
	// A predecessor's grant is fenced, and so is a bare epoch below the mark.
	_, err = f.store.PutHostRegistration(context.Background(), grantRegistrationRequest(f.older))
	if got := assertRegistryCode(t, err, RegistryErrorEpoch); got.Epoch != uint64(f.newer.Epoch()) {
		t.Fatalf("refusal names epoch %d, want the committed %d", got.Epoch, f.newer.Epoch())
	}
	_, err = f.store.PutHostRegistration(context.Background(), testPutRegistrationRequest(uint64(f.older.Epoch())))
	assertRegistryCode(t, err, RegistryErrorEpoch)
}

// A grant cannot be combined with a named epoch, and only a grant this store
// issued for THIS session is accepted. Every refusal precedes any write.
func TestPutHostRegistrationRefusesAGrantItCannotVouchFor(t *testing.T) {
	f := newRegistryGrantFixture(t)
	other := openStore(t, func() *storage.Composite {
		backend := memstore.New()
		backend.Leaser = newPermissiveLeaser()
		return backend
	}(), WithClock(newMovableClock(registryObservedAt)))
	createDispositionCatalog(t, other)
	foreign := acquireTestResidency(t, other)

	bothSet := grantRegistrationRequest(f.newer)
	bothSet.LeaseEpoch = uint64(f.newer.Epoch())
	wrongSession := grantRegistrationRequest(f.newer)
	wrongSession.SessionID = "session-other"

	for _, tt := range []struct {
		name  string
		req   PutHostRegistrationRequest
		field string
	}{
		{"grant and a named epoch", bothSet, "lease_epoch"},
		{"a grant another store issued", grantRegistrationRequest(foreign), "residency"},
		{"a grant for another session", wrongSession, "residency"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := f.store.PutHostRegistration(context.Background(), tt.req)
			if got := assertRegistryCode(t, err, RegistryErrorInvalid); got.Field != tt.field {
				t.Fatalf("field = %q, want %q", got.Field, tt.field)
			}
		})
	}
	_, err := f.store.GetHostRegistration(context.Background(), GetHostRegistrationRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	})
	assertRegistryCode(t, err, RegistryErrorNotFound)
}
