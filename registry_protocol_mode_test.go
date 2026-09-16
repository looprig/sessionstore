package sessionstore

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// bareRegistryStore opens a store on which the fixture session has NO durable
// data, which registryFixture deliberately never produces.
func bareRegistryStore(t *testing.T, backend *storage.Composite) *Store {
	t.Helper()
	return openStore(t, backend, WithClock(newMovableClock(registryObservedAt)))
}

// protocolWitnessOf reads the session's protocol witness raw; absent is
// reported as found=false.
func protocolWitnessOf(t *testing.T, store *Store) (got []byte, found bool) {
	t.Helper()
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err = store.keys.kv.Get(context.Background(), scope.SessionNamespace+"/protocol")
	if err == nil {
		return got, true
	}
	if !isKeyNotFound(err, scope.SessionNamespace+"/protocol") {
		t.Fatalf("protocol witness read: %v", err)
	}
	return nil, false
}

// TestPutHostRegistrationServesADispositionSessionThroughItsLifecycle is the
// Host scenario that found B9, reproduced inside the module: a Host acquires
// residency over a disposition-bound session (the only shape AcquireResidency
// grants), publishes its route `attaching`, and then updates it to `resident`.
//
// Over v0.9.0 BOTH writes were refused with `catalog conflict
// (binding.protocol_mode)`: PutHostRegistration bound ProtocolModeLegacy through
// bindSessionScope before the create, and updateHostRegistration bound it again
// before every compare-and-swap. No session shape could be made resident — a
// disposition session failed here and a legacy one is refused by
// AcquireResidency. The registry is a route for whichever protocol the session
// was created under; it is not a legacy writer.
func TestPutHostRegistrationServesADispositionSessionThroughItsLifecycle(t *testing.T) {
	t.Parallel()

	store := bareRegistryStore(t, memstore.New())
	createDispositionCatalog(t, store)
	grant, err := store.AcquireResidency(context.Background(), AcquireResidencyRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	})
	if err != nil {
		t.Fatalf("AcquireResidency: %v", err)
	}
	t.Cleanup(func() { _ = grant.Release(context.Background()) })
	epoch := uint64(grant.Epoch())

	attaching := testPutRegistrationRequest(epoch)
	attaching.Route.Residency = sessionwire.SessionResidencyAttaching
	attaching.Route.Accepting = false
	created, err := store.PutHostRegistration(context.Background(), attaching)
	if err != nil {
		t.Fatalf("PutHostRegistration(attaching, create): %v", err)
	}

	resident := testPutRegistrationRequest(epoch)
	updated, err := store.PutHostRegistration(context.Background(), resident)
	if err != nil {
		t.Fatalf("PutHostRegistration(resident, update): %v", err)
	}
	if updated.Revision <= created.Revision {
		t.Fatalf("revision = %d, want an advance on %d", updated.Revision, created.Revision)
	}
	if updated.Registration.Route == nil || updated.Registration.Route.Residency != sessionwire.SessionResidencyResident {
		t.Fatalf("route = %+v, want resident", updated.Registration.Route)
	}

	got, err := store.GetHostRegistration(context.Background(), GetHostRegistrationRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	})
	if err != nil {
		t.Fatalf("GetHostRegistration: %v", err)
	}
	assertSameRegistration(t, got.Registration, updated.Registration)

	// The registry did not convert the session: the catalog still says
	// disposition and the store's own disposition operations still admit it.
	entry, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	if entry.Record.Binding.ProtocolMode != ProtocolModeDisposition {
		t.Fatalf("protocol mode = %q after registry writes, want disposition", entry.Record.Binding.ProtocolMode)
	}
	if _, err := store.LoadDispositionCommandCursor(context.Background(), testLoadCursorRequest()); err != nil {
		t.Fatalf("LoadDispositionCommandCursor after registry writes: %v", err)
	}

	// And the release path is neutral too: the tombstone is written under the
	// same fence without a mode of its own.
	cleared, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(epoch))
	if err != nil {
		t.Fatalf("ClearHostRegistration: %v", err)
	}
	if cleared.Registration.Route != nil {
		t.Fatalf("route after clear = %+v, want a tombstone", cleared.Registration.Route)
	}
}

// TestHostRegistrationWritesAreProtocolModeNeutral holds create, update and
// clear to one rule across both modes a created session can be bound to: the
// registry serves the session under the mode its catalog binds and leaves the
// protocol witness exactly as the catalog create left it. The two rows are the
// same test, which is the point — a registry that treated the modes
// differently would need two.
func TestHostRegistrationWritesAreProtocolModeNeutral(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		seed func(t *testing.T, store *Store)
		want ProtocolMode
	}{
		{
			name: "legacy catalog",
			seed: func(t *testing.T, store *Store) {
				if _, _, err := store.CreateCatalogEntry(context.Background(), testCreateRequest()); err != nil {
					t.Fatalf("CreateCatalogEntry: %v", err)
				}
			},
			want: ProtocolModeLegacy,
		},
		{
			name: "disposition catalog",
			seed: func(t *testing.T, store *Store) { createDispositionCatalog(t, store) },
			want: ProtocolModeDisposition,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := bareRegistryStore(t, memstore.New())
			tc.seed(t, store)
			scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
			if err != nil {
				t.Fatal(err)
			}
			want := encodeWitness(1, scope.sessionWitness, []byte(tc.want))
			if got, found := protocolWitnessOf(t, store); !found || !bytes.Equal(got, want) {
				t.Fatalf("protocol witness before registry writes = %q (found %v), want %q", got, found, want)
			}

			created := mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
			refresh := testPutRegistrationRequest(registryEpoch)
			refresh.ExpiresAt = registryExpiresAt.Add(time.Minute)
			updated := mustPutRegistration(t, store, refresh)
			if updated.Revision <= created.Revision {
				t.Fatalf("revision = %d, want an advance on %d", updated.Revision, created.Revision)
			}
			cleared, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryNextEpoch))
			if err != nil {
				t.Fatalf("ClearHostRegistration: %v", err)
			}
			if cleared.Registration.Route != nil {
				t.Fatalf("route after clear = %+v, want a tombstone", cleared.Registration.Route)
			}
			// A successor after a release is admitted in either mode too.
			mustPutRegistration(t, store, testPutRegistrationRequest(registryNextEpoch+1))

			if got, found := protocolWitnessOf(t, store); !found || !bytes.Equal(got, want) {
				t.Fatalf("protocol witness after registry writes = %q (found %v), want %q", got, found, want)
			}
		})
	}
}

// TestHostRegistrationRefusesASessionThatDoesNotExist pins the row the legacy
// bind was actually protecting, and pins it with a REFUSAL rather than a
// silently minted mode. A registration for a session nobody created used to
// bind ProtocolModeLegacy as a side effect of publishing a route: a value that
// then bounded every later creator of that session and had been asserted by no
// one. Now the registry requires the session to exist and refuses the rest with
// the same answers the reads give — no durable data at all is the keyspace's
// binding-not-found, exactly as GetHostRegistration reports it; a protocol
// witness with no catalog behind it (a create that crashed between the two) is
// the catalog's own not_found, in either layout. No refusal writes anything, so
// the session remains creatable in EITHER mode afterwards.
func TestHostRegistrationRefusesASessionThatDoesNotExist(t *testing.T) {
	t.Parallel()

	const legacyTenant = sessionwire.TenantID("local")
	const legacySession = sessionwire.SessionID("123e4567-e89b-12d3-a456-426614174000")

	type refusal struct {
		keyspace KeyspaceErrorCode
		catalog  CatalogErrorCode
	}
	assertRefusal := func(t *testing.T, err error, want refusal) {
		t.Helper()
		if want.keyspace != "" {
			var keyspace *KeyspaceError
			if !errors.As(err, &keyspace) || keyspace.Code != want.keyspace {
				t.Fatalf("err = %v, want keyspace %q", err, want.keyspace)
			}
			return
		}
		var catalog *CatalogError
		if !errors.As(err, &catalog) || catalog.Code != want.catalog {
			t.Fatalf("err = %v, want catalog %q", err, want.catalog)
		}
	}

	cases := []struct {
		name    string
		open    func(t *testing.T) *Store
		seed    func(t *testing.T, store *Store)
		tenant  sessionwire.TenantID
		session sessionwire.SessionID
		want    refusal
		// create is how the session is then created, proving nothing was
		// pinned by the refusal.
		create func(t *testing.T, store *Store)
	}{
		{
			name:    "no durable data, derived layout",
			open:    func(t *testing.T) *Store { return bareRegistryStore(t, memstore.New()) },
			seed:    func(*testing.T, *Store) {},
			tenant:  catalogTenant,
			session: catalogSession,
			want:    refusal{keyspace: KeyspaceBindingNotFound},
			create:  createDispositionCatalog,
		},
		{
			name: "disposition witness without a catalog",
			open: func(t *testing.T) *Store { return bareRegistryStore(t, memstore.New()) },
			seed: func(t *testing.T, store *Store) {
				scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.bindSessionScopeMode(context.Background(), scope, ProtocolModeDisposition); err != nil {
					t.Fatal(err)
				}
			},
			tenant:  catalogTenant,
			session: catalogSession,
			want:    refusal{catalog: CatalogErrorNotFound},
			create:  createDispositionCatalog,
		},
		{
			name: "legacy witness without a catalog",
			open: func(t *testing.T) *Store { return bareRegistryStore(t, memstore.New()) },
			seed: func(t *testing.T, store *Store) {
				scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.bindSessionScope(context.Background(), scope); err != nil {
					t.Fatal(err)
				}
			},
			tenant:  catalogTenant,
			session: catalogSession,
			want:    refusal{catalog: CatalogErrorNotFound},
			create: func(t *testing.T, store *Store) {
				if _, _, err := store.CreateCatalogEntry(context.Background(), testCreateRequest()); err != nil {
					t.Fatalf("CreateCatalogEntry: %v", err)
				}
			},
		},
		{
			name: "no catalog, legacy single-tenant layout",
			open: func(t *testing.T) *Store {
				return openStore(t, memstore.New(), WithClock(newMovableClock(registryObservedAt)), WithLegacySingleTenant(legacyTenant))
			},
			seed:    func(*testing.T, *Store) {},
			tenant:  legacyTenant,
			session: legacySession,
			want:    refusal{catalog: CatalogErrorNotFound},
			create: func(t *testing.T, store *Store) {
				req := testCreateRequest()
				req.TenantID, req.SessionID = legacyTenant, legacySession
				if _, _, err := store.CreateCatalogEntry(context.Background(), req); err != nil {
					t.Fatalf("CreateCatalogEntry: %v", err)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := tc.open(t)
			tc.seed(t, store)
			put := testPutRegistrationRequest(registryEpoch)
			put.TenantID, put.SessionID = tc.tenant, tc.session
			_, err := store.PutHostRegistration(context.Background(), put)
			assertRefusal(t, err, tc.want)

			scope, err := store.deriveSessionScope(tc.tenant, tc.session)
			if err != nil {
				t.Fatal(err)
			}
			if _, found, err := store.readHostRegistration(context.Background(), scope, tc.tenant, tc.session); found || (err != nil && tc.want.keyspace == "") {
				t.Fatalf("after the refusal readHostRegistration = found %v, err %v", found, err)
			}

			tc.create(t, store)
			mustPutRegistration(t, store, put)
		})
	}
}

// TestHostRegistrationRefusesACatalogWitnessDisagreement is the one case the
// witness re-fence exists for: the two authorities disagree. Pinning the wrong
// mode beside a catalog cannot happen through this package's own creators, so
// the state is built directly, and the answer is the catalog conflict a
// disposition writer gives the same disagreement — not a registry code, and not
// a silent success under either authority.
func TestHostRegistrationRefusesACatalogWitnessDisagreement(t *testing.T) {
	t.Parallel()

	store := bareRegistryStore(t, memstore.New())
	createDispositionCatalog(t, store)
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatal(err)
	}
	key := scope.SessionNamespace + "/protocol"
	_, revision, err := store.keys.kv.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("protocol witness read: %v", err)
	}
	if _, err := store.keys.kv.Put(context.Background(), key, revision, encodeWitness(1, scope.sessionWitness, []byte(ProtocolModeLegacy))); err != nil {
		t.Fatalf("repinning the witness: %v", err)
	}

	_, err = store.PutHostRegistration(context.Background(), testPutRegistrationRequest(registryEpoch))
	var catalog *CatalogError
	if !errors.As(err, &catalog) || catalog.Code != CatalogErrorConflict || catalog.Field != "binding.protocol_mode" {
		t.Fatalf("PutHostRegistration over disagreeing authorities = %v, want catalog conflict on binding.protocol_mode", err)
	}
	if _, found, err := store.readHostRegistration(context.Background(), scope, catalogTenant, catalogSession); err != nil || found {
		t.Fatalf("after the refusal readHostRegistration = found %v, err %v; want absent", found, err)
	}
}
