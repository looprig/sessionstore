package sessionstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// --- fixtures and assertions ----------------------------------------------

// The timeline every case in this file places itself on. A Host observes the
// session at 11:40 and publishes a route good for five minutes; 11:45 is the
// instant the route lapses and 11:50 is well past it.
var (
	registryObservedAt = time.Date(2026, 8, 30, 11, 40, 0, 0, time.UTC)
	registryExpiresAt  = time.Date(2026, 8, 30, 11, 45, 0, 0, time.UTC)
	registryLapsedAt   = time.Date(2026, 8, 30, 11, 50, 0, 0, time.UTC)
)

const (
	// The lease epoch a fixture registers under, one that has provably lost the
	// session, and one that supersedes it.
	registryEpoch      = uint64(5)
	registryStaleEpoch = uint64(3)
	registryNextEpoch  = uint64(7)

	registryHost       = sessionwire.HostID("host-a")
	registryGeneration = uint64(2)
	registryEndpoint   = sessionwire.InternalEndpoint("wss://host-a.internal:8443/hostlink")

	// A second session of the same tenant, for the checks that ask whether a
	// record was filed where the provider said it was.
	registryOtherSession = sessionwire.SessionID("session-b")
)

func testHostRoute() *HostRoute {
	return &HostRoute{
		HostID:                 registryHost,
		HostGeneration:         registryGeneration,
		AgentID:                "agent-a",
		RuntimeCompatibilityID: "runtime-v1",
		Placement:              sessionwire.HostPlacementPooled,
		InternalEndpoint:       registryEndpoint,
		Residency:              sessionwire.SessionResidencyResident,
		Accepting:              true,
	}
}

func testHostRegistration() HostRegistration {
	return HostRegistration{
		TenantID:   catalogTenant,
		SessionID:  catalogSession,
		LeaseEpoch: registryEpoch,
		ObservedAt: registryObservedAt,
		ExpiresAt:  registryExpiresAt,
		Route:      testHostRoute(),
	}
}

// --- the stored record's codec --------------------------------------------

func TestHostRegistrationRoundTripsThroughStoredBytes(t *testing.T) {
	t.Parallel()

	record := testHostRegistration()
	encoded, _, err := encodeHostRegistration(record)
	if err != nil {
		t.Fatalf("encodeHostRegistration: %v", err)
	}
	decoded, err := decodeHostRegistration(encoded)
	if err != nil {
		t.Fatalf("decodeHostRegistration: %v", err)
	}
	if decoded.Route == nil {
		t.Fatal("a live registration decoded without its route")
	}
	if *decoded.Route != *record.Route {
		t.Fatalf("route = %+v, want %+v", *decoded.Route, *record.Route)
	}
	decoded.Route, record.Route = nil, nil
	if decoded != record {
		t.Fatalf("registration = %+v, want %+v", decoded, record)
	}
}

// --- the routing decision --------------------------------------------------

// registryFixture opens a store whose clock starts at the instant the fixture
// Host makes its observation.
func registryFixture(t *testing.T, backend *storage.Composite) (*Store, *movableClock) {
	t.Helper()
	clock := newMovableClock(registryObservedAt)
	return openStore(t, backend, WithClock(clock)), clock
}

func testPutRegistrationRequest(epoch uint64) PutHostRegistrationRequest {
	return PutHostRegistrationRequest{
		TenantID:   catalogTenant,
		SessionID:  catalogSession,
		LeaseEpoch: epoch,
		ObservedAt: registryObservedAt,
		ExpiresAt:  registryExpiresAt,
		Route:      *testHostRoute(),
	}
}

func testClearRegistrationRequest(epoch uint64) ClearHostRegistrationRequest {
	return ClearHostRegistrationRequest{
		TenantID:   catalogTenant,
		SessionID:  catalogSession,
		LeaseEpoch: epoch,
	}
}

func mustPutRegistration(t *testing.T, store *Store, req PutHostRegistrationRequest) HostRegistrationEntry {
	t.Helper()
	entry, err := store.PutHostRegistration(context.Background(), req)
	if err != nil {
		t.Fatalf("PutHostRegistration: %v", err)
	}
	return entry
}

// assertSameRegistration compares two registrations by VALUE, route included.
// A bare == would compare the route POINTERS, so two decodings of one stored
// record would differ and the comparison would fail for a reason that has
// nothing to do with what was stored.
func assertSameRegistration(t *testing.T, got, want HostRegistration) {
	t.Helper()
	if (got.Route == nil) != (want.Route == nil) {
		t.Fatalf("route presence = %v, want %v", got.Route != nil, want.Route != nil)
	}
	if got.Route != nil && *got.Route != *want.Route {
		t.Fatalf("route = %+v, want %+v", *got.Route, *want.Route)
	}
	got.Route, want.Route = nil, nil
	if got != want {
		t.Fatalf("registration = %+v, want %+v", got, want)
	}
}

func assertRegistryCode(t *testing.T, err error, want RegistryErrorCode) *RegistryError {
	t.Helper()
	var got *RegistryError
	if !errors.As(err, &got) {
		t.Fatalf("error = %T %v, want *RegistryError", err, err)
	}
	if got.Code != want {
		t.Fatalf("registry code = %q, want %q (%v)", got.Code, want, err)
	}
	return got
}

// storedRegistration reads the record the provider actually holds, bypassing
// every rule GetHostRegistration applies. It is how a test asks whether an
// expired or released registration is still THERE — which the public reader,
// by design, cannot answer.
func storedRegistrationRow(t *testing.T, store *Store) storage.OrderedRecord {
	t.Helper()
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	stored, err := store.backend.OrderedIndex.Get(context.Background(), hostRegistrationID(scope, catalogSession))
	if err != nil {
		t.Fatalf("the provider holds no registration: %v", err)
	}
	return stored
}

func storedRegistration(t *testing.T, store *Store) HostRegistration {
	t.Helper()
	stored := storedRegistrationRow(t, store)
	record, err := decodeHostRegistration(stored.Value)
	if err != nil {
		t.Fatalf("decodeHostRegistration: %v", err)
	}
	return record
}

func TestPutHostRegistrationPublishesARouteAndRefreshesIt(t *testing.T) {
	t.Parallel()

	store, _ := registryFixture(t, memstore.New())
	created := mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
	if created.Registration.Route == nil || *created.Registration.Route != *testHostRoute() {
		t.Fatalf("route = %+v, want the published one", created.Registration.Route)
	}

	got, err := store.GetHostRegistration(context.Background(), GetHostRegistrationRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	})
	if err != nil {
		t.Fatalf("GetHostRegistration: %v", err)
	}
	assertSameRegistration(t, got.Registration, created.Registration)

	// A heartbeat under the same grant moves the expiry forward. One grant
	// writes many times, so the equal epoch must be admitted.
	refresh := testPutRegistrationRequest(registryEpoch)
	refresh.ObservedAt = registryObservedAt.Add(time.Minute)
	refresh.ExpiresAt = registryExpiresAt.Add(time.Minute)
	refreshed := mustPutRegistration(t, store, refresh)
	if refreshed.Revision <= created.Revision {
		t.Fatalf("revision = %d, want an advance on %d", refreshed.Revision, created.Revision)
	}
	if !refreshed.Registration.ExpiresAt.Equal(refresh.ExpiresAt) {
		t.Fatalf("expiry = %v, want %v", refreshed.Registration.ExpiresAt, refresh.ExpiresAt)
	}
}

// TestGetHostRegistrationSeparatesAbsentExpiredAndReleased pins the three ways
// a session can have no route. None of them is a route and none is a caller
// error; the distinction exists so a future writer cannot mistake "expired" for
// "nothing was ever here" and create over the fence.
func TestGetHostRegistrationSeparatesAbsentExpiredAndReleased(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(t *testing.T, store *Store, clock *movableClock)
		want  RegistryErrorCode
	}{
		{
			// A session that exists — its catalog record binds the collision
			// witnesses — and that no Host has ever registered. A session with
			// no durable data at all is a different answer, and
			// TestRegistryReadsRefuseAnUnboundSession covers it.
			name: "no host has ever registered",
			setup: func(t *testing.T, store *Store, _ *movableClock) {
				if _, _, err := store.CreateCatalogEntry(context.Background(), testCreateRequest()); err != nil {
					t.Fatalf("CreateCatalogEntry: %v", err)
				}
			},
			want: RegistryErrorNotFound,
		},
		{
			name: "the route lapsed",
			setup: func(t *testing.T, store *Store, clock *movableClock) {
				mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
				clock.set(registryLapsedAt)
			},
			want: RegistryErrorExpired,
		},
		{
			name: "the host released the session",
			setup: func(t *testing.T, store *Store, clock *movableClock) {
				mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
				if _, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryEpoch)); err != nil {
					t.Fatalf("ClearHostRegistration: %v", err)
				}
			},
			want: RegistryErrorReleased,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store, clock := registryFixture(t, memstore.New())
			test.setup(t, store, clock)
			_, err := store.GetHostRegistration(context.Background(), GetHostRegistrationRequest{
				TenantID: catalogTenant, SessionID: catalogSession,
			})
			assertRegistryCode(t, err, test.want)
		})
	}
}

// TestExpiryHidesTheRouteAndNotTheFence is the property the whole record is
// shaped around: reading as absent must not make the record absent, because the
// bytes a reader is refused are the bytes that refuse the next stale writer.
func TestExpiryHidesTheRouteAndNotTheFence(t *testing.T) {
	t.Parallel()

	store, clock := registryFixture(t, memstore.New())
	mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
	clock.set(registryLapsedAt)

	if _, err := store.GetHostRegistration(context.Background(), GetHostRegistrationRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	}); assertRegistryCode(t, err, RegistryErrorExpired) == nil {
		t.Fatal("unreachable")
	}
	if stored := storedRegistration(t, store); stored.LeaseEpoch != registryEpoch {
		t.Fatalf("stored epoch = %d, want the retained high-water %d", stored.LeaseEpoch, registryEpoch)
	}

	// The superseded Host wakes up after its registration lapsed. The record
	// reads as absent to a router and must still refuse it.
	stale := testPutRegistrationRequest(registryStaleEpoch)
	stale.ObservedAt, stale.ExpiresAt = registryLapsedAt, registryLapsedAt.Add(5*time.Minute)
	_, err := store.PutHostRegistration(context.Background(), stale)
	got := assertRegistryCode(t, err, RegistryErrorEpoch)
	if got.Epoch != registryEpoch {
		t.Fatalf("reported high-water = %d, want %d", got.Epoch, registryEpoch)
	}
}

func TestPutHostRegistrationRefusesAStaleEpochBeforeWriting(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = recorder
	store, _ := registryFixture(t, base)
	current := mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
	recorder.reset()

	_, err := store.PutHostRegistration(context.Background(), testPutRegistrationRequest(registryStaleEpoch))
	assertRegistryCode(t, err, RegistryErrorEpoch)
	if n := recorder.countOf("update"); n != 0 {
		t.Fatalf("a refused registration reached the provider with %d updates", n)
	}
	if n := recorder.countOf("create"); n != 0 {
		t.Fatalf("a refused registration reached the provider with %d creates", n)
	}
	if stored := storedRegistration(t, store); stored.Route.HostGeneration != current.Registration.Route.HostGeneration {
		t.Fatal("a refused registration changed the stored route")
	}
}

// --- cleanup ---------------------------------------------------------------

func TestClearHostRegistrationWritesAnExpiredTombstoneKeepingTheEpoch(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = recorder
	store, clock := registryFixture(t, base)
	mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
	clock.set(registryObservedAt.Add(time.Minute))

	cleared, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryEpoch))
	if err != nil {
		t.Fatalf("ClearHostRegistration: %v", err)
	}
	if cleared.Registration.Route != nil {
		t.Fatal("a tombstone kept a route")
	}
	if cleared.Registration.LeaseEpoch != registryEpoch {
		t.Fatalf("tombstone epoch = %d, want the retained high-water %d", cleared.Registration.LeaseEpoch, registryEpoch)
	}
	// Expired at the instant it was written, and at every instant after it.
	if !cleared.Registration.ExpiresAt.Equal(clock.Now()) {
		t.Fatalf("tombstone expiry = %v, want the write instant %v", cleared.Registration.ExpiresAt, clock.Now())
	}
	if stored := storedRegistration(t, store); stored.Route != nil || stored.LeaseEpoch != registryEpoch {
		t.Fatalf("stored tombstone = %+v", stored)
	}
	// The fencing high-water lives in the row. Deleting it would hand the next
	// superseded writer a clean slate.
	if n := recorder.countOf("delete"); n != 0 {
		t.Fatalf("cleanup deleted the provider record %d times; the tombstone IS the fence", n)
	}
}

func TestClearHostRegistrationIsIdempotentAndStillRaisesTheHighWater(t *testing.T) {
	t.Parallel()

	store, _ := registryFixture(t, memstore.New())
	mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
	first, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryEpoch))
	if err != nil {
		t.Fatalf("ClearHostRegistration: %v", err)
	}

	// A retry under the same grant is the same answer, and writes nothing.
	again, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryEpoch))
	if err != nil {
		t.Fatalf("ClearHostRegistration retry: %v", err)
	}
	if again.Revision != first.Revision {
		t.Fatalf("a repeated cleanup rewrote the tombstone: revision %d then %d", first.Revision, again.Revision)
	}

	// A LATER grant releasing the same session is not a repeat. It must raise
	// the fence, or an epoch between the two could still write.
	raised, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryNextEpoch))
	if err != nil {
		t.Fatalf("ClearHostRegistration at a later epoch: %v", err)
	}
	if raised.Registration.LeaseEpoch != registryNextEpoch {
		t.Fatalf("high-water = %d, want %d", raised.Registration.LeaseEpoch, registryNextEpoch)
	}
	between := testPutRegistrationRequest(registryEpoch + 1)
	_, err = store.PutHostRegistration(context.Background(), between)
	if got := assertRegistryCode(t, err, RegistryErrorEpoch); got.Epoch != registryNextEpoch {
		t.Fatalf("reported high-water = %d, want %d", got.Epoch, registryNextEpoch)
	}
}

func TestClearHostRegistrationRefusesAStaleEpoch(t *testing.T) {
	t.Parallel()

	store, _ := registryFixture(t, memstore.New())
	live := mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))

	_, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryStaleEpoch))
	if got := assertRegistryCode(t, err, RegistryErrorEpoch); got.Epoch != registryEpoch {
		t.Fatalf("reported high-water = %d, want %d", got.Epoch, registryEpoch)
	}
	if stored := storedRegistration(t, store); stored.Route == nil {
		t.Fatal("a superseded lease released a session it no longer owns")
	}
	survived, err := store.GetHostRegistration(context.Background(), GetHostRegistrationRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	})
	if err != nil {
		t.Fatalf("the live route did not survive a refused cleanup: %v", err)
	}
	assertSameRegistration(t, survived.Registration, live.Registration)

	// The same refusal once the session has been released. Cleanup is
	// idempotent under ITS OWN grant, and a superseded lease meeting a
	// tombstone must still be told it was superseded rather than be handed the
	// success a repeat would get. That is why the fence precedes the repeat
	// check rather than following it.
	released, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryEpoch))
	if err != nil {
		t.Fatalf("ClearHostRegistration: %v", err)
	}
	_, err = store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryStaleEpoch))
	if got := assertRegistryCode(t, err, RegistryErrorEpoch); got.Epoch != registryEpoch {
		t.Fatalf("reported high-water = %d, want %d", got.Epoch, registryEpoch)
	}
	if after := storedRegistration(t, store); !after.ObservedAt.Equal(released.Registration.ObservedAt) {
		t.Fatal("a superseded cleanup rewrote the tombstone it was refused")
	}
}

// TestReleasedRegistrationAcceptsTheNextHostAndRefusesTheOldOne covers the
// reason the tombstone is retained rather than deleted: the session must be
// registrable again, but only from a lease that has not been superseded.
func TestReleasedRegistrationAcceptsTheNextHostAndRefusesTheOldOne(t *testing.T) {
	t.Parallel()

	store, clock := registryFixture(t, memstore.New())
	mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
	if _, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryEpoch)); err != nil {
		t.Fatalf("ClearHostRegistration: %v", err)
	}

	_, err := store.PutHostRegistration(context.Background(), testPutRegistrationRequest(registryStaleEpoch))
	assertRegistryCode(t, err, RegistryErrorEpoch)

	clock.set(registryObservedAt.Add(time.Minute))
	next := testPutRegistrationRequest(registryNextEpoch)
	next.ObservedAt = registryObservedAt.Add(time.Minute)
	next.ExpiresAt = registryExpiresAt.Add(time.Minute)
	successor := mustPutRegistration(t, store, next)
	if successor.Registration.Route == nil {
		t.Fatal("the successor could not re-register the session")
	}
	if _, err := store.GetHostRegistration(context.Background(), GetHostRegistrationRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	}); err != nil {
		t.Fatalf("the successor's route is not readable: %v", err)
	}
}

// TestRegistryReadsRefuseAnUnboundSession pins the boundary the case above
// leaves open. A session with no durable data at all has no collision witness
// to prove its derived name is really its own, so every read of it fails closed
// as a keyspace failure rather than as "this session has no route" — the same
// answer GetCatalogEntry gives, and deliberately not a registry code, because
// the question was never reached.
func TestRegistryReadsRefuseAnUnboundSession(t *testing.T) {
	t.Parallel()

	store, _ := registryFixture(t, memstore.New())
	tests := map[string]func() error{
		"GetHostRegistration": func() error {
			_, err := store.GetHostRegistration(context.Background(), GetHostRegistrationRequest{
				TenantID: catalogTenant, SessionID: catalogSession,
			})
			return err
		},
		"ClearHostRegistration": func() error {
			_, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryEpoch))
			return err
		},
	}
	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			var keyspace *KeyspaceError
			if err := call(); !errors.As(err, &keyspace) || keyspace.Code != KeyspaceBindingNotFound {
				t.Fatalf("%s on an unbound session = %v, want a keyspace binding failure", name, err)
			}
		})
	}
}

// --- what a stored registration may say ------------------------------------

// TestHostRegistrationRefusesAnInvalidMember covers the record's own rules and
// the rules it borrows from Core. The two are deliberately not separated in the
// table: a caller cannot tell which validator refused its route, and the point
// of routing the live shape through Observation is that there is only one
// answer to give.
func TestHostRegistrationRefusesAnInvalidMember(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*HostRegistration)
		field  string
	}{
		{name: "no tenant", mutate: func(r *HostRegistration) { r.TenantID = "" }, field: "tenant_id"},
		{name: "no session", mutate: func(r *HostRegistration) { r.SessionID = "" }, field: "session_id"},
		{name: "no epoch", mutate: func(r *HostRegistration) { r.LeaseEpoch = 0 }, field: "lease_epoch"},
		{
			name:   "an unrepresentable observation",
			mutate: func(r *HostRegistration) { r.ObservedAt = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC) },
			field:  "observed_at",
		},
		{
			name:   "an unrepresentable expiry",
			mutate: func(r *HostRegistration) { r.ExpiresAt = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC) },
			field:  "expires_at",
		},
		{
			name:   "an expiry at the observation",
			mutate: func(r *HostRegistration) { r.ExpiresAt = r.ObservedAt },
			field:  "expires_at",
		},
		{
			name:   "an expiry before the observation",
			mutate: func(r *HostRegistration) { r.ExpiresAt = r.ObservedAt.Add(-time.Second) },
			field:  "expires_at",
		},
		{name: "no host", mutate: func(r *HostRegistration) { r.Route.HostID = "" }, field: "host_id"},
		{
			name:   "no host generation",
			mutate: func(r *HostRegistration) { r.Route.HostGeneration = 0 },
			field:  "host_generation",
		},
		{name: "no agent", mutate: func(r *HostRegistration) { r.Route.AgentID = "" }, field: "agent_id"},
		{
			name:   "no runtime compatibility",
			mutate: func(r *HostRegistration) { r.Route.RuntimeCompatibilityID = "" },
			field:  "runtime_compatibility_id",
		},
		{
			name:   "an unknown placement",
			mutate: func(r *HostRegistration) { r.Route.Placement = "spot" },
			field:  "placement",
		},
		{
			name:   "a cold residency, which is what having no route means",
			mutate: func(r *HostRegistration) { r.Route.Residency = sessionwire.SessionResidencyCold },
			field:  "residency",
		},
		{
			name:   "an endpoint carrying credentials",
			mutate: func(r *HostRegistration) { r.Route.InternalEndpoint = "wss://user:pass@host-a.internal/hostlink" },
			field:  "internal_endpoint",
		},
		{
			name:   "an endpoint that is not a websocket",
			mutate: func(r *HostRegistration) { r.Route.InternalEndpoint = "https://host-a.internal/hostlink" },
			field:  "internal_endpoint",
		},
		{
			name:   "an endpoint carrying a signed query",
			mutate: func(r *HostRegistration) { r.Route.InternalEndpoint = "wss://host-a.internal/hostlink?sig=abc" },
			field:  "internal_endpoint",
		},
		{
			name:   "invalid UTF-8 in an opaque member",
			mutate: func(r *HostRegistration) { r.Route.RuntimeCompatibilityID = "runtime-\xff" },
			field:  "runtime_compatibility_id",
		},
		{
			name:   "a tombstone that had not expired when it was written",
			mutate: func(r *HostRegistration) { r.Route = nil },
			field:  "expires_at",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			record := testHostRegistration()
			test.mutate(&record)
			_, _, err := encodeHostRegistration(record)
			got := assertRegistryCode(t, err, RegistryErrorInvalid)
			if got.Field != test.field {
				t.Fatalf("field = %q, want %q (%v)", got.Field, test.field, err)
			}
		})
	}
}

func TestHostRegistrationTombstoneRoundTrips(t *testing.T) {
	t.Parallel()

	tombstone := testHostRegistration()
	tombstone.Route = nil
	tombstone.ExpiresAt = tombstone.ObservedAt

	encoded, _, err := encodeHostRegistration(tombstone)
	if err != nil {
		t.Fatalf("encodeHostRegistration: %v", err)
	}
	if bytes.Contains(encoded, []byte("route")) {
		t.Fatalf("a tombstone's bytes carry a route member: %s", encoded)
	}
	decoded, err := decodeHostRegistration(encoded)
	if err != nil {
		t.Fatalf("decodeHostRegistration: %v", err)
	}
	assertSameRegistration(t, decoded, tombstone)
	if decoded.LeaseEpoch != registryEpoch {
		t.Fatalf("tombstone epoch = %d, want the fence it retains %d", decoded.LeaseEpoch, registryEpoch)
	}
}

// TestReleasedRegistrationIsUnroutableUnderAnyClock is what a nil route buys
// over an expired timestamp. The two rules would agree for every tombstone this
// package can write, so the case is built in memory: a released registration
// whose expiry has not passed is refused on its STRUCTURE, before an instant is
// compared at all.
func TestReleasedRegistrationIsUnroutableUnderAnyClock(t *testing.T) {
	t.Parallel()

	tombstone := HostRegistration{
		TenantID:   catalogTenant,
		SessionID:  catalogSession,
		LeaseEpoch: registryEpoch,
		ObservedAt: registryObservedAt,
		ExpiresAt:  registryLapsedAt,
	}
	if err := routableAt(tombstone, registryObservedAt); err == nil {
		t.Fatal("a tombstone whose expiry has not passed was reported as a route")
	} else {
		assertRegistryCode(t, err, RegistryErrorReleased)
	}
}

// TestRouteLivenessIsHalfOpen pins the expiry instant itself, which is the only
// place an off-by-one in the interval is observable and the place the
// convention has to agree with every other deadline in this package.
func TestRouteLivenessIsHalfOpen(t *testing.T) {
	t.Parallel()

	record := testHostRegistration()
	if err := routableAt(record, record.ExpiresAt.Add(-time.Nanosecond)); err != nil {
		t.Fatalf("a route one nanosecond before its expiry was refused: %v", err)
	}
	if err := routableAt(record, record.ExpiresAt); err == nil {
		t.Fatal("a route at its expiry instant was still a route")
	} else {
		assertRegistryCode(t, err, RegistryErrorExpired)
	}
}

func TestHostRegistrationObservationProjectsTheTuple(t *testing.T) {
	t.Parallel()

	record := testHostRegistration()
	observation, err := record.Observation()
	if err != nil {
		t.Fatalf("Observation: %v", err)
	}
	want := sessionwire.HostLinkRegistryObservation{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               catalogTenant,
		SessionID:              catalogSession,
		HostID:                 registryHost,
		HostGeneration:         registryGeneration,
		AgentID:                "agent-a",
		RuntimeCompatibilityID: "runtime-v1",
		Placement:              sessionwire.HostPlacementPooled,
		InternalEndpoint:       registryEndpoint,
		Residency:              sessionwire.SessionResidencyResident,
		Accepting:              true,
		LeaseEpoch:             registryEpoch,
		ObservedAt:             registryObservedAt,
		ExpiresAt:              registryExpiresAt,
	}
	if observation != want {
		t.Fatalf("observation = %+v, want %+v", observation, want)
	}
	// The projection is Core's record, so it must survive Core's own codec.
	encoded, err := observation.MarshalJSON()
	if err != nil {
		t.Fatalf("the projection does not marshal: %v", err)
	}
	var again sessionwire.HostLinkRegistryObservation
	if err := again.UnmarshalJSON(encoded); err != nil {
		t.Fatalf("the projection does not round-trip: %v", err)
	}
	if again != observation {
		t.Fatalf("round-tripped %+v, want %+v", again, observation)
	}

	tombstone := record
	tombstone.Route = nil
	if _, err := tombstone.Observation(); err == nil {
		t.Fatal("a tombstone projected into a route")
	} else {
		assertRegistryCode(t, err, RegistryErrorReleased)
	}
}

// TestLargestAcceptableRegistrationFitsTheBound is why encodeHostRegistration's
// size refusal is a relationship rather than a live branch. Every member of
// this record is bounded by Core's identity ceiling, so the largest record the
// validators accept is a small multiple of it — and a record this package
// accepts but cannot rewrite is the state the bound exists to prevent.
func TestLargestAcceptableRegistrationFitsTheBound(t *testing.T) {
	t.Parallel()

	fill := func(prefix string) string {
		return prefix + strings.Repeat("x", sessionwire.MaxIDBytes-len(prefix))
	}
	largest := HostRegistration{
		TenantID:   sessionwire.TenantID(fill("tenant-")),
		SessionID:  sessionwire.SessionID(fill("session-")),
		LeaseEpoch: math.MaxUint64,
		ObservedAt: registryObservedAt,
		ExpiresAt:  registryExpiresAt,
		Route: &HostRoute{
			HostID:                 sessionwire.HostID(fill("host-")),
			HostGeneration:         math.MaxUint64,
			AgentID:                sessionwire.AgentID(fill("agent-")),
			RuntimeCompatibilityID: fill("runtime-"),
			Placement:              sessionwire.HostPlacementDedicated,
			InternalEndpoint:       sessionwire.InternalEndpoint(fill("wss://h.internal/")),
			Residency:              sessionwire.SessionResidencyAttaching,
			Accepting:              true,
		},
	}
	encoded, _, err := encodeHostRegistration(largest)
	if err != nil {
		t.Fatalf("the largest acceptable registration does not encode: %v", err)
	}
	if len(encoded) >= MaxHostRegistrationRecordBytes {
		t.Fatalf("the largest acceptable registration is %d bytes, at or above the %d-byte bound",
			len(encoded), MaxHostRegistrationRecordBytes)
	}
	if _, err := decodeHostRegistration(encoded); err != nil {
		t.Fatalf("the largest acceptable registration does not decode: %v", err)
	}
}

func TestDecodeHostRegistrationFailsClosed(t *testing.T) {
	t.Parallel()

	valid, _, err := encodeHostRegistration(testHostRegistration())
	if err != nil {
		t.Fatalf("encodeHostRegistration: %v", err)
	}
	rewrite := func(t *testing.T, mutate func(map[string]json.RawMessage)) []byte {
		t.Helper()
		var members map[string]json.RawMessage
		if err := json.Unmarshal(valid, &members); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		mutate(members)
		encoded, err := json.Marshal(members)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return encoded
	}

	tests := []struct {
		name  string
		value func(t *testing.T) []byte
		want  RegistryErrorCode
	}{
		{
			name:  "no bytes at all",
			value: func(*testing.T) []byte { return nil },
			want:  RegistryErrorMalformed,
		},
		{
			name:  "not JSON",
			value: func(*testing.T) []byte { return []byte("{") },
			want:  RegistryErrorMalformed,
		},
		{
			name: "a second JSON document appended",
			value: func(*testing.T) []byte {
				return append(append([]byte(nil), valid...), []byte("{}")...)
			},
			want: RegistryErrorMalformed,
		},
		{
			name: "a record version this reader does not know",
			value: func(t *testing.T) []byte {
				return rewrite(t, func(m map[string]json.RawMessage) { m["record_version"] = json.RawMessage("2") })
			},
			want: RegistryErrorVersion,
		},
		{
			name: "an undeclared member of the record",
			value: func(t *testing.T) []byte {
				return rewrite(t, func(m map[string]json.RawMessage) { m["drain_state"] = json.RawMessage(`"x"`) })
			},
			want: RegistryErrorMalformed,
		},
		{
			name: "an undeclared member of the route",
			value: func(t *testing.T) []byte {
				return rewrite(t, func(m map[string]json.RawMessage) {
					m["route"] = json.RawMessage(`{"host_id":"host-a","surprise":1}`)
				})
			},
			want: RegistryErrorMalformed,
		},
		{
			name: "a record above the bound",
			value: func(t *testing.T) []byte {
				return rewrite(t, func(m map[string]json.RawMessage) {
					m["pad"] = json.RawMessage(`"` + strings.Repeat("p", MaxHostRegistrationRecordBytes) + `"`)
				})
			},
			want: RegistryErrorTooLarge,
		},
		{
			name: "a stored record that no longer satisfies its own rules",
			value: func(t *testing.T) []byte {
				return rewrite(t, func(m map[string]json.RawMessage) { m["lease_epoch"] = json.RawMessage("0") })
			},
			want: RegistryErrorInvalid,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := decodeHostRegistration(test.value(t))
			assertRegistryCode(t, err, test.want)
		})
	}
}

// --- the request boundary ---------------------------------------------------

// TestPutHostRegistrationBoundsTheExpiry covers the two relations a stored
// record cannot express, both of them between the caller's expiry and the
// store's clock.
func TestPutHostRegistrationBoundsTheExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		expiresAt time.Time
	}{
		{name: "already lapsed", expiresAt: registryObservedAt.Add(-time.Second)},
		{name: "lapsing at this instant", expiresAt: registryObservedAt},
		{
			name:      "outliving the ceiling by a nanosecond",
			expiresAt: registryObservedAt.Add(MaxHostRegistrationTTL + time.Nanosecond),
		},
		{name: "outliving the ceiling by centuries", expiresAt: maxRankableTime},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			base := memstore.New()
			recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
			base.OrderedIndex = recorder
			store, _ := registryFixture(t, base)

			req := testPutRegistrationRequest(registryEpoch)
			req.ObservedAt = registryObservedAt.Add(-time.Hour)
			req.ExpiresAt = test.expiresAt
			_, err := store.PutHostRegistration(context.Background(), req)
			got := assertRegistryCode(t, err, RegistryErrorInvalid)
			if got.Field != "expires_at" {
				t.Fatalf("field = %q, want %q", got.Field, "expires_at")
			}
			if calls := recorder.snapshot(); len(calls) != 0 {
				t.Fatalf("a refused registration reached the provider: %+v", calls)
			}
		})
	}

	t.Run("at the ceiling exactly", func(t *testing.T) {
		t.Parallel()

		store, _ := registryFixture(t, memstore.New())
		req := testPutRegistrationRequest(registryEpoch)
		req.ExpiresAt = registryObservedAt.Add(MaxHostRegistrationTTL)
		if _, err := store.PutHostRegistration(context.Background(), req); err != nil {
			t.Fatalf("an expiry exactly at the ceiling was refused: %v", err)
		}
	})
}

// TestRegistryOperationsValidateBeforeAdmission holds every public registry
// operation to reporting the CALLER's mistake rather than the store's state.
// The order — validate, then admit, then bind, then the provider — is the
// package's, and an operation added without a row here silently stops obeying
// it.
func TestRegistryOperationsValidateBeforeAdmission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		call      func(*Store) error
		assert    func(*testing.T, error)
	}{
		{
			name:      "publish without a lease epoch",
			operation: "PutHostRegistration",
			call: func(store *Store) error {
				req := testPutRegistrationRequest(0)
				_, err := store.PutHostRegistration(context.Background(), req)
				return err
			},
			assert: assertRegistryField(RegistryErrorInvalid, "lease_epoch"),
		},
		{
			name:      "publish an endpoint carrying credentials",
			operation: "PutHostRegistration",
			call: func(store *Store) error {
				req := testPutRegistrationRequest(registryEpoch)
				req.Route.InternalEndpoint = "wss://user:pass@host-a.internal/hostlink"
				_, err := store.PutHostRegistration(context.Background(), req)
				return err
			},
			assert: assertRegistryField(RegistryErrorInvalid, "internal_endpoint"),
		},
		{
			name:      "publish an expiry that has already lapsed",
			operation: "PutHostRegistration",
			call: func(store *Store) error {
				req := testPutRegistrationRequest(registryEpoch)
				req.ExpiresAt = registryObservedAt.Add(-time.Second)
				_, err := store.PutHostRegistration(context.Background(), req)
				return err
			},
			assert: assertRegistryField(RegistryErrorInvalid, "expires_at"),
		},
		{
			name:      "read a session that is not named",
			operation: "GetHostRegistration",
			call: func(store *Store) error {
				_, err := store.GetHostRegistration(context.Background(), GetHostRegistrationRequest{
					TenantID: catalogTenant,
				})
				return err
			},
			assert: assertInvalidIdentity("SessionID"),
		},
		{
			name:      "release without a lease epoch",
			operation: "ClearHostRegistration",
			call: func(store *Store) error {
				_, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(0))
				return err
			},
			assert: assertRegistryField(RegistryErrorInvalid, "lease_epoch"),
		},
	}

	// Held to covering every public operation registry.go declares, for the
	// reason the inbox's table is: an operation added without a row silently
	// stops being held to validating its request before the store admits it,
	// and the cost — an invalid request answered with "the store is closing" —
	// is invisible until a caller meets it.
	declared := declaredStoreOperations(t, "registry.go")
	covered := map[string]bool{}
	for _, test := range tests {
		if !declared[test.operation] {
			t.Errorf("the case %q drives %s, which registry.go does not declare", test.name, test.operation)
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

			base := memstore.New()
			recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
			base.OrderedIndex = recorder
			store, _ := registryFixture(t, base)
			test.assert(t, test.call(store))
			if calls := recorder.snapshot(); len(calls) != 0 {
				t.Fatalf("a refused request reached the provider: %+v", calls)
			}

			// The same request against a store that is closing.
			closing, err := Open(context.Background(), memstore.New(), WithClock(newMovableClock(registryObservedAt)))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := closing.Close(context.Background()); err != nil {
				t.Fatalf("Close: %v", err)
			}
			err = test.call(closing)
			if errors.As(err, new(*StoreClosedError)) {
				t.Fatalf("an invalid request on a closing store reported the store's state: %v", err)
			}
			test.assert(t, err)
		})
	}
}

func assertRegistryField(code RegistryErrorCode, field string) func(*testing.T, error) {
	return func(t *testing.T, err error) {
		t.Helper()
		got := assertRegistryCode(t, err, code)
		if got.Field != field {
			t.Fatalf("field = %q, want %q (%v)", got.Field, field, err)
		}
	}
}

func assertInvalidIdentity(field string) func(*testing.T, error) {
	return func(t *testing.T, err error) {
		t.Helper()
		var identity *InvalidIdentityError
		if !errors.As(err, &identity) {
			t.Fatalf("error = %T %v, want *InvalidIdentityError", err, err)
		}
		if identity.Field != field {
			t.Fatalf("identity field = %q, want %q", identity.Field, field)
		}
	}
}

func TestRegistryOperationsRefuseAfterClose(t *testing.T) {
	store, _ := registryFixture(t, memstore.New())
	mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	operations := map[string]func() error{
		"PutHostRegistration": func() error {
			_, err := store.PutHostRegistration(context.Background(), testPutRegistrationRequest(registryEpoch))
			return err
		},
		"GetHostRegistration": func() error {
			_, err := store.GetHostRegistration(context.Background(), GetHostRegistrationRequest{
				TenantID: catalogTenant, SessionID: catalogSession,
			})
			return err
		},
		"ClearHostRegistration": func() error {
			_, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryEpoch))
			return err
		},
	}

	declared := declaredStoreOperations(t, "registry.go")
	for name := range declared {
		if operations[name] == nil {
			t.Errorf("registry.go declares the public operation %s and this test does not exercise it", name)
		}
	}
	for name := range operations {
		if !declared[name] {
			t.Errorf("this test exercises %s, which registry.go no longer declares (was it moved?)", name)
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

// --- what the provider says about its own filing ---------------------------

// TestHostRegistrationFilingIsHeldToTheRecord drives every component of a
// stored row's filing that this package checks. The rows come from a real
// write, so each case perturbs exactly one component of something a conforming
// provider produced.
func TestHostRegistrationFilingIsHeldToTheRecord(t *testing.T) {
	t.Parallel()

	store, _ := registryFixture(t, memstore.New())
	mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	conforming, err := store.backend.OrderedIndex.Get(context.Background(), hostRegistrationID(scope, catalogSession))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	otherSession, _, err := encodeHostRegistration(func() HostRegistration {
		record := testHostRegistration()
		record.SessionID = registryOtherSession
		return record
	}())
	if err != nil {
		t.Fatalf("encodeHostRegistration: %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*storage.OrderedRecord)
		want    RegistryErrorCode
		field   string
		covered string
	}{
		{
			name:    "a provider tombstone, which would be the fence destroyed",
			mutate:  func(r *storage.OrderedRecord) { r.Deleted = true },
			want:    RegistryErrorDeleted,
			field:   "record",
			covered: "Deleted",
		},
		{
			name:    "another session's record under this key",
			mutate:  func(r *storage.OrderedRecord) { r.Value = otherSession },
			want:    RegistryErrorIdentity,
			field:   "record",
			covered: "Value",
		},
		{
			name:    "filed under a stable key that is not the session",
			mutate:  func(r *storage.OrderedRecord) { r.ID.StableKey = storage.StableKey(registryOtherSession) },
			want:    RegistryErrorIdentity,
			field:   "session_id",
			covered: "ID",
		},
		{
			name:    "filed in another session's ordering scope",
			mutate:  func(r *storage.OrderedRecord) { r.ID.OrderingScope += "/elsewhere" },
			want:    RegistryErrorIdentity,
			field:   "ordering_scope",
			covered: "ID",
		},
		{
			name:    "ranked in another session's scope",
			mutate:  func(r *storage.OrderedRecord) { r.RankingScope += "/elsewhere" },
			want:    RegistryErrorIdentity,
			field:   "ranking_scope",
			covered: "RankingScope",
		},
		{
			name:    "filed into a deadline page nothing sweeps",
			mutate:  func(r *storage.OrderedRecord) { r.Due = storage.Due{State: storage.DueAt, UnixMillis: 1} },
			want:    RegistryErrorIdentity,
			field:   "due",
			covered: "Due",
		},
		{
			name:    "filed into a ranked view with no consumer",
			mutate:  func(r *storage.OrderedRecord) { r.Rank = storage.Rank{Ranked: true, Value: 1} },
			want:    RegistryErrorIdentity,
			field:   "rank",
			covered: "Rank",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			stored := conforming
			test.mutate(&stored)
			_, err := hostRegistrationEntryFor(stored, scope, catalogTenant, catalogSession)
			got := assertRegistryCode(t, err, test.want)
			if got.Field != test.field {
				t.Fatalf("field = %q, want %q (%v)", got.Field, test.field, err)
			}
		})
	}

	// The unperturbed row must pass, or every case above could be passing for
	// a reason that has nothing to do with the component it perturbs.
	if _, err := hostRegistrationEntryFor(conforming, scope, catalogTenant, catalogSession); err != nil {
		t.Fatalf("a conforming row was rejected: %v", err)
	}

	// The cross product, rather than a hand-written list standing in for one:
	// every member of the provider's record is either perturbed by a case above
	// or named here as deliberately unchecked, with the reason on
	// hostRegistrationEntryFor. A member Storage adds later belongs to one of
	// the two groups and this fails until someone decides which.
	excluded := map[string]string{
		"Revision": "provider state with no counterpart in the record",
		"Order":    "not exposed, never listed, and no counterpart in the record",
	}
	perturbed := map[string]bool{}
	for _, test := range tests {
		perturbed[test.covered] = true
	}
	recordType := reflect.TypeOf(storage.OrderedRecord{})
	for i := range recordType.NumField() {
		name := recordType.Field(i).Name
		if perturbed[name] == (excluded[name] != "") {
			t.Errorf("storage.OrderedRecord.%s is %s; it must be exactly one of perturbed here or excluded with a reason",
				name, map[bool]string{true: "both perturbed and excluded", false: "neither perturbed nor excluded"}[perturbed[name]])
		}
	}
}

// TestPutHostRegistrationChecksTheProvidersReply reaches the filing checks
// through a real write, which is the only way to prove they are WIRED into the
// write path rather than merely correct in isolation.
func TestPutHostRegistrationChecksTheProvidersReply(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store, _ := registryFixture(t, base)

	hostile.refileCreates(func(record storage.OrderedRecord) storage.OrderedRecord {
		record.ID.OrderingScope += "/elsewhere"
		return record
	})
	_, err := store.PutHostRegistration(context.Background(), testPutRegistrationRequest(registryEpoch))
	got := assertRegistryCode(t, err, RegistryErrorIdentity)
	if got.Field != "ordering_scope" {
		t.Fatalf("field = %q, want %q", got.Field, "ordering_scope")
	}
}

// TestPutHostRegistrationRefusesAReplyThatIsNotTheBytesItWrote covers the one
// claim a create makes that no record-derived check can test: that the provider
// stored THESE bytes. A substituted record satisfies every identity check and
// would become the fence the next write is measured against.
func TestPutHostRegistrationRefusesAReplyThatIsNotTheBytesItWrote(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store, _ := registryFixture(t, base)

	substitute := testHostRegistration()
	substitute.LeaseEpoch = registryStaleEpoch
	value, _, err := encodeHostRegistration(substitute)
	if err != nil {
		t.Fatalf("encodeHostRegistration: %v", err)
	}
	hostile.refileCreates(func(record storage.OrderedRecord) storage.OrderedRecord {
		record.Value = value
		return record
	})
	_, err = store.PutHostRegistration(context.Background(), testPutRegistrationRequest(registryEpoch))
	got := assertRegistryCode(t, err, RegistryErrorIdentity)
	if got.Field != "value" {
		t.Fatalf("field = %q, want %q", got.Field, "value")
	}
}

// TestRegistryReadsDoNotTreatAFailureAsAbsence is the destructive-absence rule
// for the reader that licenses a create. A stored record this package cannot
// read is a fence it cannot evaluate, and publishing over it would reset the
// high-water mark to whatever the publisher named.
func TestRegistryReadsDoNotTreatAFailureAsAbsence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arm  func(*hostileOrdered)
		want RegistryErrorCode
	}{
		{
			name: "a record this reader cannot decode",
			arm: func(hostile *hostileOrdered) {
				hostile.corruptGets(func([]byte) []byte { return []byte("{") })
			},
			want: RegistryErrorMalformed,
		},
		{
			name: "a provider that cannot answer at all",
			arm: func(hostile *hostileOrdered) {
				hostile.failGetsIn(registryNamespace, errors.New("provider is unwell"))
			},
			want: RegistryErrorBackend,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			base := memstore.New()
			hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
			recorder := &recordingOrdered{OrderedIndex: hostile}
			base.OrderedIndex = recorder
			store, _ := registryFixture(t, base)
			mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
			recorder.reset()
			test.arm(hostile)

			for name, call := range map[string]func() error{
				"PutHostRegistration": func() error {
					_, err := store.PutHostRegistration(context.Background(), testPutRegistrationRequest(registryEpoch))
					return err
				},
				"GetHostRegistration": func() error {
					_, err := store.GetHostRegistration(context.Background(), GetHostRegistrationRequest{
						TenantID: catalogTenant, SessionID: catalogSession,
					})
					return err
				},
				"ClearHostRegistration": func() error {
					_, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryEpoch))
					return err
				},
			} {
				t.Run(name, func(t *testing.T) {
					assertRegistryCode(t, call(), test.want)
				})
			}
			if n := recorder.countOf("create"); n != 0 {
				t.Fatalf("an unreadable fence was published over with %d creates", n)
			}
			if n := recorder.countOf("update"); n != 0 {
				t.Fatalf("an unreadable fence was written over with %d updates", n)
			}
		})
	}
}

// --- races ------------------------------------------------------------------

// TestPutHostRegistrationLosesTheRaceItObserved pins the compare-and-swap that
// closes the read-compare-write window. The fence alone cannot: a writer that
// read a stale epoch and then wrote unconditionally would land AFTER the
// successor's write and reinstate a route the successor had replaced.
func TestPutHostRegistrationLosesTheRaceItObserved(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	store, _ := registryFixture(t, base)
	mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))

	// A successor lands between this writer's read and its own update.
	var interposing atomic.Bool
	ordered.beforeUpdate = func() {
		if !interposing.CompareAndSwap(false, true) {
			return
		}
		successor := testPutRegistrationRequest(registryNextEpoch)
		successor.Route.HostID = "host-b"
		if _, err := store.PutHostRegistration(context.Background(), successor); err != nil {
			t.Errorf("interposed registration: %v", err)
		}
	}
	_, err := store.PutHostRegistration(context.Background(), testPutRegistrationRequest(registryEpoch))
	assertRegistryCode(t, err, RegistryErrorConflict)

	stored := storedRegistration(t, store)
	if stored.LeaseEpoch != registryNextEpoch || stored.Route.HostID != "host-b" {
		t.Fatalf("the superseded writer reinstated its own route: %+v", *stored.Route)
	}
}

// TestConcurrentHostRegistrationsAgree runs the same race without
// choreography. Whatever interleaving the runtime chooses, every call either
// succeeds or reports the conflict that tells its caller to re-read, and the
// record that survives is one a writer actually wrote.
func TestConcurrentHostRegistrationsAgree(t *testing.T) {
	t.Parallel()

	store, _ := registryFixture(t, memstore.New())
	mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))

	const writers = 8
	var wg sync.WaitGroup
	generations := make([]uint64, writers)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := testPutRegistrationRequest(registryEpoch)
			req.Route.HostGeneration = uint64(i + 1)
			entry, err := store.PutHostRegistration(context.Background(), req)
			if err != nil {
				var registry *RegistryError
				if !errors.As(err, &registry) || registry.Code != RegistryErrorConflict {
					t.Errorf("concurrent registration = %T %v, want a conflict or a success", err, err)
				}
				return
			}
			generations[i] = entry.Registration.Route.HostGeneration
			if generations[i] != uint64(i+1) {
				t.Errorf("writer %d was handed generation %d", i+1, generations[i])
			}
		}()
	}
	wg.Wait()

	stored := storedRegistration(t, store)
	if stored.Route == nil || stored.Route.HostGeneration == 0 || stored.Route.HostGeneration > writers {
		t.Fatalf("the surviving record is one no writer wrote: %+v", stored)
	}
}

// TestPutHostRegistrationReportsALostCreateAsAConflict covers the one race the
// create path can lose: a first registration arriving between this call's read
// and its own create. It is reported as a conflict carrying the revision to
// re-read, and never silently turned into an update — the record that arrived
// carries an epoch this request has never been fenced against.
func TestPutHostRegistrationReportsALostCreateAsAConflict(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	store, _ := registryFixture(t, base)
	if _, _, err := store.CreateCatalogEntry(context.Background(), testCreateRequest()); err != nil {
		t.Fatalf("CreateCatalogEntry: %v", err)
	}

	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	winner := testHostRegistration()
	winner.LeaseEpoch = registryNextEpoch
	value, _, err := encodeHostRegistration(winner)
	if err != nil {
		t.Fatalf("encodeHostRegistration: %v", err)
	}
	// A successor's first registration lands between this call's read and its
	// own create. The read has already happened, so there is no fence left to
	// consult: what refuses the write is the create finding the identity taken.
	var placed atomic.Bool
	ordered.beforeCreate = func() {
		if !placed.CompareAndSwap(false, true) {
			return
		}
		if _, _, err := ordered.OrderedIndex.Create(
			context.Background(), hostRegistrationID(scope, catalogSession), scope.SessionNamespace,
			value, storage.Rank{}, storage.Due{},
		); err != nil {
			t.Errorf("interposed create: %v", err)
		}
	}

	_, err = store.PutHostRegistration(context.Background(), testPutRegistrationRequest(registryEpoch))
	got := assertRegistryCode(t, err, RegistryErrorConflict)
	if got.Revision == 0 {
		t.Fatal("a lost create reported no revision to re-read")
	}
	if stored := storedRegistration(t, store); stored.LeaseEpoch != registryNextEpoch {
		t.Fatalf("the loser overwrote the winner: epoch %d", stored.LeaseEpoch)
	}
}

// TestRegistryWritesAreUnrankedAndNotDue pins the view state this file files,
// as opposed to the filing CHECK, which compares a stored row against the same
// derivation the writer used and so cannot notice the derivation changing.
//
// Not-due is the decision hostRegistrationDue explains: these rows are never
// deleted, so a due state at the expiry would put a permanent entry per session
// into a deadline page that nothing in this plan removes anything from.
func TestRegistryWritesAreUnrankedAndNotDue(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = recorder
	store, _ := registryFixture(t, base)

	mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
	mustPutRegistration(t, store, testPutRegistrationRequest(registryNextEpoch))
	if _, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryNextEpoch)); err != nil {
		t.Fatalf("ClearHostRegistration: %v", err)
	}

	writes := 0
	for _, call := range recorder.snapshot() {
		if call.op != "create" && call.op != "update" {
			continue
		}
		if call.id.Namespace != registryNamespace {
			continue
		}
		writes++
		if call.due != (storage.Due{}) {
			t.Errorf("a registry %s filed the due state %+v; these rows are permanent and nothing sweeps a registry due page",
				call.op, call.due)
		}
		if call.rank != (storage.Rank{}) {
			t.Errorf("a registry %s filed the rank %+v; nothing ranks a per-session registration", call.op, call.rank)
		}
	}
	if writes != 3 {
		t.Fatalf("observed %d registry writes, want the create, the update, and the tombstone", writes)
	}
}

// TestRegistryCleanupRefusesAnUnregisteredSession is the other half of "cleanup
// is idempotent": it is idempotent with respect to its own tombstone, not with
// respect to nothing. Creating one for a session no Host ever registered would
// mint a fencing high-water mark out of an epoch this store has never seen a
// registration under.
func TestRegistryCleanupRefusesAnUnregisteredSession(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = recorder
	store, _ := registryFixture(t, base)
	if _, _, err := store.CreateCatalogEntry(context.Background(), testCreateRequest()); err != nil {
		t.Fatalf("CreateCatalogEntry: %v", err)
	}
	recorder.reset()

	_, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryEpoch))
	assertRegistryCode(t, err, RegistryErrorNotFound)
	for _, call := range recorder.snapshot() {
		if call.id.Namespace == registryNamespace && call.op != "get" {
			t.Fatalf("a cleanup of an unregistered session performed a %s", call.op)
		}
	}
}

// TestHostRegistrationRefusesAnEpochlessTombstone is the zero-epoch rule on the
// shape that does not go through Core. A live record's epoch is checked twice —
// once here and once by Core's observation — so the record's own refusal is
// only observable on the tombstone, which is exactly the shape whose entire
// remaining purpose is to carry the epoch.
func TestHostRegistrationRefusesAnEpochlessTombstone(t *testing.T) {
	t.Parallel()

	tombstone := HostRegistration{
		TenantID:   catalogTenant,
		SessionID:  catalogSession,
		ObservedAt: registryObservedAt,
		ExpiresAt:  registryObservedAt,
	}
	_, _, err := encodeHostRegistration(tombstone)
	got := assertRegistryCode(t, err, RegistryErrorInvalid)
	if got.Field != "lease_epoch" {
		t.Fatalf("field = %q, want %q", got.Field, "lease_epoch")
	}
}

// TestRegistryWritesSucceedOverAnExpiredRegistration is the POSITIVE half of
// the raw-read separation, and it is the ordinary lifecycle rather than an edge
// case: a Host whose heartbeat slipped past its own expiry republishes, and a
// reconciler cleans up a session that lapsed. Both must update the row that is
// already there — keeping its fence and its immutable order — rather than fail
// because a reader would have called it absent, or create a second one.
//
// The suite otherwise only proves the negative direction, that a STALE epoch is
// still refused over an expired row. Adding "refuse if the current record has
// lapsed" to either write path leaves every other test in this file green.
func TestRegistryWritesSucceedOverAnExpiredRegistration(t *testing.T) {
	t.Parallel()

	store, clock := registryFixture(t, memstore.New())
	first := mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
	order := storedRegistrationRow(t, store).Order

	// The Host's heartbeat slipped: its own registration has lapsed and it
	// republishes under the same grant.
	clock.set(registryLapsedAt)
	refresh := testPutRegistrationRequest(registryEpoch)
	refresh.ObservedAt = registryLapsedAt
	refresh.ExpiresAt = registryLapsedAt.Add(5 * time.Minute)
	refreshed := mustPutRegistration(t, store, refresh)
	if refreshed.Revision <= first.Revision {
		t.Fatalf("revision = %d, want an advance on %d", refreshed.Revision, first.Revision)
	}
	if row := storedRegistrationRow(t, store); row.Order != order {
		t.Fatalf("the republish landed on order %d rather than the existing row's %d", row.Order, order)
	}
	if refreshed.Registration.LeaseEpoch != registryEpoch {
		t.Fatalf("epoch = %d, want %d", refreshed.Registration.LeaseEpoch, registryEpoch)
	}
	if _, err := store.GetHostRegistration(context.Background(), GetHostRegistrationRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	}); err != nil {
		t.Fatalf("the republished route is not readable: %v", err)
	}

	// And a cleanup of a session that lapsed again, which is what a reconciler
	// meeting a crashed Host does.
	clock.set(registryLapsedAt.Add(time.Hour))
	cleared, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryNextEpoch))
	if err != nil {
		t.Fatalf("ClearHostRegistration over a lapsed registration: %v", err)
	}
	if cleared.Revision <= refreshed.Revision {
		t.Fatalf("revision = %d, want an advance on %d", cleared.Revision, refreshed.Revision)
	}
	if row := storedRegistrationRow(t, store); row.Order != order {
		t.Fatalf("the cleanup landed on order %d rather than the existing row's %d", row.Order, order)
	}
	if cleared.Registration.Route != nil || cleared.Registration.LeaseEpoch != registryNextEpoch {
		t.Fatalf("tombstone = %+v", cleared.Registration)
	}
}

// --- provider failures ------------------------------------------------------

// TestRegistryClassifiesProviderFailures drives every arm of the registry's
// provider-error mapping. Only the conflict arm is reachable through a public
// operation without a non-conforming provider, so without this the doc comment
// enumerating the arms would be prose with nothing behind it.
//
// Cause preservation is asserted on every arm, not on one: a redacted error
// that does not unwrap to what the provider said leaves an operator with a
// classification and no diagnosis.
func TestRegistryClassifiesProviderFailures(t *testing.T) {
	t.Parallel()

	id := storage.OrderedID{
		Namespace:     registryNamespace,
		OrderingScope: "tenants/x/sessions/y",
		StableKey:     storage.StableKey(catalogSession),
	}
	tests := []struct {
		name string
		err  error
		want RegistryErrorCode
	}{
		{"not found", &storage.OrderedRecordNotFoundError{ID: id}, RegistryErrorNotFound},
		{"deleted", &storage.OrderedDeletedError{ID: id}, RegistryErrorDeleted},
		{
			"conflict",
			&storage.OrderedRevisionConflictError{ID: id, ExpectedRevision: 1, ActualRevision: 2},
			RegistryErrorConflict,
		},
		{
			"ambiguous",
			&storage.OrderedAmbiguousError{Operation: storage.OrderedUpdateOperation, ID: id},
			RegistryErrorUnknown,
		},
		{
			"revision exhausted",
			&storage.OrderedRevisionExhaustedError{ID: id, Revision: math.MaxUint64},
			RegistryErrorBackend,
		},
		{"anything else", errors.New("boom"), RegistryErrorBackend},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := classifyRegistryOrderedError(test.err, "field")
			assertRegistryCode(t, got, test.want)
			if !errors.Is(got, test.err) {
				t.Fatalf("cause was not preserved: %v", got)
			}
		})
	}

	// The revision is the whole value of a conflict: it is what a caller
	// re-reads to.
	conflict := classifyRegistryOrderedError(
		&storage.OrderedRevisionConflictError{ID: id, ExpectedRevision: 1, ActualRevision: 4}, "field")
	if assertRegistryCode(t, conflict, RegistryErrorConflict).Revision != 4 {
		t.Fatal("a conflict did not carry the observed revision")
	}
}

// TestRegistrySurfacesProviderErrorsWithoutLeakingProviderText holds the
// redaction rule on RegistryError to what it claims: a provider's own text
// reaches errors.Is and never Error().
func TestRegistrySurfacesProviderErrorsWithoutLeakingProviderText(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store, _ := registryFixture(t, base)
	mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))

	secret := errors.New("provider path /var/secret/tenant-a/session-a")
	hostile.failGetsIn(registryNamespace, secret)
	_, err := store.GetHostRegistration(context.Background(), GetHostRegistrationRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	})
	assertRegistryCode(t, err, RegistryErrorBackend)
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), string(catalogTenant)) {
		t.Fatalf("provider detail leaked into %q", err.Error())
	}
	if !errors.Is(err, secret) {
		t.Fatal("cause was not preserved for errors.Is")
	}
}

// TestHostRegistrationCanonicalizesItsInstantsToUTC pins the normalization that
// makes a record's stored bytes independent of the zone its writer happened to
// be spelling instants in.
func TestHostRegistrationCanonicalizesItsInstantsToUTC(t *testing.T) {
	t.Parallel()

	elsewhere := time.FixedZone("elsewhere", 5*3600)
	shifted := testHostRegistration()
	shifted.ObservedAt = shifted.ObservedAt.In(elsewhere)
	shifted.ExpiresAt = shifted.ExpiresAt.In(elsewhere)

	encoded, canonical, err := encodeHostRegistration(shifted)
	if err != nil {
		t.Fatalf("encodeHostRegistration: %v", err)
	}
	if canonical.ObservedAt.Location() != time.UTC || canonical.ExpiresAt.Location() != time.UTC {
		t.Fatalf("canonical instants are in %v and %v, want UTC",
			canonical.ObservedAt.Location(), canonical.ExpiresAt.Location())
	}
	utc, _, err := encodeHostRegistration(testHostRegistration())
	if err != nil {
		t.Fatalf("encodeHostRegistration: %v", err)
	}
	if !bytes.Equal(encoded, utc) {
		t.Fatalf("one instant spelled two ways produced two records:\n%s\n%s", encoded, utc)
	}
}
