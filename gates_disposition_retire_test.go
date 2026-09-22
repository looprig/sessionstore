package sessionstore

import (
	"context"
	"errors"
	"sync"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// This file pins v0.13.0's disposition gate-intent retirement.
//
// Up to v0.12.0 RetireGateDeadlineIntent reserved the LEGACY protocol before it
// deleted, so on a disposition session — where every Host-published gate has
// lived since host v0.4.0 — a crash-before-open remnant was refused with
// catalog conflict (binding.protocol_mode) forever and accumulated in the due
// view (found by the tests lane, I1.3).
//
// A disposition remnant is RETIRED IN PLACE rather than tombstoned: the row
// leaves the due view, and a SUCCESSOR — a strictly higher residency, whose
// restored runtime is authoritative about which of its gates are open — may
// re-publish the gate over it. A tombstone would make the gate's id
// unpublishable for good, which is exactly what a successor restoring a
// runtime whose predecessor crashed between the intent and the projection
// would hit.

// faultingOrdered fails the ordered writes a test arms it for.
type faultingOrdered struct {
	storage.OrderedIndex

	mu         sync.Mutex
	failUpdate func(storage.OrderedID) error
	pauseGet   func(storage.OrderedID)
}

var errInjectedWrite = errors.New("injected write failure")

func (o *faultingOrdered) Update(ctx context.Context, id storage.OrderedID, rev uint64, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, error) {
	o.mu.Lock()
	fail := o.failUpdate
	o.mu.Unlock()
	if fail != nil {
		if err := fail(id); err != nil {
			return storage.OrderedRecord{}, err
		}
	}
	return o.OrderedIndex.Update(ctx, id, rev, value, rank, due)
}

func (o *faultingOrdered) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	o.mu.Lock()
	pause := o.pauseGet
	o.mu.Unlock()
	record, err := o.OrderedIndex.Get(ctx, id)
	if pause != nil {
		pause(id)
	}
	return record, err
}

func (o *faultingOrdered) arm(fail func(storage.OrderedID) error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failUpdate = fail
}

// armGetPause runs inside once, after the first Get of id returns to its
// caller's store code but before that caller acts on it.
func (o *faultingOrdered) armGetPause(id storage.OrderedID, inside func()) {
	var once sync.Once
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pauseGet = func(got storage.OrderedID) {
		if got != id {
			return
		}
		once.Do(func() {
			o.mu.Lock()
			o.pauseGet = nil
			o.mu.Unlock()
			inside()
		})
	}
}

type dispositionRetireFixture struct {
	store        *Store
	clock        *movableClock
	ordered      *faultingOrdered
	older, newer *ResidencyGrant
}

func newDispositionRetireFixture(t *testing.T) dispositionRetireFixture {
	t.Helper()
	backend := memstore.New()
	backend.Leaser = newPermissiveLeaser()
	ordered := &faultingOrdered{OrderedIndex: backend.OrderedIndex}
	backend.OrderedIndex = ordered
	clock := newMovableClock(catalogActiveAt)
	store := openStore(t, backend, WithControlShards(1), WithClock(clock))
	createDispositionCatalog(t, store)
	older := acquireTestResidency(t, store)
	newer := acquireTestResidency(t, store)
	if older.Epoch() >= newer.Epoch() {
		t.Fatalf("vacuous: grants %d and %d", older.Epoch(), newer.Epoch())
	}
	return dispositionRetireFixture{store: store, clock: clock, ordered: ordered, older: older, newer: newer}
}

func (f dispositionRetireFixture) catalogID(t *testing.T) storage.OrderedID {
	t.Helper()
	scope, err := f.store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatal(err)
	}
	return catalogID(scope, catalogSession)
}

func (f dispositionRetireFixture) intentID(t *testing.T, gate sessionwire.GateID) storage.OrderedID {
	t.Helper()
	scope, err := f.store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatal(err)
	}
	return gateIntentID(scope, gate)
}

// crashBeforeProjection runs a real OpenGate under grant whose projection write
// fails, leaving exactly the remnant an open that crashed between its two
// writes leaves, and returns the remnant's revision.
func (f dispositionRetireFixture) crashBeforeProjection(t *testing.T, grant *ResidencyGrant, gate sessionwire.GateProjection) uint64 {
	t.Helper()
	catalog := f.catalogID(t)
	f.ordered.arm(func(id storage.OrderedID) error {
		if id == catalog {
			return errInjectedWrite
		}
		return nil
	})
	defer f.ordered.arm(nil)
	if _, err := openDispositionGate(f.store, grant, gate); !errors.Is(err, errInjectedWrite) {
		t.Fatalf("the crashing open returned %v, want the injected failure", err)
	}
	stored := gateIntentRecord(t, f.store, gate.GateID)
	if stored.Deleted || stored.Due.State != storage.DueAt {
		t.Fatalf("vacuous: the crashed open left no due remnant: %+v", stored)
	}
	return stored.Revision
}

func (f dispositionRetireFixture) retire(gate sessionwire.GateID, revision uint64) error {
	return f.store.RetireGateDeadlineIntent(context.Background(), retireRequest(gate, revision))
}

func (f dispositionRetireFixture) remnantAgeElapsed() {
	f.clock.set(catalogActiveAt.Add(MinGateIntentRemnantAge))
}

// assertIntentRetired holds a retired row to what retirement promises: not due,
// so no sweep ever reports it again.
func assertIntentRetired(t *testing.T, store *Store, gate sessionwire.GateID) {
	t.Helper()
	stored := gateIntentRecord(t, store, gate)
	if stored.Deleted {
		return
	}
	if stored.Due.State == storage.DueAt {
		t.Fatalf("intent %s is still due-indexed: %+v", gate, stored)
	}
	intent, err := gateIntentFor(stored)
	if err != nil {
		t.Fatalf("gateIntentFor(%s): %v", gate, err)
	}
	if !intent.Retired {
		t.Fatalf("intent %s is not due but not retired either: %+v", gate, intent)
	}
}

func assertDueViewEmpty(t *testing.T, store *Store) {
	t.Helper()
	due := mustListDueGates(t, store, catalogDeadline)
	if len(due.Gates) != 0 || len(due.Remnants) != 0 {
		t.Fatalf("due view = gates %v remnants %+v, want empty", dueGateIDs(due), due.Remnants)
	}
}

// The I1.3 trip-wire's case, in the store: a disposition remnant is retired.
func TestRetireGateDeadlineIntentRetiresADispositionRemnant(t *testing.T) {
	f := newDispositionRetireFixture(t)
	revision := f.crashBeforeProjection(t, f.older, testGate("gate-crashed", 3))
	f.remnantAgeElapsed()

	due := mustListDueGates(t, f.store, catalogDeadline)
	if len(due.Remnants) != 1 || due.Remnants[0].Revision != revision {
		t.Fatalf("vacuous: the sweep does not report the remnant: %+v", due)
	}
	if err := f.retire("gate-crashed", revision); err != nil {
		t.Fatalf("RetireGateDeadlineIntent on a disposition remnant: %v", err)
	}
	assertIntentRetired(t, f.store, "gate-crashed")
	assertDueViewEmpty(t, f.store)
	// A repeat is ordinary: the reply may have been lost.
	if err := f.retire("gate-crashed", revision); err != nil {
		t.Fatalf("repeat retirement: %v", err)
	}
	// Retirement mints no legacy pin and changes no catalog member.
	if entry := mustGetCatalog(t, f.store); entry.Record.Binding.ProtocolMode != ProtocolModeDisposition {
		t.Fatalf("binding = %+v", entry.Record.Binding)
	}
	if err := f.store.bindProtocolMode(context.Background(), mustScope(t, f.store), ProtocolModeDisposition); err != nil {
		t.Fatalf("the session is no longer disposition-pinned: %v", err)
	}
}

// v0.12.0 wrote version-1 intents on disposition sessions. Those remnants are
// retirable too.
func TestRetireGateDeadlineIntentRetiresAVersionOneDispositionRemnant(t *testing.T) {
	f := newDispositionRetireFixture(t)
	revision := seedRemnantOfACrashBeforeGateOpened(t, f.store, "gate-v1")
	f.remnantAgeElapsed()
	if err := f.retire("gate-v1", revision); err != nil {
		t.Fatalf("RetireGateDeadlineIntent on a v0.12.0 remnant: %v", err)
	}
	assertIntentRetired(t, f.store, "gate-v1")
	// An intent whose writer is unknown yields to any grant.
	// seedRemnantOfACrashBeforeGateOpened files its intent at sequence 5.
	mustOpenDispositionGate(t, f.store, f.older, testGate("gate-v1", 5))
	assertDueGate(t, f.store, "gate-v1")
}

// The retirement's own refusals hold on a disposition session: an open gate
// and a young intent are never retired.
func TestDispositionRetirementRefusesAnOpenGateAndAYoungIntent(t *testing.T) {
	f := newDispositionRetireFixture(t)
	mustOpenDispositionGate(t, f.store, f.older, testGate("gate-open", 3))
	young := f.crashBeforeProjection(t, f.older, testGate("gate-young", 4))

	assertCatalogCode(t, f.retire("gate-young", young), CatalogErrorTooSoon)
	f.remnantAgeElapsed()
	open := gateIntentRecord(t, f.store, "gate-open").Revision
	assertCatalogCode(t, f.retire("gate-open", open), CatalogErrorConflict)
	if stored := gateIntentRecord(t, f.store, "gate-open"); stored.Revision != open || stored.Due.State != storage.DueAt {
		t.Fatalf("a refused retirement touched the open gate's intent: %+v", stored)
	}
}

// A successor restoring a runtime whose predecessor crashed between the intent
// and the projection re-publishes the gate over the retired remnant, and the
// gate is due-indexed again. The predecessor itself cannot.
func TestASuccessorRepublishesAGateOverARetiredRemnant(t *testing.T) {
	f := newDispositionRetireFixture(t)
	gate := testGate("gate-restored", 3)
	revision := f.crashBeforeProjection(t, f.older, gate)
	f.remnantAgeElapsed()
	if err := f.retire("gate-restored", revision); err != nil {
		t.Fatalf("RetireGateDeadlineIntent: %v", err)
	}

	_, err := openDispositionGate(f.store, f.older, gate)
	assertCatalogCode(t, err, CatalogErrorDeleted)

	entry := mustOpenDispositionGate(t, f.store, f.newer, gate)
	if len(entry.Record.OpenGates) != 1 || entry.Record.OpenGates[0].GateID != "gate-restored" {
		t.Fatalf("the successor's re-publish is not projected: %+v", entry.Record.OpenGates)
	}
	assertDueGate(t, f.store, "gate-restored")
	intent := storedGateIntentRecord(t, f.store, "gate-restored")
	if intent.Retired || intent.Residency != f.newer.Epoch() {
		t.Fatalf("revived intent = %+v, want live at residency %d", intent, f.newer.Epoch())
	}
	if !intent.RecordedAt.Equal(f.clock.Now()) {
		t.Fatalf("revival kept the retired attempt's window: recorded at %v", intent.RecordedAt)
	}
}

// A sweep that retires between a resolve's two writes does not stop the resolve
// completing.
func TestDispositionRetirementMidResolveLetsTheResolveComplete(t *testing.T) {
	f := newDispositionRetireFixture(t)
	mustOpenDispositionGate(t, f.store, f.older, testGate("gate-a", 3))
	f.remnantAgeElapsed()
	var inner error
	// The resolve's projection write has landed and it has just read the
	// intent: the sweep retires it there.
	f.ordered.armGetPause(f.intentID(t, "gate-a"), func() {
		inner = f.retire("gate-a", gateIntentRecord(t, f.store, "gate-a").Revision)
	})
	// The resolve may lose its intent write to the sweep; it is idempotent and
	// a repeat completes it. What it must never do is resurrect the row.
	if _, err := resolveDispositionGate(f.store, f.older, "gate-a"); err != nil {
		assertCatalogCode(t, err, CatalogErrorConflict)
	}
	if inner != nil {
		t.Fatalf("the sweep inside the resolve: %v", inner)
	}
	mustResolveDispositionGate(t, f.store, f.older, "gate-a")
	assertIntentRetired(t, f.store, "gate-a")
	assertDueViewEmpty(t, f.store)
}

func assertDueGate(t *testing.T, store *Store, gate sessionwire.GateID) {
	t.Helper()
	due := mustListDueGates(t, store, catalogDeadline)
	for _, entry := range due.Gates {
		if entry.Gate.GateID == gate {
			return
		}
	}
	t.Fatalf("gate %s is not due-indexed: gates %v remnants %+v", gate, dueGateIDs(due), due.Remnants)
}
