package sessionstore

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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
// A disposition remnant is PARKED rather than tombstoned: its version-1 bytes
// are left exactly as they are and only its filing moves to not-due, so it
// leaves the due view of every reader version, and any later open of the gate
// — a successor restoring a runtime whose predecessor crashed between the
// intent and the projection, of any released version — re-files it as due. A
// tombstone would make the gate's id unpublishable for good.

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

// assertIntentRetired holds a retired row to what retirement promises: it no
// longer indexes a deadline — tombstoned, or parked (not due).
func assertIntentRetired(t *testing.T, store *Store, gate sessionwire.GateID) {
	t.Helper()
	if !gateIntentGone(t, store, gate) {
		t.Fatalf("intent %s is still due-indexed: %+v", gate, gateIntentRecord(t, store, gate))
	}
}

// gateIntentGone reports whether a gate's deadline intent no longer indexes its
// deadline: tombstoned, or parked by a disposition sweep.
func gateIntentGone(t *testing.T, store *Store, gate sessionwire.GateID) bool {
	t.Helper()
	stored := gateIntentRecord(t, store, gate)
	return stored.Deleted || stored.Due.State != storage.DueAt
}

func assertDueViewEmpty(t *testing.T, store *Store) {
	t.Helper()
	due := mustListDueGates(t, store, catalogDeadline)
	if len(due.Gates) != 0 || len(due.Remnants) != 0 {
		t.Fatalf("due view = gates %v remnants %+v, want empty", dueGateIDs(due), due.Remnants)
	}
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

// The I1.3 trip-wire's case, in the store: a disposition remnant is retired,
// and retiring it changes no byte of it.
func TestRetireGateDeadlineIntentParksADispositionRemnant(t *testing.T) {
	f := newDispositionRetireFixture(t)
	revision := f.crashBeforeProjection(t, f.older, testGate("gate-crashed", 3))
	before := gateIntentRecord(t, f.store, "gate-crashed")
	f.remnantAgeElapsed()

	due := mustListDueGates(t, f.store, catalogDeadline)
	if len(due.Remnants) != 1 || due.Remnants[0].Revision != revision {
		t.Fatalf("vacuous: the sweep does not report the remnant: %+v", due)
	}
	if err := f.retire("gate-crashed", revision); err != nil {
		t.Fatalf("RetireGateDeadlineIntent on a disposition remnant: %v", err)
	}
	after := gateIntentRecord(t, f.store, "gate-crashed")
	if after.Deleted || after.Due != (storage.Due{}) {
		t.Fatalf("the remnant was not parked: %+v", after)
	}
	// The bytes every released reader decodes, unchanged.
	if !bytes.Equal(after.Value, before.Value) {
		t.Fatalf("parking rewrote the intent:\nbefore %s\nafter  %s", before.Value, after.Value)
	}
	if _, err := decodeGateIntent(after.Value); err != nil {
		t.Fatalf("a parked intent no longer decodes: %v", err)
	}
	assertDueViewEmpty(t, f.store)
	// A repeat is ordinary: the reply may have been lost. It writes nothing.
	if err := f.retire("gate-crashed", revision); err != nil {
		t.Fatalf("repeat retirement: %v", err)
	}
	if again := gateIntentRecord(t, f.store, "gate-crashed"); again.Revision != after.Revision {
		t.Fatalf("a repeat retirement wrote: revision %d -> %d", after.Revision, again.Revision)
	}
	// Retirement mints no legacy pin and changes no catalog member.
	if entry := mustGetCatalog(t, f.store); entry.Record.Binding.ProtocolMode != ProtocolModeDisposition {
		t.Fatalf("binding = %+v", entry.Record.Binding)
	}
	if mode, bound, err := f.store.boundProtocolMode(context.Background(), mustScope(t, f.store)); err != nil || !bound || mode != ProtocolModeDisposition {
		t.Fatalf("protocol witness = %q bound=%v err=%v, want disposition", mode, bound, err)
	}
}

// The retirement's own refusals hold on a disposition session: an open gate
// and a young intent are never parked.
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

// Any later open of the gate re-files a parked remnant as due — a successor
// restoring the runtime, or the same Host retrying after a long partition.
func TestAnOpenRevivesAParkedRemnant(t *testing.T) {
	for _, tt := range []struct {
		name string
		by   func(f dispositionRetireFixture) *ResidencyGrant
	}{
		{"the successor", func(f dispositionRetireFixture) *ResidencyGrant { return f.newer }},
		{"the same residency", func(f dispositionRetireFixture) *ResidencyGrant { return f.older }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newDispositionRetireFixture(t)
			gate := testGate("gate-restored", 3)
			revision := f.crashBeforeProjection(t, f.older, gate)
			f.remnantAgeElapsed()
			if err := f.retire("gate-restored", revision); err != nil {
				t.Fatalf("RetireGateDeadlineIntent: %v", err)
			}
			entry := mustOpenDispositionGate(t, f.store, tt.by(f), gate)
			if len(entry.Record.OpenGates) != 1 || entry.Record.OpenGates[0].GateID != "gate-restored" {
				t.Fatalf("the re-publish is not projected: %+v", entry.Record.OpenGates)
			}
			assertDueGate(t, f.store, "gate-restored")
			if got := storedGateIntentRecord(t, f.store, "gate-restored").RecordedAt; !got.Equal(f.clock.Now()) {
				t.Fatalf("revival kept the parked attempt's window: recorded at %v", got)
			}
		})
	}
}

// Re-filing does not depend on the stored instant moving: an open whose clock
// reading is not later still revives the row, and still does not retract the
// instant.
func TestAnOpenRevivesAParkedRemnantWithoutRetractingItsInstant(t *testing.T) {
	f := newDispositionRetireFixture(t)
	gate := testGate("gate-g", 3)
	f.clock.set(catalogActiveAt.Add(time.Minute))
	revision := f.crashBeforeProjection(t, f.older, gate)
	f.clock.set(catalogActiveAt.Add(time.Minute + MinGateIntentRemnantAge))
	if err := f.retire("gate-g", revision); err != nil {
		t.Fatalf("RetireGateDeadlineIntent: %v", err)
	}
	parked := gateIntentRecord(t, f.store, "gate-g")
	f.clock.set(catalogActiveAt) // a reading earlier than the stored one
	mustOpenDispositionGate(t, f.store, f.newer, gate)
	assertDueGate(t, f.store, "gate-g")
	revived := gateIntentRecord(t, f.store, "gate-g")
	if !bytes.Equal(revived.Value, parked.Value) {
		t.Fatalf("revival retracted the recorded instant:\nparked  %s\nrevived %s", parked.Value, revived.Value)
	}
}

// Review F4: the compare-and-swap onto the revision the sweep OBSERVED is what
// stops a sweep parking the intent of a gate re-opened after the sweep's age
// and projection checks.
func TestADispositionSweepLosesToAnOpenThatLandsAfterItsChecks(t *testing.T) {
	f := newDispositionRetireFixture(t)
	gate := testGate("gate-g", 3)
	revision := f.crashBeforeProjection(t, f.older, gate)
	f.remnantAgeElapsed()
	var inner error
	// The sweep has read the record and found G unprojected; the successor
	// re-publishes G there, re-stamping the intent and projecting the gate.
	f.ordered.armGetPause(f.catalogID(t), func() {
		f.clock.set(catalogActiveAt.Add(MinGateIntentRemnantAge + time.Second))
		_, inner = openDispositionGate(f.store, f.newer, gate)
	})
	err := f.retire("gate-g", revision)
	if inner != nil {
		t.Fatalf("the successor's open inside the pause: %v", inner)
	}
	assertCatalogCode(t, err, CatalogErrorConflict)
	assertDueGate(t, f.store, "gate-g")
	if entry := mustGetCatalog(t, f.store); len(entry.Record.OpenGates) != 1 {
		t.Fatalf("gate not projected: %+v", entry.Record.OpenGates)
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
	// intent: the sweep parks it there.
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

// Only a disposition sweep parks. A parked filing met on a legacy session is a
// filing this package never wrote there, and is refused rather than taken as
// the repeat case.
func TestAParkedFilingOnALegacySessionIsRefused(t *testing.T) {
	store, clock := retireFixture(t)
	revision := seedRemnantOfACrashBeforeGateOpened(t, store, "gate-x")
	scope := mustScope(t, store)
	stored := storedGateIntent(t, store, "gate-x")
	if _, err := store.backend.OrderedIndex.Update(context.Background(), gateIntentID(scope, "gate-x"),
		revision, stored.Value, storage.Rank{}, storage.Due{}); err != nil {
		t.Fatal(err)
	}
	clock.set(catalogActiveAt.Add(MinGateIntentRemnantAge))
	err := store.RetireGateDeadlineIntent(context.Background(), retireRequest("gate-x", revision+1))
	assertCatalogCode(t, err, CatalogErrorIdentity)
}
