package sessionstore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// This file pins v0.12.0's fix round:
//
//   - F1: a successor FENCES its predecessor with any disposition gate write,
//     including the two that change nothing else — an idempotent OpenGate
//     replay (the restore flow's re-publish) and a ResolveGate of a gate the
//     record no longer projects. Both CAS the raised mark.
//   - F3: a gate write's authority is decided before any store I/O and before
//     the gate or gate id is validated. The two TestQG_ tests are the quality
//     gate's probes, committed as written apart from their fixture's home.

// fencedGateFixture is dispositionGateFixture with the provider wrapped so a
// test can count calls and pause a writer just before its catalog CAS.
type fencedGateFixture struct {
	store        *Store
	ordered      *recordingOrdered
	kv           *qgCountingKV
	older, newer *ResidencyGrant
}

func newFencedGateFixture(t *testing.T) fencedGateFixture {
	t.Helper()
	backend := memstore.New()
	backend.Leaser = newPermissiveLeaser()
	ordered := &recordingOrdered{OrderedIndex: backend.OrderedIndex}
	backend.OrderedIndex = ordered
	kv := &qgCountingKV{KV: backend.KV}
	backend.KV = kv
	store := openStore(t, backend, WithClock(fixedClock{registryObservedAt}))
	createDispositionCatalog(t, store)
	older := acquireTestResidency(t, store)
	newer := acquireTestResidency(t, store)
	if older.Epoch() >= newer.Epoch() {
		t.Fatalf("vacuous: grants %d and %d", older.Epoch(), newer.Epoch())
	}
	return fencedGateFixture{store: store, ordered: ordered, kv: kv, older: older, newer: newer}
}

// pauseFirstCatalogUpdate runs inside exactly once, at the first catalog
// Update after it is armed: the paused writer has already read its revision and
// is about to CAS on it.
func (f fencedGateFixture) pauseFirstCatalogUpdate(inside func()) {
	var once sync.Once
	f.ordered.beforeUpdate = func() {
		once.Do(func() {
			f.ordered.beforeUpdate = nil
			inside()
		})
	}
}

func (f fencedGateFixture) mark(t *testing.T) uint64 {
	t.Helper()
	return mustGetCatalog(t, f.store).Record.LeaseEpoch
}

func assertEpochRefusal(t *testing.T, err error, committed ResidencyEpoch) {
	t.Helper()
	got := assertCatalogCode(t, err, CatalogErrorEpoch)
	if got.Field != "lease_epoch" || got.Epoch != uint64(committed) {
		t.Fatalf("epoch refusal = %+v, want lease_epoch at %d", got, committed)
	}
}

// --- F1: the exact probe ----------------------------------------------------

// Grant 1 opens G; grant 2 re-publishes G, as the restore flow tells a
// successor to; grant 1's OpenGate and ResolveGate are then both refused.
func TestSuccessorRepublishFencesThePredecessor(t *testing.T) {
	f := newFencedGateFixture(t)
	opened := mustOpenDispositionGate(t, f.store, f.older, testGate("gate-g", 3))
	republished := mustOpenDispositionGate(t, f.store, f.newer, testGate("gate-g", 3))

	if republished.Revision <= opened.Revision {
		t.Fatalf("re-publish under a higher grant wrote nothing: revision %d -> %d", opened.Revision, republished.Revision)
	}
	if republished.Record.LeaseEpoch != uint64(f.newer.Epoch()) {
		t.Fatalf("re-publish left the mark at %d, want %d", republished.Record.LeaseEpoch, f.newer.Epoch())
	}
	// Everything but the mark is the record the replay found.
	want := opened.Record
	want.LeaseEpoch = republished.Record.LeaseEpoch
	assertSameCatalogRecord(t, republished.Record, want)

	before := mustGetCatalog(t, f.store)
	_, err := openDispositionGate(f.store, f.older, testGate("gate-h", 9))
	assertEpochRefusal(t, err, f.newer.Epoch())
	_, err = resolveDispositionGate(f.store, f.older, "gate-g")
	assertEpochRefusal(t, err, f.newer.Epoch())
	assertCatalogUnchanged(t, f.store, before)
	assertNoGateIntent(t, f.store, "gate-h")
	if gateIntentGone(t, f.store, "gate-g") {
		t.Fatal("the refused resolve retired the gate's intent")
	}
}

// A resolve of a gate the record does not project, under a higher grant, is a
// fencing write too.
func TestResolveOfAnAbsentGateUnderAHigherGrantRaisesTheMark(t *testing.T) {
	f := newFencedGateFixture(t)
	mustOpenDispositionGate(t, f.store, f.older, testGate("gate-a", 3))
	before := mustGetCatalog(t, f.store)
	resolved := mustResolveDispositionGate(t, f.store, f.newer, "gate-absent")
	if resolved.Revision <= before.Revision || resolved.Record.LeaseEpoch != uint64(f.newer.Epoch()) {
		t.Fatalf("absent resolve: revision %d -> %d, mark %d, want a write at %d",
			before.Revision, resolved.Revision, resolved.Record.LeaseEpoch, f.newer.Epoch())
	}
	want := before.Record
	want.LeaseEpoch = resolved.Record.LeaseEpoch
	assertSameCatalogRecord(t, resolved.Record, want)
	_, err := resolveDispositionGate(f.store, f.older, "gate-a")
	assertEpochRefusal(t, err, f.newer.Epoch())
}

// At or below the mark, the two no-op paths stay no-ops: idempotency is not
// traded for the fence.
func TestNoOpGateWritesAtTheMarkWriteNothing(t *testing.T) {
	f := newFencedGateFixture(t)
	opened := mustOpenDispositionGate(t, f.store, f.newer, testGate("gate-a", 3))
	f.ordered.reset()
	replay := mustOpenDispositionGate(t, f.store, f.newer, testGate("gate-a", 3))
	absent := mustResolveDispositionGate(t, f.store, f.newer, "gate-absent")
	if replay.Revision != opened.Revision || absent.Revision != opened.Revision {
		t.Fatalf("revisions %d / %d, want %d", replay.Revision, absent.Revision, opened.Revision)
	}
	for _, call := range f.ordered.snapshot() {
		if call.op == "update" {
			t.Fatalf("a no-op gate write at the mark updated the catalog: %+v", f.ordered.snapshot())
		}
	}
}

// Legacy is untouched by the fix: the same two paths under a HIGHER lease epoch
// still write nothing, as v0.11.0 did. This test passes on v0.11.0 code
// unchanged; that is recorded in the v0.12.0 result doc.
func TestLegacyNoOpGateWritesUnderAHigherEpochStillWriteNothing(t *testing.T) {
	for _, bound := range []bool{false, true} {
		t.Run(map[bool]string{false: "unbound", true: "bound legacy"}[bound], func(t *testing.T) {
			store := openStore(t, memstore.New(), WithClock(fixedClock{registryObservedAt}))
			create := testCreateRequest()
			if bound {
				create.Binding = testSessionBinding()
				create.Binding.ProtocolMode = ProtocolModeLegacy
			}
			if _, _, err := store.CreateCatalogEntry(context.Background(), create); err != nil {
				t.Fatal(err)
			}
			if _, err := store.UpdateCatalogHostState(context.Background(), testHostStateRequest(2)); err != nil {
				t.Fatal(err)
			}
			opened, err := store.OpenGate(context.Background(), OpenGateRequest{
				TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 2, Gate: testGate("gate-a", 3)})
			if err != nil {
				t.Fatal(err)
			}
			replay, err := store.OpenGate(context.Background(), OpenGateRequest{
				TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 7, Gate: testGate("gate-a", 3)})
			if err != nil {
				t.Fatal(err)
			}
			absent, err := store.ResolveGate(context.Background(), ResolveGateRequest{
				TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 9, GateID: "gate-absent"})
			if err != nil {
				t.Fatal(err)
			}
			for _, got := range []CatalogEntry{replay, absent} {
				if got.Revision != opened.Revision || got.Record.LeaseEpoch != 2 {
					t.Fatalf("legacy no-op wrote: revision %d mark %d, want %d / 2", got.Revision, got.Record.LeaseEpoch, opened.Revision)
				}
			}
			assertCatalogUnchanged(t, store, opened)
		})
	}
}

// --- F1: concurrency on both new write paths --------------------------------

// The re-publish's mark CAS loses to a write that lands inside it, and says so;
// the retry fences.
func TestRepublishMarkWriteLosesARaceAsAConflict(t *testing.T) {
	f := newFencedGateFixture(t)
	mustOpenDispositionGate(t, f.store, f.older, testGate("gate-g", 3))
	var inner error
	f.pauseFirstCatalogUpdate(func() { _, inner = openDispositionGate(f.store, f.older, testGate("gate-h", 5)) })
	_, outer := openDispositionGate(f.store, f.newer, testGate("gate-g", 3))
	if inner != nil {
		t.Fatalf("the predecessor's write inside the pause: %v", inner)
	}
	assertCatalogCode(t, outer, CatalogErrorConflict)
	if got := f.mark(t); got != uint64(f.older.Epoch()) {
		t.Fatalf("the losing re-publish moved the mark to %d", got)
	}
	mustOpenDispositionGate(t, f.store, f.newer, testGate("gate-g", 3))
	if got := f.mark(t); got != uint64(f.newer.Epoch()) {
		t.Fatalf("retry left the mark at %d", got)
	}
	_, err := openDispositionGate(f.store, f.older, testGate("gate-i", 6))
	assertEpochRefusal(t, err, f.newer.Epoch())
}

// The absent resolve's mark CAS loses the same way, and — because the CAS
// precedes the retirement — the losing call retires nothing.
func TestAbsentResolveMarkWriteLosesARaceBeforeRetiring(t *testing.T) {
	f := newFencedGateFixture(t)
	mustOpenDispositionGate(t, f.store, f.older, testGate("gate-a", 3))
	// Leave gate-b's deadline intent durable but unprojected — the remnant an
	// interrupted open leaves — so the resolve below reads gate-b as absent
	// while its intent EXISTS. That is what makes the order observable: a
	// resolve that retired before its mark CAS would tombstone it here.
	f.pauseFirstCatalogUpdate(func() { mustOpenDispositionGate(t, f.store, f.older, testGate("gate-c", 5)) })
	_, remnant := openDispositionGate(f.store, f.older, testGate("gate-b", 4))
	assertCatalogCode(t, remnant, CatalogErrorConflict)
	if gateIntentGone(t, f.store, "gate-b") {
		t.Fatal("vacuous: gate-b has no live remnant intent")
	}
	var inner error
	f.pauseFirstCatalogUpdate(func() { _, inner = openDispositionGate(f.store, f.older, testGate("gate-b", 4)) })
	// gate-b is not projected when the resolve reads, so this is the absent
	// path; the predecessor opens it inside the pause.
	_, outer := resolveDispositionGate(f.store, f.newer, "gate-b")
	if inner != nil {
		t.Fatalf("the predecessor's open inside the pause: %v", inner)
	}
	assertCatalogCode(t, outer, CatalogErrorConflict)
	if gateIntentGone(t, f.store, "gate-b") {
		t.Fatal("the resolve that lost its mark CAS retired the intent anyway")
	}
	// The retry now finds gate-b projected and resolves it under the new mark.
	resolved := mustResolveDispositionGate(t, f.store, f.newer, "gate-b")
	if resolved.Record.LeaseEpoch != uint64(f.newer.Epoch()) || len(resolved.Record.OpenGates) != 2 { // gate-a and gate-c remain
		t.Fatalf("retry: mark %d gates %+v", resolved.Record.LeaseEpoch, resolved.Record.OpenGates)
	}
	if !gateIntentGone(t, f.store, "gate-b") {
		t.Fatal("the retried resolve did not retire the intent")
	}
}

// --- F2: what the fix does and does not close ------------------------------

// Once the successor has made ANY fencing gate write, a predecessor's resolve is
// refused before it reaches the intent, so it cannot tombstone a gate the
// successor opens afterwards.
func TestAfterAFencingWriteAStaleResolveCannotRetireASuccessorsIntent(t *testing.T) {
	f := newFencedGateFixture(t)
	mustResolveDispositionGate(t, f.store, f.newer, "gate-fence") // restore's fencing write
	var inner error
	f.pauseFirstCatalogUpdate(func() { _, inner = resolveDispositionGate(f.store, f.older, "gate-g") })
	mustOpenDispositionGate(t, f.store, f.newer, testGate("gate-g", 7))
	assertEpochRefusal(t, inner, f.newer.Epoch())
	if gateIntentGone(t, f.store, "gate-g") {
		t.Fatal("a fenced predecessor tombstoned the successor's intent")
	}
	due := mustListDueGates(t, f.store, catalogDeadline)
	if len(due.Gates) != 1 {
		t.Fatalf("the successor's gate is not due-indexed: %v", dueGateIDs(due))
	}
}

// --- F3: authority before any read, and before gate validation --------------

type qgCountingKV struct {
	storage.KV
	n atomic.Int64
}

func (k *qgCountingKV) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	k.n.Add(1)
	return k.KV.Get(ctx, key)
}

func (k *qgCountingKV) Put(ctx context.Context, key string, rev uint64, v []byte) (uint64, error) {
	k.n.Add(1)
	return k.KV.Put(ctx, key, rev, v)
}

func (k *qgCountingKV) Keys(ctx context.Context, p string) ([]string, error) {
	k.n.Add(1)
	return k.KV.Keys(ctx, p)
}

func (k *qgCountingKV) Delete(ctx context.Context, key string) error {
	k.n.Add(1)
	return k.KV.Delete(ctx, key)
}

// Authority refusals decided by gateAuthorityFor must perform NO store I/O.
func TestQG_AuthorityRefusalPerformsNoRead(t *testing.T) {
	type tc struct {
		name  string
		epoch uint64
		grant func(f fencedGateFixture, t *testing.T) *ResidencyGrant
		field string
	}
	cases := []tc{
		{"neither", 0, func(fencedGateFixture, *testing.T) *ResidencyGrant { return nil }, "lease_epoch"},
		{"both", 7, func(f fencedGateFixture, _ *testing.T) *ResidencyGrant { return f.newer }, "lease_epoch"},
		{"zero-value grant", 0, func(fencedGateFixture, *testing.T) *ResidencyGrant { return &ResidencyGrant{} }, "residency"},
		{"zero epoch grant", 0, func(f fencedGateFixture, _ *testing.T) *ResidencyGrant {
			return &ResidencyGrant{store: f.store, tenant: catalogTenant, session: catalogSession}
		}, "residency"},
		{"released", 0, func(f fencedGateFixture, t *testing.T) *ResidencyGrant {
			g, err := f.store.AcquireResidency(context.Background(), AcquireResidencyRequest{TenantID: catalogTenant, SessionID: catalogSession})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			return g
		}, "residency"},
		{"other session", 0, func(f fencedGateFixture, t *testing.T) *ResidencyGrant {
			createDispositionCatalogFor(t, f.store, catalogTenant, "session-other")
			return acquireResidencyFor(t, f.store, catalogTenant, "session-other")
		}, "residency"},
		{"other store", 0, func(f fencedGateFixture, t *testing.T) *ResidencyGrant {
			other := openStore(t, f.store.backend, WithClock(fixedClock{registryObservedAt}))
			return acquireTestResidency(t, other)
		}, "residency"},
	}
	for _, op := range []string{"open", "resolve"} {
		for _, c := range cases {
			t.Run(op+"/"+c.name, func(t *testing.T) {
				f := newFencedGateFixture(t)
				g := c.grant(f, t)
				f.ordered.reset()
				f.kv.n.Store(0)
				var err error
				if op == "open" {
					_, err = f.store.OpenGate(context.Background(), OpenGateRequest{TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: c.epoch, Residency: g, Gate: testGate("gate-a", 3)})
				} else {
					_, err = f.store.ResolveGate(context.Background(), ResolveGateRequest{TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: c.epoch, Residency: g, GateID: "gate-a"})
				}
				if got := assertCatalogCode(t, err, CatalogErrorInvalid); got.Field != c.field {
					t.Fatalf("field %q want %q", got.Field, c.field)
				}
				if n := len(f.ordered.snapshot()); n != 0 {
					t.Fatalf("refusal performed %d ordered calls: %+v", n, f.ordered.snapshot())
				}
				if n := f.kv.n.Load(); n != 0 {
					t.Fatalf("refusal performed %d KV calls", n)
				}
			})
		}
	}
}

// Authority refusal precedes gate validation (v0.11.0 order for zero epoch).
func TestQG_AuthorityRefusalPrecedesGateValidation(t *testing.T) {
	f := newFencedGateFixture(t)
	bad := testGate("gate-a", 0) // Core refuses seq 0
	_, err := f.store.OpenGate(context.Background(), OpenGateRequest{TenantID: catalogTenant, SessionID: catalogSession, Residency: &ResidencyGrant{}, Gate: bad})
	if got := assertCatalogCode(t, err, CatalogErrorInvalid); got.Field != "residency" {
		t.Fatalf("field %q", got.Field)
	}
	_, err = f.store.ResolveGate(context.Background(), ResolveGateRequest{TenantID: catalogTenant, SessionID: catalogSession, Residency: &ResidencyGrant{}, GateID: sessionwire.GateID("")})
	if got := assertCatalogCode(t, err, CatalogErrorInvalid); got.Field != "residency" {
		t.Fatalf("resolve field %q", got.Field)
	}
}

// --- helpers ----------------------------------------------------------------

func assertSameCatalogRecord(t *testing.T, got, want CatalogRecord) {
	t.Helper()
	gotBytes, err := encodeCatalogRecord(got)
	if err != nil {
		t.Fatal(err)
	}
	wantBytes, err := encodeCatalogRecord(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotBytes) != string(wantBytes) {
		t.Fatalf("record differs beyond the mark:\n got %s\nwant %s", gotBytes, wantBytes)
	}
}
