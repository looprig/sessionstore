package sessionstore

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage/memstore"
)

// Every fixture here drives a DISPOSITION session and takes its residency the
// way Host takes one: through AcquireResidency. The leaser is the permissive
// fake, which issues a strictly higher epoch on every Acquire without refusing,
// so two grants can be live at once. That is the only honest way to hold a
// STALE grant that was not released — a lease taken over under its holder — on a
// provider that has neither TTL nor takeover of its own, and it keeps every
// epoch in this file one the provider actually issued.

type dispositionGateFixture struct {
	store *Store
	// older and newer are two live, unreleased grants over the same session,
	// older.Epoch() < newer.Epoch().
	older, newer *ResidencyGrant
}

func newDispositionGateFixture(t *testing.T) dispositionGateFixture {
	t.Helper()
	backend := memstore.New()
	backend.Leaser = newPermissiveLeaser()
	store := openStore(t, backend, WithClock(fixedClock{registryObservedAt}))
	createDispositionCatalog(t, store)
	older := acquireTestResidency(t, store)
	newer := acquireTestResidency(t, store)
	if older.Epoch() >= newer.Epoch() {
		t.Fatalf("vacuous: grants %d and %d leave no stale grant to refuse", older.Epoch(), newer.Epoch())
	}
	return dispositionGateFixture{store: store, older: older, newer: newer}
}

func openDispositionGate(store *Store, grant *ResidencyGrant, gate sessionwire.GateProjection) (CatalogEntry, error) {
	return store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, Residency: grant, Gate: gate})
}

func resolveDispositionGate(store *Store, grant *ResidencyGrant, gate sessionwire.GateID) (CatalogEntry, error) {
	return store.ResolveGate(context.Background(), ResolveGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, Residency: grant, GateID: gate})
}

func mustOpenDispositionGate(t *testing.T, store *Store, grant *ResidencyGrant, gate sessionwire.GateProjection) CatalogEntry {
	t.Helper()
	entry, err := openDispositionGate(store, grant, gate)
	if err != nil {
		t.Fatalf("OpenGate(%s) under residency %d: %v", gate.GateID, grant.Epoch(), err)
	}
	return entry
}

func mustResolveDispositionGate(t *testing.T, store *Store, grant *ResidencyGrant, gate sessionwire.GateID) CatalogEntry {
	t.Helper()
	entry, err := resolveDispositionGate(store, grant, gate)
	if err != nil {
		t.Fatalf("ResolveGate(%s) under residency %d: %v", gate, grant.Epoch(), err)
	}
	return entry
}

func mustGetCatalog(t *testing.T, store *Store) CatalogEntry {
	t.Helper()
	entry, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: catalogSession})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	return entry
}

// assertNoHostJournalProgress holds the members option E leaves unwritten on a
// disposition record: the store keeps no journal for such a session, so it
// records no tip, no event, and no checkpoint of one.
func assertNoHostJournalProgress(t *testing.T, record CatalogRecord) {
	t.Helper()
	if record.LastJournalSeq != 0 || record.LastEventID != "" || !record.Checkpoint.isZero() {
		t.Fatalf("a disposition gate write recorded journal progress: seq=%d event=%q checkpoint=%+v",
			record.LastJournalSeq, record.LastEventID, record.Checkpoint)
	}
}

// --- the case the v0.12.0 stop found ---------------------------------------

// A disposition record carries no journal tip, and never will: nothing writes
// LastJournalSeq on one. Under v0.11.0's OpenGate a gate therefore had to name
// a sequence at or below zero, which Core forbids — so even with the protocol
// fence lifted, no gate could be opened. This is that case, green.
func TestDispositionOpenGateSucceedsWithNoJournalTip(t *testing.T) {
	f := newDispositionGateFixture(t)
	before := mustGetCatalog(t, f.store)
	if before.Record.LastJournalSeq != 0 || before.Record.LeaseEpoch != 0 {
		t.Fatalf("vacuous: the record already has tip %d / epoch %d", before.Record.LastJournalSeq, before.Record.LeaseEpoch)
	}
	gate := testGate("gate-a", 7)
	entry := mustOpenDispositionGate(t, f.store, f.newer, gate)
	if entry.Revision <= before.Revision {
		t.Fatalf("revision %d did not advance past %d", entry.Revision, before.Revision)
	}
	if entry.Record.LeaseEpoch != uint64(f.newer.Epoch()) {
		t.Fatalf("LeaseEpoch = %d, want the grant's residency %d", entry.Record.LeaseEpoch, f.newer.Epoch())
	}
	if len(entry.Record.OpenGates) != 1 || entry.Record.OpenGates[0].GateID != gate.GateID {
		t.Fatalf("open gates = %+v", entry.Record.OpenGates)
	}
	assertNoHostJournalProgress(t, entry.Record)
	if entry.Record.Binding != before.Record.Binding {
		t.Fatalf("binding changed: %+v", entry.Record.Binding)
	}
	stored := mustGetCatalog(t, f.store)
	if stored.Revision != entry.Revision {
		t.Fatalf("stored revision %d, returned %d", stored.Revision, entry.Revision)
	}
	if gateIntentGone(t, f.store, gate.GateID) {
		t.Fatal("the deadline intent is a tombstone")
	}
}

// ReadGates is the projection Factory reads. On a disposition record its tip is
// DERIVED from the page: the highest open gate's own opening sequence.
func TestDispositionReadGatesDerivesTheTipFromTheOpenGates(t *testing.T) {
	f := newDispositionGateFixture(t)
	assertPage := func(label string, wantTip uint64, wantGates ...string) {
		t.Helper()
		page := mustReadGates(t, f.store)
		if page.JournalTip != wantTip {
			t.Fatalf("%s: JournalTip = %d, want %d", label, page.JournalTip, wantTip)
		}
		if got := gateIDs(page); !reflect.DeepEqual(got, append([]string{}, wantGates...)) {
			t.Fatalf("%s: gates = %v, want %v", label, got, wantGates)
		}
		if page.OpenGateCount != uint64(len(wantGates)) {
			t.Fatalf("%s: OpenGateCount = %d", label, page.OpenGateCount)
		}
		if err := page.Validate(); err != nil {
			t.Fatalf("%s: core refuses the page: %v", label, err)
		}
	}
	assertPage("no gates", 0)
	mustOpenDispositionGate(t, f.store, f.newer, testGate("gate-low", 3))
	assertPage("one gate", 3, "gate-low")
	// Opened second but with a HIGHER sequence, and opened out of order below:
	// the derived tip is the maximum, not the most recent.
	mustOpenDispositionGate(t, f.store, f.newer, testGate("gate-high", 9))
	mustOpenDispositionGate(t, f.store, f.newer, testGate("gate-mid", 5))
	assertPage("three gates", 9, "gate-low", "gate-mid", "gate-high")
	mustResolveDispositionGate(t, f.store, f.newer, "gate-high")
	assertPage("highest resolved", 5, "gate-low", "gate-mid")
	mustResolveDispositionGate(t, f.store, f.newer, "gate-low")
	assertPage("lowest resolved", 5, "gate-mid")
	mustResolveDispositionGate(t, f.store, f.newer, "gate-mid")
	assertPage("all resolved", 0)
	assertNoHostJournalProgress(t, mustGetCatalog(t, f.store).Record)
}

// The status projection reads the STORED tip, which a disposition record never
// carries. A Host session waiting on a gate therefore reports tip zero, and a
// consumer must not read zero as "no progress".
func TestDispositionStatusReportsAZeroTipWhileWaitingOnAGate(t *testing.T) {
	f := newDispositionGateFixture(t)
	entry := mustOpenDispositionGate(t, f.store, f.newer, testGate("gate-a", 7))
	status, err := entry.Record.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.WaitingGateID != "gate-a" || status.JournalTip != 0 {
		t.Fatalf("status = waiting %q tip %d, want gate-a at tip 0", status.WaitingGateID, status.JournalTip)
	}
}

// ListDueGates never reads the tip: a disposition gate past its deadline is
// reported exactly as a legacy one is.
func TestListDueGatesReportsADispositionGate(t *testing.T) {
	f := newDispositionGateFixture(t)
	mustOpenDispositionGate(t, f.store, f.newer, testGate("gate-a", 7))
	due := mustListDueGates(t, f.store, catalogDeadline)
	if got := dueGateIDs(due); !reflect.DeepEqual(got, []string{"session-a/gate-a"}) || len(due.Remnants) != 0 {
		t.Fatalf("due = %v remnants %+v, want gate-a alone", got, due.Remnants)
	}
	mustResolveDispositionGate(t, f.store, f.newer, "gate-a")
	if due := mustListDueGates(t, f.store, catalogDeadline); len(due.Gates) != 0 {
		t.Fatalf("a resolved gate is still due: %v", dueGateIDs(due))
	}
}

// --- what a disposition gate write refuses ---------------------------------

// Every refusal is taken BEFORE either write: the record is byte-identical
// afterwards and no deadline intent exists for the gate.
func TestDispositionGateWritesRefuseEverythingButThisSessionsGrant(t *testing.T) {
	type caller struct {
		leaseEpoch uint64
		residency  func(f dispositionGateFixture, t *testing.T) *ResidencyGrant
	}
	own := func(f dispositionGateFixture, _ *testing.T) *ResidencyGrant { return f.newer }
	none := func(dispositionGateFixture, *testing.T) *ResidencyGrant { return nil }
	cases := []struct {
		name      string
		caller    caller
		wantCode  CatalogErrorCode
		wantField string
	}{
		{"a bare epoch and no grant", caller{5, none}, CatalogErrorInvalid, "residency"},
		{"a bare epoch above every residency", caller{math.MaxUint64, none}, CatalogErrorInvalid, "residency"},
		{"a bare epoch beside a valid grant", caller{5, own}, CatalogErrorInvalid, "lease_epoch"},
		{"neither", caller{0, none}, CatalogErrorInvalid, "lease_epoch"},
		{"a zero-value grant", caller{0, func(dispositionGateFixture, *testing.T) *ResidencyGrant { return &ResidencyGrant{} }}, CatalogErrorInvalid, "residency"},
		{"a released grant", caller{0, func(f dispositionGateFixture, t *testing.T) *ResidencyGrant {
			g, err := f.store.AcquireResidency(context.Background(), AcquireResidencyRequest{TenantID: catalogTenant, SessionID: catalogSession})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			if g.Epoch() <= f.newer.Epoch() {
				t.Fatalf("vacuous: the released grant %d is not above the mark, so only release can refuse it", g.Epoch())
			}
			return g
		}}, CatalogErrorInvalid, "residency"},
		{"another session's grant", caller{0, func(f dispositionGateFixture, t *testing.T) *ResidencyGrant {
			createDispositionCatalogFor(t, f.store, catalogTenant, "session-other")
			return acquireResidencyFor(t, f.store, catalogTenant, "session-other")
		}}, CatalogErrorInvalid, "residency"},
		{"another tenant's grant for the same session id", caller{0, func(f dispositionGateFixture, t *testing.T) *ResidencyGrant {
			createDispositionCatalogFor(t, f.store, "tenant-other", catalogSession)
			return acquireResidencyFor(t, f.store, "tenant-other", catalogSession)
		}}, CatalogErrorInvalid, "residency"},
		{"another store's grant over the same backend", caller{0, func(f dispositionGateFixture, t *testing.T) *ResidencyGrant {
			other := openStore(t, f.store.backend, WithClock(fixedClock{registryObservedAt}))
			return acquireTestResidency(t, other)
		}}, CatalogErrorInvalid, "residency"},
		{"a stale grant", caller{0, func(f dispositionGateFixture, _ *testing.T) *ResidencyGrant { return f.older }}, CatalogErrorEpoch, "lease_epoch"},
	}
	for _, op := range []string{"open", "resolve"} {
		for _, tc := range cases {
			t.Run(op+"/"+tc.name, func(t *testing.T) {
				f := newDispositionGateFixture(t)
				// The mark is newer's, and gate-a is open under it, so a resolve
				// that got through would have a projection to clear.
				mustOpenDispositionGate(t, f.store, f.newer, testGate("gate-a", 3))
				before := mustGetCatalog(t, f.store)
				grant := tc.caller.residency(f, t)
				var err error
				if op == "open" {
					_, err = f.store.OpenGate(context.Background(), OpenGateRequest{
						TenantID: catalogTenant, SessionID: catalogSession,
						LeaseEpoch: tc.caller.leaseEpoch, Residency: grant, Gate: testGate("gate-b", 4)})
				} else {
					_, err = f.store.ResolveGate(context.Background(), ResolveGateRequest{
						TenantID: catalogTenant, SessionID: catalogSession,
						LeaseEpoch: tc.caller.leaseEpoch, Residency: grant, GateID: "gate-a"})
				}
				got := assertCatalogCode(t, err, tc.wantCode)
				if got.Field != tc.wantField {
					t.Fatalf("field = %q, want %q (%v)", got.Field, tc.wantField, err)
				}
				if tc.wantCode == CatalogErrorEpoch && got.Epoch != uint64(f.newer.Epoch()) {
					t.Fatalf("epoch refusal names %d, want the committed mark %d", got.Epoch, f.newer.Epoch())
				}
				assertCatalogUnchanged(t, f.store, before)
				if op == "open" {
					assertNoGateIntent(t, f.store, "gate-b")
				} else if gateIntentGone(t, f.store, "gate-a") {
					t.Fatal("a refused resolve retired the intent")
				}
			})
		}
	}
}

// A grant for a LEGACY session cannot be obtained — AcquireResidency refuses
// one — so this case is built in-package. It pins the other direction of the
// dispatch: a grant never reaches the legacy fence, and the legacy session is
// told its mode does not take one.
func TestLegacyGateWritesRefuseAGrant(t *testing.T) {
	store := openStore(t, memstore.New(), WithClock(fixedClock{registryObservedAt}))
	before := openGateFixture(t, store)
	if _, err := store.AcquireResidency(context.Background(), AcquireResidencyRequest{TenantID: catalogTenant, SessionID: catalogSession}); err == nil {
		t.Fatal("premise: AcquireResidency issued a grant over a legacy session")
	}
	forged := &ResidencyGrant{store: store, tenant: catalogTenant, session: catalogSession, epoch: 9}
	_, err := store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, Residency: forged, Gate: testGate("gate-a", 3)})
	if got := assertCatalogCode(t, err, CatalogErrorInvalid); got.Field != "binding.protocol_mode" {
		t.Fatalf("open field = %q (%v)", got.Field, err)
	}
	_, err = store.ResolveGate(context.Background(), ResolveGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, Residency: forged, GateID: "gate-a"})
	if got := assertCatalogCode(t, err, CatalogErrorInvalid); got.Field != "binding.protocol_mode" {
		t.Fatalf("resolve field = %q (%v)", got.Field, err)
	}
	assertCatalogUnchanged(t, store, before)
	assertNoGateIntent(t, store, "gate-a")
}

// A grant whose provider issued epoch zero is refused rather than written: zero
// is "no mark", and every fence in this package refuses it on the way in.
func TestDispositionGateWritesRefuseAZeroResidencyEpoch(t *testing.T) {
	f := newDispositionGateFixture(t)
	before := mustGetCatalog(t, f.store)
	zero := &ResidencyGrant{store: f.store, tenant: catalogTenant, session: catalogSession, epoch: 0}
	_, err := openDispositionGate(f.store, zero, testGate("gate-a", 3))
	if got := assertCatalogCode(t, err, CatalogErrorInvalid); got.Field != "residency" {
		t.Fatalf("open field = %q (%v)", got.Field, err)
	}
	_, err = resolveDispositionGate(f.store, zero, "gate-a")
	if got := assertCatalogCode(t, err, CatalogErrorInvalid); got.Field != "residency" {
		t.Fatalf("resolve field = %q (%v)", got.Field, err)
	}
	assertCatalogUnchanged(t, f.store, before)
}

// --- the ratchet -----------------------------------------------------------

func TestDispositionGateResidencyMarkOnlyRatchetsUpward(t *testing.T) {
	f := newDispositionGateFixture(t)
	mark := func() uint64 { return mustGetCatalog(t, f.store).Record.LeaseEpoch }

	mustOpenDispositionGate(t, f.store, f.older, testGate("gate-a", 3))
	if got := mark(); got != uint64(f.older.Epoch()) {
		t.Fatalf("first open: mark %d, want %d", got, f.older.Epoch())
	}
	// An equal residency is admitted: one grant legitimately writes many times.
	mustOpenDispositionGate(t, f.store, f.older, testGate("gate-b", 4))
	mustOpenDispositionGate(t, f.store, f.newer, testGate("gate-c", 5))
	if got := mark(); got != uint64(f.newer.Epoch()) {
		t.Fatalf("successor open: mark %d, want %d", got, f.newer.Epoch())
	}
	// The predecessor is now superseded for both edges.
	_, err := openDispositionGate(f.store, f.older, testGate("gate-d", 6))
	if got := assertCatalogCode(t, err, CatalogErrorEpoch); got.Epoch != uint64(f.newer.Epoch()) {
		t.Fatalf("stale open named %d", got.Epoch)
	}
	_, err = resolveDispositionGate(f.store, f.older, "gate-a")
	assertCatalogCode(t, err, CatalogErrorEpoch)
	// The successor resolves under its own mark, which does not move.
	mustResolveDispositionGate(t, f.store, f.newer, "gate-a")
	if got := mark(); got != uint64(f.newer.Epoch()) {
		t.Fatalf("resolve moved the mark to %d", got)
	}
	// A later successor raises it again.
	newest := acquireTestResidency(t, f.store)
	mustResolveDispositionGate(t, f.store, newest, "gate-b")
	if got := mark(); got != uint64(newest.Epoch()) {
		t.Fatalf("newest resolve: mark %d, want %d", got, newest.Epoch())
	}
	assertNoHostJournalProgress(t, mustGetCatalog(t, f.store).Record)
}

// --- what is kept on the disposition path -----------------------------------

func TestDispositionOpenGateKeepsTheSequenceUniquenessRefusal(t *testing.T) {
	f := newDispositionGateFixture(t)
	mustOpenDispositionGate(t, f.store, f.newer, testGate("gate-a", 3))
	before := mustGetCatalog(t, f.store)
	_, err := openDispositionGate(f.store, f.newer, testGate("gate-b", 3))
	if got := assertCatalogCode(t, err, CatalogErrorSequence); got.Field != "gate.opened_journal_seq" {
		t.Fatalf("field = %q", got.Field)
	}
	assertCatalogUnchanged(t, f.store, before)
	assertNoGateIntent(t, f.store, "gate-b")
}

func TestDispositionOpenGateKeepsCoresNonZeroSequence(t *testing.T) {
	f := newDispositionGateFixture(t)
	before := mustGetCatalog(t, f.store)
	_, err := openDispositionGate(f.store, f.newer, testGate("gate-a", 0))
	assertCatalogCode(t, err, CatalogErrorInvalid)
	assertCatalogUnchanged(t, f.store, before)
	assertNoGateIntent(t, f.store, "gate-a")
}

// The disposition open is idempotent over the same gate, as the legacy one is,
// and a different gate under an open identity is still a conflict.
func TestDispositionOpenGateIsIdempotentAndRefusesAReplacement(t *testing.T) {
	f := newDispositionGateFixture(t)
	first := mustOpenDispositionGate(t, f.store, f.newer, testGate("gate-a", 3))
	again := mustOpenDispositionGate(t, f.store, f.newer, testGate("gate-a", 3))
	if again.Revision != first.Revision {
		t.Fatalf("replay rewrote the record: %d -> %d", first.Revision, again.Revision)
	}
	changed := testGate("gate-a", 4)
	_, err := openDispositionGate(f.store, f.newer, changed)
	assertCatalogCode(t, err, CatalogErrorConflict)
}

// --- the counter the field now holds ---------------------------------------

// CatalogRecord.LeaseEpoch on a disposition record holds a RESIDENCY epoch as of
// v0.12.0. That is safe only because nothing wrote the field on such a record
// before: had a journal epoch ever been stored there, the two counters — which
// this module says must never be compared — would now meet in one fence.
//
// This pins the premise behaviourally: every write a disposition session can
// receive other than a gate write leaves the member zero.
func TestDispositionCatalogLeaseEpochIsWrittenOnlyByGateWrites(t *testing.T) {
	f := newDispositionGateFixture(t)
	ctx := context.Background()
	created := mustGetCatalog(t, f.store)
	if created.Record.LeaseEpoch != 0 {
		t.Fatalf("create wrote LeaseEpoch %d", created.Record.LeaseEpoch)
	}
	if _, err := f.store.UpdateCatalogDesiredState(ctx, UpdateCatalogDesiredStateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, ExpectedRevision: created.Revision,
		IdempotencyKey: "intent-2", DesiredPlacement: sessionwire.HostPlacementPooled}); err != nil {
		t.Fatalf("UpdateCatalogDesiredState: %v", err)
	}
	if _, err := f.store.PutHostRegistration(ctx, testPutRegistrationRequest(uint64(f.newer.Epoch()))); err != nil {
		t.Fatalf("PutHostRegistration: %v", err)
	}
	admitted, _, err := f.store.AdmitDispositionCommand(ctx, dispositionRequest())
	if err != nil {
		t.Fatalf("AdmitDispositionCommand: %v", err)
	}
	if _, _, err := f.store.ClaimDispositionCommand(ctx, claimRequest(admitted, f.newer, registryObservedAt.Add(time.Minute))); err != nil {
		t.Fatalf("ClaimDispositionCommand: %v", err)
	}
	if _, err := f.store.UpdateCatalogHostState(ctx, testHostStateRequest(3)); err == nil {
		t.Fatal("UpdateCatalogHostState wrote a disposition record")
	}
	if got := mustGetCatalog(t, f.store).Record.LeaseEpoch; got != 0 {
		t.Fatalf("a non-gate write set the disposition LeaseEpoch to %d", got)
	}
	mustOpenDispositionGate(t, f.store, f.newer, testGate("gate-a", 3))
	if got := mustGetCatalog(t, f.store).Record.LeaseEpoch; got != uint64(f.newer.Epoch()) {
		t.Fatalf("gate write set LeaseEpoch %d, want %d", got, f.newer.Epoch())
	}
}

// And structurally: every production site that can WRITE a LeaseEpoch member
// is on a closed allowlist. A new site fails here and has to be read against
// the premise above before the list grows.
//
// The walk is syntactic and name-based, so it states which forms it sees:
//
//   - an assignment whose target is x.LeaseEpoch (op "assign");
//   - x.LeaseEpoch++ / x.LeaseEpoch-- (op "incdec");
//   - &x.LeaseEpoch, the handle any write through a pointer needs (op "addr");
//   - a CatalogRecord composite literal with a LeaseEpoch key (op "literal").
//
// It walks every declaration in every non-test file of the package, including
// package-level variable initialisers ("<package>"), so a function literal
// outside a FuncDecl is seen too. The name match covers every type with a
// LeaseEpoch member; the envelope and journal sites write Envelope.LeaseEpoch,
// a journal frame's writer epoch, and are listed so the list stays closed.
func TestOnlyHostAndGateWritesAssignTheCatalogLeaseEpoch(t *testing.T) {
	sites := leaseEpochWriteSites(t, productionGoFiles(t))
	want := []string{
		"catalog.go:UpdateCatalogHostState:assign",
		"catalog.go:decodeCatalogRecord:literal",
		"envelope.go:decodeField:assign",
		"gates.go:OpenGate:assign",
		"gates.go:ResolveGate:assign",
		"gates.go:fenceWithoutChange:assign",
		"journal.go:stampWriterOwnedFields:assign",
	}
	if !reflect.DeepEqual(sites, want) {
		t.Fatalf("LeaseEpoch write sites = %v, want %v", sites, want)
	}
}

// The walk's own controls: each blind spot the v0.12.0 quality gate found is a
// form the walk now reports.
func TestLeaseEpochWriteSiteWalkSeesEveryForm(t *testing.T) {
	src := `package sessionstore
func inc(r *CatalogRecord)     { r.LeaseEpoch++ }
func ptr(r *CatalogRecord)     { p := &r.LeaseEpoch; *p = 7 }
func lit(r *CatalogRecord)     { *r = CatalogRecord{LeaseEpoch: 7} }
var pkg = func(r *CatalogRecord) { r.LeaseEpoch = 7 }
func other(r *HostRegistration) { *r = HostRegistration{LeaseEpoch: 7} }
`
	dir := t.TempDir()
	name := filepath.Join(dir, "probe.go")
	if err := os.WriteFile(name, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	got := leaseEpochWriteSites(t, []string{name})
	want := []string{"probe.go:<package>:assign", "probe.go:inc:incdec", "probe.go:lit:literal", "probe.go:ptr:addr"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("walk saw %v, want %v", got, want)
	}
}

func leaseEpochWriteSites(t *testing.T, files []string) []string {
	t.Helper()
	isLeaseEpoch := func(e ast.Expr) bool {
		sel, ok := ast.Unparen(e).(*ast.SelectorExpr)
		return ok && sel.Sel.Name == "LeaseEpoch"
	}
	fset := token.NewFileSet()
	var sites []string
	for _, name := range files {
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		base := filepath.Base(name)
		for _, decl := range file.Decls {
			owner := "<package>"
			if fn, ok := decl.(*ast.FuncDecl); ok {
				owner = fn.Name.Name
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.AssignStmt:
					for _, lhs := range n.Lhs {
						if isLeaseEpoch(lhs) {
							sites = append(sites, base+":"+owner+":assign")
						}
					}
				case *ast.IncDecStmt:
					if isLeaseEpoch(n.X) {
						sites = append(sites, base+":"+owner+":incdec")
					}
				case *ast.UnaryExpr:
					if n.Op == token.AND && isLeaseEpoch(n.X) {
						sites = append(sites, base+":"+owner+":addr")
					}
				case *ast.CompositeLit:
					if ident, ok := n.Type.(*ast.Ident); ok && ident.Name == "CatalogRecord" {
						for _, elt := range n.Elts {
							if kv, ok := elt.(*ast.KeyValueExpr); ok {
								if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "LeaseEpoch" {
									sites = append(sites, base+":"+owner+":literal")
								}
							}
						}
					}
				}
				return true
			})
		}
	}
	sort.Strings(sites)
	return sites
}

// --- a caller cannot name an epoch ------------------------------------------

// The only thing on the disposition path that reaches the ratchet is a
// *ResidencyGrant, and a grant exposes nothing a caller can set: every field is
// unexported, so outside this package the only way to hold one is to be handed
// it by AcquireResidency.
func TestResidencyGrantExposesNoSettableEpoch(t *testing.T) {
	typ := reflect.TypeOf(ResidencyGrant{})
	for i := range typ.NumField() {
		if typ.Field(i).IsExported() {
			t.Fatalf("ResidencyGrant exports %s", typ.Field(i).Name)
		}
	}
	for _, req := range []reflect.Type{reflect.TypeOf(OpenGateRequest{}), reflect.TypeOf(ResolveGateRequest{})} {
		field, ok := req.FieldByName("Residency")
		if !ok || field.Type != reflect.TypeOf((*ResidencyGrant)(nil)) {
			t.Fatalf("%s.Residency = %v, want *ResidencyGrant", req.Name(), field.Type)
		}
	}
}

// --- helpers ----------------------------------------------------------------

func acquireResidencyFor(t *testing.T, s *Store, tenant sessionwire.TenantID, session sessionwire.SessionID) *ResidencyGrant {
	t.Helper()
	grant, err := s.AcquireResidency(context.Background(), AcquireResidencyRequest{TenantID: tenant, SessionID: session})
	if err != nil {
		t.Fatalf("AcquireResidency(%s/%s): %v", tenant, session, err)
	}
	t.Cleanup(func() { _ = grant.Release(context.Background()) })
	return grant
}

func productionGoFiles(t *testing.T) []string {
	t.Helper()
	matches, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, name := range matches {
		if !strings.HasSuffix(name, "_test.go") {
			files = append(files, name)
		}
	}
	if len(files) == 0 {
		t.Fatal("vacuous: no production files")
	}
	return files
}
