package sessionstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// openGateFixture creates a session and gives it a durable journal tip, which
// is the precondition every open has: a gate names the event that opened it,
// and that event must already be durable.
func openGateFixture(t *testing.T, store *Store) CatalogEntry {
	t.Helper()
	mustCreateCatalog(t, store)
	entry, err := store.UpdateCatalogHostState(context.Background(), testHostStateRequest(1))
	if err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}
	return entry
}

func mustOpenGate(t *testing.T, store *Store, epoch uint64, gate sessionwire.GateProjection) CatalogEntry {
	t.Helper()
	entry, err := store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: epoch, Gate: gate,
	})
	if err != nil {
		t.Fatalf("OpenGate(%s): %v", gate.GateID, err)
	}
	return entry
}

func mustReadGates(t *testing.T, store *Store) sessionwire.GatePage {
	t.Helper()
	page, err := store.ReadGates(context.Background(), ReadGatesRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	})
	if err != nil {
		t.Fatalf("ReadGates: %v", err)
	}
	return page
}

func gateIDs(page sessionwire.GatePage) []string {
	ids := make([]string, 0, len(page.Gates))
	for _, gate := range page.Gates {
		ids = append(ids, string(gate.GateID))
	}
	return ids
}

// assertNoGateIntent fails unless the gate has no intent record at all. It
// reads the provider directly for the same reason assertNoCatalogRecord does:
// a rejected open that nevertheless wrote its deadline intent would be
// invisible through the store API.
func assertNoGateIntent(t *testing.T, store *Store, gate sessionwire.GateID) {
	t.Helper()
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	if _, err := store.backend.OrderedIndex.Get(context.Background(), gateIntentID(scope, gate)); !errors.As(err, new(*storage.OrderedRecordNotFoundError)) {
		t.Fatalf("a rejected open left a deadline intent behind: %v", err)
	}
}

func gateIntentRecord(t *testing.T, store *Store, gate sessionwire.GateID) storage.OrderedRecord {
	t.Helper()
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	record, err := store.backend.OrderedIndex.Get(context.Background(), gateIntentID(scope, gate))
	if err != nil {
		t.Fatalf("the gate has no deadline intent: %v", err)
	}
	return record
}

// --- zero, one, and many open gates ---------------------------------------

func TestReadGatesReportsNoOpenGates(t *testing.T) {
	store := openTestStore(t)
	openGateFixture(t, store)

	page := mustReadGates(t, store)
	if len(page.Gates) != 0 || page.OpenGateCount != 0 {
		t.Fatalf("a session with no gates read back %d gates (count %d)", len(page.Gates), page.OpenGateCount)
	}
	if page.JournalTip != 10 {
		t.Fatalf("journal tip = %d, want the record's durable tip 10", page.JournalTip)
	}
	if page.NextCursor != "" || page.PreviousCursor != "" {
		t.Fatal("a gate read issued a continuation cursor; gate reads are bounded and have none")
	}
}

func TestReadGatesReportsOneOpenGate(t *testing.T) {
	store := openTestStore(t)
	openGateFixture(t, store)
	mustOpenGate(t, store, 1, testGate("gate-a", 5))

	page := mustReadGates(t, store)
	if len(page.Gates) != 1 || page.Gates[0].GateID != "gate-a" {
		t.Fatalf("gates = %v, want [gate-a]", gateIDs(page))
	}
	if page.OpenGateCount != 1 {
		t.Fatalf("open gate count = %d, want 1", page.OpenGateCount)
	}
	// The whole projection must survive, not just its identity: a gate a
	// Factory cannot render is not a public gate projection.
	if page.Gates[0].Prompt.Title != "Confirm" || page.Gates[0].Answerability != sessionwire.GateAnswerabilityResident {
		t.Fatalf("the public projection did not survive the round trip: %+v", page.Gates[0])
	}
	if !page.Gates[0].Deadline.Equal(catalogDeadline) {
		t.Fatalf("deadline = %v, want %v", page.Gates[0].Deadline, catalogDeadline)
	}
}

// TestReadGatesOrdersManySimultaneousGates opens gates in an order that is
// neither their sequence order nor their identity order, so a read that
// returned insertion order would be visibly wrong.
func TestReadGatesOrdersManySimultaneousGates(t *testing.T) {
	store := openTestStore(t)
	openGateFixture(t, store)
	for _, gate := range []sessionwire.GateProjection{
		testGate("gate-c", 9),
		testGate("gate-a", 3),
		testGate("gate-b", 7),
	} {
		mustOpenGate(t, store, 1, gate)
	}

	page := mustReadGates(t, store)
	want := []string{"gate-a", "gate-b", "gate-c"}
	if got := gateIDs(page); len(got) != len(want) {
		t.Fatalf("gates = %v, want %v", got, want)
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("gates = %v, want %v (opened_seq order)", got, want)
			}
		}
	}
	if page.OpenGateCount != 3 {
		t.Fatalf("open gate count = %d, want 3", page.OpenGateCount)
	}
}

// TestResolveGateLeavesTheOtherGateOpen is the task's central read: two gates
// are open at once, one is answered, and the other must survive untouched with
// its own intent still due.
func TestResolveGateLeavesTheOtherGateOpen(t *testing.T) {
	store := openTestStore(t)
	openGateFixture(t, store)
	mustOpenGate(t, store, 1, testGate("gate-a", 3))
	mustOpenGate(t, store, 1, testGate("gate-b", 7))

	if _, err := store.ResolveGate(context.Background(), ResolveGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, GateID: "gate-a",
	}); err != nil {
		t.Fatalf("ResolveGate: %v", err)
	}

	page := mustReadGates(t, store)
	if len(page.Gates) != 1 || page.Gates[0].GateID != "gate-b" {
		t.Fatalf("gates after resolving gate-a = %v, want [gate-b]", gateIDs(page))
	}
	// The survivor's deadline intent must still be due; only the resolved
	// gate's leaves the due pages.
	if due := gateIntentRecord(t, store, "gate-b").Due; due.State != storage.DueAt {
		t.Fatalf("the surviving gate's intent is no longer due: %+v", due)
	}
	resolved := gateIntentRecord(t, store, "gate-a")
	if resolved.Due.State != storage.NotDue {
		t.Fatalf("a resolved gate is still due: %+v", resolved.Due)
	}
	// Auditable: the record and its value are retained even though it no
	// longer appears in a due page.
	if len(resolved.Value) == 0 {
		t.Fatal("resolving a gate discarded its intent record rather than retiring it")
	}
}

// --- the deadline intent and its ordering ---------------------------------

// TestOpenGateWritesIntentBeforeProjection pins the sequencing contract. The
// intent must be durable before the gate is publicly projected as open, so the
// only crash window is an intent whose open event never committed — which a due
// reader can validate away. The reverse window would publish an open gate whose
// deadline nothing durable records.
func TestOpenGateWritesIntentBeforeProjection(t *testing.T) {
	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	store := openStore(t, base)
	openGateFixture(t, store)

	before := len(ordered.snapshot())
	mustOpenGate(t, store, 1, testGate("gate-a", 5))

	intentAt, projectionAt := -1, -1
	for i, call := range ordered.snapshot()[before:] {
		switch {
		case call.op == "create" && call.id.Namespace == gateNamespace && intentAt < 0:
			intentAt = i
		case call.op == "update" && call.id.Namespace == catalogNamespace && projectionAt < 0:
			projectionAt = i
		}
	}
	if intentAt < 0 {
		t.Fatal("OpenGate wrote no deadline intent record")
	}
	if projectionAt < 0 {
		t.Fatal("OpenGate committed no open projection")
	}
	if intentAt > projectionAt {
		t.Fatalf("the open projection was committed before its deadline intent (intent at %d, projection at %d)", intentAt, projectionAt)
	}
}

// TestResolveGateRetiresIntentAfterProjection is the mirror. Clearing the
// projection first leaves the same tolerable state an interrupted open does —
// an intent with no matching open gate — whereas retiring the intent first
// would leave a gate publicly open with nothing recording its deadline.
func TestResolveGateRetiresIntentAfterProjection(t *testing.T) {
	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	store := openStore(t, base)
	openGateFixture(t, store)
	mustOpenGate(t, store, 1, testGate("gate-a", 5))

	before := len(ordered.snapshot())
	if _, err := store.ResolveGate(context.Background(), ResolveGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, GateID: "gate-a",
	}); err != nil {
		t.Fatalf("ResolveGate: %v", err)
	}

	retireAt, projectionAt := -1, -1
	for i, call := range ordered.snapshot()[before:] {
		switch {
		case call.id.Namespace == gateNamespace && (call.op == "delete" || call.op == "update") && retireAt < 0:
			retireAt = i
		case call.op == "update" && call.id.Namespace == catalogNamespace && projectionAt < 0:
			projectionAt = i
		}
	}
	if retireAt < 0 {
		t.Fatal("ResolveGate never retired the deadline intent")
	}
	if projectionAt < 0 {
		t.Fatal("ResolveGate never rewrote the open projection")
	}
	if projectionAt > retireAt {
		t.Fatalf("the deadline intent was retired before the projection was cleared (projection at %d, intent at %d)", projectionAt, retireAt)
	}
}

func TestOpenGateRecordsTheDeadlineAsAnAbsoluteDueTime(t *testing.T) {
	store := openTestStore(t)
	openGateFixture(t, store)
	gate := testGate("gate-a", 5)
	mustOpenGate(t, store, 1, gate)

	record := gateIntentRecord(t, store, "gate-a")
	if record.Due.State != storage.DueAt {
		t.Fatalf("intent due state = %v, want DueAt", record.Due.State)
	}
	if record.Due.UnixMillis != gate.Deadline.UnixMilli() {
		t.Fatalf("intent due = %d, want the gate's absolute deadline %d", record.Due.UnixMillis, gate.Deadline.UnixMilli())
	}
	if record.Rank.Ranked {
		t.Fatal("a deadline intent is ranked; it must not appear in the catalog's recency view")
	}
	intent, err := decodeGateIntent(record.Value)
	if err != nil {
		t.Fatalf("decodeGateIntent: %v", err)
	}
	if intent.OpenedEventID != gate.OpenedEventID || intent.OpenedJournalSeq != gate.OpenedJournalSeq {
		t.Fatalf("the intent does not name the durable open event: %+v", intent)
	}
	if intent.TenantID != catalogTenant || intent.SessionID != catalogSession || intent.GateID != gate.GateID {
		t.Fatalf("the intent does not carry its own identity: %+v", intent)
	}
}

// --- rejections, each of which must precede every write -------------------

func TestOpenGateRefusesAnEventAboveTheDurableTip(t *testing.T) {
	store := openTestStore(t)
	entry := openGateFixture(t, store)

	_, err := store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1,
		Gate: testGate("gate-a", entry.Record.LastJournalSeq+1),
	})
	assertCatalogCode(t, err, CatalogErrorSequence)
	assertCatalogUnchanged(t, store, entry)
	assertNoGateIntent(t, store, "gate-a")
}

func TestOpenGateRefusesASequenceAlreadyClaimedByAnOpenGate(t *testing.T) {
	store := openTestStore(t)
	openGateFixture(t, store)
	entry := mustOpenGate(t, store, 1, testGate("gate-a", 5))

	_, err := store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: testGate("gate-b", 5),
	})
	assertCatalogCode(t, err, CatalogErrorSequence)
	assertCatalogUnchanged(t, store, entry)
	assertNoGateIntent(t, store, "gate-b")
}

func TestOpenGateRefusesADifferentGateWithAnOpenIdentity(t *testing.T) {
	store := openTestStore(t)
	openGateFixture(t, store)
	entry := mustOpenGate(t, store, 1, testGate("gate-a", 5))

	changed := testGate("gate-a", 6)
	_, err := store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: changed,
	})
	assertCatalogCode(t, err, CatalogErrorConflict)
	assertCatalogUnchanged(t, store, entry)
}

func TestOpenGateIsIdempotentForTheSameOpenGate(t *testing.T) {
	store := openTestStore(t)
	openGateFixture(t, store)
	gate := testGate("gate-a", 5)
	first := mustOpenGate(t, store, 1, gate)
	second := mustOpenGate(t, store, 1, gate)

	if second.Revision != first.Revision {
		t.Fatalf("a repeated open rewrote the record: revision %d -> %d", first.Revision, second.Revision)
	}
	if page := mustReadGates(t, store); len(page.Gates) != 1 {
		t.Fatalf("a repeated open duplicated the gate: %v", gateIDs(page))
	}
}

func TestOpenGateRefusesMoreThanTheProjectionHolds(t *testing.T) {
	store := openTestStore(t)
	openGateFixture(t, store)
	tip := UpdateCatalogHostStateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1,
		State: sessionwire.SessionStateRunning, Residency: sessionwire.SessionResidencyResident,
		LastActiveAt: catalogActiveAt, LastJournalSeq: 1000, LastEventID: "event-1000",
	}
	entry, err := store.UpdateCatalogHostState(context.Background(), tip)
	if err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}
	for i := range MaxCatalogOpenGates {
		entry = mustOpenGate(t, store, 1, testGate("gate-"+string(rune('a'+i)), uint64(i+1)))
	}

	overflow := testGate("gate-overflow", 900)
	_, err = store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: overflow,
	})
	assertCatalogCode(t, err, CatalogErrorTooLarge)
	assertCatalogUnchanged(t, store, entry)
	assertNoGateIntent(t, store, "gate-overflow")
}

func TestOpenGateRefusesASupersededLeaseEpoch(t *testing.T) {
	store := openTestStore(t)
	openGateFixture(t, store)
	entry, err := store.UpdateCatalogHostState(context.Background(), testHostStateRequest(4))
	if err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}

	_, err = store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 3, Gate: testGate("gate-a", 5),
	})
	if got := assertCatalogCode(t, err, CatalogErrorEpoch); got.Epoch != 4 {
		t.Fatalf("epoch failure reported %d, want the committed high-water 4", got.Epoch)
	}
	assertCatalogUnchanged(t, store, entry)
	assertNoGateIntent(t, store, "gate-a")
}

func TestGateWritesRefuseAZeroLeaseEpoch(t *testing.T) {
	store := openTestStore(t)
	entry := openGateFixture(t, store)

	_, err := store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, Gate: testGate("gate-a", 5),
	})
	assertCatalogCode(t, err, CatalogErrorInvalid)
	_, err = store.ResolveGate(context.Background(), ResolveGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, GateID: "gate-a",
	})
	assertCatalogCode(t, err, CatalogErrorInvalid)
	assertCatalogUnchanged(t, store, entry)
	assertNoGateIntent(t, store, "gate-a")
}

func TestGateOperationsRejectInvalidIdentities(t *testing.T) {
	store := openTestStore(t)
	openGateFixture(t, store)

	if _, err := store.OpenGate(context.Background(), OpenGateRequest{
		SessionID: catalogSession, LeaseEpoch: 1, Gate: testGate("gate-a", 5),
	}); !errors.As(err, new(*InvalidIdentityError)) {
		t.Fatalf("empty tenant = %T %v", err, err)
	}
	if _, err := store.ReadGates(context.Background(), ReadGatesRequest{TenantID: catalogTenant}); !errors.As(err, new(*InvalidIdentityError)) {
		t.Fatalf("empty session = %T %v", err, err)
	}
	if _, err := store.ResolveGate(context.Background(), ResolveGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1,
	}); err == nil {
		t.Fatal("an empty gate id was accepted")
	} else {
		assertCatalogCode(t, err, CatalogErrorInvalid)
	}
	invalid := testGate("gate-a", 5)
	invalid.Kind = ""
	if _, err := store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: invalid,
	}); err == nil {
		t.Fatal("a gate projection with no kind was accepted")
	} else {
		assertCatalogCode(t, err, CatalogErrorInvalid)
	}
	assertNoGateIntent(t, store, "gate-a")
}

// --- the reader validates what the writer could not -----------------------

// TestReadGatesFailsClosedOnAGateAboveItsOwnTip reaches the one inconsistency a
// gate write cannot prevent: UpdateCatalogHostState replaces the whole
// projection wholesale and is the Host's own re-projection path, so it can
// store a gate naming an event past the record's durable tip. A page like that
// violates its own contract, so the read refuses it rather than publishing it.
func TestReadGatesFailsClosedOnAGateAboveItsOwnTip(t *testing.T) {
	store := openTestStore(t)
	openGateFixture(t, store)
	req := testHostStateRequest(1)
	req.OpenGates = []sessionwire.GateProjection{testGate("gate-a", req.LastJournalSeq+1)}
	if _, err := store.UpdateCatalogHostState(context.Background(), req); err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}

	_, err := store.ReadGates(context.Background(), ReadGatesRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	})
	assertCatalogCode(t, err, CatalogErrorSequence)
}

// --- lifecycle ------------------------------------------------------------

// declaredGateOperations enumerates gates.go's public Store operations from the
// source, for the same reason declaredCatalogOperations does it for catalog.go:
// the file that declares an operation is the file the close test enumerates, so
// a new one cannot be added without being exercised here.
func declaredGateOperations(t *testing.T) map[string]bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "gates.go", nil, 0)
	if err != nil {
		t.Fatalf("parse gates.go: %v", err)
	}
	operations := map[string]bool{}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv == nil || !function.Name.IsExported() {
			continue
		}
		receiver, ok := function.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		if name, ok := receiver.X.(*ast.Ident); ok && name.Name == "Store" {
			operations[function.Name.Name] = true
		}
	}
	if len(operations) == 0 {
		t.Fatal("no public Store operations were found in gates.go; the enumerator is not reaching the declarations")
	}
	return operations
}

func TestGateOperationsRefuseAfterClose(t *testing.T) {
	store, err := Open(context.Background(), memstore.New())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	openGateFixture(t, store)
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	operations := map[string]func() error{
		"OpenGate": func() error {
			_, err := store.OpenGate(context.Background(), OpenGateRequest{
				TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: testGate("gate-a", 5),
			})
			return err
		},
		"ResolveGate": func() error {
			_, err := store.ResolveGate(context.Background(), ResolveGateRequest{
				TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, GateID: "gate-a",
			})
			return err
		},
		"ReadGates": func() error {
			_, err := store.ReadGates(context.Background(), ReadGatesRequest{
				TenantID: catalogTenant, SessionID: catalogSession,
			})
			return err
		},
		"ListDueGates": func() error {
			_, err := store.ListDueGates(context.Background(), ListDueGatesRequest{
				DueAtOrBefore: catalogDeadline, Limit: 10,
			})
			return err
		},
	}

	declared := declaredGateOperations(t)
	for name := range declared {
		if operations[name] == nil {
			t.Errorf("gates.go declares the public operation %s and this test does not exercise it", name)
		}
	}
	for name := range operations {
		if !declared[name] {
			t.Errorf("this test exercises %s, which gates.go no longer declares (was it moved to another file?)", name)
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

var (
	_ func(*Store, context.Context, OpenGateRequest) (CatalogEntry, error)          = (*Store).OpenGate
	_ func(*Store, context.Context, ResolveGateRequest) (CatalogEntry, error)       = (*Store).ResolveGate
	_ func(*Store, context.Context, ReadGatesRequest) (sessionwire.GatePage, error) = (*Store).ReadGates
	_ func(*Store, context.Context, ListDueGatesRequest) ([]DueGate, error)         = (*Store).ListDueGates
)

// --- the due view ---------------------------------------------------------

// mustPrepareSession creates one session and gives it a durable journal tip, so
// gates opened on it can name events that already exist.
func mustPrepareSession(
	t *testing.T,
	store *Store,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	tip uint64,
) {
	t.Helper()
	mustCreateSession(t, store, tenant, session, catalogActiveAt)
	if _, err := store.UpdateCatalogHostState(context.Background(), UpdateCatalogHostStateRequest{
		TenantID: tenant, SessionID: session, LeaseEpoch: 1,
		State: sessionwire.SessionStateRunning, Residency: sessionwire.SessionResidencyResident,
		LastActiveAt: catalogActiveAt, LastJournalSeq: tip, LastEventID: "event-tip",
	}); err != nil {
		t.Fatalf("UpdateCatalogHostState(%s/%s): %v", tenant, session, err)
	}
}

func mustOpenGateOn(
	t *testing.T,
	store *Store,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	gate sessionwire.GateProjection,
) {
	t.Helper()
	if _, err := store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: tenant, SessionID: session, LeaseEpoch: 1, Gate: gate,
	}); err != nil {
		t.Fatalf("OpenGate(%s/%s/%s): %v", tenant, session, gate.GateID, err)
	}
}

func gateWithDeadline(gate sessionwire.GateProjection, deadline time.Time) sessionwire.GateProjection {
	gate.Deadline = deadline
	return gate
}

// putRawGateIntent writes one intent straight to the provider. It is how a test
// presents the reader with a state the write paths refuse to produce — an
// intent with no matching open gate, one filed under another identity, or one
// whose bytes are corrupt.
func putRawGateIntent(
	t *testing.T,
	store *Store,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	key sessionwire.GateID,
	value []byte,
	deadline time.Time,
) {
	t.Helper()
	scope, err := store.deriveSessionScope(tenant, session)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	if _, _, err := store.backend.OrderedIndex.Create(
		context.Background(), gateIntentID(scope, key), scope.SessionNamespace, value, storage.Rank{}, gateDue(deadline),
	); err != nil {
		t.Fatalf("seed intent: %v", err)
	}
}

func mustEncodeGateIntent(t *testing.T, intent gateIntent) []byte {
	t.Helper()
	value, err := encodeGateIntent(intent)
	if err != nil {
		t.Fatalf("encodeGateIntent: %v", err)
	}
	return value
}

func dueGateIDs(due []DueGate) []string {
	ids := make([]string, 0, len(due))
	for _, entry := range due {
		ids = append(ids, string(entry.SessionID)+"/"+string(entry.Gate.GateID))
	}
	return ids
}

func mustListDueGates(t *testing.T, store *Store, before time.Time) []DueGate {
	t.Helper()
	due, err := store.ListDueGates(context.Background(), ListDueGatesRequest{DueAtOrBefore: before, Limit: 50})
	if err != nil {
		t.Fatalf("ListDueGates: %v", err)
	}
	return due
}

// TestListDueGatesReportsOnlyGatesTheProjectionStillOpens is the reader half of
// the ordering contract. Every case below is a state a crash or a wholesale
// re-projection can really leave behind, and only the one with a matching
// durable open event may be reported.
func TestListDueGatesReportsOnlyGatesTheProjectionStillOpens(t *testing.T) {
	store := openTestStore(t)
	bound := catalogDeadline
	mustPrepareSession(t, store, catalogTenant, "session-1", 100)
	mustPrepareSession(t, store, catalogTenant, "session-2", 100)

	// Due and still open.
	mustOpenGateOn(t, store, catalogTenant, "session-1", gateWithDeadline(testGate("gate-due", 5), bound.Add(-time.Hour)))
	// Open but not yet due.
	mustOpenGateOn(t, store, catalogTenant, "session-1", gateWithDeadline(testGate("gate-later", 6), bound.Add(time.Hour)))
	// Due and then resolved: a tombstoned intent leaves the due pages.
	mustOpenGateOn(t, store, catalogTenant, "session-2", gateWithDeadline(testGate("gate-resolved", 7), bound.Add(-time.Hour)))
	if _, err := store.ResolveGate(context.Background(), ResolveGateRequest{
		TenantID: catalogTenant, SessionID: "session-2", LeaseEpoch: 1, GateID: "gate-resolved",
	}); err != nil {
		t.Fatalf("ResolveGate: %v", err)
	}
	// An interrupted open: the intent is durable, the projection never
	// committed. This is the remnant the reader exists to validate away.
	//
	// It is seeded on the session that DOES have an open gate, so a reader
	// that reported a due intent without checking which gate it names would
	// report that session's gate twice rather than silently reporting nothing.
	orphan := gateIntent{
		TenantID: catalogTenant, SessionID: "session-1", GateID: "gate-orphan",
		OpenedEventID: "event-gate-orphan", OpenedJournalSeq: 9, Deadline: bound.Add(-time.Hour),
	}
	putRawGateIntent(t, store, catalogTenant, "session-1", "gate-orphan", mustEncodeGateIntent(t, orphan), orphan.Deadline)
	// An intent whose session has no catalog record at all.
	stray := orphan
	stray.SessionID = "session-missing"
	stray.GateID = "gate-stray"
	stray.OpenedEventID = "event-gate-stray"
	putRawGateIntent(t, store, catalogTenant, "session-missing", "gate-stray", mustEncodeGateIntent(t, stray), stray.Deadline)

	due := mustListDueGates(t, store, bound)
	got := dueGateIDs(due)
	if len(got) != 1 || got[0] != "session-1/gate-due" {
		t.Fatalf("due gates = %v, want [session-1/gate-due]", got)
	}
	if due[0].TenantID != catalogTenant {
		t.Fatalf("due gate tenant = %q, want %q", due[0].TenantID, catalogTenant)
	}
	// The projection is returned, not a reconstruction of it from the index.
	if due[0].Gate.Prompt.Title != "Confirm" || due[0].Gate.OpenedJournalSeq != 5 {
		t.Fatalf("due gate did not carry the durable projection: %+v", due[0].Gate)
	}
}

// TestListDueGatesRejectsAnIntentThatNamesAnotherGate proves the reader holds a
// stored intent to the identity the provider filed it under rather than
// trusting either one alone.
func TestListDueGatesRejectsAnIntentThatNamesAnotherGate(t *testing.T) {
	store := openTestStore(t)
	mustPrepareSession(t, store, catalogTenant, "session-1", 100)
	impostor := gateIntent{
		TenantID: catalogTenant, SessionID: "session-1", GateID: "gate-b",
		OpenedEventID: "event-gate-b", OpenedJournalSeq: 5, Deadline: catalogDeadline.Add(-time.Hour),
	}
	putRawGateIntent(t, store, catalogTenant, "session-1", "gate-a", mustEncodeGateIntent(t, impostor), impostor.Deadline)

	_, err := store.ListDueGates(context.Background(), ListDueGatesRequest{DueAtOrBefore: catalogDeadline, Limit: 10})
	got := assertCatalogCode(t, err, CatalogErrorIdentity)
	if !strings.HasPrefix(got.Field, "due_gates[0].") {
		t.Fatalf("failure field = %q, want the failing row's position", got.Field)
	}
}

// TestListDueGatesRejectsAnIntentFiledUnderAnotherSession covers the other half
// of that identity: the record's bytes name a session, and the ordering scope
// it was filed under must be that session's.
func TestListDueGatesRejectsAnIntentFiledUnderAnotherSession(t *testing.T) {
	store := openTestStore(t)
	mustPrepareSession(t, store, catalogTenant, "session-1", 100)
	mustPrepareSession(t, store, catalogTenant, "session-2", 100)
	misfiled := gateIntent{
		TenantID: catalogTenant, SessionID: "session-1", GateID: "gate-a",
		OpenedEventID: "event-gate-a", OpenedJournalSeq: 5, Deadline: catalogDeadline.Add(-time.Hour),
	}
	// Filed under session-2's order scope while claiming session-1.
	putRawGateIntent(t, store, catalogTenant, "session-2", "gate-a", mustEncodeGateIntent(t, misfiled), misfiled.Deadline)

	_, err := store.ListDueGates(context.Background(), ListDueGatesRequest{DueAtOrBefore: catalogDeadline, Limit: 10})
	got := assertCatalogCode(t, err, CatalogErrorIdentity)
	if got.Field != "due_gates[0].ordering_scope" {
		t.Fatalf("failure field = %q, want due_gates[0].ordering_scope", got.Field)
	}
}

func TestListDueGatesFailsClosedOnACorruptIntent(t *testing.T) {
	store := openTestStore(t)
	mustPrepareSession(t, store, catalogTenant, "session-1", 100)
	putRawGateIntent(t, store, catalogTenant, "session-1", "gate-a", []byte("{not json"), catalogDeadline.Add(-time.Hour))

	_, err := store.ListDueGates(context.Background(), ListDueGatesRequest{DueAtOrBefore: catalogDeadline, Limit: 10})
	assertCatalogCode(t, err, CatalogErrorMalformed)
}

func TestListDueGatesRejectsAnInvalidRequest(t *testing.T) {
	store := openTestStore(t)
	for _, tt := range []struct {
		name string
		req  ListDueGatesRequest
	}{
		{"negative limit", ListDueGatesRequest{DueAtOrBefore: catalogDeadline, Limit: -1}},
		{"limit above the page ceiling", ListDueGatesRequest{DueAtOrBefore: catalogDeadline, Limit: storage.MaxOrderedPageLimit + 1}},
		{"zero bound", ListDueGatesRequest{Limit: 10}},
		{"unrepresentable bound", ListDueGatesRequest{DueAtOrBefore: time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC), Limit: 10}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := store.ListDueGates(context.Background(), tt.req); err == nil {
				t.Fatal("an invalid due request was accepted")
			} else {
				assertCatalogCode(t, err, CatalogErrorInvalid)
			}
		})
	}
}

func TestListDueGatesHonoursItsLimit(t *testing.T) {
	store := openTestStore(t)
	mustPrepareSession(t, store, catalogTenant, "session-1", 100)
	for i := range 3 {
		mustOpenGateOn(t, store, catalogTenant, "session-1",
			gateWithDeadline(testGate("gate-"+strconv.Itoa(i), uint64(i+1)), catalogDeadline.Add(-time.Hour)))
	}
	due, err := store.ListDueGates(context.Background(), ListDueGatesRequest{DueAtOrBefore: catalogDeadline, Limit: 2})
	if err != nil {
		t.Fatalf("ListDueGates: %v", err)
	}
	if len(due) != 2 {
		t.Fatalf("due gates = %v, want 2 rows", dueGateIDs(due))
	}
}

// --- redaction ------------------------------------------------------------

// TestGateFailuresAreRedacted holds every gate path to the same rule the rest
// of the package obeys: a returned error names the stage that failed and never
// the provider's text, the tenant, the session, the gate, or the prompt a
// caller supplied.
func TestGateFailuresAreRedacted(t *testing.T) {
	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store := openStore(t, base)
	openGateFixture(t, store)
	secret := errors.New("provider path /var/secret/tenant-a/session-a/gate-a")
	hostile.failGets(secret)

	secrets := []string{"secret", "tenant-a", "session-a", "gate-a", "Confirm", "Proceed?"}
	for name, call := range map[string]func() error{
		"OpenGate": func() error {
			_, err := store.OpenGate(context.Background(), OpenGateRequest{
				TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: testGate("gate-a", 5),
			})
			return err
		},
		"ResolveGate": func() error {
			_, err := store.ResolveGate(context.Background(), ResolveGateRequest{
				TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, GateID: "gate-a",
			})
			return err
		},
		"ReadGates": func() error {
			_, err := store.ReadGates(context.Background(), ReadGatesRequest{
				TenantID: catalogTenant, SessionID: catalogSession,
			})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := call()
			assertCatalogCode(t, err, CatalogErrorBackend)
			for _, leak := range secrets {
				if strings.Contains(err.Error(), leak) {
					t.Fatalf("%q leaked into %q", leak, err.Error())
				}
			}
			if !errors.Is(err, secret) {
				t.Fatal("cause was not preserved for errors.Is")
			}
		})
	}
}

// --- the intent codec -----------------------------------------------------

func testGateIntent() gateIntent {
	return gateIntent{
		TenantID:         catalogTenant,
		SessionID:        catalogSession,
		GateID:           "gate-a",
		OpenedEventID:    "event-gate-a",
		OpenedJournalSeq: 5,
		Deadline:         catalogDeadline,
	}
}

func TestGateIntentRoundTripsToACanonicalForm(t *testing.T) {
	intent := testGateIntent()
	intent.Deadline = catalogDeadline.In(time.FixedZone("elsewhere", 3600))
	encoded := mustEncodeGateIntent(t, intent)
	decoded, err := decodeGateIntent(encoded)
	if err != nil {
		t.Fatalf("decodeGateIntent: %v", err)
	}
	if decoded.Deadline.Location() != time.UTC {
		t.Fatalf("deadline was not canonicalized to UTC: %v", decoded.Deadline)
	}
	if !decoded.Deadline.Equal(intent.Deadline) {
		t.Fatalf("deadline instant changed: %v -> %v", intent.Deadline, decoded.Deadline)
	}
	reencoded := mustEncodeGateIntent(t, decoded)
	if !bytes.Equal(encoded, reencoded) {
		t.Fatalf("round trip changed the canonical bytes:\n%s\n%s", encoded, reencoded)
	}
}

func TestGateIntentDecodeFailsClosed(t *testing.T) {
	valid := mustEncodeGateIntent(t, testGateIntent())
	var members map[string]json.RawMessage
	if err := json.Unmarshal(valid, &members); err != nil {
		t.Fatalf("a stored intent is not JSON: %v", err)
	}
	mutate := func(apply func(map[string]json.RawMessage)) []byte {
		copied := make(map[string]json.RawMessage, len(members))
		for name, value := range members {
			copied[name] = value
		}
		apply(copied)
		encoded, err := json.Marshal(copied)
		if err != nil {
			t.Fatalf("marshal variant: %v", err)
		}
		return encoded
	}
	for _, tt := range []struct {
		name  string
		value []byte
		want  CatalogErrorCode
	}{
		{"empty", nil, CatalogErrorMalformed},
		{"too large", append(append([]byte(nil), valid...), bytes.Repeat([]byte(" "), MaxGateIntentBytes)...), CatalogErrorTooLarge},
		{"not json", []byte("{"), CatalogErrorMalformed},
		{"trailing content", append(append([]byte(nil), valid...), '{'), CatalogErrorMalformed},
		{"unknown version", mutate(func(m map[string]json.RawMessage) { m["record_version"] = json.RawMessage("2") }), CatalogErrorVersion},
		{"undeclared member", mutate(func(m map[string]json.RawMessage) { m["surprise"] = json.RawMessage("1") }), CatalogErrorMalformed},
		{"empty tenant", mutate(func(m map[string]json.RawMessage) { m["tenant_id"] = json.RawMessage(`""`) }), CatalogErrorInvalid},
		{"empty session", mutate(func(m map[string]json.RawMessage) { m["session_id"] = json.RawMessage(`""`) }), CatalogErrorInvalid},
		{"empty gate", mutate(func(m map[string]json.RawMessage) { m["gate_id"] = json.RawMessage(`""`) }), CatalogErrorInvalid},
		{"empty opening event", mutate(func(m map[string]json.RawMessage) { m["opened_event_id"] = json.RawMessage(`""`) }), CatalogErrorInvalid},
		{"zero opening sequence", mutate(func(m map[string]json.RawMessage) { m["opened_journal_seq"] = json.RawMessage("0") }), CatalogErrorInvalid},
		{"zero deadline", mutate(func(m map[string]json.RawMessage) { m["deadline"] = json.RawMessage(`"0001-01-01T00:00:00Z"`) }), CatalogErrorInvalid},
		{"unrepresentable deadline", mutate(func(m map[string]json.RawMessage) { m["deadline"] = json.RawMessage(`"3000-01-01T00:00:00Z"`) }), CatalogErrorInvalid},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeGateIntent(tt.value); err == nil {
				t.Fatal("a malformed intent decoded")
			} else {
				assertCatalogCode(t, err, tt.want)
			}
		})
	}
}

func TestGateIntentEncodeRefusesAnInvalidIntent(t *testing.T) {
	invalid := testGateIntent()
	invalid.OpenedJournalSeq = 0
	if _, err := encodeGateIntent(invalid); err == nil {
		t.Fatal("an intent naming no opening event encoded")
	} else {
		assertCatalogCode(t, err, CatalogErrorInvalid)
	}
}

// --- concurrency ----------------------------------------------------------

// TestConcurrentOpenGatesKeepEveryGate opens two gates at once on one record.
// The record is one compare-and-swap, so one writer legitimately loses; what it
// must not do is silently drop the other writer's gate, and its retry must
// find its own intent rather than colliding with it.
func TestConcurrentOpenGatesKeepEveryGate(t *testing.T) {
	store := openTestStore(t)
	openGateFixture(t, store)

	open := func(gate sessionwire.GateProjection) error {
		var err error
		for range 8 {
			_, err = store.OpenGate(context.Background(), OpenGateRequest{
				TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: gate,
			})
			var catalog *CatalogError
			if err == nil || !errors.As(err, &catalog) || catalog.Code != CatalogErrorConflict {
				return err
			}
		}
		return err
	}
	var wait sync.WaitGroup
	errs := make([]error, 2)
	gates := []sessionwire.GateProjection{testGate("gate-a", 3), testGate("gate-b", 7)}
	for i := range gates {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errs[i] = open(gates[i])
		}()
	}
	wait.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("OpenGate(%s): %v", gates[i].GateID, err)
		}
	}
	if page := mustReadGates(t, store); len(page.Gates) != 2 {
		t.Fatalf("concurrent opens left %v, want both gates", gateIDs(page))
	}
}

// --- guards the ordinary paths cannot reach -------------------------------

func TestOpenGateRefusesAnUnrepresentableDeadline(t *testing.T) {
	store := openTestStore(t)
	entry := openGateFixture(t, store)

	// Core accepts any nonzero deadline, so this is this package's own bound:
	// an instant outside the representable range would wrap into a due time at
	// the wrong end of history rather than sorting late.
	far := gateWithDeadline(testGate("gate-a", 5), time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	_, err := store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: far,
	})
	if got := assertCatalogCode(t, err, CatalogErrorInvalid); got.Field != "gate.deadline" {
		t.Fatalf("failure field = %q, want gate.deadline", got.Field)
	}
	assertCatalogUnchanged(t, store, entry)
	assertNoGateIntent(t, store, "gate-a")
}

// TestOpenGateRefusesToReopenAResolvedGate covers what the tombstone is for: a
// gate identity is spent once it has been resolved, so a late or replayed open
// cannot resurrect it under the same id.
func TestOpenGateRefusesToReopenAResolvedGate(t *testing.T) {
	store := openTestStore(t)
	openGateFixture(t, store)
	gate := testGate("gate-a", 5)
	mustOpenGate(t, store, 1, gate)
	entry, err := store.ResolveGate(context.Background(), ResolveGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, GateID: "gate-a",
	})
	if err != nil {
		t.Fatalf("ResolveGate: %v", err)
	}

	_, err = store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: gate,
	})
	assertCatalogCode(t, err, CatalogErrorDeleted)
	assertCatalogUnchanged(t, store, entry)
}

// TestOpenGateRefusesAnIdentityHeldByAnotherIntent reaches the check that the
// projection cannot make. UpdateCatalogHostState replaces the open gates
// wholesale and leaves intents alone, so after one the identity looks free in
// the projection while its deadline intent still names a different gate.
func TestOpenGateRefusesAnIdentityHeldByAnotherIntent(t *testing.T) {
	store := openTestStore(t)
	openGateFixture(t, store)
	mustOpenGate(t, store, 1, testGate("gate-a", 5))
	cleared := testHostStateRequest(1)
	entry, err := store.UpdateCatalogHostState(context.Background(), cleared)
	if err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}
	if len(entry.Record.OpenGates) != 0 {
		t.Fatal("the wholesale re-projection did not clear the open gates")
	}

	different := testGate("gate-a", 6)
	_, err = store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: different,
	})
	if got := assertCatalogCode(t, err, CatalogErrorConflict); got.Field != "gate_intent" {
		t.Fatalf("failure field = %q, want gate_intent", got.Field)
	}
	assertCatalogUnchanged(t, store, entry)
}

func TestResolveGateRefusesASupersededLeaseEpoch(t *testing.T) {
	store := openTestStore(t)
	openGateFixture(t, store)
	mustOpenGate(t, store, 1, testGate("gate-a", 5))
	if _, err := store.UpdateCatalogHostState(context.Background(), testHostStateRequest(4)); err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}

	_, err := store.ResolveGate(context.Background(), ResolveGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 3, GateID: "gate-a",
	})
	if got := assertCatalogCode(t, err, CatalogErrorEpoch); got.Epoch != 4 {
		t.Fatalf("epoch failure reported %d, want the committed high-water 4", got.Epoch)
	}
	if due := gateIntentRecord(t, store, "gate-a").Due; due.State != storage.DueAt {
		t.Fatalf("a refused resolve retired the deadline intent anyway: %+v", due)
	}
}

// TestResolveGateIsIdempotentAndQuiet covers the three states a resolve can
// arrive in that are not "the gate is open with its intent": a gate that was
// never projected, a gate projected without an intent, and a repeat of a
// completed resolve. None may fail, and none may write.
func TestResolveGateIsIdempotentAndQuiet(t *testing.T) {
	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	store := openStore(t, base)
	openGateFixture(t, store)

	resolve := func(gate sessionwire.GateID) CatalogEntry {
		t.Helper()
		entry, err := store.ResolveGate(context.Background(), ResolveGateRequest{
			TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, GateID: gate,
		})
		if err != nil {
			t.Fatalf("ResolveGate(%s): %v", gate, err)
		}
		return entry
	}

	// Never projected and never indexed.
	before, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	if entry := resolve("gate-unknown"); entry.Revision != before.Revision {
		t.Fatalf("resolving an unknown gate rewrote the record: revision %d -> %d", before.Revision, entry.Revision)
	}

	// Projected wholesale, so it has no deadline intent to retire.
	projected := testHostStateRequest(1)
	projected.OpenGates = []sessionwire.GateProjection{testGate("gate-p", 5)}
	if _, err := store.UpdateCatalogHostState(context.Background(), projected); err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}
	if page := mustReadGates(t, store); len(page.Gates) != 1 {
		t.Fatalf("fixture did not project the gate: %v", gateIDs(page))
	}
	resolve("gate-p")
	if page := mustReadGates(t, store); len(page.Gates) != 0 {
		t.Fatalf("resolving an intentless gate left it open: %v", gateIDs(page))
	}

	// A repeat of a completed resolve must not reach the provider again: the
	// intent is already a tombstone and the projection already has no gate.
	mustOpenGate(t, store, 1, testGate("gate-a", 6))
	resolve("gate-a")
	deletes, updates := ordered.countOf("delete"), ordered.countOf("update")
	settled, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	if repeat := resolve("gate-a"); repeat.Revision != settled.Revision {
		t.Fatalf("a repeated resolve advanced the revision %d -> %d", settled.Revision, repeat.Revision)
	}
	if got := ordered.countOf("delete"); got != deletes {
		t.Fatalf("a repeated resolve issued %d more deletes", got-deletes)
	}
	if got := ordered.countOf("update"); got != updates {
		t.Fatalf("a repeated resolve rewrote the record %d more times", got-updates)
	}
	if page := mustReadGates(t, store); len(page.Gates) != 0 {
		t.Fatalf("a repeated resolve reopened something: %v", gateIDs(page))
	}
}

// TestListDueGatesDoesNotAnswerOneTenantWithAnother pins the identity a due
// page caches a session record under. Session ids are unique within a tenant
// and not across them, so two tenants can legitimately hold the same session id
// and the same gate id; a page that cached by session alone would validate one
// tenant's intent against the other tenant's projection and report a gate that
// tenant never opened.
func TestListDueGatesDoesNotAnswerOneTenantWithAnother(t *testing.T) {
	store := openTestStore(t)
	const shared = sessionwire.SessionID("session-shared")
	mustPrepareSession(t, store, catalogTenant, shared, 100)
	mustPrepareSession(t, store, catalogOtherTenant, shared, 100)

	due := gateWithDeadline(testGate("gate-a", 5), catalogDeadline.Add(-time.Hour))
	mustOpenGateOn(t, store, catalogTenant, shared, due)
	// The other tenant has the same identities in an intent alone: an open
	// that never committed. Nothing about it may be answered by the first
	// tenant's record.
	orphan := gateIntent{
		TenantID: catalogOtherTenant, SessionID: shared, GateID: due.GateID,
		OpenedEventID: due.OpenedEventID, OpenedJournalSeq: due.OpenedJournalSeq, Deadline: due.Deadline,
	}
	putRawGateIntent(t, store, catalogOtherTenant, shared, due.GateID, mustEncodeGateIntent(t, orphan), orphan.Deadline)

	rows := mustListDueGates(t, store, catalogDeadline)
	if len(rows) != 1 {
		t.Fatalf("due gates = %d rows, want only the tenant that really opened one", len(rows))
	}
	if rows[0].TenantID != catalogTenant {
		t.Fatalf("due gate tenant = %q, want %q", rows[0].TenantID, catalogTenant)
	}
}

// TestListDueGatesChecksEveryRowsFiling extends the identity check past the
// first row of a session. A page resolves each session once, so a misfiled
// intent arriving behind a well-filed one for the same session is exactly the
// row a per-session check would wave through.
func TestListDueGatesChecksEveryRowsFiling(t *testing.T) {
	store := openTestStore(t)
	mustPrepareSession(t, store, catalogTenant, "session-1", 100)
	mustPrepareSession(t, store, catalogTenant, "session-2", 100)
	// Sorted first by an earlier deadline, so it resolves session-1 into the
	// page's cache before the misfiled row is read.
	mustOpenGateOn(t, store, catalogTenant, "session-1",
		gateWithDeadline(testGate("gate-a", 5), catalogDeadline.Add(-2*time.Hour)))
	misfiled := gateIntent{
		TenantID: catalogTenant, SessionID: "session-1", GateID: "gate-b",
		OpenedEventID: "event-gate-b", OpenedJournalSeq: 6, Deadline: catalogDeadline.Add(-time.Hour),
	}
	putRawGateIntent(t, store, catalogTenant, "session-2", "gate-b", mustEncodeGateIntent(t, misfiled), misfiled.Deadline)

	_, err := store.ListDueGates(context.Background(), ListDueGatesRequest{DueAtOrBefore: catalogDeadline, Limit: 10})
	if got := assertCatalogCode(t, err, CatalogErrorIdentity); got.Field != "due_gates[1].ordering_scope" {
		t.Fatalf("failure field = %q, want due_gates[1].ordering_scope", got.Field)
	}
}
