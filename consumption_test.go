package sessionstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

const (
	// consumptionEpoch is the residency epoch the cursor fixture writes under.
	// It is deliberately neither zero nor one, so a fence that compared against
	// a constant rather than against the stored high-water is visible.
	consumptionEpoch = uint64(9)

	// consumptionOrder is a cursor position that is not any order the provider
	// allocates in these fixtures, so a test that meant to exercise the ORDER
	// fence cannot pass by coinciding with a real acceptance order.
	consumptionOrder = uint64(4096)
)

// consumptionUpdatedAt is the instant the fixture's clock reads. It is the
// disposition settlement fixture's own start, because the state matrix this
// file ranges over builds claimed and applying records through the real
// settlement operations, and a clock later than that fixture's expiry would
// refuse them.
var consumptionUpdatedAt = settlementNow

// consumptionFixture opens a store on a DISPOSITION-bound session, which is the
// only kind of session anything can consume commands for.
//
// The catalog is created rather than assumed, and that is the point of the
// helper rather than a convenience: AcquireResidency refuses any session whose
// binding is not ProtocolModeDisposition, so a fixture that consumed a legacy
// session would be testing an operation no Host can reach.
func consumptionFixture(t *testing.T, backend *storage.Composite) *Store {
	t.Helper()
	store := openStore(t, backend, WithClock(newMovableClock(consumptionUpdatedAt)))
	createDispositionCatalog(t, store)
	return store
}

// createDispositionCatalogFor is createDispositionCatalog for a session other
// than the fixture's own, so the cross-session tests have real neighbours
// rather than scopes that merely fail to exist.
func createDispositionCatalogFor(
	t *testing.T, s *Store, tenant sessionwire.TenantID, session sessionwire.SessionID,
) {
	t.Helper()
	req := testCreateRequest()
	req.TenantID, req.SessionID = tenant, session
	req.Binding = testSessionBinding()
	if _, _, err := s.CreateCatalogEntry(context.Background(), req); err != nil {
		t.Fatalf("CreateCatalogEntry(%q,%q): %v", tenant, session, err)
	}
}

func testSaveCursorRequest(epoch, order uint64) SaveDispositionCommandCursorRequest {
	return SaveDispositionCommandCursorRequest{
		TenantID:      catalogTenant,
		SessionID:     catalogSession,
		LeaseEpoch:    epoch,
		ConsumedOrder: order,
	}
}

func testLoadCursorRequest() LoadDispositionCommandCursorRequest {
	return LoadDispositionCommandCursorRequest{TenantID: catalogTenant, SessionID: catalogSession}
}

func mustSaveCursor(t *testing.T, store *Store, epoch, order uint64) DispositionCommandCursorEntry {
	t.Helper()
	entry, err := store.SaveDispositionCommandCursor(context.Background(), testSaveCursorRequest(epoch, order))
	if err != nil {
		t.Fatalf("SaveDispositionCommandCursor(epoch=%d order=%d): %v", epoch, order, err)
	}
	return entry
}

func mustLoadCursor(t *testing.T, store *Store) DispositionCommandCursorEntry {
	t.Helper()
	entry, err := store.LoadDispositionCommandCursor(context.Background(), testLoadCursorRequest())
	if err != nil {
		t.Fatalf("LoadDispositionCommandCursor: %v", err)
	}
	return entry
}

// admitSessionCommands admits count disposition commands into one session, and
// returns the CommandIDs in ADMISSION order.
//
// The identities are minted so that their lexical order is the REVERSE of the
// admission order. That is the whole point of this helper: if the listing under
// test sorted by stable key, or the provider's acceptance order happened to
// agree with the key order, the ordering assertions would pass for a reason
// that has nothing to do with acceptance order.
func admitSessionCommands(
	t *testing.T,
	store *Store,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	tag string,
	count int,
) []sessionwire.CommandID {
	t.Helper()
	admitted := make([]sessionwire.CommandID, 0, count)
	for i := range count {
		// The identity carries its session, so two sessions in one store never
		// collide, and its ordinal DESCENDS, so lexical order is the reverse of
		// admission order.
		command := sessionwire.CommandID(fmt.Sprintf("command/%s/%s/%s/%04d", tenant, session, tag, count-i))
		req := dispositionRequest()
		req.TenantID, req.SessionID, req.CommandID = tenant, session, command
		if _, created, err := store.AdmitDispositionCommand(context.Background(), req); err != nil {
			t.Fatalf("AdmitDispositionCommand(%s): %v", command, err)
		} else if !created {
			t.Fatalf("AdmitDispositionCommand(%s) reported an existing command for a fresh identity", command)
		}
		admitted = append(admitted, command)
	}
	return admitted
}

// walkSessionCommands pages a whole session at one limit and returns every
// entry it saw, checking the per-page invariants as it goes: no page exceeds
// the limit, every returned order is STRICTLY above the bound that page was
// read at, and the reported continuation is the last row's order.
func walkSessionCommands(
	t *testing.T,
	store *Store,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	after uint64,
	limit int,
) []DispositionInboxEntry {
	t.Helper()
	var seen []DispositionInboxEntry
	for pages := 0; ; pages++ {
		if pages > 1000 {
			t.Fatal("paging did not terminate")
		}
		page, err := store.ListSessionDispositionCommands(
			context.Background(), ListSessionDispositionCommandsRequest{
				TenantID: tenant, SessionID: session, AfterOrder: after, Limit: limit,
			})
		if err != nil {
			t.Fatalf("ListSessionDispositionCommands(after=%d limit=%d): %v", after, limit, err)
		}
		if limit > 0 && len(page.Commands) > limit {
			t.Fatalf("page returned %d rows over a limit of %d", len(page.Commands), limit)
		}
		for _, entry := range page.Commands {
			if entry.AcceptedOrder <= after {
				t.Fatalf("order %d is not strictly after the bound %d", entry.AcceptedOrder, after)
			}
		}
		if len(page.Commands) == 0 {
			if page.NextAfterOrder != 0 {
				t.Fatalf("empty page reported a continuation at %d", page.NextAfterOrder)
			}
			return seen
		}
		last := page.Commands[len(page.Commands)-1].AcceptedOrder
		if page.NextAfterOrder != last {
			t.Fatalf("NextAfterOrder = %d, want the last row's order %d", page.NextAfterOrder, last)
		}
		seen = append(seen, page.Commands...)
		after = page.NextAfterOrder
	}
}

// walkExactly is walkSessionCommands for a caller that is about to INDEX the
// result. The count is stated rather than assumed, because a walk that returned
// fewer rows than expected would otherwise fail as an index panic — and a panic
// is not an assertion: it says a test crashed, not that a claim is false.
func walkExactly(
	t *testing.T,
	store *Store,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	after uint64,
	limit int,
	want int,
) []DispositionInboxEntry {
	t.Helper()
	seen := walkSessionCommands(t, store, tenant, session, after, limit)
	if len(seen) != want {
		t.Fatalf("walk after %d returned %d commands, want %d", after, len(seen), want)
	}
	return seen
}

// ---------------------------------------------------------------------------
// The release blocker this work exists to close
// ---------------------------------------------------------------------------

// TestBothOperationsReachASessionAHostCanOwn is the test the first version of
// this file did not have, and its absence is what let a whole release be built
// against an inbox no consumer could use.
//
// The chain is short and entirely outside this file. Host acquires residency
// through Store.AcquireResidency; AcquireResidency REFUSES any session whose
// catalog binding is not ProtocolModeDisposition; a session's protocol mode is
// a create-only immutable pin. So every session a Host can consume commands for
// is disposition-bound, and an operation that pins ProtocolModeLegacy on that
// scope is refused with a catalog conflict, permanently, with no retry that can
// help. The first version of this file did exactly that, and every one of its
// twenty tests passed, because every one of them used a legacy session.
//
// This test is therefore not about the store's internals at all. It asserts
// that the three operations are reachable on the ONE kind of session that
// matters, and it is deliberately written against the catalog a Host's own
// residency would require rather than against a scope this package invented.
func TestBothOperationsReachASessionAHostCanOwn(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New(), WithClock(newMovableClock(consumptionUpdatedAt)))
	createDispositionCatalog(t, store)

	// The binding this session carries is the one AcquireResidency demands.
	if got := testSessionBinding().ProtocolMode; got != ProtocolModeDisposition {
		t.Fatalf("the fixture binding is %q, so this test would prove nothing", got)
	}

	admitted := admitSessionCommands(t, store, catalogTenant, catalogSession, "host", 2)
	page, err := store.ListSessionDispositionCommands(
		context.Background(), ListSessionDispositionCommandsRequest{
			TenantID: catalogTenant, SessionID: catalogSession, Limit: 10,
		})
	if err != nil {
		t.Fatalf("ListSessionDispositionCommands on a session Host can own: %v", err)
	}
	if len(page.Commands) != len(admitted) {
		t.Fatalf("listing returned %d rows, want %d", len(page.Commands), len(admitted))
	}

	if _, err := store.LoadDispositionCommandCursor(context.Background(), testLoadCursorRequest()); err != nil {
		t.Fatalf("LoadDispositionCommandCursor on a session Host can own: %v", err)
	}
	saved, err := store.SaveDispositionCommandCursor(
		context.Background(), testSaveCursorRequest(consumptionEpoch, page.Commands[0].AcceptedOrder))
	if err != nil {
		t.Fatalf("SaveDispositionCommandCursor on a session Host can own: %v", err)
	}
	if saved.Cursor.ConsumedOrder != page.Commands[0].AcceptedOrder {
		t.Fatalf("saved position = %d, want %d", saved.Cursor.ConsumedOrder, page.Commands[0].AcceptedOrder)
	}

	// The CONTROL, and it is what makes the assertions above mean "disposition
	// works" rather than "everything works": the LEGACY command family is still
	// refused on this session, so the exclusivity that made the first version
	// unusable is real and is not something this change quietly removed.
	_, _, err = store.AdmitCommand(context.Background(), testAdmitRequest())
	var catalog *CatalogError
	if !errors.As(err, &catalog) || catalog.Code != CatalogErrorConflict {
		t.Fatalf("legacy AdmitCommand on a disposition session = %v, want a catalog conflict", err)
	}
}

// TestCursorOperationsRefuseASessionWithoutADispositionCatalog states the
// boundary of the zero-means-none answer, in the direction that is easy to
// over-read.
//
// Both cursor operations and the listing are NAMED READS of the disposition
// family and take that family's authority check: the catalog is read first,
// which verifies the session's collision witnesses and requires
// ProtocolModeDisposition. So "Load returns zero when none has been recorded"
// answers about a session that EXISTS and has no cursor; it does not promise
// that Load always succeeds. A session with no catalog at all, and a session
// bound to another protocol, are both refused.
//
// This is a RESIDUE, NOT A CLOSURE. It enumerates the two cursor operations and
// the listing, and two ways a session can fail to be a disposition session. It
// does NOT claim to enumerate every operation in this package whose answer
// depends on a catalog or a bound scope, nor every way a catalog read can fail,
// and a reader must not take the absence of an operation or a failure mode from
// this list as evidence about it.
func TestCursorOperationsRefuseASessionWithoutADispositionCatalog(t *testing.T) {
	t.Parallel()

	const absent = sessionwire.SessionID("session-never-created")
	const legacy = sessionwire.SessionID("session-legacy")

	store := consumptionFixture(t, memstore.New())
	// A real LEGACY session, created through the ordinary path, so the second
	// arm is about a protocol mismatch rather than about absence again.
	legacyReq := testCreateRequest()
	legacyReq.SessionID = legacy
	if _, _, err := store.CreateCatalogEntry(context.Background(), legacyReq); err != nil {
		t.Fatalf("CreateCatalogEntry(legacy): %v", err)
	}

	for _, session := range []sessionwire.SessionID{absent, legacy} {
		t.Run(string(session), func(t *testing.T) {
			if _, err := store.LoadDispositionCommandCursor(context.Background(),
				LoadDispositionCommandCursorRequest{TenantID: catalogTenant, SessionID: session}); err == nil {
				t.Fatal("LoadDispositionCommandCursor answered about a non-disposition session")
			}
			if _, err := store.SaveDispositionCommandCursor(context.Background(),
				SaveDispositionCommandCursorRequest{
					TenantID: catalogTenant, SessionID: session,
					LeaseEpoch: consumptionEpoch, ConsumedOrder: consumptionOrder,
				}); err == nil {
				t.Fatal("SaveDispositionCommandCursor wrote to a non-disposition session")
			}
			if _, err := store.ListSessionDispositionCommands(context.Background(),
				ListSessionDispositionCommandsRequest{
					TenantID: catalogTenant, SessionID: session, Limit: 10,
				}); err == nil {
				t.Fatal("ListSessionDispositionCommands answered about a non-disposition session")
			}
		})
	}

	// The CONTROL. Without it the three refusals above would also pass against
	// a store that refused every request naming any session at all.
	if _, err := store.LoadDispositionCommandCursor(context.Background(), testLoadCursorRequest()); err != nil {
		t.Fatalf("the fixture's own disposition session was refused: %v", err)
	}
}

// ---------------------------------------------------------------------------
// The per-session ordered listing
// ---------------------------------------------------------------------------

// TestListSessionCommandsIsAscendingAcceptanceOrderAtEveryLimitAndBound is the
// FOR-ALL, defended as one.
//
// Ascending acceptance order is a claim about every page size and every
// starting bound, so a single fixed fixture read once at one limit would be a
// sentence far wider than its probe: a listing that returned all rows in one
// page in admission order would satisfy it while paging, resuming and bounding
// were all untested. This walks a session of 37 commands at nine page sizes and
// then re-reads it from every one of its own row boundaries.
func TestListSessionCommandsIsAscendingAcceptanceOrderAtEveryLimitAndBound(t *testing.T) {
	t.Parallel()

	store := consumptionFixture(t, memstore.New())
	admitted := admitSessionCommands(t, store, catalogTenant, catalogSession, "mine", 37)

	for _, limit := range []int{1, 2, 3, 4, 5, 8, 13, 37, 100} {
		t.Run(fmt.Sprintf("limit_%d", limit), func(t *testing.T) {
			seen := walkSessionCommands(t, store, catalogTenant, catalogSession, 0, limit)
			if len(seen) != len(admitted) {
				t.Fatalf("walk returned %d commands, want %d", len(seen), len(admitted))
			}
			for i, entry := range seen {
				if entry.Record.Descriptor.CommandID != admitted[i] {
					t.Fatalf("position %d = %q, want %q: the walk is not in admission order",
						i, entry.Record.Descriptor.CommandID, admitted[i])
				}
				if i > 0 && entry.AcceptedOrder <= seen[i-1].AcceptedOrder {
					t.Fatalf("order at %d (%d) does not exceed its predecessor (%d)",
						i, entry.AcceptedOrder, seen[i-1].AcceptedOrder)
				}
			}
		})
	}

	full := walkExactly(t, store, catalogTenant, catalogSession, 0, 100, len(admitted))
	for cut := range full {
		bound := full[cut].AcceptedOrder
		suffix := walkSessionCommands(t, store, catalogTenant, catalogSession, bound, 5)
		if len(suffix) != len(full)-cut-1 {
			t.Fatalf("after order %d the walk returned %d rows, want %d", bound, len(suffix), len(full)-cut-1)
		}
		for i, entry := range suffix {
			if entry.Record.Descriptor.CommandID != full[cut+1+i].Record.Descriptor.CommandID {
				t.Fatalf("after order %d, position %d = %q, want %q", bound, i,
					entry.Record.Descriptor.CommandID, full[cut+1+i].Record.Descriptor.CommandID)
			}
		}
	}
}

// TestListSessionCommandsReadsOneSessionAndNotTheShardAroundIt is the
// per-session requirement, and it is stated against neighbours a cross-session
// query would actually pick up: another session of the same tenant, and another
// tenant. Both are admitted BEFORE and AFTER the subject's own commands, so a
// listing that leaked them would leak them at both ends of the order.
func TestListSessionCommandsReadsOneSessionAndNotTheShardAroundIt(t *testing.T) {
	t.Parallel()

	store := consumptionFixture(t, memstore.New())
	const otherSession = sessionwire.SessionID("session-b")
	const otherTenant = sessionwire.TenantID("tenant-b")
	createDispositionCatalogFor(t, store, catalogTenant, otherSession)
	createDispositionCatalogFor(t, store, otherTenant, catalogSession)

	before := admitSessionCommands(t, store, catalogTenant, otherSession, "before", 3)
	foreign := admitSessionCommands(t, store, otherTenant, catalogSession, "foreign", 3)
	mine := admitSessionCommands(t, store, catalogTenant, catalogSession, "mine", 5)
	after := admitSessionCommands(t, store, catalogTenant, otherSession, "after", 3)

	seen := walkSessionCommands(t, store, catalogTenant, catalogSession, 0, 2)
	if len(seen) != len(mine) {
		t.Fatalf("listing returned %d commands, want this session's %d", len(seen), len(mine))
	}
	foreignIDs := map[sessionwire.CommandID]bool{}
	for _, id := range append(append(append([]sessionwire.CommandID{}, before...), foreign...), after...) {
		foreignIDs[id] = true
	}
	for i, entry := range seen {
		d := entry.Record.Descriptor
		if d.TenantID != catalogTenant || d.SessionID != catalogSession {
			t.Fatalf("row %d belongs to (%q,%q)", i, d.TenantID, d.SessionID)
		}
		if d.CommandID != mine[i] {
			t.Fatalf("row %d = %q, want %q", i, d.CommandID, mine[i])
		}
	}
	// The CONTROL: the neighbours exist and are readable in their own
	// sessions. Without it "the listing returned only mine" would also pass
	// against a store that had never accepted the neighbours at all.
	if got := walkSessionCommands(t, store, catalogTenant, otherSession, 0, 100); len(got) != 6 {
		t.Fatalf("the same-tenant neighbour holds %d commands, want 6", len(got))
	}
	if got := walkSessionCommands(t, store, otherTenant, catalogSession, 0, 100); len(got) != 3 {
		t.Fatalf("the other tenant's session holds %d commands, want 3", len(got))
	}
	if len(foreignIDs) != 9 {
		t.Fatalf("fixture minted %d neighbour identities, want 9", len(foreignIDs))
	}
}

// dispositionStates builds one admitted command into each durable state through
// the REAL operations, so a state in this map is one the machine produces
// rather than one a fixture asserts it accepts.
//
// It is keyed on the state's own name and each entry returns the entry the
// transition left behind. `applied` runs the whole settlement protocol —
// claim, attempt, evidence — because that is the only way a disposition command
// reaches a terminal state.
var dispositionStates = map[string]func(t *testing.T, s *Store, entry DispositionInboxEntry) DispositionInboxEntry{
	"pending": func(_ *testing.T, _ *Store, entry DispositionInboxEntry) DispositionInboxEntry {
		return entry
	},
	"claimed": func(t *testing.T, s *Store, entry DispositionInboxEntry) DispositionInboxEntry {
		return fileDispositionClaim(t, s, entry,
			DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: settlementExpiry})
	},
	"applying": func(t *testing.T, s *Store, entry DispositionInboxEntry) DispositionInboxEntry {
		claimed := fileDispositionClaim(t, s, entry,
			DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: settlementExpiry})
		applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
		if err != nil {
			t.Fatalf("BeginDispositionAttempt: %v", err)
		}
		return applying
	},
}

// TestListSessionCommandsReturnsEveryDurableStateUnlikeTheDueView is where the
// new listing and ListDueDispositionCommands are held apart.
//
// They read the SAME rows through two different provider views, and the
// difference is not a filter this package applies: dispositionInboxDue files a
// terminal command NOT DUE, so it leaves the due view the instant it settles,
// while the acceptance-order stream never loses a row. A consumer that read the
// due view instead would silently stop seeing everything it had finished —
// which is exactly the mistake "filter the cross-session due query per session"
// would have been.
func TestListSessionCommandsReturnsEveryDurableStateUnlikeTheDueView(t *testing.T) {
	t.Parallel()

	for name, build := range dispositionStates {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := consumptionFixture(t, memstore.New())
			admitted, _, err := store.AdmitDispositionCommand(context.Background(), dispositionRequest())
			if err != nil {
				t.Fatalf("AdmitDispositionCommand: %v", err)
			}
			entry := build(t, store, admitted)

			seen := walkExactly(t, store, catalogTenant, catalogSession, 0, 100, 1)
			if seen[0].Record.State != entry.Record.State {
				t.Fatalf("listed state = %q, want %q", seen[0].Record.State, entry.Record.State)
			}
			if seen[0].AcceptedOrder != entry.AcceptedOrder {
				t.Fatalf("listed order = %d, want %d", seen[0].AcceptedOrder, entry.AcceptedOrder)
			}
		})
	}
}

// TestASettledCommandLeavesTheDueViewAndStaysInTheStream is the other half of
// the comparison, and it needs its own test because reaching a terminal
// disposition state requires the whole settlement protocol and an injected
// evidence reader.
//
// This is the property a cursor exists for. If the consumption stream dropped
// settled commands the way the due view does, a cursor would be meaningless:
// the position would name a row the stream can no longer produce.
func TestASettledCommandLeavesTheDueViewAndStaysInTheStream(t *testing.T) {
	t.Parallel()

	s, _, claimed := settlementFixture(t, &fakeEvidence{evidence: appliedEvidence()})
	scope, err := s.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	dueAt := func() int {
		t.Helper()
		page, err := s.ListDueDispositionCommands(context.Background(), ListDueDispositionCommandsRequest{
			Shard: int(scope.ControlShard), DueAtOrBefore: inboxDeadline.Add(time.Hour), Limit: 100,
		})
		if err != nil {
			t.Fatalf("ListDueDispositionCommands: %v", err)
		}
		return len(page.Commands)
	}

	// The CONTROL, before settlement: the command is in BOTH views.
	walkExactly(t, s, catalogTenant, catalogSession, 0, 100, 1)
	if got := dueAt(); got != 1 {
		t.Fatalf("due view holds %d rows before settlement, want 1", got)
	}

	applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
	if err != nil {
		t.Fatalf("BeginDispositionAttempt: %v", err)
	}
	settled, _, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
	if err != nil {
		t.Fatalf("SettleDispositionCommand: %v", err)
	}
	if !settled.Record.State.terminal() {
		t.Fatalf("state after settlement = %q, want a terminal state", settled.Record.State)
	}

	stream := walkExactly(t, s, catalogTenant, catalogSession, 0, 100, 1)
	if stream[0].Record.State != settled.Record.State {
		t.Fatalf("stream state = %q, want %q", stream[0].Record.State, settled.Record.State)
	}
	if got := dueAt(); got != 0 {
		t.Fatalf("due view holds %d rows after settlement, want 0", got)
	}
}

// TestASessionCommandThisReaderCannotDecodeFailsThePage is the deliberate
// DIVERGENCE from the due sweep, asserted beside the sweep so the two answers
// are compared rather than described.
//
// The due view is a weak deadline view: it steps over a row it cannot read
// because failing would switch reconciliation off for every tenant in the
// shard. The acceptance-order stream is a CONSUMPTION stream, and a consumer
// that stepped over a row would advance its durable cursor past a command that
// will now never be applied. So this one fails closed, and the blast radius of
// failing closed is one session rather than one shard.
func TestASessionCommandThisReaderCannotDecodeFailsThePage(t *testing.T) {
	t.Parallel()

	store := consumptionFixture(t, memstore.New())
	admitted := admitSessionCommands(t, store, catalogTenant, catalogSession, "mine", 5)
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}

	// The CONTROL, before the corruption: both views read all five.
	walkExactly(t, store, catalogTenant, catalogSession, 0, 100, 5)

	id := dispositionInboxID(scope, admitted[2])
	stored, err := store.backend.OrderedIndex.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := store.backend.OrderedIndex.Update(
		context.Background(), id, stored.Revision, []byte("{"), stored.Rank, stored.Due); err != nil {
		t.Fatalf("Update: %v", err)
	}

	_, err = store.ListSessionDispositionCommands(context.Background(), ListSessionDispositionCommandsRequest{
		TenantID: catalogTenant, SessionID: catalogSession, Limit: 100,
	})
	assertInboxCode(t, err, InboxErrorMalformed)

	due, err := store.ListDueDispositionCommands(context.Background(), ListDueDispositionCommandsRequest{
		Shard:         int(scope.ControlShard),
		DueAtOrBefore: inboxDeadline.Add(time.Hour),
		Limit:         100,
	})
	if err != nil {
		t.Fatalf("ListDueDispositionCommands: %v", err)
	}
	if due.Unreadable != 1 || len(due.Commands) != 4 {
		t.Fatalf("due view reported %d unreadable and %d commands, want 1 and 4",
			due.Unreadable, len(due.Commands))
	}
}

// TestListSessionCommandsRefusesAnUnusableLimit keeps the request's one numeric
// bound at this package's boundary rather than the provider's, and pins that a
// zero limit means the store's page size rather than an empty page.
func TestListSessionCommandsRefusesAnUnusableLimit(t *testing.T) {
	t.Parallel()

	store := consumptionFixture(t, memstore.New())
	admitSessionCommands(t, store, catalogTenant, catalogSession, "mine", 3)

	for _, limit := range []int{-1, storage.MaxOrderedPageLimit + 1} {
		_, err := store.ListSessionDispositionCommands(context.Background(), ListSessionDispositionCommandsRequest{
			TenantID: catalogTenant, SessionID: catalogSession, Limit: limit,
		})
		got := assertInboxCode(t, err, InboxErrorInvalid)
		if got.Field != "limit" {
			t.Fatalf("limit %d refused on field %q", limit, got.Field)
		}
	}
	page, err := store.ListSessionDispositionCommands(context.Background(), ListSessionDispositionCommandsRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	})
	if err != nil {
		t.Fatalf("ListSessionDispositionCommands(limit=0): %v", err)
	}
	if len(page.Commands) != 3 || page.Limit != storage.MaxOrderedPageLimit {
		t.Fatalf("zero limit returned %d rows at an effective limit of %d, want 3 at %d",
			len(page.Commands), page.Limit, storage.MaxOrderedPageLimit)
	}
}

// TestListSessionCommandsRefusesAnInvalidIdentity holds the two identities to
// the same boundary every other named read holds them to.
func TestListSessionCommandsRefusesAnInvalidIdentity(t *testing.T) {
	t.Parallel()

	store := consumptionFixture(t, memstore.New())
	for _, tt := range []struct{ name, tenant, session string }{
		{"empty tenant", "", string(catalogSession)},
		{"empty session", string(catalogTenant), ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := store.ListSessionDispositionCommands(context.Background(), ListSessionDispositionCommandsRequest{
				TenantID: sessionwire.TenantID(tt.tenant), SessionID: sessionwire.SessionID(tt.session), Limit: 10,
			})
			if err == nil {
				t.Fatal("an invalid identity was accepted")
			}
		})
	}
}

// TestListSessionCommandsCostsOnePageAndOneCatalogRead drives the cost claim
// rather than restating it.
//
// "ITS COST IS THE PAGE PLUS ONE CATALOG READ" is a sentence about provider
// traffic, and provider traffic is observable: the package's own
// instrumentComposite counts it. The invariance across limits is the part that
// matters — a listing that read the catalog once per row, as the due sweep
// must, would show a count that moves with the page.
func TestListSessionCommandsCostsOnePageAndOneCatalogRead(t *testing.T) {
	t.Parallel()

	backend, calls := instrumentComposite(memstore.New())
	store := consumptionFixture(t, backend)
	admitSessionCommands(t, store, catalogTenant, catalogSession, "mine", 40)

	var first providerCallSnapshot
	for i, limit := range []int{1, 5, 40} {
		before := calls.snapshot()
		page, err := store.ListSessionDispositionCommands(context.Background(), ListSessionDispositionCommandsRequest{
			TenantID: catalogTenant, SessionID: catalogSession, Limit: limit,
		})
		if err != nil {
			t.Fatalf("ListSessionDispositionCommands(limit=%d): %v", limit, err)
		}
		if len(page.Commands) != limit {
			t.Fatalf("limit %d returned %d rows", limit, len(page.Commands))
		}
		after := calls.snapshot()
		cost := providerCallSnapshot{
			Ledger:  after.Ledger - before.Ledger,
			Leaser:  after.Leaser - before.Leaser,
			KV:      after.KV - before.KV,
			Blobs:   after.Blobs - before.Blobs,
			Ordered: after.Ordered - before.Ordered,
		}
		if cost.Ledger != 0 || cost.Leaser != 0 || cost.Blobs != 0 {
			t.Fatalf("limit %d touched the journal, the leaser or a blob: %+v", limit, cost)
		}
		// Two ordered calls: the catalog read and the page. Not one per row.
		if cost.Ordered != 2 {
			t.Fatalf("limit %d issued %d ordered calls, want 2 (catalog + page)", limit, cost.Ordered)
		}
		if i == 0 {
			first = cost
			continue
		}
		if cost != first {
			t.Fatalf("cost moved with the limit: %+v at limit %d, %+v at limit 1", cost, limit, first)
		}
	}
}

// bentOrderedPage is a provider that answers a ListOrdered with a page this
// store must not take on trust.
//
// It exists because memstore cannot produce these answers and a CONFORMING
// provider never will — which is exactly why the checks have to be tested
// against a non-conforming one. A fake looser than the dependency is how a
// missing check stays invisible; this fake is TIGHTER, and these replies are
// the only things the store takes on trust unless it holds them to the rows.
type bentOrderedPage struct {
	storage.OrderedIndex
	t *testing.T
	// needs is how many rows the bend indexes. IT IS A GUARD ON THE FIXTURE,
	// not on the store: a bend handed a shorter page than it expects would
	// either panic — which is a crashed test, not a failed claim — or, worse,
	// silently return the page untouched, so the case would pass while bending
	// nothing at all. Stating the requirement here makes a fixture that stops
	// exercising its own case fail as an assertion naming that fact.
	needs int
	bend  func(storage.OrderedPage) storage.OrderedPage
}

func (o bentOrderedPage) ListOrdered(
	ctx context.Context, namespace, orderingScope string, afterOrder uint64, limit int,
) (storage.OrderedPage, error) {
	page, err := o.OrderedIndex.ListOrdered(ctx, namespace, orderingScope, afterOrder, limit)
	if err != nil {
		return page, err
	}
	if len(page.Records) < o.needs {
		o.t.Fatalf("the honest page carried %d rows in %q and this case bends %d; the fixture is not exercising what it claims",
			len(page.Records), namespace, o.needs)
	}
	return o.bend(page), nil
}

func TestListSessionCommandsHoldsTheProvidersPageToWhatItPromised(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		bend func(storage.OrderedPage) storage.OrderedPage
		want InboxErrorCode
	}{
		{
			name: "faithful",
			bend: func(page storage.OrderedPage) storage.OrderedPage { return page },
		},
		{
			name: "continuation ahead of the last row",
			bend: func(page storage.OrderedPage) storage.OrderedPage {
				page.NextAfterOrder++
				return page
			},
			want: InboxErrorIdentity,
		},
		{
			name: "continuation behind the last row",
			bend: func(page storage.OrderedPage) storage.OrderedPage {
				page.NextAfterOrder = page.Records[0].Order - 1
				return page
			},
			want: InboxErrorIdentity,
		},
		{
			name: "rows over the limit",
			bend: func(page storage.OrderedPage) storage.OrderedPage {
				page.Records = append(page.Records, page.Records[0])
				page.NextAfterOrder = page.Records[len(page.Records)-1].Order
				return page
			},
			want: InboxErrorBackend,
		},
		{
			name: "rows out of ascending order",
			bend: func(page storage.OrderedPage) storage.OrderedPage {
				page.Records[0], page.Records[1] = page.Records[1], page.Records[0]
				return page
			},
			want: InboxErrorIdentity,
		},
		{
			// THE DUPLICATE, and it is the case that pins the word "strictly"
			// in "strictly increasing AcceptedOrder".
			//
			// A page carrying the same acceptance order twice is the shape a
			// consumption stream must never be handed: the command would be
			// presented to a consumer twice inside one page, and the page would
			// still look ascending under a NON-STRICT comparison, so only `<=`
			// catches it. It is deliberately INSIDE the limit, because the
			// "rows over the limit" case above also duplicates a row but with
			// four rows against a limit of three it dies on the limit check
			// first and says nothing about ordering at all.
			name: "a duplicated acceptance order inside the limit",
			bend: func(page storage.OrderedPage) storage.OrderedPage {
				page.Records[1] = page.Records[0]
				page.NextAfterOrder = page.Records[len(page.Records)-1].Order
				return page
			},
			want: InboxErrorIdentity,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backend := memstore.New()
			ordered := backend.OrderedIndex
			store := consumptionFixture(t, backend)
			admitSessionCommands(t, store, catalogTenant, catalogSession, "mine", 3)
			store.backend.OrderedIndex = bentOrderedPage{OrderedIndex: ordered, t: t, needs: 2, bend: tt.bend}

			page, err := store.ListSessionDispositionCommands(
				context.Background(), ListSessionDispositionCommandsRequest{
					TenantID: catalogTenant, SessionID: catalogSession, Limit: 3,
				})
			if tt.want == "" {
				if err != nil {
					t.Fatalf("a faithful reply was refused: %v", err)
				}
				if len(page.Commands) != 3 {
					t.Fatalf("faithful reply returned %d rows, want 3", len(page.Commands))
				}
				return
			}
			assertInboxCode(t, err, tt.want)
		})
	}
}

// ignoringTheBound is a provider that answers every ListOrdered from the HEAD of
// the stream, whatever bound it was given.
//
// A conforming provider never does this, which is why the store's own
// enforcement of the caller's bound cannot be tested against memstore: memstore
// honours the bound, so a store that simply forwarded it and checked nothing
// would look identical to one that holds the reply to it.
type ignoringTheBound struct {
	storage.OrderedIndex
	// drop is how many rows to remove from the head of the honest answer, so
	// one case can hand back a page starting strictly below the bound and
	// another can hand back a page starting exactly AT it.
	drop int
}

func (o ignoringTheBound) ListOrdered(
	ctx context.Context, namespace, orderingScope string, _ uint64, limit int,
) (storage.OrderedPage, error) {
	page, err := o.OrderedIndex.ListOrdered(ctx, namespace, orderingScope, 0, limit+o.drop)
	if err != nil {
		return page, err
	}
	if o.drop < len(page.Records) {
		page.Records = page.Records[o.drop:]
	}
	if len(page.Records) > limit {
		page.Records = page.Records[:limit]
	}
	if len(page.Records) > 0 {
		page.NextAfterOrder = page.Records[len(page.Records)-1].Order
	}
	return page, nil
}

// TestListSessionCommandsHoldsEveryRowToTheCallersOwnBound is "STRICTLY AFTER
// afterOrder" — Host's declared contract, verbatim — defended as a store-side
// guarantee rather than a forwarded argument.
//
// The store passes the caller's bound to the provider, and a conforming
// provider honours it; but the sentence in this package's own documentation is
// about what the store RETURNS, and a caller is forbidden from re-checking it
// (that would be inferring an order). So the store seeds its ascending walk
// with the caller's bound rather than with zero, and that single assignment is
// what makes the first row's position checkable. Both directions are driven:
//
//   - a page starting strictly BELOW the bound — rows the caller has already
//     consumed and whose commands it would apply a second time;
//   - a page whose first row sits EXACTLY at the exclusive bound, which is the
//     boundary the word "strictly" is about and the one an off-by-one would
//     leak.
func TestListSessionCommandsHoldsEveryRowToTheCallersOwnBound(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		drop int
	}{
		{name: "a page answered from the head despite a bound", drop: 0},
		{name: "a page whose first row is exactly at the bound", drop: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backend := memstore.New()
			ordered := backend.OrderedIndex
			store := consumptionFixture(t, backend)
			admitSessionCommands(t, store, catalogTenant, catalogSession, "mine", 4)
			full := walkExactly(t, store, catalogTenant, catalogSession, 0, 100, 4)
			// The bound is the SECOND row's order, so `drop: 0` answers from
			// row 1 (strictly below it) and `drop: 1` answers from row 2
			// (exactly at it).
			bound := full[1].AcceptedOrder

			// The CONTROL: honestly answered, this bound returns the suffix.
			honest, err := store.ListSessionDispositionCommands(
				context.Background(), ListSessionDispositionCommandsRequest{
					TenantID: catalogTenant, SessionID: catalogSession, AfterOrder: bound, Limit: 2,
				})
			if err != nil {
				t.Fatalf("the honest bounded page was refused: %v", err)
			}
			if len(honest.Commands) != 2 || honest.Commands[0].AcceptedOrder != full[2].AcceptedOrder {
				t.Fatalf("honest page = %d rows starting at %d, want 2 starting at %d",
					len(honest.Commands), honest.Commands[0].AcceptedOrder, full[2].AcceptedOrder)
			}

			store.backend.OrderedIndex = ignoringTheBound{OrderedIndex: ordered, drop: tt.drop}
			_, err = store.ListSessionDispositionCommands(
				context.Background(), ListSessionDispositionCommandsRequest{
					TenantID: catalogTenant, SessionID: catalogSession, AfterOrder: bound, Limit: 2,
				})
			got := assertInboxCode(t, err, InboxErrorIdentity)
			if got.Field != "order" {
				t.Fatalf("refused on field %q, want %q", got.Field, "order")
			}
		})
	}
}

func mustScope(t *testing.T, store *Store) sessionScope {
	t.Helper()
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	return scope
}

// TestListSessionCommandsRefusesAContinuationOnAnExhaustedPage is the empty
// page's half of the continuation check, and it is a separate test because it
// is a separate branch: the page with rows compares against the last row, and
// the page WITHOUT rows has no row to compare against and must insist on zero.
//
// The hazard is specific. An exhausted stream's answer is "keep the bound you
// asked with"; a provider that answered an empty page with a position would
// hand a consumer a bound it was never given rows for, and the consumer would
// resume from it — stepping over every command admitted between the two.
func TestListSessionCommandsRefusesAContinuationOnAnExhaustedPage(t *testing.T) {
	t.Parallel()

	backend := memstore.New()
	ordered := backend.OrderedIndex
	store := consumptionFixture(t, backend)
	admitSessionCommands(t, store, catalogTenant, catalogSession, "mine", 3)
	full := walkExactly(t, store, catalogTenant, catalogSession, 0, 100, 3)
	end := full[len(full)-1].AcceptedOrder

	// The CONTROL: past the end, a faithful provider reports no continuation
	// and this store passes it through.
	page, err := store.ListSessionDispositionCommands(context.Background(), ListSessionDispositionCommandsRequest{
		TenantID: catalogTenant, SessionID: catalogSession, AfterOrder: end, Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListSessionDispositionCommands past the end: %v", err)
	}
	if len(page.Commands) != 0 || page.NextAfterOrder != 0 {
		t.Fatalf("exhausted page = %d rows continuing at %d", len(page.Commands), page.NextAfterOrder)
	}

	store.backend.OrderedIndex = bentOrderedPage{OrderedIndex: ordered, t: t, bend: func(p storage.OrderedPage) storage.OrderedPage {
		if len(p.Records) == 0 {
			p.NextAfterOrder = end + 1
		}
		return p
	}}
	_, err = store.ListSessionDispositionCommands(context.Background(), ListSessionDispositionCommandsRequest{
		TenantID: catalogTenant, SessionID: catalogSession, AfterOrder: end, Limit: 10,
	})
	assertInboxCode(t, err, InboxErrorIdentity)
}

// ---------------------------------------------------------------------------
// The durable consumption cursor
// ---------------------------------------------------------------------------

// TestLoadCursorReportsZeroWhenNoneHasBeenRecorded is the requirement stated
// exactly: absence is an ANSWER, not a failure.
//
// The control matters more than the claim. "Load returned zero" would also pass
// against a Load that returned zero unconditionally, so the same store is then
// made to record one and asked again.
func TestLoadCursorReportsZeroWhenNoneHasBeenRecorded(t *testing.T) {
	t.Parallel()

	store := consumptionFixture(t, memstore.New())
	entry := mustLoadCursor(t, store)
	if entry != (DispositionCommandCursorEntry{}) {
		t.Fatalf("unrecorded cursor = %+v, want the zero entry", entry)
	}

	mustSaveCursor(t, store, consumptionEpoch, consumptionOrder)
	recorded := mustLoadCursor(t, store)
	if recorded.Cursor.ConsumedOrder != consumptionOrder {
		t.Fatalf("recorded cursor = %d, want %d", recorded.Cursor.ConsumedOrder, consumptionOrder)
	}
	if recorded.Cursor.LeaseEpoch != consumptionEpoch {
		t.Fatalf("recorded epoch = %d, want %d", recorded.Cursor.LeaseEpoch, consumptionEpoch)
	}
	if recorded.Cursor.TenantID != catalogTenant || recorded.Cursor.SessionID != catalogSession {
		t.Fatalf("recorded identity = (%q,%q)", recorded.Cursor.TenantID, recorded.Cursor.SessionID)
	}
	if !recorded.Cursor.UpdatedAt.Equal(consumptionUpdatedAt) {
		t.Fatalf("recorded instant = %v, want the store's clock %v", recorded.Cursor.UpdatedAt, consumptionUpdatedAt)
	}
	if recorded.Revision == 0 {
		t.Fatal("a recorded cursor carries no revision")
	}
}

// TestCursorSurvivesAReopen asserts the cursor is DURABLE rather than a value
// the open Store happens to remember.
func TestCursorSurvivesAReopen(t *testing.T) {
	t.Parallel()

	backend := memstore.New()
	first := consumptionFixture(t, backend)
	mustSaveCursor(t, first, consumptionEpoch, consumptionOrder)

	second := openStore(t, backend, WithClock(newMovableClock(consumptionUpdatedAt)))
	reopened := mustLoadCursor(t, second)
	if reopened.Cursor.ConsumedOrder != consumptionOrder || reopened.Cursor.LeaseEpoch != consumptionEpoch {
		t.Fatalf("reopened cursor = %+v", reopened.Cursor)
	}
}

// TestSaveCursorFencesTheEpochBeforeTheOrder is the concurrency contract in its
// two halves, and the ORDER of the two fences is the assertion.
//
// THE DECIDING CASE IS `earlier_epoch, earlier_order`, AND ONLY THAT ONE.
// This was stated wrongly here once and the correction is worth keeping: a
// caller with a LOW EPOCH and a HIGH POSITION passes the order fence and is
// then refused by the epoch fence, so it receives InboxErrorEpoch under EITHER
// ordering — swapping the two calls does not change its answer, and it is
// therefore no probe at all. The caller whose answer changes is the one below
// BOTH marks: epoch-first tells it "you have lost the session", which is
// terminal and correct, while order-first would tell it "your position is
// stale" and invite it to fetch newer data and retry forever against a session
// it no longer owns. Swapping the two fence calls fails exactly this row.
func TestSaveCursorFencesTheEpochBeforeTheOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		epoch, order uint64
		want         InboxErrorCode
		wantOrder    uint64
	}{
		{name: "same epoch, later order", epoch: consumptionEpoch, order: consumptionOrder + 1, wantOrder: consumptionOrder + 1},
		{name: "later epoch, later order", epoch: consumptionEpoch + 1, order: consumptionOrder + 1, wantOrder: consumptionOrder + 1},
		{name: "later epoch, equal order", epoch: consumptionEpoch + 1, order: consumptionOrder, wantOrder: consumptionOrder},
		{name: "same epoch, equal order", epoch: consumptionEpoch, order: consumptionOrder, wantOrder: consumptionOrder},
		{name: "same epoch, earlier order", epoch: consumptionEpoch, order: consumptionOrder - 1, want: InboxErrorOrder},
		{name: "later epoch, earlier order", epoch: consumptionEpoch + 1, order: consumptionOrder - 1, want: InboxErrorOrder},
		// Below both marks. THIS is the row that decides the fence ordering.
		{name: "earlier epoch, earlier order", epoch: consumptionEpoch - 1, order: consumptionOrder - 1, want: InboxErrorEpoch},
		// Below the epoch, above the position: refused on the epoch under both
		// orderings, so it pins the ANSWER but not the ORDER. Kept because the
		// answer is still worth pinning, and labelled so nobody mistakes it for
		// the ordering probe again.
		{name: "earlier epoch, later order", epoch: consumptionEpoch - 1, order: consumptionOrder + 1, want: InboxErrorEpoch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := consumptionFixture(t, memstore.New())
			mustSaveCursor(t, store, consumptionEpoch, consumptionOrder)

			entry, err := store.SaveDispositionCommandCursor(context.Background(), testSaveCursorRequest(tt.epoch, tt.order))
			if tt.want == "" {
				if err != nil {
					t.Fatalf("SaveDispositionCommandCursor: %v", err)
				}
				if entry.Cursor.ConsumedOrder != tt.wantOrder || entry.Cursor.LeaseEpoch != tt.epoch {
					t.Fatalf("stored cursor = %+v, want order %d at epoch %d",
						entry.Cursor, tt.wantOrder, tt.epoch)
				}
				return
			}
			got := assertInboxCode(t, err, tt.want)
			if tt.want == InboxErrorEpoch && got.Epoch != consumptionEpoch {
				t.Fatalf("epoch refusal carried %d, want the committed %d", got.Epoch, consumptionEpoch)
			}
			if tt.want == InboxErrorOrder && got.Order != consumptionOrder {
				t.Fatalf("order refusal carried %d, want the committed %d", got.Order, consumptionOrder)
			}
			// A refusal leaves the stored cursor exactly as it was.
			after := mustLoadCursor(t, store)
			if after.Cursor.ConsumedOrder != consumptionOrder || after.Cursor.LeaseEpoch != consumptionEpoch {
				t.Fatalf("a refused save moved the cursor to %+v", after.Cursor)
			}
		})
	}
}

// TestSaveCursorRefusesAnUnusableRequest keeps zero out of both durable
// members.
//
// A ZERO ORDER is refused because zero is not an order any provider allocates
// and because it is this record's spelling of "nothing recorded": admitting it
// would make an absent cursor and a recorded one indistinguishable to Load,
// which is the whole basis of the zero-means-none answer.
func TestSaveCursorRefusesAnUnusableRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		epoch, order uint64
		field        string
	}{
		{name: "zero epoch", epoch: 0, order: consumptionOrder, field: "lease_epoch"},
		{name: "zero order", epoch: consumptionEpoch, order: 0, field: "consumed_order"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backend, calls := instrumentComposite(memstore.New())
			store := consumptionFixture(t, backend)
			before := calls.snapshot()

			_, err := store.SaveDispositionCommandCursor(context.Background(), testSaveCursorRequest(tt.epoch, tt.order))
			got := assertInboxCode(t, err, InboxErrorInvalid)
			if got.Field != tt.field {
				t.Fatalf("refused on field %q, want %q", got.Field, tt.field)
			}
			if after := calls.snapshot(); after != before {
				t.Fatalf("an invalid request touched the provider: before=%+v after=%+v", before, after)
			}
		})
	}
}

// TestACursorThisReaderCannotDecodeIsNotAnAbsentCursor is the narrow probe for
// the widest sentence in this file.
//
// "Load returns zero when none has been recorded" must not widen into "Load
// returns zero when it cannot read one". The direction is the hazard: absence
// licenses a CREATE at whatever epoch and order the caller named, so a Save
// that treated an undecodable row as absence would silently reset the fence a
// live consumer is being held to.
func TestACursorThisReaderCannotDecodeIsNotAnAbsentCursor(t *testing.T) {
	t.Parallel()

	store := consumptionFixture(t, memstore.New())
	mustSaveCursor(t, store, consumptionEpoch, consumptionOrder)
	id := dispositionCursorID(mustScope(t, store))
	stored, err := store.backend.OrderedIndex.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := store.backend.OrderedIndex.Update(
		context.Background(), id, stored.Revision, []byte("{"), storage.Rank{}, storage.Due{}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	_, err = store.LoadDispositionCommandCursor(context.Background(), testLoadCursorRequest())
	assertInboxCode(t, err, InboxErrorMalformed)

	// The save names an epoch and an order BELOW the ones the unreadable row
	// carries, so a create-over-absence would be visible as a regression rather
	// than only as a rewrite.
	_, err = store.SaveDispositionCommandCursor(context.Background(), testSaveCursorRequest(1, 1))
	assertInboxCode(t, err, InboxErrorMalformed)
	if got, err := store.backend.OrderedIndex.Get(context.Background(), id); err != nil {
		t.Fatalf("Get: %v", err)
	} else if string(got.Value) != "{" {
		t.Fatalf("the unreadable row was rewritten to %q", got.Value)
	}
}

// TestCursorRecordDecodeFailsClosed drives the codec's refusals directly,
// because the operations above can only reach one of them.
func TestCursorRecordDecodeFailsClosed(t *testing.T) {
	t.Parallel()

	valid, _, err := encodeDispositionCursor(DispositionCommandCursor{
		TenantID: catalogTenant, SessionID: catalogSession,
		LeaseEpoch: consumptionEpoch, ConsumedOrder: consumptionOrder, UpdatedAt: consumptionUpdatedAt,
	})
	if err != nil {
		t.Fatalf("encodeDispositionCursor: %v", err)
	}
	if _, err := decodeDispositionCursor(valid); err != nil {
		t.Fatalf("the canonical record did not decode: %v", err)
	}

	tests := []struct {
		name  string
		value []byte
		want  InboxErrorCode
	}{
		{name: "empty", value: nil, want: InboxErrorMalformed},
		{name: "truncated", value: valid[:len(valid)-3], want: InboxErrorMalformed},
		{name: "unknown version", value: []byte(`{"record_version":99,"tenant_id":"tenant-a","session_id":"session-a","lease_epoch":9,"consumed_order":4096,"updated_at":"2026-08-30T11:40:00Z"}`), want: InboxErrorVersion},
		{name: "undeclared member", value: []byte(`{"record_version":1,"tenant_id":"tenant-a","session_id":"session-a","lease_epoch":9,"consumed_order":4096,"updated_at":"2026-08-30T11:40:00Z","extra":1}`), want: InboxErrorMalformed},
		{name: "zero order", value: []byte(`{"record_version":1,"tenant_id":"tenant-a","session_id":"session-a","lease_epoch":9,"consumed_order":0,"updated_at":"2026-08-30T11:40:00Z"}`), want: InboxErrorInvalid},
		{name: "zero epoch", value: []byte(`{"record_version":1,"tenant_id":"tenant-a","session_id":"session-a","lease_epoch":0,"consumed_order":4096,"updated_at":"2026-08-30T11:40:00Z"}`), want: InboxErrorInvalid},
		{name: "empty tenant", value: []byte(`{"record_version":1,"tenant_id":"","session_id":"session-a","lease_epoch":9,"consumed_order":4096,"updated_at":"2026-08-30T11:40:00Z"}`), want: InboxErrorInvalid},
		{name: "oversized", value: append([]byte(`{"record_version":1,"tenant_id":"`), make([]byte, MaxDispositionCommandCursorRecordBytes)...), want: InboxErrorTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := decodeDispositionCursor(tt.value)
			assertInboxCode(t, err, tt.want)
		})
	}
}

// TestCursorWireGolden pins the DURABLE spelling.
//
// The exported struct carries no JSON tags at all, so there is nothing
// decorative for a reader to mistake for the durable names: those live only in
// the private wire DTO, and this literal is what makes that statement
// checkable. The chain to the stored bytes is closed by
// verifyDispositionCursorBytes, which holds the provider's reply to exactly the
// bytes this encoder produced.
func TestCursorWireGolden(t *testing.T) {
	t.Parallel()

	value, _, err := encodeDispositionCursor(DispositionCommandCursor{
		TenantID: catalogTenant, SessionID: catalogSession,
		LeaseEpoch: consumptionEpoch, ConsumedOrder: consumptionOrder, UpdatedAt: consumptionUpdatedAt,
	})
	if err != nil {
		t.Fatalf("encodeDispositionCursor: %v", err)
	}
	const want = `{"record_version":1,"tenant_id":"tenant-a","session_id":"session-a","lease_epoch":9,"consumed_order":4096,"updated_at":"2026-08-30T11:40:00Z"}`
	if string(value) != want {
		t.Fatalf("stored bytes =\n%s\nwant\n%s", value, want)
	}

	// And the bytes that reach the PROVIDER are these bytes, read back out of
	// the backend rather than re-derived from the encoder.
	store := consumptionFixture(t, memstore.New())
	mustSaveCursor(t, store, consumptionEpoch, consumptionOrder)
	stored, err := store.backend.OrderedIndex.Get(context.Background(), dispositionCursorID(mustScope(t, store)))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(stored.Value, []byte(want)) {
		t.Fatalf("stored row =\n%s\nwant\n%s", stored.Value, want)
	}
}

// TestCursorNormalizesTheInstantToUTC pins the one line in
// canonicalDispositionCursor that nothing else can see.
//
// Nothing decides on UpdatedAt, so a reader could reasonably take the UTC
// conversion for decoration. It is not: production's clock is time.Now(), so
// without it two stores in different zones would write different durable
// spellings of the same instant, and the record's "one canonical spelling,
// byte-identical on read-back" property — which verifyDispositionCursorBytes
// compares exactly — would quietly stop holding.
func TestCursorNormalizesTheInstantToUTC(t *testing.T) {
	t.Parallel()

	zone := time.FixedZone("UTC+2", 2*60*60)
	shifted := consumptionUpdatedAt.In(zone)
	if shifted.Format(time.RFC3339) == consumptionUpdatedAt.UTC().Format(time.RFC3339) {
		t.Fatal("the fixture instant spells the same in both zones; this test would prove nothing")
	}
	value, canonical, err := encodeDispositionCursor(DispositionCommandCursor{
		TenantID: catalogTenant, SessionID: catalogSession,
		LeaseEpoch: consumptionEpoch, ConsumedOrder: consumptionOrder, UpdatedAt: shifted,
	})
	if err != nil {
		t.Fatalf("encodeDispositionCursor: %v", err)
	}
	const want = `{"record_version":1,"tenant_id":"tenant-a","session_id":"session-a","lease_epoch":9,"consumed_order":4096,"updated_at":"2026-08-30T11:40:00Z"}`
	if string(value) != want {
		t.Fatalf("a non-UTC instant stored as\n%s\nwant\n%s", value, want)
	}
	if canonical.UpdatedAt.Location() != time.UTC {
		t.Fatalf("canonical instant is in %v, want UTC", canonical.UpdatedAt.Location())
	}
}

// TestConcurrentCursorSaversNeverLoseOrRegressAPosition is the concurrency
// contract, driven rather than described.
//
// EVERY ASSERTION HERE HOLDS UNDER EVERY INTERLEAVING, which is the whole
// difficulty of writing this test honestly. An earlier version asserted that
// several distinct positions were accepted, and that is a claim about the
// SCHEDULE rather than about the store: a run in which the highest writer
// happens to go first refuses every other save, correctly, and the assertion
// failed. What is asserted instead is three things a scheduler cannot change:
//
//  1. THE HIGHEST ADVANCER EVENTUALLY WINS, and the final stored position is
//     its own. It passes the order fence by construction — nothing above it is
//     ever offered — and it holds the committed epoch, so the only refusal it
//     can meet is a revision conflict, which it retries.
//
//  2. A STALE POSITION IS ALWAYS REFUSED WITH InboxErrorOrder, on every attempt,
//     whatever else is in flight. This is the order fence under contention.
//
//  3. A SUPERSEDED EPOCH IS ALWAYS REFUSED WITH InboxErrorEpoch, on every
//     attempt. Note what this does and does NOT show: it pins the epoch fence
//     under contention, but because this writer's position is far ahead it
//     would be refused on the epoch under either fence ordering, so it says
//     nothing about which fence runs first. The ordering probe lives in
//     TestSaveCursorFencesTheEpochBeforeTheOrder's `earlier_epoch, earlier_order`
//     row, and this test deliberately does not claim it.
func TestConcurrentCursorSaversNeverLoseOrRegressAPosition(t *testing.T) {
	t.Parallel()

	store := consumptionFixture(t, memstore.New())
	mustSaveCursor(t, store, consumptionEpoch, consumptionOrder)

	const (
		advancers           = 12
		attempts            = 8
		staleOrder          = consumptionOrder - 1
		supersededEpoch     = consumptionEpoch - 1
		supersededFarAhead  = consumptionOrder + advancers + 1000
		topAdvancerPosition = consumptionOrder + advancers
	)

	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := map[uint64]bool{}
	note := func(order uint64) {
		mu.Lock()
		defer mu.Unlock()
		accepted[order] = true
	}

	for i := 1; i <= advancers; i++ {
		wg.Add(1)
		go func(order uint64, mustWin bool) {
			defer wg.Done()
			for attempt := 0; attempt < attempts || mustWin; attempt++ {
				entry, err := store.SaveDispositionCommandCursor(
					context.Background(), testSaveCursorRequest(consumptionEpoch, order))
				if err == nil {
					note(entry.Cursor.ConsumedOrder)
					if mustWin {
						return
					}
					continue
				}
				typed := &InboxError{}
				if !errors.As(err, &typed) {
					t.Errorf("SaveDispositionCommandCursor: %T %v, want *InboxError", err, err)
					return
				}
				switch typed.Code {
				case InboxErrorConflict:
				case InboxErrorOrder:
					if mustWin {
						t.Errorf("the highest advancer was refused on the order fence at %d", order)
						return
					}
				case InboxErrorEpoch:
					t.Errorf("an advancer holding the committed epoch was refused on the epoch fence")
					return
				default:
					t.Errorf("SaveDispositionCommandCursor refused with %q", typed.Code)
					return
				}
				if attempt > 100000 {
					t.Error("the highest advancer never won a compare-and-swap")
					return
				}
			}
		}(consumptionOrder+uint64(i), i == advancers)
	}

	for _, stale := range []struct {
		name         string
		epoch, order uint64
		want         InboxErrorCode
	}{
		{name: "stale position", epoch: consumptionEpoch, order: staleOrder, want: InboxErrorOrder},
		{name: "superseded lease", epoch: supersededEpoch, order: supersededFarAhead, want: InboxErrorEpoch},
	} {
		wg.Add(1)
		go func(name string, epoch, order uint64, want InboxErrorCode) {
			defer wg.Done()
			for range attempts {
				_, err := store.SaveDispositionCommandCursor(
					context.Background(), testSaveCursorRequest(epoch, order))
				typed := &InboxError{}
				if !errors.As(err, &typed) {
					t.Errorf("%s: SaveDispositionCommandCursor = %v, want a refusal", name, err)
					return
				}
				if typed.Code != want {
					t.Errorf("%s: refused with %q, want %q", name, typed.Code, want)
					return
				}
			}
		}(stale.name, stale.epoch, stale.order, stale.want)
	}
	wg.Wait()

	final := mustLoadCursor(t, store)
	if final.Cursor.ConsumedOrder != topAdvancerPosition {
		t.Fatalf("final cursor = %d, want the highest advancer's %d",
			final.Cursor.ConsumedOrder, topAdvancerPosition)
	}
	if final.Cursor.LeaseEpoch != consumptionEpoch {
		t.Fatalf("final epoch = %d, want the committed %d", final.Cursor.LeaseEpoch, consumptionEpoch)
	}
	for order := range accepted {
		if order < consumptionOrder {
			t.Fatalf("an accepted save committed %d, below the position already stored", order)
		}
	}
	if !accepted[topAdvancerPosition] {
		t.Fatalf("the highest advancer never reported success; accepted = %v", accepted)
	}
}

// hidingCursorGet is a provider whose cursor Get reports the row absent while
// Create stays truthful. It is the exact interleaving of a LOST CREATE RACE:
// this caller's read found nothing, and a competitor committed before its
// Create ran.
type hidingCursorGet struct {
	storage.OrderedIndex
}

func (o hidingCursorGet) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	if id.Namespace == dispositionCursorNamespace {
		return storage.OrderedRecord{}, &storage.OrderedRecordNotFoundError{ID: id}
	}
	return o.OrderedIndex.Get(ctx, id)
}

// TestALostCreateRaceIsAConflictAndNotACorruptRecord pins the "you raced" arm of
// the three-answer contract on the one path where racing is most likely.
//
// TWO PROPERTIES, AND THE SECOND IS THE ONE WITH NO OTHER PROBE. The safety
// property — the loser does not overwrite the winner — is also enforced by
// verifyDispositionCursorBytes, which sees the winner's bytes and refuses. So a
// test that only checked the stored row would pass even if the conflict branch
// were deleted. What would change is the ANSWER: the caller would be told
// InboxErrorIdentity, "your record is corrupt, do not retry", when the correct
// instruction is InboxErrorConflict, "you raced, re-read and try again". Two
// Hosts booting on a session that has never had a cursor is exactly this race.
func TestALostCreateRaceIsAConflictAndNotACorruptRecord(t *testing.T) {
	t.Parallel()

	backend := memstore.New()
	ordered := backend.OrderedIndex
	store := consumptionFixture(t, backend)
	// The winner commits first, at marks far above the loser's, so a loser that
	// somehow succeeded would be visible as a regression as well as a wrong code.
	mustSaveCursor(t, store, consumptionEpoch+5, consumptionOrder+500)

	store.backend.OrderedIndex = hidingCursorGet{OrderedIndex: ordered}
	_, err := store.SaveDispositionCommandCursor(context.Background(), testSaveCursorRequest(1, 1))
	got := assertInboxCode(t, err, InboxErrorConflict)
	if got.Field != "create" {
		t.Fatalf("lost create refused on field %q, want %q", got.Field, "create")
	}
	if got.Revision == 0 {
		t.Fatal("a lost create reported no revision to re-read at")
	}

	// And the winner is untouched.
	store.backend.OrderedIndex = ordered
	final := mustLoadCursor(t, store)
	if final.Cursor.ConsumedOrder != consumptionOrder+500 || final.Cursor.LeaseEpoch != consumptionEpoch+5 {
		t.Fatalf("the winner's cursor was disturbed: %+v", final.Cursor)
	}
}

// bentCursorRow is a provider that answers a cursor Get with a row this store
// must not take on trust — either filed somewhere other than where this package
// files one, or carrying bytes that are not this session's.
//
// memstore cannot produce these answers and a conforming provider never will,
// which is exactly why the checks have to be tested against a non-conforming
// one: a fake no looser than the real store would leave every one of those
// checks unexercised, and a check nothing exercises is a comment.
type bentCursorRow struct {
	storage.OrderedIndex
	bend func(storage.OrderedRecord) storage.OrderedRecord
}

func (o bentCursorRow) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	stored, err := o.OrderedIndex.Get(ctx, id)
	if err != nil || id.Namespace != dispositionCursorNamespace {
		return stored, err
	}
	return o.bend(stored), nil
}

// TestLoadCursorHoldsTheProvidersRowToItsFilingAndItsBytes drives every
// component of what commandCursorEntryFor checks, on BOTH of its axes.
//
// The two axes are the point. A filing check reads what the PROVIDER says about
// where the row lives; the identity check reads the ROW'S OWN BYTES. They come
// from different sources, so a provider that filed a row perfectly and answered
// with another session's bytes passes every filing check — and would hand this
// caller another session's consumption position as its own. That last case is a
// cross-session position leak and it is the reason this matrix has a bytes row
// as well as filing rows.
func TestLoadCursorHoldsTheProvidersRowToItsFilingAndItsBytes(t *testing.T) {
	t.Parallel()

	// A perfectly-formed cursor record belonging to somebody else.
	foreign, _, err := encodeDispositionCursor(DispositionCommandCursor{
		TenantID: catalogTenant, SessionID: "session-elsewhere",
		LeaseEpoch: consumptionEpoch + 1, ConsumedOrder: 999999, UpdatedAt: consumptionUpdatedAt,
	})
	if err != nil {
		t.Fatalf("encodeDispositionCursor: %v", err)
	}

	tests := []struct {
		name  string
		bend  func(storage.OrderedRecord) storage.OrderedRecord
		want  InboxErrorCode
		field string
	}{
		{
			name: "faithful",
			bend: func(r storage.OrderedRecord) storage.OrderedRecord { return r },
		},
		{
			name:  "another session's bytes, filed perfectly",
			bend:  func(r storage.OrderedRecord) storage.OrderedRecord { r.Value = foreign; return r },
			want:  InboxErrorIdentity,
			field: "record",
		},
		{
			name:  "filed under another stable key",
			bend:  func(r storage.OrderedRecord) storage.OrderedRecord { r.ID.StableKey = "somebody-elses-role"; return r },
			want:  InboxErrorIdentity,
			field: "stable_key",
		},
		{
			name: "ranked",
			bend: func(r storage.OrderedRecord) storage.OrderedRecord {
				r.Rank = storage.Rank{Ranked: true, Value: 5}
				return r
			},
			want:  InboxErrorIdentity,
			field: "rank",
		},
		{
			name: "due",
			bend: func(r storage.OrderedRecord) storage.OrderedRecord {
				r.Due = storage.Due{State: storage.DueAt, UnixMillis: 1}
				return r
			},
			want:  InboxErrorIdentity,
			field: "due",
		},
		{
			name:  "filed in another session's ordering scope",
			bend:  func(r storage.OrderedRecord) storage.OrderedRecord { r.ID.OrderingScope = "elsewhere"; return r },
			want:  InboxErrorIdentity,
			field: "ordering_scope",
		},
		{
			name:  "a provider tombstone",
			bend:  func(r storage.OrderedRecord) storage.OrderedRecord { r.Deleted = true; return r },
			want:  InboxErrorDeleted,
			field: "record",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backend := memstore.New()
			ordered := backend.OrderedIndex
			store := consumptionFixture(t, backend)
			mustSaveCursor(t, store, consumptionEpoch, consumptionOrder)
			store.backend.OrderedIndex = bentCursorRow{OrderedIndex: ordered, bend: tt.bend}

			entry, err := store.LoadDispositionCommandCursor(context.Background(), testLoadCursorRequest())
			if tt.want == "" {
				if err != nil {
					t.Fatalf("a faithfully filed row was refused: %v", err)
				}
				if entry.Cursor.ConsumedOrder != consumptionOrder {
					t.Fatalf("faithful row read as %d", entry.Cursor.ConsumedOrder)
				}
				return
			}
			got := assertInboxCode(t, err, tt.want)
			if got.Field != tt.field {
				t.Fatalf("refused on field %q, want %q", got.Field, tt.field)
			}
		})
	}
}

// TestSaveCursorHoldsTheProvidersReplyToTheBytesItWrote is the one check on
// this record that a reply consistent with ITSELF still cannot satisfy.
//
// Every other check asks whether the row the provider returned is internally
// coherent, which a SUBSTITUTED row answers just as well as the real one. On a
// write path the provider is claiming something stronger — that it stored THESE
// bytes — and the reply is what the caller takes away and what the next write
// is fenced against, so a provider that answered with another lease's epoch
// would have its answer adopted as the record's own history.
func TestSaveCursorHoldsTheProvidersReplyToTheBytesItWrote(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"create", "update"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			backend := memstore.New()
			ordered := backend.OrderedIndex
			store := consumptionFixture(t, backend)
			if path == "update" {
				mustSaveCursor(t, store, consumptionEpoch, consumptionOrder)
			}

			substitute, _, err := encodeDispositionCursor(DispositionCommandCursor{
				TenantID: catalogTenant, SessionID: catalogSession,
				// A HIGHER epoch and position than the write names, which is
				// the shape that matters: adopted, it would raise a fence the
				// caller never earned and lock the real writer out.
				LeaseEpoch: consumptionEpoch + 100, ConsumedOrder: consumptionOrder + 100,
				UpdatedAt: consumptionUpdatedAt,
			})
			if err != nil {
				t.Fatalf("encodeDispositionCursor: %v", err)
			}
			store.backend.OrderedIndex = substitutingCursorWriter{OrderedIndex: ordered, value: substitute}

			_, err = store.SaveDispositionCommandCursor(context.Background(),
				testSaveCursorRequest(consumptionEpoch, consumptionOrder+1))
			got := assertInboxCode(t, err, InboxErrorIdentity)
			if got.Field != "value" {
				t.Fatalf("refused on field %q, want %q", got.Field, "value")
			}
		})
	}
}

// substitutingCursorWriter answers a cursor write with a coherent record that
// is not the one the write handed it.
type substitutingCursorWriter struct {
	storage.OrderedIndex
	value []byte
}

func (o substitutingCursorWriter) Create(
	ctx context.Context, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due,
) (storage.OrderedRecord, bool, error) {
	if id.Namespace == dispositionCursorNamespace {
		value = o.value
	}
	return o.OrderedIndex.Create(ctx, id, rankingScope, value, rank, due)
}

func (o substitutingCursorWriter) Update(
	ctx context.Context, id storage.OrderedID, expectedRevision uint64, value []byte, rank storage.Rank, due storage.Due,
) (storage.OrderedRecord, error) {
	if id.Namespace == dispositionCursorNamespace {
		value = o.value
	}
	return o.OrderedIndex.Update(ctx, id, expectedRevision, value, rank, due)
}

// TestCursorIsPerSessionAndPerTenant keeps one session's cursor out of
// another's, in both directions.
func TestCursorIsPerSessionAndPerTenant(t *testing.T) {
	t.Parallel()

	store := consumptionFixture(t, memstore.New())
	mustSaveCursor(t, store, consumptionEpoch, consumptionOrder)

	for _, tt := range []struct {
		name    string
		tenant  sessionwire.TenantID
		session sessionwire.SessionID
	}{
		{name: "another session", tenant: catalogTenant, session: "session-b"},
		{name: "another tenant", tenant: "tenant-b", session: catalogSession},
	} {
		// Each neighbour is made to exist, so "it has no cursor" is an answer
		// about ITS cursor rather than about its whole scope.
		createDispositionCatalogFor(t, store, tt.tenant, tt.session)
		t.Run(tt.name, func(t *testing.T) {
			entry, err := store.LoadDispositionCommandCursor(context.Background(),
				LoadDispositionCommandCursorRequest{TenantID: tt.tenant, SessionID: tt.session})
			if err != nil {
				t.Fatalf("LoadDispositionCommandCursor: %v", err)
			}
			if entry != (DispositionCommandCursorEntry{}) {
				t.Fatalf("%s reported %+v, want the zero entry", tt.name, entry)
			}
		})
	}
}
