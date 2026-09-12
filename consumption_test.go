package sessionstore

import (
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
	// consumptionEpoch is the lease epoch the cursor fixture writes under. It
	// is deliberately neither zero nor one, so a fence that compared against a
	// constant rather than against the stored high-water is visible.
	consumptionEpoch = uint64(9)

	// consumptionOrder is a cursor position that is not any order the provider
	// allocates in these fixtures, so a test that meant to exercise the ORDER
	// fence cannot pass by coinciding with a real acceptance order.
	consumptionOrder = uint64(4096)
)

// consumptionUpdatedAt is the instant the fixture's clock reads. It is the
// inbox claim fixture's own start, because the state matrix this file ranges
// over builds claimed and applying records through the real claim operation,
// and a clock later than that fixture's expiry would refuse every one of them.
var consumptionUpdatedAt = inboxClaimStart

func consumptionFixture(t *testing.T, backend *storage.Composite) *Store {
	t.Helper()
	return openStore(t, backend, WithClock(newMovableClock(consumptionUpdatedAt)))
}

func testSaveCursorRequest(epoch, order uint64) SaveCommandCursorRequest {
	return SaveCommandCursorRequest{
		TenantID:      catalogTenant,
		SessionID:     catalogSession,
		LeaseEpoch:    epoch,
		ConsumedOrder: order,
	}
}

func testLoadCursorRequest() LoadCommandCursorRequest {
	return LoadCommandCursorRequest{TenantID: catalogTenant, SessionID: catalogSession}
}

func mustSaveCursor(t *testing.T, store *Store, epoch, order uint64) CommandCursorEntry {
	t.Helper()
	entry, err := store.SaveCommandCursor(context.Background(), testSaveCursorRequest(epoch, order))
	if err != nil {
		t.Fatalf("SaveCommandCursor(epoch=%d order=%d): %v", epoch, order, err)
	}
	return entry
}

func mustLoadCursor(t *testing.T, store *Store) CommandCursorEntry {
	t.Helper()
	entry, err := store.LoadCommandCursor(context.Background(), testLoadCursorRequest())
	if err != nil {
		t.Fatalf("LoadCommandCursor: %v", err)
	}
	return entry
}

// admitSessionCommands admits count commands into one session, in an order the
// caller chose, and returns the CommandIDs in ADMISSION order.
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
		command := sessionwire.CommandID(fmt.Sprintf("command-%s-%s-%s-%04d", tenant, session, tag, count-i))
		req := testAdmitRequest()
		req.TenantID, req.SessionID, req.CommandID = tenant, session, command
		mustAdmit(t, store, req)
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
) []InboxEntry {
	t.Helper()
	var seen []InboxEntry
	for pages := 0; ; pages++ {
		if pages > 1000 {
			t.Fatal("paging did not terminate")
		}
		page, err := store.ListSessionCommands(context.Background(), ListSessionCommandsRequest{
			TenantID: tenant, SessionID: session, AfterOrder: after, Limit: limit,
		})
		if err != nil {
			t.Fatalf("ListSessionCommands(after=%d limit=%d): %v", after, limit, err)
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
				if entry.Record.CommandID != admitted[i] {
					t.Fatalf("position %d = %q, want %q: the walk is not in admission order",
						i, entry.Record.CommandID, admitted[i])
				}
				if i > 0 && entry.AcceptedOrder <= seen[i-1].AcceptedOrder {
					t.Fatalf("order at %d (%d) does not exceed its predecessor (%d)",
						i, entry.AcceptedOrder, seen[i-1].AcceptedOrder)
				}
			}
		})
	}

	full := walkSessionCommands(t, store, catalogTenant, catalogSession, 0, 100)
	for cut := range full {
		bound := full[cut].AcceptedOrder
		suffix := walkSessionCommands(t, store, catalogTenant, catalogSession, bound, 5)
		if len(suffix) != len(full)-cut-1 {
			t.Fatalf("after order %d the walk returned %d rows, want %d", bound, len(suffix), len(full)-cut-1)
		}
		for i, entry := range suffix {
			if entry.Record.CommandID != full[cut+1+i].Record.CommandID {
				t.Fatalf("after order %d, position %d = %q, want %q",
					bound, i, entry.Record.CommandID, full[cut+1+i].Record.CommandID)
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
		if entry.Record.TenantID != catalogTenant || entry.Record.SessionID != catalogSession {
			t.Fatalf("row %d belongs to (%q,%q)", i, entry.Record.TenantID, entry.Record.SessionID)
		}
		if entry.Record.CommandID != mine[i] {
			t.Fatalf("row %d = %q, want %q", i, entry.Record.CommandID, mine[i])
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

// TestListSessionCommandsReturnsEveryDurableStateUnlikeTheDueView is where the
// new listing and ListDueCommands are held apart.
//
// They read the SAME rows through two different provider views, and the
// difference is not a filter this package applies: inboxDue files a terminal
// command NOT DUE, so it leaves the due view the instant it settles, while the
// acceptance-order stream never loses a row. A consumer that read the due view
// instead would silently stop seeing everything it had finished — which is
// exactly the mistake "filter the cross-session due query per session" would
// have been.
func TestListSessionCommandsReturnsEveryDurableStateUnlikeTheDueView(t *testing.T) {
	t.Parallel()

	for name, build := range inboxStates {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := consumptionFixture(t, memstore.New())
			entry := build(t, store, mustAdmit(t, store, testAdmitRequest()))

			seen := walkSessionCommands(t, store, catalogTenant, catalogSession, 0, 100)
			if len(seen) != 1 {
				t.Fatalf("acceptance-order listing returned %d rows in state %q, want 1", len(seen), name)
			}
			if seen[0].Record.State != entry.Record.State {
				t.Fatalf("listed state = %q, want %q", seen[0].Record.State, entry.Record.State)
			}
			if seen[0].AcceptedOrder != entry.AcceptedOrder {
				t.Fatalf("listed order = %d, want %d", seen[0].AcceptedOrder, entry.AcceptedOrder)
			}

			scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
			if err != nil {
				t.Fatalf("deriveSessionScope: %v", err)
			}
			due, err := store.ListDueCommands(context.Background(), ListDueCommandsRequest{
				Shard:         int(scope.ControlShard),
				DueAtOrBefore: inboxDeadline.Add(time.Hour),
				Limit:         100,
			})
			if err != nil {
				t.Fatalf("ListDueCommands: %v", err)
			}
			wantDue := 0
			if !entry.Record.State.terminal() {
				wantDue = 1
			}
			if len(due.Commands) != wantDue {
				t.Fatalf("due view returned %d rows in state %q, want %d", len(due.Commands), name, wantDue)
			}
		})
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
	if got := walkSessionCommands(t, store, catalogTenant, catalogSession, 0, 100); len(got) != 5 {
		t.Fatalf("control walk returned %d rows, want 5", len(got))
	}

	id := inboxID(scope, admitted[2])
	stored, err := store.backend.OrderedIndex.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := store.backend.OrderedIndex.Update(
		context.Background(), id, stored.Revision, []byte("{"), stored.Rank, stored.Due); err != nil {
		t.Fatalf("Update: %v", err)
	}

	_, err = store.ListSessionCommands(context.Background(), ListSessionCommandsRequest{
		TenantID: catalogTenant, SessionID: catalogSession, Limit: 100,
	})
	assertInboxCode(t, err, InboxErrorMalformed)

	due, err := store.ListDueCommands(context.Background(), ListDueCommandsRequest{
		Shard:         int(scope.ControlShard),
		DueAtOrBefore: inboxDeadline.Add(time.Hour),
		Limit:         100,
	})
	if err != nil {
		t.Fatalf("ListDueCommands: %v", err)
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
		_, err := store.ListSessionCommands(context.Background(), ListSessionCommandsRequest{
			TenantID: catalogTenant, SessionID: catalogSession, Limit: limit,
		})
		got := assertInboxCode(t, err, InboxErrorInvalid)
		if got.Field != "limit" {
			t.Fatalf("limit %d refused on field %q", limit, got.Field)
		}
	}
	page, err := store.ListSessionCommands(context.Background(), ListSessionCommandsRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	})
	if err != nil {
		t.Fatalf("ListSessionCommands(limit=0): %v", err)
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
			_, err := store.ListSessionCommands(context.Background(), ListSessionCommandsRequest{
				TenantID: sessionwire.TenantID(tt.tenant), SessionID: sessionwire.SessionID(tt.session), Limit: 10,
			})
			if err == nil {
				t.Fatal("an invalid identity was accepted")
			}
		})
	}
}

// bentOrderedPage is a provider that answers a ListOrdered with a continuation
// that is not the last row's order.
//
// It exists because memstore cannot produce this answer and a CONFORMING
// provider never will — which is exactly why the check has to be tested against
// a non-conforming one. A fake looser than the dependency is how a missing
// check stays invisible; this fake is TIGHTER, and it is the only reply the
// store takes on trust unless it holds it to the rows.
type bentOrderedPage struct {
	storage.OrderedIndex
	bend func(storage.OrderedPage) storage.OrderedPage
}

func (o bentOrderedPage) ListOrdered(
	ctx context.Context, namespace, orderingScope string, afterOrder uint64, limit int,
) (storage.OrderedPage, error) {
	page, err := o.OrderedIndex.ListOrdered(ctx, namespace, orderingScope, afterOrder, limit)
	if err != nil {
		return page, err
	}
	return o.bend(page), nil
}

func TestListSessionCommandsHoldsTheProvidersContinuationToTheRowsItReturned(t *testing.T) {
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backend := memstore.New()
			ordered := backend.OrderedIndex
			store := consumptionFixture(t, backend)
			admitSessionCommands(t, store, catalogTenant, catalogSession, "mine", 3)
			store.backend.OrderedIndex = bentOrderedPage{OrderedIndex: ordered, bend: tt.bend}

			page, err := store.ListSessionCommands(context.Background(), ListSessionCommandsRequest{
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

// ---------------------------------------------------------------------------
// The durable consumption cursor
// ---------------------------------------------------------------------------

// TestLoadCommandCursorReportsZeroWhenNoneHasBeenRecorded is the requirement
// stated exactly: absence is an ANSWER, not a failure.
//
// The control matters more than the claim. "Load returned zero" would also pass
// against a Load that returned zero unconditionally, so the same store is then
// made to record one and asked again.
func TestLoadCommandCursorReportsZeroWhenNoneHasBeenRecorded(t *testing.T) {
	t.Parallel()

	store := consumptionFixture(t, memstore.New())
	// The session is made to EXIST before the cursor is asked for, which is the
	// state the claim is about: a session that holds commands and has never had
	// a consumer. A session with no durable data at all is a different question
	// and is answered by TestCursorOperationsRefuseASessionWithNoDurableData.
	admitSessionCommands(t, store, catalogTenant, catalogSession, "mine", 1)

	entry := mustLoadCursor(t, store)
	if entry != (CommandCursorEntry{}) {
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

// TestCommandCursorSurvivesAReopen asserts the cursor is DURABLE rather than a
// value the open Store happens to remember.
func TestCommandCursorSurvivesAReopen(t *testing.T) {
	t.Parallel()

	backend := memstore.New()
	first := consumptionFixture(t, backend)
	mustSaveCursor(t, first, consumptionEpoch, consumptionOrder)

	second := consumptionFixture(t, backend)
	reopened := mustLoadCursor(t, second)
	if reopened.Cursor.ConsumedOrder != consumptionOrder || reopened.Cursor.LeaseEpoch != consumptionEpoch {
		t.Fatalf("reopened cursor = %+v", reopened.Cursor)
	}
}

// TestSaveCommandCursorFencesTheEpochBeforeTheOrder is the concurrency contract
// in its two halves, and the ORDER of the two fences is the assertion.
//
// A superseded lease carrying a newer position must be told it has lost the
// session rather than be admitted; a live lease carrying an older position must
// be told its position is stale rather than that its authority is in doubt. The
// two ask for opposite responses, so the case that names a low epoch AND a high
// order is the one that decides which fence ran first.
func TestSaveCommandCursorFencesTheEpochBeforeTheOrder(t *testing.T) {
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
		{name: "earlier epoch, later order", epoch: consumptionEpoch - 1, order: consumptionOrder + 1, want: InboxErrorEpoch},
		{name: "earlier epoch, earlier order", epoch: consumptionEpoch - 1, order: consumptionOrder - 1, want: InboxErrorEpoch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := consumptionFixture(t, memstore.New())
			mustSaveCursor(t, store, consumptionEpoch, consumptionOrder)

			entry, err := store.SaveCommandCursor(context.Background(), testSaveCursorRequest(tt.epoch, tt.order))
			if tt.want == "" {
				if err != nil {
					t.Fatalf("SaveCommandCursor: %v", err)
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

// TestSaveCommandCursorRefusesAnUnusableRequest keeps zero out of both durable
// members.
//
// A ZERO ORDER is refused because zero is not an order any provider allocates
// and because it is this record's spelling of "nothing recorded": admitting it
// would make an absent cursor and a recorded one indistinguishable to Load,
// which is the whole basis of the zero-means-none answer.
func TestSaveCommandCursorRefusesAnUnusableRequest(t *testing.T) {
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

			_, err := store.SaveCommandCursor(context.Background(), testSaveCursorRequest(tt.epoch, tt.order))
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
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	id := commandCursorID(scope)
	stored, err := store.backend.OrderedIndex.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := store.backend.OrderedIndex.Update(
		context.Background(), id, stored.Revision, []byte("{"), storage.Rank{}, storage.Due{}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	_, err = store.LoadCommandCursor(context.Background(), testLoadCursorRequest())
	assertInboxCode(t, err, InboxErrorMalformed)

	// The save names an epoch and an order BELOW the ones the unreadable row
	// carries, so a create-over-absence would be visible as a regression rather
	// than only as a rewrite.
	_, err = store.SaveCommandCursor(context.Background(), testSaveCursorRequest(1, 1))
	assertInboxCode(t, err, InboxErrorMalformed)
	if got, err := store.backend.OrderedIndex.Get(context.Background(), id); err != nil {
		t.Fatalf("Get: %v", err)
	} else if string(got.Value) != "{" {
		t.Fatalf("the unreadable row was rewritten to %q", got.Value)
	}
}

// TestCommandCursorRecordDecodeFailsClosed drives the codec's refusals
// directly, because the operations above can only reach one of them.
func TestCommandCursorRecordDecodeFailsClosed(t *testing.T) {
	t.Parallel()

	valid, _, err := encodeCommandCursor(CommandCursor{
		TenantID: catalogTenant, SessionID: catalogSession,
		LeaseEpoch: consumptionEpoch, ConsumedOrder: consumptionOrder, UpdatedAt: consumptionUpdatedAt,
	})
	if err != nil {
		t.Fatalf("encodeCommandCursor: %v", err)
	}
	if _, err := decodeCommandCursor(valid); err != nil {
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
		{name: "oversized", value: append([]byte(`{"record_version":1,"tenant_id":"`), make([]byte, MaxCommandCursorRecordBytes)...), want: InboxErrorTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := decodeCommandCursor(tt.value)
			assertInboxCode(t, err, tt.want)
		})
	}
}

// TestCommandCursorWireGolden pins the DURABLE spelling.
//
// The exported struct's JSON tags are decorative with respect to stored bytes
// for the reason the disposition record's are: a private wire DTO fixes the
// member names, so renaming an exported field cannot move a stored record. This
// literal is what makes that statement checkable.
func TestCommandCursorWireGolden(t *testing.T) {
	t.Parallel()

	value, _, err := encodeCommandCursor(CommandCursor{
		TenantID: catalogTenant, SessionID: catalogSession,
		LeaseEpoch: consumptionEpoch, ConsumedOrder: consumptionOrder, UpdatedAt: consumptionUpdatedAt,
	})
	if err != nil {
		t.Fatalf("encodeCommandCursor: %v", err)
	}
	const want = `{"record_version":1,"tenant_id":"tenant-a","session_id":"session-a","lease_epoch":9,"consumed_order":4096,"updated_at":"2026-08-30T11:40:00Z"}`
	if string(value) != want {
		t.Fatalf("stored bytes =\n%s\nwant\n%s", value, want)
	}
}

// TestConcurrentCommandCursorSaversNeverLoseOrRegressAPosition is the
// concurrency contract, driven rather than described.
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
//     attempt, EVEN THOUGH ITS POSITION IS FAR AHEAD OF EVERY ADVANCER'S. This
//     is the fence ORDERING under contention: a store that consulted the
//     position first would admit this writer's position and hand it a
//     conflicting answer, and the final cursor would be the superseded lease's.
//
// The last one is also what makes the final-position assertion a real probe
// rather than an arithmetic restatement: a regression in either fence moves the
// answer somewhere this test can name.
func TestConcurrentCommandCursorSaversNeverLoseOrRegressAPosition(t *testing.T) {
	t.Parallel()

	store := consumptionFixture(t, memstore.New())
	mustSaveCursor(t, store, consumptionEpoch, consumptionOrder)

	const (
		advancers = 12
		attempts  = 8
		// staleOrder is below the committed position and superseded* is below
		// the committed epoch while naming a position far above every
		// advancer's, so admitting it would be unmistakable.
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

	// The advancers. Each offers ONE position, repeatedly; the highest of them
	// retries until it is durable.
	for i := 1; i <= advancers; i++ {
		wg.Add(1)
		go func(order uint64, mustWin bool) {
			defer wg.Done()
			for attempt := 0; attempt < attempts || mustWin; attempt++ {
				entry, err := store.SaveCommandCursor(
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
					t.Errorf("SaveCommandCursor: %T %v, want *InboxError", err, err)
					return
				}
				switch typed.Code {
				case InboxErrorConflict:
					// A lost compare-and-swap is the one refusal a caller
					// retries, so the writer that must win does.
				case InboxErrorOrder:
					if mustWin {
						t.Errorf("the highest advancer was refused on the order fence at %d", order)
						return
					}
				case InboxErrorEpoch:
					t.Errorf("an advancer holding the committed epoch was refused on the epoch fence")
					return
				default:
					t.Errorf("SaveCommandCursor refused with %q", typed.Code)
					return
				}
				if attempt > 100000 {
					t.Error("the highest advancer never won a compare-and-swap")
					return
				}
			}
		}(consumptionOrder+uint64(i), i == advancers)
	}

	// The two writers whose refusal is unconditional.
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
				_, err := store.SaveCommandCursor(
					context.Background(), testSaveCursorRequest(epoch, order))
				typed := &InboxError{}
				if !errors.As(err, &typed) {
					t.Errorf("%s: SaveCommandCursor = %v, want a refusal", name, err)
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

// TestCommandCursorIsPerSessionAndPerTenant keeps one session's cursor out of
// another's, in both directions.
func TestCommandCursorIsPerSessionAndPerTenant(t *testing.T) {
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
		admitSessionCommands(t, store, tt.tenant, tt.session, "neighbour", 1)
		t.Run(tt.name, func(t *testing.T) {
			entry, err := store.LoadCommandCursor(context.Background(),
				LoadCommandCursorRequest{TenantID: tt.tenant, SessionID: tt.session})
			if err != nil {
				t.Fatalf("LoadCommandCursor: %v", err)
			}
			if entry != (CommandCursorEntry{}) {
				t.Fatalf("%s reported %+v, want the zero entry", tt.name, entry)
			}
		})
	}
}

// TestCursorOperationsRefuseASessionWithNoDurableData states the boundary of
// the zero-means-none answer, in the direction that is easy to over-read.
//
// A session with no durable data has no collision witnesses, and every NAMED
// read in this package refuses one rather than answering about a scope it
// cannot verify — getPointer, readCatalogEntry and GetCommand all do. These two
// are no different, and the reason to pin it is that "Load returns zero when
// none has been recorded" is a sentence a reader could stretch into "Load
// always succeeds". It does not: it answers zero for a session that EXISTS and
// has no cursor, and refuses a session that does not exist.
//
// This is a residue rather than a closure. It enumerates the two cursor
// operations and the listing; it does not claim to enumerate every operation in
// this package whose answer depends on a bound scope, and a reader must not
// take the absence of an operation here as evidence about it.
func TestCursorOperationsRefuseASessionWithNoDurableData(t *testing.T) {
	t.Parallel()

	const unborn = sessionwire.SessionID("session-never-written")
	store := consumptionFixture(t, memstore.New())

	if _, err := store.LoadCommandCursor(context.Background(),
		LoadCommandCursorRequest{TenantID: catalogTenant, SessionID: unborn}); err == nil {
		t.Fatal("LoadCommandCursor answered about a session with no durable data")
	}
	if _, err := store.ListSessionCommands(context.Background(), ListSessionCommandsRequest{
		TenantID: catalogTenant, SessionID: unborn, Limit: 10,
	}); err == nil {
		t.Fatal("ListSessionCommands answered about a session with no durable data")
	}
	// A SAVE is the control, and it must NOT refuse: a save binds the scope, so
	// it is the operation that makes such a session exist. Without this the two
	// refusals above would also pass against a store that refused every request
	// naming this session forever.
	mustSaveCursorFor(t, store, catalogTenant, unborn, consumptionEpoch, consumptionOrder)
	entry, err := store.LoadCommandCursor(context.Background(),
		LoadCommandCursorRequest{TenantID: catalogTenant, SessionID: unborn})
	if err != nil {
		t.Fatalf("LoadCommandCursor after a save: %v", err)
	}
	if entry.Cursor.ConsumedOrder != consumptionOrder {
		t.Fatalf("cursor after a save = %d, want %d", entry.Cursor.ConsumedOrder, consumptionOrder)
	}
}

func mustSaveCursorFor(
	t *testing.T,
	store *Store,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	epoch, order uint64,
) CommandCursorEntry {
	t.Helper()
	entry, err := store.SaveCommandCursor(context.Background(), SaveCommandCursorRequest{
		TenantID: tenant, SessionID: session, LeaseEpoch: epoch, ConsumedOrder: order,
	})
	if err != nil {
		t.Fatalf("SaveCommandCursor(%q,%q): %v", tenant, session, err)
	}
	return entry
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
	full := walkSessionCommands(t, store, catalogTenant, catalogSession, 0, 100)
	end := full[len(full)-1].AcceptedOrder

	// The CONTROL: past the end, a faithful provider reports no continuation
	// and this store passes it through.
	page, err := store.ListSessionCommands(context.Background(), ListSessionCommandsRequest{
		TenantID: catalogTenant, SessionID: catalogSession, AfterOrder: end, Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListSessionCommands past the end: %v", err)
	}
	if len(page.Commands) != 0 || page.NextAfterOrder != 0 {
		t.Fatalf("exhausted page = %d rows continuing at %d", len(page.Commands), page.NextAfterOrder)
	}

	store.backend.OrderedIndex = bentOrderedPage{OrderedIndex: ordered, bend: func(p storage.OrderedPage) storage.OrderedPage {
		if len(p.Records) == 0 {
			p.NextAfterOrder = end + 1
		}
		return p
	}}
	_, err = store.ListSessionCommands(context.Background(), ListSessionCommandsRequest{
		TenantID: catalogTenant, SessionID: catalogSession, AfterOrder: end, Limit: 10,
	})
	assertInboxCode(t, err, InboxErrorIdentity)
}

// bentCursorRow is a provider that answers a cursor Get with a row filed
// somewhere other than where this package files one.
//
// memstore cannot produce these answers and a conforming provider never will,
// which is exactly why the filing checks have to be tested against a
// non-conforming one: a fake no looser than the real store would leave every
// one of those checks unexercised, and a check nothing exercises is a comment.
type bentCursorRow struct {
	storage.OrderedIndex
	bend func(storage.OrderedRecord) storage.OrderedRecord
}

func (o bentCursorRow) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	stored, err := o.OrderedIndex.Get(ctx, id)
	if err != nil || id.Namespace != commandCursorNamespace {
		return stored, err
	}
	return o.bend(stored), nil
}

// TestLoadCommandCursorHoldsTheProvidersRowToItsFiling drives every component
// of the filing commandCursorEntryFor checks.
//
// Each case is one thing a provider could get wrong about WHERE a row lives, as
// opposed to what is in it, and each has its own answer so a check that had
// collapsed into another would be visible.
func TestLoadCommandCursorHoldsTheProvidersRowToItsFiling(t *testing.T) {
	t.Parallel()

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

			entry, err := store.LoadCommandCursor(context.Background(), testLoadCursorRequest())
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

// TestSaveCommandCursorHoldsTheProvidersReplyToTheBytesItWrote is the one check
// on this record that a reply consistent with ITSELF still cannot satisfy.
//
// Every other check asks whether the row the provider returned is internally
// coherent, which a SUBSTITUTED row answers just as well as the real one. On a
// write path the provider is claiming something stronger — that it stored THESE
// bytes — and the reply is what the caller takes away and what the next write
// is fenced against, so a provider that answered with another lease's epoch
// would have its answer adopted as the record's own history.
func TestSaveCommandCursorHoldsTheProvidersReplyToTheBytesItWrote(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"create", "update"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			backend := memstore.New()
			ordered := backend.OrderedIndex
			store := consumptionFixture(t, backend)
			// The scope is bound first so the create path is reached with the
			// substituting provider already installed.
			admitSessionCommands(t, store, catalogTenant, catalogSession, "mine", 1)
			if path == "update" {
				mustSaveCursor(t, store, consumptionEpoch, consumptionOrder)
			}

			substitute, _, err := encodeCommandCursor(CommandCursor{
				TenantID: catalogTenant, SessionID: catalogSession,
				// A HIGHER epoch and position than the write names, which is
				// the shape that matters: adopted, it would raise a fence the
				// caller never earned and lock the real writer out.
				LeaseEpoch: consumptionEpoch + 100, ConsumedOrder: consumptionOrder + 100,
				UpdatedAt: consumptionUpdatedAt,
			})
			if err != nil {
				t.Fatalf("encodeCommandCursor: %v", err)
			}
			store.backend.OrderedIndex = substitutingCursorWriter{OrderedIndex: ordered, value: substitute}

			_, err = store.SaveCommandCursor(context.Background(),
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
	if id.Namespace == commandCursorNamespace {
		value = o.value
	}
	return o.OrderedIndex.Create(ctx, id, rankingScope, value, rank, due)
}

func (o substitutingCursorWriter) Update(
	ctx context.Context, id storage.OrderedID, expectedRevision uint64, value []byte, rank storage.Rank, due storage.Due,
) (storage.OrderedRecord, error) {
	if id.Namespace == commandCursorNamespace {
		value = o.value
	}
	return o.OrderedIndex.Update(ctx, id, expectedRevision, value, rank, due)
}
