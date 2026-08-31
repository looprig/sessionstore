package sessionstore

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/token"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// TestControlShardVectorsAreStable pins the placement function against silent
// change. The vectors are PINNED, not derived: they were produced by this
// implementation and are here so that a later edit to the digest domain, the
// framing, or the reduction has to change them on purpose. A vector table
// derived from the code under test would restate the code.
func TestControlShardVectorsAreStable(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New())
	for _, want := range []struct {
		tenant  sessionwire.TenantID
		session sessionwire.SessionID
		count   uint32
		shard   uint32
	}{
		{"tenant-a", "session-1", 16, 13},
		{"tenant-a", "session-2", 16, 1},
		{"tenant-b", "session-1", 16, 12},
		{"tenant-a", "session-1", 1, 0},
		{"tenant-a", "session-1", 64, 61},
		{"tenant-a", "session-1", 4096, 61},

		// The framing vectors. digestFrame length-prefixes each input, so no
		// concatenation of one pair reassociates into another's shard. These
		// are here because a naive "tenant + session" hash would place all
		// three of these identically, and nothing else in the table would
		// notice: it is the one mistake that puts unrelated sessions in one
		// shard while every vector above still looks stable.
		{"ab", "c", 16, 7},
		{"a", "bc", 16, 5},
		{"tenant-asession-1", "", 16, 9},
	} {
		got := store.keys.controlShardOf(want.tenant, want.session, want.count)
		if got != want.shard {
			t.Errorf("controlShardOf(%q, %q, %d) = %d, want %d", want.tenant, want.session, want.count, got, want.shard)
		}
	}
}

// TestControlShardsSpreadEvenlyOverALargeFixture drives the distribution rather
// than reasoning about it.
func TestControlShardsSpreadEvenlyOverALargeFixture(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New())
	const shards, tenants, sessions = 64, 200, 500
	counts := make([]int, shards)
	for tenant := range tenants {
		for session := range sessions {
			shard := store.keys.controlShardOf(
				sessionwire.TenantID(fmt.Sprintf("tenant-%d", tenant)),
				sessionwire.SessionID(fmt.Sprintf("session-%d", session)),
				shards)
			if shard >= shards {
				t.Fatalf("shard %d is outside the configured count", shard)
			}
			counts[shard]++
		}
	}
	expected := float64(tenants*sessions) / shards
	for shard, count := range counts {
		if deviation := math.Abs(float64(count)-expected) / expected; deviation > 0.15 {
			t.Errorf("shard %d holds %d of %d rows (expected ~%.0f, deviation %.3f)",
				shard, count, tenants*sessions, expected, deviation)
		}
	}
}

// TestChangingTheShardCountIsAMigrationRatherThanAFlagFlip is the whole reason
// the count is persisted. A deployment that reopened the same backend with a
// different count would place new records in shards no sweep of the old count
// ever visits, and would look for existing ones where they are not.
func TestChangingTheShardCountIsAMigrationRatherThanAFlagFlip(t *testing.T) {
	t.Parallel()

	backend := memstore.New()
	first := openStore(t, backend, WithControlShards(16))
	if first.ControlShards() != 16 {
		t.Fatalf("ControlShards() = %d, want 16", first.ControlShards())
	}

	_, err := Open(context.Background(), backend, WithControlShards(32))
	var keyspaceErr *KeyspaceError
	if !errors.As(err, &keyspaceErr) || keyspaceErr.Code != KeyspaceLayoutMismatch {
		t.Fatalf("reopen with a different shard count = %T %v, want layout_mismatch", err, err)
	}

	same := openStore(t, backend, WithControlShards(16))
	if same.ControlShards() != 16 {
		t.Fatalf("reopening with the same count = %d, want 16", same.ControlShards())
	}
}

func TestWithControlShardsBoundsTheCount(t *testing.T) {
	t.Parallel()

	for _, count := range []int{-1, 0, MaxControlShards + 1} {
		if _, err := Open(context.Background(), memstore.New(), WithControlShards(count)); !errors.As(err, new(*InvalidOptionError)) {
			t.Errorf("WithControlShards(%d) = %T %v, want *InvalidOptionError", count, err, err)
		}
	}
	for _, count := range []int{1, MaxControlShards} {
		if _, err := Open(context.Background(), memstore.New(), WithControlShards(count)); err != nil {
			t.Errorf("WithControlShards(%d) = %v, want acceptance", count, err)
		}
	}
}

// TestOutstandingRecordsAreFiledInTheirSessionsShard is the assertion the whole
// design rests on: a record's namespace is its shard, and its ORDERING SCOPE is
// still its session. If the shard were the ordering scope instead, two sessions
// that hash together would share an identity space and one could overwrite the
// other's command; if the shard were neither, ListDue could not address it,
// because ListDue takes a namespace and no scope.
func TestOutstandingRecordsAreFiledInTheirSessionsShard(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New(), WithControlShards(8))
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	shard := store.keys.controlShardOf(catalogTenant, catalogSession, 8)
	if scope.ControlShard != shard {
		t.Fatalf("scope.ControlShard = %d, want %d", scope.ControlShard, shard)
	}

	for _, filed := range []struct {
		name string
		id   storage.OrderedID
		base string
	}{
		{"inbox command", inboxID(scope, "command-1"), inboxNamespace},
		{"gate deadline intent", gateIntentID(scope, "gate-1"), gateNamespace},
	} {
		if want := shardNamespace(filed.base, shard); filed.id.Namespace != want {
			t.Errorf("%s namespace = %q, want %q", filed.name, filed.id.Namespace, want)
		}
		if filed.id.OrderingScope != scope.SessionNamespace {
			t.Errorf("%s ordering scope = %q, want the session namespace %q",
				filed.name, filed.id.OrderingScope, scope.SessionNamespace)
		}
		if err := storage.ValidateName(filed.id.Namespace); err != nil {
			t.Errorf("%s namespace is outside the storage name grammar: %v", filed.name, err)
		}
	}
}

// TestShardNamespacesAreDistinctOverTheWholeCrossProduct is the half of
// namespace distinctness that TestOrderedNamespacesAreDistinct cannot state: it
// proves the bases are distinct, and this proves that sharding cannot make two
// distinct bases collide at some shard.
func TestShardNamespacesAreDistinctOverTheWholeCrossProduct(t *testing.T) {
	t.Parallel()

	// Every base this package shards, plus two whose names differ only by a
	// trailing token-alphabet character, which is the shape a concatenation
	// without a separator turns into a longer final segment.
	bases := []string{inboxNamespace, gateNamespace, "a/b", "a/b0"}
	seen := map[string]string{}
	for _, base := range bases {
		for shard := range uint32(MaxControlShards) {
			namespace := shardNamespace(base, shard)
			key := base + "\x00" + controlShardToken(shard)
			if other, ok := seen[namespace]; ok && other != key {
				t.Fatalf("%s and %s share the namespace %q", other, key, namespace)
			}
			seen[namespace] = key
			// Every shard of every base must be a name a provider will accept.
			// The bound that could fail is the length one: a base near the
			// 512-byte ceiling plus a segment is a name this package would file
			// records under and the provider would refuse.
			if err := storage.ValidateName(namespace); err != nil {
				t.Fatalf("shardNamespace(%q, %d) is outside the storage name grammar: %v", base, shard, err)
			}
			// The BASE must be recoverable: the namespace is the base plus ONE
			// segment and nothing else. That is the form the distinctness guard
			// in inbox_test.go reasons in — it resolves the base constant from
			// source and concludes distinct bases give distinct namespaces —
			// and a shard glued onto the base's final segment would leave that
			// reasoning about a name this function no longer produces.
			token, ok := strings.CutPrefix(namespace, base+"/")
			if !ok || strings.Contains(token, "/") {
				t.Fatalf("shardNamespace(%q, %d) = %q, which is not the base plus one segment", base, shard, namespace)
			}
			// The token's width is pinned because the test helpers recognize a
			// shard segment by length rather than by parsing it. It is not a
			// safety property; see controlShardToken.
			if len(token) != 4 {
				t.Fatalf("shard token %q is %d bytes, want the fixed 4", token, len(token))
			}
		}
	}
	if len(seen) != len(bases)*MaxControlShards {
		t.Fatalf("the cross product produced %d namespaces, want %d", len(seen), len(bases)*MaxControlShards)
	}
}

// shardFixture opens a one-shard store, so every session in a test lands in the
// same sweep and a test never has to guess which shard it wrote to. The
// distribution across MORE shards is a property of controlShardOf and is
// measured where that lives, not re-measured through the query.
func shardFixture(t *testing.T) *Store {
	t.Helper()
	return openStore(t, memstore.New(), WithControlShards(1), WithClock(newMovableClock(inboxAcceptedAt)))
}

func admitInto(t *testing.T, store *Store, session sessionwire.SessionID, command sessionwire.CommandID) InboxEntry {
	t.Helper()
	req := testAdmitRequest()
	req.SessionID = session
	req.CommandID = command
	entry, _, err := store.AdmitCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("AdmitCommand(%s/%s): %v", session, command, err)
	}
	return entry
}

// TestListDueCommandsReadsOneShardsDueWorkAndNothingElse is the cost claim in
// its testable form: the page comes from the ordered index's due view of ONE
// shard, so a deployment's tenant count, its terminal commands, and its
// historical rows are not in it.
func TestListDueCommandsReadsOneShardsDueWorkAndNothingElse(t *testing.T) {
	t.Parallel()

	store := shardFixture(t)
	admitInto(t, store, "session-1", "command-1")
	admitInto(t, store, "session-2", "command-2")

	page, err := store.ListDueCommands(context.Background(), ListDueCommandsRequest{
		Shard: 0, DueAtOrBefore: inboxDeadline, Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListDueCommands: %v", err)
	}
	if len(page.Commands) != 2 || page.Examined != 2 || page.Unreadable != 0 {
		t.Fatalf("page = %+v, want the two due commands", page)
	}
	for _, due := range page.Commands {
		if due.Entry.Record.TenantID != catalogTenant {
			t.Errorf("due command carries tenant %q", due.Entry.Record.TenantID)
		}
	}

	// A bound before the deadline reaches nothing, which is what proves the
	// page is selected by the due view rather than by enumeration.
	early, err := store.ListDueCommands(context.Background(), ListDueCommandsRequest{
		Shard: 0, DueAtOrBefore: inboxDeadline.Add(-time.Millisecond), Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListDueCommands(early): %v", err)
	}
	if len(early.Commands) != 0 || early.Examined != 0 {
		t.Fatalf("an earlier bound returned %+v, want nothing", early)
	}
}

// TestListDueCommandsSkipsARowItCannotReadRatherThanFailingThePage is the rule
// this package has been bitten by twice. The due view is ascending by an
// instant that never moves and nothing rewrites an unreadable row, so a reader
// that failed the page on one would switch reconciliation off for every tenant
// in the shard, permanently.
func TestListDueCommandsSkipsARowItCannotReadRatherThanFailingThePage(t *testing.T) {
	t.Parallel()

	store := shardFixture(t)
	readable := admitInto(t, store, "session-1", "command-1")
	admitInto(t, store, "session-2", "command-2")

	scope, err := store.deriveSessionScope(catalogTenant, "session-2")
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	id := inboxID(scope, "command-2")
	stored, err := store.backend.OrderedIndex.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := store.backend.OrderedIndex.Update(
		context.Background(), id, stored.Revision, []byte("{not a record"), stored.Rank, stored.Due); err != nil {
		t.Fatalf("Update: %v", err)
	}

	page, err := store.ListDueCommands(context.Background(), ListDueCommandsRequest{
		Shard: 0, DueAtOrBefore: inboxDeadline, Limit: 10,
	})
	if err != nil {
		t.Fatalf("one corrupt row failed the whole page: %v", err)
	}
	if page.Unreadable != 1 || page.Examined != 2 || len(page.Commands) != 1 {
		t.Fatalf("page = %+v, want one readable command and one counted skip", page)
	}
	if page.Commands[0].Entry.Record.CommandID != readable.Record.CommandID {
		t.Fatalf("reported %q, want the readable command", page.Commands[0].Entry.Record.CommandID)
	}
}

// TestListDueCommandsRefusesARowFiledInAnotherShard is the check a sweep cannot
// do without: it learns a row's identity FROM the row, so a row that computes
// to a different shard than the one being swept is a provider filing this
// reader must not vouch for.
func TestListDueCommandsRefusesARowFiledInAnotherShard(t *testing.T) {
	t.Parallel()

	// Two shards, and a row written by hand into the wrong one.
	store := openStore(t, memstore.New(), WithControlShards(2), WithClock(newMovableClock(inboxAcceptedAt)))
	entry := admitInto(t, store, "session-1", "command-1")
	scope, err := store.deriveSessionScope(catalogTenant, "session-1")
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	stored, err := store.backend.OrderedIndex.Get(context.Background(), inboxID(scope, "command-1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	other := 1 - scope.ControlShard
	if _, _, err := store.backend.OrderedIndex.Create(context.Background(), storage.OrderedID{
		Namespace:     shardNamespace(inboxNamespace, other),
		OrderingScope: scope.SessionNamespace,
		StableKey:     storage.StableKey(entry.Record.CommandID),
	}, scope.SessionNamespace, stored.Value, stored.Rank, stored.Due); err != nil {
		t.Fatalf("Create: %v", err)
	}

	page, err := store.ListDueCommands(context.Background(), ListDueCommandsRequest{
		Shard: int(other), DueAtOrBefore: inboxDeadline, Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListDueCommands: %v", err)
	}
	if len(page.Commands) != 0 || page.Unreadable != 1 || page.Examined != 1 {
		t.Fatalf("page = %+v, want the misfiled row counted and dropped", page)
	}
}

// TestListDueCommandsPagesToExhaustionWithoutRepeatingOrLosingARow drives the
// round-robin caller's inner loop: a shard is swept by following its own
// continuation until there is none.
func TestListDueCommandsPagesToExhaustionWithoutRepeatingOrLosingARow(t *testing.T) {
	t.Parallel()

	store := shardFixture(t)
	const commands = 7
	for i := range commands {
		admitInto(t, store, sessionwire.SessionID(fmt.Sprintf("session-%d", i)), inboxCommand)
	}

	seen := map[sessionwire.SessionID]int{}
	req := ListDueCommandsRequest{Shard: 0, DueAtOrBefore: inboxDeadline, Limit: 2}
	pages := 0
	for {
		page, err := store.ListDueCommands(context.Background(), req)
		if err != nil {
			t.Fatalf("ListDueCommands: %v", err)
		}
		pages++
		if pages > commands+2 {
			t.Fatal("the sweep did not terminate")
		}
		for _, due := range page.Commands {
			seen[due.Entry.Record.SessionID]++
		}
		if page.NextCursor == "" {
			break
		}
		req = ListDueCommandsRequest{Shard: 0, Cursor: page.NextCursor, Limit: 2}
	}
	if len(seen) != commands {
		t.Fatalf("the sweep saw %d sessions, want %d: %v", len(seen), commands, seen)
	}
	for session, count := range seen {
		if count != 1 {
			t.Errorf("%s was reported %d times", session, count)
		}
	}
	if pages < 2 {
		t.Fatalf("the sweep finished in %d page(s); the continuation is not being exercised", pages)
	}
}

// TestADueCommandsCursorCannotBeReplayedIntoAnotherShard holds the cursor scope
// to what it is for. A token is not authority — anyone can construct a
// well-formed envelope — but a token accepted for the wrong SHARD would resume
// a sweep of shard 3 at shard 4's position and silently skip whatever lay
// between.
func TestADueCommandsCursorCannotBeReplayedIntoAnotherShard(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New(), WithControlShards(4), WithClock(newMovableClock(inboxAcceptedAt)))
	for i := range 8 {
		admitInto(t, store, sessionwire.SessionID(fmt.Sprintf("session-%d", i)), inboxCommand)
	}
	var issued sessionwire.Cursor
	var issuedShard int
	for shard := range 4 {
		page, err := store.ListDueCommands(context.Background(), ListDueCommandsRequest{
			Shard: shard, DueAtOrBefore: inboxDeadline, Limit: 1,
		})
		if err != nil {
			t.Fatalf("ListDueCommands: %v", err)
		}
		if page.NextCursor != "" {
			issued, issuedShard = page.NextCursor, shard
			break
		}
	}
	if issued == "" {
		t.Fatal("no shard issued a continuation; the fixture proves nothing")
	}
	for shard := range 4 {
		if shard == issuedShard {
			continue
		}
		_, err := store.ListDueCommands(context.Background(), ListDueCommandsRequest{Shard: shard, Cursor: issued, Limit: 1})
		assertInboxCode(t, err, InboxErrorCursor)
	}
	if _, err := store.ListDueCommands(context.Background(), ListDueCommandsRequest{
		Shard: issuedShard, Cursor: issued, Limit: 1,
	}); err != nil {
		t.Fatalf("the shard that issued the cursor refused it: %v", err)
	}
}

// --- gate deadline intent retirement --------------------------------------

// retireFixture opens a one-shard store on a movable clock with one session
// that has a durable journal tip, so gates can name events that already exist.
func retireFixture(t *testing.T) (*Store, *movableClock) {
	t.Helper()
	clock := newMovableClock(catalogActiveAt)
	store := openStore(t, memstore.New(), WithControlShards(1), WithClock(clock))
	mustPrepareSession(t, store, catalogTenant, catalogSession, 10)
	return store, clock
}

func retireRequest(gate sessionwire.GateID, revision uint64) RetireGateDeadlineIntentRequest {
	return RetireGateDeadlineIntentRequest{
		TenantID: catalogTenant, SessionID: catalogSession, GateID: gate, Revision: revision,
	}
}

// storedGateIntent reads one intent straight from the provider, so a test can
// assert what a retire did to the row rather than what the operation said.
func storedGateIntent(t *testing.T, store *Store, gate sessionwire.GateID) storage.OrderedRecord {
	t.Helper()
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	stored, err := store.backend.OrderedIndex.Get(context.Background(), gateIntentID(scope, gate))
	if err != nil {
		t.Fatalf("Get(intent %s): %v", gate, err)
	}
	return stored
}

// seedRemnantOfACrashBeforeGateOpened produces the ONE crash remnant OpenGate
// can leave: the intent is durable and the open projection never committed,
// because OpenGate writes the intent first on purpose.
func seedRemnantOfACrashBeforeGateOpened(t *testing.T, store *Store, gate sessionwire.GateID) uint64 {
	t.Helper()
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	value, err := encodeGateIntent(gateIntent{
		TenantID: catalogTenant, SessionID: catalogSession, GateID: gate,
		OpenedEventID: "event-" + sessionwire.EventID(gate), OpenedJournalSeq: 5,
		Deadline: catalogDeadline, RecordedAt: store.clock.Now(),
	})
	if err != nil {
		t.Fatalf("encodeGateIntent: %v", err)
	}
	stored, _, err := store.backend.OrderedIndex.Create(
		context.Background(), gateIntentID(scope, gate), scope.SessionNamespace,
		value, storage.Rank{}, gateDue(catalogDeadline))
	if err != nil {
		t.Fatalf("seed remnant: %v", err)
	}
	return stored.Revision
}

func TestRetireGateDeadlineIntentClearsARemnantOfACrashBeforeGateOpened(t *testing.T) {
	t.Parallel()

	store, clock := retireFixture(t)
	revision := seedRemnantOfACrashBeforeGateOpened(t, store, "gate-crashed")
	clock.set(catalogActiveAt.Add(MinGateIntentRemnantAge))

	if err := store.RetireGateDeadlineIntent(context.Background(), retireRequest("gate-crashed", revision)); err != nil {
		t.Fatalf("RetireGateDeadlineIntent: %v", err)
	}
	if stored := storedGateIntent(t, store, "gate-crashed"); !stored.Deleted {
		t.Fatal("the remnant was not tombstoned")
	}
}

func TestRetireGateDeadlineIntentClearsARemnantOfAnInterruptedResolve(t *testing.T) {
	t.Parallel()

	store, clock := retireFixture(t)
	mustOpenGate(t, store, 1, testGate("gate-a", 5))
	// A resolve clears the projection BEFORE it retires the intent, so an
	// interruption between the two leaves exactly this. It is reproduced by
	// re-projecting the session's gates wholesale, which is the ordinary path
	// documented as leaving a remnant behind.
	if _, err := store.UpdateCatalogHostState(context.Background(), UpdateCatalogHostStateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1,
		State: sessionwire.SessionStateRunning, Residency: sessionwire.SessionResidencyResident,
		LastActiveAt: catalogActiveAt, LastJournalSeq: 10, LastEventID: "event-tip",
	}); err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}
	clock.set(catalogActiveAt.Add(MinGateIntentRemnantAge))

	// The revision comes from the PAGE, not from a direct read, because that is
	// the whole path a sweeper walks: ListDueGates reports the remnant and the
	// revision, and the retirement is a compare-and-swap onto it. Reading the
	// row here instead would leave the reporting half untested and the two free
	// to disagree.
	page, err := store.ListDueGates(context.Background(), ListDueGatesRequest{
		Shard: 0, DueAtOrBefore: catalogDeadline, Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListDueGates: %v", err)
	}
	if len(page.Remnants) != 1 || len(page.Gates) != 0 {
		t.Fatalf("page = %+v, want exactly the one remnant", page)
	}
	remnant := page.Remnants[0]
	if remnant.TenantID != catalogTenant || remnant.SessionID != catalogSession || remnant.GateID != "gate-a" {
		t.Fatalf("remnant identity = %+v, want the dropped gate", remnant)
	}

	// A reported remnant carries exactly what a retirement names, so the
	// conversion is legal today. Nothing in production relies on that, and if
	// the two shapes ever diverge this line stops compiling — which is the loud
	// failure, not a silent one.
	if err := store.RetireGateDeadlineIntent(
		context.Background(), RetireGateDeadlineIntentRequest(remnant)); err != nil {
		t.Fatalf("RetireGateDeadlineIntent: %v", err)
	}
	if stored := storedGateIntent(t, store, "gate-a"); !stored.Deleted {
		t.Fatal("the remnant was not tombstoned")
	}
}

// TestRetireGateDeadlineIntentRefusesAGateTheProjectionStillOpens is the one
// outcome this operation must never produce. Retiring a live gate's deadline
// would leave it publicly open and waiting with nothing to expire it, which is
// the state gates.go promises no path produces.
func TestRetireGateDeadlineIntentRefusesAGateTheProjectionStillOpens(t *testing.T) {
	t.Parallel()

	store, clock := retireFixture(t)
	mustOpenGate(t, store, 1, testGate("gate-a", 5))
	revision := storedGateIntent(t, store, "gate-a").Revision
	clock.set(catalogActiveAt.Add(24 * time.Hour))

	err := store.RetireGateDeadlineIntent(context.Background(), retireRequest("gate-a", revision))
	assertCatalogField(t, err, CatalogErrorConflict, "gate_id")
	if stored := storedGateIntent(t, store, "gate-a"); stored.Deleted {
		t.Fatal("a live gate's deadline was retired")
	}
}

// TestRetireGateDeadlineIntentRefusesAnIntentYoungerThanTheRemnantAge drives
// the in-flight window, AT the threshold in both directions.
//
// This is the hazard the whole age rule exists for. OpenGate makes the intent
// durable before it commits the projection, so "an intent with no matching open
// gate" is produced BOTH by a crashed open and by an open that is happening
// right now — and nothing in the two records distinguishes them. Only elapsed
// time can, so retirement waits out a window longer than any single OpenGate
// call, and refuses inside it.
func TestRetireGateDeadlineIntentRefusesAnIntentYoungerThanTheRemnantAge(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		age   time.Duration
		allow bool
	}{
		{"one nanosecond inside the window", MinGateIntentRemnantAge - time.Nanosecond, false},
		{"exactly at the window", MinGateIntentRemnantAge, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store, clock := retireFixture(t)
			revision := seedRemnantOfACrashBeforeGateOpened(t, store, "gate-young")
			clock.set(catalogActiveAt.Add(tt.age))

			err := store.RetireGateDeadlineIntent(context.Background(), retireRequest("gate-young", revision))
			if tt.allow {
				if err != nil {
					t.Fatalf("RetireGateDeadlineIntent at the threshold = %v, want acceptance", err)
				}
				return
			}
			assertCatalogField(t, err, CatalogErrorTooSoon, "recorded_at")
			if stored := storedGateIntent(t, store, "gate-young"); stored.Deleted {
				t.Fatal("an intent inside the open window was retired")
			}
		})
	}
}

func TestRetireGateDeadlineIntentRefusesAStaleRevision(t *testing.T) {
	t.Parallel()

	store, clock := retireFixture(t)
	revision := seedRemnantOfACrashBeforeGateOpened(t, store, "gate-crashed")
	clock.set(catalogActiveAt.Add(MinGateIntentRemnantAge))

	err := store.RetireGateDeadlineIntent(context.Background(), retireRequest("gate-crashed", revision+1))
	assertCatalogCode(t, err, CatalogErrorConflict)
	if stored := storedGateIntent(t, store, "gate-crashed"); stored.Deleted {
		t.Fatal("a stale revision retired the row anyway")
	}
}

// TestRetireGateDeadlineIntentDoesNotReadAnAbsentIntentAsRetired enumerates the
// one not-found on this path that must NOT license the caller's conclusion.
//
// "Already retired" has a durable spelling — a tombstone — because this package
// never erases. A row that is simply absent is therefore not a retired intent;
// it is a caller retiring something this store has never held, which is what a
// stale continuation from another deployment, or a mis-decoded row, looks like.
// Answering success would tell a sweeper it had handled a row it never touched.
func TestRetireGateDeadlineIntentDoesNotReadAnAbsentIntentAsRetired(t *testing.T) {
	t.Parallel()

	store, clock := retireFixture(t)
	clock.set(catalogActiveAt.Add(MinGateIntentRemnantAge))
	err := store.RetireGateDeadlineIntent(context.Background(), retireRequest("gate-never-existed", 1))
	assertCatalogField(t, err, CatalogErrorNotFound, "gate_intent")
}

// TestRetireGateDeadlineIntentIsIdempotentOnItsOwnTombstone is the repeat case.
// A caller cannot tell a lost reply from a failure, so a second retire is the
// ordinary case rather than a mistake — and the state it finds is a tombstone,
// which is this store's own record that the work is done.
func TestRetireGateDeadlineIntentIsIdempotentOnItsOwnTombstone(t *testing.T) {
	t.Parallel()

	store, clock := retireFixture(t)
	revision := seedRemnantOfACrashBeforeGateOpened(t, store, "gate-crashed")
	clock.set(catalogActiveAt.Add(MinGateIntentRemnantAge))
	if err := store.RetireGateDeadlineIntent(context.Background(), retireRequest("gate-crashed", revision)); err != nil {
		t.Fatalf("first retire: %v", err)
	}
	// The revision moved when the tombstone was written, and the repeat still
	// succeeds: it is answered by the row's STATE, not by a second CAS the
	// caller could not have the revision for.
	if err := store.RetireGateDeadlineIntent(context.Background(), retireRequest("gate-crashed", revision)); err != nil {
		t.Fatalf("repeat retire: %v", err)
	}
}

// TestRetireGateDeadlineIntentRetiresIntentsOfASessionWithNoDurableExistence is
// the not-found that DOES license action, and it is deliberately the exact set
// noSuchSession names: an absent catalog record, a tombstoned one, and an
// unbound session witness. A session that has no durable existence cannot have
// a durably open gate, so every intent it carries is a remnant. Every OTHER
// failure reading the session is a reason to stop.
func TestRetireGateDeadlineIntentRetiresIntentsOfASessionWithNoDurableExistence(t *testing.T) {
	t.Parallel()

	clock := newMovableClock(catalogActiveAt)
	store := openStore(t, memstore.New(), WithControlShards(1), WithClock(clock))
	// No catalog record at all: the intent is the session's only durable row.
	revision := seedRemnantOfACrashBeforeGateOpened(t, store, "gate-orphan")
	clock.set(catalogActiveAt.Add(MinGateIntentRemnantAge))

	if err := store.RetireGateDeadlineIntent(context.Background(), retireRequest("gate-orphan", revision)); err != nil {
		t.Fatalf("RetireGateDeadlineIntent: %v", err)
	}
	if stored := storedGateIntent(t, store, "gate-orphan"); !stored.Deleted {
		t.Fatal("an orphaned session's remnant was not tombstoned")
	}
}

// TestAStillOpenGateStaysDueWithoutBlockingLaterCursorPages is the
// head-of-line requirement in its testable form.
//
// A gate that is genuinely open and past its deadline is CURRENT DUE WORK: it
// must keep being reported, pass after pass, because nothing has dealt with it
// and nothing may retire it. What it must not do is stop the sweep from
// reaching everything behind it — which is exactly what happened before there
// was a continuation, and what MaxPages alone did not fix, because bounding a
// pass's cost does not move the row that is blocking it.
func TestAStillOpenGateStaysDueWithoutBlockingLaterCursorPages(t *testing.T) {
	t.Parallel()

	clock := newMovableClock(catalogActiveAt)
	store := openStore(t, memstore.New(), WithControlShards(1), WithClock(clock))

	// The blocker sorts first: its deadline is the earliest, so every page read
	// from the head of the view begins with it.
	mustPrepareSession(t, store, catalogTenant, "session-blocker", 10)
	mustOpenGateOn(t, store, catalogTenant, "session-blocker",
		gateWithDeadline(testGate("gate-blocker", 5), catalogDeadline.Add(-time.Hour)))
	const behind = 4
	for i := range behind {
		session := sessionwire.SessionID(fmt.Sprintf("session-behind-%d", i))
		mustPrepareSession(t, store, catalogTenant, session, 10)
		mustOpenGateOn(t, store, catalogTenant, session,
			gateWithDeadline(testGate("gate-behind", 5), catalogDeadline.Add(time.Duration(i)*time.Minute)))
	}

	// A limit of one is the sharpest form of the hazard: without a
	// continuation, every page would consist of the blocker and nothing else,
	// forever.
	seen := map[string]int{}
	req := ListDueGatesRequest{Shard: 0, DueAtOrBefore: catalogDeadline.Add(time.Hour), Limit: 1}
	for pages := 0; ; pages++ {
		if pages > behind+4 {
			t.Fatal("the sweep did not terminate")
		}
		page, err := store.ListDueGates(context.Background(), req)
		if err != nil {
			t.Fatalf("ListDueGates: %v", err)
		}
		for _, gate := range page.Gates {
			seen[string(gate.SessionID)]++
		}
		if page.NextCursor == "" {
			break
		}
		req = ListDueGatesRequest{Shard: 0, Cursor: page.NextCursor, Limit: 1}
	}
	if seen["session-blocker"] != 1 {
		t.Errorf("the still-open gate was reported %d times, want once per pass", seen["session-blocker"])
	}
	for i := range behind {
		session := fmt.Sprintf("session-behind-%d", i)
		if seen[session] != 1 {
			t.Errorf("%s was reported %d times; the blocker starved it", session, seen[session])
		}
	}

	// And it is still there on the NEXT pass, because nothing retired it: a
	// still-open gate is current due work, not a remnant.
	next, err := store.ListDueGates(context.Background(), ListDueGatesRequest{
		Shard: 0, DueAtOrBefore: catalogDeadline.Add(time.Hour), Limit: 1,
	})
	if err != nil {
		t.Fatalf("ListDueGates: %v", err)
	}
	if len(next.Gates) != 1 || next.Gates[0].SessionID != "session-blocker" {
		t.Fatalf("the second pass began with %+v, want the still-open gate", next.Gates)
	}
	if len(next.Remnants) != 0 {
		t.Fatalf("a live gate was reported as a remnant: %+v", next.Remnants)
	}
}

func assertCatalogField(t *testing.T, err error, code CatalogErrorCode, field string) {
	t.Helper()
	got := assertCatalogCode(t, err, code)
	if got.Field != field {
		t.Fatalf("catalog field = %q, want %q (%v)", got.Field, field, err)
	}
}

// --- operation guards -----------------------------------------------------

// declaredStoreRequestOperations is declaredStoreOperations narrowed to the
// operations that TAKE a request, by SIGNATURE rather than by a hand-written
// exclusion list.
//
// It exists because shards.go declares one public method that is not an
// operation at all: ControlShards reads a persisted number, takes no context
// and no request, and therefore cannot be handed a malformed one or refused
// after Close. Naming it here would be a hand-picked exception that a second
// such accessor would silently escape; asking the SYNTAX which methods take a
// request covers the next one too.
//
// It is a composition of the source-parsing helpers this package already has
// rather than a sixth walk of its own, which is what the toolkit note in
// parseProductionFile asks for.
func declaredStoreRequestOperations(t *testing.T, filename string) map[string]bool {
	t.Helper()
	declared := declaredStoreOperations(t, filename)
	taking := map[string]bool{}
	for _, declaration := range parseProductionFile(t, filename).Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || !declared[function.Name.Name] {
			continue
		}
		if len(function.Type.Params.List) >= 2 {
			taking[function.Name.Name] = true
		}
	}
	if len(taking) == 0 {
		t.Fatalf("no request-taking operations were found in %s; the narrowing is not reaching them", filename)
	}
	if len(taking) == len(declared) {
		t.Fatalf("every operation in %s takes a request, so this narrowing is not narrowing: %v", filename, declared)
	}
	return taking
}

// TestShardOperationsRefuseAMalformedRequestBeforeTheProvider holds each
// service operation to the package's ordering rule: validation precedes
// admission precedes any provider call.
//
// The table is held to the declarations in BOTH directions, so an operation
// added to shards.go without a malformed-request case fails here rather than
// shipping unvalidated.
func TestShardOperationsRefuseAMalformedRequestBeforeTheProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		call      func(*Store) error
		assert    func(*testing.T, error)
	}{
		{
			name:      "due commands in a shard this backend does not have",
			operation: "ListDueCommands",
			call: func(store *Store) error {
				_, err := store.ListDueCommands(context.Background(), ListDueCommandsRequest{
					Shard: store.ControlShards(), DueAtOrBefore: inboxDeadline, Limit: 10})
				return err
			},
			assert: func(t *testing.T, err error) {
				t.Helper()
				if got := assertInboxCode(t, err, InboxErrorInvalid); got.Field != "shard" {
					t.Fatalf("field = %q, want shard", got.Field)
				}
			},
		},
		{
			name:      "due commands stating the bound twice",
			operation: "ListDueCommands",
			call: func(store *Store) error {
				_, err := store.ListDueCommands(context.Background(), ListDueCommandsRequest{
					Shard: 0, DueAtOrBefore: inboxDeadline, Cursor: "not-a-cursor", Limit: 10})
				return err
			},
			assert: func(t *testing.T, err error) {
				t.Helper()
				if got := assertInboxCode(t, err, InboxErrorInvalid); got.Field != "due_at_or_before" {
					t.Fatalf("field = %q, want due_at_or_before", got.Field)
				}
			},
		},
		{
			name:      "due gates in a shard this backend does not have",
			operation: "ListDueGates",
			call: func(store *Store) error {
				_, err := store.ListDueGates(context.Background(), ListDueGatesRequest{
					Shard: -1, DueAtOrBefore: catalogDeadline, Limit: 10})
				return err
			},
			assert: func(t *testing.T, err error) {
				t.Helper()
				assertCatalogField(t, err, CatalogErrorInvalid, "shard")
			},
		},
		{
			name:      "retire without a gate",
			operation: "RetireGateDeadlineIntent",
			call: func(store *Store) error {
				return store.RetireGateDeadlineIntent(context.Background(), retireRequest("", 1))
			},
			assert: func(t *testing.T, err error) {
				t.Helper()
				assertCatalogField(t, err, CatalogErrorInvalid, "gate_id")
			},
		},
	}

	// ListDueGates is declared in gates.go and is held to gates.go's own
	// tables; only the shard argument it gained is this file's business, which
	// is why the coverage check below names shards.go alone.
	declared := declaredStoreRequestOperations(t, "shards.go")
	covered := map[string]bool{}
	for _, test := range tests {
		covered[test.operation] = true
	}
	for name := range declared {
		if !covered[name] {
			t.Errorf("%s takes a request and no case here gives it a malformed one", name)
		}
	}
	for name := range covered {
		if !declared[name] && name != "ListDueGates" {
			t.Errorf("a case drives %s, which shards.go does not declare", name)
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			base := memstore.New()
			recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
			base.OrderedIndex = recorder
			store := openStore(t, base, WithControlShards(4))
			test.assert(t, test.call(store))
			if calls := recorder.snapshot(); len(calls) != 0 {
				t.Fatalf("a refused request reached the provider: %+v", calls)
			}

			closing, err := Open(context.Background(), memstore.New(), WithControlShards(4))
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

func TestShardOperationsRefuseAfterClose(t *testing.T) {
	store := openStore(t, memstore.New(), WithControlShards(1))
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	operations := map[string]func() error{
		"ListDueCommands": func() error {
			_, err := store.ListDueCommands(context.Background(), ListDueCommandsRequest{
				Shard: 0, DueAtOrBefore: inboxDeadline, Limit: 10})
			return err
		},
		"RetireGateDeadlineIntent": func() error {
			return store.RetireGateDeadlineIntent(context.Background(), retireRequest("gate-a", 1))
		},
	}

	declared := declaredStoreRequestOperations(t, "shards.go")
	for name := range declared {
		if operations[name] == nil {
			t.Errorf("shards.go declares the public operation %s and this test does not exercise it", name)
		}
	}
	for name := range operations {
		if !declared[name] {
			t.Errorf("this test exercises %s, which shards.go no longer declares (was it moved?)", name)
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

// TestRetireGateDeadlineIntentRefusesACollidingSessionWitness is the other side
// of the witness relaxation. An UNBOUND witness is an answer — the session does
// not durably exist — but a witness bound to a DIFFERENT identity is a hash
// collision, and retiring a row reached through a colliding derived name would
// destroy another session's deadline.
func TestRetireGateDeadlineIntentRefusesACollidingSessionWitness(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = recorder
	clock := newMovableClock(catalogActiveAt)
	store := openStore(t, base, WithControlShards(1), WithClock(clock))
	mustPrepareSession(t, store, catalogTenant, catalogSession, 10)
	revision := seedRemnantOfACrashBeforeGateOpened(t, store, "gate-crashed")
	clock.set(catalogActiveAt.Add(MinGateIntentRemnantAge))

	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	_, revisionOfWitness, err := store.backend.KV.Get(context.Background(), scope.sessionWitnessKey)
	if err != nil {
		t.Fatalf("Get(witness): %v", err)
	}
	if _, err := store.backend.KV.Put(
		context.Background(), scope.sessionWitnessKey, revisionOfWitness,
		[]byte("another session entirely")); err != nil {
		t.Fatalf("Put(witness): %v", err)
	}

	before := len(recorder.snapshot())
	err = store.RetireGateDeadlineIntent(context.Background(), retireRequest("gate-crashed", revision))
	var keyspaceErr *KeyspaceError
	if !errors.As(err, &keyspaceErr) || keyspaceErr.Code != KeyspaceHashCollision {
		t.Fatalf("retire under a colliding witness = %T %v, want hash_collision", err, err)
	}
	// The ORDER is the assertion, not merely the refusal. A refusal that
	// arrives after the intent has been read through the colliding name would
	// have trusted a derived record name on its own — the rule this package
	// applies on every read path — and it would leave the operation resting on
	// whichever later check happened to catch it. No ordered record may be
	// touched at all.
	if calls := recorder.snapshot()[before:]; len(calls) != 0 {
		t.Fatalf("a colliding derived name reached the ordered index before it was refused: %+v", calls)
	}
	if stored := storedGateIntent(t, store, "gate-crashed"); stored.Deleted {
		t.Fatal("a colliding derived name retired a row")
	}
}

// TestRetireGateDeadlineIntentDoesNotReadAProviderFailureAsAnAbsentSession is
// what keeps the not-found that LICENSES a retirement narrow. "The session does
// not durably exist" is a claim, and a provider that is merely failing supports
// no such claim — so a retirement that treated any read failure as permission
// would destroy live gates' deadlines during an outage, which is precisely when
// nothing is watching.
func TestRetireGateDeadlineIntentDoesNotReadAProviderFailureAsAnAbsentSession(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	clock := newMovableClock(catalogActiveAt)
	store := openStore(t, base, WithControlShards(1), WithClock(clock))
	mustPrepareSession(t, store, catalogTenant, catalogSession, 10)
	revision := seedRemnantOfACrashBeforeGateOpened(t, store, "gate-crashed")
	clock.set(catalogActiveAt.Add(MinGateIntentRemnantAge))

	// Only the catalog read fails; the intent read above it still succeeds, so
	// the operation really reaches the session check and fails there.
	hostile.failGetsIn(catalogNamespace, errors.New("provider unavailable"))
	err := store.RetireGateDeadlineIntent(context.Background(), retireRequest("gate-crashed", revision))
	assertCatalogCode(t, err, CatalogErrorBackend)

	hostile.failGetsIn(catalogNamespace, nil)
	if stored := storedGateIntent(t, store, "gate-crashed"); stored.Deleted {
		t.Fatal("a provider failure was read as permission to retire")
	}
}

// TestListDueGatesRefusesAnIntentFiledInAnotherShard is the gate half of the
// shard filing check. A due page learns a row's identity FROM the row, and the
// shard is a function of that identity, so a row whose bytes hash elsewhere is
// filed where it does not belong. Reporting it as a REMNANT would be the worse
// half: it would aim a retirement at a row the shard that owns it is still
// responsible for.
func TestListDueGatesRefusesAnIntentFiledInAnotherShard(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New(), WithControlShards(2))
	mustPrepareSession(t, store, catalogTenant, "session-1", 100)
	scope, err := store.deriveSessionScope(catalogTenant, "session-1")
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	intent := gateIntent{
		TenantID: catalogTenant, SessionID: "session-1", GateID: "gate-a",
		OpenedEventID: "event-gate-a", OpenedJournalSeq: 5,
		Deadline: catalogDeadline.Add(-time.Hour), RecordedAt: catalogActiveAt,
	}
	other := 1 - scope.ControlShard
	if _, _, err := store.backend.OrderedIndex.Create(context.Background(), storage.OrderedID{
		Namespace:     shardNamespace(gateNamespace, other),
		OrderingScope: scope.SessionNamespace,
		StableKey:     storage.StableKey(intent.GateID),
	}, scope.SessionNamespace, mustEncodeGateIntent(t, intent), storage.Rank{}, gateDue(intent.Deadline)); err != nil {
		t.Fatalf("Create: %v", err)
	}

	page, err := store.ListDueGates(context.Background(), ListDueGatesRequest{
		Shard: int(other), DueAtOrBefore: catalogDeadline, Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListDueGates: %v", err)
	}
	if page.Examined != 1 || page.Unreadable != 1 || len(page.Gates) != 0 || len(page.Remnants) != 0 {
		t.Fatalf("page = %+v, want the misfiled row counted and neither reported nor made retireable", page)
	}
}

// TestCursorScopeDomainsAreDistinct is the second half of what stops one query
// family's continuation being replayed into another's, and it is the half
// nothing was checking.
//
// TestCursorMagicsAreDistinct pins the KIND tag and calls the scope digests
// "defence in depth"; that is exactly why a duplicated scope domain would be
// invisible. Two kinds sharing a domain string means their scope digests are
// equal for equal inputs, so the magic becomes the ONLY barrier between them —
// a single-point defence described in two places as a layered one.
//
// The set is derived from source rather than listed here, for the reason both
// of the other distinctness guards derive theirs: a hand-written list covers
// the kinds its author remembered. What is scanned for is every string literal
// handed to digestFrame as its DOMAIN, which is the only way a scope domain is
// ever spelled, so a kind added later is covered whether or not anyone updates
// this test.
func TestCursorScopeDomainsAreDistinct(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	domains := map[string][]string{}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		ast.Inspect(parseProductionFile(t, name), func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			callee, ok := call.Fun.(*ast.Ident)
			if !ok || callee.Name != "digestFrame" {
				return true
			}
			literal, ok := call.Args[0].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				t.Fatalf("%s calls digestFrame with a domain that is not a string literal", name)
			}
			text, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatalf("%s: unquote domain: %v", name, err)
			}
			domains[text] = append(domains[text], name)
			return true
		})
	}

	// Anti-vacuity: the scan must reach the domains this package is known to
	// have, or a broken walk would report a vacuous pass. The named ones prove
	// it resolved a literal rather than merely visiting a call; the two here
	// are a CURSOR scope and a KEY derivation, because the same helper serves
	// both and a walk that reached only one would leave half the set unchecked.
	for _, known := range []string{
		"looprig/sessionstore/duegate/cursor/v1",
		"looprig/sessionstore/key/v1/session",
	} {
		if len(domains[known]) == 0 {
			t.Fatalf("the scan did not reach %s, a domain it is known to cover: %v", known, domains)
		}
	}
	if len(domains) < 8 {
		t.Fatalf("found %d digest domains (%v); the scan is not reaching the declarations", len(domains), domains)
	}

	for domain, sites := range domains {
		if len(sites) > 1 {
			t.Errorf("the digest domain %q is used at %v; a domain is what separates one derivation from another", domain, sites)
		}
	}
}

// TestSweepCursorsEnforceTheirPayloadBoundsOnIssueAndOnPresentation drives the
// two ends of the envelope's payload bounds that no ordinary page reaches.
//
// The codecs are exercised directly rather than through a page, because both
// bounds are about tokens the SWEEP never produces: a provider token larger
// than the envelope can carry, and a continuation with no provider position in
// it at all. No cursor literal is constructed — the tokens come from this
// store's own encoder, so opacity holds — and what is asserted is the decoder's
// answer to them.
func TestSweepCursorsEnforceTheirPayloadBoundsOnIssueAndOnPresentation(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New(), WithControlShards(2))
	oversized := storage.DueCursor(strings.Repeat("p", maxDueCommandCursorPayload))

	// ISSUE. A continuation this store could not accept back is a sweep that
	// silently reverts to making no progress at the head of the view, so the
	// ceiling is enforced where the token is built and not only where it is
	// read.
	if _, err := store.encodeDueCommandCursor(1, 0, oversized); err == nil {
		t.Fatal("an oversized due-commands continuation was issued")
	} else {
		assertInboxCode(t, err, InboxErrorBackend)
	}
	if _, err := store.encodeDueGateCursor(1, 0, storage.DueCursor(oversized)); err == nil {
		t.Fatal("an oversized due-gates continuation was issued")
	} else {
		assertCatalogCode(t, err, CatalogErrorBackend)
	}

	// PRESENTATION. A continuation this sweep issues always carries at least
	// one provider byte beyond the bound, because an exhausted view returns no
	// cursor at all. A token carrying only a bound is therefore one this store
	// never issued, and accepting it would resume a sweep at the head of the
	// view while the caller believed it was making progress.
	boundOnly, err := store.encodeDueCommandCursor(1, 0, "")
	if err != nil {
		t.Fatalf("encodeDueCommandCursor: %v", err)
	}
	if _, _, err := store.decodeDueCommandCursor(1, boundOnly); err == nil {
		t.Fatal("a due-commands continuation with no provider position was accepted")
	} else {
		assertInboxCode(t, err, InboxErrorCursor)
	}
	gateBoundOnly, err := store.encodeDueGateCursor(1, 0, "")
	if err != nil {
		t.Fatalf("encodeDueGateCursor: %v", err)
	}
	if _, _, err := store.decodeDueGateCursor(1, gateBoundOnly); err == nil {
		t.Fatal("a due-gates continuation with no provider position was accepted")
	} else {
		assertCatalogCode(t, err, CatalogErrorCursor)
	}
}

// TestConcurrentRetirementsOfOneRemnantTombstoneItOnce is what makes the
// revision the caller carries binding rather than advisory.
//
// Several replicas can read the same remnant from the same page — that is the
// ordinary case for a sweep, not a rare one — and they all present the same
// revision. Exactly one write may land. The others must be refused as
// conflicts, never succeed silently, because a second successful retirement
// would mean the operation's answer did not depend on the state it was
// evaluated against.
func TestConcurrentRetirementsOfOneRemnantTombstoneItOnce(t *testing.T) {
	store, clock := retireFixture(t)
	revision := seedRemnantOfACrashBeforeGateOpened(t, store, "gate-crashed")
	clock.set(catalogActiveAt.Add(MinGateIntentRemnantAge))

	const replicas = 8
	results := make(chan error, replicas)
	start := make(chan struct{})
	for range replicas {
		go func() {
			<-start
			results <- store.RetireGateDeadlineIntent(context.Background(), retireRequest("gate-crashed", revision))
		}()
	}
	close(start)

	succeeded := 0
	for range replicas {
		switch err := <-results; {
		case err == nil:
			succeeded++
		default:
			// A loser either lost the compare-and-swap or arrived after the
			// tombstone was durable and read it. The second is the idempotent
			// repeat, which returns nil, so anything else here must be the
			// conflict — and nothing else at all.
			assertCatalogCode(t, err, CatalogErrorConflict)
		}
	}
	if succeeded == 0 {
		t.Fatal("no replica retired the remnant")
	}
	if stored := storedGateIntent(t, store, "gate-crashed"); !stored.Deleted {
		t.Fatal("the remnant was not tombstoned")
	}
}
