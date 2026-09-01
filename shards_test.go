package sessionstore

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
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

// TestShardNamespacesAreDistinctAtEveryShard is the half of
// namespace distinctness that TestOrderedNamespacesAreDistinct cannot state: it
// proves the bases are distinct, and this proves that sharding cannot make two
// distinct bases collide at some shard.
func TestShardNamespacesAreDistinctAtEveryShard(t *testing.T) {
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
	value, _, err := encodeGateIntent(gateIntent{
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
			name:      "due gates stating the bound twice",
			operation: "ListDueGates",
			call: func(store *Store) error {
				_, err := store.ListDueGates(context.Background(), ListDueGatesRequest{
					Shard: 0, DueAtOrBefore: catalogDeadline, Cursor: "not-a-cursor", Limit: 10})
				return err
			},
			assert: func(t *testing.T, err error) {
				t.Helper()
				assertCatalogField(t, err, CatalogErrorInvalid, "due_at_or_before")
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
	unquote := func(filename string, expr ast.Expr) (string, bool) {
		literal, ok := expr.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return "", false
		}
		text, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatalf("%s: unquote domain: %v", filename, err)
		}
		return text, true
	}

	// A domain reaches digestFrame two ways, and BOTH are scanned. Most are
	// written at the call site; the two sweep cursor kinds carry theirs as a
	// field, because their codecs are one hoisted implementation parameterized
	// by kind. Exempting the second spelling would have quietly removed the two
	// newest domains from the very guard that separates them — which is the
	// failure mode this test exists for — so the grammar is EXTENDED to cover
	// it, exactly as TestOrderedNamespacesAreDistinct was extended for sharded
	// namespaces.
	//
	// It reads the field FAIL-CLOSED, through the shared reader below: a
	// sweepCursorKind literal whose domain this scan cannot read is an error
	// rather than a literal it skips. Reading only KEYED elements, which is
	// what this walk did, let a positional literal past unexamined.
	//
	// The reconciliation first, because everything after it rests on being able
	// to ATTRIBUTE a literal to this type at all.
	assertSweepCursorKindRolesAreUnderstood(t, files)
	domainField := sweepCursorFieldIndex(t, files, "domain")
	carried := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		ast.Inspect(parseProductionFile(t, name), func(node ast.Node) bool {
			composite, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			for _, literal := range sweepCursorLiteralsIn(composite) {
				expr, present := sweepCursorLiteralField(t, name, literal, "domain", domainField)
				if !present {
					// A zero-valued domain collides with nothing; see the
					// reader for why the skip is safe.
					continue
				}
				text, ok := unquote(name, expr)
				if !ok {
					t.Fatalf("%s declares a sweepCursorKind whose domain is not a string literal", name)
				}
				domains[text] = append(domains[text], name)
				carried++
			}
			return true
		})
	}
	if carried < 2 {
		t.Fatalf("found %d carried cursor domains; the sweepCursorKind scan is not reaching them", carried)
	}

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
			// A domain read from a sweepCursorKind's field was collected above.
			// Only that ONE spelling is accepted here; any other computed
			// domain still fails, because a domain this scan cannot see is a
			// separation it cannot check.
			if selector, ok := call.Args[0].(*ast.SelectorExpr); ok && selector.Sel.Name == "domain" {
				return true
			}
			text, ok := unquote(name, call.Args[0])
			if !ok {
				t.Fatalf("%s calls digestFrame with a domain that is neither a string literal nor a cursor kind's field", name)
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
	oversized := storage.DueCursor(strings.Repeat("p", maxSweepCursorPayload))

	// ISSUE. A continuation this store could not accept back is a sweep that
	// silently reverts to making no progress at the head of the view, so the
	// ceiling is enforced where the token is built and not only where it is
	// read.
	if _, err := dueCommandCursor.encode(store, 1, 0, oversized); err == nil {
		t.Fatal("an oversized due-commands continuation was issued")
	} else {
		assertInboxCode(t, err, InboxErrorBackend)
	}
	if _, err := dueGateCursor.encode(store, 1, 0, oversized); err == nil {
		t.Fatal("an oversized due-gates continuation was issued")
	} else {
		assertCatalogCode(t, err, CatalogErrorBackend)
	}

	// PRESENTATION. A continuation this sweep issues always carries at least
	// one provider byte beyond the bound, because an exhausted view returns no
	// cursor at all. A token carrying only a bound is therefore one this store
	// never issued, and accepting it would resume a sweep at the head of the
	// view while the caller believed it was making progress.
	boundOnly, err := dueCommandCursor.encode(store, 1, 0, "")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, _, err := dueCommandCursor.decode(store, 1, boundOnly); err == nil {
		t.Fatal("a due-commands continuation with no provider position was accepted")
	} else {
		assertInboxCode(t, err, InboxErrorCursor)
	}
	gateBoundOnly, err := dueGateCursor.encode(store, 1, 0, "")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, _, err := dueGateCursor.decode(store, 1, gateBoundOnly); err == nil {
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

// --- the open-retry window (F1) -------------------------------------------

// updatingCatalog reports whether the Update the recorder is about to perform
// is the catalog record's. The recorder logs a call BEFORE it runs the hook, so
// the most recent update is the one in flight; the alternative is widening a
// fake that eight other tests share.
func updatingCatalog(ordered *recordingOrdered) bool {
	call, ok := ordered.lastOf("update")
	return ok && call.id.Namespace == catalogNamespace
}

// interruptOpenAfterItsIntent runs one OpenGate that commits its deadline
// intent and then loses its projection write, which is the exact state a crash
// between the two leaves. The interruption is produced by a COMPETING catalog
// write landing between them, so the open's compare-and-swap loses: no fake
// error is injected and every byte in the store was written by a real
// operation.
func interruptOpenAfterItsIntent(
	t *testing.T,
	store *Store,
	ordered *recordingOrdered,
	gate sessionwire.GateProjection,
	tip uint64,
) {
	t.Helper()
	// A plain flag, not sync.Once: the competing write below is itself an
	// Update, so it re-enters this hook, and sync.Once DEADLOCKS on re-entry
	// rather than skipping. Everything here runs on one goroutine.
	competed := false
	ordered.beforeUpdate = func() {
		if !competed && updatingCatalog(ordered) {
			competed = true
			if _, err := store.UpdateCatalogHostState(context.Background(), UpdateCatalogHostStateRequest{
				TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1,
				State: sessionwire.SessionStateRunning, Residency: sessionwire.SessionResidencyResident,
				LastActiveAt: catalogActiveAt, LastJournalSeq: tip, LastEventID: "event-tip",
			}); err != nil {
				t.Errorf("competing write: %v", err)
			}
		}
	}
	_, err := store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: gate,
	})
	ordered.beforeUpdate = nil
	if err == nil {
		t.Fatal("the fixture did not interrupt the open; it has nothing to prove")
	}
	if page := mustReadGates(t, store); len(page.Gates) != 0 {
		t.Fatalf("the interrupted open still projected a gate: %v", gateIDs(page))
	}
	if stored := storedGateIntent(t, store, gate.GateID); stored.Deleted {
		t.Fatal("the interrupted open left no durable intent; it has nothing to prove")
	}
}

// TestAnOpenGateRetryDoesNotInheritTheFirstAttemptsRemnantWindow is the hazard
// MinGateIntentRemnantAge exists to close, in the form that needs neither clock
// skew nor a stalled process.
//
// The window is measured from RecordedAt, and RecordedAt used to be stamped
// exactly once, at the first attempt. So the ORDINARY restart path — a Host
// that crashes between OpenGate's two writes and retries after its supervisor
// brings it back — began its retry with the window ALREADY OPEN. A sweep
// landing inside the retry's own intent-to-projection gap could then retire a
// deadline whose gate was about to become publicly open, which is precisely the
// state this file's header promises no path produces, under an identity that
// can never be reused.
//
// The retry's own age is what has to govern, because the retry IS a new
// in-flight open. Nothing about the first attempt's age says anything about how
// long this one will take.
func TestAnOpenGateRetryDoesNotInheritTheFirstAttemptsRemnantWindow(t *testing.T) {
	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	clock := newMovableClock(catalogActiveAt)
	store := openStore(t, base, WithControlShards(1), WithClock(clock))
	mustPrepareSession(t, store, catalogTenant, catalogSession, 10)

	gate := gateWithDeadline(testGate("gate-a", 5), catalogDeadline)
	interruptOpenAfterItsIntent(t, store, ordered, gate, 10)

	// The Host restarts and retries well after the window would have elapsed.
	// Ten minutes is not an unusual restart; it is an ordinary one.
	clock.set(catalogActiveAt.Add(2 * MinGateIntentRemnantAge))

	// A sweep lands inside the retry's own gap: after its intent write, before
	// its projection write. This is the interleaving that is NOT protected by
	// commitGateIntent failing closed on a tombstone — that one is the sweep
	// landing FIRST, which makes the retry refuse.
	// The hook fires on the CATALOG write specifically. That is the whole
	// point of the interleaving: the sweep has to land after the retry's
	// intent write and before its projection write. Firing on the intent write
	// instead would reproduce the OTHER interleaving, the one that was already
	// safe because commitGateIntent fails closed on a tombstone.
	swept := false
	var retire error
	ordered.beforeUpdate = func() {
		if !swept && updatingCatalog(ordered) {
			swept = true
			revision := storedGateIntent(t, store, gate.GateID).Revision
			retire = store.RetireGateDeadlineIntent(
				context.Background(), retireRequest(gate.GateID, revision))
		}
	}
	_, openErr := store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: gate,
	})
	ordered.beforeUpdate = nil

	// The INVARIANT is asserted before the mechanism, so a failure reports the
	// state the system reached rather than the check that noticed.
	if openErr != nil {
		t.Fatalf("the retry failed: %v", openErr)
	}
	open := mustReadGates(t, store)
	tombstoned := storedGateIntent(t, store, gate.GateID).Deleted
	if len(open.Gates) != 0 && tombstoned {
		t.Fatalf("gate publicly open = %v with its deadline intent tombstoned (retire returned %v)",
			gateIDs(open), retire)
	}
	if len(open.Gates) != 1 || tombstoned {
		t.Fatalf("retry left open=%v tombstoned=%v, want the gate open with a live intent",
			gateIDs(open), tombstoned)
	}
	// And the mechanism: the refusal is the retry's OWN age, reported as the
	// code a sweeper acts on — retry later, rather than re-read.
	assertCatalogField(t, retire, CatalogErrorTooSoon, "recorded_at")
}

// TestAnOpenGateRetryFailsClosedOnARetirementThatLandedFirst is the other
// interleaving of the same race, and it is safe for a different reason.
//
// When the sweep retires before the retry's intent write, the retry meets a
// tombstone: Create returns the tombstoned record, commitGateIntent refuses,
// and the projection is never written. So the two orderings are covered by two
// different mechanisms — the re-stamp handles a retirement that arrives after,
// the tombstone handles one that arrives before — and neither can produce a
// publicly open gate with no live deadline.
func TestAnOpenGateRetryFailsClosedOnARetirementThatLandedFirst(t *testing.T) {
	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	clock := newMovableClock(catalogActiveAt)
	store := openStore(t, base, WithControlShards(1), WithClock(clock))
	mustPrepareSession(t, store, catalogTenant, catalogSession, 10)

	gate := gateWithDeadline(testGate("gate-a", 5), catalogDeadline)
	interruptOpenAfterItsIntent(t, store, ordered, gate, 10)
	clock.set(catalogActiveAt.Add(2 * MinGateIntentRemnantAge))

	// The sweep gets there first, on the first attempt's stale stamp, which is
	// legitimate: nothing has re-stamped, so this really is an abandoned row.
	revision := storedGateIntent(t, store, gate.GateID).Revision
	if err := store.RetireGateDeadlineIntent(
		context.Background(), retireRequest(gate.GateID, revision)); err != nil {
		t.Fatalf("RetireGateDeadlineIntent: %v", err)
	}

	_, err := store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: gate,
	})
	assertCatalogField(t, err, CatalogErrorDeleted, "gate_intent")
	if page := mustReadGates(t, store); len(page.Gates) != 0 {
		t.Fatalf("a refused open still projected a gate: %v", gateIDs(page))
	}
}

// --- the cost claim, measured (F2) ----------------------------------------

// sweepCost is what one full round-robin sweep of every shard cost at the
// provider, broken down by the only distinction that matters here: the due
// queries the sweep is allowed to make, and everything else, which it is not.
type sweepCost struct {
	listDue  int
	other    int
	reported int
	examined int
}

// sweepEveryShard runs the sweep a Factory replica runs — every shard, each
// paged to exhaustion — and measures it at the provider.
func sweepEveryShard(t *testing.T, store *Store, ordered *recordingOrdered, limit int) sweepCost {
	t.Helper()
	ordered.reset()
	var cost sweepCost
	for shard := range store.ControlShards() {
		req := ListDueCommandsRequest{Shard: shard, DueAtOrBefore: inboxDeadline, Limit: limit}
		for pages := 0; ; pages++ {
			if pages > 64 {
				t.Fatalf("shard %d did not exhaust", shard)
			}
			page, err := store.ListDueCommands(context.Background(), req)
			if err != nil {
				t.Fatalf("ListDueCommands(shard %d): %v", shard, err)
			}
			cost.reported += len(page.Commands)
			cost.examined += page.Examined
			if page.NextCursor == "" {
				break
			}
			req = ListDueCommandsRequest{Shard: shard, Cursor: page.NextCursor, Limit: limit}
		}
	}
	for _, call := range ordered.snapshot() {
		if call.op == "list_due" {
			cost.listDue++
			continue
		}
		cost.other++
	}
	return cost
}

// seedShardFixture builds a deployment: `tenants` tenants, each with one
// session carrying `terminal` commands driven to a terminal state, plus
// `due` tenants whose session also carries one command still outstanding.
//
// The terminal commands are the history the cost claim is about. They are
// driven through the real transitions rather than written by hand, because
// what makes them invisible to a sweep is inboxDue filing a terminal record
// NOT DUE — and a hand-written row would be asserting that rule rather than
// exercising it.
func seedShardFixture(t *testing.T, store *Store, tenants, terminal, due int) int {
	t.Helper()
	records := 0
	for tenant := range tenants {
		tenantID := sessionwire.TenantID(fmt.Sprintf("tenant-%02d", tenant))
		session := sessionwire.SessionID("session-a")
		for i := range terminal {
			req := testAdmitRequest()
			req.TenantID, req.SessionID = tenantID, session
			req.CommandID = sessionwire.CommandID(fmt.Sprintf("history-%d", i))
			entry, _, err := store.AdmitCommand(context.Background(), req)
			if err != nil {
				t.Fatalf("AdmitCommand: %v", err)
			}
			records++
			applying := mustBeginApplying(t, store, mustClaim(t, store, entry, 1), 1)
			if _, err := store.CompleteCommand(
				context.Background(), testCompleteRequest(applying, 1)); err != nil {
				t.Fatalf("CompleteCommand: %v", err)
			}
		}
		if tenant < due {
			req := testAdmitRequest()
			req.TenantID, req.SessionID = tenantID, session
			req.CommandID = "outstanding"
			if _, _, err := store.AdmitCommand(context.Background(), req); err != nil {
				t.Fatalf("AdmitCommand: %v", err)
			}
			records++
		}
	}
	return records
}

// TestListDueCommandsCostFollowsShardsAndDueRowsOnly is step 4's cost claim
// MEASURED rather than asserted.
//
// The claim is that one full sweep costs the fixed shard count plus the current
// due records, and nothing else — not the tenant count, not the terminal
// commands, not the history. A test that seeds two commands and reads two back
// demonstrates none of that: a reader that performed a gratuitous Get per row,
// or one whose cost grew with the deployment, would pass it. So this one builds
// two deployments that differ by an order of magnitude in tenants and history
// while carrying the SAME due work, measures both at the provider, and requires
// the two numbers to be equal.
func TestListDueCommandsCostFollowsShardsAndDueRowsOnly(t *testing.T) {
	t.Parallel()

	const shards, outstanding = 4, 3
	build := func(t *testing.T, tenants, terminal int) (*Store, *recordingOrdered, int) {
		t.Helper()
		base := memstore.New()
		ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
		base.OrderedIndex = ordered
		store := openStore(t, base, WithControlShards(shards), WithClock(newMovableClock(inboxAcceptedAt)))
		return store, ordered, seedShardFixture(t, store, tenants, terminal, outstanding)
	}

	smallStore, smallOrdered, smallRecords := build(t, 3, 1)
	largeStore, largeOrdered, largeRecords := build(t, 30, 5)

	// The DEPLOYMENTS have to differ, or "the cost did not move" is a statement
	// about two identical fixtures. This is the premise the whole comparison
	// rests on and it is the one a refactor of the seeder could quietly break.
	if largeRecords <= smallRecords {
		t.Fatalf("the large fixture holds %d records and the small one %d; they must differ",
			largeRecords, smallRecords)
	}

	small := sweepEveryShard(t, smallStore, smallOrdered, 50)
	large := sweepEveryShard(t, largeStore, largeOrdered, 50)

	// ANTI-VACUITY FIRST: two sweeps that both found nothing would also cost
	// the same. The due work has to have been reported, and it has to be all of
	// it.
	if small.reported != outstanding || large.reported != outstanding {
		t.Fatalf("reported %d and %d due commands, want %d in both", small.reported, large.reported, outstanding)
	}
	// And the terminal rows must not even have been LOOKED at. Examined counts
	// the rows the provider returned, so equality with the due work is the
	// statement that a terminal command never entered a page — which is the
	// half of the claim that lives in inboxDue rather than in this reader.
	if small.examined != outstanding || large.examined != outstanding {
		t.Fatalf("examined %d and %d rows, want only the %d due ones", small.examined, large.examined, outstanding)
	}
	if largeStore.ControlShards() != shards {
		t.Fatalf("ControlShards() = %d, want %d", largeStore.ControlShards(), shards)
	}

	// THE COST. One due query per shard, because every shard exhausts in one
	// page at this limit, and NOTHING ELSE — no per-row Get, no catalog read,
	// no walk of a session's inbox.
	if small.listDue != shards || large.listDue != shards {
		t.Errorf("due queries = %d and %d, want one per shard (%d)", small.listDue, large.listDue, shards)
	}
	if small.other != 0 || large.other != 0 {
		t.Errorf("the sweep made %d and %d provider calls that are not due queries; it must make none",
			small.other, large.other)
	}
	// The deployment grew tenfold in tenants and fivefold in history between
	// the two, and the measurement did not move.
	if small != large {
		t.Fatalf("cost moved with the deployment: %+v -> %+v", small, large)
	}
}

// TestListDueCommandsCostGrowsOnlyWithDueRows is the other half: the cost that
// IS allowed to move is the one proportional to current due work, and it moves
// by pages rather than by rows.
func TestListDueCommandsCostGrowsOnlyWithDueRows(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	store := openStore(t, base, WithControlShards(1), WithClock(newMovableClock(inboxAcceptedAt)))
	if records := seedShardFixture(t, store, 9, 2, 9); records != 27 {
		t.Fatalf("the fixture holds %d records, want 27 (9 sessions of 2 terminal plus 1 due)", records)
	}

	// One shard, nine due rows, three to a page: three pages, and the third is
	// the one that reports the view exhausted.
	cost := sweepEveryShard(t, store, ordered, 3)
	if cost.reported != 9 {
		t.Fatalf("reported %d due commands, want 9", cost.reported)
	}
	if cost.listDue != 3 {
		t.Errorf("due queries = %d, want ceil(9/3) = 3", cost.listDue)
	}
	if cost.other != 0 {
		t.Errorf("the sweep made %d provider calls that are not due queries", cost.other)
	}
}

// --- gates-side twins of rules the commands side already covered ----------

// TestADueGatesCursorCannotBeReplayedIntoAnotherShard is the gates twin of
// TestADueCommandsCursorCannotBeReplayedIntoAnotherShard, and the asymmetry it
// closes was structural: the rules written in the new file were covered on both
// sides, the ones retrofitted into gates.go on one.
//
// The FIELD is the assertion, not merely the code. The provider is a backstop
// here — storage's DueCursor contract binds a token to the exact namespace and
// bound that issued it, and a shard IS a namespace, so a mis-sharded token
// would be refused by ListDue too and surface as a cursor failure labelled
// "due_gates". This store refuses it BEFORE the provider sees it, labelled
// "cursor", and that difference is what distinguishes a binding that works from
// one that has been removed.
func TestADueGatesCursorCannotBeReplayedIntoAnotherShard(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New(), WithControlShards(4))
	for i := range 8 {
		session := sessionwire.SessionID(fmt.Sprintf("session-%d", i))
		mustPrepareSession(t, store, catalogTenant, session, 100)
		mustOpenGateOn(t, store, catalogTenant, session,
			gateWithDeadline(testGate("gate-a", 5), catalogDeadline.Add(-time.Hour)))
	}

	var issued sessionwire.Cursor
	issuedShard := -1
	for shard := range 4 {
		page, err := store.ListDueGates(context.Background(), ListDueGatesRequest{
			Shard: shard, DueAtOrBefore: catalogDeadline, Limit: 1,
		})
		if err != nil {
			t.Fatalf("ListDueGates: %v", err)
		}
		if page.NextCursor != "" {
			issued, issuedShard = page.NextCursor, shard
			break
		}
	}
	if issuedShard < 0 {
		t.Fatal("no shard issued a continuation; the fixture proves nothing")
	}
	for shard := range 4 {
		if shard == issuedShard {
			continue
		}
		_, err := store.ListDueGates(context.Background(), ListDueGatesRequest{Shard: shard, Cursor: issued, Limit: 1})
		assertCatalogField(t, err, CatalogErrorCursor, "cursor")
	}
	if _, err := store.ListDueGates(context.Background(), ListDueGatesRequest{
		Shard: issuedShard, Cursor: issued, Limit: 1,
	}); err != nil {
		t.Fatalf("the shard that issued the cursor refused it: %v", err)
	}
}

// TestOpenGateStampsTheIntentFromTheStoresOwnClock pins the single input the
// whole MinGateIntentRemnantAge argument rests on.
//
// If the stamp came from anywhere else — the caller, the gate's deadline, a
// constant — every in-flight open would be retireable the moment its intent
// landed, and the window would be a comment rather than a mechanism. The
// assertion is EQUALITY with the store's clock reading, not "recent" or
// "nonzero": an epoch stamp is nonzero.
func TestOpenGateStampsTheIntentFromTheStoresOwnClock(t *testing.T) {
	t.Parallel()

	clock := newMovableClock(catalogActiveAt)
	store := openStore(t, memstore.New(), WithControlShards(1), WithClock(clock))
	mustPrepareSession(t, store, catalogTenant, catalogSession, 10)

	opened := catalogActiveAt.Add(17 * time.Minute)
	clock.set(opened)
	gate := gateWithDeadline(testGate("gate-a", 5), catalogDeadline)
	mustOpenGate(t, store, 1, gate)

	stored := storedGateIntent(t, store, gate.GateID)
	intent, err := gateIntentFor(stored)
	if err != nil {
		t.Fatalf("gateIntentFor: %v", err)
	}
	if !intent.RecordedAt.Equal(opened) {
		t.Fatalf("recorded instant = %v, want the store's clock reading %v", intent.RecordedAt, opened)
	}
	// And the deadline is a DIFFERENT instant, so a stamp that had been taken
	// from the gate would fail this rather than coincide with it.
	if intent.RecordedAt.Equal(gate.Deadline) {
		t.Fatal("the fixture's clock reading equals the deadline; it cannot tell the two sources apart")
	}
}

// TestRetireGateDeadlineIntentRetiresIntentsOfATombstonedSession reaches
// noSuchSession's third leg.
//
// The other retire test covers an absent record and an unbound witness by never
// preparing the session at all, which is one state reached two ways rather than
// two states. A session that EXISTED and was then tombstoned is the leg neither
// of them touches, and it is the one that decides whether a reaper that removed
// a session leaves its deadline intents un-retireable forever.
func TestRetireGateDeadlineIntentRetiresIntentsOfATombstonedSession(t *testing.T) {
	t.Parallel()

	store, clock := retireFixture(t)
	revision := seedRemnantOfACrashBeforeGateOpened(t, store, "gate-orphan")

	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	record, err := store.backend.OrderedIndex.Get(context.Background(), catalogID(scope, catalogSession))
	if err != nil {
		t.Fatalf("Get(catalog): %v", err)
	}
	if _, err := store.backend.OrderedIndex.Delete(
		context.Background(), catalogID(scope, catalogSession), record.Revision); err != nil {
		t.Fatalf("Delete(catalog): %v", err)
	}
	clock.set(catalogActiveAt.Add(MinGateIntentRemnantAge))

	if err := store.RetireGateDeadlineIntent(
		context.Background(), retireRequest("gate-orphan", revision)); err != nil {
		t.Fatalf("RetireGateDeadlineIntent: %v", err)
	}
	if stored := storedGateIntent(t, store, "gate-orphan"); !stored.Deleted {
		t.Fatal("a tombstoned session's remnant was not retired")
	}
}

// TestAVersionOneLayoutMarkerIsRefused pins the operationally relevant codec
// break, which the strict-codec table does not: that one mutates the version
// byte upward, to a version nothing ever wrote.
//
// A version-1 marker is the one a previous release of this package really
// produced, and its bytes are not merely unknown — they are ACTIVELY
// MISREADABLE as version 2. The shard count occupies the two bytes a v1 marker
// used for the tenant length, so a reader that accepted one would take its
// shard count from a length field and file every outstanding record in shards
// nothing sweeps. Failing closed is the migration.
//
// THE REFUSAL IS OVER-DETERMINED, and that is recorded rather than implied,
// because it changes what this test proves. Widening the version check to admit
// version 1 does NOT make these markers readable: a v1 tenant marker is nine
// bytes and fails the header length, and a v1 legacy marker's tenant-length
// field has to double as both a valid shard count and the new length field,
// which no ordinary tenant name satisfies. So this test pins the OUTCOME — a
// marker a previous release wrote is refused — and the version byte's own
// barrier is pinned separately by TestLayoutMarkerStrictCodec's version case.
func TestAVersionOneLayoutMarkerIsRefused(t *testing.T) {
	t.Parallel()

	// The exact bytes the version-1 encoder produced, for both layouts.
	v1 := func(layout keyspaceLayout, tenant string) []byte {
		out := []byte{'L', 'R', 'K', 'S', 1, byte(layout), keyAlgorithmVersion,
			byte(len(tenant) >> 8), byte(len(tenant))}
		return append(out, tenant...)
	}
	for name, marker := range map[string][]byte{
		"tenant layout": v1(layoutTenantV1, ""),
		"legacy layout": v1(layoutLegacySingleTenantV1, "tenant"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			backend := memstore.New()
			if _, err := backend.KV.Put(context.Background(), layoutMarkerKey, 0, marker); err != nil {
				t.Fatalf("Put(marker): %v", err)
			}
			_, err := Open(context.Background(), backend, WithLegacySingleTenant("tenant"))
			assertKeyspaceCode(t, err, KeyspaceMarkerMalformed)
		})
	}
}

// TestAForgedDueCommandsCursorIsACursorFailureNotABackendOne pins the arm
// classifyInboxOrderedError gained for this path.
//
// It is reachable because the envelope's scope is a TAG and not a MAC: anyone
// who knows a shard number can build a well-formed envelope for it and put
// arbitrary bytes where the provider's own position belongs. The envelope
// accepts that token and the PROVIDER refuses it, and the refusal has to reach
// the caller as what it is. Reporting it as a backend failure would tell a
// sweeper its store was down when its token was forged — and a sweeper's
// response to those two is opposite: stop and page an operator, or drop the
// cursor and restart the shard.
func TestAForgedDueCommandsCursorIsACursorFailureNotABackendOne(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New(), WithControlShards(2), WithClock(newMovableClock(inboxAcceptedAt)))
	admitInto(t, store, "session-1", "command-1")

	// Built by this store's own encoder, so no cursor literal is constructed
	// and the envelope is genuinely well formed; only the provider's own
	// position inside it is not one the provider ever issued.
	forged, err := dueCommandCursor.encode(store, 0, inboxDeadline.UnixMilli(), "not-a-provider-position")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	_, err = store.ListDueCommands(context.Background(), ListDueCommandsRequest{
		Shard: 0, Cursor: forged, Limit: 10,
	})
	if got := assertInboxCode(t, err, InboxErrorCursor); got.Field != "due_commands" {
		t.Fatalf("field = %q, want due_commands: the refusal came from the provider, not the envelope", got.Field)
	}
}

// TestAnOpenGateRetryReportsALostRestampRatherThanAbsorbingIt pins the claim
// commitGateIntent makes about its compare-and-swap, which nothing was driving.
//
// Two replicas retrying the same interrupted open both take the re-stamp path.
// One wins; the other's expected revision is stale. Absorbing that would mean
// concluding the row is freshly stamped from a revision this caller never read
// — true today, because the only other writers of the row DELETE, but true by
// an argument about the rest of the file rather than by anything this function
// checks. Reporting it costs a retry on a path that is already the rare one.
func TestAnOpenGateRetryReportsALostRestampRatherThanAbsorbingIt(t *testing.T) {
	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	clock := newMovableClock(catalogActiveAt)
	store := openStore(t, base, WithControlShards(1), WithClock(clock))
	mustPrepareSession(t, store, catalogTenant, catalogSession, 10)

	gate := gateWithDeadline(testGate("gate-a", 5), catalogDeadline)
	interruptOpenAfterItsIntent(t, store, ordered, gate, 10)
	clock.set(catalogActiveAt.Add(time.Minute))

	// The competing replica lands its whole retry — re-stamp and projection —
	// between this one's read of the intent row and its own re-stamp.
	raced := false
	ordered.beforeUpdate = func() {
		if raced || updatingCatalog(ordered) {
			return
		}
		raced = true
		clock.set(catalogActiveAt.Add(2 * time.Minute))
		if _, err := store.OpenGate(context.Background(), OpenGateRequest{
			TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: gate,
		}); err != nil {
			t.Errorf("the competing retry failed: %v", err)
		}
	}
	_, err := store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: gate,
	})
	ordered.beforeUpdate = nil
	if !raced {
		t.Fatal("the fixture never reached the re-stamp; it has nothing to prove")
	}

	assertCatalogField(t, err, CatalogErrorConflict, "gate_intent")
	// The winner's work stands: the gate is open with a live, freshly stamped
	// intent, so the loser's retry is a retry and not a repair.
	if page := mustReadGates(t, store); len(page.Gates) != 1 {
		t.Fatalf("the winning retry did not leave the gate open: %v", gateIDs(page))
	}
	stored := storedGateIntent(t, store, gate.GateID)
	if stored.Deleted {
		t.Fatal("the race tombstoned the intent")
	}
	intent, err := gateIntentFor(stored)
	if err != nil {
		t.Fatalf("gateIntentFor: %v", err)
	}
	if !intent.RecordedAt.Equal(catalogActiveAt.Add(2 * time.Minute)) {
		t.Fatalf("recorded instant = %v, want the winner's %v",
			intent.RecordedAt, catalogActiveAt.Add(2*time.Minute))
	}
}

// --- the recorded instant is a maximum over attempts (C1) -----------------

// storedGateIntentRecord decodes one gate's stored intent.
func storedGateIntentRecord(t *testing.T, store *Store, gate sessionwire.GateID) gateIntent {
	t.Helper()
	intent, err := gateIntentFor(storedGateIntent(t, store, gate))
	if err != nil {
		t.Fatalf("gateIntentFor: %v", err)
	}
	return intent
}

// TestTheIntentsRecordedInstantNeverMovesBackward states the rule directly,
// rather than through the harm it prevents.
//
// THE WINDOW IS A MAXIMUM OVER ATTEMPTS, not a last-writer-wins value. Every
// attempt at an open restarts the window, so the stored instant must be the
// LATEST attempt's — and an attempt that cannot advance it has nothing to add.
// The stamp is read at the top of OpenGate, before the session-scope
// verification and the catalog read, so attempts commit in an order that has
// nothing to do with the order they read the clock in: a slower attempt with an
// older reading routinely arrives last. Letting it win would make the window
// describe an attempt that has already finished, which is the exact property
// the re-stamp exists to establish.
func TestTheIntentsRecordedInstantNeverMovesBackward(t *testing.T) {
	t.Parallel()

	// wantWrite is asserted as well as the stored value, because the two
	// halves of the predicate are different claims and only one of them shows
	// up in the record. An attempt that is not later must not WRITE — a write
	// storing the identical instant would leave every assertion on the value
	// passing while turning every repeat within one clock reading into a
	// provider round trip, which is the cost half of the trade commitGateIntent
	// argues.
	for _, tt := range []struct {
		name      string
		second    time.Duration
		want      time.Duration
		wantWrite bool
	}{
		{"an older reading", -time.Hour, 0, false},
		{"the same reading", 0, 0, false},
		{"one nanosecond older", -time.Nanosecond, 0, false},
		{"one nanosecond newer", time.Nanosecond, time.Nanosecond, true},
		{"a newer reading", time.Hour, time.Hour, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			first := catalogActiveAt.Add(2 * time.Hour)
			base := memstore.New()
			ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
			base.OrderedIndex = ordered
			clock := newMovableClock(first)
			store := openStore(t, base, WithControlShards(1), WithClock(clock))
			mustPrepareSession(t, store, catalogTenant, catalogSession, 10)

			gate := gateWithDeadline(testGate("gate-a", 5), catalogDeadline)
			mustOpenGate(t, store, 1, gate)
			if got := storedGateIntentRecord(t, store, gate.GateID).RecordedAt; !got.Equal(first) {
				t.Fatalf("first stamp = %v, want %v", got, first)
			}

			// A second attempt whose own clock reading is the one under test.
			// It takes the already-open repair branch, which is the same
			// commitGateIntent call a retry takes.
			clock.set(first.Add(tt.second))
			ordered.reset()
			mustOpenGate(t, store, 1, gate)

			want := first.Add(tt.want)
			if got := storedGateIntentRecord(t, store, gate.GateID).RecordedAt; !got.Equal(want) {
				t.Fatalf("stamp after the second attempt = %v, want %v", got, want)
			}
			wrote := 0
			for _, call := range ordered.snapshot() {
				if call.op == "update" && isShardOf(call.id.Namespace, gateNamespace) {
					wrote++
				}
			}
			if (wrote > 0) != tt.wantWrite {
				t.Fatalf("the second attempt made %d intent writes, want wantWrite=%v", wrote, tt.wantWrite)
			}
		})
	}
}

// TestAStaleOpenAttemptCannotReopenTheRemnantWindow composes the backward move
// with the sweeper it endangers, which is the reason the rule above is not a
// tidiness matter.
//
// An attempt reads the clock at the top of OpenGate and then spends a mutex
// acquisition and a catalog read before it writes. Another attempt can create
// or re-stamp the intent in that span, so the slow attempt arrives holding a
// CURRENT revision and a STALE reading — which is why the compare-and-swap is
// no barrier here, and why TestAnOpenGateRetryReportsALostRestampRatherThan
// AbsorbingIt does not reach this: its loser is refused on the revision.
func TestAStaleOpenAttemptCannotReopenTheRemnantWindow(t *testing.T) {
	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	late := catalogActiveAt.Add(2 * time.Hour)
	clock := newMovableClock(late)
	store := openStore(t, base, WithControlShards(1), WithClock(clock))
	mustPrepareSession(t, store, catalogTenant, catalogSession, 10)

	// The intent is durable and freshly stamped; its projection is not.
	gate := gateWithDeadline(testGate("gate-a", 5), catalogDeadline)
	interruptOpenAfterItsIntent(t, store, ordered, gate, 10)
	if got := storedGateIntentRecord(t, store, gate.GateID).RecordedAt; !got.Equal(late) {
		t.Fatalf("stamp = %v, want the late attempt's %v", got, late)
	}

	// A slow attempt that read the clock ten minutes ago and is only now
	// reaching its writes. Its catalog revision is current, so its projection
	// write will succeed.
	stale := late.Add(-2 * MinGateIntentRemnantAge)
	clock.set(stale)

	swept := false
	var retire error
	ordered.beforeUpdate = func() {
		if swept || !updatingCatalog(ordered) {
			return
		}
		swept = true
		// Real time, which is a second past the window measured from the LATE
		// attempt's stamp and ten minutes past one measured from the stale
		// reading. Only the second of those may let a retirement through.
		clock.set(late.Add(time.Second))
		revision := storedGateIntent(t, store, gate.GateID).Revision
		retire = store.RetireGateDeadlineIntent(
			context.Background(), retireRequest(gate.GateID, revision))
	}
	_, openErr := store.OpenGate(context.Background(), OpenGateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: gate,
	})
	ordered.beforeUpdate = nil
	if !swept {
		t.Fatal("the sweeper never ran; the fixture has nothing to prove")
	}

	open := mustReadGates(t, store)
	tombstoned := storedGateIntent(t, store, gate.GateID).Deleted
	if len(open.Gates) != 0 && tombstoned {
		t.Fatalf("open=%v tombstoned=%v openErr=%v retire=%v",
			gateIDs(open), tombstoned, openErr, retire)
	}
	assertCatalogField(t, retire, CatalogErrorTooSoon, "recorded_at")
	if got := storedGateIntentRecord(t, store, gate.GateID).RecordedAt; !got.Equal(late) {
		t.Fatalf("the stale attempt moved the stamp to %v; the late attempt's %v must stand", got, late)
	}
}

// TestListDueGatesReadsEachSessionOncePerPage measures the ONE per-session
// provider read either sweep performs.
//
// The commands sweep's cost claim is nearly structural — it does no per-session
// work at all — so this is the sharper half of the pair. A page is ordered by
// deadline, so one session's gates interleave with every other session's; the
// map is what makes the read once per SESSION rather than once per ROW, and a
// regression to a last-seen cache, or to no cache, is invisible in every result
// the page returns. In a page dominated by one session's gates it is the
// difference between one read and one per row.
func TestListDueGatesReadsEachSessionOncePerPage(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	store := openStore(t, base, WithControlShards(1))

	// Two sessions, their gates INTERLEAVED by deadline, so no session's rows
	// are adjacent and a one-entry last-seen cache would miss on every row.
	const gatesPerSession = 4
	sessions := []sessionwire.SessionID{"session-1", "session-2"}
	for _, session := range sessions {
		mustPrepareSession(t, store, catalogTenant, session, 100)
	}
	for i := range gatesPerSession {
		for at, session := range sessions {
			mustOpenGateOn(t, store, catalogTenant, session, gateWithDeadline(
				testGate(fmt.Sprintf("gate-%d", i), uint64(i+1)),
				catalogDeadline.Add(-time.Hour+time.Duration(i*len(sessions)+at)*time.Minute)))
		}
	}

	ordered.reset()
	page, err := store.ListDueGates(context.Background(), ListDueGatesRequest{
		Shard: 0, DueAtOrBefore: catalogDeadline, Limit: 50,
	})
	if err != nil {
		t.Fatalf("ListDueGates: %v", err)
	}

	// ANTI-VACUITY: the page has to have done the work. A page that read
	// nothing would also read each session at most once.
	rows := len(sessions) * gatesPerSession
	if page.Examined != rows || len(page.Gates) != rows {
		t.Fatalf("page examined %d and reported %d gates, want %d of each", page.Examined, len(page.Gates), rows)
	}
	// And the rows really did interleave, or the cache is not being exercised:
	// a last-seen cache is only wrong when consecutive rows differ.
	changes := 0
	for i := 1; i < len(page.Gates); i++ {
		if page.Gates[i].SessionID != page.Gates[i-1].SessionID {
			changes++
		}
	}
	if changes < rows-1 {
		t.Fatalf("the page's sessions changed %d times over %d rows; the fixture is not interleaved", changes, rows)
	}

	catalogReads := 0
	for _, call := range ordered.snapshot() {
		if call.op == "get" && call.id.Namespace == catalogNamespace {
			catalogReads++
		}
	}
	if catalogReads != len(sessions) {
		t.Fatalf("the page performed %d catalog reads over %d rows, want one per distinct session (%d)",
			catalogReads, rows, len(sessions))
	}
}

// TestListDueGatesDoesNotCacheAnUnreadableSessionAsARemnant pins the one thing
// the per-session cache must NOT remember.
//
// A read that failed row-locally — a corrupt catalog record — is a fact about
// what that read found, not a resolved session. Caching it would give every
// later row of the same session in the page an empty gate list, and an empty
// gate list is indistinguishable from "the projection does not open this gate",
// so those rows would be reported as REMNANTS: retirement candidates, aimed at
// gates that may well be open in a record nobody could decode. Leaving the
// session uncached costs one repeated failing read per row and reports every
// one of them as unreadable, which is what they are.
func TestListDueGatesDoesNotCacheAnUnreadableSessionAsARemnant(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New(), WithControlShards(1))
	mustPrepareSession(t, store, catalogTenant, "session-1", 100)
	const gates = 3
	for i := range gates {
		mustOpenGateOn(t, store, catalogTenant, "session-1", gateWithDeadline(
			testGate(fmt.Sprintf("gate-%d", i), uint64(i+1)),
			catalogDeadline.Add(-time.Hour+time.Duration(i)*time.Minute)))
	}

	// Corrupt the session's catalog record in place, leaving its filing alone,
	// so the failure is the decode rather than the scope check.
	scope, err := store.deriveSessionScope(catalogTenant, "session-1")
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	id := catalogID(scope, "session-1")
	stored, err := store.backend.OrderedIndex.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(catalog): %v", err)
	}
	if _, err := store.backend.OrderedIndex.Update(
		context.Background(), id, stored.Revision, []byte("{not a record"), stored.Rank, stored.Due); err != nil {
		t.Fatalf("Update(catalog): %v", err)
	}

	page, err := store.ListDueGates(context.Background(), ListDueGatesRequest{
		Shard: 0, DueAtOrBefore: catalogDeadline, Limit: 50,
	})
	if err != nil {
		t.Fatalf("one unreadable session ended the page: %v", err)
	}
	if page.Examined != gates {
		t.Fatalf("examined %d rows, want %d", page.Examined, gates)
	}
	if page.Unreadable != gates || len(page.Remnants) != 0 || len(page.Gates) != 0 {
		t.Fatalf("page = unreadable %d, remnants %d, gates %d; want all %d rows unreadable and none retireable",
			page.Unreadable, len(page.Remnants), len(page.Gates), gates)
	}
}

// sweepCursorFieldIndex reports the position the named field occupies in the
// sweepCursorKind declaration, which is where a POSITIONAL literal spells it.
//
// The position is DERIVED from the type rather than assumed, so reordering the
// struct cannot quietly point a positional read at the neighbouring field —
// which would be a guard reading one value and reporting on another, the worst
// of the failures available to it. Not finding the declaration at all is a
// failure for the ordinary anti-vacuity reason: a scan that cannot locate the
// type it is about would otherwise report on nothing.
func sweepCursorFieldIndex(t *testing.T, files []string, field string) int {
	t.Helper()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		index := -1
		ast.Inspect(parseProductionFile(t, name), func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok || spec.Name.Name != "sweepCursorKind" {
				return true
			}
			structure, ok := spec.Type.(*ast.StructType)
			if !ok {
				t.Fatalf("%s declares sweepCursorKind as something other than a struct", name)
			}
			position := 0
			for _, member := range structure.Fields.List {
				if len(member.Names) == 0 {
					// An embedded field still occupies one position.
					position++
					continue
				}
				for _, memberName := range member.Names {
					if memberName.Name == field {
						index = position
					}
					position++
				}
			}
			return false
		})
		if index >= 0 {
			return index
		}
	}
	t.Fatalf("no production file declares sweepCursorKind with a %s field; a positional literal's %s could not be located", field, field)
	return -1
}

// sweepCursorLiteralField returns the expression a sweepCursorKind literal gives
// the named field. The bool reports whether there is a field to read AT ALL;
// false means "nothing to collect, and nothing can escape by it", which is a
// different answer from failure and is only ever returned for the empty literal.
//
// The fail-closed half is the point everywhere else. Both cursor guards read
// only *ast.KeyValueExpr elements once, so a POSITIONAL literal —
// `sweepCursorKind{"LRDG", 1, ...}`, legal Go that vet and staticcheck accept —
// contributed nothing to either set and SURVIVED as a duplicate while still
// reaching encodeCursorEnvelope as k.magic, which the use-guard's selector
// branch waves through. A literal this reader cannot read is therefore an
// error, not a literal it passes over.
//
// THE EMPTY LITERAL IS THE ONE EXEMPTION, and it is a skip rather than a
// failure on evidence, not on convenience. `sweepCursorKind{}` is the ordinary
// zero value an error return is spelled with — `return sweepCursorKind{}, err`
// — and treating it as fatal made this guard reject idiomatic Go with no
// exemption path, which is the failure this file elsewhere calls "wrong in the
// direction that gets guards deleted". Skipping it is SAFE for a reason the
// zero value itself supplies: its magic is the empty string, which collides
// with nothing because every magic collected here is cursorMagicBytes long, and
// a zero-value kind that reached encode would not silently mis-tag a
// continuation but panic on cursor.go's len(magic) != cursorMagicBytes
// invariant. Nothing can hide in a value that cannot be used.
//
// The mixed-elements and too-few-positional arms below are unreachable twice
// over, and both reasons are worth keeping. Go rejects each spelling; and
// because this test lives in package sessionstore, a package that does not
// type-check produces `[build failed]` and the guard does not run at all. They
// are retained as assertions about a walk, not as checks that fire on real
// input — the LIVE positional path is composite.Elts[index], which is reached
// by every well-typed positional literal and is load-bearing.
func sweepCursorLiteralField(
	t *testing.T,
	filename string,
	composite *ast.CompositeLit,
	field string,
	index int,
) (ast.Expr, bool) {
	t.Helper()
	if len(composite.Elts) == 0 {
		return nil, false
	}
	if _, keyed := composite.Elts[0].(*ast.KeyValueExpr); keyed {
		for _, element := range composite.Elts {
			pair, ok := element.(*ast.KeyValueExpr)
			if !ok {
				t.Fatalf("%s mixes keyed and positional elements in a sweepCursorKind literal", filename)
			}
			if key, ok := pair.Key.(*ast.Ident); ok && key.Name == field {
				return pair.Value, true
			}
		}
		// A keyed literal may legally omit a field, and the omitted value is
		// the zero one: an empty magic or domain, which collides with nothing
		// for the same reason the empty literal does.
		return nil, false
	}
	if index >= len(composite.Elts) {
		t.Fatalf("%s declares a positional sweepCursorKind literal with %d elements, too few to carry %s at position %d",
			filename, len(composite.Elts), field, index)
	}
	return composite.Elts[index], true
}

// sweepCursorLiteralsIn returns the sweepCursorKind composite literals a node IS
// or DIRECTLY CONTAINS, and it exists because "is one" was too narrow twice.
//
// A literal that names its type is the ordinary spelling. An ELEMENT literal
// ELIDES it — `[]sweepCursorKind{{magic: "LRDG", ...}}`, and the array and map
// forms alongside it, are legal Go whose CompositeLit.Type is nil — so matching
// only on `composite.Type.(*ast.Ident)` skipped it SILENTLY rather than failing
// closed, and a duplicate magic and a duplicate domain in that form both passed
// their guards. The container's element type is what names it, so that is what
// is read.
//
// BOTH map halves are read. A map VALUE is the spelling one reaches for, but
// `map[sweepCursorKind]int{{...}: 1}` elides the type in the KEY, and the only
// thing stopping that from compiling today is that this struct carries func
// fields and so is not comparable — which shards.go's own commentary
// contemplates changing, since that error vocabulary could be codes instead.
// Reading one half would let that refactor silently reopen the gap.
//
// An element that DOES name its own type is deliberately not collected here:
// ast.Inspect reaches it in its own right, and collecting it twice would report
// a literal as colliding with itself.
//
// WHAT IT CANNOT DECODE — a container behind a named type or an alias, a nested
// container, a struct field's elided literal — is NOT left to be found later by
// a duplicate slipping through. assertSweepCursorKindRolesAreUnderstood fails on
// any occurrence of the type name in a role this function does not decode, so a
// spelling that would escape stops the guard with instructions instead.
func sweepCursorLiteralsIn(composite *ast.CompositeLit) []*ast.CompositeLit {
	if named, ok := composite.Type.(*ast.Ident); ok {
		if named.Name != "sweepCursorKind" {
			return nil
		}
		return []*ast.CompositeLit{composite}
	}
	isKind := func(expr ast.Expr) bool {
		named, ok := expr.(*ast.Ident)
		return ok && named.Name == "sweepCursorKind"
	}
	var keyed, valued bool
	switch container := composite.Type.(type) {
	case *ast.ArrayType: // slices and arrays alike
		valued = isKind(container.Elt)
	case *ast.MapType:
		keyed, valued = isKind(container.Key), isKind(container.Value)
	default:
		return nil
	}
	if !keyed && !valued {
		return nil
	}
	// Only the ELIDED spelling is collected: one that names its type is visited
	// in its own right by the walk that called this.
	elided := func(expr ast.Expr) *ast.CompositeLit {
		inner, ok := expr.(*ast.CompositeLit)
		if !ok || inner.Type != nil {
			return nil
		}
		return inner
	}
	var found []*ast.CompositeLit
	for _, element := range composite.Elts {
		pair, isPair := element.(*ast.KeyValueExpr)
		if !isPair {
			if inner := elided(element); valued && inner != nil {
				found = append(found, inner)
			}
			continue
		}
		if inner := elided(pair.Key); keyed && inner != nil {
			found = append(found, inner)
		}
		if inner := elided(pair.Value); valued && inner != nil {
			found = append(found, inner)
		}
	}
	return found
}

// assertSweepCursorKindRolesAreUnderstood is the guard on the guards: it fails
// when sweepCursorKind is NAMED anywhere the literal readers above cannot
// decode a literal from.
//
// WHY A RULE RATHER THAN A LIST OF GAPS. The header of TestCursorMagicsAreDistinct
// twice enumerated the spellings that escaped it, and both enumerations were
// incomplete the day they were written — a named container type, an alias, a
// pointer element, a nested container, a map of containers and a struct field's
// elided literal all defeat syntactic attribution, and a duplicate magic in any
// of them passed both guards. Enumerating them is a list that has to be
// maintained by whoever is least likely to think of the next one.
//
// The rule needs no maintenance because sweepCursorKind is UNEXPORTED: every
// container that can hold one is spelled inside this package, so every way a
// literal can be introduced passes through an occurrence of the identifier in a
// production file. Reconciling those occurrences against the roles the readers
// understand turns "we listed the holes we thought of" into "a hole cannot be
// opened without this failing and saying so".
//
// AN ALTERNATIVE WAS DRIVEN AND REJECTED: keying on a `magic:` field in any
// composite literal. It closes only the keyed half — a positional element in a
// named container carries a duplicate with no `magic:` key anywhere and passes —
// and `magic` is already domain vocabulary here (cursor.go's magic parameter,
// envelope.go's "magic" field, journal_reader.go's magic[4]), so hoisting the
// envelope header into a struct would have tripped it on innocent code.
//
// The accounted roles are the ones that CANNOT introduce a composite literal
// this scan would then have to attribute: the declaration itself, a function
// signature at any depth, a declared variable's type at any depth, and the
// composite-literal type positions sweepCursorLiteralsIn actually decodes.
// Nesting matters — `[][]sweepCursorKind{{{...}}}` names the type inside a
// composite-literal type that is NOT one of those positions, and that is
// precisely a case the readers cannot attribute, so it is unaccounted by
// construction rather than by an added check.
func assertSweepCursorKindRolesAreUnderstood(t *testing.T, files []string) {
	t.Helper()

	const kind = "sweepCursorKind"
	occurrences := 0
	declared := false
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}

		accounted := map[*ast.Ident]bool{}
		markAll := func(node ast.Node) {
			if node == nil {
				return
			}
			ast.Inspect(node, func(inner ast.Node) bool {
				if named, ok := inner.(*ast.Ident); ok && named.Name == kind {
					accounted[named] = true
				}
				return true
			})
		}
		markOne := func(expr ast.Expr) {
			if named, ok := expr.(*ast.Ident); ok && named.Name == kind {
				accounted[named] = true
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.TypeSpec:
				// The declaration names itself; its right-hand side does not
				// get the same pass, which is what catches a named container.
				if typed.Name.Name == kind {
					accounted[typed.Name] = true
					declared = true
				}
			case *ast.FuncDecl:
				if typed.Recv != nil {
					for _, field := range typed.Recv.List {
						markAll(field.Type)
					}
				}
				markAll(typed.Type)
			case *ast.ValueSpec:
				markAll(typed.Type)
			case *ast.TypeAssertExpr:
				markAll(typed.Type)
			case *ast.StructType:
				// A STRUCT FIELD cannot hide an elided literal, and that is a
				// fact about Go rather than a judgement: elision is permitted
				// only inside an array, slice or map literal, so
				// `zzHolder{k: {magic: "LRDG"}}` does not compile — "missing
				// type in composite literal". A literal reaching a struct field
				// must therefore name its type, which the decoder reads. Not
				// accounting these made the reconciliation reject an ordinary
				// struct that holds a cursor kind.
				for _, field := range typed.Fields.List {
					markAll(field.Type)
				}
			case *ast.FuncType:
				// Interface methods and func-typed fields, for the same reason
				// a FuncDecl's signature is accounted: a signature holds no
				// composite literal.
				markAll(typed)
			case *ast.CaseClause:
				// A type switch's case names types. Only a BARE identifier is
				// accounted, so a composite literal appearing in a value switch
				// still reaches the CompositeLit arm below.
				for _, item := range typed.List {
					markOne(item)
				}
			case *ast.CallExpr:
				// A conversion names the type in Fun; make and new name it in
				// an argument. None of the three can carry a composite literal
				// of this type, and all three are plausible enough that failing
				// on them would be the same false alarm the empty-literal arm
				// was: legal, literal-free code reported as a breach.
				markOne(typed.Fun)
				if callee, ok := typed.Fun.(*ast.Ident); ok && (callee.Name == "make" || callee.Name == "new") {
					for _, argument := range typed.Args {
						markAll(argument)
					}
				}
			case *ast.CompositeLit:
				switch container := typed.Type.(type) {
				case *ast.Ident:
					markOne(container)
				case *ast.ArrayType:
					markOne(container.Elt)
				case *ast.MapType:
					markOne(container.Key)
					markOne(container.Value)
				}
			}
			return true
		})

		ast.Inspect(file, func(node ast.Node) bool {
			named, ok := node.(*ast.Ident)
			if !ok || named.Name != kind {
				return true
			}
			occurrences++
			if accounted[named] {
				return true
			}
			position := fileSet.Position(named.Pos())
			t.Fatalf("%s:%d names %s in a role the cursor guards do not decode, so a composite literal reachable through it would be attributed to nothing and its magic and domain would go uncompared. "+
				"If that role can carry a literal — a named container type, an alias, a nested container, a struct field — extend sweepCursorLiteralsIn to decode it. "+
				"If it provably cannot, add the role to the accounted set here and say why.",
				name, position.Line, kind)
			return true
		})
	}

	// Anti-vacuity, both halves: a walk that found no occurrence, or that never
	// reached the declaration, would report a clean bill on nothing at all.
	if !declared {
		t.Fatalf("no production file declares %s; this reconciliation examined nothing", kind)
	}
	if occurrences == 0 {
		t.Fatalf("found no occurrence of %s; the reconciliation is vacuous", kind)
	}
}

// TestSweepCursorLiteralsInDecodesEachContainerShape drives the reader over
// PARSED source rather than over this package, which is the only way one of its
// arms can be reached at all.
//
// `map[sweepCursorKind]int` does not compile today: the struct carries func
// fields, so it is not comparable, and a probe placed in this package therefore
// reports a build failure rather than a result. That makes the map-KEY arm
// unexercisable in situ — and it is not dead code, because shards.go's own
// commentary contemplates replacing the func-valued error vocabulary with
// codes, which would make the type comparable, the spelling legal, and the arm
// live. Parsing a string needs no type-checking, so the arm is driven now
// instead of on the day that refactor lands.
//
// The negative rows matter as much: a container of another type contributes
// nothing, and an element that NAMES its type is not collected here, because
// the caller's walk reaches it separately and collecting it twice would report
// a literal as colliding with itself.
func TestSweepCursorLiteralsInDecodesEachContainerShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		decl  string
		want  int
		first string
	}{
		{name: "names its own type", decl: `var v = sweepCursorKind{magic: "LRAA"}`, want: 1, first: "LRAA"},
		{name: "slice element elides", decl: `var v = []sweepCursorKind{{magic: "LRBB"}}`, want: 1, first: "LRBB"},
		{name: "array element elides", decl: `var v = [1]sweepCursorKind{{magic: "LRCC"}}`, want: 1, first: "LRCC"},
		{name: "map value elides", decl: `var v = map[string]sweepCursorKind{"k": {magic: "LRDD"}}`, want: 1, first: "LRDD"},
		{name: "map key elides", decl: `var v = map[sweepCursorKind]int{{magic: "LREE"}: 1}`, want: 1, first: "LREE"},
		{name: "both map halves", decl: `var v = map[sweepCursorKind]sweepCursorKind{{magic: "LRFF"}: {magic: "LRGG"}}`, want: 2, first: "LRFF"},
		{name: "indexed slice element elides", decl: `var v = []sweepCursorKind{0: {magic: "LRHH"}}`, want: 1, first: "LRHH"},
		// Counted exactly ONCE, by the element itself rather than by its
		// container: the container declines it precisely so that the walk which
		// reaches it directly is the only one that collects it. Two would
		// report the literal as colliding with itself.
		{name: "element naming its type is counted once", decl: `var v = []sweepCursorKind{sweepCursorKind{magic: "LRII"}}`, want: 1, first: "LRII"},
		{name: "container of another type", decl: `var v = []otherKind{{magic: "LRJJ"}}`, want: 0},
		{name: "pointer element is not decoded", decl: `var v = []*sweepCursorKind{{magic: "LRKK"}}`, want: 0},
		{name: "nested container is not decoded", decl: `var v = [][]sweepCursorKind{{{magic: "LRLL"}}}`, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			file, err := parser.ParseFile(token.NewFileSet(), "probe.go", "package probe\n\n"+tt.decl+"\n", 0)
			if err != nil {
				t.Fatalf("parse %q: %v", tt.decl, err)
			}
			var found []*ast.CompositeLit
			ast.Inspect(file, func(node ast.Node) bool {
				if composite, ok := node.(*ast.CompositeLit); ok {
					found = append(found, sweepCursorLiteralsIn(composite)...)
				}
				return true
			})
			if len(found) != tt.want {
				t.Fatalf("sweepCursorLiteralsIn over %q returned %d literals, want %d", tt.decl, len(found), tt.want)
			}
			if tt.want == 0 {
				return
			}
			// The literal is the one it claims: reading the magic back proves
			// the reader returned the ELEMENT rather than its container.
			expr, present := sweepCursorLiteralField(t, "probe.go", found[0], "magic", 0)
			if !present {
				t.Fatalf("the literal decoded from %q carries no magic", tt.decl)
			}
			literal, ok := expr.(*ast.BasicLit)
			if !ok {
				t.Fatalf("the magic decoded from %q is %T, want a literal", tt.decl, expr)
			}
			text, err := strconv.Unquote(literal.Value)
			if err != nil || text != tt.first {
				t.Fatalf("decoded magic %q (%v) from %q, want %q", text, err, tt.decl, tt.first)
			}
		})
	}
}
