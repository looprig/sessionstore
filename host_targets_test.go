package sessionstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"math"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// --- fixtures and assertions ----------------------------------------------

// The timeline every case in this file places itself on. A Host observes its
// own capacity at 14:00 and promises to heartbeat within a minute; 14:01 is the
// instant the advertisement lapses and 14:05 is well past it.
var (
	targetObservedAt = time.Date(2026, 8, 30, 14, 0, 0, 0, time.UTC)
	targetExpiresAt  = time.Date(2026, 8, 30, 14, 1, 0, 0, time.UTC)
	targetLapsedAt   = time.Date(2026, 8, 30, 14, 5, 0, 0, time.UTC)
)

const (
	targetAgent   = sessionwire.AgentID("agent-a")
	targetRuntime = "runtime-v1"

	targetHost       = sessionwire.HostID("host-a")
	targetOtherHost  = sessionwire.HostID("host-b")
	targetGeneration = uint64(2)

	targetEndpoint      = sessionwire.InternalEndpoint("wss://host-a.internal:8443/hostlink")
	targetOtherEndpoint = sessionwire.InternalEndpoint("wss://host-b.internal:8443/hostlink")

	targetCapacity = uint64(4)
)

func testHostTargetKey() HostTargetKey {
	return HostTargetKey{
		AgentID:                targetAgent,
		RuntimeCompatibilityID: targetRuntime,
		Placement:              sessionwire.HostPlacementPooled,
	}
}

func testHostAdvertisement() *HostAdvertisement {
	return &HostAdvertisement{
		InternalEndpoint:  targetEndpoint,
		IsolationClass:    sessionwire.HostIsolationClassCrossTenantIsolated,
		Accepting:         true,
		AvailableCapacity: targetCapacity,
		ExpiresAt:         targetExpiresAt,
	}
}

func testHostTarget() HostTarget {
	return HostTarget{
		Key:            testHostTargetKey(),
		HostID:         targetHost,
		HostGeneration: targetGeneration,
		ObservedAt:     targetObservedAt,
		Advertisement:  testHostAdvertisement(),
	}
}

// --- the record and its derived views --------------------------------------

// TestHostTargetRoundTripsThroughStoredBytes pins the codec's fixed point for
// both states a row can be in. A canonicalizer without one would make a row's
// stored bytes depend on how many times it had been rewritten, and this row is
// rewritten on every heartbeat.
func TestHostTargetRoundTripsThroughStoredBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		record HostTarget
	}{
		{name: "an advertised target", record: testHostTarget()},
		{
			name: "a withdrawn target",
			record: func() HostTarget {
				record := testHostTarget()
				record.Advertisement = nil
				return record
			}(),
		},
		{
			name: "a dedicated target holding its one seat",
			record: func() HostTarget {
				record := testHostTarget()
				record.Key.Placement = sessionwire.HostPlacementDedicated
				record.Advertisement.AvailableCapacity = 1
				record.Advertisement.IsolationClass = sessionwire.HostIsolationClassTenantExclusive
				return record
			}(),
		},
		{
			name: "an advertised target with no seats left",
			record: func() HostTarget {
				record := testHostTarget()
				record.Advertisement.AvailableCapacity = 0
				return record
			}(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			encoded, canonical, err := encodeHostTarget(test.record)
			if err != nil {
				t.Fatalf("encodeHostTarget: %v", err)
			}
			decoded, err := decodeHostTarget(encoded)
			if err != nil {
				t.Fatalf("decodeHostTarget: %v", err)
			}
			assertSameHostTarget(t, decoded, canonical)

			again, _, err := encodeHostTarget(decoded)
			if err != nil {
				t.Fatalf("re-encode: %v", err)
			}
			if string(again) != string(encoded) {
				t.Fatalf("re-encoding is not a fixed point:\n first: %s\nsecond: %s", encoded, again)
			}
		})
	}
}

// TestHostTargetReportProjectsTheAdvertisement holds the projection to naming
// every member of the advertisement, so a member added to either side without
// the other is a failure rather than a silently dropped field.
func TestHostTargetReportProjectsTheAdvertisement(t *testing.T) {
	t.Parallel()

	record := testHostTarget()
	report, err := record.Report()
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	want := sessionwire.HostLinkCapacityReport{
		Version:                sessionwire.CurrentWireVersion,
		HostID:                 targetHost,
		HostGeneration:         targetGeneration,
		AgentID:                targetAgent,
		RuntimeCompatibilityID: targetRuntime,
		Placement:              sessionwire.HostPlacementPooled,
		InternalEndpoint:       targetEndpoint,
		IsolationClass:         sessionwire.HostIsolationClassCrossTenantIsolated,
		Accepting:              true,
		AvailableCapacity:      targetCapacity,
		ObservedAt:             targetObservedAt,
		ExpiresAt:              targetExpiresAt,
	}
	if report != want {
		t.Fatalf("report = %+v, want %+v", report, want)
	}

	withdrawn := testHostTarget()
	withdrawn.Advertisement = nil
	if _, err := withdrawn.Report(); err == nil {
		t.Fatal("a withdrawn target projected a capacity report; a withdrawal is not an advertisement")
	}
}

// TestHostTargetViewsAreDerivedFromTheRecord is the whole of "what removes a
// row from a page". Both views are functions of the stored record alone, so a
// reader can rebuild them from the bytes and no operation can file a view state
// the record does not justify.
func TestHostTargetViewsAreDerivedFromTheRecord(t *testing.T) {
	t.Parallel()

	withdrawn := testHostTarget()
	withdrawn.Advertisement = nil

	unaccepting := testHostTarget()
	unaccepting.Advertisement.Accepting = false

	empty := testHostTarget()
	empty.Advertisement.AvailableCapacity = 0

	tests := []struct {
		name   string
		record HostTarget
		rank   storage.Rank
		due    storage.Due
	}{
		{
			name:   "an accepting advertisement is ranked by its free capacity and due at its expiry",
			record: testHostTarget(),
			rank:   storage.Rank{Ranked: true, Value: int64(targetCapacity)},
			due:    storage.Due{State: storage.DueAt, UnixMillis: targetExpiresAt.UnixMilli()},
		},
		{
			name:   "an accepting advertisement with no seats is still ranked, lowest",
			record: empty,
			rank:   storage.Rank{Ranked: true, Value: 0},
			due:    storage.Due{State: storage.DueAt, UnixMillis: targetExpiresAt.UnixMilli()},
		},
		{
			name:   "a host that has stopped accepting leaves the placement view and stays due",
			record: unaccepting,
			rank:   storage.Rank{},
			due:    storage.Due{State: storage.DueAt, UnixMillis: targetExpiresAt.UnixMilli()},
		},
		{
			name:   "a withdrawal leaves both views",
			record: withdrawn,
			rank:   storage.Rank{},
			due:    storage.Due{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := hostTargetRank(test.record); got != test.rank {
				t.Errorf("rank = %+v, want %+v", got, test.rank)
			}
			if got := hostTargetDue(test.record); got != test.due {
				t.Errorf("due = %+v, want %+v", got, test.due)
			}
		})
	}
}

func assertSameHostTarget(t *testing.T, got, want HostTarget) {
	t.Helper()
	if got.Key != want.Key || got.HostID != want.HostID || got.HostGeneration != want.HostGeneration {
		t.Fatalf("identity = %+v, want %+v", got, want)
	}
	if !got.ObservedAt.Equal(want.ObservedAt) {
		t.Fatalf("observed_at = %s, want %s", got.ObservedAt, want.ObservedAt)
	}
	switch {
	case got.Advertisement == nil && want.Advertisement == nil:
	case got.Advertisement == nil || want.Advertisement == nil:
		t.Fatalf("advertisement = %+v, want %+v", got.Advertisement, want.Advertisement)
	case *got.Advertisement != *want.Advertisement:
		t.Fatalf("advertisement = %+v, want %+v", *got.Advertisement, *want.Advertisement)
	}
}

// --- publishing and heartbeating -------------------------------------------

// TestPublishHostTargetReportsALostCreateAsAConflict covers the one outcome a
// first publish has that no ordinary path reaches: another writer created the
// same (target, host) row while this create was in flight. It is reported as a
// lost race carrying the current revision rather than being turned into an
// update here, because the row that arrived carries a generation this request
// has never been compared against.
func TestPublishHostTargetReportsALostCreateAsAConflict(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = recorder
	store, _ := hostTargetFixture(t, base)

	raced := false
	recorder.beforeCreate = func() {
		if raced {
			return
		}
		raced = true
		competitor := testPublishRequest(targetGeneration + 1)
		competitor.Advertisement.AvailableCapacity = 7
		mustPublish(t, store, competitor)
	}
	_, err := store.PublishHostTarget(context.Background(), testPublishRequest(targetGeneration))
	got := assertHostTargetCode(t, err, HostTargetErrorConflict)
	if got.Field != "create" {
		t.Fatalf("field = %q, want %q", got.Field, "create")
	}
	if got.Revision == 0 {
		t.Fatal("a lost create disclosed no revision, so a caller cannot re-read against it")
	}

	// The competitor's row is intact: a lost create never overwrites.
	stored := storedHostTargetRow(t, store, testHostTargetKey(), targetHost)
	record, err := decodeHostTarget(stored.Value)
	if err != nil {
		t.Fatalf("decodeHostTarget: %v", err)
	}
	if record.Advertisement.AvailableCapacity != 7 {
		t.Fatalf("the loser overwrote the winner: %+v", record.Advertisement)
	}
}

// hostTargetFixture opens a store whose clock starts at targetObservedAt.
func hostTargetFixture(t *testing.T, backend *storage.Composite) (*Store, *movableClock) {
	t.Helper()
	clock := newMovableClock(targetObservedAt)
	return openStore(t, backend, WithClock(clock)), clock
}

func testPublishRequest(generation uint64) PublishHostTargetRequest {
	return PublishHostTargetRequest{
		Key:            testHostTargetKey(),
		HostID:         targetHost,
		HostGeneration: generation,
		ObservedAt:     targetObservedAt,
		Advertisement:  *testHostAdvertisement(),
	}
}

func mustPublish(t *testing.T, store *Store, req PublishHostTargetRequest) HostTargetEntry {
	t.Helper()
	entry, err := store.PublishHostTarget(context.Background(), req)
	if err != nil {
		t.Fatalf("PublishHostTarget: %v", err)
	}
	return entry
}

func assertHostTargetCode(t *testing.T, err error, want HostTargetErrorCode) *HostTargetError {
	t.Helper()
	var got *HostTargetError
	if !errors.As(err, &got) {
		t.Fatalf("error = %T %v, want *HostTargetError", err, err)
	}
	if got.Code != want {
		t.Fatalf("code = %q, want %q (%v)", got.Code, want, err)
	}
	return got
}

func storedHostTargetRow(t *testing.T, store *Store, key HostTargetKey, host sessionwire.HostID) storage.OrderedRecord {
	t.Helper()
	scope, err := store.deriveHostTargetScope(key)
	if err != nil {
		t.Fatalf("deriveHostTargetScope: %v", err)
	}
	stored, err := store.backend.OrderedIndex.Get(context.Background(), hostTargetID(scope, host))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	return stored
}

// TestPublishHostTargetAdvertisesThenHeartbeatsInOneWrite pins the operation
// this record is built around: a heartbeat is a republish, and it moves the
// stored value, the rank, and the due time in ONE compare-and-swap. Three
// separate writes could leave a row ranked at a capacity it no longer has or
// due at an expiry it has already passed.
func TestPublishHostTargetAdvertisesThenHeartbeatsInOneWrite(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = recorder
	store, clock := hostTargetFixture(t, base)

	first := mustPublish(t, store, testPublishRequest(targetGeneration))
	if first.Target.Advertisement.AvailableCapacity != targetCapacity {
		t.Fatalf("capacity = %d, want %d", first.Target.Advertisement.AvailableCapacity, targetCapacity)
	}

	// The heartbeat: a later observation, less free capacity, a further expiry.
	later := targetObservedAt.Add(30 * time.Second)
	clock.set(later)
	recorder.reset()
	beat := testPublishRequest(targetGeneration)
	beat.ObservedAt = later
	beat.Advertisement.AvailableCapacity = 1
	beat.Advertisement.ExpiresAt = later.Add(time.Minute)
	second, err := store.PublishHostTarget(context.Background(), beat)
	if err != nil {
		t.Fatalf("PublishHostTarget: %v", err)
	}
	if second.Revision == first.Revision {
		t.Fatalf("a heartbeat did not advance the revision (%d)", second.Revision)
	}

	if got := recorder.countOf("create"); got != 0 {
		t.Errorf("a heartbeat created %d records; it must compare-and-swap the row it read", got)
	}
	if got := recorder.countOf("update"); got != 1 {
		t.Fatalf("a heartbeat performed %d updates; value, rank, and due move in one CAS", got)
	}
	update, _ := recorder.lastOf("update")
	wantRank := storage.Rank{Ranked: true, Value: 1}
	wantDue := storage.Due{State: storage.DueAt, UnixMillis: later.Add(time.Minute).UnixMilli()}
	if update.rank != wantRank {
		t.Errorf("heartbeat rank = %+v, want %+v", update.rank, wantRank)
	}
	if update.due != wantDue {
		t.Errorf("heartbeat due = %+v, want %+v", update.due, wantDue)
	}

	stored := storedHostTargetRow(t, store, testHostTargetKey(), targetHost)
	if stored.Rank != wantRank || stored.Due != wantDue {
		t.Fatalf("stored views = %+v %+v, want %+v %+v", stored.Rank, stored.Due, wantRank, wantDue)
	}
}

// TestPublishHostTargetRefusesASupersededIncarnation pins the one ordering
// guarantee this row makes. The generation is a high-water mark over ONE Host's
// own writes: an equal generation is admitted because one incarnation
// heartbeats many times, and only a strictly lower one has provably been
// restarted away.
func TestPublishHostTargetRefusesASupersededIncarnation(t *testing.T) {
	t.Parallel()

	store, _ := hostTargetFixture(t, memstore.New())
	mustPublish(t, store, testPublishRequest(targetGeneration))
	mustPublish(t, store, testPublishRequest(targetGeneration+1))

	stale := testPublishRequest(targetGeneration)
	stale.Advertisement.AvailableCapacity = 0
	_, err := store.PublishHostTarget(context.Background(), stale)
	got := assertHostTargetCode(t, err, HostTargetErrorGeneration)
	if got.Generation != targetGeneration+1 {
		t.Fatalf("generation = %d, want the high-water %d", got.Generation, targetGeneration+1)
	}
	if got.Field != "host_generation" {
		t.Fatalf("field = %q, want %q", got.Field, "host_generation")
	}

	current := storedHostTargetRow(t, store, testHostTargetKey(), targetHost)
	record, err := decodeHostTarget(current.Value)
	if err != nil {
		t.Fatalf("decodeHostTarget: %v", err)
	}
	if record.Advertisement.AvailableCapacity != targetCapacity {
		t.Fatalf("a superseded incarnation overwrote live capacity: %+v", record.Advertisement)
	}
}

// TestHostTargetRowsAreOnePerTargetAndHost drives both multiplicities the task
// names: two Hosts serving one target are two rows in ONE ranked scope, and one
// Host serving two targets is two rows in DIFFERENT scopes. The second is what
// makes this derived capacity rather than a competing catalogue of definitions:
// nothing here says the two targets exist, only that one process serves both.
func TestHostTargetRowsAreOnePerTargetAndHost(t *testing.T) {
	t.Parallel()

	store, _ := hostTargetFixture(t, memstore.New())

	pooled := testHostTargetKey()
	dedicated := testHostTargetKey()
	dedicated.Placement = sessionwire.HostPlacementDedicated

	// Two Hosts, one target.
	mustPublish(t, store, testPublishRequest(targetGeneration))
	otherHost := testPublishRequest(targetGeneration)
	otherHost.HostID = targetOtherHost
	otherHost.Advertisement.InternalEndpoint = targetOtherEndpoint
	otherHost.Advertisement.AvailableCapacity = 9
	mustPublish(t, store, otherHost)

	// One Host, two targets.
	secondTarget := testPublishRequest(targetGeneration)
	secondTarget.Key = dedicated
	secondTarget.Advertisement.AvailableCapacity = 1
	mustPublish(t, store, secondTarget)

	pooledScope, err := store.deriveHostTargetScope(pooled)
	if err != nil {
		t.Fatalf("deriveHostTargetScope: %v", err)
	}
	dedicatedScope, err := store.deriveHostTargetScope(dedicated)
	if err != nil {
		t.Fatalf("deriveHostTargetScope: %v", err)
	}
	if pooledScope.TargetScope == dedicatedScope.TargetScope {
		t.Fatal("two targets share one ranking scope; a placement page for one would return the other's rows")
	}

	first := storedHostTargetRow(t, store, pooled, targetHost)
	second := storedHostTargetRow(t, store, pooled, targetOtherHost)
	if first.ID == second.ID {
		t.Fatal("two Hosts serving one target share one row")
	}
	if first.ID.OrderingScope != second.ID.OrderingScope {
		t.Fatalf("two Hosts serving one target are in different scopes: %q and %q",
			first.ID.OrderingScope, second.ID.OrderingScope)
	}
	across := storedHostTargetRow(t, store, dedicated, targetHost)
	if across.ID.OrderingScope == first.ID.OrderingScope {
		t.Fatal("one Host's two targets share one scope")
	}
	if across.ID.StableKey != first.ID.StableKey {
		t.Fatalf("one Host's two rows carry different stable keys: %q and %q", across.ID.StableKey, first.ID.StableKey)
	}
}

// --- the placement view -----------------------------------------------------

// TestListCompatibleHostsRefusesAContinuationItCannotReissue holds the payload
// ceiling on the ISSUE side as well as on presentation. A page that handed back
// a token larger than this reader accepts would give a caller a continuation it
// could never use, and the walk would restart forever.
func TestListCompatibleHostsRefusesAContinuationItCannotReissue(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store, _ := hostTargetFixture(t, base)
	publishCapacity(t, store, targetHost, 1, targetExpiresAt)

	hostile.answerRanked(func(page storage.RankedPage, err error) (storage.RankedPage, error) {
		if err != nil {
			return page, err
		}
		page.NextCursor = storage.RankedCursor(strings.Repeat("t", maxHostTargetCursorBytes))
		return page, nil
	})
	_, err := store.ListCompatibleHosts(context.Background(), ListCompatibleHostsRequest{Key: testHostTargetKey()})
	got := assertHostTargetCode(t, err, HostTargetErrorBackend)
	if got.Field != "next_cursor" {
		t.Fatalf("field = %q, want %q", got.Field, "next_cursor")
	}
}

// TestListCompatibleHostsAnswersAnUnadvertisedTarget pins the deliberate
// asymmetry between the reader and the writers. A listing names no row, so a
// target nothing has ever advertised has no witness to prove; requiring one
// would answer "no capacity for this target" with a failure, which is the
// question a Factory asks first about every new target.
func TestListCompatibleHostsAnswersAnUnadvertisedTarget(t *testing.T) {
	t.Parallel()

	store, _ := hostTargetFixture(t, memstore.New())
	page := mustListCompatible(t, store, ListCompatibleHostsRequest{Key: testHostTargetKey()})
	if len(page.Hosts) != 0 || page.NextCursor != "" || page.LapsedSkipped != 0 {
		t.Fatalf("page = %+v, want an empty page", page)
	}
}

// corruptStoredHostTarget overwrites one row's value in place, keeping the rank
// and due the conforming write filed. That is what a NEWER WRITER's row looks
// like to this reader: correctly filed, correctly ranked, correctly due, and
// carrying a record version this build does not know.
func corruptStoredHostTarget(t *testing.T, backend *storage.Composite, store *Store, key HostTargetKey, host sessionwire.HostID, value []byte) {
	t.Helper()
	scope, err := store.deriveHostTargetScope(key)
	if err != nil {
		t.Fatalf("deriveHostTargetScope: %v", err)
	}
	id := hostTargetID(scope, host)
	stored, err := backend.OrderedIndex.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := backend.OrderedIndex.Update(
		context.Background(), id, stored.Revision, value, stored.Rank, stored.Due); err != nil {
		t.Fatalf("Update: %v", err)
	}
}

// TestListCompatibleHostsSkipsARowItCannotRead is the wedge this reader must
// not have. A row this build cannot decode — the newer writer's row that the
// reconciler deliberately refuses to rewrite — stays ranked forever, so a
// reader that failed the whole page on it would take EVERY Host serving that
// target out of service permanently, with no automatic recovery anywhere in the
// system. Skipping and counting removes the wedge while keeping the newer
// writer's row untouched: it starts being published the moment a reader that
// understands it asks.
func TestListCompatibleHostsSkipsARowItCannotRead(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	store, clock := hostTargetFixture(t, base)
	publishCapacity(t, store, "host-a", 9, targetExpiresAt)
	publishCapacity(t, store, "host-b", 1, targetExpiresAt)
	corruptStoredHostTarget(t, base, store, testHostTargetKey(), "host-a",
		[]byte(`{"record_version":2,"agent_id":"agent-a"}`))

	page := mustListCompatible(t, store, ListCompatibleHostsRequest{Key: testHostTargetKey()})
	if len(page.Hosts) != 1 || page.Hosts[0].HostID != "host-b" {
		t.Fatalf("hosts = %+v, want the one readable Host", page.Hosts)
	}
	if page.UnreadableSkipped != 1 {
		t.Fatalf("unreadable = %d, want 1", page.UnreadableSkipped)
	}
	if page.LapsedSkipped != 0 {
		t.Fatalf("unreadable rows must not be counted as lapsed: %+v", page)
	}

	// And it stays that way across a sweep, because a sweep deliberately does
	// not rewrite a row it cannot read either.
	clock.set(targetLapsedAt)
	mustReconcile(t, store, ReconcileHostTargetsRequest{})
	after := mustListCompatible(t, store, ListCompatibleHostsRequest{Key: testHostTargetKey()})
	if after.UnreadableSkipped != 1 {
		t.Fatalf("unreadable = %d after a sweep, want 1", after.UnreadableSkipped)
	}
}

// TestListCompatibleHostsSkipsAMisfiledRowRatherThanFailingThePage reaches the
// filing checks through the READ PATH rather than through a direct call, which
// is the only way to prove they are wired into it — and, with the skip above,
// that one misfiled row cannot take a whole target's capacity out of service.
func TestListCompatibleHostsSkipsAMisfiledRowRatherThanFailingThePage(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store, _ := hostTargetFixture(t, base)
	publishCapacity(t, store, "host-a", 9, targetExpiresAt)
	publishCapacity(t, store, "host-b", 1, targetExpiresAt)

	hostile.answerRanked(func(page storage.RankedPage, err error) (storage.RankedPage, error) {
		if err != nil {
			return page, err
		}
		for i := range page.Records {
			if page.Records[i].ID.StableKey == "host-a" {
				page.Records[i].ID.OrderingScope += "/elsewhere"
			}
		}
		return page, nil
	})
	page := mustListCompatible(t, store, ListCompatibleHostsRequest{Key: testHostTargetKey()})
	if len(page.Hosts) != 1 || page.Hosts[0].HostID != "host-b" {
		t.Fatalf("hosts = %+v, want the one correctly filed Host", page.Hosts)
	}
	if page.UnreadableSkipped != 1 {
		t.Fatalf("unreadable = %d, want 1", page.UnreadableSkipped)
	}
}

// publishCapacity advertises one Host's capacity for the fixture target.
func publishCapacity(t *testing.T, store *Store, host sessionwire.HostID, capacity uint64, expires time.Time) {
	t.Helper()
	req := testPublishRequest(targetGeneration)
	req.HostID = host
	req.Advertisement.InternalEndpoint = sessionwire.InternalEndpoint("wss://" + string(host) + ".internal:8443/hostlink")
	req.Advertisement.AvailableCapacity = capacity
	req.Advertisement.ExpiresAt = expires
	mustPublish(t, store, req)
}

func mustListCompatible(t *testing.T, store *Store, req ListCompatibleHostsRequest) HostTargetPage {
	t.Helper()
	page, err := store.ListCompatibleHosts(context.Background(), req)
	if err != nil {
		t.Fatalf("ListCompatibleHosts: %v", err)
	}
	return page
}

// TestListCompatibleHostsIsOneRankedProviderQuery pins the shape of the read a
// Factory makes on every placement decision. The target restriction and the
// capacity order are both inside the provider query, so the limit applies to an
// already-restricted, already-ordered result rather than to a wider page this
// package narrows afterwards.
func TestListCompatibleHostsIsOneRankedProviderQuery(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	audit := &listAuditOrdered{OrderedIndex: base.OrderedIndex, t: t}
	base.OrderedIndex = audit
	store, _ := hostTargetFixture(t, base)

	publishCapacity(t, store, "host-a", 1, targetExpiresAt)
	publishCapacity(t, store, "host-b", 9, targetExpiresAt)
	publishCapacity(t, store, "host-c", 5, targetExpiresAt)

	scope, err := store.deriveHostTargetScope(testHostTargetKey())
	if err != nil {
		t.Fatalf("deriveHostTargetScope: %v", err)
	}
	audit.arm(scope.TargetScope)

	page := mustListCompatible(t, store, ListCompatibleHostsRequest{Key: testHostTargetKey()})
	var got []string
	for _, host := range page.Hosts {
		got = append(got, string(host.HostID))
	}
	want := []string{"host-b", "host-c", "host-a"}
	if len(got) != len(want) {
		t.Fatalf("hosts = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("hosts = %v, want %v (most free capacity first)", got, want)
		}
	}
	if calls := audit.rankedCalls(); len(calls) != 1 {
		t.Fatalf("a placement page issued %d ranked queries, want 1", len(calls))
	}
	if page.LapsedSkipped != 0 {
		t.Fatalf("lapsed = %d, want 0", page.LapsedSkipped)
	}
}

// TestListCompatibleHostsNeverOffersALapsedAdvertisement is half of step 4. A
// crashed Host's row stays RANKED until something removes it, so the row is
// still in the provider's page — but the reader must never hand a Factory an
// endpoint this store will not vouch for, and the count is what tells an
// operator the directory needs a reconciler pass rather than hiding it.
func TestListCompatibleHostsNeverOffersALapsedAdvertisement(t *testing.T) {
	t.Parallel()

	store, clock := hostTargetFixture(t, memstore.New())
	publishCapacity(t, store, "host-a", 9, targetExpiresAt)
	publishCapacity(t, store, "host-b", 1, targetObservedAt.Add(10*time.Minute))

	// Past host-a's promise and before host-b's.
	clock.set(targetLapsedAt)
	page := mustListCompatible(t, store, ListCompatibleHostsRequest{Key: testHostTargetKey()})
	if len(page.Hosts) != 1 || page.Hosts[0].HostID != "host-b" {
		t.Fatalf("hosts = %+v, want only host-b", page.Hosts)
	}
	if page.LapsedSkipped != 1 {
		t.Fatalf("lapsed = %d, want 1", page.LapsedSkipped)
	}

	// The lapsed row is still ranked, which is exactly why the reader cannot be
	// the only thing standing between it and a placement.
	stored := storedHostTargetRow(t, store, testHostTargetKey(), "host-a")
	if !stored.Rank.Ranked {
		t.Fatal("the lapsed row left the ranked view on its own; nothing in this package does that")
	}
}

// TestListCompatibleHostsIsHalfOpenAtTheExpiry pins the expiry instant itself,
// which is the only place an off-by-one in the interval is observable and the
// place this record's convention has to agree with every other deadline in the
// package.
func TestListCompatibleHostsIsHalfOpenAtTheExpiry(t *testing.T) {
	t.Parallel()

	store, clock := hostTargetFixture(t, memstore.New())
	publishCapacity(t, store, targetHost, 3, targetExpiresAt)

	clock.set(targetExpiresAt.Add(-time.Nanosecond))
	if page := mustListCompatible(t, store, ListCompatibleHostsRequest{Key: testHostTargetKey()}); len(page.Hosts) != 1 {
		t.Fatalf("an advertisement one nanosecond before its expiry was not offered: %+v", page)
	}
	clock.set(targetExpiresAt)
	page := mustListCompatible(t, store, ListCompatibleHostsRequest{Key: testHostTargetKey()})
	if len(page.Hosts) != 0 || page.LapsedSkipped != 1 {
		t.Fatalf("an advertisement AT its expiry was offered: %+v", page)
	}
}

// TestListCompatibleHostsPagesUnderOneTarget walks a target's capacity a page
// at a time and holds the continuation to the target it was issued for. The
// permissive provider is what makes the second half a live assertion: a
// conforming provider refuses a foreign token itself, so a test written over
// one could not tell which guard fired.
func TestListCompatibleHostsPagesUnderOneTarget(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	base.OrderedIndex = permissiveRankedOrdered{OrderedIndex: base.OrderedIndex}
	store, _ := hostTargetFixture(t, base)

	publishCapacity(t, store, "host-a", 3, targetExpiresAt)
	publishCapacity(t, store, "host-b", 2, targetExpiresAt)
	publishCapacity(t, store, "host-c", 1, targetExpiresAt)

	first := mustListCompatible(t, store, ListCompatibleHostsRequest{Key: testHostTargetKey(), Limit: 2})
	if len(first.Hosts) != 2 || first.NextCursor == "" {
		t.Fatalf("first page = %+v, want two hosts and a continuation", first)
	}
	second := mustListCompatible(t, store, ListCompatibleHostsRequest{
		Key: testHostTargetKey(), Limit: 2, Cursor: first.NextCursor})
	if len(second.Hosts) != 1 || second.NextCursor != "" {
		t.Fatalf("second page = %+v, want the last host and no continuation", second)
	}

	// The same token presented for another target of the same agent.
	elsewhere := testHostTargetKey()
	elsewhere.Placement = sessionwire.HostPlacementDedicated
	_, err := store.ListCompatibleHosts(context.Background(), ListCompatibleHostsRequest{
		Key: elsewhere, Limit: 2, Cursor: first.NextCursor})
	got := assertHostTargetCode(t, err, HostTargetErrorCursor)
	if got.Field != "cursor" {
		t.Fatalf("field = %q, want %q", got.Field, "cursor")
	}

	// And a token this store never issued at all.
	_, err = store.ListCompatibleHosts(context.Background(), ListCompatibleHostsRequest{
		Key: testHostTargetKey(), Cursor: "not-a-cursor"})
	assertHostTargetCode(t, err, HostTargetErrorCursor)
}

// --- withdrawal: the drain and the reconciler -------------------------------

// TestReconcileHostTargetsHoldsOneDueBoundForTheWholeSweep pins the reason the
// sweep reads the clock once. A due cursor is bound to the bound that issued
// it, so a sweep that re-read the clock per page could not page at all — and
// the failure would appear only under load, when a sweep spans a millisecond.
func TestReconcileHostTargetsHoldsOneDueBoundForTheWholeSweep(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New(), WithClock(&advancingClock{now: targetObservedAt}))
	for _, host := range []sessionwire.HostID{"host-a", "host-b", "host-c"} {
		req := testPublishRequest(targetGeneration)
		req.HostID = host
		req.Advertisement.InternalEndpoint = sessionwire.InternalEndpoint("wss://" + string(host) + ".internal:8443/hostlink")
		req.Advertisement.ExpiresAt = targetObservedAt.Add(time.Second)
		mustPublish(t, store, req)
	}

	// Push the clock past every promise, then sweep one row at a time.
	for range 4000 {
		store.clock.Now()
	}
	result := mustReconcile(t, store, ReconcileHostTargetsRequest{Limit: 1, MaxPages: 8})
	if result.Withdrawn != 3 || !result.Exhausted {
		t.Fatalf("sweep = %+v, want all three withdrawn across pages", result)
	}
}

// TestReconcileHostTargetsAcceptsItsPageBudgetBounds drives the budget check at
// both of its ends.
//
// One over the maximum is refused elsewhere in this file, but the maximum
// ITSELF was never presented, so relaxing the bound to >= would have refused
// the largest budget the constant advertises and nothing would have said so. A
// negative budget is the other end: it cannot arrive through the zero-means-
// default path, so only a caller naming one reaches it.
func TestReconcileHostTargetsAcceptsItsPageBudgetBounds(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New(), WithClock(&advancingClock{now: targetObservedAt}))
	mustReconcile(t, store, ReconcileHostTargetsRequest{Limit: 1, MaxPages: MaxHostTargetReconcilePages})

	_, err := store.ReconcileHostTargets(context.Background(), ReconcileHostTargetsRequest{
		Limit: 1, MaxPages: -1})
	got := assertHostTargetCode(t, err, HostTargetErrorInvalid)
	if got.Field != "max_pages" {
		t.Fatalf("field = %q, want max_pages", got.Field)
	}
}

// TestReconcileHostTargetsResumesPastAnExhaustedBudget is the boundary case,
// not the mechanism. A row this sweep cannot read stays due forever, so it
// heads every later ascending due page; once the unreadable population reaches
// the page budget, a sweep that always restarted at the head would reach
// NOTHING behind them, ever, and the population only grows. The continuation is
// what makes the cost bounded rather than the progress zero.
func TestReconcileHostTargetsResumesPastAnExhaustedBudget(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	store, clock := hostTargetFixture(t, base)
	for _, host := range []sessionwire.HostID{"host-a", "host-b", "host-c", "host-d"} {
		publishCapacity(t, store, host, 1, targetExpiresAt)
	}
	// Ascending (due_at, stable_key) puts the three unreadable rows in front of
	// the genuinely crashed one.
	for _, host := range []sessionwire.HostID{"host-a", "host-b", "host-c"} {
		corruptStoredHostTarget(t, base, store, testHostTargetKey(), host,
			[]byte(`{"record_version":2,"agent_id":"agent-a"}`))
	}
	clock.set(targetLapsedAt)

	// The whole budget goes to rows the sweep cannot handle.
	first := mustReconcile(t, store, ReconcileHostTargetsRequest{Limit: 1, MaxPages: 2})
	if first.Unreadable != 2 || first.Withdrawn != 0 || first.Exhausted {
		t.Fatalf("first sweep = %+v, want a budget spent entirely on unreadable rows", first)
	}
	if first.NextCursor == "" {
		t.Fatal("a sweep that ran out of budget must offer a continuation, or its progress is zero forever")
	}

	// Restarting from the head makes exactly the same non-progress, which is
	// the state this continuation exists to escape.
	repeat := mustReconcile(t, store, ReconcileHostTargetsRequest{Limit: 1, MaxPages: 2})
	if repeat.Withdrawn != 0 || repeat.Unreadable != 2 {
		t.Fatalf("a cursorless re-sweep = %+v, want the identical non-progress", repeat)
	}

	// Resuming reaches the row behind them and withdraws it. The budget is
	// deliberately too small to reach the victim from the HEAD, and the clock
	// has moved on, so a sweep that ignored the continuation would report two
	// unreadable rows and no withdrawal, and one that kept the position but
	// recomputed the due bound would present a token for a query the provider
	// never issued. Neither is distinguishable from a real resume without both.
	clock.set(targetLapsedAt.Add(time.Second))
	resumed := mustReconcile(t, store, ReconcileHostTargetsRequest{
		Limit: 1, MaxPages: 2, Cursor: first.NextCursor})
	if resumed.Withdrawn != 1 || resumed.Unreadable != 1 {
		t.Fatalf("resumed sweep = %+v, want one unreadable row stepped over and the crashed Host withdrawn", resumed)
	}
	if !resumed.Exhausted {
		t.Fatalf("resumed sweep = %+v, want the view exhausted", resumed)
	}
	if resumed.NextCursor != "" {
		t.Fatalf("an exhausted sweep issued a continuation: %+v", resumed)
	}
	stored := storedHostTargetRow(t, store, testHostTargetKey(), "host-d")
	if stored.Due != (storage.Due{}) || stored.Rank != (storage.Rank{}) {
		t.Fatalf("the row behind the unreadable ones is still in a view: %+v %+v", stored.Due, stored.Rank)
	}
}

// TestReconcileHostTargetsRefusesAForeignContinuation holds the sweep's
// continuation to its own cursor kind. A placement token and a sweep token are
// both this package's, both opaque, and both handed back by a caller; nothing
// but the kind tag stops one being presented as the other.
func TestReconcileHostTargetsRefusesAForeignContinuation(t *testing.T) {
	t.Parallel()

	store, clock := hostTargetFixture(t, memstore.New())
	publishCapacity(t, store, "host-a", 1, targetExpiresAt)
	publishCapacity(t, store, "host-b", 1, targetExpiresAt)

	placement := mustListCompatible(t, store, ListCompatibleHostsRequest{Key: testHostTargetKey(), Limit: 1})
	if placement.NextCursor == "" {
		t.Fatal("the fixture did not produce a placement continuation")
	}
	clock.set(targetLapsedAt)
	_, err := store.ReconcileHostTargets(context.Background(), ReconcileHostTargetsRequest{
		Cursor: placement.NextCursor})
	assertHostTargetField(HostTargetErrorCursor, "cursor")(t, err)

	sweep := mustReconcile(t, store, ReconcileHostTargetsRequest{Limit: 1, MaxPages: 1})
	if sweep.NextCursor == "" {
		t.Fatal("the fixture did not produce a sweep continuation")
	}
	_, err = store.ListCompatibleHosts(context.Background(), ListCompatibleHostsRequest{
		Key: testHostTargetKey(), Cursor: sweep.NextCursor})
	assertHostTargetField(HostTargetErrorCursor, "cursor")(t, err)

	_, err = store.ReconcileHostTargets(context.Background(), ReconcileHostTargetsRequest{Cursor: "not-a-cursor"})
	assertHostTargetField(HostTargetErrorCursor, "cursor")(t, err)
}

// TestReconcileHostTargetsKeepsItsPositionThroughAProviderFailure pins that
// losing the provider mid-walk does not cost the walk its place. Without the
// continuation on the failure, a caller would restart at the head of the due
// view and pay the whole cost of every unreadable row ahead of it again — which
// is exactly the cost the continuation exists to stop paying.
func TestReconcileHostTargetsKeepsItsPositionThroughAProviderFailure(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store, clock := hostTargetFixture(t, base)
	for _, host := range []sessionwire.HostID{"host-a", "host-b", "host-c"} {
		publishCapacity(t, store, host, 1, targetExpiresAt)
	}
	clock.set(targetLapsedAt)

	pages := 0
	hostile.answerDue(func(page storage.DuePage, err error) (storage.DuePage, error) {
		pages++
		if pages == 2 {
			return storage.DuePage{}, errors.New("provider went away")
		}
		return page, err
	})
	result, err := store.ReconcileHostTargets(context.Background(), ReconcileHostTargetsRequest{
		Limit: 1, MaxPages: 8})
	assertHostTargetCode(t, err, HostTargetErrorBackend)
	if result.Withdrawn != 1 {
		t.Fatalf("result = %+v, want the work done before the failure reported", result)
	}
	if result.NextCursor == "" {
		t.Fatal("a walk that lost the provider gave up its position, so the next call restarts at the head")
	}

	// And the position really does resume: the remaining rows are reached
	// without walking the first one again.
	hostile.answerDue(nil)
	resumed := mustReconcile(t, store, ReconcileHostTargetsRequest{
		Limit: 1, MaxPages: 8, Cursor: result.NextCursor})
	if resumed.Withdrawn != 2 || resumed.Scanned != 2 {
		t.Fatalf("resumed = %+v, want only the two rows behind the failure", resumed)
	}
}

// TestReconcileHostTargetsRefusesAContinuationItCannotReissue holds the sweep's
// payload ceiling on the ISSUE side, and here that is sharper than it is for a
// placement page: a continuation this sweep cannot hand back is a sweep that
// silently reverts to restarting at the head of the due view every pass, which
// is exactly the zero-progress state the continuation exists to remove.
func TestReconcileHostTargetsRefusesAContinuationItCannotReissue(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store, clock := hostTargetFixture(t, base)
	publishCapacity(t, store, targetHost, 1, targetExpiresAt)

	hostile.answerDue(func(page storage.DuePage, err error) (storage.DuePage, error) {
		if err != nil {
			return page, err
		}
		page.NextCursor = storage.DueCursor(strings.Repeat("t", maxHostTargetCursorBytes))
		return page, nil
	})
	clock.set(targetLapsedAt)
	_, err := store.ReconcileHostTargets(context.Background(), ReconcileHostTargetsRequest{MaxPages: 1})
	got := assertHostTargetCode(t, err, HostTargetErrorBackend)
	if got.Field != "next_cursor" {
		t.Fatalf("field = %q, want %q", got.Field, "next_cursor")
	}
}

func testDrainRequest(generation uint64) DrainHostTargetRequest {
	return DrainHostTargetRequest{Key: testHostTargetKey(), HostID: targetHost, HostGeneration: generation}
}

func mustDrain(t *testing.T, store *Store, req DrainHostTargetRequest) HostTargetEntry {
	t.Helper()
	entry, err := store.DrainHostTarget(context.Background(), req)
	if err != nil {
		t.Fatalf("DrainHostTarget: %v", err)
	}
	return entry
}

func mustReconcile(t *testing.T, store *Store, req ReconcileHostTargetsRequest) HostTargetReconcileResult {
	t.Helper()
	result, err := store.ReconcileHostTargets(context.Background(), req)
	if err != nil {
		t.Fatalf("ReconcileHostTargets: %v", err)
	}
	if got := result.Withdrawn + result.StillLive + result.Contended + result.Unreadable + result.Unverified; got != result.Scanned {
		t.Fatalf("a sweep did not account for every row it scanned: %+v", result)
	}
	return result
}

// TestDrainHostTargetLeavesBothViewsInOneWrite is the graceful half of "what
// removes a row". A Host shutting down stops being a placement candidate at the
// instant its withdrawal commits, without waiting for any expiry, and the row
// leaves the deadline page in the same compare-and-swap.
func TestDrainHostTargetLeavesBothViewsInOneWrite(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = recorder
	store, _ := hostTargetFixture(t, base)
	mustPublish(t, store, testPublishRequest(targetGeneration))

	recorder.reset()
	drained := mustDrain(t, store, testDrainRequest(targetGeneration))
	if drained.Target.Advertisement != nil {
		t.Fatalf("a drained row kept its advertisement: %+v", drained.Target.Advertisement)
	}
	if drained.Target.HostGeneration != targetGeneration {
		t.Fatalf("a drained row lost its generation: %d", drained.Target.HostGeneration)
	}
	if got := recorder.countOf("update"); got != 1 {
		t.Fatalf("a drain performed %d updates; the value and both views move together", got)
	}

	stored := storedHostTargetRow(t, store, testHostTargetKey(), targetHost)
	if stored.Rank != (storage.Rank{}) || stored.Due != (storage.Due{}) {
		t.Fatalf("a drained row is still in a view: rank %+v due %+v", stored.Rank, stored.Due)
	}
	if page := mustListCompatible(t, store, ListCompatibleHostsRequest{Key: testHostTargetKey()}); len(page.Hosts) != 0 {
		t.Fatalf("a drained Host is still a placement candidate: %+v", page)
	}

	// Idempotent under one incarnation, and a repeat writes nothing.
	recorder.reset()
	again := mustDrain(t, store, testDrainRequest(targetGeneration))
	if again.Revision != drained.Revision {
		t.Fatalf("a repeated drain rewrote the row: %d then %d", drained.Revision, again.Revision)
	}
	if got := recorder.countOf("update"); got != 0 {
		t.Fatalf("a repeated drain wrote %d times", got)
	}

	// A later incarnation draining an already-drained row is not a repeat: the
	// high-water must rise, or every generation in between could still write.
	raised := mustDrain(t, store, testDrainRequest(targetGeneration+3))
	if raised.Target.HostGeneration != targetGeneration+3 {
		t.Fatalf("generation = %d, want %d", raised.Target.HostGeneration, targetGeneration+3)
	}
}

// TestDrainHostTargetRefusesWhatItCannotHaveWritten covers the two requests a
// drain must not turn into a row: one from a superseded incarnation, and one
// for capacity that was never advertised at all. Creating the latter would mint
// a generation high-water out of nothing AND leave a permanent row for a Host
// that never offered anything, which is exactly the accumulation this record
// exists to avoid.
func TestDrainHostTargetRefusesWhatItCannotHaveWritten(t *testing.T) {
	t.Parallel()

	store, _ := hostTargetFixture(t, memstore.New())
	_, err := store.DrainHostTarget(context.Background(), testDrainRequest(targetGeneration))
	assertHostTargetCode(t, err, HostTargetErrorNotFound)

	mustPublish(t, store, testPublishRequest(targetGeneration+1))
	_, err = store.DrainHostTarget(context.Background(), testDrainRequest(targetGeneration))
	got := assertHostTargetCode(t, err, HostTargetErrorGeneration)
	if got.Generation != targetGeneration+1 {
		t.Fatalf("generation = %d, want %d", got.Generation, targetGeneration+1)
	}
	if page := mustListCompatible(t, store, ListCompatibleHostsRequest{Key: testHostTargetKey()}); len(page.Hosts) != 1 {
		t.Fatalf("a superseded incarnation drained live capacity: %+v", page)
	}
}

// TestAWithdrawnRowIsReusedRatherThanRetired is why nothing in this file calls
// the provider's Delete. A Host that drains at shutdown and advertises again at
// startup is this record's ordinary lifecycle; the ordered index promises an
// identity is never reusable after a tombstone, so a physical delete would
// retire that Host's ability to serve the target permanently.
func TestAWithdrawnRowIsReusedRatherThanRetired(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = recorder
	store, _ := hostTargetFixture(t, base)

	mustPublish(t, store, testPublishRequest(targetGeneration))
	mustDrain(t, store, testDrainRequest(targetGeneration))
	restarted := mustPublish(t, store, testPublishRequest(targetGeneration+1))
	if restarted.Target.Advertisement == nil {
		t.Fatal("a restarted Host could not advertise again")
	}
	if page := mustListCompatible(t, store, ListCompatibleHostsRequest{Key: testHostTargetKey()}); len(page.Hosts) != 1 {
		t.Fatalf("a restarted Host is not a placement candidate: %+v", page)
	}
	if got := recorder.countOf("delete"); got != 0 {
		t.Fatalf("this package called Delete %d times; that retires the identity forever", got)
	}
}

// TestReconcileHostTargetsWithdrawsACrashedAdvertisement is the crash half of
// "what removes a row", and the second assertion is the one that matters: the
// row leaves the deadline page in the same write that handles it, so no later
// sweep ever sees it again. A row that stayed due after being handled would sit
// at the head of an ascending due page forever and starve everything behind it.
func TestReconcileHostTargetsWithdrawsACrashedAdvertisement(t *testing.T) {
	t.Parallel()

	store, clock := hostTargetFixture(t, memstore.New())
	publishCapacity(t, store, targetHost, 9, targetExpiresAt)
	publishCapacity(t, store, targetOtherHost, 1, targetObservedAt.Add(10*time.Minute))

	clock.set(targetLapsedAt)
	result := mustReconcile(t, store, ReconcileHostTargetsRequest{})
	if result.Scanned != 1 || result.Withdrawn != 1 {
		t.Fatalf("sweep = %+v, want one row scanned and withdrawn", result)
	}
	if !result.Exhausted {
		t.Fatalf("sweep = %+v, want the due view exhausted", result)
	}

	stored := storedHostTargetRow(t, store, testHostTargetKey(), targetHost)
	if stored.Rank != (storage.Rank{}) || stored.Due != (storage.Due{}) {
		t.Fatalf("a reconciled row is still in a view: rank %+v due %+v", stored.Rank, stored.Due)
	}
	record, err := decodeHostTarget(stored.Value)
	if err != nil {
		t.Fatalf("decodeHostTarget: %v", err)
	}
	if record.Advertisement != nil || record.HostGeneration != targetGeneration {
		t.Fatalf("a reconciled row = %+v, want withdrawn with its generation kept", record)
	}

	// The row is handled exactly once, forever: a second sweep at the same
	// instant finds nothing, and the unexpired Host was never touched.
	if again := mustReconcile(t, store, ReconcileHostTargetsRequest{}); again.Scanned != 0 {
		t.Fatalf("a second sweep re-scanned %d handled rows: %+v", again.Scanned, again)
	}
	live := storedHostTargetRow(t, store, testHostTargetKey(), targetOtherHost)
	if !live.Rank.Ranked {
		t.Fatal("the sweep withdrew a Host whose promise had not lapsed")
	}
}

// TestReconcileHostTargetsRevalidatesTheStoredExpiry drives the two ways a row
// in a due page can already be live again by the time the sweep reaches it. The
// due view is weakly consistent, so a sweep that trusted the page would withdraw
// the capacity of a Host that is alive and heartbeating.
func TestReconcileHostTargetsRevalidatesTheStoredExpiry(t *testing.T) {
	t.Parallel()

	t.Run("the page names a row whose stored expiry has not lapsed", func(t *testing.T) {
		t.Parallel()

		base := memstore.New()
		hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
		base.OrderedIndex = hostile
		recorder := &recordingOrdered{OrderedIndex: hostile}
		base.OrderedIndex = recorder
		store, clock := hostTargetFixture(t, base)
		publishCapacity(t, store, targetHost, 9, targetObservedAt.Add(10*time.Minute))

		// A provider handing back a row whose due time has moved on, which the
		// ordered index explicitly permits: due changes are weakly consistent.
		hostile.answerDue(func(page storage.DuePage, err error) (storage.DuePage, error) {
			if err != nil {
				return page, err
			}
			row := storedHostTargetRow(t, store, testHostTargetKey(), targetHost)
			page.Records = append(page.Records, row)
			return page, nil
		})
		clock.set(targetLapsedAt)
		recorder.reset()
		result := mustReconcile(t, store, ReconcileHostTargetsRequest{})
		if result.Scanned != 1 || result.StillLive != 1 || result.Withdrawn != 0 {
			t.Fatalf("sweep = %+v, want one live row left alone", result)
		}
		if got := recorder.countOf("update"); got != 0 {
			t.Fatalf("the sweep wrote %d times over a live advertisement", got)
		}
	})

	t.Run("a heartbeat lands between the due page and the compare-and-swap", func(t *testing.T) {
		t.Parallel()

		base := memstore.New()
		hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
		base.OrderedIndex = hostile
		store, clock := hostTargetFixture(t, base)
		publishCapacity(t, store, targetHost, 9, targetExpiresAt)
		clock.set(targetLapsedAt)

		// The interleaving point is the DUE PAGE, not the sweep's own write,
		// and the difference is the whole claim under test. A heartbeat run
		// from a hook on the sweep's Update lands after every read the sweep
		// could make, so a sweep that re-read the row and compare-and-swapped
		// on the FRESH revision would pass such a test while still withdrawing
		// a Host that is alive. Publishing from the ListDue reply puts the
		// heartbeat strictly between the page and the write, which is the only
		// position that holds the sweep to the revision the PAGE reported.
		beaten := false
		hostile.answerDue(func(page storage.DuePage, err error) (storage.DuePage, error) {
			if err != nil || beaten {
				return page, err
			}
			beaten = true
			beat := testPublishRequest(targetGeneration)
			beat.ObservedAt = targetLapsedAt
			beat.Advertisement.ExpiresAt = targetLapsedAt.Add(time.Minute)
			mustPublish(t, store, beat)
			return page, nil
		})
		result := mustReconcile(t, store, ReconcileHostTargetsRequest{})
		if result.Scanned != 1 || result.Contended != 1 || result.Withdrawn != 0 {
			t.Fatalf("sweep = %+v, want the write refused by the revision the page reported", result)
		}
		if page := mustListCompatible(t, store, ListCompatibleHostsRequest{Key: testHostTargetKey()}); len(page.Hosts) != 1 {
			t.Fatalf("the sweep withdrew a Host that heartbeated under it: %+v", page)
		}
	})
}

// TestReconcileHostTargetsCountsAPostWriteReplyFailure covers the outcomes that
// happen AFTER the compare-and-swap has already committed: the provider's reply
// does not describe what this package wrote.
//
// The withdrawal is durable at that point, so the row is handled — but the
// sweep cannot say so, and it must not end the pass either. Ending it would
// leave the documented accounting false (a row scanned and attributed to
// nothing) and would let one misbehaving reply do to the sweep exactly what one
// unreadable row must not do to a placement page.
//
// The two cases are the two families the reply check produces, and the second
// is here because the first was not enough: the arm was originally written for
// the identity failures alone, so a reply whose VALUE could not be decoded
// still ended the pass. That is the same defect twice — a fix scoped to the
// path a test happened to drive — which is why hostTargetWriteOutcome is now
// exhaustive over the code set rather than a list of the codes anyone thought
// of. See TestHostTargetWriteOutcomeClassifiesEveryCode.
func TestReconcileHostTargetsCountsAPostWriteReplyFailure(t *testing.T) {
	t.Parallel()

	tests := map[string]func(storage.OrderedRecord) storage.OrderedRecord{
		"a reply whose view state is not what was filed": func(record storage.OrderedRecord) storage.OrderedRecord {
			record.Rank = storage.Rank{Ranked: true, Value: 99}
			return record
		},
		"a reply carrying a record this build cannot decode": func(record storage.OrderedRecord) storage.OrderedRecord {
			record.Value = []byte(`{"record_version":2,"agent_id":"agent-a"}`)
			return record
		},
		"a reply carrying a record that fails its own rules": func(record storage.OrderedRecord) storage.OrderedRecord {
			record.Value = bytes.Replace(record.Value, []byte(`"placement":"pooled"`), []byte(`"placement":"nonsense"`), 1)
			return record
		},
	}

	for name, rewrite := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			base := memstore.New()
			hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
			base.OrderedIndex = hostile
			store, clock := hostTargetFixture(t, base)
			publishCapacity(t, store, "host-a", 1, targetExpiresAt)
			publishCapacity(t, store, "host-b", 1, targetExpiresAt)
			clock.set(targetLapsedAt)

			// Only the first row's reply is rewritten, so the second proves the
			// sweep carried on rather than ending the pass.
			hostile.refileUpdates(func(record storage.OrderedRecord) storage.OrderedRecord {
				if record.ID.StableKey != "host-a" {
					return record
				}
				return rewrite(record)
			})
			result := mustReconcile(t, store, ReconcileHostTargetsRequest{})
			if result.Scanned != 2 || result.Unverified != 1 || result.Withdrawn != 1 {
				t.Fatalf("sweep = %+v, want one unverified reply and the other row withdrawn", result)
			}
			if !result.Exhausted {
				t.Fatalf("sweep = %+v, want the view exhausted", result)
			}

			// The withdrawal really did commit, which is why it is its own
			// outcome rather than a contention: nothing will revisit this row.
			stored := storedHostTargetRow(t, store, testHostTargetKey(), "host-a")
			if stored.Due != (storage.Due{}) || stored.Rank != (storage.Rank{}) {
				t.Fatalf("the withdrawal did not commit: rank %+v due %+v", stored.Rank, stored.Due)
			}
		})
	}
}

// TestHostTargetWriteOutcomeClassifiesEveryCode is the guard that makes the
// arms above exhaustive rather than a list of the failures someone remembered.
//
// The sweep's write classifier decides between three things: the write did not
// happen, the write happened and its reply cannot be vouched for, and the
// failure says nothing about this row at all. Getting a code into the wrong one
// is not cosmetic — a committed withdrawal counted as a contention makes the
// sweep understate its work, and a reply failure counted as fatal ends the pass
// and falsifies the accounting HostTargetReconcileResult promises.
//
// The code set is READ FROM SOURCE rather than listed here, for the reason
// orderedRecordMembers is: this arm has now been written twice with a
// hand-picked list and been incomplete both times.
func TestHostTargetWriteOutcomeClassifiesEveryCode(t *testing.T) {
	t.Parallel()

	// Codes a failed sweep write cannot produce, each with the reason it
	// cannot. A code that is neither classified below nor excluded here fails
	// this test until someone decides which it is.
	excluded := map[HostTargetErrorCode]string{
		HostTargetErrorBackend:    "not about this row; continuing would burn the budget failing every row",
		HostTargetErrorUnknown:    "an ambiguous mutation says nothing about whether the withdrawal committed",
		HostTargetErrorCursor:     "a write issues no listing and presents no cursor",
		HostTargetErrorWithdrawn:  "only HostTarget.Report produces it, and the sweep does not project",
		HostTargetErrorGeneration: "only a fence produces it, and the sweep names no generation of its own",
	}

	file, err := parser.ParseFile(token.NewFileSet(), "errors.go", nil, 0)
	if err != nil {
		t.Fatalf("parse errors.go: %v", err)
	}
	codes := map[string]HostTargetErrorCode{}
	for _, declaration := range file.Decls {
		generic, ok := declaration.(*ast.GenDecl)
		if !ok || generic.Tok != token.CONST {
			continue
		}
		for _, spec := range generic.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok || len(value.Names) != 1 || len(value.Values) != 1 {
				continue
			}
			named, ok := value.Type.(*ast.Ident)
			if !ok || named.Name != "HostTargetErrorCode" {
				continue
			}
			literal, ok := value.Values[0].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				continue
			}
			text, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatalf("unquote %s: %v", value.Names[0].Name, err)
			}
			codes[value.Names[0].Name] = HostTargetErrorCode(text)
		}
	}
	if len(codes) < 13 {
		t.Fatalf("found %d codes (%v); the scan is not reaching the declarations", len(codes), codes)
	}

	for name, code := range codes {
		outcome := hostTargetWriteOutcome(code)
		reason, isExcluded := excluded[code]
		switch {
		case isExcluded && outcome != hostTargetSweepFatal:
			t.Errorf("%s is excluded (%s) but is classified as a row outcome", name, reason)
		case !isExcluded && outcome == hostTargetSweepFatal:
			t.Errorf("%s is neither classified as a sweep outcome nor excluded with a reason", name)
		}
	}
}

// TestReconcileHostTargetsPagesPastARowItCannotHandle// TestReconcileHostTargetsPagesPastARowItCannotHandle is the head-of-line
// question asked directly. A row the sweep cannot read stays due, so it is at
// the head of every later ascending due page; the sweep must page PAST it and
// reach the rows behind it rather than spending every pass on the same row.
func TestReconcileHostTargetsPagesPastARowItCannotHandle(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store, clock := hostTargetFixture(t, base)

	// Ascending (due_at, stable_key) puts the two unreadable rows first.
	publishCapacity(t, store, "host-a", 1, targetExpiresAt)
	publishCapacity(t, store, "host-b", 1, targetExpiresAt)
	publishCapacity(t, store, "host-c", 1, targetExpiresAt)
	hostile.answerDue(func(page storage.DuePage, err error) (storage.DuePage, error) {
		if err != nil {
			return page, err
		}
		for i := range page.Records {
			switch page.Records[i].ID.StableKey {
			case "host-a", "host-b":
				page.Records[i].Value = []byte(`{"record_version":1,"nonsense":true}`)
			}
		}
		return page, nil
	})
	clock.set(targetLapsedAt)

	// One page of one row reaches only the first unreadable row.
	blocked := mustReconcile(t, store, ReconcileHostTargetsRequest{Limit: 1, MaxPages: 1})
	if blocked.Scanned != 1 || blocked.Unreadable != 1 || blocked.Exhausted {
		t.Fatalf("sweep = %+v, want one unreadable row and an unfinished sweep", blocked)
	}

	// A sweep with a page budget steps over both and reaches the row behind.
	swept := mustReconcile(t, store, ReconcileHostTargetsRequest{Limit: 1, MaxPages: 8})
	if swept.Unreadable != 2 || swept.Withdrawn != 1 || !swept.Exhausted {
		t.Fatalf("sweep = %+v, want two unreadable rows stepped over and the third withdrawn", swept)
	}
	stored := storedHostTargetRow(t, store, testHostTargetKey(), "host-c")
	if stored.Due != (storage.Due{}) {
		t.Fatalf("the row behind the unreadable ones is still due: %+v", stored.Due)
	}
}

// TestStaleAdvertisementsDoNotPermanentlyFillPlacementPages is step 4 asked
// end to end, and it is the property the whole due half of this record exists
// to provide. Before a sweep, a page of lapsed rows crowds a live Host off the
// first page; after one sweep the live Host is the first thing a Factory sees.
func TestStaleAdvertisementsDoNotPermanentlyFillPlacementPages(t *testing.T) {
	t.Parallel()

	store, clock := hostTargetFixture(t, memstore.New())
	for _, host := range []sessionwire.HostID{"host-a", "host-b", "host-c"} {
		publishCapacity(t, store, host, 100, targetExpiresAt)
	}
	// The live Host advertises less capacity, so it ranks behind all three.
	publishCapacity(t, store, "host-live", 1, targetObservedAt.Add(10*time.Minute))
	clock.set(targetLapsedAt)

	crowded := mustListCompatible(t, store, ListCompatibleHostsRequest{Key: testHostTargetKey(), Limit: 3})
	if len(crowded.Hosts) != 0 || crowded.LapsedSkipped != 3 {
		t.Fatalf("first page = %+v, want three lapsed rows and no candidate", crowded)
	}
	if crowded.NextCursor == "" {
		t.Fatal("a page holding only lapsed rows must still offer a continuation")
	}

	if result := mustReconcile(t, store, ReconcileHostTargetsRequest{}); result.Withdrawn != 3 {
		t.Fatalf("sweep = %+v, want the three crashed Hosts withdrawn", result)
	}

	cleared := mustListCompatible(t, store, ListCompatibleHostsRequest{Key: testHostTargetKey(), Limit: 3})
	if len(cleared.Hosts) != 1 || cleared.Hosts[0].HostID != "host-live" {
		t.Fatalf("first page after the sweep = %+v, want only the live Host", cleared)
	}
	if cleared.LapsedSkipped != 0 || cleared.NextCursor != "" {
		t.Fatalf("page = %+v, want no lapsed rows and no continuation", cleared)
	}
}

// --- the capacity/authority boundary ---------------------------------------

// TestHostTargetsCannotSpellSessionOwnership is the structural half of the
// claim HostTarget's documentation makes. Prose saying "this is capacity, not
// authority" decays; a type with nowhere to write a session identity does not.
//
// It reads the SOURCE rather than a hand-listed set of types, so a type added
// to this record later is covered without anyone remembering to add it, and it
// covers the one foreign type this record publishes as well.
// structFieldSpelling is one field of one struct, rendered the way the guards
// below ask their question of it: the field's TYPE as written in the source,
// followed by its names.
type structFieldSpelling struct {
	TypeName string
	Spelling string
}

// structFieldSpellings renders every field of every struct one file declares
// whose type name include accepts.
//
// It is shared because three record kinds now ask the same structural question
// — can this record SPELL a thing it must never carry — and the walk that
// answers it had been copied twice, byte-identically but for the message. This
// package has made that argument twice already, at checkFiledScope and at
// declaredStoreOperations, and the reason is not tidiness: what a copy is free
// to do is drift, on a path where a weakened check looks exactly like a passing
// one. This walk has more knobs than most. Drop the printer.Fprint and it stops
// seeing typed fields, so an embedded LeaseEpoch would pass; drop the
// field.Names loop and it stops seeing names, so a `Epoch uint64` would. Each
// caller keeps what is genuinely its own — its forbidden list, its message, and
// its own floor on how many fields a non-vacuous walk must reach.
func structFieldSpellings(t *testing.T, filename string, include func(string) bool) []structFieldSpelling {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", filename, err)
	}
	var fields []structFieldSpelling
	for _, declaration := range file.Decls {
		generic, ok := declaration.(*ast.GenDecl)
		if !ok || generic.Tok != token.TYPE {
			continue
		}
		for _, spec := range generic.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || !include(typeSpec.Name.Name) {
				continue
			}
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				continue
			}
			for _, field := range structType.Fields.List {
				var rendered bytes.Buffer
				if err := printer.Fprint(&rendered, token.NewFileSet(), field.Type); err != nil {
					t.Fatalf("render %s: %v", typeSpec.Name.Name, err)
				}
				spelling := rendered.String()
				for _, name := range field.Names {
					spelling += " " + name.Name
				}
				fields = append(fields, structFieldSpelling{TypeName: typeSpec.Name.Name, Spelling: spelling})
			}
		}
	}
	return fields
}

// TestStructFieldSpellingsRendersTypesAndNames guards the shared walk itself,
// which sharing alone does not.
//
// Hoisting three copies into one place removes the drift between them and
// leaves the machinery unguarded: dropping the printer.Fprint stops the walk
// seeing typed fields, and dropping the field.Names loop stops it seeing names.
// Both mutations survived all three ownership tests, because each of those asks
// only whether a FORBIDDEN word appears — a walk that renders nothing at all
// reports no forbidden word and passes. So the property is asserted positively
// here, once, on behalf of every caller: an embedded LeaseEpoch is caught by
// the type half, and a `Epoch uint64` by the name half.
func TestStructFieldSpellingsRendersTypesAndNames(t *testing.T) {
	t.Parallel()

	spelling := func(fields []structFieldSpelling, want string) bool {
		for _, field := range fields {
			if field.Spelling == want {
				return true
			}
		}
		return false
	}

	// A NAMED field renders as its source type followed by its name, so a guard
	// looking for either half finds it.
	claim := structFieldSpellings(t, "reconcile.go", func(name string) bool { return name == "ReconciliationClaim" })
	if len(claim) == 0 {
		t.Fatal("the walk found no fields on ReconciliationClaim")
	}
	if !spelling(claim, "string HolderID") {
		t.Errorf("ReconciliationClaim fields = %+v, want one spelled %q", claim, "string HolderID")
	}
	if !spelling(claim, "sessionwire.TenantID TenantID") {
		t.Errorf("ReconciliationClaim fields = %+v, want a qualified type rendered with its name", claim)
	}

	// An EMBEDDED field has no name and renders as its type alone, which is the
	// case a name-only walk would drop entirely.
	page := structFieldSpellings(t, "catalog.go", func(name string) bool { return name == "SessionPage" })
	if !spelling(page, "sessionwire.SessionPage") {
		t.Errorf("SessionPage fields = %+v, want the embedded type rendered on its own", page)
	}
}

func TestHostTargetsCannotSpellSessionOwnership(t *testing.T) {
	t.Parallel()

	forbidden := []string{"TenantID", "SessionID", "LeaseEpoch"}
	// Every type in the record's own file, plus its error type wherever the
	// package's single error home puts it.
	fields := structFieldSpellings(t, "host_targets.go", func(string) bool { return true })
	fields = append(fields, structFieldSpellings(t, "errors.go",
		func(name string) bool { return strings.HasPrefix(name, "HostTarget") })...)
	for _, field := range fields {
		for _, word := range forbidden {
			if strings.Contains(field.Spelling, word) {
				t.Errorf("%s.%s names %s; this record is capacity, and capacity is never authority",
					field.TypeName, field.Spelling, word)
			}
		}
	}
	if len(fields) < 20 {
		t.Fatalf("only %d fields were inspected; the walk is not reaching the declarations", len(fields))
	}

	// The projection a placement page publishes is core's, so the same question
	// is asked of it by reflection rather than assumed from the file above.
	report := reflect.TypeOf(sessionwire.HostLinkCapacityReport{})
	for i := range report.NumField() {
		field := report.Field(i)
		for _, word := range forbidden {
			if strings.Contains(field.Name, word) || strings.Contains(field.Type.Name(), word) {
				t.Errorf("HostLinkCapacityReport.%s names %s; a placement page must not be able to carry it",
					field.Name, word)
			}
		}
	}
}

// --- lifecycle and request validation --------------------------------------

// TestHostTargetOperationsValidateBeforeAdmission holds every public directory
// operation to reporting the CALLER's mistake rather than the store's state.
// The order — validate, then admit, then bind, then the provider — is the
// package's, and an operation added without a row here silently stops obeying
// it.
func TestHostTargetOperationsValidateBeforeAdmission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		call      func(*Store) error
		assert    func(*testing.T, error)
	}{
		{
			name:      "publish for a target that names no agent",
			operation: "PublishHostTarget",
			call: func(store *Store) error {
				req := testPublishRequest(targetGeneration)
				req.Key.AgentID = ""
				_, err := store.PublishHostTarget(context.Background(), req)
				return err
			},
			assert: assertInvalidIdentity("AgentID"),
		},
		{
			name:      "publish an endpoint carrying credentials",
			operation: "PublishHostTarget",
			call: func(store *Store) error {
				req := testPublishRequest(targetGeneration)
				req.Advertisement.InternalEndpoint = "wss://user:pass@host-a.internal/hostlink"
				_, err := store.PublishHostTarget(context.Background(), req)
				return err
			},
			assert: assertHostTargetField(HostTargetErrorInvalid, "internal_endpoint"),
		},
		{
			name:      "publish more free capacity than the rank can carry",
			operation: "PublishHostTarget",
			call: func(store *Store) error {
				req := testPublishRequest(targetGeneration)
				req.Advertisement.AvailableCapacity = MaxHostTargetAvailableCapacity + 1
				_, err := store.PublishHostTarget(context.Background(), req)
				return err
			},
			assert: assertHostTargetField(HostTargetErrorInvalid, "available_capacity"),
		},
		{
			name:      "drain without naming an incarnation",
			operation: "DrainHostTarget",
			call: func(store *Store) error {
				_, err := store.DrainHostTarget(context.Background(), testDrainRequest(0))
				return err
			},
			assert: assertHostTargetField(HostTargetErrorInvalid, "host_generation"),
		},
		{
			name:      "list with a page larger than any provider serves",
			operation: "ListCompatibleHosts",
			call: func(store *Store) error {
				_, err := store.ListCompatibleHosts(context.Background(), ListCompatibleHostsRequest{
					Key: testHostTargetKey(), Limit: storage.MaxOrderedPageLimit + 1})
				return err
			},
			assert: assertHostTargetField(HostTargetErrorInvalid, "limit"),
		},
		{
			name:      "sweep with a page larger than any provider serves",
			operation: "ReconcileHostTargets",
			call: func(store *Store) error {
				_, err := store.ReconcileHostTargets(context.Background(), ReconcileHostTargetsRequest{
					Limit: storage.MaxOrderedPageLimit + 1})
				return err
			},
			assert: assertHostTargetField(HostTargetErrorInvalid, "limit"),
		},
		{
			name:      "sweep with an unbounded page budget",
			operation: "ReconcileHostTargets",
			call: func(store *Store) error {
				_, err := store.ReconcileHostTargets(context.Background(), ReconcileHostTargetsRequest{
					MaxPages: MaxHostTargetReconcilePages + 1})
				return err
			},
			assert: assertHostTargetField(HostTargetErrorInvalid, "max_pages"),
		},
	}

	declared := declaredStoreOperations(t, "host_targets.go")
	covered := map[string]bool{}
	for _, test := range tests {
		if !declared[test.operation] {
			t.Errorf("the case %q drives %s, which host_targets.go does not declare", test.name, test.operation)
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
			store, _ := hostTargetFixture(t, base)
			test.assert(t, test.call(store))
			if calls := recorder.snapshot(); len(calls) != 0 {
				t.Fatalf("a refused request reached the provider: %+v", calls)
			}

			// The same request against a store that is closing.
			closing, err := Open(context.Background(), memstore.New(), WithClock(newMovableClock(targetObservedAt)))
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

func assertHostTargetField(code HostTargetErrorCode, field string) func(*testing.T, error) {
	return func(t *testing.T, err error) {
		t.Helper()
		got := assertHostTargetCode(t, err, code)
		if got.Field != field {
			t.Fatalf("field = %q, want %q (%v)", got.Field, field, err)
		}
	}
}

func TestHostTargetOperationsRefuseAfterClose(t *testing.T) {
	store, _ := hostTargetFixture(t, memstore.New())
	mustPublish(t, store, testPublishRequest(targetGeneration))
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	operations := map[string]func() error{
		"PublishHostTarget": func() error {
			_, err := store.PublishHostTarget(context.Background(), testPublishRequest(targetGeneration))
			return err
		},
		"DrainHostTarget": func() error {
			_, err := store.DrainHostTarget(context.Background(), testDrainRequest(targetGeneration))
			return err
		},
		"ListCompatibleHosts": func() error {
			_, err := store.ListCompatibleHosts(context.Background(), ListCompatibleHostsRequest{Key: testHostTargetKey()})
			return err
		},
		"ReconcileHostTargets": func() error {
			_, err := store.ReconcileHostTargets(context.Background(), ReconcileHostTargetsRequest{})
			return err
		},
	}

	declared := declaredStoreOperations(t, "host_targets.go")
	for name := range declared {
		if operations[name] == nil {
			t.Errorf("host_targets.go declares the public operation %s and this test does not exercise it", name)
		}
	}
	for name := range operations {
		if !declared[name] {
			t.Errorf("this test exercises %s, which host_targets.go no longer declares (was it moved?)", name)
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

// TestPublishHostTargetBoundsTheHeartbeatPromise pins the caller-supplied TTL
// at the entry point. A Host's next-heartbeat promise is the only thing keeping
// a crashed process out of a placement page, so an unbounded one is a durable
// fault a single skewed clock reading can commit alone.
func TestPublishHostTargetBoundsTheHeartbeatPromise(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expires time.Time
		wantErr bool
	}{
		{name: "already lapsed", expires: targetObservedAt.Add(-time.Second), wantErr: true},
		{name: "at the store's own instant", expires: targetObservedAt, wantErr: true},
		{name: "one nanosecond ahead", expires: targetObservedAt.Add(time.Nanosecond)},
		{name: "at the ceiling", expires: targetObservedAt.Add(MaxHostTargetTTL)},
		{name: "one nanosecond past the ceiling", expires: targetObservedAt.Add(MaxHostTargetTTL + time.Nanosecond), wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store, _ := hostTargetFixture(t, memstore.New())
			req := testPublishRequest(targetGeneration)
			req.Advertisement.ExpiresAt = test.expires
			_, err := store.PublishHostTarget(context.Background(), req)
			if !test.wantErr {
				if err != nil {
					t.Fatalf("PublishHostTarget: %v", err)
				}
				return
			}
			assertHostTargetField(HostTargetErrorInvalid, "expires_at")(t, err)
		})
	}
}

// --- what the provider says about its own filing ---------------------------

// TestHostTargetFilingIsHeldToTheRecord drives every component of a stored
// row's filing that this package checks. The rows come from a real write, so
// each case perturbs exactly one component of something a conforming provider
// produced.
func TestHostTargetFilingIsHeldToTheRecord(t *testing.T) {
	t.Parallel()

	store, _ := hostTargetFixture(t, memstore.New())
	mustPublish(t, store, testPublishRequest(targetGeneration))
	scope, err := store.deriveHostTargetScope(testHostTargetKey())
	if err != nil {
		t.Fatalf("deriveHostTargetScope: %v", err)
	}
	conforming := storedHostTargetRow(t, store, testHostTargetKey(), targetHost)
	otherHost, _, err := encodeHostTarget(func() HostTarget {
		record := testHostTarget()
		record.HostID = targetOtherHost
		record.Advertisement.InternalEndpoint = targetOtherEndpoint
		return record
	}())
	if err != nil {
		t.Fatalf("encodeHostTarget: %v", err)
	}
	// Identical in every member the filing checks compare EXCEPT the target, so
	// this row's rank and due agree with the conforming one and only the check
	// holding a record to the target the caller asked for can refuse it.
	otherTarget, _, err := encodeHostTarget(func() HostTarget {
		record := testHostTarget()
		record.Key.RuntimeCompatibilityID = "runtime-v2"
		return record
	}())
	if err != nil {
		t.Fatalf("encodeHostTarget: %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*storage.OrderedRecord)
		want    HostTargetErrorCode
		field   string
		covered string
	}{
		{
			name:    "a provider tombstone, which retires this Host's identity forever",
			mutate:  func(r *storage.OrderedRecord) { r.Deleted = true },
			want:    HostTargetErrorDeleted,
			field:   "record",
			covered: "Deleted",
		},
		{
			name:    "another Host's advertisement under this key",
			mutate:  func(r *storage.OrderedRecord) { r.Value = otherHost },
			want:    HostTargetErrorIdentity,
			field:   "record",
			covered: "Value",
		},
		{
			name:    "this Host's advertisement for a DIFFERENT target under this key",
			mutate:  func(r *storage.OrderedRecord) { r.Value = otherTarget },
			want:    HostTargetErrorIdentity,
			field:   "record",
			covered: "Value",
		},
		{
			name:    "filed under a stable key that is not the Host",
			mutate:  func(r *storage.OrderedRecord) { r.ID.StableKey = storage.StableKey(targetOtherHost) },
			want:    HostTargetErrorIdentity,
			field:   "host_id",
			covered: "ID.StableKey",
		},
		{
			name:    "filed in another target's ordering scope",
			mutate:  func(r *storage.OrderedRecord) { r.ID.OrderingScope += "/elsewhere" },
			want:    HostTargetErrorIdentity,
			field:   "ordering_scope",
			covered: "ID.OrderingScope",
		},
		{
			name:    "ranked in another target's scope",
			mutate:  func(r *storage.OrderedRecord) { r.RankingScope += "/elsewhere" },
			want:    HostTargetErrorIdentity,
			field:   "ranking_scope",
			covered: "RankingScope",
		},
		{
			// The crashed-Host-is-invisible case: an advertisement filed NOT
			// DUE would never reach the reconciler, so nothing would ever
			// remove it from the placement view. It differs from what this
			// record files only in the due STATE.
			name:    "filed outside the deadline page nothing would then sweep",
			mutate:  func(r *storage.OrderedRecord) { r.Due = storage.Due{} },
			want:    HostTargetErrorIdentity,
			field:   "due",
			covered: "Due",
		},
		{
			// And the same comparison in the other direction: due at the right
			// STATE and the wrong instant, which a state-only comparison would
			// accept and which would sweep a live Host early or never.
			name:    "due at an instant that is not the Host's promise",
			mutate:  func(r *storage.OrderedRecord) { r.Due.UnixMillis-- },
			want:    HostTargetErrorIdentity,
			field:   "due",
			covered: "Due",
		},
		{
			name:    "ranked above the capacity it actually reported",
			mutate:  func(r *storage.OrderedRecord) { r.Rank.Value += 100 },
			want:    HostTargetErrorIdentity,
			field:   "rank",
			covered: "Rank",
		},
		{
			name:    "unranked while still advertising",
			mutate:  func(r *storage.OrderedRecord) { r.Rank = storage.Rank{} },
			want:    HostTargetErrorIdentity,
			field:   "rank",
			covered: "Rank",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			stored := conforming
			test.mutate(&stored)
			_, err := hostTargetEntryFor(stored, scope.TargetScope, testHostTargetKey(), targetHost)
			got := assertHostTargetCode(t, err, test.want)
			if got.Field != test.field {
				t.Fatalf("field = %q, want %q (%v)", got.Field, test.field, err)
			}
		})
	}

	// The unperturbed row must pass, or every case above could be passing for a
	// reason that has nothing to do with the component it perturbs.
	if _, err := hostTargetEntryFor(conforming, scope.TargetScope, testHostTargetKey(), targetHost); err != nil {
		t.Fatalf("a conforming row was rejected: %v", err)
	}

	// The cross product, rather than a hand-written list standing in for one:
	// every member of the provider's record is either perturbed by a case above
	// or named here as deliberately unchecked, with the reason on
	// hostTargetEntryFor. A member Storage adds later belongs to one of the two
	// groups and this fails until someone decides which.
	excluded := map[string]string{
		"Revision":     "provider state with no counterpart in the record",
		"Order":        "not exposed, and neither view here is in acceptance order",
		"ID.Namespace": "a package constant with no counterpart in the record",
	}
	perturbed := map[string]bool{}
	for _, test := range tests {
		perturbed[test.covered] = true
	}
	for _, member := range orderedRecordMembers(t) {
		if perturbed[member] == (excluded[member] != "") {
			t.Errorf("storage.OrderedRecord.%s is %s; it must be exactly one of perturbed here or excluded with a reason",
				member, map[bool]string{true: "both perturbed and excluded", false: "neither perturbed nor excluded"}[perturbed[member]])
		}
	}
}

// TestPublishHostTargetChecksTheProvidersReply reaches the filing checks
// through a real write, which is the only way to prove they are WIRED into the
// write path rather than merely correct in isolation.
func TestPublishHostTargetChecksTheProvidersReply(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store, _ := hostTargetFixture(t, base)

	hostile.refileCreates(func(record storage.OrderedRecord) storage.OrderedRecord {
		record.Rank.Value += 100
		return record
	})
	_, err := store.PublishHostTarget(context.Background(), testPublishRequest(targetGeneration))
	got := assertHostTargetCode(t, err, HostTargetErrorIdentity)
	if got.Field != "rank" {
		t.Fatalf("field = %q, want %q", got.Field, "rank")
	}
}

// TestPublishHostTargetRefusesAReplyThatIsNotTheBytesItWrote covers the one
// claim a write makes that no record-derived check can test: that the provider
// stored THESE bytes. A substituted record satisfies every identity check and
// would be returned to a Host as its own successful advertisement.
func TestPublishHostTargetRefusesAReplyThatIsNotTheBytesItWrote(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store, _ := hostTargetFixture(t, base)

	substitute, _, err := encodeHostTarget(func() HostTarget {
		record := testHostTarget()
		record.Advertisement.InternalEndpoint = targetOtherEndpoint
		return record
	}())
	if err != nil {
		t.Fatalf("encodeHostTarget: %v", err)
	}
	hostile.refileCreates(func(record storage.OrderedRecord) storage.OrderedRecord {
		record.Value = substitute
		return record
	})
	_, err = store.PublishHostTarget(context.Background(), testPublishRequest(targetGeneration))
	got := assertHostTargetCode(t, err, HostTargetErrorIdentity)
	if got.Field != "value" {
		t.Fatalf("field = %q, want %q", got.Field, "value")
	}
}

// TestHostTargetClassifiesProviderFailures pins each arm of the classifier to
// the provider outcome it stands for, including the cursor arm the registry's
// classifier has no need of.
func TestHostTargetClassifiesProviderFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want HostTargetErrorCode
	}{
		{name: "absent", err: &storage.OrderedRecordNotFoundError{}, want: HostTargetErrorNotFound},
		{name: "tombstoned", err: &storage.OrderedDeletedError{}, want: HostTargetErrorDeleted},
		{name: "lost the revision", err: &storage.OrderedRevisionConflictError{ActualRevision: 7}, want: HostTargetErrorConflict},
		{name: "cursor", err: &storage.InvalidOrderedCursorError{}, want: HostTargetErrorCursor},
		{name: "ambiguous", err: &storage.OrderedAmbiguousError{}, want: HostTargetErrorUnknown},
		{name: "anything else", err: errors.New("provider exploded"), want: HostTargetErrorBackend},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := classifyHostTargetOrderedError(test.err, "stage")
			got := assertHostTargetCode(t, err, test.want)
			if !errors.Is(err, test.err) {
				t.Fatalf("the cause was not preserved: %v", err)
			}
			if test.want == HostTargetErrorConflict && got.Revision != 7 {
				t.Fatalf("revision = %d, want 7", got.Revision)
			}
			if strings.Contains(got.Error(), "provider exploded") {
				t.Fatalf("the message leaked provider text: %q", got.Error())
			}
		})
	}
}

// TestDecodeHostTargetFailsClosed pins that a stored row this reader cannot
// vouch for is refused rather than guessed at. A row that decoded loosely would
// be ranked into a placement page on the strength of whatever survived.
func TestDecodeHostTargetFailsClosed(t *testing.T) {
	t.Parallel()

	live, _, err := encodeHostTarget(testHostTarget())
	if err != nil {
		t.Fatalf("encodeHostTarget: %v", err)
	}

	tests := []struct {
		name  string
		value []byte
		want  HostTargetErrorCode
	}{
		{name: "empty", value: nil, want: HostTargetErrorMalformed},
		{name: "not json", value: []byte("{"), want: HostTargetErrorMalformed},
		{name: "oversized", value: append(bytes.Repeat([]byte(" "), MaxHostTargetRecordBytes), live...), want: HostTargetErrorTooLarge},
		{name: "a version this reader does not know", value: []byte(`{"record_version":2}`), want: HostTargetErrorVersion},
		{
			name:  "a member this reader does not know",
			value: []byte(`{"record_version":1,"surprise":true}`),
			want:  HostTargetErrorMalformed,
		},
		{
			name:  "an advertisement member this reader does not know",
			value: bytes.Replace(live, []byte(`"accepting"`), []byte(`"surprise":1,"accepting"`), 1),
			want:  HostTargetErrorMalformed,
		},
		{
			name:  "a row whose capacity the rank could not carry",
			value: bytes.Replace(live, []byte(`"available_capacity":4`), []byte(`"available_capacity":18446744073709551615`), 1),
			want:  HostTargetErrorInvalid,
		},
		{
			// The instant bounds are this package's own on both paths, and the
			// decode path is where they are the ONLY guard: on a publish an
			// out-of-range expiry is also outside MaxHostTargetTTL, so the
			// bounded-expiry check would mask this one.
			//
			// The observation is moved EARLIER rather than later, and that is
			// what makes the case pin what it claims to. A far-future
			// observation is refused by Core's rule that the expiry must fall
			// after it, so this case passed with the instant bound deleted; an
			// observation before the representable range leaves Core's ordering
			// satisfied and rankableTime as the only thing that can refuse it.
			name:  "an observation whose UnixNano is undefined",
			value: bytes.Replace(live, []byte(`"2026-08-30T14:00:00Z"`), []byte(`"1000-01-01T00:00:00Z"`), 1),
			want:  HostTargetErrorInvalid,
		},
		{
			name:  "a promise whose UnixMilli would be undefined",
			value: bytes.Replace(live, []byte(`"2026-08-30T14:01:00Z"`), []byte(`"3000-01-01T00:00:00Z"`), 1),
			want:  HostTargetErrorInvalid,
		},
		{
			name:  "a dedicated row claiming more than its one seat",
			value: bytes.Replace(live, []byte(`"placement":"pooled"`), []byte(`"placement":"dedicated"`), 1),
			want:  HostTargetErrorInvalid,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := decodeHostTarget(test.value)
			assertHostTargetCode(t, err, test.want)
		})
	}
}

// TestHostTargetCanonicalizesItsInstantsToUTC pins that a Host reporting in a
// local zone stores the same bytes as one reporting in UTC, which is what makes
// the codec's fixed point a fixed point at all.
func TestHostTargetCanonicalizesItsInstantsToUTC(t *testing.T) {
	t.Parallel()

	zone := time.FixedZone("elsewhere", 5*3600)
	record := testHostTarget()
	record.ObservedAt = record.ObservedAt.In(zone)
	record.Advertisement.ExpiresAt = record.Advertisement.ExpiresAt.In(zone)

	shifted, canonical, err := encodeHostTarget(record)
	if err != nil {
		t.Fatalf("encodeHostTarget: %v", err)
	}
	plain, _, err := encodeHostTarget(testHostTarget())
	if err != nil {
		t.Fatalf("encodeHostTarget: %v", err)
	}
	if !bytes.Equal(shifted, plain) {
		t.Fatalf("a zone changed the stored bytes:\n%s\n%s", shifted, plain)
	}
	if canonical.ObservedAt.Location() != time.UTC || canonical.Advertisement.ExpiresAt.Location() != time.UTC {
		t.Fatalf("canonical instants are not UTC: %+v", canonical)
	}
}

// TestCanonicalHostTargetDoesNotMutateItsArgument pins that canonicalization
// leaves the caller's advertisement alone. It relabels instants to UTC, and
// doing that through the pointer it was handed would silently rewrite a record
// its caller still holds — a Host reusing one advertisement value across
// heartbeats would find this package editing it.
//
// The zone is what makes the mutation observable: a relabelled instant is the
// same instant, so only the location distinguishes the two.
func TestCanonicalHostTargetDoesNotMutateItsArgument(t *testing.T) {
	t.Parallel()

	record := testHostTarget()
	record.Advertisement.ExpiresAt = record.Advertisement.ExpiresAt.In(time.FixedZone("elsewhere", 3600))
	before := *record.Advertisement

	canonical, err := canonicalHostTarget(record)
	if err != nil {
		t.Fatalf("canonicalHostTarget: %v", err)
	}
	if *record.Advertisement != before {
		t.Fatalf("canonicalization edited the caller's advertisement: %+v, was %+v", *record.Advertisement, before)
	}
	if canonical.Advertisement == record.Advertisement {
		t.Fatal("the canonical record shares the caller's advertisement")
	}
	if canonical.Advertisement.ExpiresAt.Location() != time.UTC {
		t.Fatalf("the canonical expiry is not UTC: %s", canonical.Advertisement.ExpiresAt)
	}
}

// TestConcurrentHostTargetHeartbeatsAgree runs the race a restarting Host makes
// against itself. Every writer either wins or is refused for a reason that
// names why, and the row that survives is decodable and holds the highest
// generation any winner named.
func TestConcurrentHostTargetHeartbeatsAgree(t *testing.T) {
	t.Parallel()

	store, _ := hostTargetFixture(t, memstore.New())
	const writers = 8

	var wait sync.WaitGroup
	won := make([]uint64, writers)
	for i := range writers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			generation := targetGeneration + uint64(i)
			req := testPublishRequest(generation)
			req.Advertisement.AvailableCapacity = uint64(i)
			if _, err := store.PublishHostTarget(context.Background(), req); err != nil {
				var failure *HostTargetError
				if !errors.As(err, &failure) {
					t.Errorf("PublishHostTarget: %T %v", err, err)
					return
				}
				switch failure.Code {
				case HostTargetErrorGeneration, HostTargetErrorConflict:
				default:
					t.Errorf("a losing writer reported %q: %v", failure.Code, err)
				}
				return
			}
			won[i] = generation
		}()
	}
	wait.Wait()

	stored := storedHostTargetRow(t, store, testHostTargetKey(), targetHost)
	record, err := decodeHostTarget(stored.Value)
	if err != nil {
		t.Fatalf("decodeHostTarget: %v", err)
	}
	var highest uint64
	for _, generation := range won {
		if generation > highest {
			highest = generation
		}
	}
	if record.HostGeneration != highest {
		t.Fatalf("stored generation = %d, want the highest winner %d", record.HostGeneration, highest)
	}
	if stored.Rank != hostTargetRank(record) || stored.Due != hostTargetDue(record) {
		t.Fatalf("the surviving row's views do not match its bytes: %+v %+v", stored.Rank, stored.Due)
	}
}

// --- derived names and their witnesses ------------------------------------

// TestHostTargetWritesBindTheTargetsWitness pins the one thing a derived scope
// name cannot prove about itself. Two different targets whose digests collide
// would otherwise share one ranking scope silently; the witness is what turns
// that into a refusal at the moment the second name is created.
//
// The collision is forced by replacing the store's digest with a constant,
// which is the same instrument the keyspace's own collision tests use: a real
// SHA-256 collision cannot be exhibited, and a test that only asserted the
// witness key's spelling would pass with the check deleted.
func TestHostTargetWritesBindTheTargetsWitness(t *testing.T) {
	t.Parallel()

	store, _ := hostTargetFixture(t, memstore.New())
	store.keys.digest = func([]byte) [32]byte { return [32]byte{1} }

	mustPublish(t, store, testPublishRequest(targetGeneration))

	// A DIFFERENT target that now derives the same scope.
	collided := testPublishRequest(targetGeneration)
	collided.Key.RuntimeCompatibilityID = "runtime-v2"
	_, err := store.PublishHostTarget(context.Background(), collided)
	assertKeyspaceCode(t, err, KeyspaceHashCollision)
}

// advancingClock moves forward by one millisecond on every reading. It is the
// only instrument that makes the sweep's ONE clock reading observable: a
// re-read bound would be a different due query, and the ordered index binds a
// due cursor to the exact bound that issued it, so the second page would be
// refused outright.
type advancingClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *advancingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(time.Millisecond)
	return c.now
}

// --- provenance of the instants each writer stores --------------------------

// TestHostTargetInstantsComeFromTheRightClock pins WHICH clock each of the
// three writers records, which is invisible while every fixture puts the Host
// and the store at the same instant. A Host's own observation is the Host's to
// report; a withdrawal is not an observation of anything and records that THIS
// STORE wrote it.
func TestHostTargetInstantsComeFromTheRightClock(t *testing.T) {
	t.Parallel()

	observed := targetObservedAt.Add(-10 * time.Second)

	t.Run("a publish stores the Host's own observation", func(t *testing.T) {
		t.Parallel()

		store, _ := hostTargetFixture(t, memstore.New())
		req := testPublishRequest(targetGeneration)
		req.ObservedAt = observed
		entry := mustPublish(t, store, req)
		if !entry.Target.ObservedAt.Equal(observed) {
			t.Fatalf("observed_at = %s, want the Host's %s (not the store's %s)",
				entry.Target.ObservedAt, observed, targetObservedAt)
		}
	})

	t.Run("a drain stores the store's own instant", func(t *testing.T) {
		t.Parallel()

		store, clock := hostTargetFixture(t, memstore.New())
		req := testPublishRequest(targetGeneration)
		req.ObservedAt = observed
		mustPublish(t, store, req)

		drained := targetObservedAt.Add(20 * time.Second)
		clock.set(drained)
		entry := mustDrain(t, store, testDrainRequest(targetGeneration))
		if !entry.Target.ObservedAt.Equal(drained) {
			t.Fatalf("observed_at = %s, want the store's %s (not the Host's %s)",
				entry.Target.ObservedAt, drained, observed)
		}
	})

	t.Run("a sweep stores the instant it swept at", func(t *testing.T) {
		t.Parallel()

		store, clock := hostTargetFixture(t, memstore.New())
		req := testPublishRequest(targetGeneration)
		req.ObservedAt = observed
		mustPublish(t, store, req)

		clock.set(targetLapsedAt)
		mustReconcile(t, store, ReconcileHostTargetsRequest{})
		stored := storedHostTargetRow(t, store, testHostTargetKey(), targetHost)
		record, err := decodeHostTarget(stored.Value)
		if err != nil {
			t.Fatalf("decodeHostTarget: %v", err)
		}
		if !record.ObservedAt.Equal(targetLapsedAt) {
			t.Fatalf("observed_at = %s, want the sweep's %s (not the crashed Host's %s)",
				record.ObservedAt, targetLapsedAt, observed)
		}
	})
}

// TestLargestAcceptableHostTargetFitsTheBound is why encodeHostTarget's size
// refusal is a RELATIONSHIP rather than a live branch. Every member of this
// record is bounded by Core's identity ceiling, so the largest record the
// validators accept is a small multiple of it and the encode-path refusal
// cannot be reached with an input this package would otherwise store. What the
// bound must guarantee is that a record this package accepts can always be
// REWRITTEN — and this row is rewritten on every heartbeat, so a record that
// could be created and not updated would freeze at whatever it last reported.
func TestLargestAcceptableHostTargetFitsTheBound(t *testing.T) {
	t.Parallel()

	largest := HostTarget{
		Key: HostTargetKey{
			AgentID:                sessionwire.AgentID(worstCaseIdentity()),
			RuntimeCompatibilityID: worstCaseIdentity(),
			Placement:              sessionwire.HostPlacementPooled,
		},
		HostID:         sessionwire.HostID(worstCaseIdentity()),
		HostGeneration: math.MaxUint64,
		ObservedAt:     targetObservedAt,
		Advertisement: &HostAdvertisement{
			InternalEndpoint:  worstCaseEndpoint(),
			IsolationClass:    sessionwire.HostIsolationClassTenantExclusive,
			Accepting:         true,
			AvailableCapacity: MaxHostTargetAvailableCapacity,
			ExpiresAt:         targetExpiresAt,
		},
	}
	encoded, _, err := encodeHostTarget(largest)
	if err != nil {
		t.Fatalf("the largest acceptable target does not encode: %v", err)
	}
	if len(encoded) >= MaxHostTargetRecordBytes {
		t.Fatalf("the largest acceptable target is %d bytes, at or above the %d-byte bound",
			len(encoded), MaxHostTargetRecordBytes)
	}
	t.Logf("largest acceptable target = %d bytes against a %d-byte bound", len(encoded), MaxHostTargetRecordBytes)
	if _, err := decodeHostTarget(encoded); err != nil {
		t.Fatalf("the largest acceptable target does not decode: %v", err)
	}
}

// worstCaseIdentity returns the LARGEST-ENCODING value the identity validators
// accept, which is not the longest one.
//
// Every identity here is any valid UTF-8 of at most sessionwire.MaxIDBytes
// bytes, and both AgentID.Validate and validateOpaque accept control
// characters. Go's JSON encoder writes each of those as \u00XX — six bytes for
// one — so a fixture filled with "x" measures a sixth of the real worst case.
// That is not a detail: an ASCII fixture asserted 1358 < 8192 while claiming
// six times the headroom it had, and the registration's equivalent asserted a
// bound its true worst case exceeded by 95 bytes.
func worstCaseIdentity() string {
	return strings.Repeat("\x01", sessionwire.MaxIDBytes)
}

// TestWorstCaseIdentityIsTheLargestEncodingOne pins the fixture the size tests
// are built from, because those tests cannot pin it themselves: a SMALLER
// fixture satisfies "the largest acceptable record fits the bound" just as well
// as the right one, so an ASCII fixture passes while measuring a sixth of what
// it claims to. What makes the fixture correct is a property of the VALUE, so
// that is what is asserted here.
func TestWorstCaseIdentityIsTheLargestEncodingOne(t *testing.T) {
	t.Parallel()

	identity := worstCaseIdentity()
	if len(identity) != sessionwire.MaxIDBytes {
		t.Fatalf("identity is %d bytes, want the ceiling %d", len(identity), sessionwire.MaxIDBytes)
	}
	// The validators must accept it, or the "largest ACCEPTABLE" claim is void.
	if err := sessionwire.AgentID(identity).Validate(); err != nil {
		t.Fatalf("the worst-case identity is not a valid AgentID: %v", err)
	}
	if err := validateOpaque(identity, "runtime_compatibility_id", catalogInvalid); err != nil {
		t.Fatalf("the worst-case identity is not a valid opaque member: %v", err)
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// Six bytes per character plus the two quotes: this is what an ASCII
	// fixture does not measure.
	if want := 6*sessionwire.MaxIDBytes + 2; len(encoded) != want {
		t.Fatalf("the worst-case identity encodes to %d bytes, want %d: it is not the largest-encoding value the validators accept",
			len(encoded), want)
	}
}

// worstCaseEndpoint is the endpoint counterpart, and it is deliberately NOT
// control characters: core requires a parseable credential-free ws:// URL, so
// the longest acceptable endpoint is ASCII and escapes one-for-one.
func worstCaseEndpoint() sessionwire.InternalEndpoint {
	const prefix = "wss://h.internal/"
	return sessionwire.InternalEndpoint(prefix + strings.Repeat("x", sessionwire.MaxIDBytes-len(prefix)))
}

// TestCursorMagicsAreDistinct pins what a cursor magic IS: the KIND tag that
// stops one query family's continuation being replayed into another's. It is
// the direct analogue of TestOrderedNamespacesAreDistinct, and it is
// defence-in-depth rather than the only barrier — the scope digests are framed
// under different domains too — which is exactly why nothing else would notice
// a duplicate.
//
// The set is DERIVED FROM SOURCE rather than listed here, for the reason
// orderedRecordMembers is: a hand-written list covers the kinds its author
// remembered. This one covered three of five, so the two journal kinds could
// have been collided with by a fourth without anything failing. Every kind this
// package has is a four-byte string constant, so that is what the scan looks
// for; a kind added later is covered whether or not anyone updates this test.
func TestCursorMagicsAreDistinct(t *testing.T) {
	t.Parallel()

	magics := map[string]string{}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, declaration := range file.Decls {
			generic, ok := declaration.(*ast.GenDecl)
			if !ok || generic.Tok != token.CONST {
				continue
			}
			for _, spec := range generic.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok || len(value.Names) != 1 || len(value.Values) != 1 {
					continue
				}
				literal, ok := value.Values[0].(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				text, err := strconv.Unquote(literal.Value)
				if err != nil || len(text) != cursorMagicBytes {
					continue
				}
				magics[value.Names[0].Name] = text
			}
		}
	}

	// Anti-vacuity: the scan must reach the kinds this package is known to
	// have, or a broken walk would report a vacuous pass.
	if len(magics) < 5 {
		t.Fatalf("found %d cursor magics (%v); the scan is not reaching the declarations", len(magics), magics)
	}
	if magics["hostTargetSweepCursorMagic"] == "" {
		t.Fatalf("the scan did not reach a magic it is known to cover: %v", magics)
	}

	seen := map[string]string{}
	for name, magic := range magics {
		if other, ok := seen[magic]; ok {
			t.Fatalf("%s and %s share the magic %q", other, name, magic)
		}
		seen[magic] = name
	}
}
