package sessionstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

const (
	catalogTenant  = sessionwire.TenantID("tenant-a")
	catalogSession = sessionwire.SessionID("session-a")
)

var (
	catalogCreatedAt = time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC)
	catalogActiveAt  = time.Date(2026, 8, 30, 11, 0, 0, 0, time.UTC)
	catalogDeadline  = time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
)

func testGate(id string, seq uint64) sessionwire.GateProjection {
	return sessionwire.GateProjection{
		GateID: sessionwire.GateID(id),
		Kind:   "ask_user",
		Prompt: sessionwire.GatePrompt{
			Title: "Confirm",
			Body:  "Proceed?",
			Schema: sessionwire.GatePromptSchema{Fields: []sessionwire.GatePromptField{
				{Name: "answer", Label: "Answer", Kind: sessionwire.GateFieldKindText, Required: true},
			}},
		},
		OpenedEventID:    sessionwire.EventID("event-" + id),
		OpenedJournalSeq: seq,
		Deadline:         catalogDeadline,
		Answerability:    sessionwire.GateAnswerabilityResident,
	}
}

func testCatalogRecord() CatalogRecord {
	return CatalogRecord{
		TenantID:               catalogTenant,
		SessionID:              catalogSession,
		AgentID:                "agent-a",
		RuntimeCompatibilityID: "runtime-v1",
		CreatedAt:              catalogCreatedAt,
		LastActiveAt:           catalogActiveAt,
		State:                  sessionwire.SessionStateWaitingOnGate,
		Residency:              sessionwire.SessionResidencyResident,
		DesiredPlacement:       sessionwire.HostPlacementPooled,
		LastJournalSeq:         42,
		LastEventID:            "event-42",
		Checkpoint: CheckpointSummary{
			JournalSeq: 40,
			Reference:  sessionwire.ObjectReference{ObjectID: "v1:checkpoint:abcd:ef01"},
			CapturedAt: catalogCreatedAt.Add(30 * time.Minute),
		},
		OpenGates:             []sessionwire.GateProjection{testGate("gate-b", 7), testGate("gate-a", 5)},
		LeaseEpoch:            3,
		DesiredIdempotencyKey: "place-1",
		DesiredGeneration:     4,
		DesiredWorkload:       testDesiredWorkload(),
	}
}

func testCreateRequest() CreateCatalogEntryRequest {
	return CreateCatalogEntryRequest{
		TenantID:               catalogTenant,
		SessionID:              catalogSession,
		AgentID:                "agent-a",
		RuntimeCompatibilityID: "runtime-v1",
		CreatedAt:              catalogCreatedAt,
		LastActiveAt:           catalogActiveAt,
		State:                  sessionwire.SessionStateIdle,
		Residency:              sessionwire.SessionResidencyCold,
		DesiredPlacement:       sessionwire.HostPlacementPooled,
		IdempotencyKey:         "create-1",
	}
}

func testHostStateRequest(epoch uint64) UpdateCatalogHostStateRequest {
	return UpdateCatalogHostStateRequest{
		TenantID:       catalogTenant,
		SessionID:      catalogSession,
		LeaseEpoch:     epoch,
		State:          sessionwire.SessionStateRunning,
		Residency:      sessionwire.SessionResidencyResident,
		LastActiveAt:   catalogActiveAt,
		LastJournalSeq: 10,
		LastEventID:    "event-10",
	}
}

// assertNoCatalogRecord fails unless the session has no ordered record at all.
// It reads the provider directly, because a rejected write that nonetheless
// persisted a bad record would surface through the store API as a decode
// failure — indistinguishable, from the outside, from never having written.
func assertNoCatalogRecord(t *testing.T, store *Store, tenant sessionwire.TenantID, session sessionwire.SessionID) {
	t.Helper()
	scope, err := store.deriveSessionScope(tenant, session)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	if _, err := store.backend.OrderedIndex.Get(context.Background(), catalogID(scope, session)); !errors.As(err, new(*storage.OrderedRecordNotFoundError)) {
		t.Fatalf("a rejected write left a record behind: %v", err)
	}
}

// assertCatalogUnchanged fails unless the session's stored record is byte-for-
// byte the one want names. It is the update-path counterpart to
// assertNoCatalogRecord: on an update a record already exists, so "nothing was
// written" is expressed as an unmoved revision plus an identical record rather
// than as absence.
func assertCatalogUnchanged(t *testing.T, store *Store, want CatalogEntry) {
	t.Helper()
	got, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{
		TenantID:  want.Record.TenantID,
		SessionID: want.Record.SessionID,
	})
	if err != nil {
		t.Fatalf("a rejected write corrupted the stored record: %v", err)
	}
	if got.Revision != want.Revision {
		t.Fatalf("a rejected write advanced the revision %d -> %d", want.Revision, got.Revision)
	}
	wantBytes, err := encodeCatalogRecord(want.Record)
	if err != nil {
		t.Fatalf("encode expected record: %v", err)
	}
	gotBytes, err := encodeCatalogRecord(got.Record)
	if err != nil {
		t.Fatalf("encode stored record: %v", err)
	}
	if !bytes.Equal(wantBytes, gotBytes) {
		t.Fatalf("a rejected write changed the record:\nwant %s\ngot  %s", wantBytes, gotBytes)
	}
}

func assertCatalogCode(t *testing.T, err error, want CatalogErrorCode) *CatalogError {
	t.Helper()
	var got *CatalogError
	if !errors.As(err, &got) {
		t.Fatalf("error = %T %v, want *CatalogError", err, err)
	}
	if got.Code != want {
		t.Fatalf("catalog code = %q, want %q (%v)", got.Code, want, err)
	}
	return got
}

func mustCreateCatalog(t *testing.T, store *Store) CatalogEntry {
	t.Helper()
	entry, created, err := store.CreateCatalogEntry(context.Background(), testCreateRequest())
	if err != nil {
		t.Fatalf("CreateCatalogEntry: %v", err)
	}
	if !created {
		t.Fatal("CreateCatalogEntry reported an existing record for a fresh session")
	}
	return entry
}

// --- codec ---------------------------------------------------------------

func TestCatalogRecordRoundTripsThroughSessionwireProjections(t *testing.T) {
	record := testCatalogRecord()
	encoded, err := encodeCatalogRecord(record)
	if err != nil {
		t.Fatalf("encodeCatalogRecord: %v", err)
	}
	decoded, err := decodeCatalogRecord(encoded)
	if err != nil {
		t.Fatalf("decodeCatalogRecord: %v", err)
	}
	reencoded, err := encodeCatalogRecord(decoded)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if !bytes.Equal(encoded, reencoded) {
		t.Fatalf("round trip changed the canonical bytes:\n%s\n%s", encoded, reencoded)
	}
	if decoded.TenantID != record.TenantID || decoded.SessionID != record.SessionID ||
		decoded.AgentID != record.AgentID || decoded.RuntimeCompatibilityID != record.RuntimeCompatibilityID {
		t.Fatalf("identity did not survive: %+v", decoded)
	}
	if !decoded.CreatedAt.Equal(record.CreatedAt) || !decoded.LastActiveAt.Equal(record.LastActiveAt) {
		t.Fatalf("timestamps did not survive: %v %v", decoded.CreatedAt, decoded.LastActiveAt)
	}
	if decoded.State != record.State || decoded.Residency != record.Residency || decoded.DesiredPlacement != record.DesiredPlacement {
		t.Fatalf("states did not survive: %+v", decoded)
	}
	if decoded.LastJournalSeq != record.LastJournalSeq || decoded.LastEventID != record.LastEventID {
		t.Fatalf("journal summary did not survive: %+v", decoded)
	}
	if decoded.Checkpoint.JournalSeq != record.Checkpoint.JournalSeq ||
		decoded.Checkpoint.Reference != record.Checkpoint.Reference ||
		!decoded.Checkpoint.CapturedAt.Equal(record.Checkpoint.CapturedAt) {
		t.Fatalf("checkpoint summary did not survive: %+v", decoded.Checkpoint)
	}
	if decoded.LeaseEpoch != record.LeaseEpoch || decoded.DesiredIdempotencyKey != record.DesiredIdempotencyKey ||
		decoded.DesiredGeneration != record.DesiredGeneration {
		t.Fatalf("ownership fields did not survive: %+v", decoded)
	}
	if len(decoded.OpenGates) != 2 {
		t.Fatalf("open gates = %d, want 2", len(decoded.OpenGates))
	}
	// Canonicalization orders gates by (opened_seq, gate_id), so the record
	// stored for gate-a at sequence 5 precedes gate-b at sequence 7 even though
	// the caller supplied them the other way round.
	if decoded.OpenGates[0].GateID != "gate-a" || decoded.OpenGates[1].GateID != "gate-b" {
		t.Fatalf("gate order = %q %q", decoded.OpenGates[0].GateID, decoded.OpenGates[1].GateID)
	}
	for _, gate := range decoded.OpenGates {
		if err := gate.Validate(); err != nil {
			t.Fatalf("decoded gate is not a valid sessionwire projection: %v", err)
		}
	}
}

func TestCatalogRecordEncodesOnlySessionwireProjections(t *testing.T) {
	encoded, err := encodeCatalogRecord(testCatalogRecord())
	if err != nil {
		t.Fatalf("encodeCatalogRecord: %v", err)
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &members); err != nil {
		t.Fatalf("stored value is not JSON: %v", err)
	}
	if string(members["record_version"]) != "1" {
		t.Fatalf("record_version = %s, want 1", members["record_version"])
	}
	var gates []sessionwire.GateProjection
	if err := json.Unmarshal(members["open_gates"], &gates); err != nil {
		t.Fatalf("open_gates is not a sessionwire projection list: %v", err)
	}
	if len(gates) != 2 {
		t.Fatalf("open_gates = %d, want 2", len(gates))
	}
	var summary sessionwire.SessionSummary
	summaryValue, err := testCatalogRecord().Summary()
	if err != nil {
		t.Fatalf("Summary: %v", err)
	}
	summaryJSON, err := json.Marshal(summaryValue)
	if err != nil {
		t.Fatalf("marshal summary: %v", err)
	}
	if err := json.Unmarshal(summaryJSON, &summary); err != nil {
		t.Fatalf("summary does not round trip through sessionwire: %v", err)
	}
	status, err := testCatalogRecord().Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if err := status.Validate(); err != nil {
		t.Fatalf("status is not a valid sessionwire projection: %v", err)
	}
	// waiting_gate_id is the deterministic first open gate, not an arbitrary
	// map iteration winner.
	if status.WaitingGateID != "gate-a" {
		t.Fatalf("waiting gate = %q, want gate-a", status.WaitingGateID)
	}
	if status.JournalTip != 42 || status.Residency != sessionwire.SessionResidencyResident {
		t.Fatalf("status = %+v", status)
	}
}

func TestCatalogRecordDecodeFailsClosed(t *testing.T) {
	valid, err := encodeCatalogRecord(testCatalogRecord())
	if err != nil {
		t.Fatalf("encodeCatalogRecord: %v", err)
	}
	rewrite := func(mutate func(map[string]json.RawMessage)) []byte {
		var members map[string]json.RawMessage
		if err := json.Unmarshal(valid, &members); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		mutate(members)
		out, err := json.Marshal(members)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return out
	}

	tests := []struct {
		name  string
		value []byte
		code  CatalogErrorCode
	}{
		{name: "empty", value: nil, code: CatalogErrorMalformed},
		{name: "not json", value: []byte("{"), code: CatalogErrorMalformed},
		{name: "trailing content", value: append(append([]byte(nil), valid...), '{'), code: CatalogErrorMalformed},
		{name: "unknown version", value: rewrite(func(m map[string]json.RawMessage) {
			m["record_version"] = json.RawMessage("3")
		}), code: CatalogErrorVersion},
		{name: "unknown member", value: rewrite(func(m map[string]json.RawMessage) {
			m["surprise"] = json.RawMessage(`"x"`)
		}), code: CatalogErrorMalformed},
		{name: "empty tenant", value: rewrite(func(m map[string]json.RawMessage) {
			m["tenant_id"] = json.RawMessage(`""`)
		}), code: CatalogErrorInvalid},
		{name: "empty session", value: rewrite(func(m map[string]json.RawMessage) {
			m["session_id"] = json.RawMessage(`""`)
		}), code: CatalogErrorInvalid},
		{name: "empty state", value: rewrite(func(m map[string]json.RawMessage) {
			m["state"] = json.RawMessage(`""`)
		}), code: CatalogErrorInvalid},
		{name: "empty residency", value: rewrite(func(m map[string]json.RawMessage) {
			m["residency"] = json.RawMessage(`""`)
		}), code: CatalogErrorInvalid},
		{name: "unknown placement", value: rewrite(func(m map[string]json.RawMessage) {
			m["desired_placement"] = json.RawMessage(`"anywhere"`)
		}), code: CatalogErrorInvalid},
		{name: "zero last active", value: rewrite(func(m map[string]json.RawMessage) {
			m["last_active_at"] = json.RawMessage(`"0001-01-01T00:00:00Z"`)
		}), code: CatalogErrorInvalid},
		{name: "unrankable last active", value: rewrite(func(m map[string]json.RawMessage) {
			m["last_active_at"] = json.RawMessage(`"3000-01-01T00:00:00Z"`)
		}), code: CatalogErrorInvalid},
		{name: "duplicate gate id", value: rewrite(func(m map[string]json.RawMessage) {
			gates, err := json.Marshal([]sessionwire.GateProjection{testGate("gate-a", 5), testGate("gate-a", 6)})
			if err != nil {
				t.Fatalf("marshal gates: %v", err)
			}
			m["open_gates"] = gates
		}), code: CatalogErrorInvalid},
		{name: "too many gates", value: rewrite(func(m map[string]json.RawMessage) {
			gates := make([]sessionwire.GateProjection, 0, MaxCatalogOpenGates+1)
			for i := 0; i <= MaxCatalogOpenGates; i++ {
				gates = append(gates, testGate("gate-"+string(rune('a'+i)), uint64(i+1)))
			}
			encoded, err := json.Marshal(gates)
			if err != nil {
				t.Fatalf("marshal gates: %v", err)
			}
			m["open_gates"] = encoded
		}), code: CatalogErrorInvalid},
		{name: "invalid checkpoint reference", value: rewrite(func(m map[string]json.RawMessage) {
			m["checkpoint"] = json.RawMessage(`{"journal_seq":1,"reference":{"object_id":""},"captured_at":"2026-08-30T10:30:00Z"}`)
		}), code: CatalogErrorInvalid},
		{name: "oversized garbage", value: bytes.Repeat([]byte("a"), MaxCatalogRecordBytes+1), code: CatalogErrorTooLarge},
		// A record that is otherwise entirely valid and merely too large is the
		// case the size bound exists for: without it this decodes happily, and
		// the store would hand back a record it can never rewrite.
		{name: "oversized valid record", value: rewrite(func(m map[string]json.RawMessage) {
			gate := testGate("gate-a", 5)
			gate.Prompt.Body = strings.Repeat("x", MaxCatalogRecordBytes)
			gates, err := json.Marshal([]sessionwire.GateProjection{gate})
			if err != nil {
				t.Fatalf("marshal gates: %v", err)
			}
			m["open_gates"] = gates
		}), code: CatalogErrorTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeCatalogRecord(tt.value); err == nil {
				t.Fatal("decode accepted an invalid record")
			} else {
				assertCatalogCode(t, err, tt.code)
			}
		})
	}
}

func TestCatalogRecordEncodeRejectsOversizedProjection(t *testing.T) {
	record := testCatalogRecord()
	gate := testGate("gate-a", 5)
	gate.Prompt.Body = strings.Repeat("x", MaxCatalogRecordBytes)
	record.OpenGates = []sessionwire.GateProjection{gate}
	if _, err := encodeCatalogRecord(record); err == nil {
		t.Fatal("encode accepted a record larger than the catalog bound")
	} else {
		assertCatalogCode(t, err, CatalogErrorTooLarge)
	}
}

// --- record identity, scope, and rank ------------------------------------

func TestCatalogUsesOneTenantScopedRankedOrderedRecord(t *testing.T) {
	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	kv := &keysCountingKV{KV: base.KV}
	base.KV = kv
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close(context.Background())

	mustCreateCatalog(t, store)
	if _, err := store.UpdateCatalogHostState(context.Background(), testHostStateRequest(1)); err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}

	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatal(err)
	}
	create, ok := ordered.lastOf("create")
	if !ok {
		t.Fatal("no Create reached the provider")
	}
	if create.id.Namespace != catalogNamespace {
		t.Fatalf("namespace = %q, want %q", create.id.Namespace, catalogNamespace)
	}
	if create.id.OrderingScope != scope.CatalogScope || create.rankingScope != scope.CatalogScope {
		t.Fatalf("scopes = %q/%q, want %q", create.id.OrderingScope, create.rankingScope, scope.CatalogScope)
	}
	if create.id.StableKey != storage.StableKey(catalogSession) {
		t.Fatalf("stable key = %q, want %q", create.id.StableKey, catalogSession)
	}
	if err := storage.ValidateOrderedID(create.id); err != nil {
		t.Fatalf("ValidateOrderedID: %v", err)
	}
	if !create.rank.Ranked || create.rank.Value != catalogActiveAt.UnixNano() {
		t.Fatalf("rank = %+v, want LastActiveAt.UnixNano() %d", create.rank, catalogActiveAt.UnixNano())
	}
	if create.due != (storage.Due{}) {
		t.Fatalf("catalog record is due: %+v", create.due)
	}
	if got := ordered.countOf("create") + ordered.countOf("update"); got != 2 {
		t.Fatalf("mutations = %d, want exactly one create and one update", got)
	}
	for _, call := range ordered.snapshot() {
		if strings.HasPrefix(call.op, "list") {
			t.Fatalf("catalog performed a listing (%s) for a direct get/update", call.op)
		}
	}
	if kv.keys.Load() != 0 {
		t.Fatalf("catalog scanned KV keys %d times", kv.keys.Load())
	}
}

func TestCatalogGetVerifiesBindingBeforeProvider(t *testing.T) {
	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close(context.Background())

	_, err = store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: catalogSession})
	if err == nil {
		t.Fatal("Get of an unbound session succeeded")
	}
	assertKeyspaceCode(t, err, KeyspaceBindingNotFound)
	if got := ordered.countOf("get"); got != 0 {
		t.Fatalf("unbound Get touched the OrderedIndex %d times", got)
	}
}

func TestCatalogReadVerifiesStoredIdentity(t *testing.T) {
	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close(context.Background())
	mustCreateCatalog(t, store)

	hostile.corruptGets(func(value []byte) []byte {
		record, err := decodeCatalogRecord(value)
		if err != nil {
			t.Fatalf("decode stored: %v", err)
		}
		record.SessionID = "someone-else"
		out, err := encodeCatalogRecord(record)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		return out
	})

	if _, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: catalogSession}); err == nil {
		t.Fatal("Get accepted a record naming another session")
	} else {
		assertCatalogCode(t, err, CatalogErrorIdentity)
	}
}

func TestCatalogCreateIsIdempotentAndTenantScoped(t *testing.T) {
	store := openTestStore(t)
	first := mustCreateCatalog(t, store)

	second := testCreateRequest()
	second.AgentID = "agent-b"
	entry, created, err := store.CreateCatalogEntry(context.Background(), second)
	if err != nil {
		t.Fatalf("duplicate CreateCatalogEntry: %v", err)
	}
	if created {
		t.Fatal("duplicate create reported created=true")
	}
	if entry.Record.AgentID != first.Record.AgentID || entry.Revision != first.Revision {
		t.Fatalf("duplicate create replaced the original: %+v", entry)
	}

	other := testCreateRequest()
	other.TenantID = "tenant-b"
	if _, created, err := store.CreateCatalogEntry(context.Background(), other); err != nil {
		t.Fatalf("second tenant CreateCatalogEntry: %v", err)
	} else if !created {
		t.Fatal("the same SessionID in another tenant aliased an existing record")
	}
}

func TestCatalogGetRejectsMissingAndDeleted(t *testing.T) {
	store := openTestStore(t)
	mustCreateCatalog(t, store)

	missing := GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: "absent"}
	if _, err := store.GetCatalogEntry(context.Background(), missing); err == nil {
		t.Fatal("Get of an absent session succeeded")
	}

	other, created, err := store.CreateCatalogEntry(context.Background(), func() CreateCatalogEntryRequest {
		req := testCreateRequest()
		req.SessionID = "session-b"
		return req
	}())
	if err != nil || !created {
		t.Fatalf("create second session: %v created=%v", err, created)
	}
	scope, err := store.deriveSessionScope(catalogTenant, "session-b")
	if err != nil {
		t.Fatal(err)
	}
	id := storage.OrderedID{Namespace: catalogNamespace, OrderingScope: scope.CatalogScope, StableKey: storage.StableKey("session-b")}
	if _, err := store.backend.OrderedIndex.Delete(context.Background(), id, other.Revision); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: "session-b"}); err == nil {
		t.Fatal("Get of a tombstone succeeded")
	} else {
		assertCatalogCode(t, err, CatalogErrorDeleted)
	}
}

func TestCatalogUsesTheLegacySingleTenantScope(t *testing.T) {
	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	store, err := Open(context.Background(), base, WithLegacySingleTenant("local"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close(context.Background())

	const legacySession = sessionwire.SessionID("123e4567-e89b-12d3-a456-426614174000")
	req := testCreateRequest()
	req.TenantID = "local"
	req.SessionID = legacySession
	if _, created, err := store.CreateCatalogEntry(context.Background(), req); err != nil || !created {
		t.Fatalf("CreateCatalogEntry: %v created=%v", err, created)
	}
	create, ok := ordered.lastOf("create")
	if !ok {
		t.Fatal("no Create reached the provider")
	}
	if create.id.OrderingScope != legacyCatalogScope || create.rankingScope != legacyCatalogScope {
		t.Fatalf("legacy scopes = %q/%q, want %q", create.id.OrderingScope, create.rankingScope, legacyCatalogScope)
	}
	entry, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: "local", SessionID: legacySession})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	if entry.Record.SessionID != legacySession {
		t.Fatalf("legacy record = %+v", entry.Record)
	}
	// Only the configured default tenant is authorized in this layout, and the
	// refusal is a keyspace decision made before any provider read.
	if _, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: "other", SessionID: legacySession}); err == nil {
		t.Fatal("an unauthorized tenant read the legacy catalog")
	} else {
		assertKeyspaceCode(t, err, KeyspaceLegacyTenant)
	}
}

// --- the epoch/revision split --------------------------------------------

func TestCatalogUpdateRejectsLowerEpoch(t *testing.T) {
	store := openTestStore(t)
	mustCreateCatalog(t, store)

	high, err := store.UpdateCatalogHostState(context.Background(), testHostStateRequest(5))
	if err != nil {
		t.Fatalf("epoch 5 UpdateCatalogHostState: %v", err)
	}
	if high.Record.LeaseEpoch != 5 {
		t.Fatalf("stored epoch = %d, want 5", high.Record.LeaseEpoch)
	}

	// The stale request differs from the accepted one in exactly one field:
	// its lease epoch. Every other member stays valid, so nothing shallower
	// than the epoch guard can reject it.
	stale := testHostStateRequest(4)
	stale.LastJournalSeq = high.Record.LastJournalSeq
	stale.State = sessionwire.SessionStateFailed
	if _, err := store.UpdateCatalogHostState(context.Background(), stale); err == nil {
		t.Fatal("a lower lease epoch was accepted")
	} else {
		catalogErr := assertCatalogCode(t, err, CatalogErrorEpoch)
		if catalogErr.Epoch != 5 {
			t.Fatalf("reported current epoch = %d, want 5", catalogErr.Epoch)
		}
	}

	assertCatalogUnchanged(t, store, high)

	// The same grant writes more than once, so the guard must reject a lower
	// epoch and admit an equal one.
	same := testHostStateRequest(5)
	same.LastJournalSeq = high.Record.LastJournalSeq + 1
	same.State = sessionwire.SessionStateIdle
	repeat, err := store.UpdateCatalogHostState(context.Background(), same)
	if err != nil {
		t.Fatalf("equal epoch was rejected: %v", err)
	}
	if repeat.Record.State != sessionwire.SessionStateIdle || repeat.Record.LeaseEpoch != 5 {
		t.Fatalf("equal-epoch update did not apply: %+v", repeat.Record)
	}

	// A successor grant with a strictly higher epoch takes ownership.
	successor := testHostStateRequest(6)
	successor.LastJournalSeq = repeat.Record.LastJournalSeq
	if next, err := store.UpdateCatalogHostState(context.Background(), successor); err != nil {
		t.Fatalf("successor epoch rejected: %v", err)
	} else if next.Record.LeaseEpoch != 6 {
		t.Fatalf("successor epoch not recorded: %d", next.Record.LeaseEpoch)
	}
}

func TestCatalogHostUpdateRequiresNonZeroEpoch(t *testing.T) {
	store := openTestStore(t)
	created := mustCreateCatalog(t, store)
	req := testHostStateRequest(0)
	if _, err := store.UpdateCatalogHostState(context.Background(), req); err == nil {
		t.Fatal("a zero lease epoch was accepted")
	} else {
		got := assertCatalogCode(t, err, CatalogErrorInvalid)
		if got.Field != "lease_epoch" {
			t.Fatalf("field = %q, want lease_epoch", got.Field)
		}
	}
	assertCatalogUnchanged(t, store, created)
}

func TestCatalogHostUpdateRejectsJournalRegression(t *testing.T) {
	store := openTestStore(t)
	mustCreateCatalog(t, store)
	forward := testHostStateRequest(2)
	forward.LastJournalSeq = 20
	advanced, err := store.UpdateCatalogHostState(context.Background(), forward)
	if err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}
	// Only the journal sequence regresses; the epoch advances, so the epoch
	// guard cannot be what rejects this.
	back := testHostStateRequest(3)
	back.LastJournalSeq = 19
	if _, err := store.UpdateCatalogHostState(context.Background(), back); err == nil {
		t.Fatal("a regressing journal sequence was accepted")
	} else {
		assertCatalogCode(t, err, CatalogErrorSequence)
	}
	// Refusing after writing would be worse than not refusing at all: the
	// caller is told the sequence was rejected while the store holds it.
	assertCatalogUnchanged(t, store, advanced)
}

func TestFactoryOwnedFieldsUseRevisionNotHostEpoch(t *testing.T) {
	// A Factory desired-state write must not be able to name a Host lease
	// epoch at all: the request type has no such member, so the mechanism is
	// unavailable rather than merely unused.
	requestType := reflect.TypeOf(UpdateCatalogDesiredStateRequest{})
	for i := range requestType.NumField() {
		if name := strings.ToLower(requestType.Field(i).Name); strings.Contains(name, "epoch") || strings.Contains(name, "lease") {
			t.Fatalf("UpdateCatalogDesiredStateRequest.%s lets Factory claim a Host lease", requestType.Field(i).Name)
		}
	}

	store := openTestStore(t)
	mustCreateCatalog(t, store)
	host, err := store.UpdateCatalogHostState(context.Background(), testHostStateRequest(9))
	if err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}

	desired := UpdateCatalogDesiredStateRequest{
		TenantID:               catalogTenant,
		SessionID:              catalogSession,
		ExpectedRevision:       host.Revision,
		IdempotencyKey:         "place-2",
		DesiredPlacement:       sessionwire.HostPlacementDedicated,
		RuntimeCompatibilityID: "runtime-v2",
	}
	updated, err := store.UpdateCatalogDesiredState(context.Background(), desired)
	if err != nil {
		t.Fatalf("UpdateCatalogDesiredState under a live Host lease: %v", err)
	}
	if updated.Record.DesiredPlacement != sessionwire.HostPlacementDedicated || updated.Record.RuntimeCompatibilityID != "runtime-v2" {
		t.Fatalf("desired state did not apply: %+v", updated.Record)
	}
	if updated.Record.LeaseEpoch != 9 {
		t.Fatalf("Factory write moved the Host lease epoch to %d", updated.Record.LeaseEpoch)
	}
	if updated.Record.State != host.Record.State || updated.Record.Residency != host.Record.Residency ||
		updated.Record.LastJournalSeq != host.Record.LastJournalSeq ||
		!updated.Record.LastActiveAt.Equal(host.Record.LastActiveAt) {
		t.Fatalf("Factory write changed Host-owned fields: %+v", updated.Record)
	}
	if updated.Revision == host.Revision {
		t.Fatal("a desired-state write did not advance the revision")
	}

	// A stale revision is refused as a revision conflict, never as an epoch
	// failure: Factory has no epoch to be stale about.
	conflicting := desired
	conflicting.IdempotencyKey = "place-3"
	conflicting.ExpectedRevision = host.Revision
	conflicting.DesiredPlacement = sessionwire.HostPlacementPooled
	if _, err := store.UpdateCatalogDesiredState(context.Background(), conflicting); err == nil {
		t.Fatal("a stale expected revision was accepted")
	} else {
		got := assertCatalogCode(t, err, CatalogErrorConflict)
		if got.Revision != updated.Revision {
			t.Fatalf("reported revision = %d, want %d", got.Revision, updated.Revision)
		}
	}
	assertCatalogUnchanged(t, store, updated)

	// A retry of the accepted write replays idempotently even though its
	// expected revision is now stale by construction.
	replay, err := store.UpdateCatalogDesiredState(context.Background(), desired)
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if replay.Revision != updated.Revision {
		t.Fatalf("replay advanced the revision %d -> %d", updated.Revision, replay.Revision)
	}

	// A Host update still advances behind the Factory write, and its epoch
	// guard still uses the recorded high-water mark.
	stale := testHostStateRequest(8)
	stale.LastJournalSeq = updated.Record.LastJournalSeq
	if _, err := store.UpdateCatalogHostState(context.Background(), stale); err == nil {
		t.Fatal("a lower Host epoch survived a Factory desired-state write")
	} else {
		assertCatalogCode(t, err, CatalogErrorEpoch)
	}
}

func TestCatalogDesiredStateRequiresIdempotencyKey(t *testing.T) {
	store := openTestStore(t)
	entry := mustCreateCatalog(t, store)
	req := UpdateCatalogDesiredStateRequest{
		TenantID:         catalogTenant,
		SessionID:        catalogSession,
		ExpectedRevision: entry.Revision,
		DesiredPlacement: sessionwire.HostPlacementPooled,
	}
	if _, err := store.UpdateCatalogDesiredState(context.Background(), req); err == nil {
		t.Fatal("an unkeyed desired-state write was accepted")
	} else {
		got := assertCatalogCode(t, err, CatalogErrorInvalid)
		if got.Field != "idempotency_key" {
			t.Fatalf("field = %q, want idempotency_key", got.Field)
		}
	}
	assertCatalogUnchanged(t, store, entry)
}

func TestCatalogConcurrentHostEpochsKeepTheHighWaterMark(t *testing.T) {
	store := openTestStore(t)
	mustCreateCatalog(t, store)

	const successor = 7
	const predecessor = 6
	apply := func(epoch uint64, state sessionwire.SessionState) error {
		for attempt := 0; attempt < 64; attempt++ {
			req := testHostStateRequest(epoch)
			req.State = state
			current, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: catalogSession})
			if err != nil {
				return err
			}
			req.LastJournalSeq = current.Record.LastJournalSeq
			_, err = store.UpdateCatalogHostState(context.Background(), req)
			var catalogErr *CatalogError
			if errors.As(err, &catalogErr) && catalogErr.Code == CatalogErrorConflict {
				continue
			}
			return err
		}
		return errors.New("exhausted attempts")
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = apply(successor, sessionwire.SessionStateRunning)
	}()
	go func() {
		defer wg.Done()
		errs[1] = apply(predecessor, sessionwire.SessionStateFailed)
	}()
	wg.Wait()

	if errs[0] != nil {
		t.Fatalf("successor write failed: %v", errs[0])
	}
	var catalogErr *CatalogError
	if errs[1] != nil && !(errors.As(errs[1], &catalogErr) && catalogErr.Code == CatalogErrorEpoch) {
		t.Fatalf("predecessor write failed for an unexpected reason: %v", errs[1])
	}

	final, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: catalogSession})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	if final.Record.LeaseEpoch != successor {
		t.Fatalf("final epoch = %d, want %d", final.Record.LeaseEpoch, successor)
	}
	if final.Record.State != sessionwire.SessionStateRunning {
		t.Fatalf("the predecessor's state survived the successor: %q", final.Record.State)
	}
}

func TestCatalogHostUpdateInterleavedWithAnotherWriterConflicts(t *testing.T) {
	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close(context.Background())
	mustCreateCatalog(t, store)

	// The hook re-enters the store, so it must disarm itself before recursing:
	// a sync.Once would deadlock on its own in-progress call.
	var interposing atomic.Bool
	ordered.beforeUpdate = func() {
		if !interposing.CompareAndSwap(false, true) {
			return
		}
		interposed := testHostStateRequest(4)
		interposed.State = sessionwire.SessionStateStopped
		if _, err := store.UpdateCatalogHostState(context.Background(), interposed); err != nil {
			t.Errorf("interposed update: %v", err)
		}
	}
	if _, err := store.UpdateCatalogHostState(context.Background(), testHostStateRequest(4)); err == nil {
		t.Fatal("an update CASing a superseded revision succeeded")
	} else {
		assertCatalogCode(t, err, CatalogErrorConflict)
	}
}

// --- lifecycle and provider errors ---------------------------------------

// declaredCatalogOperations returns the name of every public Store operation
// declared in catalog.go, parsed from the source rather than listed by hand.
//
// A hand-maintained list is the thing that failed here once already: the
// operations below have to be spelled out anyway, because each needs a valid
// request, but nothing made the spelling COMPLETE, so a fifth operation could
// be added and silently never checked. Reading the declarations back is what
// closes that: the file that declares an operation is the same file the test
// enumerates, so the two cannot drift.
//
// It is scoped to catalog.go on purpose. Each area of this package owns a close
// test for the operations it declares, and a guard that failed here for a new
// object or journal method would be pointing at the wrong test.
func declaredCatalogOperations(t *testing.T) map[string]bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "catalog.go", nil, 0)
	if err != nil {
		t.Fatalf("parse catalog.go: %v", err)
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
		t.Fatal("no public Store operations were found in catalog.go; the enumerator is not reaching the declarations")
	}
	return operations
}

// TestCatalogOperationsRefuseAfterClose holds every public catalog operation to
// the Store lifecycle. Each one admits foreground work, so each one must refuse
// once shutdown has started rather than racing the drain it was supposed to
// join.
//
// The table is checked against catalog.go's own declarations, so a new public
// operation fails this test until it is exercised here.
func TestCatalogOperationsRefuseAfterClose(t *testing.T) {
	store, err := Open(context.Background(), memstore.New())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustCreateCatalog(t, store)
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	operations := map[string]func() error{
		"CreateCatalogEntry": func() error {
			_, _, err := store.CreateCatalogEntry(context.Background(), testCreateRequest())
			return err
		},
		"GetCatalogEntry": func() error {
			_, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{
				TenantID: catalogTenant, SessionID: catalogSession,
			})
			return err
		},
		"UpdateCatalogHostState": func() error {
			_, err := store.UpdateCatalogHostState(context.Background(), testHostStateRequest(1))
			return err
		},
		"UpdateCatalogDesiredState": func() error {
			_, err := store.UpdateCatalogDesiredState(context.Background(), UpdateCatalogDesiredStateRequest{
				TenantID: catalogTenant, SessionID: catalogSession, ExpectedRevision: 1,
				IdempotencyKey: "k", DesiredPlacement: sessionwire.HostPlacementPooled,
			})
			return err
		},
		"ListSessions": func() error {
			_, err := store.ListSessions(context.Background(), ListSessionsRequest{
				TenantID: catalogTenant, Limit: 10,
			})
			return err
		},
	}

	declared := declaredCatalogOperations(t)
	for name := range declared {
		if operations[name] == nil {
			t.Errorf("catalog.go declares the public operation %s and this test does not exercise it", name)
		}
	}
	for name := range operations {
		if !declared[name] {
			t.Errorf("this test exercises %s, which catalog.go no longer declares (was it moved to another file?)", name)
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

func TestCatalogRejectsInvalidIdentities(t *testing.T) {
	store := openTestStore(t)
	bad := testCreateRequest()
	bad.TenantID = ""
	if _, _, err := store.CreateCatalogEntry(context.Background(), bad); !errors.As(err, new(*InvalidIdentityError)) {
		t.Fatalf("empty tenant = %v", err)
	}
	bad = testCreateRequest()
	bad.SessionID = ""
	if _, _, err := store.CreateCatalogEntry(context.Background(), bad); !errors.As(err, new(*InvalidIdentityError)) {
		t.Fatalf("empty session = %v", err)
	}
	// Each of these must be refused BEFORE anything is persisted. Rejecting a
	// record only when it is read back would leave the store holding a value
	// no later read can decode.
	for _, tt := range []struct {
		name  string
		apply func(*CreateCatalogEntryRequest)
	}{
		{"empty agent", func(r *CreateCatalogEntryRequest) { r.AgentID = "" }},
		{"zero last active", func(r *CreateCatalogEntryRequest) { r.LastActiveAt = time.Time{} }},
		{"unrankable last active", func(r *CreateCatalogEntryRequest) {
			r.LastActiveAt = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
		}},
		{"unknown placement", func(r *CreateCatalogEntryRequest) { r.DesiredPlacement = "anywhere" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fresh := openTestStore(t)
			req := testCreateRequest()
			tt.apply(&req)
			if _, _, err := fresh.CreateCatalogEntry(context.Background(), req); err == nil {
				t.Fatal("an invalid create was accepted")
			} else {
				assertCatalogCode(t, err, CatalogErrorInvalid)
			}
			assertNoCatalogRecord(t, fresh, req.TenantID, req.SessionID)
		})
	}
}

func TestCatalogClassifiesProviderFailures(t *testing.T) {
	id := storage.OrderedID{Namespace: catalogNamespace, OrderingScope: "tenants/x", StableKey: "s"}
	tests := []struct {
		name string
		err  error
		want CatalogErrorCode
	}{
		{"not found", &storage.OrderedRecordNotFoundError{ID: id}, CatalogErrorNotFound},
		{"deleted", &storage.OrderedDeletedError{ID: id}, CatalogErrorDeleted},
		{"conflict", &storage.OrderedRevisionConflictError{ID: id, ExpectedRevision: 1, ActualRevision: 2}, CatalogErrorConflict},
		{"ambiguous", &storage.OrderedAmbiguousError{Operation: storage.OrderedUpdateOperation, ID: id}, CatalogErrorUnknown},
		{"exhausted", &storage.OrderedRevisionExhaustedError{ID: id, Revision: math.MaxUint64}, CatalogErrorBackend},
		{"other", errors.New("boom"), CatalogErrorBackend},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyCatalogOrderedError(tt.err, "field")
			assertCatalogCode(t, got, tt.want)
			if !errors.Is(got, tt.err) {
				t.Fatalf("cause was not preserved: %v", got)
			}
		})
	}
	if conflict := classifyCatalogOrderedError(&storage.OrderedRevisionConflictError{ID: id, ExpectedRevision: 1, ActualRevision: 4}, "f"); assertCatalogCode(t, conflict, CatalogErrorConflict).Revision != 4 {
		t.Fatal("a conflict did not carry the observed revision")
	}
}

func TestCatalogSurfacesProviderErrorsWithoutLeakingProviderText(t *testing.T) {
	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close(context.Background())
	mustCreateCatalog(t, store)

	secret := errors.New("provider path /var/secret/tenant-a")
	hostile.failGets(secret)
	_, err = store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: catalogSession})
	assertCatalogCode(t, err, CatalogErrorBackend)
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "tenant-a") {
		t.Fatalf("provider detail leaked into %q", err.Error())
	}
	if !errors.Is(err, secret) {
		t.Fatal("cause was not preserved for errors.Is")
	}
}

var (
	_ func(*Store, context.Context, GetCatalogEntryRequest) (CatalogEntry, error)           = (*Store).GetCatalogEntry
	_ func(*Store, context.Context, UpdateCatalogHostStateRequest) (CatalogEntry, error)    = (*Store).UpdateCatalogHostState
	_ func(*Store, context.Context, UpdateCatalogDesiredStateRequest) (CatalogEntry, error) = (*Store).UpdateCatalogDesiredState
	_ func(*Store, context.Context, CreateCatalogEntryRequest) (CatalogEntry, bool, error)  = (*Store).CreateCatalogEntry
)

// --- create-time desired state, opaque bounds, and replace semantics ------

func TestCatalogCreateRecordsItsIdempotencyKey(t *testing.T) {
	store := openTestStore(t)
	created := mustCreateCatalog(t, store)
	if created.Record.DesiredIdempotencyKey != "create-1" {
		t.Fatalf("create key = %q, want create-1", created.Record.DesiredIdempotencyKey)
	}

	// The consequence, not just the field: a Factory retry of the very intent
	// the create already applied must short-circuit as a replay. A create that
	// dropped its key would leave this write free to apply.
	replay := UpdateCatalogDesiredStateRequest{
		TenantID:               catalogTenant,
		SessionID:              catalogSession,
		ExpectedRevision:       created.Revision,
		IdempotencyKey:         "create-1",
		DesiredPlacement:       sessionwire.HostPlacementDedicated,
		RuntimeCompatibilityID: "runtime-v9",
	}
	entry, err := store.UpdateCatalogDesiredState(context.Background(), replay)
	if err != nil {
		t.Fatalf("replay of the create key: %v", err)
	}
	if entry.Revision != created.Revision {
		t.Fatalf("replay advanced the revision %d -> %d", created.Revision, entry.Revision)
	}
	if entry.Record.DesiredPlacement != created.Record.DesiredPlacement {
		t.Fatalf("replay applied desired placement %q", entry.Record.DesiredPlacement)
	}
	if entry.Record.RuntimeCompatibilityID != created.Record.RuntimeCompatibilityID {
		t.Fatalf("replay applied runtime compatibility %q", entry.Record.RuntimeCompatibilityID)
	}

	// A different key with the same revision is a new intent and applies, so
	// the replay above is the key doing the work rather than the write being
	// rejected for some unrelated reason.
	fresh := replay
	fresh.IdempotencyKey = "place-9"
	applied, err := store.UpdateCatalogDesiredState(context.Background(), fresh)
	if err != nil {
		t.Fatalf("fresh key: %v", err)
	}
	if applied.Record.DesiredPlacement != sessionwire.HostPlacementDedicated || applied.Revision == created.Revision {
		t.Fatalf("a fresh key did not apply: %+v rev=%d", applied.Record, applied.Revision)
	}
}

func TestCatalogBoundsOpaqueFields(t *testing.T) {
	atLimit := strings.Repeat("r", sessionwire.MaxIDBytes)
	tooLong := atLimit + "r"
	invalidUTF8 := "runtime-\xff"

	tests := []struct {
		name  string
		field string
		apply func(*CreateCatalogEntryRequest)
		want  bool
	}{
		{"runtime id at limit", "", func(r *CreateCatalogEntryRequest) { r.RuntimeCompatibilityID = atLimit }, true},
		{"runtime id too long", "runtime_compatibility_id", func(r *CreateCatalogEntryRequest) { r.RuntimeCompatibilityID = tooLong }, false},
		{"runtime id invalid utf8", "runtime_compatibility_id", func(r *CreateCatalogEntryRequest) { r.RuntimeCompatibilityID = invalidUTF8 }, false},
		{"idempotency key at limit", "", func(r *CreateCatalogEntryRequest) { r.IdempotencyKey = atLimit }, true},
		{"idempotency key too long", "desired_idempotency_key", func(r *CreateCatalogEntryRequest) { r.IdempotencyKey = tooLong }, false},
		{"idempotency key invalid utf8", "desired_idempotency_key", func(r *CreateCatalogEntryRequest) { r.IdempotencyKey = invalidUTF8 }, false},
		// State and Residency are caller-supplied too. Core deliberately
		// accepts any non-empty value for forward compatibility, so this
		// package is the only place they are bounded at all.
		{"state at limit", "", func(r *CreateCatalogEntryRequest) { r.State = sessionwire.SessionState(atLimit) }, true},
		{"state too long", "state", func(r *CreateCatalogEntryRequest) { r.State = sessionwire.SessionState(tooLong) }, false},
		{"state invalid utf8", "state", func(r *CreateCatalogEntryRequest) { r.State = sessionwire.SessionState(invalidUTF8) }, false},
		{"state empty", "state", func(r *CreateCatalogEntryRequest) { r.State = "" }, false},
		{"residency at limit", "", func(r *CreateCatalogEntryRequest) { r.Residency = sessionwire.SessionResidency(atLimit) }, true},
		{"residency too long", "residency", func(r *CreateCatalogEntryRequest) { r.Residency = sessionwire.SessionResidency(tooLong) }, false},
		{"residency invalid utf8", "residency", func(r *CreateCatalogEntryRequest) { r.Residency = sessionwire.SessionResidency(invalidUTF8) }, false},
		{"residency empty", "residency", func(r *CreateCatalogEntryRequest) { r.Residency = "" }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := openTestStore(t)
			req := testCreateRequest()
			tt.apply(&req)
			_, _, err := store.CreateCatalogEntry(context.Background(), req)
			if tt.want {
				if err != nil {
					t.Fatalf("a value at the inclusive bound was rejected: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("an out-of-bounds opaque value was accepted")
			}
			if got := assertCatalogCode(t, err, CatalogErrorInvalid); got.Field != tt.field {
				t.Fatalf("field = %q, want %q", got.Field, tt.field)
			}
			assertNoCatalogRecord(t, store, req.TenantID, req.SessionID)
		})
	}
}

func TestCatalogHostUpdateReplacesCheckpointAndGates(t *testing.T) {
	store := openTestStore(t)
	mustCreateCatalog(t, store)

	withState := testHostStateRequest(1)
	withState.Checkpoint = CheckpointSummary{
		JournalSeq: 9,
		Reference:  sessionwire.ObjectReference{ObjectID: "v1:checkpoint:abcd:ef01"},
		CapturedAt: catalogCreatedAt,
	}
	withState.OpenGates = []sessionwire.GateProjection{testGate("gate-a", 5)}
	if _, err := store.UpdateCatalogHostState(context.Background(), withState); err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}
	stored, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: catalogSession})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	if stored.Record.Checkpoint.JournalSeq != 9 ||
		stored.Record.Checkpoint.Reference.ObjectID != "v1:checkpoint:abcd:ef01" ||
		!stored.Record.Checkpoint.CapturedAt.Equal(catalogCreatedAt) {
		t.Fatalf("checkpoint summary did not survive the write path: %+v", stored.Record.Checkpoint)
	}
	if len(stored.Record.OpenGates) != 1 || stored.Record.OpenGates[0].GateID != "gate-a" {
		t.Fatalf("open gates did not survive the write path: %+v", stored.Record.OpenGates)
	}

	// Replace, not merge: a later Host write that omits both simply clears
	// them. The catalog holds a summary, not the retained high-water pointer.
	cleared := testHostStateRequest(2)
	cleared.LastJournalSeq = stored.Record.LastJournalSeq
	if _, err := store.UpdateCatalogHostState(context.Background(), cleared); err != nil {
		t.Fatalf("clearing UpdateCatalogHostState: %v", err)
	}
	after, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: catalogSession})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	if !after.Record.Checkpoint.isZero() {
		t.Fatalf("an omitted checkpoint was merged forward: %+v", after.Record.Checkpoint)
	}
	if after.Record.OpenGates != nil {
		t.Fatalf("omitted gates were merged forward: %+v", after.Record.OpenGates)
	}
	// The journal summary is deliberately NOT replace-anything: it keeps its
	// monotonicity guard, so the asymmetry is intentional and pinned.
	if after.Record.LastJournalSeq != stored.Record.LastJournalSeq {
		t.Fatalf("journal summary = %d, want %d", after.Record.LastJournalSeq, stored.Record.LastJournalSeq)
	}
}

func TestCatalogHostUpdateValidatesProjectionsOnTheWritePath(t *testing.T) {
	manyGates := func() []sessionwire.GateProjection {
		gates := make([]sessionwire.GateProjection, 0, MaxCatalogOpenGates+1)
		for i := 0; i <= MaxCatalogOpenGates; i++ {
			gates = append(gates, testGate("gate-"+string(rune('a'+i)), uint64(i+1)))
		}
		return gates
	}
	tests := []struct {
		name  string
		field string
		apply func(*UpdateCatalogHostStateRequest)
	}{
		{"checkpoint without a reference", "checkpoint.reference", func(r *UpdateCatalogHostStateRequest) {
			r.Checkpoint = CheckpointSummary{JournalSeq: 3, CapturedAt: catalogCreatedAt}
		}},
		{"checkpoint without a capture time", "checkpoint.captured_at", func(r *UpdateCatalogHostStateRequest) {
			r.Checkpoint = CheckpointSummary{JournalSeq: 3, Reference: sessionwire.ObjectReference{ObjectID: "v1:checkpoint:abcd:ef01"}}
		}},
		// The bounded-gate rule must be enforced by the write path, not only by
		// the decoder that reads a record back.
		{"more gates than the bound", "open_gates", func(r *UpdateCatalogHostStateRequest) {
			r.OpenGates = manyGates()
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A fresh store per case: a case judged against a record an earlier
			// case had already corrupted would pass for the wrong reason.
			store := openTestStore(t)
			created := mustCreateCatalog(t, store)
			req := testHostStateRequest(1)
			tt.apply(&req)
			if _, err := store.UpdateCatalogHostState(context.Background(), req); err == nil {
				t.Fatal("an invalid projection was accepted by the write path")
			} else if got := assertCatalogCode(t, err, CatalogErrorInvalid); got.Field != tt.field {
				t.Fatalf("field = %q, want %q", got.Field, tt.field)
			}
			assertCatalogUnchanged(t, store, created)
		})
	}
}

// --- text validation across the whole record ------------------------------

// richCatalogRecord has exactly one gate, and that gate populates every nested
// text-bearing member core's GatePrompt can carry, so a walk over it reaches
// options and controls rather than only the members the simple fixture sets.
func richCatalogRecord() CatalogRecord {
	gate := testGate("gate-a", 5)
	gate.Prompt.Origin = "https://example.test"
	gate.Prompt.Schema.Fields = []sessionwire.GatePromptField{{
		Name:  "answer",
		Label: "Answer",
		Kind:  sessionwire.GateFieldKindSelect,
		Options: []sessionwire.GatePromptOption{
			{Value: "yes", Label: "Yes"},
			{Value: "no", Label: "No"},
		},
		Default: json.RawMessage(`"yes"`),
	}}
	gate.Prompt.Controls = []sessionwire.GateControl{{Action: "submit", Label: "Submit"}}
	record := testCatalogRecord()
	record.OpenGates = []sessionwire.GateProjection{gate}
	return record
}

func TestCatalogRejectsInvalidTextInNestedProjections(t *testing.T) {
	const bad = "va\xfflue"
	tests := []struct {
		name  string
		apply func(*sessionwire.GateProjection)
	}{
		{"kind", func(g *sessionwire.GateProjection) { g.Kind = bad }},
		{"prompt title", func(g *sessionwire.GateProjection) { g.Prompt.Title = bad }},
		{"prompt body", func(g *sessionwire.GateProjection) { g.Prompt.Body = bad }},
		{"field name", func(g *sessionwire.GateProjection) { g.Prompt.Schema.Fields[0].Name = bad }},
		{"field label", func(g *sessionwire.GateProjection) { g.Prompt.Schema.Fields[0].Label = bad }},
		{"option value", func(g *sessionwire.GateProjection) { g.Prompt.Schema.Fields[0].Options[0].Value = bad }},
		{"option label", func(g *sessionwire.GateProjection) { g.Prompt.Schema.Fields[0].Options[0].Label = bad }},
		{"control action", func(g *sessionwire.GateProjection) { g.Prompt.Controls[0].Action = bad }},
		{"control label", func(g *sessionwire.GateProjection) { g.Prompt.Controls[0].Label = bad }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := openTestStore(t)
			created := mustCreateCatalog(t, store)
			gate := richCatalogRecord().OpenGates[0]
			tt.apply(&gate)
			req := testHostStateRequest(1)
			req.OpenGates = []sessionwire.GateProjection{gate}
			if _, err := store.UpdateCatalogHostState(context.Background(), req); err == nil {
				t.Fatal("invalid UTF-8 in a nested projection was accepted")
			} else {
				got := assertCatalogCode(t, err, CatalogErrorInvalid)
				if !strings.HasPrefix(got.Field, "open_gates") {
					t.Fatalf("field = %q, want an open_gates path", got.Field)
				}
			}
			assertCatalogUnchanged(t, store, created)
		})
	}
}

// textSite is one settable string-kinded location inside a record.
type textSite struct {
	path  string
	value reflect.Value
}

// enumerateTextSites is the test's own independent walk. It deliberately does
// not call the production validator: if both used one implementation, a blind
// spot in that implementation would hide itself.
func enumerateTextSites(value reflect.Value, path string, out *[]textSite) {
	switch value.Kind() {
	case reflect.String:
		if value.CanSet() {
			*out = append(*out, textSite{path: path, value: value})
		}
	case reflect.Pointer, reflect.Interface:
		if !value.IsNil() {
			enumerateTextSites(value.Elem(), path, out)
		}
	case reflect.Slice, reflect.Array:
		// A []byte is JSON text re-emitted verbatim, not a Go string the
		// marshaller can rewrite; it is covered by its own explicit case.
		if value.Kind() == reflect.Slice && value.Type().Elem().Kind() == reflect.Uint8 {
			return
		}
		for i := range value.Len() {
			enumerateTextSites(value.Index(i), fmt.Sprintf("%s[%d]", path, i), out)
		}
	case reflect.Struct:
		// time.Time carries a *Location whose interior is neither caller
		// supplied nor stored.
		if value.Type() == reflect.TypeOf(time.Time{}) {
			return
		}
		for i := range value.NumField() {
			name := value.Type().Field(i).Name
			enumerateTextSites(value.Field(i), path+"."+name, out)
		}
	}
}

// TestCatalogTextValidationCoversEveryStringField is the guard against a fourth
// round of this: it enumerates every settable string in a fully populated
// record and requires that corrupting any one of them is refused. A new text
// member added to this record, or to a sessionwire projection it embeds, fails
// here until something validates it.
//
// Some members are enums or identities whose own validator rejects the corrupt
// value before any text rule sees it. That is a covered field either way; this
// test asserts coverage, not which rule provides it.
func TestCatalogTextValidationCoversEveryStringField(t *testing.T) {
	count := func() int {
		record := richCatalogRecord()
		record.Binding = testSessionBinding()
		var sites []textSite
		enumerateTextSites(reflect.ValueOf(&record).Elem(), "record", &sites)
		return len(sites)
	}()
	// The count is exact rather than a floor. Slack here would let a regression
	// that dropped a text member from the record — or from a nested projection
	// the enumerator walks into — narrow this test's coverage without failing
	// it. Bump this number when the record legitimately gains or loses a text
	// member, and check that the member is validated when you do.
	const wantTextSites = 31
	if count != wantTextSites {
		t.Fatalf("the enumerator found %d text sites, want exactly %d; bump me when the record gains or loses a text member", count, wantTextSites)
	}
	for i := range count {
		record := richCatalogRecord()
		record.Binding = testSessionBinding()
		var sites []textSite
		enumerateTextSites(reflect.ValueOf(&record).Elem(), "record", &sites)
		site := sites[i]
		if _, err := encodeCatalogRecord(record); err != nil {
			t.Fatalf("%s: the unmodified fixture does not encode: %v", site.path, err)
		}
		site.value.SetString(site.value.String() + "\xff")
		if _, err := encodeCatalogRecord(record); err == nil {
			t.Errorf("%s accepts invalid UTF-8 and would be stored rewritten", site.path)
		}
	}
}

func TestCatalogRetainsAdditiveGateMembersAndRedactsObjectReference(t *testing.T) {
	valid, err := encodeCatalogRecord(richCatalogRecord())
	if err != nil {
		t.Fatalf("encodeCatalogRecord: %v", err)
	}
	// Additive members inside a sessionwire projection are retained and
	// re-emitted: core captures them for forward compatibility, and the
	// record-level strictness this package adds does not reach inside them.
	withExtra := strings.Replace(string(valid), `"gate_id":"gate-a"`, `"future_member":"kept","gate_id":"gate-a"`, 1)
	if withExtra == string(valid) {
		t.Fatal("could not inject an additive gate member")
	}
	record, err := decodeCatalogRecord([]byte(withExtra))
	if err != nil {
		t.Fatalf("an additive gate member was rejected: %v", err)
	}
	reencoded, err := encodeCatalogRecord(record)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if !strings.Contains(string(reencoded), `"future_member":"kept"`) {
		t.Fatalf("an additive gate member was dropped: %s", reencoded)
	}
	// An ObjectReference is core's declared redaction boundary, so an extra
	// member there is dropped rather than proxied.
	withLeak := strings.Replace(string(valid), `{"object_id":`, `{"provider_url":"https://leak.test/x","object_id":`, 1)
	if withLeak == string(valid) {
		t.Fatal("could not inject into the object reference")
	}
	leaked, err := decodeCatalogRecord([]byte(withLeak))
	if err != nil {
		t.Fatalf("object reference decode: %v", err)
	}
	out, err := encodeCatalogRecord(leaked)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if strings.Contains(string(out), "leak.test") {
		t.Fatalf("a provider URL was proxied through the redaction boundary: %s", out)
	}
	// A member undeclared at the RECORD level is still refused outright.
	withRecordExtra := strings.Replace(string(valid), `"record_version":1`, `"record_version":1,"future_record_member":1`, 1)
	if withRecordExtra == string(valid) {
		t.Fatal("could not inject a record member")
	}
	if _, err := decodeCatalogRecord([]byte(withRecordExtra)); err == nil {
		t.Fatal("an undeclared record member was accepted")
	} else {
		assertCatalogCode(t, err, CatalogErrorMalformed)
	}
}

// TestCatalogRawJSONTextValidityIsCoresRule pins the assumption that lets
// validateProjectionText skip json.RawMessage: this package does not check the
// text validity of raw JSON because core already refuses it on both paths that
// can carry any. If either half of this stops holding, the skip in the walk
// becomes a real gap and this test is where that surfaces.
func TestCatalogRawJSONTextValidityIsCoresRule(t *testing.T) {
	field := sessionwire.GatePromptField{
		Name:    "answer",
		Label:   "Answer",
		Kind:    sessionwire.GateFieldKindText,
		Default: json.RawMessage("\"a\xffb\""),
	}
	if err := field.Validate(); err == nil {
		t.Fatal("core now accepts an invalid-UTF-8 prompt default; the walk must check raw JSON itself")
	}

	valid, err := encodeCatalogRecord(richCatalogRecord())
	if err != nil {
		t.Fatalf("encodeCatalogRecord: %v", err)
	}
	injected := strings.Replace(string(valid), `"gate_id":"gate-a"`, "\"future_member\":\"a\xffb\",\"gate_id\":\"gate-a\"", 1)
	if injected == string(valid) {
		t.Fatal("could not inject an additive member")
	}
	if _, err := decodeCatalogRecord([]byte(injected)); err == nil {
		t.Fatal("an additive member carrying invalid UTF-8 was retained; the walk must check captured extensions itself")
	}
}

// TestStatusOfAGatelessRecordNamesNoGate drives Status() in the ORDINARY state
// of a session: not waiting on anything. Every other assertion about Status()
// uses a record with open gates, so the projection's one conditional was
// entered on every call and relaxing its bound to >= indexed an empty slice.
// The public claim is that WaitingGateID is empty rather than arbitrary, and
// Core's contract is what would reject a gate id on a running session.
func TestStatusOfAGatelessRecordNamesNoGate(t *testing.T) {
	t.Parallel()

	record := testCatalogRecord()
	record.State = sessionwire.SessionStateRunning
	record.OpenGates = nil

	status, err := record.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.WaitingGateID != "" {
		t.Fatalf("waiting gate = %q, want empty on a record with no open gates", status.WaitingGateID)
	}
}

// TestCanonicalGateOrderBreaksATieBySessionGateID pins the second half of the
// (opened_seq, gate_id) order Status() documents.
//
// Two gates CAN share an opening event — one journal record may open several —
// and until now nothing had two, so the tie-break was unpinned in both of its
// operators. What rests on it is the claim that two readers of the same record
// always name the same waiting gate: an unstable comparator makes
// WaitingGateID depend on the order the gates happened to arrive in, which is
// not a property of the record at all.
//
// Both input orders are driven, because a tie-break that merely SWAPS is
// correct for exactly one of them.
func TestCanonicalGateOrderBreaksATieBySessionGateID(t *testing.T) {
	t.Parallel()

	const shared = 5
	first, second := testGate("gate-a", shared), testGate("gate-b", shared)
	for name, input := range map[string][]sessionwire.GateProjection{
		"already in order": {first, second},
		"reversed":         {second, first},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			gates, err := canonicalGates(input)
			if err != nil {
				t.Fatalf("canonicalGates: %v", err)
			}
			if len(gates) != 2 || gates[0].GateID != "gate-a" || gates[1].GateID != "gate-b" {
				t.Fatalf("canonical order = %v, want gate-a then gate-b", gateProjectionIDs(gates))
			}
			// And the projection that rests on it names the first of the two.
			record := testCatalogRecord()
			record.OpenGates = input
			status, err := record.Status()
			if err != nil {
				t.Fatalf("Status: %v", err)
			}
			if status.WaitingGateID != "gate-a" {
				t.Fatalf("waiting gate = %q, want gate-a from the canonical order", status.WaitingGateID)
			}
		})
	}
}

func gateProjectionIDs(gates []sessionwire.GateProjection) []sessionwire.GateID {
	ids := make([]sessionwire.GateID, 0, len(gates))
	for _, gate := range gates {
		ids = append(ids, gate.GateID)
	}
	return ids
}

func TestCatalogNormalizesAnEmptyGateListToAbsent(t *testing.T) {
	store := openTestStore(t)
	mustCreateCatalog(t, store)
	// The normalization is asserted where it is observable. The encoded form
	// omits an empty list either way, so a round trip through the store cannot
	// distinguish nil from empty; canonicalGates is the one place that can, and
	// it is the function the early return belongs to.
	if got, err := canonicalGates([]sessionwire.GateProjection{}); err != nil || got != nil {
		t.Fatalf("canonicalGates(empty) = %#v, %v; want nil, nil so \"no open gates\" has one spelling", got, err)
	}
	req := testHostStateRequest(1)
	// An empty-but-present slice is a distinct Go value from nil and would
	// otherwise survive the clone, giving "no open gates" two spellings.
	req.OpenGates = []sessionwire.GateProjection{}
	written, err := store.UpdateCatalogHostState(context.Background(), req)
	if err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}
	if written.Record.OpenGates != nil {
		t.Fatalf("an empty gate list was returned as %#v, want nil", written.Record.OpenGates)
	}
	read, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: catalogSession})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	if read.Record.OpenGates != nil {
		t.Fatalf("a read returned %#v, want nil", read.Record.OpenGates)
	}
}

// --- bounded recent-first listing ----------------------------------------

const catalogOtherTenant = sessionwire.TenantID("tenant-b")

// mustCreateSession creates one catalog record for an explicit tenant, session,
// and recency. Nothing else varies, so a listing assertion reads as a statement
// about rank and tenancy alone.
func mustCreateSession(
	t *testing.T,
	store *Store,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	lastActive time.Time,
) {
	t.Helper()
	req := testCreateRequest()
	req.TenantID = tenant
	req.SessionID = session
	req.LastActiveAt = lastActive
	req.IdempotencyKey = "create-" + string(session)
	entry, created, err := store.CreateCatalogEntry(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateCatalogEntry(%s/%s): %v", tenant, session, err)
	}
	if !created {
		t.Fatalf("CreateCatalogEntry(%s/%s) reported an existing record", tenant, session)
	}
	if entry.Record.SessionID != session {
		t.Fatalf("created %q, want %q", entry.Record.SessionID, session)
	}
}

func listedSessionIDs(page SessionPage) []sessionwire.SessionID {
	ids := make([]sessionwire.SessionID, 0, len(page.Sessions))
	for _, summary := range page.Sessions {
		ids = append(ids, summary.SessionID)
	}
	return ids
}

// walkSessions follows cursors to exhaustion and returns every session id in
// page order. It bounds the walk so a cursor that fails to make progress fails
// the test instead of hanging it, which is the observable consequence of the
// sort key not being a total order.
func walkSessions(t *testing.T, store *Store, tenant sessionwire.TenantID, limit int) []sessionwire.SessionID {
	t.Helper()
	var ids []sessionwire.SessionID
	cursor := sessionwire.Cursor("")
	for pages := 0; ; pages++ {
		if pages > 32 {
			t.Fatalf("cursor walk did not terminate after %d pages: %v", pages, ids)
		}
		page, err := store.ListSessions(context.Background(), ListSessionsRequest{
			TenantID: tenant,
			Cursor:   cursor,
			Limit:    limit,
		})
		if err != nil {
			t.Fatalf("ListSessions page %d: %v", pages, err)
		}
		ids = append(ids, listedSessionIDs(page)...)
		if page.NextCursor == "" {
			return ids
		}
		cursor = page.NextCursor
	}
}

// openAuditedListStore wires a store whose OrderedIndex refuses every query a
// listing must not make and whose KV counts prefix scans.
func openAuditedListStore(t *testing.T, opts ...Option) (*Store, *listAuditOrdered, *keysCountingKV) {
	t.Helper()
	base := memstore.New()
	audit := &listAuditOrdered{OrderedIndex: base.OrderedIndex, t: t}
	base.OrderedIndex = audit
	kv := &keysCountingKV{KV: base.KV}
	base.KV = kv
	return openStore(t, base, opts...), audit, kv
}

func openHostileListStore(t *testing.T) (*Store, *hostileOrdered) {
	t.Helper()
	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	return openStore(t, base), hostile
}

func mustListSessions(t *testing.T, store *Store, req ListSessionsRequest) SessionPage {
	t.Helper()
	page, err := store.ListSessions(context.Background(), req)
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	return page
}

// TestListSessionsUsesTenantRankBeforeLimit is the central claim of the listing:
// the tenant restriction and the recency order are both part of the one
// provider query, and the limit applies to that already-restricted, already
// ordered result. The fixture makes the other tenant's sessions the most recent
// in the whole store, so an implementation that ranked globally and filtered
// afterwards would return an empty or short page rather than this tenant's two
// newest sessions.
func TestListSessionsUsesTenantRankBeforeLimit(t *testing.T) {
	store, audit, kv := openAuditedListStore(t)
	base := catalogActiveAt
	mustCreateSession(t, store, catalogTenant, "session-a1", base)
	mustCreateSession(t, store, catalogTenant, "session-a2", base.Add(2*time.Minute))
	mustCreateSession(t, store, catalogTenant, "session-a3", base.Add(time.Minute))
	mustCreateSession(t, store, catalogOtherTenant, "session-b1", base.Add(time.Hour))
	mustCreateSession(t, store, catalogOtherTenant, "session-b2", base.Add(2*time.Hour))

	scope, err := store.deriveSessionScope(catalogTenant, "session-a1")
	if err != nil {
		t.Fatal(err)
	}
	audit.arm(scope.CatalogScope)

	page := mustListSessions(t, store, ListSessionsRequest{TenantID: catalogTenant, Limit: 2})
	want := []sessionwire.SessionID{"session-a2", "session-a3"}
	if got := listedSessionIDs(page); !reflect.DeepEqual(got, want) {
		t.Fatalf("sessions = %v, want %v", got, want)
	}
	if page.NextCursor == "" {
		t.Fatal("a truncated page returned no continuation cursor")
	}

	calls := audit.rankedCalls()
	if len(calls) != 1 {
		t.Fatalf("ListRanked calls = %d, want exactly one", len(calls))
	}
	call := calls[0]
	if call.namespace != catalogNamespace {
		t.Fatalf("namespace = %q, want %q", call.namespace, catalogNamespace)
	}
	if call.limit != 2 {
		t.Fatalf("provider limit = %d, want the requested 2: a listing must not over-fetch and narrow afterwards", call.limit)
	}
	if call.records != len(page.Sessions) {
		t.Fatalf("provider returned %d records and the page carried %d: a listing must not drop rows the provider selected",
			call.records, len(page.Sessions))
	}
	if kv.keys.Load() != 0 {
		t.Fatalf("a listing scanned KV keys %d times", kv.keys.Load())
	}
}

// TestListSessionsNeverReturnsAnotherTenantsSessions walks both tenants to
// exhaustion, which is the strongest available statement of separation: every
// session appears in exactly one tenant's pages.
func TestListSessionsNeverReturnsAnotherTenantsSessions(t *testing.T) {
	store := openTestStore(t)
	base := catalogActiveAt
	mustCreateSession(t, store, catalogTenant, "session-a1", base)
	mustCreateSession(t, store, catalogTenant, "session-a2", base.Add(time.Minute))
	mustCreateSession(t, store, catalogOtherTenant, "session-b1", base.Add(time.Hour))

	gotA := walkSessions(t, store, catalogTenant, 1)
	wantA := []sessionwire.SessionID{"session-a2", "session-a1"}
	if !reflect.DeepEqual(gotA, wantA) {
		t.Fatalf("tenant-a sessions = %v, want %v", gotA, wantA)
	}
	gotB := walkSessions(t, store, catalogOtherTenant, 1)
	wantB := []sessionwire.SessionID{"session-b1"}
	if !reflect.DeepEqual(gotB, wantB) {
		t.Fatalf("tenant-b sessions = %v, want %v", gotB, wantB)
	}
}

// TestListSessionsBreaksRankTiesWithoutRepeatingOrSkipping pins that equal
// recency still paginates. The tie-break itself belongs to the provider's
// frozen (rank, stable_key, ordering_scope) order, so this asserts the property
// that order buys — a total order over a stable page walk — rather than
// restating the comparator.
func TestListSessionsBreaksRankTiesWithoutRepeatingOrSkipping(t *testing.T) {
	store := openTestStore(t)
	tied := catalogActiveAt
	ids := []sessionwire.SessionID{"session-a1", "session-a2", "session-a3"}
	for _, id := range ids {
		mustCreateSession(t, store, catalogTenant, id, tied)
	}

	walked := walkSessions(t, store, catalogTenant, 1)
	if len(walked) != len(ids) {
		t.Fatalf("walk returned %v, want %d sessions exactly once", walked, len(ids))
	}
	seen := map[sessionwire.SessionID]int{}
	for _, id := range walked {
		seen[id]++
	}
	for _, id := range ids {
		if seen[id] != 1 {
			t.Fatalf("%s appeared %d times in %v", id, seen[id], walked)
		}
	}
	if again := walkSessions(t, store, catalogTenant, 1); !reflect.DeepEqual(again, walked) {
		t.Fatalf("a tied walk is not deterministic: %v then %v", walked, again)
	}
}

// TestListSessionsIncludesASessionNoHostHasWritten pins that listing reads the
// record Factory created rather than one a Host has since claimed. A session
// that has never been leased has epoch zero and its creation-time state, and it
// is exactly the session a picker most needs to see.
func TestListSessionsIncludesASessionNoHostHasWritten(t *testing.T) {
	store := openTestStore(t)
	mustCreateSession(t, store, catalogTenant, catalogSession, catalogActiveAt)

	page := mustListSessions(t, store, ListSessionsRequest{TenantID: catalogTenant, Limit: 10})
	if len(page.Sessions) != 1 {
		t.Fatalf("sessions = %v, want the one created session", listedSessionIDs(page))
	}
	summary := page.Sessions[0]
	if summary.SessionID != catalogSession || summary.AgentID != "agent-a" {
		t.Fatalf("summary identity = %+v", summary)
	}
	if summary.State != sessionwire.SessionStateIdle {
		t.Fatalf("state = %q, want the creation state %q", summary.State, sessionwire.SessionStateIdle)
	}
	if !summary.LastActiveAt.Equal(catalogActiveAt) {
		t.Fatalf("last active = %v, want %v", summary.LastActiveAt, catalogActiveAt)
	}
	if page.NextCursor != "" {
		t.Fatalf("an exhausted page returned a continuation cursor %q", page.NextCursor)
	}
}

// TestListSessionsSurvivesARankMoveBetweenPages exercises the contract's stated
// weakness: pagination resumes from a frozen (rank, stable_key, ordering_scope)
// tuple, so a record whose rank moves across that position between pages is
// repeated or skipped. SessionStore must return what the provider selected and
// must not try to repair the view by buffering or re-sorting — a repair would
// require holding the whole catalog. What it does owe is that the walk still
// terminates and every page is well formed.
func TestListSessionsSurvivesARankMoveBetweenPages(t *testing.T) {
	store, audit, _ := openAuditedListStore(t)
	base := catalogActiveAt
	mustCreateSession(t, store, catalogTenant, "session-a1", base)
	mustCreateSession(t, store, catalogTenant, "session-a2", base.Add(time.Minute))
	mustCreateSession(t, store, catalogTenant, "session-a3", base.Add(2*time.Minute))

	first := mustListSessions(t, store, ListSessionsRequest{TenantID: catalogTenant, Limit: 1})
	if got := listedSessionIDs(first); !reflect.DeepEqual(got, []sessionwire.SessionID{"session-a3"}) {
		t.Fatalf("first page = %v, want the newest session", got)
	}
	if first.NextCursor == "" {
		t.Fatal("first page of three returned no cursor")
	}

	// Move the oldest session ahead of the frozen cursor position.
	moved := testHostStateRequest(1)
	moved.SessionID = "session-a1"
	moved.LastActiveAt = base.Add(time.Hour)
	if _, err := store.UpdateCatalogHostState(context.Background(), moved); err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}

	scope, err := store.deriveSessionScope(catalogTenant, "session-a1")
	if err != nil {
		t.Fatal(err)
	}
	audit.arm(scope.CatalogScope)

	cursor := first.NextCursor
	for pages := 0; cursor != ""; pages++ {
		if pages > 8 {
			t.Fatal("a rank move made the cursor walk fail to terminate")
		}
		page, err := store.ListSessions(context.Background(), ListSessionsRequest{
			TenantID: catalogTenant,
			Cursor:   cursor,
			Limit:    1,
		})
		if err != nil {
			t.Fatalf("ListSessions after a rank move: %v", err)
		}
		if err := page.Validate(); err != nil {
			t.Fatalf("a page returned after a rank move is not well formed: %v", err)
		}
		cursor = page.NextCursor
	}
	for _, call := range audit.rankedCalls() {
		if call.records > call.limit {
			t.Fatalf("a listing asked the provider for %d records under a limit of %d", call.records, call.limit)
		}
	}
}

// --- listing cursors ------------------------------------------------------

// splitCatalogCursor returns the envelope header and the provider token a
// SessionStore listing cursor carries. Tests recombine real halves rather than
// writing a token literal: the payload is the provider's own opaque encoding,
// and a literal would test one provider's grammar instead of this package's
// binding.
func splitCatalogCursor(t *testing.T, cursor sessionwire.Cursor) (header, payload []byte) {
	t.Helper()
	token, err := base64.RawURLEncoding.DecodeString(string(cursor))
	if err != nil {
		t.Fatalf("a cursor this store issued is not base64url: %v", err)
	}
	if len(token) <= cursorPayloadAt {
		t.Fatalf("cursor is %d bytes, want more than the %d-byte envelope", len(token), cursorPayloadAt)
	}
	return token[:cursorPayloadAt:cursorPayloadAt], token[cursorPayloadAt:]
}

func joinCatalogCursor(header, payload []byte) sessionwire.Cursor {
	return sessionwire.Cursor(base64.RawURLEncoding.EncodeToString(append(append([]byte(nil), header...), payload...)))
}

// mustTruncatedCursor lists one tenant with a limit small enough to truncate and
// returns the continuation cursor the store issued.
func mustTruncatedCursor(t *testing.T, store *Store, tenant sessionwire.TenantID) sessionwire.Cursor {
	t.Helper()
	page := mustListSessions(t, store, ListSessionsRequest{TenantID: tenant, Limit: 1})
	if page.NextCursor == "" {
		t.Fatalf("tenant %s did not produce a continuation cursor", tenant)
	}
	return page.NextCursor
}

func seedTwoTenants(t *testing.T, store *Store) {
	t.Helper()
	base := catalogActiveAt
	mustCreateSession(t, store, catalogTenant, "session-a1", base)
	mustCreateSession(t, store, catalogTenant, "session-a2", base.Add(time.Minute))
	mustCreateSession(t, store, catalogOtherTenant, "session-b1", base.Add(time.Hour))
	mustCreateSession(t, store, catalogOtherTenant, "session-b2", base.Add(2*time.Hour))
}

// TestListSessionsRejectsACursorIssuedForAnotherTenant is the ordinary
// cross-tenant case over a conforming provider.
func TestListSessionsRejectsACursorIssuedForAnotherTenant(t *testing.T) {
	store := openTestStore(t)
	seedTwoTenants(t, store)
	cursor := mustTruncatedCursor(t, store, catalogOtherTenant)

	_, err := store.ListSessions(context.Background(), ListSessionsRequest{
		TenantID: catalogTenant,
		Cursor:   cursor,
		Limit:    1,
	})
	if err == nil {
		t.Fatal("a cursor issued for another tenant was accepted")
	}
	assertCatalogCode(t, err, CatalogErrorCursor)
}

// TestListSessionsBindsItsCursorToTheTenantEvenIfTheProviderDoesNot is the same
// claim against a provider that does not bind its own cursors. The conforming
// provider above would reject the token on its own, so that test alone cannot
// distinguish SessionStore's binding from the provider's; this one can, because
// here nothing else is checking.
func TestListSessionsBindsItsCursorToTheTenantEvenIfTheProviderDoesNot(t *testing.T) {
	backend := memstore.New()
	backend.OrderedIndex = permissiveRankedOrdered{OrderedIndex: backend.OrderedIndex}
	store := openStore(t, backend)
	seedTwoTenants(t, store)
	cursor := mustTruncatedCursor(t, store, catalogOtherTenant)

	_, err := store.ListSessions(context.Background(), ListSessionsRequest{
		TenantID: catalogTenant,
		Cursor:   cursor,
		Limit:    1,
	})
	if err == nil {
		t.Fatal("a cursor issued for another tenant was accepted against a provider that does not bind its own")
	}
	assertCatalogCode(t, err, CatalogErrorCursor)
}

// TestListSessionsRejectsAForgedCursorPayload forges the body rather than the
// header: the envelope names this tenant and carries this version, and only the
// provider token inside it comes from another query. The envelope check passes,
// so the rejection can only come from the provider being handed a token that
// does not bind to the query SessionStore then asked it to resume.
func TestListSessionsRejectsAForgedCursorPayload(t *testing.T) {
	store := openTestStore(t)
	seedTwoTenants(t, store)
	mine := mustTruncatedCursor(t, store, catalogTenant)
	theirs := mustTruncatedCursor(t, store, catalogOtherTenant)

	header, payload := splitCatalogCursor(t, mine)
	_, foreignPayload := splitCatalogCursor(t, theirs)
	if bytes.Equal(payload, foreignPayload) {
		t.Fatal("the two tenants' provider tokens are identical; the forgery would be a no-op")
	}
	forged := joinCatalogCursor(header, foreignPayload)
	if forged == mine {
		t.Fatal("the forged cursor equals the original")
	}

	_, err := store.ListSessions(context.Background(), ListSessionsRequest{
		TenantID: catalogTenant,
		Cursor:   forged,
		Limit:    1,
	})
	if err == nil {
		t.Fatal("a cursor carrying another query's provider token was accepted")
	}
	assertCatalogCode(t, err, CatalogErrorCursor)
}

// TestListSessionsRejectsACursorEnvelopeItDidNotIssue rewrites the header while
// leaving a genuine provider token in place, so each rejection is attributable
// to the envelope field it names.
func TestListSessionsRejectsACursorEnvelopeItDidNotIssue(t *testing.T) {
	store := openTestStore(t)
	seedTwoTenants(t, store)
	valid := mustTruncatedCursor(t, store, catalogTenant)
	header, payload := splitCatalogCursor(t, valid)

	rewrite := func(mutate func([]byte)) sessionwire.Cursor {
		copied := append([]byte(nil), header...)
		mutate(copied)
		return joinCatalogCursor(copied, payload)
	}
	journal := store.encodeJournalCursor(journalCursorPublic, catalogTenant, catalogSession, 1, 1)

	for name, cursor := range map[string]sessionwire.Cursor{
		"unknown version": rewrite(func(header []byte) { header[cursorVersionAt]++ }),
		"foreign magic":   rewrite(func(header []byte) { copy(header[cursorMagicAt:], "XXXX") }),
		"foreign scope":   rewrite(func(header []byte) { header[cursorScopeAt]++ }),
		"empty payload":   joinCatalogCursor(header, nil),
		"journal cursor":  journal,
	} {
		t.Run(name, func(t *testing.T) {
			if cursor == valid {
				t.Fatal("the mutation did not change the cursor")
			}
			_, err := store.ListSessions(context.Background(), ListSessionsRequest{
				TenantID: catalogTenant,
				Cursor:   cursor,
				Limit:    1,
			})
			if err == nil {
				t.Fatal("an unissued cursor was accepted")
			}
			assertCatalogCode(t, err, CatalogErrorCursor)
		})
	}
}

// TestListSessionsBoundsACursorBeforeDecodingIt pins the length gate as an
// allocation precondition rather than a restatement of the envelope checks. A
// decoder sizes its destination from the caller's string, so an unbounded token
// would make this reader allocate in proportion to attacker-supplied input. The
// assertion that no provider query was made is what distinguishes the gate from
// the rejection that would otherwise happen further down.
func TestListSessionsBoundsACursorBeforeDecodingIt(t *testing.T) {
	store, audit, _ := openAuditedListStore(t)
	seedTwoTenants(t, store)
	valid := mustTruncatedCursor(t, store, catalogTenant)
	header, payload := splitCatalogCursor(t, valid)
	oversized := joinCatalogCursor(header, append(payload, bytes.Repeat([]byte{'x'}, maxCatalogCursorBytes)...))

	scope, err := store.deriveSessionScope(catalogTenant, "session-a1")
	if err != nil {
		t.Fatal(err)
	}
	audit.arm(scope.CatalogScope)
	before := len(audit.rankedCalls())

	if _, err := store.ListSessions(context.Background(), ListSessionsRequest{
		TenantID: catalogTenant,
		Cursor:   oversized,
		Limit:    1,
	}); err == nil {
		t.Fatal("an oversized cursor was accepted")
	} else {
		assertCatalogCode(t, err, CatalogErrorCursor)
	}
	if got := len(audit.rankedCalls()) - before; got != 0 {
		t.Fatalf("an oversized cursor reached the provider %d times; it must be refused before it is decoded", got)
	}
}

// --- listing fail-closed paths -------------------------------------------

// TestListSessionsHoldsEveryRecordToTheRequestedIdentity covers both halves of
// the identity check a listed record must pass: the tenant it claims and the
// stable key it was stored under.
//
// The poisoned row is neither first nor last in the page, so this also proves
// the check runs for EVERY row rather than for the first one. What it no longer
// asserts is a returned error: a row that fails is stepped over and counted,
// because failing the page would make the tenant permanently unlistable and
// would take every session ranked behind the poisoned row with it. See
// TestListSessionsStepsOverARowItCannotRead for that argument in full.
func TestListSessionsHoldsEveryRecordToTheRequestedIdentity(t *testing.T) {
	const poisonedRow = 1
	for name, corrupt := range map[string]struct{ from, to string }{
		"foreign tenant": {`"tenant_id":"tenant-a"`, `"tenant_id":"tenant-z"`},
		"foreign key":    {`"session_id":"session-a2"`, `"session_id":"session-z"`},
	} {
		t.Run(name, func(t *testing.T) {
			store, hostile := openHostileListStore(t)
			// Ranked descending, so the page is a3, a2, a1 and the poisoned
			// row is a2 in the middle.
			mustCreateSession(t, store, catalogTenant, "session-a1", catalogActiveAt)
			mustCreateSession(t, store, catalogTenant, "session-a2", catalogActiveAt.Add(time.Minute))
			mustCreateSession(t, store, catalogTenant, "session-a3", catalogActiveAt.Add(2*time.Minute))
			hostile.answerRanked(func(page storage.RankedPage, err error) (storage.RankedPage, error) {
				if err != nil {
					return page, err
				}
				if len(page.Records) != 3 {
					t.Fatalf("provider returned %d records, want 3", len(page.Records))
				}
				record := &page.Records[poisonedRow]
				rewritten := bytes.Replace(record.Value, []byte(corrupt.from), []byte(corrupt.to), 1)
				if bytes.Equal(rewritten, record.Value) {
					t.Fatalf("could not corrupt %s in %s", name, record.Value)
				}
				record.Value = rewritten
				return page, nil
			})

			page := mustListSessions(t, store, ListSessionsRequest{TenantID: catalogTenant, Limit: 10})
			if page.UnreadableSkipped != 1 {
				t.Fatalf("unreadable = %d, want the one poisoned row", page.UnreadableSkipped)
			}
			for _, id := range listedSessionIDs(page) {
				if id == "session-a2" || id == "session-z" {
					t.Fatalf("a record this reader could not vouch for was published: %v", listedSessionIDs(page))
				}
			}
		})
	}
}

// TestListSessionsRejectsAnOutOfOrderProviderPage holds the provider to the
// descending order the page shape promises. A caller reads a SessionPage as
// recent-first, so a page that is not is refused rather than published.
func TestListSessionsRejectsAnOutOfOrderProviderPage(t *testing.T) {
	store, hostile := openHostileListStore(t)
	mustCreateSession(t, store, catalogTenant, "session-a1", catalogActiveAt)
	mustCreateSession(t, store, catalogTenant, "session-a2", catalogActiveAt.Add(time.Minute))
	hostile.answerRanked(func(page storage.RankedPage, err error) (storage.RankedPage, error) {
		if err != nil || len(page.Records) < 2 {
			return page, err
		}
		slices.Reverse(page.Records)
		return page, nil
	})

	_, err := store.ListSessions(context.Background(), ListSessionsRequest{TenantID: catalogTenant, Limit: 10})
	if err == nil {
		t.Fatal("an ascending page was published as a recent-first page")
	}
	assertCatalogCode(t, err, CatalogErrorBackend)
}

// TestListSessionsClassifiesProviderFailures maps the outcomes only the
// provider can produce into the catalog vocabulary.
func TestListSessionsClassifiesProviderFailures(t *testing.T) {
	for name, tt := range map[string]struct {
		err  error
		want CatalogErrorCode
	}{
		"invalid cursor": {
			err:  storage.NewInvalidOrderedCursorError(storage.RankedCursorKind, "token", storage.OrderedCursorQueryMismatch),
			want: CatalogErrorCursor,
		},
		"unavailable": {err: errors.New("provider is unavailable"), want: CatalogErrorBackend},
	} {
		t.Run(name, func(t *testing.T) {
			store, hostile := openHostileListStore(t)
			mustCreateSession(t, store, catalogTenant, "session-a1", catalogActiveAt)
			hostile.answerRanked(func(storage.RankedPage, error) (storage.RankedPage, error) {
				return storage.RankedPage{}, tt.err
			})
			_, err := store.ListSessions(context.Background(), ListSessionsRequest{TenantID: catalogTenant, Limit: 10})
			got := assertCatalogCode(t, err, tt.want)
			if !errors.Is(err, tt.err) {
				t.Fatalf("the provider cause was not preserved: %v", got)
			}
		})
	}
}

// TestListSessionsRejectsAnInvalidRequest covers the inputs refused before any
// provider work happens.
func TestListSessionsRejectsAnInvalidRequest(t *testing.T) {
	store, audit, _ := openAuditedListStore(t)
	mustCreateSession(t, store, catalogTenant, "session-a1", catalogActiveAt)
	scope, err := store.deriveSessionScope(catalogTenant, "session-a1")
	if err != nil {
		t.Fatal(err)
	}
	audit.arm(scope.CatalogScope)

	for name, req := range map[string]ListSessionsRequest{
		"negative limit":  {TenantID: catalogTenant, Limit: -1},
		"limit above max": {TenantID: catalogTenant, Limit: storage.MaxOrderedPageLimit + 1},
	} {
		t.Run(name, func(t *testing.T) {
			before := len(audit.rankedCalls())
			_, err := store.ListSessions(context.Background(), req)
			if err == nil {
				t.Fatal("an invalid request was accepted")
			}
			got := assertCatalogCode(t, err, CatalogErrorInvalid)
			if got.Field != "limit" {
				t.Fatalf("field = %q, want %q", got.Field, "limit")
			}
			if reached := len(audit.rankedCalls()) - before; reached != 0 {
				t.Fatalf("an invalid request reached the provider %d times", reached)
			}
		})
	}

	t.Run("invalid tenant", func(t *testing.T) {
		before := len(audit.rankedCalls())
		_, err := store.ListSessions(context.Background(), ListSessionsRequest{TenantID: "", Limit: 1})
		var invalid *InvalidIdentityError
		if !errors.As(err, &invalid) {
			t.Fatalf("error = %T %v, want *InvalidIdentityError", err, err)
		}
		if invalid.Field != "TenantID" {
			t.Fatalf("field = %q, want TenantID", invalid.Field)
		}
		if reached := len(audit.rankedCalls()) - before; reached != 0 {
			t.Fatalf("an invalid tenant reached the provider %d times", reached)
		}
	})
}

// TestListSessionsDefaultsAnUnsetLimitToTheStoreCeiling pins that zero means
// "the store's page size" rather than "no records".
func TestListSessionsDefaultsAnUnsetLimitToTheStoreCeiling(t *testing.T) {
	store, audit, _ := openAuditedListStore(t, WithLimits(Limits{MaxPageSize: 7}))
	mustCreateSession(t, store, catalogTenant, "session-a1", catalogActiveAt)
	scope, err := store.deriveSessionScope(catalogTenant, "session-a1")
	if err != nil {
		t.Fatal(err)
	}
	audit.arm(scope.CatalogScope)

	page := mustListSessions(t, store, ListSessionsRequest{TenantID: catalogTenant})
	if len(page.Sessions) != 1 {
		t.Fatalf("sessions = %v, want the one created session", listedSessionIDs(page))
	}
	calls := audit.rankedCalls()
	if len(calls) != 1 || calls[0].limit != 7 {
		t.Fatalf("provider calls = %+v, want one call with the store page size 7", calls)
	}
}

// TestListSessionsOfAnUnusedTenantIsEmpty pins the observable half of the
// no-witness decision: an unused tenant answers with an empty page rather than
// a binding failure, and touches KV not at all. The reasoning for the decision
// lives on ListSessions itself, where a caller reads it.
func TestListSessionsOfAnUnusedTenantIsEmpty(t *testing.T) {
	store, _, kv := openAuditedListStore(t)
	mustCreateSession(t, store, catalogOtherTenant, "session-b1", catalogActiveAt)

	page := mustListSessions(t, store, ListSessionsRequest{TenantID: catalogTenant, Limit: 10})
	if len(page.Sessions) != 0 {
		t.Fatalf("sessions = %v, want none", listedSessionIDs(page))
	}
	if page.NextCursor != "" {
		t.Fatalf("an empty page returned a cursor %q", page.NextCursor)
	}
	if kv.keys.Load() != 0 {
		t.Fatalf("a listing scanned KV keys %d times", kv.keys.Load())
	}
}

// TestListSessionsRejectsANonCanonicalCursorSpelling covers the slack in
// unpadded base64: the low bits of the final group are dropped, so several
// distinct strings decode to identical bytes. Accepting more than one of them
// would give a single page position several names and let a token be perturbed
// while still resuming, so the reader requires the exact spelling it emits.
//
// The variants are found by re-spelling a cursor this store issued rather than
// written down, and the test insists it found at least one so it cannot quietly
// stop exercising the check.
func TestListSessionsRejectsANonCanonicalCursorSpelling(t *testing.T) {
	store := openTestStore(t)
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

	variants := 0
	for size := 1; size <= 3; size++ {
		cursor, err := store.encodeCatalogCursor(catalogTenant, storage.RankedCursor(strings.Repeat("t", size)))
		if err != nil {
			t.Fatalf("encodeCatalogCursor: %v", err)
		}
		if _, err := store.decodeCatalogCursor(catalogTenant, cursor); err != nil {
			t.Fatalf("the store rejected a cursor it issued: %v", err)
		}
		want, err := base64.RawURLEncoding.DecodeString(string(cursor))
		if err != nil {
			t.Fatal(err)
		}
		for _, letter := range alphabet {
			respelled := string(cursor[:len(cursor)-1]) + string(letter)
			if respelled == string(cursor) {
				continue
			}
			got, err := base64.RawURLEncoding.DecodeString(respelled)
			if err != nil || !bytes.Equal(got, want) {
				continue
			}
			variants++
			if _, err := store.decodeCatalogCursor(catalogTenant, sessionwire.Cursor(respelled)); err == nil {
				t.Fatalf("a second spelling of one position was accepted: %q and %q", cursor, respelled)
			} else {
				assertCatalogCode(t, err, CatalogErrorCursor)
			}
		}
	}
	if variants == 0 {
		t.Fatal("no alternate spelling was found, so this test exercised nothing")
	}
}

// TestListSessionsRefusesToIssueACursorItCouldNotAccept holds the two halves of
// the cursor bound together. The payload is a provider token whose length this
// package does not control, so a provider that issued one larger than the bound
// would otherwise be handed back a page cursor that this same store refuses on
// presentation — a walk that dead-ends with no way for a caller to tell why.
func TestListSessionsRefusesToIssueACursorItCouldNotAccept(t *testing.T) {
	store, hostile := openHostileListStore(t)
	mustCreateSession(t, store, catalogTenant, "session-a1", catalogActiveAt)
	hostile.answerRanked(func(page storage.RankedPage, err error) (storage.RankedPage, error) {
		if err != nil {
			return page, err
		}
		page.NextCursor = storage.RankedCursor(strings.Repeat("t", maxCatalogCursorBytes))
		return page, nil
	})

	_, err := store.ListSessions(context.Background(), ListSessionsRequest{TenantID: catalogTenant, Limit: 10})
	if err == nil {
		t.Fatal("a cursor larger than this store will accept was issued to a caller")
	}
	got := assertCatalogCode(t, err, CatalogErrorBackend)
	if got.Field != "next_cursor" {
		t.Fatalf("field = %q, want next_cursor", got.Field)
	}
}

// TestListSessionsUnderTheLegacyLayout drives the legacy arm of the tenant
// scope derivation directly. A legacy backend authorizes exactly one tenant, so
// its whole catalog is the single ordering and ranking scope "sessions" rather
// than a per-tenant namespace, and a listing must still be one ranked query in
// that scope. The arm is reachable from a listing, which has no SessionID, so
// covering it only through deriveSessionScope would leave the path a listing
// actually takes unexercised.
func TestListSessionsUnderTheLegacyLayout(t *testing.T) {
	const legacyTenant = sessionwire.TenantID("local")
	const (
		olderSession = sessionwire.SessionID("11111111-1111-1111-1111-111111111111")
		newerSession = sessionwire.SessionID("22222222-2222-2222-2222-222222222222")
	)
	store, audit, kv := openAuditedListStore(t, WithLegacySingleTenant(legacyTenant))
	mustCreateSession(t, store, legacyTenant, olderSession, catalogActiveAt)
	mustCreateSession(t, store, legacyTenant, newerSession, catalogActiveAt.Add(time.Hour))
	audit.arm(legacyCatalogScope)

	page := mustListSessions(t, store, ListSessionsRequest{TenantID: legacyTenant, Limit: 10})
	want := []sessionwire.SessionID{newerSession, olderSession}
	if got := listedSessionIDs(page); !reflect.DeepEqual(got, want) {
		t.Fatalf("sessions = %v, want %v", got, want)
	}
	calls := audit.rankedCalls()
	if len(calls) != 1 {
		t.Fatalf("ListRanked calls = %d, want exactly one", len(calls))
	}
	if calls[0].rankingScope != legacyCatalogScope {
		t.Fatalf("ranking scope = %q, want %q", calls[0].rankingScope, legacyCatalogScope)
	}
	if kv.keys.Load() != 0 {
		t.Fatalf("a legacy listing scanned KV keys %d times", kv.keys.Load())
	}

	// A legacy backend authorizes one tenant and no other, and the refusal is
	// the layout's, not a "this tenant has no sessions" empty page.
	before := len(audit.rankedCalls())
	_, err := store.ListSessions(context.Background(), ListSessionsRequest{TenantID: catalogTenant, Limit: 10})
	if err == nil {
		t.Fatal("a legacy backend listed a tenant it does not authorize")
	}
	assertKeyspaceCode(t, err, KeyspaceLegacyTenant)
	if reached := len(audit.rankedCalls()) - before; reached != 0 {
		t.Fatalf("an unauthorized tenant reached the provider %d times", reached)
	}
}

// TestListSessionsStepsOverARowItCannotRead is the reachability property a
// paged listing has to have. A row this build cannot hold to its own identity
// stays in the tenant's ranked scope forever and nothing in this package
// rewrites it, so a reader that failed the page on one would make the tenant
// permanently unlistable — and, because a failed page issues no continuation,
// every session ranked behind that row unreachable with it.
//
// The count is what keeps the condition visible rather than hidden, which is
// the objection that kept this reader failing closed: a caller can distinguish
// "this tenant has three sessions" from "this tenant has three sessions and one
// row I could not vouch for".
func TestListSessionsStepsOverARowItCannotRead(t *testing.T) {
	const poisonedRow = 1
	for name, corrupt := range map[string]struct{ from, to string }{
		"foreign tenant":  {`"tenant_id":"tenant-a"`, `"tenant_id":"tenant-z"`},
		"foreign key":     {`"session_id":"session-a2"`, `"session_id":"session-z"`},
		"unknown version": {`"record_version":1`, `"record_version":2`},
	} {
		t.Run(name, func(t *testing.T) {
			store, hostile := openHostileListStore(t)
			mustCreateSession(t, store, catalogTenant, "session-a1", catalogActiveAt)
			mustCreateSession(t, store, catalogTenant, "session-a2", catalogActiveAt.Add(time.Minute))
			mustCreateSession(t, store, catalogTenant, "session-a3", catalogActiveAt.Add(2*time.Minute))
			hostile.answerRanked(func(page storage.RankedPage, err error) (storage.RankedPage, error) {
				if err != nil {
					return page, err
				}
				if len(page.Records) != 3 {
					t.Fatalf("provider returned %d records, want 3", len(page.Records))
				}
				record := &page.Records[poisonedRow]
				rewritten := bytes.Replace(record.Value, []byte(corrupt.from), []byte(corrupt.to), 1)
				if bytes.Equal(rewritten, record.Value) {
					t.Fatalf("could not corrupt %s in %s", name, record.Value)
				}
				record.Value = rewritten
				return page, nil
			})

			page := mustListSessions(t, store, ListSessionsRequest{TenantID: catalogTenant, Limit: 10})
			got := listedSessionIDs(page)
			if len(got) != 2 || got[0] != "session-a3" || got[1] != "session-a1" {
				t.Fatalf("sessions = %v, want the two rows this reader can vouch for", got)
			}
			if page.UnreadableSkipped != 1 {
				t.Fatalf("unreadable = %d, want 1", page.UnreadableSkipped)
			}
		})
	}
}

// TestEpochFenceIsAHighWaterMark guards the shared fence itself, which sharing
// alone does not.
//
// Three records fenced Host-owned writes with three byte-identical comparisons
// differing only in the error they built, and each of the three was free to
// drift on the one path that only runs when a Host has already been superseded.
// Hoisting them removes the drift and leaves the machinery unasserted — a fence
// that returned nil unconditionally would pass every caller's rejection test
// that only checks the ADMITTED direction, and one that passed the REQUESTED
// epoch to its failure constructor would pass every test that only checks that
// some refusal occurred. So the property is asserted positively here, once, on
// behalf of all three:
//
//   - Equal is admitted, because one grant writes many times.
//   - Higher is admitted.
//   - Strictly lower is refused, and the refusal is built from the COMMITTED
//     mark, which is the only value a caller can act on.
//
// The three vocabularies are then driven through their own fences, so a record
// that stops routing through the shared rule fails here rather than silently
// keeping a private copy.
func TestEpochFenceIsAHighWaterMark(t *testing.T) {
	t.Parallel()

	const committed = 7
	for _, requested := range []uint64{committed, committed + 1, math.MaxUint64} {
		if err := epochFence(committed, requested, func(uint64) error {
			return errors.New("refused")
		}); err != nil {
			t.Errorf("epoch %d against a committed %d was refused", requested, committed)
		}
	}
	var reported uint64
	err := epochFence(committed, committed-1, func(mark uint64) error {
		reported = mark
		return errors.New("refused")
	})
	if err == nil {
		t.Fatal("a strictly lower epoch was admitted")
	}
	if reported != committed {
		t.Fatalf("the refusal was built from %d; it must be built from the committed mark %d", reported, committed)
	}

	// Every record that fences a Host-owned write, driven through its own
	// spelling of the rule. Each reports the committed mark in its own type.
	//
	// The set is exhaustive and must stay so: a fifth record that fences a
	// Host-owned write and does not route through epochFence is a fifth copy,
	// which is what this hoist exists to prevent and what a row here is the
	// cheapest way to notice.
	fences := map[string]struct {
		refuse func(uint64) error
		mark   func(error) uint64
	}{
		"catalog": {
			refuse: func(epoch uint64) error {
				return hostEpochFence(CatalogRecord{LeaseEpoch: committed}, epoch)
			},
			mark: func(err error) uint64 {
				var typed *CatalogError
				if !errors.As(err, &typed) || typed.Code != CatalogErrorEpoch {
					t.Fatalf("catalog refusal = %T %v", err, err)
				}
				return typed.Epoch
			},
		},
		"inbox": {
			refuse: func(epoch uint64) error {
				return commandEpochFence(InboxRecord{Claim: CommandClaim{LeaseEpoch: committed}}, epoch)
			},
			mark: func(err error) uint64 {
				var typed *InboxError
				if !errors.As(err, &typed) || typed.Code != InboxErrorEpoch {
					t.Fatalf("inbox refusal = %T %v", err, err)
				}
				return typed.Epoch
			},
		},
		"registry": {
			refuse: func(epoch uint64) error {
				return registrationEpochFence(HostRegistration{LeaseEpoch: committed}, epoch)
			},
			mark: func(err error) uint64 {
				var typed *RegistryError
				if !errors.As(err, &typed) || typed.Code != RegistryErrorEpoch {
					t.Fatalf("registry refusal = %T %v", err, err)
				}
				return typed.Epoch
			},
		},
		"pointer": {
			refuse: func(epoch uint64) error {
				return pointerEpochFence(SessionPointer{LeaseEpoch: committed}, epoch)
			},
			mark: func(err error) uint64 {
				var typed *PointerError
				if !errors.As(err, &typed) || typed.Code != PointerErrorEpoch {
					t.Fatalf("pointer refusal = %T %v", err, err)
				}
				return typed.Epoch
			},
		},
	}
	for name, fence := range fences {
		t.Run(name, func(t *testing.T) {
			if err := fence.refuse(committed); err != nil {
				t.Fatalf("an equal epoch was refused: %v", err)
			}
			err := fence.refuse(committed - 1)
			if err == nil {
				t.Fatal("a strictly lower epoch was admitted")
			}
			if got := fence.mark(err); got != committed {
				t.Fatalf("refusal carried %d, want the committed mark %d", got, committed)
			}
		})
	}
}
