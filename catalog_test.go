package sessionstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
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
	if decoded.LeaseEpoch != record.LeaseEpoch || decoded.DesiredIdempotencyKey != record.DesiredIdempotencyKey {
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
			m["record_version"] = json.RawMessage("2")
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
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close(context.Background())
	mustCreateCatalog(t, store)

	base.OrderedIndex = &corruptingOrdered{OrderedIndex: base.OrderedIndex, rewrite: func(value []byte) []byte {
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
	}}
	store.backend = base

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

	after, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: catalogSession})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	if after.Record.LeaseEpoch != 5 || after.Record.State != high.Record.State || after.Revision != high.Revision {
		t.Fatalf("a rejected epoch still changed the record: %+v", after)
	}

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
	mustCreateCatalog(t, store)
	req := testHostStateRequest(0)
	if _, err := store.UpdateCatalogHostState(context.Background(), req); err == nil {
		t.Fatal("a zero lease epoch was accepted")
	} else {
		got := assertCatalogCode(t, err, CatalogErrorInvalid)
		if got.Field != "lease_epoch" {
			t.Fatalf("field = %q, want lease_epoch", got.Field)
		}
	}
}

func TestCatalogHostUpdateRejectsJournalRegression(t *testing.T) {
	store := openTestStore(t)
	mustCreateCatalog(t, store)
	forward := testHostStateRequest(2)
	forward.LastJournalSeq = 20
	if _, err := store.UpdateCatalogHostState(context.Background(), forward); err != nil {
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

func TestCatalogOperationsRefuseAfterClose(t *testing.T) {
	store, err := Open(context.Background(), memstore.New())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustCreateCatalog(t, store)
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := store.CreateCatalogEntry(context.Background(), testCreateRequest()); !errors.As(err, new(*StoreClosedError)) {
		t.Fatalf("CreateCatalogEntry after Close = %v", err)
	}
	if _, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: catalogSession}); !errors.As(err, new(*StoreClosedError)) {
		t.Fatalf("GetCatalogEntry after Close = %v", err)
	}
	if _, err := store.UpdateCatalogHostState(context.Background(), testHostStateRequest(1)); !errors.As(err, new(*StoreClosedError)) {
		t.Fatalf("UpdateCatalogHostState after Close = %v", err)
	}
	if _, err := store.UpdateCatalogDesiredState(context.Background(), UpdateCatalogDesiredStateRequest{
		TenantID: catalogTenant, SessionID: catalogSession, ExpectedRevision: 1,
		IdempotencyKey: "k", DesiredPlacement: sessionwire.HostPlacementPooled,
	}); !errors.As(err, new(*StoreClosedError)) {
		t.Fatalf("UpdateCatalogDesiredState after Close = %v", err)
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
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close(context.Background())
	mustCreateCatalog(t, store)

	secret := errors.New("provider path /var/secret/tenant-a")
	base.OrderedIndex = &failingOrdered{OrderedIndex: base.OrderedIndex, getErr: secret}
	store.backend = base
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
			after, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: catalogSession})
			if err != nil {
				t.Fatalf("the rejected write corrupted the stored record: %v", err)
			}
			if after.Revision != created.Revision || after.Record.LeaseEpoch != 0 ||
				!after.Record.Checkpoint.isZero() || after.Record.OpenGates != nil {
				t.Fatalf("a rejected write reached the record: rev=%d %+v", after.Revision, after.Record)
			}
		})
	}
}
