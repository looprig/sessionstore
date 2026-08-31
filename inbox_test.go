package sessionstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// --- fixtures and assertions ----------------------------------------------

const (
	inboxCommand = sessionwire.CommandID("command-a")
	inboxRuntime = RuntimeCommandID("2f1c7d1e-0f3a-4c5b-9f21-000000000001")
	inboxKind    = CommandKind("input")
)

var (
	inboxAcceptedAt = time.Date(2026, 8, 30, 11, 30, 0, 0, time.UTC)
	inboxDeadline   = time.Date(2026, 8, 30, 12, 30, 0, 0, time.UTC)
	inboxPayload    = []byte(`{"blocks":[{"kind":"text","text":"hello"}]}`)
)

func testAdmitRequest() AdmitCommandRequest {
	return AdmitCommandRequest{
		TenantID:                 catalogTenant,
		SessionID:                catalogSession,
		CommandID:                inboxCommand,
		ProposedRuntimeCommandID: inboxRuntime,
		Kind:                     inboxKind,
		Payload:                  bytes.Clone(inboxPayload),
		AcceptedAt:               inboxAcceptedAt,
		ApplyDeadline:            inboxDeadline,
	}
}

// testInboxRecord is a record in a state admission never writes: terminal,
// previously claimed, and carrying both an application result and a rejection
// slot. The codec has to round-trip what LATER tasks will store, so the codec
// fixture is deliberately not the record admission produces.
func testInboxRecord() InboxRecord {
	return InboxRecord{
		TenantID:         catalogTenant,
		SessionID:        catalogSession,
		CommandID:        inboxCommand,
		RuntimeCommandID: inboxRuntime,
		Kind:             inboxKind,
		Payload:          bytes.Clone(inboxPayload),
		AcceptedAt:       inboxAcceptedAt,
		ApplyDeadline:    inboxDeadline,
		State:            InboxStateApplied,
		Claim:            CommandClaim{LeaseEpoch: 4, ExpiresAt: inboxDeadline},
		Result:           CommandResult{CompletedAt: inboxAcceptedAt, EventID: "event-42", JournalSeq: 42},
	}
}

func mustAdmit(t *testing.T, store *Store, req AdmitCommandRequest) InboxEntry {
	t.Helper()
	entry, created, err := store.AdmitCommand(context.Background(), req)
	if err != nil {
		t.Fatalf("AdmitCommand(%s): %v", req.CommandID, err)
	}
	if !created {
		t.Fatalf("AdmitCommand(%s) reported an existing command for a fresh identity", req.CommandID)
	}
	return entry
}

func assertInboxCode(t *testing.T, err error, want InboxErrorCode) *InboxError {
	t.Helper()
	var got *InboxError
	if !errors.As(err, &got) {
		t.Fatalf("error = %T %v, want *InboxError", err, err)
	}
	if got.Code != want {
		t.Fatalf("inbox code = %q, want %q (%v)", got.Code, want, err)
	}
	return got
}

// assertNoInboxRecord fails unless the identity has no ordered record at all.
// It reads the provider directly for the reason assertNoCatalogRecord does: a
// refused write that nonetheless persisted something would surface through the
// store API as a decode failure, which is indistinguishable from never having
// written.
func assertNoInboxRecord(t *testing.T, store *Store, req AdmitCommandRequest) {
	t.Helper()
	scope, err := store.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		// A request refused for its identity cannot have a derivable scope, so
		// there is no key to look under and nothing could have been written.
		return
	}
	if err := req.CommandID.Validate(); err != nil {
		return
	}
	if _, err := store.backend.OrderedIndex.Get(context.Background(), inboxID(scope, req.CommandID)); !errors.As(err, new(*storage.OrderedRecordNotFoundError)) {
		t.Fatalf("a refused admission left a record behind: %v", err)
	}
}

// assertNoSessionWitnesses fails unless the session's collision witnesses are
// still unbound. Binding them is durable, observable KV state, so "the
// rejection precedes the write" is a claim about the witnesses as much as about
// the record: validation runs before admission touches the provider at all, and
// nothing but the Create count would notice if it moved.
func assertNoSessionWitnesses(t *testing.T, store *Store, req AdmitCommandRequest) {
	t.Helper()
	scope, err := store.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		// An identity that has no derivable scope has no witness key either.
		return
	}
	err = store.verifySessionScope(context.Background(), scope)
	var keyspace *KeyspaceError
	if !errors.As(err, &keyspace) || keyspace.Code != KeyspaceBindingNotFound {
		t.Fatalf("a refused admission bound the session's witnesses: %v", err)
	}
}

// --- the stored record's codec --------------------------------------------

func TestInboxRecordRoundTripsThroughStoredBytes(t *testing.T) {
	t.Parallel()

	record := testInboxRecord()
	encoded, _, err := encodeInboxRecord(record)
	if err != nil {
		t.Fatalf("encodeInboxRecord: %v", err)
	}
	decoded, err := decodeInboxRecord(encoded)
	if err != nil {
		t.Fatalf("decodeInboxRecord: %v", err)
	}
	again, _, err := encodeInboxRecord(decoded)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if !bytes.Equal(encoded, again) {
		t.Fatalf("canonicalization is not a fixed point:\n%s\n%s", encoded, again)
	}
	if !bytes.Equal(decoded.Payload, record.Payload) {
		t.Fatalf("payload = %q, want %q", decoded.Payload, record.Payload)
	}
	if decoded.RuntimeCommandID != record.RuntimeCommandID || decoded.Kind != record.Kind {
		t.Fatalf("decoded mapping = %q/%q, want %q/%q", decoded.RuntimeCommandID, decoded.Kind, record.RuntimeCommandID, record.Kind)
	}
	if !decoded.AcceptedAt.Equal(record.AcceptedAt) || !decoded.ApplyDeadline.Equal(record.ApplyDeadline) {
		t.Fatalf("timestamps = %v/%v, want %v/%v", decoded.AcceptedAt, decoded.ApplyDeadline, record.AcceptedAt, record.ApplyDeadline)
	}
	if decoded.State != record.State || decoded.Claim != record.Claim || decoded.Result != record.Result {
		t.Fatalf("state/claim/result = %v/%+v/%+v, want %v/%+v/%+v",
			decoded.State, decoded.Claim, decoded.Result, record.State, record.Claim, record.Result)
	}
}

func TestInboxRecordRoundTripsARejection(t *testing.T) {
	t.Parallel()

	record := testInboxRecord()
	record.State = InboxStateRejected
	record.Result = CommandResult{}
	record.Rejection = &sessionwire.ErrorDetail{
		Code:      sessionwire.ErrorCodeRuntimeUnavailable,
		Message:   "no compatible runtime before the apply deadline",
		Retryable: false,
	}
	encoded, _, err := encodeInboxRecord(record)
	if err != nil {
		t.Fatalf("encodeInboxRecord: %v", err)
	}
	decoded, err := decodeInboxRecord(encoded)
	if err != nil {
		t.Fatalf("decodeInboxRecord: %v", err)
	}
	if decoded.Rejection == nil {
		t.Fatal("a rejected command decoded without its typed rejection")
	}
	if *decoded.Rejection != *record.Rejection {
		t.Fatalf("rejection = %+v, want %+v", *decoded.Rejection, *record.Rejection)
	}
	again, _, err := encodeInboxRecord(decoded)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if !bytes.Equal(encoded, again) {
		t.Fatalf("canonicalization is not a fixed point:\n%s\n%s", encoded, again)
	}
}

func TestInboxRecordRoundTripsAnObjectReferencedPayload(t *testing.T) {
	t.Parallel()

	record := testInboxRecord()
	record.Payload = nil
	record.PayloadRef = sessionwire.ObjectReference{ObjectID: "object-a"}
	encoded, _, err := encodeInboxRecord(record)
	if err != nil {
		t.Fatalf("encodeInboxRecord: %v", err)
	}
	decoded, err := decodeInboxRecord(encoded)
	if err != nil {
		t.Fatalf("decodeInboxRecord: %v", err)
	}
	if decoded.PayloadRef != record.PayloadRef || decoded.Payload != nil {
		t.Fatalf("payload/ref = %q/%+v, want nil/%+v", decoded.Payload, decoded.PayloadRef, record.PayloadRef)
	}
}

// TestInboxRecordCanonicalizesEveryTimestampToUTC pins the weaker half of the
// canonicalization claim, which the fixed-point property cannot reach: JSON
// round-trips a zone offset faithfully, so a record whose timestamps were never
// normalized still re-encodes to itself. What is lost is ONE canonical
// spelling — two callers submitting the same instant in different zones would
// store different bytes for the same command, and a byte comparison of a
// stored record against a re-encoding of it would then depend on where the
// writer was.
//
// Comparison by .Equal cannot see this: it compares instants and is
// zone-blind. The assertion is therefore on the encoded BYTES, and it covers
// every timestamp the record carries rather than only the two admission sets.
func TestInboxRecordCanonicalizesEveryTimestampToUTC(t *testing.T) {
	t.Parallel()

	zone := time.FixedZone("elsewhere", -(11*3600 + 30*60))
	zoned := testInboxRecord()
	zoned.AcceptedAt = inboxAcceptedAt.In(zone)
	zoned.ApplyDeadline = inboxDeadline.In(zone)
	zoned.Claim.ExpiresAt = zoned.Claim.ExpiresAt.In(zone)
	zoned.Result.CompletedAt = zoned.Result.CompletedAt.In(zone)

	zonedBytes, _, err := encodeInboxRecord(zoned)
	if err != nil {
		t.Fatalf("encodeInboxRecord(zoned): %v", err)
	}
	utcBytes, _, err := encodeInboxRecord(testInboxRecord())
	if err != nil {
		t.Fatalf("encodeInboxRecord(utc): %v", err)
	}
	if !bytes.Equal(zonedBytes, utcBytes) {
		t.Fatalf("one instant has two stored spellings:\n%s\n%s", zonedBytes, utcBytes)
	}

	decoded, err := decodeInboxRecord(zonedBytes)
	if err != nil {
		t.Fatalf("decodeInboxRecord: %v", err)
	}
	for name, instant := range map[string]time.Time{
		"accepted_at":         decoded.AcceptedAt,
		"apply_deadline":      decoded.ApplyDeadline,
		"claim.expires_at":    decoded.Claim.ExpiresAt,
		"result.completed_at": decoded.Result.CompletedAt,
	} {
		if instant.Location() != time.UTC {
			t.Fatalf("%s decoded in %v, want UTC", name, instant.Location())
		}
	}
}

// TestEncodeInboxRecordReturnsTheCanonicalRecord makes the second return value
// load-bearing. Admission compares a candidate against a STORED record, which
// is always canonical because it comes back through the decoder; comparing the
// caller's raw request instead would compare two different normal forms, and
// today that is invisible only because the one content normalization is
// nil-versus-empty payload. This pins that what encode hands back is the form
// the stored bytes are in, so the comparison cannot drift when a second
// normalization is added.
func TestEncodeInboxRecordReturnsTheCanonicalRecord(t *testing.T) {
	t.Parallel()

	zone := time.FixedZone("elsewhere", -(11*3600 + 30*60))
	raw := testInboxRecord()
	raw.Payload = []byte{}
	raw.AcceptedAt = raw.AcceptedAt.In(zone)
	raw.ApplyDeadline = raw.ApplyDeadline.In(zone)

	encoded, canonical, err := encodeInboxRecord(raw)
	if err != nil {
		t.Fatalf("encodeInboxRecord: %v", err)
	}
	if canonical.Payload != nil {
		t.Fatalf("canonical payload = %q, want nil: an empty inline body has one spelling", canonical.Payload)
	}
	if canonical.AcceptedAt.Location() != time.UTC || canonical.ApplyDeadline.Location() != time.UTC {
		t.Fatalf("canonical timestamps are in %v/%v, want UTC",
			canonical.AcceptedAt.Location(), canonical.ApplyDeadline.Location())
	}
	// The decisive property: what encode returns is what a decode of its bytes
	// produces, so a comparison against either reaches the same answer.
	decoded, err := decodeInboxRecord(encoded)
	if err != nil {
		t.Fatalf("decodeInboxRecord: %v", err)
	}
	if !decoded.sameCommandAs(canonical) || !decoded.AcceptedAt.Equal(canonical.AcceptedAt) {
		t.Fatalf("encode returned a record a decode of its own bytes disagrees with:\n%+v\n%+v", canonical, decoded)
	}
}

func TestInboxRecordRejectsInvalidMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		field  string
		mutate func(*InboxRecord)
	}{
		{name: "tenant", field: "tenant_id", mutate: func(r *InboxRecord) { r.TenantID = "" }},
		{name: "session", field: "session_id", mutate: func(r *InboxRecord) { r.SessionID = "" }},
		{name: "command", field: "command_id", mutate: func(r *InboxRecord) { r.CommandID = "" }},
		{name: "command not utf8", field: "command_id", mutate: func(r *InboxRecord) { r.CommandID = sessionwire.CommandID([]byte{0xff}) }},
		{name: "runtime command", field: "runtime_command_id", mutate: func(r *InboxRecord) { r.RuntimeCommandID = "" }},
		{name: "runtime command not utf8", field: "runtime_command_id", mutate: func(r *InboxRecord) {
			r.RuntimeCommandID = RuntimeCommandID([]byte{0xff})
		}},
		{name: "kind", field: "kind", mutate: func(r *InboxRecord) { r.Kind = "" }},
		{name: "kind not utf8", field: "kind", mutate: func(r *InboxRecord) { r.Kind = CommandKind([]byte{0xff}) }},
		{name: "payload too large", field: "payload", mutate: func(r *InboxRecord) {
			r.Payload = make([]byte, MaxInboxPayloadBytes+1)
		}},
		{name: "payload and reference", field: "payload", mutate: func(r *InboxRecord) {
			r.PayloadRef = sessionwire.ObjectReference{ObjectID: "object-a"}
		}},
		{name: "payload reference", field: "payload_ref", mutate: func(r *InboxRecord) {
			r.Payload = nil
			r.PayloadRef = sessionwire.ObjectReference{ObjectID: string([]byte{0xff})}
		}},
		{name: "accepted at", field: "accepted_at", mutate: func(r *InboxRecord) { r.AcceptedAt = time.Time{} }},
		{name: "apply deadline", field: "apply_deadline", mutate: func(r *InboxRecord) { r.ApplyDeadline = time.Time{} }},
		{name: "state", field: "state", mutate: func(r *InboxRecord) { r.State = "surprise" }},
		{name: "claim without epoch", field: "claim", mutate: func(r *InboxRecord) {
			r.Claim = CommandClaim{ExpiresAt: inboxDeadline}
		}},
		{name: "claim without expiry", field: "claim", mutate: func(r *InboxRecord) {
			r.Claim = CommandClaim{LeaseEpoch: 3}
		}},
		{name: "result without event", field: "result", mutate: func(r *InboxRecord) {
			r.Result = CommandResult{CompletedAt: inboxAcceptedAt, JournalSeq: 7}
		}},
		{name: "result without sequence", field: "result", mutate: func(r *InboxRecord) {
			r.Result = CommandResult{CompletedAt: inboxAcceptedAt, EventID: "event-7"}
		}},
		{name: "result without completion", field: "result", mutate: func(r *InboxRecord) {
			r.Result = CommandResult{EventID: "event-7", JournalSeq: 7}
		}},
		{name: "rejection without code", field: "rejection", mutate: func(r *InboxRecord) {
			r.Rejection = &sessionwire.ErrorDetail{Message: "why"}
		}},
		{name: "rejection message not utf8", field: "rejection.message", mutate: func(r *InboxRecord) {
			r.Rejection = &sessionwire.ErrorDetail{Code: sessionwire.ErrorCodeCommandRejected, Message: string([]byte{0xff})}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			record := testInboxRecord()
			test.mutate(&record)
			encoded, _, err := encodeInboxRecord(record)
			if encoded != nil {
				t.Fatalf("encoded an invalid record: %s", encoded)
			}
			failure := assertInboxCode(t, err, InboxErrorInvalid)
			if failure.Field != test.field {
				t.Fatalf("field = %q, want %q", failure.Field, test.field)
			}
		})
	}
}

func TestInboxRecordDecodeFailsClosed(t *testing.T) {
	t.Parallel()

	valid, _, err := encodeInboxRecord(testInboxRecord())
	if err != nil {
		t.Fatalf("encodeInboxRecord: %v", err)
	}
	rewrite := func(mutate func(map[string]json.RawMessage)) []byte {
		t.Helper()
		var members map[string]json.RawMessage
		if err := json.Unmarshal(valid, &members); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		mutate(members)
		encoded, err := json.Marshal(members)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return encoded
	}

	tests := []struct {
		name  string
		value []byte
		want  InboxErrorCode
	}{
		{name: "empty", value: nil, want: InboxErrorMalformed},
		{name: "not json", value: []byte("{"), want: InboxErrorMalformed},
		{name: "trailing content", value: append(bytes.Clone(valid), '{'), want: InboxErrorMalformed},
		{name: "too large", value: make([]byte, MaxInboxRecordBytes+1), want: InboxErrorTooLarge},
		{
			name:  "unknown version",
			value: rewrite(func(m map[string]json.RawMessage) { m["record_version"] = json.RawMessage("2") }),
			want:  InboxErrorVersion,
		},
		{
			name:  "undeclared member",
			value: rewrite(func(m map[string]json.RawMessage) { m["surprise"] = json.RawMessage("1") }),
			want:  InboxErrorMalformed,
		},
		{
			name:  "corrupted member",
			value: rewrite(func(m map[string]json.RawMessage) { m["state"] = json.RawMessage(`"surprise"`) }),
			want:  InboxErrorInvalid,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			record, err := decodeInboxRecord(test.value)
			if record.CommandID != "" {
				t.Fatalf("decoded a record beside an error: %+v", record)
			}
			assertInboxCode(t, err, test.want)
			// The taxonomy is the inbox's own: a caller branching on an inbox
			// failure must not have to match the catalog's type to learn that
			// its command was not stored.
			if errors.As(err, new(*CatalogError)) {
				t.Fatalf("an inbox decode reported a *CatalogError: %v", err)
			}
		})
	}
}

func TestInboxRecordAtItsPayloadCeilingEncodes(t *testing.T) {
	t.Parallel()

	record := testInboxRecord()
	record.Payload = bytes.Repeat([]byte{'x'}, MaxInboxPayloadBytes)
	encoded, _, err := encodeInboxRecord(record)
	if err != nil {
		t.Fatalf("a record at the payload ceiling does not encode: %v", err)
	}
	// The bound the encoder enforces is a function of the real encoded bytes,
	// not of arithmetic over the members, so a member added later cannot
	// silently invalidate it. What this pins is that the two bounds are not in
	// conflict: a payload this package accepts always leaves a record it can
	// also store and rewrite.
	if len(encoded) > MaxInboxRecordBytes {
		t.Fatalf("a maximal payload produced a %d-byte record, above the %d bound", len(encoded), MaxInboxRecordBytes)
	}
	decoded, err := decodeInboxRecord(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(decoded.Payload, record.Payload) {
		t.Fatal("a maximal payload did not round-trip")
	}
}

func TestInboxRecordRefusesARecordAboveTheStoredBound(t *testing.T) {
	t.Parallel()

	record := testInboxRecord()
	record.Rejection = &sessionwire.ErrorDetail{
		Code:    sessionwire.ErrorCodeCommandRejected,
		Message: strings.Repeat("m", MaxInboxRecordBytes),
	}
	encoded, _, err := encodeInboxRecord(record)
	if encoded != nil {
		t.Fatal("encoded a record above the stored bound")
	}
	assertInboxCode(t, err, InboxErrorTooLarge)
}

// --- admission -------------------------------------------------------------

func TestAdmitCommandStoresTheAuthoritativeRecord(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	store := openStore(t, base)

	req := testAdmitRequest()
	entry := mustAdmit(t, store, req)

	if entry.Record.RuntimeCommandID != inboxRuntime {
		t.Fatalf("runtime command id = %q, want %q", entry.Record.RuntimeCommandID, inboxRuntime)
	}
	if entry.Record.State != InboxStatePending {
		t.Fatalf("state = %q, want %q", entry.Record.State, InboxStatePending)
	}
	if entry.Record.Claim != (CommandClaim{}) || entry.Record.Result != (CommandResult{}) || entry.Record.Rejection != nil {
		t.Fatalf("admission produced a claimed or terminal record: %+v", entry.Record)
	}
	if !bytes.Equal(entry.Record.Payload, inboxPayload) {
		t.Fatalf("payload = %q, want %q", entry.Record.Payload, inboxPayload)
	}
	if entry.Revision != 1 {
		t.Fatalf("revision = %d, want 1", entry.Revision)
	}
	// Nonzero is the whole of the claim: the acceptance order is the
	// provider's, it is sparse by contract, and no test may assume it starts
	// at one.
	if entry.AcceptedOrder == 0 {
		t.Fatal("admission reported a zero acceptance order")
	}

	create, ok := ordered.lastOf("create")
	if !ok {
		t.Fatal("admission performed no Create")
	}
	scope, err := store.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	if create.id != (storage.OrderedID{
		Namespace:     inboxNamespace,
		OrderingScope: scope.SessionNamespace,
		StableKey:     storage.StableKey(inboxCommand),
	}) {
		t.Fatalf("filed under %+v, want the session's inbox scope keyed by the raw command id", create.id)
	}
	if create.rankingScope != scope.SessionNamespace {
		t.Fatalf("ranking scope = %q, want %q", create.rankingScope, scope.SessionNamespace)
	}
	if create.rank != (storage.Rank{}) {
		t.Fatalf("rank = %+v, want unranked: nothing ranks commands", create.rank)
	}
	if create.due != (storage.Due{State: storage.DueAt, UnixMillis: inboxDeadline.UnixMilli()}) {
		t.Fatalf("due = %+v, want the apply deadline", create.due)
	}
}

func TestAdmitCommandUsesExactlyOneCreateOrderedCall(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	store := openStore(t, base)

	mustAdmit(t, store, testAdmitRequest())
	if _, _, err := store.AdmitCommand(context.Background(), testAdmitRequest()); err != nil {
		t.Fatalf("duplicate AdmitCommand: %v", err)
	}

	if got := ordered.countOf("create"); got != 2 {
		t.Fatalf("Create calls = %d, want one per admission", got)
	}
	for _, op := range []string{"get", "update", "delete", "list_ordered", "list_ranked", "list_due"} {
		if got := ordered.countOf(op); got != 0 {
			t.Fatalf("admission performed %d %s calls; it is one CreateOrdered and nothing else", got, op)
		}
	}
}

func TestInboxDuplicateReturnsOriginalRuntimeIDAndOrder(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New())
	first := mustAdmit(t, store, testAdmitRequest())

	// The retry proposes a different runtime id and carries a later clock
	// reading, which is what a real retry of an unknown outcome looks like.
	retry := testAdmitRequest()
	retry.ProposedRuntimeCommandID = "2f1c7d1e-0f3a-4c5b-9f21-000000000002"
	retry.AcceptedAt = inboxAcceptedAt.Add(time.Hour)
	retry.ApplyDeadline = inboxDeadline.Add(time.Hour)

	second, created, err := store.AdmitCommand(context.Background(), retry)
	if err != nil {
		t.Fatalf("duplicate AdmitCommand: %v", err)
	}
	if created {
		t.Fatal("a duplicate command id reported a fresh acceptance")
	}
	if second.Record.RuntimeCommandID != first.Record.RuntimeCommandID {
		t.Fatalf("runtime command id = %q, want the winner's %q", second.Record.RuntimeCommandID, first.Record.RuntimeCommandID)
	}
	if second.AcceptedOrder != first.AcceptedOrder {
		t.Fatalf("acceptance order = %d, want the winner's %d", second.AcceptedOrder, first.AcceptedOrder)
	}
	if !second.Record.AcceptedAt.Equal(first.Record.AcceptedAt) || !second.Record.ApplyDeadline.Equal(first.Record.ApplyDeadline) {
		t.Fatalf("duplicate returned %v/%v, want the winner's %v/%v",
			second.Record.AcceptedAt, second.Record.ApplyDeadline, first.Record.AcceptedAt, first.Record.ApplyDeadline)
	}
	if second.Revision != first.Revision {
		t.Fatalf("revision = %d, want the unchanged %d", second.Revision, first.Revision)
	}
}

func TestAdmitCommandRejectsADuplicateWithDifferentCommandData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*AdmitCommandRequest)
	}{
		{name: "kind", mutate: func(r *AdmitCommandRequest) { r.Kind = "interrupt" }},
		{name: "payload", mutate: func(r *AdmitCommandRequest) { r.Payload = []byte(`{"blocks":[]}`) }},
		{name: "payload dropped", mutate: func(r *AdmitCommandRequest) { r.Payload = nil }},
		{name: "payload reference", mutate: func(r *AdmitCommandRequest) {
			r.Payload = nil
			r.PayloadRef = sessionwire.ObjectReference{ObjectID: "object-a"}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store := openStore(t, memstore.New())
			first := mustAdmit(t, store, testAdmitRequest())

			conflicting := testAdmitRequest()
			test.mutate(&conflicting)
			entry, created, err := store.AdmitCommand(context.Background(), conflicting)
			if created {
				t.Fatal("a conflicting reuse of a command id reported a fresh acceptance")
			}
			if entry.Record.CommandID != "" {
				t.Fatalf("a refused admission returned a record: %+v", entry.Record)
			}
			assertInboxCode(t, err, InboxErrorCommandMismatch)

			// The stored command is the winner's, untouched.
			stored, created, err := store.AdmitCommand(context.Background(), testAdmitRequest())
			if err != nil || created {
				t.Fatalf("re-admitting the original: %v, created %v", err, created)
			}
			if stored.Revision != first.Revision || !bytes.Equal(stored.Record.Payload, first.Record.Payload) {
				t.Fatalf("the conflicting admission changed the stored command: %+v", stored)
			}
		})
	}
}

// TestAdmitCommandRejectsADuplicateWithADifferentPayloadReference isolates the
// object-referenced body. The inline cases above change the payload, so a
// comparison that had dropped the REFERENCE would still be killed by them for
// the wrong reason; here both commands carry no inline payload and differ only
// in the object they name.
func TestAdmitCommandRejectsADuplicateWithADifferentPayloadReference(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New())
	referenced := testAdmitRequest()
	referenced.Payload = nil
	referenced.PayloadRef = sessionwire.ObjectReference{ObjectID: "object-a"}
	first := mustAdmit(t, store, referenced)

	other := referenced
	other.PayloadRef = sessionwire.ObjectReference{ObjectID: "object-b"}
	_, created, err := store.AdmitCommand(context.Background(), other)
	if created {
		t.Fatal("a command id reused for another object body reported a fresh acceptance")
	}
	assertInboxCode(t, err, InboxErrorCommandMismatch)

	// And the same reference is a plain retry.
	entry, created, err := store.AdmitCommand(context.Background(), referenced)
	if err != nil || created {
		t.Fatalf("retry of the referenced command: %v, created %v", err, created)
	}
	if entry.Record.PayloadRef != first.Record.PayloadRef {
		t.Fatalf("payload reference = %+v, want %+v", entry.Record.PayloadRef, first.Record.PayloadRef)
	}
}

// TestAdmitCommandBindsTheSessionsCollisionWitnesses pins that a command can be
// the first durable data a session has. The witnesses are what make a derived
// physical name trustworthy, and admission is a create path, so it binds them
// rather than assuming some other path already did.
func TestAdmitCommandBindsTheSessionsCollisionWitnesses(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New())
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	if err := store.verifySessionScope(context.Background(), scope); err == nil {
		t.Fatal("an untouched session already had bound witnesses; the test proves nothing")
	}

	mustAdmit(t, store, testAdmitRequest())

	if err := store.verifySessionScope(context.Background(), scope); err != nil {
		t.Fatalf("admission did not bind the session's witnesses: %v", err)
	}
}

// TestOrderedNamespacesAreDistinct pins what a namespace IS: a provider's
// physical partition, and the unit a decoder is chosen for. Two record kinds
// sharing one would put rows with different codecs, different ranking, and
// different due semantics into one stream or table, where a ranked or due query
// issued for one kind would page through the other's rows.
func TestOrderedNamespacesAreDistinct(t *testing.T) {
	t.Parallel()

	namespaces := map[string]string{
		"catalog": catalogNamespace,
		"gates":   gateNamespace,
		"inbox":   inboxNamespace,
	}
	seen := map[string]string{}
	for kind, namespace := range namespaces {
		if other, ok := seen[namespace]; ok {
			t.Fatalf("%s and %s share the namespace %q", other, kind, namespace)
		}
		seen[namespace] = kind
	}
}

func TestAdmitCommandDuplicateSucceedsAfterTheCommandProgressed(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	store := openStore(t, base)
	first := mustAdmit(t, store, testAdmitRequest())

	// Stand in for what a later task's terminal CAS leaves behind: an applied
	// record, no longer due. A retry arriving after that must still be told the
	// mapping it asked for rather than being refused as a conflict, because the
	// progress the command made is not evidence that this retry differs.
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	applied := first.Record
	applied.State = InboxStateApplied
	applied.Result = CommandResult{CompletedAt: inboxAcceptedAt, EventID: "event-9", JournalSeq: 9}
	value, _, err := encodeInboxRecord(applied)
	if err != nil {
		t.Fatalf("encodeInboxRecord: %v", err)
	}
	if _, err := base.OrderedIndex.Update(
		context.Background(), inboxID(scope, inboxCommand), first.Revision, value, storage.Rank{}, storage.Due{},
	); err != nil {
		t.Fatalf("Update: %v", err)
	}

	entry, created, err := store.AdmitCommand(context.Background(), testAdmitRequest())
	if err != nil {
		t.Fatalf("retry of an applied command: %v", err)
	}
	if created {
		t.Fatal("a retry of an applied command reported a fresh acceptance")
	}
	if entry.Record.State != InboxStateApplied || entry.AcceptedOrder != first.AcceptedOrder {
		t.Fatalf("retry returned %+v, want the applied record at order %d", entry.Record, first.AcceptedOrder)
	}
}

func TestInboxIdentityIsScopedByTenantAndSession(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New())

	first := mustAdmit(t, store, testAdmitRequest())

	otherSession := testAdmitRequest()
	otherSession.SessionID = "session-b"
	otherSession.ProposedRuntimeCommandID = "2f1c7d1e-0f3a-4c5b-9f21-000000000002"
	second := mustAdmit(t, store, otherSession)

	otherTenant := testAdmitRequest()
	otherTenant.TenantID = catalogOtherTenant
	otherTenant.ProposedRuntimeCommandID = "2f1c7d1e-0f3a-4c5b-9f21-000000000003"
	third := mustAdmit(t, store, otherTenant)

	if second.Record.RuntimeCommandID == first.Record.RuntimeCommandID {
		t.Fatal("the same command id in another session adopted the first session's mapping")
	}
	if third.Record.RuntimeCommandID == first.Record.RuntimeCommandID {
		t.Fatal("the same command id in another tenant adopted the first tenant's mapping")
	}
	if second.Record.SessionID != "session-b" || third.Record.TenantID != catalogOtherTenant {
		t.Fatalf("admission stored the wrong identities: %+v / %+v", second.Record, third.Record)
	}
}

func TestAdmitCommandAllocatesStrictlyIncreasingOrdersWithinASession(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New())

	var previous uint64
	for _, id := range []sessionwire.CommandID{"command-a", "command-b", "command-c"} {
		req := testAdmitRequest()
		req.CommandID = id
		req.ProposedRuntimeCommandID = RuntimeCommandID("runtime-" + string(id))
		entry := mustAdmit(t, store, req)
		if entry.AcceptedOrder <= previous {
			t.Fatalf("order %d did not increase past %d", entry.AcceptedOrder, previous)
		}
		previous = entry.AcceptedOrder
	}
	// Nothing here asserts that the orders are 1, 2, 3, or that they are
	// adjacent. The ordered index allocates from a scope high-water mark that
	// a provider may share with a sequence or a stream, so density and a
	// one-based origin are explicitly not promised and a test that assumed
	// either would fail against a conforming provider.
}

func TestAdmitCommandReportsSparseProviderOrdersVerbatim(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store := openStore(t, base)

	orders := []uint64{5_000, 9_223_372_036_854_775_807}
	next := 0
	hostile.refileCreates(func(record storage.OrderedRecord) storage.OrderedRecord {
		if next < len(orders) {
			record.Order = orders[next]
			next++
		}
		return record
	})

	for index, want := range orders {
		req := testAdmitRequest()
		req.CommandID = sessionwire.CommandID("command-" + string(rune('a'+index)))
		entry := mustAdmit(t, store, req)
		if entry.AcceptedOrder != want {
			t.Fatalf("acceptance order = %d, want the provider's %d", entry.AcceptedOrder, want)
		}
	}
}

func TestAdmitCommandConcurrentDuplicatesAgreeOnOneMapping(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New())

	const racers = 8
	entries := make([]InboxEntry, racers)
	creates := make([]bool, racers)
	errs := make([]error, racers)
	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	for i := range racers {
		done.Add(1)
		go func() {
			defer done.Done()
			req := testAdmitRequest()
			req.ProposedRuntimeCommandID = RuntimeCommandID("2f1c7d1e-0f3a-4c5b-9f21-00000000000" + string(rune('0'+i)))
			start.Wait()
			entries[i], creates[i], errs[i] = store.AdmitCommand(context.Background(), req)
		}()
	}
	start.Done()
	done.Wait()

	created := 0
	for i := range racers {
		if errs[i] != nil {
			t.Fatalf("racer %d: %v", i, errs[i])
		}
		if creates[i] {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("%d racers reported creating the command, want exactly 1", created)
	}
	for i := 1; i < racers; i++ {
		if entries[i].Record.RuntimeCommandID != entries[0].Record.RuntimeCommandID {
			t.Fatalf("racers disagree about the runtime mapping: %q and %q",
				entries[i].Record.RuntimeCommandID, entries[0].Record.RuntimeCommandID)
		}
		if entries[i].AcceptedOrder != entries[0].AcceptedOrder {
			t.Fatalf("racers disagree about the acceptance order: %d and %d",
				entries[i].AcceptedOrder, entries[0].AcceptedOrder)
		}
	}
	// The winner is one of the proposals, not something invented.
	proposed := false
	for i := range racers {
		if entries[0].Record.RuntimeCommandID == RuntimeCommandID("2f1c7d1e-0f3a-4c5b-9f21-00000000000"+string(rune('0'+i))) {
			proposed = true
		}
	}
	if !proposed {
		t.Fatalf("the stored runtime id %q was proposed by no racer", entries[0].Record.RuntimeCommandID)
	}
}

func TestAdmitCommandConcurrentDistinctCommandsGetDistinctOrders(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New())

	const racers = 16
	orders := make([]uint64, racers)
	errs := make([]error, racers)
	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	for i := range racers {
		done.Add(1)
		go func() {
			defer done.Done()
			req := testAdmitRequest()
			req.CommandID = sessionwire.CommandID("command-" + string(rune('a'+i)))
			start.Wait()
			entry, created, err := store.AdmitCommand(context.Background(), req)
			if err == nil && !created {
				err = errors.New("a distinct command id reported an existing command")
			}
			orders[i], errs[i] = entry.AcceptedOrder, err
		}()
	}
	start.Done()
	done.Wait()

	seen := make(map[uint64]int, racers)
	for i := range racers {
		if errs[i] != nil {
			t.Fatalf("racer %d: %v", i, errs[i])
		}
		if orders[i] == 0 {
			t.Fatalf("racer %d received a zero acceptance order", i)
		}
		if other, ok := seen[orders[i]]; ok {
			t.Fatalf("racers %d and %d both received order %d", other, i, orders[i])
		}
		seen[orders[i]] = i
	}
	// Distinct and nonzero is everything the contract promises across
	// concurrent admissions: the orders form a strict order, but they are not
	// contiguous, not one-based, and nothing about which racer won which order
	// is knowable.
}

func TestAdmitCommandTreatsTheCommandIDAsOpaqueBytes(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = ordered
	store := openStore(t, base)

	hostile := []sessionwire.CommandID{
		"../../etc/passwd",
		"a/b/c",
		"subject.with.dots.>",
		"tab\there\nnewline",
		"emoji-\U0001F600",
		" leading and trailing ",
	}
	for _, id := range hostile {
		req := testAdmitRequest()
		req.CommandID = id
		entry := mustAdmit(t, store, req)
		if entry.Record.CommandID != id {
			t.Fatalf("command id = %q, want the caller's %q", entry.Record.CommandID, id)
		}
		create, _ := ordered.lastOf("create")
		if create.id.StableKey != storage.StableKey(id) {
			t.Fatalf("stable key = %q, want the raw command id %q", create.id.StableKey, id)
		}
		// Each one is its own identity: a second admission of it is a
		// duplicate, and none of them collided with another.
		entry, created, err := store.AdmitCommand(context.Background(), req)
		if err != nil || created {
			t.Fatalf("re-admitting %q: %v, created %v", id, err, created)
		}
		if entry.Record.CommandID != id {
			t.Fatalf("duplicate of %q returned %q", id, entry.Record.CommandID)
		}
	}
}

func TestAdmitCommandKeepsThePayloadPrivate(t *testing.T) {
	t.Parallel()

	watch := &writeWatch{}
	base := memstore.New()
	base.OrderedIndex = &watchingOrdered{OrderedIndex: base.OrderedIndex, watch: watch}
	base.KV = &watchingKV{KV: base.KV, watch: watch}
	store := openStore(t, base)

	// A catalog record exists, so a payload leaked into session state would be
	// written somewhere this watch can see it.
	mustCreateCatalog(t, store)
	secret := []byte("private-command-payload-9f21")
	req := testAdmitRequest()
	req.Payload = bytes.Clone(secret)
	mustAdmit(t, store, req)

	carrying := watch.carrying(secret)
	if len(carrying) != 1 {
		t.Fatalf("the payload was written %d times, want exactly once: %+v", len(carrying), carrying)
	}
	if carrying[0].primitive != "ordered:"+inboxNamespace {
		t.Fatalf("the payload was written to %s, want the inbox record", carrying[0].primitive)
	}

	// Nor may a failure quote it: a rejection is reported by field, never by
	// value.
	conflicting := req
	conflicting.Payload = []byte("other")
	_, _, err := store.AdmitCommand(context.Background(), conflicting)
	assertInboxCode(t, err, InboxErrorCommandMismatch)
	if strings.Contains(err.Error(), string(secret)) || strings.Contains(err.Error(), "other") {
		t.Fatalf("an inbox failure quoted a private payload: %v", err)
	}
}

func TestAdmitCommandFilesTheApplyDeadlineAsTheRecordsDueState(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	store := openStore(t, base)
	mustAdmit(t, store, testAdmitRequest())

	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	stored, err := base.OrderedIndex.Get(context.Background(), inboxID(scope, inboxCommand))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Due != (storage.Due{State: storage.DueAt, UnixMillis: inboxDeadline.UnixMilli()}) {
		t.Fatalf("due = %+v, want the apply deadline", stored.Due)
	}

	page, err := base.OrderedIndex.ListDue(context.Background(), inboxNamespace, inboxDeadline.UnixMilli(), "", 10)
	if err != nil {
		t.Fatalf("ListDue: %v", err)
	}
	if len(page.Records) != 1 {
		t.Fatalf("the due view holds %d records, want the one pending command", len(page.Records))
	}
}

func TestAdmitCommandAcceptsADeadlineAlreadyPast(t *testing.T) {
	t.Parallel()

	store := openStore(t, memstore.New())
	first := mustAdmit(t, store, testAdmitRequest())

	// A retry of an unknown outcome may arrive long after the command's apply
	// deadline has passed, and it must still be able to learn its mapping. An
	// admission-time "the deadline must be in the future" gate would refuse
	// exactly that retry and strand the caller, so there is none — the deadline
	// is validated as an instant and nothing more.
	late := testAdmitRequest()
	late.ApplyDeadline = inboxAcceptedAt.Add(-time.Hour)
	entry, created, err := store.AdmitCommand(context.Background(), late)
	if err != nil {
		t.Fatalf("retry after the deadline: %v", err)
	}
	if created || entry.AcceptedOrder != first.AcceptedOrder {
		t.Fatalf("retry after the deadline returned created=%v order=%d", created, entry.AcceptedOrder)
	}

	// And a fresh command whose deadline is already past is accepted too: it
	// is immediately eligible for the deadline reconciler a later task adds,
	// which is a better outcome than a caller that cannot record it at all.
	fresh := testAdmitRequest()
	fresh.CommandID = "command-late"
	fresh.ApplyDeadline = inboxAcceptedAt.Add(-time.Hour)
	freshEntry := mustAdmit(t, store, fresh)
	if !freshEntry.Record.ApplyDeadline.Equal(fresh.ApplyDeadline.UTC()) {
		t.Fatalf("apply deadline = %v, want %v", freshEntry.Record.ApplyDeadline, fresh.ApplyDeadline)
	}
}

func TestAdmitCommandNeedsNoCatalogRecord(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	store := openStore(t, base)

	// The V1 create command carries a client-chosen SessionID and is admitted
	// before that session's catalog record exists; requiring one here would
	// make the create command unadmittable.
	entry := mustAdmit(t, store, testAdmitRequest())
	if entry.Record.SessionID != catalogSession {
		t.Fatalf("session = %q, want %q", entry.Record.SessionID, catalogSession)
	}

	// The session's collision witnesses are bound by the admission, so a later
	// catalog create over the same identity agrees rather than colliding.
	mustCreateCatalog(t, store)
}

func TestAdmitCommandRejectsInvalidRequestsBeforeWriting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*AdmitCommandRequest)
	}{
		{name: "tenant", mutate: func(r *AdmitCommandRequest) { r.TenantID = "" }},
		{name: "session", mutate: func(r *AdmitCommandRequest) { r.SessionID = "" }},
		{name: "command", mutate: func(r *AdmitCommandRequest) { r.CommandID = "" }},
		{name: "runtime command", mutate: func(r *AdmitCommandRequest) { r.ProposedRuntimeCommandID = "" }},
		{name: "kind", mutate: func(r *AdmitCommandRequest) { r.Kind = "" }},
		{name: "accepted at", mutate: func(r *AdmitCommandRequest) { r.AcceptedAt = time.Time{} }},
		{name: "apply deadline", mutate: func(r *AdmitCommandRequest) { r.ApplyDeadline = time.Time{} }},
		{name: "payload too large", mutate: func(r *AdmitCommandRequest) {
			r.Payload = make([]byte, MaxInboxPayloadBytes+1)
		}},
		{name: "payload and reference", mutate: func(r *AdmitCommandRequest) {
			r.PayloadRef = sessionwire.ObjectReference{ObjectID: "object-a"}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			base := memstore.New()
			ordered := &recordingOrdered{OrderedIndex: base.OrderedIndex}
			base.OrderedIndex = ordered
			store := openStore(t, base)

			req := testAdmitRequest()
			test.mutate(&req)
			entry, created, err := store.AdmitCommand(context.Background(), req)
			if err == nil {
				t.Fatal("an invalid request was admitted")
			}
			if created || entry.Record.CommandID != "" {
				t.Fatalf("a refused admission returned %+v created=%v", entry, created)
			}
			if got := ordered.countOf("create"); got != 0 {
				t.Fatalf("a refused admission performed %d Create calls; the rejection must precede the write", got)
			}
			assertNoInboxRecord(t, store, req)
			assertNoSessionWitnesses(t, store, req)
		})
	}
}

// TestAdmitCommandReportsAnInvalidRequestEvenWhenClosed makes the validation
// ordering TOTAL, not merely "before the provider". A malformed request is a
// caller mistake whatever the store is doing, so it is reported as one rather
// than as whatever the store's lifecycle happened to be at the time; the
// alternative tells a caller to retry later a request that can never succeed.
//
// It also pins the ordering the rest of the package uses: CreateCatalogEntry
// validates before it admits too, and without this the two files would be free
// to drift into disagreeing about which answer an invalid request gets.
func TestAdmitCommandReportsAnInvalidRequestEvenWhenClosed(t *testing.T) {
	t.Parallel()

	store, err := Open(context.Background(), memstore.New())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	req := testAdmitRequest()
	req.Kind = ""
	_, created, err := store.AdmitCommand(context.Background(), req)
	if created {
		t.Fatal("a closed store admitted a command")
	}
	if errors.As(err, new(*StoreClosedError)) {
		t.Fatalf("an invalid request was reported as a closed store: %v", err)
	}
	failure := assertInboxCode(t, err, InboxErrorInvalid)
	if failure.Field != "kind" {
		t.Fatalf("field = %q, want %q", failure.Field, "kind")
	}
}

func TestAdmitCommandRejectsATombstonedIdentity(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	store := openStore(t, base)
	entry := mustAdmit(t, store, testAdmitRequest())

	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	if _, err := base.OrderedIndex.Delete(context.Background(), inboxID(scope, inboxCommand), entry.Revision); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// An acceptance order cannot be reused, so a retry that meets a tombstone
	// is told so rather than being handed a record whose bytes describe a
	// command that is no longer there.
	_, created, err := store.AdmitCommand(context.Background(), testAdmitRequest())
	if created {
		t.Fatal("admission recreated a tombstoned command")
	}
	assertInboxCode(t, err, InboxErrorDeleted)
}

func TestAdmitCommandHoldsTheProviderToTheRecordsOwnFiling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		field   string
		code    InboxErrorCode
		refile  func(storage.OrderedRecord) storage.OrderedRecord
		wantErr bool
	}{
		{
			name:  "stable key",
			field: "command_id",
			code:  InboxErrorIdentity,
			refile: func(r storage.OrderedRecord) storage.OrderedRecord {
				r.ID.StableKey = "another-command"
				return r
			},
		},
		{
			name:  "ordering scope",
			field: "ordering_scope",
			code:  InboxErrorIdentity,
			refile: func(r storage.OrderedRecord) storage.OrderedRecord {
				r.ID.OrderingScope += "-elsewhere"
				return r
			},
		},
		{
			name:  "ranking scope",
			field: "ranking_scope",
			code:  InboxErrorIdentity,
			refile: func(r storage.OrderedRecord) storage.OrderedRecord {
				r.RankingScope += "-elsewhere"
				return r
			},
		},
		{
			name:  "due instant",
			field: "due",
			code:  InboxErrorIdentity,
			refile: func(r storage.OrderedRecord) storage.OrderedRecord {
				r.Due.UnixMillis += 60_000
				return r
			},
		},
		{
			name:  "due state",
			field: "due",
			code:  InboxErrorIdentity,
			refile: func(r storage.OrderedRecord) storage.OrderedRecord {
				// The instant is untouched: only the DUE STATE is wrong, which
				// is the half a millisecond-only comparison would accept. A
				// pending command filed not-due participates in no due page and
				// would never be reconciled.
				r.Due.State = storage.NotDue
				return r
			},
		},
		{
			name:  "zero order",
			field: "order",
			code:  InboxErrorIdentity,
			refile: func(r storage.OrderedRecord) storage.OrderedRecord {
				// Zero is not an allocated order. Returned as an acceptance
				// order it would compare equal for every command in the
				// session and silently destroy the order consumers sort by.
				r.Order = 0
				return r
			},
		},
		{
			name:  "deleted",
			field: "record",
			code:  InboxErrorDeleted,
			refile: func(r storage.OrderedRecord) storage.OrderedRecord {
				r.Deleted = true
				return r
			},
		},
		{
			name:  "record identity",
			field: "record",
			code:  InboxErrorIdentity,
			refile: func(r storage.OrderedRecord) storage.OrderedRecord {
				// The bytes describe another session's command while the key
				// says otherwise.
				record := testInboxRecord()
				record.SessionID = "session-elsewhere"
				value, _, err := encodeInboxRecord(record)
				if err != nil {
					panic(err)
				}
				r.Value = value
				return r
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			base := memstore.New()
			hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
			base.OrderedIndex = hostile
			store := openStore(t, base)

			// The command is admitted for real first, so every case runs on the
			// RETRY path. That is deliberate on two counts. It is the only
			// contract-legal route to the tombstone case — Create returns an
			// existing or tombstoned identity as a RECORD with created false,
			// never as a created one, so a created tombstone is a reply no
			// conforming provider can produce and a subtest resting on it would
			// exercise an unreachable state. And it is the path where a
			// misfiled reply is most consequential: the bytes are the WINNER's
			// record rather than ours, so these checks are the only thing
			// between a retrying caller and another session's command handed
			// back under its own key. The created path has its own, stronger
			// check — see TestAdmitCommandHoldsACreatedReplyToTheBytesItSent.
			mustAdmit(t, store, testAdmitRequest())
			hostile.refileCreates(test.refile)

			_, created, err := store.AdmitCommand(context.Background(), testAdmitRequest())
			if created {
				t.Fatal("a misfiled record was reported as an acceptance")
			}
			failure := assertInboxCode(t, err, test.code)
			if failure.Field != test.field {
				t.Fatalf("field = %q, want %q", failure.Field, test.field)
			}
		})
	}
}

// TestAdmitCommandHoldsACreatedReplyToTheBytesItSent catches what no filing
// check can: a reply that satisfies every identity, scope, due and order check
// because it carries our identity, and carries somebody else's CONTENT under
// created=true. The reasoning is on AdmitCommand's created branch.
func TestAdmitCommandHoldsACreatedReplyToTheBytesItSent(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store := openStore(t, base)

	// Same identity, same timestamps, same pending state — so every identity,
	// scope, due and order check passes — and different content.
	substitute := InboxRecord{
		TenantID:         catalogTenant,
		SessionID:        catalogSession,
		CommandID:        inboxCommand,
		RuntimeCommandID: "SOMETHING-ELSE-ENTIRELY",
		Kind:             "interrupt",
		Payload:          []byte(`{"blocks":[]}`),
		AcceptedAt:       inboxAcceptedAt,
		ApplyDeadline:    inboxDeadline,
		State:            InboxStatePending,
	}
	value, _, err := encodeInboxRecord(substitute)
	if err != nil {
		t.Fatalf("encodeInboxRecord: %v", err)
	}
	hostile.refileCreates(func(r storage.OrderedRecord) storage.OrderedRecord {
		r.Value = value
		return r
	})

	entry, created, err := store.AdmitCommand(context.Background(), testAdmitRequest())
	if created {
		t.Fatalf("a substituted reply was reported as this caller's acceptance: runtime %q kind %q",
			entry.Record.RuntimeCommandID, entry.Record.Kind)
	}
	failure := assertInboxCode(t, err, InboxErrorIdentity)
	if failure.Field != "value" {
		t.Fatalf("field = %q, want %q", failure.Field, "value")
	}
}

func TestAdmitCommandAcceptsATerminalRecordFiledNotDue(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store := openStore(t, base)

	// The command is admitted for real first, so what follows is the RETRY
	// path — the only path on which a terminal record can be met. A terminal
	// command is filed not-due, so the due state a reader checks a record
	// against is a function of the RECORD, not of the operation that happens to
	// be reading it. Without that, every retry of a command a later task has
	// already completed would be reported as misfiled.
	mustAdmit(t, store, testAdmitRequest())

	terminal := testInboxRecord()
	value, _, err := encodeInboxRecord(terminal)
	if err != nil {
		t.Fatalf("encodeInboxRecord: %v", err)
	}
	hostile.refileCreates(func(r storage.OrderedRecord) storage.OrderedRecord {
		r.Value = value
		r.Due = storage.Due{}
		return r
	})

	entry, created, err := store.AdmitCommand(context.Background(), testAdmitRequest())
	if err != nil {
		t.Fatalf("a terminal command filed not-due was refused: %v", err)
	}
	if created {
		t.Fatal("a retry reported a fresh acceptance")
	}
	if entry.Record.State != InboxStateApplied {
		t.Fatalf("state = %q, want %q", entry.Record.State, InboxStateApplied)
	}
}

func TestAdmitCommandClassifiesProviderFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want InboxErrorCode
	}{
		{name: "ambiguous", err: &storage.OrderedAmbiguousError{}, want: InboxErrorUnknown},
		{name: "other", err: errors.New("provider is unwell"), want: InboxErrorBackend},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			base := memstore.New()
			hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
			base.OrderedIndex = hostile
			store := openStore(t, base)
			hostile.failCreates(test.err)

			_, created, err := store.AdmitCommand(context.Background(), testAdmitRequest())
			if created {
				t.Fatal("a failed Create reported an acceptance")
			}
			failure := assertInboxCode(t, err, test.want)
			if !errors.Is(err, test.err) {
				t.Fatalf("the provider cause was not preserved: %v", err)
			}
			if failure.Field != "create" {
				t.Fatalf("field = %q, want %q", failure.Field, "create")
			}
		})
	}
}

// --- the public surface and its lifecycle ---------------------------------

var _ func(*Store, context.Context, AdmitCommandRequest) (InboxEntry, bool, error) = (*Store).AdmitCommand

// declaredInboxOperations enumerates inbox.go's public Store operations from
// the source, for the same reason declaredGateOperations does it for gates.go:
// the file that declares an operation is the file the close test enumerates, so
// a new one cannot be added without being exercised here.
func declaredInboxOperations(t *testing.T) map[string]bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "inbox.go", nil, 0)
	if err != nil {
		t.Fatalf("parse inbox.go: %v", err)
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
		t.Fatal("no public Store operations were found in inbox.go; the enumerator is not reaching the declarations")
	}
	return operations
}

func TestInboxOperationsRefuseAfterClose(t *testing.T) {
	store, err := Open(context.Background(), memstore.New())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustAdmit(t, store, testAdmitRequest())
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	operations := map[string]func() error{
		"AdmitCommand": func() error {
			_, _, err := store.AdmitCommand(context.Background(), testAdmitRequest())
			return err
		},
	}

	declared := declaredInboxOperations(t)
	for name := range declared {
		if operations[name] == nil {
			t.Errorf("inbox.go declares the public operation %s and this test does not exercise it", name)
		}
	}
	for name := range operations {
		if !declared[name] {
			t.Errorf("this test exercises %s, which inbox.go no longer declares (was it moved to another file?)", name)
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
