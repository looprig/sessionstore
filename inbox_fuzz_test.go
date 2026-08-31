package sessionstore

import (
	"bytes"
	"encoding/json"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// FuzzInboxRecordCodec fuzzes stored command bytes. Its seeds are real
// encodings and derived variants of them, so a mutation starts from a value
// that already reaches the strict decoder, the identity validators, the state
// enumeration, and the claim, result, and rejection checks rather than
// bouncing off the first json.Unmarshal.
//
// The property is the one every stored record in this package states:
// canonicalization reaches a fixed point, so a record's stored bytes do not
// depend on how many times it has been rewritten.
//
// It also holds every accepted record to the due state a writer would derive
// from it, because that derivation is what decides whether the command is ever
// reconciled: a non-terminal command must be due at its apply deadline, and a
// terminal one must be due at no time at all.
func FuzzInboxRecordCodec(f *testing.F) {
	seed := func(record InboxRecord) []byte {
		encoded, _, err := encodeInboxRecord(record)
		if err != nil {
			f.Fatalf("seed does not encode: %v", err)
		}
		return encoded
	}
	pending := InboxRecord{
		TenantID:         catalogTenant,
		SessionID:        catalogSession,
		CommandID:        inboxCommand,
		RuntimeCommandID: inboxRuntime,
		Kind:             inboxKind,
		Payload:          bytes.Clone(inboxPayload),
		AcceptedAt:       inboxAcceptedAt,
		ApplyDeadline:    inboxDeadline,
		State:            InboxStatePending,
	}
	f.Add(seed(pending))
	f.Add(seed(testInboxRecord()))

	referenced := pending
	referenced.Payload = nil
	referenced.PayloadRef = sessionwire.ObjectReference{ObjectID: "object-a"}
	referenced.State = InboxStateApplying
	referenced.Claim = CommandClaim{LeaseEpoch: 7, ExpiresAt: inboxDeadline}
	f.Add(seed(referenced))

	rejected := pending
	rejected.State = InboxStateRejected
	rejected.Rejection = &sessionwire.ErrorDetail{
		Code:      sessionwire.ErrorCodeRuntimeUnavailable,
		Message:   "no compatible runtime",
		Retryable: true,
	}
	f.Add(seed(rejected))

	extreme := pending
	extreme.AcceptedAt = minRankableTime
	extreme.ApplyDeadline = maxRankableTime
	f.Add(seed(extreme))

	// One seed per fail-closed branch, derived from a real encoding, so the
	// fuzzer explores from inside each rejection path as well as from an
	// accepted record.
	var members map[string]json.RawMessage
	if err := json.Unmarshal(seed(testInboxRecord()), &members); err != nil {
		f.Fatalf("seed is not JSON: %v", err)
	}
	for _, mutate := range []func(map[string]json.RawMessage){
		func(m map[string]json.RawMessage) { m["record_version"] = json.RawMessage("2") },
		func(m map[string]json.RawMessage) { m["surprise"] = json.RawMessage("1") },
		func(m map[string]json.RawMessage) { m["command_id"] = json.RawMessage(`""`) },
		func(m map[string]json.RawMessage) { m["runtime_command_id"] = json.RawMessage(`""`) },
		func(m map[string]json.RawMessage) { m["kind"] = json.RawMessage(`""`) },
		func(m map[string]json.RawMessage) { m["state"] = json.RawMessage(`"surprise"`) },
		func(m map[string]json.RawMessage) {
			m["claim"] = json.RawMessage(`{"lease_epoch":0,"expires_at":"2026-08-30T12:30:00Z"}`)
		},
		func(m map[string]json.RawMessage) {
			m["result"] = json.RawMessage(`{"completed_at":"2026-08-30T12:30:00Z","event_id":"event-1","journal_seq":0}`)
		},
		func(m map[string]json.RawMessage) { m["rejection"] = json.RawMessage(`{"code":"","message":"x"}`) },
		func(m map[string]json.RawMessage) { m["payload_ref"] = json.RawMessage(`{"object_id":"object-a"}`) },
		func(m map[string]json.RawMessage) { m["apply_deadline"] = json.RawMessage(`"0000-01-01T00:00:00Z"`) },
	} {
		copied := make(map[string]json.RawMessage, len(members))
		for name, value := range members {
			copied[name] = value
		}
		mutate(copied)
		encoded, err := json.Marshal(copied)
		if err != nil {
			f.Fatalf("marshal seed variant: %v", err)
		}
		f.Add(encoded)
	}
	f.Add([]byte(nil))
	f.Add([]byte("{}"))

	f.Fuzz(func(t *testing.T, value []byte) {
		record, err := decodeInboxRecord(value)
		if err != nil {
			return
		}
		encoded, _, err := encodeInboxRecord(record)
		if err != nil {
			t.Fatalf("accepted a record that does not re-encode: %v", err)
		}
		if len(encoded) > MaxInboxRecordBytes {
			t.Fatalf("re-encoded record is %d bytes, above the %d bound", len(encoded), MaxInboxRecordBytes)
		}
		if len(record.Payload) > MaxInboxPayloadBytes {
			t.Fatalf("accepted a %d-byte inline payload, above the %d bound", len(record.Payload), MaxInboxPayloadBytes)
		}
		if len(record.Payload) > 0 && record.PayloadRef != (sessionwire.ObjectReference{}) {
			t.Fatal("accepted a record carrying two command bodies")
		}
		again, err := decodeInboxRecord(encoded)
		if err != nil {
			t.Fatalf("canonical form does not decode: %v", err)
		}
		reencoded, _, err := encodeInboxRecord(again)
		if err != nil {
			t.Fatalf("canonical form does not re-encode: %v", err)
		}
		if !bytes.Equal(encoded, reencoded) {
			t.Fatalf("canonicalization is not a fixed point:\n%s\n%s", encoded, reencoded)
		}
		due := inboxDue(record)
		if record.State.terminal() {
			if due != (storage.Due{}) {
				t.Fatalf("a terminal command derived the due state %+v, want not due", due)
			}
			return
		}
		if due.State != storage.DueAt || due.UnixMillis != record.ApplyDeadline.UnixMilli() {
			t.Fatalf("accepted a command with an underivable due state: %+v", due)
		}
	})
}
