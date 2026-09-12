package sessionstore

import (
	"bytes"
	"encoding/json"
	"testing"
)

// FuzzDispositionCommandCursorCodec fuzzes stored consumption-cursor bytes. Its seeds are
// real encodings rather than hand-written JSON, so a mutation starts from a
// value that already reaches the strict decoder, the two zero refusals and the
// instant rules, instead of bouncing off the first json.Unmarshal.
//
// The property is that canonicalization reaches a fixed point: anything the
// decoder accepts must re-encode, decoding that encoding must produce the
// identical bytes again, and the record the decoder hands back must already be
// the canonical one. It deliberately does not claim the decoder rejects
// non-canonical input — the decoder is a NORMALIZER, and what is guarded is
// that normalizing twice can never differ from normalizing once.
//
// The instant is where the property is most at risk. It is the only member with
// a spelling that is not its value: an offset instant and its UTC equivalent
// name the same moment, canonicalization moves one to the other, and a record
// whose stored bytes were never re-encoded through this path would otherwise
// disagree with the bytes a write produces — which verifyCommandCursorBytes
// compares exactly.
func FuzzDispositionCommandCursorCodec(f *testing.F) {
	base := DispositionCommandCursor{
		TenantID: catalogTenant, SessionID: catalogSession,
		LeaseEpoch: consumptionEpoch, ConsumedOrder: consumptionOrder,
		UpdatedAt: consumptionUpdatedAt,
	}
	seed, _, err := encodeDispositionCursor(base)
	if err != nil {
		f.Fatalf("seed does not encode: %v", err)
	}
	f.Add(seed)

	var members map[string]json.RawMessage
	if err := json.Unmarshal(seed, &members); err != nil {
		f.Fatalf("seed is not JSON: %v", err)
	}
	for _, mutate := range []func(map[string]json.RawMessage){
		func(m map[string]json.RawMessage) { m["record_version"] = json.RawMessage("2") },
		func(m map[string]json.RawMessage) { m["surprise"] = json.RawMessage("1") },
		func(m map[string]json.RawMessage) { m["lease_epoch"] = json.RawMessage("0") },
		func(m map[string]json.RawMessage) { m["consumed_order"] = json.RawMessage("0") },
		func(m map[string]json.RawMessage) { m["consumed_order"] = json.RawMessage("18446744073709551615") },
		func(m map[string]json.RawMessage) { m["tenant_id"] = json.RawMessage(`""`) },
		func(m map[string]json.RawMessage) { m["session_id"] = json.RawMessage(`""`) },
		// The instant, in all three shapes that matter: an offset spelling that
		// canonicalization moves, and the two rankable bounds, each of which is
		// the only refuser of its own direction.
		func(m map[string]json.RawMessage) { m["updated_at"] = json.RawMessage(`"2026-08-30T13:40:00+02:00"`) },
		func(m map[string]json.RawMessage) { m["updated_at"] = json.RawMessage(`"5000-01-01T00:00:00Z"`) },
		func(m map[string]json.RawMessage) { m["updated_at"] = json.RawMessage(`"0001-01-01T00:00:00Z"`) },
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

	f.Fuzz(func(t *testing.T, value []byte) {
		cursor, err := decodeDispositionCursor(value)
		if err != nil {
			return
		}
		encoded, canonical, err := encodeDispositionCursor(cursor)
		if err != nil {
			t.Fatalf("a decoded cursor did not re-encode: %v", err)
		}
		if canonical != cursor {
			t.Fatalf("a decoded cursor was not canonical: %+v want %+v", canonical, cursor)
		}
		again, err := decodeDispositionCursor(encoded)
		if err != nil {
			t.Fatalf("a re-encoded cursor did not decode: %v", err)
		}
		reencoded, _, err := encodeDispositionCursor(again)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		if !bytes.Equal(encoded, reencoded) {
			t.Fatalf("canonicalization has no fixed point:\n%s\n%s", encoded, reencoded)
		}
	})
}
