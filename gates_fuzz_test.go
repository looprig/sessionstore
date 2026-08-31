package sessionstore

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/looprig/storage"
)

// FuzzGateIntentCodec fuzzes stored deadline-intent bytes. Its seeds are real
// encodings and derived variants of them, so a mutation starts from a value
// that already reaches the strict decoder, the identity validators, and the
// deadline range check rather than bouncing off the first json.Unmarshal.
//
// The property is the one the catalog's own codec states: canonicalization
// reaches a fixed point. Anything the decoder accepts must re-encode, and
// decoding that encoding must produce the identical bytes again — otherwise an
// intent's stored bytes would depend on how many times it had been rewritten,
// and the identity a due reader validates against would not be stable.
//
// It also holds every accepted intent to the due state a writer would derive
// from it, because that is what an accepted intent is FOR: an intent whose
// deadline could not become an absolute due time would be stored and then never
// found.
func FuzzGateIntentCodec(f *testing.F) {
	seed := func(intent gateIntent) []byte {
		encoded, err := encodeGateIntent(intent)
		if err != nil {
			f.Fatalf("seed does not encode: %v", err)
		}
		return encoded
	}
	valid := gateIntent{
		TenantID:         "tenant-a",
		SessionID:        "session-a",
		GateID:           "gate-a",
		OpenedEventID:    "event-gate-a",
		OpenedJournalSeq: 5,
		Deadline:         catalogDeadline,
	}
	f.Add(seed(valid))

	extreme := valid
	extreme.OpenedJournalSeq = ^uint64(0)
	extreme.Deadline = maxRankableTime
	f.Add(seed(extreme))

	// One seed per fail-closed branch, derived from a real encoding, so the
	// fuzzer explores from inside each rejection path as well as from an
	// accepted intent.
	var members map[string]json.RawMessage
	if err := json.Unmarshal(seed(valid), &members); err != nil {
		f.Fatalf("seed is not JSON: %v", err)
	}
	for _, mutate := range []func(map[string]json.RawMessage){
		func(m map[string]json.RawMessage) { m["record_version"] = json.RawMessage("2") },
		func(m map[string]json.RawMessage) { m["surprise"] = json.RawMessage("1") },
		func(m map[string]json.RawMessage) { m["gate_id"] = json.RawMessage(`""`) },
		func(m map[string]json.RawMessage) { m["opened_journal_seq"] = json.RawMessage("0") },
		func(m map[string]json.RawMessage) { m["deadline"] = json.RawMessage(`"3000-01-01T00:00:00Z"`) },
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
		intent, err := decodeGateIntent(value)
		if err != nil {
			return
		}
		encoded, err := encodeGateIntent(intent)
		if err != nil {
			t.Fatalf("accepted an intent that does not re-encode: %v", err)
		}
		// Asserted against the ARITHMETIC ceiling rather than the storage
		// bound. MaxGateIntentBytes is what a stored record may be, and it
		// leaves nearly two kilobytes of slop that a new member could grow into
		// unnoticed; maxGateIntentEncodedBytes is the claim the file actually
		// makes about this record's size, so this is where the fuzzer can
		// falsify it.
		if len(encoded) > maxGateIntentEncodedBytes {
			t.Fatalf("re-encoded intent is %d bytes, above the %d its arithmetic allows", len(encoded), maxGateIntentEncodedBytes)
		}
		again, err := decodeGateIntent(encoded)
		if err != nil {
			t.Fatalf("canonical form does not decode: %v", err)
		}
		reencoded, err := encodeGateIntent(again)
		if err != nil {
			t.Fatalf("canonical form does not re-encode: %v", err)
		}
		if !bytes.Equal(encoded, reencoded) {
			t.Fatalf("canonicalization is not a fixed point:\n%s\n%s", encoded, reencoded)
		}
		if due := gateDue(intent.Deadline); due.State != storage.DueAt || due.UnixMillis != intent.Deadline.UnixMilli() {
			t.Fatalf("accepted an intent with an underivable due state: %+v", due)
		}
	})
}
