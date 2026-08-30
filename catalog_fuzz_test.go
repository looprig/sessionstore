package sessionstore

import (
	"bytes"
	"encoding/json"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// FuzzCatalogRecordCodec fuzzes stored catalog bytes. Its seeds are real
// encodings rather than hand-written JSON, so a mutation starts from a value
// that already reaches the strict decoder, the identity and range validators,
// the gate list, and the checkpoint branch, instead of bouncing off the first
// json.Unmarshal.
//
// The property is canonical stability: anything the decoder accepts must
// re-encode, and re-decoding that encoding must produce the identical bytes. A
// decoder that accepted two spellings of one record, or a canonicalizer that
// did not reach a fixed point, would break it.
func FuzzCatalogRecordCodec(f *testing.F) {
	seed := func(record CatalogRecord) []byte {
		encoded, err := encodeCatalogRecord(record)
		if err != nil {
			f.Fatalf("seed does not encode: %v", err)
		}
		return encoded
	}
	full := testCatalogRecord()
	f.Add(seed(full))

	minimal := full
	minimal.OpenGates = nil
	minimal.Checkpoint = CheckpointSummary{}
	minimal.LastEventID = ""
	minimal.RuntimeCompatibilityID = ""
	minimal.DesiredIdempotencyKey = ""
	minimal.LeaseEpoch = 0
	f.Add(seed(minimal))

	dedicated := full
	dedicated.DesiredPlacement = sessionwire.HostPlacementDedicated
	dedicated.OpenGates = []sessionwire.GateProjection{testGate("gate-a", 1)}
	f.Add(seed(dedicated))

	// One seed per fail-closed branch, so the fuzzer explores from inside the
	// rejection paths as well as from a valid record.
	var members map[string]json.RawMessage
	if err := json.Unmarshal(seed(full), &members); err != nil {
		f.Fatalf("seed is not JSON: %v", err)
	}
	for _, mutate := range []func(map[string]json.RawMessage){
		func(m map[string]json.RawMessage) { m["record_version"] = json.RawMessage("2") },
		func(m map[string]json.RawMessage) { m["surprise"] = json.RawMessage("1") },
		func(m map[string]json.RawMessage) { m["last_active_at"] = json.RawMessage(`"3000-01-01T00:00:00Z"`) },
		func(m map[string]json.RawMessage) { m["tenant_id"] = json.RawMessage(`""`) },
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
		record, err := decodeCatalogRecord(value)
		if err != nil {
			return
		}
		encoded, err := encodeCatalogRecord(record)
		if err != nil {
			t.Fatalf("accepted a record that does not re-encode: %v", err)
		}
		if len(encoded) > MaxCatalogRecordBytes {
			t.Fatalf("re-encoded record exceeds the catalog bound: %d", len(encoded))
		}
		again, err := decodeCatalogRecord(encoded)
		if err != nil {
			t.Fatalf("canonical form does not decode: %v", err)
		}
		reencoded, err := encodeCatalogRecord(again)
		if err != nil {
			t.Fatalf("canonical form does not re-encode: %v", err)
		}
		if !bytes.Equal(encoded, reencoded) {
			t.Fatalf("canonicalization is not a fixed point:\n%s\n%s", encoded, reencoded)
		}
		// The rank a catalog write derives must be defined for every accepted
		// record; an out-of-range instant would wrap rather than sort late.
		if rank := catalogRank(record); !rank.Ranked || rank.Value != record.LastActiveAt.UnixNano() {
			t.Fatalf("accepted a record with an underivable rank: %+v", rank)
		}
	})
}
