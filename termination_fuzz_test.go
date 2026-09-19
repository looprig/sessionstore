package sessionstore

import (
	"bytes"
	"encoding/json"
	"testing"
)

// FuzzPlacementTerminationCodec fuzzes stored termination bytes, seeded from
// real encodings so a mutation starts inside the strict decoder and the
// record's own rules rather than bouncing off the first json.Unmarshal.
//
// The property is that canonicalization reaches a fixed point: anything the
// decoder accepts must re-encode, and decoding that encoding must produce the
// identical bytes again. A canonicalizer without one would make the stored
// bytes — which carry the generation high-water every later write is measured
// against, and the content an idempotent replay is compared with — depend on
// how many times the record had been rewritten.
//
// It also asserts the pairing rules a reader of an accepted record relies on:
// graceful carries no reason and a nonzero epoch, forced carries a known
// reason, and the generation is never zero.
func FuzzPlacementTerminationCodec(f *testing.F) {
	seed := func(record PlacementTermination) []byte {
		encoded, _, err := encodePlacementTermination(record)
		if err != nil {
			f.Fatalf("seed does not encode: %v", err)
		}
		return encoded
	}
	forced := testPlacementTermination()
	f.Add(seed(forced))
	graceful := testPlacementTermination()
	graceful.Kind, graceful.ForcedReason = PlacementTerminationGraceful, ""
	f.Add(seed(graceful))

	var members map[string]json.RawMessage
	if err := json.Unmarshal(seed(forced), &members); err != nil {
		f.Fatalf("seed is not JSON: %v", err)
	}
	for _, mutate := range []func(map[string]json.RawMessage){
		func(m map[string]json.RawMessage) { m["record_version"] = json.RawMessage("2") },
		func(m map[string]json.RawMessage) { m["surprise"] = json.RawMessage("1") },
		func(m map[string]json.RawMessage) { m["generation"] = json.RawMessage("0") },
		func(m map[string]json.RawMessage) { m["kind"] = json.RawMessage(`"graceful"`) },
		func(m map[string]json.RawMessage) { delete(m, "forced_reason") },
		func(m map[string]json.RawMessage) { m["lease_epoch"] = json.RawMessage("0") },
		func(m map[string]json.RawMessage) { m["forced_reason"] = json.RawMessage(`"drain_refused"`) },
		func(m map[string]json.RawMessage) { m["objects"] = json.RawMessage(`[]`) },
		func(m map[string]json.RawMessage) { m["recorded_at"] = json.RawMessage(`"2026-09-18T09:30:00+02:00"`) },
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
		decoded, err := decodePlacementTermination(value)
		if err != nil {
			return
		}
		encoded, canonical, err := encodePlacementTermination(decoded)
		if err != nil {
			t.Fatalf("an accepted record does not re-encode: %v", err)
		}
		again, err := decodePlacementTermination(encoded)
		if err != nil {
			t.Fatalf("a canonical encoding does not decode: %v", err)
		}
		encodedAgain, _, err := encodePlacementTermination(again)
		if err != nil {
			t.Fatalf("a canonical record does not re-encode: %v", err)
		}
		if !bytes.Equal(encoded, encodedAgain) {
			t.Fatalf("canonicalization has no fixed point:\n%s\n%s", encoded, encodedAgain)
		}
		if canonical != again {
			t.Fatal("a round trip changed the record's content")
		}
		if canonical.Generation == 0 {
			t.Fatal("an accepted record has generation zero")
		}
		switch canonical.Kind {
		case PlacementTerminationGraceful:
			if canonical.ForcedReason != "" || canonical.LeaseEpoch == 0 {
				t.Fatalf("an accepted graceful record = %+v", canonical)
			}
		case PlacementTerminationForced:
			if !canonical.ForcedReason.known() {
				t.Fatalf("an accepted forced record carries reason %q", canonical.ForcedReason)
			}
		default:
			t.Fatalf("an accepted record carries kind %q", canonical.Kind)
		}
	})
}
