package sessionstore

import (
	"bytes"
	"encoding/json"
	"testing"
)

// FuzzReconciliationClaimCodec fuzzes stored claim bytes. Its seeds are real
// encodings rather than hand-written JSON, so a mutation starts from a value
// that already reaches the strict decoder, the identity validators and the
// instant rules, instead of bouncing off the first json.Unmarshal.
//
// The property is that canonicalization reaches a fixed point: anything the
// decoder accepts must re-encode, and decoding that encoding must produce the
// identical bytes again. It deliberately does not claim the decoder rejects
// non-canonical input — the decoder is a NORMALIZER, and what is guarded is
// that normalizing twice can never differ from normalizing once.
func FuzzReconciliationClaimCodec(f *testing.F) {
	seed := func(claim ReconciliationClaim) []byte {
		encoded, _, err := encodeReconciliationClaim(claim)
		if err != nil {
			f.Fatalf("seed does not encode: %v", err)
		}
		return encoded
	}
	held := testReconciliationClaim()
	f.Add(seed(held))

	// The released spelling, whose expiry equals its claim instant, is a
	// distinct branch of the instant rule and must be explored from the inside.
	released := held
	released.ExpiresAt = released.ClaimedAt
	f.Add(seed(released))

	var members map[string]json.RawMessage
	if err := json.Unmarshal(seed(held), &members); err != nil {
		f.Fatalf("seed is not JSON: %v", err)
	}
	for _, mutate := range []func(map[string]json.RawMessage){
		func(m map[string]json.RawMessage) { m["record_version"] = json.RawMessage("2") },
		func(m map[string]json.RawMessage) { m["surprise"] = json.RawMessage("1") },
		func(m map[string]json.RawMessage) { m["holder_id"] = json.RawMessage(`""`) },
		func(m map[string]json.RawMessage) { m["expires_at"] = json.RawMessage(`"1970-01-01T00:00:00Z"`) },
		func(m map[string]json.RawMessage) { m["claimed_at"] = json.RawMessage(`"3000-01-01T00:00:00Z"`) },
		// The two rankable-instant branches, one per member. Each is the only
		// refuser of its own shape, so seeding one would leave the fuzzer
		// exploring from inside one of the two rejections and not the other.
		func(m map[string]json.RawMessage) { m["expires_at"] = json.RawMessage(`"5000-01-01T00:00:00Z"`) },
		func(m map[string]json.RawMessage) { m["claimed_at"] = json.RawMessage(`"0001-01-01T00:00:00Z"`) },
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
		claim, err := decodeReconciliationClaim(value)
		if err != nil {
			return
		}
		encoded, canonical, err := encodeReconciliationClaim(claim)
		if err != nil {
			t.Fatalf("a decoded claim did not re-encode: %v", err)
		}
		if canonical != claim {
			t.Fatalf("a decoded claim was not canonical: %+v want %+v", canonical, claim)
		}
		again, err := decodeReconciliationClaim(encoded)
		if err != nil {
			t.Fatalf("a re-encoded claim did not decode: %v", err)
		}
		reencoded, _, err := encodeReconciliationClaim(again)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		if !bytes.Equal(encoded, reencoded) {
			t.Fatalf("canonicalization has no fixed point:\n%s\n%s", encoded, reencoded)
		}
	})
}
