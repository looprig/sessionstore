package sessionstore

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// FuzzHostRegistrationCodec fuzzes stored registration bytes. Its seeds are
// real encodings rather than hand-written JSON, so a mutation starts from a
// value that already reaches the strict decoder, the record's own rules, and
// the whole of Core's route validation, instead of bouncing off the first
// json.Unmarshal.
//
// The property is that canonicalization reaches a fixed point: anything the
// decoder accepts must re-encode, and decoding that encoding must produce the
// identical bytes again. A canonicalizer without a fixed point would make a
// record's stored bytes depend on how many times it had been rewritten — and
// for this record that is worse than untidy, because the bytes carry a fencing
// high-water mark that every later write is measured against.
//
// It also asserts the two invariants a reader of an accepted record relies on
// that the codec alone does not state: that a route and a tombstone are
// distinguishable in the way routableAt distinguishes them, and that an
// accepted live record can always be projected into Core's observation.
func FuzzHostRegistrationCodec(f *testing.F) {
	seed := func(record HostRegistration) []byte {
		encoded, _, err := encodeHostRegistration(record)
		if err != nil {
			f.Fatalf("seed does not encode: %v", err)
		}
		return encoded
	}
	live := testHostRegistration()
	f.Add(seed(live))

	dedicated := testHostRegistration()
	dedicated.Route.Placement = sessionwire.HostPlacementDedicated
	dedicated.Route.Accepting = false
	dedicated.Route.Residency = sessionwire.SessionResidencyReleasing
	f.Add(seed(dedicated))

	tombstone := testHostRegistration()
	tombstone.Route = nil
	tombstone.ExpiresAt = tombstone.ObservedAt
	f.Add(seed(tombstone))

	// One seed per fail-closed branch, derived from a real record rather than
	// written as a literal, so the fuzzer explores from inside the rejection
	// paths as well as from an accepted registration.
	var members map[string]json.RawMessage
	if err := json.Unmarshal(seed(live), &members); err != nil {
		f.Fatalf("seed is not JSON: %v", err)
	}
	for _, mutate := range []func(map[string]json.RawMessage){
		func(m map[string]json.RawMessage) { m["record_version"] = json.RawMessage("2") },
		func(m map[string]json.RawMessage) { m["surprise"] = json.RawMessage("1") },
		func(m map[string]json.RawMessage) { m["lease_epoch"] = json.RawMessage("0") },
		func(m map[string]json.RawMessage) { m["expires_at"] = json.RawMessage(`"3000-01-01T00:00:00Z"`) },
		func(m map[string]json.RawMessage) { m["expires_at"] = m["observed_at"] },
		func(m map[string]json.RawMessage) { delete(m, "route") },
		func(m map[string]json.RawMessage) {
			m["route"] = json.RawMessage(`{"host_id":"host-a","host_generation":1}`)
		},
		func(m map[string]json.RawMessage) {
			m["route"] = json.RawMessage(`{"host_id":"h","host_generation":1,"agent_id":"a","runtime_compatibility_id":"r","placement":"pooled","internal_endpoint":"https://h/x","residency":"resident","accepting":true}`)
		},
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
		record, err := decodeHostRegistration(value)
		if err != nil {
			return
		}
		encoded, _, err := encodeHostRegistration(record)
		if err != nil {
			t.Fatalf("accepted a record that does not re-encode: %v", err)
		}
		if len(encoded) > MaxHostRegistrationRecordBytes {
			t.Fatalf("re-encoded record exceeds the registry bound: %d", len(encoded))
		}
		again, err := decodeHostRegistration(encoded)
		if err != nil {
			t.Fatalf("canonical form does not decode: %v", err)
		}
		reencoded, _, err := encodeHostRegistration(again)
		if err != nil {
			t.Fatalf("canonical form does not re-encode: %v", err)
		}
		if !bytes.Equal(encoded, reencoded) {
			t.Fatalf("canonicalization is not a fixed point:\n%s\n%s", encoded, reencoded)
		}
		if record.Route == nil {
			// A tombstone is unroutable at every instant, including the one it
			// was written at and every instant before it.
			for _, now := range []time.Time{minRankableTime, record.ObservedAt, maxRankableTime} {
				if err := routableAt(record, now); err == nil {
					t.Fatalf("a tombstone was a route at %v", now)
				}
			}
			return
		}
		// A live record is a route strictly before its expiry and not at it,
		// and it can always be projected into the form a Factory routes with.
		if err := routableAt(record, record.ExpiresAt); err == nil {
			t.Fatal("a route survived its own expiry instant")
		}
		if err := routableAt(record, record.ObservedAt); err != nil {
			t.Fatalf("an accepted live record was not a route when it was observed: %v", err)
		}
		if _, err := record.Observation(); err != nil {
			t.Fatalf("accepted a live record that cannot be projected: %v", err)
		}
	})
}
