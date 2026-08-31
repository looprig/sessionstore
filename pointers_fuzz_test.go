package sessionstore

import (
	"bytes"
	"encoding/json"
	"testing"
)

// FuzzSessionPointerCodec fuzzes stored pointer bytes. Its seeds are real
// encodings rather than hand-written JSON, so a mutation starts from a value
// that already reaches the strict decoder, the role mapping, the target parser
// and the instant rules, instead of bouncing off the first json.Unmarshal.
//
// The property is that canonicalization reaches a fixed point: anything the
// decoder accepts must re-encode, and decoding that encoding must produce the
// identical bytes again. It deliberately does not claim the decoder rejects
// non-canonical input — the decoder is a NORMALIZER, and what is guarded is
// that normalizing twice can never differ from normalizing once.
//
// The target is where that property is most at risk, because it is the one
// member reached through another package's decoder: sessionwire's
// ObjectReference DROPS an undeclared member rather than refusing it, so a
// stored target carrying one decodes to a reference this package re-encodes
// WITHOUT it. That is a normalization, it is core's rule rather than this
// package's, and this fuzzer is what holds it to being idempotent.
func FuzzSessionPointerCodec(f *testing.F) {
	seed := func(pointer SessionPointer) []byte {
		encoded, _, err := encodeSessionPointer(pointer)
		if err != nil {
			f.Fatalf("seed does not encode: %v", err)
		}
		return encoded
	}
	// One live seed per declared role, so every arm of the role mapping is
	// explored from the inside rather than from a rejection.
	for _, kind := range sessionPointerKinds() {
		pointer := testSessionPointer()
		pointer.Kind = kind
		object, ok := kind.targetObjectKind()
		if !ok {
			f.Fatalf("%s names no object kind", kind)
		}
		target := testObjectReference(object, 1)
		pointer.Target = &target
		f.Add(seed(pointer))
	}

	// The cleared spelling is a distinct branch of the record and must be
	// explored from the inside too: it is the one shape with no target at all.
	cleared := testSessionPointer()
	cleared.Target = nil
	f.Add(seed(cleared))

	var members map[string]json.RawMessage
	if err := json.Unmarshal(seed(testSessionPointer()), &members); err != nil {
		f.Fatalf("seed is not JSON: %v", err)
	}
	for _, mutate := range []func(map[string]json.RawMessage){
		func(m map[string]json.RawMessage) { m["record_version"] = json.RawMessage("2") },
		func(m map[string]json.RawMessage) { m["surprise"] = json.RawMessage("1") },
		func(m map[string]json.RawMessage) { m["kind"] = json.RawMessage(`"invented"`) },
		func(m map[string]json.RawMessage) { m["lease_epoch"] = json.RawMessage("0") },
		func(m map[string]json.RawMessage) { m["sequence"] = json.RawMessage("0") },
		func(m map[string]json.RawMessage) { m["target"] = json.RawMessage(`{"object_id":"v1:artifact:x:y"}`) },
		// A target carrying a member core drops, which is the one member of
		// this record whose stored bytes and re-encoded bytes can differ.
		func(m map[string]json.RawMessage) {
			m["target"] = json.RawMessage(
				`{"object_id":"` + string(testObjectReference(ObjectKindWorkspaceCheckpoint, 1).ObjectID) + `","extra":1}`)
		},
		// The two rankable-instant branches, one per direction. Each is the
		// only refuser of its own shape.
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
		pointer, err := decodeSessionPointer(value)
		if err != nil {
			return
		}
		encoded, canonical, err := encodeSessionPointer(pointer)
		if err != nil {
			t.Fatalf("a decoded pointer did not re-encode: %v", err)
		}
		if canonical.cleared() != pointer.cleared() {
			t.Fatalf("canonicalization changed whether the pointer is cleared: %+v", canonical)
		}
		if !canonical.cleared() && *canonical.Target != *pointer.Target {
			t.Fatalf("a decoded target was not canonical: %+v want %+v", canonical.Target, pointer.Target)
		}
		canonical.Target, pointer.Target = nil, nil
		if canonical != pointer {
			t.Fatalf("a decoded pointer was not canonical: %+v want %+v", canonical, pointer)
		}
		again, err := decodeSessionPointer(encoded)
		if err != nil {
			t.Fatalf("a re-encoded pointer did not decode: %v", err)
		}
		reencoded, _, err := encodeSessionPointer(again)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		if !bytes.Equal(encoded, reencoded) {
			t.Fatalf("canonicalization has no fixed point:\n%s\n%s", encoded, reencoded)
		}
	})
}
