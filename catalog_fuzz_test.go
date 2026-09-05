package sessionstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// FuzzCatalogRecordCodec fuzzes stored catalog bytes. Its seeds are real
// encodings rather than hand-written JSON, so a mutation starts from a value
// that already reaches the strict decoder, the identity and range validators,
// the gate list, and the checkpoint branch, instead of bouncing off the first
// json.Unmarshal.
//
// The property is that canonicalization reaches a fixed point: anything the
// decoder accepts must re-encode, and decoding that encoding must produce the
// identical bytes again.
//
// It deliberately does NOT claim the decoder rejects non-canonical input, and
// it never compares the encoding against the fuzzer's own bytes. The decoder is
// a NORMALIZER, not a canonical-form validator: an out-of-order open_gates list
// is accepted and silently reordered, and a duplicate JSON member is accepted
// with encoding/json's last-wins rule. Normalizing is the right behaviour for a
// stored projection, so what this target guards is that normalizing twice can
// never differ from normalizing once — a canonicalizer without a fixed point
// would make a record's stored bytes depend on how many times it had been
// rewritten.
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
	bound := full
	bound.Binding = testSessionBinding()
	f.Add(seed(bound))
	bound.Binding.ProtocolMode = ProtocolModeLegacy
	f.Add(seed(bound))

	minimal := full
	minimal.OpenGates = nil
	minimal.Checkpoint = CheckpointSummary{}
	minimal.LastEventID = ""
	minimal.RuntimeCompatibilityID = ""
	minimal.DesiredIdempotencyKey = ""
	minimal.DesiredWorkload = DesiredWorkload{}
	minimal.LeaseEpoch = 0
	f.Add(seed(minimal))

	dedicated := full
	dedicated.DesiredPlacement = sessionwire.HostPlacementDedicated
	dedicated.DesiredWorkload = testDesiredWorkload()
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

// FuzzCatalogCursorCodec fuzzes the listing cursor envelope.
//
// Its seeds are cursors this store really issued, plus one derived variant per
// envelope field, so a mutation starts from a token that already reaches the
// length gate, the canonical-spelling check, the magic, the version, and the
// tenant scope comparison rather than bouncing off the base64 decoder.
//
// The property is that acceptance is exact: a cursor the decoder accepts must
// re-encode, for the same tenant, to the identical string. That is what makes
// the envelope a one-to-one wrapper around a provider token — if any accepted
// spelling re-encoded differently, two distinct strings would name one position
// and a caller could perturb a token while still being resumed.
func FuzzCatalogCursorCodec(f *testing.F) {
	store, err := Open(context.Background(), memstore.New())
	if err != nil {
		f.Fatalf("Open: %v", err)
	}
	f.Cleanup(func() { store.Close(context.Background()) })

	issue := func(token storage.RankedCursor) string {
		cursor, err := store.encodeCatalogCursor(catalogTenant, token)
		if err != nil {
			f.Fatalf("seed does not encode: %v", err)
		}
		return string(cursor)
	}
	valid := issue("provider-token")
	f.Add(valid)
	f.Add(issue(storage.RankedCursor(bytes.Repeat([]byte{0xff}, 64))))
	f.Add(issue("x"))
	f.Add(string(mustEncodeCatalogCursorFor(f, store, catalogOtherTenant)))
	f.Add(string(store.encodeJournalCursor(journalCursorPublic, catalogTenant, catalogSession, 1, 1)))

	// One seed per envelope field, derived from a real token rather than
	// written as a literal, so the fuzzer explores from inside each rejection
	// path as well as from an accepted cursor.
	token, err := base64.RawURLEncoding.DecodeString(valid)
	if err != nil {
		f.Fatalf("a cursor this store issued is not base64url: %v", err)
	}
	for _, mutate := range []func([]byte){
		func(t []byte) { copy(t[cursorMagicAt:], "XXXX") },
		func(t []byte) { t[cursorVersionAt]++ },
		func(t []byte) { t[cursorScopeAt]++ },
	} {
		copied := append([]byte(nil), token...)
		mutate(copied)
		f.Add(base64.RawURLEncoding.EncodeToString(copied))
	}
	f.Add(base64.RawURLEncoding.EncodeToString(token[:cursorPayloadAt]))
	f.Add(valid + strings.Repeat("A", maxCatalogCursorBytes))
	f.Add("")

	f.Fuzz(func(t *testing.T, cursor string) {
		next, err := store.decodeCatalogCursor(catalogTenant, sessionwire.Cursor(cursor))
		if err != nil {
			return
		}
		if next == "" {
			t.Fatal("accepted a cursor carrying no provider token")
		}
		again, err := store.encodeCatalogCursor(catalogTenant, next)
		if err != nil {
			t.Fatalf("accepted a cursor that does not re-encode: %v", err)
		}
		if string(again) != cursor {
			t.Fatalf("acceptance is not exact:\nin  %q\nout %q", cursor, again)
		}
	})
}

func mustEncodeCatalogCursorFor(f *testing.F, store *Store, tenant sessionwire.TenantID) sessionwire.Cursor {
	f.Helper()
	cursor, err := store.encodeCatalogCursor(tenant, "provider-token")
	if err != nil {
		f.Fatalf("seed does not encode: %v", err)
	}
	return cursor
}
