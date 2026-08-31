// Fuzz targets for the Host target directory: the stored record's codec and
// both of the cursors this record issues. They live in their own file for the
// reason every other record's do — a fuzz target is driven differently from a
// test and is read differently.
package sessionstore

import (
	"bytes"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// FuzzHostTargetCodec fuzzes stored advertisement bytes. Its seeds are real
// encodings rather than hand-written JSON, so a mutation starts from a value
// that already reaches the strict decoder, the record's own rules, and the
// whole of Core's capacity validation, instead of bouncing off the first
// json.Unmarshal.
//
// The property is that canonicalization reaches a fixed point: anything the
// decoder accepts must re-encode, and decoding that encoding must produce the
// identical bytes again. Without one, a row's stored bytes would depend on how
// many times it had been rewritten — and this row is rewritten on every
// heartbeat, so that is a defect the fleet would reach within minutes.
//
// It also asserts the two invariants a reader of an accepted row relies on that
// the codec alone does not state: that BOTH derived views are computable from
// any accepted record and agree with its state, and that an accepted advertised
// record can always be projected into Core's report. A record that decoded but
// could not be ranked would be a row nothing could file.
func FuzzHostTargetCodec(f *testing.F) {
	seed := func(record HostTarget) []byte {
		encoded, _, err := encodeHostTarget(record)
		if err != nil {
			f.Fatalf("seed does not encode: %v", err)
		}
		return encoded
	}
	f.Add(seed(testHostTarget()))

	dedicated := testHostTarget()
	dedicated.Key.Placement = sessionwire.HostPlacementDedicated
	dedicated.Advertisement.AvailableCapacity = 1
	dedicated.Advertisement.IsolationClass = sessionwire.HostIsolationClassTenantExclusive
	dedicated.Advertisement.Accepting = false
	f.Add(seed(dedicated))

	withdrawn := testHostTarget()
	withdrawn.Advertisement = nil
	f.Add(seed(withdrawn))

	f.Fuzz(func(t *testing.T, value []byte) {
		record, err := decodeHostTarget(value)
		if err != nil {
			return
		}
		encoded, canonical, err := encodeHostTarget(record)
		if err != nil {
			t.Fatalf("an accepted record does not re-encode: %v", err)
		}
		again, err := decodeHostTarget(encoded)
		if err != nil {
			t.Fatalf("an encoding of an accepted record does not decode: %v", err)
		}
		twice, _, err := encodeHostTarget(again)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		if !bytes.Equal(encoded, twice) {
			t.Fatalf("canonicalization has no fixed point:\n%s\n%s", encoded, twice)
		}

		// Both views must be computable and must agree with the record's state,
		// which is what every write files and every read checks.
		rank, due := hostTargetRank(canonical), hostTargetDue(canonical)
		if canonical.Advertisement == nil {
			if rank != (storage.Rank{}) || due != (storage.Due{}) {
				t.Fatalf("a withdrawn record is still in a view: %+v %+v", rank, due)
			}
			if _, err := canonical.Report(); err == nil {
				t.Fatal("a withdrawn record projected a capacity report")
			}
			return
		}
		if due != (storage.Due{State: storage.DueAt, UnixMillis: canonical.Advertisement.ExpiresAt.UnixMilli()}) {
			t.Fatalf("an advertised record is not due at its own expiry: %+v", due)
		}
		if rank.Ranked && rank.Value < 0 {
			t.Fatalf("an accepted capacity produced a negative rank: %+v", rank)
		}
		if _, err := canonical.Report(); err != nil {
			t.Fatalf("an accepted advertisement cannot be projected: %v", err)
		}
	})
}

// FuzzHostTargetCursors fuzzes both cursor decoders this record owns.
//
// The sweep cursor is the only cursor in this package with an INTERNAL PAYLOAD
// GRAMMAR — a fixed-width bound followed by a variable-length provider token —
// and therefore the only one whose decoder indexes into its payload. A payload
// shorter than the bound would panic rather than be refused, so the length gate
// that prevents it is fuzzed rather than argued.
//
// The properties are that neither decoder panics on any input, that neither
// accepts a token this store did not issue for that exact kind, and that a
// token which IS accepted round-trips to the values it was issued with.
func FuzzHostTargetCursors(f *testing.F) {
	store, err := Open(f.Context(), memstore.New())
	if err != nil {
		f.Fatalf("Open: %v", err)
	}
	f.Cleanup(func() { _ = store.Close(f.Context()) })

	key := HostTargetKey{AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1", Placement: sessionwire.HostPlacementPooled}
	placement, err := store.encodeHostTargetCursor(key, storage.RankedCursor("provider-token"))
	if err != nil {
		f.Fatalf("encodeHostTargetCursor: %v", err)
	}
	sweep, err := store.encodeHostTargetSweepCursor(1756648800000, storage.DueCursor("provider-token"))
	if err != nil {
		f.Fatalf("encodeHostTargetSweepCursor: %v", err)
	}
	f.Add(string(placement))
	f.Add(string(sweep))
	f.Add("")
	f.Add("not-a-cursor")

	f.Fuzz(func(t *testing.T, token string) {
		if payload, ok := store.decodeHostTargetCursor(key, sessionwire.Cursor(token)); ok == nil {
			// An accepted placement token must be one this store issued for
			// this target, and must carry back exactly what it was issued with.
			reissued, err := store.encodeHostTargetCursor(key, payload)
			if err != nil {
				t.Fatalf("an accepted cursor does not re-issue: %v", err)
			}
			if string(reissued) != token {
				t.Fatalf("an accepted cursor is not the one this store issues for its payload:\n%s\n%s", token, reissued)
			}
		}
		bound, after, err := store.decodeHostTargetSweepCursor(sessionwire.Cursor(token))
		if err != nil {
			return
		}
		// A sweep token this store issued always carries a provider position:
		// an exhausted walk issues no cursor at all.
		if after == "" {
			t.Fatalf("an accepted sweep cursor carried no provider position (bound %d)", bound)
		}
		reissued, err := store.encodeHostTargetSweepCursor(bound, after)
		if err != nil {
			t.Fatalf("an accepted sweep cursor does not re-issue: %v", err)
		}
		if string(reissued) != token {
			t.Fatalf("an accepted sweep cursor is not the one this store issues:\n%s\n%s", token, reissued)
		}
	})
}
