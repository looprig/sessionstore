package sessionstore

import (
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// FuzzSweepCursorCodec drives the two sweep continuations from the side an
// attacker has: arbitrary bytes presented as a cursor.
//
// It asserts the two properties a cursor codec owes its caller. It must not
// panic — the decoder slices a payload whose length it bounded before decoding,
// and that bound is the only thing between it and an index out of range. And
// what it ACCEPTS must be exactly what this store issued for this shard: a
// token accepted for another shard resumes one sweep at another's position, and
// since a due position is a frozen tuple, that means silently skipping every
// row of the target shard that sorts before it.
//
// The seeds are real encodings from this store, plus one per rejection branch,
// so the fuzzer explores from inside each fail-closed path as well as from an
// accepted token. A seed that never reaches the logic would leave the whole
// target measuring the base64 decoder.
func FuzzSweepCursorCodec(f *testing.F) {
	store, err := Open(f.Context(), memstore.New(), WithControlShards(4))
	if err != nil {
		f.Fatalf("Open: %v", err)
	}
	f.Cleanup(func() { _ = store.Close(f.Context()) })

	issue := func(shard uint32, bound int64, after string) string {
		token, err := store.encodeDueCommandCursor(shard, bound, storage.DueCursor(after))
		if err != nil {
			f.Fatalf("seed does not encode: %v", err)
		}
		return string(token)
	}
	valid := issue(1, 1_700_000_000_000, "provider-position")
	f.Add(valid)
	f.Add(issue(0, 0, "p"))
	f.Add(issue(3, -1, strings.Repeat("p", 512)))
	// One seed per fail-closed branch, derived from the accepted token.
	f.Add(issue(2, 1, "provider-position")) // another shard's scope
	f.Add(valid[:len(valid)-1])             // truncated
	f.Add(valid + "=")                      // padded, which this encoder never emits
	f.Add(strings.ToUpper(valid))           // recased
	f.Add("")
	f.Add("not-base64-$$$")
	// A due-gates token: the same grammar, a different kind, and it must never
	// be accepted here.
	gate, err := store.encodeDueGateCursor(1, 1, "provider-position")
	if err != nil {
		f.Fatalf("seed does not encode: %v", err)
	}
	f.Add(string(gate))

	f.Fuzz(func(t *testing.T, token string) {
		for shard := range uint32(4) {
			bound, after, err := store.decodeDueCommandCursor(shard, sessionwire.Cursor(token))
			if err != nil {
				continue
			}
			// Accepted. It must be exactly what this store would issue for this
			// shard from what it just decoded — which is what makes acceptance
			// a statement about origin rather than about shape.
			reissued, err := store.encodeDueCommandCursor(shard, bound, after)
			if err != nil {
				t.Fatalf("a token this decoder accepted cannot be reissued: %v", err)
			}
			if string(reissued) != token {
				t.Fatalf("accepted a noncanonical token %q; this store would issue %q", token, reissued)
			}
			if len(after) == 0 {
				t.Fatalf("accepted a continuation with no provider position: %q", token)
			}
			// And it must be accepted for THIS shard alone.
			for other := range uint32(4) {
				if other == shard {
					continue
				}
				if _, _, err := store.decodeDueCommandCursor(other, sessionwire.Cursor(token)); err == nil {
					t.Fatalf("a shard %d continuation was accepted for shard %d", shard, other)
				}
			}
			// And never as the other kind.
			if _, _, err := store.decodeDueGateCursor(shard, sessionwire.Cursor(token)); err == nil {
				t.Fatalf("a due-commands continuation was accepted as a due-gates one: %q", token)
			}
		}
	})
}
