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
		token, err := dueCommandCursor.encode(store, shard, bound, storage.DueCursor(after))
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
	gate, err := dueGateCursor.encode(store, 1, 1, "provider-position")
	if err != nil {
		f.Fatalf("seed does not encode: %v", err)
	}
	f.Add(string(gate))

	// Both kinds are driven, and each is required to refuse the other's tokens
	// and every other shard's. Driving only the commands decoder would leave
	// the gates kind measuring nothing, which is the asymmetry this package
	// has already been bitten by once.
	kinds := []struct {
		name string
		kind sweepCursorKind
	}{
		{"due_commands", dueCommandCursor},
		{"due_gates", dueGateCursor},
	}

	f.Fuzz(func(t *testing.T, token string) {
		for _, kind := range kinds {
			fuzzOneSweepCursorKind(t, store, kind.name, kind.kind, token)
		}
		// And no token is ever accepted by BOTH kinds. The two share the
		// envelope grammar and differ only in their magic and their scope
		// domain, so this is the assertion that keeps them separate rather
		// than merely differently spelled.
		for shard := range uint32(4) {
			_, _, commands := dueCommandCursor.decode(store, shard, sessionwire.Cursor(token))
			_, _, gates := dueGateCursor.decode(store, shard, sessionwire.Cursor(token))
			if commands == nil && gates == nil {
				t.Fatalf("shard %d accepted %q as both a due-commands and a due-gates continuation", shard, token)
			}
		}
	})
}

// fuzzOneSweepCursorKind holds one cursor kind to the two properties a sweep
// continuation owes its caller: it must not panic on arbitrary bytes, and what
// it ACCEPTS must be exactly what this store issued for exactly that shard.
func fuzzOneSweepCursorKind(t *testing.T, store *Store, name string, kind sweepCursorKind, token string) {
	t.Helper()
	for shard := range uint32(4) {
		bound, after, err := kind.decode(store, shard, sessionwire.Cursor(token))
		if err != nil {
			continue
		}
		// Accepted. It must be exactly what this store would issue for this
		// shard from what it just decoded — which is what makes acceptance a
		// statement about origin rather than about shape.
		reissued, err := kind.encode(store, shard, bound, after)
		if err != nil {
			t.Fatalf("%s: a token this decoder accepted cannot be reissued: %v", name, err)
		}
		if string(reissued) != token {
			t.Fatalf("%s: accepted a noncanonical token %q; this store would issue %q", name, token, reissued)
		}
		if len(after) == 0 {
			t.Fatalf("%s: accepted a continuation with no provider position: %q", name, token)
		}
		// And it must be accepted for THIS shard alone.
		for other := range uint32(4) {
			if other == shard {
				continue
			}
			if _, _, err := kind.decode(store, other, sessionwire.Cursor(token)); err == nil {
				t.Fatalf("%s: a shard %d continuation was accepted for shard %d", name, shard, other)
			}
		}
	}
}
