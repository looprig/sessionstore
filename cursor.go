package sessionstore

import (
	"bytes"
	"encoding/base64"
)

// Cursor envelope grammar, stated exactly once for the whole package.
//
//	cursor = base64url-raw( magic[4] version[1] scope[32] payload[...] )
//
// Every page token this package issues has this shape. The magic names the
// cursor KIND, so a token issued for one query family can never be replayed
// into another; the version allows the grammar to change without a reader
// guessing which spelling it was handed; and the scope is a domain-separated
// digest of the identities the cursor was issued for, so a token cannot be
// silently resumed against different ones. The payload is whatever the kind
// needs and is opaque to this file.
//
// The scope field is a binding TAG, not a MAC. The digest is unkeyed over
// public inputs, so anyone who knows a tenant or session id can construct a
// well-formed envelope naming it. What the tag prevents is a cursor being
// MISTAKENLY replayed against the wrong identities or the wrong cursor kind;
// it confers no authority whatsoever. Authorization is the identity in the
// request — a caller must authorize that before calling this package, and must
// never treat possession of a cursor as evidence of anything.
//
// The offsets are declared once and both halves use them, so encode and decode
// cannot drift apart, and a third cursor kind gets the grammar rather than a
// third copy of it.
const (
	cursorMagicBytes = 4
	cursorScopeBytes = 32

	cursorMagicAt   = 0
	cursorVersionAt = cursorMagicAt + cursorMagicBytes
	cursorScopeAt   = cursorVersionAt + 1
	cursorPayloadAt = cursorScopeAt + cursorScopeBytes
)

// encodeCursorEnvelope wraps payload in the envelope above. magic must be
// exactly cursorMagicBytes long; a caller passes a package constant, so a wrong
// one is a programming error rather than an input this can be handed.
func encodeCursorEnvelope(magic string, version byte, scope [cursorScopeBytes]byte, payload []byte) string {
	if len(magic) != cursorMagicBytes {
		panic("sessionstore: internal cursor magic invariant")
	}
	token := make([]byte, cursorPayloadAt, cursorPayloadAt+len(payload))
	copy(token[cursorMagicAt:cursorVersionAt], magic)
	token[cursorVersionAt] = version
	copy(token[cursorScopeAt:cursorPayloadAt], scope[:])
	token = append(token, payload...)
	return base64.RawURLEncoding.EncodeToString(token)
}

// decodeCursorEnvelope unwraps a cursor and returns its payload, reporting
// false for anything this package did not issue for exactly this magic,
// version, and scope. The payload length must lie in [minPayload, maxPayload];
// a kind with a fixed-size payload passes the same value for both.
//
// It returns a bool rather than an error so each caller keeps its own error
// vocabulary: a rejected cursor is a catalog failure in one caller and a
// journal failure in the other, and neither should have to translate the
// other's.
func decodeCursorEnvelope(
	magic string,
	version byte,
	scope [cursorScopeBytes]byte,
	cursor string,
	minPayload, maxPayload int,
) ([]byte, bool) {
	// Bound the decode BEFORE it allocates: DecodeString sizes its own
	// destination from the caller's string, so an unbounded cursor would make
	// this reader allocate in proportion to attacker-supplied input. For
	// RawURLEncoding DecodedLen is exact, so a successful decode after this
	// gate has a length inside these bounds and the slices below cannot be out
	// of range — no second length check is needed.
	size := base64.RawURLEncoding.DecodedLen(len(cursor))
	if size < cursorPayloadAt+minPayload || size > cursorPayloadAt+maxPayload {
		return nil, false
	}
	token, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, false
	}
	// Unpadded base64 has slack in its final character: the low bits of the
	// last group are dropped, so several distinct strings decode to identical
	// bytes. Require the exact spelling this writer emits, so one position has
	// one cursor and a token cannot be perturbed while still being accepted.
	if base64.RawURLEncoding.EncodeToString(token) != cursor {
		return nil, false
	}
	if string(token[cursorMagicAt:cursorVersionAt]) != magic || token[cursorVersionAt] != version {
		return nil, false
	}
	if !bytes.Equal(token[cursorScopeAt:cursorPayloadAt], scope[:]) {
		return nil, false
	}
	return token[cursorPayloadAt:], true
}
