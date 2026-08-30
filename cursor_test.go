package sessionstore

import "testing"

// TestEncodeCursorEnvelopeRejectsAMagicOfTheWrongWidth pins the one invariant
// the envelope cannot recover from. The magic occupies a fixed four bytes; a
// shorter one would leave the remaining bytes zero and a longer one would be
// silently truncated into the version and scope fields, in both cases producing
// a token that this package would issue and then refuse to accept back.
//
// Every caller passes a package constant, so this is a programming error rather
// than an input, and it fails loudly at the point of the mistake instead of
// becoming an unresumable cursor a user reports much later.
func TestEncodeCursorEnvelopeRejectsAMagicOfTheWrongWidth(t *testing.T) {
	for _, magic := range []string{"", "LRC", "LRCPX"} {
		t.Run(magic, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("a %d-byte magic was encoded into a %d-byte field", len(magic), cursorMagicBytes)
				}
			}()
			encodeCursorEnvelope(magic, 1, [cursorScopeBytes]byte{}, []byte("payload"))
		})
	}
}
