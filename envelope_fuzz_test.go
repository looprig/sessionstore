package sessionstore

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
)

func FuzzDecodeEnvelope(f *testing.F) {
	truncatedTLV := testFrame(EnvelopeKindRuntimeControl,
		testWireField{tagIdentity, []byte("record")},
		testWireField{tagRuntimeInline, []byte("x")},
	)
	truncatedTLV = bytes.Clone(truncatedTLV[:len(truncatedTLV)-1])
	truncatedTLV = truncatedTLV[:len(truncatedTLV):len(truncatedTLV)]
	truncatedTLV[11]--

	seeds := [][]byte{
		mustFuzzHex("4c524a45010100030000001b0100000002653102000000077b2278223a317d0400000003007274"),
		mustFuzzHex("4c524a45010200020000001501000000057265632d310400000006736563726574"),
		mustFuzzHex("4c524a45010300010000000d06000000080000000000000007"),
		mustFuzzHex("4c524a4501040004000000360100000005636d642f4106000000080000000000000009070000001000112233445566778899aabbccddeeff0800000005696e707574"),
		nil,
		make([]byte, 16),
		// Near-valid frames whose outer count and length are internally
		// consistent but whose known tags are forbidden for the kind.
		testFrame(EnvelopeKindOpeningFence,
			testWireField{tagIdentity, []byte{}},
			testWireField{tagLeaseEpoch, []byte{0, 0, 0, 0, 0, 0, 0, 1}},
		),
		testFrame(EnvelopeKindPublicEvent,
			testWireField{tagIdentity, []byte("event")},
			testWireField{tagPublicInline, []byte(`{}`)},
			testWireField{tagLeaseEpoch, make([]byte, 8)},
		),
		testFrame(EnvelopeKindPublicEvent,
			testWireField{tagIdentity, []byte("event")},
			testWireField{tagPublicInline, []byte(`{}`)},
			testWireField{tagRuntimeCommandID, make([]byte, 16)},
		),
		testFrame(EnvelopeKindPublicEvent,
			testWireField{tagIdentity, []byte("event")},
			testWireField{tagPublicInline, []byte(`{}`)},
			testWireField{tagCommandKind, []byte{}},
		),
		// Duplicate and out-of-order known tags with consistent framing.
		testFrame(EnvelopeKindPublicEvent,
			testWireField{tagIdentity, []byte("event")},
			testWireField{tagIdentity, []byte("event")},
			testWireField{tagPublicInline, []byte(`{}`)},
		),
		testFrame(EnvelopeKindPublicEvent,
			testWireField{tagPublicInline, []byte(`{}`)},
			testWireField{tagIdentity, []byte("event")},
		),
		truncatedTLV,
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input []byte) {
		env, err := DecodeEnvelope(input)
		if err != nil {
			assertEnvelopeErrorCodeIsKnown(t, err)
			return
		}

		roundTrip, err := EncodeEnvelope(env)
		if err != nil {
			t.Fatalf("successful decode did not re-encode: %v", err)
		}
		if !bytes.Equal(roundTrip, input) {
			t.Fatalf("successful decode was not canonical:\ninput %x\nround %x", input, roundTrip)
		}

		// Public and runtime bodies are independent. A public-only consumer can
		// retain its slot without observing or resolving the runtime slot.
		if env.Kind == EnvelopeKindPublicEvent && env.Public.Inline != nil {
			public := bytes.Clone(env.Public.Inline)
			if len(env.Runtime.Inline) > 0 {
				env.Runtime.Inline[0] ^= 0xff
			}
			if !bytes.Equal(env.Public.Inline, public) {
				t.Fatal("runtime body aliases public body")
			}
		}
	})
}

func FuzzEncodeEnvelopeBodies(f *testing.F) {
	f.Add([]byte(`{}`), []byte("runtime"), uint8(1))
	f.Add([]byte(`{"x":"\u003c"}`), []byte{}, uint8(2))
	f.Add([]byte("fence"), []byte{}, uint8(3))
	f.Add([]byte("prefix"), []byte("input"), uint8(4))
	f.Add([]byte("public-reference"), []byte("runtime-reference"), uint8(5))
	f.Add([]byte("runtime-reference"), []byte{}, uint8(6))
	f.Add([]byte(`null`), []byte{0xff}, uint8(9))

	f.Fuzz(func(t *testing.T, public, runtime []byte, kindByte uint8) {
		var env Envelope
		switch kindByte {
		case 1:
			env = Envelope{Kind: EnvelopeKindPublicEvent, EventID: "fuzz-event", Public: BodySlot{Inline: bytes.Clone(public)}, Runtime: BodySlot{Inline: bytes.Clone(runtime)}}
		case 2:
			env = Envelope{Kind: EnvelopeKindRuntimeControl, RecordID: "fuzz-record", Runtime: BodySlot{Inline: bytes.Clone(runtime)}}
		case 3:
			env = Envelope{Kind: EnvelopeKindOpeningFence, LeaseEpoch: 1}
		case 4:
			env = Envelope{
				Kind:             EnvelopeKindApplicationPrefix,
				CommandID:        "fuzz-command",
				RuntimeCommandID: uuid.UUID{1},
				LeaseEpoch:       1,
				CommandKind:      string(runtime),
			}
		case 5:
			digest := [32]byte{1}
			env = Envelope{
				Kind:    EnvelopeKindPublicEvent,
				EventID: "fuzz-event-reference",
				Public: BodySlot{Reference: &BodyReference{
					Reference: sessionwire.ObjectReference{ObjectID: "public-object"},
					SizeBytes: 1,
					SHA256:    digest,
				}},
				Runtime: BodySlot{Reference: &BodyReference{
					Reference: sessionwire.ObjectReference{ObjectID: "runtime-object"},
					SizeBytes: 2,
					SHA256:    digest,
				}},
			}
		case 6:
			env = Envelope{
				Kind:     EnvelopeKindRuntimeControl,
				RecordID: "fuzz-record-reference",
				Runtime: BodySlot{Reference: &BodyReference{
					Reference: sessionwire.ObjectReference{ObjectID: "runtime-object"},
					SizeBytes: 1,
					SHA256:    [32]byte{1},
				}},
			}
		default:
			env = Envelope{Kind: EnvelopeKind(kindByte)}
		}
		frame, err := EncodeEnvelope(env)
		if err != nil {
			assertEnvelopeErrorCodeIsKnown(t, err)
			return
		}
		decoded, err := DecodeEnvelope(frame)
		if err != nil {
			t.Fatalf("encoded envelope did not decode: %v", err)
		}
		assertEnvelopeEqual(t, decoded, env)
		reencoded, err := EncodeEnvelope(decoded)
		if err != nil {
			t.Fatalf("decoded envelope did not re-encode: %v", err)
		}
		if !bytes.Equal(reencoded, frame) {
			t.Fatalf("encoded frame was not canonical:\nfirst  %x\nsecond %x", frame, reencoded)
		}
	})
}

func assertEnvelopeErrorCodeIsKnown(t *testing.T, err error) {
	t.Helper()
	for _, code := range []EnvelopeErrorCode{
		EnvelopeErrorMalformed, EnvelopeErrorVersion, EnvelopeErrorKind,
		EnvelopeErrorField, EnvelopeErrorOrder, EnvelopeErrorMissing,
		EnvelopeErrorInvalid, EnvelopeErrorLength, EnvelopeErrorTooLarge,
		EnvelopeErrorDigest, EnvelopeErrorTrailing,
	} {
		var envelopeErr *EnvelopeError
		if errors.As(err, &envelopeErr) && envelopeErr.Code == code {
			if len(envelopeErr.Error()) > 160 {
				t.Fatalf("unbounded envelope error: %q", envelopeErr.Error())
			}
			return
		}
	}
	t.Fatalf("unexpected error type/code: %T %v", err, err)
}

func mustFuzzHex(value string) []byte {
	decoded, err := hex.DecodeString(value)
	if err != nil {
		panic(err)
	}
	return decoded
}
