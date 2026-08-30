package sessionstore

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
)

func FuzzDecodeEnvelope(f *testing.F) {
	seeds := []string{
		"4c524a45010100030000001b0100000002653102000000077b2278223a317d0400000003007274",
		"4c524a45010200020000001501000000057265632d310400000006736563726574",
		"4c524a45010300010000000d06000000080000000000000007",
		"4c524a4501040004000000360100000005636d642f4106000000080000000000000009070000001000112233445566778899aabbccddeeff0800000005696e707574",
		"",
		"00000000000000000000000000000000",
	}
	for _, seed := range seeds {
		f.Add(mustFuzzHex(seed))
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
	f.Add([]byte(`null`), []byte{0xff}, uint8(9))

	f.Fuzz(func(t *testing.T, public, runtime []byte, kindByte uint8) {
		kind := EnvelopeKind(kindByte)
		var env Envelope
		switch kind {
		case EnvelopeKindPublicEvent:
			env = Envelope{Kind: kind, EventID: "fuzz-event", Public: BodySlot{Inline: bytes.Clone(public)}, Runtime: BodySlot{Inline: bytes.Clone(runtime)}}
		case EnvelopeKindRuntimeControl:
			env = Envelope{Kind: kind, RecordID: "fuzz-record", Runtime: BodySlot{Inline: bytes.Clone(runtime)}}
		default:
			env = Envelope{Kind: kind}
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
