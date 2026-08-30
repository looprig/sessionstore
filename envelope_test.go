package sessionstore

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
)

func TestEnvelopeGoldenFrames(t *testing.T) {
	t.Parallel()

	publicDigest := digestSequence(0x01)
	runtimeDigest := digestSequence(0xa0)
	tests := []struct {
		name string
		env  Envelope
		hex  string
	}{
		{
			name: "public and runtime inline",
			env: Envelope{
				Kind:    EnvelopeKindPublicEvent,
				EventID: "e1",
				Public:  BodySlot{Inline: []byte(`{"x":1}`)},
				Runtime: BodySlot{Inline: []byte{0x00, 'r', 't'}},
			},
			hex: "4c524a45010100030000001b" +
				"01000000026531" +
				"02000000077b2278223a317d" +
				"0400000003007274",
		},
		{
			name: "private runtime control",
			env: Envelope{
				Kind:     EnvelopeKindRuntimeControl,
				RecordID: "rec-1",
				Runtime:  BodySlot{Inline: []byte("secret")},
			},
			hex: "4c524a450102000200000015" +
				"01000000057265632d31" +
				"0400000006736563726574",
		},
		{
			name: "independent object references",
			env: Envelope{
				Kind:    EnvelopeKindPublicEvent,
				EventID: "e-ref",
				Public: BodySlot{Reference: &BodyReference{
					Reference: sessionwire.ObjectReference{ObjectID: "pub"},
					SizeBytes: 7,
					SHA256:    publicDigest,
				}},
				Runtime: BodySlot{Reference: &BodyReference{
					Reference: sessionwire.ObjectReference{ObjectID: "run"},
					SizeBytes: 9,
					SHA256:    runtimeDigest,
				}},
			},
			hex: "4c524a450101000300000070" +
				"0100000005652d726566" +
				"030000002e0100037075620000000000000007" +
				"0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20" +
				"050000002e01000372756e0000000000000009" +
				"a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf",
		},
		{
			name: "opening fence",
			env:  Envelope{Kind: EnvelopeKindOpeningFence, LeaseEpoch: 7},
			hex: "4c524a45010300010000000d" +
				"06000000080000000000000007",
		},
		{
			name: "application prefix",
			env: Envelope{
				Kind:             EnvelopeKindApplicationPrefix,
				CommandID:        "cmd/A",
				RuntimeCommandID: uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"),
				LeaseEpoch:       9,
				CommandKind:      "input",
			},
			hex: "4c524a450104000400000036" +
				"0100000005636d642f41" +
				"06000000080000000000000009" +
				"070000001000112233445566778899aabbccddeeff" +
				"0800000005696e707574",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			want := mustHex(t, tt.hex)
			got, err := EncodeEnvelope(tt.env)
			if err != nil {
				t.Fatalf("EncodeEnvelope() error = %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("EncodeEnvelope() = %x\nwant                 %x", got, want)
			}
			decoded, err := DecodeEnvelope(want)
			if err != nil {
				t.Fatalf("DecodeEnvelope() error = %v", err)
			}
			assertEnvelopeEqual(t, decoded, tt.env)
		})
	}
}

func TestEnvelopeRejectsMalformedFrames(t *testing.T) {
	t.Parallel()

	valid, err := EncodeEnvelope(Envelope{
		Kind:    EnvelopeKindPublicEvent,
		EventID: "event-1",
		Public:  BodySlot{Inline: []byte(`{"ok":true}`)},
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		code EnvelopeErrorCode
		edit func([]byte) []byte
	}{
		{"empty", EnvelopeErrorMalformed, func([]byte) []byte { return nil }},
		{"truncated header", EnvelopeErrorLength, func(b []byte) []byte { return b[:11] }},
		{"magic", EnvelopeErrorMalformed, func(b []byte) []byte { b[0] ^= 0xff; return b }},
		{"version", EnvelopeErrorVersion, func(b []byte) []byte { b[4] = 2; return b }},
		{"kind", EnvelopeErrorKind, func(b []byte) []byte { b[5] = 99; return b }},
		{"declared fields bytes short", EnvelopeErrorTrailing, func(b []byte) []byte { b[11]--; return b }},
		{"declared fields bytes long", EnvelopeErrorLength, func(b []byte) []byte { b[11]++; return b }},
		{"count mismatch", EnvelopeErrorLength, func(b []byte) []byte { b[7]++; return b }},
		{"truncated tlv header", EnvelopeErrorLength, func(b []byte) []byte { b[7] = 1; b[11] = 4; return b[:16] }},
		{"truncated tlv value", EnvelopeErrorLength, func(b []byte) []byte {
			b = bytes.Clone(b[:len(b)-1])
			b = b[:len(b):len(b)]
			b[11]--
			return b
		}},
		{"unknown tag", EnvelopeErrorField, func(b []byte) []byte { b[12] = 9; return b }},
		{"duplicate tag", EnvelopeErrorOrder, func(b []byte) []byte { b[24] = 1; return b }},
		{"out of order tag", EnvelopeErrorOrder, func(b []byte) []byte { b[12], b[24] = b[24], b[12]; return b }},
		{"trailing byte", EnvelopeErrorTrailing, func(b []byte) []byte { b = append(b, 0); return b }},
		{"declared envelope over maximum", EnvelopeErrorTooLarge, func(b []byte) []byte { b[8], b[9], b[10], b[11] = 0, 0x10, 0, 0; return b }},
		{"declared fields bytes overflow", EnvelopeErrorTooLarge, func(b []byte) []byte { b[8], b[9], b[10], b[11] = 0xff, 0xff, 0xff, 0xff; return b }},
		{"declared tlv length overflow", EnvelopeErrorTooLarge, func(b []byte) []byte { b[13], b[14], b[15], b[16] = 0xff, 0xff, 0xff, 0xff; return b }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			input := tt.edit(bytes.Clone(valid))
			_, gotErr := DecodeEnvelope(input)
			assertEnvelopeErrorCode(t, gotErr, tt.code)
		})
	}
}

func TestEnvelopeRejectsInvalidShapesAndFields(t *testing.T) {
	t.Parallel()

	digest := digestSequence(1)
	ref := func() *BodyReference {
		return &BodyReference{Reference: sessionwire.ObjectReference{ObjectID: "object-1"}, SizeBytes: 3, SHA256: digest}
	}
	validPublic := Envelope{Kind: EnvelopeKindPublicEvent, EventID: "e", Public: BodySlot{Inline: []byte(`{}`)}}
	tests := []struct {
		name string
		code EnvelopeErrorCode
		env  Envelope
	}{
		{"zero kind", EnvelopeErrorKind, Envelope{}},
		{"missing event id", EnvelopeErrorMissing, Envelope{Kind: EnvelopeKindPublicEvent, Public: BodySlot{Inline: []byte(`{}`)}}},
		{"invalid event id", EnvelopeErrorInvalid, Envelope{Kind: EnvelopeKindPublicEvent, EventID: sessionwire.EventID(string([]byte{0xff})), Public: BodySlot{Inline: []byte(`{}`)}}},
		{"missing public body", EnvelopeErrorMissing, Envelope{Kind: EnvelopeKindPublicEvent, EventID: "e"}},
		{"public slot exclusive", EnvelopeErrorField, Envelope{Kind: EnvelopeKindPublicEvent, EventID: "e", Public: BodySlot{Inline: []byte(`{}`), Reference: ref()}}},
		{"public reference zero digest", EnvelopeErrorDigest, Envelope{Kind: EnvelopeKindPublicEvent, EventID: "e", Public: BodySlot{Reference: &BodyReference{Reference: sessionwire.ObjectReference{ObjectID: "object-1"}}}}},
		{"runtime slot exclusive", EnvelopeErrorField, Envelope{Kind: EnvelopeKindPublicEvent, EventID: "e", Public: BodySlot{Inline: []byte(`{}`)}, Runtime: BodySlot{Inline: []byte("r"), Reference: ref()}}},
		{"invalid public json", EnvelopeErrorInvalid, Envelope{Kind: EnvelopeKindPublicEvent, EventID: "e", Public: BodySlot{Inline: []byte(`{`)}}},
		{"noncanonical public json", EnvelopeErrorInvalid, Envelope{Kind: EnvelopeKindPublicEvent, EventID: "e", Public: BodySlot{Inline: []byte(`{"x":"<"}`)}}},
		{"oversized public inline", EnvelopeErrorTooLarge, Envelope{Kind: EnvelopeKindPublicEvent, EventID: "e", Public: BodySlot{Inline: make([]byte, MaxInlineBodyBytes+1)}}},
		{"oversized runtime inline", EnvelopeErrorTooLarge, Envelope{Kind: EnvelopeKindRuntimeControl, RecordID: "r", Runtime: BodySlot{Inline: make([]byte, MaxInlineBodyBytes+1)}}},
		{"total frame cap", EnvelopeErrorTooLarge, Envelope{Kind: EnvelopeKindPublicEvent, EventID: "e", Public: BodySlot{Inline: canonicalLargeJSON(MaxInlineBodyBytes)}, Runtime: BodySlot{Inline: make([]byte, MaxInlineBodyBytes)}}},
		{"runtime missing id", EnvelopeErrorMissing, Envelope{Kind: EnvelopeKindRuntimeControl, Runtime: BodySlot{Inline: []byte{}}}},
		{"runtime invalid id", EnvelopeErrorInvalid, Envelope{Kind: EnvelopeKindRuntimeControl, RecordID: string([]byte{0xff}), Runtime: BodySlot{Inline: []byte{}}}},
		{"runtime long id", EnvelopeErrorInvalid, Envelope{Kind: EnvelopeKindRuntimeControl, RecordID: strings.Repeat("r", sessionwire.MaxIDBytes+1), Runtime: BodySlot{Inline: []byte{}}}},
		{"runtime missing body", EnvelopeErrorMissing, Envelope{Kind: EnvelopeKindRuntimeControl, RecordID: "r"}},
		{"runtime has public body", EnvelopeErrorField, Envelope{Kind: EnvelopeKindRuntimeControl, RecordID: "r", Public: BodySlot{Inline: []byte(`{}`)}, Runtime: BodySlot{Inline: []byte{}}}},
		{"fence zero epoch", EnvelopeErrorInvalid, Envelope{Kind: EnvelopeKindOpeningFence}},
		{"fence has identity", EnvelopeErrorField, Envelope{Kind: EnvelopeKindOpeningFence, RecordID: "r", LeaseEpoch: 1}},
		{"prefix missing command id", EnvelopeErrorMissing, Envelope{Kind: EnvelopeKindApplicationPrefix, RuntimeCommandID: uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"), LeaseEpoch: 1, CommandKind: "input"}},
		{"prefix invalid command id", EnvelopeErrorInvalid, Envelope{Kind: EnvelopeKindApplicationPrefix, CommandID: sessionwire.CommandID(string([]byte{0xff})), RuntimeCommandID: uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"), LeaseEpoch: 1, CommandKind: "input"}},
		{"prefix zero runtime id", EnvelopeErrorInvalid, Envelope{Kind: EnvelopeKindApplicationPrefix, CommandID: "c", LeaseEpoch: 1, CommandKind: "input"}},
		{"prefix zero epoch", EnvelopeErrorInvalid, Envelope{Kind: EnvelopeKindApplicationPrefix, CommandID: "c", RuntimeCommandID: uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"), CommandKind: "input"}},
		{"prefix missing kind", EnvelopeErrorMissing, Envelope{Kind: EnvelopeKindApplicationPrefix, CommandID: "c", RuntimeCommandID: uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"), LeaseEpoch: 1}},
		{"prefix long kind", EnvelopeErrorInvalid, Envelope{Kind: EnvelopeKindApplicationPrefix, CommandID: "c", RuntimeCommandID: uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"), LeaseEpoch: 1, CommandKind: strings.Repeat("x", 65)}},
		{"prefix invalid utf8 kind", EnvelopeErrorInvalid, Envelope{Kind: EnvelopeKindApplicationPrefix, CommandID: "c", RuntimeCommandID: uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"), LeaseEpoch: 1, CommandKind: string([]byte{0xff})}},
		{"prefix has body", EnvelopeErrorField, Envelope{Kind: EnvelopeKindApplicationPrefix, CommandID: "c", RuntimeCommandID: uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"), LeaseEpoch: 1, CommandKind: "input", Runtime: BodySlot{Reference: ref()}}},
		{"reference invalid object id", EnvelopeErrorInvalid, Envelope{Kind: EnvelopeKindPublicEvent, EventID: "e", Public: BodySlot{Reference: &BodyReference{Reference: sessionwire.ObjectReference{}, SHA256: digest}}}},
		{"unrelated field", EnvelopeErrorField, func() Envelope { e := validPublic; e.LeaseEpoch = 1; return e }()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := EncodeEnvelope(tt.env)
			assertEnvelopeErrorCode(t, err, tt.code)
		})
	}
}

func TestDecodeRejectsWireShapeViolations(t *testing.T) {
	t.Parallel()

	validDigest := bytes.Repeat([]byte{1}, 32)
	referenceValue := func(algorithm byte, objectID []byte, digest []byte, trailing ...byte) []byte {
		value := []byte{algorithm, byte(len(objectID) >> 8), byte(len(objectID))}
		value = append(value, objectID...)
		value = append(value, 0, 0, 0, 0, 0, 0, 0, 1)
		value = append(value, digest...)
		return append(value, trailing...)
	}
	tests := []struct {
		name  string
		code  EnvelopeErrorCode
		input []byte
	}{
		{"public missing body", EnvelopeErrorMissing, testFrame(EnvelopeKindPublicEvent, testWireField{tagIdentity, []byte("e")})},
		{"public duplicate slots", EnvelopeErrorField, testFrame(EnvelopeKindPublicEvent,
			testWireField{tagIdentity, []byte("e")},
			testWireField{tagPublicInline, []byte(`{}`)},
			testWireField{tagPublicReference, referenceValue(1, []byte("x"), validDigest)}),
		},
		{"runtime public slot", EnvelopeErrorField, testFrame(EnvelopeKindRuntimeControl,
			testWireField{tagIdentity, []byte("r")},
			testWireField{tagPublicInline, []byte(`{}`)},
			testWireField{tagRuntimeInline, []byte("xx")},
		)},
		{"fence zero", EnvelopeErrorInvalid, testFrame(EnvelopeKindOpeningFence, testWireField{tagLeaseEpoch, make([]byte, 8)})},
		{"fence extra", EnvelopeErrorField, testFrame(EnvelopeKindOpeningFence,
			testWireField{tagIdentity, []byte("x")},
			testWireField{tagLeaseEpoch, []byte{0, 0, 0, 0, 0, 0, 0, 1}},
		)},
		{"prefix zero runtime id", EnvelopeErrorInvalid, testFrame(EnvelopeKindApplicationPrefix,
			testWireField{tagIdentity, []byte("c")},
			testWireField{tagLeaseEpoch, []byte{0, 0, 0, 0, 0, 0, 0, 1}},
			testWireField{tagRuntimeCommandID, make([]byte, 16)},
			testWireField{tagCommandKind, []byte("x")},
		)},
		{"prefix invalid command id", EnvelopeErrorInvalid, testFrame(EnvelopeKindApplicationPrefix,
			testWireField{tagIdentity, []byte{0xff}},
			testWireField{tagLeaseEpoch, []byte{0, 0, 0, 0, 0, 0, 0, 1}},
			testWireField{tagRuntimeCommandID, bytes.Repeat([]byte{1}, 16)},
			testWireField{tagCommandKind, []byte("x")},
		)},
		{"reference unknown algorithm", EnvelopeErrorDigest, testFrame(EnvelopeKindPublicEvent,
			testWireField{tagIdentity, []byte("e")},
			testWireField{tagPublicReference, referenceValue(2, []byte("x"), validDigest)},
		)},
		{"reference zero digest", EnvelopeErrorDigest, testFrame(EnvelopeKindPublicEvent,
			testWireField{tagIdentity, []byte("e")},
			testWireField{tagPublicReference, referenceValue(1, []byte("x"), make([]byte, 32))},
		)},
		{"reference invalid object id", EnvelopeErrorInvalid, testFrame(EnvelopeKindPublicEvent,
			testWireField{tagIdentity, []byte("e")},
			testWireField{tagPublicReference, referenceValue(1, nil, validDigest)},
		)},
		{"reference trailing", EnvelopeErrorLength, testFrame(EnvelopeKindPublicEvent,
			testWireField{tagIdentity, []byte("e")},
			testWireField{tagPublicReference, referenceValue(1, []byte("x"), validDigest, 0)},
		)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := DecodeEnvelope(tt.input)
			assertEnvelopeErrorCode(t, err, tt.code)
		})
	}
}

func TestDecodeRejectsOversizedFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		kind   EnvelopeKind
		fields []testWireField
		code   EnvelopeErrorCode
	}{
		{
			name: "identity",
			kind: EnvelopeKindRuntimeControl,
			fields: []testWireField{
				{tagIdentity, bytes.Repeat([]byte{'x'}, sessionwire.MaxIDBytes+1)},
				{tagRuntimeInline, []byte{}},
			},
			code: EnvelopeErrorInvalid,
		},
		{
			name: "command kind",
			kind: EnvelopeKindApplicationPrefix,
			fields: []testWireField{
				{tagIdentity, []byte("command")},
				{tagLeaseEpoch, []byte{0, 0, 0, 0, 0, 0, 0, 1}},
				{tagRuntimeCommandID, bytes.Repeat([]byte{1}, 16)},
				{tagCommandKind, bytes.Repeat([]byte{'x'}, 65)},
			},
			code: EnvelopeErrorInvalid,
		},
		{
			name: "inline body",
			kind: EnvelopeKindRuntimeControl,
			fields: []testWireField{
				{tagIdentity, []byte("record")},
				{tagRuntimeInline, make([]byte, MaxInlineBodyBytes+1)},
			},
			code: EnvelopeErrorTooLarge,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := DecodeEnvelope(testFrame(tt.kind, tt.fields...))
			assertEnvelopeErrorCode(t, err, tt.code)
		})
	}

	_, err := DecodeEnvelope(make([]byte, MaxEnvelopeBytes+1))
	assertEnvelopeErrorCode(t, err, EnvelopeErrorTooLarge)
}

func TestEnvelopeBodyOwnership(t *testing.T) {
	t.Parallel()

	public := []byte(`{"value":"original"}`)
	runtime := []byte("runtime-original")
	env := Envelope{Kind: EnvelopeKindPublicEvent, EventID: "e", Public: BodySlot{Inline: public}, Runtime: BodySlot{Inline: runtime}}
	frame, err := EncodeEnvelope(env)
	if err != nil {
		t.Fatal(err)
	}
	public[2] = 'X'
	runtime[0] = 'X'
	decoded, err := DecodeEnvelope(frame)
	if err != nil {
		t.Fatal(err)
	}
	if string(decoded.Public.Inline) != `{"value":"original"}` || string(decoded.Runtime.Inline) != "runtime-original" {
		t.Fatalf("encoded frame aliases source bodies: public=%q runtime=%q", decoded.Public.Inline, decoded.Runtime.Inline)
	}

	publicOffset := bytes.Index(frame, []byte(`{"value":"original"}`))
	if publicOffset < 0 {
		t.Fatal("public body not found in encoded frame")
	}
	frame[publicOffset+2] = 'X'
	if string(decoded.Public.Inline) != `{"value":"original"}` {
		t.Fatalf("decoded public body aliases frame: %q", decoded.Public.Inline)
	}
	runtimeOffset := bytes.Index(frame, []byte("runtime-original"))
	if runtimeOffset < 0 {
		t.Fatal("runtime body not found in encoded frame")
	}
	frame[runtimeOffset] = 'X'
	if string(decoded.Runtime.Inline) != "runtime-original" {
		t.Fatalf("decoded runtime body aliases frame: %q", decoded.Runtime.Inline)
	}
	decoded.Public.Inline[2] = 'Y'
	if decoded.Runtime.Inline[0] != 'r' {
		t.Fatal("public and runtime decoded bodies alias")
	}
}

func TestBodyReferenceObjectMetadataConversion(t *testing.T) {
	t.Parallel()

	digest := digestSequence(0x10)
	metadata := sessionwire.ObjectMetadata{
		Reference: sessionwire.ObjectReference{ObjectID: "object-1"},
		SizeBytes: 42,
		Digest:    "sha256:" + hex.EncodeToString(digest[:]),
	}
	ref, err := BodyReferenceFromObjectMetadata(metadata)
	if err != nil {
		t.Fatalf("BodyReferenceFromObjectMetadata() error = %v", err)
	}
	if ref.Reference != metadata.Reference || ref.SizeBytes != 42 || ref.SHA256 != digest {
		t.Fatalf("BodyReferenceFromObjectMetadata() = %+v", ref)
	}
	got, err := ref.ObjectMetadata()
	if err != nil {
		t.Fatalf("ObjectMetadata() error = %v", err)
	}
	if got.Reference != metadata.Reference || got.SizeBytes != metadata.SizeBytes || got.Digest != metadata.Digest {
		t.Fatalf("ObjectMetadata() = %+v, want reference/size/digest from %+v", got, metadata)
	}

	for _, bad := range []string{
		"", "sha256:abc", "SHA256:" + hex.EncodeToString(digest[:]),
		"sha256:" + strings.ToUpper(hex.EncodeToString(digest[:])),
		"sha512:" + hex.EncodeToString(digest[:]),
	} {
		badMetadata := metadata
		badMetadata.Digest = bad
		_, err := BodyReferenceFromObjectMetadata(badMetadata)
		assertEnvelopeErrorCode(t, err, EnvelopeErrorDigest)
	}
	badMetadata := metadata
	badMetadata.Reference.ObjectID = ""
	_, err = BodyReferenceFromObjectMetadata(badMetadata)
	assertEnvelopeErrorCode(t, err, EnvelopeErrorInvalid)

	badReference := ref
	badReference.SHA256 = [32]byte{}
	_, err = badReference.ObjectMetadata()
	assertEnvelopeErrorCode(t, err, EnvelopeErrorDigest)
}

func TestEnvelopeErrorIsTypedBoundedAndUnwraps(t *testing.T) {
	t.Parallel()

	cause := errors.New(strings.Repeat("secret", 1000))
	err := &EnvelopeError{Code: EnvelopeErrorMalformed, Field: strings.Repeat("x", 1000), Cause: cause}
	var typed *EnvelopeError
	if !errors.As(err, &typed) || !errors.Is(err, cause) {
		t.Fatal("EnvelopeError does not support errors.As/errors.Is")
	}
	if len(err.Error()) > 160 || strings.Contains(err.Error(), "secret") {
		t.Fatalf("EnvelopeError text is unbounded or exposes cause: len=%d text=%q", len(err.Error()), err.Error())
	}
}

func assertEnvelopeErrorCode(t *testing.T, err error, want EnvelopeErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want code %q", want)
	}
	var got *EnvelopeError
	if !errors.As(err, &got) {
		t.Fatalf("error = %T (%v), want *EnvelopeError", err, err)
	}
	if got.Code != want {
		t.Fatalf("error code = %q, want %q (error %v)", got.Code, want, err)
	}
}

func assertEnvelopeEqual(t *testing.T, got, want Envelope) {
	t.Helper()
	if got.Kind != want.Kind || got.EventID != want.EventID || got.RecordID != want.RecordID ||
		got.CommandID != want.CommandID || got.RuntimeCommandID != want.RuntimeCommandID ||
		got.LeaseEpoch != want.LeaseEpoch || got.CommandKind != want.CommandKind ||
		!bytes.Equal(got.Public.Inline, want.Public.Inline) || !bytes.Equal(got.Runtime.Inline, want.Runtime.Inline) {
		t.Fatalf("envelope mismatch:\n got  %+v\n want %+v", got, want)
	}
	assertReferenceEqual(t, got.Public.Reference, want.Public.Reference)
	assertReferenceEqual(t, got.Runtime.Reference, want.Runtime.Reference)
}

func assertReferenceEqual(t *testing.T, got, want *BodyReference) {
	t.Helper()
	if got == nil || want == nil {
		if got != want {
			t.Fatalf("reference mismatch: got %+v want %+v", got, want)
		}
		return
	}
	if *got != *want {
		t.Fatalf("reference mismatch: got %+v want %+v", *got, *want)
	}
}

func digestSequence(start byte) [32]byte {
	var digest [32]byte
	for i := range digest {
		digest[i] = start + byte(i)
	}
	return digest
}

func canonicalLargeJSON(size int) []byte {
	if size < 2 {
		panic("canonicalLargeJSON requires at least two bytes")
	}
	body := make([]byte, size)
	body[0] = '"'
	for i := 1; i < len(body)-1; i++ {
		body[i] = 'x'
	}
	body[len(body)-1] = '"'
	return body
}

func mustHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatalf("invalid test hex: %v", err)
	}
	return decoded
}

type testWireField struct {
	tag   uint8
	value []byte
}

func testFrame(kind EnvelopeKind, fields ...testWireField) []byte {
	fieldsBytes := 0
	for _, field := range fields {
		fieldsBytes += 5 + len(field.value)
	}
	frame := make([]byte, envelopeHeaderBytes, envelopeHeaderBytes+fieldsBytes)
	copy(frame, envelopeMagic[:])
	frame[4] = EnvelopeVersion
	frame[5] = byte(kind)
	binary.BigEndian.PutUint16(frame[6:8], uint16(len(fields)))
	binary.BigEndian.PutUint32(frame[8:12], uint32(fieldsBytes))
	for _, field := range fields {
		frame = append(frame, field.tag, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(frame[len(frame)-4:], uint32(len(field.value)))
		frame = append(frame, field.value...)
	}
	return frame
}
