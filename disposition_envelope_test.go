// The command-disposition envelope: the runtime's own bodiless statement of
// what became of ONE authorized dispatch attempt. The generic per-kind closure
// tables live in envelope_test.go and cover this kind through the same
// derivation every other kind is held to; the tests here are the ones specific
// to its grammar.
package sessionstore

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
)

func dispositionEnvelope() Envelope {
	return Envelope{
		Kind:                EnvelopeKindCommandDisposition,
		CommandID:           "cmd/A",
		LeaseEpoch:          9,
		RuntimeCommandID:    uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"),
		CommandKind:         "input",
		AttemptID:           "attempt/A:B",
		AttemptJournalEpoch: 9,
		DispositionKind:     string(DispositionApplied),
	}
}

// TestCommandDispositionGoldenFrame pins the wire grammar of kind 5: seven
// fields in ascending tag order, no body of either colour, and a frame that
// re-encodes to the same bytes it decoded from.
func TestCommandDispositionGoldenFrame(t *testing.T) {
	t.Parallel()

	want := mustHex(t, "4c524a45010500070000005f"+
		"0100000005636d642f41"+
		"06000000080000000000000009"+
		"070000001000112233445566778899aabbccddeeff"+
		"0800000005696e707574"+
		"090000000b617474656d70742f413a42"+
		"0a000000080000000000000009"+
		"0b000000076170706c696564")

	got, err := EncodeEnvelope(dispositionEnvelope())
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
	assertEnvelopeEqual(t, decoded, dispositionEnvelope())
	if decoded.AttemptID != "attempt/A:B" || decoded.AttemptJournalEpoch != 9 || decoded.DispositionKind != "applied" {
		t.Fatalf("decoded disposition members = %q/%d/%q", decoded.AttemptID, decoded.AttemptJournalEpoch, decoded.DispositionKind)
	}
	reencoded, err := EncodeEnvelope(decoded)
	if err != nil {
		t.Fatalf("EncodeEnvelope(decoded) error = %v", err)
	}
	if !bytes.Equal(reencoded, want) {
		t.Fatalf("accepted frame is not canonical:\n got  %x\n want %x", reencoded, want)
	}
}

// TestCommandDispositionEncodesEveryKindOfTheClosedSet holds the envelope's
// DispositionKind domain to DispositionOutcomeKind's own closed set rather than
// to a second list. A value added there without a producer here would fail this
// test rather than be silently unwritable.
func TestCommandDispositionEncodesEveryKindOfTheClosedSet(t *testing.T) {
	t.Parallel()

	for _, kind := range []DispositionOutcomeKind{DispositionApplied, DispositionNoOp, DispositionRefused, DispositionNotApplied} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			env := dispositionEnvelope()
			env.DispositionKind = string(kind)
			frame, err := EncodeEnvelope(env)
			if err != nil {
				t.Fatalf("EncodeEnvelope(%q) error = %v", kind, err)
			}
			decoded, err := DecodeEnvelope(frame)
			if err != nil {
				t.Fatalf("DecodeEnvelope(%q) error = %v", kind, err)
			}
			if decoded.DispositionKind != string(kind) {
				t.Fatalf("DispositionKind = %q, want %q", decoded.DispositionKind, kind)
			}
		})
	}
}

// TestCommandDispositionRejectsOutOfDomainMembers drives every required member
// of kind 5 to each way it can be wrong, on the ENCODE path where a member that
// is not refused would be silently dropped.
func TestCommandDispositionRejectsOutOfDomainMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		set   func(*Envelope)
		code  EnvelopeErrorCode
		field string
	}{
		{"absent command id", func(e *Envelope) { e.CommandID = "" }, EnvelopeErrorMissing, "identity"},
		{"oversized command id", func(e *Envelope) {
			e.CommandID = sessionwire.CommandID(strings.Repeat("c", sessionwire.MaxIDBytes+1))
		}, EnvelopeErrorInvalid, "identity"},
		{"zero lease epoch", func(e *Envelope) { e.LeaseEpoch = 0 }, EnvelopeErrorInvalid, "lease_epoch"},
		{"zero runtime command id", func(e *Envelope) { e.RuntimeCommandID = uuid.UUID{} }, EnvelopeErrorInvalid, "runtime_command_id"},
		{"absent command kind", func(e *Envelope) { e.CommandKind = "" }, EnvelopeErrorMissing, "command_kind"},
		{"oversized command kind", func(e *Envelope) { e.CommandKind = strings.Repeat("k", 65) }, EnvelopeErrorInvalid, "command_kind"},
		{"absent attempt id", func(e *Envelope) { e.AttemptID = "" }, EnvelopeErrorMissing, "attempt_id"},
		{"oversized attempt id", func(e *Envelope) {
			e.AttemptID = strings.Repeat("a", sessionwire.MaxIDBytes+1)
		}, EnvelopeErrorInvalid, "attempt_id"},
		{"non-utf8 attempt id", func(e *Envelope) { e.AttemptID = "\xff\xfe" }, EnvelopeErrorInvalid, "attempt_id"},
		{"zero attempt journal epoch", func(e *Envelope) { e.AttemptJournalEpoch = 0 }, EnvelopeErrorInvalid, "attempt_journal_epoch"},
		{"absent disposition kind", func(e *Envelope) { e.DispositionKind = "" }, EnvelopeErrorMissing, "disposition_kind"},
		{"unknown disposition kind", func(e *Envelope) { e.DispositionKind = "maybe" }, EnvelopeErrorInvalid, "disposition_kind"},
		{"disposition kind of another vocabulary", func(e *Envelope) { e.DispositionKind = "applying" }, EnvelopeErrorInvalid, "disposition_kind"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			env := dispositionEnvelope()
			if _, err := EncodeEnvelope(env); err != nil {
				t.Fatalf("the unmodified fixture is not valid: %v", err)
			}
			tt.set(&env)
			err := assertEnvelopeErrorCode(t, encodeEnvelopeErr(env), tt.code)
			if err.Field != tt.field {
				t.Fatalf("Field = %q, want %q", err.Field, tt.field)
			}
		})
	}
}

// encodeEnvelopeErr is EncodeEnvelope's error alone, which is all these tables
// assert on.
func encodeEnvelopeErr(env Envelope) error {
	_, err := EncodeEnvelope(env)
	return err
}

// TestCommandDispositionDecodeRequiresEveryField drops one required tag at a
// time from an otherwise valid frame. A missing tag must be refused by the
// record-shape rules rather than defaulted.
func TestCommandDispositionDecodeRequiresEveryField(t *testing.T) {
	t.Parallel()

	full := []testWireField{
		{tagIdentity, []byte("cmd/A")},
		{tagLeaseEpoch, []byte{0, 0, 0, 0, 0, 0, 0, 9}},
		{tagRuntimeCommandID, bytes.Repeat([]byte{1}, 16)},
		{tagCommandKind, []byte("input")},
		{tagAttemptID, []byte("attempt/A:B")},
		{tagAttemptJournalEpoch, []byte{0, 0, 0, 0, 0, 0, 0, 9}},
		{tagDispositionKind, []byte("applied")},
	}
	if _, err := DecodeEnvelope(testFrame(EnvelopeKindCommandDisposition, full...)); err != nil {
		t.Fatalf("the unmodified fixture frame is not valid: %v", err)
	}
	for i := range full {
		t.Run(fmt.Sprintf("without_tag_%d", full[i].tag), func(t *testing.T) {
			t.Parallel()
			fields := make([]testWireField, 0, len(full)-1)
			fields = append(fields, full[:i]...)
			fields = append(fields, full[i+1:]...)
			_, err := DecodeEnvelope(testFrame(EnvelopeKindCommandDisposition, fields...))
			assertEnvelopeErrorCode(t, err, EnvelopeErrorMissing)
		})
	}
}

// TestCommandDispositionDecodeAnswersInSchemaOrder pins WHICH rule answers a
// frame missing more than one required field.
//
// The record-shape rules run over the decoded field SET before validateEnvelope
// reads any value, and they walk the required list in ascending tag order — so a
// frame missing tags 9 and 11 is answered about the LOWER one. A single omission
// cannot see the difference: validateEnvelope refuses an absent attempt_id with
// the same code and the same field name, so without a second omission the wire
// rule for tag 9 could be deleted and nothing would notice.
func TestCommandDispositionDecodeAnswersInSchemaOrder(t *testing.T) {
	t.Parallel()

	frame := testFrame(EnvelopeKindCommandDisposition,
		testWireField{tagIdentity, []byte("cmd/A")},
		testWireField{tagLeaseEpoch, []byte{0, 0, 0, 0, 0, 0, 0, 9}},
		testWireField{tagRuntimeCommandID, bytes.Repeat([]byte{1}, 16)},
		testWireField{tagCommandKind, []byte("input")},
		testWireField{tagAttemptJournalEpoch, []byte{0, 0, 0, 0, 0, 0, 0, 9}},
	)
	_, err := DecodeEnvelope(frame)
	missing := assertEnvelopeErrorCode(t, err, EnvelopeErrorMissing)
	if missing.Field != "attempt_id" {
		t.Fatalf("Field = %q, want attempt_id: the wire-shape rule did not answer first", missing.Field)
	}
}

// TestCommandDispositionDecodeHoldsFixedWidthFields pins the two length rules a
// decoder applies before it trusts a value.
func TestCommandDispositionDecodeHoldsFixedWidthFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		field testWireField
		code  EnvelopeErrorCode
	}{
		{"short attempt journal epoch", testWireField{tagAttemptJournalEpoch, []byte{0, 9}}, EnvelopeErrorLength},
		{"oversized attempt id", testWireField{tagAttemptID, bytes.Repeat([]byte("a"), sessionwire.MaxIDBytes+1)}, EnvelopeErrorInvalid},
		{"oversized disposition kind", testWireField{tagDispositionKind, bytes.Repeat([]byte("a"), 65)}, EnvelopeErrorInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fields := []testWireField{
				{tagIdentity, []byte("cmd/A")},
				{tagLeaseEpoch, []byte{0, 0, 0, 0, 0, 0, 0, 9}},
				{tagRuntimeCommandID, bytes.Repeat([]byte{1}, 16)},
				{tagCommandKind, []byte("input")},
				{tagAttemptID, []byte("attempt/A:B")},
				{tagAttemptJournalEpoch, []byte{0, 0, 0, 0, 0, 0, 0, 9}},
				{tagDispositionKind, []byte("applied")},
			}
			for i := range fields {
				if fields[i].tag == tt.field.tag {
					fields[i] = tt.field
				}
			}
			_, err := DecodeEnvelope(testFrame(EnvelopeKindCommandDisposition, fields...))
			assertEnvelopeErrorCode(t, err, tt.code)
		})
	}
}

// TestCommandDispositionIsBodiless states the property a new KIND was chosen
// for: a disposition carries no bytes of either colour, so a reader that
// resolves bodies never has one to resolve here.
func TestCommandDispositionIsBodiless(t *testing.T) {
	t.Parallel()

	for name, set := range map[string]func(*Envelope){
		"public inline":  func(e *Envelope) { e.Public = BodySlot{Inline: []byte(`{}`)} },
		"runtime inline": func(e *Envelope) { e.Runtime = BodySlot{Inline: []byte("x")} },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := dispositionEnvelope()
			set(&env)
			shape := assertEnvelopeErrorCode(t, encodeEnvelopeErr(env), EnvelopeErrorField)
			if shape.Field != "record_shape" {
				t.Fatalf("Field = %q, want record_shape", shape.Field)
			}
		})
	}
}
