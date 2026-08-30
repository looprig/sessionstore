package sessionstore

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"unicode/utf8"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
)

const (
	// EnvelopeVersion is the independent version of the raw journal frame.
	EnvelopeVersion uint8 = 1
	// MaxEnvelopeBytes is the maximum encoded frame size accepted or produced.
	MaxEnvelopeBytes = 1 << 20
	// MaxInlineBodyBytes is the maximum size of either independent inline body.
	MaxInlineBodyBytes = 512 << 10
)

var envelopeMagic = [4]byte{'L', 'R', 'J', 'E'}

const envelopeHeaderBytes = 12

// EnvelopeKind identifies the closed set of raw journal record shapes.
type EnvelopeKind uint8

const (
	EnvelopeKindPublicEvent       EnvelopeKind = 1
	EnvelopeKindRuntimeControl    EnvelopeKind = 2
	EnvelopeKindOpeningFence      EnvelopeKind = 3
	EnvelopeKindApplicationPrefix EnvelopeKind = 4
)

const (
	tagIdentity         uint8 = 1
	tagPublicInline     uint8 = 2
	tagPublicReference  uint8 = 3
	tagRuntimeInline    uint8 = 4
	tagRuntimeReference uint8 = 5
	tagLeaseEpoch       uint8 = 6
	tagRuntimeCommandID uint8 = 7
	tagCommandKind      uint8 = 8
)

type envelopeFieldSet uint8

const (
	fieldIdentity         envelopeFieldSet = 1 << (tagIdentity - 1)
	fieldPublicInline     envelopeFieldSet = 1 << (tagPublicInline - 1)
	fieldPublicReference  envelopeFieldSet = 1 << (tagPublicReference - 1)
	fieldRuntimeInline    envelopeFieldSet = 1 << (tagRuntimeInline - 1)
	fieldRuntimeReference envelopeFieldSet = 1 << (tagRuntimeReference - 1)
	fieldLeaseEpoch       envelopeFieldSet = 1 << (tagLeaseEpoch - 1)
	fieldRuntimeCommandID envelopeFieldSet = 1 << (tagRuntimeCommandID - 1)
	fieldCommandKind      envelopeFieldSet = 1 << (tagCommandKind - 1)
)

const bodyReferenceAlgorithmSHA256 uint8 = 1

// BodySlot is one independent inline or object-backed body. A nil Inline is
// absent; a non-nil, zero-length Inline is present. Inline and Reference are
// mutually exclusive.
type BodySlot struct {
	Inline    []byte
	Reference *BodyReference
}

// BodyReference is the fixed-integrity representation stored in an envelope.
// Reference is a logical Core identity, never a provider key or signed URL.
type BodyReference struct {
	Reference sessionwire.ObjectReference
	SizeBytes uint64
	SHA256    [32]byte
}

// BodyReferenceFromObjectMetadata converts canonical SHA-256 Core metadata to
// the journal's fixed binary reference.
func BodyReferenceFromObjectMetadata(metadata sessionwire.ObjectMetadata) (BodyReference, error) {
	if err := metadata.Reference.Validate(); err != nil {
		return BodyReference{}, envelopeError(EnvelopeErrorInvalid, "object_reference", err)
	}
	const prefix = "sha256:"
	if len(metadata.Digest) != len(prefix)+hex.EncodedLen(32) || metadata.Digest[:len(prefix)] != prefix {
		return BodyReference{}, envelopeError(EnvelopeErrorDigest, "digest", nil)
	}
	digestText := metadata.Digest[len(prefix):]
	var digest [32]byte
	if _, err := hex.Decode(digest[:], []byte(digestText)); err != nil || hex.EncodeToString(digest[:]) != digestText || isZeroDigest(digest) {
		return BodyReference{}, envelopeError(EnvelopeErrorDigest, "digest", err)
	}
	return BodyReference{Reference: metadata.Reference, SizeBytes: metadata.SizeBytes, SHA256: digest}, nil
}

// ObjectMetadata converts a valid fixed reference to Core's public metadata
// shape with a canonical lowercase SHA-256 digest.
func (r BodyReference) ObjectMetadata() (sessionwire.ObjectMetadata, error) {
	if err := r.Reference.Validate(); err != nil {
		return sessionwire.ObjectMetadata{}, envelopeError(EnvelopeErrorInvalid, "object_reference", err)
	}
	if isZeroDigest(r.SHA256) {
		return sessionwire.ObjectMetadata{}, envelopeError(EnvelopeErrorDigest, "digest", nil)
	}
	return sessionwire.ObjectMetadata{
		Reference: sessionwire.ObjectReference{ObjectID: r.Reference.ObjectID},
		SizeBytes: r.SizeBytes,
		Digest:    "sha256:" + hex.EncodeToString(r.SHA256[:]),
	}, nil
}

// Envelope is one deterministic raw journal record. Public and Runtime remain
// separate so a public reader can select the public slot without inspecting or
// resolving private runtime bytes.
type Envelope struct {
	Kind EnvelopeKind

	EventID  sessionwire.EventID
	RecordID string

	Public  BodySlot
	Runtime BodySlot

	LeaseEpoch       uint64
	CommandID        sessionwire.CommandID
	RuntimeCommandID uuid.UUID
	CommandKind      string
}

// EnvelopeErrorCode is a stable machine-readable envelope failure reason.
type EnvelopeErrorCode string

const (
	EnvelopeErrorMalformed EnvelopeErrorCode = "malformed"
	EnvelopeErrorVersion   EnvelopeErrorCode = "version"
	EnvelopeErrorKind      EnvelopeErrorCode = "kind"
	EnvelopeErrorField     EnvelopeErrorCode = "field"
	EnvelopeErrorOrder     EnvelopeErrorCode = "order"
	EnvelopeErrorMissing   EnvelopeErrorCode = "missing"
	EnvelopeErrorInvalid   EnvelopeErrorCode = "invalid"
	EnvelopeErrorLength    EnvelopeErrorCode = "length"
	EnvelopeErrorTooLarge  EnvelopeErrorCode = "too-large"
	EnvelopeErrorDigest    EnvelopeErrorCode = "digest"
	EnvelopeErrorTrailing  EnvelopeErrorCode = "trailing"
)

// EnvelopeError reports a bounded codec failure and preserves its cause without
// placing attacker-controlled cause text in Error().
type EnvelopeError struct {
	Code  EnvelopeErrorCode
	Field string
	Cause error
}

func (e *EnvelopeError) Error() string {
	message := "sessionstore: envelope " + string(e.Code)
	if e.Field != "" {
		field := e.Field
		if len(field) > 48 {
			field = field[:48]
		}
		message += " (" + field + ")"
	}
	return message
}

func (e *EnvelopeError) Unwrap() error { return e.Cause }

func envelopeError(code EnvelopeErrorCode, field string, cause error) error {
	return &EnvelopeError{Code: code, Field: field, Cause: cause}
}

// EncodeEnvelope validates and deterministically encodes an envelope. The
// returned frame does not alias any caller-owned body.
func EncodeEnvelope(env Envelope) ([]byte, error) {
	if err := validateEnvelope(env); err != nil {
		return nil, err
	}

	fields := make([]encodedField, 0, 4)
	switch env.Kind {
	case EnvelopeKindPublicEvent:
		fields = append(fields, encodedField{tagIdentity, []byte(env.EventID)})
		appendBodyFields(&fields, env.Public, tagPublicInline, tagPublicReference)
		appendBodyFields(&fields, env.Runtime, tagRuntimeInline, tagRuntimeReference)
	case EnvelopeKindRuntimeControl:
		fields = append(fields, encodedField{tagIdentity, []byte(env.RecordID)})
		appendBodyFields(&fields, env.Runtime, tagRuntimeInline, tagRuntimeReference)
	case EnvelopeKindOpeningFence:
		fields = append(fields, encodedField{tagLeaseEpoch, encodeUint64(env.LeaseEpoch)})
	case EnvelopeKindApplicationPrefix:
		fields = append(fields,
			encodedField{tagIdentity, []byte(env.CommandID)},
			encodedField{tagLeaseEpoch, encodeUint64(env.LeaseEpoch)},
			encodedField{tagRuntimeCommandID, env.RuntimeCommandID[:]},
			encodedField{tagCommandKind, []byte(env.CommandKind)},
		)
	}

	fieldsBytes := 0
	for _, field := range fields {
		if fieldsBytes > MaxEnvelopeBytes-5-len(field.value) {
			return nil, envelopeError(EnvelopeErrorTooLarge, "frame", nil)
		}
		fieldsBytes += 5 + len(field.value)
	}
	if envelopeHeaderBytes+fieldsBytes > MaxEnvelopeBytes {
		return nil, envelopeError(EnvelopeErrorTooLarge, "frame", nil)
	}

	frame := make([]byte, envelopeHeaderBytes, envelopeHeaderBytes+fieldsBytes)
	copy(frame[:4], envelopeMagic[:])
	frame[4] = EnvelopeVersion
	frame[5] = byte(env.Kind)
	// The closed V1 schema emits at most four fields.
	binary.BigEndian.PutUint16(frame[6:8], uint16(len(fields))) // #nosec G115 -- len(fields) <= 4 above
	// The frame-size guard above proves fieldsBytes is below one MiB.
	binary.BigEndian.PutUint32(frame[8:12], uint32(fieldsBytes)) // #nosec G115 -- fieldsBytes < MaxEnvelopeBytes
	for _, field := range fields {
		frame = append(frame, field.tag)
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(field.value))) // #nosec G115 -- each field is bounded by the validated frame
		frame = append(frame, length[:]...)
		frame = append(frame, field.value...)
	}
	return frame, nil
}

type encodedField struct {
	tag   uint8
	value []byte
}

func appendBodyFields(fields *[]encodedField, slot BodySlot, inlineTag, referenceTag uint8) {
	if slot.Inline != nil {
		*fields = append(*fields, encodedField{inlineTag, slot.Inline})
	}
	if slot.Reference != nil {
		*fields = append(*fields, encodedField{referenceTag, encodeBodyReference(*slot.Reference)})
	}
}

// DecodeEnvelope strictly decodes one complete frame. It checks the one-MiB
// envelope bound before allocating and returns caller-owned body copies.
func DecodeEnvelope(frame []byte) (Envelope, error) {
	if len(frame) > MaxEnvelopeBytes {
		return Envelope{}, envelopeError(EnvelopeErrorTooLarge, "frame", nil)
	}
	if len(frame) == 0 {
		return Envelope{}, envelopeError(EnvelopeErrorMalformed, "frame", nil)
	}
	if len(frame) < envelopeHeaderBytes {
		return Envelope{}, envelopeError(EnvelopeErrorLength, "header", nil)
	}
	if !bytes.Equal(frame[:4], envelopeMagic[:]) {
		return Envelope{}, envelopeError(EnvelopeErrorMalformed, "magic", nil)
	}
	if frame[4] != EnvelopeVersion {
		return Envelope{}, envelopeError(EnvelopeErrorVersion, "version", nil)
	}
	kind := EnvelopeKind(frame[5])
	if !knownEnvelopeKind(kind) {
		return Envelope{}, envelopeError(EnvelopeErrorKind, "kind", nil)
	}
	fieldCount := int(binary.BigEndian.Uint16(frame[6:8]))
	fieldsBytes := uint64(binary.BigEndian.Uint32(frame[8:12]))
	if fieldsBytes > MaxEnvelopeBytes-envelopeHeaderBytes {
		return Envelope{}, envelopeError(EnvelopeErrorTooLarge, "frame", nil)
	}
	declaredEnd := uint64(envelopeHeaderBytes) + fieldsBytes
	if declaredEnd > uint64(len(frame)) {
		return Envelope{}, envelopeError(EnvelopeErrorLength, "fields", nil)
	}
	if declaredEnd < uint64(len(frame)) {
		return Envelope{}, envelopeError(EnvelopeErrorTrailing, "frame", nil)
	}

	env := Envelope{Kind: kind}
	offset := envelopeHeaderBytes
	previousTag := uint8(0)
	var fieldsSeen envelopeFieldSet
	for i := 0; i < fieldCount; i++ {
		if len(frame)-offset < 5 {
			return Envelope{}, envelopeError(EnvelopeErrorLength, "field", nil)
		}
		tag := frame[offset]
		if tag < tagIdentity || tag > tagCommandKind {
			return Envelope{}, envelopeError(EnvelopeErrorField, "tag", nil)
		}
		if tag <= previousTag {
			return Envelope{}, envelopeError(EnvelopeErrorOrder, "tag", nil)
		}
		previousTag = tag
		length32 := binary.BigEndian.Uint32(frame[offset+1 : offset+5])
		offset += 5
		if length32 > MaxEnvelopeBytes {
			return Envelope{}, envelopeError(EnvelopeErrorTooLarge, fieldName(tag), nil)
		}
		length := int(length32) // #nosec G115 -- length32 <= MaxEnvelopeBytes
		if length > len(frame)-offset {
			return Envelope{}, envelopeError(EnvelopeErrorLength, fieldName(tag), nil)
		}
		value := frame[offset : offset+length]
		offset += length
		field := envelopeFieldSet(1 << (tag - 1))
		fieldsSeen |= field
		if field&allowedEnvelopeFields(kind) == 0 {
			continue
		}
		if err := decodeField(&env, tag, value); err != nil {
			return Envelope{}, err
		}
	}
	if offset < len(frame) {
		return Envelope{}, envelopeError(EnvelopeErrorLength, "field_count", nil)
	}
	if offset > len(frame) {
		return Envelope{}, envelopeError(EnvelopeErrorLength, "fields", nil)
	}
	if err := validateDecodedFields(kind, fieldsSeen); err != nil {
		return Envelope{}, err
	}
	if err := validateEnvelope(env); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

func allowedEnvelopeFields(kind EnvelopeKind) envelopeFieldSet {
	switch kind {
	case EnvelopeKindPublicEvent:
		return fieldIdentity | fieldPublicInline | fieldPublicReference | fieldRuntimeInline | fieldRuntimeReference
	case EnvelopeKindRuntimeControl:
		return fieldIdentity | fieldRuntimeInline | fieldRuntimeReference
	case EnvelopeKindOpeningFence:
		return fieldLeaseEpoch
	case EnvelopeKindApplicationPrefix:
		return fieldIdentity | fieldLeaseEpoch | fieldRuntimeCommandID | fieldCommandKind
	default:
		return 0
	}
}

func validateDecodedFields(kind EnvelopeKind, fields envelopeFieldSet) error {
	if fields&^allowedEnvelopeFields(kind) != 0 {
		return envelopeError(EnvelopeErrorField, "record_shape", nil)
	}
	require := func(field envelopeFieldSet, tag uint8) error {
		if fields&field == 0 {
			return envelopeError(EnvelopeErrorMissing, fieldName(tag), nil)
		}
		return nil
	}
	exactlyOne := func(first, second envelopeFieldSet, name string, required bool) error {
		present := fields & (first | second)
		if present == first|second {
			return envelopeError(EnvelopeErrorField, name, nil)
		}
		if required && present == 0 {
			return envelopeError(EnvelopeErrorMissing, name, nil)
		}
		return nil
	}

	switch kind {
	case EnvelopeKindPublicEvent:
		if err := require(fieldIdentity, tagIdentity); err != nil {
			return err
		}
		if err := exactlyOne(fieldPublicInline, fieldPublicReference, "public_body", true); err != nil {
			return err
		}
		return exactlyOne(fieldRuntimeInline, fieldRuntimeReference, "runtime_body", false)
	case EnvelopeKindRuntimeControl:
		if err := require(fieldIdentity, tagIdentity); err != nil {
			return err
		}
		return exactlyOne(fieldRuntimeInline, fieldRuntimeReference, "runtime_body", true)
	case EnvelopeKindOpeningFence:
		return require(fieldLeaseEpoch, tagLeaseEpoch)
	case EnvelopeKindApplicationPrefix:
		for _, required := range []struct {
			field envelopeFieldSet
			tag   uint8
		}{
			{fieldIdentity, tagIdentity},
			{fieldLeaseEpoch, tagLeaseEpoch},
			{fieldRuntimeCommandID, tagRuntimeCommandID},
			{fieldCommandKind, tagCommandKind},
		} {
			if err := require(required.field, required.tag); err != nil {
				return err
			}
		}
	}
	return nil
}

func decodeField(env *Envelope, tag uint8, value []byte) error {
	switch tag {
	case tagIdentity:
		if len(value) > sessionwire.MaxIDBytes {
			return envelopeError(EnvelopeErrorInvalid, "identity", nil)
		}
		switch env.Kind {
		case EnvelopeKindPublicEvent:
			env.EventID = sessionwire.EventID(string(value))
		case EnvelopeKindRuntimeControl:
			env.RecordID = string(value)
		case EnvelopeKindApplicationPrefix:
			env.CommandID = sessionwire.CommandID(string(value))
		default:
			env.RecordID = string(value)
		}
	case tagPublicInline:
		if len(value) > MaxInlineBodyBytes {
			return envelopeError(EnvelopeErrorTooLarge, "public_inline", nil)
		}
		env.Public.Inline = append([]byte{}, value...)
	case tagPublicReference:
		reference, err := decodeBodyReference(value)
		if err != nil {
			return err
		}
		env.Public.Reference = &reference
	case tagRuntimeInline:
		if len(value) > MaxInlineBodyBytes {
			return envelopeError(EnvelopeErrorTooLarge, "runtime_inline", nil)
		}
		env.Runtime.Inline = append([]byte{}, value...)
	case tagRuntimeReference:
		reference, err := decodeBodyReference(value)
		if err != nil {
			return err
		}
		env.Runtime.Reference = &reference
	case tagLeaseEpoch:
		if len(value) != 8 {
			return envelopeError(EnvelopeErrorLength, "lease_epoch", nil)
		}
		env.LeaseEpoch = binary.BigEndian.Uint64(value)
	case tagRuntimeCommandID:
		if len(value) != len(env.RuntimeCommandID) {
			return envelopeError(EnvelopeErrorLength, "runtime_command_id", nil)
		}
		copy(env.RuntimeCommandID[:], value)
	case tagCommandKind:
		if len(value) > 64 {
			return envelopeError(EnvelopeErrorInvalid, "command_kind", nil)
		}
		env.CommandKind = string(value)
	}
	return nil
}

func validateEnvelope(env Envelope) error {
	if !knownEnvelopeKind(env.Kind) {
		return envelopeError(EnvelopeErrorKind, "kind", nil)
	}
	if err := validateSlot(env.Public, "public"); err != nil {
		return err
	}
	if err := validateSlot(env.Runtime, "runtime"); err != nil {
		return err
	}

	switch env.Kind {
	case EnvelopeKindPublicEvent:
		if env.EventID == "" {
			return envelopeError(EnvelopeErrorMissing, "identity", nil)
		}
		if err := env.EventID.Validate(); err != nil {
			return envelopeError(EnvelopeErrorInvalid, "identity", err)
		}
		if !env.Public.present() {
			return envelopeError(EnvelopeErrorMissing, "public_body", nil)
		}
		if env.Public.Inline != nil {
			body := bytes.Clone(env.Public.Inline)
			if err := (sessionwire.JournalEvent{EventID: env.EventID, JournalSeq: 1, Body: json.RawMessage(body)}).Validate(); err != nil {
				return envelopeError(EnvelopeErrorInvalid, "public_inline", err)
			}
		}
		if env.RecordID != "" || env.CommandID != "" || !env.RuntimeCommandID.IsZero() || env.LeaseEpoch != 0 || env.CommandKind != "" {
			return envelopeError(EnvelopeErrorField, "record_shape", nil)
		}
	case EnvelopeKindRuntimeControl:
		if env.RecordID == "" {
			return envelopeError(EnvelopeErrorMissing, "identity", nil)
		}
		if len(env.RecordID) > sessionwire.MaxIDBytes || !utf8.ValidString(env.RecordID) {
			return envelopeError(EnvelopeErrorInvalid, "identity", nil)
		}
		if !env.Runtime.present() {
			return envelopeError(EnvelopeErrorMissing, "runtime_body", nil)
		}
		if env.Public.present() || env.EventID != "" || env.CommandID != "" || !env.RuntimeCommandID.IsZero() || env.LeaseEpoch != 0 || env.CommandKind != "" {
			return envelopeError(EnvelopeErrorField, "record_shape", nil)
		}
	case EnvelopeKindOpeningFence:
		if env.LeaseEpoch == 0 {
			return envelopeError(EnvelopeErrorInvalid, "lease_epoch", nil)
		}
		if env.EventID != "" || env.RecordID != "" || env.Public.present() || env.Runtime.present() || env.CommandID != "" || !env.RuntimeCommandID.IsZero() || env.CommandKind != "" {
			return envelopeError(EnvelopeErrorField, "record_shape", nil)
		}
	case EnvelopeKindApplicationPrefix:
		if env.CommandID == "" {
			return envelopeError(EnvelopeErrorMissing, "identity", nil)
		}
		if err := env.CommandID.Validate(); err != nil {
			return envelopeError(EnvelopeErrorInvalid, "identity", err)
		}
		if env.RuntimeCommandID.IsZero() {
			return envelopeError(EnvelopeErrorInvalid, "runtime_command_id", nil)
		}
		if env.LeaseEpoch == 0 {
			return envelopeError(EnvelopeErrorInvalid, "lease_epoch", nil)
		}
		if env.CommandKind == "" {
			return envelopeError(EnvelopeErrorMissing, "command_kind", nil)
		}
		if len(env.CommandKind) > 64 || !utf8.ValidString(env.CommandKind) {
			return envelopeError(EnvelopeErrorInvalid, "command_kind", nil)
		}
		if env.EventID != "" || env.RecordID != "" || env.Public.present() || env.Runtime.present() {
			return envelopeError(EnvelopeErrorField, "record_shape", nil)
		}
	}
	return nil
}

func validateSlot(slot BodySlot, name string) error {
	if slot.Inline != nil && slot.Reference != nil {
		return envelopeError(EnvelopeErrorField, name+"_body", nil)
	}
	if len(slot.Inline) > MaxInlineBodyBytes {
		return envelopeError(EnvelopeErrorTooLarge, name+"_inline", nil)
	}
	if slot.Reference != nil {
		if err := slot.Reference.Reference.Validate(); err != nil {
			return envelopeError(EnvelopeErrorInvalid, name+"_reference", err)
		}
		if isZeroDigest(slot.Reference.SHA256) {
			return envelopeError(EnvelopeErrorDigest, name+"_reference", nil)
		}
	}
	return nil
}

func (s BodySlot) present() bool { return s.Inline != nil || s.Reference != nil }

func knownEnvelopeKind(kind EnvelopeKind) bool {
	switch kind {
	case EnvelopeKindPublicEvent, EnvelopeKindRuntimeControl, EnvelopeKindOpeningFence, EnvelopeKindApplicationPrefix:
		return true
	default:
		return false
	}
}

func encodeBodyReference(reference BodyReference) []byte {
	objectID := []byte(reference.Reference.ObjectID)
	encoded := make([]byte, 1+2+len(objectID)+8+len(reference.SHA256))
	encoded[0] = bodyReferenceAlgorithmSHA256
	// BodyReference validation limits ObjectID to sessionwire.MaxIDBytes (256).
	binary.BigEndian.PutUint16(encoded[1:3], uint16(len(objectID))) // #nosec G115 -- len(objectID) <= 256
	copy(encoded[3:], objectID)
	offset := 3 + len(objectID)
	binary.BigEndian.PutUint64(encoded[offset:offset+8], reference.SizeBytes)
	copy(encoded[offset+8:], reference.SHA256[:])
	return encoded
}

func decodeBodyReference(encoded []byte) (BodyReference, error) {
	const fixedBytes = 1 + 2 + 8 + 32
	if len(encoded) < fixedBytes {
		return BodyReference{}, envelopeError(EnvelopeErrorLength, "body_reference", nil)
	}
	if encoded[0] != bodyReferenceAlgorithmSHA256 {
		return BodyReference{}, envelopeError(EnvelopeErrorDigest, "body_reference", nil)
	}
	objectIDBytes := int(binary.BigEndian.Uint16(encoded[1:3]))
	if objectIDBytes > sessionwire.MaxIDBytes || len(encoded) != fixedBytes+objectIDBytes {
		return BodyReference{}, envelopeError(EnvelopeErrorLength, "body_reference", nil)
	}
	offset := 3 + objectIDBytes
	reference := BodyReference{
		Reference: sessionwire.ObjectReference{ObjectID: string(encoded[3:offset])},
		SizeBytes: binary.BigEndian.Uint64(encoded[offset : offset+8]),
	}
	copy(reference.SHA256[:], encoded[offset+8:])
	if err := reference.Reference.Validate(); err != nil {
		return BodyReference{}, envelopeError(EnvelopeErrorInvalid, "body_reference", err)
	}
	if isZeroDigest(reference.SHA256) {
		return BodyReference{}, envelopeError(EnvelopeErrorDigest, "body_reference", nil)
	}
	return reference, nil
}

func isZeroDigest(digest [32]byte) bool { return digest == [32]byte{} }

func encodeUint64(value uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return encoded
}

func fieldName(tag uint8) string {
	switch tag {
	case tagIdentity:
		return "identity"
	case tagPublicInline:
		return "public_inline"
	case tagPublicReference:
		return "public_reference"
	case tagRuntimeInline:
		return "runtime_inline"
	case tagRuntimeReference:
		return "runtime_reference"
	case tagLeaseEpoch:
		return "lease_epoch"
	case tagRuntimeCommandID:
		return "runtime_command_id"
	case tagCommandKind:
		return "command_kind"
	default:
		return "field"
	}
}
