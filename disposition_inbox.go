package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// DispositionInboxRecordVersion identifies the pending-only disposition inbox
// codec. Legacy v1 records retain their original codec and PayloadRef equality.
// Future attempts or terminal states require an explicit codec/API extension;
// this version refuses them and supplies no dispatch or settlement authority.
const DispositionInboxRecordVersion uint8 = 2

// A separate sharded namespace keeps legacy due sweepers away from disposition
// commands. Updated legacy mutations also check the session mode witness. Old
// binaries unaware of that witness must still be excluded operationally.
const dispositionInboxNamespace = "sessionstore/disposition-inbox"

// DispositionCommandDescriptor is immutable command identity and its winning
// runtime mapping. PayloadDigest is lowercase SHA-256 hex. PayloadSize and digest
// identify content regardless of representation or independent upload generation.
// PayloadObject, when present, contains the exact winning canonical metadata.
// Payload bytes are private and bounded by MaxInboxPayloadBytes; nil means empty
// inline content when PayloadObject is nil. Binding is the actual catalog pin.
// These struct tags are NOT the durable spelling: stored member names are pinned
// separately by this package's private wire DTO and by golden byte literals, so
// renaming a field here cannot move a stored record.
type DispositionCommandDescriptor struct {
	// PublicCreate is emitted only by AdmitPublicCreate. Its explicit presence
	// fails older strict canonical v2 decoders; Kind itself remains opaque.
	// Generic reads check catalog binding, not reservation proof. The marker
	// alone cannot authorize future public-create dispatch; only successful
	// AdmitPublicCreate verifies reservation, catalog and inbox for an ACK.
	PublicCreate     bool                        `json:"public_create,omitempty"`
	TenantID         sessionwire.TenantID        `json:"tenant_id"`
	SessionID        sessionwire.SessionID       `json:"session_id"`
	CommandID        sessionwire.CommandID       `json:"command_id"`
	Binding          SessionBinding              `json:"binding"`
	RuntimeCommandID RuntimeCommandID            `json:"runtime_command_id"`
	Kind             CommandKind                 `json:"kind"`
	PayloadDigest    string                      `json:"payload_digest"`
	PayloadSize      uint64                      `json:"payload_size"`
	Payload          []byte                      `json:"payload,omitempty"`
	PayloadObject    *sessionwire.ObjectMetadata `json:"payload_object,omitempty"`
}

// DispositionInboxRecord only admits pending commands. Timestamps, like the
// descriptor, remain exactly those chosen by the winning create.
type DispositionInboxRecord struct {
	Descriptor    DispositionCommandDescriptor `json:"descriptor"`
	AcceptedAt    time.Time                    `json:"accepted_at"`
	ApplyDeadline time.Time                    `json:"apply_deadline"`
	State         InboxState                   `json:"state"`
}

// DispositionInboxEntry carries provider revision and opaque per-session order.
// AcceptedOrder is increasing, not contiguous, and not comparable across sessions
// or protocols. Due pages are deadline-ordered, not acceptance-ordered.
type DispositionInboxEntry struct {
	Record        DispositionInboxRecord
	Revision      uint64
	AcceptedOrder uint64
}

// AdmitDispositionCommandRequest proposes a pending command for an existing
// disposition catalog session. Binding must equal its actual immutable pin.
// A retry compares binding/kind/content and returns the winning runtime ID,
// metadata or inline representation, timestamps, deadline and acceptance order.
// This is not public-create reservation: uniqueness is scoped to one session.
type AdmitDispositionCommandRequest struct {
	TenantID                 sessionwire.TenantID
	SessionID                sessionwire.SessionID
	CommandID                sessionwire.CommandID
	Binding                  SessionBinding
	ProposedRuntimeCommandID RuntimeCommandID
	Kind                     CommandKind
	Payload                  []byte
	PayloadObject            *sessionwire.ObjectMetadata
	AcceptedAt               time.Time
	ApplyDeadline            time.Time
}

// PutCommandPayloadRequest declares exact content for an orchestration inbox
// object. There is no caller-selectable kind: only command-payload is allowed.
type PutCommandPayloadRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	SizeBytes uint64
	SHA256    [32]byte
	MediaType string
	Body      io.Reader
}

// PutCommandPayload verifies an existing disposition catalog and reuses the
// verified streaming upload/index path. Command payloads live in orchestration
// inbox storage; the binding independently selects journal/artifact storage.
// This does not enable arbitrary object or journal writes for disposition mode.
// Independent same-content uploads have distinct generations; losing uploads
// may remain orphaned. No backfill or garbage collection is implemented.
func (s *Store) PutCommandPayload(ctx context.Context, req PutCommandPayloadRequest) (sessionwire.ObjectMetadata, error) {
	return s.putObject(ctx, PutObjectRequest{TenantID: req.TenantID, SessionID: req.SessionID, Kind: ObjectKindCommandPayload, SizeBytes: req.SizeBytes, SHA256: req.SHA256, MediaType: req.MediaType, Body: req.Body}, ProtocolModeDisposition)
}

func (s *Store) dispositionCatalog(ctx context.Context, scope sessionScope, tenant sessionwire.TenantID, session sessionwire.SessionID) (SessionBinding, error) {
	entry, err := s.readCatalogEntry(ctx, scope, tenant, session)
	if err != nil {
		return SessionBinding{}, err
	}
	if entry.Record.Binding.ProtocolMode != ProtocolModeDisposition {
		return SessionBinding{}, catalogInvalid("binding.protocol_mode", nil)
	}
	return entry.Record.Binding, nil
}

// AdmitDispositionCommand performs one create-only ordered write after bounded
// authority checks. Unknown write outcomes return InboxErrorUnknown; retry the
// same identity to learn the durable winner. No error licenses dispatch.
// Object metadata is checked against one exact immutable index row, without
// reading its body. That index does not prove current existence after external
// deletion; GetObject must still verify the full stream when consuming it.
func (s *Store) AdmitDispositionCommand(ctx context.Context, req AdmitDispositionCommandRequest) (DispositionInboxEntry, bool, error) {
	return s.admitDispositionCommand(ctx, req, nil)
}

func dispositionDescriptor(req AdmitDispositionCommandRequest) (DispositionCommandDescriptor, error) {
	d := DispositionCommandDescriptor{TenantID: req.TenantID, SessionID: req.SessionID, CommandID: req.CommandID, Binding: req.Binding, RuntimeCommandID: req.ProposedRuntimeCommandID, Kind: req.Kind, Payload: req.Payload, PayloadObject: req.PayloadObject}
	if d.PayloadObject == nil {
		digest := sha256.Sum256(d.Payload)
		d.PayloadDigest, d.PayloadSize = hex.EncodeToString(digest[:]), uint64(len(d.Payload))
	} else {
		parsed, err := parseObjectMetadata(*d.PayloadObject)
		if err != nil {
			return DispositionCommandDescriptor{}, err
		}
		if parsed.kind != ObjectKindCommandPayload || !d.PayloadObject.CreatedAt.IsZero() {
			return DispositionCommandDescriptor{}, inboxInvalid("payload_object", nil)
		}
		d.PayloadDigest, d.PayloadSize = hex.EncodeToString(parsed.digest[:]), d.PayloadObject.SizeBytes
	}
	return d, nil
}

func (s *Store) admitDispositionCommand(ctx context.Context, req AdmitDispositionCommandRequest, public *PublicCreateReservation) (DispositionInboxEntry, bool, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	d, err := dispositionDescriptor(req)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	d.PublicCreate = public != nil
	value, record, err := encodeDispositionInboxRecord(DispositionInboxRecord{Descriptor: d, AcceptedAt: req.AcceptedAt, ApplyDeadline: req.ApplyDeadline, State: InboxStatePending})
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	defer release()
	catalog, err := s.readCatalogEntry(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	binding := catalog.Record.Binding
	if binding.ProtocolMode != ProtocolModeDisposition {
		return DispositionInboxEntry{}, false, catalogInvalid("binding.protocol_mode", nil)
	}
	if public != nil {
		if !samePublicCreate(public, catalog.Record.PublicCreate) {
			return DispositionInboxEntry{}, false, inboxErr(InboxErrorCommandMismatch, "public_create", nil)
		}
	} else if catalog.Record.PublicCreate != nil && catalog.Record.PublicCreate.Identity.CommandID == req.CommandID {
		return DispositionInboxEntry{}, false, inboxErr(InboxErrorCommandMismatch, "public_create", nil)
	}
	if binding != d.Binding {
		return DispositionInboxEntry{}, false, inboxErr(InboxErrorCommandMismatch, "binding", nil)
	}
	if d.PayloadObject != nil {
		metadata, err := s.GetObjectMetadata(opCtx, GetObjectMetadataRequest{TenantID: req.TenantID, SessionID: req.SessionID, ExpectedKind: ObjectKindCommandPayload, Reference: d.PayloadObject.Reference})
		if err != nil {
			return DispositionInboxEntry{}, false, err
		}
		if metadata != *d.PayloadObject {
			return DispositionInboxEntry{}, false, objectErr(ObjectErrorIntegrity, "metadata", nil)
		}
	}
	if err := s.bindSessionScopeMode(opCtx, scope, ProtocolModeDisposition); err != nil {
		return DispositionInboxEntry{}, false, err
	}
	stored, created, err := s.backend.OrderedIndex.Create(opCtx, dispositionInboxID(scope, req.CommandID), scope.SessionNamespace, value, storage.Rank{}, dispositionInboxDue(record))
	if err != nil {
		return DispositionInboxEntry{}, false, classifyInboxOrderedError(err, "create")
	}
	entry, err := dispositionInboxEntryFor(stored, scope, req.TenantID, req.SessionID, req.CommandID, binding)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	if created && !bytes.Equal(value, stored.Value) {
		return DispositionInboxEntry{}, false, inboxErr(InboxErrorIdentity, "value", nil)
	}
	winner := entry.Record.Descriptor
	if winner.PublicCreate != d.PublicCreate || winner.Kind != d.Kind || winner.PayloadDigest != d.PayloadDigest || winner.PayloadSize != d.PayloadSize {
		return DispositionInboxEntry{}, false, inboxErr(InboxErrorCommandMismatch, "command", nil)
	}
	if public != nil && (winner.RuntimeCommandID != public.RuntimeCommandID || !entry.Record.AcceptedAt.Equal(public.AcceptedAt) || !entry.Record.ApplyDeadline.Equal(public.ApplyDeadline)) {
		return DispositionInboxEntry{}, false, inboxErr(InboxErrorCommandMismatch, "public_create", nil)
	}
	return entry, created, nil
}

// GetDispositionCommandRequest names one command in the disposition namespace.
type GetDispositionCommandRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID
}

// GetDispositionCommand checks actual catalog authority and reads one exact
// inbox row. A protocol witness alone never authorizes a result.
// It verifies the catalog binding, not the public-create reservation. A returned
// PublicCreate marker alone cannot authorize future public-create dispatch;
// only successful AdmitPublicCreate verifies reservation, catalog and inbox
// together to acknowledge that create.
func (s *Store) GetDispositionCommand(ctx context.Context, req GetDispositionCommandRequest) (DispositionInboxEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return DispositionInboxEntry{}, err
	}
	if err := req.CommandID.Validate(); err != nil {
		return DispositionInboxEntry{}, inboxInvalid("command_id", err)
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return DispositionInboxEntry{}, err
	}
	defer release()
	binding, err := s.dispositionCatalog(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return DispositionInboxEntry{}, err
	}
	stored, err := s.backend.OrderedIndex.Get(opCtx, dispositionInboxID(scope, req.CommandID))
	if err != nil {
		return DispositionInboxEntry{}, classifyInboxOrderedError(err, "get")
	}
	return dispositionInboxEntryFor(stored, scope, req.TenantID, req.SessionID, req.CommandID, binding)
}

func dispositionInboxID(scope sessionScope, command sessionwire.CommandID) storage.OrderedID {
	return storage.OrderedID{Namespace: shardNamespace(dispositionInboxNamespace, scope.ControlShard), OrderingScope: scope.SessionNamespace, StableKey: storage.StableKey(command)}
}

func dispositionInboxDue(r DispositionInboxRecord) storage.Due {
	return storage.Due{State: storage.DueAt, UnixMillis: r.ApplyDeadline.UnixMilli()}
}

func dispositionInboxEntryFor(stored storage.OrderedRecord, scope sessionScope, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID, binding SessionBinding) (DispositionInboxEntry, error) {
	if stored.Deleted {
		return DispositionInboxEntry{}, inboxErr(InboxErrorDeleted, "record", nil)
	}
	r, err := decodeDispositionInboxRecord(stored.Value)
	if err != nil {
		return DispositionInboxEntry{}, err
	}
	d := r.Descriptor
	if d.TenantID != tenant || d.SessionID != session || d.CommandID != command || d.Binding != binding || stored.ID != dispositionInboxID(scope, command) || stored.Order == 0 || stored.Revision == 0 {
		return DispositionInboxEntry{}, inboxErr(InboxErrorIdentity, "record", nil)
	}
	if err := checkFiledScope(stored, scope.SessionNamespace, dispositionInboxDue(r), inboxIdentity); err != nil {
		return DispositionInboxEntry{}, err
	}
	return DispositionInboxEntry{Record: r, Revision: stored.Revision, AcceptedOrder: stored.Order}, nil
}

type dispositionInboxWire struct {
	RecordVersion uint8 `json:"record_version"`
	dispositionInboxRecordWire
}

// The private DTOs below fix this record's durable member names independently of
// the exported record and descriptor's Go field names, so renaming an exported
// field cannot silently rewrite stored bytes. public_create is emitted only when
// true; that presence, its absence, and each of the two mutually exclusive
// payload representations are pinned by golden literals in
// TestDispositionInboxWireGolden.
// Canonical re-encoding rejects every other spelling, including an explicit
// "public_create":false. Conversions are exhaustive in both directions.
type dispositionInboxRecordWire struct {
	Descriptor    dispositionDescriptorWire `json:"descriptor"`
	AcceptedAt    time.Time                 `json:"accepted_at"`
	ApplyDeadline time.Time                 `json:"apply_deadline"`
	State         InboxState                `json:"state"`
}

type dispositionDescriptorWire struct {
	PublicCreate     bool                        `json:"public_create,omitempty"`
	TenantID         sessionwire.TenantID        `json:"tenant_id"`
	SessionID        sessionwire.SessionID       `json:"session_id"`
	CommandID        sessionwire.CommandID       `json:"command_id"`
	Binding          SessionBinding              `json:"binding"`
	RuntimeCommandID RuntimeCommandID            `json:"runtime_command_id"`
	Kind             CommandKind                 `json:"kind"`
	PayloadDigest    string                      `json:"payload_digest"`
	PayloadSize      uint64                      `json:"payload_size"`
	Payload          []byte                      `json:"payload,omitempty"`
	PayloadObject    *sessionwire.ObjectMetadata `json:"payload_object,omitempty"`
}

// Taking staticcheck's S1016 advice here would defeat the point. A conversion
// only compiles while the DTO's field NAMES still match the exported struct's,
// so the two would be renamed together — reintroducing exactly the coupling
// these member names are being bought independence from. Written out, each
// member is assigned once and the golden literals pin the result.
func dispositionInboxToWire(r DispositionInboxRecord) dispositionInboxRecordWire {
	d := r.Descriptor
	return dispositionInboxRecordWire{
		//lint:ignore S1016 durable member names must stay independent of the exported struct
		Descriptor: dispositionDescriptorWire{
			PublicCreate: d.PublicCreate, TenantID: d.TenantID, SessionID: d.SessionID, CommandID: d.CommandID,
			Binding: d.Binding, RuntimeCommandID: d.RuntimeCommandID, Kind: d.Kind,
			PayloadDigest: d.PayloadDigest, PayloadSize: d.PayloadSize, Payload: d.Payload, PayloadObject: d.PayloadObject,
		},
		AcceptedAt: r.AcceptedAt, ApplyDeadline: r.ApplyDeadline, State: r.State,
	}
}

func (w dispositionInboxRecordWire) record() DispositionInboxRecord {
	d := w.Descriptor
	return DispositionInboxRecord{
		//lint:ignore S1016 the reverse direction is written out for the same reason
		Descriptor: DispositionCommandDescriptor{
			PublicCreate: d.PublicCreate, TenantID: d.TenantID, SessionID: d.SessionID, CommandID: d.CommandID,
			Binding: d.Binding, RuntimeCommandID: d.RuntimeCommandID, Kind: d.Kind,
			PayloadDigest: d.PayloadDigest, PayloadSize: d.PayloadSize, Payload: d.Payload, PayloadObject: d.PayloadObject,
		},
		AcceptedAt: w.AcceptedAt, ApplyDeadline: w.ApplyDeadline, State: w.State,
	}
}

func encodeDispositionInboxRecord(r DispositionInboxRecord) ([]byte, DispositionInboxRecord, error) {
	r, err := canonicalDispositionInboxRecord(r)
	if err != nil {
		return nil, DispositionInboxRecord{}, err
	}
	value, err := json.Marshal(dispositionInboxWire{RecordVersion: DispositionInboxRecordVersion, dispositionInboxRecordWire: dispositionInboxToWire(r)})
	if err != nil {
		return nil, DispositionInboxRecord{}, inboxInvalid("record", err)
	}
	if len(value) > MaxInboxRecordBytes {
		return nil, DispositionInboxRecord{}, inboxErr(InboxErrorTooLarge, "record", nil)
	}
	return value, r, nil
}

func decodeDispositionInboxRecord(value []byte) (DispositionInboxRecord, error) {
	wire, err := decodeVersionedRecord[dispositionInboxWire](value, MaxInboxRecordBytes, DispositionInboxRecordVersion, versionedRecordFields{Record: "record", Version: "record_version"}, inboxRecordFailure)
	if err != nil {
		return DispositionInboxRecord{}, err
	}
	canonical, record, err := encodeDispositionInboxRecord(wire.record())
	if err != nil {
		return DispositionInboxRecord{}, err
	}
	// V2 is a canonical stored codec, not a user JSON input format. Exact
	// re-encoding rejects duplicate members and nested unknown fields (including
	// fields dropped by additive Core projection decoders), not just top-level
	// unknowns. Noncanonical spellings require an explicit offline conversion.
	if !bytes.Equal(canonical, value) {
		return DispositionInboxRecord{}, inboxErr(InboxErrorMalformed, "record", nil)
	}
	return record, nil
}

func canonicalDispositionInboxRecord(r DispositionInboxRecord) (DispositionInboxRecord, error) {
	d := &r.Descriptor
	// Reuse the legacy record's neutral identity/time bounds, never its payload
	// equality or transition semantics. Only a constructed pending shell is used.
	base, err := canonicalInboxRecord(InboxRecord{TenantID: d.TenantID, SessionID: d.SessionID, CommandID: d.CommandID, RuntimeCommandID: d.RuntimeCommandID, Kind: d.Kind, AcceptedAt: r.AcceptedAt, ApplyDeadline: r.ApplyDeadline, State: InboxStatePending})
	if err != nil {
		return DispositionInboxRecord{}, err
	}
	if err := d.Binding.validate(); err != nil {
		return DispositionInboxRecord{}, err
	}
	if d.Binding.ProtocolMode != ProtocolModeDisposition || r.State != InboxStatePending {
		return DispositionInboxRecord{}, inboxInvalid("protocol_state", nil)
	}
	if len(d.Payload) > MaxInboxPayloadBytes {
		return DispositionInboxRecord{}, inboxInvalid("payload", nil)
	}
	if d.PayloadObject == nil {
		digest := sha256.Sum256(d.Payload)
		if d.PayloadSize != uint64(len(d.Payload)) || d.PayloadDigest != hex.EncodeToString(digest[:]) {
			return DispositionInboxRecord{}, inboxInvalid("payload_identity", nil)
		}
	} else {
		parsed, err := parseObjectMetadata(*d.PayloadObject)
		if err != nil {
			return DispositionInboxRecord{}, err
		}
		if len(d.Payload) != 0 || parsed.kind != ObjectKindCommandPayload || d.PayloadDigest != hex.EncodeToString(parsed.digest[:]) || d.PayloadSize != d.PayloadObject.SizeBytes || !d.PayloadObject.CreatedAt.IsZero() {
			return DispositionInboxRecord{}, inboxInvalid("payload_object", nil)
		}
		metadata := *d.PayloadObject
		d.PayloadObject = &metadata
	}
	d.Payload = bytes.Clone(d.Payload)
	if len(d.Payload) == 0 {
		d.Payload = nil
	}
	r.AcceptedAt, r.ApplyDeadline = base.AcceptedAt, base.ApplyDeadline
	return r, nil
}
