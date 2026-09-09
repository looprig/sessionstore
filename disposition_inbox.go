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

// DispositionInboxRecordVersion identifies the disposition inbox codec. Legacy
// v1 records retain their original codec and PayloadRef equality. This version
// carries the claim, attempt and outcome of the settlement protocol as members
// absent until the state that requires them, so a pending record's bytes are
// exactly what they were before those states existed. It is a record format and
// not an authority: it supplies no claim, dispatch or settlement permission,
// and a further durable member still requires a version bump because decoding
// demands exact canonical re-encoding.
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

// DispositionInboxRecord is one disposition command's authoritative record.
// Timestamps, like the descriptor, remain exactly those chosen by the winning
// create; admission produces InboxStatePending and never anything else.
//
// Claim, Attempt and Outcome are the states beyond admission, and each is
// absent until the state that requires it: what each state must and must not
// carry is validateDispositionState, because that is a property of the record
// rather than of whichever transition wrote it. Once written, the attempt and
// both of its grant identities are immutable — no transition may rewrite one to
// make a successor's epochs look current. A terminal REJECTION is the one state
// that may carry none of the three: the protocol allows a command to be rejected
// before any dispatch was authorized, and such a record has no attempt for an
// outcome to be keyed by.
//
// These struct tags are NOT the durable spelling; see the private wire DTOs.
type DispositionInboxRecord struct {
	Descriptor    DispositionCommandDescriptor `json:"descriptor"`
	AcceptedAt    time.Time                    `json:"accepted_at"`
	ApplyDeadline time.Time                    `json:"apply_deadline"`
	State         InboxState                   `json:"state"`
	Claim         *DispositionClaim            `json:"claim,omitempty"`
	Attempt       *DispositionAttempt          `json:"attempt,omitempty"`
	Outcome       *DispositionOutcome          `json:"outcome,omitempty"`
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

// dispositionInboxDue files a command at its apply deadline until it is
// settled, and files a settled one nowhere. The deadline is the whole horizon:
// this protocol has no reclaim horizon to fold in, because an applying command
// is closed by evidence rather than by a claim lapsing, and a claim edge that
// would need one does not exist yet.
func dispositionInboxDue(r DispositionInboxRecord) storage.Due {
	if r.State.terminal() {
		return storage.Due{}
	}
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
	Claim         *dispositionClaimWire     `json:"claim,omitempty"`
	Attempt       *dispositionAttemptWire   `json:"attempt,omitempty"`
	Outcome       *dispositionOutcomeWire   `json:"outcome,omitempty"`
}

// The three post-admission members are pointers and omitempty, so a pending
// record's bytes are exactly what they were before they existed and the golden
// literals that pinned them still hold byte for byte. Inside each one every
// member is unconditionally present: canonical re-encoding compares whole
// bytes, so an omitempty member would pin two spellings per member for no gain.
type dispositionClaimWire struct {
	ResidencyEpoch ResidencyEpoch `json:"residency_epoch"`
	ExpiresAt      time.Time      `json:"expires_at"`
}

type dispositionAttemptWire struct {
	AttemptID      DispositionAttemptID `json:"attempt_id"`
	JournalEpoch   JournalEpoch         `json:"journal_epoch"`
	ResidencyEpoch ResidencyEpoch       `json:"residency_epoch"`
	StartedAt      time.Time            `json:"started_at"`
}

type dispositionOutcomeWire struct {
	Kind                   DispositionOutcomeKind `json:"kind"`
	AttemptID              DispositionAttemptID   `json:"attempt_id"`
	AttemptJournalEpoch    JournalEpoch           `json:"attempt_journal_epoch"`
	AuthorJournalEpoch     JournalEpoch           `json:"author_journal_epoch"`
	DispositionSeq         uint64                 `json:"disposition_seq"`
	AuthorFenceSeq         uint64                 `json:"author_fence_seq"`
	EventID                sessionwire.EventID    `json:"event_id"`
	EventSeq               uint64                 `json:"event_seq"`
	SettlingResidencyEpoch ResidencyEpoch         `json:"settling_residency_epoch"`
	SettledAt              time.Time              `json:"settled_at"`
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

// staticcheck's S1016 advice — replace both composite literals with a whole
// struct conversion — is refused, and not because a conversion would move a
// stored byte: Go ignores struct tags in conversions, so the durable spelling
// stays pinned by the DTO either way. What a conversion couples is the Go field
// names, order and types. It is refused because a conversion is a single
// statement, so the drop-a-field mutation probes that prove these conversions
// exhaustive become inexpressible and their totality rests on the compiler
// alone. Written out, each member is assigned once, every single-member drop is
// killed by a test, and the golden literals pin the result. The cost of the
// trade is that a member added to BOTH structs is not carried automatically and
// the compiler will not say so; TestWireDTOsMirrorExportedRecords and
// TestWireConversionsCarryEveryMember are what say so instead.
func dispositionInboxToWire(r DispositionInboxRecord) dispositionInboxRecordWire {
	d := r.Descriptor
	return dispositionInboxRecordWire{
		//lint:ignore S1016 written out so each member drop stays a killable mutation
		Descriptor: dispositionDescriptorWire{
			PublicCreate: d.PublicCreate, TenantID: d.TenantID, SessionID: d.SessionID, CommandID: d.CommandID,
			Binding: d.Binding, RuntimeCommandID: d.RuntimeCommandID, Kind: d.Kind,
			PayloadDigest: d.PayloadDigest, PayloadSize: d.PayloadSize, Payload: d.Payload, PayloadObject: d.PayloadObject,
		},
		AcceptedAt: r.AcceptedAt, ApplyDeadline: r.ApplyDeadline, State: r.State,
		Claim: claimToWire(r.Claim), Attempt: attemptToWire(r.Attempt), Outcome: outcomeToWire(r.Outcome),
	}
}

func claimToWire(c *DispositionClaim) *dispositionClaimWire {
	if c == nil {
		return nil
	}
	return &dispositionClaimWire{ResidencyEpoch: c.ResidencyEpoch, ExpiresAt: c.ExpiresAt}
}

func (w *dispositionClaimWire) claim() *DispositionClaim {
	if w == nil {
		return nil
	}
	return &DispositionClaim{ResidencyEpoch: w.ResidencyEpoch, ExpiresAt: w.ExpiresAt}
}

func attemptToWire(a *DispositionAttempt) *dispositionAttemptWire {
	if a == nil {
		return nil
	}
	return &dispositionAttemptWire{AttemptID: a.AttemptID, JournalEpoch: a.JournalEpoch, ResidencyEpoch: a.ResidencyEpoch, StartedAt: a.StartedAt}
}

func (w *dispositionAttemptWire) attempt() *DispositionAttempt {
	if w == nil {
		return nil
	}
	return &DispositionAttempt{AttemptID: w.AttemptID, JournalEpoch: w.JournalEpoch, ResidencyEpoch: w.ResidencyEpoch, StartedAt: w.StartedAt}
}

func outcomeToWire(o *DispositionOutcome) *dispositionOutcomeWire {
	if o == nil {
		return nil
	}
	return &dispositionOutcomeWire{
		Kind: o.Kind, AttemptID: o.AttemptID, AttemptJournalEpoch: o.AttemptJournalEpoch,
		AuthorJournalEpoch: o.AuthorJournalEpoch, DispositionSeq: o.DispositionSeq, AuthorFenceSeq: o.AuthorFenceSeq,
		EventID: o.EventID, EventSeq: o.EventSeq, SettlingResidencyEpoch: o.SettlingResidencyEpoch, SettledAt: o.SettledAt,
	}
}

func (w *dispositionOutcomeWire) outcome() *DispositionOutcome {
	if w == nil {
		return nil
	}
	return &DispositionOutcome{
		Kind: w.Kind, AttemptID: w.AttemptID, AttemptJournalEpoch: w.AttemptJournalEpoch,
		AuthorJournalEpoch: w.AuthorJournalEpoch, DispositionSeq: w.DispositionSeq, AuthorFenceSeq: w.AuthorFenceSeq,
		EventID: w.EventID, EventSeq: w.EventSeq, SettlingResidencyEpoch: w.SettlingResidencyEpoch, SettledAt: w.SettledAt,
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
		Claim: w.Claim.claim(), Attempt: w.Attempt.attempt(), Outcome: w.Outcome.outcome(),
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
	if d.Binding.ProtocolMode != ProtocolModeDisposition {
		return DispositionInboxRecord{}, inboxInvalid("protocol_state", nil)
	}
	if err := validateDispositionState(&r); err != nil {
		return DispositionInboxRecord{}, err
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

// validateDispositionState enumerates the states this record has and states,
// for each one, exactly which of the three post-admission members it must and
// must not carry. It is the record's own rule rather than a transition's, so a
// record assembled by any route — a transition, a decode, a future claim edge —
// is held to the same thing, and a member that contradicts the state cannot be
// stored at all.
//
// It also copies the three members before validating them. That is internal
// hygiene and not a guarantee to a caller: no exported API accepts a
// DispositionInboxRecord, so there is no outside caller holding a pointer it
// passed in. What the copy actually buys is that a validated record never
// aliases the value some in-package transition assembled it from, which is what
// makes "validate then encode" safe to read as one step.
//
// It also holds the one CROSS-member rule the record carries: a claim and an
// attempt that are both present must name the SAME residency. That equality is
// what every writer here already produces — BeginDispositionAttempt admits an
// attempt only at the claim's own residency and then copies it, and no
// transition rewrites either — and settlement's high-water fence leans on it,
// because fencing against the claim is interchangeable with fencing against the
// attempt only while the two agree. Until now nothing enforced it, so a record
// whose two residencies disagreed decoded cleanly and made the two spellings of
// that fence mean different things.
func validateDispositionState(r *DispositionInboxRecord) error {
	if r.Claim != nil {
		claim := *r.Claim
		if claim.ResidencyEpoch == 0 || !rankableTime(claim.ExpiresAt) {
			return inboxInvalid("claim", nil)
		}
		r.Claim = &claim
	}
	if r.Attempt != nil {
		attempt := *r.Attempt
		if err := validateDispositionAttempt(&attempt); err != nil {
			return err
		}
		r.Attempt = &attempt
	}
	if r.Outcome != nil {
		outcome := *r.Outcome
		if err := validateDispositionOutcome(outcome, r.Attempt, r.State); err != nil {
			return err
		}
		r.Outcome = &outcome
	}
	// The claim and the attempt name the SAME residency whenever both are
	// present.
	//
	// Read this as a constraint on code NOT YET WRITTEN, because that is the
	// load-bearing half. There is no in-package writer of a DispositionClaim at
	// all — claims enter only through the wire decoder — so saying "every writer
	// here already produces the equality" understates it. Any future claim edge
	// that raises a claim's residency over a stored attempt now makes that record
	// UNENCODABLE, and will have to clear the attempt or move both members
	// together. That is the intended constraint and it fails closed, but it
	// belongs written down here rather than discovered as an encode refusal by
	// whoever builds that edge.
	//
	// This is a NARROWING of the codec and it is being made while it is
	// still free: released v0.5.0's canonicalDispositionInboxRecord refuses any
	// state but pending on BOTH the encode and the decode path, and a pending
	// record carries neither member, so the rule is vacuous for every record any
	// released binary could have stored. It cannot invalidate one. After a tag
	// that ships the post-admission states this argument expires, which is why
	// the check lands here rather than later.
	if r.Claim != nil && r.Attempt != nil && r.Attempt.ResidencyEpoch != r.Claim.ResidencyEpoch {
		return inboxInvalid("attempt.residency_epoch", nil)
	}
	switch r.State {
	case InboxStatePending:
		if r.Claim != nil || r.Attempt != nil {
			return inboxInvalid("state", nil)
		}
	case InboxStateClaimed:
		if r.Claim == nil || r.Attempt != nil {
			return inboxInvalid("state", nil)
		}
	case InboxStateApplying:
		if r.Claim == nil || r.Attempt == nil {
			return inboxInvalid("state", nil)
		}
	case InboxStateApplied, InboxStateRejected:
		// A terminal record that WAS dispatched keeps the claim and the attempt
		// that produced it: they are the durable record of which grants applied
		// the command, and a settlement that cleared them would leave its own
		// outcome unkeyed.
		//
		// A rejection may also PRECEDE any dispatch. The protocol permits a
		// pending or claimed command to be rejected only while no attempt has
		// been durably authorized, so such a record has no attempt by
		// definition, has no claim at all when it was still pending, and can
		// carry no outcome — every outcome here is keyed by the attempt it
		// settles. This case exists so that shape is STORABLE. The transition
		// that writes one is NOT implemented and admission still starts every
		// record pending; a validator that accepts more never invalidates a
		// stored record, which is why widening it is free before the shape has
		// a producer and noisy afterwards.
		//
		// Only a rejection may take that shape. Applied always means a runtime
		// accepted the command under a grant, so it always carries the attempt
		// that named the grant. The cost of the wider rule is stated plainly:
		// bytes whose state member alone reads "rejected" are now a valid
		// tombstone rather than a decode failure, which is one fewer accidental
		// corruption tripwire on a state no producer writes yet.
		//
		// The `r.Outcome != nil` arm below is UNREACHABLE and is kept as a
		// belt-and-braces restatement, not as the line that decides. An outcome
		// beside no attempt is already refused above by validateDispositionOutcome,
		// which returns field "outcome.attempt_id" the moment attempt == nil — so
		// an attemptless rejection carrying an outcome never reaches this switch,
		// and the tests for that shape assert THAT field rather than "state". If
		// the outcome validation is ever moved after the switch, this arm becomes
		// the one that answers, which is the only reason it is still written.
		if r.Attempt == nil {
			if r.State != InboxStateRejected || r.Outcome != nil {
				return inboxInvalid("state", nil)
			}
			return nil
		}
		if r.Claim == nil || r.Outcome == nil {
			return inboxInvalid("state", nil)
		}
		return nil
	default:
		return inboxInvalid("state", nil)
	}
	if r.Outcome != nil {
		return inboxInvalid("outcome", nil)
	}
	return nil
}

// validateDispositionAttempt holds an attempt to its own well-formedness. Both
// grants are required and neither may be zero: an attempt that recorded one
// authority could not tell a successor which had been held, and a zero epoch is
// not a grant any provider issues.
func validateDispositionAttempt(a *DispositionAttempt) error {
	if err := validateOpaque(string(a.AttemptID), "attempt_id", inboxInvalid); err != nil {
		return err
	}
	if a.JournalEpoch == 0 {
		return inboxInvalid("journal_epoch", nil)
	}
	if a.ResidencyEpoch == 0 {
		return inboxInvalid("residency_epoch", nil)
	}
	if !rankableTime(a.StartedAt) {
		return inboxInvalid("started_at", nil)
	}
	return nil
}

// validateDispositionOutcome holds a terminal outcome to the attempt it claims
// to settle and to the state it settles into. The evidence rules themselves
// live with the settlement that verifies them; what is restated here is only
// what a STORED record must satisfy on its own, so a decoded outcome cannot
// name another attempt, another grant, or a state its kind does not produce.
func validateDispositionOutcome(o DispositionOutcome, attempt *DispositionAttempt, state InboxState) error {
	if !o.Kind.valid() || o.Kind.terminalState() != state {
		return inboxInvalid("outcome.kind", nil)
	}
	if attempt == nil || o.AttemptID != attempt.AttemptID || o.AttemptJournalEpoch != attempt.JournalEpoch {
		return inboxInvalid("outcome.attempt_id", nil)
	}
	if o.AuthorJournalEpoch == 0 || o.DispositionSeq == 0 || o.SettlingResidencyEpoch == 0 {
		return inboxInvalid("outcome", nil)
	}
	if !rankableTime(o.SettledAt) {
		return inboxInvalid("outcome.settled_at", nil)
	}
	if o.Kind == DispositionNotApplied {
		if o.AuthorJournalEpoch <= o.AttemptJournalEpoch || o.AuthorFenceSeq == 0 || o.AuthorFenceSeq >= o.DispositionSeq {
			return inboxInvalid("outcome.author_journal_epoch", nil)
		}
	} else if o.AuthorJournalEpoch != o.AttemptJournalEpoch || o.AuthorFenceSeq != 0 {
		return inboxInvalid("outcome.author_journal_epoch", nil)
	}
	if o.Kind == DispositionApplied {
		if err := o.EventID.Validate(); err != nil {
			return inboxInvalid("outcome.event_id", err)
		}
		if o.EventSeq != o.DispositionSeq {
			return inboxInvalid("outcome.event_seq", nil)
		}
		return nil
	}
	if o.EventID != "" || o.EventSeq != 0 {
		return inboxInvalid("outcome.event", nil)
	}
	return nil
}
