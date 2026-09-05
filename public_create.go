package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// PublicCreateReservationVersion is the immutable tenant-wide reservation codec.
const PublicCreateReservationVersion uint8 = 1

// CatalogPublicCreateRecordVersion retains immutable create identity separately
// from mutable desired state. Older catalog decoders reject this version.
const CatalogPublicCreateRecordVersion uint8 = 3

const publicCreateNamespace = "sessionstore/public-create"

// PublicCreateIdentity binds a public create command to one immutable launch.
// PayloadDigest is lowercase SHA-256 hex; size/digest identify content, not an
// upload generation. Kind remains opaque; the dedicated API marks public create.
type PublicCreateIdentity struct {
	TenantID      sessionwire.TenantID  `json:"tenant_id"`
	SessionID     sessionwire.SessionID `json:"session_id"`
	CommandID     sessionwire.CommandID `json:"command_id"`
	Target        HostTargetKey         `json:"target"`
	Binding       SessionBinding        `json:"binding"`
	Kind          CommandKind           `json:"kind"`
	PayloadDigest string                `json:"payload_digest"`
	PayloadSize   uint64                `json:"payload_size"`
}

// PublicCreateReservation is a durable proposal winner, NOT an accepted command.
// It can outlive a lost catalog race or a missing payload. Runtime mapping,
// times and InitialWorkload are first-writer proposals, never retry comparisons.
type PublicCreateReservation struct {
	Identity         PublicCreateIdentity `json:"identity"`
	RuntimeCommandID RuntimeCommandID     `json:"runtime_command_id"`
	AcceptedAt       time.Time            `json:"accepted_at"`
	ApplyDeadline    time.Time            `json:"apply_deadline"`
	InitialWorkload  DesiredWorkload      `json:"initial_workload"`
}

// PreparePublicCreateRequest reserves identity before payload bytes need exist.
// An identical retry returns the original mapping, times and initial workload.
type PreparePublicCreateRequest struct {
	Identity                 PublicCreateIdentity
	ProposedRuntimeCommandID RuntimeCommandID
	AcceptedAt               time.Time
	ApplyDeadline            time.Time
	InitialWorkload          DesiredWorkload
}

// PublicCreatePreparation proves matching reservation/catalog only. It is not
// an ACK, a terminal outcome, or execution authority. Upload oversized bytes with
// PutCommandPayload, then call AdmitPublicCreate. A crash may require resending
// bytes; there is no background recovery, body reconstruction or orphan reaping.
type PublicCreatePreparation struct {
	Reservation PublicCreateReservation
	Catalog     CatalogEntry
}

// AdmitPublicCreateRequest completes prepared admission with verified content.
// Inline and independently uploaded object representations compare by content;
// retries return the exact winning inbox representation and acceptance order.
type AdmitPublicCreateRequest struct {
	Identity      PublicCreateIdentity
	Payload       []byte
	PayloadObject *sessionwire.ObjectMetadata
}

type publicCreateWire struct {
	RecordVersion uint8 `json:"record_version"`
	PublicCreateReservation
}

type catalogPublicCreateWire struct {
	catalogBindingWire
	PublicCreate PublicCreateReservation `json:"public_create"`
}

func validatePublicCreateIdentity(i PublicCreateIdentity) error {
	for _, check := range []struct {
		name string
		err  error
	}{
		{"tenant_id", i.TenantID.Validate()}, {"session_id", i.SessionID.Validate()},
		{"command_id", i.CommandID.Validate()}, {"agent_id", i.Target.AgentID.Validate()},
	} {
		if check.err != nil {
			return inboxInvalid(check.name, check.err)
		}
	}
	if err := validateOpaque(i.Target.RuntimeCompatibilityID, "runtime_compatibility_id", inboxInvalid); err != nil {
		return err
	}
	if err := validateDesiredPlacement(i.Target.Placement); err != nil {
		return err
	}
	if err := validateOpaque(string(i.Kind), "kind", inboxInvalid); err != nil {
		return err
	}
	if err := i.Binding.validate(); err != nil {
		return err
	}
	if i.Binding.ProtocolMode != ProtocolModeDisposition {
		return inboxInvalid("binding.protocol_mode", nil)
	}
	if len(i.PayloadDigest) != 64 {
		return inboxInvalid("payload_digest", nil)
	}
	d, err := hex.DecodeString(i.PayloadDigest)
	if err != nil || len(d) != 32 || hex.EncodeToString(d) != i.PayloadDigest || bytes.Equal(d, make([]byte, 32)) {
		return inboxInvalid("payload_digest", nil)
	}
	if i.PayloadSize == 0 {
		empty := sha256.Sum256(nil)
		if i.PayloadDigest != hex.EncodeToString(empty[:]) {
			return inboxInvalid("payload_digest", nil)
		}
	}
	return nil
}

func canonicalPublicCreate(r PublicCreateReservation) (PublicCreateReservation, error) {
	if err := validatePublicCreateIdentity(r.Identity); err != nil {
		return PublicCreateReservation{}, err
	}
	i := r.Identity
	base, err := canonicalInboxRecord(InboxRecord{TenantID: i.TenantID, SessionID: i.SessionID, CommandID: i.CommandID, RuntimeCommandID: r.RuntimeCommandID, Kind: i.Kind, AcceptedAt: r.AcceptedAt, ApplyDeadline: r.ApplyDeadline, State: InboxStatePending})
	if err != nil {
		return PublicCreateReservation{}, err
	}
	r.AcceptedAt, r.ApplyDeadline = base.AcceptedAt, base.ApplyDeadline
	r.InitialWorkload, err = canonicalDesiredWorkload(r.InitialWorkload)
	return r, err
}

func encodePublicCreate(r PublicCreateReservation) ([]byte, PublicCreateReservation, error) {
	r, err := canonicalPublicCreate(r)
	if err != nil {
		return nil, PublicCreateReservation{}, err
	}
	v, err := json.Marshal(publicCreateWire{PublicCreateReservationVersion, r})
	if err != nil {
		return nil, PublicCreateReservation{}, inboxInvalid("reservation", err)
	}
	if len(v) > MaxCatalogRecordBytes {
		return nil, PublicCreateReservation{}, inboxErr(InboxErrorTooLarge, "reservation", nil)
	}
	return v, r, nil
}

func decodePublicCreate(v []byte) (PublicCreateReservation, error) {
	w, err := decodeVersionedRecord[publicCreateWire](v, MaxCatalogRecordBytes, PublicCreateReservationVersion, versionedRecordFields{Record: "reservation", Version: "record_version"}, inboxRecordFailure)
	if err != nil {
		return PublicCreateReservation{}, err
	}
	canonical, r, err := encodePublicCreate(w.PublicCreateReservation)
	if err != nil {
		return PublicCreateReservation{}, err
	}
	if !bytes.Equal(canonical, v) {
		return PublicCreateReservation{}, inboxErr(InboxErrorMalformed, "reservation", nil)
	}
	return r, nil
}

func publicCreateID(scope sessionScope, command sessionwire.CommandID) storage.OrderedID {
	return storage.OrderedID{Namespace: publicCreateNamespace, OrderingScope: scope.TenantNamespace, StableKey: storage.StableKey(command)}
}

func publicCreateWinner(stored storage.OrderedRecord, scope sessionScope, identity PublicCreateIdentity) (PublicCreateReservation, error) {
	if stored.Deleted {
		return PublicCreateReservation{}, inboxErr(InboxErrorDeleted, "reservation", nil)
	}
	r, err := decodePublicCreate(stored.Value)
	if err != nil {
		return PublicCreateReservation{}, err
	}
	if stored.ID != publicCreateID(scope, identity.CommandID) || stored.Order == 0 || stored.Revision != 1 || r.Identity.TenantID != identity.TenantID || r.Identity.CommandID != identity.CommandID {
		return PublicCreateReservation{}, inboxErr(InboxErrorIdentity, "reservation", nil)
	}
	if err := checkFiledScope(stored, scope.TenantNamespace, storage.Due{}, inboxIdentity); err != nil {
		return PublicCreateReservation{}, err
	}
	if r.Identity != identity {
		return PublicCreateReservation{}, inboxErr(InboxErrorCommandMismatch, "reservation", nil)
	}
	return r, nil
}

func publicCreateCatalogRequest(r PublicCreateReservation) CreateCatalogEntryRequest {
	i := r.Identity
	return CreateCatalogEntryRequest{TenantID: i.TenantID, SessionID: i.SessionID, AgentID: i.Target.AgentID, Binding: i.Binding, RuntimeCompatibilityID: i.Target.RuntimeCompatibilityID, CreatedAt: r.AcceptedAt, LastActiveAt: r.AcceptedAt, State: sessionwire.SessionStateIdle, Residency: sessionwire.SessionResidencyCold, DesiredPlacement: i.Target.Placement, DesiredWorkload: r.InitialWorkload, IdempotencyKey: string(i.CommandID)}
}

func samePublicCreate(a, b *PublicCreateReservation) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	av, _, ae := encodePublicCreate(*a)
	bv, _, be := encodePublicCreate(*b)
	return ae == nil && be == nil && bytes.Equal(av, bv)
}

// PreparePublicCreate uses a fixed sequence of exact operations. A failed or
// ambiguous write returns an error and no preparation; retry the same identity.
// A reservation that loses the catalog race remains reserved, never accepted.
func (s *Store) PreparePublicCreate(ctx context.Context, req PreparePublicCreateRequest) (PublicCreatePreparation, error) {
	scope, err := s.deriveSessionScope(req.Identity.TenantID, req.Identity.SessionID)
	if err != nil {
		return PublicCreatePreparation{}, err
	}
	v, r, err := encodePublicCreate(PublicCreateReservation{Identity: req.Identity, RuntimeCommandID: req.ProposedRuntimeCommandID, AcceptedAt: req.AcceptedAt, ApplyDeadline: req.ApplyDeadline, InitialWorkload: req.InitialWorkload})
	if err != nil {
		return PublicCreatePreparation{}, err
	}
	// Validate the exact initial catalog projection before any witness write.
	if _, err := encodeCatalogRecord(publicCreateCatalogRecord(r)); err != nil {
		return PublicCreatePreparation{}, err
	}
	if scope.layout != layoutTenantV1 {
		return PublicCreatePreparation{}, inboxInvalid("layout", nil)
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return PublicCreatePreparation{}, err
	}
	defer release()
	if err := s.bindSessionScopeMode(opCtx, scope, ProtocolModeDisposition); err != nil {
		return PublicCreatePreparation{}, err
	}
	stored, created, err := s.backend.OrderedIndex.Create(opCtx, publicCreateID(scope, req.Identity.CommandID), scope.TenantNamespace, v, storage.Rank{}, storage.Due{})
	if err != nil {
		return PublicCreatePreparation{}, classifyInboxOrderedError(err, "reservation")
	}
	if created && !bytes.Equal(v, stored.Value) {
		return PublicCreatePreparation{}, inboxErr(InboxErrorIdentity, "reservation", nil)
	}
	r, err = publicCreateWinner(stored, scope, req.Identity)
	if err != nil {
		return PublicCreatePreparation{}, err
	}
	entry, _, err := s.createCatalogEntry(opCtx, publicCreateCatalogRequest(r), &r)
	if err != nil {
		return PublicCreatePreparation{}, err
	}
	if err := opCtx.Err(); err != nil {
		return PublicCreatePreparation{}, classifyInboxOrderedError(err, "prepare")
	}
	return PublicCreatePreparation{Reservation: r, Catalog: entry}, nil
}

func publicCreateCatalogRecord(r PublicCreateReservation) CatalogRecord {
	record := catalogRecordForCreate(publicCreateCatalogRequest(r))
	record.PublicCreate = &r
	return record
}

// AdmitPublicCreate acknowledges only matching reservation, immutable catalog
// create identity and marked disposition inbox winner. It does not prepare
// missing catalogs or reconstruct missing payload bytes.
func (s *Store) AdmitPublicCreate(ctx context.Context, req AdmitPublicCreateRequest) (DispositionInboxEntry, bool, error) {
	i := req.Identity
	if err := validatePublicCreateIdentity(i); err != nil {
		return DispositionInboxEntry{}, false, err
	}
	scope, err := s.deriveSessionScope(i.TenantID, i.SessionID)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	// Validate payload shape and declared identity before provider I/O.
	d, err := dispositionDescriptor(AdmitDispositionCommandRequest{TenantID: i.TenantID, SessionID: i.SessionID, CommandID: i.CommandID, Binding: i.Binding, Kind: i.Kind, Payload: req.Payload, PayloadObject: req.PayloadObject})
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	if d.PayloadDigest != i.PayloadDigest || d.PayloadSize != i.PayloadSize {
		return DispositionInboxEntry{}, false, inboxErr(InboxErrorCommandMismatch, "payload", nil)
	}
	if len(req.Payload) > MaxInboxPayloadBytes || (req.PayloadObject != nil && len(req.Payload) != 0) {
		return DispositionInboxEntry{}, false, inboxInvalid("payload", nil)
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	defer release()
	if err := s.verifySessionScope(opCtx, scope); err != nil {
		return DispositionInboxEntry{}, false, err
	}
	stored, err := s.backend.OrderedIndex.Get(opCtx, publicCreateID(scope, i.CommandID))
	if err != nil {
		return DispositionInboxEntry{}, false, classifyInboxOrderedError(err, "reservation")
	}
	r, err := publicCreateWinner(stored, scope, i)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	entry, created, err := s.admitDispositionCommand(opCtx, AdmitDispositionCommandRequest{TenantID: i.TenantID, SessionID: i.SessionID, CommandID: i.CommandID, Binding: i.Binding, ProposedRuntimeCommandID: r.RuntimeCommandID, Kind: i.Kind, Payload: req.Payload, PayloadObject: req.PayloadObject, AcceptedAt: r.AcceptedAt, ApplyDeadline: r.ApplyDeadline}, &r)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	if err := opCtx.Err(); err != nil {
		return DispositionInboxEntry{}, false, classifyInboxOrderedError(err, "admit")
	}
	return entry, created, nil
}
