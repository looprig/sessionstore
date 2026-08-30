package sessionstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// catalogNamespace is the one OrderedIndex namespace holding session catalog
// records. Tenants are separated by the ordering and ranking scope, not by the
// namespace, so a tenant-scoped recent-first page is a single provider query.
const catalogNamespace = "sessionstore/catalog"

const (
	// CatalogRecordVersion is the independent version of the stored catalog
	// record. A reader fails closed on any other version rather than guessing
	// which members a future encoder meant.
	CatalogRecordVersion uint8 = 1

	// MaxCatalogOpenGates bounds the open-gate projections one catalog record
	// carries. The catalog is a replay-free status projection, not a gate
	// store: a session with more simultaneously open gates than this is read
	// through the gate API instead.
	MaxCatalogOpenGates = 16

	// MaxCatalogRecordBytes bounds an encoded catalog record. It is well below
	// storage.MaxOrderedValueBytes so a record that this package accepts always
	// fits in the provider, leaving no state that can be written but not
	// rewritten.
	MaxCatalogRecordBytes = 256 << 10
)

// The relationship above is the reason classifyCatalogOrderedError has no arm
// for storage.OrderedValueTooLargeError: this package refuses an oversized
// record before the provider can. Prose cannot enforce that, so state it as an
// unsigned constant that fails to compile if the catalog bound ever exceeds the
// provider's.
const _ = uint(storage.MaxOrderedValueBytes - MaxCatalogRecordBytes)

// minRankableTime and maxRankableTime bound the instants whose UnixNano is
// defined. The catalog ranks by LastActiveAt.UnixNano(), and time.Time.UnixNano
// is documented as undefined outside this range, so a timestamp beyond it would
// silently produce a wrapped rank rather than a late one.
var (
	minRankableTime = time.Unix(0, math.MinInt64).UTC()
	maxRankableTime = time.Unix(0, math.MaxInt64).UTC()

	timeType = reflect.TypeOf(time.Time{})
)

// CheckpointSummary is the bounded durable description of the active workspace
// checkpoint. Its zero value means no checkpoint has been committed. It names a
// logical Core object reference, never a provider key or signed URL.
type CheckpointSummary struct {
	JournalSeq uint64
	Reference  sessionwire.ObjectReference
	CapturedAt time.Time
}

func (c CheckpointSummary) isZero() bool {
	return c.JournalSeq == 0 && c.Reference.ObjectID == "" && c.CapturedAt.IsZero()
}

// CatalogRecord is the neutral, replay-free durable projection of one session.
//
// Its fields have two different owners, and the difference is enforced rather
// than documented. LeaseEpoch, State, Residency, LastActiveAt, the journal
// summary, the checkpoint summary, and the open-gate projections are written by
// the Host that holds the session's lease, and a write naming an epoch below the
// committed high-water mark is refused. DesiredPlacement,
// RuntimeCompatibilityID, and DesiredIdempotencyKey are Factory-authored desired
// state, guarded by revision compare-and-swap and an idempotency key; Factory
// never names a lease epoch, so it cannot claim ownership it does not have.
type CatalogRecord struct {
	TenantID               sessionwire.TenantID
	SessionID              sessionwire.SessionID
	AgentID                sessionwire.AgentID
	RuntimeCompatibilityID string

	CreatedAt    time.Time
	LastActiveAt time.Time

	State            sessionwire.SessionState
	Residency        sessionwire.SessionResidency
	DesiredPlacement sessionwire.HostPlacement

	LastJournalSeq uint64
	LastEventID    sessionwire.EventID
	Checkpoint     CheckpointSummary
	OpenGates      []sessionwire.GateProjection

	LeaseEpoch            uint64
	DesiredIdempotencyKey string
}

// CatalogEntry is a catalog record together with the revision a caller passes
// to a subsequent Factory-owned compare-and-swap. The provider's immutable
// order is deliberately not exposed: it is sparse and scope-relative, so no
// caller can correctly infer a position or a count from it.
type CatalogEntry struct {
	Record   CatalogRecord
	Revision uint64
}

// Summary projects the record into Core's recent-first list shape.
func (r CatalogRecord) Summary() (sessionwire.SessionSummary, error) {
	r, err := canonicalCatalogRecord(r)
	if err != nil {
		return sessionwire.SessionSummary{}, err
	}
	summary := sessionwire.SessionSummary{
		SessionID:    r.SessionID,
		AgentID:      r.AgentID,
		State:        r.State,
		CreatedAt:    r.CreatedAt.UTC(),
		LastActiveAt: r.LastActiveAt.UTC(),
	}
	if err := summary.Validate(); err != nil {
		return sessionwire.SessionSummary{}, catalogErr(CatalogErrorInvalid, "summary", err)
	}
	return summary, nil
}

// Status projects the record into Core's replay-free status shape. WaitingGateID
// is the first open gate in the record's canonical (opened_seq, gate_id) order,
// so two readers of the same record always name the same gate. Canonicalizing
// here rather than assuming a canonical caller keeps the gate comparator stated
// exactly once; a second defensive sort in this method is precisely the kind of
// restatement that later drifts.
func (r CatalogRecord) Status() (sessionwire.SessionStatus, error) {
	r, err := canonicalCatalogRecord(r)
	if err != nil {
		return sessionwire.SessionStatus{}, err
	}
	status := sessionwire.SessionStatus{
		SessionID:  r.SessionID,
		AgentID:    r.AgentID,
		State:      r.State,
		Residency:  r.Residency,
		JournalTip: r.LastJournalSeq,
		UpdatedAt:  r.LastActiveAt.UTC(),
	}
	if len(r.OpenGates) > 0 {
		status.WaitingGateID = r.OpenGates[0].GateID
	}
	if err := status.Validate(); err != nil {
		return sessionwire.SessionStatus{}, catalogErr(CatalogErrorInvalid, "status", err)
	}
	return status, nil
}

// CreateCatalogEntryRequest creates the authoritative record for one session.
// It is idempotent by (TenantID, SessionID): a repeat returns the stored record
// unchanged with created false.
type CreateCatalogEntryRequest struct {
	TenantID               sessionwire.TenantID
	SessionID              sessionwire.SessionID
	AgentID                sessionwire.AgentID
	RuntimeCompatibilityID string
	CreatedAt              time.Time
	LastActiveAt           time.Time
	State                  sessionwire.SessionState
	Residency              sessionwire.SessionResidency
	DesiredPlacement       sessionwire.HostPlacement
	IdempotencyKey         string
}

// GetCatalogEntryRequest reads one session's authoritative catalog record.
type GetCatalogEntryRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
}

// UpdateCatalogHostStateRequest writes the fields owned by the Host holding the
// session's journal lease. LeaseEpoch is that grant's epoch and is compared
// against the record's committed high-water mark.
//
// Every field here REPLACES its stored counterpart; nothing is merged. A write
// that omits Checkpoint zeroes the stored checkpoint summary, and a write that
// omits OpenGates clears the stored gate projections. That is deliberate and is
// why the catalog holds a *summary*: the authoritative, retained high-water
// checkpoint pointer is a separate epoch-fenced record, so clearing a summary
// here loses no durable state. Callers therefore send the complete current
// projection on every write rather than a delta.
//
// LastJournalSeq is the one exception, and its asymmetry is intentional: the
// journal is append-only and a successor fence commits above its predecessor's
// tip, so a durable sequence never moves backwards and a regressing one is
// refused rather than stored.
type UpdateCatalogHostStateRequest struct {
	TenantID       sessionwire.TenantID
	SessionID      sessionwire.SessionID
	LeaseEpoch     uint64
	State          sessionwire.SessionState
	Residency      sessionwire.SessionResidency
	LastActiveAt   time.Time
	LastJournalSeq uint64
	LastEventID    sessionwire.EventID
	Checkpoint     CheckpointSummary
	OpenGates      []sessionwire.GateProjection
}

// UpdateCatalogDesiredStateRequest writes Factory-authored desired state. It
// deliberately has no lease epoch member: desired state is guarded by revision
// compare-and-swap plus a retry-stable idempotency key, because Factory does not
// hold the Host's lease and must not be able to spell a claim on it.
//
// IdempotencyKey names the INTENT, and exactly one key is retained. A request
// whose key equals the retained one is treated as a replay of that intent and
// returns the stored record unchanged with a nil error — including when the
// rest of the request differs. Reusing a key for a NEW intent therefore
// succeeds without applying anything, so a caller must mint a fresh key per
// distinct desired state rather than per retry batch.
type UpdateCatalogDesiredStateRequest struct {
	TenantID               sessionwire.TenantID
	SessionID              sessionwire.SessionID
	ExpectedRevision       uint64
	IdempotencyKey         string
	DesiredPlacement       sessionwire.HostPlacement
	RuntimeCompatibilityID string
}

// CreateCatalogEntry binds the session's collision witnesses and creates its one
// authoritative ordered record. A duplicate identity returns the canonical
// stored record with created false and never overwrites it.
func (s *Store) CreateCatalogEntry(ctx context.Context, req CreateCatalogEntryRequest) (CatalogEntry, bool, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return CatalogEntry{}, false, err
	}
	record := CatalogRecord{
		TenantID:               req.TenantID,
		SessionID:              req.SessionID,
		AgentID:                req.AgentID,
		RuntimeCompatibilityID: req.RuntimeCompatibilityID,
		CreatedAt:              req.CreatedAt,
		LastActiveAt:           req.LastActiveAt,
		State:                  req.State,
		Residency:              req.Residency,
		DesiredPlacement:       req.DesiredPlacement,
		DesiredIdempotencyKey:  req.IdempotencyKey,
	}
	value, err := encodeCatalogRecord(record)
	if err != nil {
		return CatalogEntry{}, false, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return CatalogEntry{}, false, err
	}
	defer release()
	if err := s.bindSessionScope(opCtx, scope); err != nil {
		return CatalogEntry{}, false, err
	}
	stored, created, err := s.backend.OrderedIndex.Create(
		opCtx, catalogID(scope, req.SessionID), scope.CatalogScope, value, catalogRank(record), storage.Due{})
	if err != nil {
		return CatalogEntry{}, false, classifyCatalogOrderedError(err, "create")
	}
	entry, err := catalogEntry(stored, req.TenantID, req.SessionID)
	return entry, created, err
}

// GetCatalogEntry reads one session's record directly by identity. It verifies
// the session's collision witnesses before any provider read, so a derived name
// is never trusted on its own.
func (s *Store) GetCatalogEntry(ctx context.Context, req GetCatalogEntryRequest) (CatalogEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return CatalogEntry{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return CatalogEntry{}, err
	}
	defer release()
	return s.readCatalogEntry(opCtx, scope, req.TenantID, req.SessionID)
}

// UpdateCatalogHostState applies Host-owned fields under the record's fencing
// epoch.
//
// The stored LeaseEpoch is a high-water mark, not a lock: a request naming a
// lower epoch is refused outright, and an equal one is admitted because one
// grant legitimately writes many times. The read-compare-write is closed by the
// revision compare-and-swap below, so a request that observed a stale epoch
// cannot land after a successor's write — it loses the CAS and, on re-read,
// meets the successor's epoch.
func (s *Store) UpdateCatalogHostState(ctx context.Context, req UpdateCatalogHostStateRequest) (CatalogEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return CatalogEntry{}, err
	}
	if req.LeaseEpoch == 0 {
		return CatalogEntry{}, catalogErr(CatalogErrorInvalid, "lease_epoch", nil)
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return CatalogEntry{}, err
	}
	defer release()

	current, err := s.readCatalogEntry(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return CatalogEntry{}, err
	}
	if req.LeaseEpoch < current.Record.LeaseEpoch {
		return CatalogEntry{}, &CatalogError{Code: CatalogErrorEpoch, Field: "lease_epoch", Epoch: current.Record.LeaseEpoch}
	}
	// The journal is append-only and its successor fence commits above the
	// predecessor's tip, so a durable sequence never moves backwards. Accepting
	// one would publish a tip that no reader can resume from.
	if req.LastJournalSeq < current.Record.LastJournalSeq {
		return CatalogEntry{}, catalogErr(CatalogErrorSequence, "last_journal_seq", nil)
	}
	next := current.Record
	next.LeaseEpoch = req.LeaseEpoch
	next.State = req.State
	next.Residency = req.Residency
	next.LastActiveAt = req.LastActiveAt
	next.LastJournalSeq = req.LastJournalSeq
	next.LastEventID = req.LastEventID
	next.Checkpoint = req.Checkpoint
	next.OpenGates = req.OpenGates
	return s.writeCatalogRecord(opCtx, scope, next, current.Revision)
}

// UpdateCatalogDesiredState applies Factory-authored desired state.
//
// IdempotencyKey is checked before the revision, and the order is the contract:
// a retry of an already-applied write carries an expected revision that its own
// success invalidated, so comparing the revision first would reject exactly the
// requests idempotency exists to absorb.
func (s *Store) UpdateCatalogDesiredState(ctx context.Context, req UpdateCatalogDesiredStateRequest) (CatalogEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return CatalogEntry{}, err
	}
	if req.IdempotencyKey == "" {
		return CatalogEntry{}, catalogErr(CatalogErrorInvalid, "idempotency_key", nil)
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return CatalogEntry{}, err
	}
	defer release()

	current, err := s.readCatalogEntry(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return CatalogEntry{}, err
	}
	if current.Record.DesiredIdempotencyKey == req.IdempotencyKey {
		return current, nil
	}
	if req.ExpectedRevision != current.Revision {
		return CatalogEntry{}, &CatalogError{Code: CatalogErrorConflict, Field: "expected_revision", Revision: current.Revision}
	}
	next := current.Record
	next.DesiredPlacement = req.DesiredPlacement
	next.RuntimeCompatibilityID = req.RuntimeCompatibilityID
	next.DesiredIdempotencyKey = req.IdempotencyKey
	return s.writeCatalogRecord(opCtx, scope, next, current.Revision)
}

// readCatalogEntry verifies the session's witnesses and returns its current
// decoded record. Both update paths funnel through it so the binding check and
// the stored-identity check are stated exactly once.
func (s *Store) readCatalogEntry(
	ctx context.Context,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (CatalogEntry, error) {
	if err := s.verifySessionScope(ctx, scope); err != nil {
		return CatalogEntry{}, err
	}
	stored, err := s.backend.OrderedIndex.Get(ctx, catalogID(scope, session))
	if err != nil {
		return CatalogEntry{}, classifyCatalogOrderedError(err, "get")
	}
	return catalogEntry(stored, tenant, session)
}

// writeCatalogRecord encodes and compare-and-swaps one record. Canonicalization
// and validation belong to encodeCatalogRecord and are deliberately not
// restated here: a second call would validate the same record twice and could
// later drift from the copy that actually decides the stored bytes.
//
// Rank is recomputed from the record being written, so a path that leaves
// LastActiveAt alone necessarily leaves the rank alone too.
func (s *Store) writeCatalogRecord(
	ctx context.Context,
	scope sessionScope,
	record CatalogRecord,
	expectedRevision uint64,
) (CatalogEntry, error) {
	value, err := encodeCatalogRecord(record)
	if err != nil {
		return CatalogEntry{}, err
	}
	stored, err := s.backend.OrderedIndex.Update(
		ctx, catalogID(scope, record.SessionID), expectedRevision, value, catalogRank(record), storage.Due{})
	if err != nil {
		return CatalogEntry{}, classifyCatalogOrderedError(err, "update")
	}
	return catalogEntry(stored, record.TenantID, record.SessionID)
}

// catalogID names the one ordered record per session. The StableKey is the raw
// SessionID: it is an opaque provider-verified value, not a name, and the
// provider is responsible for canonicalizing it for its own paths or subjects.
func catalogID(scope sessionScope, session sessionwire.SessionID) storage.OrderedID {
	return storage.OrderedID{
		Namespace:     catalogNamespace,
		OrderingScope: scope.CatalogScope,
		StableKey:     storage.StableKey(session),
	}
}

// catalogRank is the single definition of a catalog record's rank. Every write
// path calls it rather than restating the expression, so no path can drift into
// ranking by something other than recency.
//
// It is safe to call with a record that has not been canonicalized yet, which
// is what the write paths do: canonicalization's only change to LastActiveAt is
// .UTC(), which relabels the zone and preserves the instant, so the rank is the
// same either way. encodeCatalogRecord on the line above has already refused
// anything whose UnixNano is undefined.
func catalogRank(record CatalogRecord) storage.Rank {
	return storage.Rank{Ranked: true, Value: record.LastActiveAt.UnixNano()}
}

// catalogEntry decodes a stored record and holds it to the identity the caller
// asked for. A provider that hashes the stable key stores the original bytes for
// exactly this verification, and a tombstone is reported as deleted rather than
// decoded.
func catalogEntry(
	stored storage.OrderedRecord,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (CatalogEntry, error) {
	if stored.Deleted {
		return CatalogEntry{}, catalogErr(CatalogErrorDeleted, "record", nil)
	}
	record, err := decodeCatalogRecord(stored.Value)
	if err != nil {
		return CatalogEntry{}, err
	}
	if record.TenantID != tenant || record.SessionID != session {
		return CatalogEntry{}, catalogErr(CatalogErrorIdentity, "record", nil)
	}
	return CatalogEntry{Record: record, Revision: stored.Revision}, nil
}

// classifyCatalogOrderedError maps an OrderedIndex outcome into the catalog
// vocabulary while preserving the cause for errors.Is and errors.As.
func classifyCatalogOrderedError(err error, field string) error {
	var notFound *storage.OrderedRecordNotFoundError
	if errors.As(err, &notFound) {
		return catalogErr(CatalogErrorNotFound, field, err)
	}
	var deleted *storage.OrderedDeletedError
	if errors.As(err, &deleted) {
		return catalogErr(CatalogErrorDeleted, field, err)
	}
	var conflict *storage.OrderedRevisionConflictError
	if errors.As(err, &conflict) {
		return &CatalogError{Code: CatalogErrorConflict, Field: field, Revision: conflict.ActualRevision, Cause: err}
	}
	var ambiguous *storage.OrderedAmbiguousError
	if errors.As(err, &ambiguous) {
		return catalogErr(CatalogErrorUnknown, field, err)
	}
	return catalogErr(CatalogErrorBackend, field, err)
}

// catalogWire is the stored JSON shape. Every public projection member is a
// sessionwire/v1 type, so the durable record and the API a Factory reads cannot
// drift into two different vocabularies.
type catalogWire struct {
	RecordVersion          uint8                        `json:"record_version"`
	TenantID               sessionwire.TenantID         `json:"tenant_id"`
	SessionID              sessionwire.SessionID        `json:"session_id"`
	AgentID                sessionwire.AgentID          `json:"agent_id"`
	RuntimeCompatibilityID string                       `json:"runtime_compatibility_id,omitempty"`
	CreatedAt              time.Time                    `json:"created_at"`
	LastActiveAt           time.Time                    `json:"last_active_at"`
	State                  sessionwire.SessionState     `json:"state"`
	Residency              sessionwire.SessionResidency `json:"residency"`
	DesiredPlacement       sessionwire.HostPlacement    `json:"desired_placement"`
	LastJournalSeq         uint64                       `json:"last_journal_seq"`
	LastEventID            sessionwire.EventID          `json:"last_event_id,omitempty"`
	Checkpoint             *checkpointWire              `json:"checkpoint,omitempty"`
	OpenGates              []sessionwire.GateProjection `json:"open_gates,omitempty"`
	LeaseEpoch             uint64                       `json:"lease_epoch"`
	DesiredIdempotencyKey  string                       `json:"desired_idempotency_key,omitempty"`
}

type checkpointWire struct {
	JournalSeq uint64                      `json:"journal_seq"`
	Reference  sessionwire.ObjectReference `json:"reference"`
	CapturedAt time.Time                   `json:"captured_at"`
}

// encodeCatalogRecord validates and encodes a catalog record. It refuses a
// record above the catalog bound here rather than letting the provider refuse
// it, so a record this package accepted can always be rewritten.
func encodeCatalogRecord(record CatalogRecord) ([]byte, error) {
	record, err := canonicalCatalogRecord(record)
	if err != nil {
		return nil, err
	}
	wire := catalogWire{
		RecordVersion:          CatalogRecordVersion,
		TenantID:               record.TenantID,
		SessionID:              record.SessionID,
		AgentID:                record.AgentID,
		RuntimeCompatibilityID: record.RuntimeCompatibilityID,
		CreatedAt:              record.CreatedAt,
		LastActiveAt:           record.LastActiveAt,
		State:                  record.State,
		Residency:              record.Residency,
		DesiredPlacement:       record.DesiredPlacement,
		LastJournalSeq:         record.LastJournalSeq,
		LastEventID:            record.LastEventID,
		OpenGates:              record.OpenGates,
		LeaseEpoch:             record.LeaseEpoch,
		DesiredIdempotencyKey:  record.DesiredIdempotencyKey,
	}
	if !record.Checkpoint.isZero() {
		wire.Checkpoint = &checkpointWire{
			JournalSeq: record.Checkpoint.JournalSeq,
			Reference:  record.Checkpoint.Reference,
			CapturedAt: record.Checkpoint.CapturedAt,
		}
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return nil, catalogErr(CatalogErrorInvalid, "record", err)
	}
	if len(encoded) > MaxCatalogRecordBytes {
		return nil, catalogErr(CatalogErrorTooLarge, "record", nil)
	}
	return encoded, nil
}

// decodeCatalogRecord strictly decodes one stored catalog record. It checks the
// catalog bound before decoding, rejects an unknown record version, and
// re-validates the decoded record so a record corrupted in place cannot be
// handed to a caller.
//
// Strictness is a RECORD-level property and stops at the record's own members:
// an undeclared member of this record is refused outright. It deliberately does
// not reach inside the nested sessionwire projections, which are additive by
// design — core captures an unknown member of a GateProjection and re-emits it,
// so a future gate member survives a round trip through an older reader. The
// exceptions are core's own declared redaction boundaries, such as
// ObjectReference, which drop an undeclared member rather than proxy it.
func decodeCatalogRecord(value []byte) (CatalogRecord, error) {
	if len(value) > MaxCatalogRecordBytes {
		return CatalogRecord{}, catalogErr(CatalogErrorTooLarge, "record", nil)
	}
	if len(value) == 0 {
		return CatalogRecord{}, catalogErr(CatalogErrorMalformed, "record", nil)
	}
	// The version is read from a tolerant first pass so an unknown version is
	// reported as a version failure rather than as whichever member the strict
	// decoder happened to trip over first.
	//
	// This pass is also the package's one well-formedness rule: json.Unmarshal
	// requires the whole value to be exactly one JSON document, so trailing
	// content is rejected here. The streaming decoder below therefore never
	// needs a second trailing check — a decoder.More() call there could not
	// observe anything this has not already refused, and two statements of one
	// rule is how one of them later rots.
	var probe struct {
		RecordVersion uint8 `json:"record_version"`
	}
	if err := json.Unmarshal(value, &probe); err != nil {
		return CatalogRecord{}, catalogErr(CatalogErrorMalformed, "record", err)
	}
	if probe.RecordVersion != CatalogRecordVersion {
		return CatalogRecord{}, catalogErr(CatalogErrorVersion, "record_version", nil)
	}

	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var wire catalogWire
	if err := decoder.Decode(&wire); err != nil {
		return CatalogRecord{}, catalogErr(CatalogErrorMalformed, "record", err)
	}
	record := CatalogRecord{
		TenantID:               wire.TenantID,
		SessionID:              wire.SessionID,
		AgentID:                wire.AgentID,
		RuntimeCompatibilityID: wire.RuntimeCompatibilityID,
		CreatedAt:              wire.CreatedAt,
		LastActiveAt:           wire.LastActiveAt,
		State:                  wire.State,
		Residency:              wire.Residency,
		DesiredPlacement:       wire.DesiredPlacement,
		LastJournalSeq:         wire.LastJournalSeq,
		LastEventID:            wire.LastEventID,
		OpenGates:              wire.OpenGates,
		LeaseEpoch:             wire.LeaseEpoch,
		DesiredIdempotencyKey:  wire.DesiredIdempotencyKey,
	}
	if wire.Checkpoint != nil {
		record.Checkpoint = CheckpointSummary{
			JournalSeq: wire.Checkpoint.JournalSeq,
			Reference:  wire.Checkpoint.Reference,
			CapturedAt: wire.Checkpoint.CapturedAt,
		}
	}
	return canonicalCatalogRecord(record)
}

// canonicalCatalogRecord validates a record and returns its one canonical
// spelling: UTC timestamps, gates in (opened_seq, gate_id) order, and no empty
// gate slice. Encoding and decoding both end here, so a record read back is
// byte-identical to the record written and two encoders cannot disagree.
func canonicalCatalogRecord(record CatalogRecord) (CatalogRecord, error) {
	if err := record.TenantID.Validate(); err != nil {
		return CatalogRecord{}, catalogErr(CatalogErrorInvalid, "tenant_id", err)
	}
	if err := record.SessionID.Validate(); err != nil {
		return CatalogRecord{}, catalogErr(CatalogErrorInvalid, "session_id", err)
	}
	if err := record.AgentID.Validate(); err != nil {
		return CatalogRecord{}, catalogErr(CatalogErrorInvalid, "agent_id", err)
	}
	if err := validateOptionalOpaque(record.RuntimeCompatibilityID, "runtime_compatibility_id"); err != nil {
		return CatalogRecord{}, err
	}
	if err := validateOpaque(string(record.State), "state"); err != nil {
		return CatalogRecord{}, err
	}
	if err := validateOpaque(string(record.Residency), "residency"); err != nil {
		return CatalogRecord{}, err
	}
	switch record.DesiredPlacement {
	case sessionwire.HostPlacementPooled, sessionwire.HostPlacementDedicated:
	default:
		return CatalogRecord{}, catalogErr(CatalogErrorInvalid, "desired_placement", nil)
	}
	if !rankableTime(record.CreatedAt) {
		return CatalogRecord{}, catalogErr(CatalogErrorInvalid, "created_at", nil)
	}
	if !rankableTime(record.LastActiveAt) {
		return CatalogRecord{}, catalogErr(CatalogErrorInvalid, "last_active_at", nil)
	}
	if record.LastEventID != "" {
		if err := record.LastEventID.Validate(); err != nil {
			return CatalogRecord{}, catalogErr(CatalogErrorInvalid, "last_event_id", err)
		}
	}
	if err := validateOptionalOpaque(record.DesiredIdempotencyKey, "desired_idempotency_key"); err != nil {
		return CatalogRecord{}, err
	}
	if !record.Checkpoint.isZero() {
		if err := record.Checkpoint.Reference.Validate(); err != nil {
			return CatalogRecord{}, catalogErr(CatalogErrorInvalid, "checkpoint.reference", err)
		}
		if !rankableTime(record.Checkpoint.CapturedAt) {
			return CatalogRecord{}, catalogErr(CatalogErrorInvalid, "checkpoint.captured_at", nil)
		}
	}
	if len(record.OpenGates) > MaxCatalogOpenGates {
		return CatalogRecord{}, catalogErr(CatalogErrorInvalid, "open_gates", nil)
	}

	record.CreatedAt = record.CreatedAt.UTC()
	record.LastActiveAt = record.LastActiveAt.UTC()
	record.Checkpoint.CapturedAt = record.Checkpoint.CapturedAt.UTC()

	gates, err := canonicalGates(record.OpenGates)
	if err != nil {
		return CatalogRecord{}, err
	}
	record.OpenGates = gates
	return record, nil
}

// canonicalGates validates the open-gate projections and returns them in the
// record's canonical (opened_seq, gate_id) order.
//
// The empty case returns nil so an in-memory record has one spelling for "no
// open gates"; slices.Clone would otherwise preserve an empty-but-present
// slice. This is a normalization, not a guard: the encoded form omits an empty
// list either way, so no test can observe its absence and none pretends to.
func canonicalGates(open []sessionwire.GateProjection) ([]sessionwire.GateProjection, error) {
	if len(open) == 0 {
		return nil, nil
	}
	gates := slices.Clone(open)
	for i := range gates {
		if err := gates[i].Validate(); err != nil {
			return nil, catalogErr(CatalogErrorInvalid, "open_gates", err)
		}
		if err := validateProjectionText(reflect.ValueOf(gates[i]), "open_gates["+strconv.Itoa(i)+"]"); err != nil {
			return nil, err
		}
		gates[i].Deadline = gates[i].Deadline.UTC()
	}
	slices.SortFunc(gates, func(a, b sessionwire.GateProjection) int {
		if a.OpenedJournalSeq != b.OpenedJournalSeq {
			if a.OpenedJournalSeq < b.OpenedJournalSeq {
				return -1
			}
			return 1
		}
		return strings.Compare(string(a.GateID), string(b.GateID))
	})
	for i := 1; i < len(gates); i++ {
		if gates[i].GateID == gates[i-1].GateID {
			return nil, catalogErr(CatalogErrorInvalid, "open_gates", nil)
		}
	}
	return gates, nil
}

// validateOpaque bounds one required caller-chosen opaque value. Every such
// field shares it, so none can drift away from the others.
//
// Valid UTF-8 is a durability requirement here rather than decoration:
// json.Marshal silently substitutes U+FFFD for an invalid byte, so a value that
// skipped this check would be persisted as something other than what the caller
// wrote and read back as something the caller never supplied. Core's own
// projections cannot supply this rule — SessionSummary, SessionStatus, and
// GateProjection all accept any non-empty State, Residency, or Kind so a future
// wire version can add one — so this package is where these fields are bounded
// at all.
func validateOpaque(value, field string) error {
	if value == "" || len(value) > sessionwire.MaxIDBytes || !utf8.ValidString(value) {
		return catalogErr(CatalogErrorInvalid, field, nil)
	}
	return nil
}

// validateOptionalOpaque is validateOpaque for a field whose absence is legal.
func validateOptionalOpaque(value, field string) error {
	if value == "" {
		return nil
	}
	return validateOpaque(value, field)
}

// validateProjectionText rejects invalid UTF-8 anywhere inside a nested
// sessionwire projection, reporting the JSON path that carries it.
//
// It walks rather than naming members on purpose. Core validates a
// GateProjection's structure but deliberately not its text — prompt titles,
// bodies, origins, field names and labels, option values and labels, and
// control actions and labels are all caller-supplied and all unchecked — and
// that list is a moving target: core may add a member in a later wire version,
// and an enumeration here would silently stop covering the record the day it
// does. The record's own top-level members are each validated explicitly above
// and are deliberately NOT walked, so no field is checked twice.
//
// The walk terminates because the projection types are non-recursive: a
// GateProjection contains a prompt, which contains fixed-size slices of leaf
// structs, plus captured extension bytes.
func validateProjectionText(value reflect.Value, path string) error {
	switch value.Kind() {
	case reflect.String:
		if !utf8.ValidString(value.String()) {
			return catalogErr(CatalogErrorInvalid, path, nil)
		}
	case reflect.Pointer, reflect.Interface:
		if !value.IsNil() {
			return validateProjectionText(value.Elem(), path)
		}
	case reflect.Slice, reflect.Array:
		// A byte slice here is a json.RawMessage, and it is deliberately not
		// checked. It is re-emitted verbatim rather than re-encoded, so no
		// substitution can occur, and its text validity is already core's rule
		// on both paths that can carry one: GatePromptField.Validate refuses an
		// invalid Default, and an additive member carrying invalid UTF-8 is
		// refused when the projection is decoded. A check here would be a second
		// statement of a rule this package does not own. Both halves of that
		// claim are pinned by TestCatalogRawJSONTextValidityIsCoresRule, so if
		// core ever relaxes either one, this decision is revisited rather than
		// silently wrong.
		if value.Kind() == reflect.Slice && value.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}
		for i := range value.Len() {
			if err := validateProjectionText(value.Index(i), path+"["+strconv.Itoa(i)+"]"); err != nil {
				return err
			}
		}
	case reflect.Map:
		for _, key := range value.MapKeys() {
			member := path + "." + key.String()
			if err := validateProjectionText(key, member); err != nil {
				return err
			}
			if err := validateProjectionText(value.MapIndex(key), member); err != nil {
				return err
			}
		}
	case reflect.Struct:
		// A time.Time's interior is a *Location that is neither caller supplied
		// nor stored; its own validity is checked as an instant, not as text.
		if value.Type() == timeType {
			return nil
		}
		for i := range value.NumField() {
			field := value.Type().Field(i)
			name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			if name == "" {
				name = field.Name
			}
			if err := validateProjectionText(value.Field(i), path+"."+name); err != nil {
				return err
			}
		}
	}
	return nil
}

// rankableTime reports whether t is an instant whose UnixNano is defined. The
// zero Time is already earlier than minRankableTime, so it needs no separate
// case: "unset" and "unrepresentable" are one rejection here, not two.
func rankableTime(t time.Time) bool {
	return !t.Before(minRankableTime) && !t.After(maxRankableTime)
}
