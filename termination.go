package sessionstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// A PLACEMENT TERMINATION IS THE DURABLE AUDIT RECORD OF HOW ONE GENERATION OF
// A SESSION'S DEDICATED WORKLOAD ENDED, AS THE CONTROLLER THAT ENDED IT SAYS.
//
// A placement controller deletes a dedicated session's platform workload after
// asking the Host in it to drain. It then records whether that ending was
// graceful or forced, and if forced, why. This file stores that statement, once
// per desired generation.
//
// THE KIND IS THE CONTROLLER'S ASSERTION, AND THE STORE PROVES NOTHING ABOUT
// IT. That is deliberate, and the reason is that nothing the store can read is
// evidence either way. The only candidate is the Host registry's released
// tombstone, and it fails in both directions:
//
//   - It can be forged. ClearHostRegistration checks only that its epoch is not
//     below the committed one; it names no author. A controller fencing a Host
//     before deletion writes exactly that tombstone, and a Host writes it too
//     after an unclean release (a failed checkpoint, a refused residency release,
//     an attach rollback). The registry is also per session, not per workload,
//     so a tombstone cannot say which generation's Host released.
//   - It falsely refuses. The tombstone disappears the moment a successor — or
//     the same Host at the same epoch, which the registry admits — publishes a
//     route again, after which an honest graceful ending could no longer be
//     recorded, and would be recorded as forced instead.
//
// A check that is forgeable one way and manufactures false "forced" rows the
// other is worse than none. And nothing downstream acts on graceful versus
// forced: no write in this package, and no Factory or Host path, reads a
// termination to decide anything. It is an AUDIT record, and AGENTS.md's rule
// lets a value that is merely recorded be caller-asserted. A controller must
// therefore decide the kind from its own state machine — graceful only when it
// observed the Host report the drain complete for THIS workload — BEFORE it
// writes any fence tombstone of its own.
//
// ONE ROW PER SESSION, KEYED LOGICALLY BY (TENANT, SESSION, GENERATION). The
// row holds the outcome of the highest generation recorded so far. That shape
// is forced by the monotonic rule rather than chosen for economy: "an outcome
// for generation N must never be written under a lower generation" is a
// comparison between two generations, and a comparison is only atomic when both
// sides live in the ONE row a revision compare-and-swap closes. One row per
// generation would put the high-water in no row at all, and there is no
// cross-record transaction here to put it anywhere else — the argument
// placement.go makes for desired state applies unchanged. A reader asking for
// an older generation is told it has been superseded and by which generation,
// and is never handed another generation's outcome in its place. A controller
// must therefore record in generation order and treat superseded as terminal.
//
// The row's SHAPE follows the Host registry's: filed in the session namespace,
// unranked, never due, read and written only by name, and NEVER DELETED. It is
// written under the session's EXISTING protocol mode, read from its catalog, and
// never proposes one — the v0.10.0 registry rule.
//
// A FUTURE RECORD VERSION BLOCKS EVERY v0.11 WRITER FOR THE SESSION. The row is
// the monotonic high-water, so a reader that cannot decode it can neither answer
// Get nor admit a Record: both report version. A v2 must therefore be rolled
// out to every reader and writer before any writer emits it. The two enums are
// frozen at v1 for the same reason: a new Kind or ForcedReason spelling without
// a version bump would reach a v1 reader as invalid.

// placementTerminationNamespace is the one OrderedIndex namespace holding
// per-session placement terminations. It is namespace-distinct from every
// other record kind for the reason TestOrderedNamespacesAreDistinct states.
const placementTerminationNamespace = "sessionstore/terminations"

const (
	// PlacementTerminationRecordVersion is the independent version of the
	// stored termination. A reader fails closed on any other version.
	PlacementTerminationRecordVersion uint8 = 1

	// MaxPlacementTerminationRecordBytes bounds an encoded termination.
	//
	// The only open-ended members are the two identities, each bounded by
	// sessionwire.MaxIDBytes, whose JSON escaping can cost six bytes for one as
	// the registry's bound explains; everything else is a fixed-width number, a
	// closed enum or an instant. TestLargestAcceptablePlacementTerminationFitsTheBound
	// builds that worst case and reports what it measures.
	MaxPlacementTerminationRecordBytes = 4 << 10
)

// Stated as an unsigned constant for the reason the other records state
// theirs: an oversized record is refused here rather than by the provider.
const _ = uint(storage.MaxOrderedValueBytes - MaxPlacementTerminationRecordBytes)

// PlacementTerminationKind is the closed set of ways a dedicated workload ends.
// It is the controller's assertion; see the file comment.
type PlacementTerminationKind string

const (
	// PlacementTerminationGraceful: the controller observed the workload's
	// Host report its drain complete before the workload was deleted. The store
	// does not and cannot verify this; a graceful record says only that the
	// controller asserted it.
	PlacementTerminationGraceful PlacementTerminationKind = "graceful"
	// PlacementTerminationForced: the workload ended without the controller
	// observing a completed drain. ForcedReason says how.
	PlacementTerminationForced PlacementTerminationKind = "forced"
)

// PlacementForcedReason is the closed set of reasons a forced termination
// carries. A graceful termination carries none. The set is frozen at record
// version 1.
type PlacementForcedReason string

const (
	// PlacementForcedDrainTimeout: the controller requested a drain, the Host
	// accepted it, and the drain did not complete within its ceiling.
	PlacementForcedDrainTimeout PlacementForcedReason = "drain_timeout"
	// PlacementForcedDrainRefused: the drain could not be requested at all —
	// the Host does not advertise the drain method, or answered
	// runtime_unavailable and went cold — so the workload was deleted without
	// one.
	PlacementForcedDrainRefused PlacementForcedReason = "drain_refused"
	// PlacementForcedPlatformDeleted: the platform deleted the workload on its
	// own — an eviction, a node loss, an operator — rather than the controller
	// ending it after a drain.
	PlacementForcedPlatformDeleted PlacementForcedReason = "platform_deleted"
	// PlacementForcedWorkloadTerminated: the workload crashed or reached a
	// terminal state of its own.
	PlacementForcedWorkloadTerminated PlacementForcedReason = "workload_terminated"
)

func (r PlacementForcedReason) known() bool {
	switch r {
	case PlacementForcedDrainTimeout, PlacementForcedDrainRefused,
		PlacementForcedPlatformDeleted, PlacementForcedWorkloadTerminated:
		return true
	default:
		return false
	}
}

// PlacementTermination is the durable outcome of ending one generation of a
// session's dedicated workload.
//
// Which members bound a future caller, and why each is safe:
//
//   - Generation BECOMES A MONOTONIC BOUND: once stored, every write naming a
//     lower generation is refused forever. So it must be a value the store
//     issued, never one a caller asserted, and it is: a write is refused unless
//     its generation is at most the catalog's DesiredGeneration, which this
//     store alone mints — one at creation and one more per applied desired-state
//     write — so every admissible value is one the store has issued. A caller
//     cannot name MaxUint64 and lock out every real generation.
//   - LeaseEpoch is the Host registry epoch the controller OBSERVED for the
//     workload it ended, recorded as given and checked against nothing: no
//     write reads it, and the registry's own epoch is caller-mintable, so a
//     check against it would add a rule without adding a guarantee. Zero means
//     no registration was observed, and is admissible only for a forced
//     outcome.
//   - Kind and ForcedReason are the controller's assertion; see the file
//     comment.
//   - RecordedAt is the STORE's clock, read when the request is validated,
//     as SessionPointer.UpdatedAt is. No decision turns on it.
//
// A pooled session's termination is accepted, deliberately. The store cannot
// know which placement a PAST generation desired — moving a session to pooled
// is itself one of the two spellings of "the dedicated workload should no
// longer exist" — so refusing on the catalog's current placement would refuse
// exactly the termination that desire produces.
type PlacementTermination struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	Generation uint64

	Kind         PlacementTerminationKind
	ForcedReason PlacementForcedReason

	LeaseEpoch uint64

	RecordedAt time.Time
}

// PlacementTerminationEntry is a termination together with its revision.
type PlacementTerminationEntry struct {
	Termination PlacementTermination
	Revision    uint64
}

// placementTerminationWire and its members pin the durable encoding. The
// exported record carries no JSON tags; these do.
type placementTerminationWire struct {
	RecordVersion uint8 `json:"record_version"`
	placementTerminationRecordWire
}

type placementTerminationRecordWire struct {
	TenantID     sessionwire.TenantID     `json:"tenant_id"`
	SessionID    sessionwire.SessionID    `json:"session_id"`
	Generation   uint64                   `json:"generation"`
	Kind         PlacementTerminationKind `json:"kind"`
	ForcedReason PlacementForcedReason    `json:"forced_reason,omitempty"`
	LeaseEpoch   uint64                   `json:"lease_epoch"`
	RecordedAt   time.Time                `json:"recorded_at"`
}

func placementTerminationToWire(r PlacementTermination) placementTerminationRecordWire {
	//lint:ignore S1016 written out so each member drop stays a killable mutation
	return placementTerminationRecordWire{
		TenantID: r.TenantID, SessionID: r.SessionID, Generation: r.Generation,
		Kind: r.Kind, ForcedReason: r.ForcedReason, LeaseEpoch: r.LeaseEpoch,
		RecordedAt: r.RecordedAt,
	}
}

func (w placementTerminationRecordWire) termination() PlacementTermination {
	//lint:ignore S1016 the reverse direction is written out for the same reason
	return PlacementTermination{
		TenantID: w.TenantID, SessionID: w.SessionID, Generation: w.Generation,
		Kind: w.Kind, ForcedReason: w.ForcedReason, LeaseEpoch: w.LeaseEpoch,
		RecordedAt: w.RecordedAt,
	}
}

// encodePlacementTermination validates and encodes one termination, returning
// the CANONICAL record beside the bytes.
func encodePlacementTermination(record PlacementTermination) ([]byte, PlacementTermination, error) {
	record, err := canonicalPlacementTermination(record)
	if err != nil {
		return nil, PlacementTermination{}, err
	}
	encoded, err := json.Marshal(placementTerminationWire{
		RecordVersion:                  PlacementTerminationRecordVersion,
		placementTerminationRecordWire: placementTerminationToWire(record),
	})
	if err != nil {
		return nil, PlacementTermination{}, terminationErr(TerminationErrorInvalid, "record", err)
	}
	// Unreachable for any record canonicalization accepts: the measured worst
	// case is far below the bound. It is kept as the same belt every sibling
	// record carries, so a member added later cannot silently exceed it.
	if len(encoded) > MaxPlacementTerminationRecordBytes {
		return nil, PlacementTermination{}, terminationErr(TerminationErrorTooLarge, "record", nil)
	}
	return encoded, record, nil
}

// decodePlacementTermination strictly decodes one stored termination and
// re-validates it, so a record corrupted in place is never handed to a caller
// or used as the generation high-water a later write is measured against.
func decodePlacementTermination(value []byte) (PlacementTermination, error) {
	wire, err := decodeVersionedRecord[placementTerminationWire](
		value, MaxPlacementTerminationRecordBytes, PlacementTerminationRecordVersion,
		versionedRecordFields{Record: "record", Version: "record_version"}, terminationRecordFailure)
	if err != nil {
		return PlacementTermination{}, err
	}
	return canonicalPlacementTermination(wire.termination())
}

// canonicalPlacementTermination validates a termination and returns its one
// canonical spelling, a UTC instant. Encoding and decoding both end here, so a
// record read back is byte-identical to the one written.
//
// Kind and reason have exactly one valid pairing each way: graceful carries no
// reason and forced carries a known one. A graceful outcome also requires a
// nonzero epoch: it asserts the controller observed a Host complete a drain,
// and a Host it could observe was registered at some epoch.
func canonicalPlacementTermination(r PlacementTermination) (PlacementTermination, error) {
	if err := r.TenantID.Validate(); err != nil {
		return PlacementTermination{}, terminationErr(TerminationErrorInvalid, "tenant_id", err)
	}
	if err := r.SessionID.Validate(); err != nil {
		return PlacementTermination{}, terminationErr(TerminationErrorInvalid, "session_id", err)
	}
	if r.Generation == 0 {
		return PlacementTermination{}, terminationErr(TerminationErrorInvalid, "generation", nil)
	}
	switch r.Kind {
	case PlacementTerminationGraceful:
		if r.ForcedReason != "" {
			return PlacementTermination{}, terminationErr(TerminationErrorInvalid, "forced_reason", nil)
		}
		if r.LeaseEpoch == 0 {
			return PlacementTermination{}, terminationErr(TerminationErrorInvalid, "lease_epoch", nil)
		}
	case PlacementTerminationForced:
		if !r.ForcedReason.known() {
			return PlacementTermination{}, terminationErr(TerminationErrorInvalid, "forced_reason", nil)
		}
	default:
		return PlacementTermination{}, terminationErr(TerminationErrorInvalid, "kind", nil)
	}
	if !rankableTime(r.RecordedAt) {
		return PlacementTermination{}, terminationErr(TerminationErrorInvalid, "recorded_at", nil)
	}
	r.RecordedAt = r.RecordedAt.UTC()
	return r, nil
}

// RecordPlacementTerminationRequest records how one generation of a session's
// dedicated workload ended.
//
// ObservedLeaseEpoch is the Host registry epoch the caller observed for the
// workload it ended, and zero when it observed no registration. It is recorded
// as given.
//
// There is no timestamp member and no expected revision: the instant is the
// store's, and the write is closed against the revision this store reads for
// itself.
type RecordPlacementTerminationRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	Generation uint64

	Kind         PlacementTerminationKind
	ForcedReason PlacementForcedReason

	ObservedLeaseEpoch uint64
}

// GetPlacementTerminationRequest names one termination by its key.
type GetPlacementTerminationRequest struct {
	TenantID   sessionwire.TenantID
	SessionID  sessionwire.SessionID
	Generation uint64
}

// RecordPlacementTermination commits the outcome of ending one generation of
// a session's dedicated workload. created reports whether this call wrote it.
//
// The checks run in this order:
//
//  1. The request's own shape (invalid), before any provider work.
//  2. The session exists (the catalog read), and its catalog-bound protocol
//     mode is re-fenced through the same create-only witness the catalog create
//     used; this call never proposes a mode.
//  3. The generation is one the catalog has issued (unissued otherwise).
//  4. The stored row: a higher generation is superseded; the SAME generation is
//     an idempotent replay when every caller-authored member is identical —
//     returning the stored record unchanged with created false — and a mismatch
//     otherwise.
//
// No refusal writes a termination row. The one write a refusal after step 2 can
// make is inherited from the v0.10.0 registry: when the catalog exists but its
// protocol witness is absent, step 2 re-binds that witness to the catalog's own
// mode before a later step refuses.
//
// Other error types a caller meets come from step 2: *KeyspaceError and
// *CatalogError, each with its existing code set.
func (s *Store) RecordPlacementTermination(ctx context.Context, req RecordPlacementTerminationRequest) (PlacementTerminationEntry, bool, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return PlacementTerminationEntry{}, false, err
	}
	value, record, err := encodePlacementTermination(PlacementTermination{
		TenantID:     req.TenantID,
		SessionID:    req.SessionID,
		Generation:   req.Generation,
		Kind:         req.Kind,
		ForcedReason: req.ForcedReason,
		LeaseEpoch:   req.ObservedLeaseEpoch,
		RecordedAt:   s.clock.Now(),
	})
	if err != nil {
		return PlacementTerminationEntry{}, false, err
	}

	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return PlacementTerminationEntry{}, false, err
	}
	defer release()

	catalog, err := s.readCatalogEntry(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return PlacementTerminationEntry{}, false, err
	}
	if err := s.bindSessionScopeMode(opCtx, scope, catalog.Record.Binding.protocolModeOrLegacy()); err != nil {
		return PlacementTerminationEntry{}, false, err
	}
	if record.Generation > catalog.Record.DesiredGeneration {
		return PlacementTerminationEntry{}, false, &TerminationError{
			Code: TerminationErrorUnissued, Field: "generation", Generation: catalog.Record.DesiredGeneration,
		}
	}

	current, found, err := s.readPlacementTermination(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return PlacementTerminationEntry{}, false, err
	}
	if !found {
		return s.createPlacementTermination(opCtx, scope, record, value)
	}
	stored := current.Termination
	if stored.Generation > record.Generation {
		return PlacementTerminationEntry{}, false, &TerminationError{
			Code: TerminationErrorSuperseded, Field: "generation", Generation: stored.Generation,
		}
	}
	if stored.Generation == record.Generation {
		if field := terminationContentMismatch(stored, record); field != "" {
			return PlacementTerminationEntry{}, false, &TerminationError{
				Code: TerminationErrorMismatch, Field: field, Generation: stored.Generation,
			}
		}
		return current, false, nil
	}
	return s.updatePlacementTermination(opCtx, scope, record, value, current.Revision)
}

// terminationContentMismatch names the first caller-authored member on which
// two terminations of one generation differ, or "" when they are the same
// content. RecordedAt is the store's and is deliberately not compared: a
// replay is the same outcome recorded again, not a different one.
func terminationContentMismatch(stored, requested PlacementTermination) string {
	switch {
	case stored.Kind != requested.Kind:
		return "kind"
	case stored.ForcedReason != requested.ForcedReason:
		return "forced_reason"
	case stored.LeaseEpoch != requested.LeaseEpoch:
		return "lease_epoch"
	default:
		return ""
	}
}

// GetPlacementTermination returns the outcome recorded for exactly one
// generation.
//
// A stored row of a HIGHER generation reports superseded, and one of a lower
// generation — or no row at all — reports not_found; both carry the stored
// generation (zero meaning no row), and neither hands back a record, so a
// caller is never given another generation's outcome in place of the one it
// asked about. "Already terminated" for generation N is exactly a nil error
// here; superseded means N can no longer be recorded at all.
//
// It verifies the session's collision witnesses before any provider read.
func (s *Store) GetPlacementTermination(ctx context.Context, req GetPlacementTerminationRequest) (PlacementTerminationEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return PlacementTerminationEntry{}, err
	}
	if req.Generation == 0 {
		return PlacementTerminationEntry{}, terminationErr(TerminationErrorInvalid, "generation", nil)
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return PlacementTerminationEntry{}, err
	}
	defer release()

	entry, found, err := s.readPlacementTermination(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return PlacementTerminationEntry{}, err
	}
	if !found {
		return PlacementTerminationEntry{}, terminationErr(TerminationErrorNotFound, "record", nil)
	}
	stored := entry.Termination.Generation
	switch {
	case stored > req.Generation:
		return PlacementTerminationEntry{}, &TerminationError{Code: TerminationErrorSuperseded, Field: "generation", Generation: stored}
	case stored < req.Generation:
		return PlacementTerminationEntry{}, &TerminationError{Code: TerminationErrorNotFound, Field: "generation", Generation: stored}
	}
	return entry, nil
}

// readPlacementTermination reads the RAW stored termination. Absence is a
// boolean; anything else, a corrupt row included, is an error, because an
// unreadable row is a generation high-water that cannot be evaluated and
// reporting it as absent would let a writer create straight over it.
func (s *Store) readPlacementTermination(
	ctx context.Context,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (PlacementTerminationEntry, bool, error) {
	if err := s.verifySessionScope(ctx, scope); err != nil {
		return PlacementTerminationEntry{}, false, err
	}
	stored, err := s.backend.OrderedIndex.Get(ctx, placementTerminationID(scope, session))
	if err != nil {
		if errors.As(err, new(*storage.OrderedRecordNotFoundError)) {
			return PlacementTerminationEntry{}, false, nil
		}
		return PlacementTerminationEntry{}, false, classifyTerminationOrderedError(err, "get")
	}
	entry, err := placementTerminationEntryFor(stored, scope, tenant, session)
	if err != nil {
		return PlacementTerminationEntry{}, false, err
	}
	return entry, true, nil
}

// createPlacementTermination creates a session's first termination row. A
// create that finds the row already there lost a race and is a conflict, as
// the registry's is: the arrived row's generation and content have not been
// compared against this request, and a retry meets them on the ordinary path.
func (s *Store) createPlacementTermination(
	ctx context.Context,
	scope sessionScope,
	record PlacementTermination,
	value []byte,
) (PlacementTerminationEntry, bool, error) {
	stored, created, err := s.backend.OrderedIndex.Create(
		ctx, placementTerminationID(scope, record.SessionID), scope.SessionNamespace,
		value, storage.Rank{}, placementTerminationDue(record))
	if err != nil {
		return PlacementTerminationEntry{}, false, classifyTerminationOrderedError(err, "create")
	}
	entry, err := placementTerminationEntryFor(stored, scope, record.TenantID, record.SessionID)
	if err != nil {
		return PlacementTerminationEntry{}, false, err
	}
	if !created {
		return PlacementTerminationEntry{}, false, &TerminationError{Code: TerminationErrorConflict, Field: "create", Revision: entry.Revision}
	}
	if err := verifyTerminationBytes(stored, value); err != nil {
		return PlacementTerminationEntry{}, false, err
	}
	return entry, true, nil
}

// updatePlacementTermination compare-and-swaps a higher generation's outcome
// onto the revision its caller read.
func (s *Store) updatePlacementTermination(
	ctx context.Context,
	scope sessionScope,
	record PlacementTermination,
	value []byte,
	expectedRevision uint64,
) (PlacementTerminationEntry, bool, error) {
	stored, err := s.backend.OrderedIndex.Update(
		ctx, placementTerminationID(scope, record.SessionID), expectedRevision,
		value, storage.Rank{}, placementTerminationDue(record))
	if err != nil {
		return PlacementTerminationEntry{}, false, classifyTerminationOrderedError(err, "update")
	}
	entry, err := placementTerminationEntryFor(stored, scope, record.TenantID, record.SessionID)
	if err != nil {
		return PlacementTerminationEntry{}, false, err
	}
	if err := verifyTerminationBytes(stored, value); err != nil {
		return PlacementTerminationEntry{}, false, err
	}
	return entry, true, nil
}

// verifyTerminationBytes holds a write's reply to the bytes the write handed
// the provider, for the reason verifyRegistrationBytes states. The comparison
// is exact because the record has no []byte member and canonicalization is a
// fixed point.
func verifyTerminationBytes(stored storage.OrderedRecord, value []byte) error {
	if !bytes.Equal(stored.Value, value) {
		return terminationErr(TerminationErrorIdentity, "value", nil)
	}
	return nil
}

// placementTerminationID names the one ordered row per session.
func placementTerminationID(scope sessionScope, session sessionwire.SessionID) storage.OrderedID {
	return storage.OrderedID{
		Namespace:     placementTerminationNamespace,
		OrderingScope: scope.SessionNamespace,
		StableKey:     storage.StableKey(session),
	}
}

// placementTerminationDue is the single definition of the row's due state. It
// is constant NOT DUE: nothing pages terminations, and the rows are permanent.
func placementTerminationDue(PlacementTermination) storage.Due { return storage.Due{} }

// placementTerminationEntryFor decodes one stored termination and holds every
// provider-supplied component of its filing to what the record's own bytes say
// it should be, plus the identity asked for — the enumeration
// hostRegistrationEntryFor gives, for the same reasons.
func placementTerminationEntryFor(
	stored storage.OrderedRecord,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (PlacementTerminationEntry, error) {
	if stored.Deleted {
		return PlacementTerminationEntry{}, terminationErr(TerminationErrorDeleted, "record", nil)
	}
	record, err := decodePlacementTermination(stored.Value)
	if err != nil {
		return PlacementTerminationEntry{}, err
	}
	if record.TenantID != tenant || record.SessionID != session {
		return PlacementTerminationEntry{}, terminationErr(TerminationErrorIdentity, "record", nil)
	}
	if storage.StableKey(record.SessionID) != stored.ID.StableKey {
		return PlacementTerminationEntry{}, terminationErr(TerminationErrorIdentity, "session_id", nil)
	}
	if err := checkFiledScope(stored, scope.SessionNamespace, placementTerminationDue(record), terminationIdentity); err != nil {
		return PlacementTerminationEntry{}, err
	}
	if stored.Rank != (storage.Rank{}) {
		return PlacementTerminationEntry{}, terminationErr(TerminationErrorIdentity, "rank", nil)
	}
	return PlacementTerminationEntry{Termination: record, Revision: stored.Revision}, nil
}

// classifyTerminationOrderedError maps an OrderedIndex outcome into this
// record's vocabulary while preserving the cause, with the arms
// classifyRegistryOrderedError documents.
func classifyTerminationOrderedError(err error, field string) error {
	var notFound *storage.OrderedRecordNotFoundError
	if errors.As(err, &notFound) {
		return terminationErr(TerminationErrorNotFound, field, err)
	}
	var deleted *storage.OrderedDeletedError
	if errors.As(err, &deleted) {
		return terminationErr(TerminationErrorDeleted, field, err)
	}
	var conflict *storage.OrderedRevisionConflictError
	if errors.As(err, &conflict) {
		return &TerminationError{Code: TerminationErrorConflict, Field: field, Revision: conflict.ActualRevision, Cause: err}
	}
	var ambiguous *storage.OrderedAmbiguousError
	if errors.As(err, &ambiguous) {
		return terminationErr(TerminationErrorUnknown, field, err)
	}
	return terminationErr(TerminationErrorBackend, field, err)
}
