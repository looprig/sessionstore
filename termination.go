package sessionstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// A PLACEMENT TERMINATION IS THE DURABLE ANSWER TO "HOW DID THIS SESSION'S
// DEDICATED WORKLOAD END?"
//
// A placement controller deletes a dedicated session's platform workload only
// after the Host running in it has drained. When the drain completes, the
// ending is graceful; when it does not — the drain timed out, the platform
// deleted the workload on its own, or the workload crashed or reached a
// terminal state — the ending is FORCED, and the one thing that must never
// happen is that a forced ending is later reported as a graceful release. This
// file stores which it was, once per desired generation, together with the
// object references the controller retained at that moment.
//
// WHAT IT IS NOT. It is not a fence on the session: no write anywhere in this
// package reads it to decide anything, and it names no lease a caller may
// write under. It is not the desired state either — "this session's dedicated
// workload should no longer exist" is already expressible on the catalog record
// (a desired-state write naming no DesiredWorkload), and a second record of
// desire would need a consistency protocol with the first. It is the durable
// OUTCOME of acting on that desire, and nothing more.
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
// and is never handed another generation's outcome in its place.
//
// The row's SHAPE follows the Host registry's: filed in the session namespace,
// unranked, never due, read and written only by name, and NEVER DELETED. It is
// written under the session's EXISTING protocol mode, read from its catalog, and
// never proposes one — the v0.10.0 registry rule.

// placementTerminationNamespace is the one OrderedIndex namespace holding
// per-session placement terminations. It is namespace-distinct from every
// other record kind for the reason TestOrderedNamespacesAreDistinct states.
const placementTerminationNamespace = "sessionstore/terminations"

const (
	// PlacementTerminationRecordVersion is the independent version of the
	// stored termination. A reader fails closed on any other version.
	PlacementTerminationRecordVersion uint8 = 1

	// MaxRetainedObjectReferences bounds the object references one termination
	// retains beside its checkpoint. It is a record bound, not a statement about
	// how many objects a session has: a controller retaining more is naming
	// data that belongs in an object of its own.
	MaxRetainedObjectReferences = 16

	// MaxPlacementTerminationRecordBytes bounds an encoded termination.
	//
	// Two members are identities bounded by sessionwire.MaxIDBytes whose JSON
	// escaping can cost six bytes for one, as the registry's bound explains;
	// every object reference is canonical ASCII of a fixed shape and encodes one
	// byte per byte. TestLargestAcceptablePlacementTerminationFitsTheBound
	// builds the worst case and reports what it measures.
	MaxPlacementTerminationRecordBytes = 8 << 10
)

// Stated as an unsigned constant for the reason the other records state
// theirs: an oversized record is refused here rather than by the provider.
const _ = uint(storage.MaxOrderedValueBytes - MaxPlacementTerminationRecordBytes)

// PlacementTerminationKind is the closed set of ways a dedicated workload ends.
type PlacementTerminationKind string

const (
	// PlacementTerminationGraceful: the Host released the session's route
	// before the workload ended. The store admits it only over that evidence.
	PlacementTerminationGraceful PlacementTerminationKind = "graceful"
	// PlacementTerminationForced: the workload ended without a proven release.
	// ForcedReason says how.
	PlacementTerminationForced PlacementTerminationKind = "forced"
)

// PlacementForcedReason is the closed set of reasons a forced termination
// carries. A graceful termination carries none.
type PlacementForcedReason string

const (
	// PlacementForcedDrainTimeout: the controller requested a drain and the
	// drain did not complete within its ceiling, so the workload was deleted.
	PlacementForcedDrainTimeout PlacementForcedReason = "drain_timeout"
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
	case PlacementForcedDrainTimeout, PlacementForcedPlatformDeleted, PlacementForcedWorkloadTerminated:
		return true
	default:
		return false
	}
}

// RetainedCheckpoint names the latest workspace checkpoint a controller
// retained when the workload ended: the journal position it was captured at
// and the object that holds it. Its zero value means none was retained.
//
// It is RECORDED, not verified. The reference must parse as a workspace
// checkpoint this store could have minted, but nothing here reads a pointer,
// reads the object, or proves the bytes still exist — a caller copies it from
// the pointer it read, and a restore must still verify the stream it gets.
type RetainedCheckpoint struct {
	Sequence  uint64
	Reference sessionwire.ObjectReference
}

func (c RetainedCheckpoint) isZero() bool {
	return c.Sequence == 0 && c.Reference.ObjectID == ""
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
//   - LeaseEpoch is RECORDED, and bounds nobody: no write reads it. It is
//     nonetheless held to the store's own evidence — never above the Host
//     registry's committed epoch, and for a graceful outcome EXACTLY the epoch
//     of the registry's released tombstone — so it cannot name an epoch no Host
//     was ever registered at. Zero means no Host registration was observed,
//     and is admissible only for a forced outcome.
//   - Kind and ForcedReason are recorded; graceful is admitted only over the
//     registry's release evidence.
//   - Checkpoint and Objects are recorded references and bound nobody.
//   - RecordedAt is the STORE's clock, read when the request is validated,
//     as SessionPointer.UpdatedAt is. No decision turns on it.
type PlacementTermination struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	Generation uint64

	Kind         PlacementTerminationKind
	ForcedReason PlacementForcedReason

	LeaseEpoch uint64

	RecordedAt time.Time

	Checkpoint RetainedCheckpoint
	// Objects is in ascending ObjectID order with no duplicates; nil when none.
	Objects []sessionwire.ObjectReference
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
	TenantID     sessionwire.TenantID          `json:"tenant_id"`
	SessionID    sessionwire.SessionID         `json:"session_id"`
	Generation   uint64                        `json:"generation"`
	Kind         PlacementTerminationKind      `json:"kind"`
	ForcedReason PlacementForcedReason         `json:"forced_reason,omitempty"`
	LeaseEpoch   uint64                        `json:"lease_epoch"`
	RecordedAt   time.Time                     `json:"recorded_at"`
	Checkpoint   *retainedCheckpointWire       `json:"checkpoint,omitempty"`
	Objects      []sessionwire.ObjectReference `json:"objects,omitempty"`
}

type retainedCheckpointWire struct {
	Sequence  uint64                      `json:"sequence"`
	Reference sessionwire.ObjectReference `json:"reference"`
}

func placementTerminationToWire(r PlacementTermination) placementTerminationRecordWire {
	wire := placementTerminationRecordWire{
		TenantID: r.TenantID, SessionID: r.SessionID, Generation: r.Generation,
		Kind: r.Kind, ForcedReason: r.ForcedReason, LeaseEpoch: r.LeaseEpoch,
		RecordedAt: r.RecordedAt, Objects: r.Objects,
	}
	if !r.Checkpoint.isZero() {
		wire.Checkpoint = &retainedCheckpointWire{Sequence: r.Checkpoint.Sequence, Reference: r.Checkpoint.Reference}
	}
	return wire
}

func (w placementTerminationRecordWire) termination() PlacementTermination {
	r := PlacementTermination{
		TenantID: w.TenantID, SessionID: w.SessionID, Generation: w.Generation,
		Kind: w.Kind, ForcedReason: w.ForcedReason, LeaseEpoch: w.LeaseEpoch,
		RecordedAt: w.RecordedAt, Objects: w.Objects,
	}
	if w.Checkpoint != nil {
		r.Checkpoint = RetainedCheckpoint{Sequence: w.Checkpoint.Sequence, Reference: w.Checkpoint.Reference}
	}
	return r
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
// canonical spelling: a UTC instant, a checkpoint wholly present or wholly
// absent, and objects in ascending ObjectID order or nil. Encoding and decoding
// both end here, so a record read back is byte-identical to the one written.
//
// Kind and reason have exactly one valid pairing each way: graceful carries no
// reason and forced carries a known one. A graceful outcome also requires a
// nonzero epoch, because the only evidence that admits it is a released
// registration, and a registration always has one.
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
	if !r.Checkpoint.isZero() {
		if r.Checkpoint.Sequence == 0 {
			return PlacementTermination{}, terminationErr(TerminationErrorInvalid, "checkpoint.sequence", nil)
		}
		// One refusal, not two: an unparseable reference has no kind, so a
		// separate parse gate would be masked by the kind gate with the identical
		// error and no test could hold it. The parse failure is kept as Cause.
		if parsed, err := parseObjectReference(r.Checkpoint.Reference); err != nil || parsed.kind != ObjectKindWorkspaceCheckpoint {
			return PlacementTermination{}, terminationErr(TerminationErrorInvalid, "checkpoint.reference", err)
		}
	}
	if len(r.Objects) > MaxRetainedObjectReferences {
		return PlacementTermination{}, terminationErr(TerminationErrorInvalid, "objects", nil)
	}
	for i, object := range r.Objects {
		if _, err := parseObjectReference(object); err != nil {
			return PlacementTermination{}, terminationErr(TerminationErrorInvalid, "objects", err)
		}
		// Strictly ascending: one order, and no duplicates, so a set of
		// references has exactly one stored spelling.
		if i > 0 && r.Objects[i-1].ObjectID >= object.ObjectID {
			return PlacementTermination{}, terminationErr(TerminationErrorInvalid, "objects", nil)
		}
	}
	if len(r.Objects) == 0 {
		r.Objects = nil
	} else {
		// A caller that kept its slice must not be able to rewrite what a
		// validated record means before it is encoded or after it is returned.
		r.Objects = slices.Clone(r.Objects)
	}
	return r, nil
}

// RecordPlacementTerminationRequest records how one generation of a session's
// dedicated workload ended.
//
// ObservedLeaseEpoch is the Host registry epoch the caller observed for the
// workload it ended, and zero when it observed no registration. It is checked
// against the registry, never taken on trust: see PlacementTermination.
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

	Checkpoint RetainedCheckpoint
	// Objects must be in ascending ObjectID order with no duplicates.
	Objects []sessionwire.ObjectReference
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
// The checks run in this order, and each refusal writes nothing:
//
//  1. The request's own shape (invalid), before any provider work.
//  2. The session exists (the catalog read) and its catalog-bound protocol
//     mode is re-fenced through the same create-only witness the catalog create
//     used; this call never proposes a mode.
//  3. The generation is one the catalog has issued (unissued otherwise).
//  4. The stored row: a higher generation is superseded; the SAME generation is
//     an idempotent replay when every caller-authored member is identical —
//     returning the stored record unchanged with created false — and a mismatch
//     otherwise. The replay is decided BEFORE the evidence below is read, so a
//     restarted controller reads back what it committed even after a successor
//     Host has overwritten the tombstone that admitted it.
//  5. The Host registry's evidence: the observed epoch may not exceed the
//     registry's committed epoch, and a graceful outcome requires a released
//     tombstone at exactly the observed epoch (not_released / epoch otherwise).
//
// The evidence read and the write are not one transaction — nothing here is —
// so the release evidence is "as read at the check". A registration moving
// after it cannot make a forced outcome read as graceful; it can only make a
// graceful one describe a release a successor has since built on.
//
// Any other error type a caller meets comes from the reads in step 2 and 5:
// *KeyspaceError, *CatalogError and *RegistryError, each with its existing code
// set.
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
		Checkpoint:   req.Checkpoint,
		Objects:      req.Objects,
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
	if found {
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
	}

	if err := s.terminationEvidence(opCtx, scope, req.TenantID, req.SessionID, record); err != nil {
		return PlacementTerminationEntry{}, false, err
	}

	if !found {
		return s.createPlacementTermination(opCtx, scope, record, value)
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
	case stored.Checkpoint != requested.Checkpoint:
		return "checkpoint"
	case !slices.Equal(stored.Objects, requested.Objects):
		return "objects"
	default:
		return ""
	}
}

// terminationEvidence holds the observed epoch, and a graceful kind, to the
// Host registry's committed state.
//
// It reads the RAW registration, released and expired ones included, because
// a released tombstone is exactly the evidence a graceful outcome needs and
// the public reader reports it as no route at all. It reads; it never writes.
func (s *Store) terminationEvidence(
	ctx context.Context,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	record PlacementTermination,
) error {
	registration, found, err := s.readHostRegistration(ctx, scope, tenant, session)
	if err != nil {
		return err
	}
	var committed uint64
	if found {
		committed = registration.Registration.LeaseEpoch
	}
	// Graceful first: its evidence is a STATE of the registration, and a
	// caller asking for graceful where there has been no release at all is told
	// that, rather than being told only that its epoch is wrong.
	if record.Kind == PlacementTerminationGraceful {
		if !found || !registration.Registration.released() {
			return &TerminationError{Code: TerminationErrorNotReleased, Field: "route", Epoch: committed}
		}
		if committed != record.LeaseEpoch {
			return &TerminationError{Code: TerminationErrorEpoch, Field: "observed_lease_epoch", Epoch: committed}
		}
		return nil
	}
	if record.LeaseEpoch > committed {
		return &TerminationError{Code: TerminationErrorEpoch, Field: "observed_lease_epoch", Epoch: committed}
	}
	return nil
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
