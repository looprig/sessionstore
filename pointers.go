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

// A POINTER IS THE AUTHORITATIVE, RETAINED NAME OF ONE IMMUTABLE OBJECT.
//
// A session accumulates immutable objects — workspace checkpoints, runtime
// checkpoints, continuations — and for each of those roles exactly one of them
// is CURRENT. This file stores that choice: one small permanent row per
// (session, role), fenced by the lease epoch that wrote it and floored by the
// journal position of the object it names.
//
// WHAT IT IS NOT. It is not the object. Nothing here reads, writes, copies or
// deletes a blob, and moving a pointer leaves every object it has ever named
// exactly where it was, byte for byte — which is what makes "replace the
// pointer" a safe operation and "delete the object" a thing this package still
// does not offer.
//
// IT IS NOT THE CATALOG'S CHECKPOINT SUMMARY EITHER, AND THE RELATIONSHIP IS
// THE POINT OF THIS FILE. CatalogRecord.Checkpoint is a SUMMARY: it is written
// wholesale by UpdateCatalogHostState, which replaces it on every projection
// write and zeroes it when a write omits it. That was accepted on the explicit
// basis that clearing the summary loses no durable state because the
// authoritative retained pointer is a separate epoch-fenced record — this one.
// Three consequences follow, and they are enforced rather than described:
//
//   - The summary is a PROJECTION OF THIS RECORD, not a second opinion.
//     SessionPointer.CheckpointSummary below is the only way to build one from
//     durable state, so a Host publishes what the pointer says rather than
//     composing a parallel answer by hand. There is exactly one summary type in
//     this package and this file does not add another.
//   - Nothing here reads the summary. A pointer write consults the pointer
//     record and nothing else, so a summary that is stale, empty, or never
//     written cannot change what a pointer decides — which is what makes the
//     catalog's replace-everything semantics harmless rather than lossy.
//   - The summary is therefore ALLOWED to lag, and a reader that must not
//     resurrect a discarded checkpoint reads the pointer. There is no
//     cross-record transaction available to keep the two in step — the argument
//     placement.go makes for desired state applies here unchanged — so the
//     divergence is bounded by naming which of the two is authoritative rather
//     than by pretending it cannot happen.
//
// The record's SHAPE follows the Host registry's, and for the same reasons: one
// row per identity, filed in the session namespace, unranked, never due, read
// and written only by name, and NEVER DELETED. Clearing writes a tombstone
// under the same fence, so the two high-water marks this record retains can
// never fall.

// pointerNamespace is the one OrderedIndex namespace holding per-session object
// pointers. It is namespace-distinct from every other record kind for the
// reason TestOrderedNamespacesAreDistinct states: a namespace is the unit a
// decoder is chosen for.
const pointerNamespace = "sessionstore/pointers"

const (
	// SessionPointerRecordVersion is the independent version of the stored
	// pointer. A reader fails closed on any other version rather than guessing
	// which members a future encoder meant.
	SessionPointerRecordVersion uint8 = 1

	// MaxSessionPointerRecordBytes bounds an encoded pointer.
	//
	// The record has no open-ended member. Two of its members are identities
	// bounded by sessionwire.MaxIDBytes, and the ceiling has to allow for JSON
	// ESCAPING of those, which is what sizes it: an identity is any valid UTF-8
	// of at most that many bytes — control characters included, which both
	// TenantID.Validate and SessionID.Validate accept — and Go escapes each of
	// those as \u00XX, six bytes for one.
	//
	// The target is NOT escaping-sensitive and is the reason this bound is
	// half the registry's rather than equal to it: an ObjectID that
	// parseObjectReference accepts is canonical lowercase ASCII of a fixed
	// shape, so it encodes one byte per byte.
	// TestLargestAcceptableSessionPointerFitsTheBound builds the worst case
	// over every declared kind and reports what it measures.
	//
	// Like the other bounds it sits below storage.MaxOrderedValueBytes, so a
	// record this package accepts always fits in the provider and there is no
	// state that can be written but not rewritten.
	MaxSessionPointerRecordBytes = 8 << 10
)

// Stated as an unsigned constant for the reason the other records state theirs:
// an oversized record is refused here rather than by the provider, and prose
// cannot enforce that relationship.
const _ = uint(storage.MaxOrderedValueBytes - MaxSessionPointerRecordBytes)

// SessionPointerKind is the closed set of roles a session's current object can
// fill. It is the vocabulary the generic kernel below is parameterized by, and
// each member has exactly one public triple of methods.
//
// ActiveContinuation is declared here and stored here and is otherwise RESERVED
// for the gate suspension/resume plan. This file stores a NAME; it does not
// define what a continuation contains, does not resume one, and no operation in
// this package reads one — which TestNothingElseInThisPackageReadsAPointer
// keeps true.
type SessionPointerKind string

const (
	SessionPointerActiveContinuation  SessionPointerKind = "active-continuation"
	SessionPointerWorkspaceCheckpoint SessionPointerKind = "workspace-checkpoint"
	SessionPointerRuntimeCheckpoint   SessionPointerKind = "runtime-checkpoint"
)

// targetObjectKind is the TOTAL function from a pointer's role to the object
// kind its target must have, and it is what makes the public methods typed in
// more than name.
//
// An ObjectID carries its kind, so this is decided from the reference itself
// before any provider work: a caller that hands the workspace-checkpoint method
// a runtime-checkpoint reference is refused here rather than discovered later
// by whatever tries to restore from it. The pointer kinds and the object kinds
// are deliberately separate closed sets — a role is not a payload format, and
// several roles could name one object kind — which is why this mapping is
// written out rather than derived from the spelling.
//
// It reports absence rather than a zero kind, so an unclassified kind fails
// CLOSED: a kind added to the enum without a mapping here stores nothing at all
// rather than accepting any object that comes along.
func (k SessionPointerKind) targetObjectKind() (ObjectKind, bool) {
	switch k {
	case SessionPointerActiveContinuation:
		return ObjectKindContinuation, true
	case SessionPointerWorkspaceCheckpoint:
		return ObjectKindWorkspaceCheckpoint, true
	case SessionPointerRuntimeCheckpoint:
		return ObjectKindRuntimeCheckpoint, true
	default:
		return "", false
	}
}

// sessionPointerKinds is the enum in one place, so a test that must range over
// every kind ranges over the kinds this package HAS rather than the ones its
// author remembered. It is the same discipline the cursor-magic and namespace
// scans apply, expressed as a declaration because this set is small and closed.
//
// Its agreement with the declared constants is not left to memory:
// TestSessionPointerKindsAreTheDeclaredOnes reads them out of the source.
func sessionPointerKinds() []SessionPointerKind {
	return []SessionPointerKind{
		SessionPointerActiveContinuation,
		SessionPointerWorkspaceCheckpoint,
		SessionPointerRuntimeCheckpoint,
	}
}

// SessionPointer is the durable record of which immutable object currently
// fills one role for one session, and of the two high-water marks that decide
// whether a later write may replace it.
//
// The two are different questions and both are permanent:
//
//   - LeaseEpoch answers MAY YOU WRITE. It is the epoch of the grant that last
//     wrote this record, and a write naming a strictly lower one has provably
//     lost the session. An equal one is admitted because one grant writes many
//     times. It never falls, which is why this record is never deleted.
//   - Sequence answers IS THIS NEWER. It is the journal position the target was
//     captured at, and a write naming a strictly lower one would republish an
//     object that a later capture has already superseded. It never falls
//     either, and it is the one thing the epoch cannot supply: two writes under
//     ONE grant are ordered only by their revision compare-and-swap, so a
//     losing writer that retried would otherwise reinstate its older
//     checkpoint over the newer one and every restore afterwards would silently
//     lose the work in between. The catalog refuses a regressing
//     LastJournalSeq for exactly this reason.
//
// A nil Target is the cleared tombstone. It is one nil rather than an
// enumeration of zero values, so a LIVE TOMBSTONE IS UNREPRESENTABLE rather
// than excluded by a check somebody has to remember, and a cleared pointer
// carries both high-waters and nothing else.
//
// UpdatedAt is the store's own clock reading at the moment the write was
// accepted, not a caller's instant, for the reason ClearHostRegistrationRequest
// carries no timestamp: nothing in this record has an expiry, so no decision
// turns on this instant, and a caller-supplied one could only be wrong. It is
// the record's audit line and the summary's capture instant.
//
// The accumulation is one small permanent row per role per session that has
// ever had one: never listed, never ranked, never due, and never read except by
// name. The registry's carry-forward contract about retention applies here word
// for word — the only safe reaper is one that removes the session's whole scope
// at once, because deleting this row alone destroys a fence while leaving the
// session writable.
type SessionPointer struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	Kind SessionPointerKind

	LeaseEpoch uint64
	Sequence   uint64

	UpdatedAt time.Time

	// Target is nil exactly when this pointer has been cleared.
	Target *sessionwire.ObjectReference
}

// cleared reports whether this pointer is the tombstone a clear leaves behind.
//
// It is a method rather than an inline nil test at each site because "no
// target" is a STATE of this record and every rule that branches on it must ask
// the same question. Its call sites are deliberately not enumerated here: a
// comment listing its own callers is one that silently stops being true.
func (p SessionPointer) cleared() bool { return p.Target == nil }

// SessionPointerEntry is a pointer together with the revision a later
// compare-and-swap names.
//
// The provider's immutable acceptance order is deliberately not exposed, for
// the reason HostRegistrationEntry's is not: a session's position in a stream
// of pointer writes is not a fact any caller acts on.
type SessionPointerEntry struct {
	Pointer  SessionPointer
	Revision uint64
}

// CheckpointSummary projects a workspace checkpoint pointer into the catalog's
// summary shape, and it is the ONLY way to build one from durable state.
//
// This is where "the catalog holds a summary while the pointer holds the truth"
// stops being prose. A Host that publishes a projection reads this pointer and
// projects it; it does not compose a second answer from whatever it happens to
// remember, and UpdateCatalogHostState's replace-everything semantics are
// therefore harmless — what it replaces is a copy.
//
// A CLEARED pointer projects to the ZERO summary with no error, because the
// zero summary is precisely what CheckpointSummary documents as "no checkpoint
// has been committed". That is the whole propagation path for a clear: the
// authoritative record says there is none, and the copy the catalog carries
// says the same thing on the next projection write.
//
// It refuses any other KIND. The catalog validates a summary's reference as an
// opaque ObjectID and no more — it cannot tell a workspace checkpoint from a
// runtime one — so a runtime-checkpoint pointer projected into the catalog's
// workspace-checkpoint summary would put an object no restore can use where one
// it can use belongs, and nothing downstream would notice.
//
// CapturedAt is this record's UpdatedAt, which is the instant the STORE
// accepted the pointer rather than the instant the Host finished writing the
// object. It is later than the true capture by one write, it is the only
// instant this record has, and it is a rendering field: no decision in this
// package or in the catalog turns on it.
func (p SessionPointer) CheckpointSummary() (CheckpointSummary, error) {
	if p.Kind != SessionPointerWorkspaceCheckpoint {
		return CheckpointSummary{}, pointerErr(PointerErrorInvalid, "kind", nil)
	}
	if p.cleared() {
		return CheckpointSummary{}, nil
	}
	return CheckpointSummary{
		JournalSeq: p.Sequence,
		Reference:  *p.Target,
		CapturedAt: p.UpdatedAt.UTC(),
	}, nil
}

// pointerWire is the stored JSON shape. Target is a pointer so a cleared
// pointer's bytes carry no empty reference object and a record has exactly one
// spelling per state.
type pointerWire struct {
	RecordVersion uint8                        `json:"record_version"`
	TenantID      sessionwire.TenantID         `json:"tenant_id"`
	SessionID     sessionwire.SessionID        `json:"session_id"`
	Kind          SessionPointerKind           `json:"kind"`
	LeaseEpoch    uint64                       `json:"lease_epoch"`
	Sequence      uint64                       `json:"sequence"`
	UpdatedAt     time.Time                    `json:"updated_at"`
	Target        *sessionwire.ObjectReference `json:"target,omitempty"`
}

// encodeSessionPointer validates and encodes one pointer. It refuses a record
// above this package's bound here rather than letting the provider refuse it,
// so a record this package accepted can always be rewritten.
//
// It returns the CANONICAL record beside the bytes, as the registry's and the
// claim's encoders do, and a caller must carry that one forward rather than the
// request-shaped value it passed in: sessionPointerDue's carry-forward contract
// is that a due horizon is derived from the record, and deriving one from a
// record the stored bytes are not in is exactly the divergence that makes every
// concurrent reader fail.
func encodeSessionPointer(pointer SessionPointer) ([]byte, SessionPointer, error) {
	pointer, err := canonicalSessionPointer(pointer)
	if err != nil {
		return nil, SessionPointer{}, err
	}
	encoded, err := json.Marshal(pointerWire{
		RecordVersion: SessionPointerRecordVersion,
		TenantID:      pointer.TenantID,
		SessionID:     pointer.SessionID,
		Kind:          pointer.Kind,
		LeaseEpoch:    pointer.LeaseEpoch,
		Sequence:      pointer.Sequence,
		UpdatedAt:     pointer.UpdatedAt,
		Target:        pointer.Target,
	})
	if err != nil {
		return nil, SessionPointer{}, pointerErr(PointerErrorInvalid, "record", err)
	}
	if len(encoded) > MaxSessionPointerRecordBytes {
		return nil, SessionPointer{}, pointerErr(PointerErrorTooLarge, "record", nil)
	}
	return encoded, pointer, nil
}

// decodeSessionPointer strictly decodes one stored pointer and re-validates it,
// so a record corrupted in place cannot be handed to a caller or, worse, be
// used as a fence a later write is measured against.
//
// Strictness reaches the target as well. sessionwire.ObjectReference drops an
// undeclared member of its own rather than refusing it, which is core's
// deliberate redaction rule; what this package owns is that the reference must
// PARSE into the pointer's own object kind, and canonicalization applies that
// to stored bytes exactly as it does to a request.
func decodeSessionPointer(value []byte) (SessionPointer, error) {
	wire, err := decodeVersionedRecord[pointerWire](
		value, MaxSessionPointerRecordBytes, SessionPointerRecordVersion,
		versionedRecordFields{Record: "record", Version: "record_version"}, pointerRecordFailure)
	if err != nil {
		return SessionPointer{}, err
	}
	return canonicalSessionPointer(SessionPointer{
		TenantID:   wire.TenantID,
		SessionID:  wire.SessionID,
		Kind:       wire.Kind,
		LeaseEpoch: wire.LeaseEpoch,
		Sequence:   wire.Sequence,
		UpdatedAt:  wire.UpdatedAt,
		Target:     wire.Target,
	})
}

// canonicalSessionPointer validates a pointer and returns its one canonical
// spelling: a UTC instant, and a target that is either wholly present and of
// this pointer's own object kind or wholly absent. Encoding and decoding both
// end here, so a record read back is byte-identical to the record written and
// two encoders cannot disagree.
//
// The target is validated by PARSING it, which is the record's strongest
// available check and the one the catalog's summary deliberately does not have:
// an ObjectID names its kind, its instance generation and its content digest in
// one canonical spelling, so a reference this function accepts is one this
// store could have minted, for an object of the kind this pointer's role
// requires. It does not — and cannot — say the object EXISTS. Existence is a
// blob read, this file performs none, and a Host writes a pointer only after
// PutObject has returned the reference it is naming.
//
// The Sequence is deliberately unconstrained here. It is a floor rather than a
// property of the record: which values are admissible depends on what is
// already stored, which is a question about a WRITE and is answered by
// pointerSequenceFence. Stating it here would make a stored pointer undecodable
// the moment the rule changed under it.
//
// The instant bounds are this package's own, as they are for every other
// record: a year Go's JSON encoder cannot spell would otherwise be refused at
// Marshal with an untyped failure rather than here.
func canonicalSessionPointer(pointer SessionPointer) (SessionPointer, error) {
	if err := pointer.TenantID.Validate(); err != nil {
		return SessionPointer{}, pointerErr(PointerErrorInvalid, "tenant_id", err)
	}
	if err := pointer.SessionID.Validate(); err != nil {
		return SessionPointer{}, pointerErr(PointerErrorInvalid, "session_id", err)
	}
	object, known := pointer.Kind.targetObjectKind()
	if !known {
		return SessionPointer{}, pointerErr(PointerErrorInvalid, "kind", nil)
	}
	if pointer.LeaseEpoch == 0 {
		return SessionPointer{}, pointerErr(PointerErrorInvalid, "lease_epoch", nil)
	}
	if !rankableTime(pointer.UpdatedAt) {
		return SessionPointer{}, pointerErr(PointerErrorInvalid, "updated_at", nil)
	}
	pointer.UpdatedAt = pointer.UpdatedAt.UTC()

	if pointer.cleared() {
		return pointer, nil
	}
	parsed, err := parseObjectReference(*pointer.Target)
	if err != nil {
		return SessionPointer{}, pointerErr(PointerErrorInvalid, "target", err)
	}
	if parsed.kind != object {
		return SessionPointer{}, pointerErr(PointerErrorInvalid, "target", nil)
	}
	// The target is deliberately NOT copied here, and the reason is that there
	// is no path on which the copy would do anything. A caller reaches this
	// record through SetSessionPointerRequest, which carries the reference BY
	// VALUE and is copied once into the record setPointer builds; a stored
	// record reaches it through the decoder, which allocates its own. A copy
	// here would be a guard no mutation could break, which is worse than no
	// guard: it reads as protection while protecting nothing. If a request ever
	// carries a *ObjectReference, the copy belongs at that entry point.
	return pointer, nil
}

// SetSessionPointerRequest names the object one role should now point at.
//
// It carries a Target by VALUE, which is what makes "set" and "clear" two
// different operations rather than one operation with a nil argument: a caller
// cannot erase a session's checkpoint by forgetting to set a member, and the
// tombstone has exactly one writer.
//
// It carries no KIND. The kind is spelled by the METHOD, so there is no way to
// name a role that has no method and no way to hand the wrong role a reference
// that the method's own object-kind rule would then have to catch by luck.
//
// LeaseEpoch is the grant's epoch and is compared against the record's
// committed high-water mark. Sequence is the journal position the target was
// captured at and is compared against the record's committed sequence; both are
// high-water marks and neither ever falls.
//
// There is no expected revision and no timestamp. A pointer is not a decision a
// caller makes about a record it has read — it is the current truth about which
// object fills a role — so the write is closed against the revision this store
// reads for itself, exactly as UpdateCatalogHostState and PutHostRegistration
// are, and the instant is the store's for the reason SessionPointer states.
type SetSessionPointerRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	LeaseEpoch uint64
	Sequence   uint64

	Target sessionwire.ObjectReference
}

// GetSessionPointerRequest reads one session's current pointer of one role.
type GetSessionPointerRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
}

// ClearSessionPointerRequest gives up one role's object, leaving the
// epoch-fenced tombstone behind.
//
// It names no sequence, and that is the shape of the operation rather than an
// omission: clearing does not publish a capture, so there is nothing for a
// sequence to describe, and the stored one is RETAINED rather than replaced. A
// request that could name one could lower it.
type ClearSessionPointerRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	LeaseEpoch uint64
}

// The nine public operations. Each is its kind's name for one of the three
// kernel operations below, and each is a single line on purpose: the RULES live
// in the kernel, stated once, so no role can drift into its own idea of what
// fencing means, and the vocabulary — which role, and therefore which object
// kind and which stored row — is the only thing a method supplies.
//
// They are separate methods rather than one method with a kind parameter
// because that parameter would be a caller's to get wrong. A Host asking for
// the workspace checkpoint cannot accidentally read the continuation, and a
// role this package has not defined cannot be named at all.

func (s *Store) SetWorkspaceCheckpointPointer(
	ctx context.Context, req SetSessionPointerRequest) (SessionPointerEntry, error) {
	return s.setPointer(ctx, SessionPointerWorkspaceCheckpoint, req)
}

func (s *Store) GetWorkspaceCheckpointPointer(
	ctx context.Context, req GetSessionPointerRequest) (SessionPointerEntry, error) {
	return s.getPointer(ctx, SessionPointerWorkspaceCheckpoint, req)
}

func (s *Store) ClearWorkspaceCheckpointPointer(
	ctx context.Context, req ClearSessionPointerRequest) (SessionPointerEntry, error) {
	return s.clearPointer(ctx, SessionPointerWorkspaceCheckpoint, req)
}

func (s *Store) SetRuntimeCheckpointPointer(
	ctx context.Context, req SetSessionPointerRequest) (SessionPointerEntry, error) {
	return s.setPointer(ctx, SessionPointerRuntimeCheckpoint, req)
}

func (s *Store) GetRuntimeCheckpointPointer(
	ctx context.Context, req GetSessionPointerRequest) (SessionPointerEntry, error) {
	return s.getPointer(ctx, SessionPointerRuntimeCheckpoint, req)
}

func (s *Store) ClearRuntimeCheckpointPointer(
	ctx context.Context, req ClearSessionPointerRequest) (SessionPointerEntry, error) {
	return s.clearPointer(ctx, SessionPointerRuntimeCheckpoint, req)
}

// The continuation triple stores a NAME and nothing more. What a continuation
// contains, when one may be activated, and what activation does are the gate
// suspension plan's, and no operation in this package reads this row.

func (s *Store) SetActiveContinuationPointer(
	ctx context.Context, req SetSessionPointerRequest) (SessionPointerEntry, error) {
	return s.setPointer(ctx, SessionPointerActiveContinuation, req)
}

func (s *Store) GetActiveContinuationPointer(
	ctx context.Context, req GetSessionPointerRequest) (SessionPointerEntry, error) {
	return s.getPointer(ctx, SessionPointerActiveContinuation, req)
}

func (s *Store) ClearActiveContinuationPointer(
	ctx context.Context, req ClearSessionPointerRequest) (SessionPointerEntry, error) {
	return s.clearPointer(ctx, SessionPointerActiveContinuation, req)
}

// setPointer is the fenced-pointer write, and it is the only one. Every typed
// Set method is this function with a kind bound.
//
// THE TWO FENCES, AND WHY THE ORDER IS LOAD-BEARING. The epoch is checked
// first, and it answers a question about the CALLER: may you write this session
// at all. The sequence is checked second, and it answers a question about the
// caller's DATA: is what you hold newer than what is stored. A superseded lease
// carrying a newer capture must be told it has lost the session rather than be
// admitted, and a live lease carrying an older capture must be told its data is
// stale rather than that its authority is in doubt — the two failures ask for
// opposite responses from a caller, and reversing the order would give a
// superseded Host a Sequence refusal it would retry forever. Both are stated
// once, here, on behalf of all three roles.
//
// It reads through readSessionPointer and never through getPointer, and that
// separation is as load-bearing here as it is in the registry: the public
// reader reports a cleared pointer as no answer at all, and a writer that
// believed it would create a fresh record over a row it could not see —
// resetting both high-water marks to whatever the superseded writer named.
func (s *Store) setPointer(
	ctx context.Context,
	kind SessionPointerKind,
	req SetSessionPointerRequest,
) (SessionPointerEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return SessionPointerEntry{}, err
	}
	target := req.Target
	// Encoding validates, so an invalid request is refused before any provider
	// work — including before the session's witnesses are bound. It returns the
	// CANONICAL record over the request-shaped one built here, so nothing below
	// can reach a form the stored bytes are not in.
	value, pointer, err := encodeSessionPointer(SessionPointer{
		TenantID:   req.TenantID,
		SessionID:  req.SessionID,
		Kind:       kind,
		LeaseEpoch: req.LeaseEpoch,
		Sequence:   req.Sequence,
		UpdatedAt:  s.clock.Now(),
		Target:     &target,
	})
	if err != nil {
		return SessionPointerEntry{}, err
	}

	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return SessionPointerEntry{}, err
	}
	defer release()
	// A pointer is durable session data and may be the first record a session
	// has, so writing one binds the session's collision witnesses exactly as
	// publishing a registration does. The binding is create-only and idempotent.
	if err := s.bindSessionScope(opCtx, scope); err != nil {
		return SessionPointerEntry{}, err
	}

	current, found, err := s.readSessionPointer(opCtx, scope, kind, req.TenantID, req.SessionID)
	if err != nil {
		return SessionPointerEntry{}, err
	}
	if !found {
		return s.createSessionPointer(opCtx, scope, pointer, value)
	}
	if err := pointerEpochFence(current.Pointer, req.LeaseEpoch); err != nil {
		return SessionPointerEntry{}, err
	}
	if err := pointerSequenceFence(current.Pointer, req.Sequence); err != nil {
		return SessionPointerEntry{}, err
	}
	return s.writeSessionPointer(opCtx, scope, pointer, value, current.Revision)
}

// getPointer returns one role's pointer, and returns it only while it names
// something.
//
// A cleared pointer reports Cleared WITHOUT the record, which is the discipline
// GetHostRegistration applies to a released registration: a caller is never
// handed state this store will not vouch for. The structural argument is
// complete on its own here — a cleared pointer's target is nil, so there is
// nothing to leak — and the reason to withhold the record anyway is that
// "cleared" is then a NAMED answer rather than a nil member every caller has to
// remember to test. The failure carries both high-water marks, which is
// everything a caller can act on and the whole of what a cleared pointer
// licenses.
//
// It verifies the session's collision witnesses before any provider read, so a
// derived name is never trusted on its own.
func (s *Store) getPointer(
	ctx context.Context,
	kind SessionPointerKind,
	req GetSessionPointerRequest,
) (SessionPointerEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return SessionPointerEntry{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return SessionPointerEntry{}, err
	}
	defer release()

	entry, found, err := s.readSessionPointer(opCtx, scope, kind, req.TenantID, req.SessionID)
	if err != nil {
		return SessionPointerEntry{}, err
	}
	if !found {
		return SessionPointerEntry{}, pointerErr(PointerErrorNotFound, "record", nil)
	}
	if entry.Pointer.cleared() {
		return SessionPointerEntry{}, &PointerError{
			Code:     PointerErrorCleared,
			Field:    "target",
			Epoch:    entry.Pointer.LeaseEpoch,
			Sequence: entry.Pointer.Sequence,
		}
	}
	return entry, nil
}

// clearPointer gives up one role's object under the pointer's fencing epoch,
// writing the tombstone rather than deleting the record.
//
// THE SEQUENCE IS CARRIED FORWARD FROM THE STORED RECORD, and that single
// assignment is what step 3's "clearing retains the high-water" means. A clear
// that reset it to zero would make the record forget which captures it had
// already superseded, and the next write — from any lease at or above the
// epoch, including one holding a copy from before the clear — could reinstate
// an object the clear existed to abandon. The epoch is retained by the same
// argument, one level up: the fence would fall and a lease that has already
// lost the session could write again.
//
// It is idempotent under one grant: a repeat returns the stored tombstone
// without writing. What carries that rule is the EQUALITY in the repeat
// condition rather than its position — an epoch equal to the tombstone's has
// already passed the fence — but it is written after the fence anyway, because
// the repeat check is not an ownership test and must never become the only
// thing standing between a superseded lease and a success.
//
// A LATER grant clearing an already-cleared pointer is not a repeat: the
// tombstone is rewritten so the high-water rises, or every lease granted in
// between would still be able to write.
//
// A role with no record at all reports NotFound. Cleanup is idempotent with
// respect to ITS OWN tombstone, not with respect to nothing: writing one for a
// pointer that never existed would mint a fencing high-water mark out of an
// unverified caller-supplied epoch, which is exactly what
// ClearHostRegistration refuses to do.
func (s *Store) clearPointer(
	ctx context.Context,
	kind SessionPointerKind,
	req ClearSessionPointerRequest,
) (SessionPointerEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return SessionPointerEntry{}, err
	}
	// The epoch is checked up front rather than through the encoder, because
	// the tombstone this operation writes cannot be built until the stored
	// sequence has been read. An epochless request is a caller mistake and is
	// refused as one, before admission and before any provider work.
	if req.LeaseEpoch == 0 {
		return SessionPointerEntry{}, pointerErr(PointerErrorInvalid, "lease_epoch", nil)
	}
	// The ROLE is deliberately not validated up front. It is the method's
	// rather than the caller's, so it cannot be wrong; and if a kind were ever
	// added to the enum without a mapping, the tombstone this operation encodes
	// below would refuse it with the identical code and field. An up-front gate
	// whose absence a deeper validator masks with the same error is a guard no
	// test can hold and a reader has to run both to learn which one decides.
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return SessionPointerEntry{}, err
	}
	defer release()

	current, found, err := s.readSessionPointer(opCtx, scope, kind, req.TenantID, req.SessionID)
	if err != nil {
		return SessionPointerEntry{}, err
	}
	if !found {
		return SessionPointerEntry{}, pointerErr(PointerErrorNotFound, "record", nil)
	}
	if err := pointerEpochFence(current.Pointer, req.LeaseEpoch); err != nil {
		return SessionPointerEntry{}, err
	}
	if current.Pointer.cleared() && current.Pointer.LeaseEpoch == req.LeaseEpoch {
		return current, nil
	}
	value, tombstone, err := encodeSessionPointer(SessionPointer{
		TenantID:   req.TenantID,
		SessionID:  req.SessionID,
		Kind:       kind,
		LeaseEpoch: req.LeaseEpoch,
		Sequence:   current.Pointer.Sequence,
		UpdatedAt:  s.clock.Now(),
	})
	if err != nil {
		return SessionPointerEntry{}, err
	}
	return s.writeSessionPointer(opCtx, scope, tombstone, value, current.Revision)
}

// pointerEpochFence admits a Host-owned write against the pointer's committed
// high-water epoch. It is epochFence in this record's vocabulary, for the
// reason registrationEpochFence is: the rule is shared, the NAME of a violation
// belongs to the record.
//
// The refusal carries both marks rather than only the epoch it refused on,
// because a caller that has to raise its epoch will have to satisfy the
// sequence too and one round trip is enough to learn both.
//
// The zero check is deliberately NOT here: each caller makes it before its read
// — a set through encodeSessionPointer, a clear explicitly — so an epochless
// request is refused as the caller mistake it is rather than being reported as
// whatever the read happened to find.
func pointerEpochFence(current SessionPointer, epoch uint64) error {
	return epochFence(current.LeaseEpoch, epoch, func(committed uint64) error {
		return &PointerError{
			Code:     PointerErrorEpoch,
			Field:    "lease_epoch",
			Epoch:    committed,
			Sequence: current.Sequence,
		}
	})
}

// pointerSequenceFence admits a write whose target is at least as new as the
// one already named.
//
// It is the SAME SHAPE as the epoch fence and a different fact, which is why it
// is a second function rather than a second call to the first: an equal
// sequence is admitted because one journal position can legitimately be
// captured twice, and only a strictly lower one is stale. The catalog states
// the identical rule for LastJournalSeq, and for the identical reason — a
// durable position never moves backwards.
func pointerSequenceFence(current SessionPointer, sequence uint64) error {
	if sequence < current.Sequence {
		return &PointerError{
			Code:     PointerErrorSequence,
			Field:    "sequence",
			Epoch:    current.LeaseEpoch,
			Sequence: current.Sequence,
		}
	}
	return nil
}

// readSessionPointer reads the RAW stored pointer under an already-derived
// scope: cleared ones included.
//
// It reports absence as a boolean rather than as an error because its callers
// give absence different meanings — a set creates, a read and a clear refuse —
// and an error would push that decision into a comparison against a code some
// later caller would get wrong.
//
// Everything else IS an error, and that matters more than it looks. A stored
// record this reader cannot decode is not an absent record: it is two fencing
// high-water marks that cannot be evaluated, and reporting it as absence would
// let a set create straight over it at any epoch and any sequence.
func (s *Store) readSessionPointer(
	ctx context.Context,
	scope sessionScope,
	kind SessionPointerKind,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (SessionPointerEntry, bool, error) {
	if err := s.verifySessionScope(ctx, scope); err != nil {
		return SessionPointerEntry{}, false, err
	}
	stored, err := s.backend.OrderedIndex.Get(ctx, sessionPointerID(scope, kind))
	if err != nil {
		if errors.As(err, new(*storage.OrderedRecordNotFoundError)) {
			return SessionPointerEntry{}, false, nil
		}
		return SessionPointerEntry{}, false, classifyPointerOrderedError(err, "get")
	}
	entry, err := sessionPointerEntryFor(stored, scope, kind, tenant, session)
	if err != nil {
		return SessionPointerEntry{}, false, err
	}
	return entry, true, nil
}

// createSessionPointer creates the first pointer a session has ever had of one
// role.
//
// A create that finds the identity already there is a lost race and is reported
// as a conflict carrying the current revision, rather than being turned into an
// update here. The reason is the fences: the record that arrived while this
// call was in flight carries two high-water marks this request has never been
// compared against, and evaluating them on this path would put a second copy of
// both in the file. A caller retries and meets them on the ordinary path.
func (s *Store) createSessionPointer(
	ctx context.Context,
	scope sessionScope,
	pointer SessionPointer,
	value []byte,
) (SessionPointerEntry, error) {
	stored, created, err := s.backend.OrderedIndex.Create(
		ctx, sessionPointerID(scope, pointer.Kind), scope.SessionNamespace,
		value, storage.Rank{}, sessionPointerDue(pointer))
	if err != nil {
		return SessionPointerEntry{}, classifyPointerOrderedError(err, "create")
	}
	entry, err := sessionPointerEntryFor(stored, scope, pointer.Kind, pointer.TenantID, pointer.SessionID)
	if err != nil {
		return SessionPointerEntry{}, err
	}
	if !created {
		return SessionPointerEntry{}, &PointerError{
			Code: PointerErrorConflict, Field: "create", Revision: entry.Revision}
	}
	return entry, verifySessionPointerBytes(stored, value)
}

// writeSessionPointer compare-and-swaps one pointer onto the revision its
// caller read.
func (s *Store) writeSessionPointer(
	ctx context.Context,
	scope sessionScope,
	pointer SessionPointer,
	value []byte,
	expectedRevision uint64,
) (SessionPointerEntry, error) {
	stored, err := s.backend.OrderedIndex.Update(
		ctx, sessionPointerID(scope, pointer.Kind), expectedRevision,
		value, storage.Rank{}, sessionPointerDue(pointer))
	if err != nil {
		return SessionPointerEntry{}, classifyPointerOrderedError(err, "update")
	}
	entry, err := sessionPointerEntryFor(stored, scope, pointer.Kind, pointer.TenantID, pointer.SessionID)
	if err != nil {
		return SessionPointerEntry{}, err
	}
	return entry, verifySessionPointerBytes(stored, value)
}

// verifySessionPointerBytes holds a write's reply to the bytes the write handed
// the provider.
//
// Every other check in this file holds the reply to the RECORD'S OWN bytes,
// which a substituted record satisfies exactly as well as the real one. On a
// path where this package wrote the value, the provider is claiming something
// stronger — that it stored THESE bytes — and a reply naming another object, or
// another lease's epoch, would otherwise be returned to the caller as its own
// successful write and be used as the fence a later write is measured against.
//
// The comparison is exact because canonicalization is a fixed point: the bytes
// were produced by encodeSessionPointer from a record that decodes and
// re-encodes to them.
func verifySessionPointerBytes(stored storage.OrderedRecord, value []byte) error {
	if !bytes.Equal(stored.Value, value) {
		return pointerErr(PointerErrorIdentity, "value", nil)
	}
	return nil
}

// sessionPointerID names the one ordered record per session per role.
//
// The ordering scope is the session's physical namespace, so the stable key
// needs to separate the ROLES within that session and nothing else — which is
// why it is the kind rather than the session identity the registry uses. A kind
// is a closed enum of this package's own lowercase words, so the keys are
// disjoint by construction and no caller-supplied text reaches this name.
func sessionPointerID(scope sessionScope, kind SessionPointerKind) storage.OrderedID {
	return storage.OrderedID{
		Namespace:     pointerNamespace,
		OrderingScope: scope.SessionNamespace,
		StableKey:     storage.StableKey(kind),
	}
}

// sessionPointerDue is the single definition of a pointer's due state, and like
// its siblings it is a function of the RECORD rather than of the operation
// writing it. Every write path calls it and the filing check compares against
// it, so no path can file a due state a reader cannot rebuild from the bytes.
//
// It is constant, and the constant is NOT DUE. A pointer has no deadline of any
// kind: it does not expire, nothing sweeps it, and it stops being current only
// when a later write replaces it. The rows are also permanent, because they
// hold two fences, so a due state here would put one entry per role per session
// into a deadline page that nothing may ever remove from — the head-of-line
// shape three other records in this package have had to be rescued from.
//
// CARRY-FORWARD CONTRACT: a later task that needs a pointer reconciler must
// derive its horizon HERE, from members of the record, never from the operation
// — the filing check compares the stored due against this derivation on every
// read, so an operation-derived due makes every concurrent reader fail with an
// identity error no retry can fix. It must also answer what removes a row from
// that page, because nothing here does.
func sessionPointerDue(SessionPointer) storage.Due { return storage.Due{} }

// sessionPointerEntryFor decodes one stored pointer and holds every
// provider-supplied component of its filing to what the record's own bytes say
// it should be, plus the identity the caller asked for.
//
// The enumeration, and why each entry is or is not here:
//
//   - Deleted — asserted first, and it is a fail-closed condition rather than a
//     lifecycle state. This package never calls Delete: clearing writes the
//     logical tombstone above, precisely so the two fences survive. A provider
//     tombstone means they have been physically destroyed by something outside
//     this package, and the only safe answer is to refuse every read and write
//     of that identity rather than let the next writer create a fresh record.
//   - The record's own TenantID and SessionID — held to the request, so a
//     provider returning another session's row cannot hand a caller a pointer
//     into someone else's objects.
//   - The record's own Kind — held to the ROLE the caller asked for. This is
//     the check the registry has no counterpart for, because it has one row per
//     session and this has one per role: without it a provider returning the
//     continuation row for a workspace-checkpoint read would be believed, and
//     the object kind embedded in the target is not enough on its own — two
//     roles could name one object kind.
//   - StableKey — held to the record's Kind. Not a restatement of the check
//     above: that one asks whether the BYTES are the role asked for, this one
//     asks whether the provider FILED them where it said it did.
//   - OrderingScope, RankingScope and Due — the triad every session-scoped
//     record files identically, through checkFiledScope.
//   - Rank — compared as a WHOLE VALUE against what this file files. This
//     record's views are fully determined here: it is written unranked and
//     not-due on every path, so "the provider's view state is exactly what this
//     package filed" is a complete statement rather than a partial one.
//   - Namespace is excluded for the reason the other records exclude it: a
//     package constant with no counterpart in any record, so comparing against
//     it could only restate that this file's constant equals itself. What keeps
//     the record kinds apart is that each owns one, which
//     TestOrderedNamespacesAreDistinct pins.
//   - Order is excluded. The entry does not expose it and nothing lists this
//     namespace in acceptance order.
//   - Revision is provider state with no meaning in the record; it is returned
//     for a later compare-and-swap rather than verified.
func sessionPointerEntryFor(
	stored storage.OrderedRecord,
	scope sessionScope,
	kind SessionPointerKind,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (SessionPointerEntry, error) {
	if stored.Deleted {
		return SessionPointerEntry{}, pointerErr(PointerErrorDeleted, "record", nil)
	}
	pointer, err := decodeSessionPointer(stored.Value)
	if err != nil {
		return SessionPointerEntry{}, err
	}
	if pointer.TenantID != tenant || pointer.SessionID != session {
		return SessionPointerEntry{}, pointerErr(PointerErrorIdentity, "record", nil)
	}
	if pointer.Kind != kind {
		return SessionPointerEntry{}, pointerErr(PointerErrorIdentity, "kind", nil)
	}
	if storage.StableKey(pointer.Kind) != stored.ID.StableKey {
		return SessionPointerEntry{}, pointerErr(PointerErrorIdentity, "stable_key", nil)
	}
	if err := checkFiledScope(stored, scope.SessionNamespace, sessionPointerDue(pointer), pointerIdentity); err != nil {
		return SessionPointerEntry{}, err
	}
	if stored.Rank != (storage.Rank{}) {
		return SessionPointerEntry{}, pointerErr(PointerErrorIdentity, "rank", nil)
	}
	return SessionPointerEntry{Pointer: pointer, Revision: stored.Revision}, nil
}

// classifyPointerOrderedError maps an OrderedIndex outcome into the pointer
// vocabulary while preserving the cause for errors.Is and errors.As.
//
// The arms and their origins:
//
//   - NotFound arises from an Update against a record that vanished between
//     this package's read and its compare-and-swap. It does NOT arise from the
//     Get, which readSessionPointer classifies for itself: absence is an answer
//     there rather than a failure.
//   - Deleted arises from an Update against a provider tombstone. It cannot
//     arise from Get or Create, both of which return one as a RECORD, which is
//     why sessionPointerEntryFor also classifies one.
//   - Conflict arises from an Update whose expected revision is stale, or from
//     a Create that lost the identity race, carrying the provider's actual
//     revision when it disclosed one. It is the code a racing writer retries
//     on, and the only one it should.
//   - Unknown is an ambiguous mutation, the one outcome that says nothing at
//     all about what is stored — and here that means nothing about whether
//     either fence advanced.
//
// A cursor failure and a limit failure have no arm: this file issues no
// listing. An oversized value is refused by encodeSessionPointer before the
// provider can see it, which the unsigned constant above pins. Revision
// exhaustion falls through to Backend deliberately, as it does for every other
// record kind here.
func classifyPointerOrderedError(err error, field string) error {
	var notFound *storage.OrderedRecordNotFoundError
	if errors.As(err, &notFound) {
		return pointerErr(PointerErrorNotFound, field, err)
	}
	var deleted *storage.OrderedDeletedError
	if errors.As(err, &deleted) {
		return pointerErr(PointerErrorDeleted, field, err)
	}
	var conflict *storage.OrderedRevisionConflictError
	if errors.As(err, &conflict) {
		return &PointerError{Code: PointerErrorConflict, Field: field, Revision: conflict.ActualRevision, Cause: err}
	}
	var ambiguous *storage.OrderedAmbiguousError
	if errors.As(err, &ambiguous) {
		return pointerErr(PointerErrorUnknown, field, err)
	}
	return pointerErr(PointerErrorBackend, field, err)
}
