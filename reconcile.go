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

// A RECONCILIATION CLAIM SUPPRESSES DUPLICATE WORK AND IS NEVER A FENCE.
//
// Any Factory replica may reconcile any session. Before it performs placement
// it takes a short-lived claim on the session, so the other replicas that
// noticed the same due work do something else instead of scaling the same
// session several times over. Claim expiry recovers a crashed replica.
//
// What makes concurrent reconcilers SAFE is not this record. It is
// deterministic command IDs, idempotent desired state, and the Host lease —
// three mechanisms that already hold with no claim in sight. This record only
// makes them cheaper to rely on.
//
// That distinction is enforced structurally rather than documented, in three
// ways a later change has to break on purpose:
//
//  1. The record CANNOT NAME ownership. There is no lease epoch, no HostID, no
//     endpoint, no residency and no journal position on it, and no epoch member
//     on its error type, so a caller cannot read authority out of a claim it
//     holds or a refusal it receives. HolderID is a plain string rather than a
//     sessionwire identity, so a HostID cannot be passed for it by accident.
//  2. NOTHING ELSE IN THIS PACKAGE READS A CLAIM. No other operation takes one,
//     checks one, or refuses without one — a claim licenses nothing here, so
//     there is nothing for it to be mistaken for. That is a property of the
//     package rather than of this file, and a test parses every other
//     production file to keep it one.
//  3. A claim is not required to do the work. A replica that ignores this
//     record entirely produces correct results and merely duplicates effort.
//
// The record's SHAPE follows the Host registry's, and for the same reasons: one
// row per session, filed in the session namespace, unranked, never due, read
// and written only by name. It is never deleted — this package writes no
// provider tombstones, because a tombstone is a fail-closed condition on every
// read path here — so a session that has been reconciled once keeps a small
// permanent row. The registry can afford that because the row IS a permanent
// fence; this one can afford it because the reader cost is nil (a direct Get,
// never a page) and because a lapsed claim needs no cleanup to stop being a
// claim. There is no sweep, and therefore none of the head-of-line hazards a
// due view brings.

// reconcileNamespace is the one OrderedIndex namespace holding per-session
// reconciliation claims. It is namespace-distinct from every other record kind
// for the reason TestOrderedNamespacesAreDistinct states: a namespace is the
// unit a decoder is chosen for.
//
// It is deliberately NOT the registry's namespace even though both records are
// keyed by session and neither is ranked. The registry row is permanent
// authority; this one is transient advice, and sharing a partition would put
// two codecs and two lifetimes into one stream.
const reconcileNamespace = "sessionstore/reconcile"

const (
	// ReconciliationClaimRecordVersion is the independent version of the stored
	// claim. A reader fails closed on any other version rather than guessing
	// which members a future encoder meant.
	ReconciliationClaimRecordVersion uint8 = 1

	// MaxReconciliationClaimRecordBytes bounds an encoded claim. The record has
	// no open-ended member — it is three identities and two instants — but the
	// ceiling still has to allow for JSON ESCAPING, which is what sizes it: an
	// identity is any valid UTF-8 of at most sessionwire.MaxIDBytes bytes,
	// control characters included, and Go escapes each of those as \u00XX, six
	// bytes for one. TestLargestAcceptableReconciliationClaimFitsTheBound
	// builds that worst case and reports what it measures, because a bound
	// measured with ASCII is a sixth of what it claims to be.
	//
	// Like the other bounds it sits below storage.MaxOrderedValueBytes, so a
	// record this package accepts always fits in the provider and there is no
	// state that can be written but not rewritten.
	MaxReconciliationClaimRecordBytes = 16 << 10
)

// Stated as an unsigned constant for the reason the other records state theirs:
// an oversized record is refused here rather than by the provider, and prose
// cannot enforce that relationship.
const _ = uint(storage.MaxOrderedValueBytes - MaxReconciliationClaimRecordBytes)

// MaxReconciliationClaimTTL bounds how far ahead of the store's clock a caller
// may place a claim's expiry.
//
// It exists because an over-long claim is a durable liveness fault one caller
// can commit alone, and the shape of the damage is the one MaxCommandClaimTTL
// describes: nothing removes a claim, and every other replica declines to
// reconcile the session for as long as it lasts. Unbounded, "as long as it
// lasts" is bounded only by rankableTime, which is centuries — one replica with
// a skewed clock takes a session out of reconciliation for the life of the
// deployment.
//
// The consequence is delay rather than incorrectness, because the claim is not
// a fence and a replica that decided to reconcile anyway would still be safe.
// That is exactly why the bound has to be here rather than in a caller's
// policy: the failure is invisible, so nothing would ever report it.
//
// It is a ceiling on caller error and clock skew, not a policy TTL. A
// reconciler chooses its own TTL far below this, and five minutes is not a
// recommendation.
const MaxReconciliationClaimTTL = 5 * time.Minute

// ReconciliationClaim is the durable record of which Factory replica is
// currently doing reconciliation work for one session, and until when.
//
// HolderID names the replica, opaquely. It is the only thing a later write is
// compared against, and it is not authority: two replicas that chose the same
// holder string are indistinguishable here, which costs exactly the duplicate
// work this record exists to reduce and costs nothing else.
//
// ClaimedAt is the store's own clock reading at the moment the claim was
// accepted; ExpiresAt is the holder's promise about when it will be finished.
// The two are stamped from different clocks on purpose. A caller-supplied claim
// instant could be placed after its own expiry, or before a takeover that has
// already happened, and nothing could tell; the horizon must be the caller's
// because only the caller knows how long its work takes, and it is bounded
// above for that reason.
//
// A claim whose expiry EQUALS its claim instant has lapsed on arrival, which is
// exactly what ReleaseReconciliationClaim writes. That is not a second state
// needing a marker of its own: "released" and "expired" are the same fact to
// every reader — no live claim — and giving them one spelling means no reader
// has to know which one it is looking at.
type ReconciliationClaim struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	HolderID string

	ClaimedAt time.Time
	ExpiresAt time.Time
}

// ReconciliationClaimEntry is a claim together with the revision a later
// compare-and-swap names.
//
// The provider's immutable acceptance order is deliberately not exposed, for
// the reason HostRegistrationEntry's is not: a session's position in a stream
// of claims is not a fact any caller acts on.
type ReconciliationClaimEntry struct {
	Claim    ReconciliationClaim
	Revision uint64
}

// claimHeldAt reports whether the claim is live at now.
//
// The interval is half-open — a claim holds up to but not including its expiry
// — which is the convention every deadline in this package uses, and it is what
// makes a released claim (expiry equal to claim instant) unheld under every
// clock reading rather than under most of them.
//
// It is a function of the RECORD and the instant, and it is the only statement
// of that rule: acquisition, release and reading all call it, so none of them
// can drift into its own idea of when a claim has run out.
//
// IT IS A COMPARISON OF INSTANTS AND NOTHING ELSE, which is where this record
// deliberately differs from the Host registry. routableAt refuses a released
// registration on its STRUCTURE, before any instant is compared, so a tombstone
// is unroutable even under a clock reading earlier than the instant it was
// written at. A released claim here has no structural marker, so under such a
// clock it reads as live until the clock catches up.
//
// That difference is a decision rather than an omission, and the argument is
// worth writing down because the obvious fix buys nothing. A backwards clock
// makes EVERY lapsed claim read as live — an expiry is an instant, and there is
// no structure that could make an ordinary lapse clock-independent — so a
// released-claim marker would close one of two identical windows and leave the
// other, which is the larger one. What the exposure costs is also different in
// kind: the registry's tombstone guards a route to a process that may be gone,
// while this one costs a delayed takeover, bounded by the skew, of work that no
// replica needed a claim to do. TestAReleasedClaimIsNotClockIndependent drives
// the scenario rather than leaving this paragraph to stand for it.
func claimHeldAt(claim ReconciliationClaim, now time.Time) bool {
	return now.Before(claim.ExpiresAt)
}

// reconcileWire is the stored JSON shape.
type reconcileWire struct {
	RecordVersion uint8                 `json:"record_version"`
	TenantID      sessionwire.TenantID  `json:"tenant_id"`
	SessionID     sessionwire.SessionID `json:"session_id"`
	HolderID      string                `json:"holder_id"`
	ClaimedAt     time.Time             `json:"claimed_at"`
	ExpiresAt     time.Time             `json:"expires_at"`
}

// encodeReconciliationClaim validates and encodes one claim. It refuses a
// record above this package's bound here rather than letting the provider
// refuse it, so a record this package accepted can always be rewritten.
//
// It returns the CANONICAL claim beside the bytes, as the registry's and the
// inbox's encoders do, and a caller must carry that one forward rather than the
// request-shaped value it passed in: reconciliationClaimDue's carry-forward
// contract is that a due horizon is derived from the record, and deriving one
// from a record the stored bytes are not in is exactly the divergence that
// makes every concurrent reader fail.
func encodeReconciliationClaim(claim ReconciliationClaim) ([]byte, ReconciliationClaim, error) {
	claim, err := canonicalReconciliationClaim(claim)
	if err != nil {
		return nil, ReconciliationClaim{}, err
	}
	encoded, err := json.Marshal(reconcileWire{
		RecordVersion: ReconciliationClaimRecordVersion,
		TenantID:      claim.TenantID,
		SessionID:     claim.SessionID,
		HolderID:      claim.HolderID,
		ClaimedAt:     claim.ClaimedAt,
		ExpiresAt:     claim.ExpiresAt,
	})
	if err != nil {
		return nil, ReconciliationClaim{}, reconcileErr(ReconcileErrorInvalid, "record", err)
	}
	if len(encoded) > MaxReconciliationClaimRecordBytes {
		return nil, ReconciliationClaim{}, reconcileErr(ReconcileErrorTooLarge, "record", nil)
	}
	return encoded, claim, nil
}

// decodeReconciliationClaim strictly decodes one stored claim and re-validates
// it, so a record corrupted in place cannot be handed to a caller.
//
// Strictness reaches every member: this record is entirely this package's own
// shape, with no additive sessionwire projection inside it, so an undeclared
// member is a corrupted record rather than a newer writer.
func decodeReconciliationClaim(value []byte) (ReconciliationClaim, error) {
	wire, err := decodeVersionedRecord[reconcileWire](
		value, MaxReconciliationClaimRecordBytes, ReconciliationClaimRecordVersion,
		versionedRecordFields{Record: "record", Version: "record_version"}, reconcileRecordFailure)
	if err != nil {
		return ReconciliationClaim{}, err
	}
	return canonicalReconciliationClaim(ReconciliationClaim{
		TenantID:  wire.TenantID,
		SessionID: wire.SessionID,
		HolderID:  wire.HolderID,
		ClaimedAt: wire.ClaimedAt,
		ExpiresAt: wire.ExpiresAt,
	})
}

// canonicalReconciliationClaim validates a claim and returns its one canonical
// spelling: UTC instants and nothing else to normalize. Encoding and decoding
// both end here, so a record read back is byte-identical to the record written
// and two encoders cannot disagree.
//
// The expiry may not fall BEFORE the claim instant. A claim that ran out before
// it was taken is not a state any writer here produces — a release writes the
// two equal, and an acquisition bounds the expiry against the same clock
// reading it stamps the claim instant with — so accepting one would be
// accepting a record only a corrupted store could hold. Equality is admitted
// because that IS the released spelling.
//
// The instant bounds are this package's own, as they are for every other
// record: a year Go's JSON encoder cannot spell would otherwise be refused at
// Marshal with an untyped failure rather than here.
//
// Note what is NOT validated here: whether the expiry is in the store's future,
// and whether it is within MaxReconciliationClaimTTL. Those are rules about a
// REQUEST measured against the store's clock, and a stored claim legitimately
// violates both the moment it lapses. Stating them here would make every claim
// undecodable the instant it ran out.
func canonicalReconciliationClaim(claim ReconciliationClaim) (ReconciliationClaim, error) {
	if err := claim.TenantID.Validate(); err != nil {
		return ReconciliationClaim{}, reconcileErr(ReconcileErrorInvalid, "tenant_id", err)
	}
	if err := claim.SessionID.Validate(); err != nil {
		return ReconciliationClaim{}, reconcileErr(ReconcileErrorInvalid, "session_id", err)
	}
	if err := validateOpaque(claim.HolderID, "holder_id", reconcileInvalid); err != nil {
		return ReconciliationClaim{}, err
	}
	if !rankableTime(claim.ClaimedAt) {
		return ReconciliationClaim{}, reconcileErr(ReconcileErrorInvalid, "claimed_at", nil)
	}
	if !rankableTime(claim.ExpiresAt) {
		return ReconciliationClaim{}, reconcileErr(ReconcileErrorInvalid, "expires_at", nil)
	}
	if claim.ExpiresAt.Before(claim.ClaimedAt) {
		return ReconciliationClaim{}, reconcileErr(ReconcileErrorInvalid, "expires_at", nil)
	}
	claim.ClaimedAt = claim.ClaimedAt.UTC()
	claim.ExpiresAt = claim.ExpiresAt.UTC()
	return claim, nil
}

// AcquireReconciliationClaimRequest takes or extends the claim on one session.
//
// HolderID is the calling replica's own identity. ExpiresAt is that replica's
// promise about when it will be done, and it must lie in the store's future and
// within MaxReconciliationClaimTTL of it.
//
// There is no claim instant and no expected revision. The claim instant is the
// store's, for the reason ReconciliationClaim states; the revision is not the
// caller's business because a claim is not a decision about a record the caller
// has read — it is "am I the one doing this?", and the answer is decided by the
// holder and the clock, closed by a compare-and-swap this store reads for
// itself.
//
// There is deliberately no lease epoch. Requiring one would say that holding a
// session's lease is relevant to doing scaling work for it, which is exactly
// backwards: reconciliation runs when NO Host owns the session.
type AcquireReconciliationClaimRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	HolderID  string
	ExpiresAt time.Time
}

// GetReconciliationClaimRequest reads one session's current claim.
type GetReconciliationClaimRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
}

// ReleaseReconciliationClaimRequest gives one session's claim back early.
//
// It carries no timestamp, and that is deliberate rather than an omission, for
// the reason ClearHostRegistrationRequest carries none: a released claim's
// instants record that THIS STORE released it, and a caller-supplied one could
// place the release in the future, producing a record that reads as released by
// intent and as live by time.
type ReleaseReconciliationClaimRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	HolderID string
}

// AcquireReconciliationClaim takes the session's claim, or extends the caller's
// own.
//
// Three cases, and the middle one is the whole operation:
//
//   - No record: the claim is created. A create that finds the identity already
//     there is a lost race, reported as a conflict so the caller re-reads and
//     meets the live claim on the ordinary path.
//   - A live claim held by SOMEONE ELSE: refused with the horizon, and NOTHING
//     IS WRITTEN. A losing replica that rewrote the row would restamp the
//     winner's claim under its own name, which is the one way this record could
//     take work away from the replica actually doing it.
//   - Anything else — a lapsed claim, or the caller's own claim, live or not:
//     taken, in one compare-and-swap. Extending one's own live claim and taking
//     over a crashed replica's lapsed one are the same write, because the
//     record does not distinguish them and nothing downstream needs to.
//
// The clock is read ONCE, before the provider read, and both the bound and the
// stored claim instant come from that reading. A second reading taken after the
// read could be later than the instant the record was evaluated at, which would
// start the machine erring toward taking claims rather than leaving them.
func (s *Store) AcquireReconciliationClaim(
	ctx context.Context,
	req AcquireReconciliationClaimRequest,
) (ReconciliationClaimEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return ReconciliationClaimEntry{}, err
	}
	now := s.clock.Now()
	// Encoding validates, so an invalid request is refused before any provider
	// work — including before the session's witnesses are bound. It returns the
	// CANONICAL claim over the request-shaped one, so nothing below can reach a
	// form the stored bytes are not in.
	value, claim, err := encodeReconciliationClaim(ReconciliationClaim{
		TenantID:  req.TenantID,
		SessionID: req.SessionID,
		HolderID:  req.HolderID,
		ClaimedAt: now,
		ExpiresAt: req.ExpiresAt,
	})
	if err != nil {
		return ReconciliationClaimEntry{}, err
	}
	if err := validateBoundedExpiry(
		claim.ExpiresAt, now, MaxReconciliationClaimTTL, "expires_at", reconcileInvalid); err != nil {
		return ReconciliationClaimEntry{}, err
	}

	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return ReconciliationClaimEntry{}, err
	}
	defer release()
	// A claim is durable session data and may be the first record a session
	// has, so taking one binds the session's collision witnesses exactly as
	// publishing a registration does. The binding is create-only and idempotent.
	if err := s.bindSessionScope(opCtx, scope); err != nil {
		return ReconciliationClaimEntry{}, err
	}

	current, found, err := s.readReconciliationClaim(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return ReconciliationClaimEntry{}, err
	}
	if !found {
		return s.createReconciliationClaim(opCtx, scope, claim, value)
	}
	if current.Claim.HolderID != req.HolderID && claimHeldAt(current.Claim, now) {
		return ReconciliationClaimEntry{}, &ReconcileError{
			Code: ReconcileErrorHeld, Field: "holder_id", ExpiresAt: current.Claim.ExpiresAt}
	}
	return s.writeReconciliationClaim(opCtx, scope, claim, value, current.Revision)
}

// GetReconciliationClaim returns one session's claim, and returns it only while
// it is a claim.
//
// A lapsed claim reports Lapsed without the record, which is the same
// discipline GetHostRegistration applies to an expired route: a caller is never
// handed state this store will not vouch for, so it cannot act on a horizon
// that has already passed. The holder of a lapsed claim is not withheld to
// protect anything — it is withheld because it is not an answer to the question
// this operation asks.
//
// It verifies the session's collision witnesses before any provider read, so a
// derived name is never trusted on its own.
func (s *Store) GetReconciliationClaim(
	ctx context.Context,
	req GetReconciliationClaimRequest,
) (ReconciliationClaimEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return ReconciliationClaimEntry{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return ReconciliationClaimEntry{}, err
	}
	defer release()

	// The clock is read before the provider read, as it is in every other
	// operation here and for the reason inbox_claim.go states: a reading taken
	// afterwards could be later than the instant the record was read at, so the
	// liveness answer would be about a moment the record was never evaluated in.
	now := s.clock.Now()
	entry, found, err := s.readReconciliationClaim(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return ReconciliationClaimEntry{}, err
	}
	if !found {
		return ReconciliationClaimEntry{}, reconcileErr(ReconcileErrorNotFound, "record", nil)
	}
	if !claimHeldAt(entry.Claim, now) {
		return ReconciliationClaimEntry{}, reconcileErr(ReconcileErrorLapsed, "expires_at", nil)
	}
	return entry, nil
}

// ReleaseReconciliationClaim gives the caller's own claim back, so the next
// replica need not wait out the TTL.
//
// It writes a claim whose expiry EQUALS its claim instant, which has lapsed on
// arrival. It does not delete the row: this package writes no provider
// tombstones, and a lapsed claim is already indistinguishable from no claim to
// every reader.
//
// A REPEAT IS A SUCCESS THAT WRITES NOTHING. A caller cannot tell a lost reply
// from a failure, so a second release is the ordinary case rather than a
// mistake, and answering it with an error would make every retry look like a
// claim that had expired mid-work. The repeat condition is "the record is the
// caller's and is not live", which a lapsed-by-timeout claim also satisfies —
// so a holder that overran its TTL is told its release succeeded. That loses a
// signal, and it is the right trade precisely because the claim licensed
// nothing: nothing the holder did was authorized by the claim, so nothing it
// did becomes wrong when the claim runs out.
//
// A claim that is NOT the caller's is refused either way, and the two codes are
// different facts rather than one fact with two names: Held means another
// replica is working now, so wait; Lapsed means nobody is, so there is nothing
// to release and no reason to wait. Telling a caller "held" for a claim that
// had run out would make it back off for a horizon already in the past.
func (s *Store) ReleaseReconciliationClaim(
	ctx context.Context,
	req ReleaseReconciliationClaimRequest,
) (ReconciliationClaimEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return ReconciliationClaimEntry{}, err
	}
	if err := validateOpaque(req.HolderID, "holder_id", reconcileInvalid); err != nil {
		return ReconciliationClaimEntry{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return ReconciliationClaimEntry{}, err
	}
	defer release()

	now := s.clock.Now()
	current, found, err := s.readReconciliationClaim(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return ReconciliationClaimEntry{}, err
	}
	if !found {
		return ReconciliationClaimEntry{}, reconcileErr(ReconcileErrorNotFound, "record", nil)
	}
	if current.Claim.HolderID != req.HolderID {
		if claimHeldAt(current.Claim, now) {
			return ReconciliationClaimEntry{}, &ReconcileError{
				Code: ReconcileErrorHeld, Field: "holder_id", ExpiresAt: current.Claim.ExpiresAt}
		}
		return ReconciliationClaimEntry{}, reconcileErr(ReconcileErrorLapsed, "holder_id", nil)
	}
	if !claimHeldAt(current.Claim, now) {
		return current, nil
	}
	value, released, err := encodeReconciliationClaim(ReconciliationClaim{
		TenantID:  req.TenantID,
		SessionID: req.SessionID,
		HolderID:  req.HolderID,
		ClaimedAt: now,
		ExpiresAt: now,
	})
	if err != nil {
		return ReconciliationClaimEntry{}, err
	}
	return s.writeReconciliationClaim(opCtx, scope, released, value, current.Revision)
}

// readReconciliationClaim reads the RAW stored claim under an already-derived
// scope: lapsed ones, released ones, and no others.
//
// It reports absence as a boolean rather than as an error because its callers
// give absence three different meanings — an acquisition creates, a read and a
// release refuse — and an error would push that decision into a comparison
// against a code some later caller would get wrong.
//
// Everything else IS an error. A stored claim this reader cannot decode is not
// an absent claim, and reporting it as absence would let a write create straight
// over a row it could not evaluate.
func (s *Store) readReconciliationClaim(
	ctx context.Context,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (ReconciliationClaimEntry, bool, error) {
	if err := s.verifySessionScope(ctx, scope); err != nil {
		return ReconciliationClaimEntry{}, false, err
	}
	stored, err := s.backend.OrderedIndex.Get(ctx, reconciliationClaimID(scope, session))
	if err != nil {
		if errors.As(err, new(*storage.OrderedRecordNotFoundError)) {
			return ReconciliationClaimEntry{}, false, nil
		}
		return ReconciliationClaimEntry{}, false, classifyReconcileOrderedError(err, "get")
	}
	entry, err := reconciliationClaimEntryFor(stored, scope, tenant, session)
	if err != nil {
		return ReconciliationClaimEntry{}, false, err
	}
	return entry, true, nil
}

// createReconciliationClaim creates the first claim a session has ever had.
//
// A create that finds the identity already there is a lost race and is reported
// as a conflict carrying the current revision, rather than being turned into an
// update here. The record that arrived while this call was in flight has a
// holder this request has never been compared against, and evaluating it on
// this path would put a second copy of the holder rule in the file. A caller
// retries and meets it on the ordinary path.
func (s *Store) createReconciliationClaim(
	ctx context.Context,
	scope sessionScope,
	claim ReconciliationClaim,
	value []byte,
) (ReconciliationClaimEntry, error) {
	stored, created, err := s.backend.OrderedIndex.Create(
		ctx, reconciliationClaimID(scope, claim.SessionID), scope.SessionNamespace,
		value, storage.Rank{}, reconciliationClaimDue(claim))
	if err != nil {
		return ReconciliationClaimEntry{}, classifyReconcileOrderedError(err, "create")
	}
	entry, err := reconciliationClaimEntryFor(stored, scope, claim.TenantID, claim.SessionID)
	if err != nil {
		return ReconciliationClaimEntry{}, err
	}
	if !created {
		return ReconciliationClaimEntry{}, &ReconcileError{
			Code: ReconcileErrorConflict, Field: "create", Revision: entry.Revision}
	}
	return entry, verifyReconciliationClaimBytes(stored, value)
}

// writeReconciliationClaim compare-and-swaps one claim onto the revision its
// caller read.
func (s *Store) writeReconciliationClaim(
	ctx context.Context,
	scope sessionScope,
	claim ReconciliationClaim,
	value []byte,
	expectedRevision uint64,
) (ReconciliationClaimEntry, error) {
	stored, err := s.backend.OrderedIndex.Update(
		ctx, reconciliationClaimID(scope, claim.SessionID), expectedRevision,
		value, storage.Rank{}, reconciliationClaimDue(claim))
	if err != nil {
		return ReconciliationClaimEntry{}, classifyReconcileOrderedError(err, "update")
	}
	entry, err := reconciliationClaimEntryFor(stored, scope, claim.TenantID, claim.SessionID)
	if err != nil {
		return ReconciliationClaimEntry{}, err
	}
	return entry, verifyReconciliationClaimBytes(stored, value)
}

// verifyReconciliationClaimBytes holds a write's reply to the bytes the write
// handed the provider.
//
// Every other check here holds the reply to the RECORD'S OWN bytes, which a
// substituted record satisfies exactly as well as the real one. On a path where
// this package wrote the value, the provider is claiming something stronger —
// that it stored THESE bytes — and a reply carrying another replica's claim
// would otherwise be returned to this caller as its own successful acquisition,
// which is the one outcome that would have two replicas both believing they had
// won.
//
// The comparison is exact because canonicalization is a fixed point: the bytes
// were produced by encodeReconciliationClaim from a claim that decodes and
// re-encodes to them.
func verifyReconciliationClaimBytes(stored storage.OrderedRecord, value []byte) error {
	if !bytes.Equal(stored.Value, value) {
		return reconcileErr(ReconcileErrorIdentity, "value", nil)
	}
	return nil
}

// reconciliationClaimID names the one ordered record per session. The ordering
// scope is the session's physical namespace and the stable key is the raw
// SessionID: an opaque provider-verified value, not a name. A provider that
// cannot place those bytes in a path or subject hashes them and stores the
// original for the verification reconciliationClaimEntryFor performs.
func reconciliationClaimID(scope sessionScope, session sessionwire.SessionID) storage.OrderedID {
	return storage.OrderedID{
		Namespace:     reconcileNamespace,
		OrderingScope: scope.SessionNamespace,
		StableKey:     storage.StableKey(session),
	}
}

// reconciliationClaimDue is the single definition of a claim's due state, and
// like inboxDue and hostRegistrationDue it is a function of the RECORD rather
// than of the operation writing it. Every write path calls it and the filing
// check compares against it, so no path can file a due state a reader cannot
// rebuild from the bytes.
//
// It is constant, and the constant is NOT DUE, which is a decision about
// accumulation rather than an oversight:
//
//   - Nothing needs to sweep a lapsed claim. It stops being a claim at its
//     expiry, from its own bytes, at the instant a reader asks — which is the
//     only moment the answer matters — and the next acquisition overwrites it.
//     A sweep would delete a row that is about to be reused.
//   - The rows are permanent, because this package writes no provider
//     tombstones. A due state at the expiry would therefore put one entry per
//     session ever reconciled into a deadline page that nothing removes from,
//     and every reconciler pass would walk them. That is the head-of-line shape
//     three other records in this package have already had to be rescued from.
//
// CARRY-FORWARD CONTRACT: a later task that needs a claim reconciler must
// derive its horizon HERE, from members of the record, never from the operation
// — inboxDue states what an operation-derived due costs: the filing check
// compares the stored due against this derivation on every read, so every
// concurrent reader would fail with an identity error no retry can fix. It must
// also answer what removes a row from that page, because nothing here does.
func reconciliationClaimDue(ReconciliationClaim) storage.Due { return storage.Due{} }

// reconciliationClaimEntryFor decodes one stored claim and holds every
// provider-supplied component of its filing to what the record's own bytes say
// it should be, plus the identity the caller asked for.
//
// The enumeration, and why each entry is or is not here:
//
//   - Deleted — a fail-closed condition rather than a lifecycle state. This
//     package never calls Delete on this namespace: releasing a claim writes
//     the lapsed record above. A provider tombstone therefore means something
//     outside this package destroyed the row, and the only safe answer is to
//     refuse rather than let the next writer create over whatever else it did.
//   - The record's own TenantID and SessionID — held to the request, so a
//     provider returning another session's row cannot tell this caller that
//     someone holds a claim on a session it did not ask about.
//   - StableKey — held to the record's SessionID. Not a restatement of the
//     check above: that one asks whether the BYTES are the session asked for,
//     this one asks whether the provider FILED them where it said it did.
//   - OrderingScope, RankingScope and Due — the triad every session-scoped
//     record files identically, through checkFiledScope.
//   - Rank — compared as a WHOLE VALUE against what this file files, as the
//     registry's is. This record's views are fully determined here: it is
//     written unranked and not-due on every path, so "the provider's view state
//     is exactly what this package filed" is a complete statement. A rank on
//     one of these rows means a provider inventing view state.
//   - Namespace is excluded for the reason the other records exclude it: a
//     package constant with no counterpart in any record, so comparing against
//     it could only restate that this file's constant equals itself.
//   - Order is excluded. The entry does not expose it and nothing lists this
//     namespace in acceptance order.
//   - Revision is provider state with no meaning in the record; it is returned
//     for a later compare-and-swap rather than verified.
func reconciliationClaimEntryFor(
	stored storage.OrderedRecord,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (ReconciliationClaimEntry, error) {
	if stored.Deleted {
		return ReconciliationClaimEntry{}, reconcileErr(ReconcileErrorDeleted, "record", nil)
	}
	claim, err := decodeReconciliationClaim(stored.Value)
	if err != nil {
		return ReconciliationClaimEntry{}, err
	}
	if claim.TenantID != tenant || claim.SessionID != session {
		return ReconciliationClaimEntry{}, reconcileErr(ReconcileErrorIdentity, "record", nil)
	}
	if storage.StableKey(claim.SessionID) != stored.ID.StableKey {
		return ReconciliationClaimEntry{}, reconcileErr(ReconcileErrorIdentity, "session_id", nil)
	}
	if err := checkFiledScope(stored, scope.SessionNamespace, reconciliationClaimDue(claim), reconcileIdentity); err != nil {
		return ReconciliationClaimEntry{}, err
	}
	if stored.Rank != (storage.Rank{}) {
		return ReconciliationClaimEntry{}, reconcileErr(ReconcileErrorIdentity, "rank", nil)
	}
	return ReconciliationClaimEntry{Claim: claim, Revision: stored.Revision}, nil
}

// classifyReconcileOrderedError maps an OrderedIndex outcome into the claim
// vocabulary while preserving the cause for errors.Is and errors.As.
//
// The arms and their origins:
//
//   - NotFound arises from an Update against a record that vanished between
//     this package's read and its compare-and-swap. It does NOT arise from the
//     Get, which readReconciliationClaim classifies for itself: absence is an
//     answer there rather than a failure.
//   - Deleted arises from an Update against a provider tombstone. It cannot
//     arise from Get or Create, both of which return one as a RECORD, which is
//     why reconciliationClaimEntryFor also classifies one.
//   - Conflict arises from an Update whose expected revision is stale, or from
//     a Create that lost the identity race, carrying the provider's actual
//     revision when it disclosed one. It is the code a racing replica retries
//     on, and the only one it should.
//   - Unknown is an ambiguous mutation, the one outcome that says nothing at all
//     about what is stored — so a caller does not know whether it holds the
//     claim, and must re-read rather than assume either way.
//
// A cursor failure and a limit failure have no arm: this file issues no
// listing. An oversized value is refused by encodeReconciliationClaim before
// the provider can see it, which the unsigned constant above pins.
func classifyReconcileOrderedError(err error, field string) error {
	var notFound *storage.OrderedRecordNotFoundError
	if errors.As(err, &notFound) {
		return reconcileErr(ReconcileErrorNotFound, field, err)
	}
	var deleted *storage.OrderedDeletedError
	if errors.As(err, &deleted) {
		return reconcileErr(ReconcileErrorDeleted, field, err)
	}
	var conflict *storage.OrderedRevisionConflictError
	if errors.As(err, &conflict) {
		return &ReconcileError{Code: ReconcileErrorConflict, Field: field, Revision: conflict.ActualRevision, Cause: err}
	}
	var ambiguous *storage.OrderedAmbiguousError
	if errors.As(err, &ambiguous) {
		return reconcileErr(ReconcileErrorUnknown, field, err)
	}
	return reconcileErr(ReconcileErrorBackend, field, err)
}
