package sessionstore

import (
	"context"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// This file is the disposition protocol's two remaining record edges:
//
//	pending -> claimed                        (ClaimDispositionCommand)
//	pending | claimed -> rejected             (RejectDispositionCommand)
//
// disposition_settlement.go adds the other two, and its header explains the
// compare-and-swap machinery both files share. Everything said there about what
// surrounds a transition — the immutable catalog binding, the protocol-mode
// fence that may WRITE its create-only witness, then the inbox read and the
// revision compare-and-swap — is true of these two as well and is not restated.
//
// # These are NOT inbox_claim.go's edges with different names
//
// The legacy lifecycle is Claim -> BeginApplying -> Complete | Reject, and a
// faithful-looking port of it here would be wrong in three specific ways. Each
// is a deliberate divergence rather than an omission.
//
// FIRST: THERE IS NO CALLER-AUTHORED REJECTION AFTER AN ATTEMPT. Legacy
// RejectCommand settles a claimed OR AN APPLYING command, and it is admitted to
// the applying case by a journal correlation it performs itself. In this
// protocol the store decides a terminal arm only from evidence a configured
// reader obtained, and a runtime error with no durable disposition leaves the
// command APPLYING until a successor runtime writes not_applied under a
// strictly later journal grant. So RejectDispositionCommand refuses any record
// that carries an attempt — including a terminal one — and that refusal is the
// edge's defining property rather than a precondition it happens to check.
//
// SECOND: THIS REJECTION HAS NO JOURNAL SCAN AND NEEDS NONE. Legacy rejection
// walks the session's stream unconditionally, because its own state machine
// cannot tell it whether an effect committed. Here the store's own record
// answers: dispatch is forbidden without a durably authorized attempt, so a
// record with no attempt has had no dispatch, and there is no effect for a
// rejection to orphan. The absence of the scan is therefore the FIRST rule
// restated, not a weakening of the second.
//
// THIRD: THE REJECTION CARRIES NO REASON. Legacy stores a typed
// sessionwire.ErrorDetail; this record has no member for one, and adding a
// durable member would require a DispositionInboxRecordVersion bump because
// decoding demands exact canonical re-encoding. The shape produced here is
// exactly the one the codec has always accepted and nothing has ever written —
// terminal rejected, no attempt, no outcome. What a caller loses is stated
// plainly: the record says a command was refused before dispatch and does not
// say why, and a consumer needing the reason must carry it outside this record
// until a version bump adds one.
//
// # Three authorities, and which one each edge needs
//
// The residency epoch guards claim and attempt, the runtime's journal epoch
// guards application, and a revision compare-and-swap guards the write. Neither
// edge here touches a journal epoch, and neither derives one from the other.
//
// ClaimDispositionCommand REQUIRES A RESIDENCY GRANT — the *ResidencyGrant
// AcquireResidency returned, not a number — because it is the only producer of
// the record's ratcheting high-water mark. See ClaimDispositionCommandRequest
// for why that one edge is gated when the others are not. A claim with no
// residency is unstorable in any case (validateDispositionClaim refuses it) and
// could not compose with BeginDispositionAttempt, which admits only the claim's
// own residency and refuses zero itself.
//
// RejectDispositionCommand's residency is OPTIONAL, for the reason legacy
// RejectCommandRequest gives: rejecting a command is also what a reconciling
// replica does to one that has run out of deadline, and such a replica may
// never have held a residency. Zero is the honest statement "I am not acting
// under a residency", and it buys exactly the authority the record grants that
// caller — it may settle a command nobody is working on, and nothing else. A
// NONZERO epoch is a consistency check on a view the caller asserts, not an
// authority boundary: nothing forces a caller to name one.
//
// # What this file does not do
//
// It does not read or write a journal, does not apply anything, and writes no
// outcome. Neither edge is reachable for a legacy session. Reaping, retention
// and gate continuation remain unimplemented.

// ClaimDispositionCommandRequest takes a short-lived claim on one admitted
// disposition command, which is the state BeginDispositionAttempt starts from.
//
// ExpectedRevision is the revision the caller read and decided on, as every
// compare-and-swap in this package requires. ClaimExpiresAt is the caller's own
// reading of when the claim lapses, evaluated against the STORE's clock, and it
// is held to MaxCommandClaimTTL for that constant's stated reason. The claim's
// expiry MAY fall after the command's apply deadline — that is what lets an
// unexpired claim win the deadline race — but it may not fall in the past.
//
// # Residency is a GRANT and not a number, and that is the load-bearing choice
//
// Every other residency-taking API in this package takes a bare
// ResidencyEpoch. This one takes the *ResidencyGrant that AcquireResidency
// returned, and the epoch is read off it. The caller cannot name an epoch at
// all.
//
// The reason is that THIS EDGE IS THE ONLY PRODUCER OF THE RECORD'S HIGH-WATER
// MARK, and that mark only ever rises. Three call sites fence against it —
// this edge, BeginDispositionAttempt and RejectDispositionCommand — so a single
// stored claim at an epoch no provider ever issued permanently supersedes every
// real Host for that command: it can never be claimed, attempted or applied
// again, and its only remaining exit is a zero-residency reconciler rejection
// once the bogus claim lapses. The command is durably lost, silently, from one
// well-formed call.
//
// A bare number could not be checked. `storage.Leaser` exposes only
// `Acquire(ctx, name) (Lease, error)`, so there is NO way to read a session's
// issued epoch without taking the lease away from whoever holds it — the store
// cannot validate a number a caller hands it, at any price short of a Storage
// contract change. A grant needs no validation: it is the store's own object,
// carrying a provider-issued epoch for a named session, so an unissued number
// is not expressible rather than merely refused.
//
// Be exact about what that buys, because the module's own warnings apply here
// too. It is NOT proof of a live lease — nothing in this package reads one, and
// a grant whose lease has expired or been taken over still passes. It IS proof
// that the epoch came from this store's provider for this session, which is the
// whole of what a ratcheting mark needs: a stale grant names a LOWER epoch and
// the fence refuses it on its own terms, and it cannot name a higher one. See
// (*ResidencyGrant).residencyFor for each conjunct.
//
// # Why the other edges keep a bare epoch, which is not an inconsistency
//
// The distinction is whether the store DECIDES from the value or merely RECORDS
// it. SettlingResidencyEpoch is recorded and never read back, which is why
// AGENTS.md can call it settlement context and leave it caller-asserted.
// BeginDispositionAttempt's epoch is fenced to equal the claim's own, so it
// cannot raise the mark. RejectDispositionCommand writes no claim at all, so it
// cannot either — and it must accept a zero, because a reconciler holds no
// residency. Only this edge writes the mark, so only this edge is gated.
type ClaimDispositionCommandRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID

	ExpectedRevision uint64
	Residency        *ResidencyGrant
	ClaimExpiresAt   time.Time
}

// RejectDispositionCommandRequest refuses one command BEFORE any dispatch of it
// was durably authorized.
//
// ExpectedRevision means what it means everywhere else here. ResidencyEpoch is
// OPTIONAL: see this file's header for why, and for what a zero buys.
//
// There is no rejection reason, and its absence is a stated cost rather than an
// oversight — see the header's third divergence.
type RejectDispositionCommandRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID

	ExpectedRevision uint64
	ResidencyEpoch   ResidencyEpoch
}

// ClaimDispositionCommand moves a pending disposition command into claimed
// under the caller's residency, and is the entry point the attempt edge's
// State == InboxStateClaimed precondition was written against. It does not
// loosen that precondition: the record it writes carries a claim whose
// residency is the caller's own, which is exactly what
// BeginDispositionAttempt's fence then requires.
//
// The second result reports whether THIS CALL wrote the claim. False with a nil
// error is the idempotent replay described below.
//
// # The order of its refusals
//
// It is the order in which the answers become permanent, so a caller meeting
// two at once is told the one that will still be true after a retry:
//
//  0. A residency that is not a live-looking grant this store issued for this
//     session is refused before anything is read — see the request type.
//  1. A settled command has no transitions left.
//  2. A command with a durably authorized ATTEMPT is not claimable at any
//     residency. Applying is a fortress in this protocol as in the legacy one,
//     and this single check is what says so: among non-terminal records an
//     attempt exists exactly when the state is applying, so a second state
//     comparison beside it would be an equivalent restatement rather than a
//     second guard.
//  3. A residency STRICTLY BELOW the record's high-water mark — its claim's
//     residency, or zero when it has no claim — has been superseded
//     permanently and is told so rather than sent to retry.
//  4. An EXACT REPLAY of a live claim is the caller's own durable claim and is
//     returned as it stands, with no write.
//  5. The apply deadline has passed, so no NEW claim may start — permanently,
//     for this command, for every caller.
//  6. A LIVE claim at the caller's own residency naming a DIFFERENT expiry is a
//     renewal, and is refused.
//
// Four and five are in that order deliberately: a replay is not a new claim,
// and the claim it replays is already durable, so the deadline has nothing left
// to prevent. Three and four are interchangeable and are written in this order
// to keep the fence first, as settlement's is: the fence cannot refuse a replay,
// because a replay names the stored claim's own residency.
//
// # Idempotency, stated exactly
//
// A replay is a RE-ISSUE OF THE SAME REQUEST — same residency, same expiry —
// against a record whose claim is STILL LIVE. It returns the stored entry with
// claimed=false and writes nothing, so a replay can never extend an expiry;
// that is also why a claim CANNOT BE RENEWED, which is legacy ClaimCommand's
// rule and its reasoning: renewal would let one writer hold a command
// indefinitely, and the state that legitimately spans a long application is
// applying.
//
// The word LIVE is not decoration, and the third outcome is named here rather
// than left to be discovered. Once the claim has LAPSED, an exact replay names
// an expiry that is now in the past, so validateBoundedExpiry refuses the
// request before the record is read at all: the answer is
// InboxErrorInvalid on claim_expires_at — neither idempotent nor claim_held. It
// fails closed, and a caller whose claim has lapsed must choose a new expiry,
// which is an ordinary re-claim over a lapsed claim rather than a replay.
//
// The idempotency is OVER THE REREAD AND NOT OVER THE LOST RESPONSE. A replay
// carrying the revision the caller originally decided on is a CONFLICT carrying
// the current revision, exactly as SettleDispositionCommand's idempotent arm
// sits behind its own revision comparison. The answer to a compare-and-swap
// whose outcome a caller did not learn is to reread, in this protocol as in
// every other path here.
//
// # What a successor may do
//
// A residency STRICTLY ABOVE the record's mark may claim a command whose claim
// is still live. That is failover rather than a special reclaim: the record's
// members carry the new claim exactly as the first one did. A claim taken over
// a LAPSED claim is likewise an ordinary claim.
func (s *Store) ClaimDispositionCommand(ctx context.Context, req ClaimDispositionCommandRequest) (DispositionInboxEntry, bool, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	if err := req.CommandID.Validate(); err != nil {
		return DispositionInboxEntry{}, false, inboxInvalid("command_id", err)
	}
	if req.ExpectedRevision == 0 {
		return DispositionInboxEntry{}, false, inboxInvalid("expected_revision", nil)
	}
	// The epoch comes off the grant and from nowhere else, so no number a caller
	// chose can reach the record's mark. Checked before the clock is read and
	// before any provider call, like every other request-shaped refusal here.
	residency, err := req.Residency.residencyFor(s, req.TenantID, req.SessionID)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	now := s.clock.Now()
	claim := DispositionClaim{ResidencyEpoch: residency, ExpiresAt: req.ClaimExpiresAt}
	// The record's OWN well-formedness rule, called rather than restated, so a
	// claim this edge writes and a claim the codec admits cannot drift apart.
	if err := validateDispositionClaim(claim); err != nil {
		return DispositionInboxEntry{}, false, err
	}
	// And the two relations a stored record cannot express, both against the
	// store's clock: a claim must lapse in the FUTURE, because one born expired
	// is indistinguishable from no claim to every guard here, and within
	// MaxCommandClaimTTL, which is where that bound's reasoning lives.
	if err := validateBoundedExpiry(req.ClaimExpiresAt, now, MaxCommandClaimTTL, "claim_expires_at", inboxInvalid); err != nil {
		return DispositionInboxEntry{}, false, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	defer release()

	current, err := s.currentDispositionEntry(opCtx, scope, req.TenantID, req.SessionID, req.CommandID, req.ExpectedRevision)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	if current.Record.Attempt != nil {
		return DispositionInboxEntry{}, false, inboxErr(InboxErrorState, "attempt", nil)
	}
	if err := dispositionRecordHighWater(current.Record, residency); err != nil {
		return DispositionInboxEntry{}, false, err
	}
	live := dispositionClaimLive(current.Record, now)
	if live && sameDispositionClaim(*current.Record.Claim, claim) {
		return current, false, nil
	}
	if !now.Before(current.Record.ApplyDeadline) {
		return DispositionInboxEntry{}, false, inboxErr(InboxErrorDeadline, "apply_deadline", nil)
	}
	if live && current.Record.Claim.ResidencyEpoch == residency {
		return DispositionInboxEntry{}, false, inboxErr(InboxErrorClaimHeld, "claim", nil)
	}
	next := current.Record
	next.State = InboxStateClaimed
	next.Claim = &claim
	entry, err := s.commitDispositionTransition(opCtx, scope, current, next)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	return entry, true, nil
}

// RejectDispositionCommand settles a command that no dispatch was ever
// authorized for, which is the only rejection this protocol has a producer for.
//
// The second result reports whether THIS CALL wrote the rejection.
//
// # It cannot become a post-attempt rejection, and that is the point
//
// Every route by which it might is closed by ONE check on the record rather
// than by a check on the caller, so no residency, no clock and no revision can
// reach the other case:
//
//   - An APPLYING record carries an attempt and is refused. Its outcome is the
//     journal's to supply through SettleDispositionCommand, and a runtime error
//     with no durable disposition leaves it applying until a successor writes
//     not_applied.
//   - A TERMINAL record that carries an attempt is refused as terminal. That
//     includes a settled `rejected` — the not_applied tombstone — which is a
//     rejection this call must never present as its own idempotent result: it
//     was settled from evidence, carries an outcome, and means something else
//     entirely.
//   - A terminal record with NO attempt is this edge's own prior result, and is
//     returned unchanged with rejected=false.
//
// # Authority
//
// A residency is optional; see this file's header. What confines a caller that
// names none is the CLAIM RULE, which is a property of the record and applies
// identically at every residency: a LIVE claim admits only its own holder. So a
// reconciler may settle a pending command or one whose claim has lapsed, and a
// live claim wins the deadline race outright — while it holds, the only caller
// that may reject is its holder, whatever the clock says.
//
// A SUCCESSOR IS ANSWERED DIFFERENTLY BY THE TWO EDGES, and the asymmetry is
// deliberate rather than an oversight. A residency above the record's mark may
// CLAIM a live claim away from its predecessor — that is failover — but may not
// REJECT the command under it: it is told claim_held and must take the claim
// first. Rejecting is a TERMINAL decision about work the holder may be in the
// middle of, so it belongs to whoever holds the claim; claiming first is the
// successor's route, and it makes the successor the holder before it decides
// anything terminal.
//
// THE IDEMPOTENT ARM ABOVE SHORT-CIRCUITS BOTH FENCES, which is the opposite
// ordering from the claim edge and is stated because a reader will expect the
// claim edge's. A superseded residency replaying a pre-dispatch rejection is
// given the idempotent success, not InboxErrorEpoch. That is sound because the
// arm WRITES NOTHING — it is a read of a record that is already terminal, and
// telling a superseded caller "this was already rejected" is both true and
// final. The claim edge's replay arm sits after its fence instead because that
// edge can go on to write.
//
// A rejection of a CLAIMED command keeps the claim. It is the durable record of
// who was working on the command when it was refused, and the codec admits an
// attemptless rejection carrying one precisely so it can be kept.
func (s *Store) RejectDispositionCommand(ctx context.Context, req RejectDispositionCommandRequest) (DispositionInboxEntry, bool, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	if err := req.CommandID.Validate(); err != nil {
		return DispositionInboxEntry{}, false, inboxInvalid("command_id", err)
	}
	if req.ExpectedRevision == 0 {
		return DispositionInboxEntry{}, false, inboxInvalid("expected_revision", nil)
	}
	now := s.clock.Now()
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	defer release()

	// dispositionEntryAtRevision rather than currentDispositionEntry, because a
	// terminal record is not uniformly a refusal here: this edge's own prior
	// result is an idempotent answer and every other terminal record is not.
	current, err := s.dispositionEntryAtRevision(opCtx, scope, req.TenantID, req.SessionID, req.CommandID, req.ExpectedRevision)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	if current.Record.State.terminal() {
		if current.Record.State == InboxStateRejected && current.Record.Attempt == nil {
			return current, false, nil
		}
		return DispositionInboxEntry{}, false, inboxErr(InboxErrorTerminal, "state", nil)
	}
	if current.Record.Attempt != nil {
		return DispositionInboxEntry{}, false, inboxErr(InboxErrorState, "attempt", nil)
	}
	// A caller that names no residency makes no assertion about the session's
	// grants and is not measured against them; it is confined instead by the
	// claim rule below, which is the whole of a reconciler's authority.
	if req.ResidencyEpoch != 0 {
		if err := dispositionRecordHighWater(current.Record, req.ResidencyEpoch); err != nil {
			return DispositionInboxEntry{}, false, err
		}
	}
	if dispositionClaimLive(current.Record, now) && current.Record.Claim.ResidencyEpoch != req.ResidencyEpoch {
		return DispositionInboxEntry{}, false, inboxErr(InboxErrorClaimHeld, "claim", nil)
	}
	next := current.Record
	next.State = InboxStateRejected
	entry, err := s.commitDispositionTransition(opCtx, scope, current, next)
	if err != nil {
		return DispositionInboxEntry{}, false, err
	}
	return entry, true, nil
}

// sameDispositionClaim reports whether a stored claim is the one a request is
// asking for, which is what makes a replay a replay.
//
// The expiry is compared with Equal and NOT with ==, and the difference is the
// whole reason this is a function. A stored claim has been through the codec, so
// its instant carries no monotonic reading and its location is whatever
// time.Parse produced; a caller's is typically time.Now().Add(ttl), which
// carries BOTH a monotonic reading and the process's local zone. struct
// equality compares all three, so == answers false for two values naming the
// SAME INSTANT and the idempotent arm becomes unreachable for every caller that
// is not already in UTC — a replay would be told its claim is held instead.
//
// It fails CLOSED, which is exactly why it would have shipped: the wrong answer
// is a spurious InboxErrorClaimHeld rather than a bad write, and no test
// written in UTC can see it. The fixtures here are all UTC, so
// TestClaimDispositionCommandReplaysAcrossTimeRepresentations exists to drive
// the case the fixtures cannot reach.
func sameDispositionClaim(stored, requested DispositionClaim) bool {
	return stored.ResidencyEpoch == requested.ResidencyEpoch && stored.ExpiresAt.Equal(requested.ExpiresAt)
}

// dispositionClaimLive reports whether the record's claim still holds at now.
//
// The interval is half-open — held up to but not including the expiry — which
// is the convention the apply deadline uses, so an instant is never
// simultaneously inside one bound and outside the other.
//
// The nil conjunct is LOAD-BEARING and not a Go formality, for the reason
// claimLive gives about its own zero check: time.Time represents instants
// before the zero one, WithClock accepts any Clock, and nothing validates what
// Now returns — rankableTime bounds the timestamps this package STORES, not the
// clock it reads. Under a clock reading earlier than the zero instant a bare
// time comparison would report every unclaimed command as claimed. Here the nil
// check additionally prevents a dereference, which is why it cannot be dropped
// and quietly survive.
func dispositionClaimLive(r DispositionInboxRecord, now time.Time) bool {
	return r.Claim != nil && now.Before(r.Claim.ExpiresAt)
}

// dispositionRecordHighWater measures a caller's residency against the mark a
// disposition record carries, which is its CLAIM's residency and is zero when
// it has no claim yet.
//
// It exists so that the two edges in this file can reach the shared superseded
// rule from a record whose claim may be ABSENT, which
// dispositionResidencyHighWater's value parameter cannot express. It adds no
// second notion of superseded: the fence itself is still epochFence, reached
// through the one function settlement uses.
func dispositionRecordHighWater(r DispositionInboxRecord, residency ResidencyEpoch) error {
	var claim DispositionClaim
	if r.Claim != nil {
		claim = *r.Claim
	}
	return dispositionResidencyHighWater(claim, residency)
}
