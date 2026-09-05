package sessionstore

import (
	"math"
	"slices"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// Desired placement is Factory-authored state on the SESSION CATALOG RECORD.
// This file adds the two members a placement controller cannot work without —
// an opaque, versioned platform workload payload and a generation that counts
// accepted desired writes — plus the rules that govern all of it. It
// deliberately declares no record of its own.
//
// A SECOND RECORD WAS THE OBVIOUS SHAPE AND IS THE WRONG ONE. The catalog
// record already carries DesiredPlacement, RuntimeCompatibilityID and
// DesiredIdempotencyKey under a revision + idempotency compare-and-swap that
// names no lease epoch, which is what stops a Factory from spelling a claim on
// a lease it does not hold. Adding a parallel desired-placement record would
// need a consistency protocol between two records with no cross-primitive
// transaction available to run it — the same argument that keeps the gate
// deadline index from carrying a second copy of a gate's content. So the
// placement API here is a projection of the catalog record and a set of rules
// applied on its write path; the compare-and-swap itself stays where it was
// accepted, in catalog.go, with exactly one statement of the ordering between
// the idempotency key and the revision.
//
// It has no error vocabulary of its own for the same reason: a placement
// failure IS a catalog failure, and a second code set would let a caller branch
// on "invalid" without knowing which record it had just been told about.
//
// PLATFORM TYPES DO NOT ENTER THIS REPOSITORY. The workload payload is a byte
// string with a caller-owned version label; nothing here parses it, and a
// Kubernetes PodSpec, a Nomad job and a future platform's manifest are all the
// same value to this package. That is a durability decision as much as a
// dependency one: a stored payload this package could parse would be a stored
// payload a later core or platform release could make undecodable, which is the
// hazard the Host registry's own header spells out at length.

// MaxDesiredWorkloadPayloadBytes bounds the opaque platform payload.
//
// It is far below MaxCatalogRecordBytes, and the gap is the point. The payload
// is the only open-ended member a Factory controls on this record, so without a
// bound of its own an oversized one would be refused as "the record is too
// large" — a limit naming a member the caller did not write and cannot shrink.
// Bounding it here reports the member that was actually too big.
//
// The value is sized for a workload SPEC rather than for workload data: a pod
// template with resources, a workspace policy, and labels is a few kilobytes.
// A payload wanting more than this is carrying data that belongs in an object,
// referenced by the spec.
//
// A byte payload has no escaping worst case to allow for, unlike the identity
// tuples the registry and the target directory bound: Go's JSON encoder writes
// a []byte as base64, so the encoded cost is a fixed 4/3 of the payload plus
// the version label, and it cannot be inflated six-fold by a caller choosing
// control characters.
const MaxDesiredWorkloadPayloadBytes = 16 << 10

// DesiredWorkload is the platform workload a Factory wants reconciled for a
// dedicated session, carried opaquely.
//
// Its zero value means no workload is desired, which is the ordinary case for a
// pooled session: pooled capacity is reconciled by scaling a Department's Hosts,
// not by creating anything per session.
//
// The two members are present together or absent together, and canonicalization
// enforces that rather than documenting it. A payload with no version is a
// document no reconciler can interpret — it would have to guess a schema — and
// a version with no payload is a claim about nothing. Requiring both also gives
// "absent" exactly one spelling, which is what keeps a record's stored bytes
// independent of which writer produced them.
//
// PayloadVersion is caller-owned text and is never interpreted here. It exists
// so that a reconciler reading a payload it does not understand can say so
// instead of misreading it.
//
// A NOTE OWED TO WHOEVER ADDS A BYTES-IDENTITY CHECK TO THE CATALOG. This
// member makes the catalog record's stored bytes a normalizer's output rather
// than a fixed point of the caller's input: encoding/json decodes a []byte with
// non-strict base64, so a stored "AR==" decodes to one byte and re-encodes as
// "AQ==". The record still round-trips CANONICALLY — decode, encode, decode
// again is stable, which is what the codec fuzzer asserts — but a check
// comparing a provider's reply against the exact bytes handed to it, as
// verifyReconciliationClaimBytes and verifyRegistrationBytes do for records
// with no []byte member, would refuse a faithful reply to a value some other
// writer had stored non-canonically. Compare re-encoded forms there, not raw
// bytes.
type DesiredWorkload struct {
	PayloadVersion string
	Payload        []byte
}

func (w DesiredWorkload) isZero() bool {
	return w.PayloadVersion == "" && len(w.Payload) == 0
}

// desiredWorkloadWire is the stored shape. How absence is spelled depends on the
// record that holds it, and both spellings are deliberate. The catalog record's
// wire form reaches it through a pointer, so an absent workload contributes no
// member at all rather than an empty object — the same reason the checkpoint
// summary and the Host route are pointers. The public-create reservation embeds
// it by value instead, so an absent workload is explicitly present there as
// {"payload_version":"","payload":null}; that record has exactly one canonical
// spelling and no omitted-member alternate.
type desiredWorkloadWire struct {
	PayloadVersion string `json:"payload_version"`
	Payload        []byte `json:"payload"`
}

// canonicalDesiredWorkload validates one workload and returns its single
// canonical spelling: either wholly absent with a nil payload, or wholly
// present with a bounded, valid-UTF-8 version and a payload owned by this
// package rather than by its caller.
//
// The copy is not defensive politeness. Payload is the only slice this record
// carries, and without it a caller that kept its slice could rewrite what a
// stored record MEANS after this package had validated it and before the
// encoder read it — and could rewrite what a record it was HANDED means, which
// would make one caller's projection alter another's.
func canonicalDesiredWorkload(workload DesiredWorkload) (DesiredWorkload, error) {
	if workload.isZero() {
		return DesiredWorkload{}, nil
	}
	// A missing version is refused by validateOpaque, which already treats an
	// empty value as invalid, and is deliberately NOT also checked here: an
	// up-front gate whose absence a deeper validator masks with the identical
	// error is a guard no test can hold, and a reader would have to run both to
	// learn that only one of them decides anything.
	if err := validateOpaque(workload.PayloadVersion, "desired_workload.payload_version", catalogInvalid); err != nil {
		return DesiredWorkload{}, err
	}
	if len(workload.Payload) == 0 {
		return DesiredWorkload{}, catalogErr(CatalogErrorInvalid, "desired_workload.payload", nil)
	}
	if len(workload.Payload) > MaxDesiredWorkloadPayloadBytes {
		return DesiredWorkload{}, catalogErr(CatalogErrorTooLarge, "desired_workload.payload", nil)
	}
	workload.Payload = slices.Clone(workload.Payload)
	return workload, nil
}

// validateDesiredPlacement holds a desired placement to the two admission
// models this system has. It is the ONE statement of that rule: the catalog
// record's canonical form calls it, and any later placement path must call it
// rather than restate the switch, because a second copy is free to admit a
// third model the rest of the system has no meaning for.
func validateDesiredPlacement(placement sessionwire.HostPlacement) error {
	switch placement {
	case sessionwire.HostPlacementPooled, sessionwire.HostPlacementDedicated:
		return nil
	default:
		return catalogErr(CatalogErrorInvalid, "desired_placement", nil)
	}
}

// initialDesiredGeneration is the generation a session is created at. Creating
// a session names a desired placement, so a created session already HAS a
// desired state and there is no interval during which it has none — which is
// what lets a stored record with generation zero be refused as corrupt rather
// than interpreted as "not yet decided".
const initialDesiredGeneration = 1

// nextDesiredGeneration advances the counter that tells a placement controller
// its work is stale.
//
// The revision cannot answer that question: every Host heartbeat moves it, so a
// controller comparing revisions would re-reconcile a session on every
// projection write and never learn whether the DESIRE had changed. The
// generation moves only when a desired-state write is applied — not on a
// replay, not on a reused key, not on a Host write — so "generation unchanged"
// means "nothing you reconcile has changed".
//
// The ceiling is refused rather than wrapped. A wrap to zero would be caught by
// the record's own rule, but a wrap past it lands on a LOWER nonzero value,
// which reads to every controller as the desired state having rolled back to
// something it has already reconciled and is therefore safe to ignore. That is
// unreachable in any real deployment and is refused anyway, because the cost of
// the check is one comparison and the cost of being wrong is silent.
func nextDesiredGeneration(current uint64) (uint64, error) {
	if current == math.MaxUint64 {
		return 0, catalogErr(CatalogErrorSequence, "desired_generation", nil)
	}
	return current + 1, nil
}

// applyDesiredState produces the record one accepted Factory-authored write
// leaves behind. It runs after UpdateCatalogDesiredState has settled the
// idempotency key and the revision, so reaching it means the intent is NEW and
// the generation must advance.
//
// Every member here REPLACES its stored counterpart; nothing is merged, exactly
// as the Host-owned projection write replaces its own. A desired-state write
// carries the complete desired state, so moving a session back to pooled by
// naming no workload clears the workload — merging would leave a dedicated
// workload spec attached to a pooled session, and the controller reconciling it
// would have no way to tell it was not meant.
//
// Nothing outside this list moves. In particular AgentID does not: the request
// type has no member for it, because every journal record, workspace and
// runtime-compatibility decision the session has is downstream of the agent it
// was created for.
func applyDesiredState(current CatalogRecord, req UpdateCatalogDesiredStateRequest) (CatalogRecord, error) {
	generation, err := nextDesiredGeneration(current.DesiredGeneration)
	if err != nil {
		return CatalogRecord{}, err
	}
	next := current
	next.DesiredPlacement = req.DesiredPlacement
	next.RuntimeCompatibilityID = req.RuntimeCompatibilityID
	next.DesiredWorkload = req.DesiredWorkload
	next.DesiredIdempotencyKey = req.IdempotencyKey
	next.DesiredGeneration = generation
	return next, nil
}

// PlacementIntent is the complete Factory-authored answer to "what should exist
// for this session", and nothing else.
//
// COMPLETE AS TO AUTHORSHIP, NOT AS TO SUFFICIENCY, and the difference decides
// whether a controller acting on one alone is correct. This is the only one of
// the record's three projections that drops State — Summary and Status both
// carry it — so an intent cannot say whether the session it describes is still
// alive. A controller holding only this could create a dedicated workload for a
// session that has ended. Read CatalogRecord.State, or Status(), from the SAME
// entry: one read of one record answers both questions, and taking them from
// one entry is what makes the pair consistent.
//
// Excluding the observation is nonetheless right, and is this type's whole
// discipline — an intent that carried liveness would be a request and a fact in
// one value, and the next reader would not know which half it was acting on.
//
// It is what a placement controller reads, and its shape is the reason it
// exists as a type rather than as a handful of catalog members a caller picks
// out. Every member here is a REQUEST. There is no lease epoch, no HostID, no
// endpoint, no residency and no journal position, so a consumer cannot read an
// observation out of it and cannot be one refactor away from treating a request
// as a fact — the Host registry's tuple is where observed placement lives, and
// it is fenced by an epoch this intent structurally cannot name.
//
// Generation is what a controller records against the workload it created. A
// controller that reconciled generation 7 and reads 7 again has nothing to do,
// however many times the record has been rewritten in between.
type PlacementIntent struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	AgentID                sessionwire.AgentID
	RuntimeCompatibilityID string

	Placement  sessionwire.HostPlacement
	Workload   DesiredWorkload
	Generation uint64
}

// PlacementIntent projects the record's desired state.
//
// It canonicalizes first, as Summary and Status do, so an intent can never be
// produced from a record this package would refuse to store — and so the
// payload it hands back is this package's copy rather than the record's own.
func (r CatalogRecord) PlacementIntent() (PlacementIntent, error) {
	r, err := canonicalCatalogRecord(r)
	if err != nil {
		return PlacementIntent{}, err
	}
	return PlacementIntent{
		TenantID:               r.TenantID,
		SessionID:              r.SessionID,
		AgentID:                r.AgentID,
		RuntimeCompatibilityID: r.RuntimeCompatibilityID,
		Placement:              r.DesiredPlacement,
		Workload:               r.DesiredWorkload,
		Generation:             r.DesiredGeneration,
	}, nil
}
