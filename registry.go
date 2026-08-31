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

// registryNamespace is the one OrderedIndex namespace holding per-session Host
// registrations. It is namespace-distinct from every other record kind for the
// reason TestOrderedNamespacesAreDistinct states: a namespace is the unit a
// decoder is chosen for.
//
// The Host TARGET DIRECTORY of the next task is deliberately NOT this
// namespace. That record is keyed by (agent, runtime, placement, host) and is
// ranked and paged by available capacity; this one is keyed by session, is
// never ranked, and is never listed. Sharing a namespace would put two codecs
// and two view disciplines into one provider partition.
const registryNamespace = "sessionstore/registry"

const (
	// HostRegistrationRecordVersion is the independent version of the stored
	// registration. A reader fails closed on any other version rather than
	// guessing which members a future encoder meant.
	HostRegistrationRecordVersion uint8 = 1

	// MaxHostRegistrationRecordBytes bounds an encoded registration. It is far
	// tighter than the catalog's and the inbox's bounds because this record has
	// no open-ended member: it is a fixed tuple of identities, each of them
	// bounded by sessionwire.MaxIDBytes, so a spelling anywhere near this
	// ceiling is already a record nothing in this package can have produced.
	//
	// Like those bounds it sits below storage.MaxOrderedValueBytes, so a record
	// this package accepts always fits in the provider and there is no state
	// that can be written but not rewritten.
	MaxHostRegistrationRecordBytes = 8 << 10
)

// Stated as an unsigned constant for the reason the catalog and the inbox state
// their own: an oversized record is refused here rather than by the provider,
// and prose cannot enforce that relationship.
const _ = uint(storage.MaxOrderedValueBytes - MaxHostRegistrationRecordBytes)

// MaxHostRegistrationTTL bounds how far ahead of the store's clock a caller may
// place a registration's expiry.
//
// It exists because an over-long expiry is a durable ROUTING fault one caller
// can commit alone, and the damage is the opposite shape from a claim's. A
// claim that outlives its usefulness parks a row nobody may settle; a
// registration that outlives its Host is worse, because every reader treats it
// as a live route and keeps sending sessions to a process that is gone. The
// registration is refreshed by heartbeat, so a legitimate one is short — this
// is a ceiling on how long a single skewed clock reading can misroute a
// session, not a tuning parameter, which is why it is a package constant with
// no deployment knob for the reason MaxCommandClaimTTL is.
const MaxHostRegistrationTTL = time.Hour

// HostRoute is the routable half of a Host registration: everything a Factory
// needs to establish a HostLink to the process currently holding the session.
//
// It is a separate pointer member rather than a group of optional fields on the
// record, and that is what makes the tombstone below correct BY CONSTRUCTION. A
// released registration has no route, and "no route" is one nil rather than an
// enumeration of eight zero values that a ninth member would silently escape.
//
// Every member is OBSERVED — what the Host reports it is actually doing — which
// is the whole difference from the catalog record. See HostRegistration.
type HostRoute struct {
	HostID         sessionwire.HostID
	HostGeneration uint64

	AgentID                sessionwire.AgentID
	RuntimeCompatibilityID string

	Placement        sessionwire.HostPlacement
	InternalEndpoint sessionwire.InternalEndpoint
	Residency        sessionwire.SessionResidency
	Accepting        bool
}

// HostRegistration is the authoritative durable record of where one session is
// currently running, and of the lease epoch that fact was observed under.
//
// It is an EXPIRING ROUTING HINT over a PERMANENT FENCE, and those two halves
// have opposite lifetimes:
//
//   - The route expires. A reader past ExpiresAt must treat the registration as
//     absent, because the Host that published it may have died at any instant
//     after ObservedAt and nothing will tell this record about it.
//   - LeaseEpoch does not expire, ever. It is the high-water mark that refuses a
//     superseded Host's write, and it is the reason an expired registration and
//     a released one are RETAINED rather than deleted. Dropping the row would
//     drop the fence, and the next write from a lease that has already lost the
//     session would be admitted.
//
// A nil Route is the released tombstone: the registration a graceful shutdown
// leaves behind. It carries the fence and nothing else, so no reader can route
// to it however its timestamps read.
//
// The accumulation that follows is intentional and affordable: one small,
// permanent row per session that has ever been registered, never listed, never
// ranked, never due, and never read except by name. The reader cost is nil,
// which is what makes permanence the right answer rather than a debt.
//
// CARRY-FORWARD CONTRACT for whoever adds retention: THE ONLY SAFE REAPER IS
// ONE THAT REMOVES THE SESSION'S WHOLE SCOPE AT ONCE — this row, its catalog
// record, its journal, its commands, and its collision witnesses — because
// deleting this row ALONE destroys the fence while leaving the session
// registrable, which is precisely the state the retention exists to prevent. A
// sweep that walks record kinds independently and reclaims the cheapest one
// first will reach this one first, and it must not.
//
// WHAT THIS RECORD OWNS, AND WHAT THE CATALOG OWNS. Three members appear in
// both records, and in every case the catalog's is Factory-authored DESIRED
// state or a durable status projection while this one's is the Host's OBSERVED
// answer:
//
//   - Placement. CatalogRecord.DesiredPlacement is what a Factory ASKED for
//     under its own revision/idempotency CAS. HostRoute.Placement is the
//     admission model the session is actually running under. The two disagree
//     for the whole interval between a desired change and its reconciliation,
//     and a router that used the desired value during that interval would bind
//     to a Host that is not serving the session.
//   - Residency. CatalogRecord.Residency is the session's last known status,
//     which is what a picker renders and which must survive this record's
//     expiry. HostRoute.Residency is the ROUTABLE residency: it is only ever
//     read together with the route it qualifies, and it disappears with it. A
//     session whose registration has expired is cold no matter what the
//     catalog's projection last said, which is exactly the statement the
//     catalog cannot make because it does not expire.
//   - RuntimeCompatibilityID and AgentID. The catalog's are what the session
//     was created for and what a Factory later desired; the route's are what
//     the running Host actually loaded. A Factory reusing a route must check
//     the observed one, because a Host that has been restarted onto a newer
//     runtime is not compatible with a session pinned to the older one.
//
// LeaseEpoch appears in both too, and there the duplication is real and
// deliberate: each record carries the epoch ITS OWN writes are fenced at.
// Neither is derived from the other, neither is read to decide the other, and
// the two advance independently — a Host that updates its catalog projection
// without refreshing its registration leaves this record at the older epoch,
// which is correct, because the fence protects the record it lives on.
type HostRegistration struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	LeaseEpoch uint64

	ObservedAt time.Time
	ExpiresAt  time.Time

	// Route is nil exactly when this registration is a released tombstone.
	Route *HostRoute
}

// released reports whether this registration is the tombstone a cleanup leaves
// behind.
//
// It is a method rather than an inline nil test at each site because "no route"
// is a STATE of this record and every rule that branches on that state must ask
// the same question — the canonical form, the routing decision, the wire
// projection, and, least obviously, the repeat check that makes cleanup
// idempotent. Those are not enumerated here on purpose: a comment claiming to
// list its own call sites is a comment that silently stops being true, and this
// one had already missed the cleanup path, which an entire paragraph of
// ClearHostRegistration is about.
func (r HostRegistration) released() bool { return r.Route == nil }

// HostRegistrationEntry is a registration together with the revision a later
// compare-and-swap names.
//
// The provider's immutable acceptance order is deliberately NOT exposed, for
// the reason CatalogEntry's is not: a session's position in a stream of
// registrations is not a fact any caller acts on. Commands expose theirs
// because arrival order is the thing consumers sort by.
type HostRegistrationEntry struct {
	Registration HostRegistration
	Revision     uint64
}

// Observation projects a live registration into Core's HostLink vocabulary,
// which is the form a Factory sends over a HostLink binding.
//
// It is also the record's OWN validator for every member of the route, and that
// is the point of routing both through one function. Core defines what a Host
// route means — that the endpoint is a credential-free WebSocket address, that
// the placement is one of two admission models, that the residency of a routed
// session is attaching, resident, or releasing rather than cold, that the
// expiry falls after the observation — and a second enumeration of those rules
// here would be free to drift from the one a Factory's peer actually applies.
// A registration this package stores is therefore always projectable, and the
// day Core adds a member to the observation this stops compiling rather than
// silently storing a record that cannot be projected.
//
// A released registration has no projection at all: a tombstone is not a route,
// and there is no version of Core's observation that expresses one.
//
// It is a projection of the BYTES and not a routing decision. It does not know
// the store's clock and therefore does not know whether the route has lapsed;
// GetHostRegistration makes that decision, and it is the only thing that does.
func (r HostRegistration) Observation() (sessionwire.HostLinkRegistryObservation, error) {
	if r.released() {
		return sessionwire.HostLinkRegistryObservation{}, registryErr(RegistryErrorReleased, "route", nil)
	}
	observation := sessionwire.HostLinkRegistryObservation{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               r.TenantID,
		SessionID:              r.SessionID,
		HostID:                 r.Route.HostID,
		HostGeneration:         r.Route.HostGeneration,
		AgentID:                r.Route.AgentID,
		RuntimeCompatibilityID: r.Route.RuntimeCompatibilityID,
		Placement:              r.Route.Placement,
		InternalEndpoint:       r.Route.InternalEndpoint,
		Residency:              r.Route.Residency,
		Accepting:              r.Route.Accepting,
		LeaseEpoch:             r.LeaseEpoch,
		ObservedAt:             r.ObservedAt.UTC(),
		ExpiresAt:              r.ExpiresAt.UTC(),
	}
	if err := observation.Validate(); err != nil {
		return sessionwire.HostLinkRegistryObservation{}, registryErr(RegistryErrorInvalid, coreValidationField(err, "route"), err)
	}
	return observation, nil
}

// coreValidationField names the member Core refused, so a failure this package
// forwards points at the same field name Core's own caller would see. A failure
// that is not a member validation reports the fallback, because inventing a
// member name for it would be worse than naming the record half that failed.
func coreValidationField(err error, fallback string) string {
	var invalid *sessionwire.RequestValidationError
	if errors.As(err, &invalid) && invalid.Field != "" {
		return invalid.Field
	}
	return fallback
}

// registryWire is the stored JSON shape. Route is a pointer so a released
// tombstone's bytes carry no empty route object and a registration has exactly
// one spelling per state.
type registryWire struct {
	RecordVersion uint8                 `json:"record_version"`
	TenantID      sessionwire.TenantID  `json:"tenant_id"`
	SessionID     sessionwire.SessionID `json:"session_id"`
	LeaseEpoch    uint64                `json:"lease_epoch"`
	ObservedAt    time.Time             `json:"observed_at"`
	ExpiresAt     time.Time             `json:"expires_at"`
	Route         *hostRouteWire        `json:"route,omitempty"`
}

type hostRouteWire struct {
	HostID                 sessionwire.HostID           `json:"host_id"`
	HostGeneration         uint64                       `json:"host_generation"`
	AgentID                sessionwire.AgentID          `json:"agent_id"`
	RuntimeCompatibilityID string                       `json:"runtime_compatibility_id"`
	Placement              sessionwire.HostPlacement    `json:"placement"`
	InternalEndpoint       sessionwire.InternalEndpoint `json:"internal_endpoint"`
	Residency              sessionwire.SessionResidency `json:"residency"`
	Accepting              bool                         `json:"accepting"`
}

// encodeHostRegistration validates and encodes one registration. It refuses a
// record above this package's bound here rather than letting the provider
// refuse it, so a record this package accepted can always be rewritten.
//
// It returns the CANONICAL record beside the bytes, as encodeInboxRecord does,
// and a caller must carry that one forward rather than the request-shaped value
// it passed in. Canonicalization only relabels timestamps today, so nothing
// this file derives from the record afterwards can currently differ — but
// hostRegistrationDue's carry-forward contract is that a due horizon is derived
// from the record, and deriving it from a record the stored bytes are not in is
// exactly the divergence that makes every concurrent reader fail.
func encodeHostRegistration(record HostRegistration) ([]byte, HostRegistration, error) {
	record, err := canonicalHostRegistration(record)
	if err != nil {
		return nil, HostRegistration{}, err
	}
	wire := registryWire{
		RecordVersion: HostRegistrationRecordVersion,
		TenantID:      record.TenantID,
		SessionID:     record.SessionID,
		LeaseEpoch:    record.LeaseEpoch,
		ObservedAt:    record.ObservedAt,
		ExpiresAt:     record.ExpiresAt,
	}
	if !record.released() {
		wire.Route = &hostRouteWire{
			HostID:                 record.Route.HostID,
			HostGeneration:         record.Route.HostGeneration,
			AgentID:                record.Route.AgentID,
			RuntimeCompatibilityID: record.Route.RuntimeCompatibilityID,
			Placement:              record.Route.Placement,
			InternalEndpoint:       record.Route.InternalEndpoint,
			Residency:              record.Route.Residency,
			Accepting:              record.Route.Accepting,
		}
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return nil, HostRegistration{}, registryErr(RegistryErrorInvalid, "record", err)
	}
	if len(encoded) > MaxHostRegistrationRecordBytes {
		return nil, HostRegistration{}, registryErr(RegistryErrorTooLarge, "record", nil)
	}
	return encoded, record, nil
}

// decodeHostRegistration strictly decodes one stored registration and
// re-validates it, so a record corrupted in place cannot be handed to a caller
// or, worse, be used as the fence a later write is measured against.
//
// Strictness is a RECORD-level property, and here it reaches the nested route
// as well: unlike the catalog's open-gate projections, the route is this
// package's own shape rather than an additive sessionwire projection, so an
// undeclared member of it is a corrupted record rather than a newer writer.
func decodeHostRegistration(value []byte) (HostRegistration, error) {
	wire, err := decodeVersionedRecord[registryWire](
		value, MaxHostRegistrationRecordBytes, HostRegistrationRecordVersion,
		versionedRecordFields{Record: "record", Version: "record_version"}, registryRecordFailure)
	if err != nil {
		return HostRegistration{}, err
	}
	record := HostRegistration{
		TenantID:   wire.TenantID,
		SessionID:  wire.SessionID,
		LeaseEpoch: wire.LeaseEpoch,
		ObservedAt: wire.ObservedAt,
		ExpiresAt:  wire.ExpiresAt,
	}
	if wire.Route != nil {
		record.Route = &HostRoute{
			HostID:                 wire.Route.HostID,
			HostGeneration:         wire.Route.HostGeneration,
			AgentID:                wire.Route.AgentID,
			RuntimeCompatibilityID: wire.Route.RuntimeCompatibilityID,
			Placement:              wire.Route.Placement,
			InternalEndpoint:       wire.Route.InternalEndpoint,
			Residency:              wire.Route.Residency,
			Accepting:              wire.Route.Accepting,
		}
	}
	return canonicalHostRegistration(record)
}

// canonicalHostRegistration validates a registration and returns its one
// canonical spelling: UTC timestamps and a route that is either wholly present
// and wholly valid or wholly absent. Encoding and decoding both end here, so a
// record read back is byte-identical to the record written and two encoders
// cannot disagree.
//
// The two shapes are validated by DIFFERENT rules, and stating which is which
// is the substance of this function:
//
//   - A LIVE registration is validated by Core, through Observation. That
//     covers the whole route plus the lease epoch and the requirement that the
//     expiry falls strictly after the observation.
//   - A RELEASED tombstone is validated here, and the one thing it has to say
//     is that its expiry EQUALS its observation — not merely that it does not
//     fall after it. Two rules ride on the equality, and only one of them is
//     about time. A tombstone that had not expired when it was written would
//     read as absent by structure and as live by time, and the two answers
//     would disagree for as long as its expiry lasted; that much a "not after"
//     rule would also give. What it would NOT give is the canonical form this
//     function's first paragraph promises: a tombstone records one instant
//     rather than measuring an interval, so an expiry a second before the
//     observation is a second spelling of one state, and one state with two
//     spellings is exactly what makes a record's stored bytes depend on which
//     writer produced them. Relaxing Equal to After is the mutation this
//     comment must not invite, and the case pinning the strictly-earlier
//     direction lives beside the strictly-later one.
//
// The timestamp bounds are this package's own on both paths: Core has no view
// on whether an instant is representable, and a year Go's JSON encoder cannot
// spell would be refused at Marshal with an untyped failure rather than here.
//
// One consequence of delegating to Core is worth stating because it is not
// local to this function: the delegation happens on the DECODE path as well as
// the encode path, so Core's rules apply to registrations that are ALREADY
// STORED. A future core release that tightens any route rule — a narrower
// endpoint grammar, a residency removed from the routable set — makes every
// stored live registration that violates the new rule undecodable, and since
// all three operations read through this function and nothing deletes these
// records, those sessions are permanently wedged with no migration path. It is
// the right trade today: core v0.7.0's route rules are pure functions of the
// record with no wall-clock or environment dependence, so a record that decoded
// once decodes forever under a fixed core version. But it makes a core version
// bump a DURABLE-DATA compatibility event for this record and for no other one
// in this package, which is a thing to check at the bump rather than discover
// after it.
func canonicalHostRegistration(record HostRegistration) (HostRegistration, error) {
	if err := record.TenantID.Validate(); err != nil {
		return HostRegistration{}, registryErr(RegistryErrorInvalid, "tenant_id", err)
	}
	if err := record.SessionID.Validate(); err != nil {
		return HostRegistration{}, registryErr(RegistryErrorInvalid, "session_id", err)
	}
	if record.LeaseEpoch == 0 {
		return HostRegistration{}, registryErr(RegistryErrorInvalid, "lease_epoch", nil)
	}
	if !rankableTime(record.ObservedAt) {
		return HostRegistration{}, registryErr(RegistryErrorInvalid, "observed_at", nil)
	}
	if !rankableTime(record.ExpiresAt) {
		return HostRegistration{}, registryErr(RegistryErrorInvalid, "expires_at", nil)
	}
	record.ObservedAt = record.ObservedAt.UTC()
	record.ExpiresAt = record.ExpiresAt.UTC()

	if record.released() {
		if !record.ExpiresAt.Equal(record.ObservedAt) {
			return HostRegistration{}, registryErr(RegistryErrorInvalid, "expires_at", nil)
		}
		return record, nil
	}
	if _, err := record.Observation(); err != nil {
		return HostRegistration{}, err
	}
	return record, nil
}

// PutHostRegistrationRequest publishes the current observed route to one
// session, creating the registration if no Host has ever registered it.
//
// It carries a HostRoute by VALUE, which is what makes "publish" and "release"
// two different operations rather than one operation with a nil argument. A
// caller cannot accidentally erase a session's route by forgetting to set a
// member, and the tombstone has exactly one writer: ClearHostRegistration.
//
// ObservedAt and ExpiresAt are the Host's own clock readings, as every other
// timestamp this package stores is. ExpiresAt is the promise the Host makes
// about its next heartbeat, so it must lie in the store's future and within
// MaxHostRegistrationTTL of it.
//
// There is no expected revision. A registration is not a decision a caller
// makes about a record it has read — it is the current truth about where the
// session is running — and the write is closed against the revision this store
// reads for itself, exactly as UpdateCatalogHostState is. The epoch is what
// establishes the right to write at all.
type PutHostRegistrationRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	LeaseEpoch uint64

	ObservedAt time.Time
	ExpiresAt  time.Time

	Route HostRoute
}

// GetHostRegistrationRequest reads one session's current route.
type GetHostRegistrationRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
}

// ClearHostRegistrationRequest releases one session's route, leaving the
// epoch-fenced tombstone behind.
//
// It carries no timestamp, and that is deliberate rather than an omission. A
// tombstone's instants are not an observation of anything — they record that
// THIS STORE wrote the tombstone — and a caller-supplied instant could place a
// tombstone's expiry in the future, producing a record that reads as released
// by structure and as live by time. The store's own clock cannot.
type ClearHostRegistrationRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID

	LeaseEpoch uint64
}

// PutHostRegistration publishes one session's observed route under the
// registration's fencing epoch.
//
// The stored LeaseEpoch is a high-water mark, not a lock: a request naming a
// lower epoch is refused outright and an equal one is admitted, because one
// lease grant heartbeats many times. The read-compare-write is closed by the
// revision compare-and-swap, so a request that observed a stale epoch cannot
// land after a successor's write.
//
// It is also the only operation that MINTS a fence. ClearHostRegistration
// refuses to build one for a session no Host ever registered, because that
// would turn an unverified caller-supplied epoch into a high-water mark; a
// first registration necessarily does exactly that, and nothing here bounds the
// value. A first registration naming MaxUint64 therefore admits only later
// writers naming MaxUint64 and fences out every real lease for the life of the
// session, permanently — the high-water never falls and this record never
// expires out of existence. That is the correct fail-closed direction, and the
// asymmetry is deliberate rather than an oversight: whether a caller's epoch is
// a lease it actually holds is a question about the lease, which this record
// cannot answer and must not pretend to.
//
// It reads through readHostRegistration and never through GetHostRegistration,
// and that separation is the single most load-bearing line in this file. The
// public reader reports an expired or released registration as no route at all;
// a writer that believed it would create a fresh record over a row it could not
// see, and creating a fresh record is exactly how a fencing high-water mark
// gets silently reset to whatever the superseded writer named.
func (s *Store) PutHostRegistration(ctx context.Context, req PutHostRegistrationRequest) (HostRegistrationEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return HostRegistrationEntry{}, err
	}
	route := req.Route
	record := HostRegistration{
		TenantID:   req.TenantID,
		SessionID:  req.SessionID,
		LeaseEpoch: req.LeaseEpoch,
		ObservedAt: req.ObservedAt,
		ExpiresAt:  req.ExpiresAt,
		Route:      &route,
	}
	// Encoding validates, so an invalid request is refused before any provider
	// work — including before the session's witnesses are bound. It returns the
	// CANONICAL record over the request-shaped one built above, so nothing
	// below can reach a form the stored bytes are not in.
	value, record, err := encodeHostRegistration(record)
	if err != nil {
		return HostRegistrationEntry{}, err
	}
	if err := validateBoundedExpiry(
		record.ExpiresAt, s.clock.Now(), MaxHostRegistrationTTL, "expires_at", registryInvalid); err != nil {
		return HostRegistrationEntry{}, err
	}

	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return HostRegistrationEntry{}, err
	}
	defer release()
	// A registration is durable session data and may be the first a session
	// has, so publishing one binds the session's collision witnesses exactly as
	// creating a catalog record or admitting a command does. The binding is
	// create-only and idempotent.
	if err := s.bindSessionScope(opCtx, scope); err != nil {
		return HostRegistrationEntry{}, err
	}

	current, found, err := s.readHostRegistration(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return HostRegistrationEntry{}, err
	}
	if !found {
		return s.createHostRegistration(opCtx, scope, record, value)
	}
	if err := registrationEpochFence(current.Registration, req.LeaseEpoch); err != nil {
		return HostRegistrationEntry{}, err
	}
	return s.updateHostRegistration(opCtx, scope, record, value, current.Revision)
}

// GetHostRegistration returns one session's route, and returns it only while it
// is a route.
//
// A released registration reports Released and an expired one reports Expired,
// each without the tuple: a caller cannot route to a Host this store will not
// vouch for, because it is never handed the endpoint. Both are the "treat an
// expired entry as absent" rule of the routing design, stated so that the two
// causes remain distinguishable to an operator while being identical to a
// router. See RegistryErrorCode.
//
// It verifies the session's collision witnesses before any provider read, so a
// derived name is never trusted on its own.
func (s *Store) GetHostRegistration(ctx context.Context, req GetHostRegistrationRequest) (HostRegistrationEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return HostRegistrationEntry{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return HostRegistrationEntry{}, err
	}
	defer release()

	entry, found, err := s.readHostRegistration(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return HostRegistrationEntry{}, err
	}
	if !found {
		return HostRegistrationEntry{}, registryErr(RegistryErrorNotFound, "record", nil)
	}
	if err := routableAt(entry.Registration, s.clock.Now()); err != nil {
		return HostRegistrationEntry{}, err
	}
	return entry, nil
}

// ClearHostRegistration releases one session's route under the registration's
// fencing epoch, writing the tombstone rather than deleting the record.
//
// It is idempotent under one grant: a repeat returns the stored tombstone
// without writing. What carries that rule is the EQUALITY in the repeat
// condition, not the order it is written in — an epoch equal to the tombstone's
// has already passed the fence, so moving the repeat check above the fence
// changes nothing a caller can observe, and a mutation that moves it survives.
// It is written after the fence anyway, because the repeat check is not an
// ownership test and must never become the only thing standing between a
// superseded lease and a success: weaken it to "is this record released" and
// the fence above is what still refuses epoch 3 a tombstone written at 5.
//
// A LATER grant releasing an already-released session is not a repeat, and this
// is the one place that distinction has teeth: the tombstone must be rewritten
// so the high-water mark rises to the later epoch. Treating it as a repeat
// would leave the fence at the older epoch, and every lease granted in between
// — all of which have provably lost the session — would still be able to write.
//
// A session with no registration at all reports NotFound. Cleanup is idempotent
// with respect to ITS OWN tombstone, not with respect to nothing: creating a
// tombstone for a session no Host ever registered would mint a fencing
// high-water mark out of an unverified caller-supplied epoch.
func (s *Store) ClearHostRegistration(ctx context.Context, req ClearHostRegistrationRequest) (HostRegistrationEntry, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return HostRegistrationEntry{}, err
	}
	if req.LeaseEpoch == 0 {
		return HostRegistrationEntry{}, registryErr(RegistryErrorInvalid, "lease_epoch", nil)
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return HostRegistrationEntry{}, err
	}
	defer release()

	current, found, err := s.readHostRegistration(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		return HostRegistrationEntry{}, err
	}
	if !found {
		return HostRegistrationEntry{}, registryErr(RegistryErrorNotFound, "record", nil)
	}
	if err := registrationEpochFence(current.Registration, req.LeaseEpoch); err != nil {
		return HostRegistrationEntry{}, err
	}
	if current.Registration.released() && current.Registration.LeaseEpoch == req.LeaseEpoch {
		return current, nil
	}
	released := s.clock.Now()
	tombstone := HostRegistration{
		TenantID:   req.TenantID,
		SessionID:  req.SessionID,
		LeaseEpoch: req.LeaseEpoch,
		ObservedAt: released,
		ExpiresAt:  released,
	}
	value, tombstone, err := encodeHostRegistration(tombstone)
	if err != nil {
		return HostRegistrationEntry{}, err
	}
	return s.updateHostRegistration(opCtx, scope, tombstone, value, current.Revision)
}

// readHostRegistration reads the RAW stored registration under an
// already-derived scope: expired ones, released ones, and no others.
//
// It reports absence as a boolean rather than as an error because its two
// callers give absence two different meanings — a publisher creates, a reader
// and a cleanup refuse — and an error would push that decision into a
// comparison against a code that some third caller would eventually get wrong.
//
// Everything else IS an error, and that matters more than it looks. A stored
// record this reader cannot decode is not an absent record: it is a fencing
// high-water mark that cannot be evaluated, and reporting it as absence would
// let a publisher create straight over it.
func (s *Store) readHostRegistration(
	ctx context.Context,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (HostRegistrationEntry, bool, error) {
	if err := s.verifySessionScope(ctx, scope); err != nil {
		return HostRegistrationEntry{}, false, err
	}
	stored, err := s.backend.OrderedIndex.Get(ctx, hostRegistrationID(scope, session))
	if err != nil {
		if errors.As(err, new(*storage.OrderedRecordNotFoundError)) {
			return HostRegistrationEntry{}, false, nil
		}
		return HostRegistrationEntry{}, false, classifyRegistryOrderedError(err, "get")
	}
	entry, err := hostRegistrationEntryFor(stored, scope, tenant, session)
	if err != nil {
		return HostRegistrationEntry{}, false, err
	}
	return entry, true, nil
}

// routableAt reports why a registration is not a route at now, or nil if it is.
// It is the ONE statement of that rule; GetHostRegistration is its only caller
// and no write path may become a second one.
//
// The order is load-bearing. A released registration is refused on its
// STRUCTURE, before any instant is compared, so a tombstone is unroutable under
// every clock a caller can configure — including one reading before the instant
// the tombstone was written, under which the expiry comparison alone would
// report the tombstone as live. WithClock accepts any Clock and nothing
// validates what Now returns; claimLive states the same reasoning for the same
// reason.
//
// The interval is half-open — a route holds up to but not including its expiry
// — which is the convention every deadline in this package uses.
func routableAt(record HostRegistration, now time.Time) error {
	// Both refusals carry the record's committed epoch, for the reason
	// RegistryError documents: these are the two codes that say a session has
	// no route, they are what a reaper acts on, and the fence is the only
	// durable fact left to check that decision against. Nothing else in this
	// package discloses it, so withholding it here would leave probing by
	// failed write as a caller's only way to read the record's most
	// consequential permanent state.
	if record.released() {
		return &RegistryError{Code: RegistryErrorReleased, Field: "route", Epoch: record.LeaseEpoch}
	}
	if !now.Before(record.ExpiresAt) {
		return &RegistryError{Code: RegistryErrorExpired, Field: "expires_at", Epoch: record.LeaseEpoch}
	}
	return nil
}

// registrationEpochFence admits a Host-owned write against the registration's
// committed high-water epoch. It is hostEpochFence's rule stated in this
// record's vocabulary, for the reason commandEpochFence is: an equal epoch is
// admitted because one grant writes many times, only a strictly lower one has
// provably lost the session, and the high-water never falls.
//
// The zero check is deliberately NOT here, as it is not there: each caller
// makes it before its read — a publisher through encodeHostRegistration, a
// cleanup explicitly — so an epochless request is refused as the caller mistake
// it is rather than being reported as whatever the read happened to find.
func registrationEpochFence(current HostRegistration, epoch uint64) error {
	if epoch < current.LeaseEpoch {
		return &RegistryError{Code: RegistryErrorEpoch, Field: "lease_epoch", Epoch: current.LeaseEpoch}
	}
	return nil
}

// createHostRegistration creates the first registration a session has ever had.
//
// A create that finds the identity already there is a lost race and is reported
// as a conflict carrying the current revision, rather than being turned into an
// update here. The reason is the fence: the record that arrived while this call
// was in flight has an epoch this request has never been compared against, and
// evaluating it on this path would put a second copy of the fence in the file.
// A caller retries and meets the fence on the ordinary path.
func (s *Store) createHostRegistration(
	ctx context.Context,
	scope sessionScope,
	record HostRegistration,
	value []byte,
) (HostRegistrationEntry, error) {
	stored, created, err := s.backend.OrderedIndex.Create(
		ctx, hostRegistrationID(scope, record.SessionID), scope.SessionNamespace,
		value, storage.Rank{}, hostRegistrationDue(record))
	if err != nil {
		return HostRegistrationEntry{}, classifyRegistryOrderedError(err, "create")
	}
	entry, err := hostRegistrationEntryFor(stored, scope, record.TenantID, record.SessionID)
	if err != nil {
		return HostRegistrationEntry{}, err
	}
	if !created {
		return HostRegistrationEntry{}, &RegistryError{Code: RegistryErrorConflict, Field: "create", Revision: entry.Revision}
	}
	return entry, verifyRegistrationBytes(stored, value)
}

// updateHostRegistration compare-and-swaps one registration onto the revision
// its caller read.
func (s *Store) updateHostRegistration(
	ctx context.Context,
	scope sessionScope,
	record HostRegistration,
	value []byte,
	expectedRevision uint64,
) (HostRegistrationEntry, error) {
	stored, err := s.backend.OrderedIndex.Update(
		ctx, hostRegistrationID(scope, record.SessionID), expectedRevision,
		value, storage.Rank{}, hostRegistrationDue(record))
	if err != nil {
		return HostRegistrationEntry{}, classifyRegistryOrderedError(err, "update")
	}
	entry, err := hostRegistrationEntryFor(stored, scope, record.TenantID, record.SessionID)
	if err != nil {
		return HostRegistrationEntry{}, err
	}
	return entry, verifyRegistrationBytes(stored, value)
}

// verifyRegistrationBytes holds a write's reply to the bytes the write handed
// the provider.
//
// Every other check in this file holds the reply to the RECORD'S OWN bytes,
// which a substituted record satisfies exactly as well as the real one. On a
// path where this package wrote the value, the provider is claiming something
// stronger — that it stored THESE bytes — and a reply carrying another Host's
// route or another lease's epoch would otherwise be returned to the caller as
// its own successful write and, worse, be used as the fence a later write is
// measured against.
//
// The comparison is exact because canonicalization is a fixed point: the bytes
// were produced by encodeHostRegistration from a record that decodes and
// re-encodes to them.
func verifyRegistrationBytes(stored storage.OrderedRecord, value []byte) error {
	if !bytes.Equal(stored.Value, value) {
		return registryErr(RegistryErrorIdentity, "value", nil)
	}
	return nil
}

// hostRegistrationID names the one ordered record per session. The ordering
// scope is the session's physical namespace and the stable key is the raw
// SessionID: an opaque provider-verified value, not a name. A provider that
// cannot place those bytes in a path or subject hashes them and stores the
// original for the verification hostRegistrationEntryFor performs.
func hostRegistrationID(scope sessionScope, session sessionwire.SessionID) storage.OrderedID {
	return storage.OrderedID{
		Namespace:     registryNamespace,
		OrderingScope: scope.SessionNamespace,
		StableKey:     storage.StableKey(session),
	}
}

// hostRegistrationDue is the single definition of a registration's due state,
// and like inboxDue it is a function of the RECORD rather than of the operation
// writing it. Every write path calls it and the filing check compares against
// it, so no path can file a due state a reader cannot rebuild from the bytes.
//
// It is constant, and the constant is NOT DUE. That is a decision about
// accumulation rather than an oversight, and it has to be read together with
// the fact that a registration is never deleted:
//
//   - Nothing in this plan pages a registry due view. The expiry is enforced by
//     routableAt, from the record's own bytes, at the instant a reader asks —
//     which is the only moment the answer matters.
//   - The rows are permanent. An expired registration is retained forever
//     because it IS the session's fencing high-water mark, so a due state at
//     its expiry would put a row into a deadline page that nothing may ever
//     remove. The page would grow to one entry per session that has ever run
//     and every reconciler pass would walk them.
//
// CARRY-FORWARD CONTRACT: a later task that needs a registry reconciler must
// derive its horizon HERE, from members of the record, exactly as inboxDue
// folds a claim expiry into the deadline — never file a due state computed by
// the operation. inboxDue states what that costs: the filing check below
// compares the stored due against this derivation on every read, so an
// operation-derived due makes every concurrent reader fail with an identity
// error no retry can fix. It must also answer what removes a row from that
// page, because nothing here does.
func hostRegistrationDue(HostRegistration) storage.Due { return storage.Due{} }

// hostRegistrationEntryFor decodes one stored registration and holds every
// provider-supplied component of its filing to what the record's own bytes say
// it should be, plus the identity the caller asked for.
//
// The enumeration, and why each entry is or is not here:
//
//   - Deleted — asserted first, and it is a fail-closed condition rather than a
//     lifecycle state. This package never calls Delete: releasing a session
//     writes the logical tombstone above, precisely so the epoch survives. A
//     provider tombstone therefore means the fencing high-water mark has been
//     physically destroyed by something outside this package, and the only safe
//     answer is to refuse every read and every write of that identity. Treating
//     it as "absent" would let the next writer create a fresh record at
//     whatever epoch it named.
//   - The record's own TenantID and SessionID — held to the request, so a
//     provider returning another session's record cannot hand a caller a route
//     into someone else's session.
//   - StableKey — held to the record's SessionID. This is the provider's choice
//     rather than the caller's, and a provider that hashes the key stores the
//     original for exactly this comparison. It is not a restatement of the
//     check above: that one asks whether the BYTES are the session asked for,
//     this one asks whether the provider FILED them where it said it did.
//   - OrderingScope, RankingScope and Due — the triad every session-scoped
//     record files identically, checked through checkFiledScope, which states
//     the rule and why each of the three is worth stating.
//   - Rank — compared as a WHOLE VALUE against what this file files, which is
//     one more than the inbox checks. This record's views are fully determined
//     here: it is written unranked and not-due on every path, so "the
//     provider's view state is exactly what this package filed" is a complete
//     statement rather than a partial one. A rank appearing on one of these
//     rows means a provider inventing view state, and hostRegistrationDue says
//     why a due state appearing on one is the more dangerous of the two.
//   - Namespace is excluded for the reason inboxEntryFor gives: it is a package
//     constant with no counterpart in any record, so comparing against it could
//     only restate that this file's constant equals itself. What keeps the
//     record kinds apart is that each owns one, which
//     TestOrderedNamespacesAreDistinct pins.
//   - Order is excluded. HostRegistrationEntry does not expose it, nothing
//     lists this namespace in acceptance order, and a session's position in a
//     stream of registrations is not a fact any caller acts on.
//   - Revision is provider state with no meaning in the record; it is returned
//     for a later compare-and-swap rather than verified.
func hostRegistrationEntryFor(
	stored storage.OrderedRecord,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) (HostRegistrationEntry, error) {
	if stored.Deleted {
		return HostRegistrationEntry{}, registryErr(RegistryErrorDeleted, "record", nil)
	}
	record, err := decodeHostRegistration(stored.Value)
	if err != nil {
		return HostRegistrationEntry{}, err
	}
	if record.TenantID != tenant || record.SessionID != session {
		return HostRegistrationEntry{}, registryErr(RegistryErrorIdentity, "record", nil)
	}
	if storage.StableKey(record.SessionID) != stored.ID.StableKey {
		return HostRegistrationEntry{}, registryErr(RegistryErrorIdentity, "session_id", nil)
	}
	if err := checkFiledScope(stored, scope.SessionNamespace, hostRegistrationDue(record), registryIdentity); err != nil {
		return HostRegistrationEntry{}, err
	}
	if stored.Rank != (storage.Rank{}) {
		return HostRegistrationEntry{}, registryErr(RegistryErrorIdentity, "rank", nil)
	}
	return HostRegistrationEntry{Registration: record, Revision: stored.Revision}, nil
}

// classifyRegistryOrderedError maps an OrderedIndex outcome into the registry
// vocabulary while preserving the cause for errors.Is and errors.As.
//
// The arms and their origins:
//
//   - NotFound arises from an Update against a record that vanished between
//     this package's read and its compare-and-swap. It does NOT arise from the
//     Get, which readHostRegistration classifies for itself: absence is an
//     answer there rather than a failure.
//   - Deleted arises from an Update against a provider tombstone. It cannot
//     arise from Get or Create, both of which return one as a RECORD, which is
//     why hostRegistrationEntryFor also classifies one.
//   - Conflict arises from an Update whose expected revision is stale, carrying
//     the provider's actual revision when it disclosed one.
//   - Unknown is an ambiguous mutation, the one outcome that says nothing at
//     all about what is stored — and here that means nothing about whether the
//     fence advanced.
//
// A cursor failure and a limit failure have no arm: this file issues no
// listing. An oversized value is refused by encodeHostRegistration before the
// provider can see it, which the unsigned constant above pins. Revision
// exhaustion falls through to Backend deliberately, as it does for the inbox.
func classifyRegistryOrderedError(err error, field string) error {
	var notFound *storage.OrderedRecordNotFoundError
	if errors.As(err, &notFound) {
		return registryErr(RegistryErrorNotFound, field, err)
	}
	var deleted *storage.OrderedDeletedError
	if errors.As(err, &deleted) {
		return registryErr(RegistryErrorDeleted, field, err)
	}
	var conflict *storage.OrderedRevisionConflictError
	if errors.As(err, &conflict) {
		return &RegistryError{Code: RegistryErrorConflict, Field: field, Revision: conflict.ActualRevision, Cause: err}
	}
	var ambiguous *storage.OrderedAmbiguousError
	if errors.As(err, &ambiguous) {
		return registryErr(RegistryErrorUnknown, field, err)
	}
	return registryErr(RegistryErrorBackend, field, err)
}
