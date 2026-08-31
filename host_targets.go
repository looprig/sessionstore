package sessionstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// hostTargetNamespace is the one OrderedIndex namespace holding Host target
// advertisements. It is namespace-distinct from every other record kind for the
// reason TestOrderedNamespacesAreDistinct states: a namespace is the unit a
// decoder is chosen for, and this one is the only namespace in the package with
// a ranked view AND a due view over the same rows.
//
// It is emphatically NOT the registry's namespace, and the two records are
// opposites in every dimension that matters to a reader. The registry holds one
// permanent, unranked, never-due row per SESSION whose lease epoch is that
// session's ownership fence. This namespace holds derived CAPACITY: rows keyed
// by (target, host), ranked by free capacity, due at their heartbeat expiry,
// carrying no tenant, no session, and no epoch, and removable from both views.
// Sharing a namespace would put two codecs and two view disciplines into one
// provider partition.
const hostTargetNamespace = "sessionstore/hosttargets"

const (
	// HostTargetRecordVersion is the independent version of the stored
	// advertisement. A reader fails closed on any other version rather than
	// guessing which members a future encoder meant.
	HostTargetRecordVersion uint8 = 1

	// MaxHostTargetRecordBytes bounds an encoded advertisement. Like the
	// registration's bound it is far tighter than the catalog's, because this
	// record has no open-ended member: it is a fixed tuple of bounded
	// identities, two instants, and three scalars.
	//
	// It allows for JSON ESCAPING, which is what sizes it rather than the sum
	// of the identity lengths. An identity is any valid UTF-8 of at most
	// MaxIDBytes bytes, control characters included, and Go escapes each of
	// those as \u00XX — six bytes for one — so the worst acceptable record is
	// about six times what an ASCII fixture measures. See
	// TestLargestAcceptableHostTargetFitsTheBound, which builds that record
	// rather than an ASCII one.
	//
	// It sits below storage.MaxOrderedValueBytes, so a record this package
	// accepts always fits in the provider and there is no state that can be
	// written but not rewritten. A heartbeat rewrites this row on a fixed
	// cadence forever, so a row that could be created but not updated would be
	// a row frozen at whatever capacity it last reported.
	//
	// On the ENCODE path the refusal is unreachable, and that is the point
	// rather than a gap: every member is bounded by Core's identity ceiling, so
	// the largest record the validators accept is a small multiple of it. What
	// holds the relationship is therefore not a test that reaches the branch —
	// none can — but the unsigned constant below, which fails to compile if the
	// bound ever exceeds the provider's, and
	// TestLargestAcceptableHostTargetFitsTheBound, which fails if the members
	// ever grow into it. On the DECODE path it is live: those bytes are not
	// this package's to bound.
	MaxHostTargetRecordBytes = 16 << 10
)

// Stated as an unsigned constant for the reason the catalog, the inbox, and the
// registry state their own: an oversized record is refused here rather than by
// the provider, and prose cannot enforce that relationship.
const _ = uint(storage.MaxOrderedValueBytes - MaxHostTargetRecordBytes)

// MaxHostTargetAvailableCapacity bounds the free capacity one Host may
// advertise for one target.
//
// It exists so that hostTargetRank is TOTAL. The rank a provider orders on is a
// signed int64 and the reported capacity is an unsigned uint64, so without a
// ceiling the conversion has an undefined region: a capacity above MaxInt64
// converts to a NEGATIVE rank, which would sort a Host claiming absurd capacity
// BELOW every real one — a wrong answer that looks like a working directory
// rather than like a refusal. Bounding the input instead makes that region
// unreachable, and the ceiling is checked where the value enters the record,
// not where the rank is computed, so no future rank expression can reintroduce
// it.
//
// The value is orders of magnitude above any real Host and is not a tuning
// parameter; a Host near it is already reporting something no process could
// serve.
const MaxHostTargetAvailableCapacity uint64 = 1 << 20

// MaxHostTargetTTL bounds how far ahead of the store's clock a Host may place
// its next heartbeat.
//
// It is tighter than MaxHostRegistrationTTL, and the asymmetry is the whole
// difference between the two records. A registration is consulted BY NAME, for
// one session a caller already knows about; a stale one misroutes that session.
// An advertisement is offered to EVERY placement decision for its target, and
// it is offered PREFERENTIALLY when its capacity ranks high — so a crashed Host
// that advertised generous capacity is the first row every placement page
// returns. This ceiling bounds how long a single skewed clock reading can hold
// that position before the row is even eligible to be reconciled away.
//
// THE COUNTER-ARGUMENT, recorded so the next reader sees both. The ceiling is
// measured against THIS STORE's clock, so it binds two populations rather than
// one: a Host whose clock runs fast, and a perfectly-clocked Host whose
// heartbeat interval is simply longer than the ceiling. Whichever it is, that
// Host's row can be offered to placement for up to the whole window after the
// process behind it has died, and five minutes was argued for on exactly that
// basis. Fifteen is kept because the two populations pull in opposite
// directions: tightening the ceiling shortens the dead-endpoint window for the
// first, and REFUSES the second outright — which removes capacity rather than
// merely mis-offering it — and this package cannot see a deployment's heartbeat
// interval to tell them apart. The choice is a judgement call within a factor
// of three, it is pinned at both boundaries by
// TestPublishHostTargetBoundsTheHeartbeatPromise, and it is cheap to change:
// nothing derives from it and no stored record embeds it.
const MaxHostTargetTTL = 15 * time.Minute

// HostTargetKey is the target half of an advertisement's identity: the
// (agent, runtime, placement) triple a Host offers capacity FOR.
//
// It is a struct rather than three parameters because it is threaded through
// the derivation, the request types, and the page, and because the whole triple
// is what selects a provider scope. A caller that dropped one member would
// otherwise be selecting a different target while looking correct.
//
// This is derived capacity, not a definition catalogue. Nothing here says an
// agent or a runtime EXISTS; it says a Host is currently willing to serve one.
type HostTargetKey struct {
	AgentID                sessionwire.AgentID
	RuntimeCompatibilityID string
	Placement              sessionwire.HostPlacement
}

// HostAdvertisement is the offered half of a Host target row: everything a
// Factory needs to decide whether to place a NEW session on this Host, and
// nothing that would let it claim an existing one.
//
// It is a separate pointer member rather than a group of optional fields, and
// that is what makes withdrawal correct BY CONSTRUCTION rather than by
// convention. A withdrawn row has no advertisement, and "no advertisement" is
// one nil rather than an enumeration of five zero values that a sixth member
// would silently escape — and, more importantly here, both derived views read
// that one nil, so a withdrawn row cannot be ranked and cannot be due.
type HostAdvertisement struct {
	InternalEndpoint  sessionwire.InternalEndpoint
	IsolationClass    sessionwire.HostIsolationClass
	Accepting         bool
	AvailableCapacity uint64

	// ExpiresAt is the promise the Host makes about its next heartbeat, and it
	// is the row's due time. It lives on the advertisement rather than on the
	// record because a withdrawn row has no next heartbeat to promise: putting
	// it here is what makes "withdrawn implies not due" a statement about the
	// record's SHAPE instead of a rule someone has to remember.
	ExpiresAt time.Time
}

// HostTarget is one Host's current advertisement for one target: the durable
// row behind a placement decision.
//
// IT IS CAPACITY, NOT AUTHORITY, AND THAT BOUNDARY IS STRUCTURAL. This record
// names no tenant, no session, and no lease epoch — there is nowhere in it to
// spell one — and the projection it publishes to a reader, core's
// HostLinkCapacityReport, has no such member either. A Factory that has read
// this row knows a Host said it could take work; it knows nothing whatsoever
// about who owns any session. The record that answers THAT question is
// HostRegistration, whose LeaseEpoch is the fence, and no code path leads from
// this file to it. TestHostTargetsCannotSpellSessionOwnership pins the
// structural half of that claim so it cannot decay into prose.
//
// HostGeneration is on the record rather than on the advertisement because a
// withdrawn row keeps it. It is a WRITE-ORDERING high-water mark over one row,
// and the distinction from the registry's epoch is worth stating precisely,
// because the two would otherwise look like the same mechanism:
//
//   - HostID is part of this row's identity, so the only writers of this row
//     are incarnations of ONE Host. The generation orders that Host's own
//     writes against each other and against nothing else.
//   - What it prevents is a liveness fault, not a safety one. A restarted Host
//     publishes at a higher generation; a heartbeat or a drain still in flight
//     from the dead incarnation would otherwise overwrite live capacity with a
//     dead process's view of it, and the target would flap.
//   - It confers NO right over any session. A Host holding the highest
//     generation on a capacity row has proven only that it is the newest
//     incarnation of itself.
//
// The rows are removable, which is the other half of the difference from the
// registry. See hostTargetDue for what removes one and why nothing here is
// retained forever.
type HostTarget struct {
	Key HostTargetKey

	HostID         sessionwire.HostID
	HostGeneration uint64

	ObservedAt time.Time

	// Advertisement is nil exactly when this row is withdrawn.
	Advertisement *HostAdvertisement
}

// withdrawn reports whether this row currently offers no capacity.
//
// It is a method rather than an inline nil test at each site because "no
// advertisement" is a STATE of this record and every rule that branches on it
// must ask the same question: the canonical form, both derived views, the
// projection, and the repeat check that makes a drain idempotent.
func (t HostTarget) withdrawn() bool { return t.Advertisement == nil }

// HostTargetEntry is one advertisement together with the revision a later
// compare-and-swap names.
//
// It is returned to the HOST that owns the row — from a publish, a drain, and
// the reconciler's own bookkeeping — and deliberately not to a placement
// reader, which receives projections instead. See HostTargetPage.
//
// The provider's immutable acceptance order is not exposed, for the reason
// CatalogEntry's is not: a row's position in a stream of advertisements is not
// a fact any caller acts on.
type HostTargetEntry struct {
	Target   HostTarget
	Revision uint64
}

// Report projects an advertised target into Core's capacity vocabulary, which
// is the form a Factory reads when choosing where to place a session.
//
// It is also the record's OWN validator for every member of the advertisement,
// and that is the point of routing both through one function. Core defines what
// a capacity report means — that the endpoint is a credential-free WebSocket
// address, that the placement is one of two admission models, that the
// isolation class is one of two boundaries, that a dedicated target cannot
// advertise more than one seat, that the expiry falls after the observation —
// and a second enumeration of those rules here would be free to drift from the
// one a Factory's peer actually applies. A target this package stores is
// therefore always projectable, and the day Core adds a member to the report
// this stops compiling rather than silently storing a record that cannot be
// projected.
//
// A withdrawn row has no projection at all: a withdrawal is not an offer of
// capacity, and there is no version of Core's report that expresses one.
//
// It is a projection of the BYTES and not a placement decision. It does not
// know the store's clock and therefore does not know whether the advertisement
// has lapsed; hostTargetLiveness makes that decision, and it is the only thing
// that does.
func (t HostTarget) Report() (sessionwire.HostLinkCapacityReport, error) {
	if t.withdrawn() {
		return sessionwire.HostLinkCapacityReport{}, hostTargetErr(HostTargetErrorWithdrawn, "advertisement", nil)
	}
	report := sessionwire.HostLinkCapacityReport{
		Version:                sessionwire.CurrentWireVersion,
		HostID:                 t.HostID,
		HostGeneration:         t.HostGeneration,
		AgentID:                t.Key.AgentID,
		RuntimeCompatibilityID: t.Key.RuntimeCompatibilityID,
		Placement:              t.Key.Placement,
		InternalEndpoint:       t.Advertisement.InternalEndpoint,
		IsolationClass:         t.Advertisement.IsolationClass,
		Accepting:              t.Advertisement.Accepting,
		AvailableCapacity:      t.Advertisement.AvailableCapacity,
		ObservedAt:             t.ObservedAt.UTC(),
		ExpiresAt:              t.Advertisement.ExpiresAt.UTC(),
	}
	if err := report.Validate(); err != nil {
		return sessionwire.HostLinkCapacityReport{}, hostTargetErr(
			HostTargetErrorInvalid, coreValidationField(err, "advertisement"), err)
	}
	return report, nil
}

// hostTargetWire is the stored JSON shape. Advertisement is a pointer so a
// withdrawn row's bytes carry no empty object and a row has exactly one
// spelling per state.
type hostTargetWire struct {
	RecordVersion          uint8                     `json:"record_version"`
	AgentID                sessionwire.AgentID       `json:"agent_id"`
	RuntimeCompatibilityID string                    `json:"runtime_compatibility_id"`
	Placement              sessionwire.HostPlacement `json:"placement"`
	HostID                 sessionwire.HostID        `json:"host_id"`
	HostGeneration         uint64                    `json:"host_generation"`
	ObservedAt             time.Time                 `json:"observed_at"`
	Advertisement          *hostAdvertisementWire    `json:"advertisement,omitempty"`
}

type hostAdvertisementWire struct {
	InternalEndpoint  sessionwire.InternalEndpoint   `json:"internal_endpoint"`
	IsolationClass    sessionwire.HostIsolationClass `json:"isolation_class"`
	Accepting         bool                           `json:"accepting"`
	AvailableCapacity uint64                         `json:"available_capacity"`
	ExpiresAt         time.Time                      `json:"expires_at"`
}

// encodeHostTarget validates and encodes one advertisement. It refuses a record
// above this package's bound here rather than letting the provider refuse it,
// so a record this package accepted can always be rewritten.
//
// It returns the CANONICAL record beside the bytes, as encodeInboxRecord and
// encodeHostRegistration do, and a caller must carry that one forward rather
// than the request-shaped value it passed in. Both derived views are computed
// from the record after this point, and computing them from a record the stored
// bytes are not in is exactly the divergence hostTargetDue warns about.
func encodeHostTarget(record HostTarget) ([]byte, HostTarget, error) {
	record, err := canonicalHostTarget(record)
	if err != nil {
		return nil, HostTarget{}, err
	}
	wire := hostTargetWire{
		RecordVersion:          HostTargetRecordVersion,
		AgentID:                record.Key.AgentID,
		RuntimeCompatibilityID: record.Key.RuntimeCompatibilityID,
		Placement:              record.Key.Placement,
		HostID:                 record.HostID,
		HostGeneration:         record.HostGeneration,
		ObservedAt:             record.ObservedAt,
	}
	if !record.withdrawn() {
		wire.Advertisement = &hostAdvertisementWire{
			InternalEndpoint:  record.Advertisement.InternalEndpoint,
			IsolationClass:    record.Advertisement.IsolationClass,
			Accepting:         record.Advertisement.Accepting,
			AvailableCapacity: record.Advertisement.AvailableCapacity,
			ExpiresAt:         record.Advertisement.ExpiresAt,
		}
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return nil, HostTarget{}, hostTargetErr(HostTargetErrorInvalid, "record", err)
	}
	if len(encoded) > MaxHostTargetRecordBytes {
		return nil, HostTarget{}, hostTargetErr(HostTargetErrorTooLarge, "record", nil)
	}
	return encoded, record, nil
}

// decodeHostTarget strictly decodes one stored advertisement and re-validates
// it, so a row corrupted in place cannot be handed to a placement caller or be
// ranked into a placement page.
//
// Strictness reaches the nested advertisement as well: unlike the catalog's
// open-gate projections, it is this package's own shape rather than an additive
// sessionwire projection, so an undeclared member of it is a corrupted record
// rather than a newer writer.
func decodeHostTarget(value []byte) (HostTarget, error) {
	wire, err := decodeVersionedRecord[hostTargetWire](
		value, MaxHostTargetRecordBytes, HostTargetRecordVersion,
		versionedRecordFields{Record: "record", Version: "record_version"}, hostTargetRecordFailure)
	if err != nil {
		return HostTarget{}, err
	}
	record := HostTarget{
		Key: HostTargetKey{
			AgentID:                wire.AgentID,
			RuntimeCompatibilityID: wire.RuntimeCompatibilityID,
			Placement:              wire.Placement,
		},
		HostID:         wire.HostID,
		HostGeneration: wire.HostGeneration,
		ObservedAt:     wire.ObservedAt,
	}
	if wire.Advertisement != nil {
		record.Advertisement = &HostAdvertisement{
			InternalEndpoint:  wire.Advertisement.InternalEndpoint,
			IsolationClass:    wire.Advertisement.IsolationClass,
			Accepting:         wire.Advertisement.Accepting,
			AvailableCapacity: wire.Advertisement.AvailableCapacity,
			ExpiresAt:         wire.Advertisement.ExpiresAt,
		}
	}
	return canonicalHostTarget(record)
}

// canonicalHostTarget validates a target row and returns its one canonical
// spelling: UTC instants and an advertisement that is either wholly present and
// wholly valid or wholly absent. Encoding and decoding both end here, so a
// record read back is byte-identical to the record written and two encoders
// cannot disagree.
//
// The two shapes are validated by different rules and the split is the
// substance of this function:
//
//   - The IDENTITY is validated here on both paths, because it is this
//     package's — it is what selects the provider scope and the stable key, and
//     core has no view on it. An identity member that failed to validate would
//     be a row filed under a scope nothing can derive again.
//   - An ADVERTISED row is then validated by Core, through Report. That covers
//     the endpoint, the isolation class, the dedicated-seat rule, and the
//     requirement that the expiry falls strictly after the observation.
//   - A WITHDRAWN row has nothing further to say. It carries no expiry, no
//     endpoint and no capacity, so there is no second spelling of the state to
//     canonicalize away: unlike the registry's tombstone, which had to be held
//     to an instant equality because it kept both timestamps, this one cannot
//     represent the disagreement at all.
//
// The instant bound is this package's own: core has no view on whether an
// instant is representable, and a year Go's JSON encoder cannot spell would be
// refused at Marshal with an untyped failure rather than here.
//
// Delegating to core on the DECODE path has the same consequence it has for the
// registration, and it is smaller here for one reason worth writing down: these
// rows are not retained. A future core release that tightened a capacity rule
// would make stored rows violating it undecodable, but a Host heartbeats a
// fresh row within MaxHostTargetTTL and the reconciler is free to withdraw what
// it cannot read, so the fleet heals rather than wedging. The registration has
// no such recovery, which is why its own note is the graver one.
func canonicalHostTarget(record HostTarget) (HostTarget, error) {
	if err := record.Key.AgentID.Validate(); err != nil {
		return HostTarget{}, hostTargetErr(HostTargetErrorInvalid, "agent_id", err)
	}
	if err := validateOpaque(record.Key.RuntimeCompatibilityID, "runtime_compatibility_id", hostTargetInvalid); err != nil {
		return HostTarget{}, err
	}
	switch record.Key.Placement {
	case sessionwire.HostPlacementPooled, sessionwire.HostPlacementDedicated:
	default:
		return HostTarget{}, hostTargetErr(HostTargetErrorInvalid, "placement", nil)
	}
	if err := record.HostID.Validate(); err != nil {
		return HostTarget{}, hostTargetErr(HostTargetErrorInvalid, "host_id", err)
	}
	if record.HostGeneration == 0 {
		return HostTarget{}, hostTargetErr(HostTargetErrorInvalid, "host_generation", nil)
	}
	if !rankableTime(record.ObservedAt) {
		return HostTarget{}, hostTargetErr(HostTargetErrorInvalid, "observed_at", nil)
	}
	record.ObservedAt = record.ObservedAt.UTC()

	if record.withdrawn() {
		return record, nil
	}
	advertisement := *record.Advertisement
	if !rankableTime(advertisement.ExpiresAt) {
		return HostTarget{}, hostTargetErr(HostTargetErrorInvalid, "expires_at", nil)
	}
	if advertisement.AvailableCapacity > MaxHostTargetAvailableCapacity {
		return HostTarget{}, hostTargetErr(HostTargetErrorInvalid, "available_capacity", nil)
	}
	advertisement.ExpiresAt = advertisement.ExpiresAt.UTC()
	// The advertisement is replaced with a fresh pointer rather than mutated in
	// place, for the reason the registration's route is: a caller's request
	// value and the record this package canonicalizes must not share memory, or
	// a later change to one silently changes the other.
	record.Advertisement = &advertisement
	if _, err := record.Report(); err != nil {
		return HostTarget{}, err
	}
	return record, nil
}

// hostTargetRank is the single definition of an advertisement's rank, and it is
// a function of the RECORD rather than of the operation writing it. Every write
// path calls it and the filing check compares against it, so no path can file a
// ranked state a reader cannot rebuild from the bytes.
//
// The placement view ranks by FREE CAPACITY, so the Host with the most room
// comes first. Ties are broken by the provider's frozen (stable_key,
// ordering_scope) tail, which is what makes paging total; nothing here may
// depend on that tail's value.
//
// Two states are UNRANKED, which is what removes a row from every placement
// page ListCompatibleHosts can produce:
//
//   - A withdrawn row. This is the graceful drain: a Host that is shutting down
//     stops being a placement candidate at the instant its withdrawal commits,
//     without waiting for any expiry.
//   - A row whose Host has stopped ACCEPTING. Core distinguishes this from zero
//     free capacity on purpose — a Host with no seats still maintains its
//     existing HostLinks and may free one, so it stays in the view, ranked
//     lowest — whereas one that is not accepting will not take new work at all
//     and has no business being offered.
//
// Deriving both from the record is why a drain needs no separate "un-rank"
// step: writing the withdrawn record IS un-ranking it, in one CAS, and the two
// cannot come apart.
func hostTargetRank(record HostTarget) storage.Rank {
	if record.withdrawn() || !record.Advertisement.Accepting {
		return storage.Rank{}
	}
	// Total by construction: no record reaches this function without passing
	// canonicalHostTarget, which refuses an AvailableCapacity above
	// MaxHostTargetAvailableCapacity — far below MaxInt64. The bound is stated
	// there rather than clamped here on purpose: a clamp would silently RANK a
	// record this package refuses to store, and would be a second statement of
	// a rule that must have exactly one.
	return storage.Rank{Ranked: true, Value: int64(record.Advertisement.AvailableCapacity)} // #nosec G115 -- bounded by MaxHostTargetAvailableCapacity
}

// hostTargetDue is the single definition of an advertisement's due state, and
// like inboxDue and hostRegistrationDue it is a function of the RECORD rather
// than of the operation writing it. Every write path calls it and the filing
// check compares against it on every read, so no path can file a due state a
// reader cannot rebuild from the bytes. inboxDue states what the alternative
// costs: an operation-derived due makes every concurrent reader fail with an
// identity error that no retry can fix.
//
// Unlike hostRegistrationDue this one is NOT constant, and the difference
// answers the question that record deliberately left open — what removes a row
// from a deadline page. Here the answer is written down:
//
//   - An advertised row is due at its own expiry. That is what puts a crashed
//     Host's row in front of the reconciler at the moment its promise lapses,
//     and it is the only mechanism by which a row nobody will ever heartbeat
//     again is noticed at all.
//   - A withdrawn row is NOT DUE, and it becomes withdrawn by the very write
//     that reconciles it. So the row leaves the due page as a consequence of
//     being handled, in the same CAS — there is no second step that could be
//     skipped, and no state in which a handled row remains due.
//
// That is the property step 4 of this task asks for, and it is worth stating in
// the negative because the failure it avoids has bitten this program three
// times: a row that stays due after being handled sits at the head of the due
// page forever, and because the page is ascending by due time it is handed to
// every later sweep BEFORE anything newer, so one such row starves every row
// behind it. Nothing in this file can produce one: due state is a function of
// the record, the reconciler's write makes the record withdrawn, and a
// withdrawn record has no due time to compute.
//
// Rows are not retained forever either, which the registration's rows are. A
// withdrawn row keeps its identity so the SAME Host can advertise the same
// target again after a restart — a provider Delete could not be used here at
// all, because the ordered index promises an identity is never reusable after a
// tombstone, and a Host draining at shutdown and re-advertising at startup is
// this record's ordinary lifecycle rather than an edge case.
func hostTargetDue(record HostTarget) storage.Due {
	if record.withdrawn() {
		return storage.Due{}
	}
	return storage.Due{State: storage.DueAt, UnixMillis: record.Advertisement.ExpiresAt.UnixMilli()}
}

// hostTargetScope names the provider surfaces one TARGET owns. It is derived
// from the (agent, runtime, placement) triple and from nothing else, so every
// Host serving one target files into one ordering and ranking scope and a
// placement page is a provider query rather than a filter over a wider one.
//
// It is deliberately not tenant-scoped, and that is a statement about what this
// record is. A Host's capacity is not a tenant's property: core's
// HostIsolationClass exists precisely because a pooled target may serve
// sessions from several tenants, and a Factory chooses on the advertised class.
// A directory partitioned by tenant could not express that, and a row that
// named a tenant would be one step from being read as a claim over that
// tenant's sessions.
type hostTargetScope struct {
	// TargetScope is the OrderedIndex ordering and ranking scope for this
	// target's rows. It obeys the storage name grammar; the identities it is
	// derived from do not, which is why it is a digest.
	TargetScope string

	witnessKey string
	witness    []byte
}

// deriveHostTargetScope is pure: it validates the target and derives
// provider-safe names without touching storage.
//
// The triple is validated here as well as in canonicalHostTarget, and the
// duplication is not a restatement of one rule in two places — it is two
// different questions asked of two different values. This one asks whether the
// caller's REQUEST names a target at all, before any name is derived from it; a
// digest of an unvalidated identity is a scope no later reader can reproduce.
// canonicalHostTarget asks whether a stored or about-to-be-stored RECORD is
// valid, which is also the question asked of bytes this store did not write.
// The listing path reaches only the first, and a target with no rows must
// answer "empty" rather than "invalid".
func (s *Store) deriveHostTargetScope(key HostTargetKey) (hostTargetScope, error) {
	if err := key.AgentID.Validate(); err != nil {
		return hostTargetScope{}, &InvalidIdentityError{Field: "AgentID", Cause: err}
	}
	if err := validateOpaque(key.RuntimeCompatibilityID, "runtime_compatibility_id", hostTargetInvalid); err != nil {
		return hostTargetScope{}, err
	}
	switch key.Placement {
	case sessionwire.HostPlacementPooled, sessionwire.HostPlacementDedicated:
	default:
		return hostTargetScope{}, hostTargetErr(HostTargetErrorInvalid, "placement", nil)
	}
	frame := digestFrame(
		"looprig/sessionstore/key/v1/hosttarget",
		[]byte(key.AgentID), []byte(key.RuntimeCompatibilityID), []byte(key.Placement))
	token := encodeDigest(s.keys.digest(frame))
	return hostTargetScope{
		TargetScope: "hosttargets/" + token,
		witnessKey:  witnessKey("hosttarget", token),
		witness:     encodeWitness(3, []byte(key.AgentID), []byte(key.RuntimeCompatibilityID), []byte(key.Placement)),
	}, nil
}

// hostTargetID names the one ordered record per (target, host). The ordering
// scope is the target's physical namespace and the stable key is the raw
// HostID: an opaque provider-verified value, not a name. A provider that cannot
// place those bytes in a path or subject hashes them and stores the original
// for the verification hostTargetEntryFor performs.
func hostTargetID(scope hostTargetScope, host sessionwire.HostID) storage.OrderedID {
	return storage.OrderedID{
		Namespace:     hostTargetNamespace,
		OrderingScope: scope.TargetScope,
		StableKey:     storage.StableKey(host),
	}
}

// PublishHostTargetRequest advertises one Host's current capacity for one
// target. It is also the HEARTBEAT: a Host republishes on its own cadence and
// this one operation moves the stored value, the rank, and the due time in a
// single compare-and-swap.
//
// It carries a HostAdvertisement by VALUE, which is what makes "publish" and
// "drain" two different operations rather than one operation with a nil
// argument. A caller cannot accidentally remove its own capacity from every
// placement page by forgetting to set a member, and the withdrawn row has
// exactly two writers: DrainHostTarget and ReconcileHostTargets.
//
// ObservedAt and the advertisement's ExpiresAt are the Host's own clock
// readings, as every other timestamp this package stores is. ExpiresAt is the
// promise the Host makes about its next heartbeat, so it must lie in the
// store's future and within MaxHostTargetTTL of it.
//
// There is no expected revision. An advertisement is not a decision a caller
// makes about a row it has read — it is the current truth about one process's
// spare capacity — and the write is closed against the revision this store
// reads for itself. The generation is what establishes the right to write at
// all, and it establishes nothing else; see HostTarget.
type PublishHostTargetRequest struct {
	Key HostTargetKey

	HostID         sessionwire.HostID
	HostGeneration uint64

	ObservedAt    time.Time
	Advertisement HostAdvertisement
}

// PublishHostTarget publishes or refreshes one Host's advertisement for one
// target.
//
// The stored HostGeneration is a high-water mark, not a lock: a request naming
// a lower generation is refused outright and an equal one is admitted, because
// one incarnation heartbeats many times. The read-compare-write is closed by
// the revision compare-and-swap, so a request that observed a stale generation
// cannot land after a successor's write.
//
// Unlike the registry's epoch, a first publish minting the high-water mark
// costs only the caller its own capacity. A Host naming an absurd generation
// fences out its own later incarnations for this one row, permanently — the
// mark never falls and a withdrawn row still carries it — but the row is one
// process's offer of capacity for one target, so the damage is a Host that
// cannot advertise. The registry's equivalent mistake is a session nobody can
// ever claim. That asymmetry is the whole practical difference between fencing
// capacity and fencing ownership.
func (s *Store) PublishHostTarget(ctx context.Context, req PublishHostTargetRequest) (HostTargetEntry, error) {
	scope, err := s.deriveHostTargetScope(req.Key)
	if err != nil {
		return HostTargetEntry{}, err
	}
	advertisement := req.Advertisement
	record := HostTarget{
		Key:            req.Key,
		HostID:         req.HostID,
		HostGeneration: req.HostGeneration,
		ObservedAt:     req.ObservedAt,
		Advertisement:  &advertisement,
	}
	// Encoding validates, so an invalid request is refused before any provider
	// work — including before the target's witness is bound. It returns the
	// CANONICAL record over the request-shaped one built above, so nothing
	// below can reach a form the stored bytes are not in.
	value, record, err := encodeHostTarget(record)
	if err != nil {
		return HostTargetEntry{}, err
	}
	if err := validateBoundedExpiry(
		record.Advertisement.ExpiresAt, s.clock.Now(), MaxHostTargetTTL, "expires_at", hostTargetInvalid); err != nil {
		return HostTargetEntry{}, err
	}

	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return HostTargetEntry{}, err
	}
	defer release()
	if err := s.bindHostTargetScope(opCtx, scope); err != nil {
		return HostTargetEntry{}, err
	}

	current, found, err := s.readHostTarget(opCtx, scope, record.Key, record.HostID)
	if err != nil {
		return HostTargetEntry{}, err
	}
	if !found {
		return s.createHostTarget(opCtx, scope, record, value)
	}
	if err := hostGenerationFence(current.Target, req.HostGeneration); err != nil {
		return HostTargetEntry{}, err
	}
	return s.writeHostTarget(opCtx, scope, record, value, current.Revision)
}

// hostGenerationFence admits a write against the row's committed high-water
// generation. An equal generation is admitted because one incarnation writes
// many times; only a strictly lower one has provably been restarted away, and
// the high-water never falls.
//
// It is NOT an ownership fence and must never be read as one. Because HostID is
// part of this row's identity, the only writers it can ever compare are
// incarnations of one Host, and what it protects is that Host's own row from
// its own dead self. registrationEpochFence answers a different question about
// a different record.
//
// The zero check is deliberately not here, as it is not in the other two
// fences. BOTH callers make it before their read, and both make it the same
// way: each builds the record it intends to write and encodes it first, and
// canonicalHostTarget refuses a generationless one. So a generationless request
// is refused as the caller mistake it is rather than being reported as whatever
// the read happened to find.
func hostGenerationFence(current HostTarget, generation uint64) error {
	if generation < current.HostGeneration {
		return &HostTargetError{
			Code: HostTargetErrorGeneration, Field: "host_generation", Generation: current.HostGeneration}
	}
	return nil
}

// bindHostTargetScope create-only binds the target's collision witness before a
// caller may create a row under a derived name.
//
// PublishHostTarget is its ONLY caller, and that is the whole rule: a witness
// answers "does this derived name belong to the target I derived it from",
// which is a question worth asking exactly when a name is first created.
// Neither of the other two operations calls it, for reasons that are different
// and are stated where each one lives — DrainHostTarget can only ever
// compare-and-swap a row that already exists and holds that row to the
// requested target by its own bytes, which is strictly stronger than a digest
// comparison; ListCompatibleHosts names no row at all, so a target nothing has
// ever advertised has no binding to prove and requiring one would answer "no
// capacity" with a failure.
//
// A digest collision is therefore refused at the second target's first publish,
// and on every other path it fails a read closed rather than disclosing one
// target's capacity as another's.
func (s *Store) bindHostTargetScope(ctx context.Context, scope hostTargetScope) error {
	return s.keys.bindWitness(ctx, scope.witnessKey, scope.witness)
}

// readHostTarget reads the RAW stored row under an already-derived scope:
// withdrawn ones, expired ones, and all others.
//
// It reports absence as a boolean rather than as an error because its callers
// give absence two different meanings — a publisher creates, a drain refuses —
// and an error would push that decision into a comparison against a code some
// third caller would eventually get wrong.
//
// Everything else IS an error. A stored row this reader cannot decode is not an
// absent row: it is an identity that already exists, and reporting it as
// absence would send a publisher into a create that the provider must then
// refuse.
func (s *Store) readHostTarget(
	ctx context.Context,
	scope hostTargetScope,
	key HostTargetKey,
	host sessionwire.HostID,
) (HostTargetEntry, bool, error) {
	stored, err := s.backend.OrderedIndex.Get(ctx, hostTargetID(scope, host))
	if err != nil {
		if errors.As(err, new(*storage.OrderedRecordNotFoundError)) {
			return HostTargetEntry{}, false, nil
		}
		return HostTargetEntry{}, false, classifyHostTargetOrderedError(err, "get")
	}
	entry, err := hostTargetEntryFor(stored, scope.TargetScope, key, host)
	if err != nil {
		return HostTargetEntry{}, false, err
	}
	return entry, true, nil
}

// createHostTarget creates the first row a (target, host) pair has ever had.
//
// A create that finds the identity already there is a lost race and is reported
// as a conflict carrying the current revision rather than being turned into an
// update here. The reason is the fence: the row that arrived while this call
// was in flight carries a generation this request has never been compared
// against, and evaluating it on this path would put a second copy of the fence
// in the file. A caller retries and meets the fence on the ordinary path.
func (s *Store) createHostTarget(
	ctx context.Context,
	scope hostTargetScope,
	record HostTarget,
	value []byte,
) (HostTargetEntry, error) {
	stored, created, err := s.backend.OrderedIndex.Create(
		ctx, hostTargetID(scope, record.HostID), scope.TargetScope,
		value, hostTargetRank(record), hostTargetDue(record))
	if err != nil {
		return HostTargetEntry{}, classifyHostTargetOrderedError(err, "create")
	}
	entry, err := hostTargetEntryFor(stored, scope.TargetScope, record.Key, record.HostID)
	if err != nil {
		return HostTargetEntry{}, err
	}
	if !created {
		return HostTargetEntry{}, &HostTargetError{
			Code: HostTargetErrorConflict, Field: "create", Revision: entry.Revision}
	}
	return entry, verifyHostTargetBytes(stored, value)
}

// writeHostTarget compare-and-swaps one row onto the revision its caller read,
// moving the value and BOTH derived views together.
//
// Every write in this file goes through it, which is what makes "value, rank,
// and due move in one CAS" a property of the code rather than a rule each
// caller has to remember. A path that wrote the value and then adjusted a view
// would leave a window in which the row is ranked at a capacity it no longer
// has, or due at an expiry it has already passed.
func (s *Store) writeHostTarget(
	ctx context.Context,
	scope hostTargetScope,
	record HostTarget,
	value []byte,
	expectedRevision uint64,
) (HostTargetEntry, error) {
	stored, err := s.backend.OrderedIndex.Update(
		ctx, hostTargetID(scope, record.HostID), expectedRevision,
		value, hostTargetRank(record), hostTargetDue(record))
	if err != nil {
		return HostTargetEntry{}, classifyHostTargetOrderedError(err, "update")
	}
	entry, err := hostTargetEntryFor(stored, scope.TargetScope, record.Key, record.HostID)
	if err != nil {
		return HostTargetEntry{}, err
	}
	return entry, verifyHostTargetBytes(stored, value)
}

// verifyHostTargetBytes holds a write's reply to the bytes the write handed the
// provider.
//
// Every other check in this file holds the reply to the RECORD'S OWN bytes,
// which a substituted record satisfies exactly as well as the real one. On a
// path where this package wrote the value, the provider is claiming something
// stronger — that it stored THESE bytes — and a reply carrying another Host's
// endpoint or another incarnation's generation would otherwise be returned to
// the caller as its own successful write.
//
// The comparison is exact because canonicalization is a fixed point: the bytes
// were produced by encodeHostTarget from a record that decodes and re-encodes
// to them.
func verifyHostTargetBytes(stored storage.OrderedRecord, value []byte) error {
	if !bytes.Equal(stored.Value, value) {
		return hostTargetErr(HostTargetErrorIdentity, "value", nil)
	}
	return nil
}

// hostTargetEntryFor decodes one stored row and holds every provider-supplied
// component of its filing to what the record's own bytes say it should be, plus
// the identity the caller asked for.
//
// scope and key are what the row is held TO, and the two callers supply them
// from different places on purpose. A named read and a placement page supply
// the REQUEST's target and its derived scope, so the row is proven to be the
// one the caller asked for. The reconciler has no request — it sweeps a due
// view across every target — so it supplies the target the record's own bytes
// name and the scope derived from that, which proves the provider filed the row
// where its contents say it belongs. Neither caller may supply a scope derived
// from anything but a validated target.
//
// The enumeration, and why each entry is or is not here:
//
//   - Deleted — asserted first. This package never calls Delete, and cannot:
//     the ordered index promises an identity is never reusable after a
//     tombstone, while a Host that drains at shutdown and advertises again at
//     startup reuses this identity as a matter of course. A provider tombstone
//     therefore means something outside this package has permanently retired a
//     live Host's ability to advertise, which is reported rather than treated
//     as absence — treating it as absence would send a publisher into a create
//     the provider must refuse, forever.
//   - The record's own target and HostID — held to what the caller supplied.
//     The TARGET half is the load-bearing one and is real on every path: a
//     provider returning another target's row cannot put a Host into a
//     placement page for capacity it never offered. The HOST half is only a
//     caller's value on the NAMED paths; a placement page has no host in its
//     request and passes the stable key the provider itself supplied, so on
//     that path this comparison is between two provider-supplied values and the
//     check below is what carries it. That is stated rather than quietly true,
//     because a reader who assumed both halves were caller-checked everywhere
//     would think the row was pinned harder than it is.
//   - StableKey — held to the record's HostID. This is the provider's choice
//     rather than the caller's, and a provider that hashes the key stores the
//     original for exactly this comparison. It is not a restatement of the
//     check above: that one asks whether the BYTES are the target asked for,
//     this one asks whether the provider FILED them where it said it did.
//   - OrderingScope, RankingScope and Due — checked through checkFiledScope,
//     which states the rule. The due comparison is the one that matters most
//     for this record: it is a whole-value comparison against hostTargetDue, so
//     a row filed not-due that should be due — a crashed Host's advertisement
//     that no sweep will ever see — is refused rather than silently becoming
//     permanent ranked capacity.
//   - Rank — compared as a WHOLE VALUE against hostTargetRank. This record's
//     views are fully determined by its bytes on every path, so "the provider's
//     view state is exactly what the record justifies" is a complete statement.
//     A row ranked higher than its capacity would be offered ahead of Hosts
//     that really have room.
//   - Namespace is excluded for the reason inboxEntryFor and
//     hostRegistrationEntryFor give: it is a package constant with no
//     counterpart in any record, so comparing against it could only restate
//     that this file's constant equals itself. What keeps the record kinds
//     apart is that each owns one, which TestOrderedNamespacesAreDistinct pins.
//   - Order is excluded. HostTargetEntry does not expose it, no view here is in
//     acceptance order, and a Host's position in a stream of advertisements is
//     not a fact any caller acts on.
//   - Revision is provider state with no meaning in the record; it is returned
//     for a later compare-and-swap rather than verified.
func hostTargetEntryFor(
	stored storage.OrderedRecord,
	scope string,
	key HostTargetKey,
	host sessionwire.HostID,
) (HostTargetEntry, error) {
	if stored.Deleted {
		return HostTargetEntry{}, hostTargetErr(HostTargetErrorDeleted, "record", nil)
	}
	record, err := decodeHostTarget(stored.Value)
	if err != nil {
		return HostTargetEntry{}, err
	}
	if record.Key != key || record.HostID != host {
		return HostTargetEntry{}, hostTargetErr(HostTargetErrorIdentity, "record", nil)
	}
	if storage.StableKey(record.HostID) != stored.ID.StableKey {
		return HostTargetEntry{}, hostTargetErr(HostTargetErrorIdentity, "host_id", nil)
	}
	if err := checkFiledScope(stored, scope, hostTargetDue(record), hostTargetIdentity); err != nil {
		return HostTargetEntry{}, err
	}
	if stored.Rank != hostTargetRank(record) {
		return HostTargetEntry{}, hostTargetErr(HostTargetErrorIdentity, "rank", nil)
	}
	return HostTargetEntry{Target: record, Revision: stored.Revision}, nil
}

// classifyHostTargetOrderedError maps an OrderedIndex outcome into the
// directory vocabulary while preserving the cause for errors.Is and errors.As.
//
// The arms and their origins:
//
//   - NotFound arises from an Update against a row that vanished between this
//     package's read and its compare-and-swap. It does NOT arise from the Get,
//     which readHostTarget classifies for itself: absence is an answer there
//     rather than a failure.
//   - Deleted arises from an Update against a provider tombstone. It cannot
//     arise from Get or Create, both of which return one as a RECORD, which is
//     why hostTargetEntryFor also classifies one.
//   - Conflict arises from an Update whose expected revision is stale, carrying
//     the provider's actual revision when it disclosed one. It is a lost race
//     and nothing more; it says nothing about the generation.
//   - Cursor arises from a page token the provider refuses. This is the one
//     record kind here that both lists and CAS-writes, so it needs an arm the
//     registry's classifier has none of.
//   - Unknown is an ambiguous mutation, the one outcome that says nothing at
//     all about what is stored.
//
// A limit failure has no arm: every limit reaching the provider has already
// passed pageLimit. An oversized value is refused by encodeHostTarget before
// the provider can see it, which the unsigned constant above pins. Revision
// exhaustion falls through to Backend deliberately, as it does for the inbox
// and the registry.
func classifyHostTargetOrderedError(err error, field string) error {
	var notFound *storage.OrderedRecordNotFoundError
	if errors.As(err, &notFound) {
		return hostTargetErr(HostTargetErrorNotFound, field, err)
	}
	var deleted *storage.OrderedDeletedError
	if errors.As(err, &deleted) {
		return hostTargetErr(HostTargetErrorDeleted, field, err)
	}
	var conflict *storage.OrderedRevisionConflictError
	if errors.As(err, &conflict) {
		return &HostTargetError{
			Code: HostTargetErrorConflict, Field: field, Revision: conflict.ActualRevision, Cause: err}
	}
	var cursor *storage.InvalidOrderedCursorError
	if errors.As(err, &cursor) {
		return hostTargetErr(HostTargetErrorCursor, field, err)
	}
	var ambiguous *storage.OrderedAmbiguousError
	if errors.As(err, &ambiguous) {
		return hostTargetErr(HostTargetErrorUnknown, field, err)
	}
	return hostTargetErr(HostTargetErrorBackend, field, err)
}

// ListCompatibleHostsRequest positions one bounded page of the Hosts currently
// offering capacity for one target, most free capacity first.
type ListCompatibleHostsRequest struct {
	Key HostTargetKey

	// Cursor is a token a previous page of THIS target issued. It is opaque:
	// retain it and hand it back, but do not parse it or derive ordering,
	// identity, or authority from it. Possessing one authorizes nothing, and a
	// cursor this store did not issue for this target is refused with
	// HostTargetErrorCursor, which means the walk restarts from the first page
	// rather than that anything is wrong with the store.
	Cursor sessionwire.Cursor

	// Limit is the page's record ceiling. Zero means the store's configured
	// page size. It bounds the rows the PROVIDER returns, not the reports this
	// page publishes; see HostTargetPage.
	Limit int
}

// HostTargetPage is one bounded page of live capacity for one target.
//
// Hosts carries core's capacity reports rather than this package's entries, and
// that is the capacity/authority boundary made structural on the read side. A
// HostLinkCapacityReport has no tenant member, no session member, and no epoch
// member, so a Factory holding one cannot mistake it for a claim on anything —
// there is nothing in it to mistake. The revision a compare-and-swap needs goes
// to the HOST that owns the row, through publish and drain, and a placement
// reader has no business rewriting another process's advertisement.
//
// TWO COUNTS say why a page is shorter than its limit, and neither is a
// diagnostic afterthought. A page that silently returned fewer entries would
// leave a caller unable to tell "this target has little capacity" from "this
// target is full of rows I passed over", and the two causes want different
// responses:
//
//   - LapsedSkipped counts rows whose heartbeat promise had already lapsed at
//     the store's clock. Such a row is still RANKED, so it still occupies a
//     position in every later page; a nonzero count means the directory is owed
//     a ReconcileHostTargets pass, which is the only thing that clears it.
//   - UnreadableSkipped counts rows this build could not decode, or that
//     disagreed with the filing they were found under.
//
// THE RULE FOR AN UNREADABLE ROW IS STATED HERE AND NOWHERE ELSE, because a
// rule restated in five places is a rule that drifts in four of them. Such a
// row is skipped rather than FAILING THE PAGE, and that is load-bearing rather
// than lenient: nothing in this package ever rewrites a row it cannot read — a
// newer writer may have produced it — so the row is permanent, and a reader
// that failed the page on one would take every Host serving that target out of
// service for as long as it existed, with no recovery path anywhere in the
// system. Skipping keeps the newer writer's row untouched and starts publishing
// it the instant a reader that understands it asks; the count keeps the
// condition visible; and README.md records that a genuinely corrupt row has no
// in-band repair at all. ListDueGates and ListSessions obey the same rule for
// the same reason.
//
// A page may therefore contain fewer than Limit entries while still issuing a
// continuation. A caller that wants a specific number of candidates pages until
// NextCursor is empty; it must not treat a short page as the end of the target.
type HostTargetPage struct {
	Hosts             []sessionwire.HostLinkCapacityReport
	LapsedSkipped     int
	UnreadableSkipped int
	NextCursor        sessionwire.Cursor
}

// hostTargetLiveness is the one statement of what makes a stored row a
// candidate at an instant. Both readers call it — the placement page, which
// omits everything that is not live, and the reconciler, which acts on exactly
// what is lapsed — so the two can never drift into different ideas of when an
// advertisement has run out.
//
// The order is load-bearing, and what enforces it is stronger than this
// comment: a withdrawn row HAS no expiry to compare, so the two arms cannot be
// swapped — reversing them dereferences a nil advertisement rather than
// silently accepting a withdrawal as live. The wrong order is unrepresentable,
// which is why no test drives it and none pretends to. routableAt states the
// clock-shaped version of this argument because a released registration does
// carry timestamps and therefore needs one; this record does not.
//
// The interval is half-open — an advertisement holds up to but not including
// its expiry — which is the convention every deadline in this package uses.
func hostTargetLiveness(record HostTarget, now time.Time) hostTargetState {
	if record.withdrawn() {
		return hostTargetWithdrawn
	}
	if !now.Before(record.Advertisement.ExpiresAt) {
		return hostTargetLapsed
	}
	return hostTargetLive
}

// hostTargetState is the closed set of answers hostTargetLiveness gives. It is
// an enum rather than a pair of booleans so a caller's handling is exhaustive
// by construction and the two non-live answers stay distinguishable: they are
// reached by different paths and the reconciler acts on only one of them.
type hostTargetState uint8

const (
	hostTargetLive hostTargetState = iota + 1
	hostTargetLapsed
	hostTargetWithdrawn
)

// ListCompatibleHosts returns one bounded page of the Hosts currently offering
// capacity for one target, most free capacity first.
//
// The whole page is one ranked provider query. The target is the ranking scope,
// so the restriction and the capacity order are both inside the query and the
// limit applies to an already-restricted, already-ordered result. Nothing here
// enumerates a prefix, sorts a directory, or narrows a wider page to a target
// afterwards: those cost work proportional to the fleet rather than to the
// page, and a Factory calls this on every placement decision.
//
// It deliberately does NOT verify the target's collision witness, which is
// where it differs from every write here. A write names a row and must prove
// the derived name before it creates one; a listing names no row, and a target
// nothing has ever advertised has no witness to prove, so requiring one would
// answer "no capacity" with a failure. Safety comes from below instead: every
// row the provider returns is held to the target it itself claims, so a scope
// two targets somehow shared would fail the page closed rather than offer one
// target's Hosts as the other's.
//
// A LAPSED ROW IS NOT PUBLISHED, and the reason it is dropped here rather than
// excluded by the query is that no ranked query can express it: the ranked view
// is ordered by capacity and knows nothing about the clock. This is the same
// "an expired entry reads as absent" rule GetHostRegistration applies, applied
// per row — a caller is never handed an endpoint this store will not vouch for.
// It is emphatically not this package's answer to stale rows accumulating: the
// row is still ranked and still occupies a position in every later page, and
// ReconcileHostTargets is what removes it. The count says so out loud.
//
// NO SINGLE ROW CAN FAIL A PAGE: every per-row refusal is counted and stepped
// over. HostTargetPage states why that is the strongest rule here rather than
// leniency. The consequence for this signature is what matters at the call
// site: a failure returned from here is always about the QUERY — a bad limit, a
// foreign cursor, a provider that could not answer — and never about one row.
func (s *Store) ListCompatibleHosts(ctx context.Context, req ListCompatibleHostsRequest) (HostTargetPage, error) {
	limit, ok := s.pageLimit(req.Limit)
	if !ok {
		return HostTargetPage{}, hostTargetErr(HostTargetErrorInvalid, "limit", nil)
	}
	scope, err := s.deriveHostTargetScope(req.Key)
	if err != nil {
		return HostTargetPage{}, err
	}
	var after storage.RankedCursor
	if req.Cursor != "" {
		if after, err = s.decodeHostTargetCursor(req.Key, req.Cursor); err != nil {
			return HostTargetPage{}, err
		}
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return HostTargetPage{}, err
	}
	defer release()

	ranked, err := s.backend.OrderedIndex.ListRanked(opCtx, hostTargetNamespace, scope.TargetScope, after, limit)
	if err != nil {
		return HostTargetPage{}, classifyHostTargetOrderedError(err, "list")
	}
	now := s.clock.Now()
	page := HostTargetPage{Hosts: make([]sessionwire.HostLinkCapacityReport, 0, len(ranked.Records))}
	for _, stored := range ranked.Records {
		entry, err := hostTargetEntryFor(
			stored, scope.TargetScope, req.Key, sessionwire.HostID(stored.ID.StableKey))
		if err != nil {
			page.UnreadableSkipped++
			continue
		}
		if hostTargetLiveness(entry.Target, now) != hostTargetLive {
			// A withdrawn row cannot reach here — it is unranked, so the ranked
			// query excludes it — and if a provider returned one anyway it is
			// counted with the lapsed rather than published. Both are the same
			// answer to a placement caller: no capacity.
			page.LapsedSkipped++
			continue
		}
		report, err := entry.Target.Report()
		if err != nil {
			// Unreachable through canonicalHostTarget, which projects every
			// advertised record it accepts. Counted rather than returned so
			// that no future relaxation of that rule can reintroduce a row that
			// fails a whole target's page.
			page.UnreadableSkipped++
			continue
		}
		page.Hosts = append(page.Hosts, report)
	}
	if ranked.NextCursor != "" {
		next, err := s.encodeHostTargetCursor(req.Key, ranked.NextCursor)
		if err != nil {
			return HostTargetPage{}, err
		}
		page.NextCursor = next
	}
	return page, nil
}

// The placement page cursor. Its payload is the provider's own ranked cursor,
// carried verbatim: this package never parses it, and "the rest of the
// envelope" is the whole rule, so no length prefix or grammar of ours reaches
// inside it. What the envelope adds is a binding the provider is not obliged to
// give a CALLER of this package, plus a kind tag, so a placement page, a
// catalog page, and a journal page cannot be replayed into each other. See the
// envelope grammar in cursor.go, including why the scope field is a binding tag
// and not a MAC.
const (
	hostTargetCursorMagic        = "LRHT"
	hostTargetCursorVersion byte = 1

	// maxHostTargetCursorBytes bounds a decoded placement page cursor, and
	// maxHostTargetCursorPayload is the largest provider token the envelope can
	// carry. The ceiling is enforced when a cursor is ISSUED as well as when
	// one is presented, so a token this store hands out is always a token it
	// will accept back and a caller can never be given an unusable
	// continuation.
	maxHostTargetCursorBytes   = 4 << 10
	maxHostTargetCursorPayload = maxHostTargetCursorBytes - cursorPayloadAt
)

// hostTargetCursorScope binds a continuation to the whole target triple. All
// three members are framed into the digest, so a token issued for one placement
// of an agent cannot be presented for the other, which is a mistake a caller
// building a request from parts can otherwise make silently.
func (s *Store) hostTargetCursorScope(key HostTargetKey) [cursorScopeBytes]byte {
	return s.keys.digest(digestFrame(
		"looprig/sessionstore/hosttarget/cursor/v1",
		[]byte(key.AgentID), []byte(key.RuntimeCompatibilityID), []byte(key.Placement)))
}

// encodeHostTargetCursor wraps one provider continuation token for one target.
func (s *Store) encodeHostTargetCursor(key HostTargetKey, next storage.RankedCursor) (sessionwire.Cursor, error) {
	if len(next) > maxHostTargetCursorPayload {
		return "", hostTargetErr(HostTargetErrorBackend, "next_cursor", nil)
	}
	token := encodeCursorEnvelope(
		hostTargetCursorMagic, hostTargetCursorVersion, s.hostTargetCursorScope(key), []byte(next))
	return sessionwire.Cursor(token), nil
}

// decodeHostTargetCursor unwraps a continuation this store issued for this
// target and returns the provider token inside it. A continuation this reader
// issued always carries at least one payload byte, because an exhausted page
// returns no cursor at all rather than an empty one.
func (s *Store) decodeHostTargetCursor(key HostTargetKey, cursor sessionwire.Cursor) (storage.RankedCursor, error) {
	payload, ok := decodeCursorEnvelope(
		hostTargetCursorMagic, hostTargetCursorVersion, s.hostTargetCursorScope(key),
		string(cursor), 1, maxHostTargetCursorPayload)
	if !ok {
		return "", hostTargetErr(HostTargetErrorCursor, "cursor", nil)
	}
	return storage.RankedCursor(payload), nil
}

// DrainHostTargetRequest withdraws one Host's advertisement for one target,
// gracefully and immediately.
//
// It carries no timestamp, and that is deliberate rather than an omission. A
// withdrawal's instant is not an observation of anything — it records that THIS
// STORE wrote the withdrawal — and a caller-supplied instant would let a Host
// place its own withdrawal in the future or the distant past, in a record that
// nothing afterwards re-derives an expiry from. The store's own clock cannot.
type DrainHostTargetRequest struct {
	Key HostTargetKey

	HostID         sessionwire.HostID
	HostGeneration uint64
}

// DrainHostTarget withdraws one Host's capacity for one target under the row's
// generation high-water mark.
//
// A drain is the graceful counterpart of the reconciler: it removes the row
// from the placement view and from the deadline view in one compare-and-swap,
// at the instant a Host decides to stop rather than at the instant its promise
// runs out. Both write the same withdrawn SHAPE — a record with no
// advertisement — so there is one withdrawn state rather than two, and both
// reach it through the one write that moves the value and both views together.
// They differ only in the generation they leave behind: a drain raises the mark
// to the incarnation that asked, while the reconciler preserves whatever the
// row already carried, because a sweep speaks for no incarnation.
//
// It is idempotent under one incarnation: a repeat returns the stored row
// without writing. What carries that rule is the EQUALITY in the repeat
// condition, not the order it is written in — a generation equal to the stored
// one has already passed the fence — so it is written after the fence
// deliberately: the repeat check is not an ownership test and must never become
// the only thing standing between a superseded incarnation and a success.
//
// A LATER incarnation draining an already-withdrawn row is not a repeat, and
// the row is rewritten so the high-water rises to it. Treating it as a repeat
// would leave the mark at the older generation and let every incarnation in
// between — all of them provably restarted away — write again.
//
// A (target, host) pair that has never advertised reports NotFound. A drain is
// idempotent with respect to ITS OWN withdrawal, not with respect to nothing:
// writing a withdrawal for capacity that was never offered would mint a
// generation high-water out of an unverified request AND leave a permanent row
// standing for capacity that never existed, which is the accumulation this
// record is built to avoid.
//
// It deliberately does not verify the target's collision witness, which
// PublishHostTarget binds. A drain creates no name — it can only ever
// compare-and-swap a row that already exists — and the row it finds is held to
// the requested target by its own bytes, which is a strictly stronger check
// than a digest comparison. Verifying here would restate a weaker form of a
// check already made and would answer "there is nothing to drain" with a
// keyspace failure.
func (s *Store) DrainHostTarget(ctx context.Context, req DrainHostTargetRequest) (HostTargetEntry, error) {
	scope, err := s.deriveHostTargetScope(req.Key)
	if err != nil {
		return HostTargetEntry{}, err
	}
	// The withdrawal is built and encoded BEFORE any provider work, which is
	// what validates the rest of the request — the Host identity, the
	// generation, and the store's own instant — through the one canonical form
	// every write here shares, rather than through a second enumeration.
	withdrawal := HostTarget{
		Key:            req.Key,
		HostID:         req.HostID,
		HostGeneration: req.HostGeneration,
		ObservedAt:     s.clock.Now(),
	}
	value, withdrawal, err := encodeHostTarget(withdrawal)
	if err != nil {
		return HostTargetEntry{}, err
	}

	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return HostTargetEntry{}, err
	}
	defer release()

	current, found, err := s.readHostTarget(opCtx, scope, req.Key, req.HostID)
	if err != nil {
		return HostTargetEntry{}, err
	}
	if !found {
		return HostTargetEntry{}, hostTargetErr(HostTargetErrorNotFound, "record", nil)
	}
	if err := hostGenerationFence(current.Target, req.HostGeneration); err != nil {
		return HostTargetEntry{}, err
	}
	if current.Target.withdrawn() && current.Target.HostGeneration == req.HostGeneration {
		return current, nil
	}
	return s.writeHostTarget(opCtx, scope, withdrawal, value, current.Revision)
}

// DefaultHostTargetReconcilePages is the number of due pages one sweep walks
// when a caller names no budget, and MaxHostTargetReconcilePages is the most it
// may name.
//
// A sweep is bounded by pages as well as by page size because it must be able
// to STEP OVER a row it cannot handle. A row this sweep cannot decode stays
// due, so it sits at the head of every later ascending due page; a single-page
// sweep would spend every pass on that row and never reach the rows behind it.
//
// THE BUDGET ALONE IS NOT THE ANSWER, and believing it was is how this record
// nearly shipped with the head-of-line failure its own deadline view exists not
// to have. A budget bounds the work ONE PASS does; it does nothing about
// progress, because every pass restarts at the head of the same ascending view
// and nothing ever removes an unreadable row, so that population is
// monotonically non-decreasing. Once it reaches MaxPages x Limit rows, every
// later pass spends its whole budget on them and withdraws nothing, forever.
//
// What actually supplies progress is the CONTINUATION on the result: a sweep
// that runs out of budget hands back where it stopped, and a caller that pages
// until Exhausted reaches every row however many unreadable ones precede them.
// The budget then means what it says — a bound on one call — and
// HostTargetReconcileResult.Unreadable is what makes the cost of those rows
// visible rather than merely survivable.
const (
	DefaultHostTargetReconcilePages = 16
	MaxHostTargetReconcilePages     = 1024
)

// ReconcileHostTargetsRequest bounds one service-owned sweep of the directory's
// deadline view. Zero means the default in Limit and MaxPages.
type ReconcileHostTargetsRequest struct {
	Limit    int
	MaxPages int

	// Cursor resumes a sweep that ran out of page budget. It is opaque: retain
	// it and hand it back, but do not parse it. Possessing one authorizes
	// nothing — this operation is service-owned and a caller must establish
	// that on its own — and a token this store did not issue for a sweep is
	// refused with HostTargetErrorCursor.
	//
	// Resuming is not an optimization; see DefaultHostTargetReconcilePages for
	// why a page budget alone leaves this sweep able to make no progress at
	// all.
	Cursor sessionwire.Cursor
}

// HostTargetReconcileResult accounts for every row one sweep scanned. The five
// outcomes sum to Scanned, which is asserted rather than assumed: a sweep that
// silently dropped a row would otherwise look like a sweep that had nothing to
// do.
//
//   - Withdrawn — the row's stored expiry had genuinely lapsed and the sweep
//     removed it from both views.
//   - StillLive — the row named by the due page had not actually lapsed when
//     the sweep revalidated its stored expiry. The due view is weakly
//     consistent and a Host may have heartbeated since; this is the count of
//     rows the revalidation SAVED.
//   - Contended — the compare-and-swap lost to a concurrent write, or the row
//     moved out from under it. Nothing was decided and a later sweep will see
//     the row again if it is still lapsed.
//   - Unreadable — the row could not be decoded, or disagreed with the filing
//     it was found under. The sweep steps over it and does NOT rewrite it, for
//     the reason HostTargetPage gives. A nonzero count is an operator's signal,
//     not a transient: nothing retires such a row.
//   - Unverified — the compare-and-swap COMMITTED and the provider's reply then
//     failed this package's checks on it. The withdrawal is durable, so the row
//     is handled and no later sweep will revisit it, but this sweep cannot say
//     that it is: the reply it was given does not describe what it wrote. It is
//     its own outcome rather than folded into Contended, which means the
//     opposite — that nothing was decided.
//
// Exhausted reports whether the sweep reached the end of the due view within
// its page budget. NextCursor is nonempty exactly when it did not, and a caller
// that wants the whole view hands it back — see ReconcileHostTargetsRequest for
// why that is a correctness property rather than a convenience.
type HostTargetReconcileResult struct {
	Scanned    int
	Withdrawn  int
	StillLive  int
	Contended  int
	Unreadable int
	Unverified int

	Exhausted  bool
	NextCursor sessionwire.Cursor
}

// ReconcileHostTargets withdraws the advertisements of Hosts that stopped
// heartbeating.
//
// IT IS A SERVICE OPERATION, NOT A TENANT ONE. It names no tenant, no session
// and no target: it sweeps the whole directory's deadline view, which is
// exactly why a caller that is not the control plane must never be given it. It
// is also the ONLY thing in this package that removes a crashed Host's row from
// a placement page — the placement reader declines to publish a lapsed row but
// leaves it ranked — so a deployment that never calls this accumulates ranked
// capacity that no longer exists.
//
// The due BOUND is fixed for one walk and the revalidation INSTANT is not, and
// the difference is deliberate rather than an oversight. The bound is fixed
// because the ordered index binds a due cursor to the exact bound that issued
// it, so a walk that recomputed it could not page at all; on a resumed call the
// bound therefore comes from the continuation while the clock reading is fresh.
//
// They are allowed to differ because the bound decides only WHICH rows a page
// contains and the revalidation decides whether any of them may be withdrawn. A
// fresh reading is monotonically at or after the bound, which is the safe
// direction: withdrawal requires the row's own stored expiry to have lapsed at
// that reading, so a row judged due at the bound and heartbeated since is still
// refused, and a row that lapsed after the bound is simply not in the page.
//
// A failure reading the due view returns the counts accrued so far beside the
// error rather than a zero result: a sweep that withdrew rows and then lost the
// provider did that work, and reporting nothing would make a caller's next
// decision — sweep again now, or wait — rest on a number it knows is false. It
// returns the walk's POSITION too, for the same reason it returns one on a
// budget exhaustion: without it a caller that lost the provider halfway pays
// the cost of every unreadable row ahead of it all over again.
//
// Each row is revalidated against its OWN STORED EXPIRY before anything is
// written, and the compare-and-swap onto the revision the page reported is what
// makes that revalidation binding rather than advisory. The two together are
// the whole guard: a Host that heartbeated after the page was read either
// presents an unlapsed expiry — in which case the sweep leaves it alone — or
// has already advanced the revision, in which case the write loses. Trusting
// the page's due state alone would withdraw the capacity of a Host that is
// alive.
func (s *Store) ReconcileHostTargets(
	ctx context.Context,
	req ReconcileHostTargetsRequest,
) (HostTargetReconcileResult, error) {
	limit, ok := s.pageLimit(req.Limit)
	if !ok {
		return HostTargetReconcileResult{}, hostTargetErr(HostTargetErrorInvalid, "limit", nil)
	}
	pages := req.MaxPages
	if pages == 0 {
		pages = DefaultHostTargetReconcilePages
	}
	if pages < 0 || pages > MaxHostTargetReconcilePages {
		return HostTargetReconcileResult{}, hostTargetErr(HostTargetErrorInvalid, "max_pages", nil)
	}

	now := s.clock.Now()
	bound := now.UnixMilli()
	var after storage.DueCursor
	if req.Cursor != "" {
		resumedBound, resumedAfter, err := s.decodeHostTargetSweepCursor(req.Cursor)
		if err != nil {
			return HostTargetReconcileResult{}, err
		}
		bound, after = resumedBound, resumedAfter
	}

	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return HostTargetReconcileResult{}, err
	}
	defer release()

	var result HostTargetReconcileResult
	for range pages {
		due, err := s.backend.OrderedIndex.ListDue(opCtx, hostTargetNamespace, bound, after, limit)
		if err != nil {
			// The walk's POSITION survives the failure. A caller that lost the
			// provider mid-walk would otherwise restart at the head of the due
			// view, paying the whole cost of every unreadable row ahead of it
			// again — which is the cost this continuation exists to stop
			// paying. The counts accrued so far travel with it for the reason
			// stated above.
			if after != "" {
				if next, cursorErr := s.encodeHostTargetSweepCursor(bound, after); cursorErr == nil {
					result.NextCursor = next
				}
			}
			return result, classifyHostTargetOrderedError(err, "list_due")
		}
		for _, stored := range due.Records {
			result.Scanned++
			if err := s.reconcileHostTargetRow(opCtx, stored, now, &result); err != nil {
				return result, err
			}
		}
		// The provider's own continuation is what steps over a row this sweep
		// could not handle: it resumes from the frozen (due_at, stable_key,
		// ordering_scope) tuple the page ended on rather than from a re-read of
		// the head, so a row left in place is passed rather than met again.
		after = due.NextCursor
		if after == "" {
			result.Exhausted = true
			return result, nil
		}
	}
	next, err := s.encodeHostTargetSweepCursor(bound, after)
	if err != nil {
		return result, err
	}
	result.NextCursor = next
	return result, nil
}

// The sweep continuation. Its payload is the due bound this sweep is querying
// at, followed by the provider's own due token carried verbatim.
//
// THE BOUND IS IN THE TOKEN because it has to be: the ordered index binds a due
// cursor to the exact bound that issued it, so a resumed call that recomputed
// the bound from its own clock would present a token for a different query and
// be refused. Carrying it is what makes resuming possible at all.
//
// Carrying it costs nothing in safety, and that is worth stating rather than
// assuming. The bound only selects WHICH rows a page contains; it decides
// nothing about them. Every row is revalidated against its own stored expiry at
// the sweep's current clock reading before anything is written, so a caller
// presenting a bound this store never issued can at worst make the sweep look
// at rows that are not lapsed, which the revalidation then declines to touch.
// The envelope's kind tag and scope are what stop a placement token being
// presented here; neither confers authority, as cursor.go states.
const (
	hostTargetSweepCursorMagic        = "LRHS"
	hostTargetSweepCursorVersion byte = 1

	hostTargetSweepBoundBytes = 8
)

// The sweep reuses the placement page's payload ceiling rather than declaring
// an equal one of its own. Two constants with the same definition are a
// distinction that is not there: they would be free to drift apart for no
// stated reason, and a reader would have to check whether the difference meant
// something. The ceiling is enforced on ISSUE as well as on presentation, so a
// token this store hands out is always one it will accept back — and for the
// sweep that is sharper than usual, because a continuation it cannot reissue is
// a sweep that silently reverts to making no progress.

// hostTargetSweepCursorScope binds the continuation to this cursor KIND and to
// nothing else. A sweep names no tenant, no target and no host — that is what
// makes it a service operation — so there is no identity to bind it to, and
// inventing one would suggest a scoping this operation does not have.
func (s *Store) hostTargetSweepCursorScope() [cursorScopeBytes]byte {
	return s.keys.digest(digestFrame("looprig/sessionstore/hosttarget/sweep/cursor/v1"))
}

func (s *Store) encodeHostTargetSweepCursor(bound int64, after storage.DueCursor) (sessionwire.Cursor, error) {
	payload := make([]byte, hostTargetSweepBoundBytes, hostTargetSweepBoundBytes+len(after))
	binary.BigEndian.PutUint64(payload, uint64(bound)) // #nosec G115 -- a signed bound round-trips through the same width
	payload = append(payload, after...)
	if len(payload) > maxHostTargetCursorPayload {
		return "", hostTargetErr(HostTargetErrorBackend, "next_cursor", nil)
	}
	token := encodeCursorEnvelope(
		hostTargetSweepCursorMagic, hostTargetSweepCursorVersion, s.hostTargetSweepCursorScope(), payload)
	return sessionwire.Cursor(token), nil
}

// decodeHostTargetSweepCursor unwraps a continuation this store issued for a
// sweep. A continuation this sweep issued always carries at least one provider
// byte beyond the bound, because an exhausted view returns no cursor at all.
func (s *Store) decodeHostTargetSweepCursor(cursor sessionwire.Cursor) (int64, storage.DueCursor, error) {
	payload, ok := decodeCursorEnvelope(
		hostTargetSweepCursorMagic, hostTargetSweepCursorVersion, s.hostTargetSweepCursorScope(),
		string(cursor), hostTargetSweepBoundBytes+1, maxHostTargetCursorPayload)
	if !ok {
		return 0, "", hostTargetErr(HostTargetErrorCursor, "cursor", nil)
	}
	bound := int64(binary.BigEndian.Uint64(payload[:hostTargetSweepBoundBytes])) // #nosec G115 -- the inverse of the encode above
	return bound, storage.DueCursor(payload[hostTargetSweepBoundBytes:]), nil
}

// reconcileHostTargetRow handles one row of a due page, counting its outcome.
//
// It returns an error only for a failure that is not about this row — a backend
// fault or an ambiguous mutation — because those say nothing about the row and
// would otherwise burn the whole page budget failing every one of them in turn.
// Everything row-specific is counted and stepped over.
//
// The row is decoded here and again inside hostTargetEntryFor, and that is
// deliberate. The sweep has no request to hold the row to, so it derives the
// target scope from the row's OWN bytes and then asks the same filing question
// a named read asks — which proves the provider filed the row where its
// contents say it belongs. Saving the second decode would mean writing a
// second, weaker filing check that only the sweep uses, and a shortcut that
// skips a check the slow path performs is exactly how a sweep starts writing
// over rows a named read would have refused.
func (s *Store) reconcileHostTargetRow(
	ctx context.Context,
	stored storage.OrderedRecord,
	now time.Time,
	result *HostTargetReconcileResult,
) error {
	record, err := decodeHostTarget(stored.Value)
	if err != nil {
		result.Unreadable++
		return nil
	}
	scope, err := s.deriveHostTargetScope(record.Key)
	if err != nil {
		result.Unreadable++
		return nil
	}
	entry, err := hostTargetEntryFor(stored, scope.TargetScope, record.Key, record.HostID)
	if err != nil {
		result.Unreadable++
		return nil
	}
	if hostTargetLiveness(entry.Target, now) != hostTargetLapsed {
		// Not lapsed. Either the Host heartbeated between the page being read
		// and this instant — the revalidation this sweep exists to perform — or
		// the provider named a row that is not due at all, which the filing
		// check above has already refused for a withdrawn one.
		result.StillLive++
		return nil
	}

	withdrawal := entry.Target
	withdrawal.Advertisement = nil
	withdrawal.ObservedAt = now
	value, withdrawal, err := encodeHostTarget(withdrawal)
	if err != nil {
		result.Unreadable++
		return nil
	}
	if _, err := s.writeHostTarget(ctx, scope, withdrawal, value, entry.Revision); err != nil {
		var failure *HostTargetError
		if errors.As(err, &failure) {
			switch failure.Code {
			case HostTargetErrorConflict, HostTargetErrorNotFound, HostTargetErrorDeleted:
				// The row moved under the sweep. Nothing was decided, which is
				// the correct outcome: a concurrent heartbeat wins.
				result.Contended++
				return nil
			case HostTargetErrorIdentity:
				// The compare-and-swap COMMITTED and the provider's reply then
				// failed this package's checks on it. That is neither a
				// withdrawal this sweep can claim nor a contention — the row is
				// durably handled and no later sweep will see it — so it is
				// counted as its own outcome and the pass continues. Ending the
				// pass here would leave the row attributed to nothing, which is
				// exactly the accounting this result promises never happens,
				// and would let one bad reply do to a sweep what one unreadable
				// row must not do to a placement page.
				result.Unverified++
				return nil
			}
		}
		return err
	}
	result.Withdrawn++
	return nil
}
