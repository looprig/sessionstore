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

// maxCatalogCursorBytes bounds a decoded catalog page cursor. It is
// unexported for the same reason the journal's cursor size is: a cursor is
// opaque, a caller can do nothing with its size but mis-size something, and the
// base64 spelling a caller actually holds is a third longer than this anyway.
const maxCatalogCursorBytes = 4 << 10

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
)

// timeType is the one instant type the projection text walk stops at. It is
// unrelated to the rank bounds above and is declared separately so neither
// doc comment describes the other's value.
var timeType = reflect.TypeOf(time.Time{})

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
	DesiredGeneration     uint64
	DesiredWorkload       DesiredWorkload
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
	DesiredWorkload        DesiredWorkload
	IdempotencyKey         string
}

// GetCatalogEntryRequest reads one session's authoritative catalog record.
type GetCatalogEntryRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
}

// ListSessionsRequest positions one bounded recent-first page of a tenant's
// sessions. Cursor is a token a previous page issued; Limit is that page's
// record ceiling, and zero means the store's configured page size.
type ListSessionsRequest struct {
	TenantID sessionwire.TenantID

	// Cursor is a token a previous page of THIS tenant issued. It is opaque:
	// retain it and hand it back, but do not parse it or derive ordering,
	// tenancy, or authority from it. Possessing one authorizes nothing — a
	// caller must authorize TenantID on its own — and a cursor this store did
	// not issue for this tenant is refused with CatalogErrorCursor, which
	// means the walk restarts from the first page rather than that anything is
	// wrong with the store.
	Cursor sessionwire.Cursor

	// Limit is the page's record ceiling. Zero means the store's configured
	// page size.
	Limit int
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
	DesiredWorkload        DesiredWorkload
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
		DesiredWorkload:        req.DesiredWorkload,
		DesiredIdempotencyKey:  req.IdempotencyKey,
		// Creating a session names its desired placement, so the create IS the
		// session's first desired-state write and the counter starts at one.
		// See nextDesiredGeneration for what a controller reads it for.
		DesiredGeneration: initialDesiredGeneration,
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
	if err := hostEpochFence(current.Record, req.LeaseEpoch); err != nil {
		return CatalogEntry{}, err
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
	next, err := applyDesiredState(current.Record, req)
	if err != nil {
		return CatalogEntry{}, err
	}
	return s.writeCatalogRecord(opCtx, scope, next, current.Revision)
}

// ListSessions returns one bounded recent-first page of a tenant's sessions.
//
// The whole page is one ranked provider query. The tenant is the ranking scope,
// so the restriction and the recency order are both inside the query and the
// limit applies to an already-restricted, already-ordered result. Nothing here
// enumerates a prefix, sorts a catalog, or filters a wider page afterwards:
// those all cost work proportional to a tenant's history rather than to the
// page, and a Factory calls this on every picker render.
//
// This deliberately does NOT verify the tenant's collision witness, which is
// where it differs from GetCatalogEntry. A direct get names a session and must
// prove that session's binding before it trusts a derived name; a list names no
// session, and a tenant that has never created one has no binding to prove, so
// requiring one would answer "this tenant is empty" with a failure. Cross-tenant
// safety instead comes from below: every record the provider returns is held to
// the tenant it itself claims, so a scope two tenants somehow shared would fail
// the page closed rather than disclose a row.
func (s *Store) ListSessions(ctx context.Context, req ListSessionsRequest) (SessionPage, error) {
	limit, ok := s.pageLimit(req.Limit)
	if !ok {
		return SessionPage{}, catalogErr(CatalogErrorInvalid, "limit", nil)
	}
	scope, err := s.deriveTenantScope(req.TenantID)
	if err != nil {
		return SessionPage{}, err
	}
	var after storage.RankedCursor
	if req.Cursor != "" {
		if after, err = s.decodeCatalogCursor(req.TenantID, req.Cursor); err != nil {
			return SessionPage{}, err
		}
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return SessionPage{}, err
	}
	defer release()

	ranked, err := s.backend.OrderedIndex.ListRanked(opCtx, catalogNamespace, scope.CatalogScope, after, limit)
	if err != nil {
		return SessionPage{}, classifyCatalogOrderedError(err, "list")
	}
	page := SessionPage{SessionPage: sessionwire.SessionPage{
		Sessions: make([]sessionwire.SessionSummary, 0, len(ranked.Records))}}
	for _, stored := range ranked.Records {
		// The stable key is the identity the provider filed the record under
		// and the record carries its own; catalogEntry holds one to the other
		// and both to the requested tenant, which is the same check a direct
		// get makes and is deliberately not restated here.
		//
		// A ROW THAT FAILS IS SKIPPED AND COUNTED, never returned as the page's
		// error. This reader failed the whole page for a long time, on the
		// argument that skipping would hide a keyspace collision — but nothing
		// in this package rewrites a row it cannot vouch for, so the row is
		// permanent, and a failed page issues no continuation, so every session
		// ranked behind it becomes unreachable too. That is not fail-closed, it
		// is a tenant permanently unlistable with no repair API to repair it
		// with. UnreadableSkipped answers the original objection directly: the
		// condition is reported to the caller rather than hidden, and the row
		// itself is still never disclosed.
		entry, err := catalogEntry(stored, req.TenantID, sessionwire.SessionID(stored.ID.StableKey))
		if err != nil {
			page.UnreadableSkipped++
			continue
		}
		summary, err := entry.Record.Summary()
		if err != nil {
			page.UnreadableSkipped++
			continue
		}
		page.Sessions = append(page.Sessions, summary)
	}
	if ranked.NextCursor != "" {
		next, err := s.encodeCatalogCursor(req.TenantID, ranked.NextCursor)
		if err != nil {
			return SessionPage{}, err
		}
		page.NextCursor = next
	}
	// Core owns what "recent-first" means for this page shape, so the order is
	// verified by asking core rather than by comparing timestamps again here.
	// A page that fails is a provider that did not deliver the descending rank
	// order ListRanked promises, and publishing it would hand a caller a page
	// whose own contract it violates.
	if err := page.Validate(); err != nil {
		return SessionPage{}, catalogErr(CatalogErrorBackend, "sessions", err)
	}
	return page, nil
}

// SessionPage is one bounded page of a tenant's sessions together with what
// producing it cost, as DueGatePage is for the deadline view.
//
// It embeds Core's page rather than replacing it, so a caller still reads
// Sessions and NextCursor directly and can hand the embedded value to anything
// that takes a sessionwire.SessionPage.
//
// UnreadableSkipped counts rows this reader could not hold to their own
// identity and therefore did not publish. It is not a diagnostic afterthought:
// it is what makes skipping such a row safe to do at all, because it leaves the
// caller able to tell "this tenant has three sessions" from "this tenant has
// three sessions and one row I could not vouch for". A nonzero count is durable
// — nothing in this package rewrites such a row — so it means a build that
// understands the row is needed, not that a retry will help.
type SessionPage struct {
	sessionwire.SessionPage

	UnreadableSkipped int
}

// The catalog page cursor. Its payload is the provider's own ranked cursor,
// carried verbatim: this package never parses it, and "the rest of the
// envelope" is the whole rule, so no length prefix or grammar of ours reaches
// inside it. What the envelope adds is a binding the provider is not obliged to
// give a CALLER of this package, plus a kind tag, so a catalog page and a
// journal page cannot be replayed into each other. See the envelope grammar in
// cursor.go, including why the scope field is a binding tag and not a MAC.
//
// It also keeps the provider's grammar out of this package's public API: a
// caller retains a SessionStore token, not a memstore or JetStream one.
const (
	catalogCursorMagic        = "LRCP"
	catalogCursorVersion byte = 1

	// maxCatalogCursorPayload is the largest provider token this envelope can
	// carry. It is enforced when a cursor is issued as well as when one is
	// presented, so a token this store hands out is always a token it will
	// accept back and a caller can never be given an unusable continuation.
	maxCatalogCursorPayload = maxCatalogCursorBytes - cursorPayloadAt
)

func (s *Store) catalogCursorScope(tenant sessionwire.TenantID) [cursorScopeBytes]byte {
	return s.keys.digest(digestFrame("looprig/sessionstore/catalog/cursor/v1", []byte(tenant)))
}

// encodeCatalogCursor wraps one provider continuation token for one tenant.
func (s *Store) encodeCatalogCursor(tenant sessionwire.TenantID, next storage.RankedCursor) (sessionwire.Cursor, error) {
	if len(next) > maxCatalogCursorPayload {
		return "", catalogErr(CatalogErrorBackend, "next_cursor", nil)
	}
	token := encodeCursorEnvelope(catalogCursorMagic, catalogCursorVersion, s.catalogCursorScope(tenant), []byte(next))
	return sessionwire.Cursor(token), nil
}

// decodeCatalogCursor unwraps a continuation token this store issued for this
// tenant and returns the provider token inside it. A continuation this reader
// issued always carries at least one payload byte, because an exhausted page
// returns no cursor at all rather than an empty one.
func (s *Store) decodeCatalogCursor(tenant sessionwire.TenantID, cursor sessionwire.Cursor) (storage.RankedCursor, error) {
	payload, ok := decodeCursorEnvelope(
		catalogCursorMagic, catalogCursorVersion, s.catalogCursorScope(tenant),
		string(cursor), 1, maxCatalogCursorPayload)
	if !ok {
		return "", catalogErr(CatalogErrorCursor, "cursor", nil)
	}
	return storage.RankedCursor(payload), nil
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

// hostEpochFence admits a Host-owned write against the record's committed
// high-water epoch. Every path that writes Host-owned fields shares it —
// UpdateCatalogHostState and both gate writes — so none of them can drift into
// a different idea of when a Host has been superseded.
//
// It is epochFence in the catalog's vocabulary; the rule, and why an equal
// epoch is admitted, are stated there.
func hostEpochFence(current CatalogRecord, epoch uint64) error {
	return epochFence(current.LeaseEpoch, epoch, func(committed uint64) error {
		return &CatalogError{Code: CatalogErrorEpoch, Field: "lease_epoch", Epoch: committed}
	})
}

// epochFence is the fencing rule every Host-owned write in this package shares,
// stated once.
//
// FOUR records had byte-identical copies of it differing only in the error they
// built: the catalog's projection, a command's claim, the Host registration,
// and the object pointers. The fourth was found after the first three were
// hoisted, which is the argument for the hoist rather than against it — the
// copies are structurally identical and easy to miss, and inbox_claim.go's own
// header had said for three tasks that its fence WAS this rule. What each copy
// was free to do was drift — to refuse an equal epoch,
// or to report the REQUESTED mark instead of the committed one — on the one
// path that only runs when a Host has already been superseded and where a
// weakened check therefore looks exactly like a passing one. It is the argument
// checkFiledScope and validateOpaque make, and it applies with more force here,
// because this is the rule that decides who may write at all.
//
// committed is the high-water mark the calling record measures against —
// usually its own stored epoch, and for a command its current CLAIM's epoch —
// and requested is the epoch the caller named. An equal epoch is ADMITTED, because one lease grant
// legitimately writes many times; only a strictly lower one has provably lost
// the session. The mark never falls, which is why every record that carries one
// is retained rather than deleted.
//
// fail receives the COMMITTED mark rather than the requested one, because that
// is the only value a refused caller can act on, and it names what a violation
// is CALLED in the calling record's vocabulary — the RULE is stated here.
//
// The zero check is deliberately not here. Whether an epochless request is a
// caller mistake or a corrupted record depends on which side supplied it, so
// each caller makes that check before its read; folding it in would report a
// caller mistake as whatever the read happened to find.
func epochFence(committed, requested uint64, fail func(committed uint64) error) error {
	if requested < committed {
		return fail(committed)
	}
	return nil
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
	// A limit error has no arm: every limit this package sends is normalized by
	// pageLimit against storage.MaxOrderedPageLimit, which is also the ceiling
	// WithLimits validates the configured page size against, so the provider
	// cannot see one it would refuse.
	var cursor *storage.InvalidOrderedCursorError
	if errors.As(err, &cursor) {
		return catalogErr(CatalogErrorCursor, field, err)
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
	DesiredGeneration      uint64                       `json:"desired_generation"`
	DesiredWorkload        *desiredWorkloadWire         `json:"desired_workload,omitempty"`
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
		DesiredGeneration:      record.DesiredGeneration,
	}
	if !record.DesiredWorkload.isZero() {
		wire.DesiredWorkload = &desiredWorkloadWire{
			PayloadVersion: record.DesiredWorkload.PayloadVersion,
			Payload:        record.DesiredWorkload.Payload,
		}
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
	wire, err := decodeVersionedRecord[catalogWire](
		value, MaxCatalogRecordBytes, CatalogRecordVersion,
		versionedRecordFields{Record: "record", Version: "record_version"}, catalogRecordFailure)
	if err != nil {
		return CatalogRecord{}, err
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
		DesiredGeneration:      wire.DesiredGeneration,
	}
	if wire.DesiredWorkload != nil {
		record.DesiredWorkload = DesiredWorkload{
			PayloadVersion: wire.DesiredWorkload.PayloadVersion,
			Payload:        wire.DesiredWorkload.Payload,
		}
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

// decodeVersionedRecord is the one strict decode this package's stored records
// share. Every durable record it writes is a versioned JSON document, and the
// rules below are properties of that shape rather than of any one record, so
// they are stated once: a second copy would be free to drop the bound, the
// version gate, or the strictness that keeps an undeclared member from being
// silently accepted.
//
// Order matters and is the reason for the two passes. The bound is checked
// before anything decodes, so a reader never allocates in proportion to a
// stored value it has already decided is too large. The version is then read
// from a tolerant first pass, so an unknown version is reported as a version
// failure rather than as whichever member the strict decoder happened to trip
// over first.
//
// That first pass is also the package's one well-formedness rule:
// json.Unmarshal requires the whole value to be exactly one JSON document, so
// trailing content is rejected there. The streaming decoder below therefore
// never needs a second trailing check — a decoder.More() call could not observe
// anything the first pass has not already refused, and two statements of one
// rule is how one of them later rots.
//
// Strictness is a RECORD-level property and stops at the record's own members.
// What a caller does with the decoded wire value — validating it, walking
// nested projections, canonicalizing it — belongs to that record's own decoder.
// fail names what a decode failure is CALLED in the caller's record vocabulary.
// The three rules — the bound, the well-formedness of the document, and the
// version gate — are properties of the shared shape and are stated here; only
// their names belong to each record, so each record keeps its own error type
// and a caller branching on an inbox failure need not match the catalog's.
//
// It is a parameter rather than a member of fields because a record that
// forgets it must not compile: a missing argument is a compile error, while a
// missing struct member would be a nil call at decode time, on the one path
// that only runs when a stored record is already suspect.
func decodeVersionedRecord[T any](
	value []byte,
	maxBytes int,
	wantVersion uint8,
	fields versionedRecordFields,
	fail func(failure versionedRecordFailure, field string, cause error) error,
) (T, error) {
	var wire T
	if len(value) > maxBytes {
		return wire, fail(versionedRecordTooLarge, fields.Record, nil)
	}
	if len(value) == 0 {
		return wire, fail(versionedRecordMalformed, fields.Record, nil)
	}
	var probe struct {
		RecordVersion uint8 `json:"record_version"`
	}
	if err := json.Unmarshal(value, &probe); err != nil {
		return wire, fail(versionedRecordMalformed, fields.Record, err)
	}
	if probe.RecordVersion != wantVersion {
		return wire, fail(versionedRecordVersion, fields.Version, nil)
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		// Deliberately the zero value rather than wire: a failed Decode may
		// have populated some members before it stopped, and handing a caller a
		// half-decoded record beside an error is how one of them ends up used.
		var zero T
		return zero, fail(versionedRecordMalformed, fields.Record, err)
	}
	return wire, nil
}

// versionedRecordFields names the members a stored record's decode failures are
// reported against. It is a struct rather than two adjacent string parameters
// because two adjacent strings of one type are silently swappable, and the two
// call sites already spell them differently enough — "record_version" against
// "gate_intent.record_version" — that a swapped pair would read as plausible.
type versionedRecordFields struct {
	Record  string
	Version string
}

// versionedRecordFailure is the closed set of failures the shared versioned
// decode can produce. It is an enum rather than a string so a caller's mapping
// is exhaustive by construction and cannot be handed the wrong spelling.
type versionedRecordFailure uint8

const (
	versionedRecordTooLarge versionedRecordFailure = iota + 1
	versionedRecordMalformed
	versionedRecordVersion
)

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
	if err := validateOptionalOpaque(record.RuntimeCompatibilityID, "runtime_compatibility_id", catalogInvalid); err != nil {
		return CatalogRecord{}, err
	}
	if err := validateOpaque(string(record.State), "state", catalogInvalid); err != nil {
		return CatalogRecord{}, err
	}
	if err := validateOpaque(string(record.Residency), "residency", catalogInvalid); err != nil {
		return CatalogRecord{}, err
	}
	if err := validateDesiredPlacement(record.DesiredPlacement); err != nil {
		return CatalogRecord{}, err
	}
	// Every catalog record is created with a desired state, so a zero
	// generation is a record this package never wrote rather than a session
	// whose placement has not been decided yet.
	if record.DesiredGeneration == 0 {
		return CatalogRecord{}, catalogErr(CatalogErrorInvalid, "desired_generation", nil)
	}
	workload, err := canonicalDesiredWorkload(record.DesiredWorkload)
	if err != nil {
		return CatalogRecord{}, err
	}
	record.DesiredWorkload = workload
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
	if err := validateOptionalOpaque(record.DesiredIdempotencyKey, "desired_idempotency_key", catalogInvalid); err != nil {
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
		if err := validateProjectionText(reflect.ValueOf(gates[i]), "open_gates["+strconv.Itoa(i)+"]", catalogInvalid); err != nil {
			return nil, err
		}
		gates[i].Deadline = gates[i].Deadline.UTC()
	}
	// The inner comparison runs only where the sequences DIFFER, so "<" and
	// "<=" are the same predicate there — an equivalent mutation, recorded so
	// it is not re-derived as a survivor. The outer inequality is not: reading
	// it as equality reverses the tie-break, which is what
	// TestCanonicalGateOrderBreaksATieBySessionGateID holds.
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
//
// fail names what a violation is called in the calling record's vocabulary. The
// RULE is stated once here; only its name belongs to the record.
func validateOpaque(value, field string, fail func(field string, cause error) error) error {
	if value == "" || len(value) > sessionwire.MaxIDBytes || !utf8.ValidString(value) {
		return fail(field, nil)
	}
	return nil
}

// checkFiledScope holds the three provider-supplied components of a stored
// record's filing that every session-scoped record kind files identically: the
// ordering scope, the ranking scope, and the due state.
//
// Every session-scoped record kind needs exactly this, for exactly the same
// reasons, and each held its own byte-identical copy of it differing only in
// the error constructor. What each copy was free to do was drift — to drop the ranking scope, or to compare
// the due state's milliseconds instead of the whole value — on the one path
// that only runs when a provider is already misbehaving and where a weakened
// check therefore looks exactly like a passing one.
//
// The two scopes are the SESSION's physical namespace, which is derived from
// the identities the record's own bytes name, so this is a comparison against
// the record rather than against the query the reader issued. Neither can
// change after Create, so a disagreement means the record was filed wrongly to
// begin with — and a wrong ordering scope means any per-session order the
// provider allocated came from another session's sequence.
//
// due is passed in rather than derived here because each record derives its own
// from its own members — inboxDue folds a claim horizon in, gateDue reads a
// deadline, hostRegistrationDue is constant — and it is compared as a WHOLE
// VALUE so the due STATE is covered as well as the instant. A record filed
// not-due that should be due participates in no deadline page and is never
// reconciled, and a millisecond-only comparison accepts exactly that.
//
// What is NOT here is each caller's business and stays in each caller's own
// enumeration: whether a tombstone is a lifecycle state or a fail-closed
// condition, what the stable key is held to, whether a rank or an order has a
// consumer worth guarding. Those differ between the records; these three do
// not. fail names what a violation is CALLED in the calling record's
// vocabulary, as it does for validateOpaque — the RULE is stated once here.
func checkFiledScope(
	stored storage.OrderedRecord,
	scope string,
	due storage.Due,
	fail func(field string, cause error) error,
) error {
	if stored.ID.OrderingScope != scope {
		return fail("ordering_scope", nil)
	}
	if stored.RankingScope != scope {
		return fail("ranking_scope", nil)
	}
	if stored.Due != due {
		return fail("due", nil)
	}
	return nil
}

// validateBoundedExpiry bounds one caller-supplied expiry against the store's
// clock. Two records already need it — a command's claim and a Host's
// registration — and they need exactly the same two rules for the same two
// reasons, so it is stated once and each record supplies only its own ceiling
// and its own error vocabulary, as validateOpaque does for text.
//
// An expiry must lapse in the FUTURE. One born expired is indistinguishable
// from an absent one to every guard that reads it, so accepting one lets a
// caller write a state it can never act on.
//
// And it must lapse within max, because an unbounded expiry is a durable
// liveness fault one caller can commit alone. Each ceiling documents what its
// own record loses without it; what is common is that nothing bounds a Clock,
// so "eventually" would otherwise mean rankableTime, which is centuries.
//
// Sub rather than now.Add(max), and the reason is that Sub needs no reasoning
// about saturation at all. Nothing bounds a Clock, so the two operands can be
// centuries apart; Sub clamps a gap it cannot represent to about 292 years,
// which still exceeds every ceiling here, so an unrepresentable gap is REFUSED
// by a defined rule.
//
// The two spellings are nonetheless EQUIVALENT here, and the argument is worth
// writing down because both earlier attempts at it were wrong about the
// mechanism. time.Time.Add saturates toward LATER — an instant ten seconds
// below the representable maximum plus an hour advances by those ten seconds
// and stops — so an Add horizon can only ever be too FAR OUT, which is the
// direction that would wrongly admit. But the two saturations have disjoint
// causes: Sub saturates when the GAP exceeds about 292 years, while Add
// saturates only when NOW ITSELF is near year 292277024627. Every caller has
// already bounded expiresAt with rankableTime, which caps it at year 2262, so
// at any now large enough to saturate Add the FIRST check has already refused
// the request — now is not before expiresAt. There is no instant at which the
// two disagree, and a mutation swapping them survives, which is recorded here
// rather than papered over.
//
// Sub is kept because its behaviour at the boundary is specified and clamps
// toward refusal, so this guard does not depend on the caller's bound staying
// where it is.
func validateBoundedExpiry(
	expiresAt, now time.Time,
	max time.Duration,
	field string,
	fail func(field string, cause error) error,
) error {
	if !now.Before(expiresAt) {
		return fail(field, nil)
	}
	if expiresAt.Sub(now) > max {
		return fail(field, nil)
	}
	return nil
}

// validateOptionalOpaque is validateOpaque for a field whose absence is legal.
func validateOptionalOpaque(value, field string, fail func(field string, cause error) error) error {
	if value == "" {
		return nil
	}
	return validateOpaque(value, field, fail)
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
func validateProjectionText(value reflect.Value, path string, fail func(field string, cause error) error) error {
	switch value.Kind() {
	case reflect.String:
		if !utf8.ValidString(value.String()) {
			return fail(path, nil)
		}
	case reflect.Pointer, reflect.Interface:
		if !value.IsNil() {
			return validateProjectionText(value.Elem(), path, fail)
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
			if err := validateProjectionText(value.Index(i), path+"["+strconv.Itoa(i)+"]", fail); err != nil {
				return err
			}
		}
	case reflect.Map:
		// NO sessionwire projection type reachable from here contains a map
		// today, so this arm is unreachable and no test drives it. It is kept
		// rather than replaced by a refusal because the walk's whole argument
		// is that an enumeration goes stale when core adds a member: an arm
		// that handles a shape core does not yet use is the same argument
		// applied to shapes rather than to names. A refusal here would turn a
		// future additive map member into a rejected record.
		for _, key := range value.MapKeys() {
			member := path + "." + key.String()
			if err := validateProjectionText(key, member, fail); err != nil {
				return err
			}
			if err := validateProjectionText(value.MapIndex(key), member, fail); err != nil {
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
			if err := validateProjectionText(value.Field(i), path+"."+name, fail); err != nil {
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
