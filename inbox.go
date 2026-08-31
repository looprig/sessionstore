package sessionstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// This file adds durable command ADMISSION and nothing else.
//
// A command becomes durable exactly once, under an identity the client chose,
// with an acceptance order the provider allocated and an apply deadline filed
// as the record's due state. What happens to it afterwards — the
// pending/claimed/applying/applied/rejected transitions, the correlation of a
// journal application prefix, the deadline reconciler — belongs to later tasks.
// The record declares those members because they are part of one authoritative
// record and a reader here must be able to decode a record a later writer
// produced; this file never moves a command out of pending.
//
// Why the inbox is its own aggregate rather than catalog state
//
// Gates live in the catalog record and their deadline index is an index INTO
// it, so gate failures speak the catalog's vocabulary. A command does not: it
// has its own record, its own identity, its own namespace, and it never reads
// or writes a session's catalog record. Its failures are therefore its own
// (see InboxError), and admission touches no catalog state at all.
//
// What admission deliberately does NOT check
//
//   - That the session has a catalog record. The V1 create command carries a
//     client-chosen SessionID and is admitted BEFORE that session exists, which
//     is what removes the circular need to allocate a recoverable SessionID
//     before a per-session inbox exists. Requiring a catalog record here would
//     make the create command unadmittable.
//   - That the apply deadline is in the future. A retry of an unknown outcome
//     may legitimately arrive after the original deadline has passed, and it
//     must still be able to learn the mapping that was durably accepted. A
//     future-deadline gate would refuse exactly that retry — the one case the
//     retry contract exists for — so the deadline is validated as an instant
//     and nothing more. A command accepted already past its deadline is
//     immediately eligible for a later task's deadline reconciler, which is a
//     better outcome than a caller unable to record it at all.

// inboxNamespace is the one OrderedIndex namespace holding command records. It
// is a single namespace for the whole deployment, with tenants and sessions
// separated by the ordering scope, for the reason gateNamespace states: a
// namespace is a provider's physical partition, and the due view is
// namespace-wide, so per-tenant namespaces would both multiply provider
// streams and make a deployment-wide due query impossible.
const inboxNamespace = "sessionstore/inbox"

const (
	// InboxRecordVersion is the independent version of the stored command
	// record. A reader fails closed on any other version rather than guessing
	// which members a future encoder meant.
	InboxRecordVersion uint8 = 1

	// MaxInboxPayloadBytes bounds an INLINE private command payload. A body
	// larger than this is stored as an object and referenced, which is what
	// InboxRecord.PayloadRef is for: the inbox is a control record that a
	// reconciler pages through, not a blob store.
	MaxInboxPayloadBytes = 64 << 10

	// MaxInboxRecordBytes bounds an encoded command record. Like the catalog's
	// bound it is well below storage.MaxOrderedValueBytes, so a record this
	// package accepts always fits in the provider and there is no state that
	// can be written but not rewritten.
	MaxInboxRecordBytes = 256 << 10
)

// Stated as an unsigned constant for the reason the catalog states its own: an
// oversized record is refused here rather than by the provider, and prose
// cannot enforce that relationship.
const _ = uint(storage.MaxOrderedValueBytes - MaxInboxRecordBytes)

// RuntimeCommandID is the runtime-facing identity a Host forwards into the
// Harness API. It is a named type rather than a bare string because it travels
// beside the public CommandID in almost every signature, and two adjacent
// strings of one type are silently swappable.
//
// This package does not impose a UUID grammar on it. Harness allocates UUIDs
// today, but the durable MAPPING is what this record exists to make
// authoritative; a grammar check here would be a second statement of a rule
// this package does not own, and it would refuse a future runtime identity
// without adding any safety. It is validated as a bounded opaque UTF-8 value,
// exactly as the catalog validates the identities it does not own.
type RuntimeCommandID string

// CommandKind names what the command asks the session to do.
//
// The set is deliberately not enumerated here. Core accepts any non-empty
// state, residency, and gate kind so a later wire version can add one, and a
// closed set in this package would refuse a command Core itself considers
// valid. It is validated as a bounded opaque UTF-8 value.
type CommandKind string

// InboxState is the durable processing state of one accepted command.
//
// The transitions between them — including which writer may make each one,
// what a claim epoch fences, and what a terminal CAS must set — are NOT
// implemented in this file. Admission produces InboxStatePending and never
// anything else; the other states are declared because one authoritative record
// carries them and a reader here must decode a record a later writer produced.
type InboxState string

const (
	InboxStatePending  InboxState = "pending"
	InboxStateClaimed  InboxState = "claimed"
	InboxStateApplying InboxState = "applying"
	InboxStateApplied  InboxState = "applied"
	InboxStateRejected InboxState = "rejected"
)

// terminal reports whether the command has reached a final state. It is stated
// once because the record's due state is derived from it: see inboxDue.
func (s InboxState) terminal() bool {
	return s == InboxStateApplied || s == InboxStateRejected
}

// CommandClaim is the current short-lived claim on a command. Its zero value
// means the command is unclaimed.
//
// LeaseEpoch is the claiming Host's grant epoch. It is recorded here rather
// than fencing anything in this file: admission never writes a claim.
type CommandClaim struct {
	LeaseEpoch uint64
	ExpiresAt  time.Time
}

func (c CommandClaim) isZero() bool { return c.LeaseEpoch == 0 && c.ExpiresAt.IsZero() }

// CommandResult is the terminal application result: the durable journal event
// that carried the command's effect. Its zero value means the command has not
// been applied.
type CommandResult struct {
	CompletedAt time.Time
	EventID     sessionwire.EventID
	JournalSeq  uint64
}

func (r CommandResult) isZero() bool {
	return r.CompletedAt.IsZero() && r.EventID == "" && r.JournalSeq == 0
}

// InboxRecord is the authoritative durable record of one accepted command.
//
// Payload and PayloadRef are PRIVATE: they are the command's body, they are
// never part of a public projection, and no failure this package returns
// carries either of them. At most one of the two is set — an inline body up to
// MaxInboxPayloadBytes, or a reference to an object persisted first — and
// neither is set for a command that has no body.
//
// The acceptance order is deliberately NOT a member here. It is allocated by
// the provider at Create and is therefore not part of the bytes this record
// encodes; it is reported on InboxEntry, where its origin is unambiguous.
type InboxRecord struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID

	RuntimeCommandID RuntimeCommandID
	Kind             CommandKind

	Payload    []byte
	PayloadRef sessionwire.ObjectReference

	AcceptedAt    time.Time
	ApplyDeadline time.Time

	State     InboxState
	Claim     CommandClaim
	Result    CommandResult
	Rejection *sessionwire.ErrorDetail
}

// InboxEntry is a command record together with the provider state a caller
// needs: the revision a later compare-and-swap names, and the immutable
// acceptance order.
//
// AcceptedOrder is exposed, and CatalogEntry's order deliberately is not. The
// difference is that a catalog record's order means nothing to anyone — a
// session's position in a creation stream is not a fact any caller acts on —
// while a command's acceptance order is the durable arrival order of commands
// within a session, which consumers sort a bounded ListOrdered page by and
// which a retry must receive unchanged as evidence that it is the same
// acceptance.
//
// It is an OPAQUE COMPARISON KEY and nothing else:
//
//   - It is strictly increasing within one session's order scope: once a
//     command has order 12, no command in that session becomes newly
//     observable at 11.
//   - It is NOT contiguous and NOT one-based. A provider may allocate from a
//     JetStream stream sequence or a shared SQL sequence, so a session's first
//     command can have order 5000 and its second 9000. Nothing may derive a
//     count, a position, or "the next" order from it.
//   - It is not comparable across sessions or tenants. Two sessions' orders
//     come from different scopes and may interleave arbitrarily.
type InboxEntry struct {
	Record        InboxRecord
	Revision      uint64
	AcceptedOrder uint64
}

// AdmitCommandRequest accepts one client command into a session's inbox. It is
// idempotent by (TenantID, SessionID, CommandID): a repeat returns the stored
// record unchanged with created false.
//
// ProposedRuntimeCommandID is a PROPOSAL. Racing replicas may propose different
// runtime identities for one public CommandID; only the one in the winning
// record is stored, returned, and used, and a losing replica receives the
// winner's rather than its own. A caller must therefore use the returned
// mapping and never the value it sent.
//
// AcceptedAt and ApplyDeadline are the caller's clock readings, as every other
// timestamp this package stores is. They belong to the WINNER: a duplicate
// returns the accepted instant and deadline that were durably committed, not
// the ones it just sent, and they take no part in deciding whether a duplicate
// conflicts — see AdmitCommand.
type AdmitCommandRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID

	ProposedRuntimeCommandID RuntimeCommandID
	Kind                     CommandKind

	Payload    []byte
	PayloadRef sessionwire.ObjectReference

	AcceptedAt    time.Time
	ApplyDeadline time.Time
}

// AdmitCommand makes one command durable and reports whether this call is the
// one that accepted it.
//
// It is exactly one CreateOrdered call. The provider's Create is atomically
// idempotent by identity, so the duplicate case needs no read of its own: a
// duplicate arrives as the winner's canonical stored record with created
// false, carrying the winning runtime mapping and the immutable acceptance
// order.
//
// What makes a duplicate a CONFLICT rather than a retry, and why:
//
//   - Kind, Payload, and PayloadRef must match. Reusing one command id for a
//     DIFFERENT command must fail closed; silently returning the first
//     command's record would tell the caller its command was accepted when
//     nothing of the kind happened.
//   - The proposed RuntimeCommandID is deliberately excluded. Disagreeing about
//     it is the normal, expected outcome of a race, and the winner's value is
//     the answer.
//   - AcceptedAt and ApplyDeadline are deliberately excluded. A retry carries a
//     fresh clock reading — a caller that computes an absolute deadline from
//     "now" produces a different one on every attempt — so comparing them would
//     turn every real retry into a conflict.
//   - State, Claim, Result, and Rejection are deliberately excluded. By the
//     time a retry arrives the command may already be claimed, applied, or
//     rejected; that progress is not evidence that this retry differs, and a
//     retry of a completed command must still receive its mapping.
func (s *Store) AdmitCommand(ctx context.Context, req AdmitCommandRequest) (InboxEntry, bool, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return InboxEntry{}, false, err
	}
	record := InboxRecord{
		TenantID:         req.TenantID,
		SessionID:        req.SessionID,
		CommandID:        req.CommandID,
		RuntimeCommandID: req.ProposedRuntimeCommandID,
		Kind:             req.Kind,
		Payload:          req.Payload,
		PayloadRef:       req.PayloadRef,
		AcceptedAt:       req.AcceptedAt,
		ApplyDeadline:    req.ApplyDeadline,
		State:            InboxStatePending,
	}
	// Encoding validates, so an invalid request is refused before any provider
	// work — including before the session's witnesses are bound. Everything
	// after this point uses the CANONICAL record it returns rather than the
	// request-shaped one built above: the due state filed and the content
	// compared against a stored record must both be derived from the same
	// normal form the stored bytes are in.
	value, candidate, err := encodeInboxRecord(record)
	if err != nil {
		return InboxEntry{}, false, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return InboxEntry{}, false, err
	}
	defer release()
	// A command is durable session data, and it may be the FIRST durable data a
	// session has, so admission binds the session's collision witnesses exactly
	// as creating a catalog record does. The binding is create-only and
	// idempotent, so a later catalog create agrees with it rather than
	// colliding.
	if err := s.bindSessionScope(opCtx, scope); err != nil {
		return InboxEntry{}, false, err
	}
	stored, created, err := s.backend.OrderedIndex.Create(
		opCtx, inboxID(scope, req.CommandID), scope.SessionNamespace, value, storage.Rank{}, inboxDue(candidate))
	if err != nil {
		return InboxEntry{}, false, classifyInboxOrderedError(err, "create")
	}
	entry, err := inboxEntryFor(stored, scope, req.TenantID, req.SessionID, req.CommandID)
	if err != nil {
		return InboxEntry{}, false, err
	}
	if created {
		// The created path is the one place the provider claims something
		// stronger than "here is the record under that identity": it claims it
		// stored the bytes THIS call handed it. Every check above holds the
		// reply to the record's own bytes, which a substituted record satisfies
		// just as well as the real one, so without this a provider could answer
		// created=true carrying another command's kind, payload and runtime
		// mapping and the caller would take it for its own fresh acceptance.
		//
		// The comparison is the whole value rather than sameCommandAs plus the
		// runtime id: on this path every member is ours, so an exact comparison
		// covers the timestamps and the state as well as the content, and it is
		// exact because canonicalization is a fixed point — the bytes were
		// produced by encodeInboxRecord and the record decodes and re-encodes
		// to them.
		//
		// It is deliberately NOT run on the duplicate path, where the stored
		// bytes are the winner's: they differ from ours in the runtime mapping
		// and the timestamps by design, and content is the only thing that can
		// be compared. See sameCommandAs.
		if !bytes.Equal(stored.Value, value) {
			return InboxEntry{}, false, inboxErr(InboxErrorIdentity, "value", nil)
		}
		return entry, true, nil
	}
	if !entry.Record.sameCommandAs(candidate) {
		return InboxEntry{}, false, inboxErr(InboxErrorCommandMismatch, "command", nil)
	}
	return entry, false, nil
}

// sameCommandAs reports whether two records describe the same command to
// admit. It compares the command's CONTENT only: the identity has already been
// held to the caller's request by inboxEntryFor, and restating it here would be
// a second copy of that rule. Everything excluded from the comparison, and why,
// is enumerated on AdmitCommand.
//
// bytes.Equal treats a nil payload and an empty one as equal, which is the same
// normalization canonicalInboxRecord applies, so a caller cannot make two
// spellings of "no inline body" look like a mismatch.
//
// CARRY-FORWARD CONTRACT, the dual of AdmitCommand's exclusion list: THE
// MEMBERS COMPARED HERE ARE IMMUTABLE FOR THE LIFE OF THE RECORD. A retry
// arrives with the command the caller originally sent, and it is compared
// against whatever the record holds NOW, so clearing or rewriting Kind, Payload
// or PayloadRef after acceptance makes every subsequent retry a permanent
// InboxErrorCommandMismatch — telling a caller, incorrectly and forever, that
// it reused a command id for a different command.
//
// This file supplies the motive to break it: MaxInboxPayloadBytes calls the
// inbox a control record rather than a blob store, which makes emptying an
// applied command's 64 KiB body the obvious housekeeping move. It is not
// available. A body that needs reclaiming has to be behind a PayloadRef whose
// VALUE stays stable while the object it names is collected, so the compared
// member never changes even though the bytes it points at are gone.
func (r InboxRecord) sameCommandAs(other InboxRecord) bool {
	return r.Kind == other.Kind &&
		bytes.Equal(r.Payload, other.Payload) &&
		r.PayloadRef == other.PayloadRef
}

// inboxID names the one ordered record per command. The ordering scope is the
// session's physical namespace, so a session's commands share one order scope
// — which is what makes the acceptance order per-session — and the stable key
// is the raw CommandID: an opaque provider-verified value, not a name. A
// provider that cannot place those bytes in a path or subject hashes them and
// stores the original for the verification inboxEntryFor performs.
func inboxID(scope sessionScope, command sessionwire.CommandID) storage.OrderedID {
	return storage.OrderedID{
		Namespace:     inboxNamespace,
		OrderingScope: scope.SessionNamespace,
		StableKey:     storage.StableKey(command),
	}
}

// inboxDue is the single definition of a command record's due state, and it is
// a function of the RECORD rather than of the operation writing it.
//
// A non-terminal command is due at its apply deadline, so it participates in
// the deadline view a reconciler pages through. A terminal command is not due
// at all: its outcome is settled, it must not be reconciled again, and it
// remains directly readable by its stable key regardless.
//
// CARRY-FORWARD CONTRACT for whoever adds retention or compaction: TERMINAL
// COMMAND RETENTION MUST BE BOUNDED BELOW BY THE CLIENT RETRY WINDOW. A
// command's identity and acceptance order can never be reused, so tombstoning a
// terminal record is not reclamation of a name — it is a permanent,
// unrecoverable answer to any caller still retrying that command. Such a caller
// gets InboxErrorDeleted from inboxEntryFor and can neither learn the mapping
// that was accepted nor re-admit the command under the same id. Sweeping a
// terminal command away therefore has to wait until no client can still be
// retrying it. Nothing in this file is wrong today; this task simply has no
// retention to state the bound in.
//
// Admission only ever writes a pending record, so only the first branch is
// reachable through AdmitCommand. The second exists because every reader that
// holds a stored row to its own bytes needs the same derivation, and a terminal
// record filed not-due by a later task's CAS would otherwise be reported as
// misfiled by every retry that met it.
//
// CARRY-FORWARD CONTRACT for every later writer: FILE Due EXACTLY AS THIS
// FUNCTION DERIVES IT FROM THE RECORD. That is not a style rule. inboxEntryFor
// compares the stored due state against this derivation on every read, so a due
// state that is a function of the OPERATION rather than of the record makes
// every concurrent retry of that command fail with InboxErrorIdentity("due") —
// a code documented as one no retry of the caller's can fix, handed to a caller
// doing the one thing the retry contract exists for.
//
// The trap is specific and it is the natural design: reclaiming an abandoned
// claim wants the command due at its CLAIM EXPIRY rather than at its apply
// deadline, and filing it that way is a permanent blinding rather than a bug
// that shows up under load. A reclaim horizon must therefore be carried in the
// record's own members and FOLDED INTO this derivation — so that a reader can
// still rebuild the due state from the bytes alone — never filed beside it.
func inboxDue(record InboxRecord) storage.Due {
	if record.State.terminal() {
		return storage.Due{}
	}
	return storage.Due{State: storage.DueAt, UnixMillis: record.ApplyDeadline.UnixMilli()}
}

// inboxEntryFor decodes one stored command record and holds every
// provider-supplied component of its filing to what the record's own bytes say
// it should be, plus the identity the caller asked for.
//
// The enumeration, and why each entry is or is not here:
//
//   - Deleted — asserted first. A tombstoned identity cannot be reused, so a
//     retry that meets one is told so rather than handed bytes describing a
//     command that is no longer there.
//   - The record's own TenantID, SessionID, and CommandID — held to the request.
//     A provider that returned another session's record under this key would
//     otherwise hand a caller someone else's command.
//   - StableKey — held to the record's CommandID. This is the provider's
//     choice, not the caller's, and a provider that hashes the key stores the
//     original for exactly this comparison. It is not a restatement of the
//     check above: that one asks whether the BYTES are the command asked for,
//     this one asks whether the provider FILED them where it said it did.
//   - OrderingScope and RankingScope — the session's physical namespace,
//     derived from the tenant and session the bytes name. Neither can change
//     after Create, so a disagreement means the record was filed wrongly to
//     begin with, and a wrong ordering scope means the acceptance order came
//     from another session's sequence.
//   - Due — the record's derived due state, compared as a WHOLE VALUE so the
//     due STATE is covered as well as the instant. A pending command filed
//     not-due participates in no deadline page and would never be reconciled,
//     and a millisecond-only comparison would accept exactly that.
//   - Order — checked for being nonzero, which is all a single record can be
//     held to: the acceptance order is allocated by the provider, so the bytes
//     carry no counterpart to compare it against, and monotonicity is a
//     property of a scope rather than of a row. Zero is not an allocated order,
//     and returned as an acceptance order it would compare equal for every
//     command in the session and silently destroy the order consumers sort by.
//   - Namespace is excluded, and NOT merely because it echoes the query this
//     reader issued — so does OrderingScope, which is checked. The difference
//     is where each one is re-derived from: OrderingScope is rebuilt from the
//     tenant and session the RECORD's own bytes name, after those have been
//     held to the request, so the check is a comparison against the record.
//     The namespace is a package constant with no counterpart in any record,
//     so a comparison against it could only restate that this file's own
//     constant equals itself. What actually keeps namespaces apart is that
//     each record kind owns one, which TestOrderedNamespacesAreDistinct pins.
//   - Rank is written unranked and nothing ranks or reads commands by rank, so
//     a check would guard a view with no consumer.
//   - Revision is provider state with no meaning in the record; it is returned
//     to the caller for a later compare-and-swap rather than verified.
func inboxEntryFor(
	stored storage.OrderedRecord,
	scope sessionScope,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	command sessionwire.CommandID,
) (InboxEntry, error) {
	if stored.Deleted {
		return InboxEntry{}, inboxErr(InboxErrorDeleted, "record", nil)
	}
	record, err := decodeInboxRecord(stored.Value)
	if err != nil {
		return InboxEntry{}, err
	}
	if record.TenantID != tenant || record.SessionID != session || record.CommandID != command {
		return InboxEntry{}, inboxErr(InboxErrorIdentity, "record", nil)
	}
	if storage.StableKey(record.CommandID) != stored.ID.StableKey {
		return InboxEntry{}, inboxErr(InboxErrorIdentity, "command_id", nil)
	}
	if stored.ID.OrderingScope != scope.SessionNamespace {
		return InboxEntry{}, inboxErr(InboxErrorIdentity, "ordering_scope", nil)
	}
	if stored.RankingScope != scope.SessionNamespace {
		return InboxEntry{}, inboxErr(InboxErrorIdentity, "ranking_scope", nil)
	}
	if stored.Due != inboxDue(record) {
		return InboxEntry{}, inboxErr(InboxErrorIdentity, "due", nil)
	}
	if stored.Order == 0 {
		return InboxEntry{}, inboxErr(InboxErrorIdentity, "order", nil)
	}
	return InboxEntry{Record: record, Revision: stored.Revision, AcceptedOrder: stored.Order}, nil
}

// classifyInboxOrderedError maps an OrderedIndex outcome into the inbox
// vocabulary while preserving the cause for errors.Is and errors.As.
//
// It has two arms because admission makes one call. Create's contract accounts
// for the rest: an identity that already exists — including as a tombstone —
// is returned as a RECORD rather than as an error, which is why the deleted
// classification lives in inboxEntryFor and not here; a not-found, a revision
// conflict, a cursor failure, and a limit failure cannot arise from Create at
// all; and an oversized value is refused by encodeInboxRecord before the
// provider can see it, which the unsigned constant above pins. A later task
// that adds an Update path extends this with the arms that path can produce.
func classifyInboxOrderedError(err error, field string) error {
	var ambiguous *storage.OrderedAmbiguousError
	if errors.As(err, &ambiguous) {
		return inboxErr(InboxErrorUnknown, field, err)
	}
	return inboxErr(InboxErrorBackend, field, err)
}

// inboxWire is the stored JSON shape. Its optional members are pointers so a
// pending command's bytes carry no empty claim, result, or rejection object and
// a record has one spelling per state.
type inboxWire struct {
	RecordVersion    uint8                        `json:"record_version"`
	TenantID         sessionwire.TenantID         `json:"tenant_id"`
	SessionID        sessionwire.SessionID        `json:"session_id"`
	CommandID        sessionwire.CommandID        `json:"command_id"`
	RuntimeCommandID RuntimeCommandID             `json:"runtime_command_id"`
	Kind             CommandKind                  `json:"kind"`
	Payload          []byte                       `json:"payload,omitempty"`
	PayloadRef       *sessionwire.ObjectReference `json:"payload_ref,omitempty"`
	AcceptedAt       time.Time                    `json:"accepted_at"`
	ApplyDeadline    time.Time                    `json:"apply_deadline"`
	State            InboxState                   `json:"state"`
	Claim            *commandClaimWire            `json:"claim,omitempty"`
	Result           *commandResultWire           `json:"result,omitempty"`
	Rejection        *sessionwire.ErrorDetail     `json:"rejection,omitempty"`
}

type commandClaimWire struct {
	LeaseEpoch uint64    `json:"lease_epoch"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type commandResultWire struct {
	CompletedAt time.Time           `json:"completed_at"`
	EventID     sessionwire.EventID `json:"event_id"`
	JournalSeq  uint64              `json:"journal_seq"`
}

// encodeInboxRecord validates and encodes one command record. It refuses a
// record above the inbox bound here rather than letting the provider refuse it,
// so a record this package accepted can always be rewritten.
//
// It returns the CANONICAL record beside the bytes, and callers that go on to
// compare a candidate against a stored record must use it. A stored record is
// always canonical — it comes back through the decoder, which ends in the same
// canonicalization — so comparing a stored record against a caller's raw
// request would be comparing two different normal forms. That is safe today
// only because the one content normalization is nil-versus-empty payload, which
// bytes.Equal absorbs; returning the canonical form makes the coupling
// impossible to break rather than merely currently unbroken.
func encodeInboxRecord(record InboxRecord) ([]byte, InboxRecord, error) {
	record, err := canonicalInboxRecord(record)
	if err != nil {
		return nil, InboxRecord{}, err
	}
	wire := inboxWire{
		RecordVersion:    InboxRecordVersion,
		TenantID:         record.TenantID,
		SessionID:        record.SessionID,
		CommandID:        record.CommandID,
		RuntimeCommandID: record.RuntimeCommandID,
		Kind:             record.Kind,
		Payload:          record.Payload,
		AcceptedAt:       record.AcceptedAt,
		ApplyDeadline:    record.ApplyDeadline,
		State:            record.State,
		Rejection:        record.Rejection,
	}
	if record.PayloadRef != (sessionwire.ObjectReference{}) {
		reference := record.PayloadRef
		wire.PayloadRef = &reference
	}
	if !record.Claim.isZero() {
		wire.Claim = &commandClaimWire{LeaseEpoch: record.Claim.LeaseEpoch, ExpiresAt: record.Claim.ExpiresAt}
	}
	if !record.Result.isZero() {
		wire.Result = &commandResultWire{
			CompletedAt: record.Result.CompletedAt,
			EventID:     record.Result.EventID,
			JournalSeq:  record.Result.JournalSeq,
		}
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return nil, InboxRecord{}, inboxErr(InboxErrorInvalid, "record", err)
	}
	if len(encoded) > MaxInboxRecordBytes {
		return nil, InboxRecord{}, inboxErr(InboxErrorTooLarge, "record", nil)
	}
	return encoded, record, nil
}

// decodeInboxRecord strictly decodes one stored command record. It checks the
// inbox bound before decoding, rejects an unknown record version, and
// re-validates the decoded record, so a record corrupted in place cannot be
// handed to a caller.
//
// Strictness stops at this record's own members, for the reason
// decodeCatalogRecord states: the nested sessionwire projections are additive
// by design, and core's own redaction boundaries drop what they do not declare.
func decodeInboxRecord(value []byte) (InboxRecord, error) {
	wire, err := decodeVersionedRecord[inboxWire](
		value, MaxInboxRecordBytes, InboxRecordVersion,
		versionedRecordFields{Record: "record", Version: "record_version"}, inboxRecordFailure)
	if err != nil {
		return InboxRecord{}, err
	}
	record := InboxRecord{
		TenantID:         wire.TenantID,
		SessionID:        wire.SessionID,
		CommandID:        wire.CommandID,
		RuntimeCommandID: wire.RuntimeCommandID,
		Kind:             wire.Kind,
		Payload:          wire.Payload,
		AcceptedAt:       wire.AcceptedAt,
		ApplyDeadline:    wire.ApplyDeadline,
		State:            wire.State,
		Rejection:        wire.Rejection,
	}
	if wire.PayloadRef != nil {
		record.PayloadRef = *wire.PayloadRef
	}
	if wire.Claim != nil {
		record.Claim = CommandClaim{LeaseEpoch: wire.Claim.LeaseEpoch, ExpiresAt: wire.Claim.ExpiresAt}
	}
	if wire.Result != nil {
		record.Result = CommandResult{
			CompletedAt: wire.Result.CompletedAt,
			EventID:     wire.Result.EventID,
			JournalSeq:  wire.Result.JournalSeq,
		}
	}
	return canonicalInboxRecord(record)
}

// canonicalInboxRecord validates a record and returns its one canonical
// spelling: UTC timestamps and one spelling for an absent inline payload.
// Encoding and decoding both end here, so a record read back is byte-identical
// to the record written and two encoders cannot disagree.
//
// It validates each member's own well-formedness and deliberately does NOT
// enforce coherence BETWEEN State and the claim, result, and rejection members.
// Which state may carry which of them is the transition machine's rule, and it
// is stated where the transitions are made rather than half-stated here, where
// it could only be a guess about what a later writer is allowed to store.
func canonicalInboxRecord(record InboxRecord) (InboxRecord, error) {
	if err := record.TenantID.Validate(); err != nil {
		return InboxRecord{}, inboxErr(InboxErrorInvalid, "tenant_id", err)
	}
	if err := record.SessionID.Validate(); err != nil {
		return InboxRecord{}, inboxErr(InboxErrorInvalid, "session_id", err)
	}
	if err := record.CommandID.Validate(); err != nil {
		return InboxRecord{}, inboxErr(InboxErrorInvalid, "command_id", err)
	}
	if err := validateOpaque(string(record.RuntimeCommandID), "runtime_command_id", inboxInvalid); err != nil {
		return InboxRecord{}, err
	}
	if err := validateOpaque(string(record.Kind), "kind", inboxInvalid); err != nil {
		return InboxRecord{}, err
	}
	if len(record.Payload) > MaxInboxPayloadBytes {
		return InboxRecord{}, inboxErr(InboxErrorInvalid, "payload", nil)
	}
	// A command has one body or none. Two would leave a reader to choose, and
	// whichever it chose the other would be durable, unread, and unnoticed.
	if len(record.Payload) > 0 && record.PayloadRef != (sessionwire.ObjectReference{}) {
		return InboxRecord{}, inboxErr(InboxErrorInvalid, "payload", nil)
	}
	if record.PayloadRef != (sessionwire.ObjectReference{}) {
		if err := record.PayloadRef.Validate(); err != nil {
			return InboxRecord{}, inboxErr(InboxErrorInvalid, "payload_ref", err)
		}
	}
	if !rankableTime(record.AcceptedAt) {
		return InboxRecord{}, inboxErr(InboxErrorInvalid, "accepted_at", nil)
	}
	// rankableTime already refuses the zero Time; the deadline is validated as
	// an instant and NOT against the clock. See the file comment.
	if !rankableTime(record.ApplyDeadline) {
		return InboxRecord{}, inboxErr(InboxErrorInvalid, "apply_deadline", nil)
	}
	switch record.State {
	case InboxStatePending, InboxStateClaimed, InboxStateApplying, InboxStateApplied, InboxStateRejected:
	default:
		return InboxRecord{}, inboxErr(InboxErrorInvalid, "state", nil)
	}
	if !record.Claim.isZero() {
		if record.Claim.LeaseEpoch == 0 || !rankableTime(record.Claim.ExpiresAt) {
			return InboxRecord{}, inboxErr(InboxErrorInvalid, "claim", nil)
		}
	}
	if !record.Result.isZero() {
		if err := record.Result.EventID.Validate(); err != nil {
			return InboxRecord{}, inboxErr(InboxErrorInvalid, "result", err)
		}
		if record.Result.JournalSeq == 0 || !rankableTime(record.Result.CompletedAt) {
			return InboxRecord{}, inboxErr(InboxErrorInvalid, "result", nil)
		}
	}
	if record.Rejection != nil {
		if err := record.Rejection.Validate(); err != nil {
			return InboxRecord{}, inboxErr(InboxErrorInvalid, "rejection", err)
		}
		// The rejection is a nested sessionwire projection carrying caller
		// text, so it goes through the reflective walk rather than an
		// enumeration of its members: core may add one, and an enumeration
		// here would silently stop covering it the day it does.
		if err := validateProjectionText(reflect.ValueOf(*record.Rejection), "rejection", inboxInvalid); err != nil {
			return InboxRecord{}, err
		}
	}

	// One spelling for "no inline body". This is a NORMALIZATION, not a guard:
	// the encoded form omits an empty payload either way, so no test can
	// observe its absence and none pretends to. It exists so an in-memory
	// record has a single canonical form, as canonicalGates' empty slice does.
	if len(record.Payload) == 0 {
		record.Payload = nil
	}
	record.AcceptedAt = record.AcceptedAt.UTC()
	record.ApplyDeadline = record.ApplyDeadline.UTC()
	record.Claim.ExpiresAt = record.Claim.ExpiresAt.UTC()
	record.Result.CompletedAt = record.Result.CompletedAt.UTC()
	return record, nil
}
