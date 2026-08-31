package sessionstore

import "fmt"

// KeyspaceErrorCode is a stable machine-readable keyspace failure class.
type KeyspaceErrorCode string

const (
	KeyspaceBackend          KeyspaceErrorCode = "backend"
	KeyspaceMarkerMalformed  KeyspaceErrorCode = "marker_malformed"
	KeyspaceLayoutMismatch   KeyspaceErrorCode = "layout_mismatch"
	KeyspaceMarkerAmbiguous  KeyspaceErrorCode = "marker_ambiguous"
	KeyspaceBindingNotFound  KeyspaceErrorCode = "binding_not_found"
	KeyspaceBindingAmbiguous KeyspaceErrorCode = "binding_ambiguous"
	KeyspaceScopeInvalid     KeyspaceErrorCode = "scope_invalid"
	KeyspaceHashCollision    KeyspaceErrorCode = "hash_collision"
	KeyspaceLegacyTenant     KeyspaceErrorCode = "legacy_tenant"
	KeyspaceLegacySession    KeyspaceErrorCode = "legacy_session"
)

// KeyspaceError reports a fail-closed layout or physical-key failure. Cause is
// available to errors.Is/As, while Error deliberately omits provider and raw ID
// details.
type KeyspaceError struct {
	Code  KeyspaceErrorCode
	Cause error
}

func (e *KeyspaceError) Error() string { return "sessionstore: keyspace " + string(e.Code) }
func (e *KeyspaceError) Unwrap() error { return e.Cause }

// InvalidIdentityError identifies which opaque identity failed validation
// without retaining or rendering its value.
type InvalidIdentityError struct {
	Field string
	Cause error
}

func (e *InvalidIdentityError) Error() string { return "sessionstore: invalid " + e.Field }
func (e *InvalidIdentityError) Unwrap() error { return e.Cause }

// InvalidBackendError reports a storage component or required capability that
// was not wired at Open. Component is one of Composite, Ledger, Leaser, KV,
// OrderedIndex, Blobs, or BlobReaderLifecycle.
type InvalidBackendError struct {
	Component string
}

func (e *InvalidBackendError) Error() string {
	return "sessionstore: invalid backend: missing " + e.Component
}

// InvalidOptionError reports an invalid Open option. When Cause is non-nil it
// is preserved for errors.Is and errors.As.
type InvalidOptionError struct {
	Field string
	Cause error
}

func (e *InvalidOptionError) Error() string {
	if e.Cause == nil {
		return "sessionstore: invalid option: " + e.Field
	}
	return fmt.Sprintf("sessionstore: invalid option %s: %v", e.Field, e.Cause)
}

func (e *InvalidOptionError) Unwrap() error { return e.Cause }

// InvalidLimitError reports a limit outside its inclusive valid range.
type InvalidLimitError struct {
	Field string
	Value int64
	Min   int64
	Max   int64
}

func (e *InvalidLimitError) Error() string {
	return fmt.Sprintf("%s %d is outside [%d,%d]", e.Field, e.Value, e.Min, e.Max)
}

// StoreClosedError reports an attempt to admit work after shutdown started.
type StoreClosedError struct{}

func (*StoreClosedError) Error() string { return "sessionstore: store is closing" }

// InvalidBackgroundWorkError reports a nil internal background work function.
type InvalidBackgroundWorkError struct{}

func (*InvalidBackgroundWorkError) Error() string {
	return "sessionstore: background work function is nil"
}

// ObjectErrorCode classifies redacted object operation failures.
//
// Digest and Integrity are deliberately distinct: Digest means the caller's own
// metadata is self-inconsistent (its Digest field disagrees with the digest
// inside its ObjectID), while Integrity means bytes or a stored key did not
// match what the object identity promised.
type ObjectErrorCode string

const (
	ObjectErrorInvalid   ObjectErrorCode = "invalid"
	ObjectErrorSize      ObjectErrorCode = "size"
	ObjectErrorDigest    ObjectErrorCode = "digest"
	ObjectErrorSource    ObjectErrorCode = "source"
	ObjectErrorBackend   ObjectErrorCode = "backend"
	ObjectErrorConflict  ObjectErrorCode = "conflict"
	ObjectErrorIntegrity ObjectErrorCode = "integrity"
	ObjectErrorCanceled  ObjectErrorCode = "canceled"
)

// ObjectError is a typed, redacted object operation failure. Field names the
// offending input or stage and never carries a provider path, key, or payload.
//
// Terminating an object stream can produce more than one failure at once — the
// cause that ended the stream, a read error observed by a racing reader, and a
// provider Close error — and those are reported as an errors.Join tree, so a
// returned error may contain several *ObjectError values. The FIRST one found
// by errors.As is the primary classification: it is the failure that caused
// termination, and later ones are subsidiary consequences of it. A caller that
// genuinely needs every code can walk the tree itself through the
// `Unwrap() []error` that errors.Join returns; the package deliberately does
// not export a set extractor, because classifying on the primary cause is the
// supported contract and an exported extractor would freeze the join shape.
type ObjectError struct {
	Code  ObjectErrorCode
	Field string
	Cause error
}

func (e *ObjectError) Error() string {
	message := "sessionstore: object " + string(e.Code)
	if e.Field != "" {
		message += " (" + e.Field + ")"
	}
	return message
}

func (e *ObjectError) Unwrap() error { return e.Cause }

func objectErr(code ObjectErrorCode, field string, cause error) error {
	return &ObjectError{Code: code, Field: field, Cause: cause}
}

// EnvelopeErrorCode is a stable machine-readable envelope failure reason.
type EnvelopeErrorCode string

const (
	EnvelopeErrorMalformed EnvelopeErrorCode = "malformed"
	EnvelopeErrorVersion   EnvelopeErrorCode = "version"
	EnvelopeErrorKind      EnvelopeErrorCode = "kind"
	EnvelopeErrorField     EnvelopeErrorCode = "field"
	EnvelopeErrorOrder     EnvelopeErrorCode = "order"
	EnvelopeErrorMissing   EnvelopeErrorCode = "missing"
	EnvelopeErrorInvalid   EnvelopeErrorCode = "invalid"
	EnvelopeErrorLength    EnvelopeErrorCode = "length"
	EnvelopeErrorTooLarge  EnvelopeErrorCode = "too-large"
	EnvelopeErrorDigest    EnvelopeErrorCode = "digest"
	EnvelopeErrorTrailing  EnvelopeErrorCode = "trailing"
)

// EnvelopeError reports a bounded codec failure and preserves its cause without
// placing attacker-controlled cause text in Error().
type EnvelopeError struct {
	Code  EnvelopeErrorCode
	Field string
	Cause error
}

func (e *EnvelopeError) Error() string {
	message := "sessionstore: envelope " + string(e.Code)
	if e.Field != "" {
		field := e.Field
		if len(field) > 48 {
			field = field[:48]
		}
		message += " (" + field + ")"
	}
	return message
}

func (e *EnvelopeError) Unwrap() error { return e.Cause }

func envelopeError(code EnvelopeErrorCode, field string, cause error) error {
	return &EnvelopeError{Code: code, Field: field, Cause: cause}
}

// CatalogErrorCode classifies a session catalog record failure.
//
// Cursor is separate from Invalid because the two name different owners. An
// invalid limit is a caller mistake in the request this package validates;
// Cursor means a continuation token was not one this store issued for this
// query, whether the envelope or the provider token inside it failed, and a
// caller's only recovery is to restart the walk from the first page.
//
// Epoch and Conflict are deliberately distinct, and the distinction is the
// whole point of the catalog's two ownership mechanisms. Epoch means a
// Host-owned write named a lease epoch below the record's committed high-water
// mark: that writer has provably been superseded and must not retry with the
// same epoch. Conflict means a compare-and-swap lost a race on the record's
// revision without any statement about ownership; the caller may re-read and
// retry. Unknown means the mutation's outcome could not be resolved at all.
type CatalogErrorCode string

const (
	CatalogErrorInvalid   CatalogErrorCode = "invalid"
	CatalogErrorCursor    CatalogErrorCode = "cursor"
	CatalogErrorNotFound  CatalogErrorCode = "not_found"
	CatalogErrorDeleted   CatalogErrorCode = "deleted"
	CatalogErrorIdentity  CatalogErrorCode = "identity"
	CatalogErrorEpoch     CatalogErrorCode = "epoch"
	CatalogErrorSequence  CatalogErrorCode = "sequence"
	CatalogErrorConflict  CatalogErrorCode = "conflict"
	CatalogErrorUnknown   CatalogErrorCode = "unknown"
	CatalogErrorBackend   CatalogErrorCode = "backend"
	CatalogErrorMalformed CatalogErrorCode = "malformed"
	CatalogErrorVersion   CatalogErrorCode = "version"
	CatalogErrorTooLarge  CatalogErrorCode = "too_large"
)

// CatalogError is a typed, redacted catalog failure. Field names the offending
// input or stage and never carries a provider name, key, or record payload.
// Epoch is populated only for CatalogErrorEpoch, where the committed high-water
// epoch is itself the answer, and Revision only for CatalogErrorConflict, where
// a backend that can safely disclose the current revision did so.
type CatalogError struct {
	Code     CatalogErrorCode
	Field    string
	Epoch    uint64
	Revision uint64
	Cause    error
}

func (e *CatalogError) Error() string {
	message := "sessionstore: catalog " + string(e.Code)
	if e.Field != "" {
		message += " (" + e.Field + ")"
	}
	return message
}

func (e *CatalogError) Unwrap() error { return e.Cause }

func catalogErr(code CatalogErrorCode, field string, cause error) error {
	return &CatalogError{Code: code, Field: field, Cause: cause}
}

// JournalErrorCode classifies a journal ownership, append, or read failure.
//
// Fenced and Unknown are deliberately distinct outcomes of one CAS append.
// Fenced is definite — a successor's record occupies the contested sequence, so
// this writer has provably lost the stream. Unknown means the outcome could not
// be resolved at all, so the writer's own tip is no longer trustworthy. Both
// end the writer permanently; only Fenced asserts that someone else won.
type JournalErrorCode string

const (
	JournalErrorInvalid   JournalErrorCode = "invalid"
	JournalErrorLeaseHeld JournalErrorCode = "lease_held"
	JournalErrorLeaseLost JournalErrorCode = "lease_lost"
	JournalErrorFenced    JournalErrorCode = "fenced"
	JournalErrorUnknown   JournalErrorCode = "unknown"
	JournalErrorClosed    JournalErrorCode = "closed"
	JournalErrorBackend   JournalErrorCode = "backend"
	JournalErrorIntegrity JournalErrorCode = "integrity"
	JournalErrorTooLarge  JournalErrorCode = "too_large"
	JournalErrorCursor    JournalErrorCode = "cursor"
)

// JournalError is a typed, redacted journal failure. Field names the offending
// input or stage and never carries a provider name, key, or record payload.
// Epoch is populated only where a fencing epoch is itself the answer — the live
// holder's epoch for lease_held, this writer's epoch for lease_lost.
type JournalError struct {
	Code  JournalErrorCode
	Field string
	Epoch uint64
	Cause error
}

func (e *JournalError) Error() string {
	message := "sessionstore: journal " + string(e.Code)
	if e.Field != "" {
		message += " (" + e.Field + ")"
	}
	return message
}

func (e *JournalError) Unwrap() error { return e.Cause }

func journalErr(code JournalErrorCode, field string, cause error) error {
	return &JournalError{Code: code, Field: field, Cause: cause}
}

// InboxErrorCode classifies a durable command record failure.
//
// It is the inbox's own vocabulary rather than the catalog's, and the reason is
// ownership. Gates reuse CatalogError because a gate IS catalog state: the
// projection lives in the catalog record and the deadline intent is an index
// into it, so a gate failure is a statement about that record. A command is a
// separate aggregate — its own namespace, its own record, its own identity, its
// own lifecycle — and admission never reads or writes a session's catalog
// record. A caller branching on an inbox failure should not have to match the
// catalog's type to learn that its command was not stored, and the command
// lifecycle's later states need failures the catalog has no business naming.
//
// CommandMismatch is definite and caller-caused: one command id was reused for
// a DIFFERENT command, and the stored command is untouched. No retry helps —
// the caller must mint a new id or send the command it originally sent.
//
// It is deliberately NOT called "conflict", and the omission is the point.
// This file already spells "conflict" two ways. CatalogErrorConflict means a
// lost revision compare-and-swap: recoverable, provider-caused, carrying the
// actual revision, and explicitly inviting a re-read and a retry.
// ObjectErrorConflict means a key already holding DIFFERENT CONTENT, which is
// the near-twin of what a reused command id is — so a reader who met that one
// first would reasonably expect "conflict" here and get the catalog's
// recovery advice instead.
//
// The tie goes to the catalog's meaning because of what this record IS: an
// OrderedIndex row has a Revision, so the command transition machine will
// compare-and-swap it and will need a name for losing that race. The object
// aggregate never will. InboxErrorConflict therefore carries the catalog's
// meaning exactly — a lost revision compare-and-swap, recoverable, reporting
// the actual revision, inviting a re-read and a retry — and the caller-caused
// content case takes a name that cannot be mistaken for either neighbour.
//
// Unknown means the mutation's outcome could not be resolved at all, so the
// caller learns nothing about what is stored and must retry the same identity
// to find out.
//
// Identity means a stored record disagreed with the identity it was filed
// under or asked for. It is not a caller error and not a conflict: it means the
// provider's answer cannot be trusted, and no retry of the caller's fixes it.
//
// The transition machine adds the rest, and each one names a DIFFERENT recovery
// so that a caller can branch without reading prose:
//
//   - NotFound — no command has ever been admitted under that identity. The
//     caller is asking about something it never accepted.
//   - Conflict — the record moved under the caller. Re-read and decide again.
//     Revision carries what the record is at now when the store could see it.
//   - Epoch — the caller named a lease epoch BELOW the epoch the record's claim
//     was taken under. That lease has provably been superseded and must not
//     retry under the same epoch; Epoch carries the committed high-water mark,
//     as CatalogErrorEpoch does.
//   - ClaimHeld — a LIVE claim is held on this command and is not provably the
//     caller's. It cannot say "someone else": a claim records the lease epoch it
//     was taken under and no claimant identity, so two writers under one epoch
//     are indistinguishable to it. That matters for the likeliest recipient,
//     which is not a rival but a claimer meeting its OWN live claim: a claim
//     cannot be renewed, so a caller that wants more time must enter applying
//     before its claim lapses, and waiting for the claim to expire — the advice
//     that fits a rival — is the one thing that caller must not do.
//   - ClaimLost — the caller does not hold the live claim the transition
//     requires, and Field says which of the two situations it is. "lease_epoch"
//     means the claim is held under another epoch, so the caller never held this
//     command; "claim" means the caller's own claim lapsed. Both are answered by
//     claiming again, which the apply deadline may no longer permit, but they
//     read very differently to an operator: the first is a writer working on a
//     command that is not its own, the second is a writer that was too slow.
//
// ClaimHeld and ClaimLost are the pair the journal already spells for lease
// ownership, and they mean the corresponding two things here.
//   - Deadline — a NEW claim was attempted at or after the command's apply
//     deadline. No retry helps: the command is now the deadline reconciler's,
//     and the caller learns its answer by reading the terminal record.
//   - State — the record is in a state this transition has no edge out of, and
//     the caller had a current revision when it asked. It is a caller mistake
//     about the machine rather than a race.
//   - Evidence — the journal does not support the settlement asked for, and
//     Field says which question it failed. "application" means the correlation
//     did not establish that no effect committed — either one did, or the
//     evidence is not readable — so the command must be finished or left alone
//     rather than rejected. "result" means a recovering successor named a
//     terminal result that is not the effect its prefix is correlated with.
//     "applying_lease" means the lease holding the record's claim is not yet
//     provably fenced out of the journal, so it could still commit the effect
//     this settlement would orphan. None of the three is a race and none is
//     answered by retrying the same call unchanged: the first two are permanent
//     for the journal as it stands, and the third becomes settleable only once
//     a later lease has opened the stream.
//   - Terminal — the command's outcome is already settled. It is separate from
//     State because it is the one state failure that is PERMANENT and that
//     carries an answer: a caller meeting it should read the record and report
//     the outcome rather than re-deciding anything.
type InboxErrorCode string

const (
	InboxErrorInvalid         InboxErrorCode = "invalid"
	InboxErrorCommandMismatch InboxErrorCode = "command_mismatch"
	InboxErrorNotFound        InboxErrorCode = "not_found"
	InboxErrorDeleted         InboxErrorCode = "deleted"
	InboxErrorIdentity        InboxErrorCode = "identity"
	InboxErrorConflict        InboxErrorCode = "conflict"
	InboxErrorEpoch           InboxErrorCode = "epoch"
	InboxErrorClaimHeld       InboxErrorCode = "claim_held"
	InboxErrorClaimLost       InboxErrorCode = "claim_lost"
	InboxErrorDeadline        InboxErrorCode = "deadline"
	InboxErrorState           InboxErrorCode = "state"
	InboxErrorEvidence        InboxErrorCode = "evidence"
	InboxErrorTerminal        InboxErrorCode = "terminal"
	InboxErrorUnknown         InboxErrorCode = "unknown"
	InboxErrorBackend         InboxErrorCode = "backend"
	InboxErrorMalformed       InboxErrorCode = "malformed"
	InboxErrorVersion         InboxErrorCode = "version"
	InboxErrorTooLarge        InboxErrorCode = "too_large"
)

// InboxError is a typed, redacted command failure. Field names the offending
// input or stage and never carries a provider name, a key, or any part of the
// command's private payload.
//
// Epoch is populated only for InboxErrorEpoch and Revision only for
// InboxErrorConflict, each carrying the value that is itself the answer, as
// CatalogError does for the same two codes.
type InboxError struct {
	Code     InboxErrorCode
	Field    string
	Epoch    uint64
	Revision uint64
	Cause    error
}

func (e *InboxError) Error() string {
	message := "sessionstore: inbox " + string(e.Code)
	if e.Field != "" {
		message += " (" + e.Field + ")"
	}
	return message
}

func (e *InboxError) Unwrap() error { return e.Cause }

func inboxErr(code InboxErrorCode, field string, cause error) error {
	return &InboxError{Code: code, Field: field, Cause: cause}
}

// inboxInvalid is the inbox's member-validation constructor, handed to the
// shared text validators so they report in this record's vocabulary.
func inboxInvalid(field string, cause error) error {
	return inboxErr(InboxErrorInvalid, field, cause)
}

// inboxRecordFailure maps a shared versioned-record decode failure into the
// inbox vocabulary.
func inboxRecordFailure(failure versionedRecordFailure, field string, cause error) error {
	switch failure {
	case versionedRecordTooLarge:
		return inboxErr(InboxErrorTooLarge, field, cause)
	case versionedRecordVersion:
		return inboxErr(InboxErrorVersion, field, cause)
	default:
		return inboxErr(InboxErrorMalformed, field, cause)
	}
}

// RegistryErrorCode classifies a Host registration failure.
//
// It is the registry's own vocabulary rather than the catalog's, and the reason
// is the one InboxErrorCode gives. Gates reuse CatalogError because a gate IS
// catalog state. A registration is a separate aggregate: its own namespace, its
// own record, its own identity, its own fencing high-water mark, and — unlike
// every other record in this package — its own LIFETIME, because it expires
// while nothing else here does. Its writes never read or write a session's
// catalog record, and a caller branching on "there is no live route" should not
// have to match the type that reports "there is no such session".
//
// The three ways a registration can fail to be a route are deliberately
// separate codes, and none of them is an error in the caller:
//
//   - NotFound — no registration has ever been written for this session. No
//     Host has ever held it, or none has ever reported holding it.
//   - Expired — a registration exists and names a Host, but its expiry has
//     passed. The Host may be alive and merely slow to heartbeat, or it may be
//     gone; this record cannot tell the difference and neither may its reader.
//   - Released — a registration exists and is the tombstone a graceful
//     shutdown left behind. The Host that held the session let it go on
//     purpose.
//
// A ROUTER MUST TREAT ALL THREE ALIKE: none of them is a route, and the
// difference between them is diagnostic. They are separate rather than
// collapsed into NotFound because of what an undifferentiated "absent" invites.
// A future writer that reads a registration, sees "not found", and creates a
// fresh record has just dropped the fencing high-water mark of whatever was
// really there — which is precisely the write the epoch fence exists to refuse.
// Nothing in this package reaches a write path through a reader that reports
// these codes, and the codes being distinct is what makes a future one that
// tries to look wrong rather than plausible.
//
// The rest name the same failures the catalog's and the inbox's codes do:
//
//   - Invalid — a caller mistake in the request or a stored record that no
//     longer satisfies its own rules.
//   - Deleted — the provider holds a TOMBSTONE for this identity. This package
//     never deletes a registration, so it means the fencing high-water mark has
//     been physically destroyed by something outside it; it is reported and
//     never worked around, because the alternative is admitting a write from a
//     lease that has already lost the session.
//   - Identity — a stored record disagreed with the identity it was filed
//     under or asked for. It is not a caller error and no retry fixes it.
//   - Epoch — the caller named a lease epoch BELOW the record's committed
//     high-water mark. That lease has provably been superseded and must not
//     retry under the same epoch; Epoch carries the high-water mark, as
//     CatalogErrorEpoch does.
//   - Conflict — a compare-and-swap lost a race on the record's revision, with
//     no statement about ownership. Re-read and retry. Revision carries what
//     the record is at now when the store could see it.
//   - Unknown — the mutation's outcome could not be resolved at all.
type RegistryErrorCode string

const (
	RegistryErrorInvalid   RegistryErrorCode = "invalid"
	RegistryErrorNotFound  RegistryErrorCode = "not_found"
	RegistryErrorExpired   RegistryErrorCode = "expired"
	RegistryErrorReleased  RegistryErrorCode = "released"
	RegistryErrorDeleted   RegistryErrorCode = "deleted"
	RegistryErrorIdentity  RegistryErrorCode = "identity"
	RegistryErrorEpoch     RegistryErrorCode = "epoch"
	RegistryErrorConflict  RegistryErrorCode = "conflict"
	RegistryErrorUnknown   RegistryErrorCode = "unknown"
	RegistryErrorBackend   RegistryErrorCode = "backend"
	RegistryErrorMalformed RegistryErrorCode = "malformed"
	RegistryErrorVersion   RegistryErrorCode = "version"
	RegistryErrorTooLarge  RegistryErrorCode = "too_large"
)

// RegistryError is a typed, redacted Host registration failure. Field names the
// offending input or stage and never carries a provider name, a key, or a
// record payload.
//
// Revision is populated only for RegistryErrorConflict, carrying the value that
// is itself the answer, as CatalogError and InboxError do for that code.
//
// Epoch is populated more widely than the catalog's and the inbox's, and the
// extra two codes are the point rather than an inconsistency. For
// RegistryErrorEpoch it is the high-water mark that refused the write, as it is
// there. For RegistryErrorExpired and RegistryErrorReleased it is the fence the
// retained record still carries — because those two codes are the whole public
// account of a session that has no route, they are what a retention sweep acts
// on, and this package offers no other way to observe a registration's epoch.
// Without it a caller's only route to the record's most consequential permanent
// state would be to attempt a write it expects to fail and read the refusal.
// RegistryErrorNotFound carries no epoch: there is no record and so no fence,
// and a zero there means exactly that.
type RegistryError struct {
	Code     RegistryErrorCode
	Field    string
	Epoch    uint64
	Revision uint64
	Cause    error
}

func (e *RegistryError) Error() string {
	message := "sessionstore: registry " + string(e.Code)
	if e.Field != "" {
		message += " (" + e.Field + ")"
	}
	return message
}

func (e *RegistryError) Unwrap() error { return e.Cause }

func registryErr(code RegistryErrorCode, field string, cause error) error {
	return &RegistryError{Code: code, Field: field, Cause: cause}
}

// registryInvalid is the registry's member-validation constructor, handed to
// the shared validators so they report in this record's vocabulary. Its one
// caller today is validateBoundedExpiry, which validates an INSTANT rather than
// text — the shape is the package's general one for a shared rule with a
// per-record name, not the text validators' alone.
func registryInvalid(field string, cause error) error {
	return registryErr(RegistryErrorInvalid, field, cause)
}

// registryRecordFailure maps a shared versioned-record decode failure into the
// registry vocabulary.
func registryRecordFailure(failure versionedRecordFailure, field string, cause error) error {
	switch failure {
	case versionedRecordTooLarge:
		return registryErr(RegistryErrorTooLarge, field, cause)
	case versionedRecordVersion:
		return registryErr(RegistryErrorVersion, field, cause)
	default:
		return registryErr(RegistryErrorMalformed, field, cause)
	}
}

// HostTargetErrorCode classifies a Host target directory failure.
//
// It is its own vocabulary rather than the registry's, and the reason is not
// merely that they are separate aggregates. It is that a caller MUST NOT be
// able to write one handler for both. A RegistryError is the public account of
// who is running a session; a HostTargetError is the public account of who
// might be able to take one. Sharing a type would let a caller branch on
// "expired" without knowing which of those two questions it had just asked, and
// the whole discipline of this record is that capacity is never authority.
//
// The codes are the ones the ordinary vocabulary supplies, with three that need
// their reasons stated:
//
//   - Withdrawn — the row exists and offers no capacity. It is what a drain
//     leaves and what the reconciler writes; it is not an error in a caller and
//     it is not a claim about any session.
//   - Generation — the caller named a Host generation BELOW the row's committed
//     high-water mark, so the write comes from a superseded incarnation of that
//     same Host. It is deliberately NOT called Epoch: RegistryErrorEpoch means
//     a lease has provably lost a SESSION, and a code sharing that name would
//     invite a reader to believe this record fences ownership. It does not. See
//     HostTarget.
//   - Cursor — a page token this store did not issue for this exact target. The
//     walk restarts from the first page; nothing is wrong with the store.
//
// Conflict means a lost revision compare-and-swap and nothing else — re-read
// and retry — which is the meaning it has for every other record kind here.
//
// TWO ERROR TYPES REACH A CALLER OF THIS RECORD, and a consumer must handle
// both. Validating a target reports *InvalidIdentityError for AgentID, because
// that is what every identity derivation in this package reports for a
// sessionwire identity, while the opaque runtime id and the placement enum
// report *HostTargetError — so one malformed request surfaces as either type
// depending on which member is wrong. That is the package's convention rather
// than this record's choice, and it is written down here because this record is
// the first one a Host or a Factory calls.
type HostTargetErrorCode string

const (
	HostTargetErrorInvalid    HostTargetErrorCode = "invalid"
	HostTargetErrorNotFound   HostTargetErrorCode = "not_found"
	HostTargetErrorWithdrawn  HostTargetErrorCode = "withdrawn"
	HostTargetErrorDeleted    HostTargetErrorCode = "deleted"
	HostTargetErrorIdentity   HostTargetErrorCode = "identity"
	HostTargetErrorGeneration HostTargetErrorCode = "generation"
	HostTargetErrorConflict   HostTargetErrorCode = "conflict"
	HostTargetErrorCursor     HostTargetErrorCode = "cursor"
	HostTargetErrorUnknown    HostTargetErrorCode = "unknown"
	HostTargetErrorBackend    HostTargetErrorCode = "backend"
	HostTargetErrorMalformed  HostTargetErrorCode = "malformed"
	HostTargetErrorVersion    HostTargetErrorCode = "version"
	HostTargetErrorTooLarge   HostTargetErrorCode = "too_large"
)

// HostTargetError is a typed, redacted Host target directory failure. Field
// names the offending input or stage and never carries a provider name, a key,
// or a record payload.
//
// Revision is populated only for HostTargetErrorConflict, carrying the value
// that is itself the answer, as the other record kinds do for that code.
//
// Generation is populated only for HostTargetErrorGeneration, carrying the
// high-water mark that refused the write so a superseded incarnation learns it
// has been superseded rather than retrying forever.
//
// There is deliberately no epoch member of any kind. A caller cannot obtain a
// lease epoch from this type because there is no lease epoch in this record to
// obtain.
type HostTargetError struct {
	Code       HostTargetErrorCode
	Field      string
	Generation uint64
	Revision   uint64
	Cause      error
}

func (e *HostTargetError) Error() string {
	message := "sessionstore: host target " + string(e.Code)
	if e.Field != "" {
		message += " (" + e.Field + ")"
	}
	return message
}

func (e *HostTargetError) Unwrap() error { return e.Cause }

func hostTargetErr(code HostTargetErrorCode, field string, cause error) error {
	return &HostTargetError{Code: code, Field: field, Cause: cause}
}

// hostTargetInvalid is the directory's member-validation constructor, handed to
// the shared validators so they report in this record's vocabulary.
func hostTargetInvalid(field string, cause error) error {
	return hostTargetErr(HostTargetErrorInvalid, field, cause)
}

// hostTargetIdentity is the directory's counterpart of the three identity
// constructors below, and it exists for the same reason they do.
func hostTargetIdentity(field string, cause error) error {
	return hostTargetErr(HostTargetErrorIdentity, field, cause)
}

// hostTargetRecordFailure maps a shared versioned-record decode failure into
// the directory vocabulary.
func hostTargetRecordFailure(failure versionedRecordFailure, field string, cause error) error {
	switch failure {
	case versionedRecordTooLarge:
		return hostTargetErr(HostTargetErrorTooLarge, field, cause)
	case versionedRecordVersion:
		return hostTargetErr(HostTargetErrorVersion, field, cause)
	default:
		return hostTargetErr(HostTargetErrorMalformed, field, cause)
	}
}

// The three identity constructors below are what let checkFiledScope be shared.
// They are separate from the Invalid constructors above them because the two
// say different things: an Invalid names a value that is wrong, while an
// Identity names a stored record that disagrees with the identity it was filed
// under or asked for — not a caller error, not a conflict, and not fixable by
// retrying. A filing check that reported Invalid would tell a caller to correct
// a request that was never at fault.
func catalogIdentity(field string, cause error) error {
	return catalogErr(CatalogErrorIdentity, field, cause)
}

func inboxIdentity(field string, cause error) error {
	return inboxErr(InboxErrorIdentity, field, cause)
}

func registryIdentity(field string, cause error) error {
	return registryErr(RegistryErrorIdentity, field, cause)
}

// catalogInvalid is the catalog's member-validation constructor. It is the
// counterpart of inboxInvalid: the shared validators state the RULE, and each
// record states what a violation of it is called.
func catalogInvalid(field string, cause error) error {
	return catalogErr(CatalogErrorInvalid, field, cause)
}

// catalogRecordFailure maps a shared versioned-record decode failure into the
// catalog vocabulary.
func catalogRecordFailure(failure versionedRecordFailure, field string, cause error) error {
	switch failure {
	case versionedRecordTooLarge:
		return catalogErr(CatalogErrorTooLarge, field, cause)
	case versionedRecordVersion:
		return catalogErr(CatalogErrorVersion, field, cause)
	default:
		return catalogErr(CatalogErrorMalformed, field, cause)
	}
}
