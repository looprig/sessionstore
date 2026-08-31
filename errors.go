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
// CatalogErrorConflict, twenty lines up in this same file, means a lost
// revision compare-and-swap: recoverable, provider-caused, carrying the actual
// revision, and explicitly inviting a re-read and a retry. That is the
// OPPOSITE recovery advice, and one spelling for two opposite meanings in one
// package is a trap for anyone who learns the vocabulary from either half of
// it. InboxErrorConflict is therefore left undefined and RESERVED for the
// revision-CAS meaning its neighbour already has, which is what the command
// transition machine will need when it compare-and-swaps this record.
//
// Unknown means the mutation's outcome could not be resolved at all, so the
// caller learns nothing about what is stored and must retry the same identity
// to find out.
//
// Identity means a stored record disagreed with the identity it was filed
// under or asked for. It is not a caller error and not a conflict: it means the
// provider's answer cannot be trusted, and no retry of the caller's fixes it.
type InboxErrorCode string

const (
	InboxErrorInvalid         InboxErrorCode = "invalid"
	InboxErrorCommandMismatch InboxErrorCode = "command_mismatch"
	InboxErrorDeleted         InboxErrorCode = "deleted"
	InboxErrorIdentity        InboxErrorCode = "identity"
	InboxErrorUnknown         InboxErrorCode = "unknown"
	InboxErrorBackend         InboxErrorCode = "backend"
	InboxErrorMalformed       InboxErrorCode = "malformed"
	InboxErrorVersion         InboxErrorCode = "version"
	InboxErrorTooLarge        InboxErrorCode = "too_large"
)

// InboxError is a typed, redacted command failure. Field names the offending
// input or stage and never carries a provider name, a key, or any part of the
// command's private payload.
type InboxError struct {
	Code  InboxErrorCode
	Field string
	Cause error
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
