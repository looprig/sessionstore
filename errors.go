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
