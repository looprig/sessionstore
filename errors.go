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
