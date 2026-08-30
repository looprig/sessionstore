package sessionstore

import "fmt"

// InvalidBackendError reports a storage component that was not wired at Open.
// Component is one of Composite, Ledger, Leaser, KV, OrderedIndex, or Blobs.
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
