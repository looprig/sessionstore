package sessionstore

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"time"

	"github.com/looprig/storage"
)

// DefaultShutdownTimeout bounds provider cleanup after Store-owned work drains.
// It matches the remote provider drain bound used by the released NATS backend.
const DefaultShutdownTimeout = 30 * time.Second

// Limits contains Store-wide ceilings. MaxPageSize bounds every provider page
// requested by SessionStore; individual operations may request a smaller page.
type Limits struct {
	MaxPageSize     int
	ShutdownTimeout time.Duration
}

// DefaultLimits returns bounded defaults for provider queries and cleanup.
func DefaultLimits() Limits {
	return Limits{
		MaxPageSize:     storage.MaxOrderedPageLimit,
		ShutdownTimeout: DefaultShutdownTimeout,
	}
}

// Clock supplies wall time to storage decisions and permits deterministic tests.
type Clock interface {
	Now() time.Time
}

// ProviderCloser is the lifecycle boundary for a provider whose ownership is
// explicitly transferred to Store.
type ProviderCloser interface {
	Close(context.Context) error
}

// Option configures Open.
type Option func(*config) error

type config struct {
	limits        Limits
	clock         Clock
	logger        *slog.Logger
	providerClose func(context.Context) error
}

func defaultConfig() config {
	return config{
		limits: DefaultLimits(),
		clock:  systemClock{},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// WithLimits sets Store-wide ceilings.
func WithLimits(limits Limits) Option {
	return func(cfg *config) error {
		if limits.MaxPageSize < 1 || limits.MaxPageSize > storage.MaxOrderedPageLimit {
			cause := &InvalidLimitError{
				Field: "MaxPageSize",
				Value: int64(limits.MaxPageSize),
				Min:   1,
				Max:   storage.MaxOrderedPageLimit,
			}
			return &InvalidOptionError{Field: "MaxPageSize", Cause: cause}
		}
		if limits.ShutdownTimeout <= 0 {
			cause := &InvalidLimitError{
				Field: "ShutdownTimeout",
				Value: int64(limits.ShutdownTimeout),
				Min:   int64(time.Nanosecond),
				Max:   int64(1<<63 - 1),
			}
			return &InvalidOptionError{Field: "ShutdownTimeout", Cause: cause}
		}
		cfg.limits = limits
		return nil
	}
}

// WithClock supplies the clock used by Store.
func WithClock(clock Clock) Option {
	return func(cfg *config) error {
		if isNilDynamic(reflect.ValueOf(clock)) {
			return &InvalidOptionError{Field: "Clock"}
		}
		cfg.clock = clock
		return nil
	}
}

// WithLogger supplies the structured logger used by Store.
func WithLogger(logger *slog.Logger) Option {
	return func(cfg *config) error {
		if logger == nil {
			return &InvalidOptionError{Field: "Logger"}
		}
		cfg.logger = logger
		return nil
	}
}

// WithProviderOwnership explicitly transfers provider lifecycle ownership to
// Store. Without this option Close never closes caller-supplied storage.
func WithProviderOwnership(closer ProviderCloser) Option {
	return func(cfg *config) error {
		if isNilDynamic(reflect.ValueOf(closer)) {
			return &InvalidOptionError{Field: "ProviderCloser"}
		}
		cfg.providerClose = closer.Close
		return nil
	}
}

// WithIOProviderOwnership explicitly transfers ownership of a provider whose
// released lifecycle contract is the standard io.Closer shape.
func WithIOProviderOwnership(closer io.Closer) Option {
	return func(cfg *config) error {
		if isNilDynamic(reflect.ValueOf(closer)) {
			return &InvalidOptionError{Field: "IOProviderCloser"}
		}
		cfg.providerClose = func(context.Context) error { return closer.Close() }
		return nil
	}
}

func isNilDynamic(value reflect.Value) bool {
	if !value.IsValid() {
		return true
	}
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
