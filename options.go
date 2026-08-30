package sessionstore

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// DefaultShutdownTimeout bounds provider cleanup after Store-owned work drains.
// It matches the remote provider drain bound used by the released NATS backend.
const DefaultShutdownTimeout = 30 * time.Second

// Limits contains Store-wide ceilings. MaxPageSize bounds every provider page
// requested by SessionStore; individual operations may request a smaller page.
type Limits struct {
	MaxPageSize int
}

// DefaultLimits returns bounded defaults for provider queries.
func DefaultLimits() Limits {
	return Limits{
		MaxPageSize: storage.MaxOrderedPageLimit,
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
	limits          Limits
	clock           Clock
	logger          *slog.Logger
	shutdownTimeout time.Duration
	providerClose   func(context.Context) error
	ioAdapter       *ioProviderAdapter
	layout          keyspaceLayout
	legacyTenant    sessionwire.TenantID
}

func defaultConfig() config {
	return config{
		limits:          DefaultLimits(),
		clock:           systemClock{},
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		shutdownTimeout: DefaultShutdownTimeout,
		layout:          layoutTenantV1,
	}
}

// WithLegacySingleTenant explicitly adopts the historical unscoped layout for
// one tenant. It never probes for legacy data; Open atomically persists the
// choice and exact tenant in the backend layout marker.
func WithLegacySingleTenant(defaultTenant sessionwire.TenantID) Option {
	return func(cfg *config) error {
		if err := defaultTenant.Validate(); err != nil {
			return &InvalidOptionError{Field: "LegacyTenant", Cause: &InvalidIdentityError{Field: "TenantID", Cause: err}}
		}
		if cfg.layout == layoutLegacySingleTenantV1 {
			return &InvalidOptionError{Field: "LegacySingleTenant"}
		}
		cfg.layout = layoutLegacySingleTenantV1
		cfg.legacyTenant = defaultTenant
		return nil
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
		cfg.limits = limits
		return nil
	}
}

// WithShutdownTimeout bounds explicitly owned provider cleanup after Store
// background work drains.
func WithShutdownTimeout(timeout time.Duration) Option {
	return func(cfg *config) error {
		if timeout <= 0 {
			cause := &InvalidLimitError{
				Field: "ShutdownTimeout",
				Value: int64(timeout),
				Min:   int64(time.Nanosecond),
				Max:   int64(1<<63 - 1),
			}
			return &InvalidOptionError{Field: "ShutdownTimeout", Cause: cause}
		}
		cfg.shutdownTimeout = timeout
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
// Store after Open succeeds. A failed Open leaves the provider caller-owned and
// never closes it. Without this option Close never closes caller-supplied storage.
func WithProviderOwnership(closer ProviderCloser) Option {
	return func(cfg *config) error {
		if isNilDynamic(reflect.ValueOf(closer)) {
			return &InvalidOptionError{Field: "ProviderCloser"}
		}
		if cfg.providerClose != nil {
			return &InvalidOptionError{Field: "ProviderOwnership"}
		}
		cfg.providerClose = closer.Close
		return nil
	}
}

// WithIOProviderOwnership explicitly transfers ownership of a provider whose
// released lifecycle contract is the standard io.Closer shape. Because
// io.Closer has no context, ShutdownTimeout can release the Store lifecycle but
// cannot force the underlying Close to return; its adapter goroutine may outlive
// the Store until the provider eventually returns. Transfer takes effect only
// after Open succeeds; a failed Open never closes the provider.
func WithIOProviderOwnership(closer io.Closer) Option {
	return func(cfg *config) error {
		if isNilDynamic(reflect.ValueOf(closer)) {
			return &InvalidOptionError{Field: "IOProviderCloser"}
		}
		if cfg.providerClose != nil {
			return &InvalidOptionError{Field: "ProviderOwnership"}
		}
		adapter := newIOProviderAdapter(closer)
		cfg.providerClose = adapter.close
		cfg.ioAdapter = adapter
		return nil
	}
}

type ioProviderAdapter struct {
	closer io.Closer
	result chan error
	done   chan struct{}
}

func newIOProviderAdapter(closer io.Closer) *ioProviderAdapter {
	return &ioProviderAdapter{
		closer: closer,
		result: make(chan error, 1),
		done:   make(chan struct{}),
	}
}

func (a *ioProviderAdapter) close(ctx context.Context) error {
	go func() {
		defer close(a.done)
		a.result <- a.closer.Close()
	}()
	select {
	case err := <-a.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
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
