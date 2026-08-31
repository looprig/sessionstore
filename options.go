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
	shards          uint32
}

func defaultConfig() config {
	return config{
		limits:          DefaultLimits(),
		clock:           systemClock{},
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		shutdownTimeout: DefaultShutdownTimeout,
		layout:          layoutTenantV1,
		shards:          DefaultControlShards,
	}
}

// WithControlShards names the number of service-control shards outstanding work
// is spread across.
//
// IT IS NOT A RUNTIME SETTING, and the option is where that has to be said,
// because the name reads like one. The count is an input to controlShardOf, so
// it decides the namespace every inbox command and every gate deadline intent
// is FILED IN. Open persists it in the backend's layout marker and refuses a
// later Open of the same backend that names a different one; changing it for a
// backend that already holds records is an offline migration that must move
// them, not a redeploy with a new flag.
//
// A larger count spreads a sweep across more replicas and makes any one shard's
// due page shorter. It is not free: a sweep visits every shard, so the count is
// a floor on the provider queries one pass costs even when nothing is due.
func WithControlShards(shards int) Option {
	return func(cfg *config) error {
		if shards < MinControlShards || shards > MaxControlShards {
			cause := &InvalidLimitError{
				Field: "ControlShards",
				Value: int64(shards),
				Min:   MinControlShards,
				Max:   MaxControlShards,
			}
			return &InvalidOptionError{Field: "ControlShards", Cause: cause}
		}
		cfg.shards = uint32(shards)
		return nil
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
//
// Only the clock VALUE is validated, and only for being non-nil. Nothing checks
// what it returns: there is no monotonicity requirement, no bound, and no
// comparison against the machine's own clock. That is a deliberate limit on what
// this package claims, and it has a consequence worth stating where the clock is
// supplied rather than leaving it to be discovered from whichever guard survived
// it.
//
// The rule the package holds itself to instead is that a guard is a function of
// the RECORD, never of the clock alone. A stored record is validated against
// rankableTime, which bounds the instants a record may CARRY; it says nothing
// about a clock, so a predicate that compares against an absent or zero instant
// has to be total on its own — see claimLive in inbox_claim.go, whose zero-claim
// conjunct exists for exactly that reason.
//
// What a wrong clock costs is therefore LIVENESS rather than safety. A clock
// running slow leaves claims looking live and deadlines looking distant, so work
// waits; one running fast expires claims early, so work is redone. Neither puts
// two writers on one command, because ownership is decided by the lease epoch
// and by the record's revision, and neither of those is a clock reading.
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
