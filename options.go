package sessionstore

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/looprig/storage"
)

// Limits contains Store-wide ceilings. MaxPageSize bounds every provider page
// requested by SessionStore; individual operations may request a smaller page.
type Limits struct {
	MaxPageSize int
}

// DefaultLimits returns the least restrictive limits guaranteed by Storage.
func DefaultLimits() Limits {
	return Limits{MaxPageSize: storage.MaxOrderedPageLimit}
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
	limits         Limits
	clock          Clock
	logger         *slog.Logger
	providerCloser ProviderCloser
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
				Value: limits.MaxPageSize,
				Min:   1,
				Max:   storage.MaxOrderedPageLimit,
			}
			return &InvalidOptionError{Field: "MaxPageSize", Cause: cause}
		}
		cfg.limits = limits
		return nil
	}
}

// WithClock supplies the clock used by Store.
func WithClock(clock Clock) Option {
	return func(cfg *config) error {
		if clock == nil {
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
		if closer == nil {
			return &InvalidOptionError{Field: "ProviderCloser"}
		}
		cfg.providerCloser = closer
		return nil
	}
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
