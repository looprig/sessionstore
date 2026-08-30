package sessionstore

import (
	"context"
	"log/slog"
	"sync"

	"github.com/looprig/storage"
)

// Store is the durable session aggregate over one complete storage backend.
type Store struct {
	backend *storage.Composite
	limits  Limits
	clock   Clock
	logger  *slog.Logger

	ctx           context.Context
	cancel        context.CancelFunc
	background    sync.WaitGroup
	providerClose func(context.Context) error
	closeOnce     sync.Once
	closeDone     chan struct{}
	closeErr      error
}

// Open constructs a Store over a complete storage composite. Open performs no
// marker, keyspace, or other provider I/O.
func Open(ctx context.Context, backend *storage.Composite, opts ...Option) (*Store, error) {
	if backend == nil {
		return nil, &InvalidBackendError{Component: "Composite"}
	}
	if backend.Ledger == nil {
		return nil, &InvalidBackendError{Component: "Ledger"}
	}
	if backend.Leaser == nil {
		return nil, &InvalidBackendError{Component: "Leaser"}
	}
	if backend.KV == nil {
		return nil, &InvalidBackendError{Component: "KV"}
	}
	if backend.OrderedIndex == nil {
		return nil, &InvalidBackendError{Component: "OrderedIndex"}
	}
	if backend.Blobs == nil {
		return nil, &InvalidBackendError{Component: "Blobs"}
	}
	cfg := defaultConfig()
	for _, option := range opts {
		if option == nil {
			return nil, &InvalidOptionError{Field: "option"}
		}
		if err := option(&cfg); err != nil {
			return nil, err
		}
	}

	ownedCtx, cancel := context.WithCancel(ctx)
	return &Store{
		backend:       backend,
		limits:        cfg.limits,
		clock:         cfg.clock,
		logger:        cfg.logger,
		ctx:           ownedCtx,
		cancel:        cancel,
		providerClose: cfg.providerClose,
		closeDone:     make(chan struct{}),
	}, nil
}

// Close initiates shutdown exactly once. It cancels Store-owned work, waits for
// it, then closes an explicitly owned provider at most once with a fresh,
// lifecycle-owned timeout. Each caller's ctx bounds only its own wait. If a
// caller stops waiting, shutdown continues and a later call can observe the one
// stable final result.
func (s *Store) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.cancel()
		go func() {
			s.background.Wait()
			if s.providerClose != nil {
				closeCtx, cancel := context.WithTimeout(context.Background(), s.limits.ShutdownTimeout)
				s.closeErr = s.providerClose(closeCtx)
				cancel()
			}
			close(s.closeDone)
		}()
	})

	select {
	case <-s.closeDone:
		return s.closeErr
	default:
	}
	select {
	case <-s.closeDone:
		return s.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
