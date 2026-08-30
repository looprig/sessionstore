package sessionstore

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func TestOpenRejectsMissingPrimitive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		component string
		mutate    func(*storage.Composite)
	}{
		{name: "nil composite", component: "Composite", mutate: func(*storage.Composite) {}},
		{name: "ledger", component: "Ledger", mutate: func(backend *storage.Composite) { backend.Ledger = nil }},
		{name: "leaser", component: "Leaser", mutate: func(backend *storage.Composite) { backend.Leaser = nil }},
		{name: "kv", component: "KV", mutate: func(backend *storage.Composite) { backend.KV = nil }},
		{name: "ordered index", component: "OrderedIndex", mutate: func(backend *storage.Composite) { backend.OrderedIndex = nil }},
		{name: "blobs", component: "Blobs", mutate: func(backend *storage.Composite) { backend.Blobs = nil }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var backend *storage.Composite
			if test.component != "Composite" {
				backend = memstore.New()
				test.mutate(backend)
			}

			store, err := Open(context.Background(), backend)
			if store != nil {
				t.Fatal("Open returned a store for an incomplete backend")
			}
			var invalid *InvalidBackendError
			if !errors.As(err, &invalid) {
				t.Fatalf("Open error = %T %v, want *InvalidBackendError", err, err)
			}
			if invalid.Component != test.component {
				t.Fatalf("Component = %q, want %q", invalid.Component, test.component)
			}
		})
	}
}

func TestOpenPreservesStorageTypedNilSemantics(t *testing.T) {
	t.Parallel()

	backend := memstore.New()
	var ledger *typedNilLedger
	backend.Ledger = ledger

	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestOpenValidatesOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		option    Option
		wantField string
		wantLimit *InvalidLimitError
	}{
		{name: "nil option", option: nil, wantField: "option"},
		{name: "zero page size", option: WithLimits(Limits{ShutdownTimeout: DefaultShutdownTimeout}), wantField: "MaxPageSize", wantLimit: &InvalidLimitError{Field: "MaxPageSize", Value: 0, Min: 1, Max: storage.MaxOrderedPageLimit}},
		{name: "negative page size", option: WithLimits(Limits{MaxPageSize: -1, ShutdownTimeout: DefaultShutdownTimeout}), wantField: "MaxPageSize", wantLimit: &InvalidLimitError{Field: "MaxPageSize", Value: -1, Min: 1, Max: storage.MaxOrderedPageLimit}},
		{name: "page size above provider maximum", option: WithLimits(Limits{MaxPageSize: storage.MaxOrderedPageLimit + 1, ShutdownTimeout: DefaultShutdownTimeout}), wantField: "MaxPageSize", wantLimit: &InvalidLimitError{Field: "MaxPageSize", Value: storage.MaxOrderedPageLimit + 1, Min: 1, Max: storage.MaxOrderedPageLimit}},
		{name: "zero shutdown timeout", option: WithLimits(Limits{MaxPageSize: 1}), wantField: "ShutdownTimeout", wantLimit: &InvalidLimitError{Field: "ShutdownTimeout", Value: 0, Min: 1, Max: int64(^uint64(0) >> 1)}},
		{name: "negative shutdown timeout", option: WithLimits(Limits{MaxPageSize: 1, ShutdownTimeout: -time.Nanosecond}), wantField: "ShutdownTimeout", wantLimit: &InvalidLimitError{Field: "ShutdownTimeout", Value: -1, Min: 1, Max: int64(^uint64(0) >> 1)}},
		{name: "nil clock", option: WithClock(nil), wantField: "Clock"},
		{name: "typed nil clock", option: WithClock((*nilClock)(nil)), wantField: "Clock"},
		{name: "nil logger", option: WithLogger(nil), wantField: "Logger"},
		{name: "nil owned provider", option: WithProviderOwnership(nil), wantField: "ProviderCloser"},
		{name: "typed nil owned provider", option: WithProviderOwnership((*recordingCloser)(nil)), wantField: "ProviderCloser"},
		{name: "nil owned io provider", option: WithIOProviderOwnership(nil), wantField: "IOProviderCloser"},
		{name: "typed nil owned io provider", option: WithIOProviderOwnership((*recordingIOCloser)(nil)), wantField: "IOProviderCloser"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store, err := Open(context.Background(), memstore.New(), test.option)
			if store != nil {
				t.Fatal("Open returned a store for an invalid option")
			}
			var invalid *InvalidOptionError
			if !errors.As(err, &invalid) {
				t.Fatalf("Open error = %T %v, want *InvalidOptionError", err, err)
			}
			if invalid.Field != test.wantField {
				t.Fatalf("Field = %q, want %q", invalid.Field, test.wantField)
			}
			if test.wantLimit != nil {
				var limit *InvalidLimitError
				if !errors.As(err, &limit) {
					t.Fatalf("Open error = %T %v, want wrapped *InvalidLimitError", err, err)
				}
				if *limit != *test.wantLimit {
					t.Fatalf("InvalidLimitError = %+v, want %+v", *limit, *test.wantLimit)
				}
			}
		})
	}
}

func TestOpenAcceptsExplicitSeams(t *testing.T) {
	t.Parallel()

	clock := fixedClock{now: time.Unix(123, 456)}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := Open(
		context.Background(),
		memstore.New(),
		WithClock(clock),
		WithLogger(logger),
		WithLimits(Limits{MaxPageSize: 17, ShutdownTimeout: 3 * time.Second}),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	if got := store.clock.Now(); !got.Equal(clock.now) {
		t.Fatalf("clock.Now() = %v, want %v", got, clock.now)
	}
	if store.logger != logger {
		t.Fatal("logger option was not retained")
	}
	if store.limits.MaxPageSize != 17 {
		t.Fatalf("MaxPageSize = %d, want 17", store.limits.MaxPageSize)
	}
	if store.limits.ShutdownTimeout != 3*time.Second {
		t.Fatalf("ShutdownTimeout = %v, want 3s", store.limits.ShutdownTimeout)
	}
}

func TestCloseDoesNotCloseUnownedProvider(t *testing.T) {
	t.Parallel()

	closer := &recordingCloser{}
	backend := memstore.New()
	backend.Ledger = &closeCapableLedger{Ledger: backend.Ledger, closer: closer}
	backend.Leaser = &closeCapableLeaser{Leaser: backend.Leaser, closer: closer}
	backend.KV = &closeCapableKV{KV: backend.KV, closer: closer}
	backend.OrderedIndex = &closeCapableOrderedIndex{OrderedIndex: backend.OrderedIndex, closer: closer}
	backend.Blobs = &closeCapableBlobs{Blobs: backend.Blobs, closer: closer}
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := closer.calls.Load(); got != 0 {
		t.Fatalf("provider Close calls = %d, want 0", got)
	}
}

func TestCloseCancelsOwnedContextBeforeClosingOwnedProvider(t *testing.T) {
	t.Parallel()

	closer := &recordingCloser{closeFn: func(context.Context) error { return errProviderClose }}
	store, err := Open(context.Background(), memstore.New(), WithProviderOwnership(closer))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	store.background.Add(1)
	workerDone := make(chan struct{})
	go func() {
		defer store.background.Done()
		<-store.ctx.Done()
		close(workerDone)
	}()

	err = store.Close(context.Background())
	if !errors.Is(err, errProviderClose) {
		t.Fatalf("Close error = %v, want %v", err, errProviderClose)
	}
	select {
	case <-workerDone:
	default:
		t.Fatal("owned context was not canceled before provider close completed")
	}
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("provider Close calls = %d, want 1", got)
	}
}

func TestCloseOwnsIOProviderExactlyOnce(t *testing.T) {
	t.Parallel()

	closer := &recordingIOCloser{closeFn: func() error { return errProviderClose }}
	store, err := Open(context.Background(), memstore.New(), WithIOProviderOwnership(closer))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := store.Close(context.Background()); !errors.Is(got, errProviderClose) {
		t.Fatalf("Close error = %v, want %v", got, errProviderClose)
	}
	if got := store.Close(context.Background()); !errors.Is(got, errProviderClose) {
		t.Fatalf("second Close error = %v, want %v", got, errProviderClose)
	}
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("provider Close calls = %d, want 1", got)
	}
}

func TestCloseIsConcurrentIdempotentAndReturnsStableResult(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	closer := &recordingCloser{closeFn: func(context.Context) error {
		<-release
		return errProviderClose
	}}
	store, err := Open(context.Background(), memstore.New(), WithProviderOwnership(closer))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const callers = 24
	start := make(chan struct{})
	results := make(chan error, callers)
	for range callers {
		go func() {
			<-start
			results <- store.Close(context.Background())
		}()
	}
	close(start)
	waitFor(t, time.Second, func() bool { return closer.calls.Load() == 1 })
	close(release)

	for range callers {
		if got := <-results; !errors.Is(got, errProviderClose) {
			t.Fatalf("Close error = %v, want stable %v", got, errProviderClose)
		}
	}
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("provider Close calls = %d, want 1", got)
	}
	if got := store.Close(context.Background()); !errors.Is(got, errProviderClose) {
		t.Fatalf("idempotent Close error = %v, want %v", got, errProviderClose)
	}
}

func TestCloseCallerDeadlineDoesNotAbandonLifecycle(t *testing.T) {
	t.Parallel()

	providerContext := make(chan error, 1)
	closer := &recordingCloser{closeFn: func(ctx context.Context) error {
		providerContext <- ctx.Err()
		return errProviderClose
	}}
	store, err := Open(
		context.Background(),
		memstore.New(),
		WithLimits(Limits{MaxPageSize: 1, ShutdownTimeout: time.Second}),
		WithProviderOwnership(closer),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	store.background.Add(1)
	workerCanceled := make(chan struct{})
	releaseWorker := make(chan struct{})
	go func() {
		defer store.background.Done()
		<-store.ctx.Done()
		close(workerCanceled)
		<-releaseWorker
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	closeResult := make(chan error, 1)
	go func() { closeResult <- store.Close(ctx) }()
	<-workerCanceled
	<-ctx.Done()
	if got := <-closeResult; !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("first Close error = %v, want deadline exceeded", got)
	}
	if got := closer.calls.Load(); got != 0 {
		t.Fatalf("provider Close calls before worker drain = %d, want 0", got)
	}

	close(releaseWorker)
	if got := <-providerContext; got != nil {
		t.Fatalf("provider context error on entry = %v, want nil", got)
	}
	if got := store.Close(context.Background()); !errors.Is(got, errProviderClose) {
		t.Fatalf("eventual Close error = %v, want stable %v", got, errProviderClose)
	}
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("provider Close calls = %d, want 1", got)
	}
}

func TestCloseAfterCompletionIgnoresCanceledCallerContext(t *testing.T) {
	t.Parallel()

	closer := &recordingCloser{closeFn: func(context.Context) error { return errProviderClose }}
	store, err := Open(context.Background(), memstore.New(), WithProviderOwnership(closer))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := store.Close(context.Background()); !errors.Is(got, errProviderClose) {
		t.Fatalf("initial Close error = %v, want %v", got, errProviderClose)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 10_000; i++ {
		if got := store.Close(canceled); !errors.Is(got, errProviderClose) {
			t.Fatalf("Close iteration %d = %v, want stable %v", i, got, errProviderClose)
		}
	}
}

func TestProviderShutdownTimeoutIsStable(t *testing.T) {
	t.Parallel()

	closer := &recordingCloser{closeFn: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	store, err := Open(
		context.Background(),
		memstore.New(),
		WithLimits(Limits{MaxPageSize: 1, ShutdownTimeout: time.Millisecond}),
		WithProviderOwnership(closer),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := store.Close(context.Background()); !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("initial Close error = %v, want deadline exceeded", got)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if got := store.Close(canceled); !errors.Is(got, context.DeadlineExceeded) || errors.Is(got, context.Canceled) {
		t.Fatalf("completed Close error = %v, want stable deadline exceeded", got)
	}
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("provider Close calls = %d, want 1", got)
	}
}

var errProviderClose = errors.New("provider close failed")

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type nilClock struct{}

func (*nilClock) Now() time.Time { return time.Time{} }

type recordingCloser struct {
	calls   atomic.Int32
	closeFn func(context.Context) error
}

type recordingIOCloser struct {
	calls   atomic.Int32
	closeFn func() error
}

func (c *recordingIOCloser) Close() error {
	c.calls.Add(1)
	if c.closeFn != nil {
		return c.closeFn()
	}
	return nil
}

type closeCapableLedger struct {
	storage.Ledger
	closer *recordingCloser
}

func (p *closeCapableLedger) Close(ctx context.Context) error { return p.closer.Close(ctx) }

type closeCapableLeaser struct {
	storage.Leaser
	closer *recordingCloser
}

func (p *closeCapableLeaser) Close(ctx context.Context) error { return p.closer.Close(ctx) }

type closeCapableKV struct {
	storage.KV
	closer *recordingCloser
}

func (p *closeCapableKV) Close(ctx context.Context) error { return p.closer.Close(ctx) }

type closeCapableOrderedIndex struct {
	storage.OrderedIndex
	closer *recordingCloser
}

func (p *closeCapableOrderedIndex) Close(ctx context.Context) error { return p.closer.Close(ctx) }

type closeCapableBlobs struct {
	storage.Blobs
	closer *recordingCloser
}

func (p *closeCapableBlobs) Close(ctx context.Context) error { return p.closer.Close(ctx) }

func (c *recordingCloser) Close(ctx context.Context) error {
	c.calls.Add(1)
	if c.closeFn != nil {
		return c.closeFn(ctx)
	}
	return nil
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not met before timeout")
		}
		time.Sleep(time.Millisecond)
	}
}

// typedNilLedger deliberately proves literal interface nil semantics: SessionStore
// matches storage.NewComposite and does not use reflection to reject typed nils.
type typedNilLedger struct{}

func (*typedNilLedger) Append(context.Context, string, uint64, []byte) error { return nil }
func (*typedNilLedger) Read(context.Context, string, uint64) (storage.Cursor, error) {
	return nil, nil
}
func (*typedNilLedger) Tip(context.Context, string) (uint64, error) { return 0, nil }
func (*typedNilLedger) Delete(context.Context, string) error        { return nil }

var _ storage.Ledger = (*typedNilLedger)(nil)
