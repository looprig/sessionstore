package sessionstore

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
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

func TestOpenRejectsInvalidBlobReaderLifecycleBeforeProviderIO(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		blobs          func(storage.Blobs, *atomic.Int32) storage.Blobs
		wantBoundCalls int32
	}{
		{
			name: "missing capability",
			blobs: func(blobs storage.Blobs, _ *atomic.Int32) storage.Blobs {
				return &baseOnlyBlobs{Blobs: blobs}
			},
		},
		{
			name: "dynamic typed nil capability",
			blobs: func(_ storage.Blobs, _ *atomic.Int32) storage.Blobs {
				var blobs *boundedLifecycleBlobs
				return blobs
			},
		},
		{
			name: "zero bound",
			blobs: func(blobs storage.Blobs, calls *atomic.Int32) storage.Blobs {
				return &boundedLifecycleBlobs{Blobs: blobs, boundCalls: calls}
			},
			wantBoundCalls: 1,
		},
		{
			name: "negative bound",
			blobs: func(blobs storage.Blobs, calls *atomic.Int32) storage.Blobs {
				return &boundedLifecycleBlobs{Blobs: blobs, bound: -time.Nanosecond, boundCalls: calls}
			},
			wantBoundCalls: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			base := memstore.New()
			backend, providerCalls := instrumentComposite(base)
			var boundCalls atomic.Int32
			backend.Blobs = test.blobs(backend.Blobs, &boundCalls)
			closer := &recordingCloser{}

			store, err := Open(context.Background(), backend, WithProviderOwnership(closer))
			if store != nil {
				t.Fatal("Open returned a Store for an invalid Blob reader lifecycle")
			}
			var invalid *InvalidBackendError
			if !errors.As(err, &invalid) || invalid.Component != "BlobReaderLifecycle" {
				t.Fatalf("Open error = %T %v, want BlobReaderLifecycle *InvalidBackendError", err, err)
			}
			if got := boundCalls.Load(); got != test.wantBoundCalls {
				t.Fatalf("BlobReaderCloseBound calls = %d, want %d", got, test.wantBoundCalls)
			}
			if got := providerCalls.snapshot(); got != (providerCallSnapshot{}) {
				t.Fatalf("provider calls before rejection = %+v, want zero", got)
			}
			if got := closer.calls.Load(); got != 0 {
				t.Fatalf("transferred provider Close calls = %d, want zero", got)
			}
			if _, _, markerErr := base.KV.Get(context.Background(), layoutMarkerKey); !isKeyNotFound(markerErr, layoutMarkerKey) {
				t.Fatalf("layout marker after rejection = %v, want absent", markerErr)
			}
		})
	}
}

func TestOpenAcceptsPositiveBlobReaderLifecycleIndependentOfShutdownTimeout(t *testing.T) {
	t.Parallel()

	backend := memstore.New()
	var boundCalls atomic.Int32
	backend.Blobs = &boundedLifecycleBlobs{Blobs: backend.Blobs, bound: time.Hour, boundCalls: &boundCalls}
	store, err := Open(context.Background(), backend, WithShutdownTimeout(time.Nanosecond))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := boundCalls.Load(); got != 1 {
		t.Fatalf("BlobReaderCloseBound calls = %d, want 1", got)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
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
		{name: "zero page size", option: WithLimits(Limits{}), wantField: "MaxPageSize", wantLimit: &InvalidLimitError{Field: "MaxPageSize", Value: 0, Min: 1, Max: storage.MaxOrderedPageLimit}},
		{name: "negative page size", option: WithLimits(Limits{MaxPageSize: -1}), wantField: "MaxPageSize", wantLimit: &InvalidLimitError{Field: "MaxPageSize", Value: -1, Min: 1, Max: storage.MaxOrderedPageLimit}},
		{name: "page size above provider maximum", option: WithLimits(Limits{MaxPageSize: storage.MaxOrderedPageLimit + 1}), wantField: "MaxPageSize", wantLimit: &InvalidLimitError{Field: "MaxPageSize", Value: storage.MaxOrderedPageLimit + 1, Min: 1, Max: storage.MaxOrderedPageLimit}},
		{name: "zero shutdown timeout", option: WithShutdownTimeout(0), wantField: "ShutdownTimeout", wantLimit: &InvalidLimitError{Field: "ShutdownTimeout", Value: 0, Min: 1, Max: int64(^uint64(0) >> 1)}},
		{name: "negative shutdown timeout", option: WithShutdownTimeout(-time.Nanosecond), wantField: "ShutdownTimeout", wantLimit: &InvalidLimitError{Field: "ShutdownTimeout", Value: -1, Min: 1, Max: int64(^uint64(0) >> 1)}},
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
		WithLimits(Limits{MaxPageSize: 17}),
		WithShutdownTimeout(3*time.Second),
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
	if store.shutdownTimeout != 3*time.Second {
		t.Fatalf("ShutdownTimeout = %v, want 3s", store.shutdownTimeout)
	}
}

func TestOpenRejectsRepeatedProviderOwnership(t *testing.T) {
	t.Parallel()

	contextCloser := &recordingCloser{}
	ioCloser := &recordingIOCloser{}
	tests := []struct {
		name    string
		options []Option
	}{
		{name: "context then context", options: []Option{WithProviderOwnership(contextCloser), WithProviderOwnership(contextCloser)}},
		{name: "io then io", options: []Option{WithIOProviderOwnership(ioCloser), WithIOProviderOwnership(ioCloser)}},
		{name: "context then io", options: []Option{WithProviderOwnership(contextCloser), WithIOProviderOwnership(ioCloser)}},
		{name: "io then context", options: []Option{WithIOProviderOwnership(ioCloser), WithProviderOwnership(contextCloser)}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store, err := Open(context.Background(), memstore.New(), test.options...)
			if store != nil {
				t.Fatal("Open returned a store for repeated ownership transfer")
			}
			var invalid *InvalidOptionError
			if !errors.As(err, &invalid) || invalid.Field != "ProviderOwnership" {
				t.Fatalf("Open error = %T %v, want ProviderOwnership *InvalidOptionError", err, err)
			}
		})
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
	backend.Blobs = &closeCapableBlobs{lifecycleBlobs: lifecycleBlobs{backend.Blobs}, closer: closer}
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

	workerDone := make(chan struct{})
	if err := store.startBackground(func(ctx context.Context) {
		<-ctx.Done()
		close(workerDone)
	}); err != nil {
		t.Fatalf("startBackground: %v", err)
	}

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

func TestIOProviderShutdownTimeoutReturnsStableResult(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	exited := make(chan struct{})
	var closerStarted atomic.Bool
	var releaseOnce sync.Once
	releaseCloser := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		releaseCloser()
		if closerStarted.Load() {
			<-exited
		}
	})

	closer := &recordingIOCloser{closeFn: func() error {
		closerStarted.Store(true)
		close(started)
		defer close(exited)
		<-release
		return errProviderClose
	}}
	store, err := Open(
		context.Background(),
		memstore.New(),
		WithShutdownTimeout(time.Millisecond),
		WithIOProviderOwnership(closer),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	result := make(chan error, 1)
	go func() { result <- store.Close(context.Background()) }()
	safety, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-started:
	case <-safety.Done():
		t.Fatal("owned io.Closer did not start")
	}
	select {
	case got := <-result:
		if !errors.Is(got, context.DeadlineExceeded) {
			t.Fatalf("Close error = %v, want deadline exceeded", got)
		}
	case <-safety.Done():
		t.Fatal("Close did not honor ShutdownTimeout")
	}

	canceled, cancelCanceled := context.WithCancel(context.Background())
	cancelCanceled()
	if got := store.Close(canceled); !errors.Is(got, context.DeadlineExceeded) || errors.Is(got, context.Canceled) {
		t.Fatalf("completed Close error = %v, want stable deadline exceeded", got)
	}
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("provider Close calls = %d, want 1", got)
	}

	releaseCloser()
	<-exited
	adapterSafety, cancelAdapterSafety := context.WithTimeout(context.Background(), time.Second)
	defer cancelAdapterSafety()
	select {
	case <-store.ioAdapter.done:
	case <-adapterSafety.Done():
		// Drain an intentionally unbuffered result mutation so its wrapper can
		// exit before the test reports the failure.
		select {
		case <-store.ioAdapter.result:
		case <-time.After(time.Second):
			t.Fatal("timed-out io.Closer adapter could not be drained")
		}
		<-store.ioAdapter.done
		t.Fatal("io.Closer adapter goroutine did not exit after provider return")
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
		WithShutdownTimeout(time.Second),
		WithProviderOwnership(closer),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	workerCanceled := make(chan struct{})
	releaseWorker := make(chan struct{})
	if err := store.startBackground(func(ctx context.Context) {
		<-ctx.Done()
		close(workerCanceled)
		<-releaseWorker
	}); err != nil {
		t.Fatalf("startBackground: %v", err)
	}

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
		WithShutdownTimeout(time.Millisecond),
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

func TestCloseRejectsWorkAdmissionAfterShutdownStarts(t *testing.T) {
	t.Parallel()

	providerClosed := make(chan struct{})
	closer := &recordingCloser{closeFn: func(context.Context) error {
		close(providerClosed)
		return nil
	}}
	store, err := Open(context.Background(), memstore.New(), WithProviderOwnership(closer))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	workerCanceled := make(chan struct{})
	releaseWorker := make(chan struct{})
	if err := store.startBackground(func(ctx context.Context) {
		<-ctx.Done()
		close(workerCanceled)
		<-releaseWorker
	}); err != nil {
		t.Fatalf("startBackground: %v", err)
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- store.Close(context.Background()) }()
	<-workerCanceled

	lateRan := make(chan struct{})
	err = store.startBackground(func(context.Context) { close(lateRan) })
	var closed *StoreClosedError
	if !errors.As(err, &closed) {
		t.Fatalf("late startBackground error = %T %v, want *StoreClosedError", err, err)
	}
	select {
	case <-lateRan:
		t.Fatal("work admitted after shutdown started")
	default:
	}
	select {
	case <-providerClosed:
		t.Fatal("provider closed before admitted work drained")
	default:
	}

	close(releaseWorker)
	if err := <-closeResult; err != nil {
		t.Fatalf("Close: %v", err)
	}
	<-providerClosed
	select {
	case <-lateRan:
		t.Fatal("late work ran after provider close")
	default:
	}
}

func TestLifecycleOperationsHoldAdmissionLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation lifecycleOperation
		invoke    func(*Store) error
	}{
		{name: "admit", operation: lifecycleAdmit, invoke: func(store *Store) error {
			return store.startBackground(func(context.Context) {})
		}},
		{name: "close", operation: lifecycleClose, invoke: func(store *Store) error {
			return store.Close(context.Background())
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store, err := Open(context.Background(), memstore.New())
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			checked := make(chan bool, 1)
			store.lifecycleLocked = func(operation lifecycleOperation) {
				if operation != test.operation {
					return
				}
				unlocked := store.lifecycleMu.TryLock()
				if unlocked {
					store.lifecycleMu.Unlock()
				}
				checked <- !unlocked
			}

			if err := test.invoke(store); err != nil {
				t.Fatalf("invoke: %v", err)
			}
			if held := <-checked; !held {
				t.Fatal("lifecycle operation reached mutation boundary without holding lifecycleMu")
			}
			if test.operation != lifecycleClose {
				store.lifecycleLocked = nil
				if err := store.Close(context.Background()); err != nil {
					t.Fatalf("Close: %v", err)
				}
			}
		})
	}
}

func TestStartBackgroundRejectsNilWorkSynchronously(t *testing.T) {
	t.Parallel()

	store, err := Open(context.Background(), memstore.New())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var invalid *InvalidBackgroundWorkError
	if err := store.startBackground(nil); !errors.As(err, &invalid) {
		t.Fatalf("startBackground(nil) error = %T %v, want *InvalidBackgroundWorkError", err, err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
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
	lifecycleBlobs
	closer *recordingCloser
}

func (p *closeCapableBlobs) Close(ctx context.Context) error { return p.closer.Close(ctx) }

type baseOnlyBlobs struct{ storage.Blobs }

type boundedLifecycleBlobs struct {
	storage.Blobs
	bound      time.Duration
	boundCalls *atomic.Int32
}

func (b *boundedLifecycleBlobs) BlobReaderCloseBound() time.Duration {
	b.boundCalls.Add(1)
	return b.bound
}

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
