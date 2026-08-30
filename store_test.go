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
	}{
		{name: "nil option", option: nil, wantField: "option"},
		{name: "zero page size", option: WithLimits(Limits{}), wantField: "MaxPageSize"},
		{name: "negative page size", option: WithLimits(Limits{MaxPageSize: -1}), wantField: "MaxPageSize"},
		{name: "page size above provider maximum", option: WithLimits(Limits{MaxPageSize: storage.MaxOrderedPageLimit + 1}), wantField: "MaxPageSize"},
		{name: "nil clock", option: WithClock(nil), wantField: "Clock"},
		{name: "nil logger", option: WithLogger(nil), wantField: "Logger"},
		{name: "nil owned provider", option: WithProviderOwnership(nil), wantField: "ProviderCloser"},
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

	release := make(chan struct{})
	closer := &recordingCloser{closeFn: func(context.Context) error {
		<-release
		return errProviderClose
	}}
	store, err := Open(context.Background(), memstore.New(), WithProviderOwnership(closer))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if got := store.Close(ctx); !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("first Close error = %v, want deadline exceeded", got)
	}
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("provider Close calls after timeout = %d, want 1", got)
	}

	close(release)
	if got := store.Close(context.Background()); !errors.Is(got, errProviderClose) {
		t.Fatalf("eventual Close error = %v, want stable %v", got, errProviderClose)
	}
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("provider Close calls = %d, want 1", got)
	}
}

var errProviderClose = errors.New("provider close failed")

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type recordingCloser struct {
	calls   atomic.Int32
	closeFn func(context.Context) error
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
