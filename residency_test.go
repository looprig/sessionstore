package sessionstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func createResidencySession(t *testing.T, s *Store) AcquireResidencyRequest {
	t.Helper()
	req := testCreateRequest()
	req.Binding = testSessionBinding()
	if _, _, err := s.CreateCatalogEntry(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	return AcquireResidencyRequest{TenantID: req.TenantID, SessionID: req.SessionID}
}

func TestResidencyIndependentGrant(t *testing.T) {
	ctx := context.Background()
	backend := memstore.New()
	log := &callLog{}
	backend.Ledger = &scriptedLedger{Ledger: backend.Ledger, log: log}
	s := openStore(t, backend)
	req := createResidencySession(t, s)
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	// The disposition journal API is still pending. Use the published Storage
	// journal lease namespace to prove domain independence without bypassing the
	// intentional legacy OpenJournal refusal.
	for range 3 {
		lease, err := backend.Leaser.Acquire(ctx, scope.LeaseName)
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Release(ctx); err != nil {
			t.Fatal(err)
		}
	}
	journal, err := backend.Leaser.Acquire(ctx, scope.LeaseName)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Release(ctx)
	grant, err := s.AcquireResidency(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if grant.Epoch() != ResidencyEpoch(1) || journal.Epoch() != 4 {
		t.Fatalf("independent counters: residency=%d journal=%d", grant.Epoch(), journal.Epoch())
	}
	_, err = s.AcquireResidency(ctx, req)
	var held *storage.LeaseHeldError
	if !errors.As(err, &held) {
		t.Fatalf("contention: %v", err)
	}
	if err := grant.Release(ctx); err != nil {
		t.Fatal(err)
	}
	next, err := s.AcquireResidency(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if next.Epoch() != 2 {
		t.Fatalf("successor = %d", next.Epoch())
	}
	if err := grant.Release(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-next.Lost():
		t.Fatal("stale release lost successor")
	default:
	}
	if err := next.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if calls := log.snapshot(); len(calls) != 0 {
		t.Fatalf("residency touched ledger: %v", calls)
	}
}

type residencyLeaser struct {
	storage.Leaser
	mu           sync.Mutex
	calls        int
	afterAcquire func(context.Context)
	lease        storage.Lease
}

func (l *residencyLeaser) Acquire(ctx context.Context, name string) (storage.Lease, error) {
	l.mu.Lock()
	l.calls++
	l.mu.Unlock()
	lease := l.lease
	if lease == nil {
		var err error
		lease, err = l.Leaser.Acquire(ctx, name)
		if err != nil {
			return nil, err
		}
	}
	if l.afterAcquire != nil {
		l.afterAcquire(ctx)
	}
	return lease, nil
}

func TestResidencyRefusalsBeforeLease(t *testing.T) {
	for _, state := range []string{"missing", "witness only", "legacy", "other tenant", "corrupt catalog", "legacy layout"} {
		t.Run(state, func(t *testing.T) {
			backend := memstore.New()
			audit := &residencyLeaser{Leaser: backend.Leaser}
			backend.Leaser = audit
			var opts []Option
			if state == "legacy layout" {
				opts = append(opts, WithLegacySingleTenant("local"))
			}
			s := openStore(t, backend, opts...)
			req := testCreateRequest()
			req.Binding = testSessionBinding()
			if state == "legacy layout" {
				req.TenantID = "local"
				req.SessionID = "123e4567-e89b-12d3-a456-426614174000"
				req.Binding.ProtocolMode = ProtocolModeLegacy
			}
			if state == "legacy" {
				req.Binding = SessionBinding{}
			}
			if state != "missing" && state != "witness only" {
				entry, _, err := s.CreateCatalogEntry(context.Background(), req)
				if err != nil {
					t.Fatal(err)
				}
				if state == "corrupt catalog" {
					scope, _ := s.deriveSessionScope(req.TenantID, req.SessionID)
					_, err = backend.OrderedIndex.Update(context.Background(), catalogID(scope, req.SessionID), entry.Revision, []byte("bad"), catalogRank(entry.Record), storage.Due{})
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			if state == "witness only" {
				scope, _ := s.deriveSessionScope(req.TenantID, req.SessionID)
				if err := s.bindSessionScopeMode(context.Background(), scope, ProtocolModeDisposition); err != nil {
					t.Fatal(err)
				}
			}
			if state == "other tenant" {
				req.TenantID = "other"
			}
			if _, err := s.AcquireResidency(context.Background(), AcquireResidencyRequest{TenantID: req.TenantID, SessionID: req.SessionID}); err == nil {
				t.Fatal("accepted invalid catalog scope/mode")
			}
			if audit.calls != 0 {
				t.Fatalf("acquired before refusal: %d", audit.calls)
			}
		})
	}
}

type residencyFaultLease struct {
	storage.Lease
	mu   sync.Mutex
	fail bool
}

func (l *residencyFaultLease) Release(ctx context.Context) error {
	l.mu.Lock()
	fail := l.fail
	l.mu.Unlock()
	if fail {
		return errors.New("release unavailable")
	}
	return l.Lease.Release(ctx)
}

func TestResidencyFailedReleaseRetainsAdmission(t *testing.T) {
	backend := memstore.New()
	provider := &fakeLease{epoch: 7, lost: make(chan struct{})}
	fault := &residencyFaultLease{Lease: provider, fail: true}
	backend.Leaser = staticLeaser{lease: fault}
	s := openStore(t, backend, WithShutdownTimeout(20*time.Millisecond))
	grant, err := s.AcquireResidency(context.Background(), createResidencySession(t, s))
	if err != nil {
		t.Fatal(err)
	}
	if grant.Lost() != provider.Lost() {
		t.Fatal("loss channel is not provider signal")
	}
	if err := grant.Release(context.Background()); err == nil {
		t.Fatal("release failure hidden")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := s.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("forgot cleanup obligation: %v", err)
	}
	select {
	case <-grant.Lost():
		t.Fatal("fabricated loss on failed release")
	default:
	}
	fault.mu.Lock()
	fault.fail = false
	fault.mu.Unlock()
	if err := grant.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestResidencyAcquisitionCancellation(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "caller", true: "store"}[shutdown], func(t *testing.T) {
			backend := memstore.New()
			audit := &residencyLeaser{Leaser: backend.Leaser}
			backend.Leaser = audit
			s := openStore(t, backend)
			req := createResidencySession(t, s)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			audit.afterAcquire = func(op context.Context) {
				if shutdown {
					go func() { _ = s.Close(context.Background()) }()
				} else {
					cancel()
				}
				<-op.Done()
			}
			grant, err := s.AcquireResidency(ctx, req)
			if err == nil || grant != nil {
				t.Fatalf("published canceled acquisition: %v %v", grant, err)
			}
			if err := s.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestResidencyCanceledAcquisitionCleanupRetry(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "caller", true: "store"}[shutdown], func(t *testing.T) {
			backend := memstore.New()
			provider := &fakeLease{epoch: 9, lost: make(chan struct{})}
			fault := &residencyFaultLease{Lease: provider, fail: true}
			audit := &residencyLeaser{Leaser: backend.Leaser, lease: fault}
			backend.Leaser = audit
			s := openStore(t, backend, WithShutdownTimeout(10*time.Millisecond))
			req := createResidencySession(t, s)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			audit.afterAcquire = func(op context.Context) {
				if shutdown {
					go func() { _ = s.Close(context.Background()) }()
				} else {
					cancel()
				}
				<-op.Done()
			}
			grant, err := s.AcquireResidency(ctx, req)
			var cleanup *ResidencyAcquireCleanupError
			if grant != nil || !errors.As(err, &cleanup) {
				t.Fatalf("missing cleanup-only error: %v %v", grant, err)
			}
			var closed *StoreClosedError
			if shutdown && !errors.As(err, &closed) || !shutdown && !errors.Is(err, context.Canceled) {
				t.Fatalf("missing refusal cause: %v", err)
			}
			var releaseErr *ResidencyError
			if !errors.As(err, &releaseErr) {
				t.Fatalf("missing cleanup cause: %v", err)
			}
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer closeCancel()
			if err := s.Close(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("lost failed-acquire accounting: %v", err)
			}
			fault.mu.Lock()
			fault.fail = false
			fault.mu.Unlock()
			if err := cleanup.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := cleanup.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestResidencyProviderLossAndCallerLifetime(t *testing.T) {
	backend := memstore.New()
	lease := &fakeLease{epoch: 12, lost: make(chan struct{})}
	backend.Leaser = staticLeaser{lease: lease}
	s := openStore(t, backend)
	ctx, cancel := context.WithCancel(context.Background())
	grant, err := s.AcquireResidency(ctx, createResidencySession(t, s))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-grant.Lost():
		t.Fatal("acquire caller owns grant lifetime")
	default:
	}
	lease.once.Do(func() { close(lease.lost) })
	select {
	case <-grant.Lost():
	default:
		t.Fatal("provider loss not exposed")
	}
	if err := grant.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type residencyBlockingRelease struct{ storage.Lease }

type residencyReleaseBarrier struct {
	storage.Lease
	entered chan struct{}
	proceed chan struct{}
}

func (l residencyReleaseBarrier) Release(ctx context.Context) error {
	select {
	case l.entered <- struct{}{}:
	default:
	}
	select {
	case <-l.proceed:
		return l.Lease.Release(ctx)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestResidencyConcurrentReleaseCallerBound(t *testing.T) {
	backend := memstore.New()
	lease := residencyReleaseBarrier{Lease: &fakeLease{epoch: 1, lost: make(chan struct{})}, entered: make(chan struct{}, 1), proceed: make(chan struct{})}
	backend.Leaser = staticLeaser{lease: lease}
	s := openStore(t, backend, WithShutdownTimeout(time.Second))
	grant, err := s.AcquireResidency(context.Background(), createResidencySession(t, s))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- grant.Release(context.Background()) }()
	<-lease.entered
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = grant.Release(ctx)
	elapsed := time.Since(start)
	close(lease.proceed)
	<-done
	if err := grant.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(err, context.DeadlineExceeded) || elapsed > 200*time.Millisecond {
		t.Fatalf("concurrent release ignored caller deadline: %v, %v", err, elapsed)
	}
}

func (l residencyBlockingRelease) Release(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }

func TestResidencyReleaseBoundAndShutdown(t *testing.T) {
	backend := memstore.New()
	lease := &fakeLease{epoch: 1, lost: make(chan struct{})}
	// A real provider lease delegates ownership; only Release is stalled.
	backend.Leaser = staticLeaser{lease: residencyBlockingRelease{Lease: lease}}
	s := openStore(t, backend, WithShutdownTimeout(10*time.Millisecond))
	grant, err := s.AcquireResidency(context.Background(), createResidencySession(t, s))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := grant.Release(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("release did not honor bound: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("release exceeded bound")
	}
	// Restore the test provider under the grant's serialization and drain it.
	grant.mu.Lock()
	grant.lease = lease
	grant.mu.Unlock()
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-grant.Lost():
	default:
		t.Fatal("shutdown did not release provider")
	}
	if _, err := s.AcquireResidency(context.Background(), AcquireResidencyRequest{TenantID: catalogTenant, SessionID: catalogSession}); err == nil {
		t.Fatal("acquired after close")
	}
}
