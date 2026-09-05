package sessionstore

import (
	"context"
	"errors"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// ResidencyEpoch identifies a Host residency grant. It is meaningful only in
// that session's residency namespace and must never be compared with, or used
// as, a journal epoch. Residency alone authorizes no journal writes or command
// application; the disposition protocol is not activated by this API.
type ResidencyEpoch uint64

// AcquireResidencyRequest names an existing disposition-mode catalog session.
type AcquireResidencyRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
}

// ResidencyError is a provider failure acquiring or releasing residency. Cause
// preserves provider errors, including storage.LeaseHeldError for contention.
// Catalog, scope and Store lifecycle refusals retain their existing typed errors.
type ResidencyError struct {
	Operation string
	Cause     error
}

func (e *ResidencyError) Error() string { return "sessionstore: residency " + e.Operation + " failed" }
func (e *ResidencyError) Unwrap() error { return e.Cause }

// ResidencyAcquireCleanupError means acquisition was canceled after a provider
// grant arrived and its release also failed. No ownership grant is returned.
// Use errors.As to retain this cleanup obligation and retry Release; until a
// release succeeds the Store admission remains held and Close may time out.
// Unwrap exposes both the acquisition refusal and provider cleanup failure.
type ResidencyAcquireCleanupError struct {
	cause error
	grant *ResidencyGrant
}

func (e *ResidencyAcquireCleanupError) Error() string {
	return "sessionstore: canceled residency acquisition requires cleanup"
}
func (e *ResidencyAcquireCleanupError) Unwrap() error { return e.cause }

// Release retries cleanup only; this error exposes no ownership capability.
func (e *ResidencyAcquireCleanupError) Release(ctx context.Context) error {
	return e.grant.Release(ctx)
}

// ResidencyGrant owns only an orchestration residency lease. Epoch and Lost
// come directly from Storage; Lost is the provider's actual notification,
// never inferred from journal events, release errors or caller cancellation.
//
// Liveness is provider-dependent. In particular memstore provides neither TTL
// nor crash takeover. Residency loss does not fence a journal: a successor
// must independently acquire the journal grant before application.
//
// The grant is safe for concurrent use and holds a Store admission until a
// successful Release. Store shutdown attempts bounded cleanup once; failed
// cleanup retains the admission for an explicit retry. Providers must honor
// Release context cancellation. There is no background retry loop.
type ResidencyGrant struct {
	store            *Store
	lease            storage.Lease
	epoch            ResidencyEpoch
	mu               sync.Mutex
	releaseGate      chan struct{}
	released         bool
	releaseAdmission func()
	stopShutdown     func() bool
}

// AcquireResidency validates the actual immutable catalog binding and acquires
// a distinct residency lease. It never opens, reads or appends a journal.
// A protocol-only witness without a catalog winner is insufficient authority.
// The caller context bounds acquisition only; the returned grant lives until
// Release or Store shutdown. An error always returns a nil grant; see
// ResidencyAcquireCleanupError for a failed rollback's retry obligation.
func (s *Store) AcquireResidency(ctx context.Context, req AcquireResidencyRequest) (*ResidencyGrant, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return nil, err
	}
	lifeCtx, release, err := s.admitForeground(context.Background())
	if err != nil {
		return nil, err
	}
	opCtx, cancel := withCancelOn(ctx, lifeCtx)
	defer cancel()
	entry, err := s.readCatalogEntry(opCtx, scope, req.TenantID, req.SessionID)
	if err != nil {
		release()
		return nil, err
	}
	if entry.Record.Binding.ProtocolMode != ProtocolModeDisposition {
		release()
		return nil, catalogInvalid("binding.protocol_mode", nil)
	}
	if err := s.bindSessionScopeMode(opCtx, scope, ProtocolModeDisposition); err != nil {
		release()
		return nil, err
	}
	lease, err := s.backend.Leaser.Acquire(opCtx, scope.SessionNamespace+"/residency/lease")
	if err != nil {
		release()
		return nil, &ResidencyError{Operation: "acquire", Cause: err}
	}
	g := &ResidencyGrant{store: s, lease: lease, epoch: ResidencyEpoch(lease.Epoch()), releaseAdmission: release, releaseGate: make(chan struct{}, 1)}
	bindCancelHandle(lifeCtx, func() { _ = g.Release(context.Background()) }, func(stop func() bool) bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.stopShutdown = stop
		return g.released
	})
	var refusal error
	if lifeCtx.Err() != nil {
		refusal = &StoreClosedError{}
	} else if ctx.Err() != nil {
		refusal = ctx.Err()
	}
	if refusal != nil {
		if err := g.Release(context.Background()); err != nil {
			return nil, &ResidencyAcquireCleanupError{cause: errors.Join(refusal, err), grant: g}
		}
		return nil, refusal
	}
	return g, nil
}

// Epoch returns the provider's epoch in the residency domain.
func (g *ResidencyGrant) Epoch() ResidencyEpoch { return g.epoch }

// Lost returns the provider's loss signal, including successful release.
func (g *ResidencyGrant) Lost() <-chan struct{} { return g.lease.Lost() }

// Release returns this specific grant to the provider. Success is idempotent;
// failure remains retryable and does not pretend cleanup completed or free the
// Store admission. Each provider attempt is bounded by the caller context and
// the Store shutdown timeout. It cannot release a successor's grant.
func (g *ResidencyGrant) Release(ctx context.Context) error {
	g.mu.Lock()
	released := g.released
	g.mu.Unlock()
	if released {
		return nil
	}
	releaseCtx, cancel := context.WithTimeout(ctx, g.store.shutdownTimeout)
	defer cancel()
	select {
	case g.releaseGate <- struct{}{}:
		defer func() { <-g.releaseGate }()
	case <-releaseCtx.Done():
		return &ResidencyError{Operation: "release", Cause: releaseCtx.Err()}
	}
	// A concurrent successful attempt may have completed while we waited.
	g.mu.Lock()
	released = g.released
	g.mu.Unlock()
	if released {
		return nil
	}
	err := g.lease.Release(releaseCtx)
	if err != nil {
		return &ResidencyError{Operation: "release", Cause: err}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.released = true
	if g.stopShutdown != nil {
		g.stopShutdown()
	}
	g.releaseAdmission()
	return nil
}
