package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// DefaultJournalOverflowThresholdBytes is the encoded body size above which a
// journal body is uploaded as an immutable object and replaced in the record by
// a fixed-integrity reference. It is well below MaxInlineBodyBytes so an
// ordinary record stays small enough that a whole page of them fits inside one
// ledger read.
const DefaultJournalOverflowThresholdBytes = 64 << 10

// DefaultJournalPageBytes bounds the resolved bytes one journal page may
// return. It exceeds MaxInlineBodyBytes, so a page always makes progress: the
// largest single public body a writer can commit still fits in one page.
const DefaultJournalPageBytes = 1 << 20

// OpenJournalRequest names the session whose stream a writer wants to own.
type OpenJournalRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
}

// JournalWriter is one epoch-fenced single-writer grant over a session's
// journal. It is safe for concurrent use; every append is serialized.
//
// Ownership algorithm. OpenJournal acquires a lease grant, reads the tip
// exactly once, and appends an opening fence at precisely that tip stamped with
// the grant's epoch. If that CAS conflicts the grant is spent: OpenJournal
// releases the lease and returns a typed conflict, and the caller may acquire a
// fresh, strictly higher epoch and reopen. It deliberately does NOT refresh the
// tip and retry, because a retry loop lets a writer silently reorder itself
// behind records it never observed.
//
// After a successful open the writer tracks only its own committed sequence and
// CASes every later append on it. It never re-reads the tip, so a successor's
// opening fence permanently fails this writer at its next append. An append
// whose outcome could not be resolved is equally terminal: rather than re-read
// and rebase onto whatever is now durable, the writer latches the failure and
// refuses every later append.
type JournalWriter struct {
	store   *Store
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
	scope   sessionScope
	lease   storage.Lease
	epoch   uint64

	lifeCtx      context.Context
	release      func()
	stopShutdown func() bool

	mu      sync.Mutex
	tracked uint64
	failure error
	closed  bool
}

// OpenJournal takes single-writer ownership of a session's journal.
//
// It binds the session's collision witnesses, acquires the lease, reads the tip
// once, and commits the opening fence at that exact tip. Every failure after
// the lease is granted releases that grant before returning, so a caller that
// retries always does so under a fresh, strictly higher epoch.
//
// The returned writer holds a Store admission until Close, so Store.Close waits
// for it; Store shutdown also closes an abandoned writer so it can never wedge
// that wait.
func (s *Store) OpenJournal(ctx context.Context, req OpenJournalRequest) (*JournalWriter, error) {
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return nil, err
	}
	// The writer outlives this call, so its admission is bound to Store
	// shutdown alone; the open's own I/O is additionally bound by ctx.
	lifeCtx, release, err := s.admitForeground(context.Background())
	if err != nil {
		return nil, err
	}
	opCtx, cancel := withCancelOn(ctx, lifeCtx)
	defer cancel()

	if err := s.bindSessionScope(opCtx, scope); err != nil {
		release()
		return nil, err
	}
	lease, err := s.backend.Leaser.Acquire(opCtx, scope.LeaseName)
	if err != nil {
		release()
		var held *storage.LeaseHeldError
		if errors.As(err, &held) {
			return nil, &JournalError{Code: JournalErrorLeaseHeld, Field: "lease", Epoch: held.HolderEpoch, Cause: err}
		}
		return nil, journalErr(JournalErrorBackend, "lease", err)
	}
	fail := func(cause error) (*JournalWriter, error) {
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
		_ = lease.Release(releaseCtx)
		releaseCancel()
		release()
		return nil, cause
	}
	fence, err := EncodeEnvelope(Envelope{Kind: EnvelopeKindOpeningFence, LeaseEpoch: lease.Epoch()})
	if err != nil {
		return fail(err)
	}
	// Nothing may sit between this tip read and the fence CAS below: any
	// intervening work widens the window in which a predecessor advances the
	// tip under a value already cached here.
	tip, err := s.backend.Ledger.Tip(opCtx, scope.JournalName)
	if err != nil {
		return fail(journalErr(JournalErrorBackend, "tip", err))
	}
	if err := storage.AppendDefinite(opCtx, s.backend.Ledger, scope.JournalName, tip, fence); err != nil {
		return fail(classifyAppendError(err, "opening_fence"))
	}

	writer := &JournalWriter{
		store:   s,
		tenant:  req.TenantID,
		session: req.SessionID,
		scope:   scope,
		lease:   lease,
		epoch:   lease.Epoch(),
		lifeCtx: lifeCtx,
		release: release,
		tracked: tip + 1,
	}
	if s.beforeJournalBind != nil {
		s.beforeJournalBind(lifeCtx)
	}
	writer.bindShutdown(lifeCtx)
	// Decide the outcome here rather than leaving it to whichever goroutine
	// took the writer's mutex first. Registering against an already-cancelled
	// lifetime spawns the hook on its own goroutine, so without this check an
	// open overtaken by shutdown returns a live-looking writer roughly half the
	// time — the exact object this refusal exists to avoid.
	//
	// The lifetime is the whole fact. bindShutdown could only observe a closed
	// writer if its hook had already run, and the hook runs only once this
	// lifetime is done, so also consulting its result would state one condition
	// twice and let either copy rot unnoticed. Close is idempotent, so it is
	// safe whether or not the hook already ran.
	if lifeCtx.Err() != nil {
		_ = writer.Close(context.Background())
		return nil, &StoreClosedError{}
	}
	return writer, nil
}

// bindShutdown wires Store shutdown to close this writer. It reports nothing:
// the caller decides the open's outcome from the lifetime itself, and the only
// thing this could add — that the hook won the publish race — is implied by a
// cancelled lifetime.
func (w *JournalWriter) bindShutdown(lifeCtx context.Context) {
	bindCancelHandle(lifeCtx, func() { _ = w.Close(context.Background()) }, func(stop func() bool) bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.stopShutdown = stop
		return w.closed
	})
}

// Epoch returns the fencing epoch of this writer's lease grant.
func (w *JournalWriter) Epoch() uint64 { return w.epoch }

// Sequence returns the last journal sequence this writer committed, starting at
// its own opening fence. It is never refreshed from the provider.
func (w *JournalWriter) Sequence() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.tracked
}

// Append commits one record and returns its journal sequence.
//
// The writer owns the fields that carry ownership: an opening fence may not be
// appended by a caller at all, and an application prefix must leave LeaseEpoch
// zero for the writer to stamp.
//
// Stamping the epoch is ALL this file checks about a prefix. What a prefix may
// be appended for — which command, in what position relative to its effect, and
// never for a command claimed under a lower grant than this writer's — is the
// writer obligation stated in inbox_recovery.go, and it is unenforceable here:
// this writer has no view of the inbox. A prefix appended against it does not
// fail, it makes its command unrecoverable, so read that list before emitting
// one. A body above the overflow threshold is uploaded
// as an immutable object and verified before its reference is appended; if the
// append then fails the verified object is deliberately left behind as an
// orphan for garbage collection rather than deleted against a provider that has
// just proved unreliable.
//
// A conflicting, unresolved, or ownership-lost append ends the writer: the
// failure is latched and every later Append returns it. A definite backend
// failure leaves the tracked tip untouched and does not latch, so the caller may
// retry the same record.
func (w *JournalWriter) Append(ctx context.Context, env Envelope) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, journalErr(JournalErrorClosed, "writer", nil)
	}
	if w.failure != nil {
		return 0, w.failure
	}
	if err := w.stampWriterOwnedFields(&env); err != nil {
		return 0, err
	}
	if env.Kind == EnvelopeKindPublicEvent {
		// Hold an oversized public body to the canonical rule BEFORE uploading
		// it: the envelope validator only sees the body while it is inline, so
		// without this an unparseable body would become a durable object and
		// only then be rejected. This gate is reached exactly when the body is
		// about to be replaced by a reference, so it is not masked by the
		// validator that runs later inside EncodeEnvelope.
		if err := validatePublicBody(env.EventID, env.Public.Inline); err != nil {
			return 0, err
		}
		// A public body must remain resolvable inside one bounded page, so it
		// is capped at the largest body a record can carry inline even when it
		// is about to be offloaded. EncodeEnvelope enforces the same ceiling
		// only for a body that STAYS inline; the overflow path below would
		// otherwise carry an unbounded body past it.
		if len(env.Public.Inline) > MaxInlineBodyBytes {
			return 0, journalErr(JournalErrorTooLarge, "public_body", nil)
		}
	}
	if !w.leaseHeld() {
		return 0, w.latch(&JournalError{Code: JournalErrorLeaseLost, Field: "lease", Epoch: w.epoch})
	}

	opCtx, cancel := withCancelOn(ctx, w.lifeCtx)
	defer cancel()
	threshold := w.store.journalOverflowThreshold()
	if err := w.store.overflowBody(opCtx, w.tenant, w.session, &env.Public, ObjectKindJournalPublic, threshold); err != nil {
		return 0, err
	}
	if err := w.store.overflowBody(opCtx, w.tenant, w.session, &env.Runtime, ObjectKindJournalRuntime, threshold); err != nil {
		return 0, err
	}
	frame, err := EncodeEnvelope(env)
	if err != nil {
		return 0, err
	}
	if err := storage.AppendDefinite(opCtx, w.store.backend.Ledger, w.scope.JournalName, w.tracked, frame); err != nil {
		return 0, w.latch(classifyAppendError(err, "append"))
	}
	w.tracked++
	return w.tracked, nil
}

// Close releases the lease grant and the Store admission. It is idempotent and
// refuses every later append.
func (w *JournalWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.stopShutdown != nil {
		w.stopShutdown()
	}
	// The release round-trip gets its own bound rather than the caller's alone,
	// so shutdown-triggered close (whose context is already canceled) can still
	// hand the grant back instead of leaking it until expiry.
	releaseCtx, cancel := context.WithTimeout(ctx, w.store.shutdownTimeout)
	err := w.lease.Release(releaseCtx)
	cancel()
	w.release()
	if err != nil {
		return journalErr(JournalErrorBackend, "lease_release", err)
	}
	return nil
}

// stampWriterOwnedFields rejects the record fields a caller may not choose and
// fills in the ones the writer owns.
func (w *JournalWriter) stampWriterOwnedFields(env *Envelope) error {
	switch env.Kind {
	case EnvelopeKindOpeningFence:
		// Ownership records are minted by OpenJournal alone. A caller-authored
		// fence would advance the tip without any grant behind it.
		return journalErr(JournalErrorInvalid, "kind", nil)
	case EnvelopeKindApplicationPrefix:
		if env.LeaseEpoch != 0 {
			return journalErr(JournalErrorInvalid, "lease_epoch", nil)
		}
		env.LeaseEpoch = w.epoch
	}
	return nil
}

// latch records a terminal ownership failure so no later append can proceed. A
// definite backend failure is not terminal: it left the tracked tip untouched,
// so the same record may simply be retried.
func (w *JournalWriter) latch(err error) error {
	var journalError *JournalError
	if errors.As(err, &journalError) {
		switch journalError.Code {
		case JournalErrorFenced, JournalErrorUnknown, JournalErrorLeaseLost:
			w.failure = err
		}
	}
	return err
}

// leaseHeld is the fast-path ownership guard. The CAS fence is the hard
// backstop that catches a loss this guard races.
func (w *JournalWriter) leaseHeld() bool {
	select {
	case <-w.lease.Lost():
		return false
	default:
		return true
	}
}

// classifyAppendError maps a storage append outcome into the journal
// vocabulary. storage.AppendDefinite has already resolved an ambiguous ack by
// comparing the contested record against the exact bytes this writer offered,
// so a ConflictError here means a foreign record genuinely holds the sequence —
// unrelated bytes are never adopted as this writer's own.
//
// Storage's documented caveat carries over (storage v0.6.0 appenddefinite.go):
// two writers offering BYTE-IDENTICAL frames are indistinguishable to that
// comparison. This package does not widen the exposure. An opening fence is
// distinguished by its lease epoch, which the leaser makes strictly increasing
// per grant, and every other record kind carries a caller-chosen identity; two
// writers can collide only by offering the same identity and the same bytes,
// which is the case where either outcome is equivalent anyway.
func classifyAppendError(err error, field string) error {
	var conflict *storage.ConflictError
	if errors.As(err, &conflict) {
		return journalErr(JournalErrorFenced, field, err)
	}
	var ambiguous *storage.AmbiguousError
	if errors.As(err, &ambiguous) {
		return journalErr(JournalErrorUnknown, field, err)
	}
	// The contested record could not be read, so the append outcome is
	// genuinely unresolved rather than a lost race.
	var verify *storage.AppendVerifyError
	if errors.As(err, &verify) {
		return journalErr(JournalErrorUnknown, field, err)
	}
	return journalErr(JournalErrorBackend, field, err)
}

// overflowBody replaces an over-threshold inline body with a fixed-integrity
// reference to an immutable object. PutObject streams, verifies, persists, and
// re-reads the bytes before returning metadata, so the reference appended after
// this call always names an object that is already durable and verified.
func (s *Store) overflowBody(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	slot *BodySlot,
	kind ObjectKind,
	threshold int,
) error {
	if slot.Inline == nil || len(slot.Inline) <= threshold {
		return nil
	}
	body := slot.Inline
	digest := sha256.Sum256(body)
	metadata, err := s.PutObject(ctx, PutObjectRequest{
		TenantID:  tenant,
		SessionID: session,
		Kind:      kind,
		SizeBytes: uint64(len(body)),
		SHA256:    digest,
		Body:      bytes.NewReader(body),
	})
	if err != nil {
		return err
	}
	reference, err := BodyReferenceFromObjectMetadata(metadata)
	if err != nil {
		return err
	}
	slot.Inline = nil
	slot.Reference = &reference
	return nil
}

func (s *Store) journalOverflowThreshold() int {
	threshold := s.overflowThreshold
	if threshold <= 0 {
		threshold = DefaultJournalOverflowThresholdBytes
	}
	if threshold > MaxInlineBodyBytes {
		threshold = MaxInlineBodyBytes
	}
	return threshold
}
