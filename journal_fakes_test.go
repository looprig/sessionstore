// Fakes the journal writer and reader tests drive the providers with. The blob
// fakes and the shared call log live in objects_fakes_test.go.
package sessionstore

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/looprig/storage"
)

// scriptedLedger wraps a real ledger so a test can log call order, run a side
// effect between Tip and the fence CAS, or substitute an append outcome.
type scriptedLedger struct {
	storage.Ledger
	log      *callLog
	onTip    func(name string)
	tipErr   error
	appendFn func(expected uint64, payload []byte) (bool, error)
	readFn   func(from uint64) (bool, storage.Cursor, error)
	// onCursor wraps the cursor a real read returned. readFn cannot serve: it
	// REPLACES the read, so a test wanting to observe the real one would have
	// to reissue it, and reissuing needs the ledger name this hook is not given.
	onCursor func(storage.Cursor) storage.Cursor
}

func (l *scriptedLedger) Tip(ctx context.Context, name string) (uint64, error) {
	if l.tipErr != nil {
		return 0, l.tipErr
	}
	tip, err := l.Ledger.Tip(ctx, name)
	if l.log != nil {
		l.log.add("tip")
	}
	if l.onTip != nil {
		l.onTip(name)
	}
	return tip, err
}

func (l *scriptedLedger) Append(ctx context.Context, name string, expected uint64, payload []byte) error {
	if l.log != nil {
		l.log.add("append")
	}
	if l.appendFn != nil {
		if handled, err := l.appendFn(expected, payload); handled {
			return err
		}
	}
	return l.Ledger.Append(ctx, name, expected, payload)
}

func (l *scriptedLedger) Read(ctx context.Context, name string, from uint64) (storage.Cursor, error) {
	if l.log != nil {
		l.log.add("read")
	}
	if l.readFn != nil {
		if handled, cur, err := l.readFn(from); handled {
			return cur, err
		}
	}
	cursor, err := l.Ledger.Read(ctx, name, from)
	if err != nil || l.onCursor == nil {
		return cursor, err
	}
	return l.onCursor(cursor), nil
}

// scriptedCursor observes and, where a test needs a provider that breaks its
// contract, rewrites what a real cursor hands back.
//
// next counts Next calls, which is the only way to observe where a walk STOPPED
// as opposed to what it concluded: a walk that overruns its bound reaches the
// same answer by asking for a record it did not need, and nothing about the
// answer says so.
//
// rewrite produces the cursor a CONFORMING ledger never would. Sequences are
// dense and bounded by the tip observed at Read, so a reader's own bound is
// unreachable while the provider behaves; a reader that trusted the cursor
// instead of its own bound would only be wrong when the provider was, which is
// exactly when it matters and exactly what cannot otherwise be driven.
type scriptedCursor struct {
	storage.Cursor
	next    *int64
	rewrite func(storage.Record) storage.Record
	fail    error
}

func (c *scriptedCursor) Next(ctx context.Context) (storage.Record, error) {
	if c.next != nil {
		atomic.AddInt64(c.next, 1)
	}
	if c.fail != nil {
		return storage.Record{}, c.fail
	}
	record, err := c.Cursor.Next(ctx)
	if err != nil || c.rewrite == nil {
		return record, err
	}
	return c.rewrite(record), nil
}

// permissiveLeaser grants an independent, strictly increasing epoch on every
// Acquire and never refuses. It exists so a test can hold two simultaneously
// live writers and exercise the CAS fence itself rather than the lease guard
// that normally hides it.
type permissiveLeaser struct {
	mu     sync.Mutex
	epochs map[string]uint64
}

func newPermissiveLeaser() *permissiveLeaser {
	return &permissiveLeaser{epochs: make(map[string]uint64)}
}

func (l *permissiveLeaser) Acquire(_ context.Context, name string) (storage.Lease, error) {
	if err := storage.ValidateName(name); err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.epochs[name]++
	return &fakeLease{epoch: l.epochs[name], lost: make(chan struct{})}, nil
}

type fakeLease struct {
	epoch      uint64
	lost       chan struct{}
	once       sync.Once
	releases   int32
	releaseErr error
	mu         sync.Mutex
}

func (l *fakeLease) Epoch() uint64         { return l.epoch }
func (l *fakeLease) Lost() <-chan struct{} { return l.lost }
func (l *fakeLease) Release(context.Context) error {
	l.mu.Lock()
	l.releases++
	l.mu.Unlock()
	l.once.Do(func() { close(l.lost) })
	return l.releaseErr
}

func (l *fakeLease) releaseCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int(l.releases)
}

// recordingLeaser hands out fakeLease values a test keeps hold of, so a failed
// open can be checked for the release that frees the grant.
type recordingLeaser struct {
	mu     sync.Mutex
	epochs map[string]uint64
	issued []*fakeLease
	err    error
}

func newRecordingLeaser() *recordingLeaser {
	return &recordingLeaser{epochs: make(map[string]uint64)}
}

func (l *recordingLeaser) Acquire(_ context.Context, name string) (storage.Lease, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return nil, l.err
	}
	l.epochs[name]++
	lease := &fakeLease{epoch: l.epochs[name], lost: make(chan struct{})}
	l.issued = append(l.issued, lease)
	return lease, nil
}

func (l *recordingLeaser) last() *fakeLease {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.issued) == 0 {
		return nil
	}
	return l.issued[len(l.issued)-1]
}

// revivableLease is a grant whose ownership can be lost and then made to look
// live again. Real leases never do that, but it is the only way to observe the
// difference between a writer that re-checks its lease on every append and one
// that has permanently latched an ownership loss.
type revivableLease struct {
	epoch uint64

	mu   sync.Mutex
	lost chan struct{}
}

func newRevivableLease(epoch uint64) *revivableLease {
	return &revivableLease{epoch: epoch, lost: make(chan struct{})}
}

func (l *revivableLease) Epoch() uint64 { return l.epoch }

func (l *revivableLease) Lost() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lost
}

func (l *revivableLease) Release(context.Context) error {
	l.lose()
	return nil
}

func (l *revivableLease) lose() {
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case <-l.lost:
	default:
		close(l.lost)
	}
}

// revive hands out a fresh, open loss channel, so a writer that consults its
// lease again sees ownership restored.
func (l *revivableLease) revive() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lost = make(chan struct{})
}

// staticLeaser always grants the same lease.
type staticLeaser struct{ lease storage.Lease }

func (l staticLeaser) Acquire(_ context.Context, name string) (storage.Lease, error) {
	if err := storage.ValidateName(name); err != nil {
		return nil, err
	}
	return l.lease, nil
}

// frameKind reports the envelope kind of an encoded frame, so a scripted ledger
// can single out the opening fence from the records that follow it.
func frameKind(payload []byte) EnvelopeKind {
	env, err := DecodeEnvelope(payload)
	if err != nil {
		return 0
	}
	return env.Kind
}
