// Fakes the catalog tests drive the OrderedIndex and KV with. The blob fakes
// and the shared call log live in objects_fakes_test.go; the journal's own
// fakes live in journal_fakes_test.go.
package sessionstore

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/looprig/storage"
)

// orderedCall is one recorded OrderedIndex mutation or read. It keeps the
// scope, rank, and due arguments a catalog test needs to prove SessionStore
// asked the provider for a tenant-scoped, rank-ordered record rather than
// sorting or filtering afterwards.
type orderedCall struct {
	op               string
	id               storage.OrderedID
	rankingScope     string
	rank             storage.Rank
	due              storage.Due
	expectedRevision uint64
	namespace        string
	limit            int
}

// recordingOrdered logs every OrderedIndex call and can run a side effect
// immediately before an Update reaches the provider, which is the only place a
// test can interleave two compare-and-swap writers deterministically.
type recordingOrdered struct {
	storage.OrderedIndex

	beforeUpdate func()

	mu    sync.Mutex
	calls []orderedCall
}

func (o *recordingOrdered) add(call orderedCall) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, call)
}

func (o *recordingOrdered) snapshot() []orderedCall {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]orderedCall(nil), o.calls...)
}

func (o *recordingOrdered) countOf(op string) int {
	n := 0
	for _, call := range o.snapshot() {
		if call.op == op {
			n++
		}
	}
	return n
}

func (o *recordingOrdered) lastOf(op string) (orderedCall, bool) {
	calls := o.snapshot()
	for i := len(calls) - 1; i >= 0; i-- {
		if calls[i].op == op {
			return calls[i], true
		}
	}
	return orderedCall{}, false
}

func (o *recordingOrdered) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	o.add(orderedCall{op: "get", id: id})
	return o.OrderedIndex.Get(ctx, id)
}

func (o *recordingOrdered) Create(ctx context.Context, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, bool, error) {
	o.add(orderedCall{op: "create", id: id, rankingScope: rankingScope, rank: rank, due: due})
	return o.OrderedIndex.Create(ctx, id, rankingScope, value, rank, due)
}

func (o *recordingOrdered) Update(ctx context.Context, id storage.OrderedID, expectedRevision uint64, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, error) {
	o.add(orderedCall{op: "update", id: id, rank: rank, due: due, expectedRevision: expectedRevision})
	if o.beforeUpdate != nil {
		o.beforeUpdate()
	}
	return o.OrderedIndex.Update(ctx, id, expectedRevision, value, rank, due)
}

func (o *recordingOrdered) Delete(ctx context.Context, id storage.OrderedID, expectedRevision uint64) (storage.OrderedRecord, error) {
	o.add(orderedCall{op: "delete", id: id, expectedRevision: expectedRevision})
	return o.OrderedIndex.Delete(ctx, id, expectedRevision)
}

func (o *recordingOrdered) ListOrdered(ctx context.Context, namespace, orderingScope string, afterOrder uint64, limit int) (storage.OrderedPage, error) {
	o.add(orderedCall{op: "list_ordered", namespace: namespace, limit: limit})
	return o.OrderedIndex.ListOrdered(ctx, namespace, orderingScope, afterOrder, limit)
}

func (o *recordingOrdered) ListRanked(ctx context.Context, namespace, rankingScope string, after storage.RankedCursor, limit int) (storage.RankedPage, error) {
	o.add(orderedCall{op: "list_ranked", namespace: namespace, rankingScope: rankingScope, limit: limit})
	return o.OrderedIndex.ListRanked(ctx, namespace, rankingScope, after, limit)
}

func (o *recordingOrdered) ListDue(ctx context.Context, namespace string, dueAtOrBefore int64, after storage.DueCursor, limit int) (storage.DuePage, error) {
	o.add(orderedCall{op: "list_due", namespace: namespace, limit: limit})
	return o.OrderedIndex.ListDue(ctx, namespace, dueAtOrBefore, after, limit)
}

// keysCountingKV counts only KV.Keys. The catalog legitimately reads KV for
// collision witnesses, so counting every call would not distinguish that from
// the prefix scan the catalog must never perform.
type keysCountingKV struct {
	storage.KV
	keys atomic.Int32
}

func (k *keysCountingKV) Keys(ctx context.Context, prefix string) ([]string, error) {
	k.keys.Add(1)
	return k.KV.Keys(ctx, prefix)
}

// failingOrdered substitutes a provider outcome for one operation so the
// catalog's error classification can be exercised without a live backend.
type failingOrdered struct {
	storage.OrderedIndex
	getErr    error
	createErr error
	updateErr error
}

func (o *failingOrdered) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	if o.getErr != nil {
		return storage.OrderedRecord{}, o.getErr
	}
	return o.OrderedIndex.Get(ctx, id)
}

func (o *failingOrdered) Create(ctx context.Context, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, bool, error) {
	if o.createErr != nil {
		return storage.OrderedRecord{}, false, o.createErr
	}
	return o.OrderedIndex.Create(ctx, id, rankingScope, value, rank, due)
}

func (o *failingOrdered) Update(ctx context.Context, id storage.OrderedID, expectedRevision uint64, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, error) {
	if o.updateErr != nil {
		return storage.OrderedRecord{}, o.updateErr
	}
	return o.OrderedIndex.Update(ctx, id, expectedRevision, value, rank, due)
}

// corruptingOrdered rewrites the stored value a Get returns, so a read can be
// held to the identity the record itself claims rather than to the identity the
// caller happened to ask for.
type corruptingOrdered struct {
	storage.OrderedIndex
	rewrite func([]byte) []byte
}

func (o *corruptingOrdered) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	record, err := o.OrderedIndex.Get(ctx, id)
	if err != nil || o.rewrite == nil {
		return record, err
	}
	record.Value = o.rewrite(record.Value)
	return record, nil
}

var (
	_ storage.OrderedIndex = (*recordingOrdered)(nil)
	_ storage.OrderedIndex = (*failingOrdered)(nil)
	_ storage.OrderedIndex = (*corruptingOrdered)(nil)
)
