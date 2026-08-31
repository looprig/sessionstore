// Fakes the catalog tests drive the OrderedIndex and KV with. The blob fakes
// and the shared call log live in objects_fakes_test.go; the journal's own
// fakes live in journal_fakes_test.go.
package sessionstore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

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
}

// The three list methods below record only the op. They look redundant next to
// listAuditOrdered, and deleting them still builds and passes — but they are
// what makes "a direct get or update performs no listing" a live assertion
// rather than a vacuous one: without an override the call reaches the embedded
// provider unrecorded, and a regression that started listing would go unseen.
//
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
	o.add(orderedCall{op: "list_ordered"})
	return o.OrderedIndex.ListOrdered(ctx, namespace, orderingScope, afterOrder, limit)
}

func (o *recordingOrdered) ListRanked(ctx context.Context, namespace, rankingScope string, after storage.RankedCursor, limit int) (storage.RankedPage, error) {
	o.add(orderedCall{op: "list_ranked"})
	return o.OrderedIndex.ListRanked(ctx, namespace, rankingScope, after, limit)
}

func (o *recordingOrdered) ListDue(ctx context.Context, namespace string, dueAtOrBefore int64, after storage.DueCursor, limit int) (storage.DuePage, error) {
	o.add(orderedCall{op: "list_due"})
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

// hostileOrdered is composed into the backend at Open and starts inert, then a
// test arms it once its fixture is in place. Arming beats swapping Store.backend
// after Open: a test that reaches into store internals is testing a state the
// production lifecycle never produces.
//
// It is the package's ONE non-conforming provider, pointed at whichever method
// a test needs: reads, the ranked and due views, and creates. A second fake for
// creates would have been a second name for that one idea.
type hostileOrdered struct {
	storage.OrderedIndex

	mu           sync.Mutex
	getErr       error
	getNamespace string
	rewrite      func([]byte) []byte
	ranked       func(storage.RankedPage, error) (storage.RankedPage, error)
	due          func(storage.DuePage, error) (storage.DuePage, error)
	createErr    error
	createRefile func(storage.OrderedRecord) storage.OrderedRecord
}

// failCreates makes every later Create return err.
func (o *hostileOrdered) failCreates(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.createErr = err
}

// refileCreates rewrites the record every later Create RETURNS, leaving what
// was stored alone. That is exactly the shape of the fault it stands for — a
// provider whose reply does not describe what it filed — and it is the only way
// to reach the filing checks, because a conforming provider's reply always
// agrees with the request.
func (o *hostileOrdered) refileCreates(rewrite func(storage.OrderedRecord) storage.OrderedRecord) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.createRefile = rewrite
}

func (o *hostileOrdered) Create(
	ctx context.Context,
	id storage.OrderedID,
	rankingScope string,
	value []byte,
	rank storage.Rank,
	due storage.Due,
) (storage.OrderedRecord, bool, error) {
	o.mu.Lock()
	createErr, refile := o.createErr, o.createRefile
	o.mu.Unlock()
	if createErr != nil {
		return storage.OrderedRecord{}, false, createErr
	}
	record, created, err := o.OrderedIndex.Create(ctx, id, rankingScope, value, rank, due)
	if err != nil || refile == nil {
		return record, created, err
	}
	return refile(record), created, nil
}

// failGets makes every later Get return err.
func (o *hostileOrdered) failGets(err error) { o.failGetsIn("", err) }

// failGetsIn makes every later Get in one namespace return err, leaving the
// other namespaces working. An empty namespace fails all of them.
//
// The namespace is a parameter rather than a second fake because an operation
// that reads two namespaces in order — a resolve reads the catalog record and
// then the gate intent — cannot have its SECOND read exercised by a provider
// that fails the first. Passing nil disarms it.
func (o *hostileOrdered) failGetsIn(namespace string, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.getErr, o.getNamespace = err, namespace
}

// corruptGets rewrites the stored value every later Get returns, so a read can
// be held to the identity the record itself claims rather than to the identity
// the caller happened to ask for.
func (o *hostileOrdered) corruptGets(rewrite func([]byte) []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.rewrite = rewrite
}

// answerRanked lets a test replace what the provider returns from ListRanked,
// which is the only way to present SessionStore with a page a conforming
// provider would never produce.
func (o *hostileOrdered) answerRanked(answer func(storage.RankedPage, error) (storage.RankedPage, error)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ranked = answer
}

// answerDue is answerRanked's counterpart for the due view, and it is the only
// way to present SessionStore with a due page a conforming provider never
// produces — a tombstoned row, for instance, which the ordered index promises
// to exclude.
func (o *hostileOrdered) answerDue(answer func(storage.DuePage, error) (storage.DuePage, error)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.due = answer
}

func (o *hostileOrdered) ListDue(ctx context.Context, namespace string, dueAtOrBefore int64, after storage.DueCursor, limit int) (storage.DuePage, error) {
	page, err := o.OrderedIndex.ListDue(ctx, namespace, dueAtOrBefore, after, limit)
	o.mu.Lock()
	answer := o.due
	o.mu.Unlock()
	if answer == nil {
		return page, err
	}
	return answer(page, err)
}

func (o *hostileOrdered) ListRanked(ctx context.Context, namespace, rankingScope string, after storage.RankedCursor, limit int) (storage.RankedPage, error) {
	page, err := o.OrderedIndex.ListRanked(ctx, namespace, rankingScope, after, limit)
	o.mu.Lock()
	answer := o.ranked
	o.mu.Unlock()
	if answer == nil {
		return page, err
	}
	return answer(page, err)
}

func (o *hostileOrdered) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	o.mu.Lock()
	getErr, namespace, rewrite := o.getErr, o.getNamespace, o.rewrite
	o.mu.Unlock()
	if getErr != nil && (namespace == "" || namespace == id.Namespace) {
		return storage.OrderedRecord{}, getErr
	}
	record, err := o.OrderedIndex.Get(ctx, id)
	if err != nil || rewrite == nil {
		return record, err
	}
	record.Value = rewrite(record.Value)
	return record, nil
}

var (
	_ storage.OrderedIndex = (*recordingOrdered)(nil)
	_ storage.OrderedIndex = (*hostileOrdered)(nil)
)

// rankedCall is one recorded ListRanked query. Its ranking scope and limit are
// what distinguish a tenant-scoped provider query from a wider scan the caller
// narrows afterwards, and records is what a test compares the returned page
// against to prove nothing was dropped after the provider answered.
type rankedCall struct {
	namespace    string
	rankingScope string
	after        storage.RankedCursor
	limit        int
	records      int
}

// listAuditOrdered fails the test if a catalog listing is anything other than
// one tenant-scoped ListRanked query. It starts inert so a fixture can be
// created through the ordinary write paths, and a test arms it with the tenant
// scope the listing under test must ask the provider for.
type listAuditOrdered struct {
	storage.OrderedIndex

	t *testing.T

	mu        sync.Mutex
	armed     bool
	wantScope string
	ranked    []rankedCall
}

// arm makes every later call other than a ListRanked in scope a test failure.
func (o *listAuditOrdered) arm(scope string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.armed, o.wantScope = true, scope
}

func (o *listAuditOrdered) refuse(op string) {
	o.mu.Lock()
	armed := o.armed
	o.mu.Unlock()
	if armed {
		o.t.Errorf("a listing reached the provider through %s; it must use ListRanked only", op)
	}
}

func (o *listAuditOrdered) rankedCalls() []rankedCall {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]rankedCall(nil), o.ranked...)
}

func (o *listAuditOrdered) ListRanked(ctx context.Context, namespace, rankingScope string, after storage.RankedCursor, limit int) (storage.RankedPage, error) {
	page, err := o.OrderedIndex.ListRanked(ctx, namespace, rankingScope, after, limit)
	o.mu.Lock()
	armed, wantScope := o.armed, o.wantScope
	o.ranked = append(o.ranked, rankedCall{
		namespace:    namespace,
		rankingScope: rankingScope,
		after:        after,
		limit:        limit,
		records:      len(page.Records),
	})
	o.mu.Unlock()
	if armed && rankingScope != wantScope {
		o.t.Errorf("ListRanked ranking scope = %q, want %q: the tenant restriction must be part of the provider query, not a filter applied to its page",
			rankingScope, wantScope)
	}
	return page, err
}

func (o *listAuditOrdered) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	o.refuse("Get")
	return o.OrderedIndex.Get(ctx, id)
}

func (o *listAuditOrdered) ListOrdered(ctx context.Context, namespace, orderingScope string, afterOrder uint64, limit int) (storage.OrderedPage, error) {
	o.refuse("ListOrdered")
	return o.OrderedIndex.ListOrdered(ctx, namespace, orderingScope, afterOrder, limit)
}

func (o *listAuditOrdered) ListDue(ctx context.Context, namespace string, dueAtOrBefore int64, after storage.DueCursor, limit int) (storage.DuePage, error) {
	o.refuse("ListDue")
	return o.OrderedIndex.ListDue(ctx, namespace, dueAtOrBefore, after, limit)
}

// permissiveRankedOrdered is a deliberately non-conforming provider: its ranked
// cursor is not bound to the query that issued it, so it accepts any token and
// simply restarts the scan.
//
// It exists so SessionStore's own cursor binding is observable. Against a
// conforming provider the provider's rejection and SessionStore's arrive as the
// same typed failure, so a test written over one could not tell which guard
// fired and would keep passing after SessionStore's was deleted.
type permissiveRankedOrdered struct{ storage.OrderedIndex }

func (o permissiveRankedOrdered) ListRanked(ctx context.Context, namespace, rankingScope string, after storage.RankedCursor, limit int) (storage.RankedPage, error) {
	page, err := o.OrderedIndex.ListRanked(ctx, namespace, rankingScope, after, limit)
	if err != nil && errors.As(err, new(*storage.InvalidOrderedCursorError)) {
		// Issue cursors as usual, but never refuse one: this is the provider
		// that leaves the binding entirely to its caller.
		return o.OrderedIndex.ListRanked(ctx, namespace, rankingScope, "", limit)
	}
	return page, err
}

var (
	_ storage.OrderedIndex = (*listAuditOrdered)(nil)
	_ storage.OrderedIndex = permissiveRankedOrdered{}
)
