// Fakes the inbox tests drive the providers with. The non-conforming ordered
// index they share with the catalog and gate tests is hostileOrdered, in
// catalog_fakes_test.go: a provider that answers with what a conforming one
// never would is ONE idea, and pointing a second name at Create would have
// been a second spelling of it. What lives here is the one capability those
// fakes do not have — a log that spans two primitives.
package sessionstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"sync"

	"github.com/looprig/storage"
)

// writeWatch records every byte string this package hands a provider, so a
// test can ask where a private payload ended up rather than assuming.
type writeWatch struct {
	mu     sync.Mutex
	writes []watchedWrite
}

type watchedWrite struct {
	primitive string
	value     []byte
}

func (w *writeWatch) add(primitive string, value []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes = append(w.writes, watchedWrite{primitive: primitive, value: bytes.Clone(value)})
}

// carrying returns every write that carries needle in any spelling this
// package can write it in. Both are checked rather than only the one the inbox
// happens to use, because the question is where the payload ENDED UP, and a
// leak into a record that spells bytes differently is exactly the leak this
// would otherwise miss.
func (w *writeWatch) carrying(needle []byte) []watchedWrite {
	w.mu.Lock()
	defer w.mu.Unlock()
	encoded := []byte(base64.StdEncoding.EncodeToString(needle))
	var found []watchedWrite
	for _, write := range w.writes {
		if bytes.Contains(write.value, needle) || bytes.Contains(write.value, encoded) {
			found = append(found, write)
		}
	}
	return found
}

type watchingOrdered struct {
	storage.OrderedIndex
	watch *writeWatch
}

func (o *watchingOrdered) Create(
	ctx context.Context,
	id storage.OrderedID,
	rankingScope string,
	value []byte,
	rank storage.Rank,
	due storage.Due,
) (storage.OrderedRecord, bool, error) {
	o.watch.add("ordered:"+id.Namespace, value)
	return o.OrderedIndex.Create(ctx, id, rankingScope, value, rank, due)
}

func (o *watchingOrdered) Update(
	ctx context.Context,
	id storage.OrderedID,
	expectedRevision uint64,
	value []byte,
	rank storage.Rank,
	due storage.Due,
) (storage.OrderedRecord, error) {
	o.watch.add("ordered:"+id.Namespace, value)
	return o.OrderedIndex.Update(ctx, id, expectedRevision, value, rank, due)
}

type watchingKV struct {
	storage.KV
	watch *writeWatch
}

func (k *watchingKV) Put(ctx context.Context, key string, expectedRev uint64, val []byte) (uint64, error) {
	k.watch.add("kv", val)
	return k.KV.Put(ctx, key, expectedRev, val)
}

var (
	_ storage.OrderedIndex = (*watchingOrdered)(nil)
	_ storage.KV           = (*watchingKV)(nil)
)
