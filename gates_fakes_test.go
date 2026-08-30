// Fakes the gate tests drive the OrderedIndex with. The catalog's recording and
// hostile providers live in catalog_fakes_test.go; what is here is the one thing
// they cannot express — a provider that fails a gate INTENT read while the
// catalog record beside it still reads normally.
package sessionstore

import (
	"context"
	"sync"

	"github.com/looprig/storage"
)

// intentFailingOrdered fails Get for gate intents only. A resolve reads the
// catalog record first and the intent second, so a provider that failed every
// Get would stop at the first one and could never reach the second — which is
// exactly the failure whose propagation needs proving.
type intentFailingOrdered struct {
	storage.OrderedIndex

	mu  sync.Mutex
	err error
}

// failIntentGets makes every later intent Get return err. Passing nil disarms
// it, so one test can prove both that the failure propagates and that a retry
// afterwards completes the resolve.
func (o *intentFailingOrdered) failIntentGets(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.err = err
}

func (o *intentFailingOrdered) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	o.mu.Lock()
	err := o.err
	o.mu.Unlock()
	if err != nil && id.Namespace == gateNamespace {
		return storage.OrderedRecord{}, err
	}
	return o.OrderedIndex.Get(ctx, id)
}

var _ storage.OrderedIndex = (*intentFailingOrdered)(nil)
