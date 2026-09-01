package sessionstore_test

import (
	"context"
	"fmt"
	"log"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
)

// ExampleOpen is the compiled twin of the composition example in README.md. It
// exists so that snippet cannot go stale: it is the whole shape of a
// composition root — pick a provider, hand its complete storage.Composite to
// Open, and close the Store — and if the signature or the flow changes, this
// stops compiling.
//
// memstore is used because it is the in-process oracle Storage ships and it
// satisfies Open's bounded Blob reader lifecycle requirement. A product picks a
// durable provider here instead; nothing else in this function changes.
func ExampleOpen() {
	ctx := context.Background()

	store, err := sessionstore.Open(ctx, memstore.New())
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := store.Close(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	createdAt := time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC)
	if _, created, err := store.CreateCatalogEntry(ctx, sessionstore.CreateCatalogEntryRequest{
		TenantID:               "tenant-a",
		SessionID:              "session-a",
		AgentID:                "agent-a",
		RuntimeCompatibilityID: "runtime-v1",
		CreatedAt:              createdAt,
		LastActiveAt:           createdAt,
		State:                  sessionwire.SessionStateIdle,
		Residency:              sessionwire.SessionResidencyCold,
		DesiredPlacement:       sessionwire.HostPlacementPooled,
		IdempotencyKey:         "create-1",
	}); err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("created:", created)
	}

	page, err := store.ListSessions(ctx, sessionstore.ListSessionsRequest{TenantID: "tenant-a", Limit: 10})
	if err != nil {
		log.Fatal(err)
	}
	for _, summary := range page.Sessions {
		fmt.Println("session:", summary.SessionID, summary.State)
	}
	fmt.Println("unreadable skipped:", page.UnreadableSkipped)

	// Output:
	// created: true
	// session: session-a idle
	// unreadable skipped: 0
}
