package sessionstore

import (
	"context"
	"fmt"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func TestPublicTailBoundsSequenceSpan(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		count, limit, defaultLimit int
		private                    bool
		wantFrom                   uint64
	}{
		{"empty", 0, 3, 3, false, 0},
		{"short", 2, 4, 3, false, 1},
		{"exact", 3, 3, 3, false, 1},
		{"public_tail", 8, 3, 3, false, 6},
		{"private_tail", 8, 3, 3, true, 6},
		{"default", 8, 0, 2, false, 7},
		{"maximum", 1002, storage.MaxOrderedPageLimit, 3, true, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := memstore.New()
			ledger := &scriptedLedger{Ledger: backend.Ledger}
			backend.Ledger = ledger
			store := openJournalStore(t, backend)
			store.limits.MaxPageSize = tc.defaultLimit
			if tc.count == 0 {
				scope, err := store.deriveSessionScope(testTenant, testSession)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.bindSessionScope(context.Background(), scope); err != nil {
					t.Fatal(err)
				}
			}
			if tc.count > 0 {
				writer := openTestJournal(t, store)
				for i := 1; i < tc.count; i++ {
					env := publicEvent(fmt.Sprintf("event-%d", i), `{"n":1}`)
					if tc.private {
						env = runtimeControl(fmt.Sprintf("private-%d", i), `{"secret":true}`)
					}
					appendOrFail(t, writer, env)
				}
			}
			var from uint64
			var next int64
			tips := 0
			ledger.onTip = func(string) { tips++ }
			ledger.readFn = func(start uint64) (bool, storage.Cursor, error) { from = start; return false, nil, nil }
			ledger.onCursor = func(cursor storage.Cursor) storage.Cursor { return &scriptedCursor{Cursor: cursor, next: &next} }
			page := readPublic(t, store, ReadPublicJournalRequest{Tail: true, Limit: tc.limit})
			if tips != 1 || from != tc.wantFrom {
				t.Fatalf("tips/start = %d/%d, want 1/%d", tips, from, tc.wantFrom)
			}
			wantReads := int64(0)
			if tc.wantFrom != 0 {
				wantReads = int64(tc.count) - int64(tc.wantFrom) + 1
			}
			if next != wantReads {
				t.Fatalf("cursor calls = %d, want bounded span %d", next, wantReads)
			}
			if page.CapturedTip != uint64(tc.count) || page.CoveredThrough != uint64(tc.count) || page.NextCursor != "" {
				t.Fatalf("page = %+v", page)
			}
			if tc.private && len(page.Events) != 0 {
				t.Fatalf("private bytes leaked: %+v", page.Events)
			}
			if !tc.private {
				wantEvents := int(wantReads)
				if tc.wantFrom == 1 {
					wantEvents--
				}
				if len(page.Events) != wantEvents {
					t.Fatalf("events = %d, want %d", len(page.Events), wantEvents)
				}
			}
		})
	}
}

func TestPublicTailRejectsConflictsBeforeProviderIO(t *testing.T) {
	for _, tc := range []struct {
		name   string
		from   uint64
		cursor sessionwire.Cursor
		limit  int
		field  string
	}{
		{"from", 1, "", 3, "tail"},
		{"cursor", 0, "not-a-cursor", 3, "tail"},
		{"negative_limit", 0, "", -1, "limit"},
		{"oversized_limit", 0, "", storage.MaxOrderedPageLimit + 1, "limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A zero Store cannot perform provider I/O, so crossing validation
			// would panic rather than manufacture the expected refusal.
			store := &Store{}
			_, err := store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{TenantID: testTenant, SessionID: testSession, Tail: true, FromSeq: tc.from, Cursor: tc.cursor, Limit: tc.limit})
			if got := requireJournalCode(t, err, JournalErrorInvalid); got.Field != tc.field {
				t.Fatalf("field = %s, want %s", got.Field, tc.field)
			}
		})
	}
}

func TestPublicTailExcludesAppendsAfterCapturedTip(t *testing.T) {
	backend := memstore.New()
	ledger := &scriptedLedger{Ledger: backend.Ledger}
	backend.Ledger = ledger
	store := openJournalStore(t, backend)
	writer := mixedSession(t, store)
	tips := 0
	ledger.onTip = func(string) {
		tips++
		if tips == 1 {
			for i := 0; i < 1000; i++ {
				appendOrFail(t, writer, runtimeControl(fmt.Sprintf("late-%d", i), `{"private":true}`))
			}
		}
	}
	var next int64
	ledger.onCursor = func(cursor storage.Cursor) storage.Cursor { return &scriptedCursor{Cursor: cursor, next: &next} }
	page := readPublic(t, store, ReadPublicJournalRequest{Tail: true, Limit: 3})
	if tips != 1 || next != 3 || page.CapturedTip != 5 || page.CoveredThrough != 5 || strings.Join(eventIDs(page), ",") != "event-2" || page.NextCursor != "" {
		t.Fatalf("tips=%d reads=%d page=%+v", tips, next, page)
	}
}

func TestPublicTailByteBudgetCursorKeepsOriginalTip(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	store.maxPageBytes = 8
	writer := openTestJournal(t, store)
	for i := 0; i < 5; i++ {
		appendOrFail(t, writer, publicEvent(fmt.Sprintf("event-%d", i), `{"body":"larger than budget"}`))
	}
	page := readPublic(t, store, ReadPublicJournalRequest{Tail: true, Limit: 3})
	if page.CapturedTip != 6 || page.CoveredThrough != 4 || page.NextCursor == "" {
		t.Fatalf("first page = %+v", page)
	}
	appendOrFail(t, writer, publicEvent("late", `{"n":1}`))
	got := eventIDs(page)
	for i := 0; page.NextCursor != ""; i++ {
		if i >= 3 {
			t.Fatal("tail cursor did not terminate")
		}
		page = readPublic(t, store, ReadPublicJournalRequest{Cursor: page.NextCursor})
		if page.CapturedTip != 6 {
			t.Fatalf("tip moved: %+v", page)
		}
		got = append(got, eventIDs(page)...)
	}
	if strings.Join(got, ",") != "event-2,event-3,event-4" || page.CoveredThrough != 6 {
		t.Fatalf("events=%v final=%+v", got, page)
	}
}
