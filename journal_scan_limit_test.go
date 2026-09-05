package sessionstore

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func TestPublicScanLimitCountsPrivateRecordsWithoutLookahead(t *testing.T) {
	backend := memstore.New()
	ledger := &scriptedLedger{Ledger: backend.Ledger}
	backend.Ledger = ledger
	store := openJournalStore(t, backend)
	writer := openTestJournal(t, store)
	for i := 0; i < 10000; i++ {
		appendOrFail(t, writer, runtimeControl(fmt.Sprintf("private-%d", i), `{"private":true}`))
	}
	var next int64
	ledger.onCursor = func(cursor storage.Cursor) storage.Cursor { return &scriptedCursor{Cursor: cursor, next: &next} }
	page := readPublic(t, store, ReadPublicJournalRequest{FromSeq: 1, Limit: 1, ScanLimit: 1})
	if next != 1 || len(page.Events) != 0 || page.CoveredThrough != 1 || page.CapturedTip != 10001 || page.NextCursor == "" {
		t.Fatalf("advances=%d page=%+v; want one examined position and continuation", next, page)
	}
	// The omitted budget is intentionally the released behavior, including
	// covering private records after exhausting the public event limit.
	next = 0
	page = readPublic(t, store, ReadPublicJournalRequest{FromSeq: 1, Limit: 1})
	if next != 10001 || page.CoveredThrough != 10001 || page.NextCursor != "" {
		t.Fatalf("legacy advances=%d page=%+v", next, page)
	}
}

func TestPublicScanLimitContinuesMixedRecordsAtOriginalTip(t *testing.T) {
	backend := memstore.New()
	ledger := &scriptedLedger{Ledger: backend.Ledger}
	backend.Ledger = ledger
	store := openJournalStore(t, backend)
	writer := mixedSession(t, store)
	var next int64
	ledger.onCursor = func(cursor storage.Cursor) storage.Cursor { return &scriptedCursor{Cursor: cursor, next: &next} }
	first := readPublic(t, store, ReadPublicJournalRequest{Limit: 1, ScanLimit: 3})
	if next != 3 || first.CoveredThrough != 3 || first.CapturedTip != 5 || strings.Join(eventIDs(first), ",") != "event-1" || first.NextCursor == "" {
		t.Fatalf("first advances=%d page=%+v", next, first)
	}
	appendOrFail(t, writer, publicEvent("late", `{"late":true}`))
	next = 0
	last := readPublic(t, store, ReadPublicJournalRequest{Cursor: first.NextCursor, Limit: 1, ScanLimit: 3})
	if next != 2 || last.CoveredThrough != 5 || last.CapturedTip != 5 || strings.Join(eventIDs(last), ",") != "event-2" || last.NextCursor != "" {
		t.Fatalf("last advances=%d page=%+v", next, last)
	}
}

func TestPublicScanLimitComposesWithEventAndByteLimits(t *testing.T) {
	for _, tc := range []struct {
		name                string
		events, scan, bytes int
		wantNext            int64
	}{
		{"scan_first", 3, 1, 1024, 1},
		{"event_first", 1, 3, 1024, 2},
		{"byte_first", 3, 3, 8, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := memstore.New()
			ledger := &scriptedLedger{Ledger: backend.Ledger}
			backend.Ledger = ledger
			store := openJournalStore(t, backend)
			store.maxPageBytes = tc.bytes
			writer := openTestJournal(t, store)
			for i := 0; i < 3; i++ {
				appendOrFail(t, writer, publicEvent(fmt.Sprintf("event-%d", i), `{"body":"large enough"}`))
			}
			var next int64
			ledger.onCursor = func(cursor storage.Cursor) storage.Cursor { return &scriptedCursor{Cursor: cursor, next: &next} }
			page := readPublic(t, store, ReadPublicJournalRequest{FromSeq: 2, Limit: tc.events, ScanLimit: tc.scan})
			if next != tc.wantNext || page.CoveredThrough != 2 || len(page.Events) != 1 || page.NextCursor == "" {
				t.Fatalf("advances=%d page=%+v", next, page)
			}
			got := eventIDs(page)
			for i := 0; page.NextCursor != ""; i++ {
				if i >= 3 {
					t.Fatal("continuation failed to terminate")
				}
				page = readPublic(t, store, ReadPublicJournalRequest{Cursor: page.NextCursor, Limit: tc.events, ScanLimit: tc.scan})
				got = append(got, eventIDs(page)...)
			}
			if strings.Join(got, ",") != "event-0,event-1,event-2" || page.CoveredThrough != 4 {
				t.Fatalf("events=%v page=%+v", got, page)
			}
		})
	}
}

func TestPublicScanLimitValidatesBeforeProviderIO(t *testing.T) {
	for _, limit := range []int{-1, storage.MaxOrderedPageLimit + 1} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			// A zero Store has no provider: crossing the input validation
			// boundary cannot manufacture the required typed refusal.
			_, err := (&Store{}).ReadPublicJournal(context.Background(), ReadPublicJournalRequest{TenantID: testTenant, SessionID: testSession, Limit: 1, ScanLimit: limit})
			if got := requireJournalCode(t, err, JournalErrorInvalid); got.Field != "scan_limit" {
				t.Fatalf("field=%q", got.Field)
			}
		})
	}
}

func TestPublicScanLimitTailAndMaximumBudget(t *testing.T) {
	backend := memstore.New()
	ledger := &scriptedLedger{Ledger: backend.Ledger}
	backend.Ledger = ledger
	store := openJournalStore(t, backend)
	writer := mixedSession(t, store)
	tips := 0
	ledger.onTip = func(string) {
		tips++
		if tips == 1 {
			appendOrFail(t, writer, publicEvent("late", `{"n":1}`))
		}
	}
	var next int64
	ledger.onCursor = func(cursor storage.Cursor) storage.Cursor { return &scriptedCursor{Cursor: cursor, next: &next} }
	page := readPublic(t, store, ReadPublicJournalRequest{Tail: true, Limit: 3, ScanLimit: 1})
	if tips != 1 || next != 1 || page.CapturedTip != 5 || page.CoveredThrough != 3 || len(page.Events) != 0 || page.NextCursor == "" {
		t.Fatalf("tips=%d advances=%d page=%+v", tips, next, page)
	}
	next = 0
	page = readPublic(t, store, ReadPublicJournalRequest{Cursor: page.NextCursor, ScanLimit: storage.MaxOrderedPageLimit})
	if next != 2 || page.CapturedTip != 5 || page.CoveredThrough != 5 || strings.Join(eventIDs(page), ",") != "event-2" || page.NextCursor != "" {
		t.Fatalf("advances=%d page=%+v", next, page)
	}
}

func TestPublicScanLimitDoesNotMaskEarlyEOF(t *testing.T) {
	backend := memstore.New()
	ledger := &scriptedLedger{Ledger: backend.Ledger}
	backend.Ledger = ledger
	store := openJournalStore(t, backend)
	mixedSession(t, store)
	ledger.onCursor = func(cursor storage.Cursor) storage.Cursor { return &scriptedCursor{Cursor: cursor, fail: io.EOF} }
	_, err := store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{TenantID: testTenant, SessionID: testSession, ScanLimit: 2})
	requireJournalCode(t, err, JournalErrorIntegrity)
}

func TestPublicScanLimitValidatesCorruptionOnlyWhenReached(t *testing.T) {
	for _, corruptSeq := range []uint64{2, 3} {
		t.Run(fmt.Sprint(corruptSeq), func(t *testing.T) {
			backend := memstore.New()
			ledger := &scriptedLedger{Ledger: backend.Ledger}
			backend.Ledger = ledger
			store := openJournalStore(t, backend)
			mixedSession(t, store)
			var next int64
			ledger.onCursor = func(cursor storage.Cursor) storage.Cursor {
				return &scriptedCursor{Cursor: cursor, next: &next, rewrite: func(record storage.Record) storage.Record {
					if record.Seq == corruptSeq {
						record.Payload = nil
					}
					return record
				}}
			}
			req := ReadPublicJournalRequest{TenantID: testTenant, SessionID: testSession, ScanLimit: 2}
			page, err := store.ReadPublicJournal(context.Background(), req)
			if corruptSeq == 2 {
				requireJournalCode(t, err, JournalErrorIntegrity)
				return
			}
			if err != nil || next != 2 || page.CoveredThrough != 2 || page.NextCursor == "" {
				t.Fatalf("advances=%d page=%+v err=%v", next, page, err)
			}
			req.Cursor = page.NextCursor
			_, err = store.ReadPublicJournal(context.Background(), req)
			requireJournalCode(t, err, JournalErrorIntegrity)
		})
	}
}
