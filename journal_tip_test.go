// Tip-only public journal reads: the captured tip without walking a record.
package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/looprig/storage/memstore"
)

// appendOffloadedPublicTip commits, at the tip, a public event whose body is
// object-backed and LARGER than MaxInlineBodyBytes -- the record Harness's
// journal writes for every public body over its default 512 KiB offload
// threshold (harness pkg/sessionstore effectiveOffloadPlan/durableBodySlot):
// PutObject the body, reference it, append the envelope. It returns the tip.
func appendOffloadedPublicTip(t *testing.T, store *Store, backend interface {
	Append(ctx context.Context, name string, expected uint64, payload []byte) error
}) uint64 {
	t.Helper()
	body := []byte(`{"body":"` + strings.Repeat("x", MaxInlineBodyBytes+1024) + `"}`)
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{
		TenantID: testTenant, SessionID: testSession, Kind: ObjectKindJournalPublic,
		SizeBytes: uint64(len(body)), SHA256: sha256.Sum256(body), Body: bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	reference, err := BodyReferenceFromObjectMetadata(metadata)
	if err != nil {
		t.Fatalf("BodyReferenceFromObjectMetadata: %v", err)
	}
	frame, err := EncodeEnvelope(Envelope{
		Kind: EnvelopeKindPublicEvent, EventID: "event-large", Public: BodySlot{Reference: &reference},
	})
	if err != nil {
		t.Fatalf("EncodeEnvelope: %v", err)
	}
	tip, err := store.backend.Ledger.Tip(context.Background(), journalName(t, store))
	if err != nil {
		t.Fatalf("Tip: %v", err)
	}
	if err := backend.Append(context.Background(), journalName(t, store), tip, frame); err != nil {
		t.Fatalf("Append: %v", err)
	}
	return tip + 1
}

// TestTailReadFailsClosedOnAnOffloadedPublicEventAtTheTip pins the defect the
// tip-only read exists for: a one-record tail read -- what a caller wanting
// only the tip asked for before TipOnly -- resolves the tip record's body, and
// an offloaded public body above MaxInlineBodyBytes is refused, so the tip
// cannot be learned at all.
func TestTailReadFailsClosedOnAnOffloadedPublicEventAtTheTip(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	mixedSession(t, store)
	appendOffloadedPublicTip(t, store, backend.Ledger)

	_, err := store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{
		TenantID: testTenant, SessionID: testSession, Tail: true, Limit: 1, ScanLimit: 1,
	})
	requireJournalCode(t, err, JournalErrorTooLarge)
}

// TestTipOnlyReadAnswersTheTipWithoutResolvingABody is the fix: TipOnly
// captures the tip under the same binding check and returns no event, so a
// record the body read refuses does not hide how far the journal has got, and
// no object stream is opened.
func TestTipOnlyReadAnswersTheTipWithoutResolvingABody(t *testing.T) {
	backend := memstore.New()
	blobs := &countingBlobs{lifecycleBlobs: lifecycleBlobs{backend.Blobs}}
	backend.Blobs = blobs
	store := openJournalStore(t, backend)
	mixedSession(t, store)
	tip := appendOffloadedPublicTip(t, store, backend.Ledger)
	before := blobs.getCount()

	for name, req := range map[string]ReadPublicJournalRequest{
		"bare":               {TipOnly: true},
		"with tail fallback": {TipOnly: true, Tail: true, Limit: 1, ScanLimit: 1},
	} {
		t.Run(name, func(t *testing.T) {
			page := readPublic(t, store, req)
			if page.CapturedTip != tip {
				t.Fatalf("CapturedTip = %d, want %d", page.CapturedTip, tip)
			}
			if len(page.Events) != 0 || page.CoveredThrough != 0 || page.NextCursor != "" || page.PreviousCursor != "" {
				t.Fatalf("page = %+v, want a tip and nothing else", page)
			}
		})
	}
	if got := blobs.getCount() - before; got != 0 {
		t.Fatalf("tip-only read opened %d object streams, want 0", got)
	}
}

func TestTipOnlyReadOfAnEmptyJournalIsZero(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	openTestJournal(t, store)
	page := readPublic(t, store, ReadPublicJournalRequest{TipOnly: true})
	if page.CapturedTip != 1 || len(page.Events) != 0 || page.CoveredThrough != 0 {
		t.Fatalf("page = %+v, want the opening fence's tip and nothing else", page)
	}
}

func TestTipOnlyReadRefusesAPosition(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	mixedSession(t, store)
	first := readPublic(t, store, ReadPublicJournalRequest{Limit: 1})
	if first.NextCursor == "" {
		t.Fatal("expected a continuation cursor")
	}
	for name, req := range map[string]ReadPublicJournalRequest{
		"from_seq": {TipOnly: true, FromSeq: 2},
		"cursor":   {TipOnly: true, Cursor: first.NextCursor},
	} {
		t.Run(name, func(t *testing.T) {
			req.TenantID, req.SessionID = testTenant, testSession
			_, err := store.ReadPublicJournal(context.Background(), req)
			journalErr := requireJournalCode(t, err, JournalErrorInvalid)
			if journalErr.Field != "tip_only" {
				t.Fatalf("Field = %q, want tip_only", journalErr.Field)
			}
		})
	}
}

// TestTipOnlyReadRequiresTheSessionBinding: the tip is a fact about a session
// the caller names, so it is refused exactly as a page read is when the
// session's binding does not verify.
func TestTipOnlyReadRequiresTheSessionBinding(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	_, err := store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{
		TenantID: testTenant, SessionID: "session/absent", TipOnly: true,
	})
	if err == nil {
		t.Fatal("a tip-only read of a session with no journal binding succeeded")
	}
}
