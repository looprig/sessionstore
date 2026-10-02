// Object-backed public bodies above the inline ceiling: the public page read
// resolves them up to MaxObjectPublicBodyBytes, a first event that alone
// exceeds the page budget comes back on its own page, and a reference whose
// declared size disagrees with its object fails closed.
package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage/memstore"
)

// offloadedPublicBody returns a canonical JSON public body of exactly size
// bytes.
func offloadedPublicBody(t *testing.T, size int) []byte {
	t.Helper()
	const framing = len(`{"body":""}`)
	if size < framing {
		t.Fatalf("body size %d is below the JSON framing", size)
	}
	return []byte(`{"body":"` + strings.Repeat("x", size-framing) + `"}`)
}

// appendOffloadedPublic commits, at the ledger tip, a public event whose body
// is held in an object -- what Harness's journal writes for every public body
// over its default 512 KiB offload threshold (harness pkg/sessionstore
// effectiveOffloadPlan/durableBodySlot): PutObject the body, reference it,
// append the envelope. It bypasses JournalWriter.Append on purpose: that
// writer caps a public body at MaxInlineBodyBytes, which is the writer's
// admission rule, not the reader's. It returns the record's sequence and the
// reference it carries.
func appendOffloadedPublic(t *testing.T, store *Store, backend interface {
	Append(ctx context.Context, name string, expected uint64, payload []byte) error
}, eventID string, body []byte) (uint64, BodyReference) {
	t.Helper()
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
	return appendPublicReference(t, store, backend, eventID, reference), reference
}

// appendPublicReference appends a public event carrying reference verbatim at
// the ledger tip and returns its sequence.
func appendPublicReference(t *testing.T, store *Store, backend interface {
	Append(ctx context.Context, name string, expected uint64, payload []byte) error
}, eventID string, reference BodyReference) uint64 {
	t.Helper()
	frame, err := EncodeEnvelope(Envelope{
		Kind: EnvelopeKindPublicEvent, EventID: sessionwire.EventID(eventID), Public: BodySlot{Reference: &reference},
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

// TestPublicPageServesAnOffloadedBodyAboveTheInlineCeiling is the fix: a
// public body held in an object is resolved byte-identically whether it is
// just above the inline ceiling -- every public body Harness offloads under its
// default threshold -- or exactly at the object ceiling.
func TestPublicPageServesAnOffloadedBodyAboveTheInlineCeiling(t *testing.T) {
	for name, size := range map[string]int{
		"inline ceiling plus one": MaxInlineBodyBytes + 1,
		"object ceiling exactly":  MaxObjectPublicBodyBytes,
	} {
		t.Run(name, func(t *testing.T) {
			backend := memstore.New()
			store := openJournalStore(t, backend)
			mixedSession(t, store)
			body := offloadedPublicBody(t, size)
			seq, _ := appendOffloadedPublic(t, store, backend.Ledger, "event-large", body)

			page := readPublic(t, store, ReadPublicJournalRequest{FromSeq: seq})
			if len(page.Events) != 1 {
				t.Fatalf("events = %v, want only event-large", eventIDs(page))
			}
			got := page.Events[0]
			if got.EventID != "event-large" || got.JournalSeq != seq {
				t.Fatalf("event = %s at %d, want event-large at %d", got.EventID, got.JournalSeq, seq)
			}
			if !bytes.Equal(got.Body, body) {
				t.Fatalf("body is %d bytes and not byte-identical to the %d committed", len(got.Body), len(body))
			}
			if page.NextCursor != "" || page.CoveredThrough != seq || page.CapturedTip != seq {
				t.Fatalf("page tip %d covered %d cursor %q, want a complete page through %d",
					page.CapturedTip, page.CoveredThrough, page.NextCursor, seq)
			}
		})
	}
}

// TestPublicPageReturnsAnOversizedEventAloneWithACursor pins the progress
// rule: the page budget bounds the aggregate, the first event of a page is
// always admitted however large, and an event that does not fit the remaining
// budget is deferred to the next page rather than refused.
func TestPublicPageReturnsAnOversizedEventAloneWithACursor(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	writer := openTestJournal(t, store)
	first := appendOrFail(t, writer, publicEvent("event-small-1", `{"n":1}`))
	body := offloadedPublicBody(t, 2*DefaultJournalPageBytes)
	if err := writer.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	large, _ := appendOffloadedPublic(t, store, backend.Ledger, "event-large", body)
	last := appendPublicInline(t, store, backend.Ledger, "event-small-2", `{"n":2}`)

	page := readPublic(t, store, ReadPublicJournalRequest{})
	if got := strings.Join(eventIDs(page), ","); got != "event-small-1" || page.NextCursor == "" || page.CoveredThrough != first {
		t.Fatalf("first page = %s covered %d cursor %q, want event-small-1 alone, deferring the large event", got, page.CoveredThrough, page.NextCursor)
	}

	page = readPublic(t, store, ReadPublicJournalRequest{Cursor: page.NextCursor})
	if got := strings.Join(eventIDs(page), ","); got != "event-large" || page.NextCursor == "" || page.CoveredThrough != large {
		t.Fatalf("second page = %s covered %d cursor %q, want event-large alone with a cursor", got, page.CoveredThrough, page.NextCursor)
	}
	if !bytes.Equal(page.Events[0].Body, body) {
		t.Fatal("the oversized event's body is not byte-identical to the committed one")
	}

	page = readPublic(t, store, ReadPublicJournalRequest{Cursor: page.NextCursor})
	if got := strings.Join(eventIDs(page), ","); got != "event-small-2" || page.NextCursor != "" || page.CoveredThrough != last {
		t.Fatalf("third page = %s covered %d cursor %q, want event-small-2 and the end", got, page.CoveredThrough, page.NextCursor)
	}
}

// TestPublicPagesWalkConsecutiveOversizedEventsOnePerPage: two adjacent events
// each above the page budget take one page apiece, and the walk ends.
func TestPublicPagesWalkConsecutiveOversizedEventsOnePerPage(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	writer := openTestJournal(t, store)
	if err := writer.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	firstBody := offloadedPublicBody(t, DefaultJournalPageBytes+1)
	secondBody := offloadedPublicBody(t, DefaultJournalPageBytes+2)
	first, _ := appendOffloadedPublic(t, store, backend.Ledger, "event-large-1", firstBody)
	second, _ := appendOffloadedPublic(t, store, backend.Ledger, "event-large-2", secondBody)

	page := readPublic(t, store, ReadPublicJournalRequest{})
	if got := strings.Join(eventIDs(page), ","); got != "event-large-1" || page.NextCursor == "" || page.CoveredThrough != first {
		t.Fatalf("first page = %s covered %d cursor %q, want event-large-1 alone with a cursor", got, page.CoveredThrough, page.NextCursor)
	}
	if !bytes.Equal(page.Events[0].Body, firstBody) {
		t.Fatal("first oversized body is not byte-identical")
	}
	page = readPublic(t, store, ReadPublicJournalRequest{Cursor: page.NextCursor})
	if got := strings.Join(eventIDs(page), ","); got != "event-large-2" || page.NextCursor != "" || page.CoveredThrough != second {
		t.Fatalf("second page = %s covered %d cursor %q, want event-large-2 and the end", got, page.CoveredThrough, page.NextCursor)
	}
	if !bytes.Equal(page.Events[0].Body, secondBody) {
		t.Fatal("second oversized body is not byte-identical")
	}
}

// TestPublicPageRefusesAReferenceWhoseDeclaredSizeDisagrees forges only the
// declared size: the object identity, digest and bytes are the real ones, so
// the object opens, and the length disagreement must fail the page closed as
// an integrity fault rather than return a truncated or extended body.
func TestPublicPageRefusesAReferenceWhoseDeclaredSizeDisagrees(t *testing.T) {
	for name, delta := range map[string]int64{"understates": -1, "overstates": +1} {
		t.Run(name, func(t *testing.T) {
			backend := memstore.New()
			store := openJournalStore(t, backend)
			mixedSession(t, store)
			body := offloadedPublicBody(t, MaxInlineBodyBytes+1)
			_, reference := appendOffloadedPublic(t, store, backend.Ledger, "event-large", body)
			reference.SizeBytes = uint64(int64(reference.SizeBytes) + delta)
			forged := appendPublicReference(t, store, backend.Ledger, "event-forged", reference)

			page, err := store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{
				TenantID: testTenant, SessionID: testSession, FromSeq: forged,
			})
			if len(page.Events) != 0 {
				t.Fatalf("returned %d events for a forged reference", len(page.Events))
			}
			journalErr := requireJournalCode(t, err, JournalErrorIntegrity)
			if journalErr.Field != "public_object" {
				t.Fatalf("Field = %q, want public_object", journalErr.Field)
			}
		})
	}
}

// TestRuntimePageLeavesAnOffloadedPublicBodyUnresolved: the privileged replay
// returns records exactly as stored, so a public reference of any declared
// size is handed back unresolved and no object stream is opened.
func TestRuntimePageLeavesAnOffloadedPublicBodyUnresolved(t *testing.T) {
	backend := memstore.New()
	blobs := &countingBlobs{lifecycleBlobs: lifecycleBlobs{backend.Blobs}}
	backend.Blobs = blobs
	store := openJournalStore(t, backend)
	mixedSession(t, store)
	seq, reference := appendOffloadedPublic(t, store, backend.Ledger, "event-large", offloadedPublicBody(t, 2*DefaultJournalPageBytes))
	before := blobs.getCount()

	page, err := store.ReadRuntimeJournal(context.Background(), ReadRuntimeJournalRequest{
		TenantID: testTenant, SessionID: testSession, FromSeq: seq,
	})
	if err != nil {
		t.Fatalf("ReadRuntimeJournal: %v", err)
	}
	if len(page.Records) != 1 || page.Records[0].Seq != seq || page.NextCursor != "" {
		t.Fatalf("runtime page = %+v, want the one record at %d", page, seq)
	}
	got := page.Records[0].Envelope.Public
	if got.Inline != nil || got.Reference == nil || *got.Reference != reference {
		t.Fatalf("public slot = %+v, want the stored reference unresolved", got)
	}
	if opened := blobs.getCount() - before; opened != 0 {
		t.Fatalf("runtime read opened %d object streams, want 0", opened)
	}
}

// appendPublicInline appends an inline public event at the ledger tip, for a
// test that has already appended around the writer.
func appendPublicInline(t *testing.T, store *Store, backend interface {
	Append(ctx context.Context, name string, expected uint64, payload []byte) error
}, eventID, body string) uint64 {
	t.Helper()
	frame, err := EncodeEnvelope(publicEvent(eventID, body))
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
