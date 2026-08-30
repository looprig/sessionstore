// Bounded journal reads: the public projection a tenant may see and the
// privileged runtime replay. Writer behaviour and shared fakes live in
// journal_test.go.
package sessionstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// mixedSession writes one fence plus a deliberately interleaved mix of public
// and private records and returns the writer that owns them.
//
//	seq 1 opening fence      (private)
//	seq 2 public  event-1
//	seq 3 runtime control    (private)
//	seq 4 public  event-2
//	seq 5 application prefix (private)
func mixedSession(t *testing.T, store *Store) *JournalWriter {
	t.Helper()
	writer := openTestJournal(t, store)
	appendOrFail(t, writer, publicEvent("event-1", `{"n":1}`))
	appendOrFail(t, writer, runtimeControl("runtime-1", `{"secret":"private-runtime-bytes"}`))
	appendOrFail(t, writer, publicEvent("event-2", `{"n":2}`))
	appendOrFail(t, writer, Envelope{
		Kind: EnvelopeKindApplicationPrefix, CommandID: "command-1",
		RuntimeCommandID: uuid.UUID{9}, CommandKind: "user_input",
	})
	return writer
}

func appendOrFail(t *testing.T, writer *JournalWriter, env Envelope) uint64 {
	t.Helper()
	seq, err := writer.Append(context.Background(), env)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return seq
}

func readPublic(t *testing.T, store *Store, req ReadPublicJournalRequest) sessionwire.JournalPage {
	t.Helper()
	if req.TenantID == "" {
		req.TenantID = testTenant
	}
	if req.SessionID == "" {
		req.SessionID = testSession
	}
	page, err := store.ReadPublicJournal(context.Background(), req)
	if err != nil {
		t.Fatalf("ReadPublicJournal: %v", err)
	}
	if err := page.Validate(); err != nil {
		t.Fatalf("page violates the Core contract: %v", err)
	}
	return page
}

func eventIDs(page sessionwire.JournalPage) []string {
	out := make([]string, 0, len(page.Events))
	for _, event := range page.Events {
		out = append(out, string(event.EventID))
	}
	return out
}

func TestPublicPageReturnsOnlyPublicBodies(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	mixedSession(t, store)

	page := readPublic(t, store, ReadPublicJournalRequest{})
	if got, want := strings.Join(eventIDs(page), ","), "event-1,event-2"; got != want {
		t.Fatalf("events = %v, want %v", got, want)
	}
	if page.Events[0].JournalSeq != 2 || page.Events[1].JournalSeq != 4 {
		t.Fatalf("sequences = %d,%d, want 2,4", page.Events[0].JournalSeq, page.Events[1].JournalSeq)
	}
	if page.CapturedTip != 5 {
		t.Fatalf("CapturedTip = %d, want 5", page.CapturedTip)
	}
	if page.CoveredThrough != 5 {
		t.Fatalf("CoveredThrough = %d, want 5 (advanced across the trailing private record)", page.CoveredThrough)
	}
	if page.NextCursor != "" {
		t.Fatalf("NextCursor = %q, want empty at the captured tip", page.NextCursor)
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, forbidden := range []string{"private-runtime-bytes", "runtime-1", "command-1", "user_input", "opening_fence"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("public page leaks %q: %s", forbidden, encoded)
		}
	}
}

func TestPublicPageAdvancesCoveredThroughAcrossTrailingPrivateRecords(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	writer := openTestJournal(t, store)
	appendOrFail(t, writer, publicEvent("event-1", `{"n":1}`))
	for i := 0; i < 3; i++ {
		appendOrFail(t, writer, runtimeControl(fmt.Sprintf("runtime-%d", i), `{"private":true}`))
	}

	// A record limit of one proves the watermark keeps advancing over private
	// records after the page has already taken every event it will return.
	page := readPublic(t, store, ReadPublicJournalRequest{Limit: 1})
	if len(page.Events) != 1 || page.Events[0].JournalSeq != 2 {
		t.Fatalf("events = %+v, want only the seq-2 public event", page.Events)
	}
	if page.CoveredThrough != 5 || page.CapturedTip != 5 {
		t.Fatalf("CoveredThrough/CapturedTip = %d/%d, want 5/5", page.CoveredThrough, page.CapturedTip)
	}
	if page.NextCursor != "" {
		t.Fatal("NextCursor set although the page reached the captured tip")
	}
}

func TestPublicPageOfOnlyPrivateRecordsIsEmptyButCovered(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	writer := openTestJournal(t, store)
	appendOrFail(t, writer, runtimeControl("runtime-1", `{"private":true}`))

	page := readPublic(t, store, ReadPublicJournalRequest{})
	if len(page.Events) != 0 {
		t.Fatalf("events = %+v, want none", page.Events)
	}
	if page.CoveredThrough != 2 || page.CapturedTip != 2 {
		t.Fatalf("CoveredThrough/CapturedTip = %d/%d, want 2/2", page.CoveredThrough, page.CapturedTip)
	}
}

func TestPublicPageFromSeqBeyondTipIsEmpty(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	mixedSession(t, store)

	page := readPublic(t, store, ReadPublicJournalRequest{FromSeq: 99})
	if len(page.Events) != 0 || page.NextCursor != "" {
		t.Fatalf("page = %+v, want an empty final page", page)
	}
	if page.CoveredThrough != 5 || page.CapturedTip != 5 {
		t.Fatalf("CoveredThrough/CapturedTip = %d/%d, want 5/5", page.CoveredThrough, page.CapturedTip)
	}
}

func TestPublicPageLimitPagesThroughEveryEventExactlyOnce(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	writer := openTestJournal(t, store)
	var want []string
	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("event-%d", i)
		appendOrFail(t, writer, publicEvent(id, `{"n":1}`))
		want = append(want, id)
		appendOrFail(t, writer, runtimeControl(fmt.Sprintf("runtime-%d", i), `{"private":true}`))
	}

	var got []string
	var covered uint64
	request := ReadPublicJournalRequest{Limit: 2}
	pages := 0
	for {
		page := readPublic(t, store, request)
		pages++
		if pages > 20 {
			t.Fatal("paging did not terminate")
		}
		if page.CoveredThrough < covered {
			t.Fatalf("CoveredThrough went backwards: %d then %d", covered, page.CoveredThrough)
		}
		covered = page.CoveredThrough
		got = append(got, eventIDs(page)...)
		if page.NextCursor == "" {
			break
		}
		request = ReadPublicJournalRequest{Cursor: page.NextCursor}
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("paged events = %v, want %v", got, want)
	}
	if covered != 13 {
		t.Fatalf("final CoveredThrough = %d, want 13", covered)
	}
}

func TestPublicPageBoundsTotalBytes(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	store.maxPageBytes = 256
	writer := openTestJournal(t, store)
	body := `{"body":"` + strings.Repeat("a", 200) + `"}`
	for i := 0; i < 4; i++ {
		appendOrFail(t, writer, publicEvent(fmt.Sprintf("event-%d", i), body))
	}

	page := readPublic(t, store, ReadPublicJournalRequest{})
	if len(page.Events) != 1 {
		t.Fatalf("events = %d, want 1 under a 256-byte budget", len(page.Events))
	}
	if page.NextCursor == "" {
		t.Fatal("NextCursor missing although the budget truncated the page")
	}
	// A single event larger than the whole budget must still make progress.
	store.maxPageBytes = 8
	page = readPublic(t, store, ReadPublicJournalRequest{})
	if len(page.Events) != 1 {
		t.Fatalf("events = %d, want 1 (progress is guaranteed)", len(page.Events))
	}
}

func TestPublicPageResolvesOverflowedPublicBody(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	store.overflowThreshold = 16
	writer := openTestJournal(t, store)
	body := `{"body":"` + strings.Repeat("b", 128) + `"}`
	appendOrFail(t, writer, publicEvent("event-1", body))

	page := readPublic(t, store, ReadPublicJournalRequest{})
	if len(page.Events) != 1 {
		t.Fatalf("events = %+v, want one", page.Events)
	}
	if string(page.Events[0].Body) != body {
		t.Fatalf("resolved body = %s, want the appended body", page.Events[0].Body)
	}
}

func TestPublicPageNeverFetchesRuntimeObjects(t *testing.T) {
	backend := memstore.New()
	blobs := &loggingBlobs{lifecycleBlobs: lifecycleBlobs{backend.Blobs}}
	backend.Blobs = blobs
	store := openJournalStore(t, backend)
	store.overflowThreshold = 16
	writer := openTestJournal(t, store)
	appendOrFail(t, writer, Envelope{
		Kind:    EnvelopeKindPublicEvent,
		EventID: "event-1",
		Public:  BodySlot{Inline: []byte(`{"n":1}`)},
		Runtime: BodySlot{Inline: []byte(`{"runtime":"` + strings.Repeat("c", 128) + `"}`)},
	})
	appendOrFail(t, writer, runtimeControl("runtime-1", `{"runtime":"`+strings.Repeat("d", 128)+`"}`))
	before := blobs.getCount()

	page := readPublic(t, store, ReadPublicJournalRequest{})
	if len(page.Events) != 1 || string(page.Events[0].Body) != `{"n":1}` {
		t.Fatalf("events = %+v, want the inline public body", page.Events)
	}
	if got := blobs.getCount() - before; got != 0 {
		t.Fatalf("public read fetched %d blobs, want 0", got)
	}
}

func TestPublicPageFailsClosedOnMissingOverflowObject(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	store.overflowThreshold = 16
	writer := openTestJournal(t, store)
	appendOrFail(t, writer, publicEvent("event-1", `{"body":"`+strings.Repeat("e", 128)+`"}`))

	scope, err := store.deriveSessionScope(testTenant, testSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	keys, err := backend.Blobs.List(context.Background(), scope.BlobPrefix)
	if err != nil || len(keys) != 1 {
		t.Fatalf("List = %v, %v", keys, err)
	}
	if err := backend.Blobs.Delete(context.Background(), keys[0]); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err = store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{TenantID: testTenant, SessionID: testSession})
	var objectError *ObjectError
	if !errors.As(err, &objectError) || objectError.Code != ObjectErrorBackend {
		t.Fatalf("error = %T %v, want a typed object backend failure", err, err)
	}
}

func TestJournalReadsRequireTheSessionBinding(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	mixedSession(t, store)

	tests := []struct {
		name    string
		tenant  sessionwire.TenantID
		session sessionwire.SessionID
	}{
		{name: "unwritten session", tenant: testTenant, session: "session/never-written"},
		{name: "another tenant", tenant: "tenant/other", session: testSession},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{TenantID: tt.tenant, SessionID: tt.session})
			var keyspaceErr *KeyspaceError
			if !errors.As(err, &keyspaceErr) || keyspaceErr.Code != KeyspaceBindingNotFound {
				t.Fatalf("public read error = %T %v, want binding_not_found", err, err)
			}
			_, err = store.ReadRuntimeJournal(context.Background(), ReadRuntimeJournalRequest{TenantID: tt.tenant, SessionID: tt.session})
			if !errors.As(err, &keyspaceErr) || keyspaceErr.Code != KeyspaceBindingNotFound {
				t.Fatalf("runtime read error = %T %v, want binding_not_found", err, err)
			}
		})
	}
}

func TestJournalReadsRejectCursorWithFromSeq(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	mixedSession(t, store)
	page := readPublic(t, store, ReadPublicJournalRequest{Limit: 1})
	if page.NextCursor == "" {
		t.Fatal("expected a next cursor")
	}

	_, err := store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{
		TenantID: testTenant, SessionID: testSession, FromSeq: 2, Cursor: page.NextCursor,
	})
	journalErr := requireJournalCode(t, err, JournalErrorInvalid)
	if journalErr.Field != "cursor" {
		t.Fatalf("Field = %q, want cursor", journalErr.Field)
	}
}

// TestPublicPageCursorRejectsForgedTokens forges the cursor PAYLOAD, not just
// its header: a re-scoped token, an inflated captured tip, a zero start, and a
// privileged runtime token presented to the public read must all fail closed.
func TestPublicPageCursorRejectsForgedTokens(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	mixedSession(t, store)
	page := readPublic(t, store, ReadPublicJournalRequest{Limit: 1})
	if page.NextCursor == "" {
		t.Fatal("expected a next cursor")
	}
	valid, err := base64.RawURLEncoding.DecodeString(string(page.NextCursor))
	if err != nil {
		t.Fatalf("cursor is not raw base64url: %v", err)
	}

	// A second session gives a genuine, well-formed cursor bound elsewhere.
	otherWriter, err := store.OpenJournal(context.Background(), OpenJournalRequest{TenantID: testTenant, SessionID: "session/other"})
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	defer otherWriter.Close(context.Background())
	for i := 0; i < 3; i++ {
		if _, err := otherWriter.Append(context.Background(), publicEvent(fmt.Sprintf("other-%d", i), `{"n":1}`)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	otherPage, err := store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{
		TenantID: testTenant, SessionID: "session/other", Limit: 1,
	})
	if err != nil || otherPage.NextCursor == "" {
		t.Fatalf("other session page = %+v, %v", otherPage, err)
	}

	runtimePage, err := store.ReadRuntimeJournal(context.Background(), ReadRuntimeJournalRequest{
		TenantID: testTenant, SessionID: testSession, Limit: 1,
	})
	if err != nil || runtimePage.NextCursor == "" {
		t.Fatalf("runtime page = %+v, %v", runtimePage, err)
	}

	mutate := func(fn func([]byte)) sessionwire.Cursor {
		forged := append([]byte(nil), valid...)
		fn(forged)
		return sessionwire.Cursor(base64.RawURLEncoding.EncodeToString(forged))
	}

	tests := []struct {
		name   string
		cursor sessionwire.Cursor
	}{
		{name: "not base64", cursor: "!!!not-base64!!!"},
		{name: "empty magic", cursor: sessionwire.Cursor(base64.RawURLEncoding.EncodeToString([]byte("short")))},
		{name: "wrong magic", cursor: mutate(func(b []byte) { b[0] ^= 0xff })},
		{name: "wrong version", cursor: mutate(func(b []byte) { b[4] ^= 0xff })},
		{name: "runtime token in a public read", cursor: runtimePage.NextCursor},
		{name: "another session's token", cursor: otherPage.NextCursor},
		{name: "forged scope digest", cursor: mutate(func(b []byte) { b[10] ^= 0x01 })},
		{name: "zero start sequence", cursor: mutate(func(b []byte) {
			for i := len(b) - 16; i < len(b)-8; i++ {
				b[i] = 0
			}
		})},
		{name: "inflated captured tip", cursor: mutate(func(b []byte) { b[len(b)-8] = 0xff })},
		// A start beyond one past the snapshot's end: every other field is
		// intact, so only the span check rejects it.
		{name: "start beyond the snapshot", cursor: mutate(func(b []byte) { b[len(b)-9] = 100 })},
		{name: "trailing byte", cursor: sessionwire.Cursor(base64.RawURLEncoding.EncodeToString(append(append([]byte(nil), valid...), 0)))},
		// Unpadded base64's final character carries slack bits that the decoder
		// discards, so this string decodes to the SAME token as the valid
		// cursor. Only a canonical-spelling check rejects it. Found by
		// FuzzJournalCursorDecode.
		{name: "noncanonical base64 tail", cursor: sessionwire.Cursor(string(page.NextCursor[:len(page.NextCursor)-1]) + noncanonicalTail(string(page.NextCursor)))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{
				TenantID: testTenant, SessionID: testSession, Cursor: tt.cursor,
			})
			requireJournalCode(t, err, JournalErrorCursor)
		})
	}

	// The untouched cursor still works, so the table above rejects the forgery
	// and not the shape of every cursor.
	if _, err := store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{
		TenantID: testTenant, SessionID: testSession, Cursor: page.NextCursor,
	}); err != nil {
		t.Fatalf("valid cursor rejected: %v", err)
	}
}

func TestPublicCursorRejectedByRuntimeRead(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	mixedSession(t, store)
	page := readPublic(t, store, ReadPublicJournalRequest{Limit: 1})
	if page.NextCursor == "" {
		t.Fatal("expected a next cursor")
	}

	_, err := store.ReadRuntimeJournal(context.Background(), ReadRuntimeJournalRequest{
		TenantID: testTenant, SessionID: testSession, Cursor: page.NextCursor,
	})
	requireJournalCode(t, err, JournalErrorCursor)
}

func TestRuntimePageReturnsEveryRecordUnresolved(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	store.overflowThreshold = 16
	writer := openTestJournal(t, store)
	runtimeBody := `{"runtime":"` + strings.Repeat("f", 128) + `"}`
	appendOrFail(t, writer, publicEvent("event-1", `{"n":1}`))
	appendOrFail(t, writer, runtimeControl("runtime-1", runtimeBody))

	page, err := store.ReadRuntimeJournal(context.Background(), ReadRuntimeJournalRequest{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatalf("ReadRuntimeJournal: %v", err)
	}
	if len(page.Records) != 3 {
		t.Fatalf("records = %d, want 3 (fence, public event, runtime control)", len(page.Records))
	}
	kinds := []EnvelopeKind{EnvelopeKindOpeningFence, EnvelopeKindPublicEvent, EnvelopeKindRuntimeControl}
	for i, record := range page.Records {
		if record.Seq != uint64(i+1) {
			t.Fatalf("record %d Seq = %d", i, record.Seq)
		}
		if record.Envelope.Kind != kinds[i] {
			t.Fatalf("record %d kind = %d, want %d", i, record.Envelope.Kind, kinds[i])
		}
	}
	if page.CapturedTip != 3 || page.CoveredThrough != 3 || page.NextCursor != "" {
		t.Fatalf("page = tip %d covered %d cursor %q", page.CapturedTip, page.CoveredThrough, page.NextCursor)
	}

	// The overflowed runtime body is returned as a reference, and the caller
	// resolves it through the ordinary object API.
	reference := page.Records[2].Envelope.Runtime.Reference
	if reference == nil {
		t.Fatalf("runtime slot = %+v, want an unresolved reference", page.Records[2].Envelope.Runtime)
	}
	metadata, err := reference.ObjectMetadata()
	if err != nil {
		t.Fatalf("ObjectMetadata: %v", err)
	}
	reader, err := store.GetObject(context.Background(), GetObjectRequest{
		TenantID: testTenant, SessionID: testSession, ExpectedKind: ObjectKindJournalRuntime, Metadata: metadata,
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer reader.Close()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != runtimeBody {
		t.Fatal("resolved runtime object differs from the appended body")
	}
}

func TestRuntimePageLimitPagesThroughEveryRecord(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	writer := openTestJournal(t, store)
	for i := 0; i < 5; i++ {
		appendOrFail(t, writer, runtimeControl(fmt.Sprintf("runtime-%d", i), `{"private":true}`))
	}

	var seqs []uint64
	request := ReadRuntimeJournalRequest{TenantID: testTenant, SessionID: testSession, Limit: 2}
	for pages := 0; ; pages++ {
		if pages > 20 {
			t.Fatal("paging did not terminate")
		}
		page, err := store.ReadRuntimeJournal(context.Background(), request)
		if err != nil {
			t.Fatalf("ReadRuntimeJournal: %v", err)
		}
		for _, record := range page.Records {
			seqs = append(seqs, record.Seq)
		}
		if page.NextCursor == "" {
			break
		}
		request = ReadRuntimeJournalRequest{TenantID: testTenant, SessionID: testSession, Cursor: page.NextCursor}
	}
	if len(seqs) != 6 {
		t.Fatalf("sequences = %v, want six records", seqs)
	}
	for i, seq := range seqs {
		if seq != uint64(i+1) {
			t.Fatalf("sequences = %v, want 1..6 exactly once", seqs)
		}
	}
}

func TestJournalReadsRejectInvalidLimit(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	mixedSession(t, store)

	// The upper case is the exact first rejected value, so a gate weakened to
	// any multiple of the ceiling still fails this test.
	for _, limit := range []int{-1, storage.MaxOrderedPageLimit + 1} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			_, err := store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{
				TenantID: testTenant, SessionID: testSession, Limit: limit,
			})
			journalErr := requireJournalCode(t, err, JournalErrorInvalid)
			if journalErr.Field != "limit" {
				t.Fatalf("Field = %q, want limit", journalErr.Field)
			}
		})
	}
}

func TestJournalReadsRefuseAfterStoreClose(t *testing.T) {
	backend := memstore.New()
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{TenantID: testTenant, SessionID: testSession}); !errors.As(err, new(*StoreClosedError)) {
		t.Fatalf("public read error = %T %v, want *StoreClosedError", err, err)
	}
	if _, err := store.ReadRuntimeJournal(context.Background(), ReadRuntimeJournalRequest{TenantID: testTenant, SessionID: testSession}); !errors.As(err, new(*StoreClosedError)) {
		t.Fatalf("runtime read error = %T %v, want *StoreClosedError", err, err)
	}
}

func TestPublicPageCursorPinsTheCapturedSnapshot(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	writer := openTestJournal(t, store)
	for i := 0; i < 3; i++ {
		appendOrFail(t, writer, publicEvent(fmt.Sprintf("event-%d", i), `{"n":1}`))
	}

	first := readPublic(t, store, ReadPublicJournalRequest{Limit: 1})
	if first.CapturedTip != 4 || first.NextCursor == "" {
		t.Fatalf("first page = tip %d cursor %q", first.CapturedTip, first.NextCursor)
	}
	// Records committed after the snapshot must not appear in its continuation.
	for i := 3; i < 6; i++ {
		appendOrFail(t, writer, publicEvent(fmt.Sprintf("late-%d", i), `{"n":1}`))
	}

	var got []string
	request := ReadPublicJournalRequest{Cursor: first.NextCursor}
	got = append(got, eventIDs(first)...)
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("paging did not terminate")
		}
		page := readPublic(t, store, request)
		if page.CapturedTip != first.CapturedTip {
			t.Fatalf("CapturedTip = %d, want the pinned %d", page.CapturedTip, first.CapturedTip)
		}
		got = append(got, eventIDs(page)...)
		if page.NextCursor == "" {
			if page.CoveredThrough != first.CapturedTip {
				t.Fatalf("final CoveredThrough = %d, want the pinned tip %d", page.CoveredThrough, first.CapturedTip)
			}
			break
		}
		request = ReadPublicJournalRequest{Cursor: page.NextCursor}
	}
	if want := "event-0,event-1,event-2"; strings.Join(got, ",") != want {
		t.Fatalf("snapshot events = %v, want %v", got, want)
	}
}

// TestPublicPageRejectsForgedOversizedPublicReference forges the record itself,
// not the cursor: a reference whose declared size exceeds what a writer may
// commit must be refused before the object stream is opened.
func TestPublicPageRejectsForgedOversizedPublicReference(t *testing.T) {
	backend := memstore.New()
	blobs := &loggingBlobs{lifecycleBlobs: lifecycleBlobs{backend.Blobs}}
	backend.Blobs = blobs
	store := openJournalStore(t, backend)
	store.overflowThreshold = 16
	writer := openTestJournal(t, store)
	appendOrFail(t, writer, publicEvent("event-1", `{"body":"`+strings.Repeat("g", 128)+`"}`))

	records := readLedger(t, backend, journalName(t, store))
	forged := records[1]
	reference := *forged.Public.Reference
	reference.SizeBytes = MaxInlineBodyBytes + 1
	forged.Public = BodySlot{Reference: &reference}
	frame, err := EncodeEnvelope(forged)
	if err != nil {
		t.Fatalf("EncodeEnvelope: %v", err)
	}
	if err := backend.Ledger.Append(context.Background(), journalName(t, store), 2, frame); err != nil {
		t.Fatalf("Append: %v", err)
	}
	before := blobs.getCount()

	_, err = store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{
		TenantID: testTenant, SessionID: testSession, FromSeq: 3,
	})
	journalErr := requireJournalCode(t, err, JournalErrorTooLarge)
	if journalErr.Field != "public_reference" {
		t.Fatalf("Field = %q, want public_reference", journalErr.Field)
	}
	if got := blobs.getCount() - before; got != 0 {
		t.Fatalf("opened %d object streams for an oversized reference, want 0", got)
	}
}

// TestJournalCursorIsBoundedBeforeDecoding pins the pre-allocation gate rather
// than the shape check that follows it. base64.DecodeString sizes its own
// destination from the caller's string, so without a length check first an
// oversized cursor makes this reader allocate in proportion to attacker input.
// A shape check placed AFTER the decode returns the same typed error, so only
// the allocation itself distinguishes the two orderings.
func TestJournalCursorIsBoundedBeforeDecoding(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	mixedSession(t, store)
	const cursorBytes = 32 << 20
	huge := sessionwire.Cursor(strings.Repeat("A", cursorBytes))

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{
		TenantID: testTenant, SessionID: testSession, Cursor: huge,
	})
	runtime.ReadMemStats(&after)

	requireJournalCode(t, err, JournalErrorCursor)
	if grew := after.TotalAlloc - before.TotalAlloc; grew > cursorBytes/8 {
		t.Fatalf("rejecting a %d-byte cursor allocated %d bytes; it was decoded before being bounded", cursorBytes, grew)
	}
}

// FuzzJournalCursorDecode fuzzes the one attacker-facing parser this task adds.
//
// The invariant is canonicality, not merely "does not panic": any cursor the
// decoder accepts must be byte-identical to the cursor this reader would have
// issued for the position it decoded. That makes every field load-bearing, so a
// parser that silently ignored one could not pass.
//
// The seeds are real encoded cursors and single-byte edits of one, so they
// reach the scope-digest comparison and the snapshot bounds rather than
// bouncing off the magic or the length gate.
func FuzzJournalCursorDecode(f *testing.F) {
	backend := memstore.New()
	store, err := Open(context.Background(), backend)
	if err != nil {
		f.Fatalf("Open: %v", err)
	}
	f.Cleanup(func() { _ = store.Close(context.Background()) })

	const liveTip = 5
	valid := store.encodeJournalCursor(journalCursorPublic, testTenant, testSession, 3, liveTip)
	f.Add(string(valid))
	f.Add(string(store.encodeJournalCursor(journalCursorPublic, testTenant, testSession, 1, 0)))
	f.Add(string(store.encodeJournalCursor(journalCursorPublic, testTenant, testSession, liveTip+1, liveTip)))
	f.Add(string(store.encodeJournalCursor(journalCursorRuntime, testTenant, testSession, 3, liveTip)))
	raw, err := base64.RawURLEncoding.DecodeString(string(valid))
	if err != nil {
		f.Fatalf("seed cursor: %v", err)
	}
	// Edits inside the scope digest, the start, and the captured tip: each seed
	// still passes the magic, version, and length gates, so the fuzzer starts
	// from inputs that exercise the comparisons behind them.
	for _, offset := range []int{6, 36, 44, 52} {
		edited := append([]byte(nil), raw...)
		edited[offset] ^= 0x01
		f.Add(base64.RawURLEncoding.EncodeToString(edited))
	}
	f.Add("")
	f.Add("A")

	f.Fuzz(func(t *testing.T, cursor string) {
		position, err := store.decodeJournalCursor(journalCursorPublic, testTenant, testSession, sessionwire.Cursor(cursor), liveTip)
		if err != nil {
			var journalErr *JournalError
			if !errors.As(err, &journalErr) || journalErr.Code != JournalErrorCursor {
				t.Fatalf("rejection error = %T %v, want a typed cursor error", err, err)
			}
			return
		}
		if position.nextSeq < 1 || position.capturedTip > liveTip || position.capturedTip+1 < position.nextSeq {
			t.Fatalf("accepted an out-of-range position %+v", position)
		}
		reissued := store.encodeJournalCursor(journalCursorPublic, testTenant, testSession, position.nextSeq, position.capturedTip)
		if string(reissued) != cursor {
			t.Fatalf("accepted %q, which this reader would have issued as %q", cursor, reissued)
		}
	})
}

// noncanonicalTail returns a final base64url character that decodes to the same
// byte as the cursor's own final character but is not the spelling the encoder
// produces.
func noncanonicalTail(cursor string) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	last := cursor[len(cursor)-1]
	index := strings.IndexByte(alphabet, last)
	if index < 0 {
		return string(last)
	}
	// The encoded cursor is 71 characters, so its last character holds two
	// significant bits and four slack bits. Flipping the lowest slack bit
	// leaves the decoded bytes unchanged.
	return string(alphabet[index^1])
}

// TestResolvePublicBodyRejectsAnAbsentSlot exercises the helper's precondition
// directly. The public read path cannot present this slot — the envelope
// decoder rejects a public event that carries neither body half — so the guard
// is only reachable from here, and this is what stops it from being dead code
// that merely looks load-bearing.
func TestResolvePublicBodyRejectsAnAbsentSlot(t *testing.T) {
	backend := memstore.New()
	blobs := &loggingBlobs{lifecycleBlobs: lifecycleBlobs{backend.Blobs}}
	backend.Blobs = blobs
	store := openJournalStore(t, backend)
	mixedSession(t, store)

	body, err := store.resolvePublicBody(context.Background(), testTenant, testSession, BodySlot{})
	if body != nil {
		t.Fatalf("body = %q, want none", body)
	}
	journalError := requireJournalCode(t, err, JournalErrorIntegrity)
	if journalError.Field != "public_body" {
		t.Fatalf("Field = %q, want public_body", journalError.Field)
	}
	if got := blobs.getCount(); got != 0 {
		t.Fatalf("opened %d object streams for an absent slot, want 0", got)
	}

	// The decoder really does refuse the record that would reach it, which is
	// why the guard above is the helper's precondition and not a second copy of
	// the envelope rule.
	if _, err := EncodeEnvelope(Envelope{Kind: EnvelopeKindPublicEvent, EventID: "event-1"}); err == nil {
		t.Fatal("EncodeEnvelope accepted a public event with no body")
	}
}

// TestJournalReadsFailClosedOnACorruptStoredFrame covers the other integrity
// path: a ledger record this package did not write, or one that has been
// damaged in place, must stop the walk rather than be skipped or zero-valued.
func TestJournalReadsFailClosedOnACorruptStoredFrame(t *testing.T) {
	tests := []struct {
		name  string
		frame []byte
	}{
		{name: "foreign bytes", frame: []byte("not an envelope at all")},
		{name: "empty record", frame: nil},
		{name: "truncated header", frame: []byte("LRJE")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := memstore.New()
			store := openJournalStore(t, backend)
			writer := openTestJournal(t, store)
			appendOrFail(t, writer, publicEvent("event-1", `{"n":1}`))
			name := journalName(t, store)
			if err := backend.Ledger.Append(context.Background(), name, 2, tt.frame); err != nil {
				t.Fatalf("Append: %v", err)
			}

			_, err := store.ReadPublicJournal(context.Background(), ReadPublicJournalRequest{
				TenantID: testTenant, SessionID: testSession,
			})
			publicError := requireJournalCode(t, err, JournalErrorIntegrity)
			if publicError.Field != "record" {
				t.Fatalf("Field = %q, want record", publicError.Field)
			}
			var envelopeError *EnvelopeError
			if !errors.As(err, &envelopeError) {
				t.Fatalf("cause = %v, want a wrapped *EnvelopeError", err)
			}
			_, err = store.ReadRuntimeJournal(context.Background(), ReadRuntimeJournalRequest{
				TenantID: testTenant, SessionID: testSession,
			})
			requireJournalCode(t, err, JournalErrorIntegrity)
		})
	}
}
