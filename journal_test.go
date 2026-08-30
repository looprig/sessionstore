// Epoch-fenced journal writer behaviour. The reader lives in
// journal_reader_test.go; fakes shared by both live here.
package sessionstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// ---------------------------------------------------------------- test fakes

// callLog records the ordered provider calls a test cares about. Every hook
// appends through the same mutex so a -race run over concurrent writers sees a
// consistent order.
type callLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *callLog) add(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, name)
}

func (l *callLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.calls...)
}

func (l *callLog) count(name string) int {
	n := 0
	for _, call := range l.snapshot() {
		if call == name {
			n++
		}
	}
	return n
}

// scriptedLedger wraps a real ledger so a test can log call order, run a side
// effect between Tip and the fence CAS, or substitute an append outcome.
type scriptedLedger struct {
	storage.Ledger
	log      *callLog
	onTip    func(name string)
	appendFn func(expected uint64, payload []byte) (bool, error)
	readFn   func(from uint64) (bool, storage.Cursor, error)
}

func (l *scriptedLedger) Tip(ctx context.Context, name string) (uint64, error) {
	tip, err := l.Ledger.Tip(ctx, name)
	if l.log != nil {
		l.log.add("tip")
	}
	if l.onTip != nil {
		l.onTip(name)
	}
	return tip, err
}

func (l *scriptedLedger) Append(ctx context.Context, name string, expected uint64, payload []byte) error {
	if l.log != nil {
		l.log.add("append")
	}
	if l.appendFn != nil {
		if handled, err := l.appendFn(expected, payload); handled {
			return err
		}
	}
	return l.Ledger.Append(ctx, name, expected, payload)
}

func (l *scriptedLedger) Read(ctx context.Context, name string, from uint64) (storage.Cursor, error) {
	if l.log != nil {
		l.log.add("read")
	}
	if l.readFn != nil {
		if handled, cur, err := l.readFn(from); handled {
			return cur, err
		}
	}
	return l.Ledger.Read(ctx, name, from)
}

// loggingBlobs records blob traffic into the shared call log so a test can
// prove an overflow object is written and re-read before its reference is
// appended.
type loggingBlobs struct {
	lifecycleBlobs
	log  *callLog
	gets int
	mu   sync.Mutex
}

func (b *loggingBlobs) Put(ctx context.Context, key string, r io.Reader) error {
	if b.log != nil {
		b.log.add("blob_put")
	}
	return b.Blobs.Put(ctx, key, r)
}

func (b *loggingBlobs) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	b.mu.Lock()
	b.gets++
	b.mu.Unlock()
	if b.log != nil {
		b.log.add("blob_get")
	}
	return b.Blobs.Get(ctx, key)
}

func (b *loggingBlobs) getCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.gets
}

// permissiveLeaser grants an independent, strictly increasing epoch on every
// Acquire and never refuses. It exists so a test can hold two simultaneously
// live writers and exercise the CAS fence itself rather than the lease guard
// that normally hides it.
type permissiveLeaser struct {
	mu     sync.Mutex
	epochs map[string]uint64
}

func newPermissiveLeaser() *permissiveLeaser {
	return &permissiveLeaser{epochs: make(map[string]uint64)}
}

func (l *permissiveLeaser) Acquire(_ context.Context, name string) (storage.Lease, error) {
	if err := storage.ValidateName(name); err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.epochs[name]++
	return &fakeLease{epoch: l.epochs[name], lost: make(chan struct{})}, nil
}

type fakeLease struct {
	epoch      uint64
	lost       chan struct{}
	once       sync.Once
	releases   int32
	releaseErr error
	mu         sync.Mutex
}

func (l *fakeLease) Epoch() uint64         { return l.epoch }
func (l *fakeLease) Lost() <-chan struct{} { return l.lost }
func (l *fakeLease) Release(context.Context) error {
	l.mu.Lock()
	l.releases++
	l.mu.Unlock()
	l.once.Do(func() { close(l.lost) })
	return l.releaseErr
}

func (l *fakeLease) releaseCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int(l.releases)
}

// recordingLeaser hands out fakeLease values a test keeps hold of, so a failed
// open can be checked for the release that frees the grant.
type recordingLeaser struct {
	mu     sync.Mutex
	epochs map[string]uint64
	issued []*fakeLease
	err    error
}

func newRecordingLeaser() *recordingLeaser {
	return &recordingLeaser{epochs: make(map[string]uint64)}
}

func (l *recordingLeaser) Acquire(_ context.Context, name string) (storage.Lease, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return nil, l.err
	}
	l.epochs[name]++
	lease := &fakeLease{epoch: l.epochs[name], lost: make(chan struct{})}
	l.issued = append(l.issued, lease)
	return lease, nil
}

func (l *recordingLeaser) last() *fakeLease {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.issued) == 0 {
		return nil
	}
	return l.issued[len(l.issued)-1]
}

// ------------------------------------------------------------- test helpers

const (
	testTenant  = sessionwire.TenantID("tenant/raw")
	testSession = sessionwire.SessionID("session/raw")
)

func openJournalStore(t *testing.T, backend *storage.Composite) *Store {
	t.Helper()
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	return store
}

func openTestJournal(t *testing.T, store *Store) *JournalWriter {
	t.Helper()
	writer, err := store.OpenJournal(context.Background(), OpenJournalRequest{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close(context.Background()) })
	return writer
}

func publicEvent(id string, body string) Envelope {
	return Envelope{Kind: EnvelopeKindPublicEvent, EventID: sessionwire.EventID(id), Public: BodySlot{Inline: []byte(body)}}
}

func runtimeControl(id string, body string) Envelope {
	return Envelope{Kind: EnvelopeKindRuntimeControl, RecordID: id, Runtime: BodySlot{Inline: []byte(body)}}
}

func journalName(t *testing.T, store *Store) string {
	t.Helper()
	scope, err := store.deriveSessionScope(testTenant, testSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	return scope.JournalName
}

func leaseName(t *testing.T, store *Store) string {
	t.Helper()
	scope, err := store.deriveSessionScope(testTenant, testSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	return scope.LeaseName
}

// readLedger returns every decoded envelope currently durable, in order.
func readLedger(t *testing.T, backend *storage.Composite, name string) []Envelope {
	t.Helper()
	cur, err := backend.Ledger.Read(context.Background(), name, 1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer cur.Close()
	var out []Envelope
	for {
		rec, err := cur.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		env, err := DecodeEnvelope(rec.Payload)
		if err != nil {
			t.Fatalf("DecodeEnvelope at seq %d: %v", rec.Seq, err)
		}
		out = append(out, env)
	}
}

func requireJournalCode(t *testing.T, err error, want JournalErrorCode) *JournalError {
	t.Helper()
	var journalErr *JournalError
	if !errors.As(err, &journalErr) {
		t.Fatalf("error = %T %v, want *JournalError", err, err)
	}
	if journalErr.Code != want {
		t.Fatalf("error code = %q (%v), want %q", journalErr.Code, err, want)
	}
	return journalErr
}

// ------------------------------------------------------------------- tests

func TestOpenJournalWritesOpeningFenceAtTip(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	writer := openTestJournal(t, store)

	if writer.Epoch() != 1 {
		t.Fatalf("Epoch = %d, want 1", writer.Epoch())
	}
	if writer.Sequence() != 1 {
		t.Fatalf("Sequence = %d, want 1", writer.Sequence())
	}
	records := readLedger(t, backend, journalName(t, store))
	if len(records) != 1 || records[0].Kind != EnvelopeKindOpeningFence || records[0].LeaseEpoch != 1 {
		t.Fatalf("ledger = %+v, want one opening fence at epoch 1", records)
	}
}

// TestOpeningFenceConflictRequiresFreshGrant is the deliberate inversion of the
// Harness behaviour: a predecessor append that lands between the tip read and
// the fence CAS fails this grant outright. The writer does not refresh the tip
// and retry; the caller must acquire a fresh, higher epoch and reopen.
func TestOpeningFenceConflictRequiresFreshGrant(t *testing.T) {
	backend := memstore.New()
	log := &callLog{}
	base := backend.Ledger
	injected := false
	scripted := &scriptedLedger{Ledger: base, log: log}
	backend.Ledger = scripted
	store := openJournalStore(t, backend)
	name := journalName(t, store)
	foreign, err := EncodeEnvelope(runtimeControl("predecessor", `{"predecessor":true}`))
	if err != nil {
		t.Fatalf("EncodeEnvelope: %v", err)
	}
	scripted.onTip = func(string) {
		if injected {
			return
		}
		injected = true
		if err := base.Append(context.Background(), name, 0, foreign); err != nil {
			t.Errorf("inject: %v", err)
		}
	}

	writer, err := store.OpenJournal(context.Background(), OpenJournalRequest{TenantID: testTenant, SessionID: testSession})
	if err == nil {
		_ = writer.Close(context.Background())
		t.Fatal("OpenJournal succeeded, want a fenced conflict")
	}
	requireJournalCode(t, err, JournalErrorFenced)
	if writer != nil {
		t.Fatalf("writer = %v, want nil on a failed open", writer)
	}
	if got := log.count("tip"); got != 1 {
		t.Fatalf("Tip called %d times, want exactly 1 (no refresh-and-rebase)", got)
	}

	// A fresh grant at a higher epoch reopens over the predecessor's record.
	next := openTestJournal(t, store)
	if next.Epoch() != 2 {
		t.Fatalf("second Epoch = %d, want 2", next.Epoch())
	}
	if next.Sequence() != 2 {
		t.Fatalf("second Sequence = %d, want 2", next.Sequence())
	}
	records := readLedger(t, backend, name)
	if len(records) != 2 || records[1].Kind != EnvelopeKindOpeningFence || records[1].LeaseEpoch != 2 {
		t.Fatalf("ledger = %+v, want the predecessor record then an epoch-2 fence", records)
	}
}

func TestOpeningFenceFailureReleasesLease(t *testing.T) {
	tests := []struct {
		name     string
		script   func(base storage.Ledger, name string, scripted *scriptedLedger)
		wantCode JournalErrorCode
	}{
		{
			name: "fence conflict",
			script: func(base storage.Ledger, name string, scripted *scriptedLedger) {
				injected := false
				scripted.onTip = func(string) {
					if injected {
						return
					}
					injected = true
					frame, _ := EncodeEnvelope(runtimeControl("predecessor", `{"a":1}`))
					_ = base.Append(context.Background(), name, 0, frame)
				}
			},
			wantCode: JournalErrorFenced,
		},
		{
			name: "append backend failure",
			script: func(_ storage.Ledger, _ string, scripted *scriptedLedger) {
				scripted.appendFn = func(uint64, []byte) (bool, error) { return true, errors.New("private backend failure") }
			},
			wantCode: JournalErrorBackend,
		},
		{
			name: "unresolved ambiguous append",
			script: func(_ storage.Ledger, name string, scripted *scriptedLedger) {
				scripted.appendFn = func(expected uint64, _ []byte) (bool, error) {
					return true, &storage.AmbiguousError{Name: name, Expected: expected}
				}
			},
			wantCode: JournalErrorUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := memstore.New()
			leaser := newRecordingLeaser()
			base := backend.Ledger
			scripted := &scriptedLedger{Ledger: base}
			backend.Ledger = scripted
			backend.Leaser = leaser
			store := openJournalStore(t, backend)
			tt.script(base, journalName(t, store), scripted)

			_, err := store.OpenJournal(context.Background(), OpenJournalRequest{TenantID: testTenant, SessionID: testSession})
			if err == nil {
				t.Fatal("OpenJournal succeeded, want failure")
			}
			requireJournalCode(t, err, tt.wantCode)
			lease := leaser.last()
			if lease == nil {
				t.Fatal("no lease was acquired")
			}
			if got := lease.releaseCount(); got != 1 {
				t.Fatalf("Release called %d times on the failed grant, want 1", got)
			}
		})
	}
}

func TestOpeningFenceFailureFreesTheNameForAFreshGrant(t *testing.T) {
	backend := memstore.New()
	base := backend.Ledger
	scripted := &scriptedLedger{Ledger: base}
	backend.Ledger = scripted
	store := openJournalStore(t, backend)
	scripted.appendFn = func(uint64, []byte) (bool, error) { return true, errors.New("private backend failure") }

	if _, err := store.OpenJournal(context.Background(), OpenJournalRequest{TenantID: testTenant, SessionID: testSession}); err == nil {
		t.Fatal("OpenJournal succeeded, want failure")
	}
	// memstore refuses a second Acquire while a grant is live, so a successful
	// re-acquire is direct evidence the failed grant was released.
	lease, err := backend.Leaser.Acquire(context.Background(), leaseName(t, store))
	if err != nil {
		t.Fatalf("re-acquire after failed open: %v", err)
	}
	_ = lease.Release(context.Background())
}

func TestOpenJournalRejectsHeldLease(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	held, err := backend.Leaser.Acquire(context.Background(), leaseName(t, store))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer held.Release(context.Background())

	_, err = store.OpenJournal(context.Background(), OpenJournalRequest{TenantID: testTenant, SessionID: testSession})
	journalErr := requireJournalCode(t, err, JournalErrorLeaseHeld)
	if journalErr.Epoch != held.Epoch() {
		t.Fatalf("Epoch = %d, want the live holder's %d", journalErr.Epoch, held.Epoch())
	}
	if records := readLedger(t, backend, journalName(t, store)); len(records) != 0 {
		t.Fatalf("ledger = %+v, want nothing written without ownership", records)
	}
}

// TestOpenJournalClaimsTipWithNoInterveningIO pins the ordering that closes the
// hydration race: the fence CAS is the very next ledger call after the tip read,
// so no slow post-ownership work can sit between them.
func TestOpenJournalClaimsTipWithNoInterveningIO(t *testing.T) {
	backend := memstore.New()
	log := &callLog{}
	blobs := &loggingBlobs{lifecycleBlobs: lifecycleBlobs{backend.Blobs}, log: log}
	backend.Blobs = blobs
	backend.Ledger = &scriptedLedger{Ledger: backend.Ledger, log: log}
	store := openJournalStore(t, backend)
	openTestJournal(t, store)

	if got, want := log.snapshot(), []string{"tip", "append"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("provider calls = %v, want %v", got, want)
	}
}

func TestAppendAdvancesTrackedTipWithoutRereadingIt(t *testing.T) {
	backend := memstore.New()
	log := &callLog{}
	backend.Ledger = &scriptedLedger{Ledger: backend.Ledger, log: log}
	store := openJournalStore(t, backend)
	writer := openTestJournal(t, store)

	for i := 1; i <= 3; i++ {
		seq, err := writer.Append(context.Background(), publicEvent(fmt.Sprintf("event-%d", i), `{"n":1}`))
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if want := uint64(i + 1); seq != want {
			t.Fatalf("Append %d = %d, want %d", i, seq, want)
		}
	}
	if got := log.count("tip"); got != 1 {
		t.Fatalf("Tip called %d times, want exactly 1 (only the open)", got)
	}
	if got := log.count("append"); got != 4 {
		t.Fatalf("Append called %d times, want 4 (fence plus three records)", got)
	}
}

// TestStaleWriterAppendFailsPermanently uses two simultaneously live grants so
// the CAS fence — not the lease guard — is what rejects the stale writer.
func TestStaleWriterAppendFailsPermanently(t *testing.T) {
	backend := memstore.New()
	log := &callLog{}
	backend.Ledger = &scriptedLedger{Ledger: backend.Ledger, log: log}
	backend.Leaser = newPermissiveLeaser()
	store := openJournalStore(t, backend)
	name := journalName(t, store)

	stale := openTestJournal(t, store)
	successor := openTestJournal(t, store)
	if successor.Epoch() <= stale.Epoch() {
		t.Fatalf("successor epoch %d must exceed %d", successor.Epoch(), stale.Epoch())
	}
	before := log.count("tip")

	_, err := stale.Append(context.Background(), publicEvent("stale-1", `{"stale":true}`))
	first := requireJournalCode(t, err, JournalErrorFenced)
	_, err = stale.Append(context.Background(), publicEvent("stale-2", `{"stale":true}`))
	second := requireJournalCode(t, err, JournalErrorFenced)
	if first.Code != second.Code {
		t.Fatalf("second failure = %v, want the same latched conflict", second)
	}
	if got := log.count("tip"); got != before {
		t.Fatalf("Tip called %d more times after the conflict, want 0 (never rebase)", got-before)
	}
	records := readLedger(t, backend, name)
	if len(records) != 2 {
		t.Fatalf("ledger has %d records, want only the two fences", len(records))
	}
	for _, record := range records {
		if record.Kind != EnvelopeKindOpeningFence {
			t.Fatalf("ledger = %+v, want no stale writer record", records)
		}
	}
	if _, err := successor.Append(context.Background(), publicEvent("live-1", `{"live":true}`)); err != nil {
		t.Fatalf("successor Append: %v", err)
	}
}

func TestWriterNeverRebasesAfterUnknownAppend(t *testing.T) {
	tests := []struct {
		name  string
		setup func(name string, scripted *scriptedLedger)
	}{
		{
			name: "unresolved ambiguous ack",
			setup: func(name string, scripted *scriptedLedger) {
				scripted.appendFn = func(expected uint64, payload []byte) (bool, error) {
					if frameKind(payload) == EnvelopeKindOpeningFence {
						return false, nil
					}
					return true, &storage.AmbiguousError{Name: name, Expected: expected}
				}
			},
		},
		{
			name: "unreadable contested tip",
			setup: func(name string, scripted *scriptedLedger) {
				scripted.appendFn = func(expected uint64, payload []byte) (bool, error) {
					if frameKind(payload) == EnvelopeKindOpeningFence {
						return false, nil
					}
					return true, &storage.ConflictError{Name: name, Expected: expected}
				}
				scripted.readFn = func(uint64) (bool, storage.Cursor, error) {
					return true, nil, errors.New("private read failure")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := memstore.New()
			log := &callLog{}
			scripted := &scriptedLedger{Ledger: backend.Ledger, log: log}
			backend.Ledger = scripted
			store := openJournalStore(t, backend)
			writer := openTestJournal(t, store)
			tt.setup(journalName(t, store), scripted)

			_, err := writer.Append(context.Background(), publicEvent("event-1", `{"n":1}`))
			requireJournalCode(t, err, JournalErrorUnknown)
			tipsAfterUnknown := log.count("tip")

			// The backend recovers, but the writer's tip is permanently unknown:
			// it must fail rather than re-read and rebase onto whatever is there.
			scripted.appendFn = nil
			scripted.readFn = nil
			_, err = writer.Append(context.Background(), publicEvent("event-2", `{"n":2}`))
			requireJournalCode(t, err, JournalErrorUnknown)
			if got := log.count("tip"); got != tipsAfterUnknown {
				t.Fatalf("Tip re-read %d times after an unknown append, want 0", got-tipsAfterUnknown)
			}
			if writer.Sequence() != 1 {
				t.Fatalf("Sequence = %d, want the fence sequence 1 (no speculative advance)", writer.Sequence())
			}
		})
	}
}

func TestAppendResolvesAmbiguousAckByRecordIdentity(t *testing.T) {
	backend := memstore.New()
	base := backend.Ledger
	scripted := &scriptedLedger{Ledger: base}
	backend.Ledger = scripted
	store := openJournalStore(t, backend)
	writer := openTestJournal(t, store)
	name := journalName(t, store)

	// The append really lands but the ack is lost; the retry then conflicts and
	// AppendDefinite must recognise its own record at expected+1.
	scripted.appendFn = func(expected uint64, payload []byte) (bool, error) {
		scripted.appendFn = nil
		if err := base.Append(context.Background(), name, expected, payload); err != nil {
			return true, err
		}
		return true, &storage.AmbiguousError{Name: name, Expected: expected}
	}
	seq, err := writer.Append(context.Background(), publicEvent("event-1", `{"n":1}`))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seq != 2 {
		t.Fatalf("Append = %d, want 2", seq)
	}
	records := readLedger(t, backend, name)
	if len(records) != 2 || records[1].EventID != "event-1" {
		t.Fatalf("ledger = %+v, want exactly one committed event", records)
	}
}

func TestAppendNeverAdoptsAForeignRecord(t *testing.T) {
	backend := memstore.New()
	base := backend.Ledger
	scripted := &scriptedLedger{Ledger: base}
	backend.Ledger = scripted
	store := openJournalStore(t, backend)
	writer := openTestJournal(t, store)
	name := journalName(t, store)

	// A foreign writer occupies expected+1 and this writer's append is reported
	// ambiguous. Resolution must compare identity and refuse to claim it.
	scripted.appendFn = func(expected uint64, _ []byte) (bool, error) {
		scripted.appendFn = nil
		foreign, _ := EncodeEnvelope(runtimeControl("foreign", `{"foreign":true}`))
		if err := base.Append(context.Background(), name, expected, foreign); err != nil {
			return true, err
		}
		return true, &storage.AmbiguousError{Name: name, Expected: expected}
	}
	_, err := writer.Append(context.Background(), publicEvent("event-1", `{"n":1}`))
	requireJournalCode(t, err, JournalErrorFenced)
	records := readLedger(t, backend, name)
	if len(records) != 2 || records[1].RecordID != "foreign" {
		t.Fatalf("ledger = %+v, want the foreign record untouched", records)
	}
}

func TestAppendRefusesAfterLeaseLost(t *testing.T) {
	backend := memstore.New()
	leaser := newRecordingLeaser()
	backend.Leaser = leaser
	store := openJournalStore(t, backend)
	writer := openTestJournal(t, store)
	_ = leaser.last().Release(context.Background())

	_, err := writer.Append(context.Background(), publicEvent("event-1", `{"n":1}`))
	requireJournalCode(t, err, JournalErrorLeaseLost)
	// The failure latches: a lease that somehow looked live again cannot revive
	// a writer that already lost ownership.
	_, err = writer.Append(context.Background(), publicEvent("event-2", `{"n":2}`))
	requireJournalCode(t, err, JournalErrorLeaseLost)
	if records := readLedger(t, backend, journalName(t, store)); len(records) != 1 {
		t.Fatalf("ledger = %+v, want only the fence", records)
	}
}

func TestAppendRejectsWriterOwnedFields(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	writer := openTestJournal(t, store)

	tests := []struct {
		name  string
		env   Envelope
		field string
	}{
		{
			name:  "opening fence is writer owned",
			env:   Envelope{Kind: EnvelopeKindOpeningFence, LeaseEpoch: 7},
			field: "kind",
		},
		{
			name: "application prefix epoch is writer owned",
			env: Envelope{
				Kind: EnvelopeKindApplicationPrefix, CommandID: "command-1", LeaseEpoch: 9,
				RuntimeCommandID: uuid.UUID{1}, CommandKind: "user_input",
			},
			field: "lease_epoch",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := writer.Append(context.Background(), tt.env)
			journalErr := requireJournalCode(t, err, JournalErrorInvalid)
			if journalErr.Field != tt.field {
				t.Fatalf("Field = %q, want %q", journalErr.Field, tt.field)
			}
		})
	}
	if records := readLedger(t, backend, journalName(t, store)); len(records) != 1 {
		t.Fatalf("ledger = %+v, want only the fence", records)
	}

	// A rejected envelope does not latch the writer.
	seq, err := writer.Append(context.Background(), Envelope{
		Kind: EnvelopeKindApplicationPrefix, CommandID: "command-1",
		RuntimeCommandID: uuid.UUID{1}, CommandKind: "user_input",
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seq != 2 {
		t.Fatalf("Append = %d, want 2", seq)
	}
	records := readLedger(t, backend, journalName(t, store))
	if len(records) != 2 || records[1].LeaseEpoch != writer.Epoch() {
		t.Fatalf("prefix epoch = %+v, want the writer's own epoch %d", records, writer.Epoch())
	}
}

func TestAppendOverflowsOversizedBodiesToObjects(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	store.overflowThreshold = 16
	writer := openTestJournal(t, store)

	big := `{"body":"` + strings.Repeat("x", 64) + `"}`
	small := `{"s":1}`
	seq, err := writer.Append(context.Background(), Envelope{
		Kind:    EnvelopeKindPublicEvent,
		EventID: "event-1",
		Public:  BodySlot{Inline: []byte(big)},
		Runtime: BodySlot{Inline: []byte(small)},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	records := readLedger(t, backend, journalName(t, store))
	stored := records[seq-1]
	if stored.Public.Reference == nil || stored.Public.Inline != nil {
		t.Fatalf("public slot = %+v, want an object reference", stored.Public)
	}
	if stored.Runtime.Inline == nil || stored.Runtime.Reference != nil {
		t.Fatalf("runtime slot = %+v, want the small body inline", stored.Runtime)
	}
	if want := sha256.Sum256([]byte(big)); stored.Public.Reference.SHA256 != want {
		t.Fatal("overflow reference digest does not match the body")
	}
	if stored.Public.Reference.SizeBytes != uint64(len(big)) {
		t.Fatalf("SizeBytes = %d, want %d", stored.Public.Reference.SizeBytes, len(big))
	}
	metadata, err := stored.Public.Reference.ObjectMetadata()
	if err != nil {
		t.Fatalf("ObjectMetadata: %v", err)
	}
	reader, err := store.GetObject(context.Background(), GetObjectRequest{
		TenantID: testTenant, SessionID: testSession, ExpectedKind: ObjectKindJournalPublic, Metadata: metadata,
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer reader.Close()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != big {
		t.Fatal("overflow object bytes differ from the appended body")
	}
}

func TestAppendPersistsAndVerifiesOverflowBeforeAppending(t *testing.T) {
	backend := memstore.New()
	log := &callLog{}
	blobs := &loggingBlobs{lifecycleBlobs: lifecycleBlobs{backend.Blobs}, log: log}
	backend.Blobs = blobs
	backend.Ledger = &scriptedLedger{Ledger: backend.Ledger, log: log}
	store := openJournalStore(t, backend)
	store.overflowThreshold = 8
	writer := openTestJournal(t, store)

	log.calls = nil
	if _, err := writer.Append(context.Background(), publicEvent("event-1", `{"body":"`+strings.Repeat("y", 40)+`"}`)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got, want := log.snapshot(), []string{"blob_put", "blob_get", "append"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("provider calls = %v, want %v (upload, re-verify, then append)", got, want)
	}
}

func TestFailedAppendLeavesAVerifiedOrphanObject(t *testing.T) {
	backend := memstore.New()
	base := backend.Ledger
	scripted := &scriptedLedger{Ledger: base}
	backend.Ledger = scripted
	store := openJournalStore(t, backend)
	store.overflowThreshold = 8
	writer := openTestJournal(t, store)
	name := journalName(t, store)

	scripted.appendFn = func(expected uint64, _ []byte) (bool, error) {
		return true, &storage.ConflictError{Name: name, Expected: expected}
	}
	body := `{"body":"` + strings.Repeat("z", 40) + `"}`
	_, err := writer.Append(context.Background(), publicEvent("event-1", body))
	requireJournalCode(t, err, JournalErrorFenced)

	scope, err := store.deriveSessionScope(testTenant, testSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	keys, err := backend.Blobs.List(context.Background(), scope.BlobPrefix)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("blob keys = %v, want exactly one orphan", keys)
	}
	reader, err := backend.Blobs.Get(context.Background(), keys[0])
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer reader.Close()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != body {
		t.Fatal("orphan bytes differ from the body that was uploaded")
	}
	if records := readLedger(t, backend, name); len(records) != 1 {
		t.Fatalf("ledger = %+v, want no record referencing the orphan", records)
	}
}

func TestAppendValidatesOversizedPublicBodyBeforeUploading(t *testing.T) {
	backend := memstore.New()
	log := &callLog{}
	blobs := &loggingBlobs{lifecycleBlobs: lifecycleBlobs{backend.Blobs}, log: log}
	backend.Blobs = blobs
	store := openJournalStore(t, backend)
	store.overflowThreshold = 8
	writer := openTestJournal(t, store)

	_, err := writer.Append(context.Background(), publicEvent("event-1", strings.Repeat("not json", 8)))
	var envelopeErr *EnvelopeError
	if !errors.As(err, &envelopeErr) {
		t.Fatalf("error = %T %v, want *EnvelopeError", err, err)
	}
	if got := log.count("blob_put"); got != 0 {
		t.Fatalf("blob_put called %d times, want 0 for an invalid public body", got)
	}
}

func TestCloseReleasesLeaseAndRefusesFurtherAppends(t *testing.T) {
	backend := memstore.New()
	store := openJournalStore(t, backend)
	writer, err := store.OpenJournal(context.Background(), OpenJournalRequest{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	_, err = writer.Append(context.Background(), publicEvent("event-1", `{"n":1}`))
	requireJournalCode(t, err, JournalErrorClosed)

	lease, err := backend.Leaser.Acquire(context.Background(), leaseName(t, store))
	if err != nil {
		t.Fatalf("re-acquire after Close: %v", err)
	}
	_ = lease.Release(context.Background())
}

func TestOpenJournalRejectsInvalidIdentitiesBeforeProvider(t *testing.T) {
	backend := memstore.New()
	log := &callLog{}
	backend.Ledger = &scriptedLedger{Ledger: backend.Ledger, log: log}
	store := openJournalStore(t, backend)

	tests := []struct {
		name    string
		request OpenJournalRequest
		field   string
	}{
		{name: "empty tenant", request: OpenJournalRequest{SessionID: testSession}, field: "TenantID"},
		{name: "empty session", request: OpenJournalRequest{TenantID: testTenant}, field: "SessionID"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := store.OpenJournal(context.Background(), tt.request)
			var identityErr *InvalidIdentityError
			if !errors.As(err, &identityErr) || identityErr.Field != tt.field {
				t.Fatalf("error = %T %v, want invalid %s", err, err, tt.field)
			}
		})
	}
	if got := log.snapshot(); len(got) != 0 {
		t.Fatalf("provider calls = %v, want none", got)
	}
}

func TestOpenJournalRefusesAfterStoreClose(t *testing.T) {
	backend := memstore.New()
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = store.OpenJournal(context.Background(), OpenJournalRequest{TenantID: testTenant, SessionID: testSession})
	var closed *StoreClosedError
	if !errors.As(err, &closed) {
		t.Fatalf("error = %T %v, want *StoreClosedError", err, err)
	}
}

// TestConcurrentWritersProduceNoGapsOrDuplicates is the stale-writer stress: many
// simultaneously live grants race on one ledger. Every accepted sequence must be
// claimed exactly once, and a writer that loses a race must stay failed.
func TestConcurrentWritersProduceNoGapsOrDuplicates(t *testing.T) {
	backend := memstore.New()
	backend.Leaser = newPermissiveLeaser()
	store := openJournalStore(t, backend)
	name := journalName(t, store)

	const writers = 8
	const appends = 12
	var mu sync.Mutex
	claimed := make(map[uint64]string)

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			writer, err := store.OpenJournal(context.Background(), OpenJournalRequest{TenantID: testTenant, SessionID: testSession})
			if err != nil {
				requireConcurrentFailure(t, err)
				return
			}
			defer writer.Close(context.Background())
			mu.Lock()
			if prior, ok := claimed[writer.Sequence()]; ok {
				mu.Unlock()
				t.Errorf("fence sequence %d claimed twice (%s and writer-%d)", writer.Sequence(), prior, w)
				return
			}
			claimed[writer.Sequence()] = fmt.Sprintf("writer-%d fence", w)
			mu.Unlock()

			for i := 0; i < appends; i++ {
				seq, err := writer.Append(context.Background(), publicEvent(fmt.Sprintf("event-%d-%d", w, i), `{"n":1}`))
				if err != nil {
					requireConcurrentFailure(t, err)
					// A failed writer must stay failed for every later append.
					if _, err := writer.Append(context.Background(), publicEvent(fmt.Sprintf("after-%d-%d", w, i), `{"n":1}`)); err == nil {
						t.Errorf("writer-%d append succeeded after a terminal failure", w)
					}
					return
				}
				mu.Lock()
				if prior, ok := claimed[seq]; ok {
					mu.Unlock()
					t.Errorf("sequence %d claimed twice (%s and writer-%d)", seq, prior, w)
					return
				}
				claimed[seq] = fmt.Sprintf("writer-%d event-%d", w, i)
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	tip, err := backend.Ledger.Tip(context.Background(), name)
	if err != nil {
		t.Fatalf("Tip: %v", err)
	}
	if uint64(len(claimed)) != tip {
		t.Fatalf("%d sequences claimed but the ledger holds %d records", len(claimed), tip)
	}
	for seq := uint64(1); seq <= tip; seq++ {
		if _, ok := claimed[seq]; !ok {
			t.Fatalf("sequence %d is durable but was never claimed", seq)
		}
	}
	records := readLedger(t, backend, name)
	seen := make(map[string]bool)
	for i, record := range records {
		identity := string(record.Kind) + "/" + string(record.EventID) + "/" + record.RecordID
		if record.Kind == EnvelopeKindOpeningFence {
			identity = fmt.Sprintf("fence/%d", record.LeaseEpoch)
		}
		if seen[identity] {
			t.Fatalf("record %d (%s) is durable twice", i+1, identity)
		}
		seen[identity] = true
	}
}

func requireConcurrentFailure(t *testing.T, err error) {
	t.Helper()
	var journalErr *JournalError
	if !errors.As(err, &journalErr) {
		t.Errorf("concurrent failure = %T %v, want *JournalError", err, err)
		return
	}
	switch journalErr.Code {
	case JournalErrorFenced, JournalErrorUnknown, JournalErrorLeaseLost, JournalErrorFailed:
	default:
		t.Errorf("concurrent failure code = %q, want a terminal ownership failure", journalErr.Code)
	}
}

// TestOpenJournalRaceWithPredecessorAppends stresses the ownership handoff: a
// predecessor keeps appending while openers race for the tip. Every successful
// open must own a fence that is genuinely the tip it claimed.
func TestOpenJournalRaceWithPredecessorAppends(t *testing.T) {
	backend := memstore.New()
	backend.Leaser = newPermissiveLeaser()
	store := openJournalStore(t, backend)
	name := journalName(t, store)

	// Bind the session and seed the ledger through a first legitimate writer.
	seed := openTestJournal(t, store)
	_ = seed

	stop := make(chan struct{})
	var predecessor sync.WaitGroup
	predecessor.Add(1)
	go func() {
		defer predecessor.Done()
		frame, err := EncodeEnvelope(runtimeControl("predecessor", `{"p":1}`))
		if err != nil {
			t.Error(err)
			return
		}
		for {
			select {
			case <-stop:
				return
			default:
			}
			tip, err := backend.Ledger.Tip(context.Background(), name)
			if err != nil {
				t.Error(err)
				return
			}
			_ = backend.Ledger.Append(context.Background(), name, tip, frame)
		}
	}()

	var wg sync.WaitGroup
	var mu sync.Mutex
	fences := make(map[uint64]uint64)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			writer, err := store.OpenJournal(context.Background(), OpenJournalRequest{TenantID: testTenant, SessionID: testSession})
			if err != nil {
				requireConcurrentFailure(t, err)
				return
			}
			defer writer.Close(context.Background())
			mu.Lock()
			defer mu.Unlock()
			if prior, ok := fences[writer.Sequence()]; ok {
				t.Errorf("fence sequence %d claimed by epochs %d and %d", writer.Sequence(), prior, writer.Epoch())
				return
			}
			fences[writer.Sequence()] = writer.Epoch()
		}()
	}
	wg.Wait()
	close(stop)
	predecessor.Wait()

	records := readLedger(t, backend, name)
	for seq, epoch := range fences {
		if seq == 0 || seq > uint64(len(records)) {
			t.Fatalf("fence sequence %d is outside the ledger", seq)
		}
		record := records[seq-1]
		if record.Kind != EnvelopeKindOpeningFence || record.LeaseEpoch != epoch {
			t.Fatalf("record at %d = %+v, want the epoch-%d fence its writer reported", seq, record, epoch)
		}
	}
}

// frameKind reports the envelope kind of an encoded frame, so a scripted ledger
// can single out the opening fence from the records that follow it.
func frameKind(payload []byte) EnvelopeKind {
	env, err := DecodeEnvelope(payload)
	if err != nil {
		return 0
	}
	return env.Kind
}

// TestDefiniteBackendAppendFailureIsRetryable pins the deliberate asymmetry in
// latching: a definite non-conflict failure left the tracked tip untouched, so
// the writer stays usable and the same record can simply be offered again.
func TestDefiniteBackendAppendFailureIsRetryable(t *testing.T) {
	backend := memstore.New()
	scripted := &scriptedLedger{Ledger: backend.Ledger}
	backend.Ledger = scripted
	store := openJournalStore(t, backend)
	writer := openTestJournal(t, store)

	scripted.appendFn = func(uint64, []byte) (bool, error) {
		scripted.appendFn = nil
		return true, errors.New("private backend failure")
	}
	if _, err := writer.Append(context.Background(), publicEvent("event-1", `{"n":1}`)); err == nil {
		t.Fatal("Append succeeded, want a backend failure")
	} else {
		requireJournalCode(t, err, JournalErrorBackend)
	}

	seq, err := writer.Append(context.Background(), publicEvent("event-1", `{"n":1}`))
	if err != nil {
		t.Fatalf("retry after a definite backend failure: %v", err)
	}
	if seq != 2 {
		t.Fatalf("retry sequence = %d, want 2", seq)
	}
}

// TestAppendRejectsPublicBodyAboveTheInlineCeiling covers the writer-side
// ceiling that the overflow path would otherwise carry an unbounded body past:
// EncodeEnvelope only sees the body while it is still inline.
func TestAppendRejectsPublicBodyAboveTheInlineCeiling(t *testing.T) {
	backend := memstore.New()
	log := &callLog{}
	blobs := &loggingBlobs{lifecycleBlobs: lifecycleBlobs{backend.Blobs}, log: log}
	backend.Blobs = blobs
	store := openJournalStore(t, backend)
	store.overflowThreshold = 16
	writer := openTestJournal(t, store)

	oversized := `{"body":"` + strings.Repeat("h", MaxInlineBodyBytes) + `"}`
	_, err := writer.Append(context.Background(), publicEvent("event-1", oversized))
	journalError := requireJournalCode(t, err, JournalErrorTooLarge)
	if journalError.Field != "public_body" {
		t.Fatalf("Field = %q, want public_body", journalError.Field)
	}
	if got := log.count("blob_put"); got != 0 {
		t.Fatalf("uploaded %d objects for a rejected body, want 0", got)
	}
	if records := readLedger(t, backend, journalName(t, store)); len(records) != 1 {
		t.Fatalf("ledger = %d records, want only the fence", len(records))
	}
}
