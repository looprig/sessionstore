package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func TestDispositionOversizedPayloadAndInlineBounds(t *testing.T) {
	s := openTestStore(t)
	createDispositionCatalog(t, s)
	req := dispositionRequest()
	req.Payload = bytes.Repeat([]byte("x"), MaxInboxPayloadBytes+1)
	if _, _, err := s.AdmitDispositionCommand(t.Context(), req); err == nil {
		t.Fatal("oversized inline accepted")
	}
	m := uploadCommandPayload(t, s, req.Payload)
	req.Payload = nil
	req.PayloadObject = &m
	entry, _, err := s.AdmitDispositionCommand(t.Context(), req)
	if err != nil || entry.Record.Descriptor.PayloadSize != MaxInboxPayloadBytes+1 {
		t.Fatalf("large object: %+v %v", entry, err)
	}
	req.Payload = []byte("two bodies")
	if _, _, err := s.AdmitDispositionCommand(t.Context(), req); err == nil {
		t.Fatal("two representations accepted")
	}
	for name, mutate := range map[string]func(*PutCommandPayloadRequest){
		"short":  func(r *PutCommandPayloadRequest) { r.SizeBytes++ },
		"long":   func(r *PutCommandPayloadRequest) { r.SizeBytes-- },
		"digest": func(r *PutCommandPayloadRequest) { r.SHA256 = sha256.Sum256([]byte("other")) },
		"nil":    func(r *PutCommandPayloadRequest) { r.Body = nil },
	} {
		t.Run(name, func(t *testing.T) {
			r := PutCommandPayloadRequest{TenantID: catalogTenant, SessionID: catalogSession, SizeBytes: 3, SHA256: sha256.Sum256([]byte("abc")), Body: bytes.NewReader([]byte("abc"))}
			mutate(&r)
			if _, err := s.PutCommandPayload(t.Context(), r); err == nil {
				t.Fatal("invalid upload accepted")
			}
		})
	}
}

func TestDispositionUnreadablePageContinues(t *testing.T) {
	backend := memstore.New()
	s := openStore(t, backend, WithControlShards(1))
	createDispositionCatalog(t, s)
	req := dispositionRequest()
	first, _, err := s.AdmitDispositionCommand(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	req.CommandID = "later"
	req.ApplyDeadline = req.ApplyDeadline.Add(time.Minute)
	second, _, err := s.AdmitDispositionCommand(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	scope, _ := s.deriveSessionScope(req.TenantID, req.SessionID)
	id := dispositionInboxID(scope, first.Record.Descriptor.CommandID)
	if _, err := backend.OrderedIndex.Update(t.Context(), id, first.Revision, []byte("corrupt"), storage.Rank{}, dispositionInboxDue(first.Record)); err != nil {
		t.Fatal(err)
	}
	page, err := s.ListDueDispositionCommands(t.Context(), ListDueDispositionCommandsRequest{Shard: 0, DueAtOrBefore: req.ApplyDeadline, Limit: 1})
	if err != nil || page.Examined != 1 || page.Unreadable != 1 || len(page.Commands) != 0 || page.NextCursor == "" {
		t.Fatalf("unreadable page: %+v %v", page, err)
	}
	page, err = s.ListDueDispositionCommands(t.Context(), ListDueDispositionCommandsRequest{Shard: 0, Cursor: page.NextCursor, Limit: 1})
	if err != nil || len(page.Commands) != 1 || !reflect.DeepEqual(page.Commands[0], second) {
		t.Fatalf("next: %+v %v", page, err)
	}
	if _, err := s.GetDispositionCommand(t.Context(), GetDispositionCommandRequest{TenantID: req.TenantID, SessionID: req.SessionID, CommandID: first.Record.Descriptor.CommandID}); err == nil {
		t.Fatal("corrupt exact read accepted")
	}
}

func TestDispositionEntryFilingValidation(t *testing.T) {
	s := openTestStore(t)
	createDispositionCatalog(t, s)
	req := dispositionRequest()
	entry, _, err := s.AdmitDispositionCommand(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	scope, _ := s.deriveSessionScope(req.TenantID, req.SessionID)
	stored, err := s.backend.OrderedIndex.Get(t.Context(), dispositionInboxID(scope, req.CommandID))
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*storage.OrderedRecord){
		"namespace":     func(r *storage.OrderedRecord) { r.ID.Namespace = inboxNamespace },
		"stable key":    func(r *storage.OrderedRecord) { r.ID.StableKey = "other" },
		"scope":         func(r *storage.OrderedRecord) { r.ID.OrderingScope = "other" },
		"ranking scope": func(r *storage.OrderedRecord) { r.RankingScope = "other" },
		"due":           func(r *storage.OrderedRecord) { r.Due = storage.Due{} },
		"order":         func(r *storage.OrderedRecord) { r.Order = 0 },
		"revision":      func(r *storage.OrderedRecord) { r.Revision = 0 },
		"deleted":       func(r *storage.OrderedRecord) { r.Deleted = true },
	} {
		t.Run(name, func(t *testing.T) {
			bad := stored
			mutate(&bad)
			if _, err := dispositionInboxEntryFor(bad, scope, req.TenantID, req.SessionID, req.CommandID, req.Binding); err == nil {
				t.Fatal("misfiled record accepted")
			}
		})
	}
	if entry.Record.Descriptor.Binding != req.Binding {
		t.Fatal("binding changed")
	}
}

type dispositionCancelCreate struct {
	storage.OrderedIndex
	cancel context.CancelFunc
}

func (f *dispositionCancelCreate) Create(ctx context.Context, id storage.OrderedID, rankScope string, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, bool, error) {
	r, c, err := f.OrderedIndex.Create(ctx, id, rankScope, value, rank, due)
	if strings.HasPrefix(id.Namespace, dispositionInboxNamespace) && f.cancel != nil {
		f.cancel()
		return storage.OrderedRecord{}, false, &storage.OrderedAmbiguousError{ID: id}
	}
	return r, c, err
}

func TestDispositionCanceledCommittedCreateRetainsWinner(t *testing.T) {
	backend := memstore.New()
	fault := &dispositionCancelCreate{OrderedIndex: backend.OrderedIndex}
	backend.OrderedIndex = fault
	s := openStore(t, backend)
	createDispositionCatalog(t, s)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	fault.cancel = cancel
	req := dispositionRequest()
	if _, _, err := s.AdmitDispositionCommand(ctx, req); err == nil {
		t.Fatal("ambiguous committed cancellation accepted")
	}
	fault.cancel = nil
	if err := s.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, backend)
	req.ProposedRuntimeCommandID = inboxRetryRuntime
	got, created, err := s.AdmitDispositionCommand(t.Context(), req)
	if err != nil || created || got.Record.Descriptor.RuntimeCommandID != inboxRuntime {
		t.Fatalf("retry: %+v %v %v", got, created, err)
	}
}
