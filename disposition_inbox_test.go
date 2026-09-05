package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func dispositionRequest() AdmitDispositionCommandRequest {
	return AdmitDispositionCommandRequest{TenantID: catalogTenant, SessionID: catalogSession, CommandID: "public/command:1", Binding: testSessionBinding(), ProposedRuntimeCommandID: inboxRuntime, Kind: inboxKind, Payload: bytes.Clone(inboxPayload), AcceptedAt: inboxAcceptedAt, ApplyDeadline: inboxDeadline}
}

func TestDispositionDuePagesAreBoundedAndSeparate(t *testing.T) {
	backend := memstore.New()
	s := openStore(t, backend, WithControlShards(1))
	createDispositionCatalog(t, s)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		req := dispositionRequest()
		req.CommandID = sessionwire.CommandID(fmt.Sprintf("command/%d", i))
		req.ApplyDeadline = req.ApplyDeadline.Add(-time.Duration(i) * time.Minute)
		if _, _, err := s.AdmitDispositionCommand(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	legacy, err := s.ListDueCommands(ctx, ListDueCommandsRequest{Shard: 0, DueAtOrBefore: inboxDeadline, Limit: 1})
	if err != nil || legacy.Examined != 0 {
		t.Fatalf("legacy saw new commands: %+v %v", legacy, err)
	}
	req := ListDueDispositionCommandsRequest{Shard: 0, DueAtOrBefore: inboxDeadline, Limit: 1}
	var orders []uint64
	for i := 0; i < 3; i++ {
		page, err := s.ListDueDispositionCommands(ctx, req)
		if err != nil || page.Examined != 1 || page.Limit != 1 || page.Unreadable != 0 || len(page.Commands) != 1 {
			t.Fatalf("page: %+v %v", page, err)
		}
		orders = append(orders, page.Commands[0].AcceptedOrder)
		if i < 2 && page.NextCursor == "" {
			t.Fatal("continuation missing")
		}
		if page.NextCursor != "" {
			_, err = s.ListDueCommands(ctx, ListDueCommandsRequest{Shard: 0, Cursor: page.NextCursor, Limit: 1})
			assertInboxCode(t, err, InboxErrorCursor)
		}
		req = ListDueDispositionCommandsRequest{Shard: 0, Cursor: page.NextCursor, Limit: 1}
	}
	if !(orders[0] > orders[1] && orders[1] > orders[2]) {
		t.Fatalf("due order incorrectly interpreted as accepted order: %v", orders)
	}
}

type dispositionAuthorityFault struct {
	storage.OrderedIndex
	fail    error
	corrupt bool
}

func (f *dispositionAuthorityFault) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	if id.Namespace == catalogNamespace {
		if f.fail != nil {
			return storage.OrderedRecord{}, f.fail
		}
		r, err := f.OrderedIndex.Get(ctx, id)
		if f.corrupt {
			r.Value = []byte("bad catalog")
		}
		return r, err
	}
	return f.OrderedIndex.Get(ctx, id)
}

func TestDispositionDueCatalogFailureAndCorruption(t *testing.T) {
	backend := memstore.New()
	fault := &dispositionAuthorityFault{OrderedIndex: backend.OrderedIndex}
	backend.OrderedIndex = fault
	s := openStore(t, backend, WithControlShards(1))
	createDispositionCatalog(t, s)
	if _, _, err := s.AdmitDispositionCommand(context.Background(), dispositionRequest()); err != nil {
		t.Fatal(err)
	}
	req := ListDueDispositionCommandsRequest{Shard: 0, DueAtOrBefore: inboxDeadline, Limit: 1}
	fault.fail = errors.New("catalog unavailable")
	if page, err := s.ListDueDispositionCommands(context.Background(), req); err == nil || len(page.Commands) != 0 {
		t.Fatalf("unavailable authority became success: %+v %v", page, err)
	}
	fault.fail = nil
	fault.corrupt = true
	page, err := s.ListDueDispositionCommands(context.Background(), req)
	if err != nil || page.Examined != 1 || page.Unreadable != 1 || len(page.Commands) != 0 {
		t.Fatalf("corrupt catalog not counted: %+v %v", page, err)
	}
	fault.corrupt = false
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ListDueDispositionCommands(ctx, req); err == nil {
		t.Fatal("cancellation accepted")
	}
}

func createDispositionCatalog(t *testing.T, s *Store) {
	t.Helper()
	req := testCreateRequest()
	req.Binding = testSessionBinding()
	if _, _, err := s.CreateCatalogEntry(context.Background(), req); err != nil {
		t.Fatal(err)
	}
}

func uploadCommandPayload(t *testing.T, s *Store, body []byte) sessionwire.ObjectMetadata {
	t.Helper()
	m, err := s.PutCommandPayload(context.Background(), PutCommandPayloadRequest{TenantID: catalogTenant, SessionID: catalogSession, SizeBytes: uint64(len(body)), SHA256: sha256.Sum256(body), MediaType: "application/octet-stream", Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestDispositionAdmissionWinningIdentitySurvivesRestart(t *testing.T) {
	for _, objectFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "inline winner", true: "object winner"}[objectFirst], func(t *testing.T) {
			ctx := context.Background()
			backend := memstore.New()
			s := openStore(t, backend)
			createDispositionCatalog(t, s)
			req := dispositionRequest()
			firstUpload := uploadCommandPayload(t, s, req.Payload)
			secondUpload := uploadCommandPayload(t, s, req.Payload)
			if firstUpload.Reference == secondUpload.Reference {
				t.Fatal("independent uploads shared generation")
			}
			if objectFirst {
				req.Payload = nil
				req.PayloadObject = &firstUpload
			}
			want, created, err := s.AdmitDispositionCommand(ctx, req)
			if err != nil || !created {
				t.Fatalf("admit: %+v %v %v", want, created, err)
			}
			digest := sha256.Sum256(inboxPayload)
			if want.Record.Descriptor.PayloadDigest != hex.EncodeToString(digest[:]) || want.Record.Descriptor.PayloadSize != uint64(len(inboxPayload)) || want.Record.State != InboxStatePending || want.AcceptedOrder == 0 {
				t.Fatalf("descriptor: %+v", want)
			}
			if err := s.Close(ctx); err != nil {
				t.Fatal(err)
			}
			s = openStore(t, backend)
			req.ProposedRuntimeCommandID = inboxRetryRuntime
			req.AcceptedAt = req.AcceptedAt.Add(time.Hour)
			req.ApplyDeadline = req.ApplyDeadline.Add(time.Hour)
			for _, object := range []*sessionwire.ObjectMetadata{&secondUpload, nil} {
				req.PayloadObject = object
				req.Payload = nil
				if object == nil {
					req.Payload = bytes.Clone(inboxPayload)
				}
				got, created, err := s.AdmitDispositionCommand(ctx, req)
				if err != nil || created || !reflect.DeepEqual(got, want) {
					t.Fatalf("retry lost winner: %+v %v %v; want %+v", got, created, err, want)
				}
			}
			got, err := s.GetDispositionCommand(ctx, GetDispositionCommandRequest{TenantID: req.TenantID, SessionID: req.SessionID, CommandID: req.CommandID})
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("read: %+v %v", got, err)
			}
		})
	}
}

func TestDispositionAdmissionEmptyIdentityAndConflicts(t *testing.T) {
	s := openTestStore(t)
	createDispositionCatalog(t, s)
	req := dispositionRequest()
	req.Payload = nil
	want, _, err := s.AdmitDispositionCommand(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(nil)
	if want.Record.Descriptor.PayloadDigest != hex.EncodeToString(digest[:]) || want.Record.Descriptor.PayloadSize != 0 {
		t.Fatal("empty identity missing")
	}
	for name, mutate := range map[string]func(*AdmitDispositionCommandRequest){
		"content": func(r *AdmitDispositionCommandRequest) { r.Payload = []byte("different") },
		"kind":    func(r *AdmitDispositionCommandRequest) { r.Kind = "another" },
		"binding": func(r *AdmitDispositionCommandRequest) { r.Binding.BindingVersion = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			retry := req
			mutate(&retry)
			_, _, err := s.AdmitDispositionCommand(context.Background(), retry)
			if err == nil {
				t.Fatal("conflict accepted")
			}
		})
	}
}
