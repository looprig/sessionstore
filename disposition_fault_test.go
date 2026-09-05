package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func TestDispositionRequiresActualCatalog(t *testing.T) {
	for _, mode := range []string{"absent", "witness only", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			s := openTestStore(t)
			ctx := context.Background()
			req := dispositionRequest()
			if mode == "witness only" {
				scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
				if err != nil {
					t.Fatal(err)
				}
				if err := s.bindSessionScopeMode(ctx, scope, ProtocolModeDisposition); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "legacy" {
				if _, _, err := s.CreateCatalogEntry(ctx, testCreateRequest()); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := s.AdmitDispositionCommand(ctx, req); err == nil {
				t.Fatal("admission without actual disposition catalog")
			}
			if _, err := s.GetDispositionCommand(ctx, GetDispositionCommandRequest{TenantID: req.TenantID, SessionID: req.SessionID, CommandID: req.CommandID}); err == nil {
				t.Fatal("read without authority")
			}
			if _, err := s.PutCommandPayload(ctx, PutCommandPayloadRequest{TenantID: req.TenantID, SessionID: req.SessionID, Body: bytes.NewReader(nil), SHA256: sha256.Sum256(nil)}); err == nil {
				t.Fatal("upload without actual disposition catalog")
			}
		})
	}
}

func TestDispositionMetadataAuthorityAndBoundedReads(t *testing.T) {
	backend, calls := instrumentComposite(memstore.New())
	s := openStore(t, backend)
	createDispositionCatalog(t, s)
	req := dispositionRequest()
	metadata := uploadCommandPayload(t, s, req.Payload)
	req.Payload = nil
	req.PayloadObject = &metadata
	for _, change := range []string{"size", "media", "scope", "kind"} {
		t.Run(change, func(t *testing.T) {
			r := req
			m := metadata
			r.PayloadObject = &m
			switch change {
			case "size":
				m.SizeBytes++
			case "media":
				m.MediaType = "text/plain"
			case "scope":
				r.SessionID = "other-session"
			case "kind":
				parsed, _ := parseObjectMetadata(m)
				m = objectMetadataFor(ObjectKindToolResult, [16]byte{1}, parsed.size, parsed.digest, m.MediaType)
			}
			if _, _, err := s.AdmitDispositionCommand(t.Context(), r); err == nil {
				t.Fatal("unverified metadata accepted")
			}
		})
	}
	before := calls.snapshot()
	entry, _, err := s.AdmitDispositionCommand(t.Context(), req)
	after := calls.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if after.Blobs != before.Blobs || after.Ledger != before.Ledger || after.Leaser != before.Leaser || after.Ordered-before.Ordered != 2 || after.KV-before.KV > 10 {
		t.Fatalf("admission cost: %+v -> %+v", before, after)
	}
	before = calls.snapshot()
	got, err := s.GetDispositionCommand(t.Context(), GetDispositionCommandRequest{TenantID: req.TenantID, SessionID: req.SessionID, CommandID: req.CommandID})
	after = calls.snapshot()
	if err != nil || !reflect.DeepEqual(got, entry) {
		t.Fatalf("read: %+v %v", got, err)
	}
	if after.Ordered-before.Ordered != 2 || after.KV-before.KV != 2 || after.Blobs != before.Blobs || after.Ledger != before.Ledger {
		t.Fatalf("exact read cost: %+v -> %+v", before, after)
	}
	// Removing the body out of band does not turn indexed metadata into a body
	// existence proof. Admission retains the winner; actual consumption fails.
	scope, _ := s.deriveSessionScope(req.TenantID, req.SessionID)
	parsed, _ := parseObjectMetadata(metadata)
	if err := backend.Blobs.Delete(t.Context(), objectKey(scope, parsed)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AdmitDispositionCommand(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetObject(t.Context(), GetObjectRequest{TenantID: req.TenantID, SessionID: req.SessionID, ExpectedKind: ObjectKindCommandPayload, Metadata: metadata}); err == nil {
		t.Fatal("deleted body considered present")
	}
}

func TestDispositionAdmissionAmbiguousAndCanceledRestart(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "prewrite", true: "committed unknown"}[committed], func(t *testing.T) {
			backend := memstore.New()
			fault := &bindingCreateFault{OrderedIndex: backend.OrderedIndex, ambiguous: committed}
			backend.OrderedIndex = fault
			s := openStore(t, backend)
			createDispositionCatalog(t, s)
			req := dispositionRequest()
			fault.fail = true
			if _, _, err := s.AdmitDispositionCommand(t.Context(), req); err == nil {
				t.Fatal("fault ignored")
			}
			if err := s.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			s = openStore(t, backend)
			retry := req
			retry.ProposedRuntimeCommandID = inboxRetryRuntime
			entry, created, err := s.AdmitDispositionCommand(t.Context(), retry)
			if err != nil || created == committed {
				t.Fatalf("retry: %+v %v %v", entry, created, err)
			}
			want := retry.ProposedRuntimeCommandID
			if committed {
				want = req.ProposedRuntimeCommandID
			}
			if entry.Record.Descriptor.RuntimeCommandID != want {
				t.Fatal("wrong winning mapping")
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			retry.CommandID = "canceled"
			if _, _, err := s.AdmitDispositionCommand(ctx, retry); err == nil || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel: %v", err)
			}
			if _, err := s.GetDispositionCommand(t.Context(), GetDispositionCommandRequest{TenantID: retry.TenantID, SessionID: retry.SessionID, CommandID: retry.CommandID}); err == nil {
				t.Fatal("canceled call created command")
			}
		})
	}
}

func TestDispositionRacingReplicasRetainSingleWinner(t *testing.T) {
	backend := memstore.New()
	first := openStore(t, backend)
	second := openStore(t, backend)
	createDispositionCatalog(t, first)
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan DispositionInboxEntry, 12)
	created := make(chan bool, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			s := first
			if i%2 == 1 {
				s = second
			}
			req := dispositionRequest()
			if i%2 == 1 {
				req.ProposedRuntimeCommandID = inboxRetryRuntime
			}
			m := uploadCommandPayload(t, s, req.Payload)
			req.Payload = nil
			req.PayloadObject = &m
			entry, new, err := s.AdmitDispositionCommand(t.Context(), req)
			if err != nil {
				t.Error(err)
				return
			}
			results <- entry
			created <- new
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(created)
	count := 0
	for c := range created {
		if c {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("creators=%d", count)
	}
	var winner *DispositionInboxEntry
	for entry := range results {
		if winner == nil {
			copy := entry
			winner = &copy
		} else if !reflect.DeepEqual(entry, *winner) {
			t.Fatal("replicas disagree on winner")
		}
	}
}

func TestDispositionUploadPreservesLegacyFences(t *testing.T) {
	s := openTestStore(t)
	createDispositionCatalog(t, s)
	uploadCommandPayload(t, s, []byte("payload"))
	for _, kind := range allObjectKinds(t) {
		t.Run(string(kind), func(t *testing.T) {
			_, err := s.PutObject(t.Context(), PutObjectRequest{TenantID: catalogTenant, SessionID: catalogSession, Kind: kind, Body: bytes.NewReader(nil), SHA256: sha256.Sum256(nil)})
			if err == nil {
				t.Fatal("legacy object writer entered disposition mode")
			}
		})
	}
	for name, write := range legacyBindingWriters(t) {
		t.Run(name, func(t *testing.T) {
			if err := write(s); err == nil {
				t.Fatal("legacy writer entered new session")
			}
		})
	}
	legacy := openTestStore(t)
	if _, _, err := legacy.CreateCatalogEntry(t.Context(), testCreateRequest()); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.PutCommandPayload(t.Context(), PutCommandPayloadRequest{TenantID: catalogTenant, SessionID: catalogSession, Body: bytes.NewReader(nil), SHA256: sha256.Sum256(nil)}); err == nil {
		t.Fatal("new upload adopted legacy session")
	}
}

// Compile-time use ensures fault wrappers preserve the real provider boundary.
var _ storage.OrderedIndex = (*dispositionAuthorityFault)(nil)
