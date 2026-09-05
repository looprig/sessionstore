package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func TestObjectMetadataSurvivesReopenEveryKind(t *testing.T) {
	for _, kind := range allObjectKinds(t) {
		t.Run(string(kind), func(t *testing.T) {
			ctx := context.Background()
			backend := memstore.New()
			store, err := Open(ctx, backend)
			if err != nil {
				t.Fatal(err)
			}
			body := []byte("retained bytes")
			var results []sessionwire.ObjectMetadata
			for range 2 {
				metadata, err := store.PutObject(ctx, PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: kind, SizeBytes: uint64(len(body)), SHA256: sha256.Sum256(body), MediaType: "text/plain", Body: bytes.NewReader(body)})
				if err != nil {
					t.Fatal(err)
				}
				results = append(results, metadata)
			}
			if results[0].Reference == results[1].Reference {
				t.Fatal("same bytes reused a generation")
			}
			if err := store.Close(ctx); err != nil {
				t.Fatal(err)
			}
			store, err = Open(ctx, backend)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close(ctx)
			for _, want := range results {
				got, err := store.GetObjectMetadata(ctx, GetObjectMetadataRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: kind, Reference: want.Reference})
				if err != nil || got != want {
					t.Fatalf("lookup = %+v, %v; want %+v", got, err, want)
				}
				reader, err := store.GetObject(ctx, GetObjectRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: kind, Metadata: got})
				if err != nil {
					t.Fatal(err)
				}
				data, readErr := io.ReadAll(reader)
				closeErr := reader.Close()
				if readErr != nil || closeErr != nil || !bytes.Equal(data, body) {
					t.Fatalf("read=%q, %v; close=%v", data, readErr, closeErr)
				}
			}
		})
	}
}

func metadataFixture(t *testing.T, backend *storage.Composite) (*Store, sessionwire.ObjectMetadata, GetObjectMetadataRequest) {
	t.Helper()
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	body := []byte("retained bytes")
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindToolResult, SizeBytes: uint64(len(body)), SHA256: sha256.Sum256(body), MediaType: "text/plain", Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	return store, metadata, GetObjectMetadataRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: ObjectKindToolResult, Reference: metadata.Reference}
}

func TestObjectMetadataLookupBoundAndPreIOValidation(t *testing.T) {
	backend, calls := instrumentComposite(memstore.New())
	store, want, req := metadataFixture(t, backend)
	before := calls.snapshot()
	got, err := store.GetObjectMetadata(context.Background(), req)
	after := calls.snapshot()
	if err != nil || got != want {
		t.Fatalf("lookup = %+v, %v", got, err)
	}
	before.KV += 3 // two collision witnesses, one exact metadata read
	if after != before {
		t.Fatalf("provider work=%+v; want %+v", after, before)
	}
	for name, change := range map[string]func(*GetObjectMetadataRequest){
		"tenant":    func(r *GetObjectMetadataRequest) { r.TenantID = "" },
		"session":   func(r *GetObjectMetadataRequest) { r.SessionID = "" },
		"kind":      func(r *GetObjectMetadataRequest) { r.ExpectedKind = "unknown" },
		"mismatch":  func(r *GetObjectMetadataRequest) { r.ExpectedKind = ObjectKindArtifact },
		"reference": func(r *GetObjectMetadataRequest) { r.Reference.ObjectID = "not-a-reference" },
		"overlong":  func(r *GetObjectMetadataRequest) { r.Reference.ObjectID = strings.Repeat("x", 100000) },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := req
			change(&invalid)
			before := calls.snapshot()
			got, err := store.GetObjectMetadata(context.Background(), invalid)
			if err == nil || got != (sessionwire.ObjectMetadata{}) {
				t.Fatalf("invalid lookup = %+v, %v", got, err)
			}
			if calls.snapshot() != before {
				t.Fatal("invalid request touched provider")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before = calls.snapshot()
	if _, err := store.GetObjectMetadata(ctx, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled lookup = %v", err)
	}
	if calls.snapshot() != before {
		t.Fatal("already canceled lookup touched provider")
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetObjectMetadata(context.Background(), req); !errors.As(err, new(*StoreClosedError)) {
		t.Fatalf("closed lookup = %v", err)
	}
}

func TestObjectMetadataMissingIndexPreservesExplicitGetAndStaleIndexIsNotBodyProof(t *testing.T) {
	for _, removeIndex := range []bool{true, false} {
		t.Run(map[bool]string{true: "pre-index", false: "deleted-body"}[removeIndex], func(t *testing.T) {
			ctx := context.Background()
			backend := memstore.New()
			store, metadata, req := metadataFixture(t, backend)
			scope, _ := store.deriveSessionScope(req.TenantID, req.SessionID)
			parsed, _ := parseObjectMetadata(metadata)
			if removeIndex {
				if err := backend.KV.Delete(ctx, objectMetadataKey(scope, parsed)); err != nil {
					t.Fatal(err)
				}
			} else if err := store.deleteObject(ctx, req.TenantID, req.SessionID, req.ExpectedKind, req.Reference); err != nil {
				t.Fatal(err)
			}
			got, lookupErr := store.GetObjectMetadata(ctx, req)
			reader, readErr := store.GetObject(ctx, GetObjectRequest{TenantID: req.TenantID, SessionID: req.SessionID, ExpectedKind: req.ExpectedKind, Metadata: metadata})
			if removeIndex {
				var oe *ObjectError
				if !errors.As(lookupErr, &oe) || oe.Code != ObjectErrorMetadataUnavailable || oe.Field != "metadata" || got != (sessionwire.ObjectMetadata{}) {
					t.Fatalf("missing metadata = %+v, %v", got, lookupErr)
				}
				if !errors.As(lookupErr, new(*storage.KeyNotFoundError)) {
					t.Fatal("missing metadata lost provider cause")
				}
				if readErr != nil {
					t.Fatal(readErr)
				}
				_, err := io.Copy(io.Discard, reader)
				closeErr := reader.Close()
				if err != nil || closeErr != nil {
					t.Fatalf("legacy read = %v, %v", err, closeErr)
				}
			} else {
				if lookupErr != nil || got != metadata {
					t.Fatalf("stale index = %+v, %v", got, lookupErr)
				}
				if !errors.As(readErr, new(*storage.BlobNotFoundError)) || reader != nil {
					t.Fatalf("deleted body = %v", readErr)
				}
			}
		})
	}
}

func TestObjectMetadataScopeAndStoredIdentityFailClosed(t *testing.T) {
	backend := memstore.New()
	store, metadata, req := metadataFixture(t, backend)
	ctx := context.Background()
	for _, other := range []GetObjectMetadataRequest{
		{TenantID: "other", SessionID: req.SessionID, ExpectedKind: req.ExpectedKind, Reference: req.Reference},
		{TenantID: req.TenantID, SessionID: "other", ExpectedKind: req.ExpectedKind, Reference: req.Reference},
	} {
		scope, _ := store.deriveSessionScope(other.TenantID, other.SessionID)
		if err := store.bindSessionScope(ctx, scope); err != nil {
			t.Fatal(err)
		}
		_, err := store.GetObjectMetadata(ctx, other)
		var oe *ObjectError
		if !errors.As(err, &oe) || oe.Code != ObjectErrorMetadataUnavailable {
			t.Fatalf("cross scope = %v", err)
		}
		// A copied valid record must not authorize a different scope, even if
		// a faulty provider serves it under the requested physical key.
		parsed, _ := parseObjectMetadata(metadata)
		data := encodeObjectMetadataRecord(req.TenantID, req.SessionID, metadata)
		if _, err := backend.KV.Put(ctx, objectMetadataKey(scope, parsed), 0, data); err != nil {
			t.Fatal(err)
		}
		_, err = store.GetObjectMetadata(ctx, other)
		if !errors.As(err, &oe) || oe.Code != ObjectErrorIntegrity {
			t.Fatalf("copied metadata = %v", err)
		}
	}
}

func TestObjectMetadataCodecRejectsCorruption(t *testing.T) {
	backend := memstore.New()
	store, metadata, req := metadataFixture(t, backend)
	valid := encodeObjectMetadataRecord(req.TenantID, req.SessionID, metadata)
	cases := map[string][]byte{
		"trailing": append(bytes.Clone(valid), 0),
		"oversize": bytes.Repeat([]byte{0}, maxObjectMetadataRecordBytes+1),
	}
	for i := range len(valid) {
		cases[fmt.Sprintf("truncated-%d", i)] = bytes.Clone(valid[:i])
	}
	for name, offset := range map[string]int{"magic": 0, "version": 4} {
		data := bytes.Clone(valid)
		data[offset] ^= 0xff
		cases[name] = data
	}
	data := bytes.Clone(valid)
	binary.BigEndian.PutUint16(data[13:15], 257)
	cases["field-limit"] = data
	for name, change := range map[string]func(*sessionwire.ObjectMetadata){
		"digest": func(m *sessionwire.ObjectMetadata) { m.Digest = "sha256:bad" },
		"media":  func(m *sessionwire.ObjectMetadata) { m.MediaType = "invalid media type" },
		"ref":    func(m *sessionwire.ObjectMetadata) { m.Reference.ObjectID = "invalid" },
		"different-ref": func(m *sessionwire.ObjectMetadata) {
			*m = objectMetadataFor(ObjectKindToolResult, [16]byte{8}, m.SizeBytes, sha256.Sum256([]byte("retained bytes")), m.MediaType)
		},
		"different-kind": func(m *sessionwire.ObjectMetadata) {
			*m = objectMetadataFor(ObjectKindArtifact, [16]byte{8}, m.SizeBytes, sha256.Sum256([]byte("retained bytes")), m.MediaType)
		},
	} {
		m := metadata
		change(&m)
		cases[name] = encodeObjectMetadataRecord(req.TenantID, req.SessionID, m)
	}
	scope, _ := store.deriveSessionScope(req.TenantID, req.SessionID)
	parsed, _ := parseObjectMetadata(metadata)
	key := objectMetadataKey(scope, parsed)
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			_, rev, err := backend.KV.Get(context.Background(), key)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = backend.KV.Put(context.Background(), key, rev, bad); err != nil {
				t.Fatal(err)
			}
			got, err := store.GetObjectMetadata(context.Background(), req)
			var oe *ObjectError
			if !errors.As(err, &oe) || oe.Code != ObjectErrorIntegrity || got != (sessionwire.ObjectMetadata{}) {
				t.Fatalf("corrupt record accepted: %+v, %v", got, err)
			}
		})
	}
}

// The real memstore owns all data; this wrapper injects faults only at the
// metadata boundary and forbids an enumeration fallback.
type metadataFaultKV struct {
	storage.KV
	put func(context.Context, string, uint64, []byte) (uint64, error)
	get func(context.Context, string) ([]byte, uint64, error)
	log *callLog
}

func (k *metadataFaultKV) Put(ctx context.Context, key string, rev uint64, value []byte) (uint64, error) {
	if strings.Contains(key, "/object-metadata/") {
		if k.log != nil {
			k.log.add("metadata_put")
		}
		if k.put != nil {
			return k.put(ctx, key, rev, value)
		}
	}
	return k.KV.Put(ctx, key, rev, value)
}

func (k *metadataFaultKV) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	if strings.Contains(key, "/object-metadata/") {
		if k.log != nil {
			k.log.add("metadata_get")
		}
		if k.get != nil {
			return k.get(ctx, key)
		}
	}
	return k.KV.Get(ctx, key)
}

func (*metadataFaultKV) Keys(context.Context, string) ([]string, error) {
	panic("metadata operation must not enumerate KV")
}

func TestObjectMetadataCommitFailuresAndCanonicalWinner(t *testing.T) {
	for _, mode := range []string{"before-commit", "commit-then-error", "same-winner-conflict", "different-winner", "corrupt-winner", "read-fails", "cancel-after-commit"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			base := memstore.New()
			sentinel := errors.New("private metadata backend failure")
			log := &callLog{}
			var createdKey string
			var createdValue []byte
			kv := &metadataFaultKV{KV: base.KV, log: log}
			kv.put = func(ctx context.Context, key string, rev uint64, value []byte) (uint64, error) {
				if rev != 0 {
					t.Fatal("metadata write must be create-only")
				}
				createdKey, createdValue = key, bytes.Clone(value)
				if mode == "before-commit" || mode == "read-fails" {
					return 0, sentinel
				}
				if mode == "different-winner" {
					value = bytes.Clone(value)
					value[12] ^= 1
				}
				if mode == "corrupt-winner" {
					value = []byte("corrupt")
				}
				if _, err := base.KV.Put(ctx, key, 0, value); err != nil {
					t.Fatal(err)
				}
				if mode == "same-winner-conflict" {
					return 0, &storage.ConflictError{Name: key, Expected: 0}
				}
				if mode == "cancel-after-commit" {
					cancel()
					return 0, context.Canceled
				}
				return 0, sentinel
			}
			if mode == "read-fails" {
				kv.get = func(context.Context, string) ([]byte, uint64, error) { return nil, 0, sentinel }
			}
			blobs := &countingBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}, log: log}
			backend := &storage.Composite{Ledger: base.Ledger, Leaser: base.Leaser, KV: kv, OrderedIndex: base.OrderedIndex, Blobs: blobs}
			store, err := Open(ctx, backend)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close(context.Background())
			body := []byte("payload")
			got, err := store.PutObject(ctx, PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: sha256.Sum256(body), Body: bytes.NewReader(body)})
			success := mode == "commit-then-error" || mode == "same-winner-conflict"
			if success {
				if err != nil || got.Reference.ObjectID == "" {
					t.Fatalf("resolved winner = %+v, %v", got, err)
				}
				lookup, err := store.GetObjectMetadata(context.Background(), GetObjectMetadataRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: ObjectKindArtifact, Reference: got.Reference})
				if err != nil || lookup != got {
					t.Fatalf("winner lookup = %+v, %v", lookup, err)
				}
			} else {
				var oe *ObjectError
				if !errors.As(err, &oe) || got != (sessionwire.ObjectMetadata{}) {
					t.Fatalf("failed commit returned metadata: %+v, %v", got, err)
				}
				wantCode := ObjectErrorBackend
				if mode == "different-winner" || mode == "corrupt-winner" {
					wantCode = ObjectErrorConflict
				}
				if oe.Code != wantCode || oe.Field != "metadata_commit" {
					t.Fatalf("failure = %+v", oe)
				}
				if strings.Contains(err.Error(), sentinel.Error()) {
					t.Fatal("error leaked backend detail")
				}
			}
			calls := log.snapshot()
			wantCalls := []string{"blob_put", "blob_get", "metadata_put", "metadata_get"}
			if mode == "cancel-after-commit" {
				wantCalls = wantCalls[:3]
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled commit = %v", err)
				}
			}
			if len(calls) < len(wantCalls) || strings.Join(calls[:len(wantCalls)], ",") != strings.Join(wantCalls, ",") {
				t.Fatalf("write order = %v", calls)
			}
			if !success && len(calls) != len(wantCalls) {
				t.Fatalf("ambiguous resolution retried or scanned: %v", calls)
			}
			if createdKey == "" || len(createdValue) == 0 {
				t.Fatal("test did not reach metadata persistence")
			}
			scope, _ := store.deriveSessionScope("tenant", "session")
			keys, err := base.Blobs.List(context.Background(), scope.BlobPrefix)
			if err != nil || len(keys) != 1 {
				t.Fatalf("committed orphan was lost: %v, %v", keys, err)
			}
		})
	}
}

func TestObjectMetadataNeverPrecedesBlobVerification(t *testing.T) {
	for _, failure := range []string{"put", "post-read", "post-close", "digest"} {
		t.Run(failure, func(t *testing.T) {
			base := memstore.New()
			log := &callLog{}
			kv := &metadataFaultKV{KV: base.KV, log: log}
			blobs := &scriptedBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}}
			sentinel := errors.New("blob boundary fault")
			body := []byte("payload")
			switch failure {
			case "put":
				blobs.putFn = func(key string, r io.Reader) error {
					if err := base.Blobs.Put(context.Background(), key, r); err != nil {
						t.Fatal(err)
					}
					return sentinel
				}
			case "post-read":
				blobs.getFn = func(string) (io.ReadCloser, error) { return nil, sentinel }
			case "post-close":
				blobs.getFn = func(string) (io.ReadCloser, error) {
					return &readCloser{Reader: bytes.NewReader(body), closeErr: sentinel}, nil
				}
			case "digest":
				blobs.getFn = func(string) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("garbage")), nil }
			}
			backend := &storage.Composite{Ledger: base.Ledger, Leaser: base.Leaser, KV: kv, OrderedIndex: base.OrderedIndex, Blobs: blobs}
			store, err := Open(context.Background(), backend)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close(context.Background())
			got, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: sha256.Sum256(body), Body: bytes.NewReader(body)})
			if err == nil || got != (sessionwire.ObjectMetadata{}) {
				t.Fatalf("unverified put = %+v, %v", got, err)
			}
			if calls := log.snapshot(); len(calls) != 0 {
				t.Fatalf("unverified blob published index: %v", calls)
			}
		})
	}
}

func TestObjectMetadataLookupCancellationAndShutdownReleaseAdmission(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprintf("shutdown-%v", shutdown), func(t *testing.T) {
			base := memstore.New()
			kv := &metadataFaultKV{KV: base.KV}
			backend := &storage.Composite{Ledger: base.Ledger, Leaser: base.Leaser, KV: kv, OrderedIndex: base.OrderedIndex, Blobs: base.Blobs}
			store, _, req := metadataFixture(t, backend)
			entered := make(chan struct{})
			kv.get = func(ctx context.Context, _ string) ([]byte, uint64, error) {
				close(entered)
				<-ctx.Done()
				return nil, 0, ctx.Err()
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := store.GetObjectMetadata(ctx, req); done <- err }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("lookup did not enter KV")
			}
			if shutdown {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				if err := store.Close(closeCtx); err != nil {
					t.Fatal(err)
				}
			} else {
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled KV = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("lookup did not release")
			}
		})
	}
}

func TestObjectMetadataPreservesMaximumAcceptedInputs(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	tenant := sessionwire.TenantID(strings.Repeat("t", sessionwire.MaxIDBytes))
	session := sessionwire.SessionID(strings.Repeat("s", sessionwire.MaxIDBytes))
	media := "text/plain;x=" + strings.Repeat("a", 256-len("text/plain;x="))
	for _, media := range []string{"", "token", media} {
		got, err := store.PutObject(ctx, PutObjectRequest{TenantID: tenant, SessionID: session, Kind: ObjectKindArtifact, SHA256: sha256.Sum256(nil), MediaType: media, Body: bytes.NewReader(nil)})
		if err != nil {
			t.Fatalf("previously accepted input refused: %v", err)
		}
		want, err := store.GetObjectMetadata(ctx, GetObjectMetadataRequest{TenantID: tenant, SessionID: session, ExpectedKind: ObjectKindArtifact, Reference: got.Reference})
		if err != nil || got != want {
			t.Fatalf("maximum input lookup = %+v, %v", want, err)
		}
	}
}

func TestObjectMetadataPutRetainsDispositionFence(t *testing.T) {
	base := memstore.New()
	log := &callLog{}
	kv := &metadataFaultKV{KV: base.KV, log: log}
	blobs := &countingBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}, log: log}
	backend := &storage.Composite{Ledger: base.Ledger, Leaser: base.Leaser, KV: kv, OrderedIndex: base.OrderedIndex, Blobs: blobs}
	store := openStore(t, backend)
	req := testCreateRequest()
	req.Binding = testSessionBinding()
	if _, _, err := store.CreateCatalogEntry(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	got, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: req.TenantID, SessionID: req.SessionID, Kind: ObjectKindArtifact, SHA256: sha256.Sum256(nil), Body: bytes.NewReader(nil)})
	assertCatalogCode(t, err, CatalogErrorConflict)
	if got != (sessionwire.ObjectMetadata{}) || len(log.snapshot()) != 0 {
		t.Fatalf("disposition session mutated: %+v, %v", got, log.snapshot())
	}
}

func TestObjectMetadataLegacyLayoutLookup(t *testing.T) {
	store := openStore(t, memstore.New(), WithLegacySingleTenant("local"))
	ctx := context.Background()
	session := sessionwire.SessionID("123e4567-e89b-12d3-a456-426614174000")
	metadata, err := store.PutObject(ctx, PutObjectRequest{TenantID: "local", SessionID: session, Kind: ObjectKindArtifact, SHA256: sha256.Sum256(nil), Body: bytes.NewReader(nil)})
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.GetObjectMetadata(ctx, GetObjectMetadataRequest{TenantID: "local", SessionID: session, ExpectedKind: ObjectKindArtifact, Reference: metadata.Reference})
	if err != nil || got != metadata {
		t.Fatalf("legacy lookup = %+v, %v", got, err)
	}
}

func TestObjectMetadataBackendFailureIsNotUnavailable(t *testing.T) {
	base := memstore.New()
	kv := &metadataFaultKV{KV: base.KV}
	backend := &storage.Composite{Ledger: base.Ledger, Leaser: base.Leaser, KV: kv, OrderedIndex: base.OrderedIndex, Blobs: base.Blobs}
	store, _, req := metadataFixture(t, backend)
	for _, cause := range []error{errors.New("offline"), &storage.KeyNotFoundError{Key: "another-key"}} {
		kv.get = func(context.Context, string) ([]byte, uint64, error) { return nil, 0, cause }
		got, err := store.GetObjectMetadata(context.Background(), req)
		var oe *ObjectError
		if !errors.As(err, &oe) || oe.Code != ObjectErrorBackend || !errors.Is(err, cause) || got != (sessionwire.ObjectMetadata{}) {
			t.Fatalf("backend error = %+v, %v", got, err)
		}
	}
}
