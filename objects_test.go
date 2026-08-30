// Package-level object API behaviour: PutObject and GetObject as a caller
// sees them. Identity and key grammar live in objects_identity_test.go, the
// stream state machine in objects_stream_test.go, the unexported
// administrative operations in objects_admin_test.go, and every fake in
// objects_fakes_test.go.
package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func TestObjectRoundTripEveryKind(t *testing.T) {
	kinds := []ObjectKind{
		ObjectKindJournalPublic, ObjectKindJournalRuntime, ObjectKindCommandPayload,
		ObjectKindToolResult, ObjectKindWorkspaceCheckpoint, ObjectKindRuntimeCheckpoint,
		ObjectKindArtifact, ObjectKindAttachment, ObjectKindContinuation, ObjectKindRuntimeObject,
	}
	for i, kind := range kinds {
		t.Run(string(kind), func(t *testing.T) {
			backend := memstore.New()
			store, err := Open(context.Background(), backend)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close(context.Background())
			store.objectGeneration = func() ([16]byte, error) { var g [16]byte; g[0] = byte(i + 1); return g, nil }
			body := bytes.Repeat([]byte{0, 1, 2, 0xff}, i*90000)
			digest := sha256.Sum256(body)
			metadata, err := store.PutObject(context.Background(), PutObjectRequest{
				TenantID: "tenant/raw", SessionID: "session/raw", Kind: kind,
				SizeBytes: uint64(len(body)), SHA256: digest, MediaType: "application/octet-stream", Body: bytes.NewReader(body),
			})
			if err != nil {
				t.Fatalf("PutObject: %v", err)
			}
			generation := [16]byte{}
			generation[0] = byte(i + 1)
			generationText := strings.ToLower(base32.HexEncoding.WithPadding(base32.NoPadding).EncodeToString(generation[:]))
			digestText := hex.EncodeToString(digest[:])
			wantID := "v1:" + string(kind) + ":" + generationText + ":" + digestText
			if metadata.Reference.ObjectID != wantID || metadata.SizeBytes != uint64(len(body)) || metadata.Digest != "sha256:"+digestText || metadata.MediaType != "application/octet-stream" {
				t.Fatalf("metadata = %+v, want id=%q size=%d", metadata, wantID, len(body))
			}
			scope, _ := store.deriveSessionScope("tenant/raw", "session/raw")
			key := scope.BlobPrefix + "v1/" + string(kind) + "/" + digestText + "/" + generationText
			if err := storage.ValidateName(key); err != nil {
				t.Fatalf("key: %v", err)
			}
			if strings.Contains(key, "tenant/raw") || strings.Contains(key, "session/raw") {
				t.Fatalf("key leaks raw IDs: %q", key)
			}
			keys, err := backend.Blobs.List(context.Background(), scope.BlobPrefix)
			if err != nil || len(keys) != 1 || keys[0] != key {
				t.Fatalf("stored keys = %v, %v; want %q", keys, err, key)
			}
			reader, err := store.GetObject(context.Background(), GetObjectRequest{TenantID: "tenant/raw", SessionID: "session/raw", ExpectedKind: kind, Metadata: metadata})
			if err != nil {
				t.Fatalf("GetObject: %v", err)
			}
			got, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if err := reader.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if !bytes.Equal(got, body) {
				t.Fatalf("body length=%d want=%d", len(got), len(body))
			}
		})
	}
}

func TestPutObjectRejectsInvalidStreamsWithoutMetadata(t *testing.T) {
	body := []byte("expected bytes")
	digest := sha256.Sum256(body)
	tests := []struct {
		name   string
		size   uint64
		digest [32]byte
		body   io.Reader
	}{
		{name: "under", size: uint64(len(body) + 1), digest: digest, body: bytes.NewReader(body)},
		{name: "over", size: uint64(len(body) - 1), digest: digest, body: bytes.NewReader(body)},
		{name: "digest", size: uint64(len(body)), digest: sha256.Sum256([]byte("wrong")), body: bytes.NewReader(body)},
		{name: "source", size: uint64(len(body)), digest: digest, body: io.MultiReader(bytes.NewReader(body[:3]), failingReader())},
		{name: "stalled", size: uint64(len(body)), digest: digest, body: stalledReader()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := openTestStore(t)
			metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: tt.size, SHA256: tt.digest, Body: tt.body})
			if err == nil || metadata.Reference.ObjectID != "" {
				t.Fatalf("PutObject = %+v, %v", metadata, err)
			}
			var objectErr *ObjectError
			if !errors.As(err, &objectErr) {
				t.Fatalf("error = %T, want *ObjectError", err)
			}
			scope, deriveErr := store.deriveSessionScope("tenant", "session")
			if deriveErr != nil {
				t.Fatal(deriveErr)
			}
			keys, listErr := store.backend.Blobs.List(context.Background(), scope.BlobPrefix)
			if listErr != nil || len(keys) != 0 {
				t.Fatalf("failed Put committed blobs = %v, %v", keys, listErr)
			}
		})
	}
}

func TestGetObjectMissingBindingDoesNotTouchBlobs(t *testing.T) {
	base := memstore.New()
	blobs := &countingBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}}
	base.Blobs = blobs
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	body := []byte("x")
	digest := sha256.Sum256(body)
	var generation [16]byte
	generation[0] = 1
	metadata := objectMetadata(ObjectKindArtifact, generation, uint64(len(body)), digest, "")
	_, err = store.GetObject(context.Background(), GetObjectRequest{TenantID: "tenant", SessionID: "absent", ExpectedKind: ObjectKindArtifact, Metadata: metadata})
	if err == nil {
		t.Fatal("GetObject succeeded")
	}
	if blobs.getCount() != 0 || blobs.putCount() != 0 {
		t.Fatalf("blob calls get=%d put=%d", blobs.getCount(), blobs.putCount())
	}
}

func TestPutObjectRequiresProviderDrainAndPostVerify(t *testing.T) {
	body := []byte("persist exactly")
	digest := sha256.Sum256(body)
	tests := []struct {
		name     string
		put      func(string, io.Reader) error
		get      func(string) (io.ReadCloser, error)
		wantCode ObjectErrorCode
	}{
		{name: "provider does not read", put: func(string, io.Reader) error { return nil }, wantCode: ObjectErrorIntegrity},
		{name: "provider reads prefix only", put: func(_ string, r io.Reader) error { p := make([]byte, 2); _, _ = r.Read(p); return nil }, wantCode: ObjectErrorIntegrity},
		{name: "error after full read", put: func(_ string, r io.Reader) error { _, _ = io.Copy(io.Discard, r); return errors.New("lost ack") }, wantCode: ObjectErrorBackend},
		{name: "persisted short", put: drainPut, get: func(string) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body[:3])), nil }, wantCode: ObjectErrorSize},
		{name: "persisted long", put: drainPut, get: func(string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(append(append([]byte(nil), body...), 'x'))), nil
		}, wantCode: ObjectErrorSize},
		{name: "persisted digest", put: drainPut, get: func(string) (io.ReadCloser, error) {
			corrupt := append([]byte(nil), body...)
			corrupt[0] ^= 1
			return io.NopCloser(bytes.NewReader(corrupt)), nil
		}, wantCode: ObjectErrorIntegrity},
		{name: "persisted close failure", put: drainPut, get: func(string) (io.ReadCloser, error) {
			return &readCloser{Reader: bytes.NewReader(body), closeErr: errors.New("private close failure")}, nil
		}, wantCode: ObjectErrorBackend},
		{name: "persisted read failure", put: drainPut, get: func(string) (io.ReadCloser, error) {
			return io.NopCloser(io.MultiReader(bytes.NewReader(body[:3]), failingReader())), nil
		}, wantCode: ObjectErrorBackend},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := memstore.New()
			script := &scriptedBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}, putFn: tt.put, getFn: tt.get}
			base.Blobs = script
			store, err := Open(context.Background(), base)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close(context.Background())
			metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)})
			if err == nil || metadata.Reference.ObjectID != "" {
				t.Fatalf("PutObject = %+v, %v", metadata, err)
			}
			var objectErr *ObjectError
			if !errors.As(err, &objectErr) || objectErr.Code != tt.wantCode {
				t.Fatalf("error = %T %v, want code %s", err, err, tt.wantCode)
			}
			if tt.get == nil && script.gets != 0 {
				t.Fatalf("post Get called %d times", script.gets)
			}
		})
	}
}

func TestPutObjectProviderErrorAfterCommitReturnsNoReferenceAndLeavesOrphan(t *testing.T) {
	base := memstore.New()
	underlying := base.Blobs
	script := &scriptedBlobs{lifecycleBlobs: lifecycleBlobs{underlying}, putFn: func(key string, r io.Reader) error {
		if err := underlying.Put(context.Background(), key, r); err != nil {
			return err
		}
		return errors.New("ack lost after commit")
	}}
	base.Blobs = script
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	body := []byte("orphan")
	digest := sha256.Sum256(body)
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)})
	var objectErr *ObjectError
	if !errors.As(err, &objectErr) || objectErr.Code != ObjectErrorBackend || metadata.Reference.ObjectID != "" {
		t.Fatalf("PutObject = %+v, %T %v", metadata, err, err)
	}
	scope, _ := store.deriveSessionScope("tenant", "session")
	keys, listErr := underlying.List(context.Background(), scope.BlobPrefix)
	if listErr != nil || len(keys) != 1 {
		t.Fatalf("orphan keys = %v, %v; want one", keys, listErr)
	}
	if script.gets != 0 {
		t.Fatalf("post-verify Get after failed Put = %d, want 0", script.gets)
	}
}

func TestGetObjectExpectedKindRejectedBeforeProvider(t *testing.T) {
	backend, calls := instrumentComposite(memstore.New())
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	body := []byte("x")
	digest := sha256.Sum256(body)
	metadata := objectMetadata(ObjectKindJournalRuntime, [16]byte{1}, 1, digest, "")
	before := calls.snapshot()
	_, err = store.GetObject(context.Background(), GetObjectRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: ObjectKindJournalPublic, Metadata: metadata})
	var objectErr *ObjectError
	if !errors.As(err, &objectErr) || objectErr.Field != "expected_kind" {
		t.Fatalf("error = %T %v", err, err)
	}
	if got := calls.snapshot(); got != before {
		t.Fatalf("provider calls changed: before=%+v after=%+v", before, got)
	}
}

func TestPutObjectBindsBeforeBlobAndPostVerifies(t *testing.T) {
	base := memstore.New()
	checking := &bindingCheckingBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}, kv: base.KV}
	base.Blobs = checking
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	body := []byte("ordered")
	digest := sha256.Sum256(body)
	if _, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}
	if !checking.boundAtPut {
		t.Fatal("Blob.Put occurred before both witnesses")
	}
	if checking.gets != 1 {
		t.Fatalf("post-Put Gets = %d, want 1", checking.gets)
	}
}

func TestPutObjectConflictIsTypedAndRedacted(t *testing.T) {
	base := memstore.New()
	// Blobs permits an identical re-Put as an idempotent no-op. This fake models
	// the provider's different-content collision for one forced generation.
	script := &scriptedBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}, putFn: func(key string, _ io.Reader) error { return &storage.BlobConflictError{Key: key} }}
	base.Blobs = script
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	store.objectGeneration = func() ([16]byte, error) { return [16]byte{1}, nil }
	body := []byte("x")
	digest := sha256.Sum256(body)
	_, err = store.PutObject(context.Background(), PutObjectRequest{TenantID: "RawTenantSecret", SessionID: "RawSessionSecret", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader(body)})
	var objectErr *ObjectError
	if !errors.As(err, &objectErr) || objectErr.Code != ObjectErrorConflict {
		t.Fatalf("error = %T %v", err, err)
	}
	if strings.Contains(err.Error(), "RawTenantSecret") || strings.Contains(err.Error(), "RawSessionSecret") {
		t.Fatalf("error leaks scope: %v", err)
	}
}

func TestPutObjectWrongSubjectConflictIsBackend(t *testing.T) {
	base := memstore.New()
	script := &scriptedBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}, putFn: func(string, io.Reader) error {
		return &storage.BlobConflictError{Key: "different/private/key"}
	}}
	base.Blobs = script
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	body := []byte("x")
	digest := sha256.Sum256(body)
	_, err = store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader(body)})
	var objectErr *ObjectError
	if !errors.As(err, &objectErr) || objectErr.Code != ObjectErrorBackend {
		t.Fatalf("error = %T %v, want backend", err, err)
	}
}

func TestPutObjectStaticValidationPrecedesProviderAndGeneration(t *testing.T) {
	backend, calls := instrumentComposite(memstore.New())
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	generations := 0
	store.objectGeneration = func() ([16]byte, error) { generations++; return [16]byte{}, nil }
	before := calls.snapshot()
	invalidUTF8 := string([]byte{0xff})
	digest := sha256.Sum256(nil)
	for _, mediaType := range []string{"not a media type", invalidUTF8, strings.Repeat("x", 257)} {
		_, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SHA256: digest, Body: bytes.NewReader(nil), MediaType: mediaType})
		var objectErr *ObjectError
		if !errors.As(err, &objectErr) || objectErr.Field != "media_type" {
			t.Fatalf("media type %q: error = %T %v", mediaType, err, err)
		}
	}
	if generations != 0 {
		t.Fatalf("generation calls = %d, want 0", generations)
	}
	if got := calls.snapshot(); got != before {
		t.Fatalf("provider calls changed: before=%+v after=%+v", before, got)
	}
}

func TestGetObjectCrossTenantFailsBeforeBlob(t *testing.T) {
	base := memstore.New()
	blobs := &countingBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}}
	base.Blobs = blobs
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	body := []byte("private")
	digest := sha256.Sum256(body)
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant-a", SessionID: "same", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	beforeGets := blobs.getCount()
	_, err = store.GetObject(context.Background(), GetObjectRequest{TenantID: "tenant-b", SessionID: "same", ExpectedKind: ObjectKindArtifact, Metadata: metadata})
	if err == nil {
		t.Fatal("cross-tenant Get succeeded")
	}
	if blobs.getCount() != beforeGets {
		t.Fatalf("cross-tenant Get touched Blobs: %d -> %d", beforeGets, blobs.getCount())
	}
}

var _ func(*Store, context.Context, GetObjectRequest) (io.ReadCloser, error) = (*Store).GetObject

func TestPutObjectRejectsNilBodiesAndZeroDigestBeforeAdmission(t *testing.T) {
	var typedNil *bytes.Reader
	bodyDigest := sha256.Sum256([]byte("x"))
	for _, req := range []PutObjectRequest{
		{TenantID: "tenant", SessionID: "session", Kind: "unknown", SizeBytes: 1, SHA256: bodyDigest, Body: bytes.NewReader([]byte("x"))},
		{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: bodyDigest, Body: nil},
		{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: bodyDigest, Body: typedNil},
		{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, Body: bytes.NewReader([]byte("x"))},
		{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: bodyDigest, MediaType: "not a media type", Body: bytes.NewReader([]byte("x"))},
	} {
		backend, calls := instrumentComposite(memstore.New())
		store, err := Open(context.Background(), backend)
		if err != nil {
			t.Fatal(err)
		}
		generations, admissions := 0, 0
		store.objectGeneration = func() ([16]byte, error) { generations++; return [16]byte{1}, nil }
		store.lifecycleLocked = func(op lifecycleOperation) {
			if op == lifecycleAdmit {
				admissions++
			}
		}
		before := calls.snapshot()
		if _, err := store.PutObject(context.Background(), req); err == nil {
			t.Fatal("PutObject accepted invalid static request")
		}
		if generations != 0 || admissions != 0 || calls.snapshot() != before {
			t.Fatalf("generation=%d admission=%d provider before=%+v after=%+v", generations, admissions, before, calls.snapshot())
		}
		_ = store.Close(context.Background())
	}
}
