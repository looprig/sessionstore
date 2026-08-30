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
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
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
		{name: "source", size: uint64(len(body)), digest: digest, body: io.MultiReader(bytes.NewReader(body[:3]), errorReader{})},
		{name: "stalled", size: uint64(len(body)), digest: digest, body: stalledReader{}},
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
	blobs := &countingBlobs{Blobs: base.Blobs}
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
	if blobs.gets != 0 || blobs.puts != 0 {
		t.Fatalf("blob calls get=%d put=%d", blobs.gets, blobs.puts)
	}
}

func TestObjectReaderHoldsLifecycleUntilClose(t *testing.T) {
	store := openTestStore(t)
	body := []byte("reader lifetime")
	digest := sha256.Sum256(body)
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.GetObject(context.Background(), GetObjectRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: ObjectKindArtifact, Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- store.Close(context.Background()) }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel idle reader")
	}
	var objectErr *ObjectError
	if err := reader.Close(); !errors.As(err, &objectErr) || objectErr.Code != ObjectErrorCanceled {
		t.Fatalf("reader Close = %T %v, want canceled", err, err)
	}
}

func TestObjectReaderFinishesExactlyOnceOnEOFErrorAndClose(t *testing.T) {
	body := []byte("body")
	digest := sha256.Sum256(body)
	tests := []struct {
		name   string
		source io.Reader
		action func(*objectReader)
	}{
		{name: "eof", source: bytes.NewReader(body), action: func(r *objectReader) { _, _ = io.ReadAll(r) }},
		{name: "error", source: errorReader{}, action: func(r *objectReader) { _, _ = r.Read(make([]byte, 1)) }},
		{name: "close", source: bytes.NewReader(body), action: func(r *objectReader) { _ = r.Close() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			underlying := &countingReadCloser{Reader: tt.source}
			releases := 0
			reader := &objectReader{verifier: newExactVerifier(context.Background(), underlying, uint64(len(body)), digest), underlying: underlying, release: func() { releases++ }}
			tt.action(reader)
			_ = reader.Close()
			_ = reader.Close()
			if underlying.closes != 1 || releases != 1 {
				t.Fatalf("closes=%d releases=%d, want 1 each", underlying.closes, releases)
			}
		})
	}
}

func TestPutObjectRNGFailureTouchesNoProvider(t *testing.T) {
	backend, calls := instrumentComposite(memstore.New())
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	store.objectGeneration = func() ([16]byte, error) { return [16]byte{}, errors.New("rng unavailable") }
	before := calls.snapshot()
	body := []byte("x")
	digest := sha256.Sum256(body)
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader(body)})
	if err == nil || metadata.Reference.ObjectID != "" {
		t.Fatalf("PutObject = %+v, %v", metadata, err)
	}
	if got := calls.snapshot(); got != before {
		t.Fatalf("provider calls changed: before=%+v after=%+v", before, got)
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
		}, wantCode: ObjectErrorDigest},
		{name: "persisted close failure", put: drainPut, get: func(string) (io.ReadCloser, error) {
			return &errorCloseReader{Reader: bytes.NewReader(body)}, nil
		}, wantCode: ObjectErrorBackend},
		{name: "persisted read failure", put: drainPut, get: func(string) (io.ReadCloser, error) {
			return io.NopCloser(io.MultiReader(bytes.NewReader(body[:3]), errorReader{})), nil
		}, wantCode: ObjectErrorBackend},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := memstore.New()
			script := &scriptedBlobs{Blobs: base.Blobs, putFn: tt.put, getFn: tt.get}
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
	script := &scriptedBlobs{Blobs: underlying, putFn: func(key string, r io.Reader) error {
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

func TestLegacyPutObjectUsesVersionedBlobKeyWithoutWitnesses(t *testing.T) {
	backend := memstore.New()
	store, err := Open(context.Background(), backend, WithLegacySingleTenant("local"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	store.objectGeneration = func() ([16]byte, error) { return [16]byte{1}, nil }
	body := []byte("legacy new object")
	digest := sha256.Sum256(body)
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "local", SessionID: "123e4567-e89b-12d3-a456-426614174000", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseObjectMetadata(metadata)
	if err != nil {
		t.Fatal(err)
	}
	scope, _ := store.deriveSessionScope("local", "123e4567-e89b-12d3-a456-426614174000")
	keys, _ := backend.Blobs.List(context.Background(), scope.BlobPrefix)
	want := objectKey(scope, parsed)
	if len(keys) != 1 || keys[0] != want {
		t.Fatalf("blob keys = %v, want %q", keys, want)
	}
	witnesses, _ := backend.KV.Keys(context.Background(), "sessionstore/witnesses/")
	if len(witnesses) != 0 {
		t.Fatalf("legacy witnesses = %v", witnesses)
	}
}

func TestParseObjectMetadataRejectsNoncanonicalForms(t *testing.T) {
	body := []byte("x")
	digest := sha256.Sum256(body)
	var generation [16]byte
	for i := range generation {
		generation[i] = 0xff
	}
	valid := objectMetadata(ObjectKindArtifact, generation, 1, digest, "")
	mutations := map[string]func(*sessionwire.ObjectMetadata){
		"trailing component": func(m *sessionwire.ObjectMetadata) { m.Reference.ObjectID += ":tail" },
		"unknown version":    func(m *sessionwire.ObjectMetadata) { m.Reference.ObjectID = "v2" + m.Reference.ObjectID[2:] },
		"unknown kind": func(m *sessionwire.ObjectMetadata) {
			p := strings.Split(m.Reference.ObjectID, ":")
			p[1] = "unknown"
			m.Reference.ObjectID = strings.Join(p, ":")
		},
		"uppercase generation": func(m *sessionwire.ObjectMetadata) {
			p := strings.Split(m.Reference.ObjectID, ":")
			p[2] = strings.ToUpper(p[2])
			m.Reference.ObjectID = strings.Join(p, ":")
		},
		"uppercase digest": func(m *sessionwire.ObjectMetadata) {
			p := strings.Split(m.Reference.ObjectID, ":")
			p[3] = strings.ToUpper(p[3])
			m.Reference.ObjectID = strings.Join(p, ":")
			m.Digest = "sha256:" + p[3]
		},
		"digest mismatch": func(m *sessionwire.ObjectMetadata) { m.Digest = "sha256:" + strings.Repeat("0", 64) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if _, err := parseObjectMetadata(candidate); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestPutObjectBindsBeforeBlobAndPostVerifies(t *testing.T) {
	base := memstore.New()
	checking := &bindingCheckingBlobs{Blobs: base.Blobs, kv: base.KV}
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
	script := &scriptedBlobs{Blobs: base.Blobs, putFn: func(key string, _ io.Reader) error { return &storage.BlobConflictError{Key: key} }}
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
	script := &scriptedBlobs{Blobs: base.Blobs, putFn: func(string, io.Reader) error {
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

func TestPutObjectCanceledContextReturnsNoMetadata(t *testing.T) {
	store := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	body := []byte("x")
	digest := sha256.Sum256(body)
	metadata, err := store.PutObject(ctx, PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader(body)})
	if err == nil || metadata.Reference.ObjectID != "" {
		t.Fatalf("PutObject = %+v, %v", metadata, err)
	}
}

func TestGetObjectDetectsCorruptionAndReleasesLifecycle(t *testing.T) {
	body := []byte("persisted")
	digest := sha256.Sum256(body)
	base := memstore.New()
	getCount := 0
	script := &scriptedBlobs{Blobs: base.Blobs, putFn: drainPut, getFn: func(string) (io.ReadCloser, error) {
		getCount++
		value := body
		if getCount > 1 {
			value = []byte("corrupt!!")
		}
		return io.NopCloser(bytes.NewReader(value)), nil
	}}
	base.Blobs = script
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.GetObject(context.Background(), GetObjectRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: ObjectKindArtifact, Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(reader); err == nil {
		t.Fatal("corrupt Get reached successful EOF")
	}
	if err := reader.Close(); err == nil {
		t.Fatal("Close lost latched corruption error")
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestGetObjectProviderReadFailureIsBackend(t *testing.T) {
	body := []byte("persisted")
	digest := sha256.Sum256(body)
	base := memstore.New()
	getCount := 0
	script := &scriptedBlobs{Blobs: base.Blobs, putFn: drainPut, getFn: func(string) (io.ReadCloser, error) {
		getCount++
		if getCount == 1 {
			return io.NopCloser(bytes.NewReader(body)), nil
		}
		return io.NopCloser(io.MultiReader(bytes.NewReader(body[:3]), errorReader{})), nil
	}}
	base.Blobs = script
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.GetObject(context.Background(), GetObjectRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: ObjectKindArtifact, Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(reader)
	var objectErr *ObjectError
	if !errors.As(err, &objectErr) || objectErr.Code != ObjectErrorBackend {
		t.Fatalf("ReadAll error = %T %v, want backend", err, err)
	}
}

func TestGetObjectCallerCancellationClosesAndReleases(t *testing.T) {
	body := []byte("persisted")
	digest := sha256.Sum256(body)
	store := openTestStore(t)
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	reader, err := store.GetObject(ctx, GetObjectRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: ObjectKindArtifact, Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	_, err = reader.Read(make([]byte, 1))
	var objectErr *ObjectError
	if !errors.As(err, &objectErr) || objectErr.Code != ObjectErrorCanceled {
		t.Fatalf("Read error = %T %v, want canceled", err, err)
	}
	if err := reader.Close(); !errors.As(err, &objectErr) || objectErr.Code != ObjectErrorCanceled {
		t.Fatalf("Close error = %T %v, want canceled", err, err)
	}
}

func TestPutObjectMintsNewGenerationAndAdmitsOnce(t *testing.T) {
	store := openTestStore(t)
	generationCalls := 0
	store.objectGeneration = func() ([16]byte, error) {
		generationCalls++
		var g [16]byte
		g[0] = byte(generationCalls)
		return g, nil
	}
	admissions := 0
	store.lifecycleLocked = func(op lifecycleOperation) {
		if op == lifecycleAdmit {
			admissions++
		}
	}
	body := []byte("same bytes")
	digest := sha256.Sum256(body)
	put := func() sessionwire.ObjectMetadata {
		metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)})
		if err != nil {
			t.Fatal(err)
		}
		return metadata
	}
	first, second := put(), put()
	if first.Reference.ObjectID == second.Reference.ObjectID {
		t.Fatal("two Puts reused one generation")
	}
	if generationCalls != 2 || admissions != 2 {
		t.Fatalf("generation=%d admissions=%d, want 2/2", generationCalls, admissions)
	}
}

func TestGetObjectCrossTenantFailsBeforeBlob(t *testing.T) {
	base := memstore.New()
	blobs := &countingBlobs{Blobs: base.Blobs}
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
	beforeGets := blobs.gets
	_, err = store.GetObject(context.Background(), GetObjectRequest{TenantID: "tenant-b", SessionID: "same", ExpectedKind: ObjectKindArtifact, Metadata: metadata})
	if err == nil {
		t.Fatal("cross-tenant Get succeeded")
	}
	if blobs.gets != beforeGets {
		t.Fatalf("cross-tenant Get touched Blobs: %d -> %d", beforeGets, blobs.gets)
	}
}

func TestExactVerifierAcceptsDataWithEOFAndEmpty(t *testing.T) {
	for _, body := range [][]byte{nil, []byte("one read")} {
		digest := sha256.Sum256(body)
		verifier := newExactVerifier(context.Background(), &eofTogetherReader{body: body}, uint64(len(body)), digest)
		got, err := io.ReadAll(verifier)
		if err != nil || !verifier.verified || !bytes.Equal(got, body) {
			t.Fatalf("body=%x got=%x err=%v verified=%v", body, got, err, verifier.verified)
		}
	}
}

func TestExactVerifierNeverReturnsOverflowBytes(t *testing.T) {
	for _, body := range [][]byte{{'x'}, {'o', 'k', 'x'}} {
		expected := body[:len(body)-1]
		digest := sha256.Sum256(expected)
		verifier := newExactVerifier(context.Background(), bytes.NewReader(body), uint64(len(expected)), digest)
		got, err := io.ReadAll(verifier)
		var objectErr *ObjectError
		if !errors.As(err, &objectErr) || objectErr.Code != ObjectErrorSize {
			t.Fatalf("body=%q error=%T %v, want size", body, err, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("body=%q returned %q, want exact prefix %q", body, got, expected)
		}
	}
}

func drainPut(_ string, r io.Reader) error { _, err := io.Copy(io.Discard, r); return err }

type scriptedBlobs struct {
	storage.Blobs
	putFn      func(string, io.Reader) error
	getFn      func(string) (io.ReadCloser, error)
	puts, gets int
}

func (b *scriptedBlobs) Put(_ context.Context, key string, r io.Reader) error {
	b.puts++
	if b.putFn != nil {
		return b.putFn(key, r)
	}
	return b.Blobs.Put(context.Background(), key, r)
}
func (b *scriptedBlobs) Get(_ context.Context, key string) (io.ReadCloser, error) {
	b.gets++
	if b.getFn != nil {
		return b.getFn(key)
	}
	return b.Blobs.Get(context.Background(), key)
}
func (b *scriptedBlobs) BlobReaderCloseBound() time.Duration {
	return forwardBlobReaderCloseBound(b.Blobs)
}

type bindingCheckingBlobs struct {
	storage.Blobs
	kv         storage.KV
	boundAtPut bool
	gets       int
}

func (b *bindingCheckingBlobs) Put(ctx context.Context, key string, r io.Reader) error {
	keys, _ := b.kv.Keys(ctx, "sessionstore/witnesses/")
	b.boundAtPut = len(keys) == 2
	return b.Blobs.Put(ctx, key, r)
}
func (b *bindingCheckingBlobs) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	b.gets++
	return b.Blobs.Get(ctx, key)
}
func (b *bindingCheckingBlobs) BlobReaderCloseBound() time.Duration {
	return forwardBlobReaderCloseBound(b.Blobs)
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("private source failure") }

type stalledReader struct{}

func (stalledReader) Read([]byte) (int, error) { return 0, nil }

type errorCloseReader struct{ io.Reader }

func (*errorCloseReader) Close() error { return errors.New("private close failure") }

type countingReadCloser struct {
	io.Reader
	closes int
}

func (r *countingReadCloser) Close() error { r.closes++; return nil }

type eofTogetherReader struct {
	body []byte
	done bool
}

func (r *eofTogetherReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	return copy(p, r.body), io.EOF
}

type countingBlobs struct {
	storage.Blobs
	gets, puts int
}

func (b *countingBlobs) Put(ctx context.Context, key string, r io.Reader) error {
	b.puts++
	return b.Blobs.Put(ctx, key, r)
}
func (b *countingBlobs) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	b.gets++
	return b.Blobs.Get(ctx, key)
}
func (b *countingBlobs) BlobReaderCloseBound() time.Duration {
	return forwardBlobReaderCloseBound(b.Blobs)
}

func objectMetadata(kind ObjectKind, generation [16]byte, size uint64, digest [32]byte, mediaType string) sessionwire.ObjectMetadata {
	g := strings.ToLower(base32.HexEncoding.WithPadding(base32.NoPadding).EncodeToString(generation[:]))
	d := hex.EncodeToString(digest[:])
	return sessionwire.ObjectMetadata{Reference: sessionwire.ObjectReference{ObjectID: "v1:" + string(kind) + ":" + g + ":" + d}, SizeBytes: size, Digest: "sha256:" + d, MediaType: mediaType}
}
