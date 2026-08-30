package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func TestObjectReaderCloseRequiresObservedTerminalEOF(t *testing.T) {
	body := []byte("exact")
	digest := sha256.Sum256(body)
	for _, readBytes := range []int{0, 2, len(body)} {
		t.Run(string(rune('0'+readBytes)), func(t *testing.T) {
			underlying := &countingReadCloser{Reader: bytes.NewReader(body)}
			reader := &objectReader{verifier: newBackendExactVerifier(context.Background(), underlying, uint64(len(body)), digest), underlying: underlying, release: func() {}}
			if readBytes > 0 {
				p := make([]byte, readBytes)
				if n, err := io.ReadFull(reader, p); n != readBytes || err != nil {
					t.Fatalf("ReadFull = %d, %v", n, err)
				}
			}
			err := reader.Close()
			var objectErr *ObjectError
			if !errors.As(err, &objectErr) || objectErr.Code != ObjectErrorIntegrity {
				t.Fatalf("Close = %T %v, want integrity", err, err)
			}
		})
	}
}

func TestObjectReaderEOFAutoFinishesBeforeExplicitClose(t *testing.T) {
	store := openTestStore(t)
	body := []byte("verified")
	digest := sha256.Sum256(body)
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.GetObject(context.Background(), GetObjectRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: ObjectKindArtifact, Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- store.Close(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Store.Close waited for reader after terminal EOF")
	}
}

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

func TestPutObjectAdmitsBeforeRNGAndClosedStoreSkipsRNG(t *testing.T) {
	backend, calls := instrumentComposite(memstore.New())
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	admitted := false
	store.lifecycleLocked = func(op lifecycleOperation) {
		if op == lifecycleAdmit {
			admitted = true
		}
	}
	store.objectGeneration = func() ([16]byte, error) {
		if !admitted {
			t.Fatal("RNG called before admission")
		}
		return [16]byte{}, errors.New("rng")
	}
	digest := sha256.Sum256([]byte("x"))
	before := calls.snapshot()
	_, _ = store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader([]byte("x"))})
	if !admitted || calls.snapshot() != before {
		t.Fatalf("admitted=%v provider changed=%v", admitted, calls.snapshot() != before)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	gens := 0
	store.objectGeneration = func() ([16]byte, error) { gens++; return [16]byte{}, nil }
	before = calls.snapshot()
	_, err = store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader([]byte("x"))})
	var closed *StoreClosedError
	if !errors.As(err, &closed) || gens != 0 || calls.snapshot() != before {
		t.Fatalf("closed Put err=%T %v gens=%d provider changed=%v", err, err, gens, calls.snapshot() != before)
	}
}

func TestPutObjectRejectsNilProviderReader(t *testing.T) {
	for _, typed := range []bool{false, true} {
		base := memstore.New()
		var typedNil *countingReadCloser
		script := &scriptedBlobs{Blobs: base.Blobs, putFn: drainPut, getFn: func(string) (io.ReadCloser, error) {
			if typed {
				return typedNil, nil
			}
			return nil, nil
		}}
		base.Blobs = script
		store, err := Open(context.Background(), base)
		if err != nil {
			t.Fatal(err)
		}
		body := []byte("x")
		digest := sha256.Sum256(body)
		metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader(body)})
		var objectErr *ObjectError
		if !errors.As(err, &objectErr) || objectErr.Code != ObjectErrorBackend || metadata.Reference.ObjectID != "" {
			t.Fatalf("typed=%v Put=%+v error=%T %v", typed, metadata, err, err)
		}
		_ = store.Close(context.Background())
	}
}

func TestGetObjectRejectsNilProviderReader(t *testing.T) {
	for _, typed := range []bool{false, true} {
		base := memstore.New()
		getCount := 0
		script := &scriptedBlobs{Blobs: base.Blobs, putFn: drainPut, getFn: func(string) (io.ReadCloser, error) {
			getCount++
			if getCount == 1 {
				return io.NopCloser(bytes.NewReader([]byte("x"))), nil
			}
			if typed {
				var typedNil *countingReadCloser
				return typedNil, nil
			}
			return nil, nil
		}}
		base.Blobs = script
		store, err := Open(context.Background(), base)
		if err != nil {
			t.Fatal(err)
		}
		body := []byte("x")
		digest := sha256.Sum256(body)
		metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader(body)})
		if err != nil {
			t.Fatal(err)
		}
		reader, err := store.GetObject(context.Background(), GetObjectRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: ObjectKindArtifact, Metadata: metadata})
		var objectErr *ObjectError
		if reader != nil || !errors.As(err, &objectErr) || objectErr.Code != ObjectErrorBackend {
			t.Fatalf("typed=%v Get reader=%v error=%T %v", typed, reader, err, err)
		}
		_ = store.Close(context.Background())
	}
}

func TestGetObjectAfterStoreCloseTouchesNoProvider(t *testing.T) {
	backend, calls := instrumentComposite(memstore.New())
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("x")
	digest := sha256.Sum256(body)
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := calls.snapshot()
	reader, err := store.GetObject(context.Background(), GetObjectRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: ObjectKindArtifact, Metadata: metadata})
	var closed *StoreClosedError
	if reader != nil || !errors.As(err, &closed) || calls.snapshot() != before {
		t.Fatalf("reader=%v err=%T %v provider changed=%v", reader, err, err, calls.snapshot() != before)
	}
}

func TestOrdinaryPutObjectMintsDistinctRandomGenerations(t *testing.T) {
	store := openTestStore(t)
	body := []byte("same")
	digest := sha256.Sum256(body)
	put := func() sessionwire.ObjectMetadata {
		metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)})
		if err != nil {
			t.Fatal(err)
		}
		return metadata
	}
	first, second := put(), put()
	if first.Reference == second.Reference {
		t.Fatalf("ordinary Puts reused reference %q", first.Reference.ObjectID)
	}
}

func TestStoreCloseCancelsIdleObjectReaderBeforeProvider(t *testing.T) {
	body := []byte("x")
	digest := sha256.Sum256(body)
	base := memstore.New()
	blocking := newBlockingReadCloser()
	getCount := 0
	script := &scriptedBlobs{Blobs: base.Blobs, putFn: drainPut, getFn: func(string) (io.ReadCloser, error) {
		getCount++
		if getCount == 1 {
			return io.NopCloser(bytes.NewReader(body)), nil
		}
		return blocking, nil
	}}
	base.Blobs = script
	owned := &orderedProviderCloser{reader: blocking}
	store, err := Open(context.Background(), base, WithProviderOwnership(owned))
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.GetObject(context.Background(), GetObjectRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: ObjectKindArtifact, Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if blocking.closeCalls.Load() != 1 || !owned.readerClosed.Load() {
		t.Fatalf("reader closes=%d provider observed closed=%v", blocking.closeCalls.Load(), owned.readerClosed.Load())
	}
	var objectErr *ObjectError
	if _, err := reader.Read(make([]byte, 1)); !errors.As(err, &objectErr) || objectErr.Code != ObjectErrorCanceled {
		t.Fatalf("Read after shutdown = %T %v", err, err)
	}
}

func TestStoreCloseUnblocksActiveObjectRead(t *testing.T) {
	body := []byte("x")
	digest := sha256.Sum256(body)
	base := memstore.New()
	blocking := newBlockingReadCloser()
	getCount := 0
	script := &scriptedBlobs{Blobs: base.Blobs, putFn: drainPut, getFn: func(string) (io.ReadCloser, error) {
		getCount++
		if getCount == 1 {
			return io.NopCloser(bytes.NewReader(body)), nil
		}
		return blocking, nil
	}}
	base.Blobs = script
	owned := &orderedProviderCloser{reader: blocking}
	store, err := Open(context.Background(), base, WithProviderOwnership(owned))
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.GetObject(context.Background(), GetObjectRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: ObjectKindArtifact, Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() { _, err := reader.Read(make([]byte, 1)); readDone <- err }()
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("Read did not block")
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		codes := objectErrorCodes(err)
		if !codes[ObjectErrorCanceled] || !codes[ObjectErrorBackend] {
			t.Fatalf("Read error=%v codes=%v, want canceled+backend", err, codes)
		}
	case <-time.After(time.Second):
		t.Fatal("Read remained blocked")
	}
	if blocking.closeCalls.Load() != 1 || !owned.readerClosed.Load() {
		t.Fatalf("reader closes=%d provider observed closed=%v", blocking.closeCalls.Load(), owned.readerClosed.Load())
	}
}

func TestStoreCloseCancelsBlockedPutBeforeProviderClose(t *testing.T) {
	base := memstore.New()
	owned := &putOrderingCloser{}
	store, err := Open(context.Background(), base, WithProviderOwnership(owned))
	if err != nil {
		t.Fatal(err)
	}
	body := &contextBlockingReader{ctx: store.ctx, started: make(chan struct{})}
	digest := sha256.Sum256([]byte("x"))
	putDone := make(chan error, 1)
	go func() {
		_, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: body})
		owned.putFinished.Store(true)
		putDone <- err
	}()
	select {
	case <-body.started:
	case <-time.After(time.Second):
		t.Fatal("Put did not block")
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-putDone:
		var objectErr *ObjectError
		if !errors.As(err, &objectErr) || objectErr.Code != ObjectErrorCanceled {
			t.Fatalf("Put error=%T %v, want canceled", err, err)
		}
	case <-time.After(time.Second):
		t.Fatal("Put remained blocked")
	}
	if !owned.observedFinished.Load() {
		t.Fatal("provider closed before Put released")
	}
}

func TestObjectReaderJoinsPrematureAndProviderCloseErrors(t *testing.T) {
	body := []byte("x")
	digest := sha256.Sum256(body)
	closeCause := errors.New("private close")
	underlying := &namedErrorReadCloser{Reader: bytes.NewReader(body), closeErr: closeCause}
	reader := &objectReader{verifier: newBackendExactVerifier(context.Background(), underlying, 1, digest), underlying: underlying, release: func() {}}
	err := reader.Close()
	codes := objectErrorCodes(err)
	if !codes[ObjectErrorIntegrity] || !codes[ObjectErrorBackend] || !errors.Is(err, closeCause) {
		t.Fatalf("Close error=%v codes=%v", err, codes)
	}
}

func TestPutObjectPostVerifyJoinsReadAndCloseErrors(t *testing.T) {
	readCause := errors.New("private read")
	closeCause := errors.New("private close")
	base := memstore.New()
	script := &scriptedBlobs{Blobs: base.Blobs, putFn: drainPut, getFn: func(string) (io.ReadCloser, error) {
		return &namedErrorReadCloser{Reader: readerFunc(func([]byte) (int, error) { return 0, readCause }), closeErr: closeCause}, nil
	}}
	base.Blobs = script
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("x")
	digest := sha256.Sum256(body)
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader(body)})
	if metadata.Reference.ObjectID != "" || !errors.Is(err, readCause) || !errors.Is(err, closeCause) {
		t.Fatalf("Put=%+v error=%v; want joined read+close", metadata, err)
	}
	codes := objectErrorCodes(err)
	if !codes[ObjectErrorBackend] || codes[ObjectErrorSource] {
		t.Fatalf("error codes=%v, want backend without source", codes)
	}
	if strings.Contains(err.Error(), "private read") || strings.Contains(err.Error(), "private close") {
		t.Fatalf("error leaked provider detail: %v", err)
	}
}

func TestAdministrativeObjectListAndDeleteAreExactlyScoped(t *testing.T) {
	base := memstore.New()
	admin := &adminRecordingBlobs{Blobs: base.Blobs}
	base.Blobs = admin
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	store.objectGeneration = func() ([16]byte, error) { return [16]byte{1}, nil }
	body := []byte("artifact")
	digest := sha256.Sum256(body)
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	scope, _ := store.deriveSessionScope("tenant", "session")
	wantPrefix := scope.BlobPrefix + "v1/artifact/"
	refs, err := store.listObjectReferences(context.Background(), "tenant", "session", ObjectKindArtifact)
	if err != nil {
		t.Fatal(err)
	}
	if admin.listPrefix != wantPrefix || len(refs) != 1 || refs[0] != metadata.Reference {
		t.Fatalf("prefix=%q refs=%+v, want %q/%+v", admin.listPrefix, refs, wantPrefix, metadata.Reference)
	}
	if err := store.deleteObject(context.Background(), "tenant", "session", ObjectKindArtifact, metadata.Reference); err != nil {
		t.Fatal(err)
	}
	parsed, _ := parseObjectMetadata(metadata)
	if admin.deleteKey != objectKey(scope, parsed) {
		t.Fatalf("delete key=%q want=%q", admin.deleteKey, objectKey(scope, parsed))
	}
	if _, err := base.Blobs.Get(context.Background(), admin.deleteKey); err == nil {
		t.Fatal("deleted object remains")
	}
}

func TestAdministrativeObjectMissingBindingTouchesNoBlobs(t *testing.T) {
	base := memstore.New()
	admin := &adminRecordingBlobs{Blobs: base.Blobs}
	base.Blobs = admin
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("x"))
	ref := objectMetadata(ObjectKindArtifact, [16]byte{1}, 1, digest, "").Reference
	if _, err := store.listObjectReferences(context.Background(), "tenant", "absent", ObjectKindArtifact); err == nil {
		t.Fatal("list succeeded")
	}
	if err := store.deleteObject(context.Background(), "tenant", "absent", ObjectKindArtifact, ref); err == nil {
		t.Fatal("delete succeeded")
	}
	if admin.lists != 0 || admin.deletes != 0 {
		t.Fatalf("list=%d delete=%d, want zero", admin.lists, admin.deletes)
	}
}

func TestAdministrativeObjectCrossTenantTouchesNoBlobs(t *testing.T) {
	base := memstore.New()
	admin := &adminRecordingBlobs{Blobs: base.Blobs}
	base.Blobs = admin
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("x")
	digest := sha256.Sum256(body)
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant-a", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	admin.lists, admin.deletes = 0, 0
	if _, err := store.listObjectReferences(context.Background(), "tenant-b", "session", ObjectKindArtifact); err == nil {
		t.Fatal("cross-tenant list succeeded")
	}
	if err := store.deleteObject(context.Background(), "tenant-b", "session", ObjectKindArtifact, metadata.Reference); err == nil {
		t.Fatal("cross-tenant delete succeeded")
	}
	if admin.lists != 0 || admin.deletes != 0 {
		t.Fatalf("list=%d delete=%d, want zero", admin.lists, admin.deletes)
	}
}

func TestAdministrativeObjectStaticValidationPrecedesAdmission(t *testing.T) {
	base := memstore.New()
	admin := &adminRecordingBlobs{Blobs: base.Blobs}
	base.Blobs = admin
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	admissions := 0
	store.lifecycleLocked = func(op lifecycleOperation) {
		if op == lifecycleAdmit {
			admissions++
		}
	}
	digest := sha256.Sum256([]byte("x"))
	ref := objectMetadata(ObjectKindArtifact, [16]byte{1}, 1, digest, "").Reference
	if _, err := store.listObjectReferences(context.Background(), "tenant", "session", "unknown"); err == nil {
		t.Fatal("invalid kind list succeeded")
	}
	if err := store.deleteObject(context.Background(), "tenant", "session", ObjectKindAttachment, ref); err == nil {
		t.Fatal("wrong kind delete succeeded")
	}
	if err := store.deleteObject(context.Background(), "tenant", "session", ObjectKindArtifact, sessionwire.ObjectReference{ObjectID: "../escape"}); err == nil {
		t.Fatal("malformed ref delete succeeded")
	}
	if admissions != 0 || admin.lists != 0 || admin.deletes != 0 {
		t.Fatalf("admissions=%d list=%d delete=%d", admissions, admin.lists, admin.deletes)
	}
}

func TestAdministrativeObjectListFailsClosed(t *testing.T) {
	digest := sha256.Sum256([]byte("x"))
	valid := objectMetadata(ObjectKindArtifact, [16]byte{1}, 1, digest, "").Reference
	for name, mutate := range map[string]func(string) []string{
		"malformed": func(prefix string) []string { return []string{prefix + "../escape"} },
		"out of prefix": func(string) []string {
			return []string{"other/v1/artifact/" + hex.EncodeToString(digest[:]) + "/04000000000000000000000000"}
		},
		"duplicate": func(prefix string) []string {
			parts := strings.Split(valid.ObjectID, ":")
			key := prefix + parts[3] + "/" + parts[2]
			return []string{key, key}
		},
	} {
		t.Run(name, func(t *testing.T) {
			base := memstore.New()
			admin := &adminRecordingBlobs{Blobs: base.Blobs}
			base.Blobs = admin
			store, err := Open(context.Background(), base)
			if err != nil {
				t.Fatal(err)
			}
			body := []byte("bind")
			bodyDigest := sha256.Sum256(body)
			if _, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindAttachment, SizeBytes: uint64(len(body)), SHA256: bodyDigest, Body: bytes.NewReader(body)}); err != nil {
				t.Fatal(err)
			}
			admin.listFn = mutate
			if _, err := store.listObjectReferences(context.Background(), "tenant", "session", ObjectKindArtifact); err == nil {
				t.Fatal("malformed list succeeded")
			}
		})
	}
}

func TestAdministrativeObjectListSortsReferencesByObjectID(t *testing.T) {
	base := memstore.New()
	admin := &adminRecordingBlobs{Blobs: base.Blobs}
	base.Blobs = admin
	store, err := Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("bind")
	digest := sha256.Sum256(body)
	if _, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindAttachment, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}
	first := objectMetadata(ObjectKindArtifact, [16]byte{1}, 1, sha256.Sum256([]byte("a")), "").Reference
	second := objectMetadata(ObjectKindArtifact, [16]byte{2}, 1, sha256.Sum256([]byte("b")), "").Reference
	want := []sessionwire.ObjectReference{first, second}
	if want[1].ObjectID < want[0].ObjectID {
		want[0], want[1] = want[1], want[0]
	}
	admin.listFn = func(prefix string) []string {
		keys := make([]string, 0, 2)
		for _, ref := range want {
			parts := strings.Split(ref.ObjectID, ":")
			keys = append(keys, prefix+parts[3]+"/"+parts[2])
		}
		return []string{keys[1], keys[0]}
	}
	refs, err := store.listObjectReferences(context.Background(), "tenant", "session", ObjectKindArtifact)
	if err != nil || len(refs) != 2 || refs[0] != want[0] || refs[1] != want[1] {
		t.Fatalf("refs=%+v err=%v want=%+v", refs, err, want)
	}
}

func TestAdministrativeLegacyListExcludesHistoricalDigestKey(t *testing.T) {
	base := memstore.New()
	store, err := Open(context.Background(), base, WithLegacySingleTenant("local"))
	if err != nil {
		t.Fatal(err)
	}
	store.objectGeneration = func() ([16]byte, error) { return [16]byte{1}, nil }
	session := sessionwire.SessionID("123e4567-e89b-12d3-a456-426614174000")
	body := []byte("new")
	digest := sha256.Sum256(body)
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "local", SessionID: session, Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	scope, _ := store.deriveSessionScope("local", session)
	historical := scope.BlobPrefix + hex.EncodeToString(digest[:])
	if err := base.Blobs.Put(context.Background(), historical, bytes.NewReader([]byte("historical"))); err != nil {
		t.Fatal(err)
	}
	refs, err := store.listObjectReferences(context.Background(), "local", session, ObjectKindArtifact)
	if err != nil || len(refs) != 1 || refs[0] != metadata.Reference {
		t.Fatalf("refs=%+v err=%v", refs, err)
	}
	if strings.HasPrefix(historical, scope.BlobPrefix+"v1/artifact/") {
		t.Fatalf("historical key aliases new prefix: %q", historical)
	}
}

func TestObjectKeyLiteralGoldensDoNotAliasHistorical(t *testing.T) {
	digest := sha256.Sum256([]byte("x"))
	generation := [16]byte{1}
	metadata := objectMetadata(ObjectKindArtifact, generation, 1, digest, "")
	parts := strings.Split(metadata.Reference.ObjectID, ":")
	for _, legacy := range []bool{false, true} {
		base := memstore.New()
		opts := []Option(nil)
		tenant, session := sessionwire.TenantID("tenant/raw"), sessionwire.SessionID("session/raw")
		if legacy {
			tenant, session = "local", "123e4567-e89b-12d3-a456-426614174000"
			opts = append(opts, WithLegacySingleTenant(tenant))
		}
		store, err := Open(context.Background(), base, opts...)
		if err != nil {
			t.Fatal(err)
		}
		scope, _ := store.deriveSessionScope(tenant, session)
		want := scope.BlobPrefix + "v1/artifact/" + parts[3] + "/" + parts[2]
		parsed, _ := parseObjectMetadata(metadata)
		if got := objectKey(scope, parsed); got != want {
			t.Fatalf("legacy=%v key=%q want literal=%q", legacy, got, want)
		}
		if want == scope.BlobPrefix+parts[3] {
			t.Fatal("new key aliases historical digest-only key")
		}
	}
}

type adminRecordingBlobs struct {
	storage.Blobs
	listFn                func(string) []string
	listErr, deleteErr    error
	listPrefix, deleteKey string
	lists, deletes        int
}

func (b *adminRecordingBlobs) List(ctx context.Context, prefix string) ([]string, error) {
	b.lists++
	b.listPrefix = prefix
	if b.listErr != nil {
		return nil, b.listErr
	}
	if b.listFn != nil {
		return b.listFn(prefix), nil
	}
	return b.Blobs.List(ctx, prefix)
}
func (b *adminRecordingBlobs) Delete(ctx context.Context, key string) error {
	b.deletes++
	b.deleteKey = key
	if b.deleteErr != nil {
		return b.deleteErr
	}
	return b.Blobs.Delete(ctx, key)
}

type blockingReadCloser struct {
	started    chan struct{}
	closed     chan struct{}
	startOnce  sync.Once
	once       sync.Once
	closeCalls atomic.Int32
}

func newBlockingReadCloser() *blockingReadCloser {
	return &blockingReadCloser{started: make(chan struct{}), closed: make(chan struct{})}
}
func (r *blockingReadCloser) Read([]byte) (int, error) {
	r.startOnce.Do(func() { close(r.started) })
	<-r.closed
	return 0, errors.New("reader closed")
}
func (r *blockingReadCloser) Close() error {
	r.closeCalls.Add(1)
	r.once.Do(func() { close(r.closed) })
	return nil
}

type orderedProviderCloser struct {
	reader       *blockingReadCloser
	readerClosed atomic.Bool
}

func (c *orderedProviderCloser) Close(context.Context) error {
	c.readerClosed.Store(c.reader.closeCalls.Load() > 0)
	return nil
}

type contextBlockingReader struct {
	ctx     context.Context
	started chan struct{}
	once    sync.Once
}

func (r *contextBlockingReader) Read([]byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	<-r.ctx.Done()
	time.Sleep(10 * time.Millisecond)
	return 0, r.ctx.Err()
}

type putOrderingCloser struct {
	putFinished, observedFinished atomic.Bool
}

func (c *putOrderingCloser) Close(context.Context) error {
	c.observedFinished.Store(c.putFinished.Load())
	return nil
}

type namedErrorReadCloser struct {
	io.Reader
	closeErr error
}

func (r *namedErrorReadCloser) Close() error { return r.closeErr }

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

func objectErrorCodes(err error) map[ObjectErrorCode]bool {
	result := make(map[ObjectErrorCode]bool)
	var visit func(error)
	visit = func(value error) {
		if value == nil {
			return
		}
		if objectErr, ok := value.(*ObjectError); ok {
			result[objectErr.Code] = true
		}
		switch unwrapped := value.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range unwrapped.Unwrap() {
				visit(child)
			}
		case interface{ Unwrap() error }:
			visit(unwrapped.Unwrap())
		}
	}
	visit(err)
	return result
}

var _ storage.Blobs = (*scriptedBlobs)(nil)
