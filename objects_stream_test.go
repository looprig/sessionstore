// The object stream state machine: exactVerifier, objectReader, and their
// interaction with cancellation and Store shutdown.
package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage/memstore"
)

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
		{name: "error", source: failingReader(), action: func(r *objectReader) { _, _ = r.Read(make([]byte, 1)) }},
		{name: "close", source: bytes.NewReader(body), action: func(r *objectReader) { _ = r.Close() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			underlying := &readCloser{Reader: tt.source}
			releases := 0
			reader := &objectReader{verifier: newExactVerifier(context.Background(), underlying, uint64(len(body)), digest), underlying: underlying, release: func() { releases++ }}
			tt.action(reader)
			_ = reader.Close()
			_ = reader.Close()
			if underlying.closes.Load() != 1 || releases != 1 {
				t.Fatalf("closes=%d releases=%d, want 1 each", underlying.closes.Load(), releases)
			}
		})
	}
}

// TestExactVerifierRejectsOverlongReaderCount covers the overflow arm of the
// reader-count guard: a source claiming more bytes than the slice it was handed
// must not be trusted, and none of those bytes may reach the caller.
func TestExactVerifierRejectsOverlongReaderCount(t *testing.T) {
	body := []byte("exact")
	verifier := newExactVerifier(context.Background(), readerFunc(func(p []byte) (int, error) {
		return len(p) + 1, nil
	}), uint64(len(body)), sha256.Sum256(body))
	n, err := verifier.Read(make([]byte, len(body)))
	var objErr *ObjectError
	if n != 0 || !errors.As(err, &objErr) || objErr.Code != ObjectErrorSource {
		t.Fatalf("Read = %d, %T %v, want 0 and a source failure", n, err, err)
	}
}

func TestPutObjectCanceledContextReturnsNoMetadata(t *testing.T) {
	store := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	body := []byte("x")
	digest := sha256.Sum256(body)
	metadata, err := store.PutObject(ctx, PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader(body)})
	var objErr *ObjectError
	if !errors.As(err, &objErr) || objErr.Code != ObjectErrorCanceled {
		t.Fatalf("PutObject error = %T %v, want canceled", err, err)
	}
	if metadata != (sessionwire.ObjectMetadata{}) {
		t.Fatalf("PutObject = %+v, want zero metadata", metadata)
	}
}

func TestGetObjectDetectsCorruptionAndReleasesLifecycle(t *testing.T) {
	body := []byte("persisted")
	digest := sha256.Sum256(body)
	base := memstore.New()
	getCount := 0
	script := &scriptedBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}, putFn: drainPut, getFn: func(string) (io.ReadCloser, error) {
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
	script := &scriptedBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}, putFn: drainPut, getFn: func(string) (io.ReadCloser, error) {
		getCount++
		if getCount == 1 {
			return io.NopCloser(bytes.NewReader(body)), nil
		}
		return io.NopCloser(io.MultiReader(bytes.NewReader(body[:3]), failingReader())), nil
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

func TestExactVerifierAcceptsDataWithEOFAndEmpty(t *testing.T) {
	for _, body := range [][]byte{nil, []byte("one read")} {
		digest := sha256.Sum256(body)
		verifier := newExactVerifier(context.Background(), eofTogetherReader(body), uint64(len(body)), digest)
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

func TestExactVerifierRejectsJoinedEOFAndNegativeCount(t *testing.T) {
	digest := sha256.Sum256(nil)
	joinedCause := errors.New("private joined error")
	tests := []struct {
		name   string
		size   uint64
		reader io.Reader
	}{
		{name: "joined eof", size: 0, reader: readerFunc(func([]byte) (int, error) { return 0, errors.Join(io.EOF, joinedCause) })},
		{name: "negative count", size: 1, reader: readerFunc(func([]byte) (int, error) { return -1, nil })},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verifier := newExactVerifier(context.Background(), tt.reader, tt.size, digest)
			_, err := io.ReadAll(verifier)
			var objectErr *ObjectError
			if !errors.As(err, &objectErr) || objectErr.Code != ObjectErrorSource || verifier.verified {
				t.Fatalf("error=%T %v verified=%v", err, err, verifier.verified)
			}
		})
	}
}

func TestExactVerifierTerminalProbeRejectsNegativeCountWithEOF(t *testing.T) {
	body := []byte("x")
	digest := sha256.Sum256(body)
	reads := 0
	source := readerFunc(func(p []byte) (int, error) {
		reads++
		if reads == 1 {
			return copy(p, body), nil
		}
		return -1, io.EOF
	})
	verifier := newExactVerifier(context.Background(), source, uint64(len(body)), digest)
	_, err := io.ReadAll(verifier)
	var objectErr *ObjectError
	if !errors.As(err, &objectErr) || objectErr.Code != ObjectErrorSource || verifier.verified {
		t.Fatalf("error=%T %v verified=%v", err, err, verifier.verified)
	}
}

func TestObjectReaderCloseRequiresObservedTerminalEOF(t *testing.T) {
	body := []byte("exact")
	digest := sha256.Sum256(body)
	for _, readBytes := range []int{0, 2, len(body)} {
		t.Run(string(rune('0'+readBytes)), func(t *testing.T) {
			underlying := &readCloser{Reader: bytes.NewReader(body)}
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

func TestPutObjectRejectsNilProviderReader(t *testing.T) {
	for _, typed := range []bool{false, true} {
		base := memstore.New()
		var typedNil *readCloser
		script := &scriptedBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}, putFn: drainPut, getFn: func(string) (io.ReadCloser, error) {
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
		script := &scriptedBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}, putFn: drainPut, getFn: func(string) (io.ReadCloser, error) {
			getCount++
			if getCount == 1 {
				return io.NopCloser(bytes.NewReader([]byte("x"))), nil
			}
			if typed {
				var typedNil *readCloser
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

func TestStoreCloseCancelsIdleObjectReaderBeforeProvider(t *testing.T) {
	body := []byte("x")
	digest := sha256.Sum256(body)
	base := memstore.New()
	blocking := newBlockingReadCloser()
	getCount := 0
	script := &scriptedBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}, putFn: drainPut, getFn: func(string) (io.ReadCloser, error) {
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
	script := &scriptedBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}, putFn: drainPut, getFn: func(string) (io.ReadCloser, error) {
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
	underlying := &readCloser{Reader: bytes.NewReader(body), closeErr: closeCause}
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
	script := &scriptedBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}, putFn: drainPut, getFn: func(string) (io.ReadCloser, error) {
		return &readCloser{Reader: readerFunc(func([]byte) (int, error) { return 0, readCause }), closeErr: closeCause}, nil
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

// TestExactVerifierCancellationBoundsNextRead proves the pre-read cancellation
// check: once the operation context is done, the verifier must refuse the next
// read instead of entering a source read that may never return.
func TestExactVerifierCancellationBoundsNextRead(t *testing.T) {
	body := []byte("ab")
	digest := sha256.Sum256(body)
	unblock := make(chan struct{})
	t.Cleanup(func() { close(unblock) })
	reads := 0
	source := readerFunc(func(p []byte) (int, error) {
		reads++
		if reads == 1 {
			p[0] = body[0]
			return 1, nil
		}
		<-unblock
		return 0, io.EOF
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	verifier := newExactVerifier(ctx, source, uint64(len(body)), digest)
	p := make([]byte, 1)
	if n, err := verifier.Read(p); n != 1 || err != nil {
		t.Fatalf("first Read = %d, %v", n, err)
	}
	cancel()
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := verifier.Read(p)
		done <- result{n: n, err: err}
	}()
	select {
	case got := <-done:
		var objErr *ObjectError
		if !errors.As(got.err, &objErr) || objErr.Code != ObjectErrorCanceled {
			t.Fatalf("post-cancel Read = %d, %T %v, want canceled", got.n, got.err, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("post-cancel Read entered a blocking source read instead of failing closed")
	}
}

// TestObjectReaderConcurrentReadsAndCloseTerminateOnce pins the documented
// concurrency invariants: whichever of two readers and a closer wins, the
// provider is closed once, admission is released once, and Close then keeps
// returning the same terminal error.
func TestObjectReaderConcurrentReadsAndCloseTerminateOnce(t *testing.T) {
	body := []byte("concurrent")
	digest := sha256.Sum256(body)
	for iteration := 0; iteration < 200; iteration++ {
		underlying := &readCloser{Reader: bytes.NewReader(body)}
		var releases atomic.Int32
		reader := &objectReader{
			verifier:   newExactVerifier(context.Background(), underlying, uint64(len(body)), digest),
			underlying: underlying,
			release:    func() { releases.Add(1) },
		}
		var wg sync.WaitGroup
		wg.Add(3)
		for worker := 0; worker < 2; worker++ {
			go func() {
				defer wg.Done()
				_, _ = io.ReadAll(reader)
			}()
		}
		go func() {
			defer wg.Done()
			_ = reader.Close()
		}()
		wg.Wait()
		if got := underlying.closes.Load(); got != 1 {
			t.Fatalf("iteration %d: provider closes = %d, want 1", iteration, got)
		}
		if got := releases.Load(); got != 1 {
			t.Fatalf("iteration %d: releases = %d, want 1", iteration, got)
		}
		if first, second := reader.Close(), reader.Close(); first != second {
			t.Fatalf("iteration %d: Close is not stable: %v then %v", iteration, first, second)
		}
	}
}

// TestGetObjectRejectsProviderErrorWrappingEOF pins the identity-based EOF
// policy where it decides an integrity outcome. A provider that returns an
// error merely wrapping io.EOF on byte-perfect content has reported something
// besides end of stream, so the stream is unverified: completeTermination must
// compare with == and not errors.Is, or Close reports success on data that was
// never verified.
func TestGetObjectRejectsProviderErrorWrappingEOF(t *testing.T) {
	body := []byte("persisted")
	digest := sha256.Sum256(body)
	base := memstore.New()
	gets := 0
	script := &scriptedBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}, putFn: drainPut, getFn: func(string) (io.ReadCloser, error) {
		gets++
		if gets == 1 {
			return io.NopCloser(bytes.NewReader(body)), nil
		}
		sent := false
		return &readCloser{Reader: readerFunc(func(p []byte) (int, error) {
			if sent {
				return 0, io.EOF
			}
			sent = true
			return copy(p, body), fmt.Errorf("provider drained: %w", io.EOF)
		})}, nil
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
	if _, err := io.ReadAll(reader); err == nil {
		t.Fatal("read of a stream whose provider wrapped io.EOF reached success")
	}
	closeErr := reader.Close()
	var objErr *ObjectError
	if !errors.As(closeErr, &objErr) || objErr.Code != ObjectErrorBackend {
		t.Fatalf("Close = %T %v, want a backend failure, not success on unverified data", closeErr, closeErr)
	}
}

// gatedBlobs holds a Get inside the provider until the test releases it, so a
// test can place Store shutdown at an exact point in GetObject rather than
// hoping the scheduler lands it there.
type gatedBlobs struct {
	lifecycleBlobs
	gate chan struct{}
	// awaitCancel additionally holds the Get until the OPERATION context is
	// cancelled. Store shutdown propagates to that context through its own
	// context.AfterFunc goroutine, so releasing on the Store context alone
	// still usually reaches the constructor with a live operation context.
	awaitCancel atomic.Bool
}

func (b *gatedBlobs) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	<-b.gate
	if b.awaitCancel.Load() {
		<-ctx.Done()
	}
	return b.Blobs.Get(context.WithoutCancel(ctx), key)
}

// TestGetObjectRacesStoreClose guards the object stream's half of the shared
// cancellation handshake (bindCancelHandle). GetObject registers a shutdown
// hook that reaches into the reader it is still constructing, and
// context.AfterFunc runs that hook in a NEW goroutine immediately when the
// operation context is already done — so the hook's completeTermination can
// read stopCancel before the constructor has published it.
//
// The window is placed deterministically: the provider Get is held open until
// shutdown has demonstrably cancelled the Store context, so registration always
// happens against an already-done context. Probabilistic scheduling does not
// reach this reliably — an earlier version of this test looped a hundred times
// without once exposing an unsynchronized publish.
func TestGetObjectRacesStoreClose(t *testing.T) {
	// The repetition is what makes this guard reliable, and is not incidental:
	// the defect it catches has no consequence other than the race itself, so
	// only -race can see it, and only when the accesses actually interleave. A
	// single window kills the unsynchronized publish about four times in five.
	// Each window costs a few milliseconds; do not collapse this back into one.
	for attempt := 0; attempt < 24; attempt++ {
		getObjectShutdownWindow(t)
		if t.Failed() {
			return
		}
	}
}

func getObjectShutdownWindow(t *testing.T) {
	t.Helper()
	body := []byte("streamed during shutdown")
	digest := sha256.Sum256(body)
	backend := memstore.New()
	gate := make(chan struct{})
	backend.Blobs = &gatedBlobs{lifecycleBlobs: lifecycleBlobs{backend.Blobs}, gate: gate}
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	close(gate)
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{
		TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact,
		SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	gate = make(chan struct{})
	blobs := backend.Blobs.(*gatedBlobs)
	blobs.gate = gate
	blobs.awaitCancel.Store(true)
	// Many readers are released into the window at once: the hook goroutine and
	// the constructor that must publish its handle are only a few instructions
	// apart, so a single reader almost always wins the assignment and hides the
	// unsynchronized publish behind the mutex it takes next.
	var wg sync.WaitGroup
	for reader := 0; reader < 64; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream, err := store.GetObject(context.Background(), GetObjectRequest{
				TenantID: "tenant", SessionID: "session", ExpectedKind: ObjectKindArtifact, Metadata: metadata,
			})
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, stream)
			_ = stream.Close()
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = store.Close(closeCtx)
	}()
	// Close cancels the Store context before it waits for admitted work, so this
	// returns while the reader is still parked inside the provider. The parked
	// Get then waits for its own operation context, so the hook is always
	// registered against an already-done context.
	<-store.ctx.Done()
	close(gate)
	wg.Wait()

	drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := store.Close(drainCtx); err != nil {
		t.Fatalf("shutdown did not drain: %v", err)
	}
}
