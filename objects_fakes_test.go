// Fakes and helpers shared by the object tests.
package sessionstore

import (
	"context"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// readerFunc is the one reader fake: anything a test needs a source to do is a
// closure, so no per-behaviour reader type is necessary.
type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// failingReader fails every read with a message that must never reach a caller.
func failingReader() readerFunc {
	return func([]byte) (int, error) { return 0, errors.New("private source failure") }
}

// stalledReader makes no progress: no bytes and no error, forever.
func stalledReader() readerFunc {
	return func([]byte) (int, error) { return 0, nil }
}

// eofTogetherReader returns its whole body together with io.EOF on the first
// read, the legal but awkward shape io.Reader permits.
func eofTogetherReader(body []byte) readerFunc {
	done := false
	return func(p []byte) (int, error) {
		if done {
			return 0, io.EOF
		}
		done = true
		return copy(p, body), io.EOF
	}
}

// readCloser is the one ReadCloser fake: it counts Close calls and optionally
// reports a Close error.
type readCloser struct {
	io.Reader
	closeErr error
	closes   atomic.Int32
}

func (r *readCloser) Close() error {
	r.closes.Add(1)
	return r.closeErr
}

func drainPut(_ string, r io.Reader) error { _, err := io.Copy(io.Discard, r); return err }

// lifecycleBlobs supplies the storage.BlobReaderLifecycle capability that Open
// requires, by forwarding to the wrapped provider. Every Blobs fake in the
// package embeds it instead of restating the forwarder.
type lifecycleBlobs struct{ storage.Blobs }

func (b lifecycleBlobs) BlobReaderCloseBound() time.Duration {
	return b.Blobs.(storage.BlobReaderLifecycle).BlobReaderCloseBound()
}

type scriptedBlobs struct {
	lifecycleBlobs
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

type bindingCheckingBlobs struct {
	lifecycleBlobs
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

type countingBlobs struct {
	lifecycleBlobs
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
func objectMetadata(kind ObjectKind, generation [16]byte, size uint64, digest [32]byte, mediaType string) sessionwire.ObjectMetadata {
	g := strings.ToLower(base32.HexEncoding.WithPadding(base32.NoPadding).EncodeToString(generation[:]))
	d := hex.EncodeToString(digest[:])
	return sessionwire.ObjectMetadata{Reference: sessionwire.ObjectReference{ObjectID: "v1:" + string(kind) + ":" + g + ":" + d}, SizeBytes: size, Digest: "sha256:" + d, MediaType: mediaType}
}

type adminRecordingBlobs struct {
	lifecycleBlobs
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

var _ storage.BlobReaderLifecycle = (*scriptedBlobs)(nil)
