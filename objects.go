package sessionstore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"mime"
	"strings"
	"sync"
	"unicode/utf8"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

type ObjectKind string

const (
	ObjectKindJournalPublic       ObjectKind = "journal-public"
	ObjectKindJournalRuntime      ObjectKind = "journal-runtime"
	ObjectKindCommandPayload      ObjectKind = "command-payload"
	ObjectKindToolResult          ObjectKind = "tool-result"
	ObjectKindWorkspaceCheckpoint ObjectKind = "workspace-checkpoint"
	ObjectKindRuntimeCheckpoint   ObjectKind = "runtime-checkpoint"
	ObjectKindArtifact            ObjectKind = "artifact"
	ObjectKindAttachment          ObjectKind = "attachment"
	ObjectKindContinuation        ObjectKind = "continuation"
	ObjectKindRuntimeObject       ObjectKind = "runtime-object"
)

func (k ObjectKind) valid() bool {
	switch k {
	case ObjectKindJournalPublic, ObjectKindJournalRuntime, ObjectKindCommandPayload,
		ObjectKindToolResult, ObjectKindWorkspaceCheckpoint, ObjectKindRuntimeCheckpoint,
		ObjectKindArtifact, ObjectKindAttachment, ObjectKindContinuation, ObjectKindRuntimeObject:
		return true
	default:
		return false
	}
}

type PutObjectRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	Kind      ObjectKind
	SizeBytes uint64
	SHA256    [32]byte
	MediaType string
	Body      io.Reader
}

type GetObjectRequest struct {
	TenantID     sessionwire.TenantID
	SessionID    sessionwire.SessionID
	ExpectedKind ObjectKind
	Metadata     sessionwire.ObjectMetadata
}

type ObjectErrorCode string

const (
	ObjectErrorInvalid   ObjectErrorCode = "invalid"
	ObjectErrorSize      ObjectErrorCode = "size"
	ObjectErrorDigest    ObjectErrorCode = "digest"
	ObjectErrorSource    ObjectErrorCode = "source"
	ObjectErrorBackend   ObjectErrorCode = "backend"
	ObjectErrorConflict  ObjectErrorCode = "conflict"
	ObjectErrorIntegrity ObjectErrorCode = "integrity"
	ObjectErrorCanceled  ObjectErrorCode = "canceled"
)

type ObjectError struct {
	Code  ObjectErrorCode
	Field string
	Cause error
}

func (e *ObjectError) Error() string {
	message := "sessionstore: object " + string(e.Code)
	if e.Field != "" {
		message += " (" + e.Field + ")"
	}
	return message
}
func (e *ObjectError) Unwrap() error { return e.Cause }
func objectErr(code ObjectErrorCode, field string, cause error) error {
	return &ObjectError{Code: code, Field: field, Cause: cause}
}

func randomObjectGeneration() ([16]byte, error) {
	var generation [16]byte
	_, err := io.ReadFull(rand.Reader, generation[:])
	return generation, err
}

func (s *Store) PutObject(ctx context.Context, req PutObjectRequest) (sessionwire.ObjectMetadata, error) {
	if !req.Kind.valid() {
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorInvalid, "kind", nil)
	}
	if req.Body == nil {
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorInvalid, "body", nil)
	}
	if err := validateMediaType(req.MediaType); err != nil {
		return sessionwire.ObjectMetadata{}, err
	}
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return sessionwire.ObjectMetadata{}, err
	}
	generation, err := s.objectGeneration()
	if err != nil {
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorSource, "generation", err)
	}
	metadata := objectMetadataFor(req.Kind, generation, req.SizeBytes, req.SHA256, req.MediaType)
	parsed, err := parseObjectMetadata(metadata)
	if err != nil {
		return sessionwire.ObjectMetadata{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return sessionwire.ObjectMetadata{}, err
	}
	defer release()
	if err := s.bindSessionScope(opCtx, scope); err != nil {
		return sessionwire.ObjectMetadata{}, err
	}
	key := objectKey(scope, parsed)
	verifier := newExactVerifier(opCtx, req.Body, req.SizeBytes, req.SHA256)
	if err := s.backend.Blobs.Put(opCtx, key, verifier); err != nil {
		if verifier.failure != nil {
			return sessionwire.ObjectMetadata{}, verifier.failure
		}
		var conflict *storage.BlobConflictError
		if errors.As(err, &conflict) && conflict.Key == key {
			return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorConflict, "blob", err)
		}
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorBackend, "put", err)
	}
	if !verifier.verified {
		if verifier.failure != nil {
			return sessionwire.ObjectMetadata{}, verifier.failure
		}
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorIntegrity, "put_eof", nil)
	}
	stored, err := s.backend.Blobs.Get(opCtx, key)
	if err != nil {
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorBackend, "post_get", err)
	}
	post := newBackendExactVerifier(opCtx, stored, req.SizeBytes, req.SHA256)
	_, copyErr := io.Copy(io.Discard, post)
	closeErr := stored.Close()
	if copyErr != nil {
		return sessionwire.ObjectMetadata{}, copyErr
	}
	if !post.verified {
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorIntegrity, "post_eof", post.failure)
	}
	if closeErr != nil {
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorBackend, "post_close", closeErr)
	}
	return metadata, nil
}

func (s *Store) GetObject(ctx context.Context, req GetObjectRequest) (io.ReadCloser, error) {
	if !req.ExpectedKind.valid() {
		return nil, objectErr(ObjectErrorInvalid, "expected_kind", nil)
	}
	parsed, err := parseObjectMetadata(req.Metadata)
	if err != nil {
		return nil, err
	}
	if parsed.kind != req.ExpectedKind {
		return nil, objectErr(ObjectErrorInvalid, "expected_kind", nil)
	}
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return nil, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.verifySessionScope(opCtx, scope); err != nil {
		release()
		return nil, err
	}
	reader, err := s.backend.Blobs.Get(opCtx, objectKey(scope, parsed))
	if err != nil {
		release()
		return nil, objectErr(ObjectErrorBackend, "get", err)
	}
	return &objectReader{verifier: newBackendExactVerifier(opCtx, reader, parsed.size, parsed.digest), underlying: reader, release: release}, nil
}

type parsedObject struct {
	kind       ObjectKind
	generation string
	digest     [32]byte
	size       uint64
}

func parseObjectMetadata(metadata sessionwire.ObjectMetadata) (parsedObject, error) {
	if err := metadata.Reference.Validate(); err != nil {
		return parsedObject{}, objectErr(ObjectErrorInvalid, "object_id", err)
	}
	parts := strings.Split(metadata.Reference.ObjectID, ":")
	if len(parts) != 4 || parts[0] != "v1" {
		return parsedObject{}, objectErr(ObjectErrorInvalid, "object_id", nil)
	}
	kind := ObjectKind(parts[1])
	if !kind.valid() {
		return parsedObject{}, objectErr(ObjectErrorInvalid, "kind", nil)
	}
	if len(parts[2]) != 26 {
		return parsedObject{}, objectErr(ObjectErrorInvalid, "generation", nil)
	}
	gen, err := base32.HexEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(parts[2]))
	if err != nil || len(gen) != 16 || strings.ToLower(base32.HexEncoding.WithPadding(base32.NoPadding).EncodeToString(gen)) != parts[2] {
		return parsedObject{}, objectErr(ObjectErrorInvalid, "generation", err)
	}
	if len(parts[3]) != 64 {
		return parsedObject{}, objectErr(ObjectErrorInvalid, "digest", nil)
	}
	var digest [32]byte
	if _, err := hex.Decode(digest[:], []byte(parts[3])); err != nil || hex.EncodeToString(digest[:]) != parts[3] {
		return parsedObject{}, objectErr(ObjectErrorInvalid, "digest", err)
	}
	if metadata.Digest != "sha256:"+parts[3] {
		return parsedObject{}, objectErr(ObjectErrorDigest, "digest", nil)
	}
	if err := validateMediaType(metadata.MediaType); err != nil {
		return parsedObject{}, err
	}
	return parsedObject{kind: kind, generation: parts[2], digest: digest, size: metadata.SizeBytes}, nil
}

func objectMetadataFor(kind ObjectKind, generation [16]byte, size uint64, digest [32]byte, mediaType string) sessionwire.ObjectMetadata {
	g := strings.ToLower(base32.HexEncoding.WithPadding(base32.NoPadding).EncodeToString(generation[:]))
	d := hex.EncodeToString(digest[:])
	return sessionwire.ObjectMetadata{Reference: sessionwire.ObjectReference{ObjectID: "v1:" + string(kind) + ":" + g + ":" + d}, SizeBytes: size, MediaType: mediaType, Digest: "sha256:" + d}
}

func objectKey(scope sessionScope, object parsedObject) string {
	return scope.BlobPrefix + "v1/" + string(object.kind) + "/" + hex.EncodeToString(object.digest[:]) + "/" + object.generation
}

func validateMediaType(value string) error {
	if value == "" {
		return nil
	}
	if len(value) > 256 || !utf8.ValidString(value) {
		return objectErr(ObjectErrorInvalid, "media_type", nil)
	}
	if _, _, err := mime.ParseMediaType(value); err != nil {
		return objectErr(ObjectErrorInvalid, "media_type", err)
	}
	return nil
}

type exactVerifier struct {
	ctx      context.Context
	source   io.Reader
	expected uint64
	digest   [32]byte
	hash     hash.Hash
	read     uint64
	verified bool
	failure  error
	readCode ObjectErrorCode
}

func newExactVerifier(ctx context.Context, source io.Reader, size uint64, digest [32]byte) *exactVerifier {
	return newExactVerifierWithReadCode(ctx, source, size, digest, ObjectErrorSource)
}

func newBackendExactVerifier(ctx context.Context, source io.Reader, size uint64, digest [32]byte) *exactVerifier {
	return newExactVerifierWithReadCode(ctx, source, size, digest, ObjectErrorBackend)
}

func newExactVerifierWithReadCode(ctx context.Context, source io.Reader, size uint64, digest [32]byte, readCode ObjectErrorCode) *exactVerifier {
	return &exactVerifier{ctx: ctx, source: source, expected: size, digest: digest, hash: sha256.New(), readCode: readCode}
}
func (v *exactVerifier) Read(p []byte) (int, error) {
	if v.verified {
		return 0, io.EOF
	}
	if v.failure != nil {
		return 0, v.failure
	}
	select {
	case <-v.ctx.Done():
		v.failure = objectErr(ObjectErrorCanceled, "stream", v.ctx.Err())
		return 0, v.failure
	default:
	}
	if len(p) == 0 {
		return 0, nil
	}
	remaining := v.expected - v.read
	if remaining == 0 {
		var probe [1]byte
		n, err := v.source.Read(probe[:])
		if n > 0 {
			v.failure = objectErr(ObjectErrorSize, "stream", nil)
			return 0, v.failure
		}
		return v.finishRead(0, err)
	}
	limit := uint64(len(p))
	if remaining < limit {
		limit = remaining
	}
	n, err := v.source.Read(p[:limit])
	if n < 0 || uint64(n) > limit {
		v.failure = objectErr(v.readCode, "stream", errors.New("invalid reader count"))
		return 0, v.failure
	}
	if n > 0 {
		v.read += uint64(n)
		_, _ = v.hash.Write(p[:n])
	}
	return v.finishRead(n, err)
}

func (v *exactVerifier) finishRead(n int, err error) (int, error) {
	if err != nil && !errors.Is(err, io.EOF) {
		v.failure = objectErr(v.readCode, "stream", err)
		return n, v.failure
	}
	if errors.Is(err, io.EOF) {
		if v.read != v.expected {
			v.failure = objectErr(ObjectErrorSize, "stream", nil)
			return n, v.failure
		}
		if !equalBytes(v.hash.Sum(nil), v.digest[:]) {
			v.failure = objectErr(ObjectErrorDigest, "stream", nil)
			return n, v.failure
		}
		v.verified = true
		return n, io.EOF
	}
	if n == 0 {
		v.failure = objectErr(v.readCode, "stream", io.ErrNoProgress)
		return 0, v.failure
	}
	return n, nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

type objectReader struct {
	verifier   *exactVerifier
	underlying io.Closer
	release    func()
	once       sync.Once
	closeErr   error
}

func (r *objectReader) Read(p []byte) (int, error) {
	n, err := r.verifier.Read(p)
	if err != nil {
		r.finish()
	}
	return n, err
}
func (r *objectReader) Close() error { r.finish(); return r.closeErr }
func (r *objectReader) finish()      { r.once.Do(func() { r.closeErr = r.underlying.Close(); r.release() }) }
