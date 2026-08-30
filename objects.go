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
	"reflect"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// ObjectKind is a closed semantic class for immutable session objects.
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

// PutObjectRequest declares an immutable object's exact content properties.
// MediaType is optional, bounded, validated descriptive metadata; it is
// untrusted and does not participate in object identity.
type PutObjectRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	Kind      ObjectKind
	SizeBytes uint64
	SHA256    [32]byte
	MediaType string
	Body      io.Reader
}

// GetObjectRequest names a verified object and the semantic kind the caller is
// authorized to consume.
type GetObjectRequest struct {
	TenantID     sessionwire.TenantID
	SessionID    sessionwire.SessionID
	ExpectedKind ObjectKind
	Metadata     sessionwire.ObjectMetadata
}

// ObjectErrorCode classifies redacted object operation failures.
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

// ObjectError is a typed, redacted object operation failure.
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

// PutObject streams, verifies, persists, and re-verifies an immutable object
// before returning its metadata.
func (s *Store) PutObject(ctx context.Context, req PutObjectRequest) (sessionwire.ObjectMetadata, error) {
	if !req.Kind.valid() {
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorInvalid, "kind", nil)
	}
	if isNilDynamic(reflect.ValueOf(req.Body)) {
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorInvalid, "body", nil)
	}
	if req.SHA256 == ([32]byte{}) {
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorInvalid, "sha256", nil)
	}
	if err := validateMediaType(req.MediaType); err != nil {
		return sessionwire.ObjectMetadata{}, err
	}
	scope, err := s.deriveSessionScope(req.TenantID, req.SessionID)
	if err != nil {
		return sessionwire.ObjectMetadata{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return sessionwire.ObjectMetadata{}, err
	}
	defer release()
	generation, err := s.objectGeneration()
	if err != nil {
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorSource, "generation", err)
	}
	metadata := objectMetadataFor(req.Kind, generation, req.SizeBytes, req.SHA256, req.MediaType)
	parsed, err := parseObjectMetadata(metadata)
	if err != nil {
		return sessionwire.ObjectMetadata{}, err
	}
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
	if isNilDynamic(reflect.ValueOf(stored)) {
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorBackend, "post_get", nil)
	}
	post := newBackendExactVerifier(opCtx, stored, req.SizeBytes, req.SHA256)
	_, copyErr := io.Copy(io.Discard, post)
	closeErr := stored.Close()
	if copyErr != nil {
		if closeErr != nil {
			return sessionwire.ObjectMetadata{}, errors.Join(copyErr, objectErr(ObjectErrorBackend, "post_close", closeErr))
		}
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

// GetObject returns a lifecycle-held verified stream. A caller establishes
// integrity only by reading through terminal EOF; premature Close is an error.
// Provider readers must make concurrent Close unblock Read so Store shutdown
// can cancel outstanding streams before closing an owned provider. A shutdown-
// triggered reader Close error is latched on that reader; Store.Close orders the
// cleanup but does not aggregate an error from a reader the caller abandoned.
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
	if isNilDynamic(reflect.ValueOf(reader)) {
		release()
		return nil, objectErr(ObjectErrorBackend, "get", nil)
	}
	result := &objectReader{verifier: newBackendExactVerifier(opCtx, reader, parsed.size, parsed.digest), underlying: reader, release: release}
	stopCancel := context.AfterFunc(opCtx, func() {
		result.beginTermination(objectErr(ObjectErrorCanceled, "stream", opCtx.Err()))
	})
	result.mu.Lock()
	result.stopCancel = stopCancel
	done := result.done
	result.mu.Unlock()
	if done {
		stopCancel()
	}
	return result, nil
}

type parsedObject struct {
	kind       ObjectKind
	generation string
	digest     [32]byte
	size       uint64
}

func parseObjectMetadata(metadata sessionwire.ObjectMetadata) (parsedObject, error) {
	parsed, err := parseObjectReference(metadata.Reference)
	if err != nil {
		return parsedObject{}, err
	}
	if metadata.Digest != "sha256:"+hex.EncodeToString(parsed.digest[:]) {
		return parsedObject{}, objectErr(ObjectErrorDigest, "digest", nil)
	}
	if err := validateMediaType(metadata.MediaType); err != nil {
		return parsedObject{}, err
	}
	parsed.size = metadata.SizeBytes
	return parsed, nil
}

func parseObjectReference(reference sessionwire.ObjectReference) (parsedObject, error) {
	if err := reference.Validate(); err != nil {
		return parsedObject{}, objectErr(ObjectErrorInvalid, "object_id", err)
	}
	parts := strings.Split(reference.ObjectID, ":")
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
	if digest == ([32]byte{}) {
		return parsedObject{}, objectErr(ObjectErrorInvalid, "digest", nil)
	}
	return parsedObject{kind: kind, generation: parts[2], digest: digest}, nil
}

func objectMetadataFor(kind ObjectKind, generation [16]byte, size uint64, digest [32]byte, mediaType string) sessionwire.ObjectMetadata {
	g := strings.ToLower(base32.HexEncoding.WithPadding(base32.NoPadding).EncodeToString(generation[:]))
	d := hex.EncodeToString(digest[:])
	return sessionwire.ObjectMetadata{Reference: sessionwire.ObjectReference{ObjectID: "v1:" + string(kind) + ":" + g + ":" + d}, SizeBytes: size, MediaType: mediaType, Digest: "sha256:" + d}
}

func objectKey(scope sessionScope, object parsedObject) string {
	return scope.BlobPrefix + "v1/" + string(object.kind) + "/" + hex.EncodeToString(object.digest[:]) + "/" + object.generation
}

// listObjectReferences is an internal administrative operation over one
// verified tenant/session and one exact V1 kind prefix. Historical legacy
// digest-only keys are intentionally excluded for the later replay resolver.
func (s *Store) listObjectReferences(ctx context.Context, tenantID sessionwire.TenantID, sessionID sessionwire.SessionID, kind ObjectKind) ([]sessionwire.ObjectReference, error) {
	if !kind.valid() {
		return nil, objectErr(ObjectErrorInvalid, "kind", nil)
	}
	scope, err := s.deriveSessionScope(tenantID, sessionID)
	if err != nil {
		return nil, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.verifySessionScope(opCtx, scope); err != nil {
		return nil, err
	}
	prefix := scope.BlobPrefix + "v1/" + string(kind) + "/"
	keys, err := s.backend.Blobs.List(opCtx, prefix)
	if err != nil {
		return nil, objectErr(ObjectErrorBackend, "list", err)
	}
	refs := make([]sessionwire.ObjectReference, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		parsed, err := parsePhysicalObjectKey(prefix, kind, key)
		if err != nil {
			return nil, err
		}
		ref := objectMetadataFor(parsed.kind, parsedGeneration(parsed.generation), 0, parsed.digest, "").Reference
		if _, exists := seen[ref.ObjectID]; exists {
			return nil, objectErr(ObjectErrorIntegrity, "list_duplicate", nil)
		}
		seen[ref.ObjectID] = struct{}{}
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].ObjectID < refs[j].ObjectID })
	return refs, nil
}

// deleteObject is an internal administrative deletion of one strictly parsed
// V1 reference after Get-only scope verification. It never resolves historical
// digest-only keys and never binds a missing session.
func (s *Store) deleteObject(ctx context.Context, tenantID sessionwire.TenantID, sessionID sessionwire.SessionID, expectedKind ObjectKind, reference sessionwire.ObjectReference) error {
	if !expectedKind.valid() {
		return objectErr(ObjectErrorInvalid, "expected_kind", nil)
	}
	parsed, err := parseObjectReference(reference)
	if err != nil {
		return err
	}
	if parsed.kind != expectedKind {
		return objectErr(ObjectErrorInvalid, "expected_kind", nil)
	}
	scope, err := s.deriveSessionScope(tenantID, sessionID)
	if err != nil {
		return err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return err
	}
	defer release()
	if err := s.verifySessionScope(opCtx, scope); err != nil {
		return err
	}
	if err := s.backend.Blobs.Delete(opCtx, objectKey(scope, parsed)); err != nil {
		return objectErr(ObjectErrorBackend, "delete", err)
	}
	return nil
}

func parsePhysicalObjectKey(prefix string, kind ObjectKind, key string) (parsedObject, error) {
	if prefix == "" || !strings.HasPrefix(key, prefix) {
		return parsedObject{}, objectErr(ObjectErrorIntegrity, "list_key", nil)
	}
	parts := strings.Split(strings.TrimPrefix(key, prefix), "/")
	if len(parts) != 2 {
		return parsedObject{}, objectErr(ObjectErrorIntegrity, "list_key", nil)
	}
	reference := sessionwire.ObjectReference{ObjectID: "v1:" + string(kind) + ":" + parts[1] + ":" + parts[0]}
	parsed, err := parseObjectReference(reference)
	if err != nil || prefix+parts[0]+"/"+parts[1] != key {
		return parsedObject{}, objectErr(ObjectErrorIntegrity, "list_key", err)
	}
	return parsed, nil
}

func parsedGeneration(value string) [16]byte {
	decoded, _ := base32.HexEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(value))
	var generation [16]byte
	copy(generation[:], decoded)
	return generation
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
		if n < 0 {
			v.failure = objectErr(v.readCode, "stream", errors.New("invalid reader count"))
			return 0, v.failure
		}
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
	if v.ctx.Err() != nil {
		v.failure = objectErr(ObjectErrorCanceled, "stream", v.ctx.Err())
		if err != nil && err != io.EOF {
			v.failure = joinErrors(v.failure, objectErr(v.readCode, "stream", err))
		}
		return n, v.failure
	}
	if err != nil && err != io.EOF {
		v.failure = objectErr(v.readCode, "stream", err)
		return n, v.failure
	}
	if err == io.EOF {
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
	mu         sync.Mutex
	cond       *sync.Cond
	reading    bool
	closing    bool
	done       bool
	primary    error
	readErr    error
	terminal   error
	closeErr   error
	stopCancel func() bool
}

func (r *objectReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	cond := r.condLocked()
	for r.reading && !r.closing {
		cond.Wait()
	}
	if r.done {
		terminal := r.terminal
		r.mu.Unlock()
		return 0, terminal
	}
	if r.closing {
		for !r.done {
			cond.Wait()
		}
		terminal := r.terminal
		r.mu.Unlock()
		return 0, terminal
	}
	r.reading = true
	r.mu.Unlock()

	n, err := r.verifier.Read(p)
	r.mu.Lock()
	r.reading = false
	cond.Broadcast()
	owner := false
	if err != nil {
		if !r.closing {
			r.closing = true
			r.primary = err
			owner = true
		} else {
			r.readErr = err
		}
	}
	if err == nil && !r.closing {
		r.mu.Unlock()
		return n, nil
	}
	r.mu.Unlock()
	if owner {
		r.completeTermination()
	}
	return n, r.waitTerminal()
}
func (r *objectReader) Close() error {
	r.beginTermination(objectErr(ObjectErrorIntegrity, "incomplete", nil))
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closeErr
}

func (r *objectReader) beginTermination(cause error) {
	r.mu.Lock()
	cond := r.condLocked()
	if r.done {
		r.mu.Unlock()
		return
	}
	if r.closing {
		for !r.done {
			cond.Wait()
		}
		r.mu.Unlock()
		return
	}
	r.closing = true
	r.primary = cause
	r.mu.Unlock()
	r.completeTermination()
}

func (r *objectReader) completeTermination() {
	r.mu.Lock()
	stopCancel := r.stopCancel
	r.mu.Unlock()
	if stopCancel != nil {
		stopCancel()
	}
	closeErr := r.underlying.Close()
	var wrappedClose error
	if closeErr != nil {
		wrappedClose = objectErr(ObjectErrorBackend, "close", closeErr)
	}

	r.mu.Lock()
	cond := r.condLocked()
	for r.reading {
		cond.Wait()
	}
	r.terminal = joinErrors(r.primary, r.readErr, wrappedClose)
	if errors.Is(r.primary, io.EOF) {
		r.closeErr = joinErrors(r.readErr, wrappedClose)
	} else {
		r.closeErr = r.terminal
	}
	r.done = true
	cond.Broadcast()
	r.mu.Unlock()
	r.release()
}

func (r *objectReader) waitTerminal() error {
	r.mu.Lock()
	cond := r.condLocked()
	for !r.done {
		cond.Wait()
	}
	terminal := r.terminal
	r.mu.Unlock()
	return terminal
}

func (r *objectReader) condLocked() *sync.Cond {
	if r.cond == nil {
		r.cond = sync.NewCond(&r.mu)
	}
	return r.cond
}

func joinErrors(values ...error) error {
	var result error
	for _, value := range values {
		if value == nil {
			continue
		}
		if result == nil {
			result = value
		} else {
			result = errors.Join(result, value)
		}
	}
	return result
}
