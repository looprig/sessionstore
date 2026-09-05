package sessionstore

import (
	"bytes"
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
//
// SizeBytes is the exact byte length of Body, not a hint: a body that ends
// early or runs long is rejected. SessionStore imposes no ceiling on it, by
// design — the effective bound is whatever the storage provider accepts.
// Verification streams through a fixed buffer and nothing here allocates in
// proportion to SizeBytes, so a large declared size costs a rejected write, not
// memory.
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

// objectEntropy is the process-wide source of object generation randomness. It
// is a variable only so tests can substitute a faulting source; production code
// never rebinds it and the package exposes no way to inject one.
var objectEntropy io.Reader = rand.Reader

// randomObjectGeneration draws a cryptographically random 128-bit immutable
// instance generation. Any entropy fault, including a short read, fails closed
// with a zero generation so no caller can persist a partially random one.
func randomObjectGeneration() ([16]byte, error) {
	var generation [16]byte
	if _, err := io.ReadFull(objectEntropy, generation[:]); err != nil {
		return [16]byte{}, err
	}
	return generation, nil
}

// PutObject streams, verifies, persists, and re-verifies an immutable object
// before returning its metadata. The stages are: static validation, admission,
// minting an identity, writing the blob, re-reading it back, and create-only
// persistence of its scoped metadata index for GetObjectMetadata.
//
// The declared SizeBytes and SHA256 are exact: the body is accepted only if it
// ends at that length with that digest, and neither the caller's metadata nor
// any reference is produced otherwise.
//
// Orphan policy. The identity is minted before the write, and PutObject returns
// a reference only after the persisted bytes have been read back and verified
// and the immutable metadata index has committed. A failure after the blob
// commits can therefore leave a blob that no caller was ever told about — an
// orphan, whose verification may also have failed. That is
// deliberate: the alternative, deleting on a post-commit error, would issue a
// delete against a provider that has just proved unreliable, and the blob is
// content- and generation-addressed so it can never be mistaken for another
// object. Reclaiming orphans is the store operator's job, over the
// tenant/session blob prefix; this package's only enumeration path,
// listObjectReferences, is intentionally not exported, so no caller-facing GC
// exists yet.
func (s *Store) PutObject(ctx context.Context, req PutObjectRequest) (sessionwire.ObjectMetadata, error) {
	return s.putObject(ctx, req, ProtocolModeLegacy)
}

func (s *Store) putObject(ctx context.Context, req PutObjectRequest, mode ProtocolMode) (sessionwire.ObjectMetadata, error) {
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
	if mode == ProtocolModeDisposition {
		if req.Kind != ObjectKindCommandPayload {
			return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorInvalid, "kind", nil)
		}
		if _, err := s.dispositionCatalog(opCtx, scope, req.TenantID, req.SessionID); err != nil {
			return sessionwire.ObjectMetadata{}, err
		}
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
	if err := s.bindSessionScopeMode(opCtx, scope, mode); err != nil {
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
	if err := s.verifyPersisted(opCtx, key, req.SizeBytes, req.SHA256); err != nil {
		return sessionwire.ObjectMetadata{}, err
	}
	if err := s.persistObjectMetadata(opCtx, scope, req, metadata, parsed); err != nil {
		return sessionwire.ObjectMetadata{}, err
	}
	return metadata, nil
}

// verifyPersisted reads the just-written blob back and requires it to end at
// exactly the promised length and digest. A Put that the provider accepted but
// stored wrongly is caught here rather than at some later Get, and the Close
// error is reported alongside a read failure rather than replacing it.
//
// A nil copy error already means verification succeeded: exactVerifier returns
// io.EOF only after it has confirmed the length and digest, and reports every
// other outcome as an error, so there is no separate "copied but unverified"
// case to check for here.
func (s *Store) verifyPersisted(ctx context.Context, key string, size uint64, digest [32]byte) error {
	stored, err := s.backend.Blobs.Get(ctx, key)
	if err != nil {
		return objectErr(ObjectErrorBackend, "post_get", err)
	}
	if isNilDynamic(reflect.ValueOf(stored)) {
		return objectErr(ObjectErrorBackend, "post_get", nil)
	}
	post := newBackendExactVerifier(ctx, stored, size, digest)
	_, copyErr := io.Copy(io.Discard, post)
	closeErr := stored.Close()
	if copyErr != nil {
		if closeErr != nil {
			return errors.Join(copyErr, objectErr(ObjectErrorBackend, "post_close", closeErr))
		}
		return copyErr
	}
	if closeErr != nil {
		return objectErr(ObjectErrorBackend, "post_close", closeErr)
	}
	return nil
}

// GetObject returns a lifecycle-held verified stream. A caller establishes
// integrity only by reading through terminal EOF; premature Close is an error.
// Open requires storage.BlobReaderLifecycle so concurrent Close bounds an active
// provider Read and Store shutdown can cancel outstanding streams before closing
// an owned provider. A shutdown-triggered reader Close error is latched on that
// reader; Store.Close orders the cleanup but does not aggregate an error from a
// reader the caller abandoned.
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
	result := &objectReader{
		verifier:   newBackendExactVerifier(opCtx, reader, parsed.size, parsed.digest),
		underlying: reader,
		release:    release,
	}
	bindCancelHandle(opCtx,
		func() { result.beginTermination(objectErr(ObjectErrorCanceled, "stream", opCtx.Err())) },
		func(stop func() bool) bool {
			result.mu.Lock()
			defer result.mu.Unlock()
			result.stopCancel = stop
			return result.done
		})
	return result, nil
}

// Object identity grammar, stated exactly once.
//
//	ObjectID = "v1:" kind ":" generation ":" digest
//	blob key = <session blob prefix> "v1/" kind "/" digest "/" generation
//
// The generation is the 128-bit instance generation in padding-free base32hex
// and the digest is the SHA-256 of the content in hex, both lowercase and both
// canonical: parsing rejects any other spelling of the same bytes. The digest
// precedes the generation in the physical key so that every instance of one
// content digest is adjacent under a List prefix.
const (
	objectSchemeV1     = "v1"
	objectDigestPrefix = "sha256:"
)

// objectGenerationEncoding is the one base32 alphabet generations are spelled
// in; nothing else in the package may choose an encoding.
var objectGenerationEncoding = base32.HexEncoding.WithPadding(base32.NoPadding)

// errNoncanonicalObjectComponent reports an ObjectID component that decodes but
// is not spelled the one canonical way.
var errNoncanonicalObjectComponent = errors.New("sessionstore: noncanonical object component")

func encodeObjectGeneration(generation [16]byte) string {
	return strings.ToLower(objectGenerationEncoding.EncodeToString(generation[:]))
}

// decodeObjectGeneration is the inverse of encodeObjectGeneration and rejects
// any noncanonical spelling, including uppercase and a wrong length. Unlike
// decodeObjectDigest it needs no explicit length precondition, because
// base32.Encoding.DecodeString allocates its own destination rather than
// writing into a fixed array; the canonical re-encode below is what rejects a
// wrong length here.
func decodeObjectGeneration(value string) ([16]byte, error) {
	decoded, err := objectGenerationEncoding.DecodeString(strings.ToUpper(value))
	if err != nil {
		return [16]byte{}, err
	}
	// A wrong decoded length needs no separate check: copy leaves the
	// remainder zero or truncates, and the canonical re-encode below then
	// disagrees with the input.
	var generation [16]byte
	copy(generation[:], decoded)
	if encodeObjectGeneration(generation) != value {
		return [16]byte{}, errNoncanonicalObjectComponent
	}
	return generation, nil
}

// decodeObjectDigest rejects a noncanonical or all-zero content digest. An
// all-zero digest is never minted, so accepting one would name a key the store
// cannot have written.
//
// The length check is a memory-safety precondition, NOT a redundant restatement
// of the canonical round trip below, and must not be deleted as one. hex.Decode
// writes len(src)/2 bytes into the destination and does not bound them by its
// capacity, so any value longer than hex.EncodedLen(32) indexes past the end of
// this fixed array and panics. The input is caller-controlled: sessionwire's
// ObjectReference.Validate caps only the whole ObjectID at MaxIDBytes, which
// leaves roughly 230 characters for this component, so the panic is reachable
// from PutObject, GetObject, and deleteObject before any admission or provider
// I/O. decodeObjectGeneration needs no such check only because
// base32.Encoding.DecodeString allocates its own destination.
func decodeObjectDigest(value string) ([32]byte, error) {
	var digest [32]byte
	if len(value) != hex.EncodedLen(len(digest)) {
		return [32]byte{}, errNoncanonicalObjectComponent
	}
	if _, err := hex.Decode(digest[:], []byte(value)); err != nil {
		return [32]byte{}, err
	}
	if hex.EncodeToString(digest[:]) != value || digest == ([32]byte{}) {
		return [32]byte{}, errNoncanonicalObjectComponent
	}
	return digest, nil
}

// objectIDFor states the ObjectID grammar; parseObjectReference is its inverse.
func objectIDFor(kind ObjectKind, generation, digest string) sessionwire.ObjectReference {
	return sessionwire.ObjectReference{ObjectID: objectSchemeV1 + ":" + string(kind) + ":" + generation + ":" + digest}
}

// objectKindPrefix is the List prefix holding every object of one kind in one
// session.
func objectKindPrefix(scope sessionScope, kind ObjectKind) string {
	return scope.BlobPrefix + objectSchemeV1 + "/" + string(kind) + "/"
}

// objectKeySuffix and splitObjectKeySuffix are inverses and are the only
// statements of the digest-before-generation key ordering. Keep them adjacent:
// changing one without the other makes written keys unresolvable.
func objectKeySuffix(digest, generation string) string {
	return digest + "/" + generation
}

func splitObjectKeySuffix(suffix string) (digest, generation string, ok bool) {
	fields := strings.Split(suffix, "/")
	if len(fields) != 2 {
		return "", "", false
	}
	return fields[0], fields[1], true
}

type parsedObject struct {
	kind       ObjectKind
	generation string
	digest     [32]byte
	size       uint64
}

// parseObjectMetadata validates that caller-supplied metadata is internally
// consistent and canonical, and returns the fields the physical key needs.
func parseObjectMetadata(metadata sessionwire.ObjectMetadata) (parsedObject, error) {
	parsed, err := parseObjectReference(metadata.Reference)
	if err != nil {
		return parsedObject{}, err
	}
	if metadata.Digest != objectDigestPrefix+hex.EncodeToString(parsed.digest[:]) {
		return parsedObject{}, objectErr(ObjectErrorDigest, "digest", nil)
	}
	if err := validateMediaType(metadata.MediaType); err != nil {
		return parsedObject{}, err
	}
	parsed.size = metadata.SizeBytes
	return parsed, nil
}

// parseObjectReference is the inverse of objectIDFor. It accepts only the
// canonical spelling of an identity: lowercase base32hex for the generation and
// lowercase hex for the digest, so one object has exactly one ObjectID and
// therefore exactly one blob key. Component lengths are not restated here; a
// wrong length cannot survive the canonical round trip.
func parseObjectReference(reference sessionwire.ObjectReference) (parsedObject, error) {
	if err := reference.Validate(); err != nil {
		return parsedObject{}, objectErr(ObjectErrorInvalid, "object_id", err)
	}
	parts := strings.Split(reference.ObjectID, ":")
	if len(parts) != 4 || parts[0] != objectSchemeV1 {
		return parsedObject{}, objectErr(ObjectErrorInvalid, "object_id", nil)
	}
	kind := ObjectKind(parts[1])
	if !kind.valid() {
		return parsedObject{}, objectErr(ObjectErrorInvalid, "kind", nil)
	}
	if _, err := decodeObjectGeneration(parts[2]); err != nil {
		return parsedObject{}, objectErr(ObjectErrorInvalid, "generation", err)
	}
	digest, err := decodeObjectDigest(parts[3])
	if err != nil {
		return parsedObject{}, objectErr(ObjectErrorInvalid, "digest", err)
	}
	return parsedObject{kind: kind, generation: parts[2], digest: digest}, nil
}

// objectMetadataFor mints the metadata for one freshly identified object.
func objectMetadataFor(
	kind ObjectKind,
	generation [16]byte,
	size uint64,
	digest [32]byte,
	mediaType string,
) sessionwire.ObjectMetadata {
	encodedDigest := hex.EncodeToString(digest[:])
	return sessionwire.ObjectMetadata{
		Reference: objectIDFor(kind, encodeObjectGeneration(generation), encodedDigest),
		SizeBytes: size,
		MediaType: mediaType,
		Digest:    objectDigestPrefix + encodedDigest,
	}
}

func objectKey(scope sessionScope, object parsedObject) string {
	return objectKindPrefix(scope, object.kind) + objectKeySuffix(hex.EncodeToString(object.digest[:]), object.generation)
}

// listObjectReferences is an internal administrative operation over one
// verified tenant/session and one exact V1 kind prefix. Historical legacy
// digest-only keys are intentionally excluded for the later replay resolver.
func (s *Store) listObjectReferences(
	ctx context.Context,
	tenantID sessionwire.TenantID,
	sessionID sessionwire.SessionID,
	kind ObjectKind,
) ([]sessionwire.ObjectReference, error) {
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
	prefix := objectKindPrefix(scope, kind)
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
		ref := objectIDFor(parsed.kind, parsed.generation, hex.EncodeToString(parsed.digest[:]))
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
func (s *Store) deleteObject(
	ctx context.Context,
	tenantID sessionwire.TenantID,
	sessionID sessionwire.SessionID,
	expectedKind ObjectKind,
	reference sessionwire.ObjectReference,
) error {
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

// parsePhysicalObjectKey recovers the identity of a listed key. The suffix is
// split by splitObjectKeySuffix, the inverse of the layout objectKey writes, and
// the recovered components must then satisfy the same canonical grammar as a
// caller-supplied ObjectID, so a key the store cannot have written is refused
// instead of resolved.
func parsePhysicalObjectKey(prefix string, kind ObjectKind, key string) (parsedObject, error) {
	if prefix == "" || !strings.HasPrefix(key, prefix) {
		return parsedObject{}, objectErr(ObjectErrorIntegrity, "list_key", nil)
	}
	digest, generation, ok := splitObjectKeySuffix(strings.TrimPrefix(key, prefix))
	if !ok {
		return parsedObject{}, objectErr(ObjectErrorIntegrity, "list_key", nil)
	}
	parsed, err := parseObjectReference(objectIDFor(kind, generation, digest))
	if err != nil {
		return parsedObject{}, objectErr(ObjectErrorIntegrity, "list_key", err)
	}
	return parsed, nil
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

// exactVerifier wraps a byte source and lets exactly the promised content
// through: the stream must end at expected bytes with a SHA-256 equal to digest,
// and verified is set only when that terminal EOF is observed. Any other outcome
// latches failure, which every later Read repeats, so a failed stream can never
// recover into a success.
//
// It is single-consumer and NOT safe for concurrent use: none of its fields are
// guarded by a mutex. The only concurrent user is objectReader, whose reading
// flag serializes calls into it.
//
// EOF policy is deliberately strict and identity-based: only a bare io.EOF ends
// the stream. A source returning an error that merely wraps io.EOF is treated as
// a read failure even on byte-perfect content, because a wrapped EOF means the
// source is reporting something in addition to end-of-stream and this type
// fails closed on anything it does not exactly understand. Every EOF comparison
// in this file is == for that reason; do not relax one to errors.Is without
// relaxing the contract on purpose.
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

func newExactVerifierWithReadCode(
	ctx context.Context,
	source io.Reader,
	size uint64,
	digest [32]byte,
	readCode ObjectErrorCode,
) *exactVerifier {
	return &exactVerifier{
		ctx:      ctx,
		source:   source,
		expected: size,
		digest:   digest,
		hash:     sha256.New(),
		readCode: readCode,
	}
}

// Read passes through at most the bytes still promised, hashing what it sees.
// Once the promised length is reached it stops handing the source a real buffer
// and instead probes for the terminal EOF, so a source with more to give is
// detected as oversized rather than silently truncated. A source reporting a
// negative count, or more bytes than the slice it was handed, is rejected
// outright: those counts cannot be hashed and would corrupt the accounting.
func (v *exactVerifier) Read(p []byte) (int, error) {
	if v.verified {
		return 0, io.EOF
	}
	if v.failure != nil {
		return 0, v.failure
	}
	// Refuse a new source read once the operation context is done. The
	// post-read check cannot bound a source that never returns from Read, so
	// this window is real and is covered by
	// TestExactVerifierCancellationBoundsNextRead.
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

// finishRead classifies the outcome of one source read. Cancellation wins over
// any other classification, a wrapped EOF is a read failure, and a bare EOF
// verifies only when both the length and the digest match. A source that
// returns neither bytes nor an error is refused as no-progress rather than
// spun on.
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
		if !bytes.Equal(v.hash.Sum(nil), v.digest[:]) {
			v.failure = objectErr(ObjectErrorIntegrity, "stream", nil)
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

// objectReader is the caller-visible object stream. It owns one exactVerifier,
// the provider reader beneath it, and the admission release, and it guarantees
// each is finished exactly once no matter which of three events terminates the
// stream first: the verifier failing or reaching terminal EOF, the caller
// calling Close, or the operation context being canceled (Store shutdown or the
// caller's own context).
//
// Concurrency contract. mu guards every field below it: cond, reading, closing,
// done, primary, readErr, terminal, closeErr, and stopCancel. reading is true
// only while a single goroutine is inside verifier.Read, which is what keeps the
// unsynchronized exactVerifier single-consumer; a second Read waits for it.
// Only the goroutine that flips closing from false to true owns termination and
// calls completeTermination, so the provider Close and release run exactly once.
// done means terminal and closeErr are final; every later call returns them
// unchanged, so Close is idempotent with a stable error.
//
// Read may return n > 0 together with a terminal error. Those bytes are
// unverified and must be discarded: integrity is established only by reading
// through terminal EOF, which is exactly what io.ReadAll and io.Copy do.
//
// Close reports nil if and only if terminal EOF was observed and the provider
// closed cleanly; abandoning a stream early is an integrity error, not a
// convenience.
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

// Read serializes callers, performs one verifier read, and on any error becomes
// the terminating goroutine if no one else already is. A caller that arrives
// while termination is in flight waits for it and observes the same terminal
// error.
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

// Close terminates the stream if it is not already terminated and returns the
// latched result: nil after a verified terminal EOF, otherwise the terminal
// error, including ObjectErrorIntegrity for a stream the caller abandoned.
// Repeat calls return the same value.
func (r *objectReader) Close() error {
	r.beginTermination(objectErr(ObjectErrorIntegrity, "incomplete", nil))
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closeErr
}

// beginTermination starts termination with cause, or waits for the termination
// already in flight. It is the single entry point for the two asynchronous
// terminators, Close and the context AfterFunc, and it returns only once the
// stream is done.
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

// completeTermination runs exactly once, on the goroutine that won the
// false-to-true transition of closing. It stops the cancellation hook, closes
// the provider reader — which is what bounds a Read blocked inside the provider,
// hence the storage.BlobReaderLifecycle requirement at Open — waits for any
// in-flight read to land so its error can be joined, publishes the terminal
// result, and releases admission last so Store.Close cannot outrun it.
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
	// == and not errors.Is, matching exactVerifier's identity-based EOF policy.
	// Only a bare io.EOF means the stream was verified through its terminal
	// EOF. A provider error that merely wraps io.EOF reaches here as an
	// *ObjectError whose cause chain contains io.EOF, and errors.Is would
	// therefore report Close success on content this store never verified.
	if r.primary == io.EOF {
		r.closeErr = joinErrors(r.readErr, wrappedClose)
	} else {
		r.closeErr = r.terminal
	}
	r.done = true
	cond.Broadcast()
	r.mu.Unlock()
	r.release()
}

// waitTerminal blocks until the terminal result is published and returns it.
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

// joinErrors differs from errors.Join in exactly one way, and that difference is
// its entire reason to exist: a single non-nil error is returned as itself
// rather than boxed in a join. Callers and tests compare the terminal error's
// identity and format, so boxing a lone cause would change both. Do not
// "simplify" this to errors.Join.
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
