package sessionstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// GetObjectMetadataRequest resolves one logical reference in a caller-authorized
// tenant/session scope and an explicit semantic kind.
type GetObjectMetadataRequest struct {
	TenantID     sessionwire.TenantID
	SessionID    sessionwire.SessionID
	ExpectedKind ObjectKind
	Reference    sessionwire.ObjectReference
}

// GetObjectMetadata returns immutable metadata recorded after PutObject verified
// the persisted blob. It does not verify current body presence or integrity and
// grants no authorization to consume it. GetObject still verifies through EOF.
//
// Lookup performs at most two scope-witness reads and one exact metadata read;
// it never enumerates or reads blob bodies. Missing metadata is reported as
// ObjectErrorMetadataUnavailable, not proof that bytes are absent. Objects
// written before the index was introduced remain readable through GetObject
// with explicit metadata. A stale index may survive blob deletion, in which
// case GetObject preserves the provider's storage.BlobNotFoundError as a cause.
func (s *Store) GetObjectMetadata(ctx context.Context, req GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error) {
	if !req.ExpectedKind.valid() {
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorInvalid, "expected_kind", nil)
	}
	parsed, err := parseObjectReference(req.Reference)
	if err != nil {
		return sessionwire.ObjectMetadata{}, err
	}
	if parsed.kind != req.ExpectedKind {
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorInvalid, "expected_kind", nil)
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
	if err := opCtx.Err(); err != nil {
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorCanceled, "metadata", err)
	}
	if err := s.verifySessionScope(opCtx, scope); err != nil {
		return sessionwire.ObjectMetadata{}, err
	}
	key := objectMetadataKey(scope, parsed)
	data, _, err := s.backend.KV.Get(opCtx, key)
	if err != nil {
		if isKeyNotFound(err, key) {
			return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorMetadataUnavailable, "metadata", err)
		}
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorBackend, "metadata_get", err)
	}
	return decodeObjectMetadataRecord(data, req)
}

// The immutable index is deliberately separate from blob storage. No read here
// opens a blob or lists keys. Administrative/external blob deletion can leave a
// stale index; absence can mean a pre-index object, not absent bytes. There is no
// automatic backfill. Capture authorization remains the referencing journal's
// owner's responsibility, not this index's.
func objectMetadataKey(scope sessionScope, object parsedObject) string {
	return scope.SessionNamespace + "/object-metadata/v1/" + string(object.kind) + "/" +
		objectKeySuffix(hex.EncodeToString(object.digest[:]), object.generation)
}

// persistObjectMetadata runs only after persisted-blob verification. Every error
// from the create may be a committed write, so resolve it with one exact read.
// Only the exact canonical winner licenses success. Never delete the blob or
// overwrite the winning row to repair a conflict or uncertain provider outcome.
func (s *Store) persistObjectMetadata(ctx context.Context, scope sessionScope, req PutObjectRequest, metadata sessionwire.ObjectMetadata, parsed parsedObject) error {
	if err := ctx.Err(); err != nil {
		return objectErr(ObjectErrorCanceled, "metadata_commit", err)
	}
	key := objectMetadataKey(scope, parsed)
	want := encodeObjectMetadataRecord(req.TenantID, req.SessionID, metadata)
	if _, err := s.backend.KV.Put(ctx, key, 0, want); err != nil {
		if canceled := ctx.Err(); canceled != nil {
			return objectErr(ObjectErrorBackend, "metadata_commit", errors.Join(err, canceled))
		}
		got, _, readErr := s.backend.KV.Get(ctx, key)
		if readErr != nil {
			return objectErr(ObjectErrorBackend, "metadata_commit", errors.Join(err, readErr))
		}
		if !bytes.Equal(got, want) {
			return objectErr(ObjectErrorConflict, "metadata_commit", err)
		}
	}
	return nil
}

// LROM v1: magic, version, uint64 size, then five uint16-length strings:
// tenant, session, ObjectID, Digest, MediaType. Integers are big-endian. The
// current PutObject result always has zero CreatedAt; v1 preserves that value.
// All strings are at most 256 bytes, matching existing ID and media validators.
// KV.Get owns its allocation before returning; this ceiling bounds decoding and
// accepted records, not a hostile provider's allocation inside Get.
const (
	objectMetadataStringBytes    = 256
	maxObjectMetadataRecordBytes = 5 + 8 + 5*(2+objectMetadataStringBytes)
)

func encodeObjectMetadataRecord(tenant sessionwire.TenantID, session sessionwire.SessionID, metadata sessionwire.ObjectMetadata) []byte {
	data := make([]byte, 0, maxObjectMetadataRecordBytes)
	data = append(data, 'L', 'R', 'O', 'M', 1)
	data = binary.BigEndian.AppendUint64(data, metadata.SizeBytes)
	for _, value := range []string{string(tenant), string(session), metadata.Reference.ObjectID, metadata.Digest, metadata.MediaType} {
		data = binary.BigEndian.AppendUint16(data, checkedUint16Length(len(value)))
		data = append(data, value...)
	}
	return data
}

func decodeObjectMetadataRecord(data []byte, req GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error) {
	bad := func(cause error) (sessionwire.ObjectMetadata, error) {
		return sessionwire.ObjectMetadata{}, objectErr(ObjectErrorIntegrity, "metadata", cause)
	}
	if len(data) < 13 || len(data) > maxObjectMetadataRecordBytes || string(data[:5]) != "LROM\x01" {
		return bad(nil)
	}
	size := binary.BigEndian.Uint64(data[5:13])
	rest := data[13:]
	var fields [5]string
	for i := range fields {
		if len(rest) < 2 {
			return bad(nil)
		}
		n := int(binary.BigEndian.Uint16(rest[:2]))
		rest = rest[2:]
		if n > objectMetadataStringBytes || n > len(rest) {
			return bad(nil)
		}
		fields[i] = string(rest[:n])
		rest = rest[n:]
	}
	if len(rest) != 0 || fields[0] != string(req.TenantID) || fields[1] != string(req.SessionID) || fields[2] != req.Reference.ObjectID {
		return bad(nil)
	}
	metadata := sessionwire.ObjectMetadata{Reference: sessionwire.ObjectReference{ObjectID: fields[2]}, SizeBytes: size, Digest: fields[3], MediaType: fields[4]}
	parsed, err := parseObjectMetadata(metadata)
	if err != nil {
		return bad(err)
	}
	if parsed.kind != req.ExpectedKind {
		return bad(nil)
	}
	return metadata, nil
}
