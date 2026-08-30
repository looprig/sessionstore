package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/looprig/storage/memstore"
)

var _ func(*Store, context.Context, GetObjectRequest) (io.ReadCloser, error) = (*Store).GetObject

func TestParseObjectReferenceRejectsZeroDigestBeforeAdmission(t *testing.T) {
	zero := [32]byte{}
	metadata := objectMetadata(ObjectKindArtifact, [16]byte{1}, 1, zero, "")
	backend, calls := instrumentComposite(memstore.New())
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	admissions := 0
	store.lifecycleLocked = func(op lifecycleOperation) {
		if op == lifecycleAdmit {
			admissions++
		}
	}
	before := calls.snapshot()
	if _, err := store.GetObject(context.Background(), GetObjectRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: ObjectKindArtifact, Metadata: metadata}); err == nil {
		t.Fatal("Get accepted zero digest")
	}
	if err := store.deleteObject(context.Background(), "tenant", "session", ObjectKindArtifact, metadata.Reference); err == nil {
		t.Fatal("delete accepted zero digest")
	}
	if admissions != 0 || calls.snapshot() != before {
		t.Fatalf("admissions=%d provider before=%+v after=%+v", admissions, before, calls.snapshot())
	}
}

func TestAdministrativeListRejectsZeroDigestReference(t *testing.T) {
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
	admin.listFn = func(prefix string) []string {
		return []string{prefix + strings.Repeat("0", 64) + "/04000000000000000000000000"}
	}
	refs, err := store.listObjectReferences(context.Background(), "tenant", "session", ObjectKindArtifact)
	if err == nil || len(refs) != 0 {
		t.Fatalf("refs=%v err=%v", refs, err)
	}
}

func TestParseObjectMetadataRejectsInvalidMediaTypes(t *testing.T) {
	digest := sha256.Sum256([]byte("x"))
	valid := objectMetadata(ObjectKindArtifact, [16]byte{1}, 1, digest, "")
	for _, mediaType := range []string{string([]byte{0xff}), "not a media type", strings.Repeat("x", 257)} {
		candidate := valid
		candidate.MediaType = mediaType
		if _, err := parseObjectMetadata(candidate); err == nil {
			t.Fatalf("accepted media type %q", mediaType)
		}
	}
}

func TestAdministrativeBackendErrorsAreTypedAndRedacted(t *testing.T) {
	for _, operation := range []string{"list", "delete"} {
		t.Run(operation, func(t *testing.T) {
			base := memstore.New()
			cause := errors.New("private backend secret")
			admin := &adminRecordingBlobs{Blobs: base.Blobs, listErr: cause, deleteErr: cause}
			base.Blobs = admin
			store, err := Open(context.Background(), base)
			if err != nil {
				t.Fatal(err)
			}
			body := []byte("x")
			digest := sha256.Sum256(body)
			admin.listErr, admin.deleteErr = nil, nil
			metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader(body)})
			if err != nil {
				t.Fatal(err)
			}
			admin.listErr, admin.deleteErr = cause, cause
			if operation == "list" {
				_, err = store.listObjectReferences(context.Background(), "tenant", "session", ObjectKindArtifact)
			} else {
				err = store.deleteObject(context.Background(), "tenant", "session", ObjectKindArtifact, metadata.Reference)
			}
			var objectErr *ObjectError
			if !errors.As(err, &objectErr) || objectErr.Code != ObjectErrorBackend || !errors.Is(err, cause) {
				t.Fatalf("error=%T %v", err, err)
			}
			if strings.Contains(err.Error(), "private backend secret") {
				t.Fatalf("error leaked cause: %v", err)
			}
		})
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
