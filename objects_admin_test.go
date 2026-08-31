// The unexported administrative object operations: listObjectReferences and
// deleteObject.
package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage/memstore"
)

// TestAdministrativeListRejectsNoncanonicalPhysicalKeys pins the parser side of
// the key grammar: a listed key that the store could not have written must fail
// closed instead of resolving to some reference.
func TestAdministrativeListRejectsNoncanonicalPhysicalKeys(t *testing.T) {
	var generationBytes [16]byte
	for i := range generationBytes {
		generationBytes[i] = 0xfe
	}
	reference := objectMetadata(ObjectKindArtifact, generationBytes, 1, sha256.Sum256([]byte("x")), "").Reference
	parts := strings.Split(reference.ObjectID, ":")
	generation, digest := parts[2], parts[3]
	for name, suffix := range map[string]string{
		"uppercase digest":     strings.ToUpper(digest) + "/" + generation,
		"uppercase generation": digest + "/" + strings.ToUpper(generation),
		"swapped components":   generation + "/" + digest,
		"extra segment":        digest + "/" + generation + "/extra",
		"missing generation":   digest,
		// A key whose digest segment is longer than 64 hex characters must be
		// refused, not decoded into the fixed 32-byte array behind it.
		"overlong digest": strings.Repeat("ab", 100) + "/" + generation,
	} {
		t.Run(name, func(t *testing.T) {
			base := memstore.New()
			admin := &adminRecordingBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}}
			base.Blobs = admin
			store, err := Open(context.Background(), base)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close(context.Background())
			body := []byte("bind")
			bodyDigest := sha256.Sum256(body)
			if _, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindAttachment, SizeBytes: uint64(len(body)), SHA256: bodyDigest, Body: bytes.NewReader(body)}); err != nil {
				t.Fatal(err)
			}
			admin.listFn = func(prefix string) []string { return []string{prefix + suffix} }
			refs, err := store.listObjectReferences(context.Background(), "tenant", "session", ObjectKindArtifact)
			var objErr *ObjectError
			if refs != nil || !errors.As(err, &objErr) || objErr.Code != ObjectErrorIntegrity || objErr.Field != "list_key" {
				t.Fatalf("refs=%+v err=%T %v, want integrity/list_key", refs, err, err)
			}
		})
	}
}

func TestAdministrativeListRejectsZeroDigestReference(t *testing.T) {
	base := memstore.New()
	admin := &adminRecordingBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}}
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

func TestAdministrativeBackendErrorsAreTypedAndRedacted(t *testing.T) {
	for _, operation := range []string{"list", "delete"} {
		t.Run(operation, func(t *testing.T) {
			base := memstore.New()
			cause := errors.New("private backend secret")
			admin := &adminRecordingBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}, listErr: cause, deleteErr: cause}
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

func TestAdministrativeObjectListAndDeleteAreExactlyScoped(t *testing.T) {
	base := memstore.New()
	admin := &adminRecordingBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}}
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
	admin := &adminRecordingBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}}
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
	admin := &adminRecordingBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}}
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
	admin := &adminRecordingBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}}
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
		// Out of prefix AND canonically splittable, which is what reaches the
		// prefix guard at all. The obvious spelling of this case — a whole
		// foreign path — is refused one line earlier by the suffix split,
		// since it has five fields rather than two, so it would pass while
		// proving nothing about the guard it names.
		"out of prefix": func(string) []string {
			parts := strings.Split(valid.ObjectID, ":")
			return []string{parts[3] + "/" + parts[2]}
		},
		"out of prefix under a foreign path": func(string) []string {
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
			admin := &adminRecordingBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}}
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
	admin := &adminRecordingBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}}
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
