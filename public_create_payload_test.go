package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

type createPayloadBlobFault struct {
	lifecycleBlobs
	fail, commit bool
}

func (f *createPayloadBlobFault) Put(ctx context.Context, key string, body io.Reader) error {
	if !f.fail {
		return f.Blobs.Put(ctx, key, body)
	}
	f.fail = false
	if f.commit {
		if err := f.Blobs.Put(ctx, key, body); err != nil {
			return err
		}
	}
	return errors.New("payload write failure")
}

type createPayloadIndexFault struct {
	storage.KV
	fail, commit bool
	failedKey    string
}

func (f *createPayloadIndexFault) Put(ctx context.Context, key string, rev uint64, v []byte) (uint64, error) {
	if !f.fail || !strings.Contains(key, "/object-metadata/v1/") {
		return f.KV.Put(ctx, key, rev, v)
	}
	f.fail = false
	f.failedKey = key
	if f.commit {
		if _, err := f.KV.Put(ctx, key, rev, v); err != nil {
			return 0, err
		}
	}
	return 0, errors.New("payload index write failure")
}
func (f *createPayloadIndexFault) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	if key == f.failedKey {
		return nil, 0, errors.New("ambiguous index read unavailable")
	}
	return f.KV.Get(ctx, key)
}

func TestPublicCreatePayloadFailuresRequireResupplyAndNeverACK(t *testing.T) {
	for _, stage := range []string{"blob", "index"} {
		for _, commit := range []bool{false, true} {
			t.Run(stage+map[bool]string{false: " before", true: " after"}[commit], func(t *testing.T) {
				b := memstore.New()
				blob := &createPayloadBlobFault{lifecycleBlobs: lifecycleBlobs{b.Blobs}}
				index := &createPayloadIndexFault{KV: b.KV}
				b.Blobs = blob
				b.KV = index
				s := openStore(t, b)
				r := publicCreateRequest()
				if _, err := s.PreparePublicCreate(t.Context(), r); err != nil {
					t.Fatal(err)
				}
				if stage == "blob" {
					blob.fail, blob.commit = true, commit
				} else {
					index.fail, index.commit = true, commit
				}
				m, err := s.PutCommandPayload(t.Context(), PutCommandPayloadRequest{TenantID: r.Identity.TenantID, SessionID: r.Identity.SessionID, Body: bytes.NewReader(inboxPayload), SizeBytes: uint64(len(inboxPayload)), SHA256: sha256.Sum256(inboxPayload)})
				if err == nil || m.Reference.ObjectID != "" {
					t.Fatalf("failed upload returned metadata: %+v %v", m, err)
				}
				if _, err := s.GetDispositionCommand(t.Context(), GetDispositionCommandRequest{TenantID: r.Identity.TenantID, SessionID: r.Identity.SessionID, CommandID: r.Identity.CommandID}); err == nil {
					t.Fatal("payload write admitted command")
				}
				if err := s.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
				index.failedKey = ""
				s = openStore(t, b)
				if _, err := s.PreparePublicCreate(t.Context(), r); err != nil {
					t.Fatal(err)
				}
				m = uploadCommandPayload(t, s, inboxPayload)
				if _, _, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, PayloadObject: &m}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestPublicCreateIndexedPayloadRequiredAndGenericCannotAdoptMarkedWinner(t *testing.T) {
	b := memstore.New()
	s := openStore(t, b)
	r := publicCreateRequest()
	if _, err := s.PreparePublicCreate(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	fake := objectMetadataFor(ObjectKindCommandPayload, [16]byte{1}, uint64(len(inboxPayload)), sha256.Sum256(inboxPayload), "")
	if _, _, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, PayloadObject: &fake}); err == nil {
		t.Fatal("unindexed payload acknowledged")
	}
	if _, _, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, Payload: inboxPayload}); err != nil {
		t.Fatal(err)
	}
	g := dispositionRequest()
	g.CommandID = r.Identity.CommandID
	if _, _, err := s.AdmitDispositionCommand(t.Context(), g); err == nil {
		t.Fatal("generic adopted marked winner")
	}
	g.CommandID = "different command same opaque kind"
	e, _, err := s.AdmitDispositionCommand(t.Context(), g)
	if err != nil {
		t.Fatal(err)
	}
	if e.Record.Descriptor.PublicCreate {
		t.Fatal("opaque kind guessed create")
	}
	e.Record.Descriptor.PublicCreate = true
	v, _, err := encodeDispositionInboxRecord(e.Record)
	if err != nil {
		t.Fatal(err)
	}
	scope, _ := s.deriveSessionScope(g.TenantID, g.SessionID)
	if _, err := b.OrderedIndex.Update(t.Context(), dispositionInboxID(scope, g.CommandID), e.Revision, v, storage.Rank{}, dispositionInboxDue(e.Record)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AdmitDispositionCommand(t.Context(), g); err == nil {
		t.Fatal("generic adopted marked second command")
	}
	for name, write := range legacyBindingWriters(t) {
		t.Run(name, func(t *testing.T) {
			if err := write(s); err == nil {
				t.Fatal("legacy mutation entered public create session")
			}
		})
	}
	if _, err := s.PutObject(t.Context(), PutObjectRequest{TenantID: r.Identity.TenantID, SessionID: r.Identity.SessionID, Kind: ObjectKindCommandPayload, Body: bytes.NewReader(nil), SHA256: sha256.Sum256(nil)}); err == nil {
		t.Fatal("legacy object mutation entered public create session")
	}
}
