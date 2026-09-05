package sessionstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// Frozen pre-public-create descriptor, deliberately not an alias of the current
// type: an old binary must fail closed on the newly marked v2 record.
type prePublicCreateDescriptor struct {
	TenantID         sessionwire.TenantID        `json:"tenant_id"`
	SessionID        sessionwire.SessionID       `json:"session_id"`
	CommandID        sessionwire.CommandID       `json:"command_id"`
	Binding          SessionBinding              `json:"binding"`
	RuntimeCommandID RuntimeCommandID            `json:"runtime_command_id"`
	Kind             CommandKind                 `json:"kind"`
	PayloadDigest    string                      `json:"payload_digest"`
	PayloadSize      uint64                      `json:"payload_size"`
	Payload          []byte                      `json:"payload,omitempty"`
	PayloadObject    *sessionwire.ObjectMetadata `json:"payload_object,omitempty"`
}

func TestPublicCreateCodecsFailClosed(t *testing.T) {
	s := openTestStore(t)
	req := publicCreateRequest()
	p, err := s.PreparePublicCreate(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := encodePublicCreate(p.Reservation)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func([]byte) []byte{
		"version": func(v []byte) []byte {
			return bytes.Replace(v, []byte(`"record_version":1`), []byte(`"record_version":2`), 1)
		},
		"unknown": func(v []byte) []byte { return append([]byte(`{"unknown":1,`), v[1:]...) },
		"duplicate nested": func(v []byte) []byte {
			return bytes.Replace(v, []byte(`"tenant_id":`), []byte(`"tenant_id":"other","tenant_id":`), 1)
		},
		"trailing": func(v []byte) []byte { return append(v, '{') },
		"oversize": func(v []byte) []byte { return bytes.Repeat([]byte("x"), MaxCatalogRecordBytes+1) },
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodePublicCreate(mutate(bytes.Clone(v))); err == nil {
				t.Fatal("corrupt reservation accepted")
			}
		})
	}
	c, err := encodeCatalogRecord(p.Catalog.Record)
	if err != nil {
		t.Fatal(err)
	}
	fields := versionedRecordFields{Record: "record", Version: "record_version"}
	if _, err := decodeVersionedRecord[catalogWire](c, MaxCatalogRecordBytes, CatalogRecordVersion, fields, catalogRecordFailure); err == nil {
		t.Fatal("v1 decoder accepted v3")
	}
	if _, err := decodeVersionedRecord[catalogBindingWire](c, MaxCatalogRecordBytes, CatalogBindingRecordVersion, fields, catalogRecordFailure); err == nil {
		t.Fatal("v2 decoder accepted v3")
	}
	duplicate := bytes.Replace(c, []byte(`"command_id":`), []byte(`"command_id":"evil","command_id":`), 1)
	if _, err := decodeCatalogRecord(duplicate); err == nil {
		t.Fatal("duplicate immutable catalog identity accepted")
	}
	e, _, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: req.Identity, Payload: inboxPayload})
	if err != nil {
		t.Fatal(err)
	}
	inbox, _, err := encodeDispositionInboxRecord(e.Record)
	if err != nil {
		t.Fatal(err)
	}
	var old struct {
		RecordVersion uint8                     `json:"record_version"`
		Descriptor    prePublicCreateDescriptor `json:"descriptor"`
		AcceptedAt    time.Time                 `json:"accepted_at"`
		ApplyDeadline time.Time                 `json:"apply_deadline"`
		State         InboxState                `json:"state"`
	}
	if err := json.Unmarshal(inbox, &old); err != nil {
		t.Fatal(err)
	}
	oldCanonical, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(oldCanonical, inbox) {
		t.Fatal("old canonical decoder would accept marker")
	}
	// The exact same old shape still accepts an unmarked record after canonical
	// re-encoding, so the compatibility check is not vacuously rejecting all v2.
	unmarked := e.Record
	unmarked.Descriptor.PublicCreate = false
	legacyV2, _, err := encodeDispositionInboxRecord(unmarked)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(legacyV2, &old); err != nil {
		t.Fatal(err)
	}
	oldCanonical, err = json.Marshal(old)
	if err != nil || !bytes.Equal(oldCanonical, legacyV2) {
		t.Fatal("frozen old descriptor is not compatible with unmarked v2")
	}
	if _, err := decodeInboxRecord(inbox); err == nil {
		t.Fatal("legacy inbox accepted create")
	}
}

func TestPublicCreateInvalidInputsPerformNoIO(t *testing.T) {
	b, calls := instrumentComposite(memstore.New())
	b.OrderedIndex = publicCreateExactOnly{b.OrderedIndex}
	s := openStore(t, b)
	r := publicCreateRequest()
	for name, change := range map[string]func(*PreparePublicCreateRequest){
		"tenant":         func(r *PreparePublicCreateRequest) { r.Identity.TenantID = "" },
		"command":        func(r *PreparePublicCreateRequest) { r.Identity.CommandID = "" },
		"target":         func(r *PreparePublicCreateRequest) { r.Identity.Target.RuntimeCompatibilityID = "" },
		"placement":      func(r *PreparePublicCreateRequest) { r.Identity.Target.Placement = "invalid" },
		"binding":        func(r *PreparePublicCreateRequest) { r.Identity.Binding.RuntimeSessionID = "" },
		"digest":         func(r *PreparePublicCreateRequest) { r.Identity.PayloadDigest = "bad" },
		"empty identity": func(r *PreparePublicCreateRequest) { r.Identity.PayloadSize = 0 },
		"runtime":        func(r *PreparePublicCreateRequest) { r.ProposedRuntimeCommandID = "" },
		"time":           func(r *PreparePublicCreateRequest) { r.ApplyDeadline = time.Time{} },
		"workload":       func(r *PreparePublicCreateRequest) { r.InitialWorkload.Payload = []byte("bad") },
	} {
		t.Run(name, func(t *testing.T) {
			bad := r
			change(&bad)
			before := calls.snapshot()
			if _, err := s.PreparePublicCreate(t.Context(), bad); err == nil {
				t.Error("invalid preparation accepted")
			}
			if after := calls.snapshot(); after != before {
				t.Fatalf("invalid request did I/O: %+v -> %+v", before, after)
			}
		})
	}
	if _, err := s.PreparePublicCreate(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	before := calls.snapshot()
	if _, _, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, Payload: []byte("wrong")}); err == nil {
		t.Fatal("wrong payload accepted")
	}
	if calls.snapshot() != before {
		t.Fatal("bad content performed I/O")
	}
	before = calls.snapshot()
	if _, err := s.PreparePublicCreate(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	after := calls.snapshot()
	if after.Ordered-before.Ordered != 2 || after.KV-before.KV > 12 || after.Ledger != before.Ledger || after.Leaser != before.Leaser || after.Blobs != before.Blobs {
		t.Fatalf("unbounded prepare: %+v -> %+v", before, after)
	}
	before = after
	if _, _, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, Payload: inboxPayload}); err != nil {
		t.Fatal(err)
	}
	after = calls.snapshot()
	if after.Ordered-before.Ordered != 3 || after.KV-before.KV > 10 || after.Ledger != before.Ledger || after.Leaser != before.Leaser || after.Blobs != before.Blobs {
		t.Fatalf("unbounded admission: %+v -> %+v", before, after)
	}
}

type publicCreateExactOnly struct{ storage.OrderedIndex }

func (publicCreateExactOnly) ListOrdered(context.Context, string, string, uint64, int) (storage.OrderedPage, error) {
	return storage.OrderedPage{}, errors.New("unexpected ordered scan")
}
func (publicCreateExactOnly) ListRanked(context.Context, string, string, storage.RankedCursor, int) (storage.RankedPage, error) {
	return storage.RankedPage{}, errors.New("unexpected ranked scan")
}
func (publicCreateExactOnly) ListDue(context.Context, string, int64, storage.DueCursor, int) (storage.DuePage, error) {
	return storage.DuePage{}, errors.New("unexpected due scan")
}

func FuzzPublicCreateReservationCodec(f *testing.F) {
	req := publicCreateRequest()
	v, _, err := encodePublicCreate(PublicCreateReservation{Identity: req.Identity, RuntimeCommandID: req.ProposedRuntimeCommandID, AcceptedAt: req.AcceptedAt, ApplyDeadline: req.ApplyDeadline})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(v)
	f.Add([]byte(`{"record_version":1}`))
	f.Add([]byte(`null`))
	f.Fuzz(func(t *testing.T, v []byte) {
		r, err := decodePublicCreate(v)
		if err != nil {
			return
		}
		encoded, canonical, err := encodePublicCreate(r)
		if err != nil || !bytes.Equal(encoded, v) || !reflect.DeepEqual(canonical, r) {
			t.Fatal("reservation codec is not canonical")
		}
	})
}

func FuzzPublicCreateCatalogCodec(f *testing.F) {
	req := publicCreateRequest()
	r := PublicCreateReservation{Identity: req.Identity, RuntimeCommandID: req.ProposedRuntimeCommandID, AcceptedAt: req.AcceptedAt, ApplyDeadline: req.ApplyDeadline, InitialWorkload: testDesiredWorkload()}
	v, err := encodeCatalogRecord(publicCreateCatalogRecord(r))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(v)
	f.Add([]byte(`{"record_version":3}`))
	f.Fuzz(func(t *testing.T, v []byte) {
		r, err := decodeCatalogRecord(v)
		if err != nil || r.PublicCreate == nil {
			return
		}
		encoded, err := encodeCatalogRecord(r)
		if err != nil || !bytes.Equal(encoded, v) {
			t.Fatal("public catalog is not canonical")
		}
	})
}
