package sessionstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func TestPublicCreateInboxWinnerMustAgreeExactly(t *testing.T) {
	for _, mismatch := range []string{"marker", "runtime", "accepted", "deadline", "kind", "payload"} {
		t.Run(mismatch, func(t *testing.T) {
			b := memstore.New()
			s := openStore(t, b)
			r := publicCreateRequest()
			if _, err := s.PreparePublicCreate(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			d, err := dispositionDescriptor(AdmitDispositionCommandRequest{TenantID: r.Identity.TenantID, SessionID: r.Identity.SessionID, CommandID: r.Identity.CommandID, Binding: r.Identity.Binding, Kind: r.Identity.Kind, ProposedRuntimeCommandID: r.ProposedRuntimeCommandID, Payload: inboxPayload})
			if err != nil {
				t.Fatal(err)
			}
			d.PublicCreate = true
			record := DispositionInboxRecord{Descriptor: d, AcceptedAt: r.AcceptedAt, ApplyDeadline: r.ApplyDeadline, State: InboxStatePending}
			switch mismatch {
			case "marker":
				record.Descriptor.PublicCreate = false
			case "runtime":
				record.Descriptor.RuntimeCommandID = inboxRetryRuntime
			case "accepted":
				record.AcceptedAt = record.AcceptedAt.Add(time.Second)
			case "deadline":
				record.ApplyDeadline = record.ApplyDeadline.Add(time.Second)
			case "kind":
				record.Descriptor.Kind = "different"
			case "payload":
				record.Descriptor.Payload = []byte("other")
				digest := sha256.Sum256(record.Descriptor.Payload)
				record.Descriptor.PayloadDigest = hex.EncodeToString(digest[:])
				record.Descriptor.PayloadSize = uint64(len(record.Descriptor.Payload))
			}
			v, _, err := encodeDispositionInboxRecord(record)
			if err != nil {
				t.Fatal(err)
			}
			scope, _ := s.deriveSessionScope(r.Identity.TenantID, r.Identity.SessionID)
			if _, _, err := b.OrderedIndex.Create(t.Context(), dispositionInboxID(scope, r.Identity.CommandID), scope.SessionNamespace, v, storage.Rank{}, dispositionInboxDue(record)); err != nil {
				t.Fatal(err)
			}
			if e, created, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, Payload: inboxPayload}); err == nil || created || !reflect.DeepEqual(e, DispositionInboxEntry{}) {
				t.Fatalf("mismatched winner acknowledged: %+v %v %v", e, created, err)
			}
		})
	}
}

func TestPublicCreateDeletedCatalogCannotBeRecreatedOrAcknowledged(t *testing.T) {
	b := memstore.New()
	s := openStore(t, b)
	r := publicCreateRequest()
	p, err := s.PreparePublicCreate(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, Payload: inboxPayload}); err != nil {
		t.Fatal(err)
	}
	scope, _ := s.deriveSessionScope(r.Identity.TenantID, r.Identity.SessionID)
	if _, err := b.OrderedIndex.Delete(t.Context(), catalogID(scope, r.Identity.SessionID), p.Catalog.Revision); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, b)
	if _, err := s.PreparePublicCreate(t.Context(), r); err == nil {
		t.Fatal("deleted catalog recreated")
	}
	if _, _, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, Payload: inboxPayload}); err == nil {
		t.Fatal("deleted catalog acknowledged")
	}
	generic := testCreateRequest()
	generic.Binding = r.Identity.Binding
	if _, _, err := s.CreateCatalogEntry(t.Context(), generic); err == nil {
		t.Fatal("generic recreation adopted deleted catalog")
	}
}

func TestPublicCreateOpaqueIDsAndTenantGlobalScope(t *testing.T) {
	b := memstore.New()
	s := openStore(t, b)
	rng := rand.New(rand.NewPCG(451, 822))
	alphabet := []byte("AaZz09/:")
	for n := 0; n < 24; n++ {
		var id bytes.Buffer
		fmt.Fprintf(&id, "Random/%d:", n)
		for j := 0; j < 30; j++ {
			id.WriteByte(alphabet[rng.IntN(len(alphabet))])
		}
		for _, tenant := range []sessionwire.TenantID{"Tenant/A:B", "Tenant/B:A"} {
			r := publicCreateRequest()
			r.Identity.TenantID = tenant
			r.Identity.SessionID = sessionwire.SessionID(id.String())
			r.Identity.CommandID = sessionwire.CommandID(id.String())
			if _, err := s.PreparePublicCreate(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			e, _, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, Payload: inboxPayload})
			if err != nil || e.Record.Descriptor.TenantID != tenant {
				t.Fatalf("opaque tenant alias: %+v %v", e, err)
			}
		}
	}
}

func TestPublicCreateEmptyInlineAndObjectContentAreEquivalent(t *testing.T) {
	s := openTestStore(t)
	r := publicCreateRequest()
	d := sha256.Sum256(nil)
	r.Identity.PayloadSize = 0
	r.Identity.PayloadDigest = hex.EncodeToString(d[:])
	if _, err := s.PreparePublicCreate(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	e, _, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity})
	if err != nil {
		t.Fatal(err)
	}
	m := uploadCommandPayload(t, s, nil)
	again, created, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, PayloadObject: &m})
	if err != nil || created || !reflect.DeepEqual(e, again) {
		t.Fatal("empty content representation changed winner")
	}
}
