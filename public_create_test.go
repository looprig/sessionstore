package sessionstore

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage/memstore"
)

// Preparation and acceptance must be distinct entry points: an oversized
// create needs its catalog before the verified payload uploader can run.
func TestPublicCreateHasSeparatePreparationAndAcceptance(t *testing.T) {
	typ := reflect.TypeOf((*Store)(nil))
	for _, name := range []string{"PreparePublicCreate", "AdmitPublicCreate"} {
		if _, ok := typ.MethodByName(name); !ok {
			t.Errorf("missing resumable public create entry point %s", name)
		}
	}
}

func publicCreateRequest() PreparePublicCreateRequest {
	d := sha256.Sum256(inboxPayload)
	return PreparePublicCreateRequest{
		Identity:                 PublicCreateIdentity{TenantID: catalogTenant, SessionID: catalogSession, CommandID: "Create/A:B", Target: HostTargetKey{AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1", Placement: sessionwire.HostPlacementPooled}, Binding: testSessionBinding(), Kind: inboxKind, PayloadDigest: hex.EncodeToString(d[:]), PayloadSize: uint64(len(inboxPayload))},
		ProposedRuntimeCommandID: inboxRuntime, AcceptedAt: inboxAcceptedAt, ApplyDeadline: inboxDeadline,
	}
}

func TestPublicCreatePreparationIsNotAcceptanceAndRestartPreservesWinners(t *testing.T) {
	b := memstore.New()
	s := openStore(t, b)
	r := publicCreateRequest()
	p, err := s.PreparePublicCreate(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetDispositionCommand(t.Context(), GetDispositionCommandRequest{TenantID: r.Identity.TenantID, SessionID: r.Identity.SessionID, CommandID: r.Identity.CommandID}); err == nil {
		t.Fatal("preparation admitted command")
	}
	if err := s.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, b)
	r.ProposedRuntimeCommandID = inboxRetryRuntime
	r.AcceptedAt = r.AcceptedAt.Add(time.Second)
	r.ApplyDeadline = r.ApplyDeadline.Add(time.Second)
	again, err := s.PreparePublicCreate(t.Context(), r)
	if err != nil || !reflect.DeepEqual(p, again) {
		t.Fatalf("preparation reminted winner: %+v %v", again, err)
	}
	e, created, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, Payload: inboxPayload})
	if err != nil || !created {
		t.Fatalf("admit: %+v %v %v", e, created, err)
	}
	if !e.Record.Descriptor.PublicCreate || e.Record.Descriptor.RuntimeCommandID != inboxRuntime || e.Record.AcceptedAt != inboxAcceptedAt || e.Record.ApplyDeadline != inboxDeadline {
		t.Fatalf("lost winner: %+v", e)
	}
	duplicate, created, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, Payload: inboxPayload})
	if err != nil || created || !reflect.DeepEqual(duplicate, e) {
		t.Fatalf("duplicate: %+v %v %v", duplicate, created, err)
	}
}

func TestPublicCreateConflictsAndGenericFences(t *testing.T) {
	s := openTestStore(t)
	r := publicCreateRequest()
	if _, err := s.PreparePublicCreate(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*PublicCreateIdentity){
		"session":         func(i *PublicCreateIdentity) { i.SessionID = "other" },
		"command":         func(i *PublicCreateIdentity) { i.CommandID = "other" },
		"agent":           func(i *PublicCreateIdentity) { i.Target.AgentID = "other" },
		"runtime":         func(i *PublicCreateIdentity) { i.Target.RuntimeCompatibilityID = "other" },
		"placement":       func(i *PublicCreateIdentity) { i.Target.Placement = sessionwire.HostPlacementDedicated },
		"binding":         func(i *PublicCreateIdentity) { i.Binding.BindingVersion = "other" },
		"binding profile": func(i *PublicCreateIdentity) { i.Binding.StorageBindingID = "other" },
		"runtime session": func(i *PublicCreateIdentity) { i.Binding.RuntimeSessionID = "other" },
		"protocol":        func(i *PublicCreateIdentity) { i.Binding.ProtocolMode = ProtocolModeLegacy },
		"digest":          func(i *PublicCreateIdentity) { d := sha256.Sum256(nil); i.PayloadDigest = hex.EncodeToString(d[:]) },
		"size":            func(i *PublicCreateIdentity) { i.PayloadSize++ },
	} {
		t.Run(name, func(t *testing.T) {
			bad := r
			mutate(&bad.Identity)
			if _, err := s.PreparePublicCreate(t.Context(), bad); err == nil {
				t.Fatal("conflict accepted")
			}
			if _, _, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: bad.Identity, Payload: inboxPayload}); err == nil {
				t.Fatal("losing reservation acknowledged")
			}
		})
	}
	g := dispositionRequest()
	g.CommandID = r.Identity.CommandID
	if _, _, err := s.AdmitDispositionCommand(t.Context(), g); err == nil {
		t.Fatal("generic admission used create identity")
	}
	c := testCreateRequest()
	c.Binding = r.Identity.Binding
	if _, _, err := s.CreateCatalogEntry(t.Context(), c); err == nil {
		t.Fatal("generic catalog adopted public create")
	}
	for _, bound := range []bool{false, true} {
		other := openTestStore(t)
		c := testCreateRequest()
		if bound {
			c.Binding = r.Identity.Binding
		}
		if _, _, err := other.CreateCatalogEntry(t.Context(), c); err != nil {
			t.Fatal(err)
		}
		if _, err := other.PreparePublicCreate(t.Context(), r); err == nil {
			t.Fatal("public create adopted generic/legacy catalog")
		}
	}
}
