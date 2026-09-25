package sessionstore

import (
	"context"
	"maps"
	"reflect"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

func testPrincipal() *sessionwire.Principal {
	return &sessionwire.Principal{Tenant: catalogTenant, Subject: "user/alex", Kind: sessionwire.PrincipalKindActor}
}

func TestDispositionAttributionShape(t *testing.T) {
	t.Parallel()
	principal := func(r *AdmitDispositionCommandRequest) { r.Principal = testPrincipal() }
	metadata := func(r *AdmitDispositionCommandRequest) { r.Metadata = testMetadata() }
	both := func(r *AdmitDispositionCommandRequest) { principal(r); metadata(r) }
	for _, tc := range []struct {
		name, kind string
		set        func(*AdmitDispositionCommandRequest)
		field      string
	}{
		{"principal create", "create", principal, ""},
		{"principal input", "input", principal, ""},
		{"principal interrupt", "interrupt", principal, ""},
		{"principal restore", "restore", principal, ""},
		{"principal gate_response", "gate_response", principal, ""},
		{"principal opaque", "Kind/A:B", principal, ""},
		{"metadata create", "create", metadata, ""},
		{"metadata input", "input", metadata, ""},
		{"both create", "create", both, ""},
		{"both input", "input", both, ""},
		{"metadata interrupt", "interrupt", metadata, "metadata"},
		{"metadata restore", "restore", metadata, "metadata"},
		{"metadata gate_response", "gate_response", metadata, "metadata"},
		{"metadata opaque", "Kind/A:B", metadata, "metadata"},
		{"both interrupt", "interrupt", both, "metadata"},
		{"invalid principal kind", "input", func(r *AdmitDispositionCommandRequest) { p := testPrincipal(); p.Kind = "admin"; r.Principal = p }, "principal"},
		{"empty principal subject", "interrupt", func(r *AdmitDispositionCommandRequest) { p := testPrincipal(); p.Subject = ""; r.Principal = p }, "principal"},
		{"empty principal tenant", "restore", func(r *AdmitDispositionCommandRequest) { p := testPrincipal(); p.Tenant = ""; r.Principal = p }, "principal"},
		{"invalid metadata key", "input", func(r *AdmitDispositionCommandRequest) { r.Metadata = sessionwire.MessageMetadata{"Space": "family"} }, "metadata"},
		{"reserved metadata key", "create", func(r *AdmitDispositionCommandRequest) { r.Metadata = sessionwire.MessageMetadata{"looprig_x": "v"} }, "metadata"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := openTestStore(t)
			createDispositionCatalog(t, s)
			req := dispositionRequest()
			req.Kind = CommandKind(tc.kind)
			tc.set(&req)
			entry, created, err := s.AdmitDispositionCommand(ctx, req)
			get := GetDispositionCommandRequest{TenantID: req.TenantID, SessionID: req.SessionID, CommandID: req.CommandID}
			if tc.field != "" {
				if got := assertInboxCode(t, err, InboxErrorInvalid); got.Field != tc.field {
					t.Fatalf("field = %q, want %q", got.Field, tc.field)
				}
				if created {
					t.Fatal("refused command reported created")
				}
				_, err := s.GetDispositionCommand(ctx, get)
				assertInboxCode(t, err, InboxErrorNotFound)
				return
			}
			if err != nil || !created {
				t.Fatalf("admit: %v created=%v", err, created)
			}
			d := entry.Record.Descriptor
			if !reflect.DeepEqual(d.Principal, req.Principal) || !maps.Equal(d.Metadata, req.Metadata) {
				t.Fatalf("descriptor attribution = %+v %v", d.Principal, d.Metadata)
			}
			read, err := s.GetDispositionCommand(ctx, get)
			if err != nil || !reflect.DeepEqual(read, entry) {
				t.Fatalf("read back: %+v %v; want %+v", read, err, entry)
			}
		})
	}
}

func TestDispositionAttributionIsCopiedAndNormalized(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	createDispositionCatalog(t, s)
	req := dispositionRequest()
	req.Principal, req.Metadata = testPrincipal(), testMetadata()
	entry, _, err := s.AdmitDispositionCommand(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	req.Principal.Subject = "user/mallory"
	req.Metadata["space"] = "work"
	if entry.Record.Descriptor.Principal == nil || entry.Record.Descriptor.Principal.Subject != "user/alex" || entry.Record.Descriptor.Metadata["space"] != "family" {
		t.Fatalf("returned descriptor aliases request: %+v", entry.Record.Descriptor)
	}
	empty := dispositionRequest()
	empty.CommandID, empty.Kind, empty.Metadata = "public/command:empty", "interrupt", sessionwire.MessageMetadata{}
	got, _, err := s.AdmitDispositionCommand(ctx, empty)
	if err != nil {
		t.Fatalf("empty bag on non-message kind should be absent: %v", err)
	}
	if got.Record.Descriptor.Metadata != nil {
		t.Fatalf("empty metadata stored as %#v", got.Record.Descriptor.Metadata)
	}
}

func testMetadata() sessionwire.MessageMetadata {
	return sessionwire.MessageMetadata{"space": "family", "client": "oxy-ios"}
}

func TestDispositionAttributionIsDeclared(t *testing.T) {
	t.Parallel()
	if testPrincipal().Kind != sessionwire.PrincipalKindActor || len(testMetadata()) != 2 {
		t.Fatal("invalid attribution fixture")
	}
	want := map[string]reflect.Type{
		"Principal": reflect.TypeOf((*sessionwire.Principal)(nil)),
		"Metadata":  reflect.TypeOf(sessionwire.MessageMetadata(nil)),
	}
	for _, typ := range []reflect.Type{reflect.TypeOf(DispositionCommandDescriptor{}), reflect.TypeOf(AdmitDispositionCommandRequest{}), reflect.TypeOf(AdmitPublicCreateRequest{})} {
		for name, wantType := range want {
			field, ok := typ.FieldByName(name)
			if !ok || field.Type != wantType {
				t.Errorf("%s.%s: present %v type %v, want %v", typ.Name(), name, ok, field.Type, wantType)
			}
		}
	}
}
