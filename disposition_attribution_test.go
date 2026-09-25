package sessionstore

import (
	"bytes"
	"context"
	"maps"
	"reflect"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

func TestDispositionInboxWireGoldenAttributed(t *testing.T) {
	t.Parallel()
	const identityHead = `"tenant_id":"Tenant/A:B","session_id":"Session/A:B","command_id":"Create/A:B","binding":{"storage_binding_id":"agent-pool/east","binding_version":"config-2026-09","runtime_session_id":"runtime/session-a","protocol_mode":"disposition"},"runtime_command_id":"2f1c7d1e-0f3a-4c5b-9f21-000000000001","kind":"`
	const payload = `","payload_digest":"47ffa3ea45a70b8a41c2c0825df323c00a8b7a01c1ea06083cc41dddcc001123","payload_size":3,"payload":"AP8B"`
	const principalWire = `,"principal":{"tenant":"Tenant/A:B","subject":"user/A:B","kind":"actor"}`
	const metadataWire = `,"metadata":{"client":"oxy-ios","space":"family"}`
	const tail = `},"accepted_at":"2026-08-30T11:30:00Z","apply_deadline":"2026-08-30T12:30:00Z","state":"pending"}`
	const v2, v3 = `{"record_version":2,"descriptor":{`, `{"record_version":3,"descriptor":{`
	principal := &sessionwire.Principal{Tenant: "Tenant/A:B", Subject: "user/A:B", Kind: sessionwire.PrincipalKindActor}
	metadata := sessionwire.MessageMetadata{"space": "family", "client": "oxy-ios"}
	for _, tc := range []struct {
		name      string
		kind      CommandKind
		principal *sessionwire.Principal
		metadata  sessionwire.MessageMetadata
		want      string
	}{
		{"both input", "input", principal, metadata, v3 + identityHead + "input" + payload + principalWire + metadataWire + tail},
		{"principal interrupt", "interrupt", principal, nil, v3 + identityHead + "interrupt" + payload + principalWire + tail},
		{"principal gate response", "gate_response", principal, nil, v3 + identityHead + "gate_response" + payload + principalWire + tail},
		{"metadata create", "create", nil, metadata, v3 + identityHead + "create" + payload + metadataWire + tail},
		{"plain v2", "input", nil, nil, v2 + identityHead + "input" + payload + tail},
		{"empty bag v2", "input", nil, sessionwire.MessageMetadata{}, v2 + identityHead + "input" + payload + tail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := DispositionCommandDescriptor{TenantID: "Tenant/A:B", SessionID: "Session/A:B", CommandID: "Create/A:B", Binding: testSessionBinding(), RuntimeCommandID: inboxRuntime, Kind: tc.kind, Payload: []byte{0, 255, 1}, PayloadDigest: "47ffa3ea45a70b8a41c2c0825df323c00a8b7a01c1ea06083cc41dddcc001123", PayloadSize: 3, Principal: tc.principal, Metadata: tc.metadata}
			r := DispositionInboxRecord{Descriptor: d, AcceptedAt: inboxAcceptedAt, ApplyDeadline: inboxDeadline, State: InboxStatePending}
			got, canonical, err := encodeDispositionInboxRecord(r)
			if err != nil || string(got) != tc.want {
				t.Fatalf("encode:\n got %s %v\nwant %s", got, err, tc.want)
			}
			decoded, err := decodeDispositionInboxRecord([]byte(tc.want))
			if err != nil || !reflect.DeepEqual(decoded, canonical) {
				t.Fatalf("decode: %+v %v; want %+v", decoded, err, canonical)
			}
		})
	}
}

func rawDispositionRow(t *testing.T, s *Store, command sessionwire.CommandID) []byte {
	t.Helper()
	scope, err := s.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := s.backend.OrderedIndex.Get(context.Background(), dispositionInboxID(scope, command))
	if err != nil {
		t.Fatal(err)
	}
	return stored.Value
}

func TestDispositionAdmissionSelectsRecordVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	createDispositionCatalog(t, s)
	plain := dispositionRequest()
	if _, _, err := s.AdmitDispositionCommand(ctx, plain); err != nil {
		t.Fatal(err)
	}
	raw := rawDispositionRow(t, s, plain.CommandID)
	if !bytes.HasPrefix(raw, []byte(`{"record_version":2,`)) || bytes.Contains(raw, []byte(`"principal"`)) || bytes.Contains(raw, []byte(`"metadata"`)) {
		t.Fatalf("plain row changed: %s", raw)
	}
	attributed := dispositionRequest()
	attributed.CommandID, attributed.Principal = "public/command:2", testPrincipal()
	if _, _, err := s.AdmitDispositionCommand(ctx, attributed); err != nil {
		t.Fatal(err)
	}
	if raw := rawDispositionRow(t, s, attributed.CommandID); !bytes.HasPrefix(raw, []byte(`{"record_version":3,`)) {
		t.Fatalf("attributed row is not v3: %s", raw)
	}
}

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
