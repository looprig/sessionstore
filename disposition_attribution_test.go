package sessionstore

import (
	"reflect"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

func testPrincipal() *sessionwire.Principal {
	return &sessionwire.Principal{Tenant: catalogTenant, Subject: "user/alex", Kind: sessionwire.PrincipalKindActor}
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
