package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// These literals pin stored bytes independently of the domain structs and DTOs.
// Empty workload members and zero payload_size are deliberately present.
func TestPublicCreateWireGolden(t *testing.T) {
	const identity = `"identity":{"tenant_id":"Tenant/A:B","session_id":"Session/A:B","command_id":"Create/A:B","target":{"agent_id":"Agent/A:B","runtime_compatibility_id":"Runtime/A:B","placement":"pooled"},"binding":{"storage_binding_id":"agent-pool/east","binding_version":"config-2026-09","runtime_session_id":"runtime/session-a","protocol_mode":"disposition"},"kind":"Kind/A:B","payload_digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","payload_size":0}`
	const times = `,"runtime_command_id":"2f1c7d1e-0f3a-4c5b-9f21-000000000001","accepted_at":"2026-08-30T11:30:00Z","apply_deadline":"2026-08-30T12:30:00Z","initial_workload":`
	const catalog = `{"record_version":3,"tenant_id":"Tenant/A:B","session_id":"Session/A:B","agent_id":"Agent/A:B","runtime_compatibility_id":"Runtime/A:B","created_at":"2026-08-30T11:30:00Z","last_active_at":"2026-08-30T11:30:00Z","state":"idle","residency":"cold","desired_placement":"pooled","last_journal_seq":0,"lease_epoch":0,"desired_idempotency_key":"Create/A:B","desired_generation":1`
	const binding = `,"binding":{"storage_binding_id":"agent-pool/east","binding_version":"config-2026-09","runtime_session_id":"runtime/session-a","protocol_mode":"disposition"},"public_create":`
	for _, tc := range []struct {
		name          string
		workload      DesiredWorkload
		wire, desired string
	}{
		{"empty", DesiredWorkload{}, `{"payload_version":"","payload":null}`, ""},
		{"nonempty", DesiredWorkload{PayloadVersion: "Workload/A:B", Payload: []byte{0, 255, 1}}, `{"payload_version":"Workload/A:B","payload":"AP8B"}`, `,"desired_workload":{"payload_version":"Workload/A:B","payload":"AP8B"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := publicCreateRequest()
			req.Identity.TenantID, req.Identity.SessionID = "Tenant/A:B", "Session/A:B"
			req.Identity.Target.AgentID, req.Identity.Target.RuntimeCompatibilityID = "Agent/A:B", "Runtime/A:B"
			req.Identity.Kind = "Kind/A:B"
			req.Identity.PayloadDigest, req.Identity.PayloadSize = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0
			r := PublicCreateReservation{Identity: req.Identity, RuntimeCommandID: req.ProposedRuntimeCommandID, AcceptedAt: req.AcceptedAt, ApplyDeadline: req.ApplyDeadline, InitialWorkload: tc.workload}
			body := "{" + identity + times + tc.wire + "}"
			want := `{"record_version":1,` + body[1:]
			got, _, err := encodePublicCreate(r)
			if err != nil || string(got) != want {
				t.Fatalf("reservation golden: %s, %v; want %s", got, err, want)
			}
			decoded, err := decodePublicCreate([]byte(want))
			if err != nil || !reflect.DeepEqual(decoded, r) {
				t.Fatalf("reservation decode: %+v %v", decoded, err)
			}
			wantCatalog := catalog + tc.desired + binding + body + "}"
			gotCatalog, err := encodeCatalogRecord(publicCreateCatalogRecord(r))
			if err != nil || string(gotCatalog) != wantCatalog {
				t.Fatalf("catalog golden: %s, %v; want %s", gotCatalog, err, wantCatalog)
			}
			decodedCatalog, err := decodeCatalogRecord([]byte(wantCatalog))
			if err != nil || !reflect.DeepEqual(decodedCatalog.PublicCreate, &r) {
				t.Fatalf("catalog decode: %+v %v", decodedCatalog, err)
			}
			for _, source := range []string{want, wantCatalog} {
				for _, bad := range [][]byte{
					bytes.Replace([]byte(source), []byte(`,"initial_workload":`+tc.wire), nil, 1),
					bytes.Replace([]byte(source), []byte(`"initial_workload":`+tc.wire), []byte(`"initial_workload":{}`), 1),
					bytes.Replace([]byte(source), []byte(`,"payload_size":0`), nil, 1),
				} {
					if source == want {
						_, err = decodePublicCreate(bad)
					} else {
						_, err = decodeCatalogRecord(bad)
					}
					if err == nil {
						t.Fatal("noncanonical omitted/default members accepted")
					}
				}
			}
		})
	}
}

// Only the first top-level occurrence changes; provenance and JSON ordering
// stay intact. Re-encoding cannot substitute for checking immutable agreement.
func TestPublicCreateCatalogProvenanceIdentity(t *testing.T) {
	for _, tc := range []struct{ name, from, to string }{
		{"tenant", `"tenant_id":"tenant-a"`, `"tenant_id":"other"`},
		{"session", `"session_id":"session-a"`, `"session_id":"other"`},
		{"agent", `"agent_id":"agent-a"`, `"agent_id":"other"`},
		{"binding", `"binding_version":"config-2026-09"`, `"binding_version":"other"`},
		{"created at", `"created_at":"2026-08-30T11:30:00Z"`, `"created_at":"2026-08-30T11:29:00Z"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := memstore.New()
			fault := &publicCreateReadFault{OrderedIndex: b.OrderedIndex}
			b.OrderedIndex = fault
			s := openStore(t, b)
			req := publicCreateRequest()
			p, err := s.PreparePublicCreate(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			v, err := encodeCatalogRecord(p.Catalog.Record)
			if err != nil {
				t.Fatal(err)
			}
			bad := bytes.Replace(v, []byte(tc.from), []byte(tc.to), 1)
			if bytes.Equal(bad, v) {
				t.Fatal("mutation missed its target")
			}
			_, err = decodeCatalogRecord(bad)
			var catalogErr *CatalogError
			if !errors.As(err, &catalogErr) || catalogErr.Code != CatalogErrorIdentity || catalogErr.Field != "public_create" {
				t.Errorf("codec accepted mismatched provenance: %v", err)
			}
			fault.namespace = catalogNamespace
			fault.mutate = func(r *storage.OrderedRecord) { r.Value = bad }
			_, err = s.GetCatalogEntry(t.Context(), GetCatalogEntryRequest{TenantID: req.Identity.TenantID, SessionID: req.Identity.SessionID})
			if !errors.As(err, &catalogErr) || catalogErr.Code != CatalogErrorIdentity || catalogErr.Field != "public_create" {
				t.Fatalf("read accepted mismatched provenance: %v", err)
			}
		})
	}
}

type publicCreateReadFault struct {
	storage.OrderedIndex
	namespace string
	mutate    func(*storage.OrderedRecord)
}

func (f *publicCreateReadFault) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	r, err := f.OrderedIndex.Get(ctx, id)
	if err == nil && id.Namespace == f.namespace {
		f.mutate(&r)
	}
	return r, err
}

func TestPublicCreateReservationFilingValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*storage.OrderedRecord)
		kind   InboxErrorCode
	}{
		{"namespace", func(r *storage.OrderedRecord) { r.ID.Namespace = "other" }, InboxErrorIdentity},
		{"stable key", func(r *storage.OrderedRecord) { r.ID.StableKey = "other" }, InboxErrorIdentity},
		{"ordering scope", func(r *storage.OrderedRecord) { r.ID.OrderingScope = "other" }, InboxErrorIdentity},
		{"ranking scope", func(r *storage.OrderedRecord) { r.RankingScope = "other" }, InboxErrorIdentity},
		{"due", func(r *storage.OrderedRecord) { r.Due = storage.Due{UnixMillis: 1} }, InboxErrorIdentity},
		{"order", func(r *storage.OrderedRecord) { r.Order = 0 }, InboxErrorIdentity},
		{"zero revision", func(r *storage.OrderedRecord) { r.Revision = 0 }, InboxErrorIdentity},
		// The revision guard is defense in depth: catalog provenance separately
		// prevents admission of a rewritten reservation with different proposals.
		{"rewritten revision", func(r *storage.OrderedRecord) { r.Revision = 2 }, InboxErrorIdentity},
		{"deleted", func(r *storage.OrderedRecord) { r.Deleted = true }, InboxErrorDeleted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := memstore.New()
			fault := &publicCreateReadFault{OrderedIndex: b.OrderedIndex}
			b.OrderedIndex = fault
			s := openStore(t, b)
			req := publicCreateRequest()
			if _, err := s.PreparePublicCreate(t.Context(), req); err != nil {
				t.Fatal(err)
			}
			scope, err := s.deriveSessionScope(req.Identity.TenantID, req.Identity.SessionID)
			if err != nil {
				t.Fatal(err)
			}
			stored, err := b.OrderedIndex.Get(t.Context(), publicCreateID(scope, req.Identity.CommandID))
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(&stored)
			_, err = publicCreateWinner(stored, scope, req.Identity)
			var inboxErr *InboxError
			if !errors.As(err, &inboxErr) || inboxErr.Code != tc.kind {
				t.Errorf("winner classification: %v, want %s", err, tc.kind)
			}
			fault.namespace, fault.mutate = publicCreateNamespace, tc.mutate
			entry, created, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: req.Identity, Payload: inboxPayload})
			if !errors.As(err, &inboxErr) || inboxErr.Code != tc.kind || created || !reflect.DeepEqual(entry, DispositionInboxEntry{}) {
				t.Fatalf("misfiled reservation acknowledged: %+v %v %v", entry, created, err)
			}
		})
	}
}

// The disposition inbox row is the third record AdmitPublicCreate writes. These
// literals pin its stored member names independently of the exported descriptor,
// including the public_create marker present and absent — it is omitempty, so
// both spellings need pinning — and the mutually exclusive inline payload and
// payload_object members.
func TestDispositionInboxWireGolden(t *testing.T) {
	const head = `{"record_version":2,"descriptor":{`
	const identity = `"tenant_id":"Tenant/A:B","session_id":"Session/A:B","command_id":"Create/A:B","binding":{"storage_binding_id":"agent-pool/east","binding_version":"config-2026-09","runtime_session_id":"runtime/session-a","protocol_mode":"disposition"},"runtime_command_id":"2f1c7d1e-0f3a-4c5b-9f21-000000000001","kind":"Kind/A:B","payload_digest":"`
	const tail = `},"accepted_at":"2026-08-30T11:30:00Z","apply_deadline":"2026-08-30T12:30:00Z","state":"pending"}`
	const inlineDigest = "47ffa3ea45a70b8a41c2c0825df323c00a8b7a01c1ea06083cc41dddcc001123"
	object := objectMetadataFor(ObjectKindCommandPayload, [16]byte{1}, 3, sha256.Sum256([]byte{0, 255, 1}), "text/plain")
	objectWire, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		d    DispositionCommandDescriptor
		want string
	}{
		{
			"marked inline",
			DispositionCommandDescriptor{PublicCreate: true, Payload: []byte{0, 255, 1}, PayloadDigest: inlineDigest, PayloadSize: 3},
			head + `"public_create":true,` + identity + inlineDigest + `","payload_size":3,"payload":"AP8B"` + tail,
		},
		{
			"unmarked inline",
			DispositionCommandDescriptor{Payload: []byte{0, 255, 1}, PayloadDigest: inlineDigest, PayloadSize: 3},
			head + identity + inlineDigest + `","payload_size":3,"payload":"AP8B"` + tail,
		},
		{
			"marked object",
			DispositionCommandDescriptor{PublicCreate: true, PayloadObject: &object, PayloadDigest: inlineDigest, PayloadSize: 3},
			head + `"public_create":true,` + identity + inlineDigest + `","payload_size":3,"payload_object":` + string(objectWire) + tail,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := tc.d
			d.TenantID, d.SessionID, d.CommandID = "Tenant/A:B", "Session/A:B", "Create/A:B"
			d.Binding, d.RuntimeCommandID, d.Kind = testSessionBinding(), inboxRuntime, "Kind/A:B"
			r := DispositionInboxRecord{Descriptor: d, AcceptedAt: inboxAcceptedAt, ApplyDeadline: inboxDeadline, State: InboxStatePending}
			got, canonical, err := encodeDispositionInboxRecord(r)
			if err != nil || string(got) != tc.want {
				t.Fatalf("inbox golden: %s, %v; want %s", got, err, tc.want)
			}
			decoded, err := decodeDispositionInboxRecord([]byte(tc.want))
			if err != nil || !reflect.DeepEqual(decoded, canonical) {
				t.Fatalf("inbox decode: %+v %v", decoded, err)
			}
			// Every alternate spelling of a durable member must be refused,
			// including an explicit rendering of the omitted marker. Simply
			// dropping the marker is NOT a spelling error — it is the valid
			// unmarked record above — so the winner comparison in
			// admitDispositionCommand, not the codec, is what refuses it.
			for _, bad := range [][]byte{
				bytes.Replace([]byte(tc.want), []byte(`"public_create"`), []byte(`"pc"`), 1),
				bytes.Replace([]byte(tc.want), []byte(`"descriptor":`), []byte(`"command":`), 1),
				bytes.Replace([]byte(tc.want), []byte(`,"payload_size":3`), nil, 1),
				[]byte(strings.Replace(tc.want, `"descriptor":{`, `"descriptor":{"public_create":false,`, 1)),
			} {
				if !bytes.Equal(bad, []byte(tc.want)) {
					if _, err := decodeDispositionInboxRecord(bad); err == nil {
						t.Fatalf("noncanonical member spelling accepted: %s", bad)
					}
				}
			}
		})
	}
}
