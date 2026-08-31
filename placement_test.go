package sessionstore

import (
	"bytes"
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// --- fixtures --------------------------------------------------------------

// testDesiredWorkload is the platform payload a Factory asks a dedicated
// workload to be created from. Its bytes are deliberately not JSON this package
// could be tempted to parse: the payload is opaque here and its schema belongs
// to whatever reconciles it.
func testDesiredWorkload() DesiredWorkload {
	return DesiredWorkload{
		PayloadVersion: "looprig.dev/dedicated-workload/v1",
		Payload:        []byte("\x00resources=2cpu\xff\x01workspace=persistent"),
	}
}

func mustPlacementIntent(t *testing.T, record CatalogRecord) PlacementIntent {
	t.Helper()
	intent, err := record.PlacementIntent()
	if err != nil {
		t.Fatalf("PlacementIntent: %v", err)
	}
	return intent
}

// storeCatalogRecordDirectly writes one record straight through the provider,
// bypassing every rule the public write paths apply. It exists for the cases
// that must start from a stored record no public operation could have written.
func storeCatalogRecordDirectly(t *testing.T, store *Store, record CatalogRecord) {
	t.Helper()
	scope, err := store.deriveSessionScope(record.TenantID, record.SessionID)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	value, err := encodeCatalogRecord(record)
	if err != nil {
		t.Fatalf("encodeCatalogRecord: %v", err)
	}
	id := catalogID(scope, record.SessionID)
	stored, err := store.backend.OrderedIndex.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := store.backend.OrderedIndex.Update(
		context.Background(), id, stored.Revision, value, catalogRank(record), storage.Due{}); err != nil {
		t.Fatalf("Update: %v", err)
	}
}

// --- the desired-state generation ------------------------------------------

// TestDesiredGenerationCountsAcceptedDesiredWrites pins what the generation IS:
// the number of desired-state writes this record has ACCEPTED. A controller
// cannot use the revision for that question — every Host heartbeat moves the
// revision — so without a counter that only Factory-authored writes advance,
// "has the desired state changed since I reconciled it?" has no durable answer.
func TestDesiredGenerationCountsAcceptedDesiredWrites(t *testing.T) {
	store := openTestStore(t)

	created := mustCreateCatalog(t, store)
	if created.Record.DesiredGeneration != 1 {
		t.Fatalf("generation at create = %d, want 1; a created session already has a desired state",
			created.Record.DesiredGeneration)
	}

	// A Host write moves the revision and must not move the generation.
	host, err := store.UpdateCatalogHostState(context.Background(), testHostStateRequest(4))
	if err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}
	if host.Revision == created.Revision {
		t.Fatal("a Host write did not advance the revision, so this case proves nothing")
	}
	if host.Record.DesiredGeneration != 1 {
		t.Fatalf("a Host write moved the generation to %d", host.Record.DesiredGeneration)
	}

	desired := UpdateCatalogDesiredStateRequest{
		TenantID:               catalogTenant,
		SessionID:              catalogSession,
		ExpectedRevision:       host.Revision,
		IdempotencyKey:         "place-2",
		DesiredPlacement:       sessionwire.HostPlacementDedicated,
		RuntimeCompatibilityID: "runtime-v2",
		DesiredWorkload:        testDesiredWorkload(),
	}
	applied, err := store.UpdateCatalogDesiredState(context.Background(), desired)
	if err != nil {
		t.Fatalf("UpdateCatalogDesiredState: %v", err)
	}
	if applied.Record.DesiredGeneration != 2 {
		t.Fatalf("generation after one desired write = %d, want 2", applied.Record.DesiredGeneration)
	}

	// A replay of that same intent applies nothing, so it must not advance the
	// generation either: a controller that saw the generation move would go
	// looking for a change that never happened.
	replay, err := store.UpdateCatalogDesiredState(context.Background(), desired)
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if replay.Record.DesiredGeneration != 2 {
		t.Fatalf("a replay advanced the generation to %d", replay.Record.DesiredGeneration)
	}

	// So does a key reused for a DIFFERENT intent, which is the same no-op
	// reported as success.
	reused := desired
	reused.DesiredPlacement = sessionwire.HostPlacementPooled
	reusedEntry, err := store.UpdateCatalogDesiredState(context.Background(), reused)
	if err != nil {
		t.Fatalf("reused key: %v", err)
	}
	if reusedEntry.Record.DesiredGeneration != 2 || reusedEntry.Record.DesiredPlacement != sessionwire.HostPlacementDedicated {
		t.Fatalf("a reused key applied a new intent: %+v", reusedEntry.Record)
	}

	// A lost revision compare-and-swap writes nothing at all.
	conflicting := desired
	conflicting.IdempotencyKey = "place-3"
	conflicting.ExpectedRevision = host.Revision
	if _, err := store.UpdateCatalogDesiredState(context.Background(), conflicting); err == nil {
		t.Fatal("a stale expected revision was accepted")
	}
	assertCatalogUnchanged(t, store, applied)

	// And the next accepted intent advances by exactly one.
	next := desired
	next.IdempotencyKey = "place-4"
	next.ExpectedRevision = applied.Revision
	next.DesiredPlacement = sessionwire.HostPlacementPooled
	nextEntry, err := store.UpdateCatalogDesiredState(context.Background(), next)
	if err != nil {
		t.Fatalf("UpdateCatalogDesiredState: %v", err)
	}
	if nextEntry.Record.DesiredGeneration != 3 {
		t.Fatalf("generation after two desired writes = %d, want 3", nextEntry.Record.DesiredGeneration)
	}
}

// TestStoredCatalogRecordAlwaysHasADesiredGeneration makes "this session has no
// desired state" unrepresentable rather than merely unusual. Every catalog
// record is created with one, so a zero generation is a corrupted record and is
// refused on the decode path as well as the encode path.
func TestStoredCatalogRecordAlwaysHasADesiredGeneration(t *testing.T) {
	t.Parallel()

	record := testCatalogRecord()
	record.DesiredGeneration = 0
	if _, err := encodeCatalogRecord(record); err == nil {
		t.Fatal("a record with no desired generation was encoded")
	} else {
		got := assertCatalogCode(t, err, CatalogErrorInvalid)
		if got.Field != "desired_generation" {
			t.Fatalf("field = %q, want desired_generation", got.Field)
		}
	}

	// The same rule on the way back in, reached through a document that is
	// otherwise a perfectly good record.
	valid, err := encodeCatalogRecord(testCatalogRecord())
	if err != nil {
		t.Fatalf("encodeCatalogRecord: %v", err)
	}
	generation := testCatalogRecord().DesiredGeneration
	if generation == 0 {
		t.Fatal("the fixture has no desired generation, so the substitution below proves nothing")
	}
	spelled := `"desired_generation":` + strconv.FormatUint(generation, 10)
	zeroed := strings.Replace(string(valid), spelled, `"desired_generation":0`, 1)
	if zeroed == string(valid) {
		t.Fatalf("could not zero the stored generation (%s): %s", spelled, valid)
	}
	if _, err := decodeCatalogRecord([]byte(zeroed)); err == nil {
		t.Fatal("a stored record with no desired generation decoded")
	} else {
		assertCatalogCode(t, err, CatalogErrorInvalid)
	}
}

// TestDesiredGenerationRefusesToWrapAtTheCeiling covers the one arithmetic that
// could silently undo the whole mechanism. A generation that wrapped to zero
// would be refused by the record's own rule above, but a wrap to a LOWER
// nonzero value would be accepted and would tell every controller that the
// desired state had rolled back to something it had already reconciled.
func TestDesiredGenerationRefusesToWrapAtTheCeiling(t *testing.T) {
	store := openTestStore(t)
	created := mustCreateCatalog(t, store)

	saturated := created.Record
	saturated.DesiredGeneration = math.MaxUint64
	storeCatalogRecordDirectly(t, store, saturated)
	current, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}

	_, err = store.UpdateCatalogDesiredState(context.Background(), UpdateCatalogDesiredStateRequest{
		TenantID:         catalogTenant,
		SessionID:        catalogSession,
		ExpectedRevision: current.Revision,
		IdempotencyKey:   "place-over",
		DesiredPlacement: sessionwire.HostPlacementDedicated,
	})
	if err == nil {
		t.Fatal("a desired-state write wrapped the generation")
	}
	got := assertCatalogCode(t, err, CatalogErrorSequence)
	if got.Field != "desired_generation" {
		t.Fatalf("field = %q, want desired_generation", got.Field)
	}
	assertCatalogUnchanged(t, store, current)
}

// --- the opaque platform workload -------------------------------------------

// TestDesiredWorkloadRoundTripsAsOpaqueBytes pins that this package stores the
// payload and nothing else. The bytes are not UTF-8, not JSON, and not
// inspected: a Kubernetes PodSpec, a Nomad job, or a future platform's manifest
// all pass through identically, which is what keeps platform types out of this
// repository.
func TestDesiredWorkloadRoundTripsAsOpaqueBytes(t *testing.T) {
	t.Parallel()

	record := testCatalogRecord()
	record.DesiredWorkload = testDesiredWorkload()
	encoded, err := encodeCatalogRecord(record)
	if err != nil {
		t.Fatalf("encodeCatalogRecord: %v", err)
	}
	decoded, err := decodeCatalogRecord(encoded)
	if err != nil {
		t.Fatalf("decodeCatalogRecord: %v", err)
	}
	if decoded.DesiredWorkload.PayloadVersion != record.DesiredWorkload.PayloadVersion {
		t.Fatalf("payload version = %q, want %q",
			decoded.DesiredWorkload.PayloadVersion, record.DesiredWorkload.PayloadVersion)
	}
	if !bytes.Equal(decoded.DesiredWorkload.Payload, record.DesiredWorkload.Payload) {
		t.Fatalf("payload = %q, want %q", decoded.DesiredWorkload.Payload, record.DesiredWorkload.Payload)
	}
	reencoded, err := encodeCatalogRecord(decoded)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if !bytes.Equal(encoded, reencoded) {
		t.Fatalf("round trip changed the canonical bytes:\n%s\n%s", encoded, reencoded)
	}
}

// TestDesiredWorkloadHasOneSpellingForAbsent holds the payload and its version
// to being present together or absent together. A payload with no version is a
// document nothing can interpret; a version with no payload is a claim about
// nothing; and an empty-but-present slice is a second spelling of absent, which
// is what makes a record's stored bytes depend on which writer produced them.
func TestDesiredWorkloadHasOneSpellingForAbsent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		workload DesiredWorkload
		field    string
	}{
		{
			name:     "a payload no reader can interpret",
			workload: DesiredWorkload{Payload: []byte("spec")},
			field:    "desired_workload.payload_version",
		},
		{
			name:     "a version describing nothing",
			workload: DesiredWorkload{PayloadVersion: "workload/v1"},
			field:    "desired_workload.payload",
		},
		{
			name:     "a version that is not valid UTF-8",
			workload: DesiredWorkload{PayloadVersion: "work\xffload", Payload: []byte("spec")},
			field:    "desired_workload.payload_version",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			record := testCatalogRecord()
			record.DesiredWorkload = test.workload
			_, err := encodeCatalogRecord(record)
			if err == nil {
				t.Fatal("a half-spelled workload was accepted")
			}
			got := assertCatalogCode(t, err, CatalogErrorInvalid)
			if got.Field != test.field {
				t.Fatalf("field = %q, want %q (%v)", got.Field, test.field, err)
			}
		})
	}

	// An empty-but-present payload slice is normalized to absent rather than
	// stored as a second spelling of it.
	record := testCatalogRecord()
	record.DesiredWorkload = DesiredWorkload{Payload: []byte{}}
	canonical, err := canonicalCatalogRecord(record)
	if err != nil {
		t.Fatalf("an empty payload slice was refused rather than normalized: %v", err)
	}
	if canonical.DesiredWorkload.Payload != nil {
		t.Fatalf("an empty payload slice survived canonicalization as %#v", canonical.DesiredWorkload.Payload)
	}

	// And an absent workload is absent from the stored bytes entirely.
	bare := testCatalogRecord()
	bare.DesiredWorkload = DesiredWorkload{}
	encoded, err := encodeCatalogRecord(bare)
	if err != nil {
		t.Fatalf("encodeCatalogRecord: %v", err)
	}
	if strings.Contains(string(encoded), "desired_workload") {
		t.Fatalf("an absent workload was still spelled in the record: %s", encoded)
	}
}

// TestDesiredWorkloadPayloadIsBounded holds the payload to a bound of its own
// rather than to the record's. A caller that oversizes the one open-ended
// member it controls must be told which member it was; "the record is too
// large" names a limit the caller cannot act on.
func TestDesiredWorkloadPayloadIsBounded(t *testing.T) {
	t.Parallel()

	atCeiling := testCatalogRecord()
	atCeiling.DesiredWorkload = DesiredWorkload{
		PayloadVersion: "workload/v1",
		Payload:        bytes.Repeat([]byte{0xff}, MaxDesiredWorkloadPayloadBytes),
	}
	if _, err := encodeCatalogRecord(atCeiling); err != nil {
		t.Fatalf("a payload exactly at the ceiling was refused: %v", err)
	}

	over := testCatalogRecord()
	over.DesiredWorkload = DesiredWorkload{
		PayloadVersion: "workload/v1",
		Payload:        bytes.Repeat([]byte{0xff}, MaxDesiredWorkloadPayloadBytes+1),
	}
	_, err := encodeCatalogRecord(over)
	if err == nil {
		t.Fatal("an oversized payload was accepted")
	}
	got := assertCatalogCode(t, err, CatalogErrorTooLarge)
	if got.Field != "desired_workload.payload" {
		t.Fatalf("field = %q, want desired_workload.payload (%v)", got.Field, err)
	}
}

// TestDesiredStateReplacesTheWorkloadWholesale states the rule the request type
// cannot: a desired-state write carries the COMPLETE desired state, so moving a
// session back to pooled by omitting the workload clears it. Merging instead
// would leave a dedicated workload spec attached to a pooled session, and the
// controller reconciling it has no way to tell that it was not meant.
func TestDesiredStateReplacesTheWorkloadWholesale(t *testing.T) {
	store := openTestStore(t)
	created := mustCreateCatalog(t, store)

	dedicated, err := store.UpdateCatalogDesiredState(context.Background(), UpdateCatalogDesiredStateRequest{
		TenantID:         catalogTenant,
		SessionID:        catalogSession,
		ExpectedRevision: created.Revision,
		IdempotencyKey:   "place-dedicated",
		DesiredPlacement: sessionwire.HostPlacementDedicated,
		DesiredWorkload:  testDesiredWorkload(),
	})
	if err != nil {
		t.Fatalf("UpdateCatalogDesiredState: %v", err)
	}
	if !bytes.Equal(dedicated.Record.DesiredWorkload.Payload, testDesiredWorkload().Payload) {
		t.Fatalf("the workload did not apply: %+v", dedicated.Record.DesiredWorkload)
	}

	pooled, err := store.UpdateCatalogDesiredState(context.Background(), UpdateCatalogDesiredStateRequest{
		TenantID:         catalogTenant,
		SessionID:        catalogSession,
		ExpectedRevision: dedicated.Revision,
		IdempotencyKey:   "place-pooled",
		DesiredPlacement: sessionwire.HostPlacementPooled,
	})
	if err != nil {
		t.Fatalf("UpdateCatalogDesiredState: %v", err)
	}
	if !pooled.Record.DesiredWorkload.isZero() {
		t.Fatalf("a write that named no workload left one behind: %+v", pooled.Record.DesiredWorkload)
	}
}

// TestDesiredWorkloadIsNotAliasedToItsCaller covers the hazard a []byte member
// brings that no string member does: a caller that keeps its slice can rewrite
// what a record means after this package has validated it.
func TestDesiredWorkloadIsNotAliasedToItsCaller(t *testing.T) {
	t.Parallel()

	record := testCatalogRecord()
	record.DesiredWorkload = testDesiredWorkload()
	original := bytes.Clone(record.DesiredWorkload.Payload)

	canonical, err := canonicalCatalogRecord(record)
	if err != nil {
		t.Fatalf("canonicalCatalogRecord: %v", err)
	}
	canonical.DesiredWorkload.Payload[0] ^= 0xff
	if !bytes.Equal(record.DesiredWorkload.Payload, original) {
		t.Fatal("canonicalization returned a record aliased to its argument")
	}

	intent := mustPlacementIntent(t, record)
	intent.Workload.Payload[0] ^= 0xff
	if !bytes.Equal(record.DesiredWorkload.Payload, original) {
		t.Fatal("the placement projection is aliased to the record it came from")
	}
}

// --- what the desired state may and may not say -----------------------------

// TestPlacementIntentCarriesOnlyDesiredState is this file's counterpart of the
// Host target directory's ownership test, and it exists for the same reason:
// desired placement is what a Factory ASKED for, and a consumer that could read
// an epoch, a Host, or a route out of it would be one refactor away from
// treating a request as a fact.
func TestPlacementIntentCarriesOnlyDesiredState(t *testing.T) {
	t.Parallel()

	forbidden := []string{"LeaseEpoch", "Epoch", "HostID", "Endpoint", "Residency", "JournalSeq", "Route"}
	intentType := reflect.TypeOf(PlacementIntent{})
	for i := range intentType.NumField() {
		field := intentType.Field(i)
		for _, word := range forbidden {
			if strings.Contains(field.Name, word) || strings.Contains(field.Type.Name(), word) {
				t.Errorf("PlacementIntent.%s names %s; desired placement is a request, never an observation",
					field.Name, word)
			}
		}
	}

	// The same question of every type the file declares, from source, so a
	// member added to one of them cannot slip past the reflection above.
	fields := structFieldSpellings(t, "placement.go", func(string) bool { return true })
	for _, field := range fields {
		for _, word := range forbidden {
			if strings.Contains(field.Spelling, word) {
				t.Errorf("%s.%s names %s; desired placement is a request, never an observation",
					field.TypeName, field.Spelling, word)
			}
		}
	}
	if len(fields) < 6 {
		t.Fatalf("only %d fields were inspected; the walk is not reaching the declarations", len(fields))
	}
}

// TestPlacementIntentIsTheRecordsOwnDesiredState keeps the projection honest in
// the other direction: a projection that carried nothing would pass the test
// above trivially.
func TestPlacementIntentIsTheRecordsOwnDesiredState(t *testing.T) {
	t.Parallel()

	record := testCatalogRecord()
	record.DesiredWorkload = testDesiredWorkload()
	intent := mustPlacementIntent(t, record)
	if intent.TenantID != record.TenantID || intent.SessionID != record.SessionID {
		t.Fatalf("identity = %+v, want the record's", intent)
	}
	if intent.AgentID != record.AgentID || intent.RuntimeCompatibilityID != record.RuntimeCompatibilityID {
		t.Fatalf("requirements = %+v, want the record's", intent)
	}
	if intent.Placement != record.DesiredPlacement {
		t.Fatalf("placement = %q, want %q", intent.Placement, record.DesiredPlacement)
	}
	if intent.Generation != record.DesiredGeneration {
		t.Fatalf("generation = %d, want %d", intent.Generation, record.DesiredGeneration)
	}
	if intent.Workload.PayloadVersion != record.DesiredWorkload.PayloadVersion ||
		!bytes.Equal(intent.Workload.Payload, record.DesiredWorkload.Payload) {
		t.Fatalf("workload = %+v, want %+v", intent.Workload, record.DesiredWorkload)
	}

	// It validates the record it projects, as Summary and Status do, so a
	// projection can never be produced from a record this package would refuse
	// to store.
	broken := record
	broken.DesiredPlacement = "anywhere"
	if _, err := broken.PlacementIntent(); err == nil {
		t.Fatal("a record with an unknown placement projected an intent")
	} else {
		assertCatalogCode(t, err, CatalogErrorInvalid)
	}
}

// TestDesiredStateCannotRebindTheAgent pins the one identity a Factory may not
// desire its way out of. The AgentID is fixed when the session is created —
// every journal record, every workspace, and every runtime compatibility
// decision is downstream of it — so the request type has no member for it and
// the mechanism is unavailable rather than merely unused.
func TestDesiredStateCannotRebindTheAgent(t *testing.T) {
	requestType := reflect.TypeOf(UpdateCatalogDesiredStateRequest{})
	for i := range requestType.NumField() {
		if name := strings.ToLower(requestType.Field(i).Name); strings.Contains(name, "agent") {
			t.Fatalf("UpdateCatalogDesiredStateRequest.%s lets a desired write rebind the session's agent",
				requestType.Field(i).Name)
		}
	}

	store := openTestStore(t)
	created := mustCreateCatalog(t, store)
	applied, err := store.UpdateCatalogDesiredState(context.Background(), UpdateCatalogDesiredStateRequest{
		TenantID:               catalogTenant,
		SessionID:              catalogSession,
		ExpectedRevision:       created.Revision,
		IdempotencyKey:         "place-agent",
		DesiredPlacement:       sessionwire.HostPlacementDedicated,
		RuntimeCompatibilityID: "runtime-v9",
	})
	if err != nil {
		t.Fatalf("UpdateCatalogDesiredState: %v", err)
	}
	if applied.Record.AgentID != created.Record.AgentID {
		t.Fatalf("agent = %q, want %q", applied.Record.AgentID, created.Record.AgentID)
	}
	// The runtime requirement, by contrast, IS desired state and moves.
	if applied.Record.RuntimeCompatibilityID != "runtime-v9" {
		t.Fatalf("runtime requirement = %q, want runtime-v9", applied.Record.RuntimeCompatibilityID)
	}
}

// TestCreateCarriesTheFirstDesiredWorkload closes the gap a create-shaped API
// invites: a dedicated session created with no way to say what to create would
// need a second write before it could be placed, and the interval between them
// is a session whose desired state is a lie.
func TestCreateCarriesTheFirstDesiredWorkload(t *testing.T) {
	store := openTestStore(t)

	req := testCreateRequest()
	req.DesiredPlacement = sessionwire.HostPlacementDedicated
	req.DesiredWorkload = testDesiredWorkload()
	created, _, err := store.CreateCatalogEntry(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateCatalogEntry: %v", err)
	}
	intent := mustPlacementIntent(t, created.Record)
	if intent.Placement != sessionwire.HostPlacementDedicated || intent.Generation != 1 {
		t.Fatalf("intent = %+v", intent)
	}
	if !bytes.Equal(intent.Workload.Payload, testDesiredWorkload().Payload) {
		t.Fatalf("workload = %+v", intent.Workload)
	}

	// A malformed workload is refused before any provider work, as every other
	// member of the create request is.
	base := memstore.New()
	recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = recorder
	fresh := openStore(t, base)
	bad := testCreateRequest()
	bad.DesiredWorkload = DesiredWorkload{Payload: []byte("no version")}
	if _, _, err := fresh.CreateCatalogEntry(context.Background(), bad); err == nil {
		t.Fatal("a malformed workload was created")
	} else {
		var invalid *CatalogError
		if !errors.As(err, &invalid) || invalid.Code != CatalogErrorInvalid {
			t.Fatalf("error = %T %v, want an invalid CatalogError", err, err)
		}
	}
	if calls := recorder.snapshot(); len(calls) != 0 {
		t.Fatalf("a refused create reached the provider: %+v", calls)
	}
}

// TestOnlyTheDesiredStatePathsWriteTheGeneration is the cross product the
// behavioural test above cannot be.
//
// `TestDesiredGenerationCountsAcceptedDesiredWrites` drives ONE non-desired
// write path — UpdateCatalogHostState — and asserts the counter does not move.
// But writeCatalogRecord has five callers, two of them in gates.go, and adding
// `next.DesiredGeneration++` to OpenGate passes the whole suite green while
// destroying the counter's meaning: every gate opened would tell every
// placement controller that its reconciliation was stale. Driving each caller
// by hand is a hand-written list where the invariant is a cross product over
// write paths, and a sixth caller added later would be covered by nobody.
//
// So the assertion is structural, in the shape of the claim isolation guard:
// find every function in this package that WRITES the member, and require each
// to be one this file expects. A path added later fails here until someone
// classifies it, which is the point — the two codec functions below are exactly
// such a classification, and they are excluded with a reason rather than
// omitted.
func TestOnlyTheDesiredStatePathsWriteTheGeneration(t *testing.T) {
	t.Parallel()

	// Where the generation may be written, and why each is legitimate.
	expected := map[string]string{
		"placement.go/applyDesiredState": "the one path that ADVANCES it, after the key and the revision have settled",
		"catalog.go/CreateCatalogEntry":  "the one path that mints it, because creating a session names its desired placement",
		"catalog.go/encodeCatalogRecord": "codec carry-through: copies the record's value into the wire shape unchanged",
		"catalog.go/decodeCatalogRecord": "codec carry-through: copies the wire value back into the record unchanged",
	}

	found := map[string]bool{}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	inspected := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		inspected++
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			site := name + "/" + function.Name.Name
			ast.Inspect(function.Body, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.AssignStmt:
					// `x.DesiredGeneration = ...`, in any assignment form.
					for _, target := range node.Lhs {
						if writesDesiredGeneration(target) {
							found[site] = true
						}
					}
				case *ast.IncDecStmt:
					// `x.DesiredGeneration++`, which is the mutation this test
					// exists for and is not an AssignStmt.
					if writesDesiredGeneration(node.X) {
						found[site] = true
					}
				case *ast.KeyValueExpr:
					// `DesiredGeneration: ...` inside a composite literal, which
					// is how both codec paths and the create path write it.
					if key, ok := node.Key.(*ast.Ident); ok && key.Name == "DesiredGeneration" {
						found[site] = true
					}
				}
				return true
			})
		}
	}
	if inspected < 8 {
		t.Fatalf("only %d production files were inspected; the walk is not reaching them", inspected)
	}

	for site := range found {
		if expected[site] == "" {
			t.Errorf("%s writes DesiredGeneration and this test does not expect it; "+
				"a write path outside the desired-state paths makes the counter mean something else", site)
		}
	}
	for site, reason := range expected {
		if !found[site] {
			t.Errorf("%s no longer writes DesiredGeneration (%s); the walk or the expectation is stale", site, reason)
		}
	}
}

// writesDesiredGeneration reports whether an expression names the record's
// generation member as the target of a write.
func writesDesiredGeneration(target ast.Expr) bool {
	selector, ok := target.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "DesiredGeneration"
}

// TestAStoredWorkloadPayloadIsNormalizedNotPreserved pins the fact
// DesiredWorkload's own comment states, because that comment is a warning to a
// later task and a warning nothing exercises is a warning that rots.
//
// encoding/json decodes a []byte with NON-STRICT base64, so a stored payload
// whose final quantum has dirty padding bits decodes to the same bytes and
// re-encodes to a different spelling. The record is still canonical in the
// sense the codec fuzzer asserts — normalizing twice equals normalizing once —
// but it is NOT byte-identical to what a non-canonical writer stored, which is
// what a bytes-identity check on a write reply would assume.
func TestAStoredWorkloadPayloadIsNormalizedNotPreserved(t *testing.T) {
	t.Parallel()

	record := testCatalogRecord()
	record.DesiredWorkload = DesiredWorkload{PayloadVersion: "workload/v1", Payload: []byte{0x01}}
	canonical, err := encodeCatalogRecord(record)
	if err != nil {
		t.Fatalf("encodeCatalogRecord: %v", err)
	}
	if !strings.Contains(string(canonical), `"payload":"AQ=="`) {
		t.Fatalf("the canonical spelling of one 0x01 byte is not what this test assumes: %s", canonical)
	}

	// The same byte, spelled with dirty padding bits, which base64's decoder
	// accepts and this package therefore stores.
	dirty := []byte(strings.Replace(string(canonical), `"payload":"AQ=="`, `"payload":"AR=="`, 1))
	if bytes.Equal(dirty, canonical) {
		t.Fatal("could not build the non-canonical spelling")
	}
	decoded, err := decodeCatalogRecord(dirty)
	if err != nil {
		t.Fatalf("a non-canonical payload spelling was refused: %v", err)
	}
	if !bytes.Equal(decoded.DesiredWorkload.Payload, []byte{0x01}) {
		t.Fatalf("payload = %v, want [1]", decoded.DesiredWorkload.Payload)
	}

	// Re-encoding it does NOT reproduce the stored bytes — the point of the
	// warning — while decoding the re-encoding is stable, which is the property
	// the codec actually promises.
	reencoded, err := encodeCatalogRecord(decoded)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if bytes.Equal(reencoded, dirty) {
		t.Fatal("the stored bytes survived a round trip; the warning on DesiredWorkload is now false")
	}
	if !bytes.Equal(reencoded, canonical) {
		t.Fatalf("re-encoding did not reach the canonical form:\n%s\n%s", reencoded, canonical)
	}
	again, err := decodeCatalogRecord(reencoded)
	if err != nil {
		t.Fatalf("decode the re-encoding: %v", err)
	}
	third, err := encodeCatalogRecord(again)
	if err != nil {
		t.Fatalf("third encode: %v", err)
	}
	if !bytes.Equal(third, reencoded) {
		t.Fatal("canonicalization has no fixed point")
	}
}
