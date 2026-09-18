package sessionstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// --- fixtures ---------------------------------------------------------------

// terminationRecordedAt is the instant the fixture store's clock reads when a
// termination is committed. It is deliberately NOT the registry fixture's
// observation instant, so a record stamped from a caller-visible or registry
// instant instead of the store's clock would be caught.
var terminationRecordedAt = time.Date(2026, 9, 18, 9, 30, 0, 0, time.UTC)

// terminationFixture opens a store whose fixture session exists as a legacy
// catalog record at desired generation one, with an instrumented ordered index
// so a test can count the writes one operation makes.
func terminationFixture(t *testing.T) (*Store, *movableClock, *recordingOrdered) {
	t.Helper()
	backend := memstore.New()
	audit := &recordingOrdered{OrderedIndex: backend.OrderedIndex}
	backend.OrderedIndex = audit
	clock := newMovableClock(registryObservedAt)
	store := openStore(t, backend, WithClock(clock))
	if _, _, err := store.CreateCatalogEntry(context.Background(), testCreateRequest()); err != nil {
		t.Fatalf("CreateCatalogEntry: %v", err)
	}
	return store, clock, audit
}

// advanceDesiredGeneration applies n accepted desired-state writes, so the
// catalog has ISSUED generations 1..n+1.
func advanceDesiredGeneration(t *testing.T, store *Store, n int) {
	t.Helper()
	for i := range n {
		entry, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: catalogSession})
		if err != nil {
			t.Fatal(err)
		}
		_, err = store.UpdateCatalogDesiredState(context.Background(), UpdateCatalogDesiredStateRequest{
			TenantID: catalogTenant, SessionID: catalogSession,
			ExpectedRevision: entry.Revision,
			IdempotencyKey:   "desire-" + string(rune('a'+i)),
			DesiredPlacement: sessionwire.HostPlacementDedicated,
		})
		if err != nil {
			t.Fatalf("UpdateCatalogDesiredState: %v", err)
		}
	}
}

func testRetainedCheckpoint() RetainedCheckpoint {
	return RetainedCheckpoint{Sequence: pointerSequence, Reference: testObjectReference(ObjectKindWorkspaceCheckpoint, 1)}
}

func testForcedRequest(generation, observed uint64) RecordPlacementTerminationRequest {
	return RecordPlacementTerminationRequest{
		TenantID:           catalogTenant,
		SessionID:          catalogSession,
		Generation:         generation,
		Kind:               PlacementTerminationForced,
		ForcedReason:       PlacementForcedDrainTimeout,
		ObservedLeaseEpoch: observed,
		Checkpoint:         testRetainedCheckpoint(),
		Objects: []sessionwire.ObjectReference{
			testObjectReference(ObjectKindRuntimeCheckpoint, 2),
		},
	}
}

func testGracefulRequest(generation, observed uint64) RecordPlacementTerminationRequest {
	req := testForcedRequest(generation, observed)
	req.Kind, req.ForcedReason = PlacementTerminationGraceful, ""
	return req
}

func testPlacementTermination() PlacementTermination {
	return PlacementTermination{
		TenantID:     catalogTenant,
		SessionID:    catalogSession,
		Generation:   3,
		Kind:         PlacementTerminationForced,
		ForcedReason: PlacementForcedPlatformDeleted,
		LeaseEpoch:   registryEpoch,
		RecordedAt:   terminationRecordedAt,
		Checkpoint:   testRetainedCheckpoint(),
		// Canonical order is ascending ObjectID, which here is ascending kind.
		Objects: []sessionwire.ObjectReference{
			testObjectReference(ObjectKindContinuation, 3),
			testObjectReference(ObjectKindRuntimeCheckpoint, 2),
		},
	}
}

func assertTerminationCode(t *testing.T, err error, want TerminationErrorCode) *TerminationError {
	t.Helper()
	var got *TerminationError
	if !errors.As(err, &got) {
		t.Fatalf("error = %T %v, want *TerminationError %q", err, err, want)
	}
	if got.Code != want {
		t.Fatalf("termination code = %q, want %q (%v)", got.Code, want, err)
	}
	return got
}

func mustRecordTermination(t *testing.T, store *Store, req RecordPlacementTerminationRequest) PlacementTerminationEntry {
	t.Helper()
	entry, _, err := store.RecordPlacementTermination(context.Background(), req)
	if err != nil {
		t.Fatalf("RecordPlacementTermination: %v", err)
	}
	return entry
}

func assertSameTermination(t *testing.T, got, want PlacementTermination) {
	t.Helper()
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("termination =\n %s\nwant\n %s", gotJSON, wantJSON)
	}
}

// storedTermination reads the raw row, reporting absence as found=false.
func storedTermination(t *testing.T, store *Store) (PlacementTerminationEntry, bool) {
	t.Helper()
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatal(err)
	}
	entry, found, err := store.readPlacementTermination(context.Background(), scope, catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("readPlacementTermination: %v", err)
	}
	return entry, found
}

// --- the stored record and its codec -------------------------------------------

func TestPlacementTerminationRoundTripsThroughStoredBytes(t *testing.T) {
	t.Parallel()

	for name, record := range map[string]PlacementTermination{
		"forced with everything": testPlacementTermination(),
		"graceful with nothing retained": func() PlacementTermination {
			r := testPlacementTermination()
			r.Kind, r.ForcedReason = PlacementTerminationGraceful, ""
			r.Checkpoint, r.Objects = RetainedCheckpoint{}, nil
			return r
		}(),
		"forced with no Host ever observed": func() PlacementTermination {
			r := testPlacementTermination()
			r.ForcedReason, r.LeaseEpoch = PlacementForcedWorkloadTerminated, 0
			return r
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			encoded, canonical, err := encodePlacementTermination(record)
			if err != nil {
				t.Fatalf("encodePlacementTermination: %v", err)
			}
			assertSameTermination(t, canonical, record)
			decoded, err := decodePlacementTermination(encoded)
			if err != nil {
				t.Fatalf("decodePlacementTermination: %v", err)
			}
			assertSameTermination(t, decoded, record)
			again, _, err := encodePlacementTermination(decoded)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(again, encoded) {
				t.Fatalf("re-encoding is not a fixed point:\n%s\n%s", encoded, again)
			}
		})
	}
}

// TestPlacementTerminationStoredBytesArePinned is the durable wire format,
// byte for byte. It is spelled by the private DTO; the exported struct has no
// JSON tags and changing it cannot move these bytes.
func TestPlacementTerminationStoredBytesArePinned(t *testing.T) {
	t.Parallel()

	record := testPlacementTermination()
	record.RecordedAt = time.Date(2026, 9, 18, 9, 30, 0, 0, time.FixedZone("x", 3600))
	encoded, _, err := encodePlacementTermination(record)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"record_version":1,"tenant_id":"tenant-a","session_id":"session-a","generation":3,` +
		`"kind":"forced","forced_reason":"platform_deleted","lease_epoch":5,` +
		`"recorded_at":"2026-09-18T08:30:00Z",` +
		`"checkpoint":{"sequence":9,"reference":{"object_id":"` + testObjectReference(ObjectKindWorkspaceCheckpoint, 1).ObjectID + `"}},` +
		`"objects":[{"object_id":"` + testObjectReference(ObjectKindContinuation, 3).ObjectID + `"},` +
		`{"object_id":"` + testObjectReference(ObjectKindRuntimeCheckpoint, 2).ObjectID + `"}]}`
	if string(encoded) != want {
		t.Fatalf("stored bytes =\n%s\nwant\n%s", encoded, want)
	}

	graceful := testPlacementTermination()
	graceful.Kind, graceful.ForcedReason = PlacementTerminationGraceful, ""
	graceful.Checkpoint, graceful.Objects = RetainedCheckpoint{}, nil
	encoded, _, err = encodePlacementTermination(graceful)
	if err != nil {
		t.Fatal(err)
	}
	want = `{"record_version":1,"tenant_id":"tenant-a","session_id":"session-a","generation":3,` +
		`"kind":"graceful","lease_epoch":5,"recorded_at":"2026-09-18T09:30:00Z"}`
	if string(encoded) != want {
		t.Fatalf("graceful stored bytes =\n%s\nwant\n%s", encoded, want)
	}
}

// TestPlacementTerminationCanonicalFormRefuses holds every rule of the one
// canonical spelling. Each row asserts the exact code AND field.
func TestPlacementTerminationCanonicalFormRefuses(t *testing.T) {
	t.Parallel()

	ref := func(kind ObjectKind, seed byte) sessionwire.ObjectReference { return testObjectReference(kind, seed) }
	cases := []struct {
		name   string
		mutate func(*PlacementTermination)
		field  string
	}{
		{"tenant", func(r *PlacementTermination) { r.TenantID = "" }, "tenant_id"},
		{"session", func(r *PlacementTermination) { r.SessionID = "" }, "session_id"},
		{"zero generation", func(r *PlacementTermination) { r.Generation = 0 }, "generation"},
		{"unknown kind", func(r *PlacementTermination) { r.Kind = "vanished" }, "kind"},
		{"empty kind", func(r *PlacementTermination) { r.Kind = "" }, "kind"},
		{"graceful with a reason", func(r *PlacementTermination) { r.Kind = PlacementTerminationGraceful }, "forced_reason"},
		{"forced without a reason", func(r *PlacementTermination) { r.ForcedReason = "" }, "forced_reason"},
		{"forced with an unknown reason", func(r *PlacementTermination) { r.ForcedReason = "meteor" }, "forced_reason"},
		{"graceful with no epoch", func(r *PlacementTermination) {
			r.Kind, r.ForcedReason, r.LeaseEpoch = PlacementTerminationGraceful, "", 0
		}, "lease_epoch"},
		{"unrepresentable instant", func(r *PlacementTermination) { r.RecordedAt = time.Time{} }, "recorded_at"},
		{"checkpoint without a sequence", func(r *PlacementTermination) { r.Checkpoint.Sequence = 0 }, "checkpoint.sequence"},
		{"checkpoint sequence without a reference", func(r *PlacementTermination) {
			r.Checkpoint.Reference = sessionwire.ObjectReference{}
		}, "checkpoint.reference"},
		{"checkpoint of another kind", func(r *PlacementTermination) {
			r.Checkpoint.Reference = ref(ObjectKindRuntimeCheckpoint, 1)
		}, "checkpoint.reference"},
		{"checkpoint unparseable", func(r *PlacementTermination) {
			r.Checkpoint.Reference = sessionwire.ObjectReference{ObjectID: "not-an-object"}
		}, "checkpoint.reference"},
		{"object unparseable", func(r *PlacementTermination) {
			r.Objects = []sessionwire.ObjectReference{{ObjectID: "not-an-object"}}
		}, "objects"},
		{"objects out of order", func(r *PlacementTermination) {
			r.Objects = []sessionwire.ObjectReference{ref(ObjectKindRuntimeCheckpoint, 2), ref(ObjectKindContinuation, 3)}
		}, "objects"},
		{"objects duplicated", func(r *PlacementTermination) {
			r.Objects = []sessionwire.ObjectReference{ref(ObjectKindContinuation, 3), ref(ObjectKindContinuation, 3)}
		}, "objects"},
		{"too many objects", func(r *PlacementTermination) {
			r.Objects = nil
			for seed := byte(1); seed <= MaxRetainedObjectReferences+1; seed++ {
				r.Objects = append(r.Objects, ref(ObjectKindArtifact, seed))
			}
			sortObjectReferences(r.Objects)
		}, "objects"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			record := testPlacementTermination()
			tc.mutate(&record)
			_, _, err := encodePlacementTermination(record)
			if got := assertTerminationCode(t, err, TerminationErrorInvalid); got.Field != tc.field {
				t.Fatalf("field = %q, want %q (%v)", got.Field, tc.field, err)
			}
		})
	}

	// The positive controls for the two list boundaries: exactly the maximum,
	// and an empty (non-nil) list, which canonicalizes to absent.
	t.Run("control: the maximum number of objects", func(t *testing.T) {
		t.Parallel()
		record := testPlacementTermination()
		record.Objects = nil
		for seed := byte(1); seed <= MaxRetainedObjectReferences; seed++ {
			record.Objects = append(record.Objects, ref(ObjectKindArtifact, seed))
		}
		sortObjectReferences(record.Objects)
		if _, _, err := encodePlacementTermination(record); err != nil {
			t.Fatalf("the maximum was refused: %v", err)
		}
	})
	t.Run("control: an empty list is absent", func(t *testing.T) {
		t.Parallel()
		record := testPlacementTermination()
		record.Objects = []sessionwire.ObjectReference{}
		_, canonical, err := encodePlacementTermination(record)
		if err != nil {
			t.Fatal(err)
		}
		if canonical.Objects != nil {
			t.Fatalf("objects = %#v, want nil", canonical.Objects)
		}
	})
}

// TestPlacementTerminationDecodeFailsClosed holds the decoder to the shared
// versioned-record rules in this record's own vocabulary.
func TestPlacementTerminationDecodeFailsClosed(t *testing.T) {
	t.Parallel()

	encoded, _, err := encodePlacementTermination(testPlacementTermination())
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		value []byte
		code  TerminationErrorCode
		field string
	}{
		{"empty", nil, TerminationErrorMalformed, "record"},
		{"version", bytes.Replace(encoded, []byte(`"record_version":1`), []byte(`"record_version":2`), 1), TerminationErrorVersion, "record_version"},
		{"unknown member", bytes.Replace(encoded, []byte(`{"record_version":1,`), []byte(`{"record_version":1,"surprise":1,`), 1), TerminationErrorMalformed, "record"},
		{"too large", append(bytes.Repeat([]byte(" "), MaxPlacementTerminationRecordBytes), encoded...), TerminationErrorTooLarge, "record"},
		{"invalid content", bytes.Replace(encoded, []byte(`"generation":3`), []byte(`"generation":0`), 1), TerminationErrorInvalid, "generation"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := decodePlacementTermination(tc.value)
			if got := assertTerminationCode(t, err, tc.code); got.Field != tc.field {
				t.Fatalf("field = %q, want %q", got.Field, tc.field)
			}
		})
	}
}

// TestLargestAcceptablePlacementTerminationFitsTheBound builds the worst case
// the validators accept — maximal identities made entirely of characters JSON
// escapes six-fold, every reason at its longest, and the maximum number of
// retained objects — and requires it to encode under the bound.
func TestLargestAcceptablePlacementTerminationFitsTheBound(t *testing.T) {
	t.Parallel()

	record := testPlacementTermination()
	record.TenantID = sessionwire.TenantID(strings.Repeat("\x01", sessionwire.MaxIDBytes))
	record.SessionID = sessionwire.SessionID(strings.Repeat("\x01", sessionwire.MaxIDBytes))
	record.Generation = ^uint64(0)
	record.LeaseEpoch = ^uint64(0)
	record.Checkpoint.Sequence = ^uint64(0)
	record.ForcedReason = PlacementForcedWorkloadTerminated
	record.RecordedAt = time.Date(2026, 12, 31, 23, 59, 59, 999999999, time.UTC)
	record.Objects = nil
	for seed := byte(1); seed <= MaxRetainedObjectReferences; seed++ {
		record.Objects = append(record.Objects, testObjectReference(ObjectKindWorkspaceCheckpoint, seed))
	}
	sortObjectReferences(record.Objects)
	if err := record.TenantID.Validate(); err != nil {
		t.Fatalf("the worst-case tenant is not one the validators accept: %v", err)
	}
	encoded, _, err := encodePlacementTermination(record)
	if err != nil {
		t.Fatalf("the largest acceptable termination was refused: %v", err)
	}
	t.Logf("largest acceptable termination: %d of %d bytes", len(encoded), MaxPlacementTerminationRecordBytes)
}

// --- the write path: refusals --------------------------------------------------

// TestRecordPlacementTerminationRefusesAnInvalidRequestBeforeAnyProviderWork
// holds every request-shape refusal to "nothing was read or written".
func TestRecordPlacementTerminationRefusesAnInvalidRequestBeforeAnyProviderWork(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(*RecordPlacementTerminationRequest)
		field  string
	}{
		{"zero generation", func(r *RecordPlacementTerminationRequest) { r.Generation = 0 }, "generation"},
		{"unknown kind", func(r *RecordPlacementTerminationRequest) { r.Kind = "vanished" }, "kind"},
		{"graceful with a reason", func(r *RecordPlacementTerminationRequest) { r.Kind = PlacementTerminationGraceful }, "forced_reason"},
		{"forced without a reason", func(r *RecordPlacementTerminationRequest) { r.ForcedReason = "" }, "forced_reason"},
		{"graceful with no epoch", func(r *RecordPlacementTerminationRequest) {
			r.Kind, r.ForcedReason, r.ObservedLeaseEpoch = PlacementTerminationGraceful, "", 0
		}, "lease_epoch"},
		{"checkpoint of another kind", func(r *RecordPlacementTerminationRequest) {
			r.Checkpoint.Reference = testObjectReference(ObjectKindRuntimeCheckpoint, 1)
		}, "checkpoint.reference"},
		{"objects out of order", func(r *RecordPlacementTerminationRequest) {
			r.Objects = []sessionwire.ObjectReference{testObjectReference(ObjectKindRuntimeCheckpoint, 2), testObjectReference(ObjectKindContinuation, 3)}
		}, "objects"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _, audit := terminationFixture(t)
			audit.reset()
			req := testForcedRequest(1, 0)
			tc.mutate(&req)
			_, _, err := store.RecordPlacementTermination(context.Background(), req)
			if got := assertTerminationCode(t, err, TerminationErrorInvalid); got.Field != tc.field {
				t.Fatalf("field = %q, want %q", got.Field, tc.field)
			}
			if calls := audit.snapshot(); len(calls) != 0 {
				t.Fatalf("an invalid request reached the provider: %+v", calls)
			}
		})
	}
}

// TestRecordPlacementTerminationRefusesAnUnissuedGeneration is the rule that
// makes the generation STORE-ISSUED. The generation becomes a monotonic bound
// on every later writer, so a caller must not be able to name one the catalog
// has not minted: a caller naming MaxUint64 would otherwise lock every real
// generation out of this session for good.
func TestRecordPlacementTerminationRefusesAnUnissuedGeneration(t *testing.T) {
	t.Parallel()

	store, _, audit := terminationFixture(t)
	audit.reset()
	for _, generation := range []uint64{2, ^uint64(0)} {
		_, _, err := store.RecordPlacementTermination(context.Background(), testForcedRequest(generation, 0))
		got := assertTerminationCode(t, err, TerminationErrorUnissued)
		if got.Field != "generation" || got.Generation != 1 {
			t.Fatalf("refusal = %+v, want field generation carrying the issued generation 1", got)
		}
	}
	if n := audit.countOf("create") + audit.countOf("update"); n != 0 {
		t.Fatalf("an unissued generation wrote %d times", n)
	}

	// Control: once the catalog issues it, the same generation is admitted.
	advanceDesiredGeneration(t, store, 1)
	entry := mustRecordTermination(t, store, testForcedRequest(2, 0))
	if entry.Termination.Generation != 2 {
		t.Fatalf("generation = %d, want 2", entry.Termination.Generation)
	}
}

// TestRecordPlacementTerminationIsMonotonicOnGeneration: an outcome for
// generation N is never written or overwritten under a lower generation, and
// the refusal carries the stored high-water.
func TestRecordPlacementTerminationIsMonotonicOnGeneration(t *testing.T) {
	t.Parallel()

	store, _, audit := terminationFixture(t)
	advanceDesiredGeneration(t, store, 2)
	stored := mustRecordTermination(t, store, testForcedRequest(2, 0))

	audit.reset()
	for _, req := range []RecordPlacementTerminationRequest{
		testForcedRequest(1, 0),
		func() RecordPlacementTerminationRequest {
			r := testForcedRequest(1, 0)
			r.ForcedReason = PlacementForcedPlatformDeleted
			return r
		}(),
	} {
		_, _, err := store.RecordPlacementTermination(context.Background(), req)
		got := assertTerminationCode(t, err, TerminationErrorSuperseded)
		if got.Field != "generation" || got.Generation != 2 {
			t.Fatalf("refusal = %+v, want field generation carrying the stored generation 2", got)
		}
	}
	if n := audit.countOf("create") + audit.countOf("update"); n != 0 {
		t.Fatalf("a lower generation wrote %d times", n)
	}
	after, found := storedTermination(t, store)
	if !found || after.Revision != stored.Revision {
		t.Fatalf("stored row moved: found %v revision %d, want %d", found, after.Revision, stored.Revision)
	}
	assertSameTermination(t, after.Termination, stored.Termination)

	// Control: a HIGHER generation replaces it.
	next := mustRecordTermination(t, store, testForcedRequest(3, 0))
	if next.Termination.Generation != 3 || next.Revision <= stored.Revision {
		t.Fatalf("higher generation = %+v", next)
	}
}

// TestRecordPlacementTerminationIsCreateOnlyPerGeneration: a repeat with
// identical content is idempotent and writes nothing; a repeat with any
// different caller-authored member is a mismatch that writes nothing.
func TestRecordPlacementTerminationIsCreateOnlyPerGeneration(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(*RecordPlacementTerminationRequest)
		field  string
	}{
		{"kind", func(r *RecordPlacementTerminationRequest) { r.Kind, r.ForcedReason = PlacementTerminationGraceful, "" }, "kind"},
		{"forced reason", func(r *RecordPlacementTerminationRequest) { r.ForcedReason = PlacementForcedPlatformDeleted }, "forced_reason"},
		{"observed epoch", func(r *RecordPlacementTerminationRequest) { r.ObservedLeaseEpoch = registryStaleEpoch }, "lease_epoch"},
		{"checkpoint", func(r *RecordPlacementTerminationRequest) { r.Checkpoint.Sequence++ }, "checkpoint"},
		{"checkpoint dropped", func(r *RecordPlacementTerminationRequest) { r.Checkpoint = RetainedCheckpoint{} }, "checkpoint"},
		{"objects", func(r *RecordPlacementTerminationRequest) { r.Objects = nil }, "objects"},
		{"another object", func(r *RecordPlacementTerminationRequest) {
			r.Objects = []sessionwire.ObjectReference{testObjectReference(ObjectKindRuntimeCheckpoint, 9)}
		}, "objects"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, clock, audit := terminationFixture(t)
			mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
			if _, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryEpoch)); err != nil {
				t.Fatal(err)
			}
			clock.set(terminationRecordedAt)
			first := mustRecordTermination(t, store, testForcedRequest(1, registryEpoch))

			// A later instant proves the replay returns the STORED record
			// rather than re-stamping it.
			clock.set(terminationRecordedAt.Add(time.Hour))
			audit.reset()
			replay, created, err := store.RecordPlacementTermination(context.Background(), testForcedRequest(1, registryEpoch))
			if err != nil || created {
				t.Fatalf("identical replay = created %v, err %v; want an idempotent success", created, err)
			}
			if replay.Revision != first.Revision {
				t.Fatalf("replay revision = %d, want %d", replay.Revision, first.Revision)
			}
			assertSameTermination(t, replay.Termination, first.Termination)
			if n := audit.countOf("create") + audit.countOf("update"); n != 0 {
				t.Fatalf("an identical replay wrote %d times", n)
			}

			req := testForcedRequest(1, registryEpoch)
			tc.mutate(&req)
			_, _, err = store.RecordPlacementTermination(context.Background(), req)
			got := assertTerminationCode(t, err, TerminationErrorMismatch)
			if got.Field != tc.field || got.Generation != 1 {
				t.Fatalf("refusal = %+v, want field %q carrying generation 1", got, tc.field)
			}
			if n := audit.countOf("create") + audit.countOf("update"); n != 0 {
				t.Fatalf("a mismatched repeat wrote %d times", n)
			}
			after, _ := storedTermination(t, store)
			if after.Revision != first.Revision {
				t.Fatalf("stored revision = %d, want %d", after.Revision, first.Revision)
			}
		})
	}
}

// TestRecordPlacementTerminationReplayOutlivesItsEvidence: the replay check
// runs BEFORE the release evidence is consulted, so a controller that restarts
// after a new owner registered still reads back the graceful outcome it
// committed rather than being told the release it recorded never happened.
func TestRecordPlacementTerminationReplayOutlivesItsEvidence(t *testing.T) {
	t.Parallel()

	store, _, _ := terminationFixture(t)
	mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
	if _, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryEpoch)); err != nil {
		t.Fatal(err)
	}
	first := mustRecordTermination(t, store, testGracefulRequest(1, registryEpoch))

	// A successor takes the session: the tombstone is gone.
	mustPutRegistration(t, store, testPutRegistrationRequest(registryNextEpoch))

	replay, created, err := store.RecordPlacementTermination(context.Background(), testGracefulRequest(1, registryEpoch))
	if err != nil || created || replay.Revision != first.Revision {
		t.Fatalf("replay after the evidence moved = %+v created %v err %v", replay, created, err)
	}
}

// TestRecordPlacementTerminationGracefulRequiresReleaseEvidence is "never
// report graceful release" made a store rule: graceful is admitted only when
// the registry holds a released tombstone AT the observed epoch.
func TestRecordPlacementTerminationGracefulRequiresReleaseEvidence(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		seed     func(t *testing.T, store *Store)
		observed uint64
		code     TerminationErrorCode
		field    string
		epoch    uint64
	}{
		{
			name:     "no registration at all",
			seed:     func(*testing.T, *Store) {},
			observed: registryEpoch,
			code:     TerminationErrorNotReleased, field: "route", epoch: 0,
		},
		{
			name: "a live route",
			seed: func(t *testing.T, store *Store) {
				mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
			},
			observed: registryEpoch,
			code:     TerminationErrorNotReleased, field: "route", epoch: registryEpoch,
		},
		{
			name: "an expired but unreleased route",
			seed: func(t *testing.T, store *Store) {
				mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
				store.clock.(*movableClock).set(registryLapsedAt)
			},
			observed: registryEpoch,
			code:     TerminationErrorNotReleased, field: "route", epoch: registryEpoch,
		},
		{
			name: "released below the observation",
			seed: func(t *testing.T, store *Store) {
				mustPutRegistration(t, store, testPutRegistrationRequest(registryStaleEpoch))
				if _, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryStaleEpoch)); err != nil {
					t.Fatal(err)
				}
			},
			observed: registryEpoch,
			code:     TerminationErrorEpoch, field: "observed_lease_epoch", epoch: registryStaleEpoch,
		},
		{
			name: "released above the observation",
			seed: func(t *testing.T, store *Store) {
				mustPutRegistration(t, store, testPutRegistrationRequest(registryNextEpoch))
				if _, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryNextEpoch)); err != nil {
					t.Fatal(err)
				}
			},
			observed: registryEpoch,
			code:     TerminationErrorEpoch, field: "observed_lease_epoch", epoch: registryNextEpoch,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _, audit := terminationFixture(t)
			tc.seed(t, store)
			audit.reset()
			_, _, err := store.RecordPlacementTermination(context.Background(), testGracefulRequest(1, tc.observed))
			got := assertTerminationCode(t, err, tc.code)
			if got.Field != tc.field || got.Epoch != tc.epoch {
				t.Fatalf("refusal = %+v, want field %q carrying epoch %d", got, tc.field, tc.epoch)
			}
			if n := audit.countOf("create") + audit.countOf("update"); n != 0 {
				t.Fatalf("a refused graceful wrote %d times", n)
			}
		})
	}

	t.Run("control: released at the observation", func(t *testing.T) {
		t.Parallel()
		store, clock, _ := terminationFixture(t)
		mustPutRegistration(t, store, testPutRegistrationRequest(registryEpoch))
		if _, err := store.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryEpoch)); err != nil {
			t.Fatal(err)
		}
		clock.set(terminationRecordedAt)
		entry, created, err := store.RecordPlacementTermination(context.Background(), testGracefulRequest(1, registryEpoch))
		if err != nil || !created {
			t.Fatalf("graceful over a released tombstone = created %v err %v", created, err)
		}
		want := PlacementTermination{
			TenantID: catalogTenant, SessionID: catalogSession, Generation: 1,
			Kind: PlacementTerminationGraceful, LeaseEpoch: registryEpoch,
			RecordedAt: terminationRecordedAt, Checkpoint: testRetainedCheckpoint(),
			Objects: []sessionwire.ObjectReference{testObjectReference(ObjectKindRuntimeCheckpoint, 2)},
		}
		assertSameTermination(t, entry.Termination, want)
	})
}

// TestRecordPlacementTerminationForcedEpochIsBoundedByTheRegistry: a forced
// outcome may record an observation at or below the registry's committed
// epoch — an older generation's Host terminated after a successor registered
// is ordinary — and never one above it, which would record an epoch no Host
// ever registered at.
func TestRecordPlacementTerminationForcedEpochIsBoundedByTheRegistry(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		seed     func(t *testing.T, store *Store)
		observed uint64
		ok       bool
		epoch    uint64
	}{
		{"no registration, nothing observed", func(*testing.T, *Store) {}, 0, true, 0},
		{"no registration, an epoch asserted", func(*testing.T, *Store) {}, 1, false, 0},
		{"live route, observed at it", func(t *testing.T, s *Store) {
			mustPutRegistration(t, s, testPutRegistrationRequest(registryEpoch))
		}, registryEpoch, true, 0},
		{"live route, observed below it", func(t *testing.T, s *Store) {
			mustPutRegistration(t, s, testPutRegistrationRequest(registryEpoch))
		}, registryStaleEpoch, true, 0},
		{"live route, observed above it", func(t *testing.T, s *Store) {
			mustPutRegistration(t, s, testPutRegistrationRequest(registryEpoch))
		}, registryEpoch + 1, false, registryEpoch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _, audit := terminationFixture(t)
			tc.seed(t, store)
			audit.reset()
			entry, _, err := store.RecordPlacementTermination(context.Background(), testForcedRequest(1, tc.observed))
			if tc.ok {
				if err != nil {
					t.Fatalf("forced observation %d refused: %v", tc.observed, err)
				}
				if entry.Termination.LeaseEpoch != tc.observed {
					t.Fatalf("recorded epoch = %d, want exactly %d", entry.Termination.LeaseEpoch, tc.observed)
				}
				return
			}
			got := assertTerminationCode(t, err, TerminationErrorEpoch)
			if got.Field != "observed_lease_epoch" || got.Epoch != tc.epoch {
				t.Fatalf("refusal = %+v, want observed_lease_epoch carrying %d", got, tc.epoch)
			}
			if n := audit.countOf("create") + audit.countOf("update"); n != 0 {
				t.Fatalf("a refused forced outcome wrote %d times", n)
			}
		})
	}
}

// TestRecordPlacementTerminationStampsTheStoreClock: RecordedAt is the
// store's instant, and the request has no member through which a caller could
// supply one.
func TestRecordPlacementTerminationStampsTheStoreClock(t *testing.T) {
	t.Parallel()

	store, clock, _ := terminationFixture(t)
	clock.set(terminationRecordedAt.In(time.FixedZone("y", -7200)))
	entry := mustRecordTermination(t, store, testForcedRequest(1, 0))
	if !entry.Termination.RecordedAt.Equal(terminationRecordedAt) || entry.Termination.RecordedAt.Location() != time.UTC {
		t.Fatalf("recorded at = %v, want exactly %v in UTC", entry.Termination.RecordedAt, terminationRecordedAt)
	}
}

// --- protocol neutrality and existence ----------------------------------------

// TestPlacementTerminationIsProtocolModeNeutral holds the write to the
// catalog-first rule the v0.10.0 registry follows: it serves a session under
// whichever mode its catalog binds and never proposes one of its own.
func TestPlacementTerminationIsProtocolModeNeutral(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		seed func(t *testing.T, store *Store)
		want ProtocolMode
	}{
		{"legacy catalog", func(t *testing.T, store *Store) {
			if _, _, err := store.CreateCatalogEntry(context.Background(), testCreateRequest()); err != nil {
				t.Fatal(err)
			}
		}, ProtocolModeLegacy},
		{"disposition catalog", createDispositionCatalog, ProtocolModeDisposition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := bareRegistryStore(t, memstore.New())
			tc.seed(t, store)
			scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
			if err != nil {
				t.Fatal(err)
			}
			want := encodeWitness(1, scope.sessionWitness, []byte(tc.want))
			mustRecordTermination(t, store, testForcedRequest(1, 0))
			if _, err := store.GetPlacementTermination(context.Background(), GetPlacementTerminationRequest{
				TenantID: catalogTenant, SessionID: catalogSession, Generation: 1,
			}); err != nil {
				t.Fatalf("GetPlacementTermination: %v", err)
			}
			if got, found := protocolWitnessOf(t, store); !found || !bytes.Equal(got, want) {
				t.Fatalf("protocol witness = %q (found %v), want %q", got, found, want)
			}
		})
	}
}

// TestPlacementTerminationRefusesASessionThatDoesNotExist: no refusal writes
// anything, and in particular none mints a protocol pin.
func TestPlacementTerminationRefusesASessionThatDoesNotExist(t *testing.T) {
	t.Parallel()

	t.Run("no durable data", func(t *testing.T) {
		t.Parallel()
		store := bareRegistryStore(t, memstore.New())
		_, _, err := store.RecordPlacementTermination(context.Background(), testForcedRequest(1, 0))
		var keyspace *KeyspaceError
		if !errors.As(err, &keyspace) || keyspace.Code != KeyspaceBindingNotFound {
			t.Fatalf("err = %v, want keyspace binding_not_found", err)
		}
		if _, found := protocolWitnessOf(t, store); found {
			t.Fatal("a refused termination minted a protocol pin")
		}
		createDispositionCatalog(t, store)
	})
	t.Run("witness without a catalog", func(t *testing.T) {
		t.Parallel()
		store := bareRegistryStore(t, memstore.New())
		scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.bindSessionScopeMode(context.Background(), scope, ProtocolModeDisposition); err != nil {
			t.Fatal(err)
		}
		_, _, err = store.RecordPlacementTermination(context.Background(), testForcedRequest(1, 0))
		assertCatalogCode(t, err, CatalogErrorNotFound)
		if _, found, err := store.readPlacementTermination(context.Background(), scope, catalogTenant, catalogSession); err != nil || found {
			t.Fatalf("after the refusal: found %v err %v", found, err)
		}
	})
	t.Run("catalog and witness disagree", func(t *testing.T) {
		t.Parallel()
		store := bareRegistryStore(t, memstore.New())
		createDispositionCatalog(t, store)
		scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
		if err != nil {
			t.Fatal(err)
		}
		key := scope.SessionNamespace + "/protocol"
		_, revision, err := store.keys.kv.Get(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.keys.kv.Put(context.Background(), key, revision, encodeWitness(1, scope.sessionWitness, []byte(ProtocolModeLegacy))); err != nil {
			t.Fatal(err)
		}
		_, _, err = store.RecordPlacementTermination(context.Background(), testForcedRequest(1, 0))
		if got := assertCatalogCode(t, err, CatalogErrorConflict); got.Field != "binding.protocol_mode" {
			t.Fatalf("field = %q, want binding.protocol_mode", got.Field)
		}
		if _, found, err := store.readPlacementTermination(context.Background(), scope, catalogTenant, catalogSession); err != nil || found {
			t.Fatalf("after the refusal: found %v err %v", found, err)
		}
	})
}

// --- the read path ------------------------------------------------------------

// TestGetPlacementTerminationAnswersByKey: found at exactly the stored
// generation; superseded below it; not_found above it or with no row. The
// two refusals carry the stored generation (zero meaning no row at all), and
// neither hands back a record, so a superseded generation is never reported
// with another generation's kind.
func TestGetPlacementTerminationAnswersByKey(t *testing.T) {
	t.Parallel()

	store, _, _ := terminationFixture(t)
	get := func(generation uint64) (PlacementTerminationEntry, error) {
		return store.GetPlacementTermination(context.Background(), GetPlacementTerminationRequest{
			TenantID: catalogTenant, SessionID: catalogSession, Generation: generation,
		})
	}

	_, err := get(1)
	if got := assertTerminationCode(t, err, TerminationErrorNotFound); got.Generation != 0 || got.Field != "record" {
		t.Fatalf("empty refusal = %+v, want record carrying generation 0", got)
	}
	_, err = get(0)
	if got := assertTerminationCode(t, err, TerminationErrorInvalid); got.Field != "generation" {
		t.Fatalf("zero generation refusal = %+v", got)
	}

	advanceDesiredGeneration(t, store, 2)
	recorded := mustRecordTermination(t, store, testForcedRequest(2, 0))

	entry, err := get(2)
	if err != nil {
		t.Fatalf("get(2): %v", err)
	}
	if entry.Revision != recorded.Revision {
		t.Fatalf("revision = %d, want %d", entry.Revision, recorded.Revision)
	}
	assertSameTermination(t, entry.Termination, recorded.Termination)

	entry, err = get(1)
	if got := assertTerminationCode(t, err, TerminationErrorSuperseded); got.Generation != 2 || got.Field != "generation" {
		t.Fatalf("lower refusal = %+v, want generation carrying 2", got)
	}
	if entry.Termination.Kind != "" || entry.Revision != 0 {
		t.Fatalf("a superseded read handed back %+v", entry)
	}
	entry, err = get(3)
	if got := assertTerminationCode(t, err, TerminationErrorNotFound); got.Generation != 2 || got.Field != "generation" {
		t.Fatalf("higher refusal = %+v, want generation carrying 2", got)
	}
	if entry.Termination.Kind != "" || entry.Revision != 0 {
		t.Fatalf("a not-found read handed back %+v", entry)
	}
}

// --- provider faults ----------------------------------------------------------

// TestRecordPlacementTerminationReportsALostRace: a create that loses to a
// concurrent writer, and an update whose revision moved, are conflicts that
// carry the current revision; a retry then meets the ordinary rules.
func TestRecordPlacementTerminationReportsALostRace(t *testing.T) {
	t.Parallel()

	t.Run("create", func(t *testing.T) {
		t.Parallel()
		store, _, audit := terminationFixture(t)
		scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
		if err != nil {
			t.Fatal(err)
		}
		racer := testPlacementTermination()
		racer.Generation, racer.LeaseEpoch, racer.ForcedReason = 1, 0, PlacementForcedDrainTimeout
		value, racer, err := encodePlacementTermination(racer)
		if err != nil {
			t.Fatal(err)
		}
		var once sync.Once
		audit.beforeCreate = func() {
			once.Do(func() {
				// The racer's row lands between this call's read and its
				// create, written beneath the instrumented index.
				if _, _, err := audit.OrderedIndex.Create(context.Background(), placementTerminationID(scope, catalogSession),
					scope.SessionNamespace, value, storage.Rank{}, placementTerminationDue(racer)); err != nil {
					t.Error(err)
				}
			})
		}
		req := testForcedRequest(1, 0)
		req.ForcedReason = PlacementForcedPlatformDeleted
		_, _, err = store.RecordPlacementTermination(context.Background(), req)
		got := assertTerminationCode(t, err, TerminationErrorConflict)
		if got.Field != "create" || got.Revision == 0 {
			t.Fatalf("refusal = %+v, want create carrying the winner's revision", got)
		}
		_, _, err = store.RecordPlacementTermination(context.Background(), req)
		assertTerminationCode(t, err, TerminationErrorMismatch)
	})
	t.Run("update", func(t *testing.T) {
		t.Parallel()
		store, _, audit := terminationFixture(t)
		advanceDesiredGeneration(t, store, 2)
		mustRecordTermination(t, store, testForcedRequest(1, 0))
		scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
		if err != nil {
			t.Fatal(err)
		}
		var once sync.Once
		audit.beforeUpdate = func() {
			once.Do(func() {
				current, err := audit.OrderedIndex.Get(context.Background(), placementTerminationID(scope, catalogSession))
				if err != nil {
					t.Error(err)
					return
				}
				racer := testPlacementTermination()
				racer.Generation, racer.LeaseEpoch = 3, 0
				value, racer, err := encodePlacementTermination(racer)
				if err != nil {
					t.Error(err)
					return
				}
				if _, err := audit.OrderedIndex.Update(context.Background(), placementTerminationID(scope, catalogSession),
					current.Revision, value, storage.Rank{}, placementTerminationDue(racer)); err != nil {
					t.Error(err)
				}
			})
		}
		_, _, err = store.RecordPlacementTermination(context.Background(), testForcedRequest(2, 0))
		got := assertTerminationCode(t, err, TerminationErrorConflict)
		if got.Field != "update" || got.Revision == 0 {
			t.Fatalf("refusal = %+v, want update carrying the current revision", got)
		}
		_, _, err = store.RecordPlacementTermination(context.Background(), testForcedRequest(2, 0))
		if got := assertTerminationCode(t, err, TerminationErrorSuperseded); got.Generation != 3 {
			t.Fatalf("retry = %+v, want superseded by 3", got)
		}
	})
}

// TestPlacementTerminationFilingIsHeldToTheRecord: a row the provider filed
// under another identity, or a provider tombstone, is refused rather than
// trusted or treated as absent.
func TestPlacementTerminationFilingIsHeldToTheRecord(t *testing.T) {
	t.Parallel()

	store, _, _ := terminationFixture(t)
	mustRecordTermination(t, store, testForcedRequest(1, 0))
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := store.backend.OrderedIndex.Get(context.Background(), placementTerminationID(scope, catalogSession))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := placementTerminationEntryFor(stored, scope, catalogTenant, catalogSession); err != nil {
		t.Fatalf("control: the conforming row was refused: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(storage.OrderedRecord) storage.OrderedRecord
		code   TerminationErrorCode
		field  string
	}{
		{"provider tombstone", func(r storage.OrderedRecord) storage.OrderedRecord { r.Deleted = true; return r }, TerminationErrorDeleted, "record"},
		{"stable key", func(r storage.OrderedRecord) storage.OrderedRecord { r.ID.StableKey = "session-b"; return r }, TerminationErrorIdentity, "session_id"},
		{"ordering scope", func(r storage.OrderedRecord) storage.OrderedRecord { r.ID.OrderingScope += "x"; return r }, TerminationErrorIdentity, ""},
		{"rank", func(r storage.OrderedRecord) storage.OrderedRecord {
			r.Rank = storage.Rank{Ranked: true, Value: 1}
			return r
		}, TerminationErrorIdentity, "rank"},
		{"another session's bytes", func(r storage.OrderedRecord) storage.OrderedRecord {
			r.Value = bytes.Replace(r.Value, []byte(`"session_id":"session-a"`), []byte(`"session_id":"session-b"`), 1)
			return r
		}, TerminationErrorIdentity, "record"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := placementTerminationEntryFor(tc.mutate(stored), scope, catalogTenant, catalogSession)
			got := assertTerminationCode(t, err, tc.code)
			if tc.field != "" && got.Field != tc.field {
				t.Fatalf("field = %q, want %q", got.Field, tc.field)
			}
		})
	}
}

// TestPlacementTerminationWriteVerifiesTheProviderReply: a create or update
// reply carrying bytes other than those written is an identity failure.
func TestPlacementTerminationWriteVerifiesTheProviderReply(t *testing.T) {
	t.Parallel()

	backend := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: backend.OrderedIndex}
	backend.OrderedIndex = hostile
	store := openStore(t, backend, WithClock(newMovableClock(registryObservedAt)))
	if _, _, err := store.CreateCatalogEntry(context.Background(), testCreateRequest()); err != nil {
		t.Fatal(err)
	}
	hostile.refileCreates(func(r storage.OrderedRecord) storage.OrderedRecord {
		if r.ID.Namespace != placementTerminationNamespace {
			return r
		}
		r.Value = bytes.Replace(r.Value, []byte(`"drain_timeout"`), []byte(`"platform_deleted"`), 1)
		return r
	})
	_, _, err := store.RecordPlacementTermination(context.Background(), testForcedRequest(1, 0))
	if got := assertTerminationCode(t, err, TerminationErrorIdentity); got.Field != "value" {
		t.Fatalf("field = %q, want value", got.Field)
	}
}

// TestPlacementTerminationClassifiesProviderFailures maps each ordered-index
// failure into this record's vocabulary and preserves the cause.
func TestPlacementTerminationClassifiesProviderFailures(t *testing.T) {
	t.Parallel()

	cause := errors.New("boom")
	cases := []struct {
		err  error
		code TerminationErrorCode
	}{
		{&storage.OrderedRecordNotFoundError{}, TerminationErrorNotFound},
		{&storage.OrderedDeletedError{}, TerminationErrorDeleted},
		{&storage.OrderedRevisionConflictError{ActualRevision: 9}, TerminationErrorConflict},
		{&storage.OrderedAmbiguousError{}, TerminationErrorUnknown},
		{cause, TerminationErrorBackend},
	}
	for _, tc := range cases {
		err := classifyTerminationOrderedError(tc.err, "update")
		got := assertTerminationCode(t, err, tc.code)
		if !errors.Is(err, tc.err) || got.Field != "update" {
			t.Fatalf("%T: cause or field lost: %+v", tc.err, got)
		}
		if tc.code == TerminationErrorConflict && got.Revision != 9 {
			t.Fatalf("conflict revision = %d, want 9", got.Revision)
		}
	}
}

// TestTerminationErrorRendersNoPayload keeps the message free of identities.
func TestTerminationErrorRendersNoPayload(t *testing.T) {
	t.Parallel()
	err := &TerminationError{Code: TerminationErrorEpoch, Field: "observed_lease_epoch", Epoch: 5, Cause: errors.New("tenant-a")}
	if got := err.Error(); got != "sessionstore: termination epoch (observed_lease_epoch)" {
		t.Fatalf("Error() = %q", got)
	}
	if got := (&TerminationError{Code: TerminationErrorBackend}).Error(); got != "sessionstore: termination backend" {
		t.Fatalf("Error() = %q", got)
	}
}

// sortObjectReferences puts references in the canonical order a termination
// stores them in. The record refuses any other order rather than sorting, so
// two callers cannot disagree about whether a reordered list is the same
// content.
func sortObjectReferences(objects []sessionwire.ObjectReference) {
	slices.SortFunc(objects, func(a, b sessionwire.ObjectReference) int { return strings.Compare(a.ObjectID, b.ObjectID) })
}

// TestDeletionDesireIsADesiredStateWriteNamingNoWorkload is the evidence for
// v0.11.0 adding NO deletion-desire state: a Factory already expresses "this
// dedicated session's workload should no longer exist" with an ordinary
// desired-state write that names no workload, the generation advances, and the
// intent a controller projects says exactly that — dedicated, zero workload —
// with a generation the termination record can then be keyed by.
func TestDeletionDesireIsADesiredStateWriteNamingNoWorkload(t *testing.T) {
	t.Parallel()

	store, _, _ := terminationFixture(t)
	update := func(key string, workload DesiredWorkload) CatalogEntry {
		t.Helper()
		entry, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: catalogSession})
		if err != nil {
			t.Fatal(err)
		}
		updated, err := store.UpdateCatalogDesiredState(context.Background(), UpdateCatalogDesiredStateRequest{
			TenantID: catalogTenant, SessionID: catalogSession, ExpectedRevision: entry.Revision,
			IdempotencyKey: key, DesiredPlacement: sessionwire.HostPlacementDedicated,
			RuntimeCompatibilityID: "runtime-v1", DesiredWorkload: workload,
		})
		if err != nil {
			t.Fatalf("UpdateCatalogDesiredState: %v", err)
		}
		return updated
	}
	wanted := update("want-workload", DesiredWorkload{PayloadVersion: "pod.v1", Payload: []byte("spec")})
	deleted := update("want-none", DesiredWorkload{})
	if deleted.Record.DesiredGeneration != wanted.Record.DesiredGeneration+1 {
		t.Fatalf("generation = %d, want exactly %d", deleted.Record.DesiredGeneration, wanted.Record.DesiredGeneration+1)
	}
	intent, err := deleted.Record.PlacementIntent()
	if err != nil {
		t.Fatal(err)
	}
	if intent.Placement != sessionwire.HostPlacementDedicated || intent.Workload.PayloadVersion != "" || intent.Workload.Payload != nil {
		t.Fatalf("intent = %+v, want dedicated with no workload", intent)
	}
	// The workload that generation superseded ends, and its outcome is recorded
	// under the generation that created it.
	entry := mustRecordTermination(t, store, testForcedRequest(wanted.Record.DesiredGeneration, 0))
	if entry.Termination.Generation != wanted.Record.DesiredGeneration {
		t.Fatalf("termination generation = %d", entry.Termination.Generation)
	}
}

// TestPlacementTerminationBoundsArePinned: the two exported bounds are API and
// durable-format limits, and the refusal tests use them symbolically — so a
// moved constant would move the tests with it. This pins the values.
func TestPlacementTerminationBoundsArePinned(t *testing.T) {
	t.Parallel()
	if MaxRetainedObjectReferences != 16 {
		t.Fatalf("MaxRetainedObjectReferences = %d, want exactly 16", MaxRetainedObjectReferences)
	}
	if MaxPlacementTerminationRecordBytes != 8<<10 {
		t.Fatalf("MaxPlacementTerminationRecordBytes = %d, want exactly %d", MaxPlacementTerminationRecordBytes, 8<<10)
	}
	if PlacementTerminationRecordVersion != 1 {
		t.Fatalf("PlacementTerminationRecordVersion = %d, want exactly 1", PlacementTerminationRecordVersion)
	}
}

// TestPlacementTerminationCheckpointRefusalKeepsTheParseCause: the single
// checkpoint-reference refusal carries the parse failure when there is one and
// no cause when the reference parsed as another kind.
func TestPlacementTerminationCheckpointRefusalKeepsTheParseCause(t *testing.T) {
	t.Parallel()
	record := testPlacementTermination()
	record.Checkpoint.Reference = sessionwire.ObjectReference{ObjectID: "not-an-object"}
	_, _, err := encodePlacementTermination(record)
	var object *ObjectError
	if got := assertTerminationCode(t, err, TerminationErrorInvalid); got.Field != "checkpoint.reference" || !errors.As(err, &object) {
		t.Fatalf("unparseable refusal = %+v, want checkpoint.reference caused by *ObjectError", got)
	}
	record.Checkpoint.Reference = testObjectReference(ObjectKindRuntimeCheckpoint, 1)
	_, _, err = encodePlacementTermination(record)
	if got := assertTerminationCode(t, err, TerminationErrorInvalid); got.Field != "checkpoint.reference" || got.Cause != nil {
		t.Fatalf("wrong-kind refusal = %+v, want checkpoint.reference with no cause", got)
	}
}
