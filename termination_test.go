package sessionstore

import (
	"bytes"
	"context"
	"errors"
	"go/ast"
	"go/token"
	"sort"
	"strconv"
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
// observation instant, so a record stamped from any other instant is caught.
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

// hostileTerminationFixture is terminationFixture over the package's one
// non-conforming provider, inert until a test arms it.
func hostileTerminationFixture(t *testing.T) (*Store, *hostileOrdered) {
	t.Helper()
	backend := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: backend.OrderedIndex}
	backend.OrderedIndex = hostile
	store := openStore(t, backend, WithClock(newMovableClock(registryObservedAt)))
	if _, _, err := store.CreateCatalogEntry(context.Background(), testCreateRequest()); err != nil {
		t.Fatal(err)
	}
	return store, hostile
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

func testForcedRequest(generation, observed uint64) RecordPlacementTerminationRequest {
	return RecordPlacementTerminationRequest{
		TenantID:           catalogTenant,
		SessionID:          catalogSession,
		Generation:         generation,
		Kind:               PlacementTerminationForced,
		ForcedReason:       PlacementForcedDrainTimeout,
		ObservedLeaseEpoch: observed,
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

func getTermination(store *Store, generation uint64) (PlacementTerminationEntry, error) {
	return store.GetPlacementTermination(context.Background(), GetPlacementTerminationRequest{
		TenantID: catalogTenant, SessionID: catalogSession, Generation: generation,
	})
}

// --- the stored record and its codec -------------------------------------------

func TestPlacementTerminationRoundTripsThroughStoredBytes(t *testing.T) {
	t.Parallel()

	for name, record := range map[string]PlacementTermination{
		"forced": testPlacementTermination(),
		"graceful": func() PlacementTermination {
			r := testPlacementTermination()
			r.Kind, r.ForcedReason = PlacementTerminationGraceful, ""
			return r
		}(),
		"forced with no Host observed": func() PlacementTermination {
			r := testPlacementTermination()
			r.ForcedReason, r.LeaseEpoch = PlacementForcedDrainRefused, 0
			return r
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			encoded, canonical, err := encodePlacementTermination(record)
			if err != nil {
				t.Fatalf("encodePlacementTermination: %v", err)
			}
			if canonical != record {
				t.Fatalf("canonical = %+v, want %+v", canonical, record)
			}
			decoded, err := decodePlacementTermination(encoded)
			if err != nil {
				t.Fatalf("decodePlacementTermination: %v", err)
			}
			if decoded != record {
				t.Fatalf("decoded = %+v, want %+v", decoded, record)
			}
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
// byte for byte, for each shape a record can take. It is spelled by the private
// DTO; the exported struct has no JSON tags and changing it cannot move these
// bytes. The epoch-zero row is what catches an omitempty added to lease_epoch.
func TestPlacementTerminationStoredBytesArePinned(t *testing.T) {
	t.Parallel()

	forced := testPlacementTermination()
	forced.RecordedAt = time.Date(2026, 9, 18, 9, 30, 0, 0, time.FixedZone("x", 3600))
	graceful := testPlacementTermination()
	graceful.Kind, graceful.ForcedReason = PlacementTerminationGraceful, ""
	unobserved := testPlacementTermination()
	unobserved.ForcedReason, unobserved.LeaseEpoch = PlacementForcedDrainRefused, 0

	for _, tc := range []struct {
		name   string
		record PlacementTermination
		want   string
	}{
		{"forced", forced, `{"record_version":1,"tenant_id":"tenant-a","session_id":"session-a","generation":3,` +
			`"kind":"forced","forced_reason":"platform_deleted","lease_epoch":5,"recorded_at":"2026-09-18T08:30:00Z"}`},
		{"graceful", graceful, `{"record_version":1,"tenant_id":"tenant-a","session_id":"session-a","generation":3,` +
			`"kind":"graceful","lease_epoch":5,"recorded_at":"2026-09-18T09:30:00Z"}`},
		{"forced, no Host observed", unobserved, `{"record_version":1,"tenant_id":"tenant-a","session_id":"session-a","generation":3,` +
			`"kind":"forced","forced_reason":"drain_refused","lease_epoch":0,"recorded_at":"2026-09-18T09:30:00Z"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			encoded, _, err := encodePlacementTermination(tc.record)
			if err != nil {
				t.Fatal(err)
			}
			if string(encoded) != tc.want {
				t.Fatalf("stored bytes =\n%s\nwant\n%s", encoded, tc.want)
			}
		})
	}
}

// TestPlacementTerminationDurableNamesArePinned pins every durable or
// consumer-pinned string by VALUE: the namespace a provider files the row
// under, the two enums' spellings (frozen at record version 1) and every code
// of the new vocabulary. A symbolic test would move with a renamed constant.
func TestPlacementTerminationDurableNamesArePinned(t *testing.T) {
	t.Parallel()

	if placementTerminationNamespace != "sessionstore/terminations" {
		t.Fatalf("namespace = %q", placementTerminationNamespace)
	}
	for got, want := range map[PlacementTerminationKind]string{
		PlacementTerminationGraceful: "graceful",
		PlacementTerminationForced:   "forced",
	} {
		if string(got) != want {
			t.Errorf("kind %q, want exactly %q", got, want)
		}
	}
	reasons := map[PlacementForcedReason]string{
		PlacementForcedDrainTimeout:       "drain_timeout",
		PlacementForcedDrainRefused:       "drain_refused",
		PlacementForcedPlatformDeleted:    "platform_deleted",
		PlacementForcedWorkloadTerminated: "workload_terminated",
	}
	for got, want := range reasons {
		if string(got) != want {
			t.Errorf("reason %q, want exactly %q", got, want)
		}
		if !got.known() {
			t.Errorf("reason %q is not known", got)
		}
	}
	if len(reasons) != 4 {
		t.Fatalf("%d distinct reasons, want 4", len(reasons))
	}
	codes := map[TerminationErrorCode]string{
		TerminationErrorInvalid:    "invalid",
		TerminationErrorNotFound:   "not_found",
		TerminationErrorUnissued:   "unissued",
		TerminationErrorSuperseded: "superseded",
		TerminationErrorMismatch:   "mismatch",
		TerminationErrorDeleted:    "deleted",
		TerminationErrorIdentity:   "identity",
		TerminationErrorConflict:   "conflict",
		TerminationErrorUnknown:    "unknown",
		TerminationErrorBackend:    "backend",
		TerminationErrorMalformed:  "malformed",
		TerminationErrorVersion:    "version",
		TerminationErrorTooLarge:   "too_large",
	}
	if len(codes) != 13 {
		t.Fatalf("%d distinct codes, want 13", len(codes))
	}
	for got, want := range codes {
		if string(got) != want {
			t.Errorf("code %q, want exactly %q", got, want)
		}
	}
	// And the declared set IS this set: a code added to errors.go without a
	// line here fails, so growing the vocabulary is never silent.
	declared := declaredStringConstantsOfType(t, "errors.go", "TerminationErrorCode")
	pinned := make([]string, 0, len(codes))
	for _, want := range codes {
		pinned = append(pinned, want)
	}
	sort.Strings(pinned)
	if strings.Join(declared, ",") != strings.Join(pinned, ",") {
		t.Fatalf("declared codes %v, pinned %v", declared, pinned)
	}
}

// TestPlacementTerminationBoundsArePinned: the exported bounds are API and
// durable-format limits, and the refusal tests use them symbolically — so a
// moved constant would move the tests with it.
func TestPlacementTerminationBoundsArePinned(t *testing.T) {
	t.Parallel()
	if MaxPlacementTerminationRecordBytes != 4<<10 {
		t.Fatalf("MaxPlacementTerminationRecordBytes = %d, want exactly %d", MaxPlacementTerminationRecordBytes, 4<<10)
	}
	if PlacementTerminationRecordVersion != 1 {
		t.Fatalf("PlacementTerminationRecordVersion = %d, want exactly 1", PlacementTerminationRecordVersion)
	}
}

// TestPlacementTerminationCanonicalFormRefuses holds every rule of the one
// canonical spelling. Each row asserts the exact code AND field.
func TestPlacementTerminationCanonicalFormRefuses(t *testing.T) {
	t.Parallel()

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
		{"a dropped v0.11 candidate member", bytes.Replace(encoded, []byte(`{"record_version":1,`), []byte(`{"record_version":1,"objects":[],`), 1), TerminationErrorMalformed, "record"},
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
// escapes six-fold, every number at its widest, the longest reason and the
// widest instant — and requires it to encode under the bound.
func TestLargestAcceptablePlacementTerminationFitsTheBound(t *testing.T) {
	t.Parallel()

	record := testPlacementTermination()
	record.TenantID = sessionwire.TenantID(strings.Repeat("\x01", sessionwire.MaxIDBytes))
	record.SessionID = sessionwire.SessionID(strings.Repeat("\x01", sessionwire.MaxIDBytes))
	record.Generation = ^uint64(0)
	record.LeaseEpoch = ^uint64(0)
	record.ForcedReason = PlacementForcedWorkloadTerminated
	record.RecordedAt = time.Date(2026, 12, 31, 23, 59, 59, 999999999, time.UTC)
	if err := record.TenantID.Validate(); err != nil {
		t.Fatalf("the worst-case tenant is not one the validators accept: %v", err)
	}
	if err := record.SessionID.Validate(); err != nil {
		t.Fatalf("the worst-case session is not one the validators accept: %v", err)
	}
	encoded, _, err := encodePlacementTermination(record)
	if err != nil {
		t.Fatalf("the largest acceptable termination was refused: %v", err)
	}
	t.Logf("largest acceptable termination: %d of %d bytes", len(encoded), MaxPlacementTerminationRecordBytes)
}

// --- the write path ----------------------------------------------------------

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
// has not minted.
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
		testGracefulRequest(1, registryEpoch),
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
	if !found || after.Revision != stored.Revision || after.Termination != stored.Termination {
		t.Fatalf("stored row moved: %+v found %v, want %+v", after, found, stored)
	}

	// Control: a HIGHER generation replaces it.
	next := mustRecordTermination(t, store, testForcedRequest(3, 0))
	if next.Termination.Generation != 3 || next.Revision <= stored.Revision {
		t.Fatalf("higher generation = %+v", next)
	}
}

// TestRecordPlacementTerminationIsCreateOnlyPerGeneration: a repeat with
// identical content is idempotent and writes nothing; a repeat with any
// different caller-authored member — in either direction — is a mismatch that
// writes nothing.
func TestRecordPlacementTerminationIsCreateOnlyPerGeneration(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(*RecordPlacementTerminationRequest)
		field  string
	}{
		{"kind", func(r *RecordPlacementTerminationRequest) { r.Kind, r.ForcedReason = PlacementTerminationGraceful, "" }, "kind"},
		{"forced reason", func(r *RecordPlacementTerminationRequest) { r.ForcedReason = PlacementForcedPlatformDeleted }, "forced_reason"},
		{"lower observed epoch", func(r *RecordPlacementTerminationRequest) { r.ObservedLeaseEpoch = registryStaleEpoch }, "lease_epoch"},
		{"higher observed epoch", func(r *RecordPlacementTerminationRequest) { r.ObservedLeaseEpoch = registryNextEpoch }, "lease_epoch"},
		{"no epoch observed", func(r *RecordPlacementTerminationRequest) { r.ObservedLeaseEpoch = 0 }, "lease_epoch"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, clock, audit := terminationFixture(t)
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
			if replay != first {
				t.Fatalf("replay = %+v, want exactly %+v", replay, first)
			}
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
			if after != first {
				t.Fatalf("stored = %+v, want exactly %+v", after, first)
			}
		})
	}
}

// TestRecordPlacementTerminationKindIsTheCallersAssertion pins the contract
// the ruling chose: the store records the kind and the observed epoch as
// given and consults the Host registry for NEITHER. A graceful outcome is
// accepted over no registration and over a live route; a forced one over an
// epoch above anything the registry holds; and a graceful one after a
// successor has registered — the honest case an evidence gate used to refuse.
func TestRecordPlacementTerminationKindIsTheCallersAssertion(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		seed func(t *testing.T, store *Store)
		req  RecordPlacementTerminationRequest
	}{
		{"graceful, no registration", func(*testing.T, *Store) {}, testGracefulRequest(1, registryEpoch)},
		{"graceful, live route at the epoch", func(t *testing.T, s *Store) {
			mustPutRegistration(t, s, testPutRegistrationRequest(registryEpoch))
		}, testGracefulRequest(1, registryEpoch)},
		{"graceful, successor registered above it", func(t *testing.T, s *Store) {
			mustPutRegistration(t, s, testPutRegistrationRequest(registryEpoch))
			if _, err := s.ClearHostRegistration(context.Background(), testClearRegistrationRequest(registryEpoch)); err != nil {
				t.Fatal(err)
			}
			mustPutRegistration(t, s, testPutRegistrationRequest(registryNextEpoch))
		}, testGracefulRequest(1, registryEpoch)},
		{"forced, epoch above the registry", func(t *testing.T, s *Store) {
			mustPutRegistration(t, s, testPutRegistrationRequest(registryEpoch))
		}, testForcedRequest(1, registryNextEpoch)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, clock, _ := terminationFixture(t)
			tc.seed(t, store)
			clock.set(terminationRecordedAt)
			entry, created, err := store.RecordPlacementTermination(context.Background(), tc.req)
			if err != nil || !created {
				t.Fatalf("created %v, err %v; want the assertion recorded", created, err)
			}
			want := PlacementTermination{
				TenantID: catalogTenant, SessionID: catalogSession, Generation: 1,
				Kind: tc.req.Kind, ForcedReason: tc.req.ForcedReason,
				LeaseEpoch: tc.req.ObservedLeaseEpoch, RecordedAt: terminationRecordedAt,
			}
			if entry.Termination != want {
				t.Fatalf("recorded %+v, want exactly %+v", entry.Termination, want)
			}
		})
	}
}

// TestRecordPlacementTerminationAcceptsAPooledSession pins the F10 ruling: a
// termination is accepted whatever placement the catalog desires NOW, because
// the store cannot know what a past generation desired, and moving to pooled
// is itself a spelling of "the dedicated workload should no longer exist".
func TestRecordPlacementTerminationAcceptsAPooledSession(t *testing.T) {
	t.Parallel()

	store, _, _ := terminationFixture(t)
	entry, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{TenantID: catalogTenant, SessionID: catalogSession})
	if err != nil {
		t.Fatal(err)
	}
	if entry.Record.DesiredPlacement != sessionwire.HostPlacementPooled {
		t.Fatalf("fixture placement = %q, want pooled", entry.Record.DesiredPlacement)
	}
	mustRecordTermination(t, store, testForcedRequest(1, 0))
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

// TestPlacementTerminationRowIsFiledNeverDueAndUnranked reads the committed
// row straight from the provider and pins its filing independently of
// placementTerminationDue, which the filing check itself reads.
func TestPlacementTerminationRowIsFiledNeverDueAndUnranked(t *testing.T) {
	t.Parallel()

	store, _, _ := terminationFixture(t)
	mustRecordTermination(t, store, testForcedRequest(1, 0))
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := store.backend.OrderedIndex.Get(context.Background(), storage.OrderedID{
		Namespace: "sessionstore/terminations", OrderingScope: scope.SessionNamespace, StableKey: storage.StableKey(catalogSession),
	})
	if err != nil {
		t.Fatalf("the row is not at its pinned identity: %v", err)
	}
	if stored.Due != (storage.Due{}) || stored.Rank != (storage.Rank{}) || stored.RankingScope != scope.SessionNamespace {
		t.Fatalf("filed due %+v rank %+v scope %q; want never due, unranked, session scope", stored.Due, stored.Rank, stored.RankingScope)
	}
}

// --- protocol neutrality, existence, lifecycle ---------------------------------

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
			if _, err := getTermination(store, 1); err != nil {
				t.Fatalf("GetPlacementTermination: %v", err)
			}
			if got, found := protocolWitnessOf(t, store); !found || !bytes.Equal(got, want) {
				t.Fatalf("protocol witness = %q (found %v), want %q", got, found, want)
			}
		})
	}
}

// TestPlacementTerminationRefusesASessionThatDoesNotExist: no refusal writes a
// termination row, and none mints a protocol pin.
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

// TestGetPlacementTerminationVerifiesTheSessionWitness: a read names a derived
// row, and a derived name is never trusted alone. With the session witness
// removed, Get is refused by the keyspace even though the row is readable.
func TestGetPlacementTerminationVerifiesTheSessionWitness(t *testing.T) {
	t.Parallel()

	store, _, _ := terminationFixture(t)
	mustRecordTermination(t, store, testForcedRequest(1, 0))
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.keys.kv.Delete(context.Background(), scope.sessionWitnessKey); err != nil {
		t.Fatalf("removing the witness: %v", err)
	}
	_, err = getTermination(store, 1)
	var keyspace *KeyspaceError
	if !errors.As(err, &keyspace) || keyspace.Code != KeyspaceBindingNotFound {
		t.Fatalf("err = %v, want keyspace binding_not_found", err)
	}
}

// TestPlacementTerminationOperationsRefuseAfterClose: both operations are
// admitted through the store's lifecycle and refuse once it is closing.
func TestPlacementTerminationOperationsRefuseAfterClose(t *testing.T) {
	t.Parallel()

	store, err := Open(context.Background(), memstore.New())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateCatalogEntry(context.Background(), testCreateRequest()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for name, op := range map[string]func() error{
		"Record": func() error {
			_, _, err := store.RecordPlacementTermination(context.Background(), testForcedRequest(1, 0))
			return err
		},
		"Get": func() error { _, err := getTermination(store, 1); return err },
	} {
		var closed *StoreClosedError
		if err := op(); !errors.As(err, &closed) {
			t.Fatalf("%s after Close = %v, want *StoreClosedError", name, err)
		}
	}
}

// --- the read path ------------------------------------------------------------

// TestGetPlacementTerminationAnswersByKey: found at exactly the stored
// generation; superseded below it; not_found above it or with no row. The
// two refusals carry the stored generation (zero meaning no row at all), and
// neither hands back a record.
func TestGetPlacementTerminationAnswersByKey(t *testing.T) {
	t.Parallel()

	store, _, _ := terminationFixture(t)

	_, err := getTermination(store, 1)
	if got := assertTerminationCode(t, err, TerminationErrorNotFound); got.Generation != 0 || got.Field != "record" {
		t.Fatalf("empty refusal = %+v, want record carrying generation 0", got)
	}
	_, err = getTermination(store, 0)
	if got := assertTerminationCode(t, err, TerminationErrorInvalid); got.Field != "generation" {
		t.Fatalf("zero generation refusal = %+v", got)
	}

	advanceDesiredGeneration(t, store, 2)
	recorded := mustRecordTermination(t, store, testForcedRequest(2, 0))

	entry, err := getTermination(store, 2)
	if err != nil {
		t.Fatalf("get(2): %v", err)
	}
	if entry != recorded {
		t.Fatalf("get(2) = %+v, want exactly %+v", entry, recorded)
	}

	entry, err = getTermination(store, 1)
	if got := assertTerminationCode(t, err, TerminationErrorSuperseded); got.Generation != 2 || got.Field != "generation" {
		t.Fatalf("lower refusal = %+v, want generation carrying 2", got)
	}
	if entry != (PlacementTerminationEntry{}) {
		t.Fatalf("a superseded read handed back %+v", entry)
	}
	entry, err = getTermination(store, 3)
	if got := assertTerminationCode(t, err, TerminationErrorNotFound); got.Generation != 2 || got.Field != "generation" {
		t.Fatalf("higher refusal = %+v, want generation carrying 2", got)
	}
	if entry != (PlacementTerminationEntry{}) {
		t.Fatalf("a not-found read handed back %+v", entry)
	}
}

// TestPlacementTerminationCorruptRowFailsClosed: a row this reader cannot
// decode is a high-water it cannot evaluate. Get reports the decode failure
// rather than "nothing recorded", and Record refuses rather than creating
// straight over it.
func TestPlacementTerminationCorruptRowFailsClosed(t *testing.T) {
	t.Parallel()

	store, hostile := hostileTerminationFixture(t)
	mustRecordTermination(t, store, testForcedRequest(1, 0))
	hostile.corruptGetsIn(placementTerminationNamespace, func([]byte) []byte { return []byte("{") })

	_, err := getTermination(store, 1)
	if got := assertTerminationCode(t, err, TerminationErrorMalformed); got.Field != "record" {
		t.Fatalf("Get over a corrupt row = %+v, want malformed record", got)
	}
	_, _, err = store.RecordPlacementTermination(context.Background(), testForcedRequest(1, 0))
	if got := assertTerminationCode(t, err, TerminationErrorMalformed); got.Field != "record" {
		t.Fatalf("Record over a corrupt row = %+v, want malformed record", got)
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
		{"ordering scope", func(r storage.OrderedRecord) storage.OrderedRecord { r.ID.OrderingScope += "x"; return r }, TerminationErrorIdentity, "ordering_scope"},
		{"due", func(r storage.OrderedRecord) storage.OrderedRecord {
			r.Due = storage.Due{State: storage.DueAt, UnixMillis: 1}
			return r
		}, TerminationErrorIdentity, "due"},
		{"rank", func(r storage.OrderedRecord) storage.OrderedRecord {
			r.Rank = storage.Rank{Ranked: true, Value: 1}
			return r
		}, TerminationErrorIdentity, "rank"},
		{"another session's bytes", func(r storage.OrderedRecord) storage.OrderedRecord {
			r.Value = bytes.Replace(r.Value, []byte(`"session_id":"session-a"`), []byte(`"session_id":"session-b"`), 1)
			return r
		}, TerminationErrorIdentity, "record"},
		{"another tenant's bytes", func(r storage.OrderedRecord) storage.OrderedRecord {
			r.Value = bytes.Replace(r.Value, []byte(`"tenant_id":"tenant-a"`), []byte(`"tenant_id":"tenant-b"`), 1)
			return r
		}, TerminationErrorIdentity, "record"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := placementTerminationEntryFor(tc.mutate(stored), scope, catalogTenant, catalogSession)
			if got := assertTerminationCode(t, err, tc.code); got.Field != tc.field {
				t.Fatalf("field = %q, want %q", got.Field, tc.field)
			}
		})
	}
}

// TestPlacementTerminationWriteVerifiesTheProviderReply: a create or update
// reply carrying bytes other than those written, or filed elsewhere, is an
// identity failure on both write paths.
func TestPlacementTerminationWriteVerifiesTheProviderReply(t *testing.T) {
	t.Parallel()

	swapReason := func(r storage.OrderedRecord) storage.OrderedRecord {
		if r.ID.Namespace == placementTerminationNamespace {
			r.Value = bytes.Replace(r.Value, []byte(`"drain_timeout"`), []byte(`"platform_deleted"`), 1)
		}
		return r
	}
	misfile := func(r storage.OrderedRecord) storage.OrderedRecord {
		if r.ID.Namespace == placementTerminationNamespace {
			r.Rank = storage.Rank{Ranked: true, Value: 1}
		}
		return r
	}
	cases := []struct {
		name  string
		arm   func(*hostileOrdered)
		prime bool
		field string
	}{
		{"create bytes", func(h *hostileOrdered) { h.refileCreates(swapReason) }, false, "value"},
		{"create filing", func(h *hostileOrdered) { h.refileCreates(misfile) }, false, "rank"},
		{"update bytes", func(h *hostileOrdered) { h.refileUpdates(swapReason) }, true, "value"},
		{"update filing", func(h *hostileOrdered) { h.refileUpdates(misfile) }, true, "rank"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, hostile := hostileTerminationFixture(t)
			generation := uint64(1)
			if tc.prime {
				advanceDesiredGeneration(t, store, 1)
				mustRecordTermination(t, store, testForcedRequest(1, 0))
				generation = 2
			}
			tc.arm(hostile)
			_, _, err := store.RecordPlacementTermination(context.Background(), testForcedRequest(generation, 0))
			if got := assertTerminationCode(t, err, TerminationErrorIdentity); got.Field != tc.field {
				t.Fatalf("field = %q, want %q", got.Field, tc.field)
			}
		})
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

// TestPlacementTerminationProviderReadFailureReachesTheCaller drives the
// backend arm end to end: a failing read is backend, carrying its cause, from
// both operations.
func TestPlacementTerminationProviderReadFailureReachesTheCaller(t *testing.T) {
	t.Parallel()

	store, hostile := hostileTerminationFixture(t)
	cause := errors.New("provider down")
	hostile.failGetsIn(placementTerminationNamespace, cause)
	_, err := getTermination(store, 1)
	if got := assertTerminationCode(t, err, TerminationErrorBackend); got.Field != "get" || !errors.Is(err, cause) {
		t.Fatalf("Get = %+v", got)
	}
	_, _, err = store.RecordPlacementTermination(context.Background(), testForcedRequest(1, 0))
	if got := assertTerminationCode(t, err, TerminationErrorBackend); got.Field != "get" || !errors.Is(err, cause) {
		t.Fatalf("Record = %+v", got)
	}
}

// TestTerminationErrorRendersNoPayload keeps the message free of identities.
func TestTerminationErrorRendersNoPayload(t *testing.T) {
	t.Parallel()
	err := &TerminationError{Code: TerminationErrorSuperseded, Field: "generation", Generation: 5, Cause: errors.New("tenant-a")}
	if got := err.Error(); got != "sessionstore: termination superseded (generation)" {
		t.Fatalf("Error() = %q", got)
	}
	if got := (&TerminationError{Code: TerminationErrorBackend}).Error(); got != "sessionstore: termination backend" {
		t.Fatalf("Error() = %q", got)
	}
}

// TestDeletionDesireIsADesiredStateWriteNamingNoWorkload is the evidence for
// v0.11.0 adding NO deletion-desire state: a Factory already expresses "this
// dedicated session's workload should no longer exist" with an ordinary
// desired-state write that names no workload, the generation advances, and the
// intent a controller projects says exactly that — dedicated, zero workload.
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
	entry := mustRecordTermination(t, store, testForcedRequest(wanted.Record.DesiredGeneration, 0))
	if entry.Termination.Generation != wanted.Record.DesiredGeneration {
		t.Fatalf("termination generation = %d", entry.Termination.Generation)
	}
}

// declaredStringConstantsOfType returns, sorted, the string values of every
// constant a production file declares with the named type.
func declaredStringConstantsOfType(t *testing.T, filename, typeName string) []string {
	t.Helper()
	var values []string
	for _, declaration := range parseProductionFile(t, filename).Decls {
		generic, ok := declaration.(*ast.GenDecl)
		if !ok || generic.Tok != token.CONST {
			continue
		}
		for _, spec := range generic.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok || len(value.Values) != 1 {
				continue
			}
			ident, ok := value.Type.(*ast.Ident)
			if !ok || ident.Name != typeName {
				continue
			}
			literal, ok := value.Values[0].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				t.Fatalf("%s constant %s is not a string literal", typeName, value.Names[0].Name)
			}
			text, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatal(err)
			}
			values = append(values, text)
		}
	}
	if len(values) == 0 {
		t.Fatalf("vacuous: %s declares no %s constants", filename, typeName)
	}
	sort.Strings(values)
	return values
}
