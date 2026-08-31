package sessionstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// --- fixtures and assertions ------------------------------------------------

// The timeline every case in this file places itself on. A Factory replica
// claims the session at 09:00 for thirty seconds; 09:00:30 is the instant the
// claim lapses and 09:01 is well past it.
var (
	reconcileClaimedAt = time.Date(2026, 8, 31, 9, 0, 0, 0, time.UTC)
	reconcileExpiresAt = reconcileClaimedAt.Add(30 * time.Second)
	reconcileLapsedAt  = reconcileClaimedAt.Add(time.Minute)
)

const (
	reconcileHolder      = "factory-a"
	reconcileOtherHolder = "factory-b"
)

func testReconciliationClaim() ReconciliationClaim {
	return ReconciliationClaim{
		TenantID:  catalogTenant,
		SessionID: catalogSession,
		HolderID:  reconcileHolder,
		ClaimedAt: reconcileClaimedAt,
		ExpiresAt: reconcileExpiresAt,
	}
}

func reconcileFixture(t *testing.T, backend *storage.Composite) (*Store, *movableClock) {
	t.Helper()
	clock := newMovableClock(reconcileClaimedAt)
	return openStore(t, backend, WithClock(clock)), clock
}

func testAcquireRequest(holder string) AcquireReconciliationClaimRequest {
	return AcquireReconciliationClaimRequest{
		TenantID:  catalogTenant,
		SessionID: catalogSession,
		HolderID:  holder,
		ExpiresAt: reconcileExpiresAt,
	}
}

func testReleaseRequest(holder string) ReleaseReconciliationClaimRequest {
	return ReleaseReconciliationClaimRequest{
		TenantID:  catalogTenant,
		SessionID: catalogSession,
		HolderID:  holder,
	}
}

func testGetClaimRequest() GetReconciliationClaimRequest {
	return GetReconciliationClaimRequest{TenantID: catalogTenant, SessionID: catalogSession}
}

func mustAcquireClaim(t *testing.T, store *Store, req AcquireReconciliationClaimRequest) ReconciliationClaimEntry {
	t.Helper()
	entry, err := store.AcquireReconciliationClaim(context.Background(), req)
	if err != nil {
		t.Fatalf("AcquireReconciliationClaim: %v", err)
	}
	return entry
}

func assertReconcileCode(t *testing.T, err error, want ReconcileErrorCode) *ReconcileError {
	t.Helper()
	var got *ReconcileError
	if !errors.As(err, &got) {
		t.Fatalf("error = %T %v, want *ReconcileError", err, err)
	}
	if got.Code != want {
		t.Fatalf("reconcile code = %q, want %q (%v)", got.Code, want, err)
	}
	return got
}

func assertReconcileField(code ReconcileErrorCode, field string) func(*testing.T, error) {
	return func(t *testing.T, err error) {
		t.Helper()
		got := assertReconcileCode(t, err, code)
		if got.Field != field {
			t.Fatalf("field = %q, want %q (%v)", got.Field, field, err)
		}
	}
}

// assertClaimUnchanged fails unless the session's stored claim is byte-for-byte
// the one want names. It is this record's counterpart of assertCatalogUnchanged,
// and every rejection case on a write path uses it: a guard that refuses AFTER
// writing passes a test that only inspects the returned error.
func assertClaimUnchanged(t *testing.T, store *Store, want ReconciliationClaimEntry) {
	t.Helper()
	scope, err := store.deriveSessionScope(want.Claim.TenantID, want.Claim.SessionID)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	stored, err := store.backend.OrderedIndex.Get(
		context.Background(), reconciliationClaimID(scope, want.Claim.SessionID))
	if err != nil {
		t.Fatalf("a rejected write corrupted the stored claim: %v", err)
	}
	if stored.Revision != want.Revision {
		t.Fatalf("a rejected write advanced the revision %d -> %d", want.Revision, stored.Revision)
	}
	wantBytes, _, err := encodeReconciliationClaim(want.Claim)
	if err != nil {
		t.Fatalf("encode expected claim: %v", err)
	}
	if !bytes.Equal(stored.Value, wantBytes) {
		t.Fatalf("a rejected write changed the claim:\nwant %s\ngot  %s", wantBytes, stored.Value)
	}
}

// --- the stored record's codec ----------------------------------------------

func TestReconciliationClaimRoundTripsThroughStoredBytes(t *testing.T) {
	t.Parallel()

	claim := testReconciliationClaim()
	encoded, _, err := encodeReconciliationClaim(claim)
	if err != nil {
		t.Fatalf("encodeReconciliationClaim: %v", err)
	}
	decoded, err := decodeReconciliationClaim(encoded)
	if err != nil {
		t.Fatalf("decodeReconciliationClaim: %v", err)
	}
	if decoded != claim {
		t.Fatalf("claim = %+v, want %+v", decoded, claim)
	}
	reencoded, _, err := encodeReconciliationClaim(decoded)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if !bytes.Equal(encoded, reencoded) {
		t.Fatalf("round trip changed the canonical bytes:\n%s\n%s", encoded, reencoded)
	}
}

func TestDecodeReconciliationClaimFailsClosed(t *testing.T) {
	t.Parallel()

	valid, _, err := encodeReconciliationClaim(testReconciliationClaim())
	if err != nil {
		t.Fatalf("encodeReconciliationClaim: %v", err)
	}
	// A substitution that did not substitute would present the VALID document
	// and the case would pass by decoding it successfully, which is the
	// opposite of what it claims. Every perturbation goes through here.
	perturb := func(old, new string) []byte {
		out := strings.Replace(string(valid), old, new, 1)
		if out == string(valid) {
			t.Fatalf("the fixture does not contain %q: %s", old, valid)
		}
		return []byte(out)
	}
	tests := []struct {
		name  string
		value []byte
		code  ReconcileErrorCode
	}{
		{"empty", nil, ReconcileErrorMalformed},
		{"not JSON", []byte("{"), ReconcileErrorMalformed},
		{"trailing content", append(bytes.Clone(valid), '{'), ReconcileErrorMalformed},
		{"an undeclared member", perturb(`"record_version":1`, `"record_version":1,"surprise":1`), ReconcileErrorMalformed},
		{"a future record version", perturb(`"record_version":1`, `"record_version":2`), ReconcileErrorVersion},
		{"no holder", perturb(`"holder_id":"`+reconcileHolder+`"`, `"holder_id":""`), ReconcileErrorInvalid},
		{"an expiry before the claim", perturb(
			`"expires_at":"2026-08-31T09:00:30Z"`, `"expires_at":"2026-08-31T08:59:00Z"`), ReconcileErrorInvalid},
		{"oversized", bytes.Repeat([]byte("x"), MaxReconciliationClaimRecordBytes+1), ReconcileErrorTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := decodeReconciliationClaim(test.value); err == nil {
				t.Fatalf("a claim that should not decode did: %s", test.value)
			} else {
				assertReconcileCode(t, err, test.code)
			}
		})
	}
}

func TestReconciliationClaimCanonicalizesItsInstantsToUTC(t *testing.T) {
	t.Parallel()

	zone := time.FixedZone("elsewhere", 5*3600)
	claim := testReconciliationClaim()
	claim.ClaimedAt = claim.ClaimedAt.In(zone)
	claim.ExpiresAt = claim.ExpiresAt.In(zone)
	_, canonical, err := encodeReconciliationClaim(claim)
	if err != nil {
		t.Fatalf("encodeReconciliationClaim: %v", err)
	}
	if canonical.ClaimedAt.Location() != time.UTC || canonical.ExpiresAt.Location() != time.UTC {
		t.Fatalf("instants were not canonicalized: %+v", canonical)
	}
	if !canonical.ClaimedAt.Equal(claim.ClaimedAt) || !canonical.ExpiresAt.Equal(claim.ExpiresAt) {
		t.Fatalf("canonicalization moved an instant: %+v", canonical)
	}
}

// TestLargestAcceptableReconciliationClaimFitsTheBound builds the worst case
// both validators accept and reports what it measures. An identity is any valid
// UTF-8 of at most MaxIDBytes bytes, control characters included, and Go escapes
// each of those as six bytes — so a bound measured with ASCII would be a sixth
// of what it claimed to be.
func TestLargestAcceptableReconciliationClaimFitsTheBound(t *testing.T) {
	t.Parallel()

	claim := ReconciliationClaim{
		TenantID:  sessionwire.TenantID(worstCaseIdentity()),
		SessionID: sessionwire.SessionID(worstCaseIdentity()),
		HolderID:  worstCaseIdentity(),
		ClaimedAt: reconcileClaimedAt,
		ExpiresAt: reconcileExpiresAt,
	}
	encoded, _, err := encodeReconciliationClaim(claim)
	if err != nil {
		t.Fatalf("the largest acceptable claim was refused: %v", err)
	}
	t.Logf("largest acceptable claim: %d bytes against a %d bound",
		len(encoded), MaxReconciliationClaimRecordBytes)
	if len(encoded) > MaxReconciliationClaimRecordBytes {
		t.Fatalf("it does not fit: %d > %d", len(encoded), MaxReconciliationClaimRecordBytes)
	}
}

// --- a claim is never authority ---------------------------------------------

// TestReconciliationClaimCannotSpellSessionOwnership is the structural half of
// "a claim suppresses duplicate scaling but is never the session ownership
// fence". The Host lease is the fence; a claim that could NAME an epoch, a
// Host, or a route would be one refactor away from being read as one.
func TestReconciliationClaimCannotSpellSessionOwnership(t *testing.T) {
	t.Parallel()

	forbidden := []string{"Epoch", "HostID", "Endpoint", "Residency", "Accepting", "JournalSeq", "Route", "Generation"}
	fields := 0
	inspect := func(filename string, include func(string) bool) {
		file, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", filename, err)
		}
		for _, declaration := range file.Decls {
			generic, ok := declaration.(*ast.GenDecl)
			if !ok || generic.Tok != token.TYPE {
				continue
			}
			for _, spec := range generic.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if !ok || !include(typeSpec.Name.Name) {
					continue
				}
				structType, ok := typeSpec.Type.(*ast.StructType)
				if !ok {
					continue
				}
				for _, field := range structType.Fields.List {
					fields++
					var rendered bytes.Buffer
					if err := printer.Fprint(&rendered, token.NewFileSet(), field.Type); err != nil {
						t.Fatalf("render %s: %v", typeSpec.Name.Name, err)
					}
					spelling := rendered.String()
					for _, name := range field.Names {
						spelling += " " + name.Name
					}
					for _, word := range forbidden {
						if strings.Contains(spelling, word) {
							t.Errorf("%s.%s names %s; a claim suppresses duplicate work and is never a fence",
								typeSpec.Name.Name, spelling, word)
						}
					}
				}
			}
		}
	}
	inspect("reconcile.go", func(string) bool { return true })
	inspect("errors.go", func(name string) bool { return strings.HasPrefix(name, "Reconcile") })
	if fields < 12 {
		t.Fatalf("only %d fields were inspected; the walk is not reaching the declarations", fields)
	}

	// The holder is deliberately NOT a sessionwire identity type. A HostID here
	// would make "the Host holding the lease" and "the replica doing the
	// scaling" the same kind of thing to a reader, which is exactly the
	// confusion this record must not invite.
	holder, ok := reflect.TypeOf(ReconciliationClaim{}).FieldByName("HolderID")
	if !ok {
		t.Fatal("ReconciliationClaim has no HolderID")
	}
	if holder.Type.Kind() != reflect.String || holder.Type.PkgPath() != "" {
		t.Fatalf("HolderID is %s; it must be a plain string, not an identity of the session domain", holder.Type)
	}
}

// TestNothingInThisPackageReadsAClaimToDecideAWrite is the other structural
// half, and the stronger one. The rule is not "a claim must not be used as a
// fence" — it is that NO operation in this package consults a claim at all, so
// there is nothing a claim can license. A later task that wires one into a
// write path fails here and has to argue for it.
func TestNothingInThisPackageReadsAClaimToDecideAWrite(t *testing.T) {
	t.Parallel()

	// Every identifier that names this record, its state, or its physical home.
	// A production file that USES any of them is consulting a claim.
	claimIdentifiers := []string{
		"ReconciliationClaim", "reconciliationClaim", "reconcileNamespace", "claimHeldAt",
	}
	// Identifiers are read from the parsed syntax rather than from the file's
	// text, so a doc comment naming an operation is not mistaken for a call to
	// it — errors.go documents this record's vocabulary and must be free to say
	// its name.
	used := func(filename string) map[string]bool {
		file, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", filename, err)
		}
		names := map[string]bool{}
		ast.Inspect(file, func(node ast.Node) bool {
			if ident, ok := node.(*ast.Ident); ok {
				for _, identifier := range claimIdentifiers {
					if strings.HasPrefix(ident.Name, identifier) {
						names[identifier] = true
					}
				}
			}
			return true
		})
		return names
	}

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	inspected := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") || name == "reconcile.go" {
			continue
		}
		inspected++
		for identifier := range used(name) {
			t.Errorf("%s uses %s; nothing outside reconcile.go may consult a claim, because a claim licenses nothing",
				name, identifier)
		}
	}
	if inspected < 8 {
		t.Fatalf("only %d production files were inspected; the walk is not reaching them", inspected)
	}

	// Anti-vacuity: the identifiers must actually be found where they are known
	// to be, or the sweep above would pass against a typo in this list.
	if found := used("reconcile.go"); len(found) != len(claimIdentifiers) {
		t.Fatalf("reconcile.go uses %v of %v; the sweep above is looking for the wrong names",
			found, claimIdentifiers)
	}
}

// TestReconcileErrorCarriesNoEpoch pins the same rule on the type a caller
// actually handles: there is no lease epoch in this record, so there is none to
// read out of its failures either.
func TestReconcileErrorCarriesNoEpoch(t *testing.T) {
	t.Parallel()

	errorType := reflect.TypeOf(ReconcileError{})
	for i := range errorType.NumField() {
		if name := strings.ToLower(errorType.Field(i).Name); strings.Contains(name, "epoch") || strings.Contains(name, "lease") {
			t.Fatalf("ReconcileError.%s lets a caller read ownership out of a claim failure", errorType.Field(i).Name)
		}
	}
}

// --- acquiring, renewing, and taking over -----------------------------------

func TestAcquireReconciliationClaimCreatesThenRenews(t *testing.T) {
	store, clock := reconcileFixture(t, memstore.New())

	first := mustAcquireClaim(t, store, testAcquireRequest(reconcileHolder))
	if first.Claim.HolderID != reconcileHolder {
		t.Fatalf("holder = %q, want %q", first.Claim.HolderID, reconcileHolder)
	}
	if !first.Claim.ClaimedAt.Equal(reconcileClaimedAt) || !first.Claim.ExpiresAt.Equal(reconcileExpiresAt) {
		t.Fatalf("claim = %+v", first.Claim)
	}

	// The same replica extends its own live claim in one compare-and-swap.
	clock.set(reconcileClaimedAt.Add(10 * time.Second))
	renewal := testAcquireRequest(reconcileHolder)
	renewal.ExpiresAt = reconcileClaimedAt.Add(40 * time.Second)
	renewed := mustAcquireClaim(t, store, renewal)
	if renewed.Revision == first.Revision {
		t.Fatal("a renewal did not write")
	}
	if !renewed.Claim.ExpiresAt.Equal(renewal.ExpiresAt) {
		t.Fatalf("renewed expiry = %v, want %v", renewed.Claim.ExpiresAt, renewal.ExpiresAt)
	}
	if !renewed.Claim.ClaimedAt.Equal(reconcileClaimedAt.Add(10 * time.Second)) {
		t.Fatalf("a renewal did not restamp the claim instant: %v", renewed.Claim.ClaimedAt)
	}
}

// TestAcquireRefusesALiveClaimOfAnotherHolder is the duplicate-suppression the
// whole record exists for, and it asserts the refusal WRITES NOTHING: a losing
// replica that rewrote the row would restamp the winner's claim under its own
// name, which is the one way this record could take work away from the replica
// actually doing it.
func TestAcquireRefusesALiveClaimOfAnotherHolder(t *testing.T) {
	base := memstore.New()
	recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = recorder
	store, _ := reconcileFixture(t, base)

	held := mustAcquireClaim(t, store, testAcquireRequest(reconcileHolder))
	recorder.reset()

	_, err := store.AcquireReconciliationClaim(context.Background(), testAcquireRequest(reconcileOtherHolder))
	got := assertReconcileCode(t, err, ReconcileErrorHeld)
	if !got.ExpiresAt.Equal(reconcileExpiresAt) {
		t.Fatalf("the refusal did not report the backoff horizon: %v", got.ExpiresAt)
	}
	// The provider call SEQUENCE is the assertion, not merely the outcome: a
	// refusal that reached Update and lost a compare-and-swap would look
	// identical from the outside while having raced the winner's row.
	var sequence []string
	for _, call := range recorder.snapshot() {
		sequence = append(sequence, call.op)
	}
	// Exactly one read and nothing else. The positive half matters as much as
	// the negative one: an assertion that only counted writes would pass just
	// as well against a recorder that recorded nothing at all.
	if len(sequence) != 1 || sequence[0] != "get" {
		t.Fatalf("provider calls = %v, want exactly one read", sequence)
	}
	// Read the stored row only AFTER the sequence has been captured; this
	// assertion is itself a provider read.
	assertClaimUnchanged(t, store, held)
}

func TestALapsedClaimIsTakenOverAfterACrash(t *testing.T) {
	store, clock := reconcileFixture(t, memstore.New())
	crashed := mustAcquireClaim(t, store, testAcquireRequest(reconcileHolder))

	clock.set(reconcileLapsedAt)
	takeover := testAcquireRequest(reconcileOtherHolder)
	takeover.ExpiresAt = reconcileLapsedAt.Add(30 * time.Second)
	taken := mustAcquireClaim(t, store, takeover)
	if taken.Claim.HolderID != reconcileOtherHolder {
		t.Fatalf("holder = %q, want %q", taken.Claim.HolderID, reconcileOtherHolder)
	}
	if taken.Revision == crashed.Revision {
		t.Fatal("a takeover did not write")
	}

	// And the crashed replica, coming back with a stale idea of the world, is
	// refused rather than allowed to resume: the claim is now someone else's.
	resume := testAcquireRequest(reconcileHolder)
	resume.ExpiresAt = reconcileLapsedAt.Add(10 * time.Second)
	if _, err := store.AcquireReconciliationClaim(context.Background(), resume); err == nil {
		t.Fatal("a crashed replica resumed a claim that had been taken over")
	} else {
		assertReconcileCode(t, err, ReconcileErrorHeld)
	}
	assertClaimUnchanged(t, store, taken)
}

// TestReconciliationClaimIsHalfOpenAtItsExpiry pins the convention every
// deadline in this package uses: a claim holds up to but not including its
// expiry.
func TestReconciliationClaimIsHalfOpenAtItsExpiry(t *testing.T) {
	store, clock := reconcileFixture(t, memstore.New())
	mustAcquireClaim(t, store, testAcquireRequest(reconcileHolder))

	clock.set(reconcileExpiresAt.Add(-time.Nanosecond))
	if _, err := store.AcquireReconciliationClaim(context.Background(), testAcquireRequest(reconcileOtherHolder)); err == nil {
		t.Fatal("a claim one nanosecond before its expiry was taken over")
	} else {
		assertReconcileCode(t, err, ReconcileErrorHeld)
	}

	clock.set(reconcileExpiresAt)
	takeover := testAcquireRequest(reconcileOtherHolder)
	takeover.ExpiresAt = reconcileExpiresAt.Add(30 * time.Second)
	if _, err := store.AcquireReconciliationClaim(context.Background(), takeover); err != nil {
		t.Fatalf("a claim exactly at its expiry was not takeable: %v", err)
	}
}

// TestAcquireBoundsTheClaimTTL bounds the one caller-supplied horizon on this
// record. An unbounded claim is a durable liveness fault a single replica with
// a skewed clock can commit alone: nothing removes it, and every other replica
// declines to reconcile the session for as long as it lasts.
func TestAcquireBoundsTheClaimTTL(t *testing.T) {
	t.Parallel()

	// Which rule refuses which row is worth stating, because two of them are
	// indistinguishable from the outside: an expiry BEFORE the store's instant
	// is refused by the record's own canonical form (an expiry may not precede
	// its claim instant, and the claim instant is the store's clock), while the
	// equal case and the ceiling case are refused by validateBoundedExpiry.
	// Both report invalid(expires_at), so a reader of this table cannot tell —
	// and deleting the bound leaves the last two failing, which is what pins it.
	tests := []struct {
		name      string
		expiresAt time.Time
	}{
		{"already lapsed", reconcileClaimedAt.Add(-time.Second)},
		{"at the store's own instant", reconcileClaimedAt},
		{"beyond the ceiling", reconcileClaimedAt.Add(MaxReconciliationClaimTTL + time.Second)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			base := memstore.New()
			recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
			base.OrderedIndex = recorder
			store, _ := reconcileFixture(t, base)
			req := testAcquireRequest(reconcileHolder)
			req.ExpiresAt = test.expiresAt
			_, err := store.AcquireReconciliationClaim(context.Background(), req)
			assertReconcileField(ReconcileErrorInvalid, "expires_at")(t, err)
			if calls := recorder.snapshot(); len(calls) != 0 {
				t.Fatalf("a refused acquisition reached the provider: %+v", calls)
			}
		})
	}

	t.Run("at the ceiling exactly", func(t *testing.T) {
		t.Parallel()

		store, _ := reconcileFixture(t, memstore.New())
		req := testAcquireRequest(reconcileHolder)
		req.ExpiresAt = reconcileClaimedAt.Add(MaxReconciliationClaimTTL)
		if _, err := store.AcquireReconciliationClaim(context.Background(), req); err != nil {
			t.Fatalf("an expiry exactly at the ceiling was refused: %v", err)
		}
	})
}

// TestReconciliationClaimInstantsComeFromTheStoreClock pins which clock stamps
// which member. The horizon is the caller's promise, bounded above; the claim
// instant records that THIS STORE accepted the claim, and a caller-supplied one
// could place a claim's start after its own expiry or before another replica's
// takeover.
func TestReconciliationClaimInstantsComeFromTheStoreClock(t *testing.T) {
	t.Parallel()

	requestType := reflect.TypeOf(AcquireReconciliationClaimRequest{})
	for i := range requestType.NumField() {
		if name := strings.ToLower(requestType.Field(i).Name); strings.Contains(name, "claimedat") {
			t.Fatalf("AcquireReconciliationClaimRequest.%s lets a caller stamp the claim instant",
				requestType.Field(i).Name)
		}
	}

	store, clock := reconcileFixture(t, memstore.New())
	clock.set(reconcileClaimedAt.Add(3 * time.Second))
	entry := mustAcquireClaim(t, store, testAcquireRequest(reconcileHolder))
	if !entry.Claim.ClaimedAt.Equal(reconcileClaimedAt.Add(3 * time.Second)) {
		t.Fatalf("claimed at %v, want the store's clock", entry.Claim.ClaimedAt)
	}
}

// --- releasing ---------------------------------------------------------------

func TestReleaseReconciliationClaimReturnsItEarly(t *testing.T) {
	store, clock := reconcileFixture(t, memstore.New())
	held := mustAcquireClaim(t, store, testAcquireRequest(reconcileHolder))

	clock.set(reconcileClaimedAt.Add(5 * time.Second))
	released, err := store.ReleaseReconciliationClaim(context.Background(), testReleaseRequest(reconcileHolder))
	if err != nil {
		t.Fatalf("ReleaseReconciliationClaim: %v", err)
	}
	if released.Revision == held.Revision {
		t.Fatal("a release did not write")
	}
	// A released claim has lapsed by construction: its expiry IS the instant
	// the store recorded it, so it is unclaimed under every later clock reading
	// without anything having to expire.
	if !released.Claim.ExpiresAt.Equal(released.Claim.ClaimedAt) {
		t.Fatalf("a released claim still has a horizon: %+v", released.Claim)
	}

	// It is immediately takeable, without waiting out the TTL — which is the
	// whole point of releasing rather than letting a claim lapse.
	takeover := testAcquireRequest(reconcileOtherHolder)
	takeover.ExpiresAt = reconcileClaimedAt.Add(35 * time.Second)
	if _, err := store.AcquireReconciliationClaim(context.Background(), takeover); err != nil {
		t.Fatalf("a released claim was not takeable: %v", err)
	}
}

func TestReleaseReconciliationClaimIsIdempotentForItsHolder(t *testing.T) {
	store, _ := reconcileFixture(t, memstore.New())
	mustAcquireClaim(t, store, testAcquireRequest(reconcileHolder))

	first, err := store.ReleaseReconciliationClaim(context.Background(), testReleaseRequest(reconcileHolder))
	if err != nil {
		t.Fatalf("ReleaseReconciliationClaim: %v", err)
	}
	// A retry after a lost reply must not be an error, and must not write: a
	// caller cannot distinguish a timed-out success from a failure, so a second
	// release is the ordinary case rather than a mistake.
	repeat, err := store.ReleaseReconciliationClaim(context.Background(), testReleaseRequest(reconcileHolder))
	if err != nil {
		t.Fatalf("a repeated release was refused: %v", err)
	}
	if repeat.Revision != first.Revision {
		t.Fatalf("a repeated release wrote: %d -> %d", first.Revision, repeat.Revision)
	}
	assertClaimUnchanged(t, store, first)
}

func TestReleaseReconciliationClaimRefusesAClaimThatIsNotItsOwn(t *testing.T) {
	store, clock := reconcileFixture(t, memstore.New())
	held := mustAcquireClaim(t, store, testAcquireRequest(reconcileHolder))

	// While the other replica's claim is live.
	err := func() error {
		_, err := store.ReleaseReconciliationClaim(context.Background(), testReleaseRequest(reconcileOtherHolder))
		return err
	}()
	assertReconcileField(ReconcileErrorHeld, "holder_id")(t, err)
	assertClaimUnchanged(t, store, held)

	// And after it has lapsed, where there is still nothing of this caller's to
	// release. It is a different answer because it is a different fact: nobody
	// holds the claim, so a caller told "held" would back off for no reason.
	clock.set(reconcileLapsedAt)
	_, err = store.ReleaseReconciliationClaim(context.Background(), testReleaseRequest(reconcileOtherHolder))
	assertReconcileField(ReconcileErrorLapsed, "holder_id")(t, err)
	assertClaimUnchanged(t, store, held)
}

func TestReleaseReconciliationClaimReportsAnUnclaimedSession(t *testing.T) {
	store, _ := reconcileFixture(t, memstore.New())
	// A session that exists but has never been reconciled. Absence of the CLAIM
	// is what is under test, so the session's witnesses must already be bound;
	// an unbound session fails earlier and for a different reason, which
	// TestReconciliationClaimWritesBindTheSessionsWitness covers.
	mustCreateCatalog(t, store)
	_, err := store.ReleaseReconciliationClaim(context.Background(), testReleaseRequest(reconcileHolder))
	assertReconcileCode(t, err, ReconcileErrorNotFound)
}

// --- reading -----------------------------------------------------------------

func TestGetReconciliationClaimReportsOnlyALiveClaim(t *testing.T) {
	store, clock := reconcileFixture(t, memstore.New())
	mustCreateCatalog(t, store)

	if _, err := store.GetReconciliationClaim(context.Background(), testGetClaimRequest()); err == nil {
		t.Fatal("an unclaimed session reported a claim")
	} else {
		assertReconcileCode(t, err, ReconcileErrorNotFound)
	}

	mustAcquireClaim(t, store, testAcquireRequest(reconcileHolder))
	live, err := store.GetReconciliationClaim(context.Background(), testGetClaimRequest())
	if err != nil {
		t.Fatalf("GetReconciliationClaim: %v", err)
	}
	if live.Claim.HolderID != reconcileHolder {
		t.Fatalf("holder = %q, want %q", live.Claim.HolderID, reconcileHolder)
	}

	// Past the expiry the claim is reported as lapsed and the reader hands back
	// no claim at all, so a caller cannot act on a horizon that has passed.
	clock.set(reconcileLapsedAt)
	lapsed, err := store.GetReconciliationClaim(context.Background(), testGetClaimRequest())
	if err == nil {
		t.Fatal("a lapsed claim was reported as a claim")
	}
	assertReconcileField(ReconcileErrorLapsed, "expires_at")(t, err)
	if lapsed.Claim != (ReconciliationClaim{}) {
		t.Fatalf("a lapsed read disclosed the claim: %+v", lapsed.Claim)
	}
}

// --- two Factories, one session ----------------------------------------------

// TestTwoFactoriesRaceForOneClaim drives the scenario the record exists for
// rather than reasoning about it: several replicas reconcile the same session
// at once, from an unclaimed start, each retrying a lost compare-and-swap.
// Exactly one may end up holding the claim, and every loser must be told so in
// terms it can act on.
func TestTwoFactoriesRaceForOneClaim(t *testing.T) {
	store, _ := reconcileFixture(t, memstore.New())

	const replicas = 8
	var (
		start   sync.WaitGroup
		done    sync.WaitGroup
		mu      sync.Mutex
		winners []string
		losers  int
	)
	start.Add(1)
	for replica := range replicas {
		done.Add(1)
		holder := "factory-" + string(rune('a'+replica))
		go func() {
			defer done.Done()
			start.Wait()
			for attempt := 0; attempt < 64; attempt++ {
				entry, err := store.AcquireReconciliationClaim(context.Background(), testAcquireRequest(holder))
				var reconcile *ReconcileError
				switch {
				case err == nil:
					mu.Lock()
					winners = append(winners, entry.Claim.HolderID)
					mu.Unlock()
					return
				case errors.As(err, &reconcile) && reconcile.Code == ReconcileErrorHeld:
					mu.Lock()
					losers++
					mu.Unlock()
					return
				case errors.As(err, &reconcile) && reconcile.Code == ReconcileErrorConflict:
					continue // a lost create or compare-and-swap: re-read and retry.
				default:
					t.Errorf("%s: %v", holder, err)
					return
				}
			}
			t.Errorf("%s never settled", holder)
		}()
	}
	start.Done()
	done.Wait()

	if len(winners) != 1 {
		t.Fatalf("winners = %v, want exactly one", winners)
	}
	if losers != replicas-1 {
		t.Fatalf("losers = %d, want %d", losers, replicas-1)
	}
	entry, err := store.GetReconciliationClaim(context.Background(), testGetClaimRequest())
	if err != nil {
		t.Fatalf("GetReconciliationClaim: %v", err)
	}
	if entry.Claim.HolderID != winners[0] {
		t.Fatalf("the stored claim is %q, but %q was told it won", entry.Claim.HolderID, winners[0])
	}
}

// --- what the provider says about its own filing -----------------------------

func TestReconciliationClaimFilingIsHeldToTheRecord(t *testing.T) {
	t.Parallel()

	store, _ := reconcileFixture(t, memstore.New())
	mustAcquireClaim(t, store, testAcquireRequest(reconcileHolder))
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	conforming, err := store.backend.OrderedIndex.Get(
		context.Background(), reconciliationClaimID(scope, catalogSession))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	otherSession, _, err := encodeReconciliationClaim(func() ReconciliationClaim {
		claim := testReconciliationClaim()
		claim.SessionID = registryOtherSession
		return claim
	}())
	if err != nil {
		t.Fatalf("encodeReconciliationClaim: %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*storage.OrderedRecord)
		want    ReconcileErrorCode
		field   string
		covered string
	}{
		{
			name:    "a provider tombstone, which this package never writes",
			mutate:  func(r *storage.OrderedRecord) { r.Deleted = true },
			want:    ReconcileErrorDeleted,
			field:   "record",
			covered: "Deleted",
		},
		{
			name:    "another session's claim under this session's name",
			mutate:  func(r *storage.OrderedRecord) { r.Value = otherSession },
			want:    ReconcileErrorIdentity,
			field:   "record",
			covered: "Value",
		},
		{
			name:    "filed under a stable key that is not the session",
			mutate:  func(r *storage.OrderedRecord) { r.ID.StableKey = storage.StableKey(registryOtherSession) },
			want:    ReconcileErrorIdentity,
			field:   "session_id",
			covered: "ID.StableKey",
		},
		{
			name:    "filed in another session's ordering scope",
			mutate:  func(r *storage.OrderedRecord) { r.ID.OrderingScope += "/elsewhere" },
			want:    ReconcileErrorIdentity,
			field:   "ordering_scope",
			covered: "ID.OrderingScope",
		},
		{
			name:    "ranked in another session's scope",
			mutate:  func(r *storage.OrderedRecord) { r.RankingScope += "/elsewhere" },
			want:    ReconcileErrorIdentity,
			field:   "ranking_scope",
			covered: "RankingScope",
		},
		{
			name: "filed into a deadline page nothing sweeps",
			mutate: func(r *storage.OrderedRecord) {
				r.Due = storage.Due{State: storage.DueAt, UnixMillis: reconcileExpiresAt.UnixMilli()}
			},
			want:    ReconcileErrorIdentity,
			field:   "due",
			covered: "Due",
		},
		{
			name:    "ranked, in a namespace nothing ranks",
			mutate:  func(r *storage.OrderedRecord) { r.Rank = storage.Rank{Ranked: true, Value: 1} },
			want:    ReconcileErrorIdentity,
			field:   "rank",
			covered: "Rank",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			stored := conforming
			test.mutate(&stored)
			_, err := reconciliationClaimEntryFor(stored, scope, catalogTenant, catalogSession)
			got := assertReconcileCode(t, err, test.want)
			if got.Field != test.field {
				t.Fatalf("field = %q, want %q (%v)", got.Field, test.field, err)
			}
		})
	}

	// The unperturbed row must pass, or every case above could be passing for a
	// reason that has nothing to do with the component it perturbs.
	if _, err := reconciliationClaimEntryFor(conforming, scope, catalogTenant, catalogSession); err != nil {
		t.Fatalf("a conforming row was rejected: %v", err)
	}

	excluded := map[string]string{
		"Revision":     "provider state with no counterpart in the record",
		"Order":        "not exposed, and nothing lists this namespace in acceptance order",
		"ID.Namespace": "a package constant with no counterpart in the record",
	}
	perturbed := map[string]bool{}
	for _, test := range tests {
		perturbed[test.covered] = true
	}
	for _, member := range orderedRecordMembers(t) {
		if perturbed[member] == (excluded[member] != "") {
			t.Errorf("storage.OrderedRecord.%s is %s; it must be exactly one of perturbed here or excluded with a reason",
				member, map[bool]string{true: "both perturbed and excluded", false: "neither perturbed nor excluded"}[perturbed[member]])
		}
	}
}

// TestAcquireChecksTheProvidersReply reaches the filing checks through a real
// write, which is the only way to prove they are WIRED into the write path
// rather than merely correct in isolation.
func TestAcquireChecksTheProvidersReply(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store, _ := reconcileFixture(t, base)

	hostile.refileCreates(func(record storage.OrderedRecord) storage.OrderedRecord {
		record.Rank = storage.Rank{Ranked: true, Value: 1}
		return record
	})
	_, err := store.AcquireReconciliationClaim(context.Background(), testAcquireRequest(reconcileHolder))
	assertReconcileField(ReconcileErrorIdentity, "rank")(t, err)
}

// TestAcquireRefusesAReplyThatIsNotTheBytesItWrote covers the one claim a
// provider makes that the record's own members cannot check: that it stored
// THESE bytes. A substituted claim satisfies every identity check and would be
// returned to the caller as its own successful acquisition.
func TestAcquireRefusesAReplyThatIsNotTheBytesItWrote(t *testing.T) {
	t.Parallel()

	substitute, _, err := encodeReconciliationClaim(func() ReconciliationClaim {
		claim := testReconciliationClaim()
		claim.HolderID = reconcileOtherHolder
		return claim
	}())
	if err != nil {
		t.Fatalf("encodeReconciliationClaim: %v", err)
	}
	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store, _ := reconcileFixture(t, base)

	hostile.refileCreates(func(record storage.OrderedRecord) storage.OrderedRecord {
		record.Value = substitute
		return record
	})
	_, err = store.AcquireReconciliationClaim(context.Background(), testAcquireRequest(reconcileHolder))
	assertReconcileField(ReconcileErrorIdentity, "value")(t, err)
}

func TestReconcileClassifiesProviderFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want ReconcileErrorCode
	}{
		{name: "absent", err: &storage.OrderedRecordNotFoundError{}, want: ReconcileErrorNotFound},
		{name: "tombstoned", err: &storage.OrderedDeletedError{}, want: ReconcileErrorDeleted},
		{name: "lost the revision", err: &storage.OrderedRevisionConflictError{ActualRevision: 7}, want: ReconcileErrorConflict},
		{name: "ambiguous", err: &storage.OrderedAmbiguousError{}, want: ReconcileErrorUnknown},
		{name: "anything else", err: errors.New("provider exploded"), want: ReconcileErrorBackend},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := classifyReconcileOrderedError(test.err, "stage")
			got := assertReconcileCode(t, err, test.want)
			if !errors.Is(err, test.err) {
				t.Fatalf("the cause was not preserved: %v", err)
			}
			if test.want == ReconcileErrorConflict && got.Revision != 7 {
				t.Fatalf("revision = %d, want 7", got.Revision)
			}
			if strings.Contains(got.Error(), "provider exploded") {
				t.Fatalf("the message leaked provider text: %q", got.Error())
			}
		})
	}
}

// --- lifecycle and request validation ---------------------------------------

func TestReconcileOperationsValidateBeforeAdmission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		call      func(*Store) error
		assert    func(*testing.T, error)
	}{
		{
			name:      "acquire without a holder",
			operation: "AcquireReconciliationClaim",
			call: func(store *Store) error {
				_, err := store.AcquireReconciliationClaim(context.Background(), testAcquireRequest(""))
				return err
			},
			assert: assertReconcileField(ReconcileErrorInvalid, "holder_id"),
		},
		{
			name:      "acquire with an expiry that has already lapsed",
			operation: "AcquireReconciliationClaim",
			call: func(store *Store) error {
				req := testAcquireRequest(reconcileHolder)
				req.ExpiresAt = reconcileClaimedAt.Add(-time.Second)
				_, err := store.AcquireReconciliationClaim(context.Background(), req)
				return err
			},
			assert: assertReconcileField(ReconcileErrorInvalid, "expires_at"),
		},
		{
			name:      "acquire for a session that is not named",
			operation: "AcquireReconciliationClaim",
			call: func(store *Store) error {
				req := testAcquireRequest(reconcileHolder)
				req.SessionID = ""
				_, err := store.AcquireReconciliationClaim(context.Background(), req)
				return err
			},
			assert: assertInvalidIdentity("SessionID"),
		},
		{
			name:      "read a session that is not named",
			operation: "GetReconciliationClaim",
			call: func(store *Store) error {
				_, err := store.GetReconciliationClaim(context.Background(), GetReconciliationClaimRequest{
					TenantID: catalogTenant,
				})
				return err
			},
			assert: assertInvalidIdentity("SessionID"),
		},
		{
			name:      "release without a holder",
			operation: "ReleaseReconciliationClaim",
			call: func(store *Store) error {
				_, err := store.ReleaseReconciliationClaim(context.Background(), testReleaseRequest(""))
				return err
			},
			assert: assertReconcileField(ReconcileErrorInvalid, "holder_id"),
		},
	}

	declared := declaredStoreOperations(t, "reconcile.go")
	covered := map[string]bool{}
	for _, test := range tests {
		if !declared[test.operation] {
			t.Errorf("the case %q drives %s, which reconcile.go does not declare", test.name, test.operation)
		}
		covered[test.operation] = true
	}
	for name := range declared {
		if !covered[name] {
			t.Errorf("%s takes a request and no case here gives it a malformed one", name)
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			base := memstore.New()
			recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
			base.OrderedIndex = recorder
			store, _ := reconcileFixture(t, base)
			test.assert(t, test.call(store))
			if calls := recorder.snapshot(); len(calls) != 0 {
				t.Fatalf("a refused request reached the provider: %+v", calls)
			}

			closing, err := Open(context.Background(), memstore.New(), WithClock(newMovableClock(reconcileClaimedAt)))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := closing.Close(context.Background()); err != nil {
				t.Fatalf("Close: %v", err)
			}
			err = test.call(closing)
			if errors.As(err, new(*StoreClosedError)) {
				t.Fatalf("an invalid request on a closing store reported the store's state: %v", err)
			}
			test.assert(t, err)
		})
	}
}

func TestReconcileOperationsRefuseAfterClose(t *testing.T) {
	store, _ := reconcileFixture(t, memstore.New())
	mustAcquireClaim(t, store, testAcquireRequest(reconcileHolder))
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	operations := map[string]func() error{
		"AcquireReconciliationClaim": func() error {
			_, err := store.AcquireReconciliationClaim(context.Background(), testAcquireRequest(reconcileHolder))
			return err
		},
		"GetReconciliationClaim": func() error {
			_, err := store.GetReconciliationClaim(context.Background(), testGetClaimRequest())
			return err
		},
		"ReleaseReconciliationClaim": func() error {
			_, err := store.ReleaseReconciliationClaim(context.Background(), testReleaseRequest(reconcileHolder))
			return err
		},
	}

	declared := declaredStoreOperations(t, "reconcile.go")
	for name := range declared {
		if operations[name] == nil {
			t.Errorf("reconcile.go declares the public operation %s and this test does not exercise it", name)
		}
	}
	for name := range operations {
		if !declared[name] {
			t.Errorf("this test exercises %s, which reconcile.go no longer declares (was it moved?)", name)
		}
	}

	for name, call := range operations {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.As(err, new(*StoreClosedError)) {
				t.Fatalf("%s after Close = %T %v, want *StoreClosedError", name, err, err)
			}
		})
	}
}

// TestReconciliationClaimWritesBindTheSessionsWitness holds the claim to the
// package's collision discipline: a derived record name is never trusted on its
// own, on the write path or the read path. Taking a claim may be the first
// durable thing a session has, so it BINDS the witnesses; every later read
// verifies them.
func TestReconciliationClaimWritesBindTheSessionsWitness(t *testing.T) {
	t.Parallel()

	store, _ := reconcileFixture(t, memstore.New())
	// No catalog record and no registration: the acquisition itself is what
	// binds this session, or the read below could not verify anything.
	mustAcquireClaim(t, store, testAcquireRequest(reconcileHolder))
	if _, err := store.GetReconciliationClaim(context.Background(), testGetClaimRequest()); err != nil {
		t.Fatalf("an acquisition did not bind the session it wrote: %v", err)
	}

	// A session whose witness was never bound cannot be read however its name
	// is derived.
	if _, err := store.GetReconciliationClaim(context.Background(), GetReconciliationClaimRequest{
		TenantID: catalogTenant, SessionID: "session-never-created",
	}); err == nil {
		t.Fatal("an unbound session read a claim")
	}
}

// TestAClaimAndARegistrationCoexistForOneSession pins the consequence of two
// session-scoped records sharing a stable key and an ordering scope: they are
// separated by the NAMESPACE alone. A provider that keyed a row by anything
// less would have the claim and the registration overwrite each other, and the
// registration is a session's ownership fence.
func TestAClaimAndARegistrationCoexistForOneSession(t *testing.T) {
	t.Parallel()

	store, _ := reconcileFixture(t, memstore.New())
	mustAcquireClaim(t, store, testAcquireRequest(reconcileHolder))
	registration := testPutRegistrationRequest(registryEpoch)
	registration.ObservedAt = reconcileClaimedAt
	registration.ExpiresAt = reconcileClaimedAt.Add(5 * time.Minute)
	if _, err := store.PutHostRegistration(context.Background(), registration); err != nil {
		t.Fatalf("PutHostRegistration: %v", err)
	}

	claim, err := store.GetReconciliationClaim(context.Background(), testGetClaimRequest())
	if err != nil {
		t.Fatalf("the registration displaced the claim: %v", err)
	}
	if claim.Claim.HolderID != reconcileHolder {
		t.Fatalf("holder = %q, want %q", claim.Claim.HolderID, reconcileHolder)
	}
	route, err := store.GetHostRegistration(context.Background(), GetHostRegistrationRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
	})
	if err != nil {
		t.Fatalf("the claim displaced the registration: %v", err)
	}
	if route.Registration.LeaseEpoch != registryEpoch {
		t.Fatalf("lease epoch = %d, want %d", route.Registration.LeaseEpoch, registryEpoch)
	}
}

// FuzzReconciliationClaimCodec fuzzes stored claim bytes. Its seeds are real
// encodings rather than hand-written JSON, so a mutation starts from a value
// that already reaches the strict decoder, the identity validators and the
// instant rules, instead of bouncing off the first json.Unmarshal.
//
// The property is that canonicalization reaches a fixed point: anything the
// decoder accepts must re-encode, and decoding that encoding must produce the
// identical bytes again. It deliberately does not claim the decoder rejects
// non-canonical input — the decoder is a NORMALIZER, and what is guarded is
// that normalizing twice can never differ from normalizing once.
func FuzzReconciliationClaimCodec(f *testing.F) {
	seed := func(claim ReconciliationClaim) []byte {
		encoded, _, err := encodeReconciliationClaim(claim)
		if err != nil {
			f.Fatalf("seed does not encode: %v", err)
		}
		return encoded
	}
	held := testReconciliationClaim()
	f.Add(seed(held))

	// The released spelling, whose expiry equals its claim instant, is a
	// distinct branch of the instant rule and must be explored from the inside.
	released := held
	released.ExpiresAt = released.ClaimedAt
	f.Add(seed(released))

	var members map[string]json.RawMessage
	if err := json.Unmarshal(seed(held), &members); err != nil {
		f.Fatalf("seed is not JSON: %v", err)
	}
	for _, mutate := range []func(map[string]json.RawMessage){
		func(m map[string]json.RawMessage) { m["record_version"] = json.RawMessage("2") },
		func(m map[string]json.RawMessage) { m["surprise"] = json.RawMessage("1") },
		func(m map[string]json.RawMessage) { m["holder_id"] = json.RawMessage(`""`) },
		func(m map[string]json.RawMessage) { m["expires_at"] = json.RawMessage(`"1970-01-01T00:00:00Z"`) },
		func(m map[string]json.RawMessage) { m["claimed_at"] = json.RawMessage(`"3000-01-01T00:00:00Z"`) },
	} {
		copied := make(map[string]json.RawMessage, len(members))
		for name, value := range members {
			copied[name] = value
		}
		mutate(copied)
		encoded, err := json.Marshal(copied)
		if err != nil {
			f.Fatalf("marshal seed variant: %v", err)
		}
		f.Add(encoded)
	}

	f.Fuzz(func(t *testing.T, value []byte) {
		claim, err := decodeReconciliationClaim(value)
		if err != nil {
			return
		}
		encoded, canonical, err := encodeReconciliationClaim(claim)
		if err != nil {
			t.Fatalf("a decoded claim did not re-encode: %v", err)
		}
		if canonical != claim {
			t.Fatalf("a decoded claim was not canonical: %+v want %+v", canonical, claim)
		}
		again, err := decodeReconciliationClaim(encoded)
		if err != nil {
			t.Fatalf("a re-encoded claim did not decode: %v", err)
		}
		reencoded, _, err := encodeReconciliationClaim(again)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		if !bytes.Equal(encoded, reencoded) {
			t.Fatalf("canonicalization has no fixed point:\n%s\n%s", encoded, reencoded)
		}
	})
}

// TestAReleasedClaimIsNotClockIndependent drives the scenario claimHeldAt's
// comment is about instead of reasoning about it. A released claim and an
// ordinary lapsed one behave IDENTICALLY under a clock that has moved
// backwards, which is what makes a structural release marker pointless here:
// it would close the smaller of two identical windows.
func TestAReleasedClaimIsNotClockIndependent(t *testing.T) {
	t.Parallel()

	// One store whose claim was released, and one whose claim merely ran out.
	released, releasedClock := reconcileFixture(t, memstore.New())
	mustAcquireClaim(t, released, testAcquireRequest(reconcileHolder))
	if _, err := released.ReleaseReconciliationClaim(
		context.Background(), testReleaseRequest(reconcileHolder)); err != nil {
		t.Fatalf("ReleaseReconciliationClaim: %v", err)
	}

	lapsed, lapsedClock := reconcileFixture(t, memstore.New())
	mustAcquireClaim(t, lapsed, testAcquireRequest(reconcileHolder))

	// Both are unclaimed at any later instant, which is the ordinary case.
	releasedClock.set(reconcileLapsedAt)
	lapsedClock.set(reconcileLapsedAt)
	for name, store := range map[string]*Store{"released": released, "lapsed": lapsed} {
		if _, err := store.GetReconciliationClaim(context.Background(), testGetClaimRequest()); err == nil {
			t.Fatalf("%s: an unclaimed session reported a claim", name)
		} else {
			assertReconcileCode(t, err, ReconcileErrorLapsed)
		}
	}

	// And both read as live under a clock that has stepped backwards past the
	// instant each stopped being a claim. Neither is worse than the other, and
	// no structure available here would separate them.
	releasedClock.set(reconcileClaimedAt.Add(-time.Hour))
	lapsedClock.set(reconcileClaimedAt.Add(-time.Hour))
	for name, store := range map[string]*Store{"released": released, "lapsed": lapsed} {
		if _, err := store.GetReconciliationClaim(context.Background(), testGetClaimRequest()); err != nil {
			t.Fatalf("%s: the two states differ under a backwards clock: %v", name, err)
		}
	}
}
