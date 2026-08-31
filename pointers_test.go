package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"math"
	"path/filepath"
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

// The timeline every case in this file places itself on. A Host commits a
// checkpoint at 11:00 and a later one at 11:01.
var (
	pointerUpdatedAt = time.Date(2026, 8, 31, 11, 0, 0, 0, time.UTC)
	pointerLaterAt   = pointerUpdatedAt.Add(time.Minute)
)

const (
	pointerEpoch    = uint64(4)
	pointerSequence = uint64(9)
)

// testObjectReference mints a canonical ObjectID of one kind. The seed varies
// the generation and the digest so two references of one kind are distinct; it
// must be nonzero, because an all-zero digest is one the store never mints and
// parseObjectReference refuses.
func testObjectReference(kind ObjectKind, seed byte) sessionwire.ObjectReference {
	var generation [16]byte
	var digest [32]byte
	for i := range generation {
		generation[i] = seed
	}
	for i := range digest {
		digest[i] = seed
	}
	return objectIDFor(kind, encodeObjectGeneration(generation), hex.EncodeToString(digest[:]))
}

func testSessionPointer() SessionPointer {
	target := testObjectReference(ObjectKindWorkspaceCheckpoint, 1)
	return SessionPointer{
		TenantID:   catalogTenant,
		SessionID:  catalogSession,
		Kind:       SessionPointerWorkspaceCheckpoint,
		LeaseEpoch: pointerEpoch,
		Sequence:   pointerSequence,
		UpdatedAt:  pointerUpdatedAt,
		Target:     &target,
	}
}

// --- the record and its codec -----------------------------------------------

func TestSessionPointerRoundTripsThroughStoredBytes(t *testing.T) {
	t.Parallel()

	for _, kind := range testPointerKinds() {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()

			pointer := testSessionPointer()
			pointer.Kind = kind
			object, ok := kind.targetObjectKind()
			if !ok {
				t.Fatalf("%s names no object kind", kind)
			}
			target := testObjectReference(object, 2)
			pointer.Target = &target

			encoded, canonical, err := encodeSessionPointer(pointer)
			if err != nil {
				t.Fatalf("encodeSessionPointer: %v", err)
			}
			decoded, err := decodeSessionPointer(encoded)
			if err != nil {
				t.Fatalf("decodeSessionPointer: %v", err)
			}
			if decoded.Target == nil || *decoded.Target != *canonical.Target {
				t.Fatalf("target did not survive: %+v", decoded)
			}
			decoded.Target, canonical.Target = nil, nil
			if decoded != canonical {
				t.Fatalf("record did not survive: %+v want %+v", decoded, canonical)
			}
		})
	}
}

// testPointerKinds is every kind the package declares, derived from the closed
// enum rather than listed here, so a kind added later is covered by every case
// that ranges over it whether or not anyone remembers this file.
func testPointerKinds() []SessionPointerKind {
	return sessionPointerKinds()
}

func TestAClearedPointerHasExactlyOneSpelling(t *testing.T) {
	t.Parallel()

	cleared := testSessionPointer()
	cleared.Target = nil
	encoded, _, err := encodeSessionPointer(cleared)
	if err != nil {
		t.Fatalf("a cleared pointer did not encode: %v", err)
	}
	decoded, err := decodeSessionPointer(encoded)
	if err != nil {
		t.Fatalf("a cleared pointer did not decode: %v", err)
	}
	if decoded.Target != nil {
		t.Fatalf("a cleared pointer decoded with a target: %+v", decoded.Target)
	}
	// The retained high-waters are the whole content of a tombstone.
	if decoded.LeaseEpoch != pointerEpoch || decoded.Sequence != pointerSequence {
		t.Fatalf("a cleared pointer lost its high-waters: %+v", decoded)
	}
}

// TestSessionPointerCanonicalizesItsInstantToUTC is the case every sibling
// record has and this one did not: deleting the .UTC() from
// canonicalSessionPointer survived the whole suite, while the function's own
// doc promised "its one canonical spelling: a UTC instant".
//
// The zone is not cosmetic and the ASSERTION IS THE BYTES. Go encodes a
// time.Time with its offset, so two writers at ONE instant in two zones would
// produce two different encodings of one record — which is precisely what
// "encoding and decoding both end here, so two encoders cannot disagree" denies,
// and what verifySessionPointerBytes would then reject on a faithful provider
// reply. The offset round-trips, so the codec fuzzer's fixed point holds either
// way and cannot stand in for this.
//
// Both directions are driven: a request-shaped record encoded, and stored bytes
// carrying an offset decoded, because canonicalization is claimed for both.
func TestSessionPointerCanonicalizesItsInstantToUTC(t *testing.T) {
	t.Parallel()

	zone := time.FixedZone("elsewhere", 5*3600)
	elsewhere := testSessionPointer()
	elsewhere.UpdatedAt = elsewhere.UpdatedAt.In(zone)
	if elsewhere.UpdatedAt.Location() == time.UTC {
		t.Fatal("the fixture is already UTC; this case proves nothing")
	}
	encoded, canonical, err := encodeSessionPointer(elsewhere)
	if err != nil {
		t.Fatalf("encodeSessionPointer: %v", err)
	}
	if canonical.UpdatedAt.Location() != time.UTC {
		t.Fatalf("the instant was not canonicalized: %v", canonical.UpdatedAt)
	}
	if !canonical.UpdatedAt.Equal(elsewhere.UpdatedAt) {
		t.Fatalf("canonicalization moved the instant: %v", canonical.UpdatedAt)
	}

	// One instant, two zones, one encoding. This is the byte identity every
	// concurrent writer and every provider-reply check depends on.
	utc := testSessionPointer()
	utcEncoded, _, err := encodeSessionPointer(utc)
	if err != nil {
		t.Fatalf("encodeSessionPointer: %v", err)
	}
	if !bytes.Equal(encoded, utcEncoded) {
		t.Fatalf("two writers at one instant encoded differently:\n%s\n%s", encoded, utcEncoded)
	}

	// The decode path makes the same promise, so a record stored with an offset
	// by an older writer reads back canonical.
	var members map[string]json.RawMessage
	if err := json.Unmarshal(utcEncoded, &members); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	offset, err := json.Marshal(pointerUpdatedAt.In(zone))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if bytes.Equal(members["updated_at"], offset) {
		t.Fatal("the stored spelling already carries the offset; this case proves nothing")
	}
	members["updated_at"] = offset
	stored, err := json.Marshal(members)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	decoded, err := decodeSessionPointer(stored)
	if err != nil {
		t.Fatalf("decodeSessionPointer: %v", err)
	}
	if decoded.UpdatedAt.Location() != time.UTC || !decoded.UpdatedAt.Equal(pointerUpdatedAt) {
		t.Fatalf("a stored offset instant did not decode canonical: %v", decoded.UpdatedAt)
	}
}

// TestAPointerCannotNameAnObjectOfAnotherKind is the typed half of this record:
// the pointer kind DECIDES the object kind its target must be, so a workspace
// checkpoint pointer cannot be made to name a runtime checkpoint by a caller
// that passed the wrong reference to the right method.
func TestAPointerCannotNameAnObjectOfAnotherKind(t *testing.T) {
	t.Parallel()

	for _, kind := range testPointerKinds() {
		want, ok := kind.targetObjectKind()
		if !ok {
			t.Fatalf("%s names no object kind", kind)
		}
		for _, object := range allObjectKinds(t) {
			pointer := testSessionPointer()
			pointer.Kind = kind
			target := testObjectReference(object, 3)
			pointer.Target = &target
			_, _, err := encodeSessionPointer(pointer)
			if object == want {
				if err != nil {
					t.Errorf("%s refused its own object kind %s: %v", kind, object, err)
				}
				continue
			}
			if err == nil {
				t.Errorf("%s accepted a %s object as its target", kind, object)
				continue
			}
			assertPointerField(PointerErrorInvalid, "target")(t, err)
		}
	}
}

// allObjectKinds is every object kind objects.go declares, READ OUT OF THE
// SOURCE.
//
// It was a literal slice, with a doc comment claiming exactly what this one now
// does. That is the defect TestOrderedNamespacesAreDistinct had one file over
// and that this commit fixed there: a hand-written list covers the kinds its
// author remembered, so an eleventh ObjectKind would have left the cross
// product above quietly testing ten of eleven with the whole suite green. A
// comment asserting a property the code lacks is worse than no comment,
// because the next reader stops checking.
//
// The walk is the same one TestSessionPointerKindsAreTheDeclaredOnes uses on
// the pointer roles: constants declared with the named type. Its own
// correctness is asserted by TestAllObjectKindsIsTheDeclaredSet, which holds
// the derived set to what ObjectKind.valid() accepts in BOTH directions — a
// broken walk returns fewer, and a kind declared without being made valid (or
// made valid without being declared) is a disagreement between the two.
func allObjectKinds(t *testing.T) []ObjectKind {
	t.Helper()
	var kinds []ObjectKind
	for _, kind := range declaredObjectKinds(t) {
		kinds = append(kinds, kind)
	}
	return kinds
}

// declaredObjectKinds returns every ObjectKind constant objects.go declares, by
// CONSTANT NAME, so the two guards below can compare names as well as values.
func declaredObjectKinds(t *testing.T) map[string]ObjectKind {
	t.Helper()
	kinds := map[string]ObjectKind{}
	for _, declaration := range parseProductionFile(t, "objects.go").Decls {
		generic, ok := declaration.(*ast.GenDecl)
		if !ok || generic.Tok != token.CONST {
			continue
		}
		for _, spec := range generic.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			named, ok := value.Type.(*ast.Ident)
			if !ok || named.Name != "ObjectKind" {
				continue
			}
			for i, literal := range value.Values {
				basic, ok := literal.(*ast.BasicLit)
				if !ok || basic.Kind != token.STRING {
					t.Fatalf("an ObjectKind constant is not a string literal: %v", literal)
				}
				text, err := strconv.Unquote(basic.Value)
				if err != nil {
					t.Fatalf("unquote: %v", err)
				}
				kinds[value.Names[i].Name] = ObjectKind(text)
			}
		}
	}
	return kinds
}

// acceptedObjectKinds returns the constant names ObjectKind.valid() accepts,
// read out of its case clause.
//
// It reads the SWITCH rather than probing valid() with candidate values,
// because probing can only ask about kinds the prober already thought of —
// which is how "a kind valid() accepts that nothing declares" slipped past the
// first version of this guard. A case expression that is not a plain constant
// name is reported here rather than silently skipped: a literal
// ObjectKind("widget") in that list is exactly the shape being excluded.
func acceptedObjectKinds(t *testing.T) map[string]bool {
	t.Helper()
	accepted := map[string]bool{}
	for _, declaration := range parseProductionFile(t, "objects.go").Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "valid" || function.Recv == nil {
			continue
		}
		receiver, ok := function.Recv.List[0].Type.(*ast.Ident)
		if !ok || receiver.Name != "ObjectKind" {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			clause, ok := node.(*ast.CaseClause)
			if !ok {
				return true
			}
			for _, expression := range clause.List {
				name, ok := expression.(*ast.Ident)
				if !ok {
					t.Errorf("ObjectKind.valid() accepts an expression that is not a declared constant: %#v", expression)
					continue
				}
				accepted[name.Name] = true
			}
			return true
		})
	}
	return accepted
}

func parseProductionFile(t *testing.T, filename string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", filename, err)
	}
	return file
}

// TestAllObjectKindsIsTheDeclaredSet guards the walk the cross product above
// depends on, in both directions.
//
// The two sets it compares are independent statements of the same fact, each
// read from the source: the const block says which kinds EXIST, and
// ObjectKind.valid()'s case clause says which ones a reference may name. A
// disagreement is a real defect wherever it comes from.
//
// Both directions are load-bearing and neither is decoration. A walk that
// returned nothing satisfies "every derived kind is valid" completely, and the
// first version of the reverse direction was a HAND-WRITTEN list of the ten
// kinds its author knew — which is the very defect this test exists to prevent,
// and which let a kind made valid without being declared survive. Reading
// valid()'s own case clause is what closes that.
func TestAllObjectKindsIsTheDeclaredSet(t *testing.T) {
	t.Parallel()

	declared := declaredObjectKinds(t)
	accepted := acceptedObjectKinds(t)
	if len(declared) < 10 {
		t.Fatalf("found %d declared object kinds (%v); the scan is not reaching the const block", len(declared), declared)
	}
	if len(accepted) < 10 {
		t.Fatalf("found %d accepted object kinds (%v); the scan is not reaching valid()", len(accepted), accepted)
	}
	for name, kind := range declared {
		if !accepted[name] {
			t.Errorf("objects.go declares %s (%q) and ObjectKind.valid() does not accept it", name, kind)
		}
		if !kind.valid() {
			t.Errorf("%s (%q) is declared and the compiled valid() refuses it", name, kind)
		}
	}
	for name := range accepted {
		if _, ok := declared[name]; !ok {
			t.Errorf("ObjectKind.valid() accepts %s, which the const block does not declare", name)
		}
	}

	values := map[ObjectKind]string{}
	for name, kind := range declared {
		if other, ok := values[kind]; ok {
			t.Errorf("%s and %s are both %q", other, name, kind)
		}
		values[kind] = name
	}

	// And the roles this file's cross product is built from must all be
	// reachable in the derived set, or the "for every object kind" claim is
	// about a set that does not contain the one kind each role requires.
	for _, role := range sessionPointerKinds() {
		object, ok := role.targetObjectKind()
		if !ok || values[object] == "" {
			t.Errorf("the role %s requires the object kind %q, which the scan did not find", role, object)
		}
	}
}

// --- assertions -------------------------------------------------------------

func assertPointerCode(t *testing.T, err error, want PointerErrorCode) *PointerError {
	t.Helper()
	var got *PointerError
	if !errors.As(err, &got) {
		t.Fatalf("error = %T %v, want *PointerError", err, err)
	}
	if got.Code != want {
		t.Fatalf("pointer code = %q, want %q (%v)", got.Code, want, err)
	}
	return got
}

func assertPointerField(code PointerErrorCode, field string) func(*testing.T, error) {
	return func(t *testing.T, err error) {
		t.Helper()
		got := assertPointerCode(t, err, code)
		if got.Field != field {
			t.Fatalf("field = %q, want %q (%v)", got.Field, field, err)
		}
	}
}

// --- the operation matrix ---------------------------------------------------

// pointerOperations is the cross product this file drives every operational
// case over: each declared kind, with the three typed methods that are its only
// public surface.
//
// It is a table of FUNCTION VALUES rather than a list of names, so every case
// below reaches the real methods, and the table itself is held to the declared
// kinds and to the operations pointers.go declares by
// TestPointerOperationsCoverEveryDeclaredKind. A kind added without its methods
// fails there rather than quietly going untested here.
type pointerOperations struct {
	Kind  SessionPointerKind
	Noun  string
	Set   func(*Store, context.Context, SetSessionPointerRequest) (SessionPointerEntry, error)
	Get   func(*Store, context.Context, GetSessionPointerRequest) (SessionPointerEntry, error)
	Clear func(*Store, context.Context, ClearSessionPointerRequest) (SessionPointerEntry, error)
}

func pointerOperationMatrix() []pointerOperations {
	return []pointerOperations{
		{
			Kind: SessionPointerActiveContinuation, Noun: "ActiveContinuationPointer",
			Set:   (*Store).SetActiveContinuationPointer,
			Get:   (*Store).GetActiveContinuationPointer,
			Clear: (*Store).ClearActiveContinuationPointer,
		},
		{
			Kind: SessionPointerWorkspaceCheckpoint, Noun: "WorkspaceCheckpointPointer",
			Set:   (*Store).SetWorkspaceCheckpointPointer,
			Get:   (*Store).GetWorkspaceCheckpointPointer,
			Clear: (*Store).ClearWorkspaceCheckpointPointer,
		},
		{
			Kind: SessionPointerRuntimeCheckpoint, Noun: "RuntimeCheckpointPointer",
			Set:   (*Store).SetRuntimeCheckpointPointer,
			Get:   (*Store).GetRuntimeCheckpointPointer,
			Clear: (*Store).ClearRuntimeCheckpointPointer,
		},
	}
}

func pointerFixture(t *testing.T, backend *storage.Composite) (*Store, *movableClock) {
	t.Helper()
	clock := newMovableClock(pointerUpdatedAt)
	return openStore(t, backend, WithClock(clock)), clock
}

// testSetRequest names a target of the kind ops requires, so a case that means
// to exercise a fence is never refused for naming the wrong object kind.
func (ops pointerOperations) testSetRequest(t *testing.T, epoch, sequence uint64, seed byte) SetSessionPointerRequest {
	t.Helper()
	object, ok := ops.Kind.targetObjectKind()
	if !ok {
		t.Fatalf("%s names no object kind", ops.Kind)
	}
	return SetSessionPointerRequest{
		TenantID:   catalogTenant,
		SessionID:  catalogSession,
		LeaseEpoch: epoch,
		Sequence:   sequence,
		Target:     testObjectReference(object, seed),
	}
}

func testGetPointerRequest() GetSessionPointerRequest {
	return GetSessionPointerRequest{TenantID: catalogTenant, SessionID: catalogSession}
}

func testClearPointerRequest(epoch uint64) ClearSessionPointerRequest {
	return ClearSessionPointerRequest{TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: epoch}
}

func (ops pointerOperations) mustSet(t *testing.T, store *Store, req SetSessionPointerRequest) SessionPointerEntry {
	t.Helper()
	entry, err := ops.Set(store, context.Background(), req)
	if err != nil {
		t.Fatalf("Set%s: %v", ops.Noun, err)
	}
	return entry
}

func (ops pointerOperations) mustGet(t *testing.T, store *Store) SessionPointerEntry {
	t.Helper()
	entry, err := ops.Get(store, context.Background(), testGetPointerRequest())
	if err != nil {
		t.Fatalf("Get%s: %v", ops.Noun, err)
	}
	return entry
}

// TestPointerOperationsCoverEveryDeclaredKind holds the matrix above to the two
// sets it claims to cover, in both directions: every declared kind has a row,
// and every public operation pointers.go declares is one of the nine methods
// those rows name. A kind added without methods, or a method added without a
// row, fails here.
func TestPointerOperationsCoverEveryDeclaredKind(t *testing.T) {
	t.Parallel()

	rows := map[SessionPointerKind]bool{}
	for _, ops := range pointerOperationMatrix() {
		rows[ops.Kind] = true
	}
	for _, kind := range sessionPointerKinds() {
		if !rows[kind] {
			t.Errorf("the kind %s has no row in the operation matrix", kind)
		}
	}
	if len(rows) != len(sessionPointerKinds()) {
		t.Errorf("the matrix covers %d kinds and the package declares %d", len(rows), len(sessionPointerKinds()))
	}

	declared := declaredStoreOperations(t, "pointers.go")
	named := map[string]bool{}
	for _, ops := range pointerOperationMatrix() {
		for _, verb := range []string{"Set", "Get", "Clear"} {
			named[verb+ops.Noun] = true
		}
	}
	for name := range declared {
		if !named[name] {
			t.Errorf("pointers.go declares the public operation %s and the matrix does not name it", name)
		}
	}
	for name := range named {
		if !declared[name] {
			t.Errorf("the matrix names %s, which pointers.go does not declare", name)
		}
	}
}

// --- epoch and sequence fencing --------------------------------------------

func TestSetPointerCreatesThenReplaces(t *testing.T) {
	t.Parallel()

	for _, ops := range pointerOperationMatrix() {
		t.Run(string(ops.Kind), func(t *testing.T) {
			t.Parallel()

			store, clock := pointerFixture(t, memstore.New())
			first := ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch, pointerSequence, 1))
			if first.Pointer.Kind != ops.Kind {
				t.Fatalf("%s stored kind %q", ops.Noun, first.Pointer.Kind)
			}
			if !first.Pointer.UpdatedAt.Equal(pointerUpdatedAt) {
				t.Fatalf("updated_at = %v, want the store's clock reading", first.Pointer.UpdatedAt)
			}

			clock.set(pointerLaterAt)
			second := ops.testSetRequest(t, pointerEpoch, pointerSequence+1, 2)
			replaced := ops.mustSet(t, store, second)
			if *replaced.Pointer.Target != second.Target {
				t.Fatalf("target = %+v, want the second one", replaced.Pointer.Target)
			}
			if replaced.Pointer.Sequence != pointerSequence+1 {
				t.Fatalf("sequence = %d, want it to advance", replaced.Pointer.Sequence)
			}
			if replaced.Revision == first.Revision {
				t.Fatal("a replacement did not move the revision")
			}
			if got := ops.mustGet(t, store); *got.Pointer.Target != second.Target {
				t.Fatalf("a read after the replacement returned %+v", got.Pointer.Target)
			}
		})
	}
}

// TestPointerEpochFenceAdmitsEqualAndHigherAndRefusesLower is step 1's first
// case. The equal arm is the one that carries the operational weight: one lease
// grant checkpoints many times, so an equal epoch must be admitted or a Host
// could write exactly once per grant.
func TestPointerEpochFenceAdmitsEqualAndHigherAndRefusesLower(t *testing.T) {
	t.Parallel()

	for _, ops := range pointerOperationMatrix() {
		t.Run(string(ops.Kind), func(t *testing.T) {
			t.Parallel()

			store, _ := pointerFixture(t, memstore.New())
			ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch, pointerSequence, 1))

			// Equal: admitted.
			ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch, pointerSequence, 2))
			// Higher: admitted, and the high-water rises with it.
			raised := ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch+3, pointerSequence, 3))
			if raised.Pointer.LeaseEpoch != pointerEpoch+3 {
				t.Fatalf("lease epoch = %d, want it raised", raised.Pointer.LeaseEpoch)
			}

			// Lower: refused, with the high-water it must beat, and nothing
			// written. A superseded lease writing a checkpoint would name an
			// object from a session it no longer holds.
			stale := ops.testSetRequest(t, pointerEpoch, pointerSequence+5, 4)
			_, err := ops.Set(store, context.Background(), stale)
			got := assertPointerCode(t, err, PointerErrorEpoch)
			if got.Epoch != pointerEpoch+3 || got.Sequence != pointerSequence {
				t.Fatalf("refusal carried epoch %d sequence %d, want %d and %d",
					got.Epoch, got.Sequence, pointerEpoch+3, pointerSequence)
			}
			if after := ops.mustGet(t, store); after.Revision != raised.Revision {
				t.Fatalf("a refused write changed the record: %+v", after)
			}

			// A request that is stale in BOTH ways is refused on its EPOCH,
			// which is the order the two fences are applied in and the only
			// case that can tell that order apart. A caller told its sequence
			// is stale would fetch a newer capture and retry forever; one told
			// its epoch is stale knows it has lost the session and must stop.
			_, err = ops.Set(store, context.Background(), ops.testSetRequest(t, pointerEpoch, pointerSequence-1, 5))
			assertPointerField(PointerErrorEpoch, "lease_epoch")(t, err)
		})
	}
}

// TestPointerSequenceFenceRefusesAnOlderTargetUnderOneGrant is the fence the
// epoch cannot supply, and the scenario is driven rather than argued: two
// writes under ONE grant are ordered only by their revision compare-and-swap,
// so the loser retrying would reinstate its older capture and every restore
// afterwards would silently lose the work in between.
func TestPointerSequenceFenceRefusesAnOlderTargetUnderOneGrant(t *testing.T) {
	t.Parallel()

	for _, ops := range pointerOperationMatrix() {
		t.Run(string(ops.Kind), func(t *testing.T) {
			t.Parallel()

			store, _ := pointerFixture(t, memstore.New())
			// The Host captures at 9 and then at 12, both under one grant.
			ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch, 9, 1))
			newest := ops.testSetRequest(t, pointerEpoch, 12, 2)
			ops.mustSet(t, store, newest)

			// The retry of the older capture, arriving late under the SAME
			// epoch, is refused on its sequence rather than on its authority.
			_, err := ops.Set(store, context.Background(), ops.testSetRequest(t, pointerEpoch, 9, 1))
			got := assertPointerCode(t, err, PointerErrorSequence)
			if got.Field != "sequence" {
				t.Fatalf("field = %q, want sequence", got.Field)
			}
			if got.Epoch != pointerEpoch || got.Sequence != 12 {
				t.Fatalf("refusal carried epoch %d sequence %d, want %d and 12", got.Epoch, got.Sequence, pointerEpoch)
			}
			if after := ops.mustGet(t, store); *after.Pointer.Target != newest.Target {
				t.Fatalf("the older capture was reinstated: %+v", after.Pointer.Target)
			}

			// An EQUAL sequence is admitted: one journal position can be
			// captured twice, and the second capture is not stale.
			recaptured := ops.testSetRequest(t, pointerEpoch, 12, 3)
			if entry := ops.mustSet(t, store, recaptured); *entry.Pointer.Target != recaptured.Target {
				t.Fatalf("a recapture at the same position was not stored: %+v", entry.Pointer.Target)
			}
		})
	}
}

// TestAHigherEpochCannotRewindTheSequence is the ORDER of the two fences asked
// as a question about the world. A successor lease is admitted by the epoch
// fence and is still refused an older target: authority to write is not
// authority to go backwards, and a new Host that resumed from a stale local
// copy would otherwise publish it over the newer capture.
func TestAHigherEpochCannotRewindTheSequence(t *testing.T) {
	t.Parallel()

	for _, ops := range pointerOperationMatrix() {
		t.Run(string(ops.Kind), func(t *testing.T) {
			t.Parallel()

			store, _ := pointerFixture(t, memstore.New())
			ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch, 12, 1))
			_, err := ops.Set(store, context.Background(), ops.testSetRequest(t, pointerEpoch+1, 11, 2))
			assertPointerField(PointerErrorSequence, "sequence")(t, err)
			if after := ops.mustGet(t, store); after.Pointer.LeaseEpoch != pointerEpoch {
				t.Fatalf("a refused write raised the epoch anyway: %+v", after.Pointer)
			}
		})
	}
}

// --- clearing ---------------------------------------------------------------

// TestClearPointerWritesATombstoneThatRetainsBothHighWaters is step 3. Clearing
// never deletes: it writes the one nil target, and both marks survive so
// neither fence can fall.
func TestClearPointerWritesATombstoneThatRetainsBothHighWaters(t *testing.T) {
	t.Parallel()

	for _, ops := range pointerOperationMatrix() {
		t.Run(string(ops.Kind), func(t *testing.T) {
			t.Parallel()

			store, clock := pointerFixture(t, memstore.New())
			ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch, 12, 1))

			clock.set(pointerLaterAt)
			cleared, err := ops.Clear(store, context.Background(), testClearPointerRequest(pointerEpoch))
			if err != nil {
				t.Fatalf("Clear%s: %v", ops.Noun, err)
			}
			if cleared.Pointer.Target != nil {
				t.Fatalf("a cleared pointer kept a target: %+v", cleared.Pointer.Target)
			}
			if cleared.Pointer.LeaseEpoch != pointerEpoch || cleared.Pointer.Sequence != 12 {
				t.Fatalf("clearing dropped a high-water: %+v", cleared.Pointer)
			}
			if !cleared.Pointer.UpdatedAt.Equal(pointerLaterAt) {
				t.Fatalf("updated_at = %v, want the store's clock reading", cleared.Pointer.UpdatedAt)
			}

			// A read reports the clear as its own answer, with both marks, and
			// hands back no record.
			_, err = ops.Get(store, context.Background(), testGetPointerRequest())
			got := assertPointerCode(t, err, PointerErrorCleared)
			if got.Epoch != pointerEpoch || got.Sequence != 12 {
				t.Fatalf("a cleared read carried epoch %d sequence %d", got.Epoch, got.Sequence)
			}

			// And the fences still hold: a lower epoch is refused, and so is an
			// older capture, exactly as they were before the clear.
			_, err = ops.Set(store, context.Background(), ops.testSetRequest(t, pointerEpoch-1, 13, 2))
			assertPointerField(PointerErrorEpoch, "lease_epoch")(t, err)
			_, err = ops.Set(store, context.Background(), ops.testSetRequest(t, pointerEpoch, 11, 2))
			assertPointerField(PointerErrorSequence, "sequence")(t, err)

			// What a cleared pointer DOES license: a write at or above both.
			restored := ops.testSetRequest(t, pointerEpoch, 12, 5)
			if entry := ops.mustSet(t, store, restored); *entry.Pointer.Target != restored.Target {
				t.Fatalf("a cleared pointer refused a conforming write: %+v", entry.Pointer)
			}
		})
	}
}

// TestClearPointerIsIdempotentUnderOneGrantAndRewrittenByALater is the registry
// tombstone rule in this record's vocabulary. A repeat under one grant writes
// nothing; a LATER grant rewrites, because leaving the fence at the older epoch
// would let every lease granted in between — all of which have provably lost
// the session — write again.
func TestClearPointerIsIdempotentUnderOneGrantAndRewrittenByALater(t *testing.T) {
	t.Parallel()

	for _, ops := range pointerOperationMatrix() {
		t.Run(string(ops.Kind), func(t *testing.T) {
			t.Parallel()

			store, _ := pointerFixture(t, memstore.New())
			ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch, 12, 1))
			first, err := ops.Clear(store, context.Background(), testClearPointerRequest(pointerEpoch))
			if err != nil {
				t.Fatalf("Clear%s: %v", ops.Noun, err)
			}
			repeat, err := ops.Clear(store, context.Background(), testClearPointerRequest(pointerEpoch))
			if err != nil {
				t.Fatalf("a repeated clear failed: %v", err)
			}
			if repeat.Revision != first.Revision {
				t.Fatalf("a repeated clear wrote: revision %d then %d", first.Revision, repeat.Revision)
			}

			later, err := ops.Clear(store, context.Background(), testClearPointerRequest(pointerEpoch+2))
			if err != nil {
				t.Fatalf("a later grant could not clear: %v", err)
			}
			if later.Pointer.LeaseEpoch != pointerEpoch+2 || later.Revision == first.Revision {
				t.Fatalf("a later grant's clear did not raise the fence: %+v", later)
			}
			// And the raised fence has teeth: the grant in between is refused.
			_, err = ops.Set(store, context.Background(), ops.testSetRequest(t, pointerEpoch+1, 13, 2))
			assertPointerField(PointerErrorEpoch, "lease_epoch")(t, err)
		})
	}
}

// TestClearRefusesASessionThatHasNoPointer is the other half of the registry's
// argument: cleanup is idempotent with respect to ITS OWN tombstone, not with
// respect to nothing. Creating one for a pointer that never existed would mint
// a fencing high-water mark out of an unverified caller-supplied epoch.
func TestClearRefusesASessionThatHasNoPointer(t *testing.T) {
	t.Parallel()

	for _, ops := range pointerOperationMatrix() {
		t.Run(string(ops.Kind), func(t *testing.T) {
			t.Parallel()

			store, _ := pointerFixture(t, memstore.New())
			// Another kind's pointer exists, so the SESSION is bound and the
			// refusal is about this pointer rather than about the session.
			other := pointerOperationMatrix()[0]
			if other.Kind == ops.Kind {
				other = pointerOperationMatrix()[1]
			}
			other.mustSet(t, store, other.testSetRequest(t, pointerEpoch, 12, 1))

			_, err := ops.Clear(store, context.Background(), testClearPointerRequest(pointerEpoch))
			assertPointerField(PointerErrorNotFound, "record")(t, err)
			_, err = ops.Get(store, context.Background(), testGetPointerRequest())
			assertPointerField(PointerErrorNotFound, "record")(t, err)
		})
	}
}

// --- one session, several pointers ------------------------------------------

// TestPointersOfDifferentKindsAreIndependentRows is what the typed methods buy
// operationally: a session has one row per role, each with its own fences, and
// clearing one leaves the others untouched.
func TestPointersOfDifferentKindsAreIndependentRows(t *testing.T) {
	t.Parallel()

	store, _ := pointerFixture(t, memstore.New())
	matrix := pointerOperationMatrix()
	for i, ops := range matrix {
		ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch+uint64(i), uint64(10+i), byte(i+1)))
	}
	for i, ops := range matrix {
		entry := ops.mustGet(t, store)
		if entry.Pointer.Kind != ops.Kind || entry.Pointer.LeaseEpoch != pointerEpoch+uint64(i) {
			t.Fatalf("%s returned %+v", ops.Noun, entry.Pointer)
		}
		object, _ := ops.Kind.targetObjectKind()
		if entry.Pointer.Target.ObjectID != testObjectReference(object, byte(i+1)).ObjectID {
			t.Fatalf("%s returned another kind's target: %+v", ops.Noun, entry.Pointer.Target)
		}
	}

	first := matrix[0]
	if _, err := first.Clear(store, context.Background(), testClearPointerRequest(pointerEpoch)); err != nil {
		t.Fatalf("Clear%s: %v", first.Noun, err)
	}
	for _, ops := range matrix[1:] {
		if entry := ops.mustGet(t, store); entry.Pointer.Target == nil {
			t.Fatalf("clearing %s cleared %s too", first.Noun, ops.Noun)
		}
	}
}

// --- what survives a restart ------------------------------------------------

// TestPointersSurviveARestart is step 1's restart case, and the assertion is
// not merely that the target comes back. A new Store over the same backend is
// what a Host process replacement IS, and what must survive it is the FENCE: a
// superseded lease must still be refused, and a stale capture must still be
// refused, by a process that has never seen either write.
func TestPointersSurviveARestart(t *testing.T) {
	t.Parallel()

	backend := memstore.New()
	for _, ops := range pointerOperationMatrix() {
		store, _ := pointerFixture(t, backend)
		ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch+2, 12, 1))
	}

	// A different Store, over the same durable state.
	restarted, _ := pointerFixture(t, backend)
	for _, ops := range pointerOperationMatrix() {
		entry, err := ops.Get(restarted, context.Background(), testGetPointerRequest())
		if err != nil {
			t.Fatalf("Get%s after a restart: %v", ops.Noun, err)
		}
		if entry.Pointer.LeaseEpoch != pointerEpoch+2 || entry.Pointer.Sequence != 12 {
			t.Fatalf("%s came back as %+v", ops.Noun, entry.Pointer)
		}
		_, err = ops.Set(restarted, context.Background(), ops.testSetRequest(t, pointerEpoch+1, 13, 2))
		assertPointerField(PointerErrorEpoch, "lease_epoch")(t, err)
		_, err = ops.Set(restarted, context.Background(), ops.testSetRequest(t, pointerEpoch+2, 11, 2))
		assertPointerField(PointerErrorSequence, "sequence")(t, err)
	}

	// And a clear written before the restart still reads as cleared afterwards,
	// with both marks, rather than as a session that never had a pointer.
	ops := pointerOperationMatrix()[0]
	writer, _ := pointerFixture(t, backend)
	if _, err := ops.Clear(writer, context.Background(), testClearPointerRequest(pointerEpoch+2)); err != nil {
		t.Fatalf("Clear%s: %v", ops.Noun, err)
	}
	again, _ := pointerFixture(t, backend)
	_, err := ops.Get(again, context.Background(), testGetPointerRequest())
	got := assertPointerCode(t, err, PointerErrorCleared)
	if got.Epoch != pointerEpoch+2 || got.Sequence != 12 {
		t.Fatalf("a cleared pointer came back as epoch %d sequence %d", got.Epoch, got.Sequence)
	}
}

// --- filing integrity -------------------------------------------------------

// TestSessionPointerFilingIsHeldToTheRecord perturbs every provider-supplied
// component of a stored row, one at a time, and holds the reader to refusing
// each. The cross product below is over storage.OrderedRecord's real members
// rather than a hand-written list, so a member added to that type is either
// perturbed here or excluded with a reason.
func TestSessionPointerFilingIsHeldToTheRecord(t *testing.T) {
	t.Parallel()

	store, _ := pointerFixture(t, memstore.New())
	ops := pointerOperationMatrix()[1]
	ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch, pointerSequence, 1))
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	conforming, err := store.backend.OrderedIndex.Get(context.Background(), sessionPointerID(scope, ops.Kind))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	otherSession, _, err := encodeSessionPointer(func() SessionPointer {
		pointer := testSessionPointer()
		pointer.SessionID = registryOtherSession
		return pointer
	}())
	if err != nil {
		t.Fatalf("encodeSessionPointer: %v", err)
	}
	// Another ROLE's record, under this role's name. It is a valid record of
	// its own kind, so only the kind check refuses it.
	otherKind, _, err := encodeSessionPointer(func() SessionPointer {
		pointer := testSessionPointer()
		pointer.Kind = SessionPointerRuntimeCheckpoint
		target := testObjectReference(ObjectKindRuntimeCheckpoint, 1)
		pointer.Target = &target
		return pointer
	}())
	if err != nil {
		t.Fatalf("encodeSessionPointer: %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*storage.OrderedRecord)
		want    PointerErrorCode
		field   string
		covered string
	}{
		{
			name:    "a provider tombstone, which this package never writes",
			mutate:  func(r *storage.OrderedRecord) { r.Deleted = true },
			want:    PointerErrorDeleted,
			field:   "record",
			covered: "Deleted",
		},
		{
			name:    "another session's pointer under this session's name",
			mutate:  func(r *storage.OrderedRecord) { r.Value = otherSession },
			want:    PointerErrorIdentity,
			field:   "record",
			covered: "Value",
		},
		{
			name:    "filed under a stable key that is not the role",
			mutate:  func(r *storage.OrderedRecord) { r.ID.StableKey = storage.StableKey("elsewhere") },
			want:    PointerErrorIdentity,
			field:   "stable_key",
			covered: "ID.StableKey",
		},
		{
			name:    "filed in another session's ordering scope",
			mutate:  func(r *storage.OrderedRecord) { r.ID.OrderingScope += "/elsewhere" },
			want:    PointerErrorIdentity,
			field:   "ordering_scope",
			covered: "ID.OrderingScope",
		},
		{
			name:    "ranked in another session's scope",
			mutate:  func(r *storage.OrderedRecord) { r.RankingScope += "/elsewhere" },
			want:    PointerErrorIdentity,
			field:   "ranking_scope",
			covered: "RankingScope",
		},
		{
			name: "filed into a deadline page nothing sweeps",
			mutate: func(r *storage.OrderedRecord) {
				r.Due = storage.Due{State: storage.DueAt, UnixMillis: pointerUpdatedAt.UnixMilli()}
			},
			want:    PointerErrorIdentity,
			field:   "due",
			covered: "Due",
		},
		{
			name:    "ranked, in a namespace nothing ranks",
			mutate:  func(r *storage.OrderedRecord) { r.Rank = storage.Rank{Ranked: true, Value: 1} },
			want:    PointerErrorIdentity,
			field:   "rank",
			covered: "Rank",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			stored := conforming
			test.mutate(&stored)
			_, err := sessionPointerEntryFor(stored, scope, ops.Kind, catalogTenant, catalogSession)
			got := assertPointerCode(t, err, test.want)
			if got.Field != test.field {
				t.Fatalf("field = %q, want %q (%v)", got.Field, test.field, err)
			}
		})
	}

	// The role check has no OrderedRecord member of its own — it is a
	// disagreement between the bytes and the row the caller asked for — so it
	// sits beside the cross product rather than in it.
	t.Run("another role's pointer under this role's name", func(t *testing.T) {
		t.Parallel()

		stored := conforming
		stored.Value = otherKind
		_, err := sessionPointerEntryFor(stored, scope, ops.Kind, catalogTenant, catalogSession)
		assertPointerField(PointerErrorIdentity, "kind")(t, err)
	})

	// The unperturbed row must pass, or every case above could be passing for a
	// reason that has nothing to do with the component it perturbs.
	if _, err := sessionPointerEntryFor(conforming, scope, ops.Kind, catalogTenant, catalogSession); err != nil {
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

// TestSetPointerChecksTheProvidersReply reaches the filing checks through a
// real write, which is the only way to prove they are WIRED into the write path
// rather than merely correct in isolation.
func TestSetPointerChecksTheProvidersReply(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store, _ := pointerFixture(t, base)
	ops := pointerOperationMatrix()[1]

	hostile.refileCreates(func(record storage.OrderedRecord) storage.OrderedRecord {
		record.Rank = storage.Rank{Ranked: true, Value: 1}
		return record
	})
	_, err := ops.Set(store, context.Background(), ops.testSetRequest(t, pointerEpoch, pointerSequence, 1))
	assertPointerField(PointerErrorIdentity, "rank")(t, err)
}

// TestSetPointerRefusesAReplyThatIsNotTheBytesItWrote covers the one claim a
// provider makes that the record's own members cannot check: that it stored
// THESE bytes. A substituted pointer of the same kind and session satisfies
// every identity check and would be returned to the caller as its own
// successful write — and then be the fence the next write is measured against.
func TestSetPointerRefusesAReplyThatIsNotTheBytesItWrote(t *testing.T) {
	t.Parallel()

	substitute, _, err := encodeSessionPointer(func() SessionPointer {
		pointer := testSessionPointer()
		target := testObjectReference(ObjectKindWorkspaceCheckpoint, 9)
		pointer.Target = &target
		return pointer
	}())
	if err != nil {
		t.Fatalf("encodeSessionPointer: %v", err)
	}
	base := memstore.New()
	hostile := &hostileOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = hostile
	store, _ := pointerFixture(t, base)
	ops := pointerOperationMatrix()[1]

	hostile.refileCreates(func(record storage.OrderedRecord) storage.OrderedRecord {
		record.Value = substitute
		return record
	})
	_, err = ops.Set(store, context.Background(), ops.testSetRequest(t, pointerEpoch, pointerSequence, 1))
	assertPointerField(PointerErrorIdentity, "value")(t, err)
}

func TestPointerClassifiesProviderFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want PointerErrorCode
	}{
		{name: "absent", err: &storage.OrderedRecordNotFoundError{}, want: PointerErrorNotFound},
		{name: "tombstoned", err: &storage.OrderedDeletedError{}, want: PointerErrorDeleted},
		{name: "lost the revision", err: &storage.OrderedRevisionConflictError{ActualRevision: 7}, want: PointerErrorConflict},
		{name: "ambiguous", err: &storage.OrderedAmbiguousError{}, want: PointerErrorUnknown},
		// Revision exhaustion falls through to Backend deliberately, as it does
		// for every sibling record. It is in the table because an arm that is
		// absent by decision and an arm that is absent by oversight look
		// identical, and only a case here tells them apart.
		{name: "exhausted", err: &storage.OrderedRevisionExhaustedError{Revision: math.MaxUint64}, want: PointerErrorBackend},
		{name: "anything else", err: errors.New("provider exploded"), want: PointerErrorBackend},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := classifyPointerOrderedError(test.err, "stage")
			got := assertPointerCode(t, err, test.want)
			if !errors.Is(err, test.err) {
				t.Fatalf("the cause was not preserved: %v", err)
			}
			if test.want == PointerErrorConflict && got.Revision != 7 {
				t.Fatalf("revision = %d, want 7", got.Revision)
			}
			if strings.Contains(got.Error(), "provider exploded") {
				t.Fatalf("the message leaked provider text: %q", got.Error())
			}
		})
	}
}

func TestALostPointerCreateIsReportedAsAConflict(t *testing.T) {
	t.Parallel()

	base := memstore.New()
	recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = recorder
	store, _ := pointerFixture(t, base)
	ops := pointerOperationMatrix()[1]

	// A racing writer lands between this call's read and its create. The hook
	// is disarmed on entry rather than guarded by a sync.Once, because the
	// competing write below reaches this same hook and a Once would re-enter
	// its own Do on this goroutine and deadlock. There is one goroutine here,
	// which is the point of placing the interleave rather than racing for it.
	winner := ops.testSetRequest(t, pointerEpoch, pointerSequence, 5)
	recorder.beforeCreate = func() {
		recorder.beforeCreate = nil
		ops.mustSet(t, store, winner)
	}
	_, err := ops.Set(store, context.Background(), ops.testSetRequest(t, pointerEpoch, pointerSequence, 1))
	got := assertPointerCode(t, err, PointerErrorConflict)
	if got.Field != "create" {
		t.Fatalf("field = %q, want create (%v)", got.Field, err)
	}
	if got.Revision == 0 {
		t.Fatal("a lost create did not report the revision the caller must re-read from")
	}
	// The winner's pointer is what is stored, unmodified by the loser.
	if entry := ops.mustGet(t, store); *entry.Pointer.Target != winner.Target {
		t.Fatalf("a lost create overwrote the winner: %+v", entry.Pointer.Target)
	}
}

// --- the codec's closed doors -----------------------------------------------

func TestDecodeSessionPointerFailsClosed(t *testing.T) {
	t.Parallel()

	valid, _, err := encodeSessionPointer(testSessionPointer())
	if err != nil {
		t.Fatalf("encodeSessionPointer: %v", err)
	}
	rewrite := func(t *testing.T, mutate func(map[string]json.RawMessage)) []byte {
		t.Helper()
		var members map[string]json.RawMessage
		if err := json.Unmarshal(valid, &members); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		mutate(members)
		encoded, err := json.Marshal(members)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return encoded
	}

	tests := []struct {
		name  string
		value func(t *testing.T) []byte
		want  PointerErrorCode
		field string
	}{
		{
			name:  "empty",
			value: func(*testing.T) []byte { return nil },
			want:  PointerErrorMalformed, field: "record",
		},
		{
			name:  "not JSON",
			value: func(*testing.T) []byte { return []byte("{") },
			want:  PointerErrorMalformed, field: "record",
		},
		{
			name: "a version this reader does not know",
			value: func(t *testing.T) []byte {
				return rewrite(t, func(m map[string]json.RawMessage) { m["record_version"] = json.RawMessage("2") })
			},
			want: PointerErrorVersion, field: "record_version",
		},
		{
			name: "a member this reader does not declare",
			value: func(t *testing.T) []byte {
				return rewrite(t, func(m map[string]json.RawMessage) { m["surprise"] = json.RawMessage("1") })
			},
			want: PointerErrorMalformed, field: "record",
		},
		{
			name: "above the record bound",
			value: func(t *testing.T) []byte {
				return append(bytes.Repeat([]byte(" "), MaxSessionPointerRecordBytes), valid...)
			},
			want: PointerErrorTooLarge, field: "record",
		},
		{
			name: "a role this package does not have",
			value: func(t *testing.T) []byte {
				return rewrite(t, func(m map[string]json.RawMessage) { m["kind"] = json.RawMessage(`"invented"`) })
			},
			want: PointerErrorInvalid, field: "kind",
		},
		{
			name: "a target of another object kind",
			value: func(t *testing.T) []byte {
				return rewrite(t, func(m map[string]json.RawMessage) {
					reference := testObjectReference(ObjectKindRuntimeCheckpoint, 1)
					encoded, err := json.Marshal(reference)
					if err != nil {
						t.Fatalf("marshal: %v", err)
					}
					m["target"] = encoded
				})
			},
			want: PointerErrorInvalid, field: "target",
		},
		{
			name: "a target that is not an object identity at all",
			value: func(t *testing.T) []byte {
				return rewrite(t, func(m map[string]json.RawMessage) {
					m["target"] = json.RawMessage(`{"object_id":"not-an-object"}`)
				})
			},
			want: PointerErrorInvalid, field: "target",
		},
		{
			name: "no lease epoch, which is no fence",
			value: func(t *testing.T) []byte {
				return rewrite(t, func(m map[string]json.RawMessage) { m["lease_epoch"] = json.RawMessage("0") })
			},
			want: PointerErrorInvalid, field: "lease_epoch",
		},
		{
			name: "an instant no rank can be built from",
			value: func(t *testing.T) []byte {
				return rewrite(t, func(m map[string]json.RawMessage) {
					m["updated_at"] = json.RawMessage(`"5000-01-01T00:00:00Z"`)
				})
			},
			want: PointerErrorInvalid, field: "updated_at",
		},
		{
			name: "no tenant",
			value: func(t *testing.T) []byte {
				return rewrite(t, func(m map[string]json.RawMessage) { m["tenant_id"] = json.RawMessage(`""`) })
			},
			want: PointerErrorInvalid, field: "tenant_id",
		},
		{
			name: "no session",
			value: func(t *testing.T) []byte {
				return rewrite(t, func(m map[string]json.RawMessage) { m["session_id"] = json.RawMessage(`""`) })
			},
			want: PointerErrorInvalid, field: "session_id",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := decodeSessionPointer(test.value(t))
			assertPointerField(test.want, test.field)(t, err)
		})
	}
}

// TestLargestAcceptableSessionPointerFitsTheBound is why encodeSessionPointer's
// size refusal is a relationship rather than a live branch: a record this
// package accepts but cannot rewrite is the state the bound exists to prevent.
//
// It is measured over EVERY declared kind rather than one, because the target's
// encoded length depends on the object kind the role requires and the longest
// of those is not a thing to remember.
func TestLargestAcceptableSessionPointerFitsTheBound(t *testing.T) {
	t.Parallel()

	for _, kind := range sessionPointerKinds() {
		object, ok := kind.targetObjectKind()
		if !ok {
			t.Fatalf("%s names no object kind", kind)
		}
		target := worstCaseObjectReference(t, object)
		largest := SessionPointer{
			TenantID:   sessionwire.TenantID(worstCaseIdentity()),
			SessionID:  sessionwire.SessionID(worstCaseIdentity()),
			Kind:       kind,
			LeaseEpoch: math.MaxUint64,
			Sequence:   math.MaxUint64,
			UpdatedAt:  pointerUpdatedAt,
			Target:     &target,
		}
		encoded, _, err := encodeSessionPointer(largest)
		if err != nil {
			t.Fatalf("the largest acceptable %s pointer does not encode: %v", kind, err)
		}
		if len(encoded) >= MaxSessionPointerRecordBytes {
			t.Fatalf("the largest acceptable %s pointer is %d bytes, at or above the %d-byte bound",
				kind, len(encoded), MaxSessionPointerRecordBytes)
		}
		t.Logf("largest acceptable %s pointer = %d bytes against a %d-byte bound",
			kind, len(encoded), MaxSessionPointerRecordBytes)
		if _, err := decodeSessionPointer(encoded); err != nil {
			t.Fatalf("the largest acceptable %s pointer does not decode: %v", kind, err)
		}
	}
}

// worstCaseObjectReference is the LARGEST-ENCODING reference of one kind that
// parseObjectReference accepts, which is not a free identity: an ObjectID is
// canonical lowercase ASCII of a fixed shape, so its worst case is its only
// shape. TestWorstCaseObjectReferenceIsTheLargestEncodingOne pins that claim,
// because a fixture that merely happens to be accepted proves nothing about
// the bound built on top of it.
func worstCaseObjectReference(t *testing.T, kind ObjectKind) sessionwire.ObjectReference {
	t.Helper()
	return testObjectReference(kind, 0xff)
}

func TestWorstCaseObjectReferenceIsTheLargestEncodingOne(t *testing.T) {
	t.Parallel()

	for _, kind := range sessionPointerKinds() {
		object, _ := kind.targetObjectKind()
		reference := worstCaseObjectReference(t, object)
		if _, err := parseObjectReference(reference); err != nil {
			t.Fatalf("the worst-case reference is not acceptable: %v", err)
		}
		encoded, err := json.Marshal(reference.ObjectID)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		// One byte per byte plus the two quotes: an ObjectID has no escaping
		// worst case, which is what makes this fixture the largest one and the
		// pointer's bound half the registry's rather than equal to it.
		if want := len(reference.ObjectID) + 2; len(encoded) != want {
			t.Fatalf("the reference encodes to %d bytes, want %d: it is escaping after all",
				len(encoded), want)
		}
		// And it is the only length an acceptable reference of this kind has:
		// the scheme, the kind, a fixed 26-character generation and a fixed
		// 64-character digest, joined by three colons.
		if want := len("v1:") + len(object) + 1 + 26 + 1 + 64; len(reference.ObjectID) != want {
			t.Fatalf("the reference is %d bytes, want the one acceptable length %d",
				len(reference.ObjectID), want)
		}
	}
}

// --- lifecycle and request validation ---------------------------------------

// pointerMalformedCases is the malformed-request table, built as a CROSS
// PRODUCT over the operation matrix rather than written out. Every kind gets
// every verb, so a role added to the enum is covered here the day its methods
// exist, and the coverage check below is what makes that a guarantee rather
// than a hope.
func pointerMalformedCases(t *testing.T) []struct {
	name      string
	operation string
	call      func(*Store) error
	assert    func(*testing.T, error)
} {
	t.Helper()
	type malformed = struct {
		name      string
		operation string
		call      func(*Store) error
		assert    func(*testing.T, error)
	}
	var cases []malformed
	for _, ops := range pointerOperationMatrix() {
		cases = append(cases,
			malformed{
				name:      "set a pointer on a session that is not named (" + string(ops.Kind) + ")",
				operation: "Set" + ops.Noun,
				call: func(store *Store) error {
					req := ops.testSetRequest(t, pointerEpoch, pointerSequence, 1)
					req.SessionID = ""
					_, err := ops.Set(store, context.Background(), req)
					return err
				},
				assert: assertInvalidIdentity("SessionID"),
			},
			malformed{
				name:      "set a pointer at no epoch (" + string(ops.Kind) + ")",
				operation: "Set" + ops.Noun,
				call: func(store *Store) error {
					req := ops.testSetRequest(t, 0, pointerSequence, 1)
					_, err := ops.Set(store, context.Background(), req)
					return err
				},
				assert: assertPointerField(PointerErrorInvalid, "lease_epoch"),
			},
			malformed{
				name:      "set a pointer at an object of another kind (" + string(ops.Kind) + ")",
				operation: "Set" + ops.Noun,
				call: func(store *Store) error {
					req := ops.testSetRequest(t, pointerEpoch, pointerSequence, 1)
					req.Target = testObjectReference(ObjectKindArtifact, 1)
					_, err := ops.Set(store, context.Background(), req)
					return err
				},
				assert: assertPointerField(PointerErrorInvalid, "target"),
			},
			malformed{
				name:      "read a pointer on a session that is not named (" + string(ops.Kind) + ")",
				operation: "Get" + ops.Noun,
				call: func(store *Store) error {
					_, err := ops.Get(store, context.Background(), GetSessionPointerRequest{TenantID: catalogTenant})
					return err
				},
				assert: assertInvalidIdentity("SessionID"),
			},
			malformed{
				name:      "clear a pointer at no epoch (" + string(ops.Kind) + ")",
				operation: "Clear" + ops.Noun,
				call: func(store *Store) error {
					_, err := ops.Clear(store, context.Background(), testClearPointerRequest(0))
					return err
				},
				assert: assertPointerField(PointerErrorInvalid, "lease_epoch"),
			},
		)
	}
	return cases
}

// TestPointerOperationsValidateBeforeAdmission holds every operation to the
// package's order: validation precedes admission precedes scope binding
// precedes the provider call. A refused request reaches no provider, and a
// refused request on a CLOSING store is still refused for its own reason rather
// than for the store's state — otherwise a caller retrying against a healthy
// store would repeat a request that can never succeed.
func TestPointerOperationsValidateBeforeAdmission(t *testing.T) {
	t.Parallel()

	tests := pointerMalformedCases(t)
	declared := declaredStoreOperations(t, "pointers.go")
	covered := map[string]bool{}
	for _, test := range tests {
		if !declared[test.operation] {
			t.Errorf("the case %q drives %s, which pointers.go does not declare", test.name, test.operation)
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
			store, _ := pointerFixture(t, base)
			test.assert(t, test.call(store))
			if calls := recorder.snapshot(); len(calls) != 0 {
				t.Fatalf("a refused request reached the provider: %+v", calls)
			}

			closing, err := Open(context.Background(), memstore.New(), WithClock(newMovableClock(pointerUpdatedAt)))
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

func TestPointerOperationsRefuseAfterClose(t *testing.T) {
	store, _ := pointerFixture(t, memstore.New())
	seed := pointerOperationMatrix()[1]
	seed.mustSet(t, store, seed.testSetRequest(t, pointerEpoch, pointerSequence, 1))
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	operations := map[string]func() error{}
	for _, ops := range pointerOperationMatrix() {
		operations["Set"+ops.Noun] = func() error {
			_, err := ops.Set(store, context.Background(), ops.testSetRequest(t, pointerEpoch, pointerSequence, 1))
			return err
		}
		operations["Get"+ops.Noun] = func() error {
			_, err := ops.Get(store, context.Background(), testGetPointerRequest())
			return err
		}
		operations["Clear"+ops.Noun] = func() error {
			_, err := ops.Clear(store, context.Background(), testClearPointerRequest(pointerEpoch))
			return err
		}
	}

	declared := declaredStoreOperations(t, "pointers.go")
	for name := range declared {
		if operations[name] == nil {
			t.Errorf("pointers.go declares the public operation %s and this test does not exercise it", name)
		}
	}
	for name := range operations {
		if !declared[name] {
			t.Errorf("this test exercises %s, which pointers.go no longer declares (was it moved?)", name)
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

// TestPointerWritesBindTheSessionsWitness holds the pointer to the package's
// collision discipline: a derived record name is never trusted on its own, on
// the write path or the read path. A pointer may be the first durable thing a
// session has, so setting one BINDS the witnesses; every later read verifies
// them.
func TestPointerWritesBindTheSessionsWitness(t *testing.T) {
	t.Parallel()

	store, _ := pointerFixture(t, memstore.New())
	ops := pointerOperationMatrix()[1]
	// No catalog record and no registration: the set itself is what binds this
	// session, or the read below could not verify anything.
	ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch, pointerSequence, 1))
	ops.mustGet(t, store)

	// A session whose witness was never bound cannot be read however its name
	// is derived — and the CLASS of the refusal is the assertion, not merely
	// that one exists. Deleting verifySessionScope from the read path leaves
	// this call failing with pointer not_found, which is a different claim
	// about the world: "this session has no pointer" rather than "this session
	// has never been bound and its derived name is not to be trusted".
	_, err := ops.Get(store, context.Background(), GetSessionPointerRequest{
		TenantID: catalogTenant, SessionID: "session-never-created",
	})
	var keyspaceErr *KeyspaceError
	if !errors.As(err, &keyspaceErr) || keyspaceErr.Code != KeyspaceBindingNotFound {
		t.Fatalf("unbound read error = %T %v, want binding_not_found", err, err)
	}
}

// TestAPointerThisReaderCannotDecodeIsNotAnAbsentPointer pins the rule
// readSessionPointer's own comment states: a stored row this reader cannot
// decode is an ERROR on every path, never absence.
//
// The direction is what makes it worth a test. Absence licenses action — a read
// reports "this session has no checkpoint", a clear reports "there is nothing
// to clear", and a SET CREATES A FRESH RECORD at whatever epoch and sequence it
// named. That last one is the whole hazard: the two high-water marks would be
// silently reset by a writer that could not read them.
func TestAPointerThisReaderCannotDecodeIsNotAnAbsentPointer(t *testing.T) {
	t.Parallel()

	store, _ := pointerFixture(t, memstore.New())
	ops := pointerOperationMatrix()[1]
	ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch+5, 20, 1))
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatalf("deriveSessionScope: %v", err)
	}
	stored, err := store.backend.OrderedIndex.Get(context.Background(), sessionPointerID(scope, ops.Kind))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := store.backend.OrderedIndex.Update(
		context.Background(), sessionPointerID(scope, ops.Kind), stored.Revision,
		[]byte("{"), storage.Rank{}, storage.Due{}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// A set must not create over it, and must not be told the row is absent.
	_, err = ops.Set(store, context.Background(), ops.testSetRequest(t, 1, 1, 2))
	assertPointerField(PointerErrorMalformed, "record")(t, err)
	_, err = ops.Get(store, context.Background(), testGetPointerRequest())
	assertPointerField(PointerErrorMalformed, "record")(t, err)
	_, err = ops.Clear(store, context.Background(), testClearPointerRequest(1))
	assertPointerField(PointerErrorMalformed, "record")(t, err)
}

// --- the closed role set ----------------------------------------------------

// TestSessionPointerKindsAreTheDeclaredOnes holds sessionPointerKinds to the
// source, because everything else in this file ranges over it. A kind declared
// and left out of that slice would be a role with no test, no worst-case size
// measurement and no operation-matrix row, and every "for every kind" claim
// here would quietly stop covering it.
func TestSessionPointerKindsAreTheDeclaredOnes(t *testing.T) {
	t.Parallel()

	declared := map[string]bool{}
	file, err := parser.ParseFile(token.NewFileSet(), "pointers.go", nil, 0)
	if err != nil {
		t.Fatalf("parse pointers.go: %v", err)
	}
	for _, declaration := range file.Decls {
		generic, ok := declaration.(*ast.GenDecl)
		if !ok || generic.Tok != token.CONST {
			continue
		}
		for _, spec := range generic.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			named, ok := value.Type.(*ast.Ident)
			if !ok || named.Name != "SessionPointerKind" {
				continue
			}
			for _, literal := range value.Values {
				basic, ok := literal.(*ast.BasicLit)
				if !ok || basic.Kind != token.STRING {
					t.Fatalf("a SessionPointerKind constant is not a string literal: %v", literal)
				}
				text, err := strconv.Unquote(basic.Value)
				if err != nil {
					t.Fatalf("unquote: %v", err)
				}
				declared[text] = true
			}
		}
	}
	if len(declared) < 3 {
		t.Fatalf("found %d declared kinds (%v); the scan is not reaching the constants", len(declared), declared)
	}

	listed := map[string]bool{}
	for _, kind := range sessionPointerKinds() {
		if !declared[string(kind)] {
			t.Errorf("sessionPointerKinds names %q, which pointers.go does not declare", kind)
		}
		listed[string(kind)] = true
	}
	for kind := range declared {
		if !listed[kind] {
			t.Errorf("pointers.go declares the kind %q and sessionPointerKinds omits it", kind)
		}
	}
}

// TestEveryPointerKindNamesADistinctObjectKindAndUnknownFailsClosed asserts the
// mapping's three properties on behalf of every caller of it, positively.
//
// The negative half — that a wrong object kind is refused — is asserted by the
// cross product above, and it cannot distinguish a mapping that is correct from
// one that classifies nothing: a function returning false for everything
// refuses every wrong kind and every right one too. So totality is asserted
// here, and so is the direction an unclassified kind fails in.
func TestEveryPointerKindNamesADistinctObjectKindAndUnknownFailsClosed(t *testing.T) {
	t.Parallel()

	seen := map[ObjectKind]SessionPointerKind{}
	for _, kind := range sessionPointerKinds() {
		object, ok := kind.targetObjectKind()
		if !ok {
			t.Errorf("%s names no object kind; a role with no mapping can store nothing", kind)
			continue
		}
		if !object.valid() {
			t.Errorf("%s names %q, which is not an object kind this package has", kind, object)
		}
		if other, ok := seen[object]; ok {
			t.Errorf("%s and %s both name the object kind %q, so a filing check cannot tell them apart",
				other, kind, object)
		}
		seen[object] = kind
	}

	// An unclassified kind fails CLOSED, which is what makes the default arm a
	// decision rather than an oversight: it stores nothing at all rather than
	// accepting whatever object comes along.
	if object, ok := SessionPointerKind("invented").targetObjectKind(); ok || object != "" {
		t.Fatalf("an unknown role mapped to %q; it must fail closed", object)
	}
}

// --- nothing else reads a pointer -------------------------------------------

// TestNothingElseInThisPackageReadsAPointer is the structural half of "the
// continuation is reserved" and of "the catalog's summary is a copy": no
// operation in this package consults a pointer, so there is nothing a pointer
// can license here and nothing that can start depending on one without saying
// so.
//
// The identifiers are DERIVED rather than listed, and the derivation is
// "declared by pointers.go and by no other production file" — a name two files
// declare is not pointer-specific, and matching it would fire on an unrelated
// use. That subtraction is what lets this record declare a CheckpointSummary
// projection at all: the name belongs to catalog.go, so catalog.go using it is
// not a violation, while every genuinely pointer-specific name — the nine
// operations, the kernel, the record and its fences — is covered whether or not
// anyone updates this test.
func TestNothingElseInThisPackageReadsAPointer(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	// A file that declares nothing at all is skipped, and doing so costs the
	// sweep nothing: it can neither shadow one of these names nor contain a
	// call to one. It is asked as a property of the file rather than by name,
	// so package documentation moving between files does not break this.
	declaresSomething := func(filename string) bool {
		file, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", filename, err)
		}
		for _, declaration := range file.Decls {
			generic, ok := declaration.(*ast.GenDecl)
			if !ok || generic.Tok != token.IMPORT {
				return true
			}
		}
		return false
	}
	production := []string{}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") || name == "pointers.go" || !declaresSomething(name) {
			continue
		}
		production = append(production, name)
	}
	pointerIdentifiers := declaredNames(t, "pointers.go")
	for _, name := range production {
		for shared := range declaredNames(t, name) {
			delete(pointerIdentifiers, shared)
		}
	}

	used := func(filename string) map[string]bool {
		file, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", filename, err)
		}
		names := map[string]bool{}
		ast.Inspect(file, func(node ast.Node) bool {
			if ident, ok := node.(*ast.Ident); ok && pointerIdentifiers[ident.Name] {
				names[ident.Name] = true
			}
			return true
		})
		return names
	}

	// Anti-vacuity on the derived set: it must reach the nine operations and
	// the kernel, because those are what a later task wiring a pointer into
	// another record's write path would call.
	wanted := []string{"setPointer", "getPointer", "clearPointer", "SessionPointer", "pointerNamespace"}
	for _, ops := range pointerOperationMatrix() {
		wanted = append(wanted, "Set"+ops.Noun, "Get"+ops.Noun, "Clear"+ops.Noun)
	}
	for _, name := range wanted {
		if !pointerIdentifiers[name] {
			t.Fatalf("the derived set does not name %s; it is not reaching pointers.go's declarations", name)
		}
	}

	for _, name := range production {
		for identifier := range used(name) {
			t.Errorf("%s uses %s; nothing outside pointers.go may consult a pointer", name, identifier)
		}
	}
	if len(production) < 8 {
		t.Fatalf("only %d production files were inspected; the walk is not reaching them", len(production))
	}

	// THE POSITIVE CONTROL: the walk must find CALL SITES, not declarations. A
	// visitor that only reached declarations would report no violation anywhere
	// and leave the sweep above silent while a real cross-file call sat in the
	// package. This file calls all nine operations and declares none of them.
	callSites := used("pointers_test.go")
	for _, ops := range pointerOperationMatrix() {
		for _, verb := range []string{"Set", "Get", "Clear"} {
			if !callSites[verb+ops.Noun] {
				t.Fatalf("the walk does not find %s where it is CALLED; it is matching declarations only",
					verb+ops.Noun)
			}
		}
	}
	if declaredNames(t, "pointers_test.go")["SetWorkspaceCheckpointPointer"] {
		t.Fatal("the control file declares a name it is supposed only to call; it is no longer a control")
	}
}

// --- the pointer and the catalog's summary ----------------------------------

// TestClearingTheCatalogSummaryLeavesThePointerIntact drives the scenario
// UpdateCatalogHostState's replace-everything semantics were ACCEPTED on: "the
// catalog holds a summary while the authoritative retained high-water pointer
// is a separate epoch-fenced record, so clearing a summary here loses no
// durable state". Until this record existed there was no separate record, and
// the claim could not be tested. It can now, and it is the only thing that
// stops the catalog's most aggressive write from being a data-loss path.
func TestClearingTheCatalogSummaryLeavesThePointerIntact(t *testing.T) {
	t.Parallel()

	store, _ := pointerFixture(t, memstore.New())
	ops := pointerOperationMatrix()[1]
	if ops.Kind != SessionPointerWorkspaceCheckpoint {
		t.Fatalf("this case is about the workspace checkpoint, not %s", ops.Kind)
	}
	set := ops.testSetRequest(t, pointerEpoch, 12, 1)
	ops.mustSet(t, store, set)

	mustCreateCatalog(t, store)
	// A Host publishes the summary derived FROM the pointer, which is the only
	// way to build one from durable state.
	pointer := ops.mustGet(t, store)
	summary, err := pointer.Pointer.CheckpointSummary()
	if err != nil {
		t.Fatalf("CheckpointSummary: %v", err)
	}
	if summary.Reference != set.Target || summary.JournalSeq != 12 {
		t.Fatalf("the summary is not a projection of the pointer: %+v", summary)
	}
	withSummary := testHostStateRequest(pointerEpoch)
	withSummary.Checkpoint = summary
	if _, err := store.UpdateCatalogHostState(context.Background(), withSummary); err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}

	// The next projection write omits the checkpoint, which ZEROES the stored
	// summary. That is the accepted behaviour, and it must cost nothing.
	if _, err := store.UpdateCatalogHostState(
		context.Background(), testHostStateRequest(pointerEpoch)); err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}
	entry, err := store.GetCatalogEntry(context.Background(), GetCatalogEntryRequest{
		TenantID: catalogTenant, SessionID: catalogSession})
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if !entry.Record.Checkpoint.isZero() {
		t.Fatalf("the summary was not cleared, so this case proves nothing: %+v", entry.Record.Checkpoint)
	}

	// The authoritative record is untouched, and the Host can rebuild the
	// summary from it.
	after := ops.mustGet(t, store)
	if *after.Pointer.Target != set.Target || after.Pointer.Sequence != 12 {
		t.Fatalf("clearing the summary disturbed the pointer: %+v", after.Pointer)
	}
	rebuilt, err := after.Pointer.CheckpointSummary()
	if err != nil {
		t.Fatalf("CheckpointSummary: %v", err)
	}
	if rebuilt != summary {
		t.Fatalf("the rebuilt summary differs: %+v want %+v", rebuilt, summary)
	}
}

// TestTheCatalogSummaryIsNeverConsultedByAPointer is the other direction, and
// it is the one that keeps the two records from becoming a distributed
// agreement problem. A summary that is stale, absent, or plain wrong changes
// nothing a pointer decides: the fences are read from the pointer's own row.
func TestTheCatalogSummaryIsNeverConsultedByAPointer(t *testing.T) {
	t.Parallel()

	store, _ := pointerFixture(t, memstore.New())
	ops := pointerOperationMatrix()[1]
	mustCreateCatalog(t, store)
	ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch, 12, 1))

	// A summary naming an object that is NOT the pointer's, at a position ahead
	// of it. Nothing validates the two against each other, which is exactly the
	// divergence this case exists to characterise.
	divergent := testHostStateRequest(pointerEpoch)
	divergent.Checkpoint = CheckpointSummary{
		JournalSeq: 99,
		Reference:  testObjectReference(ObjectKindWorkspaceCheckpoint, 7),
		CapturedAt: pointerUpdatedAt,
	}
	if _, err := store.UpdateCatalogHostState(context.Background(), divergent); err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}

	// The pointer still says what it said, and its sequence fence is still its
	// own: a write at 12 is admitted although the summary claims 99.
	if entry := ops.mustGet(t, store); entry.Pointer.Sequence != 12 {
		t.Fatalf("the summary moved the pointer: %+v", entry.Pointer)
	}
	ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch, 12, 2))
	// And a write BELOW the pointer's own mark is still refused, although the
	// summary would have licensed anything up to 99.
	_, err := ops.Set(store, context.Background(), ops.testSetRequest(t, pointerEpoch, 11, 3))
	assertPointerField(PointerErrorSequence, "sequence")(t, err)
}

// TestCheckpointSummaryProjectsOnlyTheWorkspaceCheckpointPointer holds the one
// rule the catalog cannot enforce for itself. A summary's reference is
// validated there as an opaque ObjectID, so a runtime checkpoint or a
// continuation projected into it would be stored without complaint and no
// restore would be able to use it.
func TestCheckpointSummaryProjectsOnlyTheWorkspaceCheckpointPointer(t *testing.T) {
	t.Parallel()

	for _, kind := range sessionPointerKinds() {
		pointer := testSessionPointer()
		pointer.Kind = kind
		object, _ := kind.targetObjectKind()
		target := testObjectReference(object, 1)
		pointer.Target = &target

		summary, err := pointer.CheckpointSummary()
		if kind != SessionPointerWorkspaceCheckpoint {
			assertPointerField(PointerErrorInvalid, "kind")(t, err)
			continue
		}
		if err != nil {
			t.Fatalf("the workspace checkpoint pointer did not project: %v", err)
		}
		if summary.Reference != target || summary.JournalSeq != pointer.Sequence ||
			!summary.CapturedAt.Equal(pointer.UpdatedAt) {
			t.Fatalf("projection = %+v, want the pointer's own members", summary)
		}
		if summary.isZero() {
			t.Fatal("a live pointer projected to the summary that means NO checkpoint")
		}
	}

	// A CLEARED pointer projects to the zero summary and no error, because the
	// zero summary is exactly what "no checkpoint has been committed" is
	// spelled as. That is how a clear reaches the catalog on the next
	// projection write.
	cleared := testSessionPointer()
	cleared.Target = nil
	summary, err := cleared.CheckpointSummary()
	if err != nil {
		t.Fatalf("a cleared pointer did not project: %v", err)
	}
	if !summary.isZero() {
		t.Fatalf("a cleared pointer projected %+v, want the zero summary", summary)
	}
}

// --- the objects a pointer names --------------------------------------------

// TestMovingAPointerNeverTouchesAnObject is step 1's physical immutability
// case, driven rather than argued. A pointer is a NAME: replacing one and
// clearing one leave every object this session has exactly where it was, byte
// for byte, and neither operation performs any blob traffic at all — which is
// the stronger statement, because an operation that reads no blob cannot
// corrupt one however it is later changed.
func TestMovingAPointerNeverTouchesAnObject(t *testing.T) {
	t.Parallel()

	instrumented, calls := instrumentComposite(memstore.New())
	store, _ := pointerFixture(t, instrumented)
	ops := pointerOperationMatrix()[1]
	object, _ := ops.Kind.targetObjectKind()

	put := func(body []byte) sessionwire.ObjectMetadata {
		t.Helper()
		digest := sha256.Sum256(body)
		metadata, err := store.PutObject(context.Background(), PutObjectRequest{
			TenantID:  catalogTenant,
			SessionID: catalogSession,
			Kind:      object,
			SizeBytes: uint64(len(body)),
			SHA256:    digest,
			Body:      bytes.NewReader(body),
		})
		if err != nil {
			t.Fatalf("PutObject: %v", err)
		}
		return metadata
	}
	read := func(metadata sessionwire.ObjectMetadata) []byte {
		t.Helper()
		reader, err := store.GetObject(context.Background(), GetObjectRequest{
			TenantID: catalogTenant, SessionID: catalogSession, ExpectedKind: object, Metadata: metadata,
		})
		if err != nil {
			t.Fatalf("GetObject: %v", err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("read object: %v", err)
		}
		if err := reader.Close(); err != nil {
			t.Fatalf("close object: %v", err)
		}
		return body
	}

	firstBody := []byte("the checkpoint a pointer stops naming")
	secondBody := []byte("the checkpoint it names instead")
	first, second := put(firstBody), put(secondBody)

	before := calls.snapshot().Blobs
	set := SetSessionPointerRequest{
		TenantID: catalogTenant, SessionID: catalogSession,
		LeaseEpoch: pointerEpoch, Sequence: 10, Target: first.Reference,
	}
	ops.mustSet(t, store, set)
	set.Sequence, set.Target = 11, second.Reference
	ops.mustSet(t, store, set)
	ops.mustGet(t, store)
	if _, err := ops.Clear(store, context.Background(), testClearPointerRequest(pointerEpoch)); err != nil {
		t.Fatalf("Clear%s: %v", ops.Noun, err)
	}
	if after := calls.snapshot().Blobs; after != before {
		t.Fatalf("pointer operations made %d blob calls; a pointer is a name, not an object", after-before)
	}

	// Both objects are still there, unchanged — including the one no pointer
	// names any more, and including after the pointer was cleared entirely.
	if got := read(first); !bytes.Equal(got, firstBody) {
		t.Fatalf("the superseded object changed: %q", got)
	}
	if got := read(second); !bytes.Equal(got, secondBody) {
		t.Fatalf("the named object changed: %q", got)
	}
}

// --- concurrency ------------------------------------------------------------

// TestTwoHostsRaceForOnePointer drives the race the revision compare-and-swap
// exists for. Whatever interleaving occurs, the record that survives is one
// that was actually written, both marks are monotone, and no loser is told it
// won.
func TestTwoHostsRaceForOnePointer(t *testing.T) {
	t.Parallel()

	store, _ := pointerFixture(t, memstore.New())
	ops := pointerOperationMatrix()[1]
	object, _ := ops.Kind.targetObjectKind()
	ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch, 10, 1))

	const writers = 8
	var wg sync.WaitGroup
	winners := make([]SessionPointerEntry, writers)
	failures := make([]error, writers)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := SetSessionPointerRequest{
				TenantID: catalogTenant, SessionID: catalogSession,
				LeaseEpoch: pointerEpoch + uint64(i%2),
				Sequence:   uint64(11 + i),
				Target:     testObjectReference(object, byte(i+2)),
			}
			winners[i], failures[i] = ops.Set(store, context.Background(), req)
		}()
	}
	wg.Wait()

	final := ops.mustGet(t, store)
	accepted := 0
	for i, err := range failures {
		if err == nil {
			accepted++
			continue
		}
		// Every refusal is one of the three this record can produce, and each
		// says something true about the state that beat it.
		var typed *PointerError
		if !errors.As(err, &typed) {
			t.Fatalf("writer %d failed with %T %v", i, err, err)
		}
		switch typed.Code {
		case PointerErrorConflict, PointerErrorSequence, PointerErrorEpoch:
		default:
			t.Fatalf("writer %d failed with %q", i, typed.Code)
		}
	}
	if accepted == 0 {
		t.Fatal("no writer won; the race proves nothing")
	}
	if final.Pointer.Sequence < 10 || final.Pointer.LeaseEpoch < pointerEpoch {
		t.Fatalf("a high-water fell under contention: %+v", final.Pointer)
	}
	// The surviving record is one somebody actually wrote, at the sequence that
	// writer named.
	survived := false
	for _, entry := range winners {
		if entry.Pointer.Target != nil && *entry.Pointer.Target == *final.Pointer.Target &&
			entry.Pointer.Sequence == final.Pointer.Sequence {
			survived = true
		}
	}
	if !survived {
		t.Fatalf("the stored pointer %+v was never returned to any writer", final.Pointer)
	}
}

// TestAnUnmappableRoleIsRefusedWithoutWriting drives what the three kernel
// operations actually do with a role that has no object-kind mapping.
//
// It exists because the answers are NOT the same and a comment said they were.
// A set is refused as invalid, by the encoder, before any provider work. A read
// and a clear report not_found, because they read first and a row of an
// unmappable role can never exist — the set that would have created one is
// refused. All three are fail-closed and none of them writes, which is the
// property that matters and the one asserted here.
//
// The case is unreachable through the public surface: the role is the METHOD's,
// not the caller's. It is driven through the kernel for exactly that reason —
// what a later kind added without a mapping would meet is otherwise untested.
func TestAnUnmappableRoleIsRefusedWithoutWriting(t *testing.T) {
	t.Parallel()

	const unmappable = SessionPointerKind("invented")
	if _, ok := unmappable.targetObjectKind(); ok {
		t.Fatal("the fixture role is mapped; this case proves nothing")
	}

	base := memstore.New()
	recorder := &recordingOrdered{OrderedIndex: base.OrderedIndex}
	base.OrderedIndex = recorder
	store, _ := pointerFixture(t, base)
	// A real pointer of a real role exists, so the session is bound and any
	// refusal below is about the role rather than about the session.
	ops := pointerOperationMatrix()[1]
	ops.mustSet(t, store, ops.testSetRequest(t, pointerEpoch, pointerSequence, 1))
	recorder.reset()

	_, err := store.setPointer(context.Background(), unmappable, ops.testSetRequest(t, pointerEpoch, pointerSequence, 1))
	assertPointerField(PointerErrorInvalid, "kind")(t, err)
	// A set is refused by the encoder, so it reaches no provider at all.
	if calls := recorder.snapshot(); len(calls) != 0 {
		t.Fatalf("a set of an unmappable role reached the provider: %+v", calls)
	}

	_, err = store.getPointer(context.Background(), unmappable, testGetPointerRequest())
	assertPointerField(PointerErrorNotFound, "record")(t, err)
	_, err = store.clearPointer(context.Background(), unmappable, testClearPointerRequest(pointerEpoch))
	assertPointerField(PointerErrorNotFound, "record")(t, err)

	// The read and the clear read, and that is all they do: no create, no
	// update, and nothing of the real pointer disturbed.
	for _, call := range recorder.snapshot() {
		if call.op != "get" {
			t.Fatalf("an unmappable role reached a %q; only reads are allowed to happen", call.op)
		}
	}
	if entry := ops.mustGet(t, store); entry.Pointer.LeaseEpoch != pointerEpoch {
		t.Fatalf("an unmappable role disturbed a real pointer: %+v", entry.Pointer)
	}
}
