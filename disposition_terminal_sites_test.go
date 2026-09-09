package sessionstore

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/looprig/storage/memstore"
)

// everyDispositionState is the durable state space, DERIVED from inbox.go's
// constant declarations rather than written down here.
//
// It used to be a literal of five, guarded by a `!= 5` count. That guard was
// worth less than it looked: a count cannot see the enum GROW — add a sixth
// state and the literal keeps five, the count keeps passing, and every "for all
// states" property in this file silently becomes "for the five it was shown".
// declaredInboxStates already parses those constants for the legacy state
// machine, so the derivation costs one call. Sorted for a stable subtest order.
func everyDispositionState(t *testing.T) []InboxState {
	t.Helper()
	declared := declaredInboxStates(t)
	states := make([]InboxState, 0, len(declared))
	for name := range declared {
		states = append(states, InboxState(name))
	}
	slices.Sort(states)
	if len(states) < 2 {
		t.Fatalf("vacuous: inbox.go declares %d states", len(states))
	}
	return states
}

// TestInboxStateTerminalPartitionsTheStateSpace pins the predicate itself over
// the whole domain rather than at the two states a caller happened to reach.
// It is the innermost of the three checks in this file: terminal() must admit
// exactly the settled states and refuse exactly the in-flight ones.
func TestInboxStateTerminalPartitionsTheStateSpace(t *testing.T) {
	t.Parallel()
	settled := []InboxState{InboxStateApplied, InboxStateRejected}
	states := everyDispositionState(t)
	for _, state := range states {
		want := slices.Contains(settled, state)
		if got := state.terminal(); got != want {
			t.Errorf("%q.terminal() = %v, want %v", state, got, want)
		}
	}
	// Not a count. Every state inbox.go declares must have been ruled on above,
	// and every state named settled must be one it declares — so a sixth
	// constant fails here instead of being silently excluded from the domain.
	for _, state := range settled {
		if !slices.Contains(states, state) {
			t.Errorf("%q is named settled here and inbox.go does not declare it", state)
		}
	}
	if len(states) <= len(settled) {
		t.Fatalf("vacuous: %d declared states, %d of them settled — nothing in-flight is being ruled on", len(states), len(settled))
	}
}

// dispositionSettlementKinds maps every settlement outcome this protocol can
// produce to the evidence that produces it.
//
// This is the DOMAIN of the cross-product below, and it is taken from the
// outcome vocabulary rather than from terminal() on purpose. A test whose domain
// is computed from the predicate it probes shrinks silently when that predicate
// is narrowed — which is exactly the mutation at issue — so the domain must come
// from somewhere else. DispositionOutcomeKind.terminalState() is that somewhere:
// it is the independent authority on which state each settlement stores.
//
// It IS hand-maintained, and an earlier comment above wrongly called
// everyDispositionState "the only hand-maintained list in this file" while this
// map sat below it with no staleness guard at all. It has one now:
// TestDispositionSettlementKindsCoverTheDeclaredVocabulary parses the kind
// constants out of disposition_settlement.go and requires this map's keys to
// equal them, so a fourth outcome kind cannot be added without something
// noticing that no site is ever driven with it.
var dispositionSettlementKinds = map[DispositionOutcomeKind]func() DispositionEvidence{
	DispositionApplied:    appliedEvidence,
	DispositionNoOp:       noOpEvidence,
	DispositionNotApplied: notAppliedEvidence,
}

// settleDispositionOfKind runs one command all the way to the terminal state
// that kind produces, and hands back the store, the applying entry and the
// settled entry so a site driver can exercise the command before and after.
func settleDispositionOfKind(t *testing.T, kind DispositionOutcomeKind, opts ...Option) (*Store, DispositionInboxEntry, DispositionInboxEntry) {
	t.Helper()
	evidence, ok := dispositionSettlementKinds[kind]
	if !ok {
		t.Fatalf("no evidence for kind %q", kind)
	}
	reader := &fakeEvidence{evidence: evidence()}
	opts = append([]Option{WithClock(newMovableClock(settlementNow)), WithDispositionEvidence(reader)}, opts...)
	s := openStore(t, memstore.New(), opts...)
	createDispositionCatalog(t, s)
	admitted, _, err := s.AdmitDispositionCommand(context.Background(), dispositionRequest())
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	claimed := fileDispositionClaim(t, s, admitted, DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: settlementExpiry})
	applying, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed))
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	settled, done, err := s.SettleDispositionCommand(context.Background(), settleRequest(applying, settlementResidenc))
	if err != nil || !done {
		t.Fatalf("settle %q: %+v %v %v", kind, settled, done, err)
	}
	if settled.Record.State != kind.terminalState() {
		t.Fatalf("settled state = %q, want %q for kind %q", settled.Record.State, kind.terminalState(), kind)
	}
	return s, applying, settled
}

// dispositionTerminalSite is one place the disposition protocol reads
// InboxState.terminal(). fn is the enclosing PRODUCTION function name, and it is
// not a label: TestEveryDispositionTerminalCallSiteIsDriven parses the sources
// and requires this set to equal the set of call sites that actually exist.
type dispositionTerminalSite struct {
	// fn is the QUALIFIED declaration: "(*Store).SettleDispositionCommand" for a
	// method, a bare name for a plain function. The qualification is the fix for
	// a real escape — keying on the bare declared name let a new, undriven site
	// inside a method of the SAME name on ANOTHER type be attributed to this
	// driver and pass. The receiver is part of the identity of a call site.
	fn    string
	what  string
	drive func(t *testing.T, kind DispositionOutcomeKind)
}

// dispositionTerminalSites is the cross-product's other axis.
//
// "A settled command is finished" means something DIFFERENT at each of these,
// and a table per site grows one row per review — which is how `applied` came to
// be the only state any of the three read. The previous round found the attempt
// edge read one state, fixed that site, and the same blindness was still sitting
// at the other two. Deriving the set is the fix; adding rows is not.
var dispositionTerminalSites = []dispositionTerminalSite{
	{
		fn:   "(*Store).currentDispositionEntry",
		what: "the attempt edge refuses a settled command as terminal, not as not-claimed",
		drive: func(t *testing.T, kind DispositionOutcomeKind) {
			s, _, settled := settleDispositionOfKind(t, kind)
			req := beginRequest(settled)
			req.AttemptID = "attempt/after-settlement"
			_, err := s.BeginDispositionAttempt(context.Background(), req)
			assertInboxCode(t, err, InboxErrorTerminal)
			got, err := s.GetDispositionCommand(context.Background(), GetDispositionCommandRequest{TenantID: req.TenantID, SessionID: req.SessionID, CommandID: req.CommandID})
			if err != nil || got.Revision != settled.Revision || *got.Record.Outcome != *settled.Record.Outcome {
				t.Fatalf("refused attempt disturbed a settled record: %+v %v", got, err)
			}
		},
	},
	{
		fn:   "(*Store).SettleDispositionCommand",
		what: "re-settling a settled command at its own revision is idempotent and reads no evidence",
		drive: func(t *testing.T, kind DispositionOutcomeKind) {
			s, _, settled := settleDispositionOfKind(t, kind)
			again, done, err := s.SettleDispositionCommand(context.Background(), settleRequest(settled, settlementResidenc))
			if err != nil || done {
				t.Fatalf("idempotent re-settle: %+v %v %v", again, done, err)
			}
			if again.Revision != settled.Revision || again.Record.State != settled.Record.State || *again.Record.Outcome != *settled.Record.Outcome {
				t.Fatalf("re-settle did not return the settled record unchanged: %+v, want %+v", again, settled)
			}
		},
	},
	{
		fn:   "dispositionInboxDue",
		what: "a settled command leaves the due view",
		drive: func(t *testing.T, kind DispositionOutcomeKind) {
			// The store must be single-shard or the command need not land in
			// shard 0 and "nothing is due" would pass for the wrong reason.
			s, _, _ := settleDispositionOfKind(t, kind, WithControlShards(1))
			page, err := s.ListDueDispositionCommands(context.Background(), ListDueDispositionCommandsRequest{Shard: 0, DueAtOrBefore: inboxDeadline, Limit: 10})
			if err != nil || len(page.Commands) != 0 || page.Examined != 0 {
				t.Fatalf("settled command still due: %+v %v", page, err)
			}
		},
	},
}

// TestDispositionTerminalStatesAreReadAtEveryCallSite drives the cross-product
// of every settlement kind against every call site that reads terminal().
//
// The positive control is structural rather than a row: settleDispositionOfKind
// asserts the command actually reached kind.terminalState() before any site is
// exercised, so a fixture that silently failed to settle cannot make a
// "nothing is due" or "the refusal fired" assertion pass for the wrong reason.
// The due site carries a second, sharper one below, because its assertion is a
// pure absence.
func TestDispositionTerminalStatesAreReadAtEveryCallSite(t *testing.T) {
	t.Parallel()
	if len(dispositionSettlementKinds) == 0 || len(dispositionTerminalSites) == 0 {
		t.Fatal("vacuous: the cross-product is empty")
	}
	reached := map[InboxState]bool{}
	for kind := range dispositionSettlementKinds {
		if !kind.valid() {
			t.Fatalf("kind %q is not a valid settlement outcome", kind)
		}
		reached[kind.terminalState()] = true
	}
	// The kinds must between them reach EVERY settled state, or the
	// cross-product is a sample of the domain again.
	for _, state := range everyDispositionState(t) {
		if state.terminal() && !reached[state] {
			t.Fatalf("no settlement kind reaches terminal state %q; the domain is incomplete", state)
		}
	}
	for kind := range dispositionSettlementKinds {
		for _, site := range dispositionTerminalSites {
			t.Run(string(kind)+"/"+site.fn, func(t *testing.T) {
				t.Parallel()
				site.drive(t, kind)
			})
		}
	}
}

// TestDispositionDueViewPositiveControl is the due site's positive control, kept
// as its own test because its whole job is to fail if the negative assertion
// above could pass vacuously. An unsettled command MUST be due in the same
// fixture, same shard, same window; if it is not, "nothing is due after
// settlement" proves nothing.
func TestDispositionDueViewPositiveControl(t *testing.T) {
	t.Parallel()
	reader := &fakeEvidence{evidence: appliedEvidence()}
	s := openStore(t, memstore.New(), WithClock(newMovableClock(settlementNow)), WithControlShards(1), WithDispositionEvidence(reader))
	createDispositionCatalog(t, s)
	admitted, _, err := s.AdmitDispositionCommand(context.Background(), dispositionRequest())
	if err != nil {
		t.Fatal(err)
	}
	claimed := fileDispositionClaim(t, s, admitted, DispositionClaim{ResidencyEpoch: settlementResidenc, ExpiresAt: settlementExpiry})
	if _, err := s.BeginDispositionAttempt(context.Background(), beginRequest(claimed)); err != nil {
		t.Fatal(err)
	}
	page, err := s.ListDueDispositionCommands(context.Background(), ListDueDispositionCommandsRequest{Shard: 0, DueAtOrBefore: inboxDeadline, Limit: 10})
	if err != nil || len(page.Commands) != 1 {
		t.Fatalf("POSITIVE CONTROL: an applying command is not due in this fixture, so the settled-command absence assertion proves nothing: %+v %v", page, err)
	}
}

// dispositionTerminalExclusions are the terminal() call sites this file
// deliberately does NOT drive, each recorded with the test that does.
//
// They are the LEGACY command protocol. Folding them into the cross-product
// would put one property under two owners; excluding them silently would make
// "every call site" a sentence about a subset. So they are named here, and each
// name is a measurement rather than a claim: mutating each site to
// `== InboxStateApplied` was run, and each dies to exactly the test named.
//
// A stale exclusion is an error too — see below — so an entry cannot outlive
// the site it excuses.
var dispositionTerminalExclusions = map[string]string{
	"inboxDue":                   "TestTerminalTransitionsLeaveTheCommandNotDueAndDirectlyGettable and FuzzInboxRecordCodec",
	"(*Store).currentInboxEntry": "TestCommandStateMachineAdmitsExactlyItsTransitions",
}

// TestEveryDispositionTerminalCallSiteIsDriven is the reader that stops the
// table from growing one row per review.
//
// # What it keys on, and therefore what it cannot see
//
// The unit of analysis is the QUALIFIED DECLARATION — "(*Store).Method" or a
// bare function name — over EVERY production .go file in the package root. Both
// halves of that were escapes and are stated because a guard's docstring must
// name its mechanism:
//
//   - Keying on the bare name let an undriven site inside a same-named method on
//     another type be attributed to an existing driver and pass.
//   - Scanning a FILENAME PREFIX while the comment said "the disposition
//     protocol" meant the same site in a file named otherwise — inbox_recovery.go
//     was the gate's example — was invisible. The mechanism is widened rather
//     than the sentence narrowed: the scan is now package-wide and the legacy
//     sites are named exclusions, so a new site in a new file fails by default.
//
// It still cannot see: a call in another package, in a sub-package directory, or
// through a function value rather than a direct call. Those are outside a
// syntactic scan and are stated rather than implied.
//
// And it is partly a SPELLING LOCK, which is a real cost and is accepted
// knowingly. Inlining the predicate at a site — writing `== applied || ==
// rejected` in place of `terminal()` — is behaviourally equivalent and this test
// still fails it, as "the driver has outlived its site". That is the price of a
// syntactic guard: it pins how the property is written, not only that it holds.
// It is worth paying here because the failure it prevents is silent and the
// failure it causes is loud and takes one line to resolve — but a reader hitting
// it should know it is an equivalent mutant being refused, not a defect.
//
// # Which way it errs
//
// CLOSED, in every direction it has. The scan cannot resolve types, so it may
// match some other type's terminal() — that demands a driver for a site that may
// not need one. A call it cannot attribute to a declaration is recorded under a
// sentinel that is in neither the driven nor the excluded set, so it fails
// rather than disappearing. An undriven site fails; a driver whose site is gone
// fails; an exclusion whose site is gone fails. There is no arm on which an
// unrecognised call passes.
func TestEveryDispositionTerminalCallSiteIsDriven(t *testing.T) {
	t.Parallel()
	found, err := terminalCallSites(".")
	if err != nil {
		t.Fatalf("scan for terminal() call sites: %v", err)
	}
	if len(found) == 0 {
		t.Fatal("vacuous: the scanner found no terminal() call sites at all")
	}
	driven := make([]string, 0, len(dispositionTerminalSites))
	for _, site := range dispositionTerminalSites {
		driven = append(driven, site.fn)
	}
	slices.Sort(found)
	slices.Sort(driven)
	for _, fn := range found {
		if slices.Contains(driven, fn) {
			continue
		}
		if by, ok := dispositionTerminalExclusions[fn]; ok {
			if by == "" {
				t.Errorf("%s is excluded with no test named as covering it", fn)
			}
			continue
		}
		t.Errorf("%s reads InboxState.terminal() and nothing drives it: add it to dispositionTerminalSites, or to dispositionTerminalExclusions naming the test that covers it. Derive the cross-product rather than adding one row", fn)
	}
	for _, fn := range driven {
		if !slices.Contains(found, fn) {
			t.Errorf("dispositionTerminalSites drives %s, which no longer reads InboxState.terminal(); the driver has outlived its site", fn)
		}
	}
	for fn := range dispositionTerminalExclusions {
		if !slices.Contains(found, fn) {
			t.Errorf("dispositionTerminalExclusions excuses %s, which no longer reads InboxState.terminal(); the exclusion has outlived its site", fn)
		}
	}
}

// terminalCallSites reports the qualified declarations, across every production
// .go file in root, that call a method named "terminal" with no arguments.
//
// A method is reported as "(*Recv).Name" or "Recv.Name"; a plain function as
// "Name". A matching call that is not inside a top-level function declaration —
// a package-level var initializer, say — is reported under the sentinel
// "<unattributed>", which no driver and no exclusion names, so it FAILS. That is
// the direction a guard of this kind must err in: something it cannot explain
// must not be something it ignores.
func terminalCallSites(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var sites []string
	add := func(name string) {
		if !slices.Contains(sites, name) {
			sites = append(sites, name)
		}
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, filepath.Join(root, name), nil, 0)
		if err != nil {
			return nil, err
		}
		// Attribute by walking each declaration, then compare against a
		// file-wide count so a call outside every FuncDecl cannot be lost.
		attributed := 0
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if isTerminalCall(n) {
					attributed++
					add(qualifiedFuncName(fn))
				}
				return true
			})
		}
		total := 0
		ast.Inspect(file, func(n ast.Node) bool {
			if isTerminalCall(n) {
				total++
			}
			return true
		})
		if total > attributed {
			add("<unattributed>")
		}
	}
	return sites, nil
}

func isTerminalCall(n ast.Node) bool {
	call, ok := n.(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "terminal" && len(call.Args) == 0
}

// qualifiedFuncName renders a declaration the way dispositionTerminalSites and
// dispositionTerminalExclusions spell one. The receiver is part of the identity:
// two methods of the same name on different types are two call sites.
func qualifiedFuncName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	switch receiver := fn.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if ident, ok := receiver.X.(*ast.Ident); ok {
			return "(*" + ident.Name + ")." + fn.Name.Name
		}
	case *ast.Ident:
		return receiver.Name + "." + fn.Name.Name
	}
	// An unrecognised receiver spelling is reported as unattributable rather
	// than collapsed to the bare name, which is the escape this guard just closed.
	return "<unattributed>"
}

// TestDispositionSettlementKindsCoverTheDeclaredVocabulary is the staleness
// guard dispositionSettlementKinds lacked. The cross-product's domain is that
// map, so a kind the map does not name is a settlement outcome no call site is
// ever driven with — the same "for all X defended by a sample" shape as the call
// sites, one axis over.
func TestDispositionSettlementKindsCoverTheDeclaredVocabulary(t *testing.T) {
	t.Parallel()
	declared := declaredDispositionOutcomeKinds(t)
	for kind := range declared {
		if _, ok := dispositionSettlementKinds[DispositionOutcomeKind(kind)]; !ok {
			t.Errorf("disposition_settlement.go declares the outcome kind %q and no evidence in dispositionSettlementKinds produces it", kind)
		}
	}
	for kind := range dispositionSettlementKinds {
		if !declared[string(kind)] {
			t.Errorf("dispositionSettlementKinds names %q, which disposition_settlement.go no longer declares", kind)
		}
	}
}

// declaredDispositionOutcomeKinds enumerates the DispositionOutcomeKind
// constants from source, the way declaredInboxStates does for the states. The
// type is named on the spec, so a const block that also declares something else
// cannot smuggle a non-kind in.
func declaredDispositionOutcomeKinds(t *testing.T) map[string]bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "disposition_settlement.go", nil, 0)
	if err != nil {
		t.Fatalf("parse disposition_settlement.go: %v", err)
	}
	kinds := map[string]bool{}
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, spec := range general.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			if name, ok := value.Type.(*ast.Ident); !ok || name.Name != "DispositionOutcomeKind" {
				continue
			}
			for _, literal := range value.Values {
				text, ok := literal.(*ast.BasicLit)
				if !ok || text.Kind != token.STRING {
					continue
				}
				unquoted, err := strconv.Unquote(text.Value)
				if err != nil {
					t.Fatalf("unquote %s: %v", text.Value, err)
				}
				kinds[unquoted] = true
			}
		}
	}
	if len(kinds) == 0 {
		t.Fatal("no DispositionOutcomeKind constants were found; the enumerator is not reaching the declarations")
	}
	return kinds
}
