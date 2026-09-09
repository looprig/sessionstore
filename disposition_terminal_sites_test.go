package sessionstore

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/looprig/storage/memstore"
)

// everyDispositionState is the durable state space, written down ONCE. It is the
// only hand-maintained list in this file; the terminal subset, the settlement
// kinds that reach it and the call sites that read it are all derived from it or
// from an authority independent of the predicate under test.
var everyDispositionState = []InboxState{
	InboxStatePending,
	InboxStateClaimed,
	InboxStateApplying,
	InboxStateApplied,
	InboxStateRejected,
}

// TestInboxStateTerminalPartitionsTheStateSpace pins the predicate itself over
// the whole domain rather than at the two states a caller happened to reach.
// It is the innermost of the three checks in this file: terminal() must admit
// exactly the settled states and refuse exactly the in-flight ones.
func TestInboxStateTerminalPartitionsTheStateSpace(t *testing.T) {
	t.Parallel()
	settled := []InboxState{InboxStateApplied, InboxStateRejected}
	for _, state := range everyDispositionState {
		want := slices.Contains(settled, state)
		if got := state.terminal(); got != want {
			t.Errorf("%q.terminal() = %v, want %v", state, got, want)
		}
	}
	if len(everyDispositionState) != 5 || len(settled) != 2 {
		t.Fatalf("vacuous: %d states, %d settled — the domain is no longer the one this test was derived over", len(everyDispositionState), len(settled))
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
		fn:   "currentDispositionEntry",
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
		fn:   "SettleDispositionCommand",
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
	for _, state := range everyDispositionState {
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

// TestEveryDispositionTerminalCallSiteIsDriven is the reader that stops the
// table from growing one row per review.
//
// It parses the disposition protocol's production sources, collects every
// enclosing function that calls terminal(), and requires that set to EQUAL the
// set dispositionTerminalSites drives. A fourth call site cannot be added
// without this failing, and a driver cannot outlive the site it was written for.
// Without it, "every call site" is a claim about code nobody re-reads.
//
// The scope is the disposition protocol, and that is a deliberate line rather
// than the reach of the file-name prefix. terminal() is also read by the LEGACY
// command protocol, in inbox.go and inbox_claim.go, whose own suite drives both
// states through those sites; folding them in here would put one property under
// two owners. What this guard must never allow is a FOURTH disposition site
// appearing with no driver, which is exactly how the three known ones came to be
// read for one state each.
func TestEveryDispositionTerminalCallSiteIsDriven(t *testing.T) {
	t.Parallel()
	found, err := terminalCallSites(".", "disposition_")
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
		if !slices.Contains(driven, fn) {
			t.Errorf("%s reads InboxState.terminal() and no case in dispositionTerminalSites drives it; derive the cross-product rather than adding one row", fn)
		}
	}
	for _, fn := range driven {
		if !slices.Contains(found, fn) {
			t.Errorf("dispositionTerminalSites drives %s, which no longer reads InboxState.terminal(); the driver has outlived its site", fn)
		}
	}
}

// terminalCallSites reports the names of the functions in root's production
// files whose base name starts with prefix that call a method named "terminal".
//
// It is deliberately syntactic. It cannot resolve types, so it would also match
// some other type's terminal() — which fails CLOSED, by demanding a driver for a
// site that does not need one, and is the safe direction for a guard whose
// purpose is to notice a site nobody thought about.
func terminalCallSites(root, prefix string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var sites []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, filepath.Join(root, name), nil, 0)
		if err != nil {
			return nil, err
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "terminal" || len(call.Args) != 0 {
					return true
				}
				if !slices.Contains(sites, fn.Name.Name) {
					sites = append(sites, fn.Name.Name)
				}
				return true
			})
		}
	}
	return sites, nil
}
