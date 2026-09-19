package sessionstore

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage/memstore"
)

var updateLegacyGateTranscript = flag.Bool("update-legacy-gate-transcript", false,
	"rewrite testdata/legacy_gate_transcript.golden from the current code")

// TestLegacyGateWritesAreByteIdenticalToV0110 replays one fixed script of gate
// writes and reads over a LEGACY session — both the unbound version-1 record
// and a record bound to ProtocolModeLegacy — and compares everything the store
// answered and stored against a transcript captured from the released v0.11.0
// code before v0.12.0 touched the gate path.
//
// "Everything" is: each call's typed error (code, field, epoch, revision and
// message), each returned entry's revision and canonical record bytes, the
// provider's raw stored catalog bytes, every deadline intent's raw bytes and due
// state, and every gate page. That is what "a legacy caller's behaviour is
// byte-identical" means, measured rather than asserted: v0.12.0 added a
// protocol-mode dispatch in front of the legacy fence, and a dispatch that
// reordered one refusal or wrote one extra member would show here as a diff.
//
// The golden was written by running this test with
// -update-legacy-gate-transcript at v0.11.0 (329c68f), before any production
// change; it must never be regenerated to make a v0.12.0 diff pass.
func TestLegacyGateWritesAreByteIdenticalToV0110(t *testing.T) {
	var transcript strings.Builder
	for _, bound := range []bool{false, true} {
		fmt.Fprintf(&transcript, "== bound=%v\n", bound)
		legacyGateScript(t, &transcript, bound)
	}
	path := filepath.Join("testdata", "legacy_gate_transcript.golden")
	if *updateLegacyGateTranscript {
		if err := os.WriteFile(path, []byte(transcript.String()), 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if got := transcript.String(); got != string(want) {
		t.Fatalf("legacy gate transcript diverged from v0.11.0\n--- got\n%s\n--- want\n%s", got, want)
	}
}

func legacyGateScript(t *testing.T, out *strings.Builder, bound bool) {
	t.Helper()
	backend := memstore.New()
	store := openStore(t, backend, WithClock(fixedClock{registryObservedAt}))
	ctx := context.Background()
	create := testCreateRequest()
	if bound {
		create.Binding = testSessionBinding()
		create.Binding.ProtocolMode = ProtocolModeLegacy
	}
	if _, _, err := store.CreateCatalogEntry(ctx, create); err != nil {
		t.Fatalf("CreateCatalogEntry: %v", err)
	}
	host := testHostStateRequest(2)
	host.LastJournalSeq = 5
	host.LastEventID = "event-5"
	if _, err := store.UpdateCatalogHostState(ctx, host); err != nil {
		t.Fatalf("UpdateCatalogHostState: %v", err)
	}
	open := func(name string, epoch uint64, gate sessionwire.GateProjection) {
		entry, err := store.OpenGate(ctx, OpenGateRequest{
			TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: epoch, Gate: gate})
		transcribeCatalogCall(t, out, name, entry, err)
	}
	resolve := func(name string, epoch uint64, gate sessionwire.GateID) {
		entry, err := store.ResolveGate(ctx, ResolveGateRequest{
			TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: epoch, GateID: gate})
		transcribeCatalogCall(t, out, name, entry, err)
	}
	read := func(name string) {
		page, err := store.ReadGates(ctx, ReadGatesRequest{TenantID: catalogTenant, SessionID: catalogSession})
		fmt.Fprintf(out, "%s err=%s\n", name, describeCatalogErr(err))
		if err == nil {
			encoded, err := json.Marshal(page)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(out, "%s page=%s\n", name, encoded)
		}
	}

	open("open zero epoch", 0, testGate("gate-a", 3))
	open("open a", 2, testGate("gate-a", 3))
	open("open a replay", 2, testGate("gate-a", 3))
	open("open b same seq", 2, testGate("gate-b", 3))
	open("open c above tip", 2, testGate("gate-c", 9))
	open("open d stale", 1, testGate("gate-d", 4))
	open("open d ratchet", 3, testGate("gate-d", 4))
	read("read after opens")
	resolve("resolve a stale", 2, "gate-a")
	resolve("resolve zero epoch", 0, "gate-a")
	resolve("resolve a", 3, "gate-a")
	resolve("resolve a replay", 3, "gate-a")
	resolve("resolve never opened", 3, "gate-z")
	read("read after resolves")

	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := backend.OrderedIndex.Get(ctx, catalogID(scope, catalogSession))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(out, "stored catalog rev=%d value=%s\n", stored.Revision, hex.EncodeToString(stored.Value))
	for _, gate := range []sessionwire.GateID{"gate-a", "gate-d"} {
		intent := gateIntentRecord(t, store, gate)
		fmt.Fprintf(out, "stored intent %s rev=%d deleted=%v due=%+v value=%s\n",
			gate, intent.Revision, intent.Deleted, intent.Due, hex.EncodeToString(intent.Value))
	}
}

func transcribeCatalogCall(t *testing.T, out *strings.Builder, name string, entry CatalogEntry, err error) {
	t.Helper()
	fmt.Fprintf(out, "%s err=%s\n", name, describeCatalogErr(err))
	if err != nil {
		return
	}
	encoded, encErr := encodeCatalogRecord(entry.Record)
	if encErr != nil {
		t.Fatalf("%s: encode returned record: %v", name, encErr)
	}
	fmt.Fprintf(out, "%s rev=%d record=%s\n", name, entry.Revision, hex.EncodeToString(encoded))
}

func describeCatalogErr(err error) string {
	if err == nil {
		return "nil"
	}
	var catalog *CatalogError
	if errors.As(err, &catalog) {
		return fmt.Sprintf("catalog{code=%s field=%s epoch=%d revision=%d msg=%q}",
			catalog.Code, catalog.Field, catalog.Epoch, catalog.Revision, err.Error())
	}
	return fmt.Sprintf("%T %q", err, err.Error())
}
