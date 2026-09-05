package sessionstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func testSessionBinding() SessionBinding {
	return SessionBinding{StorageBindingID: "agent-pool/east", BindingVersion: "config-2026-09", RuntimeSessionID: "runtime/session-a", ProtocolMode: ProtocolModeDisposition}
}

func TestSessionBindingRefusesImplicitLegacyConversion(t *testing.T) {
	for _, legacyCatalog := range []bool{false, true} {
		t.Run(map[bool]string{false: "old witnesses only", true: "old catalog"}[legacyCatalog], func(t *testing.T) {
			store := openTestStore(t)
			ctx := context.Background()
			scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
			if err != nil {
				t.Fatal(err)
			}
			// Literal old state: pre-protocol code wrote these witnesses directly.
			for key, value := range map[string][]byte{scope.tenantWitnessKey: scope.tenantWitness, scope.sessionWitnessKey: scope.sessionWitness} {
				if err := store.keys.bindWitness(ctx, key, value); err != nil {
					t.Fatal(err)
				}
			}
			if legacyCatalog {
				value, err := encodeCatalogRecord(testCatalogRecord())
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := store.backend.OrderedIndex.Create(ctx, catalogID(scope, catalogSession), scope.CatalogScope, value, catalogRank(testCatalogRecord()), storage.Due{}); err != nil {
					t.Fatal(err)
				}
			}
			req := testCreateRequest()
			req.Binding = testSessionBinding()
			_, _, err = store.CreateCatalogEntry(ctx, req)
			assertCatalogCode(t, err, CatalogErrorConflict)
			// Refusal did not strand an old caller or mutate its protocol.
			if _, _, err := store.CreateCatalogEntry(ctx, testCreateRequest()); err != nil {
				t.Fatal(err)
			}
		})
	}
	store := openStore(t, memstore.New(), WithLegacySingleTenant("local"))
	req := testCreateRequest()
	req.TenantID = "local"
	req.SessionID = "123e4567-e89b-12d3-a456-426614174000"
	req.Binding = testSessionBinding()
	_, _, err := store.CreateCatalogEntry(context.Background(), req)
	assertCatalogCode(t, err, CatalogErrorInvalid)
	req.Binding.ProtocolMode = ProtocolModeLegacy
	if _, _, err := store.CreateCatalogEntry(context.Background(), req); err != nil {
		t.Fatal(err)
	}
}

type bindingCreateFault struct {
	storage.OrderedIndex
	ambiguous bool
	fail      bool
}

func (f *bindingCreateFault) Create(ctx context.Context, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, bool, error) {
	if !f.fail {
		return f.OrderedIndex.Create(ctx, id, rankingScope, value, rank, due)
	}
	f.fail = false
	if f.ambiguous {
		if _, _, err := f.OrderedIndex.Create(ctx, id, rankingScope, value, rank, due); err != nil {
			return storage.OrderedRecord{}, false, err
		}
		return storage.OrderedRecord{}, false, &storage.OrderedAmbiguousError{ID: id}
	}
	return storage.OrderedRecord{}, false, errors.New("lost before catalog creation")
}

func TestSessionBindingCatalogFailureRecovery(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		t.Run(map[bool]string{false: "witness only", true: "ambiguous committed catalog"}[ambiguous], func(t *testing.T) {
			backend := memstore.New()
			fault := &bindingCreateFault{OrderedIndex: backend.OrderedIndex, ambiguous: ambiguous, fail: true}
			backend.OrderedIndex = fault
			store := openStore(t, backend)
			ctx := context.Background()
			req := testCreateRequest()
			req.Binding = testSessionBinding()
			if _, _, err := store.CreateCatalogEntry(ctx, req); err == nil {
				t.Fatal("fault not surfaced")
			}
			other := req
			other.Binding.StorageBindingID = "different-pool"
			entry, created, err := store.CreateCatalogEntry(ctx, other)
			if ambiguous {
				assertCatalogCode(t, err, CatalogErrorConflict)
				entry, created, err = store.CreateCatalogEntry(ctx, req)
				if err != nil || created || entry.Record.Binding != req.Binding {
					t.Fatalf("canonical retry: %+v %v %v", entry, created, err)
				}
			} else if err != nil || !created || entry.Record.Binding != other.Binding {
				t.Fatalf("mode witness became binding authority: %+v %v %v", entry, created, err)
			}
		})
	}
}

func TestSessionBindingTenantIsolation(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	req := testCreateRequest()
	req.Binding = testSessionBinding()
	first, _, err := store.CreateCatalogEntry(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	req.TenantID = "tenant-b"
	req.Binding.StorageBindingID = "tenant-b-pool"
	second, created, err := store.CreateCatalogEntry(ctx, req)
	if err != nil || !created || second.Record.Binding != req.Binding {
		t.Fatalf("other tenant: %+v %v %v", second, created, err)
	}
	assertCatalogUnchanged(t, store, first)
}

type bindingWitnessFault struct {
	storage.KV
	key                 string
	committed           bool
	fail                bool
	readbackUnavailable bool
}

func (f *bindingWitnessFault) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	if key == f.key && !f.fail && f.readbackUnavailable {
		return nil, 0, errors.New("witness readback unavailable")
	}
	return f.KV.Get(ctx, key)
}

func (f *bindingWitnessFault) Put(ctx context.Context, key string, rev uint64, value []byte) (uint64, error) {
	if !f.fail || key != f.key {
		return f.KV.Put(ctx, key, rev, value)
	}
	f.fail = false
	if f.committed {
		if _, err := f.KV.Put(ctx, key, rev, value); err != nil {
			return 0, err
		}
	}
	return 0, errors.New("witness acknowledgement lost")
}

func TestSessionBindingWitnessFailureRecovery(t *testing.T) {
	for _, protocol := range []bool{false, true} {
		for _, committed := range []bool{false, true} {
			t.Run(map[bool]string{false: "collision", true: "protocol"}[protocol]+map[bool]string{false: " absent", true: " committed"}[committed], func(t *testing.T) {
				backend := memstore.New()
				fault := &bindingWitnessFault{KV: backend.KV, committed: committed}
				backend.KV = fault
				store := openStore(t, backend)
				ctx := context.Background()
				scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
				if err != nil {
					t.Fatal(err)
				}
				fault.key = scope.sessionWitnessKey
				if protocol {
					fault.key = scope.SessionNamespace + "/protocol"
				}
				fault.fail = true
				req := testCreateRequest()
				req.Binding = testSessionBinding()
				first, created, err := store.CreateCatalogEntry(ctx, req)
				if protocol && committed {
					if err != nil || !created || first.Record.Binding != req.Binding {
						t.Fatalf("definite committed witness: %+v %v %v", first, created, err)
					}
				} else {
					if err == nil {
						t.Fatal("injected witness failure not returned")
					}
					assertNoCatalogRecord(t, store, catalogTenant, catalogSession)
					// Even after both collision and protocol witnesses exist, no binding
					// has won until the catalog commits.
					req.Binding.BindingVersion = "another-config"
				}
				got, _, err := store.CreateCatalogEntry(ctx, req)
				if err != nil || got.Record.Binding != req.Binding {
					t.Fatalf("witness retry: %+v %v", got, err)
				}
			})
		}
	}
}

func TestSessionBindingAmbiguousWitnessUnavailableReadFailsClosed(t *testing.T) {
	backend := memstore.New()
	fault := &bindingWitnessFault{KV: backend.KV, committed: true, readbackUnavailable: true}
	backend.KV = fault
	store := openStore(t, backend)
	ctx := context.Background()
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatal(err)
	}
	fault.key = scope.SessionNamespace + "/protocol"
	fault.fail = true
	req := testCreateRequest()
	req.Binding = testSessionBinding()
	_, _, err = store.CreateCatalogEntry(ctx, req)
	assertCatalogCode(t, err, CatalogErrorBackend)
	assertNoCatalogRecord(t, store, catalogTenant, catalogSession)
	fault.readbackUnavailable = false
	req.Binding.StorageBindingID = "retry-pool"
	got, created, err := store.CreateCatalogEntry(ctx, req)
	if err != nil || !created || got.Record.Binding != req.Binding {
		t.Fatalf("retry after readback restored: %+v %v %v", got, created, err)
	}
}

type bindingRaceKV struct {
	storage.KV
	arrivals chan struct{}
	release  chan struct{}
}

func (r *bindingRaceKV) Put(ctx context.Context, key string, rev uint64, value []byte) (uint64, error) {
	if strings.HasSuffix(key, "/protocol") {
		r.arrivals <- struct{}{}
		select {
		case <-r.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return r.KV.Put(ctx, key, rev, value)
}

func TestSessionBindingRejectsLegacyInboxTransitionsBeforeEvidenceOrMutation(t *testing.T) {
	for _, state := range []InboxState{InboxStatePending, InboxStateClaimed, InboxStateApplying} {
		t.Run(string(state), func(t *testing.T) {
			backend := memstore.New()
			audit := &recordingOrdered{OrderedIndex: backend.OrderedIndex}
			backend.OrderedIndex = audit
			store := openStore(t, backend, WithClock(fixedClock{inboxClaimStart}))
			ctx := context.Background()
			req := testCreateRequest()
			req.Binding = testSessionBinding()
			if _, _, err := store.CreateCatalogEntry(ctx, req); err != nil {
				t.Fatal(err)
			}
			scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
			if err != nil {
				t.Fatal(err)
			}
			record := testInboxRecord()
			record.State = state
			record.Result = CommandResult{}
			if state == InboxStatePending {
				record.Claim = CommandClaim{}
			}
			// Such mixed state cannot be created by these APIs. Inject it to prove
			// transition entry points reject a legacy row even if a provider returns one.
			value, record, err := encodeInboxRecord(record)
			if err != nil {
				t.Fatal(err)
			}
			stored, _, err := backend.OrderedIndex.Create(ctx, inboxID(scope, inboxCommand), scope.SessionNamespace, value, storage.Rank{}, inboxDue(record))
			if err != nil {
				t.Fatal(err)
			}
			entry := InboxEntry{Record: record, Revision: stored.Revision}
			audit.reset()
			operations := []func() error{
				func() error { _, err := store.ClaimCommand(ctx, testClaimRequest(entry, 4)); return err },
				func() error {
					_, err := store.BeginApplyingCommand(ctx, testBeginApplyingRequest(entry, 4))
					return err
				},
				func() error { _, err := store.CompleteCommand(ctx, testCompleteRequest(entry, 4)); return err },
				func() error { _, err := store.RejectCommand(ctx, testRejectRequest(entry, 4)); return err },
			}
			for _, operation := range operations {
				assertCatalogCode(t, operation(), CatalogErrorConflict)
			}
			if audit.countOf("update") != 0 || audit.countOf("create") != 0 {
				t.Fatal("legacy transition wrote before mode refusal")
			}
		})
	}
}

func TestSessionBindingGateWritesRefuseDispositionMode(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	req := testCreateRequest()
	req.Binding = testSessionBinding()
	entry, _, err := store.CreateCatalogEntry(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.OpenGate(ctx, OpenGateRequest{TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Gate: testGate("gate-a", 1)})
	if got := assertCatalogCode(t, err, CatalogErrorInvalid); got.Field != "binding.protocol_mode" {
		t.Fatalf("gate mode error = %v", err)
	}
	_, err = store.ResolveGate(ctx, ResolveGateRequest{TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, GateID: "gate-a"})
	if got := assertCatalogCode(t, err, CatalogErrorInvalid); got.Field != "binding.protocol_mode" {
		t.Fatalf("gate mode error = %v", err)
	}
	assertCatalogUnchanged(t, store, entry)
}

func TestSessionBindingLegacyClearsCannotMutateMixedRows(t *testing.T) {
	backend := memstore.New()
	audit := &recordingOrdered{OrderedIndex: backend.OrderedIndex}
	backend.OrderedIndex = audit
	store := openStore(t, backend, WithClock(fixedClock{registryObservedAt}))
	ctx := context.Background()
	req := testCreateRequest()
	req.Binding = testSessionBinding()
	if _, _, err := store.CreateCatalogEntry(ctx, req); err != nil {
		t.Fatal(err)
	}
	scope, err := store.deriveSessionScope(catalogTenant, catalogSession)
	if err != nil {
		t.Fatal(err)
	}
	registration := testHostRegistration()
	value, registration, err := encodeHostRegistration(registration)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := backend.OrderedIndex.Create(ctx, hostRegistrationID(scope, catalogSession), scope.SessionNamespace, value, storage.Rank{}, hostRegistrationDue(registration)); err != nil {
		t.Fatal(err)
	}
	audit.reset()
	_, err = store.ClearHostRegistration(ctx, testClearRegistrationRequest(registryNextEpoch))
	assertCatalogCode(t, err, CatalogErrorConflict)
	if audit.countOf("update") != 0 {
		t.Fatal("registration clear mutated before mode refusal")
	}
	for _, ops := range pointerOperationMatrix() {
		t.Run(ops.Noun, func(t *testing.T) {
			pointer := testSessionPointer()
			pointer.Kind = ops.Kind
			kind, ok := ops.Kind.targetObjectKind()
			if !ok {
				t.Fatal("missing object kind")
			}
			target := testObjectReference(kind, 1)
			pointer.Target = &target
			value, pointer, err := encodeSessionPointer(pointer)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := backend.OrderedIndex.Create(ctx, sessionPointerID(scope, ops.Kind), scope.SessionNamespace, value, storage.Rank{}, sessionPointerDue(pointer)); err != nil {
				t.Fatal(err)
			}
			audit.reset()
			_, err = ops.Clear(store, ctx, testClearPointerRequest(pointerEpoch+1))
			assertCatalogCode(t, err, CatalogErrorConflict)
			if audit.countOf("update") != 0 {
				t.Fatal("pointer clear mutated before mode refusal")
			}
		})
	}
}

func legacyBindingWriters(t *testing.T) map[string]func(*Store) error {
	t.Helper()
	return map[string]func(*Store) error{
		"journal": func(s *Store) error {
			w, err := s.OpenJournal(context.Background(), OpenJournalRequest{TenantID: catalogTenant, SessionID: catalogSession})
			if err == nil {
				return w.Close(context.Background())
			}
			return err
		},
		"inbox": func(s *Store) error {
			_, _, err := s.AdmitCommand(context.Background(), testAdmitRequest())
			return err
		},
		"registration": func(s *Store) error {
			_, err := s.PutHostRegistration(context.Background(), testPutRegistrationRequest(1))
			return err
		},
		"workspace pointer": func(s *Store) error {
			_, err := s.SetWorkspaceCheckpointPointer(context.Background(), SetSessionPointerRequest{TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Sequence: 1, Target: testObjectReference(ObjectKindWorkspaceCheckpoint, 1)})
			return err
		},
		"runtime pointer": func(s *Store) error {
			_, err := s.SetRuntimeCheckpointPointer(context.Background(), SetSessionPointerRequest{TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Sequence: 1, Target: testObjectReference(ObjectKindRuntimeCheckpoint, 1)})
			return err
		},
		"continuation pointer": func(s *Store) error {
			_, err := s.SetActiveContinuationPointer(context.Background(), SetSessionPointerRequest{TenantID: catalogTenant, SessionID: catalogSession, LeaseEpoch: 1, Sequence: 1, Target: testObjectReference(ObjectKindContinuation, 1)})
			return err
		},
	}
}

func TestSessionBindingFencesLegacyWriterFamilies(t *testing.T) {
	for name, write := range legacyBindingWriters(t) {
		t.Run(name, func(t *testing.T) {
			store := openStore(t, memstore.New(), WithClock(fixedClock{registryObservedAt}))
			req := testCreateRequest()
			req.Binding = testSessionBinding()
			entry, _, err := store.CreateCatalogEntry(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if err := write(store); err == nil {
				t.Fatal("legacy write entered disposition session")
			}
			assertCatalogUnchanged(t, store, entry)
		})
	}
}

func TestSessionBindingRacesLegacyWriterFamilies(t *testing.T) {
	for name, write := range legacyBindingWriters(t) {
		t.Run(name, func(t *testing.T) {
			for n := 0; n < 20; n++ {
				backend := memstore.New()
				race := &bindingRaceKV{KV: backend.KV, arrivals: make(chan struct{}, 2), release: make(chan struct{})}
				backend.KV = race
				store := openStore(t, backend, WithClock(fixedClock{registryObservedAt}))
				req := testCreateRequest()
				req.Binding = testSessionBinding()
				start := make(chan struct{})
				var writeErr, createErr error
				var created bool
				var wg sync.WaitGroup
				wg.Add(2)
				go func() { defer wg.Done(); <-start; writeErr = write(store) }()
				go func() {
					defer wg.Done()
					<-start
					_, created, createErr = store.CreateCatalogEntry(context.Background(), req)
				}()
				close(start)
				// Hold both real operations at their protocol CAS, so every run
				// exercises concurrent proposals rather than a lucky serial schedule.
				for range 2 {
					select {
					case <-race.arrivals:
					case <-time.After(5 * time.Second):
						close(race.release)
						t.Fatal("both writers did not reach the protocol CAS")
					}
				}
				close(race.release)
				wg.Wait()
				if writeErr == nil && createErr == nil {
					t.Fatal("both protocols won")
				}
				if writeErr != nil && createErr != nil {
					t.Fatalf("neither won: write %v create %v", writeErr, createErr)
				}
				if createErr == nil && !created {
					t.Fatal("no bound row created")
				}
			}
		})
	}
}

func TestSessionBindingCodecVersions(t *testing.T) {
	legacy := []byte(`{"record_version":1,"tenant_id":"tenant-a","session_id":"session-a","agent_id":"agent-a","created_at":"2026-08-30T10:00:00Z","last_active_at":"2026-08-30T11:00:00Z","state":"idle","residency":"cold","desired_placement":"pooled","last_journal_seq":0,"lease_epoch":0,"desired_generation":1}`)
	record, err := decodeCatalogRecord(legacy)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeCatalogRecord(record)
	if err != nil || !bytes.Equal(encoded, legacy) {
		t.Fatalf("legacy roundtrip = %s, %v", encoded, err)
	}
	record.Binding = testSessionBinding()
	encoded, err = encodeCatalogRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"record_version":2`)) {
		t.Fatalf("bound record = %s", encoded)
	}
	got, err := decodeCatalogRecord(encoded)
	if err != nil || got.Binding != record.Binding {
		t.Fatalf("binding roundtrip = %+v, %v", got.Binding, err)
	}
	for name, mutate := range map[string]func(map[string]any){
		"missing":                func(m map[string]any) { delete(m, "binding") },
		"null":                   func(m map[string]any) { m["binding"] = nil },
		"partial":                func(m map[string]any) { delete(m["binding"].(map[string]any), "runtime_session_id") },
		"unknown mode":           func(m map[string]any) { m["binding"].(map[string]any)["protocol_mode"] = "future" },
		"unknown binding member": func(m map[string]any) { m["binding"].(map[string]any)["credential"] = "secret" },
		"oversized id": func(m map[string]any) {
			m["binding"].(map[string]any)["storage_binding_id"] = strings.Repeat("a", 4096)
		},
		"v1 with binding": func(m map[string]any) { m["record_version"] = 1 },
		"unknown version": func(m map[string]any) { m["record_version"] = 3 },
	} {
		t.Run(name, func(t *testing.T) {
			var members map[string]any
			if err := json.Unmarshal(encoded, &members); err != nil {
				t.Fatal(err)
			}
			mutate(members)
			bad, err := json.Marshal(members)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeCatalogRecord(bad); err == nil {
				t.Fatalf("accepted %s", bad)
			}
		})
	}
}

func TestSessionBindingCreationWinnerAndImmutableRetry(t *testing.T) {
	store := openTestStore(t)
	req := testCreateRequest()
	req.Binding = testSessionBinding()
	winner, created, err := store.CreateCatalogEntry(context.Background(), req)
	if err != nil || !created {
		t.Fatalf("create: %v %v", created, err)
	}
	retry := req
	retry.RuntimeCompatibilityID = "new-default"
	got, created, err := store.CreateCatalogEntry(context.Background(), retry)
	if err != nil || created || got.Revision != winner.Revision || got.Record.RuntimeCompatibilityID != req.RuntimeCompatibilityID {
		t.Fatalf("retry: %+v %v %v", got, created, err)
	}
	for name, mutate := range map[string]func(*CreateCatalogEntryRequest){
		"binding id":      func(r *CreateCatalogEntryRequest) { r.Binding.StorageBindingID += "other" },
		"binding version": func(r *CreateCatalogEntryRequest) { r.Binding.BindingVersion += "other" },
		"runtime":         func(r *CreateCatalogEntryRequest) { r.Binding.RuntimeSessionID += "other" },
		"mode":            func(r *CreateCatalogEntryRequest) { r.Binding.ProtocolMode = ProtocolModeLegacy },
		"agent":           func(r *CreateCatalogEntryRequest) { r.AgentID = "other" },
		"legacy":          func(r *CreateCatalogEntryRequest) { r.Binding = SessionBinding{} },
	} {
		t.Run(name, func(t *testing.T) {
			conflict := req
			mutate(&conflict)
			_, _, err := store.CreateCatalogEntry(context.Background(), conflict)
			assertCatalogCode(t, err, CatalogErrorConflict)
		})
	}
	assertCatalogUnchanged(t, store, winner)
}

func TestSessionBindingConcurrentCreateHasOneWinner(t *testing.T) {
	store := openTestStore(t)
	requests := []CreateCatalogEntryRequest{testCreateRequest(), testCreateRequest()}
	for i := range requests {
		requests[i].Binding = testSessionBinding()
	}
	requests[1].Binding.StorageBindingID = "another-pool"
	type result struct {
		entry   CatalogEntry
		created bool
		err     error
	}
	results := make([]result, 2)
	var wg sync.WaitGroup
	for i := range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i].entry, results[i].created, results[i].err = store.CreateCatalogEntry(context.Background(), requests[i])
		}()
	}
	wg.Wait()
	winners := 0
	for i, result := range results {
		if result.created {
			winners++
			if result.err != nil || result.entry.Record.Binding != requests[i].Binding {
				t.Fatalf("winner: %+v", result)
			}
			assertCatalogUnchanged(t, store, result.entry)
		} else {
			assertCatalogCode(t, result.err, CatalogErrorConflict)
		}
	}
	if winners != 1 {
		t.Fatalf("winners = %d", winners)
	}
}

func TestSessionBindingDesiredPreservationAndHostModeFence(t *testing.T) {
	for _, mode := range []ProtocolMode{ProtocolModeLegacy, ProtocolModeDisposition} {
		t.Run(string(mode), func(t *testing.T) {
			store := openTestStore(t)
			req := testCreateRequest()
			req.Binding = testSessionBinding()
			req.Binding.ProtocolMode = mode
			initial, _, err := store.CreateCatalogEntry(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			desired, err := store.UpdateCatalogDesiredState(context.Background(), UpdateCatalogDesiredStateRequest{TenantID: req.TenantID, SessionID: req.SessionID, ExpectedRevision: initial.Revision, IdempotencyKey: "new-intent", DesiredPlacement: req.DesiredPlacement})
			if err != nil || desired.Record.Binding != req.Binding {
				t.Fatalf("desired: %+v %v", desired, err)
			}
			host, err := store.UpdateCatalogHostState(context.Background(), testHostStateRequest(1))
			if mode == ProtocolModeDisposition {
				assertCatalogCode(t, err, CatalogErrorInvalid)
				assertCatalogUnchanged(t, store, desired)
			} else if err != nil || host.Record.Binding != req.Binding {
				t.Fatalf("host: %+v %v", host, err)
			}
		})
	}
}
