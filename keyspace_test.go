package sessionstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func TestUnmarkedBackendDefaultsToTenantLayout(t *testing.T) {
	backend := memstore.New()
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close(context.Background())

	got, _, err := backend.KV.Get(context.Background(), layoutMarkerKey)
	if err != nil {
		t.Fatalf("Get(marker): %v", err)
	}
	want := []byte{'L', 'R', 'K', 'S', markerCodecVersion, byte(layoutTenantV1), 1, 0, DefaultControlShards, 0, 0}
	if !bytes.Equal(got, want) {
		t.Fatalf("marker = %x, want %x", got, want)
	}
	if store.keys.layout != layoutTenantV1 {
		t.Fatalf("layout = %v, want tenant-v1", store.keys.layout)
	}
}

func TestExplicitLegacyLayoutWritesTenantBoundMarker(t *testing.T) {
	backend := memstore.New()
	tenant := sessionwire.TenantID("local/tenant")
	store, err := Open(context.Background(), backend, WithLegacySingleTenant(tenant))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close(context.Background())

	got, _, err := backend.KV.Get(context.Background(), layoutMarkerKey)
	if err != nil {
		t.Fatalf("Get(marker): %v", err)
	}
	want := append([]byte{'L', 'R', 'K', 'S', markerCodecVersion, byte(layoutLegacySingleTenantV1), 1,
		0, DefaultControlShards, 0, byte(len(tenant))}, []byte(tenant)...)
	if !bytes.Equal(got, want) {
		t.Fatalf("marker = %x, want %x", got, want)
	}
}

func TestLayoutMarkerMismatchAndMalformedFailClosed(t *testing.T) {
	t.Run("layout mismatch", func(t *testing.T) {
		backend := memstore.New()
		legacy, err := Open(context.Background(), backend, WithLegacySingleTenant("tenant-a"))
		if err != nil {
			t.Fatalf("legacy Open: %v", err)
		}
		legacy.Close(context.Background())

		got, err := Open(context.Background(), backend)
		if got != nil {
			t.Fatal("Open returned Store on mismatch")
		}
		assertKeyspaceCode(t, err, KeyspaceLayoutMismatch)
	})

	t.Run("legacy tenant mismatch", func(t *testing.T) {
		backend := memstore.New()
		first, err := Open(context.Background(), backend, WithLegacySingleTenant("tenant-a"))
		if err != nil {
			t.Fatalf("first Open: %v", err)
		}
		first.Close(context.Background())
		_, err = Open(context.Background(), backend, WithLegacySingleTenant("tenant-b"))
		assertKeyspaceCode(t, err, KeyspaceLayoutMismatch)
	})

	t.Run("malformed", func(t *testing.T) {
		backend := memstore.New()
		if _, err := backend.KV.Put(context.Background(), layoutMarkerKey, 0, []byte("tenant-v1")); err != nil {
			t.Fatalf("seed marker: %v", err)
		}
		_, err := Open(context.Background(), backend)
		assertKeyspaceCode(t, err, KeyspaceMarkerMalformed)
	})
}

func TestLayoutMarkerCASConvergence(t *testing.T) {
	tests := []struct {
		name string
		a    []Option
		b    []Option
		same bool
	}{
		{name: "same tenant layout", same: true},
		{name: "same legacy tenant", a: []Option{WithLegacySingleTenant("a")}, b: []Option{WithLegacySingleTenant("a")}, same: true},
		{name: "different layouts", b: []Option{WithLegacySingleTenant("a")}},
		{name: "different legacy tenants", a: []Option{WithLegacySingleTenant("a")}, b: []Option{WithLegacySingleTenant("b")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := memstore.New()
			start := make(chan struct{})
			results := make(chan error, 2)
			open := func(opts []Option) {
				<-start
				s, err := Open(context.Background(), backend, opts...)
				if s != nil {
					s.Close(context.Background())
				}
				results <- err
			}
			go open(tt.a)
			go open(tt.b)
			close(start)
			errA, errB := <-results, <-results
			if tt.same {
				if errA != nil || errB != nil {
					t.Fatalf("same choice errors = (%v, %v)", errA, errB)
				}
				return
			}
			if (errA == nil) == (errB == nil) {
				t.Fatalf("conflicting choices errors = (%v, %v), want exactly one success", errA, errB)
			}
			if errA != nil {
				assertKeyspaceCode(t, errA, KeyspaceLayoutMismatch)
			} else {
				assertKeyspaceCode(t, errB, KeyspaceLayoutMismatch)
			}
		})
	}
}

func TestLayoutMarkerBackendStateMachineAndOwnership(t *testing.T) {
	sentinel := errors.New("secret backend failure")
	tests := []struct {
		name     string
		get      func(int) ([]byte, uint64, error)
		put      func(int, []byte) (uint64, error)
		wantCode KeyspaceErrorCode
		wantGets int
		wantPuts int
	}{
		{name: "initial get", get: func(int) ([]byte, uint64, error) { return nil, 0, sentinel }, wantCode: KeyspaceBackend, wantGets: 1},
		{name: "put definite failure", get: notFoundGet, put: func(int, []byte) (uint64, error) { return 0, sentinel }, wantCode: KeyspaceBackend, wantGets: 1, wantPuts: 1},
		{name: "wrong not-found key", get: func(int) ([]byte, uint64, error) { return nil, 0, &storage.KeyNotFoundError{Key: "other"} }, wantCode: KeyspaceBackend, wantGets: 1},
		{name: "conflict wrong name", get: notFoundGet, put: func(int, []byte) (uint64, error) { return 0, &storage.ConflictError{Name: "other", Expected: 0} }, wantCode: KeyspaceBackend, wantGets: 1, wantPuts: 1},
		{name: "conflict wrong expected", get: notFoundGet, put: func(int, []byte) (uint64, error) {
			return 0, &storage.ConflictError{Name: layoutMarkerKey, Expected: 7}
		}, wantCode: KeyspaceBackend, wantGets: 1, wantPuts: 1},
		{name: "conflict reread absent", get: notFoundGet, put: conflictPut, wantCode: KeyspaceMarkerAmbiguous, wantGets: 2, wantPuts: 1},
		{name: "conflict reread wrong not-found key", get: func(n int) ([]byte, uint64, error) {
			if n == 1 {
				return notFoundGet(n)
			}
			return nil, 0, &storage.KeyNotFoundError{Key: "other"}
		}, put: conflictPut, wantCode: KeyspaceBackend, wantGets: 2, wantPuts: 1},
		{name: "conflict reread backend failure", get: func(n int) ([]byte, uint64, error) {
			if n == 1 {
				return notFoundGet(n)
			}
			return nil, 0, sentinel
		}, put: conflictPut, wantCode: KeyspaceBackend, wantGets: 2, wantPuts: 1},
		{name: "conflict reread different layout", get: func(n int) ([]byte, uint64, error) {
			if n == 1 {
				return notFoundGet(n)
			}
			return encodeLayoutMarker(layoutLegacySingleTenantV1, "tenant", DefaultControlShards), 1, nil
		}, put: conflictPut, wantCode: KeyspaceLayoutMismatch, wantGets: 2, wantPuts: 1},
		{name: "conflict reread malformed", get: func(n int) ([]byte, uint64, error) {
			if n == 1 {
				return notFoundGet(n)
			}
			return []byte("malformed"), 1, nil
		}, put: conflictPut, wantCode: KeyspaceMarkerMalformed, wantGets: 2, wantPuts: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kv := &scriptKV{getFn: tt.get, putFn: tt.put}
			backend := memstore.New()
			backend.KV = kv
			closer := &countingProviderCloser{}
			store, err := Open(context.Background(), backend, WithProviderOwnership(closer))
			if store != nil {
				t.Fatal("Open returned Store")
			}
			assertKeyspaceCode(t, err, tt.wantCode)
			if strings.Contains(err.Error(), sentinel.Error()) {
				t.Fatalf("error leaked backend details: %v", err)
			}
			if kv.gets != tt.wantGets || kv.puts != tt.wantPuts {
				t.Fatalf("calls Get=%d Put=%d, want %d/%d", kv.gets, kv.puts, tt.wantGets, tt.wantPuts)
			}
			if closer.calls.Load() != 0 {
				t.Fatalf("failed Open closed provider %d times", closer.calls.Load())
			}
		})
	}
}

func TestTenantSessionScopeUsesOpaqueSafeNames(t *testing.T) {
	tests := []struct {
		name    string
		tenant  sessionwire.TenantID
		session sessionwire.SessionID
	}{
		{name: "traversal and separators", tenant: "../a/b\\c", session: "../../session:one"},
		{name: "unicode", tenant: "租户/α", session: "会話/🙂"},
		{name: "nfc", tenant: "é", session: "same"},
		{name: "nfd", tenant: "e\u0301", session: "same"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := openTestStore(t)
			scope, err := store.deriveSessionScope(tt.tenant, tt.session)
			if err != nil {
				t.Fatalf("sessionScope: %v", err)
			}
			for _, name := range []string{scope.TenantNamespace, scope.SessionNamespace, scope.JournalName} {
				if err := storage.ValidateName(name); err != nil {
					t.Errorf("ValidateName(%q): %v", name, err)
				}
				if strings.Contains(name, string(tt.tenant)) || strings.Contains(name, string(tt.session)) {
					t.Errorf("physical name %q contains a raw identity", name)
				}
			}
		})
	}

	store := openTestStore(t)
	a, err := store.deriveSessionScope("tenant-a", "same")
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.deriveSessionScope("tenant-b", "same")
	if err != nil {
		t.Fatal(err)
	}
	if a.SessionNamespace == b.SessionNamespace {
		t.Fatal("same SessionID in different tenants aliased")
	}
	if lastNameSegment(a.SessionNamespace) == lastNameSegment(b.SessionNamespace) {
		t.Fatal("session digest did not include tenant identity")
	}
	nfc, _ := store.deriveSessionScope("é", "same")
	nfd, _ := store.deriveSessionScope("e\u0301", "same")
	if nfc.TenantNamespace == nfd.TenantNamespace {
		t.Fatal("NFC and NFD identities were normalized")
	}
}

func TestTenantSessionTokenGolden(t *testing.T) {
	store := openTestStore(t)
	scope, err := store.deriveSessionScope("tenant-a", "session-a")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := scope.TenantNamespace, "tenants/h15e6gobd2bsuolsu422oa8imdn76iho3hj88h2fnimb7t2d5em0"; got != want {
		t.Fatalf("TenantNamespace = %q, want %q", got, want)
	}
	if got, want := scope.SessionNamespace, scope.TenantNamespace+"/sessions/0l176u4vnt2ldqnqa6el9mg3b9p0ulasj3tngninlj5ne6925fq0"; got != want {
		t.Fatalf("SessionNamespace = %q, want %q", got, want)
	}
	wantFields := map[string]string{
		"TenantNamespace":  scope.TenantNamespace,
		"SessionNamespace": scope.SessionNamespace,
		"LedgerName":       scope.SessionNamespace + "/journal",
		"JournalName":      scope.SessionNamespace + "/journal",
		"LeaseName":        scope.SessionNamespace + "/lease",
		"CatalogKey":       scope.SessionNamespace + "/catalog",
		"CatalogScope":     scope.TenantNamespace,
		"BlobPrefix":       scope.SessionNamespace + "/blobs/",
	}
	gotFields := map[string]string{
		"TenantNamespace": scope.TenantNamespace, "SessionNamespace": scope.SessionNamespace,
		"LedgerName": scope.LedgerName, "JournalName": scope.JournalName,
		"LeaseName": scope.LeaseName, "CatalogKey": scope.CatalogKey,
		"CatalogScope": scope.CatalogScope,
		"BlobPrefix":   scope.BlobPrefix,
	}
	for field, want := range wantFields {
		got := gotFields[field]
		if got != want {
			t.Errorf("%s = %q, want %q", field, got, want)
		}
		validName := strings.TrimSuffix(got, "/")
		if err := storage.ValidateName(validName); err != nil {
			t.Errorf("ValidateName(%s %q): %v", field, validName, err)
		}
		if len(validName) > 512 {
			t.Errorf("%s length = %d", field, len(validName))
		}
		if strings.Contains(got, "tenant-a") || strings.Contains(got, "session-a") {
			t.Errorf("%s leaks raw identity: %q", field, got)
		}
	}
}

func TestCanonicalLeaseNameNeverUsesRawTenantID(t *testing.T) {
	store := openTestStore(t)
	scope, err := store.deriveSessionScope("RawTenantID", "RawSessionID")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(scope.LeaseName, "RawTenantID") || strings.Contains(scope.LeaseName, "RawSessionID") {
		t.Fatalf("LeaseName leaks raw identity: %q", scope.LeaseName)
	}
}

func TestWitnessEncodingGoldenAndTupleBoundaries(t *testing.T) {
	store := openTestStore(t)
	scope, err := store.deriveSessionScope("a", "bc")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := scope.tenantWitness, []byte{'L', 'R', 'W', 'B', 1, 1, 0, 1, 'a'}; !bytes.Equal(got, want) {
		t.Fatalf("tenant witness = %x, want %x", got, want)
	}
	if got, want := scope.sessionWitness, []byte{'L', 'R', 'W', 'B', 1, 2, 0, 1, 'a', 0, 2, 'b', 'c'}; !bytes.Equal(got, want) {
		t.Fatalf("session witness = %x, want %x", got, want)
	}
	other, err := store.deriveSessionScope("ab", "c")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(scope.sessionWitness, other.sessionWitness) {
		t.Fatal("length framing aliased (a,bc) with (ab,c)")
	}
}

func TestVerifySessionScopeDoesNotCreateMissingWitness(t *testing.T) {
	backend := memstore.New()
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	scope, err := store.deriveSessionScope("tenant", "absent")
	if err != nil {
		t.Fatal(err)
	}
	err = store.verifySessionScope(context.Background(), scope)
	assertKeyspaceCode(t, err, KeyspaceBindingNotFound)
	keys, err := backend.KV.Keys(context.Background(), "sessionstore/witnesses/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("read verification created witnesses: %v", keys)
	}
}

func TestBindThenVerifySessionScope(t *testing.T) {
	store := openTestStore(t)
	scope, err := store.deriveSessionScope("tenant", "session")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.bindSessionScope(context.Background(), scope); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if err := store.verifySessionScope(context.Background(), scope); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestWitnessStateMachine(t *testing.T) {
	key := "sessionstore/witnesses/session/token"
	want := []byte("wanted")
	sentinel := errors.New("private backend detail")
	notFound := func(subject string) exactGetResult {
		return exactGetResult{err: &storage.KeyNotFoundError{Key: subject}}
	}
	conflict := func(subject string, expected uint64) error {
		return &storage.ConflictError{Name: subject, Expected: expected}
	}
	tests := []struct {
		name               string
		gets               []exactGetResult
		putErr             error
		wantCode           KeyspaceErrorCode
		wantGets, wantPuts int
	}{
		{name: "initial get backend error", gets: []exactGetResult{{err: sentinel}}, wantCode: KeyspaceBackend, wantGets: 1},
		{name: "wrong not-found subject", gets: []exactGetResult{notFound("other")}, wantCode: KeyspaceBackend, wantGets: 1},
		{name: "create put definite failure", gets: []exactGetResult{notFound(key)}, putErr: sentinel, wantCode: KeyspaceBackend, wantGets: 1, wantPuts: 1},
		{name: "conflict wrong name", gets: []exactGetResult{notFound(key)}, putErr: conflict("other", 0), wantCode: KeyspaceBackend, wantGets: 1, wantPuts: 1},
		{name: "conflict wrong expected", gets: []exactGetResult{notFound(key)}, putErr: conflict(key, 9), wantCode: KeyspaceBackend, wantGets: 1, wantPuts: 1},
		{name: "conflict then absent", gets: []exactGetResult{notFound(key), notFound(key)}, putErr: conflict(key, 0), wantCode: KeyspaceBindingAmbiguous, wantGets: 2, wantPuts: 1},
		{name: "conflict then wrong not-found key", gets: []exactGetResult{notFound(key), notFound("other")}, putErr: conflict(key, 0), wantCode: KeyspaceBackend, wantGets: 2, wantPuts: 1},
		{name: "conflict then backend error", gets: []exactGetResult{notFound(key), {err: sentinel}}, putErr: conflict(key, 0), wantCode: KeyspaceBackend, wantGets: 2, wantPuts: 1},
		{name: "conflict then same", gets: []exactGetResult{notFound(key), {value: want, rev: 1}}, putErr: conflict(key, 0), wantGets: 2, wantPuts: 1},
		{name: "conflict then different", gets: []exactGetResult{notFound(key), {value: []byte("different"), rev: 1}}, putErr: conflict(key, 0), wantCode: KeyspaceHashCollision, wantGets: 2, wantPuts: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kv := &exactScriptKV{getResults: append([]exactGetResult(nil), tt.gets...), putErr: tt.putErr}
			err := (keyspace{kv: kv}).bindWitness(context.Background(), key, want)
			if tt.wantCode == "" {
				if err != nil {
					t.Fatalf("bindWitness: %v", err)
				}
			} else {
				assertKeyspaceCode(t, err, tt.wantCode)
			}
			if kv.getCalls != tt.wantGets || kv.putCalls != tt.wantPuts {
				t.Fatalf("calls Get=%d Put=%d, want %d/%d", kv.getCalls, kv.putCalls, tt.wantGets, tt.wantPuts)
			}
			for _, got := range kv.getKeys {
				if got != key {
					t.Errorf("Get key = %q", got)
				}
			}
			for _, put := range kv.puts {
				if put.key != key || put.expected != 0 || !bytes.Equal(put.value, want) {
					t.Errorf("Put = key %q expected %d value %x", put.key, put.expected, put.value)
				}
			}
		})
	}
}

func TestVerifyWitnessWrongNotFoundKeyIsBackendError(t *testing.T) {
	key := "sessionstore/witnesses/session/token"
	kv := &exactScriptKV{getResults: []exactGetResult{{err: &storage.KeyNotFoundError{Key: "other"}}}}
	err := (keyspace{kv: kv}).verifyWitness(context.Background(), key, []byte("wanted"))
	assertKeyspaceCode(t, err, KeyspaceBackend)
	if kv.getCalls != 1 || kv.putCalls != 0 {
		t.Fatalf("calls Get=%d Put=%d, want 1/0", kv.getCalls, kv.putCalls)
	}
}

func TestUnmarkedBackendDoesNotProbeLegacyData(t *testing.T) {
	backend := memstore.New()
	legacyName := "sessions/123e4567-e89b-12d3-a456-426614174000"
	if err := backend.Ledger.Append(context.Background(), legacyName, 0, []byte("legacy")); err != nil {
		t.Fatal(err)
	}
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	if store.keys.layout != layoutTenantV1 {
		t.Fatalf("layout = %v, want tenant-v1", store.keys.layout)
	}
}

func TestExistingLayoutMarkerIsImmutable(t *testing.T) {
	backend := memstore.New()
	first, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	first.Close(context.Background())
	before, rev, err := backend.KV.Get(context.Background(), layoutMarkerKey)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	second.Close(context.Background())
	after, afterRev, err := backend.KV.Get(context.Background(), layoutMarkerKey)
	if err != nil {
		t.Fatal(err)
	}
	if rev != afterRev || !bytes.Equal(before, after) {
		t.Fatalf("marker changed: rev/value %d/%x -> %d/%x", rev, before, afterRev, after)
	}
}

func TestLayoutMarkerStrictCodec(t *testing.T) {
	valid := []byte{'L', 'R', 'K', 'S', markerCodecVersion, byte(layoutTenantV1), 1, 0, DefaultControlShards, 0, 0}
	tests := map[string][]byte{
		"short":            valid[:len(valid)-1],
		"magic":            append([]byte{'X'}, valid[1:]...),
		"codec version":    append([]byte(nil), valid...),
		"layout":           append([]byte(nil), valid...),
		"key algorithm":    append([]byte(nil), valid...),
		"length":           append([]byte(nil), valid...),
		"zero shards":      append([]byte(nil), valid...),
		"oversized shards": append([]byte(nil), valid...),
		"tenant on tenant": append(append([]byte(nil), valid[:len(valid)-1]...), 1, 'x'),
	}
	tests["codec version"][4]++
	tests["layout"][5] = 99
	tests["key algorithm"][6]++
	tests["length"][10] = 1
	// The shard count is refused as a READ value, not merely written correctly.
	// A marker naming zero or more than MaxControlShards would otherwise reach
	// controlShardOf's bound panic on the first session this store derived, so
	// the difference between these two cases and no check at all is a refusal
	// against a crash.
	binary.BigEndian.PutUint16(tests["zero shards"][7:9], 0)
	binary.BigEndian.PutUint16(tests["oversized shards"][7:9], MaxControlShards+1)
	for name, marker := range tests {
		t.Run(name, func(t *testing.T) {
			backend := memstore.New()
			if _, err := backend.KV.Put(context.Background(), layoutMarkerKey, 0, marker); err != nil {
				t.Fatal(err)
			}
			_, err := Open(context.Background(), backend)
			assertKeyspaceCode(t, err, KeyspaceMarkerMalformed)
		})
	}
}

func TestLegacyLayoutMarkerRawCorpus(t *testing.T) {
	legacy := func(tenant []byte, declared int) []byte {
		out := []byte{'L', 'R', 'K', 'S', markerCodecVersion, byte(layoutLegacySingleTenantV1), 1,
			0, DefaultControlShards, byte(declared >> 8), byte(declared)}
		return append(out, tenant...)
	}
	longTenant := bytes.Repeat([]byte{'x'}, sessionwire.MaxIDBytes+1)
	tests := map[string][]byte{
		"zero tenant":    legacy(nil, 0),
		"invalid utf8":   legacy([]byte{0xff}, 1),
		"over max":       legacy(longTenant, len(longTenant)),
		"trailing":       append(legacy([]byte("tenant"), 6), 'x'),
		"declared short": legacy([]byte("tenant"), 5),
		"declared long":  legacy([]byte("tenant"), 7),
	}
	for name, marker := range tests {
		t.Run(name, func(t *testing.T) {
			backend := memstore.New()
			if _, err := backend.KV.Put(context.Background(), layoutMarkerKey, 0, marker); err != nil {
				t.Fatal(err)
			}
			_, err := Open(context.Background(), backend, WithLegacySingleTenant("tenant"))
			assertKeyspaceCode(t, err, KeyspaceMarkerMalformed)
		})
	}
	t.Run("same tenant reopens", func(t *testing.T) {
		backend := memstore.New()
		if _, err := backend.KV.Put(context.Background(), layoutMarkerKey, 0, legacy([]byte("tenant"), 6)); err != nil {
			t.Fatal(err)
		}
		store, err := Open(context.Background(), backend, WithLegacySingleTenant("tenant"))
		if err != nil {
			t.Fatal(err)
		}
		store.Close(context.Background())
	})
}

func TestLayoutMismatchBothDirections(t *testing.T) {
	for _, legacyFirst := range []bool{false, true} {
		backend := memstore.New()
		first, second := []Option(nil), []Option{WithLegacySingleTenant("tenant")}
		if legacyFirst {
			first, second = second, first
		}
		store, err := Open(context.Background(), backend, first...)
		if err != nil {
			t.Fatal(err)
		}
		store.Close(context.Background())
		_, err = Open(context.Background(), backend, second...)
		assertKeyspaceCode(t, err, KeyspaceLayoutMismatch)
	}
}

func TestMarkerConflictRereadsExactlyOnceAndConverges(t *testing.T) {
	want := []byte{'L', 'R', 'K', 'S', markerCodecVersion, byte(layoutTenantV1), 1, 0, DefaultControlShards, 0, 0}
	kv := &scriptKV{
		getFn: func(n int) ([]byte, uint64, error) {
			if n == 1 {
				return notFoundGet(n)
			}
			return append([]byte(nil), want...), 1, nil
		},
		putFn: conflictPut,
	}
	backend := memstore.New()
	backend.KV = kv
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	store.Close(context.Background())
	if kv.gets != 2 || kv.puts != 1 {
		t.Fatalf("calls Get=%d Put=%d, want 2/1", kv.gets, kv.puts)
	}
}

func TestLayoutMarkerCreateUsesExactKeyAndRevisionZero(t *testing.T) {
	kv := &exactScriptKV{getResults: []exactGetResult{{err: &storage.KeyNotFoundError{Key: layoutMarkerKey}}}}
	backend := memstore.New()
	backend.KV = kv
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	store.Close(context.Background())
	if kv.getCalls != 1 || kv.putCalls != 1 {
		t.Fatalf("calls Get=%d Put=%d", kv.getCalls, kv.putCalls)
	}
	if len(kv.puts) != 1 || kv.puts[0].key != layoutMarkerKey || kv.puts[0].expected != 0 {
		t.Fatalf("marker Put = %+v", kv.puts)
	}
	want := []byte{'L', 'R', 'K', 'S', markerCodecVersion, byte(layoutTenantV1), 1, 0, DefaultControlShards, 0, 0}
	if !bytes.Equal(kv.puts[0].value, want) {
		t.Fatalf("marker value = %x, want %x", kv.puts[0].value, want)
	}
}

func lastNameSegment(name string) string {
	if at := strings.LastIndexByte(name, '/'); at >= 0 {
		return name[at+1:]
	}
	return name
}

func TestSessionScopeRejectsInvalidOpaqueIDsBeforeProviderCall(t *testing.T) {
	invalid := []string{"", strings.Repeat("a", sessionwire.MaxIDBytes+1), string([]byte{0xff})}
	for _, value := range invalid {
		for _, field := range []string{"TenantID", "SessionID"} {
			backend := memstore.New()
			counting := &countingKV{KV: backend.KV}
			backend.KV = counting
			store, err := Open(context.Background(), backend)
			if err != nil {
				t.Fatal(err)
			}
			before := counting.calls.Load()
			tenant, session := sessionwire.TenantID("tenant"), sessionwire.SessionID("session")
			if field == "TenantID" {
				tenant = sessionwire.TenantID(value)
			} else {
				session = sessionwire.SessionID(value)
			}
			_, err = store.deriveSessionScope(tenant, session)
			var invalidErr *InvalidIdentityError
			if !errors.As(err, &invalidErr) || invalidErr.Field != field {
				t.Fatalf("%s invalid error = %T %v", field, err, err)
			}
			if counting.calls.Load() != before {
				t.Fatalf("%s invalid input touched provider", field)
			}
			store.Close(context.Background())
		}
	}
}

func TestLegacyLayoutRejectsForeignTenantAndNoncanonicalSession(t *testing.T) {
	backend := memstore.New()
	counting := &countingKV{KV: backend.KV}
	backend.KV = counting
	store, err := Open(context.Background(), backend, WithLegacySingleTenant("local"))
	if err != nil {
		t.Fatal(err)
	}
	before := counting.calls.Load()
	if _, err := store.deriveSessionScope("foreign", "123e4567-e89b-12d3-a456-426614174000"); err == nil {
		t.Fatal("foreign tenant accepted")
	} else {
		assertKeyspaceCode(t, err, KeyspaceLegacyTenant)
	}
	if counting.calls.Load() != before {
		t.Fatal("foreign legacy tenant touched provider")
	}

	for _, id := range []sessionwire.SessionID{"opaque", "123E4567-E89B-12D3-A456-426614174000", "{123e4567-e89b-12d3-a456-426614174000}"} {
		if _, err := store.deriveSessionScope("local", id); err == nil {
			t.Fatalf("legacy session %q accepted", id)
		} else {
			assertKeyspaceCode(t, err, KeyspaceLegacySession)
		}
	}
	scope, err := store.deriveSessionScope("local", "123e4567-e89b-12d3-a456-426614174000")
	if err != nil {
		t.Fatal(err)
	}
	if scope.SessionNamespace != "sessions/123e4567-e89b-12d3-a456-426614174000" {
		t.Fatalf("legacy namespace = %q", scope.SessionNamespace)
	}
	if scope.TenantNamespace != "" {
		t.Fatalf("legacy tenant namespace = %q, want empty", scope.TenantNamespace)
	}
	if scope.LedgerName != scope.SessionNamespace {
		t.Fatalf("legacy ledger = %q, want %q", scope.LedgerName, scope.SessionNamespace)
	}
	if scope.JournalName != scope.SessionNamespace {
		t.Fatalf("legacy journal = %q, want %q", scope.JournalName, scope.SessionNamespace)
	}
	if scope.CatalogKey != scope.SessionNamespace {
		t.Fatalf("legacy catalog = %q, want %q", scope.CatalogKey, scope.SessionNamespace)
	}
	if scope.LeaseName != scope.SessionNamespace {
		t.Fatalf("legacy lease = %q, want %q", scope.LeaseName, scope.SessionNamespace)
	}
	if scope.CatalogScope != legacyCatalogScope {
		t.Fatalf("legacy catalog scope = %q, want %q", scope.CatalogScope, legacyCatalogScope)
	}
	if err := storage.ValidateName(scope.CatalogScope); err != nil {
		t.Fatalf("legacy catalog scope is not a storage name: %v", err)
	}
	if scope.BlobPrefix != scope.SessionNamespace+"/blobs/" {
		t.Fatalf("legacy blob prefix = %q", scope.BlobPrefix)
	}
	for label, name := range map[string]string{"ledger": scope.LedgerName, "lease": scope.LeaseName, "catalog": scope.CatalogKey} {
		if strings.HasSuffix(name, "/journal") || strings.HasSuffix(name, "/lease") || strings.HasSuffix(name, "/catalog") {
			t.Errorf("legacy %s incorrectly gained suffix: %q", label, name)
		}
	}
	for label, name := range map[string]string{
		"tenant namespace": scope.TenantNamespace, "namespace": scope.SessionNamespace, "journal": scope.JournalName,
		"ledger": scope.LedgerName, "lease": scope.LeaseName,
		"catalog": scope.CatalogKey, "blobs": scope.BlobPrefix,
	} {
		for _, forbidden := range []string{"tenants/", "/journal", "/lease", "/writer", "/catalog", "/objects"} {
			if strings.Contains(name, forbidden) {
				t.Errorf("legacy %s %q contains forbidden %q", label, name, forbidden)
			}
		}
	}
	if counting.calls.Load() != before {
		t.Fatal("legacy scope derivation touched provider")
	}
	if err := store.verifySessionScope(context.Background(), scope); err != nil {
		t.Fatalf("legacy verify: %v", err)
	}
	if err := store.bindSessionScope(context.Background(), scope); err != nil {
		t.Fatalf("legacy bind: %v", err)
	}
	if counting.calls.Load() != before {
		t.Fatal("legacy verify/bind touched provider")
	}
	keys, err := backend.KV.Keys(context.Background(), "sessionstore/witnesses/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("legacy created canonical witnesses: %v", keys)
	}
	zero, err := store.deriveSessionScope("local", "00000000-0000-0000-0000-000000000000")
	if err != nil {
		t.Fatalf("canonical zero UUID: %v", err)
	}
	if zero.SessionNamespace != "sessions/00000000-0000-0000-0000-000000000000" {
		t.Fatalf("zero UUID namespace = %q", zero.SessionNamespace)
	}
}

func TestVerifyAndBindRejectMalformedInternalScope(t *testing.T) {
	store := openTestStore(t)
	for name, scope := range map[string]sessionScope{
		"zero":                        {},
		"canonical without witnesses": {layout: layoutTenantV1},
		"legacy with witness":         {layout: layoutLegacySingleTenantV1, tenantWitnessKey: "unexpected"},
	} {
		t.Run(name, func(t *testing.T) {
			assertKeyspaceCode(t, store.verifySessionScope(context.Background(), scope), KeyspaceScopeInvalid)
			assertKeyspaceCode(t, store.bindSessionScope(context.Background(), scope), KeyspaceScopeInvalid)
		})
	}
}

func TestVerifyAndBindRejectValidCrossLayoutScopeWithoutProviderIO(t *testing.T) {
	canonicalSource := openTestStore(t)
	legacySourceBackend := memstore.New()
	legacySource, err := Open(context.Background(), legacySourceBackend, WithLegacySingleTenant("local"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { legacySource.Close(context.Background()) })

	canonicalScope, err := canonicalSource.deriveSessionScope("tenant", "session")
	if err != nil {
		t.Fatal(err)
	}
	legacyScope, err := legacySource.deriveSessionScope("local", "123e4567-e89b-12d3-a456-426614174000")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		options []Option
		scope   sessionScope
	}{
		{name: "canonical store with legacy scope", scope: legacyScope},
		{name: "legacy store with canonical scope", options: []Option{WithLegacySingleTenant("local")}, scope: canonicalScope},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend, calls := instrumentComposite(memstore.New())
			store, err := Open(context.Background(), backend, tt.options...)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close(context.Background())
			before := calls.snapshot()
			assertKeyspaceCode(t, store.verifySessionScope(context.Background(), tt.scope), KeyspaceScopeInvalid)
			if got := calls.snapshot(); got != before {
				t.Fatalf("verify touched provider: before=%+v after=%+v", before, got)
			}
			assertKeyspaceCode(t, store.bindSessionScope(context.Background(), tt.scope), KeyspaceScopeInvalid)
			if got := calls.snapshot(); got != before {
				t.Fatalf("bind touched provider: before=%+v after=%+v", before, got)
			}
		})
	}
}

func TestSessionScopeDetectsInjectedDigestCollision(t *testing.T) {
	tests := []struct {
		name                        string
		firstTenant, secondTenant   sessionwire.TenantID
		firstSession, secondSession sessionwire.SessionID
	}{
		{name: "tenant witness", firstTenant: "tenant-a", firstSession: "session", secondTenant: "tenant-b", secondSession: "session"},
		{name: "session witness", firstTenant: "tenant", firstSession: "session-a", secondTenant: "tenant", secondSession: "session-b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := memstore.New()
			store, err := Open(context.Background(), backend)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close(context.Background())
			store.keys.digest = func([]byte) [32]byte { return [32]byte{1} }
			first, err := store.deriveSessionScope(tt.firstTenant, tt.firstSession)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.bindSessionScope(context.Background(), first); err != nil {
				t.Fatalf("first bind: %v", err)
			}

			token := "04" + strings.Repeat("0", 50)
			wantKeys := []string{"sessionstore/witnesses/session/" + token, "sessionstore/witnesses/tenant/" + token}
			gotKeys, err := backend.KV.Keys(context.Background(), "sessionstore/witnesses/")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotKeys, wantKeys) {
				t.Fatalf("witness keys = %v, want %v", gotKeys, wantKeys)
			}
			if got, _, err := backend.KV.Get(context.Background(), first.tenantWitnessKey); err != nil || !bytes.Equal(got, first.tenantWitness) {
				t.Fatalf("tenant witness = %x, %v; want %x", got, err, first.tenantWitness)
			}
			if got, _, err := backend.KV.Get(context.Background(), first.sessionWitnessKey); err != nil || !bytes.Equal(got, first.sessionWitness) {
				t.Fatalf("session witness = %x, %v; want %x", got, err, first.sessionWitness)
			}

			second, err := store.deriveSessionScope(tt.secondTenant, tt.secondSession)
			if err != nil {
				t.Fatal(err)
			}
			err = store.verifySessionScope(context.Background(), second)
			assertKeyspaceCode(t, err, KeyspaceHashCollision)
			err = store.bindSessionScope(context.Background(), second)
			assertKeyspaceCode(t, err, KeyspaceHashCollision)
			afterKeys, _ := backend.KV.Keys(context.Background(), "sessionstore/witnesses/")
			if !reflect.DeepEqual(afterKeys, wantKeys) {
				t.Fatalf("collision mutated witnesses: %v", afterKeys)
			}
			if tip, err := backend.Ledger.Tip(context.Background(), first.LedgerName); err != nil || tip != 0 {
				t.Fatalf("ledger mutated: tip=%d err=%v", tip, err)
			}
			if _, _, err := backend.KV.Get(context.Background(), first.CatalogKey); !errors.As(err, new(*storage.KeyNotFoundError)) {
				t.Fatalf("catalog mutated: %v", err)
			}
			if blobs, err := backend.Blobs.List(context.Background(), first.BlobPrefix); err != nil || len(blobs) != 0 {
				t.Fatalf("blobs mutated: %v %v", blobs, err)
			}
		})
	}
}

func TestWithLegacySingleTenantRejectsInvalidTenantBeforeProviderCall(t *testing.T) {
	backend := memstore.New()
	counting := &countingKV{KV: backend.KV}
	backend.KV = counting
	for _, tenant := range []sessionwire.TenantID{"", sessionwire.TenantID(string([]byte{0xff})), sessionwire.TenantID(strings.Repeat("x", 257))} {
		store, err := Open(context.Background(), backend, WithLegacySingleTenant(tenant))
		if store != nil || err == nil {
			t.Fatalf("Open(%d bytes) = %v, %v", len(tenant), store, err)
		}
		if counting.calls.Load() != 0 {
			t.Fatal("invalid option touched provider")
		}
	}
}

func TestInvalidAndForeignLegacyIdentitiesTouchNoProviderPrimitive(t *testing.T) {
	t.Run("invalid canonical", func(t *testing.T) {
		backend, calls := instrumentComposite(memstore.New())
		store, err := Open(context.Background(), backend)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close(context.Background())
		before := calls.snapshot()
		if _, err := store.deriveSessionScope("", "session"); err == nil {
			t.Fatal("invalid tenant accepted")
		}
		if got := calls.snapshot(); got != before {
			t.Fatalf("provider calls changed from %+v to %+v", before, got)
		}
	})
	t.Run("foreign legacy", func(t *testing.T) {
		backend, calls := instrumentComposite(memstore.New())
		store, err := Open(context.Background(), backend, WithLegacySingleTenant("local"))
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close(context.Background())
		before := calls.snapshot()
		if _, err := store.deriveSessionScope("foreign", "123e4567-e89b-12d3-a456-426614174000"); err == nil {
			t.Fatal("foreign tenant accepted")
		}
		if got := calls.snapshot(); got != before {
			t.Fatalf("provider calls changed from %+v to %+v", before, got)
		}
	})
}

func TestBindingFailurePrecedesEverySessionDataPrimitive(t *testing.T) {
	backend, calls := instrumentComposite(memstore.New())
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	store.keys.digest = func([]byte) [32]byte { return [32]byte{1} }
	first, _ := store.deriveSessionScope("tenant-a", "session")
	if err := store.bindSessionScope(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	before := calls.snapshot()
	second, _ := store.deriveSessionScope("tenant-b", "session")
	err = store.bindSessionScope(context.Background(), second)
	assertKeyspaceCode(t, err, KeyspaceHashCollision)
	after := calls.snapshot()
	if after.Ledger != before.Ledger || after.Leaser != before.Leaser || after.Ordered != before.Ordered || after.Blobs != before.Blobs {
		t.Fatalf("binding failure touched session data primitives: before=%+v after=%+v", before, after)
	}
	if after.KV <= before.KV {
		t.Fatalf("binding validation did not touch witness KV: before=%+v after=%+v", before, after)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	return openStore(t, memstore.New())
}

func assertKeyspaceCode(t *testing.T, err error, want KeyspaceErrorCode) {
	t.Helper()
	var got *KeyspaceError
	if !errors.As(err, &got) {
		t.Fatalf("error = %T %v, want *KeyspaceError", err, err)
	}
	if got.Code != want {
		t.Fatalf("KeyspaceError.Code = %q, want %q", got.Code, want)
	}
}

type countingKV struct {
	storage.KV
	calls atomic.Int32
}

func (k *countingKV) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	k.calls.Add(1)
	return k.KV.Get(ctx, key)
}
func (k *countingKV) Put(ctx context.Context, key string, rev uint64, val []byte) (uint64, error) {
	k.calls.Add(1)
	return k.KV.Put(ctx, key, rev, val)
}
func (k *countingKV) Keys(ctx context.Context, prefix string) ([]string, error) {
	k.calls.Add(1)
	return k.KV.Keys(ctx, prefix)
}
func (k *countingKV) Delete(ctx context.Context, key string) error {
	k.calls.Add(1)
	return k.KV.Delete(ctx, key)
}

type scriptKV struct {
	mu         sync.Mutex
	gets, puts int
	getFn      func(int) ([]byte, uint64, error)
	putFn      func(int, []byte) (uint64, error)
}

func (k *scriptKV) Get(context.Context, string) ([]byte, uint64, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.gets++
	return k.getFn(k.gets)
}
func (k *scriptKV) Put(_ context.Context, _ string, _ uint64, val []byte) (uint64, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.puts++
	if k.putFn == nil {
		return 1, nil
	}
	return k.putFn(k.puts, val)
}
func (*scriptKV) Keys(context.Context, string) ([]string, error) { return nil, nil }
func (*scriptKV) Delete(context.Context, string) error           { return nil }
func notFoundGet(int) ([]byte, uint64, error) {
	return nil, 0, &storage.KeyNotFoundError{Key: layoutMarkerKey}
}
func conflictPut(int, []byte) (uint64, error) {
	return 0, &storage.ConflictError{Name: layoutMarkerKey, Expected: 0}
}

type countingProviderCloser struct{ calls atomic.Int32 }

func (c *countingProviderCloser) Close(context.Context) error { c.calls.Add(1); return nil }

type exactGetResult struct {
	value []byte
	rev   uint64
	err   error
}

type exactPutCall struct {
	key      string
	expected uint64
	value    []byte
}

type exactScriptKV struct {
	getResults         []exactGetResult
	putErr             error
	getCalls, putCalls int
	getKeys            []string
	puts               []exactPutCall
}

func (k *exactScriptKV) Get(_ context.Context, key string) ([]byte, uint64, error) {
	k.getCalls++
	k.getKeys = append(k.getKeys, key)
	if len(k.getResults) == 0 {
		return nil, 0, errors.New("unexpected Get")
	}
	result := k.getResults[0]
	k.getResults = k.getResults[1:]
	return append([]byte(nil), result.value...), result.rev, result.err
}
func (k *exactScriptKV) Put(_ context.Context, key string, expected uint64, value []byte) (uint64, error) {
	k.putCalls++
	k.puts = append(k.puts, exactPutCall{key: key, expected: expected, value: append([]byte(nil), value...)})
	if k.putErr != nil {
		return 0, k.putErr
	}
	return 1, nil
}
func (*exactScriptKV) Keys(context.Context, string) ([]string, error) { return nil, nil }
func (*exactScriptKV) Delete(context.Context, string) error           { return nil }

type providerCallSnapshot struct{ Ledger, Leaser, KV, Ordered, Blobs int32 }
type providerCalls struct{ ledger, leaser, kv, ordered, blobs atomic.Int32 }

func (c *providerCalls) snapshot() providerCallSnapshot {
	return providerCallSnapshot{c.ledger.Load(), c.leaser.Load(), c.kv.Load(), c.ordered.Load(), c.blobs.Load()}
}

func instrumentComposite(base *storage.Composite) (*storage.Composite, *providerCalls) {
	calls := &providerCalls{}
	return &storage.Composite{
		Ledger:       &countAllLedger{Ledger: base.Ledger, calls: calls},
		Leaser:       &countAllLeaser{Leaser: base.Leaser, calls: calls},
		KV:           &countAllKV{KV: base.KV, calls: calls},
		Blobs:        &countAllBlobs{lifecycleBlobs: lifecycleBlobs{base.Blobs}, calls: calls},
		OrderedIndex: &countAllOrdered{OrderedIndex: base.OrderedIndex, calls: calls},
	}, calls
}

type countAllLedger struct {
	storage.Ledger
	calls *providerCalls
}

func (c *countAllLedger) Append(ctx context.Context, name string, expected uint64, value []byte) error {
	c.calls.ledger.Add(1)
	return c.Ledger.Append(ctx, name, expected, value)
}
func (c *countAllLedger) Read(ctx context.Context, name string, from uint64) (storage.Cursor, error) {
	c.calls.ledger.Add(1)
	return c.Ledger.Read(ctx, name, from)
}
func (c *countAllLedger) Tip(ctx context.Context, name string) (uint64, error) {
	c.calls.ledger.Add(1)
	return c.Ledger.Tip(ctx, name)
}
func (c *countAllLedger) Delete(ctx context.Context, name string) error {
	c.calls.ledger.Add(1)
	return c.Ledger.Delete(ctx, name)
}

type countAllLeaser struct {
	storage.Leaser
	calls *providerCalls
}

func (c *countAllLeaser) Acquire(ctx context.Context, name string) (storage.Lease, error) {
	c.calls.leaser.Add(1)
	return c.Leaser.Acquire(ctx, name)
}

type countAllKV struct {
	storage.KV
	calls *providerCalls
}

func (c *countAllKV) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	c.calls.kv.Add(1)
	return c.KV.Get(ctx, key)
}
func (c *countAllKV) Put(ctx context.Context, key string, rev uint64, value []byte) (uint64, error) {
	c.calls.kv.Add(1)
	return c.KV.Put(ctx, key, rev, value)
}
func (c *countAllKV) Keys(ctx context.Context, prefix string) ([]string, error) {
	c.calls.kv.Add(1)
	return c.KV.Keys(ctx, prefix)
}
func (c *countAllKV) Delete(ctx context.Context, key string) error {
	c.calls.kv.Add(1)
	return c.KV.Delete(ctx, key)
}

type countAllBlobs struct {
	lifecycleBlobs
	calls *providerCalls
}

func (c *countAllBlobs) Put(ctx context.Context, key string, r io.Reader) error {
	c.calls.blobs.Add(1)
	return c.Blobs.Put(ctx, key, r)
}
func (c *countAllBlobs) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	c.calls.blobs.Add(1)
	return c.Blobs.Get(ctx, key)
}
func (c *countAllBlobs) Delete(ctx context.Context, key string) error {
	c.calls.blobs.Add(1)
	return c.Blobs.Delete(ctx, key)
}
func (c *countAllBlobs) List(ctx context.Context, prefix string) ([]string, error) {
	c.calls.blobs.Add(1)
	return c.Blobs.List(ctx, prefix)
}

type countAllOrdered struct {
	storage.OrderedIndex
	calls *providerCalls
}

func (c *countAllOrdered) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	c.calls.ordered.Add(1)
	return c.OrderedIndex.Get(ctx, id)
}
func (c *countAllOrdered) Create(ctx context.Context, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, bool, error) {
	c.calls.ordered.Add(1)
	return c.OrderedIndex.Create(ctx, id, rankingScope, value, rank, due)
}
func (c *countAllOrdered) Update(ctx context.Context, id storage.OrderedID, rev uint64, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, error) {
	c.calls.ordered.Add(1)
	return c.OrderedIndex.Update(ctx, id, rev, value, rank, due)
}
func (c *countAllOrdered) Delete(ctx context.Context, id storage.OrderedID, rev uint64) (storage.OrderedRecord, error) {
	c.calls.ordered.Add(1)
	return c.OrderedIndex.Delete(ctx, id, rev)
}
func (c *countAllOrdered) ListOrdered(ctx context.Context, namespace, scope string, after uint64, limit int) (storage.OrderedPage, error) {
	c.calls.ordered.Add(1)
	return c.OrderedIndex.ListOrdered(ctx, namespace, scope, after, limit)
}
func (c *countAllOrdered) ListRanked(ctx context.Context, namespace, scope string, after storage.RankedCursor, limit int) (storage.RankedPage, error) {
	c.calls.ordered.Add(1)
	return c.OrderedIndex.ListRanked(ctx, namespace, scope, after, limit)
}
func (c *countAllOrdered) ListDue(ctx context.Context, namespace string, before int64, after storage.DueCursor, limit int) (storage.DuePage, error) {
	c.calls.ordered.Add(1)
	return c.OrderedIndex.ListDue(ctx, namespace, before, after, limit)
}

// TestDeriveSessionScopeValidatesTenantBeforeSession pins the precedence of the
// four identity gates, which is otherwise invisible: every ordering of them
// rejects the same inputs, so only a request that fails more than one gate can
// tell them apart.
//
// The order matters because the answers differ in what they tell a caller to
// do. A tenant a backend does not authorize is a configuration failure, and
// reporting a malformed session id instead would send the caller to fix the
// wrong thing. Within the legacy layout the same holds one level down: an
// unusable session id is a caller error, while a well-formed but non-canonical
// one is a layout restriction.
func TestDeriveSessionScopeValidatesTenantBeforeSession(t *testing.T) {
	canonical := openTestStore(t)
	legacyBackend := memstore.New()
	legacy, err := Open(context.Background(), legacyBackend, WithLegacySingleTenant("local"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { legacy.Close(context.Background()) })

	t.Run("invalid tenant precedes invalid session", func(t *testing.T) {
		_, err := canonical.deriveSessionScope("", "")
		var invalid *InvalidIdentityError
		if !errors.As(err, &invalid) {
			t.Fatalf("error = %T %v, want *InvalidIdentityError", err, err)
		}
		if invalid.Field != "TenantID" {
			t.Fatalf("field = %q, want TenantID", invalid.Field)
		}
	})

	t.Run("unauthorized legacy tenant precedes invalid session", func(t *testing.T) {
		_, err := legacy.deriveSessionScope("someone-else", "")
		assertKeyspaceCode(t, err, KeyspaceLegacyTenant)
	})

	t.Run("invalid session precedes the legacy session form", func(t *testing.T) {
		// An empty session id is both invalid and non-canonical for the legacy
		// layout. It must be reported as invalid: the layout restriction is a
		// statement about well-formed ids this backend cannot store.
		_, err := legacy.deriveSessionScope("local", "")
		var invalid *InvalidIdentityError
		if !errors.As(err, &invalid) {
			t.Fatalf("error = %T %v, want *InvalidIdentityError", err, err)
		}
		if invalid.Field != "SessionID" {
			t.Fatalf("field = %q, want SessionID", invalid.Field)
		}
	})

	t.Run("a valid non-canonical legacy session is a layout restriction", func(t *testing.T) {
		_, err := legacy.deriveSessionScope("local", "not-a-uuid")
		assertKeyspaceCode(t, err, KeyspaceLegacySession)
	})
}
