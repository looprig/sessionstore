package sessionstore

import (
	"bytes"
	"context"
	"errors"
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
	want := []byte{'L', 'R', 'K', 'S', 1, byte(layoutTenantV1), 1, 0, 0}
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
	want := append([]byte{'L', 'R', 'K', 'S', 1, byte(layoutLegacySingleTenantV1), 1, 0, byte(len(tenant))}, []byte(tenant)...)
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
		{name: "conflict reread absent", get: notFoundGet, put: conflictPut, wantCode: KeyspaceMarkerAmbiguous, wantGets: 2, wantPuts: 1},
		{name: "conflict reread backend failure", get: func(n int) ([]byte, uint64, error) {
			if n == 1 {
				return notFoundGet(n)
			}
			return nil, 0, sentinel
		}, put: conflictPut, wantCode: KeyspaceBackend, wantGets: 2, wantPuts: 1},
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
			scope, err := store.sessionScope(context.Background(), tt.tenant, tt.session)
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
	a, err := store.sessionScope(context.Background(), "tenant-a", "same")
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.sessionScope(context.Background(), "tenant-b", "same")
	if err != nil {
		t.Fatal(err)
	}
	if a.SessionNamespace == b.SessionNamespace {
		t.Fatal("same SessionID in different tenants aliased")
	}
	if lastNameSegment(a.SessionNamespace) == lastNameSegment(b.SessionNamespace) {
		t.Fatal("session digest did not include tenant identity")
	}
	nfc, _ := store.sessionScope(context.Background(), "é", "same")
	nfd, _ := store.sessionScope(context.Background(), "e\u0301", "same")
	if nfc.TenantNamespace == nfd.TenantNamespace {
		t.Fatal("NFC and NFD identities were normalized")
	}
}

func TestTenantSessionTokenGolden(t *testing.T) {
	store := openTestStore(t)
	scope, err := store.sessionScope(context.Background(), "tenant-a", "session-a")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := scope.TenantNamespace, "tenants/h15e6gobd2bsuolsu422oa8imdn76iho3hj88h2fnimb7t2d5em0"; got != want {
		t.Fatalf("TenantNamespace = %q, want %q", got, want)
	}
	if got, want := scope.SessionNamespace, scope.TenantNamespace+"/sessions/0l176u4vnt2ldqnqa6el9mg3b9p0ulasj3tngninlj5ne6925fq0"; got != want {
		t.Fatalf("SessionNamespace = %q, want %q", got, want)
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
	valid := []byte{'L', 'R', 'K', 'S', 1, byte(layoutTenantV1), 1, 0, 0}
	tests := map[string][]byte{
		"short":            valid[:8],
		"magic":            append([]byte{'X'}, valid[1:]...),
		"codec version":    append([]byte(nil), valid...),
		"layout":           append([]byte(nil), valid...),
		"key algorithm":    append([]byte(nil), valid...),
		"length":           append([]byte(nil), valid...),
		"tenant on tenant": append(append([]byte(nil), valid[:8]...), 1, 'x'),
	}
	tests["codec version"][4]++
	tests["layout"][5] = 99
	tests["key algorithm"][6]++
	tests["length"][8] = 1
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

func TestMarkerConflictRereadsExactlyOnceAndConverges(t *testing.T) {
	want := []byte{'L', 'R', 'K', 'S', 1, byte(layoutTenantV1), 1, 0, 0}
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
			_, err = store.sessionScope(context.Background(), tenant, session)
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
	if _, err := store.sessionScope(context.Background(), "foreign", "123e4567-e89b-12d3-a456-426614174000"); err == nil {
		t.Fatal("foreign tenant accepted")
	} else {
		assertKeyspaceCode(t, err, KeyspaceLegacyTenant)
	}
	if counting.calls.Load() != before {
		t.Fatal("foreign legacy tenant touched provider")
	}

	for _, id := range []sessionwire.SessionID{"opaque", "123E4567-E89B-12D3-A456-426614174000", "{123e4567-e89b-12d3-a456-426614174000}"} {
		if _, err := store.sessionScope(context.Background(), "local", id); err == nil {
			t.Fatalf("legacy session %q accepted", id)
		} else {
			assertKeyspaceCode(t, err, KeyspaceLegacySession)
		}
	}
	scope, err := store.sessionScope(context.Background(), "local", "123e4567-e89b-12d3-a456-426614174000")
	if err != nil {
		t.Fatal(err)
	}
	if scope.SessionNamespace != "sessions/123e4567-e89b-12d3-a456-426614174000" {
		t.Fatalf("legacy namespace = %q", scope.SessionNamespace)
	}
	if counting.calls.Load() != before {
		t.Fatal("legacy scope derivation touched provider")
	}
}

func TestSessionScopeDetectsInjectedDigestCollision(t *testing.T) {
	store := openTestStore(t)
	store.keys.digest = func([]byte) [32]byte { return [32]byte{1} }
	if _, err := store.sessionScope(context.Background(), "tenant-a", "session-a"); err != nil {
		t.Fatalf("first scope: %v", err)
	}
	_, err := store.sessionScope(context.Background(), "tenant-b", "session-b")
	assertKeyspaceCode(t, err, KeyspaceHashCollision)
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

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(context.Background(), memstore.New())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return store
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
