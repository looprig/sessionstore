// Object identity: the ObjectID and blob key grammar, canonical encodings,
// and the generation entropy behind them.
package sessionstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage/memstore"
)

func TestPutObjectRNGFailureTouchesNoProvider(t *testing.T) {
	backend, calls := instrumentComposite(memstore.New())
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	store.objectGeneration = func() ([16]byte, error) { return [16]byte{}, errors.New("rng unavailable") }
	before := calls.snapshot()
	body := []byte("x")
	digest := sha256.Sum256(body)
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader(body)})
	if err == nil || metadata.Reference.ObjectID != "" {
		t.Fatalf("PutObject = %+v, %v", metadata, err)
	}
	if got := calls.snapshot(); got != before {
		t.Fatalf("provider calls changed: before=%+v after=%+v", before, got)
	}
}

func TestLegacyPutObjectUsesVersionedBlobKeyWithoutWitnesses(t *testing.T) {
	backend := memstore.New()
	store, err := Open(context.Background(), backend, WithLegacySingleTenant("local"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close(context.Background())
	store.objectGeneration = func() ([16]byte, error) { return [16]byte{1}, nil }
	body := []byte("legacy new object")
	digest := sha256.Sum256(body)
	metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "local", SessionID: "123e4567-e89b-12d3-a456-426614174000", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseObjectMetadata(metadata)
	if err != nil {
		t.Fatal(err)
	}
	scope, _ := store.deriveSessionScope("local", "123e4567-e89b-12d3-a456-426614174000")
	keys, _ := backend.Blobs.List(context.Background(), scope.BlobPrefix)
	want := objectKey(scope, parsed)
	if len(keys) != 1 || keys[0] != want {
		t.Fatalf("blob keys = %v, want %q", keys, want)
	}
	witnesses, _ := backend.KV.Keys(context.Background(), "sessionstore/witnesses/")
	if len(witnesses) != 0 {
		t.Fatalf("legacy witnesses = %v", witnesses)
	}
}

func TestParseObjectMetadataRejectsNoncanonicalForms(t *testing.T) {
	body := []byte("x")
	digest := sha256.Sum256(body)
	var generation [16]byte
	for i := range generation {
		generation[i] = 0xff
	}
	valid := objectMetadata(ObjectKindArtifact, generation, 1, digest, "")
	mutations := map[string]func(*sessionwire.ObjectMetadata){
		"trailing component": func(m *sessionwire.ObjectMetadata) { m.Reference.ObjectID += ":tail" },
		"unknown version":    func(m *sessionwire.ObjectMetadata) { m.Reference.ObjectID = "v2" + m.Reference.ObjectID[2:] },
		"unknown kind": func(m *sessionwire.ObjectMetadata) {
			p := strings.Split(m.Reference.ObjectID, ":")
			p[1] = "unknown"
			m.Reference.ObjectID = strings.Join(p, ":")
		},
		"uppercase generation": func(m *sessionwire.ObjectMetadata) {
			p := strings.Split(m.Reference.ObjectID, ":")
			p[2] = strings.ToUpper(p[2])
			m.Reference.ObjectID = strings.Join(p, ":")
		},
		"uppercase digest": func(m *sessionwire.ObjectMetadata) {
			p := strings.Split(m.Reference.ObjectID, ":")
			p[3] = strings.ToUpper(p[3])
			m.Reference.ObjectID = strings.Join(p, ":")
			m.Digest = "sha256:" + p[3]
		},
		"digest mismatch": func(m *sessionwire.ObjectMetadata) { m.Digest = "sha256:" + strings.Repeat("0", 64) },
		// The metadata Digest field stays canonical in these four, so only the
		// canonicality of the ObjectID components themselves can reject them.
		"uppercase digest inside object id only": func(m *sessionwire.ObjectMetadata) {
			p := strings.Split(m.Reference.ObjectID, ":")
			p[3] = strings.ToUpper(p[3])
			m.Reference.ObjectID = strings.Join(p, ":")
		},
		"short digest": func(m *sessionwire.ObjectMetadata) {
			p := strings.Split(m.Reference.ObjectID, ":")
			p[3] = p[3][:62]
			m.Reference.ObjectID = strings.Join(p, ":")
		},
		// A digest longer than 64 hex characters is a memory-safety case, not
		// only a canonicality one: hex.Decode writes len(value)/2 bytes into a
		// fixed 32-byte array. sessionwire caps the whole ObjectID at 256 bytes,
		// which still leaves room for a digest component that would index past
		// the end of that array.
		"long digest": func(m *sessionwire.ObjectMetadata) {
			p := strings.Split(m.Reference.ObjectID, ":")
			p[3] = strings.Repeat("ab", 100)
			m.Reference.ObjectID = strings.Join(p, ":")
		},
		"short generation": func(m *sessionwire.ObjectMetadata) {
			p := strings.Split(m.Reference.ObjectID, ":")
			p[2] = p[2][:24]
			m.Reference.ObjectID = strings.Join(p, ":")
		},
		"long generation": func(m *sessionwire.ObjectMetadata) {
			p := strings.Split(m.Reference.ObjectID, ":")
			p[2] += "vvvvvvvv"
			m.Reference.ObjectID = strings.Join(p, ":")
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if _, err := parseObjectMetadata(candidate); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestPutObjectMintsNewGenerationAndAdmitsOnce(t *testing.T) {
	store := openTestStore(t)
	generationCalls := 0
	store.objectGeneration = func() ([16]byte, error) {
		generationCalls++
		var g [16]byte
		g[0] = byte(generationCalls)
		return g, nil
	}
	admissions := 0
	store.lifecycleLocked = func(op lifecycleOperation) {
		if op == lifecycleAdmit {
			admissions++
		}
	}
	body := []byte("same bytes")
	digest := sha256.Sum256(body)
	put := func() sessionwire.ObjectMetadata {
		metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)})
		if err != nil {
			t.Fatal(err)
		}
		return metadata
	}
	first, second := put(), put()
	if first.Reference.ObjectID == second.Reference.ObjectID {
		t.Fatal("two Puts reused one generation")
	}
	if generationCalls != 2 || admissions != 2 {
		t.Fatalf("generation=%d admissions=%d, want 2/2", generationCalls, admissions)
	}
}

func TestParseObjectReferenceRejectsZeroDigestBeforeAdmission(t *testing.T) {
	zero := [32]byte{}
	metadata := objectMetadata(ObjectKindArtifact, [16]byte{1}, 1, zero, "")
	backend, calls := instrumentComposite(memstore.New())
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	admissions := 0
	store.lifecycleLocked = func(op lifecycleOperation) {
		if op == lifecycleAdmit {
			admissions++
		}
	}
	before := calls.snapshot()
	if _, err := store.GetObject(context.Background(), GetObjectRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: ObjectKindArtifact, Metadata: metadata}); err == nil {
		t.Fatal("Get accepted zero digest")
	}
	if err := store.deleteObject(context.Background(), "tenant", "session", ObjectKindArtifact, metadata.Reference); err == nil {
		t.Fatal("delete accepted zero digest")
	}
	if admissions != 0 || calls.snapshot() != before {
		t.Fatalf("admissions=%d provider before=%+v after=%+v", admissions, before, calls.snapshot())
	}
}

func TestParseObjectMetadataRejectsInvalidMediaTypes(t *testing.T) {
	digest := sha256.Sum256([]byte("x"))
	valid := objectMetadata(ObjectKindArtifact, [16]byte{1}, 1, digest, "")
	for _, mediaType := range []string{string([]byte{0xff}), "not a media type", strings.Repeat("x", 257)} {
		candidate := valid
		candidate.MediaType = mediaType
		if _, err := parseObjectMetadata(candidate); err == nil {
			t.Fatalf("accepted media type %q", mediaType)
		}
	}
}

func TestPutObjectAdmitsBeforeRNGAndClosedStoreSkipsRNG(t *testing.T) {
	backend, calls := instrumentComposite(memstore.New())
	store, err := Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	admitted := false
	store.lifecycleLocked = func(op lifecycleOperation) {
		if op == lifecycleAdmit {
			admitted = true
		}
	}
	store.objectGeneration = func() ([16]byte, error) {
		if !admitted {
			t.Fatal("RNG called before admission")
		}
		return [16]byte{}, errors.New("rng")
	}
	digest := sha256.Sum256([]byte("x"))
	before := calls.snapshot()
	_, _ = store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader([]byte("x"))})
	if !admitted || calls.snapshot() != before {
		t.Fatalf("admitted=%v provider changed=%v", admitted, calls.snapshot() != before)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	gens := 0
	store.objectGeneration = func() ([16]byte, error) { gens++; return [16]byte{}, nil }
	before = calls.snapshot()
	_, err = store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader([]byte("x"))})
	var closed *StoreClosedError
	if !errors.As(err, &closed) || gens != 0 || calls.snapshot() != before {
		t.Fatalf("closed Put err=%T %v gens=%d provider changed=%v", err, err, gens, calls.snapshot() != before)
	}
}

func TestOrdinaryPutObjectMintsDistinctRandomGenerations(t *testing.T) {
	store := openTestStore(t)
	body := []byte("same")
	digest := sha256.Sum256(body)
	put := func() sessionwire.ObjectMetadata {
		metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: uint64(len(body)), SHA256: digest, Body: bytes.NewReader(body)})
		if err != nil {
			t.Fatal(err)
		}
		return metadata
	}
	first, second := put(), put()
	if first.Reference == second.Reference {
		t.Fatalf("ordinary Puts reused reference %q", first.Reference.ObjectID)
	}
}

func TestObjectKeyLiteralGoldensDoNotAliasHistorical(t *testing.T) {
	digest := sha256.Sum256([]byte("x"))
	generation := [16]byte{1}
	metadata := objectMetadata(ObjectKindArtifact, generation, 1, digest, "")
	parts := strings.Split(metadata.Reference.ObjectID, ":")
	for _, legacy := range []bool{false, true} {
		base := memstore.New()
		opts := []Option(nil)
		tenant, session := sessionwire.TenantID("tenant/raw"), sessionwire.SessionID("session/raw")
		if legacy {
			tenant, session = "local", "123e4567-e89b-12d3-a456-426614174000"
			opts = append(opts, WithLegacySingleTenant(tenant))
		}
		store, err := Open(context.Background(), base, opts...)
		if err != nil {
			t.Fatal(err)
		}
		scope, _ := store.deriveSessionScope(tenant, session)
		want := scope.BlobPrefix + "v1/artifact/" + parts[3] + "/" + parts[2]
		parsed, _ := parseObjectMetadata(metadata)
		if got := objectKey(scope, parsed); got != want {
			t.Fatalf("legacy=%v key=%q want literal=%q", legacy, got, want)
		}
		if want == scope.BlobPrefix+parts[3] {
			t.Fatal("new key aliases historical digest-only key")
		}
	}
}

func withObjectEntropy(t *testing.T, source io.Reader) {
	t.Helper()
	previous := objectEntropy
	objectEntropy = source
	t.Cleanup(func() { objectEntropy = previous })
}

// shortEntropy yields limit bytes in total and then reports EOF, modelling a
// truncated entropy source.
func shortEntropy(limit int) io.Reader {
	remaining := limit
	return readerFunc(func(p []byte) (int, error) {
		if remaining == 0 {
			return 0, io.EOF
		}
		if len(p) > remaining {
			p = p[:remaining]
		}
		for i := range p {
			p[i] = 0xa5
		}
		remaining -= len(p)
		return len(p), nil
	})
}

func TestRandomObjectGenerationFailsClosedOnEntropyFault(t *testing.T) {
	for name, source := range map[string]io.Reader{
		"read error": readerFunc(func([]byte) (int, error) { return 0, errors.New("entropy unavailable") }),
		"short read": shortEntropy(15),
	} {
		t.Run(name, func(t *testing.T) {
			withObjectEntropy(t, source)
			generation, err := randomObjectGeneration()
			if err == nil {
				t.Fatalf("randomObjectGeneration() = %x, want error", generation)
			}
			if generation != ([16]byte{}) {
				t.Fatalf("randomObjectGeneration() leaked partial generation %x", generation)
			}
		})
	}
}

func TestPutObjectEntropyFaultTouchesNoProviderAndMintsNoReference(t *testing.T) {
	for name, source := range map[string]io.Reader{
		"read error": readerFunc(func([]byte) (int, error) { return 0, errors.New("entropy unavailable") }),
		"short read": shortEntropy(15),
	} {
		t.Run(name, func(t *testing.T) {
			withObjectEntropy(t, source)
			backend, calls := instrumentComposite(memstore.New())
			store, err := Open(context.Background(), backend)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close(context.Background())
			before := calls.snapshot()
			body := []byte("x")
			digest := sha256.Sum256(body)
			metadata, err := store.PutObject(context.Background(), PutObjectRequest{TenantID: "tenant", SessionID: "session", Kind: ObjectKindArtifact, SizeBytes: 1, SHA256: digest, Body: bytes.NewReader(body)})
			var objErr *ObjectError
			if !errors.As(err, &objErr) || objErr.Code != ObjectErrorSource || objErr.Field != "generation" {
				t.Fatalf("PutObject error = %T %v, want source/generation", err, err)
			}
			if objErr.Unwrap() == nil {
				t.Fatalf("PutObject error dropped its cause: %v", err)
			}
			if metadata != (sessionwire.ObjectMetadata{}) {
				t.Fatalf("PutObject minted metadata %+v on entropy fault", metadata)
			}
			if got := calls.snapshot(); got != before {
				t.Fatalf("provider calls changed: before=%+v after=%+v", before, got)
			}
		})
	}
}

// TestRandomObjectGenerationIsUnpredictable rejects any generation source whose
// draws are ordered or whose byte positions are near-constant, which is what a
// sequential counter (of either endianness) produces.
func TestRandomObjectGenerationIsUnpredictable(t *testing.T) {
	const draws = 64
	generations := make([][16]byte, 0, draws)
	distinct := make(map[[16]byte]struct{}, draws)
	for i := 0; i < draws; i++ {
		generation, err := randomObjectGeneration()
		if err != nil {
			t.Fatalf("randomObjectGeneration: %v", err)
		}
		if _, repeated := distinct[generation]; repeated {
			t.Fatalf("draw %d repeated generation %x", i, generation)
		}
		distinct[generation] = struct{}{}
		generations = append(generations, generation)
	}
	ascending, descending := true, true
	for i := 1; i < draws; i++ {
		if bytes.Compare(generations[i][:], generations[i-1][:]) <= 0 {
			ascending = false
		}
		if bytes.Compare(generations[i][:], generations[i-1][:]) >= 0 {
			descending = false
		}
	}
	if ascending || descending {
		t.Fatalf("generations are monotonic (ascending=%v descending=%v); source is not random", ascending, descending)
	}
	for position := 0; position < 16; position++ {
		values := make(map[byte]struct{}, draws)
		for _, generation := range generations {
			values[generation[position]] = struct{}{}
		}
		if len(values) < 16 {
			t.Fatalf("byte %d took only %d distinct values across %d draws; source is not random", position, len(values), draws)
		}
	}
}

// objectEntropyAtInit captures the production binding during package variable
// initialization, before any test body (and therefore before any
// withObjectEntropy swap or its t.Cleanup restore) can run. Asserting on it as
// well as on the live variable makes the default independent of test ordering.
var objectEntropyAtInit = objectEntropy

// TestObjectEntropyDefaultsToCryptoRandReader pins the binding structurally: a
// statistically flat but predictable PRNG passes every distributional check, so
// the default must be the crypto source itself, not merely something random
// looking.
func TestObjectEntropyDefaultsToCryptoRandReader(t *testing.T) {
	if objectEntropyAtInit != rand.Reader {
		t.Fatalf("objectEntropy initialized to %T (%v), want crypto/rand.Reader", objectEntropyAtInit, objectEntropyAtInit)
	}
	if objectEntropy != rand.Reader {
		t.Fatalf("objectEntropy is %T (%v), want crypto/rand.Reader", objectEntropy, objectEntropy)
	}
}
