package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/binary"
	"errors"
	"math"
	"strings"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

const layoutMarkerKey = "sessionstore/layout"

const (
	markerCodecVersion  byte = 1
	keyAlgorithmVersion byte = 1
)

type keyspaceLayout byte

const (
	layoutTenantV1 keyspaceLayout = iota + 1
	layoutLegacySingleTenantV1
)

type keyspace struct {
	kv           storage.KV
	layout       keyspaceLayout
	legacyTenant sessionwire.TenantID
	digest       func([]byte) [32]byte
}

type sessionScope struct {
	TenantNamespace   string
	SessionNamespace  string
	LedgerName        string
	LeaseName         string
	CatalogKey        string
	CatalogListPrefix string
	BlobPrefix        string
	JournalName       string
	tenantWitnessKey  string
	tenantWitness     []byte
	sessionWitnessKey string
	sessionWitness    []byte
}

func newKeyspace(kv storage.KV, layout keyspaceLayout, legacyTenant sessionwire.TenantID) keyspace {
	return keyspace{kv: kv, layout: layout, legacyTenant: legacyTenant, digest: sha256.Sum256}
}

func (k keyspace) initialize(ctx context.Context) error {
	want := encodeLayoutMarker(k.layout, k.legacyTenant)
	got, _, err := k.kv.Get(ctx, layoutMarkerKey)
	if err == nil {
		return compareLayoutMarker(got, want)
	}
	if !isKeyNotFound(err, layoutMarkerKey) {
		return &KeyspaceError{Code: KeyspaceBackend, Cause: err}
	}
	if _, err = k.kv.Put(ctx, layoutMarkerKey, 0, want); err == nil {
		return nil
	}
	if !isCreateConflict(err, layoutMarkerKey) {
		return &KeyspaceError{Code: KeyspaceBackend, Cause: err}
	}
	got, _, err = k.kv.Get(ctx, layoutMarkerKey)
	if err != nil {
		if isKeyNotFound(err, layoutMarkerKey) {
			return &KeyspaceError{Code: KeyspaceMarkerAmbiguous, Cause: err}
		}
		return &KeyspaceError{Code: KeyspaceBackend, Cause: err}
	}
	return compareLayoutMarker(got, want)
}

func encodeLayoutMarker(layout keyspaceLayout, tenant sessionwire.TenantID) []byte {
	tenantBytes := []byte(tenant)
	out := make([]byte, 9+len(tenantBytes))
	copy(out, "LRKS")
	out[4] = markerCodecVersion
	out[5] = byte(layout)
	out[6] = keyAlgorithmVersion
	binary.BigEndian.PutUint16(out[7:9], checkedUint16Length(len(tenantBytes)))
	copy(out[9:], tenantBytes)
	return out
}

func compareLayoutMarker(got, want []byte) error {
	if err := validateLayoutMarker(got); err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return &KeyspaceError{Code: KeyspaceLayoutMismatch}
	}
	return nil
}

func validateLayoutMarker(data []byte) error {
	malformed := func(cause error) error { return &KeyspaceError{Code: KeyspaceMarkerMalformed, Cause: cause} }
	if len(data) < 9 || string(data[:4]) != "LRKS" || data[4] != markerCodecVersion || data[6] != keyAlgorithmVersion {
		return malformed(nil)
	}
	length := int(binary.BigEndian.Uint16(data[7:9]))
	if len(data) != 9+length {
		return malformed(nil)
	}
	switch keyspaceLayout(data[5]) {
	case layoutTenantV1:
		if length != 0 {
			return malformed(nil)
		}
	case layoutLegacySingleTenantV1:
		tenant := sessionwire.TenantID(string(data[9:]))
		if err := tenant.Validate(); err != nil {
			return malformed(err)
		}
	default:
		return malformed(nil)
	}
	return nil
}

// deriveSessionScope is pure: it validates identities and derives provider-safe
// names without touching storage. Callers must use verifySessionScope before a
// read or bindSessionScope before creating durable session data.
func (s *Store) deriveSessionScope(tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionScope, error) {
	if err := tenant.Validate(); err != nil {
		return sessionScope{}, &InvalidIdentityError{Field: "TenantID", Cause: err}
	}
	if s.keys.layout == layoutLegacySingleTenantV1 && tenant != s.keys.legacyTenant {
		return sessionScope{}, &KeyspaceError{Code: KeyspaceLegacyTenant}
	}
	if err := session.Validate(); err != nil {
		return sessionScope{}, &InvalidIdentityError{Field: "SessionID", Cause: err}
	}
	if s.keys.layout == layoutLegacySingleTenantV1 {
		if !isCanonicalLegacySessionID(string(session)) {
			return sessionScope{}, &KeyspaceError{Code: KeyspaceLegacySession}
		}
		prefix := "sessions/" + string(session)
		return sessionScope{
			SessionNamespace:  prefix,
			LedgerName:        prefix,
			LeaseName:         prefix,
			CatalogKey:        prefix,
			CatalogListPrefix: "sessions/",
			BlobPrefix:        prefix + "/blobs/",
			JournalName:       prefix,
		}, nil
	}

	tenantFrame := digestFrame("looprig/sessionstore/key/v1/tenant", []byte(tenant))
	sessionFrame := digestFrame("looprig/sessionstore/key/v1/session", []byte(tenant), []byte(session))
	tenantToken := encodeDigest(s.keys.digest(tenantFrame))
	sessionToken := encodeDigest(s.keys.digest(sessionFrame))
	tenantNamespace := "tenants/" + tenantToken
	sessionNamespace := tenantNamespace + "/sessions/" + sessionToken
	return sessionScope{
		TenantNamespace:   tenantNamespace,
		SessionNamespace:  sessionNamespace,
		LedgerName:        sessionNamespace + "/journal",
		LeaseName:         sessionNamespace + "/lease",
		CatalogKey:        sessionNamespace + "/catalog",
		CatalogListPrefix: tenantNamespace + "/sessions/",
		BlobPrefix:        sessionNamespace + "/blobs/",
		JournalName:       sessionNamespace + "/journal",
		tenantWitnessKey:  witnessKey("tenant", tenantToken),
		tenantWitness:     encodeWitness(1, []byte(tenant)),
		sessionWitnessKey: witnessKey("session", sessionToken),
		sessionWitness:    encodeWitness(2, []byte(tenant), []byte(session)),
	}, nil
}

// verifySessionScope verifies collision bindings for an already-derived
// canonical scope without creating metadata. A missing binding fails closed.
func (s *Store) verifySessionScope(ctx context.Context, scope sessionScope) error {
	if scope.tenantWitnessKey == "" {
		return nil
	}
	if err := s.keys.verifyWitness(ctx, scope.tenantWitnessKey, scope.tenantWitness); err != nil {
		return err
	}
	if err := s.keys.verifyWitness(ctx, scope.sessionWitnessKey, scope.sessionWitness); err != nil {
		return err
	}
	return nil
}

// bindSessionScope create-only binds collision witnesses before a caller may
// create any canonical session data. Legacy scopes require no witnesses.
func (s *Store) bindSessionScope(ctx context.Context, scope sessionScope) error {
	if scope.tenantWitnessKey == "" {
		return nil
	}
	if err := s.keys.bindWitness(ctx, scope.tenantWitnessKey, scope.tenantWitness); err != nil {
		return err
	}
	if err := s.keys.bindWitness(ctx, scope.sessionWitnessKey, scope.sessionWitness); err != nil {
		return err
	}
	return nil
}

func digestFrame(domain string, values ...[]byte) []byte {
	size := 4 + len(domain)
	for _, value := range values {
		size += 4 + len(value)
	}
	out := make([]byte, 0, size)
	out = binary.BigEndian.AppendUint32(out, checkedUint32Length(len(domain)))
	out = append(out, domain...)
	for _, value := range values {
		out = binary.BigEndian.AppendUint32(out, checkedUint32Length(len(value)))
		out = append(out, value...)
	}
	return out
}

func encodeDigest(sum [32]byte) string {
	return strings.ToLower(base32.HexEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:]))
}

func encodeWitness(kind byte, values ...[]byte) []byte {
	size := 6
	for _, value := range values {
		size += 2 + len(value)
	}
	out := make([]byte, 0, size)
	out = append(out, 'L', 'R', 'W', 'B', 1, kind)
	for _, value := range values {
		out = binary.BigEndian.AppendUint16(out, checkedUint16Length(len(value)))
		out = append(out, value...)
	}
	return out
}

func checkedUint16Length(length int) uint16 {
	if length < 0 || length > math.MaxUint16 {
		panic("sessionstore: internal uint16 length invariant")
	}
	return uint16(length)
}

func checkedUint32Length(length int) uint32 {
	if length < 0 || uint64(length) > math.MaxUint32 {
		panic("sessionstore: internal uint32 length invariant")
	}
	return uint32(length)
}

func witnessKey(kind, token string) string { return "sessionstore/witnesses/" + kind + "/" + token }

func (k keyspace) verifyWitness(ctx context.Context, key string, want []byte) error {
	got, _, err := k.kv.Get(ctx, key)
	if err != nil {
		if isKeyNotFound(err, key) {
			return &KeyspaceError{Code: KeyspaceBindingNotFound, Cause: err}
		}
		return &KeyspaceError{Code: KeyspaceBackend, Cause: err}
	}
	if !bytes.Equal(got, want) {
		return &KeyspaceError{Code: KeyspaceHashCollision}
	}
	return nil
}

func (k keyspace) bindWitness(ctx context.Context, key string, want []byte) error {
	got, _, err := k.kv.Get(ctx, key)
	if err == nil {
		if !bytes.Equal(got, want) {
			return &KeyspaceError{Code: KeyspaceHashCollision}
		}
		return nil
	}
	if !isKeyNotFound(err, key) {
		return &KeyspaceError{Code: KeyspaceBackend, Cause: err}
	}
	if _, err = k.kv.Put(ctx, key, 0, want); err == nil {
		return nil
	}
	if !isCreateConflict(err, key) {
		return &KeyspaceError{Code: KeyspaceBackend, Cause: err}
	}
	got, _, err = k.kv.Get(ctx, key)
	if err != nil {
		if isKeyNotFound(err, key) {
			return &KeyspaceError{Code: KeyspaceBindingAmbiguous, Cause: err}
		}
		return &KeyspaceError{Code: KeyspaceBackend, Cause: err}
	}
	if !bytes.Equal(got, want) {
		return &KeyspaceError{Code: KeyspaceHashCollision}
	}
	return nil
}

func isKeyNotFound(err error, key string) bool {
	var target *storage.KeyNotFoundError
	return errors.As(err, &target) && target.Key == key
}

func isCreateConflict(err error, key string) bool {
	var target *storage.ConflictError
	return errors.As(err, &target) && target.Name == key && target.Expected == 0
}

func isCanonicalLegacySessionID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i := 0; i < len(id); i++ {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if id[i] != '-' {
				return false
			}
			continue
		}
		if !((id[i] >= '0' && id[i] <= '9') || (id[i] >= 'a' && id[i] <= 'f')) {
			return false
		}
	}
	return true
}
