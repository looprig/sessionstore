package sessionstore

import (
	"bytes"
	"context"
)

// ProtocolMode identifies a session's immutable dispatch protocol. Record
// upgrades do not convert this mode; conversion requires an offline migration.
type ProtocolMode string

const (
	// ProtocolModeLegacy uses the released single-store epoch protocol.
	ProtocolModeLegacy ProtocolMode = "legacy"
	// ProtocolModeDisposition reserves the independent ownership/settlement
	// protocol. Its execution APIs are not implemented by this prerequisite.
	ProtocolModeDisposition ProtocolMode = "disposition"
)

// SessionBinding pins the storage configuration and runtime identity chosen at
// creation. IDs are bounded opaque configuration names, not provider keys or
// credentials. BindingVersion identifies immutable configuration, never the
// current agent default. The all-zero value denotes an unbound legacy record.
// A nonzero binding must contain all four members.
type SessionBinding struct {
	StorageBindingID string       `json:"storage_binding_id"`
	BindingVersion   string       `json:"binding_version"`
	RuntimeSessionID string       `json:"runtime_session_id"`
	ProtocolMode     ProtocolMode `json:"protocol_mode"`
}

func (b SessionBinding) validate() error {
	for _, field := range []struct{ name, value string }{
		{"binding.storage_binding_id", b.StorageBindingID},
		{"binding.binding_version", b.BindingVersion},
		{"binding.runtime_session_id", b.RuntimeSessionID},
	} {
		if err := validateOpaque(field.value, field.name, catalogInvalid); err != nil {
			return err
		}
	}
	if b.ProtocolMode != ProtocolModeLegacy && b.ProtocolMode != ProtocolModeDisposition {
		return catalogInvalid("binding.protocol_mode", nil)
	}
	return nil
}

// catalogBindingWire deliberately embeds the unchanged v1 wire. A v1 decoder
// has no binding field to silently accept; a v2 decoder requires it below.
type catalogBindingWire struct {
	catalogWire
	Binding SessionBinding `json:"binding"`
}

// CatalogBindingRecordVersion adds a required complete immutable binding to
// the catalog. Legacy records retain CatalogRecordVersion and their bytes.
const CatalogBindingRecordVersion uint8 = 2

// bindProtocolMode is a create-only race fence, NOT a storage binding. Its
// bytes contain only the collision identity and protocol. The catalog's one
// OrderedIndex.Create still chooses the entire winning SessionBinding. A crash
// here can reserve a mode without a catalog; any same-mode proposal can retry
// and win that catalog, even with different binding/configuration IDs.
//
// An old session collision witness without a protocol witness is conservatively
// legacy, including one that never wrote data after binding its scope. This
// prevents a late catalog create from converting prior journals or inbox rows.
// All new legacy creation paths pin their mode BEFORE binding collision
// witnesses, closing the race between that test and the create-only pin.
// Old binaries do not know this fence and MUST be excluded from stores serving
// disposition sessions. This witness cannot fence unaware writers.
func (s *Store) bindProtocolMode(ctx context.Context, scope sessionScope, mode ProtocolMode) error {
	if err := s.validateSessionScope(scope); err != nil {
		return err
	}
	if mode != ProtocolModeLegacy && mode != ProtocolModeDisposition {
		return catalogInvalid("binding.protocol_mode", nil)
	}
	if scope.layout == layoutLegacySingleTenantV1 {
		if mode != ProtocolModeLegacy {
			return catalogInvalid("binding.protocol_mode", nil)
		}
		return nil
	}
	key := scope.SessionNamespace + "/protocol"
	want := encodeWitness(1, scope.sessionWitness, []byte(mode))
	check := func(got []byte) error {
		if !bytes.Equal(got, want) {
			other := ProtocolModeLegacy
			if mode == ProtocolModeLegacy {
				other = ProtocolModeDisposition
			}
			if bytes.Equal(got, encodeWitness(1, scope.sessionWitness, []byte(other))) {
				return catalogErr(CatalogErrorConflict, "binding.protocol_mode", nil)
			}
			return &KeyspaceError{Code: KeyspaceHashCollision}
		}
		return nil
	}
	got, _, err := s.keys.kv.Get(ctx, key)
	if err == nil {
		return check(got)
	}
	if !isKeyNotFound(err, key) {
		return catalogErr(CatalogErrorBackend, "binding.protocol_mode", err)
	}
	if mode == ProtocolModeDisposition {
		_, _, oldErr := s.keys.kv.Get(ctx, scope.sessionWitnessKey)
		if oldErr == nil {
			// Another same-mode creator may have installed both witnesses since our
			// first read. Only its committed mode pin can permit this retry.
			got, _, err = s.keys.kv.Get(ctx, key)
			if err == nil {
				return check(got)
			}
			if !isKeyNotFound(err, key) {
				return catalogErr(CatalogErrorBackend, "binding.protocol_mode", err)
			}
			return catalogErr(CatalogErrorConflict, "binding.protocol_mode", nil)
		}
		if !isKeyNotFound(oldErr, scope.sessionWitnessKey) {
			return catalogErr(CatalogErrorBackend, "binding.protocol_mode", oldErr)
		}
	}
	if _, err := s.keys.kv.Put(ctx, key, 0, want); err != nil {
		// CAS conflicts and ambiguous acknowledgements are resolved only by the
		// committed immutable pin. An unavailable read never grants permission.
		got, _, readErr := s.keys.kv.Get(ctx, key)
		if readErr != nil {
			return catalogErr(CatalogErrorBackend, "binding.protocol_mode", readErr)
		}
		return check(got)
	}
	return nil
}
