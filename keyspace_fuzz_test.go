package sessionstore

import (
	"bytes"
	"encoding/binary"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

func FuzzLayoutMarkerCodec(f *testing.F) {
	// The seeds spell the CURRENT codec version. A seed carrying an older one
	// is refused by validateLayoutMarker and returns before the round trip
	// below, so it would be a seed that never reaches the logic under test.
	f.Add([]byte{'L', 'R', 'K', 'S', markerCodecVersion, byte(layoutTenantV1), 1, 0, DefaultControlShards, 0, 0})
	f.Add(encodeLayoutMarker(layoutLegacySingleTenantV1, "tenant", DefaultControlShards))
	f.Add(encodeLayoutMarker(layoutTenantV1, "", MaxControlShards))
	f.Add([]byte("tenant-v1"))
	f.Add([]byte{'L', 'R', 'K', 'S', markerCodecVersion, byte(layoutLegacySingleTenantV1), 1, 0, 1, 0, 1, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		if err := validateLayoutMarker(data); err != nil {
			return
		}
		layout := keyspaceLayout(data[5])
		tenant := sessionwire.TenantID(string(data[layoutMarkerHeaderBytes:]))
		shards := uint32(binary.BigEndian.Uint16(data[7:9]))
		if !bytes.Equal(data, encodeLayoutMarker(layout, tenant, shards)) {
			t.Fatalf("accepted noncanonical marker %x", data)
		}
	})
}
