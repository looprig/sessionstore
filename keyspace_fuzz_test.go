package sessionstore

import (
	"bytes"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

func FuzzLayoutMarkerCodec(f *testing.F) {
	f.Add([]byte{'L', 'R', 'K', 'S', 1, byte(layoutTenantV1), 1, 0, 0})
	f.Add(encodeLayoutMarker(layoutLegacySingleTenantV1, "tenant"))
	f.Add([]byte("tenant-v1"))
	f.Add([]byte{'L', 'R', 'K', 'S', 1, byte(layoutLegacySingleTenantV1), 1, 0, 1, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		if err := validateLayoutMarker(data); err != nil {
			return
		}
		layout := keyspaceLayout(data[5])
		tenant := sessionwire.TenantID(string(data[9:]))
		if !bytes.Equal(data, encodeLayoutMarker(layout, tenant)) {
			t.Fatalf("accepted noncanonical marker %x", data)
		}
	})
}
