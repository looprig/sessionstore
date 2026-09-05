package sessionstore

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

func FuzzObjectMetadataRecord(f *testing.F) {
	metadata := objectMetadataFor(ObjectKindToolResult, [16]byte{1}, 7, sha256.Sum256([]byte("payload")), "text/plain")
	req := GetObjectMetadataRequest{TenantID: "tenant", SessionID: "session", ExpectedKind: ObjectKindToolResult, Reference: metadata.Reference}
	valid := encodeObjectMetadataRecord(req.TenantID, req.SessionID, metadata)
	f.Add(valid)
	f.Add([]byte("LROM\x01"))
	f.Add(append(bytes.Clone(valid), 0))
	f.Add(bytes.Repeat([]byte{0}, maxObjectMetadataRecordBytes+1))
	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := decodeObjectMetadataRecord(data, req)
		if bytes.Equal(data, valid) && (err != nil || got != metadata) {
			t.Fatalf("valid seed rejected: %+v, %v", got, err)
		}
		if err == nil {
			if len(data) > maxObjectMetadataRecordBytes {
				t.Fatal("accepted oversized record")
			}
			if _, err := parseObjectMetadata(got); err != nil {
				t.Fatalf("accepted invalid metadata: %v", err)
			}
			if !bytes.Equal(encodeObjectMetadataRecord(req.TenantID, req.SessionID, got), data) {
				t.Fatal("accepted noncanonical record")
			}
		}
	})
}
