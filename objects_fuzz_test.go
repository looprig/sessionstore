package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

func FuzzParseObjectMetadata(f *testing.F) {
	body := []byte("seed")
	digest := sha256.Sum256(body)
	valid := objectMetadataFor(ObjectKindArtifact, [16]byte{1}, uint64(len(body)), digest, "application/octet-stream")
	f.Add(valid.Reference.ObjectID, valid.Digest, valid.SizeBytes, valid.MediaType)
	f.Add("v1:artifact:bad:bad", "sha256:bad", uint64(0), "")
	f.Fuzz(func(t *testing.T, id, digest string, size uint64, mediaType string) {
		_, _ = parseObjectMetadata(sessionwire.ObjectMetadata{Reference: sessionwire.ObjectReference{ObjectID: id}, Digest: digest, SizeBytes: size, MediaType: mediaType})
	})
}

func FuzzExactVerifier(f *testing.F) {
	f.Add([]byte("seed"), uint64(4), true)
	f.Add([]byte{}, uint64(0), true)
	f.Add([]byte("long"), uint64(3), false)
	f.Fuzz(func(t *testing.T, body []byte, size uint64, matching bool) {
		if size > uint64(len(body)+8) {
			size = uint64(len(body) + 8)
		}
		digest := sha256.Sum256(body)
		if !matching {
			digest[0] ^= 1
		}
		verifier := newExactVerifier(context.Background(), bytes.NewReader(body), size, digest)
		_, err := io.ReadAll(verifier)
		if err == nil != verifier.verified {
			t.Fatalf("err=%v verified=%v", err, verifier.verified)
		}
	})
}
