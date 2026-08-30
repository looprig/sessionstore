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
	bodyDigest := sha256.Sum256(body)
	valid := objectMetadataFor(ObjectKindArtifact, [16]byte{1}, uint64(len(body)), bodyDigest, "application/octet-stream")
	f.Add(valid.Reference.ObjectID, valid.Digest, valid.SizeBytes, valid.MediaType)
	f.Add("v1:artifact:bad:bad", "sha256:bad", uint64(0), "")
	f.Fuzz(func(t *testing.T, id, digest string, size uint64, mediaType string) {
		metadata := sessionwire.ObjectMetadata{Reference: sessionwire.ObjectReference{ObjectID: id}, Digest: digest, SizeBytes: size, MediaType: mediaType}
		parsed, err := parseObjectMetadata(metadata)
		if metadata == valid && err != nil {
			t.Fatalf("canonical seed rejected: %v", err)
		}
		if err == nil {
			generation, decodeErr := decodeObjectGeneration(parsed.generation)
			if decodeErr != nil {
				t.Fatalf("accepted generation %q that does not decode: %v", parsed.generation, decodeErr)
			}
			roundTrip := objectMetadataFor(parsed.kind, generation, parsed.size, parsed.digest, metadata.MediaType)
			if roundTrip != metadata {
				t.Fatalf("accepted noncanonical metadata: got=%+v canonical=%+v", metadata, roundTrip)
			}
			if _, err := parseObjectMetadata(roundTrip); err != nil {
				t.Fatalf("canonical reparse: %v", err)
			}
		}
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
		wantSuccess := matching && size == uint64(len(body))
		if (err == nil) != wantSuccess || verifier.verified != wantSuccess {
			t.Fatalf("len=%d size=%d matching=%v err=%v verified=%v wantSuccess=%v", len(body), size, matching, err, verifier.verified, wantSuccess)
		}
	})
}
