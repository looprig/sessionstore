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
	// Exact length with a wrong digest: the one seed that reaches the digest
	// comparison rather than failing on size first.
	f.Add([]byte("seed"), uint64(4), false)
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
		// Draining without an error must imply verification, and verification
		// must imply a clean drain. verifyPersisted relies on the first half to
		// have no separate "copied but unverified" branch, so the implication is
		// enforced here rather than argued in a comment.
		if (err == nil) != verifier.verified {
			t.Fatalf("drain and verification disagree: err=%v verified=%v (len=%d size=%d matching=%v)", err, verifier.verified, len(body), size, matching)
		}
		// The oracle: exactly a body of the declared length with the declared
		// digest verifies, and nothing else does.
		wantSuccess := matching && size == uint64(len(body))
		if verifier.verified != wantSuccess {
			t.Fatalf("verified=%v wantSuccess=%v (len=%d size=%d matching=%v err=%v)", verifier.verified, wantSuccess, len(body), size, matching, err)
		}
	})
}
