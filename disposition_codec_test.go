package sessionstore

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestDispositionCodecRejectsCorruptionAndLegacy(t *testing.T) {
	s := openTestStore(t)
	createDispositionCatalog(t, s)
	req := dispositionRequest()
	entry, _, err := s.AdmitDispositionCommand(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	value, _, err := encodeDispositionInboxRecord(entry.Record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeInboxRecord(value); err == nil {
		t.Fatal("legacy decoder accepted disposition bytes")
	}
	legacy, _, err := encodeInboxRecord(testInboxRecord())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeDispositionInboxRecord(legacy); err == nil {
		t.Fatal("new decoder adopted legacy")
	}
	for name, mutate := range map[string]func([]byte) []byte{
		"future version": func(v []byte) []byte {
			return bytes.Replace(v, []byte(`"record_version":2`), []byte(`"record_version":3`), 1)
		},
		"nonpending": func(v []byte) []byte {
			return bytes.Replace(v, []byte(`"state":"pending"`), []byte(`"state":"applying"`), 1)
		},
		"duplicate field": func(v []byte) []byte {
			return bytes.Replace(v, []byte(`"kind":"input"`), []byte(`"kind":"other","kind":"input"`), 1)
		},
		"unknown field": func(v []byte) []byte { return append([]byte(`{"surprise":true,`), v[1:]...) },
		"size mismatch": func(v []byte) []byte {
			return bytes.Replace(v, []byte(`"payload_size":43`), []byte(`"payload_size":44`), 1)
		},
		"digest mismatch": func(v []byte) []byte {
			return bytes.Replace(v, []byte(`"payload_digest":"8`), []byte(`"payload_digest":"9`), 1)
		},
		"legacy mode": func(v []byte) []byte {
			return bytes.Replace(v, []byte(`"protocol_mode":"disposition"`), []byte(`"protocol_mode":"legacy"`), 1)
		},
		"empty runtime": func(v []byte) []byte {
			return bytes.Replace(v, []byte(`"runtime_command_id":"`+string(inboxRuntime)+`"`), []byte(`"runtime_command_id":""`), 1)
		},
		"trailing value": func(v []byte) []byte { return append(v, []byte(`{}`)...) },
		"oversized":      func(v []byte) []byte { return bytes.Repeat([]byte("x"), MaxInboxRecordBytes+1) },
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeDispositionInboxRecord(mutate(bytes.Clone(value))); err == nil {
				t.Fatal("corrupt record accepted")
			}
		})
	}
}

func FuzzDispositionInboxCodec(f *testing.F) {
	req := dispositionRequest()
	// Literal seed built independently of the production encoder exercises the
	// versioned boundary even if the encoder later changes its shape.
	digest := "8fd68685d18fb8be484a01e488c472af1b73b2248216183d8bdc7545f3e2d361"
	r := DispositionInboxRecord{Descriptor: DispositionCommandDescriptor{TenantID: req.TenantID, SessionID: req.SessionID, CommandID: req.CommandID, Binding: req.Binding, RuntimeCommandID: req.ProposedRuntimeCommandID, Kind: req.Kind, Payload: req.Payload, PayloadDigest: digest, PayloadSize: 43}, AcceptedAt: req.AcceptedAt, ApplyDeadline: req.ApplyDeadline, State: InboxStatePending}
	v, _, err := encodeDispositionInboxRecord(r)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(v)
	f.Add([]byte(`{"record_version":1}`))
	f.Add([]byte(`null`))
	object := objectMetadataFor(ObjectKindCommandPayload, [16]byte{1}, 43, [32]byte{1}, "text/plain")
	r.Descriptor.Payload = nil
	r.Descriptor.PayloadObject = &object
	r.Descriptor.PayloadDigest = object.Digest[len("sha256:"):]
	v, _, err = encodeDispositionInboxRecord(r)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(v)
	f.Fuzz(func(t *testing.T, value []byte) {
		r, err := decodeDispositionInboxRecord(value)
		if err != nil {
			return
		}
		encoded, canonical, err := encodeDispositionInboxRecord(r)
		if err != nil {
			t.Fatal(err)
		}
		again, err := decodeDispositionInboxRecord(encoded)
		if err != nil {
			t.Fatal(err)
		}
		a, _ := json.Marshal(canonical)
		b, _ := json.Marshal(again)
		if !bytes.Equal(a, b) {
			t.Fatal("codec not fixed point")
		}
		if r.State != InboxStatePending {
			t.Fatal("codec grants progress")
		}
	})
}
