package sessionstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

type publicCreateFault struct {
	storage.OrderedIndex
	namespace   string
	commit      bool
	cancel      context.CancelFunc
	winner      storage.OrderedRecord
	acknowledge bool
}

func (f *publicCreateFault) Create(ctx context.Context, id storage.OrderedID, scope string, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, bool, error) {
	if f.namespace == "" || !strings.HasPrefix(id.Namespace, f.namespace) {
		return f.OrderedIndex.Create(ctx, id, scope, value, rank, due)
	}
	f.namespace = ""
	if f.commit {
		r, _, err := f.OrderedIndex.Create(ctx, id, scope, value, rank, due)
		if err != nil {
			return storage.OrderedRecord{}, false, err
		}
		f.winner = r
	}
	if f.cancel != nil {
		f.cancel()
	}
	if f.acknowledge {
		return f.winner, true, nil
	}
	return storage.OrderedRecord{}, false, errors.New("injected ambiguous write")
}

func TestPublicCreateCanceledSuccessfulProviderResponseIsNotACK(t *testing.T) {
	for _, stage := range []string{catalogNamespace, dispositionInboxNamespace} {
		t.Run(stage, func(t *testing.T) {
			b := memstore.New()
			f := &publicCreateFault{OrderedIndex: b.OrderedIndex}
			b.OrderedIndex = f
			s := openStore(t, b)
			r := publicCreateRequest()
			if stage == dispositionInboxNamespace {
				if _, err := s.PreparePublicCreate(t.Context(), r); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			f.namespace, f.commit, f.cancel, f.acknowledge = stage, true, cancel, true
			if stage == catalogNamespace {
				if _, err := s.PreparePublicCreate(ctx, r); err == nil {
					t.Fatal("canceled catalog response returned preparation")
				}
			} else {
				if _, _, err := s.AdmitPublicCreate(ctx, AdmitPublicCreateRequest{Identity: r.Identity, Payload: inboxPayload}); err == nil {
					t.Fatal("canceled inbox response acknowledged")
				}
			}
		})
	}
}

func TestPublicCreateWriteFailuresResumeAcrossRealRestart(t *testing.T) {
	for _, stage := range []string{publicCreateNamespace, catalogNamespace, dispositionInboxNamespace} {
		for _, commit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/commit=%v", stage, commit), func(t *testing.T) {
				b := memstore.New()
				f := &publicCreateFault{OrderedIndex: b.OrderedIndex}
				b.OrderedIndex = f
				s := openStore(t, b)
				r := publicCreateRequest()
				if stage == dispositionInboxNamespace {
					if _, err := s.PreparePublicCreate(t.Context(), r); err != nil {
						t.Fatal(err)
					}
				}
				f.namespace, f.commit = stage, commit
				if stage == dispositionInboxNamespace {
					e, created, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, Payload: inboxPayload})
					if err == nil || created || !reflect.DeepEqual(e, DispositionInboxEntry{}) {
						t.Fatalf("false ACK: %+v %v %v", e, created, err)
					}
				} else {
					p, err := s.PreparePublicCreate(t.Context(), r)
					if err == nil || !reflect.DeepEqual(p, PublicCreatePreparation{}) {
						t.Fatalf("false preparation: %+v %v", p, err)
					}
				}
				if err := s.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
				s = openStore(t, b)
				r.ProposedRuntimeCommandID = inboxRetryRuntime
				r.AcceptedAt = r.AcceptedAt.Add(time.Minute)
				r.ApplyDeadline = r.ApplyDeadline.Add(time.Minute)
				p, err := s.PreparePublicCreate(t.Context(), r)
				if err != nil {
					t.Fatal(err)
				}
				want := inboxRuntime
				if stage == publicCreateNamespace && !commit {
					want = inboxRetryRuntime
				}
				if p.Reservation.RuntimeCommandID != want {
					t.Fatalf("reservation reminted: %+v", p)
				}
				e, created, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, Payload: inboxPayload})
				if err != nil || created == (stage == dispositionInboxNamespace && commit) {
					t.Fatalf("resume: %+v %v %v", e, created, err)
				}
				if e.Record.Descriptor.RuntimeCommandID != want || !e.Record.AcceptedAt.Equal(p.Reservation.AcceptedAt) || !e.Record.ApplyDeadline.Equal(p.Reservation.ApplyDeadline) {
					t.Fatal("winner lost")
				}
				if stage == dispositionInboxNamespace && commit {
					if e.AcceptedOrder != f.winner.Order || e.Revision != f.winner.Revision {
						t.Fatal("inbox order/revision lost")
					}
				}
			})
		}
	}
}

func TestPublicCreateTwelveReplicasAndCompetingIdentities(t *testing.T) {
	for _, mode := range []string{"duplicate", "different commands", "different sessions"} {
		t.Run(mode, func(t *testing.T) {
			b := memstore.New()
			stores := make([]*Store, 12)
			for i := range stores {
				stores[i] = openStore(t, b)
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			results := make(chan DispositionInboxEntry, 12)
			var errs sync.Map
			for n, s := range stores {
				wg.Add(1)
				go func(n int, s *Store) {
					defer wg.Done()
					<-start
					r := publicCreateRequest()
					r.ProposedRuntimeCommandID = RuntimeCommandID(fmt.Sprintf("runtime:%d", n))
					if mode == "different commands" {
						r.Identity.CommandID = sessionwire.CommandID(fmt.Sprintf("Command/%d:Upper", n))
					}
					if mode == "different sessions" {
						r.Identity.SessionID = sessionwire.SessionID(fmt.Sprintf("Session/%d:Upper", n))
					}
					if _, err := s.PreparePublicCreate(t.Context(), r); err != nil {
						errs.Store(n, err)
						return
					}
					e, _, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, Payload: inboxPayload})
					if err != nil {
						errs.Store(n, err)
						return
					}
					results <- e
				}(n, s)
			}
			close(start)
			wg.Wait()
			close(results)
			count := 0
			var winner DispositionInboxEntry
			for e := range results {
				if count > 0 && !reflect.DeepEqual(winner, e) {
					t.Fatal("replicas disagree")
				}
				winner = e
				count++
			}
			want := 1
			if mode == "duplicate" {
				want = 12
			}
			if count != want {
				t.Fatalf("acknowledgments=%d want %d errors=%v", count, want, &errs)
			}
		})
	}
}

func TestPublicCreateLargePayloadIndependentUploadsAndDesiredMoves(t *testing.T) {
	b := memstore.New()
	s := openStore(t, b)
	r := publicCreateRequest()
	body := bytes.Repeat([]byte("x"), MaxInboxPayloadBytes+1)
	d := sha256.Sum256(body)
	r.Identity.PayloadDigest = hex.EncodeToString(d[:])
	r.Identity.PayloadSize = uint64(len(body))
	r.InitialWorkload = testDesiredWorkload()
	p, err := s.PreparePublicCreate(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	first := uploadCommandPayload(t, s, body)
	second := uploadCommandPayload(t, s, body)
	if first.Reference == second.Reference {
		t.Fatal("uploads not independent")
	}
	e, _, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, PayloadObject: &first})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.UpdateCatalogDesiredState(t.Context(), UpdateCatalogDesiredStateRequest{TenantID: r.Identity.TenantID, SessionID: r.Identity.SessionID, ExpectedRevision: p.Catalog.Revision, IdempotencyKey: "move", DesiredPlacement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: "new-runtime"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, b)
	r.InitialWorkload = DesiredWorkload{}
	got, err := s.PreparePublicCreate(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Reservation, p.Reservation) || got.Catalog.Record.DesiredPlacement != sessionwire.HostPlacementDedicated {
		t.Fatal("mutable desired state affected reservation")
	}
	again, created, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, PayloadObject: &second})
	if err != nil || created || !reflect.DeepEqual(again, e) {
		t.Fatalf("upload retry: %+v %v %v", again, created, err)
	}
}

func TestPublicCreateCancellationNeverAcknowledges(t *testing.T) {
	b := memstore.New()
	f := &publicCreateFault{OrderedIndex: b.OrderedIndex}
	b.OrderedIndex = f
	s := openStore(t, b)
	r := publicCreateRequest()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.PreparePublicCreate(ctx, r); err == nil {
		t.Fatal("canceled preparation")
	}
	if _, err := s.PreparePublicCreate(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(t.Context())
	f.namespace = dispositionInboxNamespace
	f.commit = true
	f.cancel = cancel
	if e, created, err := s.AdmitPublicCreate(ctx, AdmitPublicCreateRequest{Identity: r.Identity, Payload: inboxPayload}); err == nil || created || !reflect.DeepEqual(e, DispositionInboxEntry{}) {
		t.Fatal("canceled committed write acknowledged")
	}
	if err := s.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, b)
	if _, created, err := s.AdmitPublicCreate(t.Context(), AdmitPublicCreateRequest{Identity: r.Identity, Payload: inboxPayload}); err != nil || created {
		t.Fatalf("retry: %v %v", created, err)
	}
}
