package sessionstore

import (
	"context"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
)

// This file is the PRODUCTION settlement evidence boundary: the one
// implementation of DispositionEvidenceReader in this module, over the bound
// session's own journal.
//
// Until it existed the only implementation anywhere was a test fake, which is
// why the settlement verifier one file over could be changed without breaking a
// consumer. It is a narrow, trusted reader and nothing more: it resolves the
// session named in the store-derived request, walks that session's journal to
// its captured tip, and reports the ONE durable disposition record that names
// the attempt. It applies no SETTLEMENT policy — it decides nothing about the
// command — though it does refuse four things outright, listed on
// ReadDispositionEvidence below; every conclusion drawn from what it returns is
// drawn by verifiedDispositionOutcome.

// WithJournalDispositionEvidence makes the Store its own settlement evidence
// reader, over each session's bound journal.
//
// It is opt-in rather than the default because reading evidence is a privileged
// whole-journal replay, and a deployment that obtains its evidence elsewhere —
// or that never settles a disposition command at all — should not acquire that
// behaviour by omission.
//
// It does not stack with WithDispositionEvidence, in either order, and it does
// not stack with itself. A store with two configured readers has two answers to
// one question and no rule for choosing between them, and silently keeping the
// last one would make the ORDER of two options decide which journal a
// settlement believes.
func WithJournalDispositionEvidence() Option {
	return func(cfg *config) error {
		if cfg.evidence != nil || cfg.journalEvidence {
			return &InvalidOptionError{Field: "DispositionEvidence"}
		}
		cfg.journalEvidence = true
		return nil
	}
}

// ReadDispositionEvidence reports the single committed disposition record that
// names the request's attempt, read from the bound session's journal.
//
// The scope is the request's OWN session, which the store derived from its
// immutable inbox record; nothing a settlement caller supplied reaches here.
// The walk is the privileged runtime replay, continued page by page to the tip
// the first page captured, so the answer is true of one consistent snapshot.
//
// Four refusals are the contract, and each is a refusal rather than a quieter
// answer for a stated reason:
//
//   - ABSENCE is refused. A journal holding no disposition for this attempt is
//     not proof that the runtime did not apply the command; it is proof only
//     that nothing said so yet. Returning empty evidence would hand the verifier
//     a zero value, and the design of DispositionEvidenceReader is that a reader
//     which does that settles nothing.
//   - MORE THAN ONE record is refused. Two records naming one attempt are two
//     answers and this reader has no rule for choosing. They are necessarily
//     distinct: they sit at different sequences.
//   - A CONFLICTING record — one that names this attempt but disagrees about
//     the command, the durable runtime mapping or the kind — is refused as a
//     conflict and never stepped over as though it were about something else.
//     Stepping over it would report "no disposition" for a journal that plainly
//     holds one.
//   - A record whose own LeaseEpoch disagrees with the nearest preceding
//     OPENING FENCE is refused, and the fence is what the author grant is taken
//     from. This is required rather than defensive: harness bypasses this
//     package's JournalWriter — it calls EncodeEnvelope and appends the bytes
//     itself — so stampWriterOwnedFields never runs and the stored LeaseEpoch is
//     a CLAIM BY THE WRITER. Believing it would let a record written under grant
//     9 settle as a recovery closure authored by grant 10, which is exactly the
//     statement a closure is trusted for.
//
// A journal fault — an undecodable frame, a stream that ends short of the tip it
// captured, an unavailable or cancelled provider — propagates in the journal's
// own vocabulary. It is a fault of the stream, never a finding about the
// command, and a caller separates the two by type.
func (s *Store) ReadDispositionEvidence(ctx context.Context, req DispositionEvidenceRequest) (DispositionEvidence, error) {
	// The durable runtime mapping is compared as the UUID the envelope carries.
	// A stored mapping that is not a UUID parses to the ZERO UUID, and no
	// decoded disposition can carry one — validateEnvelope refuses it — so such
	// a record is reported as a conflict rather than silently matching.
	runtime, _ := uuid.Parse(string(req.RuntimeCommandID))

	var (
		found      DispositionEvidence
		haveFound  bool
		fenceEpoch uint64
		fenceSeq   uint64
		cursor     sessionwire.Cursor
	)
	for {
		page, err := s.ReadRuntimeJournal(ctx, ReadRuntimeJournalRequest{
			TenantID:  req.TenantID,
			SessionID: req.SessionID,
			Cursor:    cursor,
		})
		if err != nil {
			return DispositionEvidence{}, err
		}
		for _, record := range page.Records {
			env := record.Envelope
			if env.Kind == EnvelopeKindOpeningFence {
				fenceEpoch, fenceSeq = env.LeaseEpoch, record.Seq
				continue
			}
			if env.Kind != EnvelopeKindCommandDisposition || env.AttemptID != string(req.Attempt.AttemptID) {
				continue
			}
			if env.CommandID != req.CommandID {
				return DispositionEvidence{}, inboxErr(InboxErrorEvidence, "command_id", nil)
			}
			if env.RuntimeCommandID != runtime {
				return DispositionEvidence{}, inboxErr(InboxErrorEvidence, "runtime_command_id", nil)
			}
			if env.CommandKind != string(req.Kind) {
				return DispositionEvidence{}, inboxErr(InboxErrorEvidence, "command_kind", nil)
			}
			// A disposition ahead of every opening fence has no author grant to
			// be read under at all. It is refused here rather than defaulted to
			// the record's own claim, which is the whole point of the check
			// below.
			if fenceEpoch == 0 {
				return DispositionEvidence{}, inboxErr(InboxErrorEvidence, "opening_fence", nil)
			}
			if env.LeaseEpoch != fenceEpoch {
				return DispositionEvidence{}, inboxErr(InboxErrorEvidence, "lease_epoch", nil)
			}
			if haveFound {
				return DispositionEvidence{}, inboxErr(InboxErrorEvidence, "disposition", nil)
			}
			kind := DispositionOutcomeKind(env.DispositionKind)
			evidence := DispositionEvidence{
				AttemptID:           req.Attempt.AttemptID,
				Kind:                kind,
				AttemptJournalEpoch: JournalEpoch(env.AttemptJournalEpoch),
				AuthorJournalEpoch:  JournalEpoch(fenceEpoch),
				DispositionSeq:      record.Seq,
			}
			// Only a recovery closure names an author fence, because only a
			// closure is authored by a grant the attempt did not select and so
			// only a closure has a later fence to be held to. An application or
			// a refusal names none, and the verifier refuses one that does.
			if kind == DispositionNotApplied {
				evidence.AuthorFenceSeq = fenceSeq
			}
			found, haveFound = evidence, true
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if !haveFound {
		return DispositionEvidence{}, inboxErr(InboxErrorEvidence, "disposition", nil)
	}
	return found, nil
}
