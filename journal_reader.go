package sessionstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

// ReadPublicJournalRequest positions one bounded public journal page. FromSeq
// and Cursor are mutually exclusive: a caller starts with FromSeq (inclusive,
// zero meaning the first record) and then follows the returned cursor.
type ReadPublicJournalRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	FromSeq   uint64
	Cursor    sessionwire.Cursor
	Limit     int
}

// ReadRuntimeJournalRequest positions one bounded privileged replay page. Its
// positioning rules match ReadPublicJournalRequest.
type ReadRuntimeJournalRequest struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	FromSeq   uint64
	Cursor    sessionwire.Cursor
	Limit     int
}

// RuntimeRecord is one raw journal record as it is stored. Object-backed bodies
// stay unresolved: the caller decides which of them to fetch, through the
// ordinary object API, so a replay never pays for bytes it does not want.
type RuntimeRecord struct {
	Seq      uint64
	Envelope Envelope
}

// RuntimePage is a bounded privileged replay page captured at CapturedTip.
type RuntimePage struct {
	Records        []RuntimeRecord
	CapturedTip    uint64
	CoveredThrough uint64
	NextCursor     sessionwire.Cursor
}

// ReadPublicJournal returns a bounded page of a session's public events.
//
// Only a public event's stored canonical public body and its Core metadata are
// returned. Every other record — runtime control, ownership fence, application
// prefix — is withheld entirely: it contributes nothing to the page except an
// advance of CoveredThrough, the authenticated watermark that lets a client
// close a sequence gap without learning the kind or the bytes of what filled
// it. A public body held in an object is resolved through the same verified
// object path a caller would use; a private runtime object is never fetched.
func (s *Store) ReadPublicJournal(ctx context.Context, req ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	scan, release, err := s.planJournalRead(ctx, journalCursorPublic, req.TenantID, req.SessionID, req.FromSeq, req.Cursor, req.Limit)
	if err != nil {
		return sessionwire.JournalPage{}, err
	}
	defer release()

	events, err := walkJournal(scan,
		func(env Envelope) bool { return env.Kind == EnvelopeKindPublicEvent },
		func(seq uint64, env Envelope, _ []byte) (sessionwire.JournalEvent, int, error) {
			body, err := s.resolvePublicBody(scan.ctx, req.TenantID, req.SessionID, env.Public)
			if err != nil {
				return sessionwire.JournalEvent{}, 0, err
			}
			return sessionwire.JournalEvent{
				EventID:    env.EventID,
				JournalSeq: seq,
				Body:       json.RawMessage(body),
			}, len(body), nil
		})
	if err != nil {
		return sessionwire.JournalPage{}, err
	}
	return sessionwire.JournalPage{
		Events:         events,
		CapturedTip:    scan.capturedTip,
		CoveredThrough: scan.covered,
		NextCursor:     scan.nextCursor(s, journalCursorPublic, req.TenantID, req.SessionID),
	}, nil
}

// ReadRuntimeJournal returns a bounded page of every record in a session's
// journal, public and private alike, exactly as stored. It is the privileged
// replay path; product-facing readers use ReadPublicJournal.
func (s *Store) ReadRuntimeJournal(ctx context.Context, req ReadRuntimeJournalRequest) (RuntimePage, error) {
	scan, release, err := s.planJournalRead(ctx, journalCursorRuntime, req.TenantID, req.SessionID, req.FromSeq, req.Cursor, req.Limit)
	if err != nil {
		return RuntimePage{}, err
	}
	defer release()

	records, err := walkJournal(scan,
		func(Envelope) bool { return true },
		func(seq uint64, env Envelope, frame []byte) (RuntimeRecord, int, error) {
			return RuntimeRecord{Seq: seq, Envelope: env}, len(frame), nil
		})
	if err != nil {
		return RuntimePage{}, err
	}
	return RuntimePage{
		Records:        records,
		CapturedTip:    scan.capturedTip,
		CoveredThrough: scan.covered,
		NextCursor:     scan.nextCursor(s, journalCursorRuntime, req.TenantID, req.SessionID),
	}, nil
}

// resolvePublicBody returns the canonical public body of one public event.
//
// The declared size of an object-backed body is checked against the same
// ceiling the writer enforces before the object is opened. That is a bound on
// untrusted stored metadata, not a restatement of the writer's admission rule:
// the writer's ceiling governs what may be created, this one governs what a
// forged or corrupted reference may make this reader allocate.
func (s *Store) resolvePublicBody(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	slot BodySlot,
) ([]byte, error) {
	if slot.Inline != nil {
		return bytes.Clone(slot.Inline), nil
	}
	if slot.Reference == nil {
		return nil, journalErr(JournalErrorIntegrity, "public_body", nil)
	}
	if slot.Reference.SizeBytes > MaxInlineBodyBytes {
		return nil, journalErr(JournalErrorTooLarge, "public_reference", nil)
	}
	metadata, err := slot.Reference.ObjectMetadata()
	if err != nil {
		return nil, err
	}
	reader, err := s.GetObject(ctx, GetObjectRequest{
		TenantID:     tenant,
		SessionID:    session,
		ExpectedKind: ObjectKindJournalPublic,
		Metadata:     metadata,
	})
	if err != nil {
		return nil, err
	}
	body, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return body, nil
}

// journalScan is one bounded walk over a session's journal.
type journalScan struct {
	ctx         context.Context
	store       *Store
	scope       sessionScope
	from        uint64
	capturedTip uint64
	limit       int
	maxBytes    int

	covered   uint64
	truncated bool
	nextSeq   uint64
}

// planJournalRead validates positioning, verifies the session binding, captures
// the tip, and returns a scan ready to walk.
func (s *Store) planJournalRead(
	ctx context.Context,
	kind journalCursorKind,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	fromSeq uint64,
	cursor sessionwire.Cursor,
	limit int,
) (*journalScan, func(), error) {
	if limit < 0 || limit > storage.MaxOrderedPageLimit {
		return nil, nil, journalErr(JournalErrorInvalid, "limit", nil)
	}
	if limit == 0 {
		limit = s.limits.MaxPageSize
	}
	if cursor != "" && fromSeq != 0 {
		return nil, nil, journalErr(JournalErrorInvalid, "cursor", nil)
	}
	scope, err := s.deriveSessionScope(tenant, session)
	if err != nil {
		return nil, nil, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err := s.verifySessionScope(opCtx, scope); err != nil {
		release()
		return nil, nil, err
	}
	tip, err := s.backend.Ledger.Tip(opCtx, scope.JournalName)
	if err != nil {
		release()
		return nil, nil, journalErr(JournalErrorBackend, "tip", err)
	}

	from := fromSeq
	capturedTip := tip
	if cursor != "" {
		position, err := s.decodeJournalCursor(kind, tenant, session, cursor, tip)
		if err != nil {
			release()
			return nil, nil, err
		}
		from = position.nextSeq
		// The captured tip is pinned by the first page, so a cursor walk is one
		// consistent snapshot rather than a window that grows underneath it.
		capturedTip = position.capturedTip
	}
	if from < 1 {
		from = 1
	}
	covered := from - 1
	if covered > capturedTip {
		covered = capturedTip
	}
	maxBytes := s.maxPageBytes
	if maxBytes <= 0 {
		maxBytes = DefaultJournalPageBytes
	}
	return &journalScan{
		ctx:         opCtx,
		store:       s,
		scope:       scope,
		from:        from,
		capturedTip: capturedTip,
		limit:       limit,
		maxBytes:    maxBytes,
		covered:     covered,
	}, release, nil
}

// walkJournal reads records from the scan's start through the captured tip.
//
// selects decides whether a record occupies a slot in the page; build produces
// the page item and reports its resolved byte cost. A record selects returns
// false for is WITHHELD: it contributes nothing to the page but still advances
// CoveredThrough, which is what lets the watermark cross private records
// without revealing their kind or bytes — including past the record limit, so a
// page is never cut short by private traffic it is not returning.
//
// The walk stops at the record limit, at the byte budget, or at the captured
// tip. The first selected record of a page is always admitted, so a page can
// never fail to make progress; a record that does not fit the remaining budget
// is deferred to the next page, which means its body may be resolved twice.
func walkJournal[T any](
	scan *journalScan,
	selects func(Envelope) bool,
	build func(seq uint64, env Envelope, frame []byte) (T, int, error),
) ([]T, error) {
	if scan.from > scan.capturedTip {
		return nil, nil
	}
	cursor, err := scan.store.backend.Ledger.Read(scan.ctx, scan.scope.JournalName, scan.from)
	if err != nil {
		return nil, journalErr(JournalErrorBackend, "read", err)
	}
	defer cursor.Close()

	var page []T
	used := 0
	for {
		record, err := cursor.Next(scan.ctx)
		if errors.Is(err, io.EOF) {
			return page, nil
		}
		if err != nil {
			return nil, journalErr(JournalErrorBackend, "read", err)
		}
		if record.Seq > scan.capturedTip {
			return page, nil
		}
		env, err := DecodeEnvelope(record.Payload)
		if err != nil {
			return nil, journalErr(JournalErrorIntegrity, "record", err)
		}
		if selects(env) {
			if len(page) >= scan.limit {
				scan.truncate(record.Seq)
				return page, nil
			}
			item, cost, err := build(record.Seq, env, record.Payload)
			if err != nil {
				return nil, err
			}
			if len(page) > 0 && used+cost > scan.maxBytes {
				scan.truncate(record.Seq)
				return page, nil
			}
			page = append(page, item)
			used += cost
		}
		scan.covered = record.Seq
		if record.Seq == scan.capturedTip {
			return page, nil
		}
	}
}

// truncate marks the page as ending before seq, which is where the next page
// resumes.
func (s *journalScan) truncate(seq uint64) {
	s.truncated = true
	s.nextSeq = seq
}

func (s *journalScan) nextCursor(
	store *Store,
	kind journalCursorKind,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
) sessionwire.Cursor {
	if !s.truncated {
		return ""
	}
	return store.encodeJournalCursor(kind, tenant, session, s.nextSeq, s.capturedTip)
}

// Journal cursor grammar, stated exactly once.
//
//	cursor = base64url-raw( magic[4] version[1] scope[32] next_seq[8] tip[8] )
//
// The magic distinguishes the public projection from the privileged runtime
// replay, so a runtime cursor can never be replayed into a public read. The
// scope token is a domain-separated digest of the tenant and session the cursor
// was issued for, so a token cannot be moved between sessions. The captured tip
// is validated against the live tip, so an inflated one cannot widen the
// snapshot a page walks.
type journalCursorKind string

const (
	journalCursorPublic  journalCursorKind = "LRJP"
	journalCursorRuntime journalCursorKind = "LRJR"
)

const (
	journalCursorVersion byte = 1
	journalCursorBytes        = 4 + 1 + 32 + 8 + 8
)

type journalCursorPosition struct {
	nextSeq     uint64
	capturedTip uint64
}

func (s *Store) journalCursorScope(tenant sessionwire.TenantID, session sessionwire.SessionID) [32]byte {
	return s.keys.digest(digestFrame("looprig/sessionstore/journal/cursor/v1", []byte(tenant), []byte(session)))
}

func (s *Store) encodeJournalCursor(
	kind journalCursorKind,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	nextSeq uint64,
	capturedTip uint64,
) sessionwire.Cursor {
	scope := s.journalCursorScope(tenant, session)
	token := make([]byte, 0, journalCursorBytes)
	token = append(token, kind...)
	token = append(token, journalCursorVersion)
	token = append(token, scope[:]...)
	token = binary.BigEndian.AppendUint64(token, nextSeq)
	token = binary.BigEndian.AppendUint64(token, capturedTip)
	return sessionwire.Cursor(base64.RawURLEncoding.EncodeToString(token))
}

func (s *Store) decodeJournalCursor(
	kind journalCursorKind,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	cursor sessionwire.Cursor,
	liveTip uint64,
) (journalCursorPosition, error) {
	invalid := func() (journalCursorPosition, error) {
		return journalCursorPosition{}, journalErr(JournalErrorCursor, "cursor", nil)
	}
	// Bound the decode BEFORE it allocates: DecodeString sizes its own
	// destination from the caller's string, so an unbounded cursor would make
	// this reader allocate in proportion to attacker-supplied input. For
	// RawURLEncoding DecodedLen is exact, so a successful decode after this
	// gate is necessarily journalCursorBytes long and no second length check
	// is needed.
	if base64.RawURLEncoding.DecodedLen(len(cursor)) != journalCursorBytes {
		return invalid()
	}
	token, err := base64.RawURLEncoding.DecodeString(string(cursor))
	if err != nil {
		return invalid()
	}
	// Unpadded base64 has slack in its final character: the low bits of the
	// last group are dropped, so several distinct strings decode to identical
	// bytes. Require the exact spelling this reader emits, so one position has
	// one cursor and a token cannot be perturbed while still being accepted.
	if base64.RawURLEncoding.EncodeToString(token) != string(cursor) {
		return invalid()
	}
	if string(token[:4]) != string(kind) || token[4] != journalCursorVersion {
		return invalid()
	}
	scope := s.journalCursorScope(tenant, session)
	if !bytes.Equal(token[5:37], scope[:]) {
		return invalid()
	}
	position := journalCursorPosition{
		nextSeq:     binary.BigEndian.Uint64(token[37:45]),
		capturedTip: binary.BigEndian.Uint64(token[45:53]),
	}
	// Sequences are 1-based, so a zero start is not a position this reader ever
	// issued.
	if position.nextSeq < 1 {
		return invalid()
	}
	// A cursor cannot name a snapshot wider than the stream actually is. This
	// runs before the span check so that capturedTip is known to be a real
	// record count and the increment below cannot wrap.
	if position.capturedTip > liveTip {
		return invalid()
	}
	// The start must lie inside the snapshot, at most one past its end. Written
	// as an increment rather than nextSeq-1 so a zero start does not wrap into
	// a value this check would reject on its own — that would silently mask the
	// 1-based gate above.
	if position.capturedTip+1 < position.nextSeq {
		return invalid()
	}
	return position, nil
}
