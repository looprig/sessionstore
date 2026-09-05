package sessionstore

import (
	"context"
	"errors"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/storage"
)

var dueDispositionCursor = sweepCursorKind{
	magic: "LRDD", version: 1, domain: "looprig/sessionstore/duedisposition/cursor/v1",
	cursorErr:  func() error { return inboxErr(InboxErrorCursor, "cursor", nil) },
	invalidErr: func(field string) error { return inboxInvalid(field, nil) },
	backendErr: func(field string) error { return inboxErr(InboxErrorBackend, field, nil) },
}

// ListDueDispositionCommandsRequest pages one disposition-only control shard.
// Supply DueAtOrBefore on the first request or Cursor on continuations, never
// both. The cursor retains the original bound and cannot cross protocol kinds.
type ListDueDispositionCommandsRequest struct {
	Shard         int
	DueAtOrBefore time.Time
	Limit         int
	Cursor        sessionwire.Cursor
}

// DispositionDueCommandPage is a weak deadline-ordered view, not an acceptance
// stream or evidence of nonexistence. Examined counts provider rows, including
// Unreadable rows skipped for malformed/misfiled descriptors or missing, deleted,
// corrupt, legacy-mode or mismatched catalogs. Such rows advance continuation.
// Provider unavailability, cancellation and scope-check failures fail the page.
// No command is returned without its actual immutable catalog binding checked.
type DispositionDueCommandPage struct {
	Commands   []DispositionInboxEntry
	Examined   int
	Unreadable int
	Limit      int
	NextCursor sessionwire.Cursor
}

// ListDueDispositionCommands issues one bounded index page and at most one
// exact catalog read (plus its scope witnesses) per examined row. It performs
// no global scan, blob reads or mutations. Only the provider's continuation
// advances the view; an empty Commands slice does not imply an exhausted page.
func (s *Store) ListDueDispositionCommands(ctx context.Context, req ListDueDispositionCommandsRequest) (DispositionDueCommandPage, error) {
	shard, err := s.validateShard(req.Shard, inboxInvalid)
	if err != nil {
		return DispositionDueCommandPage{}, err
	}
	limit, ok := s.pageLimit(req.Limit)
	if !ok {
		return DispositionDueCommandPage{}, inboxInvalid("limit", nil)
	}
	bound, after, err := dueDispositionCursor.position(s, shard, req.Cursor, req.DueAtOrBefore)
	if err != nil {
		return DispositionDueCommandPage{}, err
	}
	opCtx, release, err := s.admitForeground(ctx)
	if err != nil {
		return DispositionDueCommandPage{}, err
	}
	defer release()
	provider, err := s.backend.OrderedIndex.ListDue(opCtx, shardNamespace(dispositionInboxNamespace, shard), bound, after, limit)
	if err != nil {
		return DispositionDueCommandPage{}, classifyInboxOrderedError(err, "due_commands")
	}
	if len(provider.Records) > limit {
		return DispositionDueCommandPage{}, inboxErr(InboxErrorBackend, "limit", nil)
	}
	page := DispositionDueCommandPage{Commands: make([]DispositionInboxEntry, 0, len(provider.Records)), Examined: len(provider.Records), Limit: limit}
	for _, stored := range provider.Records {
		if err := opCtx.Err(); err != nil {
			return DispositionDueCommandPage{}, err
		}
		entry, readable, err := s.dueDispositionFor(opCtx, stored, shard, bound)
		if err != nil {
			return DispositionDueCommandPage{}, err
		}
		if !readable {
			page.Unreadable++
			continue
		}
		page.Commands = append(page.Commands, entry)
	}
	if provider.NextCursor != "" {
		page.NextCursor, err = dueDispositionCursor.encode(s, shard, bound, provider.NextCursor)
		if err != nil {
			return DispositionDueCommandPage{}, err
		}
	}
	return page, nil
}

func (s *Store) dueDispositionFor(ctx context.Context, stored storage.OrderedRecord, shard uint32, bound int64) (DispositionInboxEntry, bool, error) {
	r, err := decodeDispositionInboxRecord(stored.Value)
	if err != nil {
		return DispositionInboxEntry{}, false, nil
	}
	d := r.Descriptor
	scope, err := s.deriveSessionScope(d.TenantID, d.SessionID)
	if err != nil || scope.ControlShard != shard || r.ApplyDeadline.UnixMilli() > bound {
		return DispositionInboxEntry{}, false, nil
	}
	binding, err := s.dispositionCatalog(ctx, scope, d.TenantID, d.SessionID)
	if err != nil {
		// Only definite catalog-record refusals are skippable. Infrastructure
		// and witness failures must not masquerade as an empty successful page.
		var catalog *CatalogError
		if errors.As(err, &catalog) {
			switch catalog.Code {
			case CatalogErrorNotFound, CatalogErrorDeleted, CatalogErrorInvalid, CatalogErrorIdentity, CatalogErrorMalformed, CatalogErrorVersion, CatalogErrorTooLarge:
				return DispositionInboxEntry{}, false, nil
			}
		}
		return DispositionInboxEntry{}, false, err
	}
	entry, err := dispositionInboxEntryFor(stored, scope, d.TenantID, d.SessionID, d.CommandID, binding)
	return entry, err == nil, nil
}
