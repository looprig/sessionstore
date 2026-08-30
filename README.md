# sessionstore

`sessionstore` is Looprig's transport-neutral durable session aggregate. It composes
the primitives from [`github.com/looprig/storage`](https://github.com/looprig/storage)
with canonical records from
[`github.com/looprig/core/sessionwire/v1`](https://github.com/looprig/core) so durable
session state can be shared without coupling Factory, Host, or Harness to one another.

The module will own journal fencing and replay, catalog and gate projections, durable
command admission, host and placement records, fenced pointers, reconciliation
claims, and session-scoped object references. Concrete storage providers are selected
by the product composition root.

Production imports are intentionally limited to the Go standard library, Core, and
Storage. Published module files use exact released versions and contain no local
`replace` directives or vendor tree.

## Provider compatibility

`Open` requires the backend's `Blobs` primitive to implement Storage's optional
`BlobReaderLifecycle` capability, be a concrete non-nil implementation, and
advertise a positive close bound. This lets Store shutdown stop an outstanding
object read before an explicitly owned provider is closed. The reader close bound
and `WithShutdownTimeout` cover separate shutdown phases and are not compared.

Storage v0.6.0's memory backend and natsstore v0.5.1 satisfy the requirement.
fsstore v0.5.1 intentionally does not claim bounded reader shutdown and is
rejected with `*InvalidBackendError` naming `BlobReaderLifecycle`. Any other
provider is compatible only after it implements the capability and its
provider-specific blocked-I/O proof. Capability rejection happens before layout
marker or other provider I/O; a provider passed to a failed `Open` remains
caller-owned.

## Layout compatibility

An unmarked backend is atomically initialized as the tenant-scoped `tenant-v1`
layout. The historical `sessions/<uuid>` layout is available only through
`WithLegacySingleTenant`, which persists and enforces the exact configured tenant.
SessionStore never probes for old data, auto-migrates, or dual-writes layouts.

Migration must be performed offline with SessionStore stopped, into a new backend
already initialized for `tenant-v1`. Validate the migrated data before switching
the composition root; do not rewrite a live backend's immutable layout marker.

Provider ownership options take effect only after `Open` has successfully validated
and bound the backend layout. If `Open` fails, the provider remains caller-owned and
SessionStore does not close it.

## Object orphans

`PutObject` mints an object's identity before writing it and returns a reference
only after re-reading the persisted bytes and verifying them against the declared
length and digest. A provider failure after the blob has committed therefore
returns an error and no reference while leaving a verified blob behind — an
orphan.

That is deliberate. Deleting on a post-commit failure would issue a delete
against a provider that has just proved unreliable, and every object key is
content- and generation-addressed, so an orphan can never be confused with, or
served as, another object. Reclaiming orphans is the store operator's
responsibility, over the tenant- and session-scoped blob prefix; SessionStore's
only enumeration path is internal and unexported, so no caller-facing garbage
collector exists yet.

## Journal ownership: no rebasing after a fence conflict

`OpenJournal` acquires the session lease, reads the ledger tip exactly once, and
appends an opening fence at precisely that tip stamped with the grant's epoch. If
that CAS conflicts, the grant is spent: the lease is released and a typed
`*JournalError` with code `fenced` is returned. The caller may acquire a fresh,
strictly higher epoch and reopen.

It deliberately does not refresh the tip and retry. A retry loop lets a writer
reorder itself behind records it never observed, under an epoch that a
predecessor may still believe it holds; failing the grant instead makes epoch
order and ledger order agree.

After a successful open the writer tracks only its own committed sequence and
CASes every later append on it. It never re-reads the tip, so a successor's
opening fence permanently fails it. An append whose outcome could not be
resolved — an unresolved ambiguous ack, or a contested record that could not be
read back — is equally terminal (code `unknown`): the writer latches the failure
rather than rebasing onto whatever is now durable. A *definite* backend failure
is not terminal, because it left the tracked tip untouched and the same record
can simply be offered again.

An over-threshold body is uploaded and verified as an immutable object before its
reference is appended. If the append then fails, the verified object is left
behind as an orphan for the same reason `PutObject` leaves one.

## Public journal reads

`ReadPublicJournal` returns only a public event's stored canonical public body
and its Core metadata. Runtime control records, ownership fences, and application
prefixes are withheld entirely: they contribute nothing to a page except an
advance of `covered_through`, the authenticated watermark that lets a client
close a sequence gap without learning the kind or bytes of what filled it. A
public body held in an object is resolved through the ordinary verified object
path; a private runtime object is never fetched by a public read.

Page cursors are opaque and bound to the projection they were issued for, to the
exact tenant and session, and to the tip captured by the first page — a runtime
cursor cannot be replayed into a public read, a cursor cannot be moved between
sessions, and an inflated captured tip cannot widen the snapshot a walk covers.
`ReadRuntimeJournal` is the privileged counterpart and returns every record as
stored, leaving object-backed bodies unresolved for the caller to fetch.
