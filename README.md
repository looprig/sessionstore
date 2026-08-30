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
