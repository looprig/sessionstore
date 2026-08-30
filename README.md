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
