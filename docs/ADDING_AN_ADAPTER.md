# Adding an adapter

An adapter translates one packaged client surface to protocol v34. It is not a
runner and not a verifier.

## Checklist

- Implement `hello` and return an exact capability set.
- Keep stdout protocol-only and diagnostics on stderr.
- Preserve command and operation IDs exactly.
- Report admission separately from terminal completion.
- Preserve delivery certainty rather than collapsing failures.
- Advertise concurrent actors only when public producer calls and directly
  assigned receives can run behind an explicit start barrier, retain exact
  identities, and rejoin without exposing private client state.
- Preserve exact read-only admin results without receiving scenario
  expectations through the adapter command.
- Preserve caller order and one exact public outcome per resource in admin batch
  completions; do not collapse a mixed-result batch into `command_failed`.
- Keep scenario-only expected per-resource errors out of adapter commands.
- Map singleton public admin failures to one correlated `command_failed`; do not
  receive or infer their scenario-only expected codes.
- Preserve exact selected configuration values and use incremental alteration;
  never report a sensitive or unavailable value as observed broker truth.
- Preserve complete public metrics snapshots without receiving scenario-owned
  thresholds or treating client counters as independent broker truth.
- Advertise assigned-consumer controls only when replacement, incremental
  add/remove, seek, pause, and resume use public calls with explicit positions,
  bounded admission, and exact operation-identified completions.
- Advertise group-consumer controls only when pause, resume, and seek use public
  hosted-consumer calls and retain exact operation, consumer, partition, and
  position identity without receiving later record expectations.
- Advertise group-consumer configuration only when missing-offset reset and
  read isolation are fixed through public builder calls before membership
  starts; never receive the record expected to prove those selections.
- Advertise group-consumer shutdown only when clone-shared public requests are
  idempotent and public event observation can distinguish terminal stream
  closure; never report that closure as broker-visible leave truth.
- Advertise Share-consumer configuration only when record and acquisition-range
  limits are fixed through public builder calls before membership starts and
  the public retained batch exposes its acquisition count; never receive the
  expected records or acquisition count in the adapter command.
- Forward the exact validate-only flag for supported admin builders and emit the
  distinct validation completion; never report a mutation completion for a
  request that only validated.
- Use stable normalized codes and bounded diagnostics.
- Emit `command_failed` and exit successfully for a normal public API failure.
- Settle close and shutdown explicitly.
- Exit nonzero after an unrecoverable `fatal` event.
- Use no private test-only client APIs.
- Test an installed or packaged artifact.

## Rust

Call the curated public kafkars API from a resolved subject checkout. Do not
reach into engine internals.

## C

Use a real C or C++ fixture linked against the packaged ABI. Exercise copy-in,
polling, retained events, release order, and shutdown as foreign callers do.

## Java

Load the packaged Java and native artifacts from a clean fixture project. Class
loading, native resolution, dispatch, and shutdown hooks are product behavior.
