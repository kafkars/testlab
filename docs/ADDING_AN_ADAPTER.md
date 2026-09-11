# Adding an adapter

An adapter translates one packaged client surface to protocol v70. It is not a
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
- Preserve canonical unfiltered transaction listings, caller-ordered exact
  transaction descriptions, and caller-ordered producer-fencing identities
  without receiving expected states or fields.
- Preserve selected-partition log-directory broker order, canonical paths,
  optional capacity metadata, and exact replica sizes, lags, and future markers
  without receiving the expected replica count.
- Preserve selected-replica caller order and every current, absent, or future
  path and signed lag without receiving the expected current-placement count.
- Preserve replica log-directory alteration caller order, exact targets,
  throttle, and per-replica failures; never replace the public completion with
  later CLI placement state.
- Preserve canonical metadata-quorum leader, watermark, voter, observer,
  directory, timestamp, and optional controller-listener facts without
  substituting later CLI state for the public result.
- Preserve caller order, exact topic keys, topic IDs, internal markers, ordered
  partitions, and per-topic or per-partition errors for plural topic
  descriptions.
- Preserve caller order, exact topic keys, and every success or per-topic error
  for plural topic deletion without receiving expected outcomes or replacing
  the public result with later metadata absence.
- Preserve Share-group state, epochs, assignor, ordered members, subscriptions,
  topic IDs, and partition assignments without replacing them with CLI state.
- Preserve caller order, exact outer and inner group identities, full detailed
  descriptions, and per-group errors for plural Share-group descriptions.
- Preserve caller order across one mixed classic and KIP-848 consumer-group
  description call, including group state, protocol-specific epochs and
  assignor data, every member identity, subscription, typed assignment, raw
  classic payload, and per-group error without receiving expected values.
- Preserve singleton and caller-ordered plural selected Share-group start
  offset, leader epoch, lag, topic identity, group errors, and partition-scoped
  errors without receiving scenario expectations.
- Preserve Share-group offset-alteration partition identity, nonzero topic ID,
  and partition-scoped errors; resulting lag remains verifier-owned.
- Preserve Share-group topic-offset deletion identity, nonzero topic ID, and
  topic-scoped errors; the independently checked partition remains verifier-owned.
- Preserve caller order, exact group identity, and per-group errors for plural
  Share-group deletion; never replace public outcomes with CLI listing state.
- Preserve caller order, exact group identity, and per-group errors for plural
  consumer-group deletion; never replace public outcomes with later group
  absence.
- Preserve caller order, explicit and high-watermark record-deletion selectors,
  exact topic-partition identities, successful low watermarks, and per-target
  errors without receiving scenario-owned baseline watermarks.
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
- Advertise assigned-consumer configuration only when read isolation is fixed
  through the public builder before the client host starts; never receive the
  expected record used to prove that selection.
- Advertise group-consumer controls only when pause, resume, and seek use public
  hosted-consumer calls and retain exact operation, consumer, partition, and
  position identity without receiving later record expectations.
- Advertise group-consumer configuration only when missing-offset reset, read
  isolation, and an optional classic assignor are fixed through public builder
  calls before membership starts; reject a classic assignor for KIP-848 and
  never receive the record or description expected to prove those selections.
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
