# Adding an adapter

An adapter translates one packaged client surface to protocol v101. It is not a
runner and not a verifier.

## Checklist

- Implement `hello` and return an exact capability set.
- Keep stdout protocol-only and diagnostics on stderr.
- Preserve command and operation IDs exactly.
- Report admission separately from terminal completion.
- Preserve the selected single-record producer method. Advertise
  `producer_waiting_send` only when `method = "send"` reaches the client's
  bounded waiting API instead of an immediate-admission retry loop.
- Preserve that method on cancellation commands and cancel its actual retained
  observer rather than constructing or substituting the other producer path.
- Preserve delivery certainty rather than collapsing failures.
- Advertise independent handles only when each selected producer or directly
  assigned consumer starts a private execution and lifecycle owner from the
  originating client's exact configuration. Preserve the original shared path
  for commands that select `shared`.
- Preserve a caller-selected record timestamp in the public producer receipt
  and public consumer record whenever those surfaces expose it.
- Advertise `producer_receipt_metadata` only when a selected ordinary send
  freshly resolves its topic through the producer's public client, applies the
  nonzero UUID before admission, and returns the public topic, UUID, optional
  leader epoch, and nullable serialized key/value sizes. Do not receive the
  expected UUID from the scenario.
- For `java_keyed` sends, omit the scenario record's expected partition from
  the public producer call and return the public receipt's selected partition.
- Advertise concurrent actors only when public producer calls and directly
  assigned receives can run behind an explicit start barrier, retain exact
  identities, and rejoin without exposing private client state.
- Preserve exact read-only admin results without receiving scenario
  expectations through the adapter command.
- Preserve the selected generic-topic or dedicated client-metrics resource
  listing surface, its throttle, and canonical type-tagged identities without
  receiving the scenario-required resource names.
- Preserve validation-only finalized-feature update order, throttle, and every
  per-feature outcome; never substitute the independently compared CLI state.
- Preserve canonical unfiltered transaction listings, caller-ordered exact
  transaction descriptions, and caller-ordered producer-fencing identities
  without receiving expected states or fields.
- Advertise `transaction_batch_send` only when `method = "send_batch"` stages
  the exact homogeneous caller-ordered set through one public transaction batch
  request and expands its single acknowledgment into exact per-record offsets.
- Advertise `transaction_topic_uuid_validation` only when the adapter resolves
  every distinct record topic through public Admin, binds each record to that
  nonzero UUID, and completes a fresh validation of the current transaction
  revision before commit. Preserve the ordered validated IDs in completion.
- Submit broker unregistration only once, preserve the exact broker ID and
  throttle, and never receive the scenario-owned remaining or restored cluster
  expectations.
- For `admin_partition_abort`, derive the exact singleton producer and
  coordinator identity from public `DescribeProducers`, retain public state on
  both sides of the Admin mutation, and report completion before transaction
  cleanup can substitute for that mutation.
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
- Preserve Share-group state, epochs, assignor, ordered members, rack identities,
  subscriptions, topic IDs, and partition assignments without replacing them
  with CLI state.
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
- Advertise assigned-consumer immediate batches only when a selected
  `try_take_batch` receive repeatedly calls that exact public method and never
  substitutes the waiting `recv` observer.
- Advertise assigned-consumer events only when selected `next_event` and
  `try_take_event` observations call those exact public methods and retain the
  complete public fence and failure category without receiving expectations.
- Advertise assigned-consumer configuration only when read isolation, every
  supplied Fetch value, and retained-delivery capacities are fixed through the
  public builder before the client host starts; never receive the expected
  record used to prove that selection.
- Advertise assigned-consumer record transfer only when a borrowed batch is
  consumed through the public owned-batch and owned-record APIs, the resulting
  producer record settles through a public producer, and the retained source
  record remains readable after that terminal. Append the reserved transfer
  operation header without replacing any source header.
- Advertise group-consumer controls only when pause, resume, and seek use public
  hosted-consumer calls and retain exact operation, consumer, partition, and
  position identity without receiving later record expectations.
- Advertise group-consumer immediate batches only when a selected
  `try_take_batch` receive repeatedly calls that exact hosted-consumer method
  and never substitutes the waiting `recv` observer.
- Advertise group-consumer acknowledgement only when a selected nonzero delay
  calls the public processing-acknowledgement method with the retained batch's
  assignment-fenced checkpoint before committing that same batch.
- Advertise partial group checkpoints only when the declared processed count
  marks that exact ordered batch prefix through the public checkpoint builder;
  never pass harness-only expected operation identities to the adapter.
- Pass every group-create topic to the public subscription builder in caller
  order. Do not select only the first topic or infer topics from expected
  records.
- Pass every Share-create topic to the public subscription builder in caller
  order. Do not select only the first topic or infer topics from expected
  acquisitions. Pass an optional rack through the public builder and require
  the resulting public handle to retain it.
- Advertise group-consumer configuration only when missing-offset reset, read
  isolation, every supplied Fetch and retained-delivery value, every shared
  runtime deadline, an optional classic assignor, and every supplied classic
  timing are fixed through public builder calls before membership starts.
  Preserve all three reset policies: `error` must fail closed with the
  correlated public `state` error, while `earliest` and `latest` select their
  exact public positions.
  Reject all classic-only fields for KIP-848 and never receive the record or
  description expected to prove those selections.
- Advertise group-consumer shutdown only when clone-shared public requests are
  idempotent and public event observation can distinguish terminal stream
  closure; never report that closure as broker-visible leave truth.
- Advertise Share-consumer configuration only when every supplied long-poll,
  byte, record, acquisition-range, attempt-timeout, membership-start, and close
  value is fixed through public builder calls before membership starts and the
  public retained batch exposes its acquisition count; never receive the
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
