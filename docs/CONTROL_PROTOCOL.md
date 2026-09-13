# Adapter control protocol

## Transport

Protocol v148 is UTF-8 JSON Lines over stdin and stdout.
This cut pairs it with scenario schema v152 and evidence schema v138.

- One line is one complete JSON object.
- Adapter stdout is protocol-only; diagnostics use stderr.
- A line larger than 4 MiB is invalid before unbounded allocation.
- Every event repeats the protocol version and originating command ID.

Lifecycle verdicts join each creation, readiness, assignment, flush, close,
shutdown, and finish event to that exact originating command. Repeating a
public operation on one resource therefore requires one completion per command;
resource-level event totals are not a substitute for correlation.
The scenario's readiness, producer flush and close, client shutdown, assigned
and group consumer close, group abandonment, and transactional-producer close
requests must also equal the complete issued command stream in scenario order.
This makes missing, substituted, reordered, or duplicate lifecycle requests a
contract failure even when every command that remains has a terminal event.

Client creation may require one exact broker-issued cluster ID when the adapter
advertises `expected_cluster_identity`. The adapter must configure that value
through the public builder before startup and retain it in the returned public
client. Construction and each later readiness probe must fail with `identity`
when Kafka reports another ID. The expected failure code remains scenario-only;
a rejected construction must not retain a client handle, and the qualification
reuses that same client identity immediately with the correct cluster ID.
Every baseline client creation command appears exactly once in scenario order,
retaining its client identity and optional expected cluster-ID guard. This
prevents an unguarded successful construction from replacing the intended
reject-then-success sequence.

Producer and directly assigned consumer creation carry an exact child ownership
selection. `shared` uses the originating client's execution and lifecycle
owner. `independent` starts a private owner from that client's exact
configuration and requires the `independent_handles` capability. Scenario files
may omit the field only to select the backward-compatible `shared` default;
adapter commands never default it across the process boundary. Parent metrics
or shutdown do not establish private-owner behavior: sibling, replacement, and
dual-cursor scenarios require later public operations on the independent
handles. Every ordinary producer and assigned-consumer creation must have one
exact command preserving its client, child identity, and ownership selection;
another child kind or a duplicate command cannot satisfy that registration.

Configured client creation carries the complete client-wide producer policy:
delivery timeout, one of the five public compression selections, bounded retry
count and backoff, and portable active, waiting, batching, request, in-flight,
and linger limits. It also carries the exact public configuration method:
`producer_config` applies the aggregate `ProducerConfig`, while
`individual_setters` applies the equivalent delivery-timeout, compression,
retry, and limits setters. Release scenarios select both paths explicitly;
adapter commands never default the method across the process boundary. These
values are fixed before the public client host starts.
Durability cannot be downgraded: idempotence and `acks=all` remain client-owned
invariants outside the protocol vocabulary. Each codec has an independent
real-Kafka scenario checked by ordinary terminal and broker-observation rules.

Configured assigned-consumer client creation carries immutable read isolation,
broker Fetch policy, and bounded Fetch-call and retained-delivery capacities.
Each supplied value is fixed through the public builder before the client host
starts. Its issued command must match that complete policy exactly; a same-ID
plain or producer-configured client is not equivalent. Fetch byte fields use
Kafka's signed 32-bit domain, and the hard decoded batch ceiling must cover the
per-partition Fetch ceiling without exceeding the total retained-byte capacity.
The adapter receives no expected record;
read-committed behavior and the configured Fetch path are proved by a direct
assignment from the beginning returning only a nontransactional sentinel after
an independently verified aborted transaction.

A directly assigned `receive` names its exact public retained-batch observer.
`recv` is the default waiting path; `try_take_batch` selects repeated immediate
observation and requires the `assigned_consumer_immediate_batch` capability.
The command retains that selection, consumer identity, receive identity, and
bound while the expected producer operation remains scenario-only.
When the scenario selects `observe_fetch_evidence`, the adapter must advertise
`assigned_consumer_fetch_evidence` and append the public batch's topic, nonzero
UUID, partition, requested and next offsets, log bounds, high watermark,
retained-byte charge, and independent checkpoint offset to `receive_completed`.
Testctl does not send expected values to the adapter; the verifier joins them to
prior independent topic-ID and watermark snapshots and the broker record.

`transfer_assigned_record` waits for one public direct-consumer batch and
selects either `into_owned_batch` (`RecordBatch::into_owned`, then
`OwnedConsumerBatch::into_records`) or the direct `into_owned_records`
conversion. It transfers the only resulting record to a caller-selected topic
and partition through an ordinary producer. It requires the
`assigned_consumer_record_transfer` capability.
The expected source operation remains scenario-only. The adapter appends one
reserved `testlab-transfer-operation-id` header so independent observation can
distinguish the destination without removing the preserved source correlation
or any application header. It emits ordinary admission and terminal events,
then `assigned_record_transfer_completed` after re-reading the retained source.
The command timeout bounds the source-batch wait; producer configuration and
the outer scenario deadline independently bound destination settlement.

`observe_assigned_consumer_event` similarly names one exact public retained-event
observer. `next_event` is the default waiting path; `try_take_event` selects
repeated immediate observation. Both require `assigned_consumer_events`. The
command retains only the operation, consumer, method, and Testlab-owned bound;
the expected target and failure remain scenario-only. Its completion preserves
the public topic-partition fence, positive assignment and position generations,
optional Fetch revision, terminal category, and exact broker code.

Every direct `receive` command is required exactly once in scenario order. It
retains the consumer, receive identity, complete observation timeout, and exact
public retained-batch observer. `recv` is the default waiting path;
`try_take_batch` repeatedly selects immediate observation within the same bound.
The expected producer operation remains scenario-only.

Every ordinary producer command is required exactly once in scenario order. A
single `send` retains its producer and operation identities, complete record,
partitioning and UUID-validation choices, and exact public producer method.
`try_send` is the default immediate-admission path; `send` selects bounded FIFO
waiting admission and requires the `producer_waiting_send` capability. A
`send_batch` retains one producer and its complete caller-ordered operation and
record set, so individual commands cannot substitute for the public batch call.
A cancellation command retains the same method selection and invokes
`Delivery::cancel` for `try_send` or `Send::cancel` for `send`.

An ordinary send may reference one prior singleton `topic_id` description.
The expected ID remains scenario-only; the command sets `validate_topic_uuid`
and requires `producer_receipt_metadata`. The adapter freshly resolves that
topic through the producer's originating public client and applies its nonzero
ID through `Record::expected_topic_uuid` before admission.

An ordinary `send` carries an explicit partition by default. A `java_keyed`
selection instead carries the logical topic partition count and a keyed record;
the adapter must omit the record's scenario-only expected partition from the
public producer call. The expected partition remains available to provisioning,
independent observation, and verification and must equal Kafka's positive
Murmur2 result over the serialized key. Successful operation terminals report
the partition returned by the public producer receipt; failed or uncertain
terminals report no partition.

Client metrics observation carries only stable client and operation identities
to the adapter, exactly once in scenario order. Scenario-only record floors and
required idle, accepting, or healthy states remain in testctl and never cross
the adapter boundary. The completion preserves every public calls, failures,
mailbox, latency, and producer snapshot getter. Immediate metrics backpressure
is retried within a bounded admission window; one accepted observer is waited
exactly once.

`list_transactions` carries caller-ordered state and signed producer-ID
filters, an optional nonnegative duration in milliseconds, and an optional
Kafka-owned transactional-ID regular expression. Expected rows and the earlier
unfiltered baseline operation remain scenario-only. After every public result,
Testlab deliberately runs an unfiltered pinned CLI query so filtered evidence
proves both exact narrowing and unchanged complete fixture state. Duration
selection is qualified on Kafka 3.8 and later; the regular-expression path is
kept on Kafka 4.3 cells that negotiate ListTransactions v2.

## Handshake

Testctl sends `hello` with run ID, scenario ID, ordered environment endpoints, and a
non-secret security policy. TLS and SASL secrets are referenced by environment
variable name and passed only in the adapter process environment. The adapter
replies `ready` with implementation identity, version, and exact capabilities.

## Commands

- `hello`
- `create_client`
- `create_configured_client`
- `create_assigned_consumer_client`
- `await_client_ready`
- `observe_client_metrics`
- `create_producer`
- `send`
- `cancel_producer_send`
- `send_batch`
- `create_assigned_consumer`
- `assign_beginning`
- `assign_beginning_batch`
- `control_assigned_consumer`
- `observe_assigned_consumer_event`
- `receive`
- `transfer_assigned_record`
- `start_concurrent_actors`
- `join_concurrent_actors`
- `close_assigned_consumer`
- `create_group_consumer`
- `group_receive`
- `observe_group_assignments`
- `group_receive_set`
- `control_group_consumer`
- `abandon_group_consumer`
- `shutdown_group_consumer`
- `close_group_consumer`
- `create_share_consumer`
- `share_receive`
- `share_acknowledge`
- `drop_share_batch`
- `close_share_consumer`
- `create_topic`
- `create_topics_batch`
- `create_partitions`
- `delete_topic`
- `delete_topics`
- `describe_topic`
- `describe_topics`
- `list_topics`
- `list_config_resources`
- `list_offsets`
- `list_offsets_batch`
- `delete_records`
- `delete_records_batch`
- `describe_topic_config`
- `describe_topic_configs`
- `alter_topic_configs`
- `alter_topic_config`
- `describe_cluster`
- `unregister_broker`
- `describe_features`
- `validate_feature_updates`
- `exercise_delegation_token_lifecycle`
- `exercise_streams_group_admin_lifecycle`
- `describe_producers`
- `describe_log_dirs`
- `describe_replica_log_dirs`
- `alter_replica_log_dirs`
- `describe_metadata_quorum`
- `list_transactions`
- `describe_transactions`
- `fence_producers`
- `alter_partition_reassignments`
- `list_partition_reassignments`
- `list_consumer_groups`
- `describe_consumer_group`
- `describe_consumer_groups`
- `describe_share_group`
- `describe_share_groups`
- `list_share_group_offsets`
- `list_share_groups_offsets`
- `alter_share_group_offsets`
- `delete_share_group_offsets`
- `delete_share_groups`
- `list_consumer_group_offsets`
- `list_consumer_group_offsets_batch`
- `list_consumer_groups_offsets`
- `alter_consumer_group_offset`
- `alter_consumer_group_offsets`
- `delete_consumer_group_offset`
- `delete_consumer_group_offsets`
- `delete_consumer_group`
- `delete_consumer_groups`
- `remove_consumer_group_members`
- `describe_classic_groups`
- `create_acls`
- `describe_acls`
- `delete_acls`
- `alter_client_quota`
- `describe_client_quota`
- `alter_user_scram_credential`
- `describe_user_scram_credential`
- `create_transactional_producer`
- `execute_transaction`
- `execute_transactional_transform`
- `fence_transaction`
- `close_transactional_producer`
- `flush`
- `close_producer`
- `shutdown_client`
- `finish`
- `abort`

Model-broker controls, protocol-adversary controls, real-cluster broker
restarts, targeted broker-role disruptions, and typed broker ACL or quota
changes are owned directly by testctl and do not cross the adapter boundary. A
restart, role disruption, policy transition, or adversary arm occurs between
public client commands while the same adapter process remains alive.

## Network proxy control

Scenario schema v26 adds typed `alter_network_fault` and
`cut_network_connections` environment actions. The external proxy uses its own
protocol v1 JSON Lines control channel. Each control carries a stable
environment operation ID, a one-based broker route, and a bounded timeout.
Persistent faults are exact apply/remove pairs: a bidirectional blackhole or a
per-chunk delay in the client-to-broker or broker-to-client direction.

The worker binds the adapter-facing loopback routes acknowledged by its `ready`
event and forwards opaque TCP bytes to hidden broker-facing routes. It does not
parse Kafka frames. `fault_applied`, `fault_removed`, and `connections_cut`
events must match the requested identity and protocol version exactly. Removal
and cut acknowledgements carry independently measured effect observations.
Malformed, unexpected, or mismatched events poison the supervised process
terminal and invalidate execution.

This cut enables the proxy only for plaintext unauthenticated environments.
Broker-visible truth is captured through separately advertised observer
listeners, so neither the adapter nor the fault route supplies observation
truth.

## Broker policy control

Scenario schema v25 adds one typed `alter_broker_policy` environment action for
literal deny ACLs and user producer or consumer byte-rate quotas. Each policy
is applied and later removed as an exact pair. Testctl runs the pinned Kafka CLI
inside the Compose broker through an internal superuser listener, retains the
raw alter terminal, performs a separate exact query, and emits a normalized
policy observation only when that query confirms the requested state.

The packaged adapter authenticates as `User:kafkars`; it never receives the
environment control or its expected error. ACL scenarios require the exact
public denial while the independently confirmed policy is active, followed by
public success after independently confirmed removal. Quota scenarios require
completed public producer or consumer progress inside a minimum observed
active interval. Adapter strings cannot establish policy state or throttling.

## Protocol adversary control

The private adversary worker uses protocol v1 JSON Lines on separate child
stdin and stdout pipes. A control has one stable environment operation ID, one
Kafka API, an application count from 1 through 32, and one exact fault: partial
frame, wrong correlation ID, stale response, bounded stall, or selected
disconnect point. Testctl records the validated control only after receiving an
exact `armed` acknowledgement.

Worker events are `ready`, `armed`, `observation`, and `fatal`. Each observation
records monotonic connection and request identities, API key and version,
correlation ID, complete request bytes, actual response bytes written, optional
control identity, and the exact selected outcome. Baseline support is limited
to the APIs needed by the minimized metadata and producer scenarios. Stale
responses replay a retained complete response from a different API so they
cannot accidentally become a current same-shape response.

The worker is an external environment process, not an adapter library or client
hook. Its stdout is protocol-only and bounded. Fatal output, malformed JSON,
missing acknowledgements, unexercised controls, nonzero exit, and supervision
timeouts invalidate evidence.

## Events

- `ready`
- `client_created`
- `client_ready`
- `client_metrics_observed`
- `producer_created`
- `operation_accepted`
- `operation_rejected`
- `operation_terminal`
- `producer_cancellation_completed`
- `batch_completed`
- `assigned_consumer_created`
- `assignment_completed`
- `assigned_consumer_control_completed`
- `assigned_consumer_event_observed`
- `receive_completed`
- `concurrent_actors_started`
- `concurrent_actor_completed`
- `concurrent_actors_completed`
- `assigned_consumer_closed`
- `group_consumer_created`
- `group_receive_completed`
- `group_assignments_observed`
- `group_receive_set_completed`
- `group_consumer_control_completed`
- `group_consumer_abandoned`
- `group_consumer_shutdown_completed`
- `group_consumer_closed`
- `share_consumer_created`
- `share_receive_completed`
- `share_acknowledgement_completed`
- `share_batch_dropped`
- `share_consumer_closed`
- `topic_created`
- `topics_creation_completed`
- `topic_partitions_created`
- `topic_deleted`
- `topics_deleted`
- `topic_described`
- `topics_described`
- `topics_listed`
- `config_resources_listed`
- `offset_listed`
- `offsets_listed`
- `records_deleted`
- `records_batch_deleted`
- `topic_config_described`
- `topic_configs_described`
- `topic_configs_altered`
- `topic_config_altered`
- `cluster_described`
- `broker_unregistered`
- `features_described`
- `feature_updates_validated`
- `delegation_token_lifecycle_exercised`
- `streams_group_admin_lifecycle_exercised`
- `producers_described`
- `transactions_listed`
- `transactions_described`
- `producers_fenced`
- `consumer_groups_listed`
- `consumer_group_described`
- `consumer_groups_described`
- `share_group_described`
- `share_groups_described`
- `share_group_offsets_listed`
- `share_groups_offsets_listed`
- `share_group_offsets_altered`
- `share_group_offsets_deleted`
- `share_groups_deleted`
- `consumer_group_offset_listed`
- `consumer_group_offsets_listed`
- `consumer_groups_offsets_listed`
- `consumer_group_offset_altered`
- `consumer_group_offsets_altered`
- `consumer_group_offset_deleted`
- `consumer_group_offsets_deleted`
- `consumer_group_deleted`
- `consumer_groups_deleted`
- `consumer_group_members_removed`
- `classic_groups_described`
- `acls_created`
- `acls_described`
- `acls_deleted`
- `client_quota_altered`
- `client_quota_alteration_validated`
- `client_quota_described`
- `user_scram_credential_altered`
- `user_scram_credential_described`
- `transactional_producer_created`
- `transaction_completed`
- `transactional_transform_completed`
- `assigned_record_transfer_completed`
- `transaction_fence_completed`
- `transactional_producer_closed`
- `flush_completed`
- `producer_closed`
- `client_shutdown`
- `command_failed`
- `finished`
- `aborted`
- `fatal`

A send emits one admission decision. Accepted operations later emit exactly one
terminal event. Rejected operations emit no terminal. A batch emits one
admission outcome per input operation, one terminal per accepted operation, and
then `batch_completed`. A batch contains at most 31 records so the complete
command remains within the bounded event budget.

Every record may carry one caller-selected `timestamp_millis`. An adapter maps
that value through its public record builder without replacing it. A successful
`operation_terminal` retains the timestamp exposed by the public producer
receipt, and each public consumer record retains its exposed timestamp. The
independent broker observer records Kafka's timestamp separately from those
adapter claims.

A successful ordinary or batch producer terminal may additionally retain a
typed receipt with the public topic, optional topic UUID and leader epoch, and
nullable serialized key and value sizes. `None` distinguishes a null key or
value from `Some(0)` for present empty bytes. UUID-selected ordinary sends
require this receipt and join its ID to the prior public and independent topic
identity before independent record coordinates and timestamp establish the
acknowledgement.

A cancellation command names whether it obtains public producer ownership
through immediate `try_send` or bounded waiting `send`, retains that method's
sole terminal observer, and invokes its public cancellation method twice with
bounded backpressure retry. It reports both immediate stage-aware outcomes in
order only after the retained observer emits its authoritative terminal.
`cancelled_not_sent` requires a definitely-not-sent `cancelled` terminal and the
next attempt must be `already_terminal`. `too_late` preserves zero-or-one broker
visibility and may only remain `too_late` or advance to `already_terminal`;
Testlab never strengthens it into a not-sent promise.

A concurrent actor group contains two through eight exact actors. Testctl sends
one `start_concurrent_actors` command carrying the stable group identity,
caller-ordered actor identities, and public producer-send or directly assigned
consumer-receive operations. The adapter validates every referenced public
handle, moves each receive consumer into exactly one worker, clones only public
producer handles, and releases all workers behind one start barrier. Its exact
`concurrent_actors_started` event establishes the start command boundary; it
does not claim that operating-system scheduling began every public call at the
same instant.

Testctl may execute typed environment controls while those public calls are in
flight, then sends one bounded `join_concurrent_actors` command for the same
group. The adapter emits each actor's normal public operation events under the
join command, followed by an exact `concurrent_actor_completed` in declared
actor order and one `concurrent_actors_completed`. A worker failure remains an
actor outcome; malformed membership, an unjoined group, or loss of a retained
consumer makes execution invalid. One public consumer cannot back multiple
actors in the same group. Scenario-only receive expectations are stripped
before translation and never enter the adapter command.

A bounded receive polls the packaged consumer's public receive future and emits
the exact records it observed. An empty completion means no public record
arrived before the declared receive deadline; the verifier treats a missing
expected record as a client failure, not manufactured success.

A bounded assigned-consumer event observation polls either the public
`next_event` future or exact `try_take_event` method until one retained failure
arrives. This outer harness bound does not become a client Fetch or event
deadline. The broker-policy scenario independently installs a topic READ deny,
requires both observers to expose `PositionResolutionFailed(Broker(29))`, then
removes that same policy before an independent consumer receives the seeded
record.

Every group receive command is required exactly once in scenario order and names
its exact public retained-batch observer and full-batch checkpoint conversion.
`recv` is the default waiting path; `try_take_batch` selects repeated immediate
observation and requires `group_consumer_immediate_batch`. `checkpoint` is the
canonical conversion; `into_checkpoint` selects its compatibility alias. The
command retains both methods, consumer, receive identity, processing
acknowledgement delay, processed prefix, and bound while expected producer
operations remain scenario-only. It converts the exact public batch into its
assignment-fenced checkpoint and attempts a bounded public commit. Its completion
reports both the exact records and whether that checkpoint committed; the
deterministic verifier requires both the expected record and a successful commit.

Single-member and multi-member group receives also drain public assignment
events and complete revocation leases within the receive's original deadline.
They retain those transitions for the next matching assignment observation,
preserving member identity and order. The adapter fails if its 256-transition
evidence capacity is exhausted instead of silently dropping observed facts.
Neither event handling nor receive observation extends background Fetch work.

A single direct assignment retains its exact consumer and topic-partition. A
batch direct assignment retains its exact consumer, complete caller-ordered
partition set, and completion timeout, then replaces that set through one public
call. The verifier requires all direct assignment commands exactly once in
scenario order; a single/batch substitution, reordered batch, altered field, or
extra assignment cannot qualify. Stable group assignment observation drains
public assigned, revoking, and lost transitions, explicitly completes current
revocation leases, and requires two identical complete assignment snapshots. Each
observation selects either repeated immediate `try_take_event` calls or bounded
polling of the runtime-neutral `next_event` future; the command and completion
retain that exact public method. A state-error
response for an old revocation is superseded only when the same public consumer
exposes a strictly newer assignment fence; it is not reported as a successful
acknowledgment. Observation may await that newer fence only within its original
deadline. Missing, equal, or older fences at expiry and other error kinds remain
failures.
Repeated observation of the same previously observed member set does not require
a new rebalance event: disrupting a non-coordinator need not change membership.
Initial observation or a changed member set still requires a public transition.
Scenario-only expected partitions never cross the adapter boundary. The verifier requires the
public assignment union to equal that expectation with pairwise-disjoint member
ownership and matching positive membership and assignment fences.

`control_assigned_consumer` carries a stable operation and consumer identity,
one bounded timeout, and exactly one public direct-consumer mutation. Replacement
and incremental addition entries carry explicit beginning, end, or nonnegative
offset positions. Removal, seek, pause, and resume carry exact topic-partition
identities; seek also carries its replacement position. The adapter invokes only
the packaged public replacement, add, remove, seek, pause, or resume call and
emits one completion with the same operation, consumer, and structural control
kind. Scenario record expectations never enter this command. Later receives are
still verified against independent broker coordinates and bytes, so a successful
control event cannot manufacture positioning, isolation, or cursor truth.

`group_receive` carries the exact public batch observer and checkpoint
conversion, receive identity, processing-acknowledgement delay, optional
processed-record count, and complete timeout. A nonzero delay requires an explicit processing timeout and
`group_consumer_acknowledge`; each delay is shorter than that timeout, the two
delays together exceed it, and both fit in the command timeout. The adapter
waits once, publicly acknowledges an assignment-fenced checkpoint, waits again,
and commits the same retained batch. Classic and KIP-848 scenarios thereby
prove that acknowledgement renews a processing window instead of merely
exercising a fast commit.

A processed-record count requires `group_consumer_partial_checkpoint`, at least
two distinct same-partition records declared in increasing sequence order, and
a nonempty proper prefix. Those expected record identities remain harness-only.
The adapter marks exactly that many records through the public checkpoint
builder and commits the result; partial receives reject the full-batch
`into_checkpoint` selector. Independent group-offset evidence must stop at
the prefix; after the first member closes, a replacement must receive the exact
unprocessed suffix before the final offset advances.

A group receive set carries its receive identity, structural record count,
ordered live member set, and complete timeout exactly once in scenario order.
It round-robins public batches, commits every assignment-fenced checkpoint, and
attributes exact records to the member that received them. The scenario's
expected operation identities remain harness-only; the verifier requires the
exact set once and binds every record to the latest stable public owner
assignment. Each public record is also joined to its exact independent broker
topic, partition, offset, key, value, and ordered headers.

Group creation may carry one capability-gated public configuration block.
Every registration requires one exact command preserving the client, member,
and group identities, caller-ordered subscription, selected protocol, and
complete optional configuration; a same-ID direct or Share consumer is not
equivalent.
Missing-offset reset selects fail-closed error, earliest, or latest behavior,
and read isolation selects
uncommitted or committed visibility before membership starts. Optional Fetch
and retained-delivery blocks obey the same bounded byte envelope as a directly
assigned consumer and are fixed before membership starts. Optional
processing, membership-start, seek, and close durations select the public
hosted runtime deadlines for either group protocol; every supplied value is
from one through `i32::MAX` milliseconds. `operation_config_method` selects
either the individual seek/close setters or one public aggregate
`GroupConsumerOperationConfig`; the aggregate path requires both durations.
An optional nonempty
`group_instance_id` selects static membership. Classic membership may
also select the range or cooperative-sticky assignor and each public classic
timing: session timeout, rebalance timeout, heartbeat interval, heartbeat
attempt timeout, rejoin backoff, and rejoin attempt timeout. Every supplied
timing is from one through `i32::MAX` milliseconds. Classic-only fields are
rejected for KIP-848 membership. Omitted timing fields retain their public
defaults, and an omitted assignor retains the classic range default. An omitted
block retains Testlab's established earliest, read-uncommitted, dynamic-member
behavior. The adapter receives no
expected record identity: fail-closed reset is proved by a correlated public
`state` failure and no successful receive for a new group with no committed
offset, latest reset is proved by a stable assignment that skips an independently
visible pre-join record, and read-committed isolation is proved by returning only
a nontransactional sentinel after a separately verified aborted transaction.

`control_group_consumer` carries a stable operation and consumer identity plus
one public pause, resume, or seek mutation. Pause and resume preserve the exact
ordered unique current topic-partition set. Seek carries one current partition
and an explicit beginning, end, or nonnegative offset position, then waits its
sole public observer. The completion echoes only the operation, consumer, and
structural control kind. Scenario record expectations remain harness-only;
classic and KIP-848 scenarios prove pause isolation, resumption, and seek replay
through committed public receives joined to independent broker coordinates.

`abandon_group_consumer` carries one exact consumer identity. The adapter drops
that public consumer owner without invoking its explicit close or shutdown
surface and emits one correlated `group_consumer_abandoned` event. This is
adapter-reported lifecycle truth, not proof of broker membership. Static-member
scenarios shut down the owning client and use a named public Admin description
plus an immediate independent group query inside the configured session window
to prove whether the broker still retains the identity.

`shutdown_group_consumer` carries a stable operation and consumer identity, a
request count from one through eight, and one complete observation bound. The
adapter clones the public shutdown control, issues that exact number of
idempotent requests, drains public assignment transitions, and completes only
when the public event stream returns terminal `None` and the public close
observer confirms the already-requested shutdown. Stream termination alone
closes observation, not necessarily broker membership. The later close joins
the same accepted leave and original deadline; it does not start a second
shutdown. Only then does Testlab release its owned handle. This completion is adapter-reported
lifecycle truth only; a following packaged Admin description and immediate
independent consumer-group query must both report zero members.

A Share create command carries 1 to 32 distinct topics in caller order, and the
adapter passes the complete list to the public subscription builder. Creation
completes only after the public assignment contains every requested topic;
assignments outside that list invalidate the run. Multi-topic scenarios require
exact publicly acquired records from every topic, each joined to independent
broker observation. An optional rack identity is passed to the public builder
and must be retained by the resulting public handle. Every registration
requires one exact command preserving client, member, and group identities,
caller-ordered topics, optional rack, both deadlines, and the complete optional
acquisition policy; a same-ID direct or ordinary group consumer is not
equivalent.

A share receive command retains its consumer, retained-batch identity, and
complete observation timeout exactly once in scenario order. Its public result
retains the exact ordered linear acquisition batch behind that identity until
one later acknowledgement, explicit drop, or consumer close. Share creation may
carry immutable broker long-poll, minimum-byte,
maximum-byte, maximum-record, acquisition-range, and attempt-timeout settings
only when `share_consumer_configuration` is advertised; omitted configuration
retains Testlab's bounded defaults. Kafka byte and time fields use the positive
signed 32-bit domain except that minimum bytes may be zero. The complete
membership-start and close bounds are likewise passed through the public
builder. Expected producer identities and expected acquisition counts remain
harness-only. The receive event reports the public batch's acquisition count.
Acknowledgement commands carry one record-ordered public disposition per
retained record plus the exact public batch conversion. The default
`into_acknowledgement` path applies those explicit decisions; `accept_all`
requires every declared disposition to be Accept and invokes the dedicated
all-record convenience path. The adapter rejects a structural count mismatch.
Every explicit batch drop and consumer close command also appears exactly once
in scenario order, preserving its command kind, consumer, and retained-batch
identity when applicable. The close-success expectation remains harness-only
and is checked against the public terminal certainty.
Acknowledgement and close events report success or the public delivery
certainty of failure; Testlab
never infers a stronger terminal. Delivery counts, acquisition count, and
positive membership fences remain in the correlated receive event so exact
configured range size, release or drop redelivery, and concurrent-member claims
are deterministic. Every retained public record is separately compared with
its exact independent broker coordinates and bytes; a configuration echo alone
cannot pass a scenario.

Singleton topic creation, ordered batch topic creation, and partition expansion
use the packaged client's public admin handle. Admin-created scenario topics are
deliberately excluded from independent environment provisioning, and broker
auto-creation is disabled. Immediately after each public completion, independent
metadata queries require the requested partition sets. A singleton creation or
each item in a batch may carry up to 32 unique caller-ordered non-sensitive
configuration entries. The adapter applies each entry through the packaged
topic builder and retains every complete ordered list on the wire. After a
successful item's topology proof, a later public description and immediate
independent librdkafka query must prove every selected value in the same order;
scenario validation rejects a missing or reordered description chain before
execution. A singleton creation may instead carry one
contiguous caller-ordered partition assignment per partition.
Its declared partition count and replication factor must exactly match those
assignments, and the adapter calls the packaged manual-placement constructor.
Immediate independent metadata must then show each exact replica order, a leader
inside that replica list, and the complete replica set in sync. Automatic
placement does not claim broker-selected replica topology. A partition expansion
may likewise carry one caller-ordered broker list per newly added partition plus
the scenario-only exact current count. The adapter calls the packaged
manual-assignment builder, while immediate metadata must expose the exact complete
partition set and each new partition's requested replica order, replica leader,
and full ISR. The current-count expectation never crosses the adapter boundary.

An expected duplicate creation repeats the exact public create-topic command
after a successful identical creation. The scenario-only expected error never
crosses the adapter boundary. The correlated public failure must report the
exact normalized `broker:broker_36` code, emit no creation completion, and is
followed by an immediate independent metadata query proving that the original
partition set remains unchanged. A declared expected failure may be followed by
later recovery steps in the same adapter session.

Expected singleton failures for partition creation, topic deletion, topic
description, and selected-offset listing likewise keep `expected_error_code`
only in the scenario. Each uses its normal wire command, emits no success event,
and may continue only when later scenario steps declare recovery work.
Missing-topic operations require one exact correlated `command_failed` with
normalized `broker:broker_3` and one nonpolling absence snapshot. The
invalid-partition ListOffsets operation instead requires normalized `routing`:
current metadata cannot route the absent partition, so no ListOffsets request
reaches a broker. Its independently provisioned topic contains every lower
partition, and the immediate topology snapshot proves the queried partition is
absent without treating an adapter diagnostic as broker truth.

An ordered `create_topics_batch` command carries multiple topic requests in
caller order and emits exactly one `topics_creation_completed` event. Its
`outcomes` array has the same length and order as the command's `topics` array,
with each item reporting its topic and optional normalized public error code.
Per-resource failures remain outcomes in that completion instead of collapsing
a partial batch into `command_failed`. Scenario-only expected error codes never
cross the adapter boundary; the verifier compares them with the corresponding
ordered outcomes and independently checks broker metadata for every requested
topic. Each item retains its caller-ordered configuration entries in that same
wire command. A successful configured item then requires the ordered public and
independent value chain described above; failed siblings do not erase its proof.

Named topic description, all-topic listing, and offset listing also use the
packaged public admin handle. Their adapter commands omit the scenario's
expected partitions, page boundaries, topic inclusion expectations, expected
offset, and expected errors. A named description explicitly selects either the
complete metadata-backed public operation or `DescribeTopicPartitions` with an
exact positive response-partition limit and cursor-following selection. Each
returned continuation cursor starts one separately submitted public page only
when requested. The single completion preserves every page's exact partition
subset and returned cursor as well as the sorted aggregate partition set. The
verifier requires the declared page boundaries, exact next-topic and
next-partition cursors, and immediate independent metadata for the aggregate.
An all-topic listing carries the exact
`include_internal` and `include_authorized_operations` options. Its single
completion preserves one byte-sorted unique outcome per returned name,
including the full successful topic ID, internal marker, authorization bitfield,
and error-aware partition description, or one normalized resource error. Every
expected topic is observed independently and must follow its declared public
inclusion; each included topic must be a success whose partition topology
matches that observation exactly. Every successful outcome has authorization
metadata if and only if requested. A classic group commit materializes canonical
`__consumer_offsets` before paired calls prove that disabling internal topics
omits it and enabling them includes it with the public internal marker. This
does not claim exhaustive topic listing, independently verified topic IDs,
replica topology, or internal-marker correctness beyond the canonical offsets
topic.

`describe_topics` carries two through 32 unique scenario-owned topic names in
caller order, the exact `include_authorized_operations` selection, and either
the default `name` or `topic_id` key. The topic-ID path resolves each name once
under the same deadline and invokes the public ID-keyed operation with the
resulting nonzero unique UUIDs. Scenario-only partitions and expected errors
stay in Testlab. Its single `topics_described` completion preserves the same
outer order, exact request UUID when ID-keyed, complete successful partition
results, matching nonzero inner topic IDs, internal-topic flags, the requested
authorization bitfields, and per-topic or per-partition errors. Name-keyed calls
use immediate independent metadata. ID-keyed calls use one immediate pinned
Kafka topic-CLI snapshot per topic to prove both UUID and topology in caller
order.

`delete_topics` carries two through 32 unique scenario-owned topic names in
caller order and selects either the default name-keyed operation or the public
topic-ID operation. Scenario-only expected errors stay in Testlab. Its single
`topics_deleted` completion preserves outer order, each exact ID request key
when selected, and every success or normalized per-topic error; a mixed result
does not collapse into `command_failed`. A matching prior plural description
and independent observation establish every selected topic's state and, for
ID-keyed deletion, the exact UUID deletion fence. After the public result,
independent metadata is polled within the original observation bound until
every selected name is absent, then recorded with consecutive ordinals.

Offset listing selects `earliest`, `latest`, the record carrying the greatest
timestamp, or the earliest record at or after one exact nonnegative timestamp
for an isolated partition after two acknowledged records or deterministic
environment seeding. An immediate independent librdkafka query captures both
low and high watermarks after the public result. Boundary selections must equal
the corresponding watermark. Record-timestamp selections return an offset and
associated timestamp matching independent record truth. The maximum-timestamp
fixture places its greatest timestamp before a later lower timestamp; the
caller-timestamp fixture places a lower timestamp before its selected record.
Those orderings distinguish both paths from earliest and latest selection.
Every singleton command carries exact `read_committed` or `read_uncommitted`
isolation; omitted scenario fields select the backward-compatible
`read_committed` default. The paired release scenario selects both values over
ordinary committed records and proves each exact command plus its immediate
independent watermark result. It does not claim an open-transaction last-stable-
offset distinction. Other broker-relative positions and leader epochs are
outside this slice.

`list_offsets_batch` carries two through 32 unique topic-partition selections
in caller order, exact `read_committed` or `read_uncommitted` isolation for the
whole request, and invokes one public Admin operation. Omitted scenario fields
select the backward-compatible `read_committed` default; the wire command never
defaults. Scenario-only expected offsets stay in Testlab. Its single
`offsets_listed` completion preserves one nullable offset and optional
normalized error per requested identity in that same order; an unexpected
resource error cannot disappear into a successful batch claim. Immediate
independent watermark queries run in the declared order and must select every
expected earliest or latest offset exactly. The packaged read-uncommitted batch
uses ordinary committed records and does not claim open-transaction LSO
differentiation.

Singleton record deletion selects one explicit positive cutoff on a fresh
independently seeded partition. Ordered earliest and latest queries establish
the precondition. The packaged public result reports its low watermark, then a
polling independent query requires the low watermark to reach the cutoff while
the high watermark remains unchanged.

`delete_records_batch` carries two through 32 distinct topic-partitions in
caller order and invokes one public Admin operation. Each target selects either
an explicit positive cutoff or Kafka's high-watermark sentinel; scenario-only
baseline watermarks do not cross the adapter boundary. Its single
`records_batch_deleted` completion preserves every identity, successful low
watermark, or normalized per-target error in caller order. Ordered prior
earliest/latest queries and immediate polling watermark observations prove each
partition's independent before-and-after range without accepting adapter output
as broker truth.

Topic-configuration description selects one exact key. Its wire command omits
the scenario's expected value, and the public nullable value must match an
immediate independent librdkafka query. `describe_topic_configs` carries two
through 32 distinct topics with one selected key each into one public call; its
single completion retains every public value or normalized error in caller
order, and contiguous immediate independent reads must confirm every expected
non-sensitive value in that order. Its exact `include_synonyms` and
`include_documentation` flags select those public builder options. Successful
selected entries preserve read-only, source, sensitive, synonym, type, and
documentation facts; requested synonyms must include the effective value, and
requested documentation must include a type and nonempty text. Incremental
alteration is restricted to one
exact `SET` replacement after a separately identified description proves a
different baseline. The public completion and a polling independent query must
then establish the requested value. Sensitive or unavailable values invalidate
the evidence instead of being converted into a definite result.

`alter_topic_configs` carries two through 32 distinct topics with one exact
mutation method each into one public call. `set` carries the requested final
value; `append` and `subtract` carry only their list operand; `delete` and
`restore_default` carry no value. Only `set` carries its expected final value;
every other expected final value remains scenario-side. Its named prior
`describe_topic_configs` baseline must match every topic, key, and previous value
in caller order, each final value must differ, and no selected key may be
mutated between baseline observation and submission. The single completion
retains every public per-topic success or normalized error in caller order;
contiguous immediate independent polling must confirm all requested values.

Plural topic-configuration description and alteration carry an `api` selector.
The default `topic` path exercises the topic-specific convenience methods;
`resource` exercises the generic configuration-resource methods with exact
type-2 topic resources while preserving the same caller-order and independent
state requirements. Mutation commands additionally accept `legacy_topic` and
`legacy_resource`, selecting the corresponding public full-snapshot replacement
surface while retaining the exact named description baseline and post-state
requirements. Legacy selectors accept only `set` and `restore_default`, while
incremental selectors accept `set`, `delete`, `append`, and `subtract`. A
restoration's expected final broker value remains scenario-side, and the wire
command requires the adapter to use the public default-restoration constructor.
`list_config_resources` carries an `api` selector but
sends no expected names over the wire. `resource` filters the generic public
request to topic resources and requires every dynamically configured topic in
the canonical type-tagged result plus immediate independent metadata.
`client_metrics` invokes the dedicated public client-metrics resource listing,
maps each name to Kafka resource type 16, and requires its exact canonical set
to match one immediate pinned `kafka-client-metrics.sh --list` snapshot.

Topic creation, partition increase, incremental topic-configuration
replacement, and client-quota alteration carry an exact `validate_only` wire flag. Successful validation
uses a distinct correlated completion rather than the corresponding mutation
completion. Immediate non-polling independent metadata or configuration reads
must retain the exact pre-request state; partition and configuration validation
also require a separately observed exact baseline. Client-quota validation keeps
its scenario-only exact current rate off the wire and requires an immediate
independent Kafka CLI read of that unchanged rate. A finite snapshot does not
claim that Kafka could never mutate later, so the scenarios add later public
description or real-mutation barriers that expose delayed effects.

The singleton `describe_consumer_group` command carries one exact group and the
`include_authorized_operations` selection while keeping the expected member
count scenario-side. Its public result preserves the optional raw bitfield and
member count; an immediate independent group query confirms the same live group
and count. The paired packaged calls exclude and then include the bitfield while
the membership remains unchanged.

The consumer-group offset slice selects one exact group and topic-partition
after a public classic-group receive commits its checkpoint. The adapter command
carries the requested stable-read option but omits the expected offset. Its
result preserves an absent committed offset as absence. A separate librdkafka
consumer that never joins or commits independently queries that exact group and
topic-partition immediately after the correlated public action; both public and
independent results must equal the declared offset.

Protocol v22 adds bounded caller-ordered plural offset operations. One
`list_consumer_group_offsets_batch` selects multiple topic-partitions from one
group, while `list_consumer_groups_offsets` carries ordered groups with ordered
selections inside each group. Scenario-only expected offsets do not cross the
adapter boundary. Their correlated listing events retain exact per-resource
offsets and normalized errors in caller order. Immediate independent reads emit
one existing committed-offset fact per selected key in the same flattened order;
listing observations do not poll for a future value. Passing claims require all
group-level and per-resource public errors to be absent.

`alter_consumer_group_offsets` and `delete_consumer_group_offsets` mutate an
ordered set of distinct topic-partitions in one exact group. Their completion
events retain one ordered per-resource error outcome. Every selected key requires
a distinct earlier public listing corroborated by an immediate independent read;
an alteration additionally requires a different baseline value. The alteration
command can carry an optional nonnegative retention duration within Kafka's
signed 64-bit millisecond range. The adapter calls the public
`retention_time` builder method only when that value is present, and the sealed
command must equal the scenario selection exactly. Polling independent reads
after the completion establish every requested offset or explicit absence; they
do not claim eventual expiry. One successful sibling cannot hide a failed or
reordered outcome.

`describe_classic_groups` carries ordered group IDs and the exact
`include_authorized_operations` selection but keeps expected member counts
scenario-only. Its one correlated event retains ordered nullable member counts,
requested authorization bitfields, and per-group errors. Each public result
must match an immediate independent group-existence and member-count fact.
Classic membership is not inferred from that broker fact: every counted live
member must be an explicitly
declared classic consumer with a prior successful committed `group_receive` and
a positive classic group epoch.

`describe_consumer_groups` carries caller-ordered group IDs, the exact
`include_authorized_operations` selection, and one deadline. Its correlated
event preserves each classic or KIP-848 public description variant, state,
assignor, epochs, member identities, subscriptions, typed assignments, raw
classic payloads, requested authorization bitfield, and per-group error.
Expected values remain scenario-side. Every declared live member requires a prior committed
receive with a matching positive protocol epoch, and an immediate independent
query must return the same member counts in the same caller order.

`delete_consumer_groups` carries two through 32 distinct group IDs in caller
order and invokes one public Admin operation under one deadline. Its completion
retains one success or normalized error per group without receiving scenario
expectations. A prior caller-ordered classic-group description and its immediate
independent snapshot must prove every group exists with zero members. After the
public deletion, an independent group query polls until every requested group is
absent and then emits consecutive observations in the same caller order.

`remove_consumer_group_members` carries one exact group, two through 32
caller-ordered static `group_instance_id` values, a nonempty broker-visible
reason, and one deadline. Its command omits the scenario-only named description
baseline. The correlated `consumer_group_members_removed` event retains the
group, nonnegative throttle, and one normalized outcome per caller position.
The scenario first abandons each configured classic static consumer owner,
shuts down each owning client without a group close, and proves that all members
remain registered through matching public and independent descriptions inside
an explicit session-timeout window covering the scenario deadline. After
removal, the independent group observer polls for zero members.

ACL administration is bounded to one through 32 caller-ordered concrete
bindings over literal topic, group, or transactional-ID resources, exact
`User:` principals, wildcard host `*`, the read, write, or create operation,
and allow or deny permission. Creation and deletion preserve one public outcome
per caller position, including exact nested deletion matches and normalized
broker errors. Description uses one exact public filter. After every successful
public terminal, the environment independently invokes the pinned Kafka ACL CLI
for each binding in caller order. Creation and description require exact
presence; deletion requires exact absence. Unsupported public result shapes,
CLI output, reordering, partial success, or an observation outside the command
window invalidates the corresponding claim.

Client-quota administration is bounded to producer and consumer byte-rate
overrides for one exact non-default user entity. Rates are whole numbers from
one through `u32::MAX`; alteration either replaces one rate or removes it, and
description selects the same exact user and key. Every description command
carries exact strict or non-strict filter intent; omitted scenario fields retain
Testlab's backward-compatible strict default, while the wire never defaults.
The paired fixture returns the same simple named-user entity for both choices
and does not claim composite-entity result divergence. Every successful public
terminal is followed immediately by a pinned Kafka CLI query whose raw output
is retained. The verifier requires the public entity or value and independent
resulting broker state to agree exactly; unknown keys, fractional values,
ambiguous entities, or malformed CLI output invalidate the claim.

User SCRAM administration is bounded to one exact non-default user and one
SCRAM-SHA-256 or SCRAM-SHA-512 mechanism. Iteration counts are 4096 through
16384. An alteration either upserts one credential using password bytes read
only inside the adapter process from `TESTLAB_KAFKA_SASL_PASSWORD`, or deletes
that mechanism; protocol commands, events, history, and diagnostics never carry
the password. Description selects the exact user and mechanism and retains
either its iteration count or Kafka's exact resource-not-found broker code 91.
Every successful public terminal is immediately followed by a pinned Kafka CLI
query whose raw output is retained. The verifier requires public and independent
non-secret state to agree; extra users, mechanisms, rows, quota values, or
malformed CLI output invalidate the claim.

Share-group description selects one exact active group and the exact
`include_authorized_operations` option while its public member retains an
acquired batch. Scenario-owned state, member-count, rack, topic, and partition
expectations stay in `testctl`. The completion preserves Kafka's
public state, group and assignment epochs, assignor, ordered members, rack
identities, subscriptions, nonzero topic IDs, exact partition assignments, and
the requested authorization bitfield.
An immediate pinned `kafka-share-groups.sh --describe --state` query
independently confirms the stable state and member count; that CLI snapshot
cannot substitute for the detailed public assignment.

Plural Share-group description carries two through 32 distinct group
identities in caller order plus the exact authorization option while each
modeled member retains an acquired batch. Scenario-owned state, member-count,
topic, and partition expectations stay in `testctl`. One public call preserves
an exact success or failure per
group in caller order; every success retains the same complete detailed public
description as the singleton operation. Separate immediate pinned
`kafka-share-groups.sh --describe --state` queries preserve that order and
independently confirm each stable state and member count.

Share-group offset listing selects one exact topic-partition from one group.
Scenario-owned start-offset and lag expectations stay in `testctl`. The public
completion preserves the selected identity, nonzero topic ID, optional start
offset, optional leader epoch, optional lag, and any partition-scoped failure.
An immediate pinned `kafka-share-groups.sh --describe --offsets` query
independently confirms the exact start offset and lag.

Plural Share-group offset listing carries two through 32 distinct groups in
caller order, each with one through 32 distinct selected topic-partitions.
Scenario-owned start-offset and lag expectations stay in `testctl`. One public
call preserves exact group-level and partition-level success or failure
outcomes in caller order. Separate immediate pinned
`kafka-share-groups.sh --describe --offsets` queries run once per group and
retain every selected start offset and lag in the same flattened order.

Share-group offset alteration carries one exact nonnegative requested start
offset but keeps expected resulting lag in `testctl`. Scenario validation
requires a distinct earlier listing and successful closure of every modeled
member in the group. The completion preserves exact topic-partition identity,
Kafka's nonzero topic ID, and any partition-scoped failure. An immediate pinned
`kafka-share-groups.sh --describe --offsets` query confirms the requested start
offset and expected lag; a later public listing can independently exercise the
same post-state without serving as the mutation result.

Share-group offset deletion carries one exact group and topic; its selected
partition remains scenario-only because Kafka deletes the topic's complete
Share-group offset state. Scenario validation requires a prior independently
corroborated listing and successful closure of every modeled member. The public
completion preserves exact topic identity, Kafka's nonzero topic ID, and any
topic-scoped failure. An immediate pinned
`kafka-share-groups.sh --describe --offsets` query must report no start offset
or lag for the selected partition.

Share-group deletion carries two through 32 distinct group identities in caller
order. Scenario validation requires every modeled member of every selected
group to close successfully first. The public completion preserves one exact
success or failure per requested group in caller order. One immediate pinned
`kafka-share-groups.sh --list` snapshot remains read-only and independently
reports whether each selected group still exists; extra unselected groups do
not affect the result.

The Streams-group scenario independently provisions two distinct modern
WordCount applications and their exact stable input offset. The
`exercise_streams_group_admin_lifecycle` wire command carries those two
application IDs, the selected input topic, one different valid replacement
offset, exact authorization, full-topology, and stable-offset selections, and
one complete deadline; the expected initial offset and fixed output topic remain
in Testlab. Under that single bound the packaged public Admin describes the
primary group, describes both groups in caller order, lists selected offsets
through the singleton and plural APIs, alters and
then deletes the primary offset with public reads after each transition, and
deletes both groups in caller order. Descriptions retain Empty state, epochs,
initialized topology sources, requested authorization bits, and a requested v1
topology-description status with a graph size only when available. Offset results retain nullable
committed offsets, leader epochs, and bounded metadata. One completion owns all
nine call results and throttles. An immediate pinned
`kafka-streams-groups.sh --list` projection then independently proves both
deleted identities absent without retaining unrelated group rows.
The paired release scenario disables all three selections. Its description
results must omit authorization data and either omit the v1 topology status or
retain only Kafka's `NotRequested` status; its returned group offsets remain
anchored by the same exact lifecycle and final independent absence.

Topic deletion is preceded by a public description of the exact
harness-provisioned topic. Independent metadata queries confirm the declared
partitions after description and boundedly poll for explicit absence after the
delete completion. Cluster description carries the exact
`include_fenced_brokers` and `include_authorized_operations` options and reports
the public cluster identity, all returned broker IDs, fenced-broker markers,
and the requested authorization bitfield. The environment independently
queries the identity and active broker IDs and owns the expected broker count
through its declared topology; that snapshot does not substitute for the public
option-specific metadata. A reversible three-broker control describes the full
baseline, gracefully stops broker 3, and invokes paired public descriptions
with `include_fenced_brokers` disabled and enabled. Both calls must agree with
an immediate independent two-active-broker snapshot; the disabled result must
exclude broker 3, while the enabled result must include broker 3 with its public
fenced marker. The broker is then restarted and the complete three-broker state
is described again. This control is limited to disposable plaintext
three-broker cells.

Broker unregistration carries only the exact client, operation, broker ID, and
deadline. Scenario validation confines it to a contiguous public cluster
baseline, matching environment-owned broker stop, unregistration, matching
broker start, and restored public cluster description. The completion preserves
the requested broker ID and Kafka throttle. Before the broker can restart, an
independent librdkafka metadata query polls for the exact scenario-owned
remaining broker set. The later public and independent descriptions must restore
the original broker set under the same nonempty cluster ID. This destructive
scenario is configured only for the disposable plaintext three-broker cell.

Cluster feature description carries only client identity, operation identity,
and one complete deadline. Its public completion retains canonical supported
and finalized numeric ranges, the supported-range completeness marker, the
finalized epoch, and ZK-migration readiness. The scenario's expected migration
flag stays outside the wire command. An immediate independent
`kafka-features.sh describe` snapshot retains every supported row and epoch;
symbolic `metadata.version` labels are resolved through Testlab's exact pinned
Apache Kafka stable-level map.

`validate_feature_updates` carries one through 32 unique caller-ordered feature
updates and always invokes the public builder with `validate_only(true)`. The
scenario-only baseline operation stays off the wire. Its completion retains
the nonnegative throttle and every per-feature public outcome in caller order;
exact pinned CLI snapshots before and immediately after must retain identical
feature rows and finalized epoch. The checked-in scenario is limited to Kafka
4.3.1 and validates the stable `metadata.version=30` level.

`exercise_delegation_token_lifecycle` carries an optional exact
`expire_after_ms` selection without any token secret. The checked-in Kafka 4.3.1
scenario sets it to zero, requiring the adapter to call the public
`expire_after(Duration::ZERO)` builder instead of sending Kafka's omitted
immediate-expiry sentinel. The sanitized independent observer still requires
zero live owner tokens immediately after completion.

Active-producer description carries one exact topic-partition, an optional
nonnegative broker route, and a complete deadline. Absence retains automatic
partition-leader routing. Exact broker routing is exercised only on declared
single-broker cells whose fixed broker identity is one. The scenario-owned
expected count stays outside the wire command.
Its public completion retains producer ID, producer epoch, last sequence, last
timestamp, coordinator epoch, and optional current-transaction start offset in
canonical producer-ID order. An immediate independent
`kafka-transactions.sh describe-producers` snapshot retains the same fields.

Log-directory description carries one exact topic-partition and complete
deadline; the scenario-owned expected replica count stays outside the wire
command. The adapter discovers the public broker set, reverses it to exercise
caller-order preservation, and selects the exact partition through
`describe_log_dirs`. Its completion retains throttle, canonical paths, optional
capacity and cordon fields, and exact replica size, lag, and future markers. An
immediate independent `kafka-log-dirs.sh --describe --topic-list` JSON snapshot
retains every shared field in canonical broker order.

Replica log-directory description carries the same exact topic-partition and
complete deadline while its expected current-placement count stays outside the
wire command. The adapter discovers every broker, queries the corresponding
replica identities in descending broker order through
`describe_replica_log_dirs`, and preserves current, absent, and future paths
with exact signed lags. The immediate pinned `kafka-log-dirs.sh` snapshot
provides the independently canonicalized placement state.

Replica log-directory alteration carries two through 32 selected replicas for
one topic-partition in caller order, one absolute non-control target path per
replica, and one complete deadline. Its public completion preserves that order,
maximum throttle, and one explicit success or per-replica failure per target.
The environment repeatedly records pinned `kafka-log-dirs.sh` snapshots until
every selected replica has exactly one current placement at its requested path
and no future placement; those later snapshots never replace the public result.

Metadata-quorum description carries only client identity, operation identity,
and one complete deadline. Its public completion retains leader identity,
epoch, high watermark, canonical voters and observers, optional directory IDs,
log-end offsets, optional timestamps, and optional v2 node listeners. Immediate
`kafka-metadata-quorum.sh describe --status` and `--replication` snapshots are
joined outside the adapter across both legacy ID-only and directory-aware CLI
layouts. The verifier requires stable membership, leader, represented
directory, and endpoint agreement while allowing only nonregressing offsets,
watermark, and timestamps between the public call and those sequential views.

Transaction listing carries no fixture expectations or filters across the
wire. Its public completion retains every transactional ID, producer ID, and
Kafka-owned state in canonical transactional-ID order. Caller-ordered
transaction description carries only the selected IDs and complete deadline;
its completion retains state, configured timeout, optional start time,
producer identity and epoch, and canonical topic-partition participation.
Immediate pinned `kafka-transactions.sh list` and one
`kafka-transactions.sh describe` query per selected ID retain independent exact
snapshots. Scenario validation requires all selected transactional producers
to be initialized and closed before either read.

Caller-ordered producer fencing carries only the selected transactional IDs
and complete deadline. Its completion retains the maximum broker throttle and
one returned producer ID and epoch per caller position. One immediate pinned
transaction description per selected ID must match those post-fence identities
and the scenario-owned stable state. Its broker-reported transaction timeout
must be positive and no greater than the complete public deadline because the
client derives that timeout from the operation's remaining budget. Scenario
validation requires every owner to be initialized and closed before the
mutation, avoiding concurrent fixture changes between the public result and
independent snapshots.

Partition-reassignment mutation carries one through 32 caller-ordered exact
topic-partition replacement replica lists, the explicit replication-factor
change policy, and one complete deadline. Its public completion retains Kafka's
throttle and one success or normalized error per caller position. An immediate
independent metadata observer polls until every successful target has the exact
requested replica order, full canonical ISR, and a leader within that set.
Selected listing carries caller-ordered topic-partitions, while an absent
selection invokes the separate all-active public path. Expectations stay in
Testlab; both completion forms preserve deterministic rows and throttle. One
immediate pinned `kafka-reassign-partitions.sh --list` snapshot independently
confirms the active rows. The stable scenario first waits for convergence, then
requires both listing forms and the CLI to report no remaining movement.

Leader election carries an explicit preferred or unclean policy, one complete
deadline, and either caller-ordered selected partitions or an absent selection
for the distinct cluster-wide path. Required cluster-wide partitions and the
leader-change requirement remain scenario-only. Selected outcomes preserve
caller order; cluster-wide outcomes use canonical topic-byte and partition
order. For each stable preferred-election path, Testlab independently stops the
exact original leader, proves a distinct replacement and public progress,
restores the original replica into the full ISR, and polls metadata until that
preferred replica is leader again.

Group listing carries an exact `api` selector for the consumer-only compatibility
view or the generic `ListGroups` view. Both commands may carry caller-ordered
broker-side `state_filters` and `group_type_filters`; only the generic view may
add caller-ordered client-side `protocol_type_filters`. Required group IDs remain
scenario-only. Either public result must contain the independently listed live
groups without broker-local errors. Consumer-group description likewise omits
the expected member count. All independent queries run immediately after their
public results, before a later scenario step can close or otherwise change the
membership.

Consumer-group offset alteration and deletion operate on one exact inactive
classic group and topic-partition. A preceding public offset listing and its
independent query establish the committed-offset baseline. Alteration is
followed by another public and independent listing at the requested offset;
deletion requires an independent explicit absence. Group deletion is preceded
by a public description of zero members and an exact offset listing, then
requires an independent not-found group result. Observer errors and timeouts
invalidate these claims rather than manufacturing absence.

One `create_transactional_producer` command retains the exact client and producer
identities, transactional ID, broker-side transaction timeout, and public
initialization deadline applied through `Client::transactional_producer`,
`transaction_timeout`, and `deadline_after`. Repeated denied then recovered
initialization of one producer identity requires the same ordered command twice;
an ordinary producer, a fencing command, or an altered field cannot substitute.

One `execute_transaction` command owns a complete linear begin, ordered staging,
and terminal operation because the public transaction token borrows its producer
until it ends. The default `send` method stages each record independently;
`send_batch` requires `transaction_batch_send` and stages one nonempty record
set sharing an exact topic and explicit partition through the public homogeneous
batch method. The command retains that method selection and the complete
caller-ordered record set. A scenario may reference one prior `topic_id`
description without sending its expected IDs across the process boundary. That
sets `validate_topic_uuids` on the command and requires
`transaction_topic_uuid_validation`: the adapter independently resolves the
distinct record topics through public Admin, binds each record to its nonzero
UUID during admission, and waits for fresh validation of the current
transaction revision after all staging and before commit. The completion
retains the same IDs in first-record topic order. `commit` and `abort` otherwise
use the transaction token directly.
`admin_partition_abort` is restricted to one exact staged record: the adapter
uses public `DescribeProducers` to obtain its producer and coordinator identity,
emits the open state, calls public Admin partition abort, and emits the cleared
state before the token can drop. Each accepted record reports
`transaction_staged`, then one exact `transaction_completed` event reports the
requested terminal operation. The full declared operation set must stage exactly
once before that completion. The
independent observer uses `read_committed`: every committed operation must appear
exactly once with matching record bytes, public offset, and strictly increasing
caller order within a partition, while the complete aborted set must remain
absent. For Admin partition abort, an immediate pinned Kafka CLI producer-state
snapshot must preserve the cleared public producer identity and sequence while
allowing a later control-record timestamp. That absence does not assert
whether an aborted record occupied a physical log position. A later transaction
on the same public producer may not begin staging before the prior completion
boundary.

One `execute_transactional_transform` command receives one public group batch,
retains its public membership metadata and assignment-fenced checkpoint,
stages the declared output set, transfers that exact checkpoint through public
`send_offsets`, and commits or aborts the same linear transaction. The
scenario's expected input operation remains harness-only. Its completion
reports the exact public input records, group identity, protocol-specific
membership epoch, checkpoint topic-partition, and next offset. A committed
checkpoint is immediately corroborated by public Admin plus an independent
librdkafka group-offset query. An aborted checkpoint is proved unadvanced only
when a replacement group member receives the exact source record again.

One `fence_transaction` command keeps the original public transaction open and
carries an exact `fence_method`. `replacement_initialization`, the scenario
default, initializes a replacement producer with the same transactional ID
before the old commit. `admin_force_termination` instead invokes the singleton
public Admin termination operation before the old commit and does not initialize
the replacement until that commit result has been obtained. Both methods report
the staged record, replacement producer creation, and normalized old-commit
result separately. The verifier requires `fenced`, independently requires the
staged record to remain absent under `read_committed` isolation, and requires a
later replacement transaction to commit normally.

Lifecycle commands target one exact public handle. Repeated readiness probes
and producer flushes retain distinct command identities. Closing one independent
producer does not close a sibling or prevent a distinct replacement built from
the same live client configuration; shutting down one client does not affect
another client.
Scenarios prove those boundaries only through later public completions and
independently observed Kafka records on the still-open handle.

## Failure behavior

A normal public client API failure emits one correlated `command_failed` event
and leaves the adapter session alive. Testctl stops issuing dependent steps and
sends `abort`, while a declared negative expectation may retain the same public
client for later recovery steps. Testctl records the targeted independent
metadata postconditions, and only the exact correlated expected codes can
satisfy the deterministic contract. A completed admin batch with mixed
per-resource outcomes remains a completion; only an operation-wide public
failure emits `command_failed`. An adapter or protocol failure instead emits
`fatal` and exits nonzero. A crash, malformed stdout, wrong version, wrong
command ID, or timeout invalidates the run.

## Evolution

Group creation explicitly selects classic or KIP-848 consumer membership. A
create command carries 1 to 32 distinct topics in caller order, and the adapter
passes the complete list to the public subscription builder. Multi-topic
scenarios require public assignments and exact independently observed records
from every subscribed topic. A successful group receive reports the public
membership epoch observed after its
assignment-fenced checkpoint commits. The verifier requires that epoch to be
positive and from the requested protocol family, preventing silent fallback to
classic membership.

Protocol v148 is an exact semantic contract. New capabilities may be declared
from the existing vocabulary, but adding or removing fields, changing meaning,
or narrowing accepted values requires a new protocol version.
