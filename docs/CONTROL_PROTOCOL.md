# Adapter control protocol

## Transport

Protocol v34 is UTF-8 JSON Lines over stdin and stdout.
This cut pairs it with scenario schema v37 and evidence schema v26.

- One line is one complete JSON object.
- Adapter stdout is protocol-only; diagnostics use stderr.
- A line larger than 4 MiB is invalid before unbounded allocation.
- Every event repeats the protocol version and originating command ID.

Lifecycle verdicts join each creation, readiness, assignment, flush, close,
shutdown, and finish event to that exact originating command. Repeating a
public operation on one resource therefore requires one completion per command;
resource-level event totals are not a substitute for correlation.

Configured client creation carries the complete client-wide producer policy:
delivery timeout, one of the five public compression selections, bounded retry
count and backoff, and portable active, waiting, batching, request, in-flight,
and linger limits. These values are fixed before the public client host starts.
Durability cannot be downgraded: idempotence and `acks=all` remain client-owned
invariants outside the protocol vocabulary. Each codec has an independent
real-Kafka scenario checked by ordinary terminal and broker-observation rules.

Client metrics observation carries only stable client and operation identities
to the adapter. Scenario-only record floors and required idle, accepting, or
healthy states remain in testctl and never cross the adapter boundary. The
completion preserves every public calls, failures, mailbox, latency, and
producer snapshot getter. Immediate metrics backpressure is retried within a
bounded admission window; one accepted observer is waited exactly once.

## Handshake

Testctl sends `hello` with run ID, scenario ID, ordered environment endpoints, and a
non-secret security policy. TLS and SASL secrets are referenced by environment
variable name and passed only in the adapter process environment. The adapter
replies `ready` with implementation identity, version, and exact capabilities.

## Commands

- `hello`
- `create_client`
- `create_configured_client`
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
- `receive`
- `start_concurrent_actors`
- `join_concurrent_actors`
- `close_assigned_consumer`
- `create_group_consumer`
- `group_receive`
- `observe_group_assignments`
- `group_receive_set`
- `control_group_consumer`
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
- `describe_topic`
- `list_topics`
- `list_offsets`
- `delete_records`
- `describe_topic_config`
- `alter_topic_config`
- `describe_cluster`
- `list_consumer_groups`
- `describe_consumer_group`
- `list_consumer_group_offsets`
- `list_consumer_group_offsets_batch`
- `list_consumer_groups_offsets`
- `alter_consumer_group_offset`
- `alter_consumer_group_offsets`
- `delete_consumer_group_offset`
- `delete_consumer_group_offsets`
- `delete_consumer_group`
- `describe_classic_groups`
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
- `topic_described`
- `topics_listed`
- `offset_listed`
- `records_deleted`
- `topic_config_described`
- `topic_config_altered`
- `cluster_described`
- `consumer_groups_listed`
- `consumer_group_described`
- `consumer_group_offset_listed`
- `consumer_group_offsets_listed`
- `consumer_groups_offsets_listed`
- `consumer_group_offset_altered`
- `consumer_group_offsets_altered`
- `consumer_group_offset_deleted`
- `consumer_group_offsets_deleted`
- `consumer_group_deleted`
- `classic_groups_described`
- `transactional_producer_created`
- `transaction_completed`
- `transactional_transform_completed`
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

A cancellation command first obtains public producer ownership of one exact
record, retains its sole terminal observer, and invokes public cancellation
twice with bounded backpressure retry. It reports both immediate stage-aware
outcomes in order only after the retained observer emits its authoritative
terminal. `cancelled_not_sent` requires a definitely-not-sent `cancelled`
terminal and the next attempt must be `already_terminal`. `too_late` preserves
zero-or-one broker visibility and may only remain `too_late` or advance to
`already_terminal`; Testlab never strengthens it into a not-sent promise.

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

A group receive additionally consumes the exact public batch into its
assignment-fenced checkpoint and attempts a bounded public commit. Its
completion reports both the exact records and whether that checkpoint committed;
the deterministic verifier requires both the expected record and a successful
commit.

Single-member and multi-member group receives also drain public assignment
events and complete revocation leases within the receive's original deadline.
They retain those transitions for the next matching assignment observation,
preserving member identity and order. The adapter fails if its 256-transition
evidence capacity is exhausted instead of silently dropping observed facts.
Neither event handling nor receive observation extends background Fetch work.

A batch direct assignment replaces the complete caller-ordered partition set
through one public call. Stable group assignment observation drains public
assigned, revoking, and lost transitions, explicitly completes current revocation
leases, and requires two identical complete assignment snapshots. A state-error
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

A group receive set carries only a structural record count and an ordered live
member set. It round-robins public batches, commits every assignment-fenced
checkpoint, and attributes exact records to the member that received them. The
scenario's expected operation identities remain harness-only; the verifier
requires the exact set once and binds every record to the latest stable public
owner assignment. Each public record is also joined to its exact independent
broker topic, partition, offset, key, value, and ordered headers.

Group creation may carry one capability-gated public configuration block.
Missing-offset reset selects earliest or latest, and read isolation selects
uncommitted or committed visibility before membership starts. An omitted block
retains Testlab's established earliest and read-uncommitted behavior. The
adapter receives no expected record identity: latest reset is proved by a
stable assignment that skips an independently visible pre-join record, while
read-committed isolation is proved by returning only a nontransactional
sentinel after a separately verified aborted transaction.

`control_group_consumer` carries a stable operation and consumer identity plus
one public pause, resume, or seek mutation. Pause and resume preserve the exact
ordered unique current topic-partition set. Seek carries one current partition
and an explicit beginning, end, or nonnegative offset position, then waits its
sole public observer. The completion echoes only the operation, consumer, and
structural control kind. Scenario record expectations remain harness-only;
classic and KIP-848 scenarios prove pause isolation, resumption, and seek replay
through committed public receives joined to independent broker coordinates.

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

A share receive retains the exact ordered linear acquisition batch behind its
receive identity until one later acknowledgement, explicit drop, or consumer
close. Share creation may carry immutable `max_records` and `batch_size`
settings only when `share_consumer_configuration` is advertised; omitted
configuration retains Testlab's bounded defaults. Expected producer identities
and expected acquisition counts remain harness-only. The receive event reports
the public batch's acquisition count. Acknowledgement
commands carry only one record-ordered public disposition per retained record,
and the adapter rejects a structural count mismatch. Acknowledgement and close
events report success or the public delivery certainty of failure; Testlab
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
metadata queries require the requested partition sets. This slice does not claim
replica topology.

An expected duplicate creation repeats the exact public create-topic command
after a successful identical creation. The scenario-only expected error never
crosses the adapter boundary. The correlated public failure must report the
exact normalized `broker:broker_36` code, emit no creation completion, and is
followed by an immediate independent metadata query proving that the original
partition set remains unchanged. A declared expected failure may be followed by
later recovery steps in the same adapter session.

Expected singleton failures for partition creation, topic deletion, topic
description, and selected-offset listing likewise keep `expected_error_code`
only in the scenario. Each uses its normal wire command and must emit one exact
correlated `command_failed` with normalized `broker:broker_3`, no success event,
and may continue only when later scenario steps declare recovery work.
Missing-topic cases are followed by one nonpolling absence snapshot. The
invalid-partition case provisions the topic
with every lower partition and snapshots that topology, proving the queried
partition is absent without treating an adapter error string as broker truth.

An ordered `create_topics_batch` command carries multiple topic requests in
caller order and emits exactly one `topics_creation_completed` event. Its
`outcomes` array has the same length and order as the command's `topics` array,
with each item reporting its topic and optional normalized public error code.
Per-resource failures remain outcomes in that completion instead of collapsing
a partial batch into `command_failed`. Scenario-only expected error codes never
cross the adapter boundary; the verifier compares them with the corresponding
ordered outcomes and independently checks broker metadata for every requested
topic.

Named topic description, all-topic listing, and offset listing also use the
packaged public admin handle. Their adapter commands omit the scenario's
expected partitions, required topics, expected offset, and expected errors. A named description
must report the exact declared partition indices, which an immediate independent
metadata query confirms. An all-topic listing preserves the public byte-sorted
unique order and must contain the declared required topics, whose existence is
likewise established by independent metadata observations. These checks do not
claim exhaustive topic
listing, internal-topic filtering, topic IDs, or replica topology.

Offset listing selects `earliest` or `latest` for one isolated partition after
two acknowledged records or deterministic environment seeding. An immediate
independent librdkafka query captures both low and high watermarks after the
public result, and the selected public offset must equal the corresponding
watermark without treating an adapter echo as broker truth. Timestamp selectors,
other broker-relative positions, and leader epochs are outside this slice.

Record deletion selects one explicit positive cutoff on a fresh independently
seeded partition. Ordered earliest and latest queries establish the precondition.
The packaged public result reports its low watermark, then a polling independent
query requires the low watermark to reach the cutoff while the high watermark
remains unchanged. Multiple targets and the high-watermark sentinel are outside
this first slice.

Topic-configuration description selects one exact key. Its wire command omits
the scenario's expected value, and the public nullable value must match an
immediate independent librdkafka query. Incremental alteration is restricted to
one exact `SET` replacement after a separately identified description proves a
different baseline. The public completion and a polling independent query must
then establish the requested value. Sensitive or unavailable values invalidate
the evidence instead of being converted into a definite result.

Topic creation, partition increase, and incremental topic-configuration
replacement carry an exact `validate_only` wire flag. Successful validation
uses a distinct correlated completion rather than the corresponding mutation
completion. Immediate non-polling independent metadata or configuration reads
must retain the exact pre-request state; partition and configuration validation
also require a separately observed exact baseline. A finite snapshot does not
claim that Kafka could never mutate later, so the scenarios add later public
description or real-mutation barriers that expose delayed effects.

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
an alteration additionally requires a different baseline value. Polling
independent reads after the completion must establish every requested offset or
explicit absence. One successful sibling cannot hide a failed or reordered
outcome.

`describe_classic_groups` carries ordered group IDs but keeps expected member
counts scenario-only. Its one correlated event retains ordered nullable member
counts and per-group errors. Each public result must match an immediate
independent group-existence and member-count fact. Classic membership is not
inferred from that broker fact: every counted live member must be an explicitly
declared classic consumer with a prior successful committed `group_receive` and
a positive classic group epoch.

Topic deletion is preceded by a public description of the exact
harness-provisioned topic. Independent metadata queries confirm the declared
partitions after description and boundedly poll for explicit absence after the
delete completion. Cluster description reports the public cluster identity and
broker IDs; the environment independently queries the same facts and owns the
expected broker count through its declared topology.

Consumer-group listing omits required group IDs from the adapter command and
requires the public result to contain the independently listed live groups
without broker-local errors. Consumer-group description likewise omits the
expected member count. Both independent queries run immediately after their
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

One `execute_transaction` command owns a complete linear begin, ordered send,
and commit-or-abort sequence because the public transaction token borrows its
producer until it ends. Each accepted record reports `transaction_staged`, then
one exact `transaction_completed` event reports the public disposition. The full
declared operation set must stage exactly once before that completion. The
independent observer uses `read_committed`: every committed operation must appear
exactly once with matching record bytes, public offset, and strictly increasing
caller order within a partition, while the complete aborted set must remain
absent. That absence does not assert whether an aborted record occupied a
physical log position. A later transaction on the same public producer may not
begin staging before the prior completion boundary.

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

One `fence_transaction` command keeps the original public transaction open
while it initializes a replacement producer with the same transactional ID.
It reports the staged record, replacement producer creation, and the normalized
old-commit result separately. The verifier requires `fenced` and independently
requires the staged record to remain absent under `read_committed` isolation.

Lifecycle commands target one exact public handle. Repeated readiness probes
and producer flushes retain distinct command identities. Closing one producer
does not close a sibling or prevent a distinct replacement producer on the
same live client; shutting down one client does not affect another client.
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
successful group receive reports the public membership epoch observed after its
assignment-fenced checkpoint commits. The verifier requires that epoch to be
positive and from the requested protocol family, preventing silent fallback to
classic membership.

Protocol v34 is an exact semantic contract. New capabilities may be declared
from the existing vocabulary, but adding or removing fields, changing meaning,
or narrowing accepted values requires a new protocol version.
