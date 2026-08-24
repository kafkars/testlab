# Adapter control protocol

## Transport

Protocol v14 is UTF-8 JSON Lines over stdin and stdout.

- One line is one complete JSON object.
- Adapter stdout is protocol-only; diagnostics use stderr.
- A line larger than 4 MiB is invalid before unbounded allocation.
- Every event repeats the protocol version and originating command ID.

## Handshake

Testctl sends `hello` with run ID, scenario ID, ordered environment endpoints, and a
non-secret security policy. TLS and SASL secrets are referenced by environment
variable name and passed only in the adapter process environment. The adapter
replies `ready` with implementation identity, version, and exact capabilities.

## Commands

- `hello`
- `create_client`
- `await_client_ready`
- `create_producer`
- `send`
- `send_batch`
- `create_assigned_consumer`
- `assign_beginning`
- `receive`
- `close_assigned_consumer`
- `create_group_consumer`
- `group_receive`
- `close_group_consumer`
- `create_topic`
- `create_partitions`
- `describe_topic`
- `list_topics`
- `list_offsets`
- `create_transactional_producer`
- `execute_transaction`
- `fence_transaction`
- `close_transactional_producer`
- `flush`
- `close_producer`
- `shutdown_client`
- `finish`

Model-broker controls and real-cluster broker restarts are owned directly by
testctl and do not cross the adapter boundary. A restart occurs between public
client commands while the same adapter process and client handles remain alive.

## Events

- `ready`
- `client_created`
- `client_ready`
- `producer_created`
- `operation_accepted`
- `operation_rejected`
- `operation_terminal`
- `batch_completed`
- `assigned_consumer_created`
- `assignment_completed`
- `receive_completed`
- `assigned_consumer_closed`
- `group_consumer_created`
- `group_receive_completed`
- `group_consumer_closed`
- `topic_created`
- `topic_partitions_created`
- `topic_described`
- `topics_listed`
- `offset_listed`
- `transactional_producer_created`
- `transaction_completed`
- `transaction_fence_completed`
- `transactional_producer_closed`
- `flush_completed`
- `producer_closed`
- `client_shutdown`
- `command_failed`
- `finished`
- `fatal`

A send emits one admission decision. Accepted operations later emit exactly one
terminal event. Rejected operations emit no terminal. A batch emits one
admission outcome per input operation, one terminal per accepted operation, and
then `batch_completed`. A batch contains at most 31 records so the complete
command remains within the bounded event budget.

A bounded receive polls the packaged consumer's public receive future and emits
the exact records it observed. An empty completion means no public record
arrived before the declared receive deadline; the verifier treats a missing
expected record as a client failure, not manufactured success.

A group receive additionally consumes the exact public batch into its
assignment-fenced checkpoint and attempts a bounded public commit. Its
completion reports both the exact records and whether that checkpoint committed;
the deterministic verifier requires both the expected record and a successful
commit.

Topic creation and partition expansion use the packaged client's public admin
handle and return one exact per-topic batch outcome. Admin-created scenario
topics are deliberately excluded from independent environment provisioning,
and broker auto-creation is disabled. A later independently observed producer
record proves a created topic was usable; a record on a newly added partition
proves that partition was usable without manufacturing an independently
observed exact final partition count.

Named topic description, all-topic listing, and offset listing also use the
packaged public admin handle. Their adapter commands omit the scenario's
expected partitions, required topics, and expected offset. A named description
must report the exact declared partition indices, each of which is later
exercised by an independently observed record. An all-topic listing preserves
the public byte-sorted unique order and must contain the declared required
topics, whose existence is likewise established by independent record
observations. These checks do not claim exhaustive topic
listing, internal-topic filtering, topic IDs, or replica topology.

The initial offset-listing slice selects `latest` for one isolated partition
after two acknowledged records and requires end offset 2. Independent record
observations at offsets 0 and 1 establish the claim without treating an adapter
echo as broker truth. Other offset positions, timestamps, and leader epochs are
outside this slice.

One `execute_transaction` command owns a complete linear begin, ordered send,
and commit-or-abort sequence because the public transaction token borrows its
producer until it ends. Each accepted record reports `transaction_staged`, then
one exact `transaction_completed` event reports the public disposition. The
independent observer uses `read_committed`: committed records must appear once,
while aborted records must remain absent.

One `fence_transaction` command keeps the original public transaction open
while it initializes a replacement producer with the same transactional ID.
It reports the staged record, replacement producer creation, and the normalized
old-commit result separately. The verifier requires `fenced` and independently
requires the staged record to remain absent under `read_committed` isolation.

## Failure behavior

A normal public client API failure emits one correlated `command_failed` event
and exits successfully. Testctl stops issuing dependent steps, retains an
independent broker snapshot, and produces valid failing evidence. An adapter or
protocol failure instead emits `fatal` and exits nonzero. A crash, malformed
stdout, wrong version, wrong command ID, or timeout invalidates the run.

## Evolution

Group creation explicitly selects classic or KIP-848 consumer membership. A
successful group receive reports the public membership epoch observed after its
assignment-fenced checkpoint commits. The verifier requires that epoch to be
positive and from the requested protocol family, preventing silent fallback to
classic membership.

Protocol v14 is an exact semantic contract. New capabilities may be declared
from the existing vocabulary, but adding or removing fields, changing meaning,
or narrowing accepted values requires a new protocol version.
