# Evidence model

Console output is for humans. Sealed evidence is the product.

A run is written to `<run-id>.partial` and renamed only after required files and
digests exist.

## Required artifacts

- `manifest.json`
- `scenario.json`
- `subject.json`
- `environment.json`
- `adapter.json` after a successful handshake
- `history.jsonl`
- `broker-observations.jsonl`
- `broker-state-observations.jsonl`
- `verdict.json`
- `summary.md`
- `reproduction.sh`
- `digests.json`

Evidence schema v65 records the exact environment identity in `manifest.json`,
retains protocol-v76 direct and hosted-group consumer controls, abandonment,
and shutdown,
consumer ownership observations, multi-member receive
completions, and concurrent actor boundaries, ordered protocol-adversary
controls and wire observations, and
independently selected broker-role disruption, broker-policy facts, and
network-proxy controls and effect observations.
Protocol v78 and record digest v2 retain an optional caller-selected timestamp
in commands, public producer terminals, public consumer records, and independent
broker observations.
Protocol v79 and evidence schema v68 retain every caller-ordered Share
subscription topic at creation. The two-topic scenario requires public
assignment coverage and one exact accepted acquisition from each topic, with
both records joined to independent broker observations.
Protocol v80 and evidence schema v69 retain the optional Share rack selected at
creation and the rack Kafka reports for every publicly described Share member.
Protocol v81 and evidence schema v70 retain all six optional classic membership
timings. Live single-broker and three-broker recovery scenarios set every value
through the public builder while exact records and assignments remain joined to
independent broker observations.
Protocol v82 and evidence schema v71 retain optional processing,
membership-start, seek, and close durations shared by classic and KIP-848 group
registrations. Both protocols use non-default values during public seek replay.
Protocol v83 and evidence schema v72 retain the complete optional Fetch and
retained-delivery capacity blocks for assigned, classic, and KIP-848 consumers.
All three run non-default policy while delivering exact public records joined
to independent broker observations.
Protocol v84 and evidence schema v73 retain all six Share Fetch fields plus
membership-start and close deadlines. Both bounded-delivery scenarios use
non-default policy and exact independently observed records.
Protocol v85 and evidence schema v74 retain an optional expected cluster ID at
client creation while the normalized mismatch oracle remains scenario-only.
The cluster scenario proves rejection and same-identity reuse before joining a
successful construction, repeated public readiness, public Admin description,
and independent librdkafka cluster metadata to the exact environment identity.
Protocol v86, scenario schema v89, and evidence schema v75 add an exact
transaction-fencing method to the adapter command.
Protocol v87, scenario schema v90, and evidence schema v76 add the bounded
single-partition Admin-abort terminal operation and retain its public producer
state before and after the mutation.
Protocol v88, scenario schema v91, and evidence schema v77 add reversible
broker-unregistration evidence: the public completion, exact immediate
independent remaining broker set, and same-cluster restoration after restart.
Protocol v89, scenario schema v92, and evidence schema v78 retain the exact
single-record producer method. PROD-016 requires a scenario-selected
`Producer::send` to remain distinct from default immediate `try_send` admission;
its ordinary terminal and independent-record evidence still applies.
Protocol v90, scenario schema v93, and evidence schema v79 carry the same method
selection through producer cancellation. PROD-017 prevents `Delivery::cancel`
and `Send::cancel` from being substituted for one another in retained history.
Protocol v91, scenario schema v94, and evidence schema v80 retain the selected
direct-consumer batch observer. CONS-015 distinguishes immediate
`try_take_batch` from waiting `recv` without sending the expected record to the
adapter.
Protocol v92, scenario schema v95, and evidence schema v81 add direct-consumer
failure events. CONS-016 binds each selected `next_event` or `try_take_event`
command to one ordered public completion with the exact consumer, target,
positive fence generations, terminal category, and broker code while keeping
the expected failure outside the adapter.
Protocol v93, scenario schema v96, and evidence schema v82 add the fail-closed
group missing-offset policy. CONS-017 binds each expected group receive failure
to one exact configured creation, one correlated command and public error, and
the absence of a successful receive. Classic and KIP-848 scenarios require the
normalized public `state` error from new groups rather than an inferred offset.
Protocol v94, scenario schema v97, and evidence schema v83 retain the selected
hosted group batch observer. CONS-018 distinguishes repeated immediate
`try_take_batch` from waiting `recv` for classic and KIP-848 consumers while
ordinary checkpoint, epoch, record, and independent broker evidence still
applies.
Protocol v95, scenario schema v98, and evidence schema v84 retain the selected
transaction record-staging method. TXN-009 binds each declared homogeneous
batch to one exact `send_batch` command while ordinary staging, disposition,
offset-order, record-fidelity, and independent read-committed evidence still
applies to both commit and abort.
Protocol v96, scenario schema v99, and evidence schema v85 add bounded group
processing acknowledgement. CONS-019 binds each classic or KIP-848 receive to
one exact renewal plan, then requires the same assignment-fenced batch to stay
live beyond its original processing window and commit against independent
broker truth.
Protocol v97, scenario schema v100, and evidence schema v86 add ordered partial
group checkpoints. CONS-020 binds each classic or KIP-848 receive to one exact
processed-prefix command, an independently observed prefix offset, and exact
replacement-member delivery of the previously unprocessed suffix.
Protocol v98, scenario schema v101, and evidence schema v87 add UUID-bound
transaction commits. TXN-010 joins caller-ordered public and independent topic
IDs to the exact validation-enabled transaction command and its matching fresh
commit seal; ordinary staging and read-committed record contracts still prove
the resulting record set.
Protocol v99, scenario schema v102, and evidence schema v88 add UUID-bound
ordinary sends and typed producer receipts. PROD-018 joins the prior public and
independent topic identity to the exact validation-enabled send, then preserves
the public topic, UUID, optional leader epoch, and nullable serialized sizes
alongside independently checked coordinates and timestamp.
Protocol v100, scenario schema v103, and evidence schema v89 add owned direct-
consumer record transfer. CONS-021 requires one exact transfer command, ordinary
destination admission and terminal, source evidence read again after that
terminal, and distinct independent source and destination records. A reserved
transfer-operation header gives the copy its own observation identity without
removing or replacing any source header.
Protocol v101, scenario schema v104, and evidence schema v90 add direct-consumer
Fetch evidence. CONS-022 binds the public batch's topic UUID, requested and next
offsets, log bounds, high watermark, retained-byte charge, and checkpoint to
prior independent topic-ID, watermark, and record observations.
Protocol v102, scenario schema v105, and evidence schema v91 retain the selected
Share batch conversion. SHARE-011 requires the exact command to distinguish
explicit `into_acknowledgement` decisions from `accept_all` while the existing
Share contracts retain the same terminal and independent record truth.
Protocol v103, scenario schema v106, and evidence schema v92 add singleton
timestamp offset selection. ADMIN-077 binds the exact public selector and
returned timestamp to an independently observed record and immediate partition
watermarks that rule out earliest- or latest-offset substitution.
Protocol v104, scenario schema v107, and evidence schema v93 add singleton
maximum-timestamp offset selection. ADMIN-078 binds the exact public selector,
offset, and returned timestamp to the unique independently observed record with
the greatest timestamp. A later lower-timestamp record and immediate watermarks
rule out earliest- or latest-offset substitution.
Protocol v105, scenario schema v108, and evidence schema v94 add direct
owned-record conversion. CONS-023 requires the exact `into_owned_records`
selection while retaining CONS-021's public destination terminal, post-terminal
source evidence, and distinct independent source and destination records.
Protocol v106, scenario schema v109, and evidence schema v95 retain the selected
hosted-group operation-configuration method. CONS-024 requires the exact
individual or aggregate selection and explicit durations in the issued creation
command. Classic seek exercises `operation_config`; KIP-848 seek retains the
individual setters under the existing control and lifecycle contracts.
Protocol v107, scenario schema v110, and evidence schema v96 retain the selected
full-batch group checkpoint conversion. CONS-025 requires one exact
`into_checkpoint` command while the ordinary committed record and independent
broker evidence remain unchanged; other group receives retain canonical
`checkpoint` coverage, and partial receives remain builder-only.
Protocol v108, scenario schema v111, and evidence schema v97 add legacy plural
configuration default restoration. ADMIN-079 keeps the expected broker default
scenario-side, requires an absent wire value and exact public restoration call,
and binds the completion to distinct prior and independently observed final
values for every selected topic through both legacy public surfaces.
Protocol v109, scenario schema v112, and evidence schema v98 separate each
non-Set configuration method and operation operand from independently expected
final state. ADMIN-080 exercises exact public Delete, Append, and Subtract
constructors through topic-specific and generic-resource surfaces; only the two
list operations carry operands across the adapter boundary.
Protocol v110, scenario schema v113, and evidence schema v99 retain the exact
public group-transition observer. CONS-026 binds every assignment observation's
selected `next_event` or `try_take_event` method across its ordered command and
completion; classic and KIP-848 ownership scenarios exercise both methods while
the existing assignment and independent broker contracts retain state truth.
Protocol v111, scenario schema v114, and evidence schema v100 retain exact
DescribeConfigs option selection and complete public entry metadata. ADMIN-081
binds synonym and documentation flags across topic-specific and generic-resource
builders, while immediate independent reads continue to establish value truth.
Protocol v112, scenario schema v115, and evidence schema v101 retain exact
group-listing filter intent. ADMIN-082 covers state and group-type filters on
both consumer-only and generic public builders plus the generic builder's
client-side protocol-type filter, while immediate independent group queries
continue to establish required live identities.
Protocol v113, scenario schema v116, and evidence schema v102 retain all four
transaction-listing selectors. ADMIN-083 covers exact state, signed producer-ID,
duration, and transactional-ID-pattern intent, and rejects vacuous results by
requiring strict narrowing from an earlier nonempty public baseline while an
immediate unfiltered Kafka CLI snapshot proves that full baseline is unchanged.
Protocol v114, scenario schema v117, and evidence schema v103 retain the exact
authorization-bitfield request and response across dedicated classic-group,
mixed classic/KIP-848 consumer-group, and singleton and plural Share-group
descriptions. Existing immediate CLI membership snapshots continue to anchor
each live group independently while the public result must retain the requested
option-specific metadata.
Protocol v115, scenario schema v118, and evidence schema v104 extend that exact
option-and-result treatment to caller-ordered topic descriptions by name and by
Kafka UUID. Immediate metadata and pinned topic-CLI snapshots continue to
independently anchor topic identity and topology; only the public result proves
that the requested authorization bitfield survived the client surface.
Protocol v116, scenario schema v119, and evidence schema v105 apply the same
separation to cluster description. The exact request and returned authorization
bitfield remain public evidence, while an immediate metadata snapshot remains
the independent authority for cluster identity and broker membership.
Protocol v117, scenario schema v120, and evidence schema v106 retain full
all-topic listing outcomes and the exact authorization-bitfield option. The
public result proves option-specific authorization metadata and error-aware
partition detail, while immediate metadata independently anchors every required
topic's exact topology; internal-topic filtering remains unclaimed.
Scenario schema v121 and evidence schema v107 retain protocol v117 while adding
explicit per-topic inclusion expectations. A classic group commit materializes
canonical `__consumer_offsets`, and immediate metadata proves it exists around
paired calls. The public call must omit it when internal topics are disabled,
then include it with the internal marker when enabled. This is a canonical
filter control, not exhaustive enumeration of every internal topic class.
Protocol v118, scenario schema v122, and evidence schema v108 retain the exact
fenced-broker request option and public broker markers. A reversible
three-broker control independently observes only brokers 1 and 2 after broker 3
stops, requires the disabled public result to exclude broker 3, requires the
enabled public result to include broker 3 with its fenced marker, and restores
the complete active cluster before shutdown. This proves one graceful-stop
fencing path; it does not claim every broker-registration transition.
Protocol v119, scenario schema v123, and evidence schema v109 add exact
`DescribeTopicPartitions` response limits and cursor-following intent. Each
public page retains its partition subset and returned continuation cursor; the
verifier requires the exact page chain and joins its sorted aggregate to the
immediate independent metadata topology. Every continuation is a separately
submitted public operation within the original command deadline.
Protocol v120, scenario schema v124, and evidence schema v110 add exact manual
topic replica-placement intent. ADMIN-084 binds each caller-ordered partition
index and broker list to the public manual-placement constructor, then reuses
immediate metadata evidence to require the exact complete partition set, each
replica order, a replica leader, and the complete replica set in sync on
three-broker release cells.
Protocol v121, scenario schema v125, and evidence schema v111 add exact manual
partition-expansion placement. ADMIN-085 binds each caller-ordered new-partition
broker list to the public assignment builder, keeps the exact prior count out of
the wire command, and requires the complete expanded partition set plus exact
replica order, a replica leader, and full ISR on three-broker release cells.
Protocol v122, scenario schema v126, and evidence schema v112 add validation-only
client-quota alteration. ADMIN-086 binds the exact proposed rate and validation
flag to a distinct public completion, keeps the required current rate out of the
wire command, and requires an immediate independent Kafka CLI observation of
that unchanged prior rate.
Protocol v123, scenario schema v127, and evidence schema v113 add an explicit
delegation-token expiration delay. ADMIN-087 preserves a selected zero-millisecond
delay on the wire and joins the public immediate expiration result to the same
sanitized independent owner-filtered absence required by ADMIN-073.
Protocol v124, scenario schema v128, and evidence schema v114 add explicit
singleton Admin offset read isolation. ADMIN-088 preserves a selected
read-uncommitted value on the wire and joins its exact earliest result to an
immediate independent watermark. A paired read-committed latest query retains
the compatibility path over the same committed records; unresolved-transaction
last-stable-offset differentiation remains unclaimed.
Protocol v125, scenario schema v129, and evidence schema v115 extend exact Admin
offset read isolation to caller-ordered batches. ADMIN-089 preserves one
read-uncommitted selection for the complete public call and joins every ordered
earliest or latest result to contiguous immediate independent watermarks over
committed records.
Protocol v126, scenario schema v130, and evidence schema v116 add exact
client-quota description strictness. ADMIN-090 preserves a non-strict named-user
filter on the wire and joins its result to an immediate independent Kafka CLI
value; the paired strict call proves both selections over one simple entity
without claiming composite-entity result divergence.
Protocol v127, scenario schema v131, and evidence schema v117 add exact
singleton consumer-group authorization selection. ADMIN-091 preserves an
included bitfield request on the wire and requires its public presence beside
the exact member count; the paired excluded call and immediate group queries
prove both selections over unchanged live membership.
Protocol v128, scenario schema v132, and evidence schema v118 add exact
Streams-group description and offset option selection. ADMIN-092 preserves
false authorization, full-topology, and stable-offset flags across five public
builder calls while the paired ADMIN-074 lifecycle retains all three true.
Protocol v129, scenario schema v133, and evidence schema v119 add exact
active-producer broker routing. ADMIN-093 preserves broker ID 1 on the wire in
declared single-broker cells and retains ADMIN-051's immediate exact Kafka CLI
producer-state comparison; automatic leader routing remains separately covered.
Protocol v130, scenario schema v134, and evidence schema v120 add configured
topic creation. ADMIN-094 preserves caller-ordered configuration entries on the
wire and retains ADMIN-001's immediate topology proof, then requires ordered
public descriptions and immediate independent librdkafka values for every
selected non-sensitive entry.
Every effectful environment terminal operation carries a stable identity in
`history.jsonl`; retained stdout and stderr are named by that operation.
Docker environments pull and then inspect the declared digest as separate
operations before Compose receives the image reference. Scenario-owned broker
restarts and their Kafka readiness probes are recorded as distinct operations
without stopping the packaged adapter process.
If a broker process exits before initial readiness, Testlab retains its failed
readiness, process state, and logs, then permits one deadline-bounded start of
the same container. A repeated exit makes the run invalid.
If Docker loses a reserved loopback port before Compose owns it, Testlab retains
the failed start, removes its partial project, assigns a fresh reserved port
set, and permits one deadline-bounded Compose retry. A second collision makes
the run invalid.
Provisioning waits for every harness-created scenario partition to report a
leader and the topology's full in-sync replica count before starting the
packaged client.
Real-Kafka runs also record one `broker_observe` operation. Its librdkafka
snapshot targets only record-bearing adapter commands actually issued in the
recorded harness history; an issued concurrent actor group, batch, transaction,
fencing, or owned-record transfer command
contributes every contained operation. The snapshot uses broker watermarks and
emits structured observations with exact partition, offset, timestamp, key,
value, and ordered header bytes.

The record comparison preserves byte-level distinctions: null and empty keys or
values are different, a tombstone has a null value, and duplicate headers retain
their original order with null, empty, text, or binary values. PROD-010 binds an
acknowledged public terminal offset to its one exact independent observation and
forbids offsets on uncertain or definitely-unsent terminals. PROD-013 requires
an explicit scenario timestamp to equal both the public producer receipt and
the independent Kafka record. Digest v2 covers the observed timestamp; when a
scenario omits one, only that field remains unconstrained so existing records
may retain their client-generated time. CONS-012 directly binds assigned,
ordinary group, and every group receive-set record, including records from
multi-topic subscriptions, to the
independent topic, partition, offset, timestamp, key, value, and ordered headers.
SHARE-006 applies that same independent comparison to each exact Share
acquisition, including every topic in a multi-topic subscription, without
replacing delivery-count or membership-fence checks.
SHARE-007 binds a multi-record acquisition to the complete caller-declared
ordered record set. SHARE-008 retains the exact record-ordered public
disposition vector, and SHARE-009 requires released or dropped records to
return with increased delivery counts without accepting settled siblings as
substitutes. SHARE-010 requires the exact scenario-declared public acquisition
count for a configured Share receive while SHARE-006 still binds every record
in that batch to independent broker coordinates and bytes. SHARE-011 binds the
scenario-selected batch conversion to the exact adapter command, including the
public all-record Accept path.
CONS-021 separately binds an owned direct-consumer source record to its original
independent coordinates and bytes, then binds its transferred producer record
and receipt to a distinct destination observation. The completion must follow
the destination terminal and expose the unchanged retained source record.
CONS-022 separately requires one selected direct receive to retain complete
broker-correlated Fetch evidence. Its UUID must match the pinned CLI topic
identity; its requested, progress, log-start, last-stable, and high-watermark
offsets must match the prior independent watermark window and record; its
checkpoint must equal Fetch progress; and its retained-byte charge must be
positive.
CONS-023 applies the complete CONS-021 transfer proof to the exact direct
`RecordBatch::into_owned_records` command selection. It cannot be satisfied by
the separately retained `RecordBatch::into_owned` then
`OwnedConsumerBatch::into_records` path.
PROD-011 requires strictly increasing independent offsets for sequential sends
and caller-ordered batch records targeting one partition; concurrent actor
declarations do not claim an execution order.
PROD-014 binds the partition returned by every successful public producer
receipt to its independent Kafka observation and forbids partition claims on
failed or uncertain terminals. PROD-015 additionally proves that a keyed send
crossed protocol v78 as `java_keyed`, that the adapter selected the public
automatic path, and that both receipt and broker record match the independent
positive-Murmur2 oracle over the serialized key and declared logical partition
count. The scenario's expected partition is retained for provisioning and
observation but is not passed to Kafkars as an explicit partition.
PROD-016 additionally requires the issued command to preserve a selected
`Producer::send`, its producer, partitioning mode, and complete record. The
packaged adapter invokes that exact bounded FIFO waiting method; the normal
receipt and independent Kafka observation remain governed by PROD-001 through
PROD-010 and PROD-014.

Configured-client history retains the complete requested public producer
policy under its stable command identity, and lifecycle evidence requires the
correlated client creation. Each codec scenario then proves exact public
delivery and independent broker-visible bytes. The independent observer reads
Kafka records after broker decoding; it does not inspect Produce request frames
and therefore does not independently claim which compression codec was used on
the wire. Public builder-selection tests establish the adapter mapping without
turning that adapter fact into broker truth.

Configured assigned-consumer history retains the requested read isolation and
its correlated client creation. The read-committed scenario assigns at the
beginning after an aborted transaction and requires the public receive to
contain only the nontransactional sentinel. CONS-002 enforces that exact
receive, CONS-012 binds it to independent broker coordinates and bytes, and
TXN-002 independently requires the aborted operation to remain absent. The
public builder-selection test establishes the adapter mapping without treating
that adapter fact as broker truth.

PROD-012 retains two ordered public cancellation outcomes and the same
immediate-delivery or waiting-send observer's authoritative terminal. It
enforces stage monotonicity, requires `cancelled_not_sent` to agree with
definitely-not-sent `cancelled` truth, and leaves `too_late` broker visibility
uncertain until ordinary terminal and independent observation contracts resolve
it. PROD-017 separately requires one exact issued cancellation command retaining
the scenario-selected observer type, producer, record, and timeout.

PROD-018 requires each selected ordinary send's public and independent nonzero
topic ID evidence to precede one exact `validate_topic_uuid` command. Its sole
acknowledged terminal must preserve that ID and topic, a nonnegative leader
epoch when Kafka supplies one, and exact serialized key/value lengths with
`None` distinct from `Some(0)`. The existing independent record contracts prove
the same partition, offset, timestamp, and bytes; receipt metadata alone cannot
manufacture broker truth.

Client metrics history retains the expectation-free command and one exact
correlated public snapshot. METRICS-001 binds that snapshot to its client and
operation identities. METRICS-002 checks cumulative call and failure ordering,
mailbox bounds, latency summaries, producer throughput, and request-concurrency
relationships. METRICS-003 checks the scenario-owned produced-record floor and
required idle, accepting, and healthy state. These counters are client-reported
operational facts, not broker truth; the scenario's record remains subject to
the ordinary independent broker visibility, byte, and offset contracts.

Direct-consumer scenarios retain every assignment, operation-identified
replacement, incremental add/remove, seek, pause/resume, receive, and completion
under its originating command identity. CONS-013 requires one exact issued
control and one matching public completion. CONS-015 separately requires an
immediate-batch scenario to retain the exact consumer, method, receive identity,
and timeout in its issued command; the ordinary receive and independent-record
contracts still establish the returned batch truth. CONS-016 binds each direct
failure-event observer to its exact command and public fence, kind, and broker
code; independently recorded ACL state establishes the policy transition rather
than trusting that client event as broker truth. Successive receives are
joined to their declared independently observed records in order. Exact offset
and end starts, seek replay, paused-partition isolation, and survivor cursors
after incremental mutation are therefore broker-backed outcomes rather than adapter
success claims. Two direct consumers may still independently expose the same
coordinate. LIFE-003 and LIFE-009 evaluate repeated flushes and legacy
assignments per command rather than by aggregate resource counts.
The configured read-committed scenario also retains non-default public Fetch
and capacity policy, then joins its visible sentinel to independent broker and
aborted-transaction evidence; configuration alone cannot pass.

Configured-group history retains the requested missing-offset and read-isolation
policy plus any Fetch and retained-delivery policy, processing,
membership-start, seek, and close durations, their selected individual or
aggregate public builder surface, static
group-instance identity, classic assignor, and all six classic
membership-timing values in the issued protocol command. That
adapter-reported configuration is not the verdict: exact group receives must
still join to independently observed records, positive protocol epochs, and
aborted-transaction visibility evidence. A dedicated three-broker recovery
scenario runs with every classic timing set to a non-default value.
Classic and KIP-848 seek scenarios run with every shared runtime duration set
to a non-default value and with non-default Fetch and capacity policy. Classic
selects the aggregate public operation configuration while KIP-848 selects the
individual setters, then both exercise public membership, Fetch, seek, and close.
The issued group-create command also retains the complete caller-ordered
subscription, while assignment and record evidence prove that every declared
topic reached the public consumer.
ADMIN-068 additionally requires each public consumer owner to be abandoned,
its owning client to shut down without a group close, and both static identities
to remain broker-registered inside the configured session window until the
exact removal command.

Configured-Share history retains the complete requested long-poll, byte,
record, acquisition-range, attempt-timeout, membership-start, and close policy
in the issued create command. Its acknowledgement command also retains whether
the public batch used explicit decisions or `accept_all`. The adapter reports
only the public batch's acquisition count, records, delivery counts, and
membership fences. SHARE-010 checks the scenario-owned acquisition expectation,
SHARE-011 checks its exact conversion, and the ordinary Share record contracts
independently check the delivered broker records.

Lifecycle-isolation scenarios retain every readiness, flush, close, and
shutdown completion under its originating command. A later send on a sibling,
replacement, or independently owned producer must still satisfy the ordinary
admission, terminal, coordinate, and broker-visibility contracts; a lifecycle
success string alone cannot establish handle isolation.
Two independently owned assigned consumers must each expose the same declared
broker records in order through their own public receive stream. Shared
operation IDs, aggregate counts, or one consumer's observations cannot
substitute for the other consumer's exact coordinates and bytes.

Transaction evidence keeps public staging, public disposition, and independent
visibility separate. TXN-004 requires every declared member to have one exact
accepted-to-`transaction_staged` history before its completion. TXN-002 treats
visibility as a complete set: every committed member appears exactly once under
the observer's `read_committed` isolation and every aborted member is absent.
TXN-005 then binds committed members to the declared bytes, public and
independent topic-partition-offset coordinates, and caller order within each
partition. TXN-006 prevents staging for a successive transaction on one public
producer from crossing the prior completion. No aborted-record contract infers
a physical append from public staging metadata or read-committed absence.
TXN-003 requires the old active transaction to report an explicit broker fence
and its staged record to remain absent from independent read-committed
observation. Separate scenarios use replacement initialization and singleton
public Admin force termination; in the Admin case no replacement exists until
after the old commit result is obtained. Each scenario additionally requires a
later replacement transaction to commit normally under TXN-001 and TXN-002.
TXN-007 binds each consume-transform-produce command to one exact independently
observed input record, positive classic or KIP-848 membership fence, and the
public assignment-fenced next-offset checkpoint. TXN-008 requires a committed
checkpoint to match an immediate public Admin result and independent broker
query; abort is established by closing the member and receiving that exact
input again from the unchanged group position.
TXN-010 requires the referenced public topic-ID result and its independent
broker observations to precede one exact validation-enabled transaction
command. Their nonzero IDs must remain unique and caller-ordered through the
adapter's committed validation result. The record set separately remains
subject to TXN-004, TXN-005, and independent read-committed observation.

Targeted broker-role scenarios retain two exact `broker_role_observe`
operations around each disruption: one owner immediately before the stop and a
distinct elected owner while the original service remains offline. Partition
leaders are discovered through librdkafka metadata. Controllers and classic,
KIP-848, or transaction coordinators are discovered through bounded Kafka
Metadata or FindCoordinator requests to declared plaintext endpoints. Each
observation records the typed role target, phase, broker node, and Compose
service. The environment then records exact stop, start, and readiness
operations for that original service. These facts come from the environment,
not the adapter, and failure to discover a unique eligible owner invalidates
the run.

Broker-policy scenarios retain an ordered `broker_policy_alter` terminal, a
separate `broker_policy_query` terminal, and one synthetic
`broker_policy_observe` fact for each apply or removal. The fact names the exact
non-secret principal, literal resource and operation, or quota direction and
rate. It is emitted only after a fail-closed parser confirms the exact query
state. CLI failure, unknown query output, a state mismatch, missing cleanup, or
an unpaired transition invalidates execution rather than becoming a packaged
client result.

Earliest- and latest-offset Admin claims use immediate independent librdkafka
watermark queries. Timestamp-offset claims additionally require one exact
independent record at the selected offset and timestamp, plus an earlier record
inside the same watermark bounds. Singleton and caller-ordered plural record
deletion retain
exact pre- and post-operation low and high watermarks for every partition,
proving that each explicit prefix or high-watermark selection became unavailable
without accepting an adapter echo as broker truth. Topic creation, expansion,
description, and listing use immediate
metadata facts. An ordered batch creation retains one command and one completion
with one caller-ordered public outcome per requested topic, followed by immediate
independent metadata observations for those topics. Expected per-resource error
codes remain scenario-only and are not part of the adapter command. The evidence
does not imply exhaustive topic discovery, internal-topic classification,
replica topology, or untested offset selectors.

`broker-state-observations.jsonl` retains independently queried broker state
that is not a record snapshot. It includes exact topic metadata, one selected
non-sensitive topic-configuration value, cluster identity and broker IDs,
cluster feature ranges, active partition producer states, canonical cluster
transaction listings, caller-ordered transaction descriptions, and post-fence
producer identities,
consumer-group existence and member count, active Share-group state and member
count, caller-ordered selected Share-group partition start offsets and lags or
explicit absence, one consumer-group committed offset,
exact partition low and high watermarks, exact literal wildcard-host ACL
presence, exact named-user byte-rate quota state, and exact non-secret named-user
SCRAM mechanism and iteration state or absence. Each query runs
immediately after its exact correlated public admin command while later
scenario steps are paused. A state query is never emitted for a public command
that was not actually issued.

After a topic-configuration mutation, the observer waits for the selected value
on every broker in the environment's exact metadata topology, using broker-targeted
DescribeConfigs requests and the original observation deadline. This prevents a
later public read from racing a lagging broker's configuration update. The sealed
value is independently observed; query and validate-only observations remain
non-polling snapshots and retain mismatches.

Protocol-v36 plural group-offset, batch offset, and classic-group operations
retain the same broker-state fact shapes in scenario schema v50. Plural offset
operations retain one existing `ConsumerGroupOffset` observation per selected
key, with contiguous observation ordinals in caller-flattened order. Classic
batch descriptions retain one existing `ConsumerGroupState` observation per
requested group in caller order. Listings use immediate non-polling reads;
mutations poll for the declared offset or explicit absence.

The state-query consumer never joins the target group, subscribes, assigns, or
commits. Topic and group absence remain explicit typed facts. Observer errors,
authorization failures, and timeouts invalidate the run rather than manufacture
a client result. ADMIN-003 accepts only the explicitly selected metadata-backed
or `DescribeTopicPartitions` public command, requires exact public page and
cursor evidence when pagination is selected, and joins its aggregate partition
set to an immediate independent metadata snapshot. ADMIN-006 through ADMIN-016 compare
public results and temporal mutations with these independently observed facts.
ADMIN-017 additionally
requires a distinct pre-deletion watermark baseline and an unchanged high
watermark. ADMIN-018 binds each caller-ordered batch result to its independently
observed topic state, including an exact expected duplicate-topic code without
turning the successful sibling result into a failure. ADMIN-014 binds an exact
correlated topic-already-exists public failure to an unchanged topic snapshot;
an unrelated or differently coded failure cannot satisfy it. ADMIN-019 applies
the same correlation and no-success rules to exact unknown-topic broker failures
during partition creation, deletion, and description, and to an exact public
routing failure for a selected-offset request whose partition is independently
proven absent. ADMIN-020 through ADMIN-022 require distinct
validate-only completions, forbid the corresponding mutation completions, and
join immediate non-polling independent reads to unchanged topic existence,
partition topology, or selected configuration state. Partition and
configuration validation also retain a distinct independently observed exact
baseline. ADMIN-023 requires one ordered single-group multi-partition public
listing with no per-resource errors and matching immediate independent offset
facts for every selected key. ADMIN-024 applies the same rule to ordered groups
and their ordered nested selections, including no group or per-resource errors
and the exact flattened independent observation order.
ADMIN-025 requires one ordered plural alteration completion with no per-resource
error, one distinct corroborated different baseline per key, and polling
independent post-state at every requested offset. ADMIN-026 requires the same
ordered completion and one present corroborated baseline per key, then polling
independent explicit absence for every deletion. ADMIN-027 joins one ordered
classic-group public description with its exact authorization-bitfield option
to immediate independent existence and member counts with no group errors,
requires the bitfield on every success, and requires every counted live member
to have a prior committed receive with a positive classic epoch; broker
membership facts alone do not establish classicness. ADMIN-028 binds one caller-ordered public
batch offset result to contiguous immediate watermark observations for every
unique topic-partition selection and requires each selected earliest or latest
offset exactly. ADMIN-029 applies the generic public group listing to the same
independent group facts without narrowing the result to consumer groups.
ADMIN-010 retains the singleton excluded-authorization description and exact
live member count. ADMIN-091 applies the same public and immediate independent
membership proof to the paired included-authorization request and additionally
requires the raw public bitfield to be present.
ADMIN-082 requires exact group-listing filter intent on the wire. Both public
listing paths retain caller-ordered state and group-type filters; the generic
path additionally retains its client-side protocol-type filter. Each filtered
result must still contain every scenario-selected group that an immediate
independent query proves live, with no broker-local errors.
ADMIN-030 through ADMIN-032 bind caller-ordered public ACL creation, one exact
public description, and caller-ordered exact deletion matches to contiguous
Kafka-CLI observations of presence or absence after each terminal. Public and
independent ACL facts remain separate; neither stream can substitute for the
other. ADMIN-033 and ADMIN-034 bind exact named-user byte-rate description,
replacement, and removal terminals to immediate Kafka-CLI observations of the
same user, key, and whole-number value or explicit absence. ADMIN-086 separately
binds the proposed validation-only alteration and its distinct public terminal
to an immediate Kafka-CLI observation proving the exact prior rate did not
change. ADMIN-090 separately binds an exact non-strict filter selection to the
same public and independent named-user value used by the paired strict call.
The fixture contains no composite quota entity, so it proves option preservation
without asserting that the two result sets differ. Public and independent
client-quota facts remain separate. ADMIN-035 and ADMIN-036 bind
exact named-user SCRAM-SHA-256/512 description, upsert, and deletion terminals
to immediate Kafka-CLI observations of the same non-secret mechanism,
iterations, or absence. The password exists only in the adapter process
environment and never enters protocol or evidence. Public and independent
SCRAM facts remain separate. ADMIN-015 and ADMIN-016 retain selected
topic-configuration values and require a distinct independent pre-mutation
baseline. In particular, mutation baselines use distinct preceding list or
describe operation IDs, so history order preserves precondition and
postcondition meaning without trusting an adapter echo.

ADMIN-037 binds one active public Share-group description to the exact requested
authorization-bitfield option and expected state, epochs, assignor, member rack,
subscription, nonzero topic identity, and partition assignment. A separate
immediate Kafka CLI state query must agree on
the group, stable state, and member count. The coarse independent snapshot
cannot manufacture the detailed public assignment, and the public completion
cannot manufacture broker-visible membership.

ADMIN-038 binds one selected public Share-group offset outcome to its exact
group, topic, partition, nonzero topic identity, start offset, leader epoch,
and lag. A separate immediate Kafka CLI offsets query must agree on the group,
topic-partition, start offset, and lag; neither side substitutes for the other.

ADMIN-039 requires a distinct ADMIN-038 baseline, successful public closure of
every modeled member before the alteration command, one exact successful public
partition outcome with a nonzero topic ID, and an immediate independent Kafka
CLI post-state at the requested start offset and scenario-owned resulting lag.
The requested offset crosses the adapter boundary; the expected lag does not.

ADMIN-040 requires a distinct ADMIN-038 baseline, successful public closure of
every modeled member before the deletion command, one exact successful public
topic outcome with a nonzero topic ID, and an immediate independent Kafka CLI
observation with no start offset or lag for the scenario-selected partition.
The selected partition stays out of the topic-wide public deletion command.

ADMIN-041 requires successful public closure of every modeled member in every
selected Share group, one exact public deletion outcome per group in caller
order, and one immediate read-only Kafka CLI list snapshot. The normalized
independent observations retain consecutive ordinals in caller order and must
report every selected group absent; unrelated listed groups remain irrelevant.

ADMIN-042 binds one caller-ordered public batch and exact requested
authorization-bitfield option to a complete detailed description, including
exact configured rack identity, for every selected
active Share group. Each modeled member must retain an exact acquired public
batch with positive member and assignment fences before the admin command.
Separate immediate read-only Kafka CLI state queries retain consecutive history
and observation order and must agree on every group, stable state, and member
count.

ADMIN-043 binds one caller-ordered public batch to every selected Share-group
and topic-partition outcome, including nonzero topic identity, start offset,
leader epoch, lag, and absence of group-level or partition-level errors.
Separate immediate read-only Kafka CLI offset queries run once per group; their
normalized facts retain consecutive history and observation order and must
agree on every selected start offset and lag.

ADMIN-044 binds one caller-ordered public topic-description batch and its exact
authorization-bitfield option to each full successful description or exact
missing-topic error. Successful descriptions retain a nonzero topic identity,
the internal marker, the requested authorization bitfield, and every ordered
partition without hidden errors. Immediate metadata facts retain consecutive
history and observation order and must prove each exact partition topology or
topic absence before the next command.

ADMIN-045 requires a prior caller-ordered plural description whose independent
metadata proves each selected name present with its exact topology or absent as
declared. One public name-based deletion then preserves every success or exact
missing-topic error in that order. Immediate independent metadata polling must
settle with every selected name absent; its normalized facts retain consecutive
history and observation order before the next command.

ADMIN-046 requires a prior caller-ordered classic-group description whose
independent snapshot proves every selected group exists with zero members. One
public consumer-group batch deletion must then return one successful outcome per
group in that order. Immediate independent group polling must settle with every
selected group absent; the final facts retain consecutive history and
observation order before the next command.

ADMIN-047 requires caller-ordered earliest and latest baselines for every fresh
record-deletion target. One public batch must preserve the exact target order,
explicit offset or high-watermark selection, and successful low watermark.
Immediate independent polling must retain consecutive facts in that same order,
with each low watermark at the selected boundary and each high watermark
unchanged before the next command.

ADMIN-048 binds one caller-ordered public selected-configuration batch to every
topic, key, non-sensitive value, and absence of a per-topic error. Immediate
independent configuration facts retain consecutive history and observation
order and must confirm every exact selected value before the next command.

ADMIN-049 binds one caller-ordered public incremental configuration mutation to
an exact named ADMIN-048 baseline with a distinct prior value for every selected
topic and key and no intervening mutation. Every public per-topic outcome must
succeed in caller order, and consecutive immediate independent polling must
confirm every exact replacement before the next command.

ADMIN-050 binds one public feature description to an immediate pinned Kafka CLI
snapshot. Supported ranges must match every CLI row visible through the
negotiated response; an incomplete older response may omit only independently
reported minimum-level-zero features. Every nonzero CLI finalized maximum and
the shared finalized epoch must match the public result. Symbolic metadata
versions use Testlab's pinned Apache Kafka stable-level map. Canonical ordering,
range coherence, and the scenario-declared migration flag remain explicit.

ADMIN-051 binds one public active-producer description to an immediate pinned
Kafka CLI snapshot for the exact topic-partition. The public result must contain
the scenario-declared nonzero producer count in canonical producer-ID order,
and every producer ID, epoch, last sequence, last timestamp, coordinator epoch,
and optional current-transaction start offset must match exactly. The fixture
closes its producer after an acknowledged send so the compared broker state is
not changing between snapshots.
ADMIN-093 applies the same producer-state equality to a distinct request whose
command preserves broker ID 1 and whose public builder selects that exact
broker. The scenario is admitted only to fixed broker-one single-broker packs;
multi-broker packs retain automatic leader routing without guessing ownership.

ADMIN-052 binds one unfiltered public transaction listing to one immediate
pinned Kafka CLI snapshot. The fixture initializes and closes every modeled
transactional producer first. Public rows must be canonical by transactional
ID, contain exactly the scenario-declared identities and states, preserve each
nonnegative producer ID, and exactly equal the independently parsed rows; any
unknown filter or broker error prevents the public completion.

ADMIN-083 binds each filtered public transaction listing to its exact wire
selector and an earlier unfiltered public baseline from the same client. The
baseline must be nonempty and precede the filtered command. The filtered result
must be canonical, exactly match the scenario-selected identities and states,
preserve each producer ID from an immediate unfiltered Kafka CLI snapshot, and
strictly narrow that snapshot. The same snapshot must still equal the complete
baseline, preventing an empty or partial fixture from masquerading as filter
coverage. State, producer-ID, and duration paths run across their compatible
matrix; transactional-ID regular expressions remain on Kafka 4.3 gating cells.

ADMIN-053 binds one caller-ordered public description batch to one immediate
pinned Kafka CLI description per selected transactional ID. Public and
independent rows must agree exactly on state, timeout, start-time presence and
value, producer identity and epoch, and canonical topic-partition membership.
CLI observations retain contiguous history and observation order. Every
selected producer is initialized and closed before the read, keeping the
compared `Empty` fixture state stable between public and independent snapshots.

ADMIN-054 binds one selected-partition public log-directory description to one
immediate pinned `kafka-log-dirs.sh --describe` JSON snapshot. The adapter first
discovers the exact broker set, submits it to the public API in descending order,
and must receive that order unchanged. Public and independent results must agree
on every broker, path, replica size, offset lag, and future marker after the
independent snapshot is canonicalized by broker ID. Public throttle and optional
volume capacity or cordon fields remain explicit and must be internally valid.
Scenario validation ties the selected partition and expected current-replica
count to a prior successful topic creation, and the closed-producer fixture makes
the compared replica log stable between snapshots.

ADMIN-055 binds one caller-ordered public selected-replica log-directory
description to one immediate pinned `kafka-log-dirs.sh --describe` JSON
snapshot. The adapter queries the same topic-partition identity on every
publicly discovered broker in descending broker order, including brokers where
that replica is absent. The verifier requires the public order, current or
absent path, and exact signed lag to match canonical CLI state for every broker;
the scenario-owned closed-producer fixture requires its declared current
replica count and rejects any transient future placement.

ADMIN-056 binds one public metadata-quorum description to immediate status and
replication snapshots from Kafka's pinned quorum CLI. The observer normalizes
legacy ID-only and directory-aware layouts. Both CLI views must agree on
canonical voter and observer identities, roles, represented directory IDs,
leader state, watermark, offsets, and advertised controller endpoints before
one independent state observation is sealed. The public result must match
membership, leader, epoch, represented directories, and optional v2 listeners
exactly. Known offsets, watermark, and timestamps may only advance before the
CLI snapshot; the later snapshot may resolve an earlier unknown value but may
not lose one.

ADMIN-057 binds one caller-ordered public producer-fencing batch to one
immediate pinned Kafka CLI transaction description per selected ID. Every
successful public outcome must retain its exact caller position and a
nonnegative producer ID and epoch. Contiguous CLI observations must match those
post-fence identities exactly and independently confirm each scenario-declared
stable transaction state, start-time presence, and topic-partition set. The
broker-reported transaction timeout must be positive and no greater than the
complete public deadline; it is deliberately not compared with the original
producer timeout because fencing derives it from the remaining operation
budget. Every selected producer is initialized and closed before fencing, so
no fixture owner can race the public result and independent snapshots.

ADMIN-058 binds one caller-ordered public partition-reassignment mutation to
one immediate independent metadata observation. Every public per-partition
outcome must retain its caller position and succeed. The observer polls within
the original action window until each target exposes the exact requested
ordered replica list, full ISR as a canonical broker set, and a live leader
inside that set. The three-broker fixture deliberately changes replication
factor, so request policy and broker mutation are both exercised.

ADMIN-084 binds one successful manually placed topic creation to one immediate
independent metadata observation. The scenario and wire retain every contiguous
partition index and caller-ordered broker list; the declared partition count and
replication factor must agree with that placement. The observer polls within the
original action window until metadata exposes exactly the requested topic
partition set and every partition has the exact replica order, full ISR as a
canonical broker set, and a live leader inside its replicas.

ADMIN-094 binds one successful automatically placed topic creation to exact
caller-ordered configuration entries on its wire command. The ordinary immediate
metadata snapshot must first prove the complete topic topology. Each selected
entry then requires a later matching public topic-configuration description and
an immediate independent librdkafka query with the exact non-sensitive value, in
caller order; an unrelated configuration read cannot satisfy creation evidence.

ADMIN-085 binds one successful manually placed partition expansion to one
immediate independent metadata observation. The scenario and wire retain one
caller-ordered broker list for each newly added partition, while the scenario-only
prior count maps those lists to exact partition indices and must match the modeled
topic state. The observer polls within the original action window until metadata
exposes exactly the complete expanded partition set and each new partition has
the requested replica order, full ISR as a canonical broker set, and a live
leader inside its replicas.

ADMIN-059 binds selected and all-active public partition-reassignment listings
to immediate pinned `kafka-reassign-partitions.sh --list` snapshots. Public
selected rows follow caller order with inactive selections omitted; all-active
rows and CLI state use canonical topic-byte and partition order. The stable
fixture lists only after ADMIN-058 has independently converged, requiring both
public paths and the exact Kafka 4.3.1 CLI empty result to agree that no active
movement remains.

ADMIN-060 binds selected and cluster-wide public preferred elections to exact
broker-role transitions and immediate metadata. Each public path operates on a
separate three-replica partition whose independently observed original leader
is stopped, replaced, exercised by an acknowledged produce, restored into the
full ISR, and still observed as a follower before the election command. Public
selected outcomes preserve caller order and cluster-wide outcomes use canonical
topic-byte and partition order while containing the required fixture partition.
The immediate metadata observation must show the assignment's first replica as
leader and the complete replica set in the ISR.

ADMIN-061 uses scenario-owned names only to resolve the broker-assigned topic
UUIDs under the same public deadline. The target public call is
`describe_topics_by_id` with the exact authorization-bitfield option: every
caller-positioned result must retain its exact nonzero request UUID, repeat that
UUID inside the successful description, retain the requested authorization
bitfield, and preserve the expected partition topology without hidden errors.
One pinned Kafka topic-CLI query per name immediately supplies independent UUID
and partition facts in the same contiguous caller order.

ADMIN-062 requires a prior ADMIN-061 description over the same ordered topics.
The independently observed UUIDs become the deletion baseline, so a topic
deleted and recreated under the same name cannot satisfy the contract. The
target public `delete_topics_by_id` call must return those exact UUID keys in
caller order with no per-topic errors, after which independent metadata polling
must prove every selected name absent.

ADMIN-063 binds one topic-filtered public `list_config_resources` result to its
exact command and immediate independent metadata. The result must retain a
deadline-bounded nonnegative throttle, contain only type-2 topic identities in
strict type-code then name order, and include every scenario-required topic.
The scenario first creates dynamic configurations because Kafka lists resources
with non-default configuration properties; each required topic must also appear
as a contiguous independently present metadata fact before the next command.

ADMIN-064 applies the ADMIN-048 caller-order, selected-key, value, timing, and
independent-read requirements to the resource-generic public description path.
Its exact wire command must select `resource`, preventing a topic-convenience
call from satisfying the generic-resource claim.

ADMIN-065 applies the ADMIN-049 named-baseline, distinct-transition,
caller-order, timing, and independent-polling requirements to the
resource-generic incremental-alter path. Both the baseline and mutation must
select `resource`; mixed API paths cannot satisfy the contract.

ADMIN-066 and ADMIN-067 apply the same named-baseline, distinct-transition,
caller-order, timing, and independent-polling requirements to the public legacy
full-snapshot topic and generic-resource replacement paths. Their mutation
commands must select `legacy_topic` or `legacy_resource`, while their baselines
must use the matching `topic` or `resource` description surface. An incremental
completion cannot satisfy either legacy contract.

ADMIN-079 applies those plural legacy requirements to default restoration
through both topic-specific and generic-resource public surfaces. The scenario
retains each expected final value for independent comparison, but the exact
adapter command must omit every replacement value. The selected public legacy
restoration constructor must therefore establish each distinct final value
without receiving that expected broker state.

ADMIN-080 binds non-Set incremental configuration methods to their exact wire
method, allowed operand, caller order, named baseline, public completion, and
immediate independent post-state through both public surfaces. Delete carries
no value and must restore the effective default. Append and Subtract carry only
their exact list operand; the independently required combined or reduced value
never crosses the adapter boundary.

ADMIN-081 binds exact `include_synonyms` and `include_documentation` flags to
one preceding plural DescribeConfigs command and one completion through both
topic-specific and generic-resource surfaces. Every selected successful entry
must retain public source, mutability, sensitivity, configuration type, and
nonempty documentation. Requested synonyms must be nonempty and contain the
effective public value. Immediate independent configuration reads still prove
that value; metadata does not substitute for broker-state evidence.

ADMIN-068 binds configured classic static membership to one explicit Admin
removal. Two unique `group_instance_id` values first participate in a complete
public assignment. The adapter then abandons both public owners without a group
close and shuts down their owning clients. Inside their explicit classic
session window covering the scenario deadline, the named description baseline
and its immediate independent group query must still report both members,
distinguishing retained static registration from an ordinary dynamic leave.
The later wire command omits that
scenario-only baseline, retains the nonempty broker reason, and must return both
selected instance identities successfully in caller order with a throttle
bounded by the public deadline. One immediate independent group query must then
report the exact group with zero members.

ADMIN-069 binds one caller-ordered public description batch spanning classic
and KIP-848 consumer groups, including the exact requested authorization
bitfield on every success, to the scenario's live committed members. The
classic member explicitly selects cooperative-sticky before joining, and the
public result must preserve that selected assignor plus exact protocol variants,
state, epochs, member identities, subscriptions, typed assignments, raw classic
payloads, and per-group errors without receiving those expectations. Matching
positive membership epochs and contiguous immediate independent member-count
observations establish the broker-visible live groups in the same caller order.

ADMIN-070 binds each caller-ordered public replica log-directory alteration to
the exact requested topic, partition, broker, and target path. Every public
per-replica outcome must retain its caller position and succeed, with throttle
bounded by the operation deadline. Contiguous pinned `kafka-log-dirs.sh`
observations then poll until each selected replica has exactly one current
placement at its requested path and no future placement. The public completion
must precede that settled independent state and cannot be reconstructed from it.

ADMIN-071 binds the dedicated public client-metrics resource listing to its
exact API-selecting command and one immediate independent pinned CLI snapshot.
Testlab provisions two distinct named configurations before adapter startup;
the public result and CLI state must both equal that exact set as type-16
resources in strict UTF-8 byte order. Scenario-required names never cross the
adapter wire, and later CLI state cannot replace the public completion.

ADMIN-072 binds one public finalized-feature update to its exact caller-ordered
request and requires the validation-only builder selection. Every per-feature
outcome must succeed in caller order and the throttle must fit the operation
deadline. Exact pinned Kafka CLI feature rows and the finalized epoch must be
unchanged from the prior ADMIN-050 baseline to the immediate post-completion
snapshot. This contract is configured only for Kafka 4.3.1 cells.

ADMIN-073 binds one authenticated public Admin command to four sequential
Kafkars operations under one deadline: create, owner-filtered describe, renew,
and immediate expire. The adapter compares the two returned HMAC values only
inside its process and retains only the secret length and equality result;
neither command nor completion can serialize HMAC bytes. After the public
completion, Testlab invokes Kafka's pinned delegation-token CLI through a
separate SASL-authenticated observer listener. A fail-closed shell projection
emits only the owner-filtered token count, so even unexpected live-token rows
cannot put CLI HMACs into terminal artifacts. The polled final count must be
zero and contiguous with the public completion.

ADMIN-087 narrows that lifecycle to an explicit
`ExpireDelegationTokenBuilder::expire_after(Duration::ZERO)` selection. The
zero-millisecond value must remain exact in the command, the public expiration
timestamp must satisfy the ADMIN-073 lifecycle bounds, and the immediately
following sanitized CLI observation must still report zero live owner tokens.

ADMIN-074 binds one composite command to all seven public Streams-group Admin
methods under one deadline. Two genuine `group.protocol=streams` WordCount
applications first commit the exact fixture position and shut down cleanly.
The completion must retain the singleton and caller-ordered plural Empty-group
descriptions with authorization and initialized topology detail, singleton and
plural stable offsets, the exact altered and deleted-offset post-reads, both
successful group deletions in caller order, and nine bounded throttles. The
public completion is followed immediately by a fail-closed projection of
Kafka's pinned Streams-group CLI; it retains only the two selected identities'
presence or absence and must prove both deleted groups absent.
ADMIN-092 repeats the same seven-method lifecycle with authorization metadata,
the full topology graph, and stable-offset reads disabled. The public
descriptions must omit authorization values and retain only an absent or exact
`NotRequested` full-topology status, command evidence binds all five affected
builder calls, and the ordinary offset and final CLI contracts retain the same
state truth without claiming unstable-offset divergence.

ADMIN-075 binds one exact single-record transaction command to public
`DescribeProducers` state before and after public Admin partition abort. The
first state must contain one open transaction. The second must preserve the
producer ID, epoch, last sequence, and coordinator epoch, advance the timestamp
monotonically, and clear `current_transaction_start_offset`. It must precede the
transaction completion so ordinary token-drop cleanup cannot establish the
transition. One immediate pinned Kafka CLI snapshot before any later command
must preserve that cleared producer identity, sequence, coordinator epoch, and
monotonic timestamp; token-drop cleanup may append a later abort marker.

ADMIN-076 binds one exact public broker-unregistration command to a broker that
Testlab has just stopped in its disposable three-broker environment. The public
completion must preserve the selected broker ID and bounded throttle, followed
before any later adapter command by an independently polled exact remaining
broker set. A preceding and later public cluster description, each immediately
corroborated by independent metadata, must expose the complete original broker
set and the same nonempty cluster ID. Exact successful environment stop and
start terminals for the selected broker must bracket the mutation and restored
description, making the release scenario reversible without treating restart
success as mutation evidence.

ADMIN-077 binds one exact public timestamp `ListOffsets` command to a selected
record strictly inside immediate independent partition watermarks. The public
offset and returned timestamp must equal one exact independent broker record,
and an earlier timestamped record must be present so an earliest selector cannot
satisfy the same evidence. The selected offset below the high watermark also
prevents a latest-offset substitution from passing.

ADMIN-078 binds one exact public maximum-timestamp `ListOffsets` command to the
unique independently observed record carrying the greatest timestamp. The
public offset and returned timestamp must match that record and lie within
immediate independent partition watermarks. A later record with a lower
timestamp makes the greatest-timestamp result differ from both the earliest
boundary result and the latest offset.

ADMIN-088 binds one exact singleton public `ListOffsets` command to its
caller-selected `read_uncommitted` isolation. The packaged pair uses only
acknowledged nontransactional records: its public earliest result must equal the
immediate independent low watermark, while the paired `read_committed` latest
query continues to satisfy ADMIN-005 against the high watermark. This proves
the two exact public selections and their results without claiming behavior in
the presence of an unresolved transaction.

ADMIN-089 applies exact `read_uncommitted` selection to one public batch
`ListOffsets` call. Its deliberately non-partition-sorted earliest/latest
queries must retain caller order, succeed without per-resource errors, and
equal contiguous immediate independent low or high watermarks respectively.
The fixture uses acknowledged nontransactional records and therefore does not
claim an unresolved-transaction last-stable-offset distinction.

CONS-005 through CONS-011 retain public assignment transitions, stable member
snapshots, and multi-member receive attribution. These public facts prove which
packaged consumer claimed each partition; independent record observations,
committed-offset queries, and consumer-group member counts separately
corroborate the broker-visible effects. Testlab does not parse private assignment
state or infer a definite owner from an adapter success string.

CONS-026 additionally requires each assignment observation's exact selected
public `next_event` or `try_take_event` method in one preceding command and one
completion. It does not replace the assignment-state or broker-visible truth
required by CONS-005 through CONS-011.

Assignment observation cannot settle on a fence that a drained revoking or lost
event invalidated; it waits for a newer public assignment under the original
observation deadline. Independent watermark capture retries broker leadership
transitions under its original deadline and preserves permanent query errors.

CONS-011 additionally requires three distinct successful broker stop/start
pairs and a committed group receive, or a nonempty Share acquisition followed
by its exact successful Accept acknowledgement, while each broker remains stopped, so one
of the three disruptions necessarily covers the original coordinator.
CONS-014 retains each exact group pause, resume, or seek command and one matching
operation-identified completion. The completion proves only the public control
result. Subsequent classic and KIP-848 committed receives, positive protocol
epochs, assignment snapshots, and independent broker records separately prove
partition isolation, resumed progress, and exact seek replay.
CONS-024 separately retains each exact individual or aggregate
operation-configuration method and its durations in the group-create command.
CONS-014 and LIFE-012 then bind the selected registration's public seek and
explicit close outcomes.
CONS-025 retains the exact `into_checkpoint` selector on one full-batch group
receive. Its successful commit, positive group epoch, exact public record, and
independent broker observation remain required by the ordinary consumer
contracts; the command evidence prevents canonical `checkpoint` substitution.
LIFE-015 retains the exact clone-shared shutdown request count and requires one
correlated public event-stream termination before the adapter releases its
hosted group handle. That terminal does not claim a broker leave: the scenario's
packaged Admin description and immediate independent group-state query both
must report zero live members under the ordinary Admin contracts.
LIFE-016 requires every exact group-consumer abandonment command to emit one
correlated completion after dropping its public owner without invoking the
group close or shutdown surface. It does not itself claim retained membership;
ADMIN-068 requires the later public and independent broker observations.

CONCUR-001 requires one exact ordered start and join boundary for each declared
concurrent group. CONCUR-002 requires the completion set and order to equal the
declared actor identities and operation identities exactly. CONCUR-003 binds
each actor's normal public terminal or receive completion to the join command
and to the exact started/completed event window; a group completion cannot stand
in for a missing public outcome. CONCUR-004 independently joins every producer
actor to its broker-observed record and every consumer actor to the exact public
record expected by the scenario. Adapter scheduling claims cannot manufacture
overlap, and broker visibility cannot manufacture a public client result.

FAULT-001 requires one exact pre-stop owner and a distinct post-election owner
for each typed broker-role target. FAULT-002 binds the observed original owner
to one ordered successful stop, restore, and readiness sequence. Restore accepts
the recorded Compose `start` or `restart --no-deps` command for the exact same
project and service. Contiguous readiness attempts retain their failures and
must end in a successful probe of that same project and service. Restoring a
partition leader additionally waits until the exact restarted broker rejoins
the partition's full ISR, then records the still-current leader as an
`after_restore` role fact. FAULT-003
requires matching public progress after the replacement election and before
the original owner is restored: an acknowledged produce, successful topic
creation, committed group receive, or committed transaction according to the
targeted role. Adapter success cannot establish the role owner or election,
and environment observations cannot manufacture public client progress.

POLICY-001 requires one exact ordered alter, query, and normalized observation
chain for both policy application and removal. POLICY-002 binds an active deny
ACL to the exact normalized public producer, group, admin, or transaction
failure without trusting that error as broker policy truth. POLICY-003 requires
restored public progress after observed ACL removal, corroborated by record,
topic, group receive, or transaction evidence as appropriate. POLICY-004
requires public producer or consumer progress while an independently timed
quota window remains active for at least the declared duration. PROD-009 pins
the exact scenario-declared producer error code independently from admission,
delivery certainty, and broker visibility.

Network-fault runs additionally retain `network-proxy.jsonl` and
`network-proxy.stderr.txt`, named from one terminal `network_proxy` environment
operation. Ordered `network_proxy_control` entries record only exact
acknowledged cuts, applications, and removals. Completed cuts record the exact
number of selected live connections closed. Completed fault windows record
connection counts, forwarded and delayed bytes by direction, blackhole wait
intervals, and worker timestamps. The adapter connects only to proxy listener
ports while broker observations use separate direct observer ports.

NET-001 requires an exact declared control set and coherent contiguous effect
observations. NET-002 requires one successful supervised proxy process with
the exact terminal artifacts. NET-003 binds each exact active fault window to
its required public producer outcome: `possibly_sent` for a blackhole and
`acknowledged` for bounded one-way delay. NET-004 requires a later acknowledged
send after every removal or connection cut. Proxy facts cannot manufacture a
client result, and adapter success cannot establish that a transport fault
occurred.

Protocol-adversary runs additionally retain `protocol-adversary.jsonl` and
`protocol-adversary.stderr.txt`, named from one terminal
`protocol_adversary` environment operation. Ordered `adversary_control` history
entries prove which scenario control testctl acknowledged. Ordered
`adversary_observation` entries independently prove the complete request frame,
actual response bytes, selected fault, and stable control identity. ADV-001 and
ADV-002 require one exact control and its declared number of matching ordered
applications. ADV-003 requires coherent contiguous observations and one
successful external worker. ADV-004 binds a metadata fault to its exact public
topic-description success or failure without treating adapter output as wire
truth. A worker that exits with an armed but unexercised control seals invalid
evidence.

## Qualification evidence

`testctl qualify` creates one `<qualification-run-id>.partial` tree, executes
every reviewed environment/pack cell into `cells/<cell-id>/<run-id>`, derives
cell and top-level status from the sealed run verdicts, recursively digests the
complete tree, and only then publishes the qualification directory.
Cells declare a bounded attempt count. Every scenario run records its one-based
attempt ordinal, and any failed or invalid repetition contributes to the cell.

`--cell <cell-id>` runs one unchanged cell under the distinct qualification ID
`<qualification-id>--<cell-id>`. A passing shard is not a complete qualification.
`testctl aggregate-qualification --qualification <manifest> --shard <directory>`
accepts one sealed shard directory per expected cell (repeat `--shard`). It
requires exact cell membership, scenario order, all declared attempts, matching
catalog definitions, intact recursive digests, and identical subject package
names, versions, and SHA-256 checksums. Runner-specific adapter executable paths
may differ for packaged subjects; other subject configuration must agree.
Missing, duplicate, partial, corrupt, or mixed-candidate shards fail closed.
Only the resulting complete aggregate is eligible release evidence.

The PR tier executes its pack once for timely feedback. Release repetitions and
the full broker/security matrix remain unchanged. Clients may schedule cells on
separate runners and retain each shard even when another cell fails. Aggregation
preserves failed and invalid verdicts and emits the existing qualification
evidence schema, with cells in the reviewed manifest order.

At least one cell must be gating. For gating cells, `invalid` outranks `failed`
and `failed` outranks `passed`. Non-gating cells remain visible but cannot make
the release-facing aggregate pass or fail.

## Status

`passed` means valid evidence and no semantic violation.

`failed` means valid evidence with one or more semantic violations. An
adapter-reported public client API error is such a violation unless one negative
scenario declares that exact correlated code and independently proves its
postcondition. Public failures remain distinguishable from adapter, protocol,
process, or environment invalidity.

`invalid` means process, protocol, timeout, capability, environment, broker, or
harness failure prevents a client claim.

## Evidence references

Violations cite stable locations such as `history:12`,
`broker-observation:3`, `broker-state-observation:0`, and
`scenario:operation:op-1`.

## Future LLM summary

An LLM receives a deterministic packet containing status, contract IDs, metric
IDs, and bounded excerpts. Its output is derived narrative. It never alters the
verdict or raw evidence.
