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

Evidence schema v60 records the exact environment identity in `manifest.json`,
retains protocol-v71 direct and hosted-group consumer controls, abandonment,
and shutdown,
consumer ownership observations, multi-member receive
completions, and concurrent actor boundaries, ordered protocol-adversary
controls and wire observations, and
independently selected broker-role disruption, broker-policy facts, and
network-proxy controls and effect observations.
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
or fencing command
contributes every contained operation. The snapshot uses broker watermarks and
emits structured observations with exact partition, offset, key, value, and
ordered header bytes.

The record comparison preserves byte-level distinctions: null and empty keys or
values are different, a tombstone has a null value, and duplicate headers retain
their original order with null, empty, text, or binary values. PROD-010 binds an
acknowledged public terminal offset to its one exact independent observation and
forbids offsets on uncertain or definitely-unsent terminals. CONS-012 directly
binds assigned, ordinary group, and every multi-member group receive-set record
to the independent topic, partition, offset, key, value, and ordered headers.
SHARE-006 applies that same independent comparison to each exact Share
acquisition without replacing delivery-count or membership-fence checks.
SHARE-007 binds a multi-record acquisition to the complete caller-declared
ordered record set. SHARE-008 retains the exact record-ordered public
disposition vector, and SHARE-009 requires released or dropped records to
return with increased delivery counts without accepting settled siblings as
substitutes. SHARE-010 requires the exact scenario-declared public acquisition
count for a configured Share receive while SHARE-006 still binds every record
in that batch to independent broker coordinates and bytes.
PROD-011 requires strictly increasing independent offsets for sequential sends
and caller-ordered batch records targeting one partition; concurrent actor
declarations do not claim an execution order.

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
delivery's authoritative terminal. It enforces stage monotonicity, requires
`cancelled_not_sent` to agree with definitely-not-sent `cancelled` truth, and
leaves `too_late` broker visibility uncertain until ordinary terminal and
independent observation contracts resolve it.

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
control and one matching public completion. Successive receives are joined to
their declared independently observed records in order. Exact offset and end
starts, seek replay, paused-partition isolation, and survivor cursors after
incremental mutation are therefore broker-backed outcomes rather than adapter
success claims. Two direct consumers may still independently expose the same
coordinate. LIFE-003 and LIFE-009 evaluate repeated flushes and legacy
assignments per command rather than by aggregate resource counts.

Configured-group history retains the requested missing-offset and read-isolation
policy plus any static group-instance identity and classic session timeout in
the issued protocol command. That adapter-reported configuration is not the
verdict: exact group receives must still join to independently observed records,
positive protocol epochs, and aborted-transaction visibility evidence.
ADMIN-068 additionally requires each public consumer owner to be abandoned,
its owning client to shut down without a group close, and both static identities
to remain broker-registered inside the configured session window until the
exact removal command.

Configured-Share history retains the requested `max_records` and `batch_size`
policy in the issued create command. The adapter reports only the public batch's
acquisition count, records, delivery counts, and membership fences. SHARE-010
checks the scenario-owned acquisition expectation, while the ordinary Share
record contracts independently check the delivered broker records.

Lifecycle-isolation scenarios retain every readiness, flush, close, and
shutdown completion under its originating command. A later send on a sibling,
replacement, or independently owned producer must still satisfy the ordinary
admission, terminal, coordinate, and broker-visibility contracts; a lifecycle
success string alone cannot establish handle isolation.

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
TXN-007 binds each consume-transform-produce command to one exact independently
observed input record, positive classic or KIP-848 membership fence, and the
public assignment-fenced next-offset checkpoint. TXN-008 requires a committed
checkpoint to match an immediate public Admin result and independent broker
query; abort is established by closing the member and receiving that exact
input again from the unchanged group position.

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
watermark queries. Singleton and caller-ordered plural record deletion retain
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
or `DescribeTopicPartitions` public command and joins its exact partition set to
an immediate independent metadata snapshot. ADMIN-006 through ADMIN-016 compare
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
classic-group public description to immediate independent existence and member
counts with no group errors, and requires every counted live member to have a
prior committed receive with a positive classic epoch; broker membership facts
alone do not establish classicness. ADMIN-028 binds one caller-ordered public
batch offset result to contiguous immediate watermark observations for every
unique topic-partition selection and requires each selected earliest or latest
offset exactly. ADMIN-029 applies the generic public group listing to the same
independent group facts without narrowing the result to consumer groups.
ADMIN-030 through ADMIN-032 bind caller-ordered public ACL creation, one exact
public description, and caller-ordered exact deletion matches to contiguous
Kafka-CLI observations of presence or absence after each terminal. Public and
independent ACL facts remain separate; neither stream can substitute for the
other. ADMIN-033 and ADMIN-034 bind exact named-user byte-rate description,
replacement, and removal terminals to immediate Kafka-CLI observations of the
same user, key, and whole-number value or explicit absence. Public and
independent client-quota facts remain separate. ADMIN-035 and ADMIN-036 bind
exact named-user SCRAM-SHA-256/512 description, upsert, and deletion terminals
to immediate Kafka-CLI observations of the same non-secret mechanism,
iterations, or absence. The password exists only in the adapter process
environment and never enters protocol or evidence. Public and independent
SCRAM facts remain separate. ADMIN-015 and ADMIN-016 retain selected
topic-configuration values and require a distinct independent pre-mutation
baseline. In particular, mutation baselines use distinct preceding list or
describe operation IDs, so history order preserves precondition and
postcondition meaning without trusting an adapter echo.

ADMIN-037 binds one active public Share-group description to the exact expected
state, epochs, assignor, member subscription, nonzero topic identity, and
partition assignment. A separate immediate Kafka CLI state query must agree on
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

ADMIN-042 binds one caller-ordered public batch to a complete detailed
description for every selected active Share group. Each modeled member must
retain an exact acquired public batch with positive member and assignment
fences before the admin command. Separate immediate read-only Kafka CLI state
queries retain consecutive history and observation order and must agree on
every group, stable state, and member count.

ADMIN-043 binds one caller-ordered public batch to every selected Share-group
and topic-partition outcome, including nonzero topic identity, start offset,
leader epoch, lag, and absence of group-level or partition-level errors.
Separate immediate read-only Kafka CLI offset queries run once per group; their
normalized facts retain consecutive history and observation order and must
agree on every selected start offset and lag.

ADMIN-044 binds one caller-ordered public topic-description batch to each full
successful description or exact missing-topic error. Successful descriptions
retain a nonzero topic identity, the internal marker, and every ordered
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

ADMIN-052 binds one unfiltered public transaction listing to one immediate
pinned Kafka CLI snapshot. The fixture initializes and closes every modeled
transactional producer first. Public rows must be canonical by transactional
ID, contain exactly the scenario-declared identities and states, preserve each
nonnegative producer ID, and exactly equal the independently parsed rows; any
unknown filter or broker error prevents the public completion.

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
`describe_topics_by_id`: every caller-positioned result must retain its exact
nonzero request UUID, repeat that UUID inside the successful description, and
preserve the expected partition topology without hidden errors. One pinned
Kafka topic-CLI query per name immediately supplies independent UUID and
partition facts in the same contiguous caller order.

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
and KIP-848 consumer groups to the scenario's live committed members. The
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

CONS-005 through CONS-011 retain public assignment transitions, stable member
snapshots, and multi-member receive attribution. These public facts prove which
packaged consumer claimed each partition; independent record observations,
committed-offset queries, and consumer-group member counts separately
corroborate the broker-visible effects. Testlab does not parse private assignment
state or infer a definite owner from an adapter success string.

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
