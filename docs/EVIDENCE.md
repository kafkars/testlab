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

Evidence schema v26 records the exact environment identity in `manifest.json`,
retains protocol-v34 direct and hosted-group consumer controls and shutdown,
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
policy in the issued protocol command. That adapter-reported configuration is
not the verdict: exact group receives must still join to independently observed
records, positive protocol epochs, and aborted-transaction visibility evidence.

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

Earliest- and latest-offset Admin claims use an immediate independent
librdkafka watermark query for one exact partition. Record deletion retains
ordered pre- and post-operation low and high watermarks, proving that the
declared prefix became unavailable without accepting an adapter echo as broker
truth. Topic creation, expansion, description, and listing use immediate
metadata facts. An ordered batch creation retains one command and one completion
with one caller-ordered public outcome per requested topic, followed by immediate
independent metadata observations for those topics. Expected per-resource error
codes remain scenario-only and are not part of the adapter command. The evidence
does not imply exhaustive topic discovery, internal-topic classification,
replica topology, or untested offset selectors.

`broker-state-observations.jsonl` retains independently queried broker state
that is not a record snapshot. Schema v17 includes exact topic metadata, one
selected non-sensitive topic-configuration value, cluster identity and broker
IDs, consumer-group existence and member count, and one consumer-group
committed offset, plus exact partition low and high watermarks. Each query runs
immediately after its exact correlated public admin command while later scenario
steps are paused. A state query is never emitted for a public command that was
not actually issued.

Protocol-v22 plural group-offset and classic-group operations retain the same
broker-state fact shapes in schema v17. Plural offset
operations retain one existing `ConsumerGroupOffset` observation per selected
key, with contiguous observation ordinals in caller-flattened order. Classic
batch descriptions retain one existing `ConsumerGroupState` observation per
requested group in caller order. Listings use immediate non-polling reads;
mutations poll for the declared offset or explicit absence.

The state-query consumer never joins the target group, subscribes, assigns, or
commits. Topic and group absence remain explicit typed facts. Observer errors,
authorization failures, and timeouts invalidate the run rather than manufacture
a client result. ADMIN-006 through ADMIN-016 compare public results and temporal
mutations with these independently observed facts. ADMIN-017 additionally
requires a distinct pre-deletion watermark baseline and an unchanged high
watermark. ADMIN-018 binds each caller-ordered batch result to its independently
observed topic state, including an exact expected duplicate-topic code without
turning the successful sibling result into a failure. ADMIN-014 binds an exact
correlated topic-already-exists public failure to an unchanged topic snapshot;
an unrelated or differently coded failure cannot satisfy it. ADMIN-019 applies
the same correlation and no-success rules to unknown-topic partition creation,
deletion, and description, and to a selected-offset request for an independently
proven absent partition. ADMIN-020 through ADMIN-022 require distinct
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
alone do not establish classicness. ADMIN-015 and ADMIN-016 retain selected
topic-configuration values and require a distinct independent pre-mutation
baseline. In particular, mutation baselines use distinct preceding list or
describe operation IDs, so history order preserves precondition and
postcondition meaning without trusting an adapter echo.

CONS-005 through CONS-011 retain public assignment transitions, stable member
snapshots, and multi-member receive attribution. These public facts prove which
packaged consumer claimed each partition; independent record observations,
committed-offset queries, and consumer-group member counts separately
corroborate the broker-visible effects. Testlab does not parse private assignment
state or infer a definite owner from an adapter success string.
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
must end in a successful probe of that same project and service. FAULT-003
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
