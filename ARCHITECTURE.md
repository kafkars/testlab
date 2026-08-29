# Testlab architecture

## 1. Purpose

Testlab is the external trust boundary for kafkars clients. It exercises built
artifacts through public APIs, observes broker-visible state independently, and
emits replayable evidence.

It answers:

1. Did the client report a coherent public history?
2. Did the broker observe a compatible history?
3. Did the client preserve delivery uncertainty and lifecycle ownership?
4. Is the run valid enough to support a release claim?

Performance belongs in `kafkars/benchmarks`.

## 2. Trust boundaries

### `testctl`

Owns run identity, the absolute deadline, subject lifetime, command correlation,
scenario execution, concurrent actor scheduling, evidence sealing, and process
exit status.

### Adapter

Translates one packaged public client surface into normalized events. It reports
what the client said. For concurrent scenarios it schedules only public client
calls behind an explicit barrier and retains their exact actor and operation
identities. It never decides whether the broker agrees.

### Environment

Owns external effects and observations. A tiny model broker self-tests the
harness but never supports Kafka compatibility claims. Docker Compose
environments run pinned real Kafka clusters, retain lifecycle and disruption
operations, and observe records and targeted broker state independently through
librdkafka. The environment also owns typed broker-role discovery and exact
service disruption. It discovers partition leaders through librdkafka and uses
bounded Kafka Metadata or FindCoordinator requests for controllers and group or
transaction coordinators on declared plaintext endpoints. It also owns typed
deny ACL and user-quota transitions: raw Kafka CLI terminals and separate
fail-closed queries precede every normalized policy fact. The scripted Kafka
protocol adversary runs as a separate supervised process, owns a loopback Kafka
listener, applies versioned scenario controls, and reports exact request and
response-side byte counts independently from the adapter. An external network
fault proxy runs as another supervised process between the packaged
client and a hidden plaintext broker listener. It forwards opaque TCP bytes,
owns typed connection cuts, blackholes, and one-way delay controls, and reports
measured effects. Independent broker observations use a separate direct
listener and never traverse the proxy.

### Verifier

Owns deterministic decisions over scenario intent, adapter history, and broker
observations. Broker-role recovery requires an independently observed owner
change, exact stop/restore evidence, and matching public progress while the
original owner remains offline. Broker-policy verification independently joins
observed policy windows to exact public denial and recovery or bounded quota
progress. The verifier has no process control, network I/O, or LLM dependency.
Network-fault verification joins exact scenario controls, proxy observations,
the external worker terminal, public client outcomes, and post-fault recovery.
Concurrent verification joins the harness-owned actor schedule, exact public
operation events, and independent broker observations without treating runtime
scheduling as broker truth.

### Evidence writer

Writes `<run-id>.partial`, syncs required files, computes digests, and renames to
`<run-id>` only when complete.

## 3. Process topology

```text
                 commands (JSONL)
            ┌────────────────────────┐
            │                        ▼
        ┌─────────┐             ┌──────────┐
        │ testctl │             │ adapter  │
        └────┬────┘             └────┬─────┘
             │                        │ public client API
 environment │                        ▼
 controls    │                  ┌───────────┐
             └─────────────────►│ Kafka or  │
                                │ test peer │
                                └─────┬─────┘
                                      │ independent observations
                                      ▼
                                deterministic verifier
                                      │
                                      ▼
                                 sealed evidence
```

## 4. One scenario, many surfaces

The scenario vocabulary is public-product behavior, not Rust implementation
behavior. A future Rust, C, or Java adapter consumes the same operation IDs and
produces the same normalized admission, terminal, and lifecycle events.

Reference clients are compared only over a genuinely shared semantic
intersection. Raw error strings and runtime-specific scheduling are not
conformance keys.

## 5. Delivery truth

Admission and delivery are separate:

- `operation_rejected`: ownership never transferred; no terminal may follow;
- `operation_accepted`: ownership transferred; exactly one terminal must follow;
- `acknowledged`: the client reports broker acknowledgment;
- `definitely_not_sent`: the client knows persistence was impossible;
- `possibly_sent`: persistence cannot be known, commonly because a response was
  lost after a request may have crossed the boundary.

The verifier then compares those claims to independent observations. Producer
terminals and assigned, group, multi-member group, and Share records retain
their exact public topic, partition, offset, and bytes when one exact broker
record exists.
Direct assignments are replacement boundaries. Repeated receives retain their
declared broker-coordinate order, a new beginning assignment may replay an
earlier exact record, and separate directly assigned consumers retain separate
cursors. Repeated assignment and flush completions are verified against their
originating command identities instead of being aggregated by resource handle.
Sequential sends and caller-ordered batch records for one partition must appear
at strictly increasing broker offsets. Concurrent actor declaration order is
not treated as execution order. The verifier never converts uncertainty into
success or definite failure.

Transactions are verified as declared sets, not as unrelated sends. Every
operation must be accepted and staged exactly once before its public completion.
A committed set must be independently read-committed exactly once with matching
bytes and public broker coordinates, preserving caller order within each
partition. An aborted set must be entirely absent from that read-committed view;
Testlab does not turn staging metadata or absence into a claim about physical
log appends. Successive transactions on one public producer have non-overlapping
staging and disposition boundaries.

## 6. Deadlines and ownership

One absolute deadline starts before environment and subject setup. Every command
uses the remaining duration. On timeout, testctl kills and waits for the child,
settles reader threads, snapshots available broker observations, and seals an
invalid verdict.

Concurrent groups have exact start and join commands. The adapter validates the
complete group before releasing two through eight workers behind one barrier,
and testctl may apply environment controls before the bounded join. Producer
workers clone only public producer handles; directly assigned consumers move to
one worker and return on join. Normal operation events retain their public
meaning and are correlated with stable actor, operation, group, and command
identities. An unjoined group or lost handle invalidates execution.

Subject environments are cleared by default. Manifests explicitly declare
non-secret values and names of caller environment variables to pass through.

The adversary child has a separate versioned JSON Lines control channel. Its
stdout contains only protocol events; diagnostics use stderr. Testctl waits for
an exact arm acknowledgement before sending the next adapter command. A
malformed event, failed worker, unconsumed control, or missing terminal process
operation invalidates the run rather than becoming a client failure.

Adversary facts retain their source:

- the adapter reports only the packaged public client outcome;
- the environment reports the parsed Kafka API identity and exact bytes read or
  written for each request;
- the harness reports the ordered control and supervised process terminal;
- the verifier joins those facts without inferring an unobserved response or
changing public delivery certainty.

The network-proxy child follows the same process and stream ownership rules.
Its versioned controls and observations remain environment facts. It does not
parse Kafka, inspect adapter state, or share the observer connection. A
blackholed send must retain public uncertainty; delay windows require measured
bytes in the selected direction; every removal or cut requires later public
recovery.

## 7. Scenario evolution

Protocol and manifest schema versions are exact compatibility boundaries.
Adding or removing a field, changing meaning, or narrowing accepted input
requires a new version. Test evidence always records the exact versions used.

## 8. Testing layers

| Layer | Home | Evidence |
| --- | --- | --- |
| Unit/invariant | client repositories | local semantic rules |
| Deterministic simulation | client repositories | long virtual-time histories |
| Scripted protocol adversary | client + testlab | exact hostile wire behavior |
| External network fault proxy | client + Kafka + testlab | observed transport-fault recovery |
| Black-box conformance | testlab | packaged public behavior |
| Real-cluster chaos | testlab | Kafka/version/security/failure compatibility |
| Soak/release gauntlet | testlab | boundedness and rare recovery behavior |

## 9. LLM boundary

A future reporting stage may summarize deterministic analysis packets. It may
cluster failures, label hypotheses, and propose experiments. It may not compute
authoritative counts, change a verdict, hide invalidity, or decide release
eligibility.
