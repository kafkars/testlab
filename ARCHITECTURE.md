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
scenario execution, evidence sealing, and process exit status.

### Adapter

Translates one packaged public client surface into normalized events. It reports
what the client said. It never decides whether the broker agrees.

### Environment

Owns external effects and observations. A tiny model broker self-tests the
harness but never supports Kafka compatibility claims. Docker Compose
environments run pinned real Kafka clusters, retain lifecycle and disruption
operations, and observe records and targeted broker state independently through
librdkafka. A future Kafka protocol adversary and network fault proxy will
extend this boundary.

### Verifier

Owns deterministic decisions over scenario intent, adapter history, and broker
observations. It has no process control, network I/O, or LLM dependency.

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

The verifier then compares those claims to independent observations. It never
converts uncertainty into success or definite failure.

## 6. Deadlines and ownership

One absolute deadline starts before environment and subject setup. Every command
uses the remaining duration. On timeout, testctl kills and waits for the child,
settles reader threads, snapshots available broker observations, and seals an
invalid verdict.

Subject environments are cleared by default. Manifests explicitly declare
non-secret values and names of caller environment variables to pass through.

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
| Black-box conformance | testlab | packaged public behavior |
| Real-cluster chaos | testlab | Kafka/version/security/failure compatibility |
| Soak/release gauntlet | testlab | boundedness and rare recovery behavior |

## 9. LLM boundary

A future reporting stage may summarize deterministic analysis packets. It may
cluster failures, label hypotheses, and propose experiments. It may not compute
authoritative counts, change a verdict, hide invalidity, or decide release
eligibility.
