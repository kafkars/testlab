# testlab

black-box correctness, compatibility, chaos, and release evidence for Kafka clients

`kafkars/testlab` attacks packaged client surfaces from the outside. It runs the
same machine-readable scenarios through Rust, C, Java, and reference-client
adapters; observes Kafka independently; and seals deterministic evidence for
pass, fail, or invalid outcomes.

The client repository proves internal invariants. Testlab distrusts the client
and verifies public behavior.

## Repository boundary

| Repository | Owns |
| --- | --- |
| `kafka-client` | Unit tests, invariant tests, deterministic simulation, implementation-aware loopback tests, private fuzz targets |
| `testlab` | Black-box adapters, scenarios, independent verification, compatibility, chaos, packaging, release evidence |
| `benchmarks` | Performance methodology, request economics, profiling, and performance reports |

Testlab may consume public artifacts. It never imports private client state.

## What this first cut contains

- a versioned JSON Lines process protocol for client adapters;
- typed scenario, subject, history, evidence, and verdict schemas;
- one absolute scenario deadline and owned child-process supervision;
- deterministic producer and lifecycle verification;
- an independent model broker used only to self-test the harness;
- a reference Rust adapter;
- atomic evidence sealing with SHA-256 digests and replay commands;
- zrail policy for 300-line files, facade-only modules, and separate tests;
- catalog integrity validation through the same public manifest loader used by
  `testctl`;
- three end-to-end scenarios: acknowledgment, definite rejection, and a lost
  response that must remain `possibly_sent`.

The model broker is **not Kafka compatibility evidence**. The next implementation
batch adds the real `kafkars-rust` adapter, a Kafka-protocol adversary, and a
pinned real-cluster lane.

## Quick start

```bash
scripts/check
scripts/run-reference-pack
```

Or:

```bash
cargo build -p testctl -p testlab-reference-adapter

target/debug/testctl validate --root .

target/debug/testctl run-pack \
  --root . \
  --pack packs/repository-pr.toml \
  --subject subjects/reference-rust.toml \
  --evidence-dir evidence
```

A run seals a directory like:

```text
evidence/run-.../
├── adapter.json
├── broker-observations.jsonl
├── digests.json
├── history.jsonl
├── manifest.json
├── reproduction.sh
├── scenario.json
├── subject.json
├── summary.md
└── verdict.json
```

## Adapter boundary

Every client surface is a standalone executable:

```text
testctl
  ├── kafkars-rust-adapter
  ├── kafkars-c-adapter
  ├── kafkars-java-adapter
  ├── apache-java-adapter
  └── librdkafka-c-adapter
```

Adapters receive commands on stdin and emit normalized events on stdout. The
process boundary catches packaging, ABI, loader, shutdown, crash, and runtime
behavior that an in-process Rust trait would hide.

## Verdicts

- **passed** — valid evidence and every deterministic contract held;
- **failed** — valid evidence, but one or more semantic contracts failed;
- **invalid** — process, protocol, environment, timeout, broker, or harness
  failure prevents a product claim.

Retries never overwrite evidence. Every attempt has a new run identity.

## Core contracts

- Every send emits exactly one accepted or rejected admission decision.
- Every accepted send settles exactly once; rejected sends do not settle later.
- Acknowledged records are broker-visible exactly once.
- Definitely-not-sent records are absent.
- Possibly-sent records are visible zero or one times, never twice.
- Broker-visible bytes and the environment-reported digest are independently
  recomputed and checked.
- Client, producer, flush, close, shutdown, and finish events settle exactly
  once.

The registry lives in `contracts/conformance.toml`.

## Design

- [`ARCHITECTURE.md`](ARCHITECTURE.md)
- [`docs/CONTROL_PROTOCOL.md`](docs/CONTROL_PROTOCOL.md)
- [`docs/EVIDENCE.md`](docs/EVIDENCE.md)
- [`docs/ROADMAP.md`](docs/ROADMAP.md)
- [`docs/ADDING_AN_ADAPTER.md`](docs/ADDING_AN_ADAPTER.md)
