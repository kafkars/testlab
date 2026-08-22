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
- an exact packaged `kafkars 0.0.1` adapter;
- atomic evidence sealing with SHA-256 digests and replay commands;
- zrail policy for 300-line files, facade-only modules, and separate tests;
- catalog integrity validation through the same public manifest loader used by
  `testctl`;
- seven immutable Apache Kafka 3.7.2–4.3.1 versions plus TLS, SASL/PLAIN, and
  SCRAM-SHA-256/512 environments with owned digest pull, inspection, readiness,
  snapshots, logs, cleanup, and sealed terminal evidence;
- fail-closed qualification manifests that aggregate ordered scenario evidence;
- bounded per-cell repetition so intermittent failures block qualification;
- independent real-Kafka observation through pinned librdkafka;
- three end-to-end scenarios: acknowledgment, definite rejection, and a lost
  response that must remain `possibly_sent`.

The model broker is **not Kafka compatibility evidence**. The `kafkars-pr`
qualification runs the packaged Kafkars adapter against pinned Apache Kafka and
uses a separate librdkafka consumer to verify broker-visible records.

## Quick start

```bash
scripts/check
scripts/run-reference-qualification
scripts/run-kafkars-qualification # requires Docker
scripts/run-kafkars-release-qualification # version and security release matrix
scripts/qualify-kafkars-candidate ../kafkars pr # packages the checkout first
```

The candidate command packages `kafka-client-core`, `kafka-client-engine`, and
`kafkars`, hashes each `.crate`, extracts them, and builds the external adapter
only against those extracted packages. Use `release` instead of `pr` for the
seven-version matrix plus TLS, SASL/PLAIN, and both SCRAM mechanisms. Dirty
source is rejected unless `--allow-dirty` is the third argument.

Kafkars CI can call the same boundary without copying any broker logic:

```yaml
- uses: kafkars/testlab@<full-testlab-commit-sha>
  with:
    kafkars-path: ${{ github.workspace }}
    qualification: pr
    evidence-directory: testlab-evidence
- if: ${{ always() }}
  uses: actions/upload-artifact@v4
  with:
    name: testlab-evidence
    path: testlab-evidence
```

The workflow only selects a qualification tier. Testlab owns Kafka image
digests, Docker lifecycle, scenario repetition, independent observation, and
the release-facing verdict.

Or:

```bash
cargo build -p testctl -p testlab-reference-adapter

target/debug/testctl validate --root .

target/debug/testctl qualify \
  --root . \
  --qualification qualifications/repository-pr.toml \
  --subject subjects/reference-rust.toml \
  --evidence-dir evidence
```

The command prints exactly one release-facing status and evidence path. Every
qualification cell and scenario run remains inspectable beneath that directory.

A run seals a directory like:

```text
evidence/run-.../
├── adapter.json
├── broker-observations.jsonl
├── digests.json
├── environment.json
├── history.jsonl
├── manifest.json
├── reproduction.sh
├── scenario.json
├── subject.json
├── summary.md
└── verdict.json
```

A qualification seals a recursively digested tree:

```text
evidence/qualification-.../
├── cells/<cell-id>/<run-id>/...
├── digests.json
├── manifest.json
├── qualification.json
├── reproduction.sh
├── subject.json
└── summary.md
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

The Kafka observer is environment-owned and uses librdkafka, not Kafkars. Real
Kafka scenarios carry `testlab-operation-id` and `testlab-sequence` headers so
observations remain correlated and exact without trusting adapter claims.

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
- Client creation, readiness, producer creation, flush, close, shutdown, and
  finish events settle exactly once.

The registry lives in `contracts/conformance.toml`.

## Design

- [`ARCHITECTURE.md`](ARCHITECTURE.md)
- [`docs/CONTROL_PROTOCOL.md`](docs/CONTROL_PROTOCOL.md)
- [`docs/EVIDENCE.md`](docs/EVIDENCE.md)
- [`docs/ROADMAP.md`](docs/ROADMAP.md)
- [`docs/ADDING_AN_ADAPTER.md`](docs/ADDING_AN_ADAPTER.md)
