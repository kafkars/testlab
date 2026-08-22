# Adapter control protocol

## Transport

Protocol v7 is UTF-8 JSON Lines over stdin and stdout.

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
- `flush`
- `close_producer`
- `shutdown_client`
- `finish`

Model-broker controls are owned directly by testctl and do not cross the adapter
boundary.

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

## Failure behavior

A normal public client API failure emits one correlated `command_failed` event
and exits successfully. Testctl stops issuing dependent steps, retains an
independent broker snapshot, and produces valid failing evidence. An adapter or
protocol failure instead emits `fatal` and exits nonzero. A crash, malformed
stdout, wrong version, wrong command ID, or timeout invalidates the run.

## Evolution

Protocol v7 is an exact semantic contract. New capabilities may be declared
from the existing vocabulary, but adding or removing fields, changing meaning,
or narrowing accepted values requires a new protocol version.
