# Adapter control protocol

## Transport

Protocol v3 is UTF-8 JSON Lines over stdin and stdout.

- One line is one complete JSON object.
- Adapter stdout is protocol-only; diagnostics use stderr.
- A line larger than 4 MiB is invalid before unbounded allocation.
- Every event repeats the protocol version and originating command ID.

## Handshake

Testctl sends `hello` with run ID, scenario ID, and environment endpoint. The
adapter replies `ready` with implementation identity, version, and exact
capabilities.

## Commands

- `hello`
- `create_client`
- `await_client_ready`
- `create_producer`
- `send`
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
- `flush_completed`
- `producer_closed`
- `client_shutdown`
- `command_failed`
- `finished`
- `fatal`

A send emits one admission decision. Accepted operations later emit exactly one
terminal event. Rejected operations emit no terminal.

## Failure behavior

A normal public client API failure emits one correlated `command_failed` event
and exits successfully. Testctl stops issuing dependent steps, retains an
independent broker snapshot, and produces valid failing evidence. An adapter or
protocol failure instead emits `fatal` and exits nonzero. A crash, malformed
stdout, wrong version, wrong command ID, or timeout invalidates the run.

## Evolution

Protocol v3 is an exact semantic contract. New capabilities may be declared
from the existing vocabulary, but adding or removing fields, changing meaning,
or narrowing accepted values requires a new protocol version.
