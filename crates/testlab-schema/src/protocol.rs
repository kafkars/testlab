//! Versioned JSON Lines commands and events cross the adapter process boundary.

use serde::{Deserialize, Serialize};

use crate::{
    AdapterDescriptor, ClientId, CommandId, OperationId, ProducerId, RecordSpec, RunId, ScenarioId,
};

/// Current adapter control protocol version.
pub const PROTOCOL_VERSION: u16 = 3;

/// One correlated command sent from testctl to an adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommandEnvelope {
    /// Exact control protocol version.
    pub protocol_version: u16,
    /// Stable command correlation identity.
    pub command_id: CommandId,
    /// Command payload.
    #[serde(flatten)]
    pub command: AdapterCommand,
}

impl CommandEnvelope {
    /// Creates one protocol-v3 command envelope.
    pub fn new(command_id: CommandId, command: AdapterCommand) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            command_id,
            command,
        }
    }
}

/// Public operation requested from an adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdapterCommand {
    /// Starts one adapter session and declares the environment endpoint.
    Hello {
        /// Unique test attempt.
        run_id: RunId,
        /// Stable scenario identity.
        scenario_id: ScenarioId,
        /// Broker or test-peer endpoint selected by testctl.
        broker_endpoint: String,
    },
    /// Creates one public client handle.
    CreateClient {
        /// Scenario-local client identity.
        client_id: ClientId,
    },
    /// Waits for one public client readiness probe.
    AwaitClientReady {
        /// Existing client identity.
        client_id: ClientId,
    },
    /// Creates one public producer handle.
    CreateProducer {
        /// Owning client.
        client_id: ClientId,
        /// Scenario-local producer identity.
        producer_id: ProducerId,
    },
    /// Offers one record through the public producer surface.
    Send {
        /// Producer receiving the record.
        producer_id: ProducerId,
        /// Stable operation identity.
        operation_id: OperationId,
        /// Exact logical record.
        record: RecordSpec,
    },
    /// Flushes one producer.
    Flush {
        /// Producer to flush.
        producer_id: ProducerId,
    },
    /// Closes one producer.
    CloseProducer {
        /// Producer to close.
        producer_id: ProducerId,
    },
    /// Shuts down one client.
    ShutdownClient {
        /// Client to shut down.
        client_id: ClientId,
    },
    /// Ends the adapter session after lifecycle work settles.
    Finish,
}

/// Normalized terminal delivery certainty.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalStatus {
    /// The client reports a broker acknowledgment.
    Acknowledged,
    /// The client knows the broker could not have accepted the operation.
    DefinitelyNotSent,
    /// The client cannot know whether the broker accepted the operation.
    PossiblySent,
}

/// One correlated event emitted by an adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AdapterEventEnvelope {
    /// Exact control protocol version.
    pub protocol_version: u16,
    /// Command that caused the event.
    pub command_id: CommandId,
    /// Event payload.
    #[serde(flatten)]
    pub event: AdapterEvent,
}

impl AdapterEventEnvelope {
    /// Creates one protocol-v3 event envelope.
    pub fn new(command_id: CommandId, event: AdapterEvent) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            command_id,
            event,
        }
    }
}

/// Normalized public event emitted by an adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdapterEvent {
    /// Successful handshake and capability declaration.
    Ready {
        /// Adapter identity and capabilities.
        descriptor: AdapterDescriptor,
    },
    /// Public client construction completed.
    ClientCreated {
        /// Created client.
        client_id: ClientId,
    },
    /// Public client readiness completed.
    ClientReady {
        /// Ready client.
        client_id: ClientId,
    },
    /// Public producer construction completed.
    ProducerCreated {
        /// Created producer.
        producer_id: ProducerId,
    },
    /// The public producer accepted ownership of one operation.
    OperationAccepted {
        /// Accepted operation.
        operation_id: OperationId,
    },
    /// The public producer rejected admission and retained no ownership.
    OperationRejected {
        /// Rejected operation.
        operation_id: OperationId,
        /// Stable normalized rejection code.
        code: String,
    },
    /// One accepted operation reached its only terminal outcome.
    OperationTerminal {
        /// Settled operation.
        operation_id: OperationId,
        /// Delivery certainty.
        status: TerminalStatus,
        /// Stable normalized outcome code.
        code: Option<String>,
        /// Broker offset when exposed by the public surface.
        offset: Option<i64>,
    },
    /// Producer flush completed.
    FlushCompleted {
        /// Flushed producer.
        producer_id: ProducerId,
    },
    /// Producer close completed.
    ProducerClosed {
        /// Closed producer.
        producer_id: ProducerId,
    },
    /// Client shutdown completed.
    ClientShutdown {
        /// Shut down client.
        client_id: ClientId,
    },
    /// One public client command returned a normal API failure.
    CommandFailed {
        /// Stable normalized client error code.
        code: String,
        /// Bounded public diagnostic retained as evidence.
        diagnostic: String,
    },
    /// Adapter session settled and may exit.
    Finished,
    /// Adapter cannot continue the session.
    Fatal {
        /// Stable adapter failure code.
        code: String,
        /// Bounded diagnostic context.
        diagnostic: String,
    },
}
