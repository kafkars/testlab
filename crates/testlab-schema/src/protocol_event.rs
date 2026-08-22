//! Adapter events normalize only facts exposed through a public client surface.

use serde::{Deserialize, Serialize};

use crate::{
    AdapterDescriptor, ClientId, ConsumedRecord, ConsumerId, GroupMembershipEpoch, OperationId,
    ProducerId, TerminalStatus, TransactionDisposition,
};

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
    /// One public batch call emitted every per-operation outcome.
    BatchCompleted {
        /// Producer that handled the batch.
        producer_id: ProducerId,
    },
    /// One directly assigned consumer was claimed.
    AssignedConsumerCreated {
        /// Created consumer.
        consumer_id: ConsumerId,
    },
    /// One direct assignment replacement completed.
    AssignmentCompleted {
        /// Assigned consumer.
        consumer_id: ConsumerId,
    },
    /// One bounded receive observation completed.
    ReceiveCompleted {
        /// Stable receive operation identity.
        receive_id: OperationId,
        /// Exact records returned through the public API.
        records: Vec<ConsumedRecord>,
    },
    /// One directly assigned consumer closed.
    AssignedConsumerClosed {
        /// Closed consumer.
        consumer_id: ConsumerId,
    },
    /// One consumer-group member registered.
    GroupConsumerCreated {
        /// Created consumer.
        consumer_id: ConsumerId,
    },
    /// One group batch observation and checkpoint attempt completed.
    GroupReceiveCompleted {
        /// Stable receive operation identity.
        receive_id: OperationId,
        /// Exact records returned through the public API.
        records: Vec<ConsumedRecord>,
        /// Whether the assignment-fenced checkpoint committed.
        committed: bool,
        /// Public group metadata observed after the commit.
        group_epoch: Option<GroupMembershipEpoch>,
    },
    /// One group consumer closed.
    GroupConsumerClosed {
        /// Closed consumer.
        consumer_id: ConsumerId,
    },
    /// One public admin topic creation completed successfully.
    TopicCreated {
        /// Stable admin operation identity.
        operation_id: OperationId,
        /// Exact topic reported by the public batch result.
        topic: String,
    },
    /// Public transactional producer initialization completed.
    TransactionalProducerCreated {
        /// Created transactional producer.
        producer_id: ProducerId,
    },
    /// One public transaction reached an observed terminal disposition.
    TransactionCompleted {
        /// Stable transaction operation identity.
        transaction_id: OperationId,
        /// Observed commit or abort outcome.
        disposition: TransactionDisposition,
    },
    /// One idle public transactional producer closed.
    TransactionalProducerClosed {
        /// Closed transactional producer.
        producer_id: ProducerId,
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
