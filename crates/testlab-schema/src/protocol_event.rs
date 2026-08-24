//! Adapter events normalize only facts exposed through a public client surface.

use serde::{Deserialize, Serialize};

use crate::{
    AdapterDescriptor, ClientId, ConsumedRecord, ConsumerId, GroupMembershipEpoch, OperationId,
    ProducerId, ShareConsumedRecord, ShareDisposition, TerminalStatus, TransactionDisposition,
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
    /// One share-group member registered.
    ShareConsumerCreated {
        /// Created share consumer.
        consumer_id: ConsumerId,
    },
    /// One share batch was retained behind its receive identity.
    ShareReceiveCompleted {
        /// Share consumer that retained the batch.
        consumer_id: ConsumerId,
        /// Stable retained-batch identity.
        receive_id: OperationId,
        /// Exact records returned by the public API.
        records: Vec<ShareConsumedRecord>,
        /// Positive broker member epoch when observed.
        member_epoch: Option<i32>,
        /// Positive local assignment fence when observed.
        assignment_epoch: Option<u64>,
    },
    /// One retained share batch reached an acknowledged broker terminal.
    ShareAcknowledgementCompleted {
        /// Stable acknowledgement identity.
        acknowledgement_id: OperationId,
        /// Retained batch consumed by the acknowledgement.
        receive_id: OperationId,
        /// Public disposition sent for every record.
        disposition: ShareDisposition,
        /// Whether every partition reached a successful broker terminal.
        success: bool,
        /// Exact delivery certainty for a failed acknowledgement.
        delivery: Option<TerminalStatus>,
        /// Stable normalized failure code, when failed.
        code: Option<String>,
    },
    /// One retained batch was dropped without network acknowledgement.
    ShareBatchDropped {
        /// Retained batch abandoned without acknowledgement.
        receive_id: OperationId,
    },
    /// One share close reached its public terminal, including uncertainty.
    ShareConsumerClosed {
        /// Unique share consumer consumed by close.
        consumer_id: ConsumerId,
        /// Whether the public close succeeded.
        success: bool,
        /// Exact delivery certainty for a failed close.
        delivery: Option<TerminalStatus>,
        /// Stable normalized failure code, when failed.
        code: Option<String>,
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
    /// One old transaction exposed its public commit result after replacement initialization.
    TransactionFenceCompleted {
        /// Stable fenced transaction identity.
        transaction_id: OperationId,
        /// Normalized public commit error, or none when the old commit unexpectedly succeeded.
        commit_error_code: Option<String>,
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
    /// Adapter session was abandoned after a scenario failure.
    Aborted,
    /// Adapter cannot continue the session.
    Fatal {
        /// Stable adapter failure code.
        code: String,
        /// Bounded diagnostic context.
        diagnostic: String,
    },
}
