//! State errors distinguish adapter lifecycle misuse from public client failures.

#[cfg(kafkars_share_candidate)]
use testlab_schema::OperationId;
use testlab_schema::{ClientId, ConsumerId, ProducerId};
use thiserror::Error;

use crate::connection_security::SecurityError;

#[derive(Debug, Error)]
pub(crate) enum StateError {
    #[error("hello must be the first command")]
    HelloRequired,
    #[error("hello was received more than once")]
    DuplicateHello,
    #[error("client {0} already exists")]
    DuplicateClient(ClientId),
    #[error("client {0} does not exist")]
    MissingClient(ClientId),
    #[error("producer {0} already exists")]
    DuplicateProducer(ProducerId),
    #[error("producer {0} does not exist")]
    MissingProducer(ProducerId),
    #[error("consumer {0} already exists")]
    DuplicateConsumer(ConsumerId),
    #[error("consumer {0} does not exist")]
    MissingConsumer(ConsumerId),
    #[cfg(kafkars_share_candidate)]
    #[error("share batch {0} already exists")]
    DuplicateShareBatch(OperationId),
    #[cfg(kafkars_share_candidate)]
    #[error("share batch {0} does not exist")]
    MissingShareBatch(OperationId),
    #[cfg(kafkars_share_candidate)]
    #[error("share batch {receive_id} is not owned by consumer {consumer_id}")]
    ShareBatchOwner {
        receive_id: OperationId,
        consumer_id: ConsumerId,
    },
    #[cfg(kafkars_share_candidate)]
    #[error("packaged Kafkars share surface was invalid: {0}")]
    ShareSurface(String),
    #[error("client {0} still owns an open producer")]
    OpenProducer(ClientId),
    #[error("client {0} still owns an open consumer")]
    OpenConsumer(ClientId),
    #[error("adapter finished with open producers")]
    UnclosedProducers,
    #[error("adapter finished with open consumers")]
    UnclosedConsumers,
    #[error("adapter finished with open clients")]
    UnclosedClients,
    #[error("packaged Kafkars operation failed: {0}")]
    Client(kafkars::KafkaError),
    #[error("adapter connection security failed: {0}")]
    Security(#[from] SecurityError),
}
