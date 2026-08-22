//! State errors distinguish adapter lifecycle misuse from public client failures.

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
