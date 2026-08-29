//! Concurrent verifier fixtures settle every public handle after the joined actor group.

use testlab_schema::{AdapterCommand, AdapterEvent, ClientId, ConsumerId, ProducerId};

use crate::concurrent_fixture_history_test::HistoryBuilder;

pub(crate) fn settle(
    history: &mut HistoryBuilder,
    client: ClientId,
    producer: ProducerId,
    consumer: ConsumerId,
) {
    history.command(
        "close-consumer",
        AdapterCommand::CloseAssignedConsumer {
            consumer_id: consumer.clone(),
        },
    );
    history.event(
        "close-consumer",
        AdapterEvent::AssignedConsumerClosed {
            consumer_id: consumer,
        },
    );
    history.command(
        "flush",
        AdapterCommand::Flush {
            producer_id: producer.clone(),
        },
    );
    history.event(
        "flush",
        AdapterEvent::FlushCompleted {
            producer_id: producer.clone(),
        },
    );
    history.command(
        "close-producer",
        AdapterCommand::CloseProducer {
            producer_id: producer.clone(),
        },
    );
    history.event(
        "close-producer",
        AdapterEvent::ProducerClosed {
            producer_id: producer,
        },
    );
    history.command(
        "shutdown",
        AdapterCommand::ShutdownClient {
            client_id: client.clone(),
        },
    );
    history.event(
        "shutdown",
        AdapterEvent::ClientShutdown { client_id: client },
    );
    history.command("finish", AdapterCommand::Finish);
    history.event("finish", AdapterEvent::Finished);
}
