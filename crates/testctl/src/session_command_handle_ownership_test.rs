//! Child ownership must cross the adapter boundary without inference.

use testlab_schema::{
    AdapterCommand, ChildHandleOwnership, ClientId, ConsumerId, ProducerId, ScenarioAction,
};

#[test]
fn independent_producer_ownership_is_preserved() {
    let action = ScenarioAction::CreateProducer {
        client_id: id(ClientId::new("client-1")),
        producer_id: id(ProducerId::new("producer-1")),
        ownership: ChildHandleOwnership::Independent,
        delivery_timeout_ms: None,
    };

    let Some((AdapterCommand::CreateProducer { ownership, .. }, _)) =
        crate::session_command::translate(&action)
    else {
        panic!("producer creation must translate");
    };

    assert_eq!(ownership, ChildHandleOwnership::Independent);
}

#[test]
fn producer_handle_timeout_is_preserved() {
    let action = ScenarioAction::CreateProducer {
        client_id: id(ClientId::new("client-1")),
        producer_id: id(ProducerId::new("producer-1")),
        ownership: ChildHandleOwnership::Shared,
        delivery_timeout_ms: Some(15_000),
    };

    let Some((
        AdapterCommand::CreateProducer {
            delivery_timeout_ms,
            ..
        },
        _,
    )) = crate::session_command::translate(&action)
    else {
        panic!("producer creation must translate");
    };

    assert_eq!(delivery_timeout_ms, Some(15_000));
}

#[test]
fn independent_assigned_consumer_ownership_is_preserved() {
    let action = ScenarioAction::CreateAssignedConsumer {
        client_id: id(ClientId::new("client-1")),
        consumer_id: id(ConsumerId::new("consumer-1")),
        ownership: ChildHandleOwnership::Independent,
    };

    let Some((AdapterCommand::CreateAssignedConsumer { ownership, .. }, _)) =
        crate::session_command::translate(&action)
    else {
        panic!("assigned-consumer creation must translate");
    };

    assert_eq!(ownership, ChildHandleOwnership::Independent);
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
