//! Consumer command translation keeps semantic expectations in the scenario.

use testlab_schema::{
    AdapterCommand, AssignedConsumerControl, AssignedConsumerControlAction, AssignedStartPosition,
    ClientId, ConsumerId, GroupConsumerConfiguration, GroupConsumerControl,
    GroupConsumerControlAction, GroupConsumerShutdownAction, GroupOffsetReset, GroupProtocol,
    GroupReadIsolation, GroupReceiveSetAction, ObserveGroupAssignmentsAction, OperationId,
    ScenarioAction, TopicPartitionIdentity,
};

use crate::runner_protocol::ExpectedEvent;
use crate::session_command_consumer::translate;

#[test]
fn assignment_observation_strips_expected_partitions() {
    let operation_id = id(OperationId::new("observe-1"));
    let consumer_id = id(ConsumerId::new("consumer-1"));
    let action = ScenarioAction::ObserveGroupAssignments(ObserveGroupAssignmentsAction {
        operation_id: operation_id.clone(),
        consumer_ids: vec![consumer_id.clone()],
        partitions: vec![partition(0), partition(1)],
        timeout_ms: 30_000,
    });

    let Some((AdapterCommand::ObserveGroupAssignments(command), expected)) = translate(&action)
    else {
        panic!("observation must translate");
    };

    assert_eq!(command.operation_id, operation_id);
    assert_eq!(command.consumer_ids, vec![consumer_id]);
    assert!(matches!(
        expected,
        ExpectedEvent::GroupAssignmentsObserved(_)
    ));
}

#[test]
fn receive_set_sends_only_the_structural_record_count() {
    let receive_id = id(OperationId::new("receive-set-1"));
    let action = ScenarioAction::GroupReceiveSet(GroupReceiveSetAction {
        receive_id: receive_id.clone(),
        consumer_ids: vec![
            id(ConsumerId::new("consumer-1")),
            id(ConsumerId::new("consumer-2")),
        ],
        expected_operation_ids: vec![
            id(OperationId::new("send-1")),
            id(OperationId::new("send-2")),
        ],
        timeout_ms: 30_000,
    });

    let Some((AdapterCommand::GroupReceiveSet(command), expected)) = translate(&action) else {
        panic!("receive set must translate");
    };

    assert_eq!(command.receive_id, receive_id);
    assert_eq!(command.record_count, 2);
    assert!(matches!(
        expected,
        ExpectedEvent::GroupReceiveSetCompleted(_)
    ));
}

#[test]
fn assigned_control_preserves_command_and_completion_identity() {
    let operation_id = id(OperationId::new("control-1"));
    let consumer_id = id(ConsumerId::new("consumer-1"));
    let action = ScenarioAction::ControlAssignedConsumer(AssignedConsumerControlAction {
        operation_id: operation_id.clone(),
        consumer_id: consumer_id.clone(),
        control: AssignedConsumerControl::Seek {
            partition: partition(0),
            position: AssignedStartPosition::Offset { offset: 7 },
        },
        timeout_ms: 20_000,
    });
    let Some((AdapterCommand::ControlAssignedConsumer(command), expected)) = translate(&action)
    else {
        panic!("assigned control must translate");
    };
    assert_eq!(command.operation_id, operation_id);
    assert_eq!(command.consumer_id, consumer_id);
    assert_eq!(
        command.control,
        match action {
            ScenarioAction::ControlAssignedConsumer(action) => action.control,
            _ => unreachable!(),
        }
    );
    assert!(matches!(
        expected,
        ExpectedEvent::AssignedConsumerControlCompleted(_)
    ));
}

#[test]
fn group_control_preserves_command_and_completion_identity() {
    let operation_id = id(OperationId::new("group-control-1"));
    let consumer_id = id(ConsumerId::new("consumer-1"));
    let action = ScenarioAction::ControlGroupConsumer(GroupConsumerControlAction {
        operation_id: operation_id.clone(),
        consumer_id: consumer_id.clone(),
        control: GroupConsumerControl::Pause {
            partitions: vec![partition(0)],
        },
        timeout_ms: 30_000,
    });
    let Some((AdapterCommand::ControlGroupConsumer(command), expected)) = translate(&action) else {
        panic!("group control must translate");
    };
    assert_eq!(command.operation_id, operation_id);
    assert_eq!(command.consumer_id, consumer_id);
    assert!(matches!(
        expected,
        ExpectedEvent::GroupConsumerControlCompleted(_)
    ));
}

#[test]
fn group_shutdown_preserves_request_count_and_completion_identity() {
    let operation_id = id(OperationId::new("group-shutdown-1"));
    let consumer_id = id(ConsumerId::new("consumer-1"));
    let action = ScenarioAction::ShutdownGroupConsumer(GroupConsumerShutdownAction {
        operation_id: operation_id.clone(),
        consumer_id: consumer_id.clone(),
        request_count: 2,
        timeout_ms: 45_000,
    });
    let Some((AdapterCommand::ShutdownGroupConsumer(command), expected)) = translate(&action)
    else {
        panic!("group shutdown must translate");
    };
    assert_eq!(command.operation_id, operation_id);
    assert_eq!(command.consumer_id, consumer_id);
    assert_eq!(command.request_count, 2);
    assert!(matches!(
        expected,
        ExpectedEvent::GroupConsumerShutdownCompleted(_)
    ));
}

#[test]
fn group_creation_preserves_public_configuration() {
    let expected_configuration = GroupConsumerConfiguration {
        offset_reset: GroupOffsetReset::Latest,
        read_isolation: GroupReadIsolation::ReadCommitted,
        fetch: Some(testlab_schema::ConsumerFetchConfiguration {
            max_wait_ms: 250,
            min_bytes: 2,
            max_bytes: 524_288,
            partition_max_bytes: 262_144,
            attempt_timeout_ms: 17_000,
        }),
        limits: Some(testlab_schema::ConsumerLimitsConfiguration {
            in_flight_fetches: 3,
            buffered_batches: 4,
            buffered_bytes: 2_097_152,
            max_batch_bytes: 524_288,
        }),
        processing_timeout_ms: Some(60_000),
        membership_start_timeout_ms: Some(25_000),
        seek_timeout_ms: Some(15_000),
        close_timeout_ms: Some(20_000),
        group_instance_id: Some("worker-static-1".to_owned()),
        classic_assignor: Some(testlab_schema::GroupClassicAssignor::CooperativeSticky),
        classic_session_timeout_ms: Some(120_000),
        classic_rebalance_timeout_ms: Some(150_000),
        classic_heartbeat_interval_ms: Some(2_000),
        classic_heartbeat_attempt_timeout_ms: Some(9_000),
        classic_rejoin_backoff_ms: Some(250),
        classic_rejoin_attempt_timeout_ms: Some(45_000),
    };
    let action = ScenarioAction::CreateGroupConsumer {
        client_id: id(ClientId::new("client-1")),
        consumer_id: id(ConsumerId::new("consumer-1")),
        group_id: "workers".to_owned(),
        topics: vec!["orders".to_owned(), "returns".to_owned()],
        protocol: GroupProtocol::Classic,
        configuration: Some(expected_configuration.clone()),
    };
    let Some((
        AdapterCommand::CreateGroupConsumer {
            configuration,
            topics,
            ..
        },
        expected,
    )) = translate(&action)
    else {
        panic!("configured group creation must translate");
    };
    assert_eq!(configuration, Some(expected_configuration));
    assert_eq!(topics, vec!["orders".to_owned(), "returns".to_owned()]);
    assert!(matches!(expected, ExpectedEvent::GroupConsumerCreated(_)));
}

#[test]
fn share_creation_preserves_caller_topics_and_configuration() {
    let configuration = testlab_schema::ShareConsumerFetchConfiguration {
        max_wait_ms: 250,
        min_bytes: 2,
        max_bytes: 524_288,
        max_records: 3,
        batch_size: 1,
        attempt_timeout_ms: 17_000,
    };
    let action = ScenarioAction::CreateShareConsumer {
        client_id: id(ClientId::new("client-1")),
        consumer_id: id(ConsumerId::new("share-1")),
        group_id: "share-workers".to_owned(),
        topics: vec!["orders".to_owned(), "returns".to_owned()],
        rack: Some("rack-a".to_owned()),
        membership_timeout_ms: 25_000,
        close_timeout_ms: 20_000,
        configuration: Some(configuration),
    };

    let Some((
        AdapterCommand::CreateShareConsumer {
            topics,
            rack,
            membership_timeout_ms,
            close_timeout_ms,
            configuration: selected,
            ..
        },
        expected,
    )) = translate(&action)
    else {
        panic!("Share creation must translate");
    };

    assert_eq!(topics, vec!["orders".to_owned(), "returns".to_owned()]);
    assert_eq!(rack.as_deref(), Some("rack-a"));
    assert_eq!(membership_timeout_ms, 25_000);
    assert_eq!(close_timeout_ms, 20_000);
    assert_eq!(selected, Some(configuration));
    assert!(matches!(expected, ExpectedEvent::ShareConsumerCreated(_)));
}

#[test]
fn group_abandonment_preserves_the_exact_consumer_identity() {
    let consumer_id = id(ConsumerId::new("consumer-static"));
    let action = ScenarioAction::AbandonGroupConsumer(testlab_schema::GroupConsumerAbandonment {
        consumer_id: consumer_id.clone(),
    });
    let Some((AdapterCommand::AbandonGroupConsumer(actual), expected)) = translate(&action) else {
        panic!("group abandonment must translate");
    };
    assert_eq!(actual.consumer_id, consumer_id);
    assert!(
        matches!(expected, ExpectedEvent::GroupConsumerAbandoned(value) if value == actual.consumer_id)
    );
}

fn partition(partition: i32) -> TopicPartitionIdentity {
    TopicPartitionIdentity {
        topic: "orders".to_owned(),
        partition,
    }
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
