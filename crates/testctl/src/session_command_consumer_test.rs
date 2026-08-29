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
    };
    let action = ScenarioAction::CreateGroupConsumer {
        client_id: id(ClientId::new("client-1")),
        consumer_id: id(ConsumerId::new("consumer-1")),
        group_id: "workers".to_owned(),
        topic: "orders".to_owned(),
        protocol: GroupProtocol::Consumer,
        configuration: Some(expected_configuration),
    };
    let Some((AdapterCommand::CreateGroupConsumer { configuration, .. }, expected)) =
        translate(&action)
    else {
        panic!("configured group creation must translate");
    };
    assert_eq!(configuration, Some(expected_configuration));
    assert!(matches!(expected, ExpectedEvent::GroupConsumerCreated(_)));
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
