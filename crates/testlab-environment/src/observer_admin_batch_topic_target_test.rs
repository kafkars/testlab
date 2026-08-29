//! Batch topic targets preserve request order and exclude verifier-owned expectations.

use testlab_schema::{
    AdapterCommand, ClientId, CreateTopicBatchActionItem, CreateTopicsBatchAction, OperationId,
    ScenarioAction, TOPIC_ALREADY_EXISTS_ERROR_CODE,
};

use crate::observer_admin_target::AdminTarget;

#[test]
fn batch_maps_to_one_exact_command_and_ordered_topic_observations() {
    let action = ScenarioAction::CreateTopicsBatch(CreateTopicsBatchAction {
        client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}")),
        operation_id: operation(),
        topics: vec![
            item("fresh", 2, None),
            item(
                "existing",
                3,
                Some(TOPIC_ALREADY_EXISTS_ERROR_CODE.to_owned()),
            ),
        ],
        timeout_ms: 500,
    });
    let (command, target) = super::observer_admin_batch_topic_target::match_action(&action)
        .unwrap_or_else(|error| panic!("target: {error}"))
        .unwrap_or_else(|| panic!("missing batch target"));

    let AdapterCommand::CreateTopicsBatch(command_value) = &command else {
        panic!("batch command kind");
    };
    assert_eq!(command_value.topics[0].topic, "fresh");
    assert_eq!(command_value.topics[1].topic, "existing");
    let AdminTarget::Topics(target_value) = &target else {
        panic!("batch target kind");
    };
    assert_eq!(target_value.names, ["fresh", "existing"]);
    assert_eq!(target.observation_count(), 2);
    assert_eq!(
        AdminTarget::from_exact(&action, &command).ok().flatten(),
        Some(target)
    );
}

#[test]
fn batch_wire_mismatch_is_rejected() {
    let action = ScenarioAction::CreateTopicsBatch(CreateTopicsBatchAction {
        client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}")),
        operation_id: operation(),
        topics: vec![item("fresh", 1, None), item("existing", 1, None)],
        timeout_ms: 500,
    });
    let (mut command, _) = super::observer_admin_batch_topic_target::match_action(&action)
        .unwrap_or_else(|error| panic!("target: {error}"))
        .unwrap_or_else(|| panic!("missing batch target"));
    let AdapterCommand::CreateTopicsBatch(value) = &mut command else {
        panic!("batch command kind");
    };
    value.topics.swap(0, 1);

    assert!(AdminTarget::from_exact(&action, &command).is_err());
}

fn item(
    topic: &str,
    partitions: i32,
    expected_error_code: Option<String>,
) -> CreateTopicBatchActionItem {
    CreateTopicBatchActionItem {
        topic: topic.to_owned(),
        partitions,
        replication_factor: 1,
        expected_error_code,
    }
}

fn operation() -> OperationId {
    OperationId::new("batch-create").unwrap_or_else(|error| panic!("operation: {error}"))
}
