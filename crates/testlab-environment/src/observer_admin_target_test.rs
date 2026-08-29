//! Topic target tests pin exact correlation and independent topology expectations.

use testlab_schema::{
    AdapterCommand, AdminOffsetPosition, ClientId, CreatePartitionsAction, CreateTopicAction,
    DeleteRecordsAction, DeleteTopicAction, DescribeTopicAction, ListOffsetsAction,
    ListTopicsAction, OperationId, ScenarioAction,
};

use crate::observer_admin_target::AdminTarget;

#[test]
fn topic_mutations_map_to_exact_expected_topology() {
    let create = ScenarioAction::CreateTopic(CreateTopicAction {
        client_id: client(),
        operation_id: operation("create-topic"),
        topic: "orders".to_owned(),
        partitions: 3,
        replication_factor: 2,
        validate_only: false,
        expected_error_code: None,
        timeout_ms: 500,
    });
    let create_target = exact(&create);
    let AdminTarget::Topic(create_target) = create_target else {
        panic!("create topic target kind");
    };
    assert_eq!(create_target.expected_partitions, Some(vec![0, 1, 2]));
    assert!(create_target.expected_exists);
    assert!(create_target.poll_expected);

    let ScenarioAction::CreateTopic(mut duplicate) = create.clone() else {
        panic!("create topic action kind");
    };
    duplicate.expected_error_code =
        Some(testlab_schema::TOPIC_ALREADY_EXISTS_ERROR_CODE.to_owned());
    assert_eq!(
        exact(&ScenarioAction::CreateTopic(duplicate)),
        AdminTarget::Topic(create_target)
    );

    let expand = ScenarioAction::CreatePartitions(CreatePartitionsAction {
        client_id: client(),
        operation_id: operation("expand-topic"),
        topic: "orders".to_owned(),
        total_count: 4,
        validate_only: false,
        expected_current_count: None,
        expected_error_code: None,
        timeout_ms: 600,
    });
    let AdminTarget::Topic(expand_target) = exact(&expand) else {
        panic!("create partitions target kind");
    };
    assert_eq!(expand_target.expected_partitions, Some(vec![0, 1, 2, 3]));
    assert!(expand_target.expected_exists);

    let delete = ScenarioAction::DeleteTopic(DeleteTopicAction {
        client_id: client(),
        operation_id: operation("delete-topic"),
        topic: "orders".to_owned(),
        expected_error_code: None,
        timeout_ms: 700,
    });
    let AdminTarget::Topic(delete_target) = exact(&delete) else {
        panic!("delete topic target kind");
    };
    assert!(!delete_target.expected_exists);
    assert!(delete_target.poll_expected);
}

#[test]
fn topic_reads_keep_scenario_only_expectations_out_of_wire_matching() {
    let describe = ScenarioAction::DescribeTopic(DescribeTopicAction {
        client_id: client(),
        operation_id: operation("describe-topic"),
        topic: "orders".to_owned(),
        expected_partitions: Some(vec![0, 1]),
        expected_error_code: None,
        timeout_ms: 500,
    });
    let AdminTarget::Topic(target) = exact(&describe) else {
        panic!("describe topic target kind");
    };
    assert_eq!(target.expected_partitions, Some(vec![0, 1]));
    assert!(!target.poll_expected);

    let list = ScenarioAction::ListTopics(ListTopicsAction {
        client_id: client(),
        operation_id: operation("list-topics"),
        include_internal: false,
        required_topics: vec!["orders".to_owned(), "audit".to_owned()],
        timeout_ms: 500,
    });
    let AdminTarget::Topics(target) = exact(&list) else {
        panic!("list topics target kind");
    };
    assert_eq!(target.names, ["orders", "audit"]);
}

#[test]
fn expected_admin_failures_map_to_immediate_broker_truth() {
    let expected_error_code =
        Some(testlab_schema::UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE.to_owned());
    for action in [
        ScenarioAction::CreatePartitions(CreatePartitionsAction {
            client_id: client(),
            operation_id: operation("expand-missing"),
            topic: "missing-expand".to_owned(),
            total_count: 2,
            validate_only: false,
            expected_current_count: None,
            expected_error_code: expected_error_code.clone(),
            timeout_ms: 500,
        }),
        ScenarioAction::DeleteTopic(DeleteTopicAction {
            client_id: client(),
            operation_id: operation("delete-missing"),
            topic: "missing-delete".to_owned(),
            expected_error_code: expected_error_code.clone(),
            timeout_ms: 500,
        }),
        ScenarioAction::DescribeTopic(DescribeTopicAction {
            client_id: client(),
            operation_id: operation("describe-missing"),
            topic: "missing-describe".to_owned(),
            expected_partitions: None,
            expected_error_code: expected_error_code.clone(),
            timeout_ms: 500,
        }),
    ] {
        let AdminTarget::Topic(target) = exact(&action) else {
            panic!("expected failure topic target kind");
        };
        assert!(!target.expected_exists);
        assert_eq!(target.expected_partitions, None);
        assert!(!target.poll_expected);
    }

    let action = ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id: client(),
        operation_id: operation("list-missing-partition"),
        topic: "offsets".to_owned(),
        partition: 1,
        position: AdminOffsetPosition::Latest,
        expected_offset: None,
        expected_error_code,
        timeout_ms: 500,
    });
    let AdminTarget::Topic(target) = exact(&action) else {
        panic!("list offsets failure topic target kind");
    };
    assert!(target.expected_exists);
    assert_eq!(target.expected_partitions, Some(vec![0]));
    assert!(!target.poll_expected);

    let authorization = ScenarioAction::CreateTopic(CreateTopicAction {
        client_id: client(),
        operation_id: operation("create-denied"),
        topic: "denied".to_owned(),
        partitions: 2,
        replication_factor: 1,
        validate_only: false,
        expected_error_code: Some(testlab_schema::ADMIN_TOPIC_AUTHORIZATION_ERROR_CODE.to_owned()),
        timeout_ms: 500,
    });
    let AdminTarget::Topic(target) = exact(&authorization) else {
        panic!("authorization failure topic target kind");
    };
    assert!(!target.expected_exists);
    assert_eq!(target.expected_partitions, None);
    assert!(!target.poll_expected);
}

#[test]
fn any_wire_payload_mismatch_is_rejected() {
    let action = ScenarioAction::DeleteTopic(DeleteTopicAction {
        client_id: client(),
        operation_id: operation("delete-topic"),
        topic: "orders".to_owned(),
        expected_error_code: None,
        timeout_ms: 500,
    });
    let (mut command, _) = matched(&action);
    let AdapterCommand::DeleteTopic(payload) = &mut command else {
        panic!("delete topic command kind");
    };
    payload.timeout_ms += 1;

    assert!(AdminTarget::from_exact(&action, &command).is_err());
}

#[test]
fn record_deletion_maps_to_polling_watermark_expectations() {
    let action = ScenarioAction::DeleteRecords(DeleteRecordsAction {
        client_id: client(),
        operation_id: operation("delete-records"),
        topic: "orders".to_owned(),
        partition: 2,
        before_offset: 4,
        expected_high_watermark: 6,
        timeout_ms: 500,
    });
    let (command, target) = crate::observer_admin_partition_offsets_target::match_action(&action)
        .unwrap_or_else(|| panic!("missing delete records target"));
    let AdminTarget::PartitionOffsets(target) = target else {
        panic!("delete records target kind");
    };

    assert!(AdminTarget::from_exact(&action, &command).is_ok());
    assert_eq!(target.expected_low, Some(4));
    assert_eq!(target.expected_high, Some(6));
    assert!(target.poll_expected);
}

#[test]
fn duplicate_scenario_targets_are_rejected() {
    let action = ScenarioAction::ListTopics(ListTopicsAction {
        client_id: client(),
        operation_id: operation("list-topics"),
        include_internal: false,
        required_topics: vec!["orders".to_owned(), "orders".to_owned()],
        timeout_ms: 500,
    });
    let command = AdapterCommand::ListTopics(testlab_schema::ListTopicsCommand {
        client_id: client(),
        operation_id: operation("list-topics"),
        include_internal: false,
        timeout_ms: 500,
    });

    assert!(AdminTarget::from_exact(&action, &command).is_err());
}

#[test]
fn unsupported_action_has_no_admin_observation_target() {
    let action = ScenarioAction::CreateClient {
        client_id: client(),
    };
    let command = AdapterCommand::CreateClient {
        client_id: client(),
    };

    assert_eq!(AdminTarget::from_exact(&action, &command).ok(), Some(None));
}

fn exact(action: &ScenarioAction) -> AdminTarget {
    let (command, target) = matched(action);
    assert_eq!(
        AdminTarget::from_exact(action, &command).ok().flatten(),
        Some(target.clone())
    );
    target
}

fn matched(action: &ScenarioAction) -> (AdapterCommand, AdminTarget) {
    crate::observer_admin_topic_target::match_action(action)
        .ok()
        .flatten()
        .unwrap_or_else(|| panic!("topic action must match"))
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
