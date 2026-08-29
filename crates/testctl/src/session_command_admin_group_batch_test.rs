//! Plural group-admin translation tests keep verifier expectations off the wire.

use testlab_schema::{
    AdapterCommand, AlterConsumerGroupOffsetsAction, AlterConsumerGroupOffsetsCommand,
    ClassicGroupExpectation, ClientId, ConsumerGroupOffsetAlteration,
    ConsumerGroupOffsetExpectation, ConsumerGroupOffsetSelection, ConsumerGroupOffsetsExpectation,
    ConsumerGroupOffsetsSelection, DeleteConsumerGroupOffsetsAction,
    DeleteConsumerGroupOffsetsCommand, DescribeClassicGroupsAction, DescribeClassicGroupsCommand,
    ListConsumerGroupOffsetsBatchAction, ListConsumerGroupOffsetsBatchCommand,
    ListConsumerGroupsOffsetsAction, ListConsumerGroupsOffsetsCommand, OperationId, ScenarioAction,
};

use crate::runner_protocol::ExpectedEvent;
use crate::session_command_admin_group_batch::translate;

#[test]
fn one_group_listing_strips_expected_offsets() {
    let action =
        ScenarioAction::ListConsumerGroupOffsetsBatch(ListConsumerGroupOffsetsBatchAction {
            client_id: client(),
            operation_id: operation("list-one"),
            group_id: "group-a".to_owned(),
            require_stable: true,
            partitions: expectations(),
            timeout_ms: 2_000,
        });
    let Some((command, expected)) = translate(&action) else {
        panic!("plural listing must translate");
    };
    assert_eq!(
        command,
        AdapterCommand::ListConsumerGroupOffsetsBatch(ListConsumerGroupOffsetsBatchCommand {
            client_id: client(),
            operation_id: operation("list-one"),
            group_id: "group-a".to_owned(),
            require_stable: true,
            partitions: selections(),
            timeout_ms: 2_000,
        })
    );
    assert!(matches!(
        expected,
        ExpectedEvent::ConsumerGroupOffsetsListed { operation_id }
            if operation_id == operation("list-one")
    ));
}

#[test]
fn multi_group_listing_preserves_nested_caller_order() {
    let action = ScenarioAction::ListConsumerGroupsOffsets(ListConsumerGroupsOffsetsAction {
        client_id: client(),
        operation_id: operation("list-many"),
        require_stable: false,
        groups: vec![group_expectation("group-b"), group_expectation("group-a")],
        timeout_ms: 2_000,
    });
    let Some((command, expected)) = translate(&action) else {
        panic!("multi-group listing must translate");
    };
    assert_eq!(
        command,
        AdapterCommand::ListConsumerGroupsOffsets(ListConsumerGroupsOffsetsCommand {
            client_id: client(),
            operation_id: operation("list-many"),
            require_stable: false,
            groups: vec![group_selection("group-b"), group_selection("group-a")],
            timeout_ms: 2_000,
        })
    );
    assert!(matches!(
        expected,
        ExpectedEvent::ConsumerGroupsOffsetsListed { operation_id }
            if operation_id == operation("list-many")
    ));
}

#[test]
fn plural_mutations_preserve_exact_ordered_targets() {
    let offsets = alterations();
    let alter = ScenarioAction::AlterConsumerGroupOffsets(AlterConsumerGroupOffsetsAction {
        client_id: client(),
        operation_id: operation("alter-many"),
        group_id: "group-a".to_owned(),
        offsets: offsets.clone(),
        timeout_ms: 2_000,
    });
    let delete = ScenarioAction::DeleteConsumerGroupOffsets(DeleteConsumerGroupOffsetsAction {
        client_id: client(),
        operation_id: operation("delete-many"),
        group_id: "group-a".to_owned(),
        partitions: selections(),
        timeout_ms: 2_000,
    });
    assert_eq!(
        translate(&alter).map(|value| value.0),
        Some(AdapterCommand::AlterConsumerGroupOffsets(
            AlterConsumerGroupOffsetsCommand {
                client_id: client(),
                operation_id: operation("alter-many"),
                group_id: "group-a".to_owned(),
                offsets,
                timeout_ms: 2_000,
            }
        ))
    );
    assert_eq!(
        translate(&delete).map(|value| value.0),
        Some(AdapterCommand::DeleteConsumerGroupOffsets(
            DeleteConsumerGroupOffsetsCommand {
                client_id: client(),
                operation_id: operation("delete-many"),
                group_id: "group-a".to_owned(),
                partitions: selections(),
                timeout_ms: 2_000,
            }
        ))
    );
}

#[test]
fn classic_description_strips_expected_member_counts() {
    let action = ScenarioAction::DescribeClassicGroups(DescribeClassicGroupsAction {
        client_id: client(),
        operation_id: operation("describe-classic"),
        groups: vec![classic("group-b", 2), classic("group-a", 1)],
        timeout_ms: 2_000,
    });
    let Some((command, expected)) = translate(&action) else {
        panic!("classic description must translate");
    };
    assert_eq!(
        command,
        AdapterCommand::DescribeClassicGroups(DescribeClassicGroupsCommand {
            client_id: client(),
            operation_id: operation("describe-classic"),
            group_ids: vec!["group-b".to_owned(), "group-a".to_owned()],
            timeout_ms: 2_000,
        })
    );
    assert!(matches!(
        expected,
        ExpectedEvent::ClassicGroupsDescribed { operation_id }
            if operation_id == operation("describe-classic")
    ));
}

fn expectations() -> Vec<ConsumerGroupOffsetExpectation> {
    vec![
        ConsumerGroupOffsetExpectation {
            topic: "topic-b".to_owned(),
            partition: 1,
            expected_offset: 8,
        },
        ConsumerGroupOffsetExpectation {
            topic: "topic-a".to_owned(),
            partition: 0,
            expected_offset: 5,
        },
    ]
}

fn selections() -> Vec<ConsumerGroupOffsetSelection> {
    expectations()
        .into_iter()
        .map(|value| ConsumerGroupOffsetSelection {
            topic: value.topic,
            partition: value.partition,
        })
        .collect()
}

fn alterations() -> Vec<ConsumerGroupOffsetAlteration> {
    expectations()
        .into_iter()
        .map(|value| ConsumerGroupOffsetAlteration {
            topic: value.topic,
            partition: value.partition,
            offset: value.expected_offset,
        })
        .collect()
}

fn group_expectation(group_id: &str) -> ConsumerGroupOffsetsExpectation {
    ConsumerGroupOffsetsExpectation {
        group_id: group_id.to_owned(),
        partitions: expectations(),
    }
}

fn group_selection(group_id: &str) -> ConsumerGroupOffsetsSelection {
    ConsumerGroupOffsetsSelection {
        group_id: group_id.to_owned(),
        partitions: selections(),
    }
}

fn classic(group_id: &str, expected_member_count: u32) -> ClassicGroupExpectation {
    ClassicGroupExpectation {
        group_id: group_id.to_owned(),
        expected_member_count,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
