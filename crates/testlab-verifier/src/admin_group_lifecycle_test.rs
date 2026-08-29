//! Group discovery and deletion tests require matching public and broker-visible identities.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminBrokerError, AdminConsumerGroupCompletion,
    AdminConsumerGroupDescription, AdminConsumerGroupsListing, BrokerConsumerGroupState,
    BrokerStateObservation, DeleteConsumerGroupAction, DeleteConsumerGroupCommand,
    DescribeConsumerGroupAction, DescribeConsumerGroupCommand, HistoryEntry, HistoryPayload,
    ListConsumerGroupsAction, ListConsumerGroupsCommand, OperationId, ScenarioAction,
    TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn complete_sorted_group_listing_passes() {
    assert!(violations(list_action(), &list_history(Vec::new())).is_empty());
}

#[test]
fn group_listing_rejects_retained_broker_errors() {
    let errors = vec![AdminBrokerError {
        broker_id: 1,
        code: 15,
    }];

    assert_contract(
        &violations(list_action(), &list_history(errors)),
        "ADMIN-009",
    );
}

#[test]
fn matching_group_description_passes() {
    assert!(violations(describe_action(), &describe_history(2, 2)).is_empty());
}

#[test]
fn group_description_rejects_independent_member_mismatch() {
    assert_contract(
        &violations(describe_action(), &describe_history(2, 1)),
        "ADMIN-010",
    );
}

#[test]
fn deleted_group_with_independent_absence_passes() {
    assert!(violations(delete_action(), &delete_history(1, false, None)).is_empty());
}

#[test]
fn group_deletion_rejects_duplicate_public_results_or_present_state() {
    for history in [
        delete_history(2, false, None),
        delete_history(1, true, Some(0)),
    ] {
        assert_contract(&violations(delete_action(), &history), "ADMIN-013");
    }
}

fn list_history(broker_errors: Vec<AdminBrokerError>) -> Vec<HistoryEntry> {
    let operation_id = operation("admin-list-groups-1");
    vec![
        command(
            0,
            AdapterCommand::ListConsumerGroups(ListConsumerGroupsCommand {
                client_id: client(),
                operation_id: operation_id.clone(),
                timeout_ms: 1_000,
            }),
        ),
        event(
            1,
            AdapterEvent::ConsumerGroupsListed(AdminConsumerGroupsListing {
                operation_id: operation_id.clone(),
                group_ids: vec!["group-a".to_owned(), "group-b".to_owned()],
                broker_errors,
            }),
        ),
        group_state(2, operation_id.clone(), "group-a", true, Some(0)),
        group_state(3, operation_id, "group-b", true, Some(1)),
    ]
}

fn describe_history(public_members: u32, observed_members: u32) -> Vec<HistoryEntry> {
    let operation_id = operation("admin-describe-group-1");
    vec![
        command(
            0,
            AdapterCommand::DescribeConsumerGroup(DescribeConsumerGroupCommand {
                client_id: client(),
                operation_id: operation_id.clone(),
                group_id: "group-a".to_owned(),
                timeout_ms: 1_000,
            }),
        ),
        event(
            1,
            AdapterEvent::ConsumerGroupDescribed(AdminConsumerGroupDescription {
                operation_id: operation_id.clone(),
                group_id: "group-a".to_owned(),
                member_count: public_members,
            }),
        ),
        group_state(2, operation_id, "group-a", true, Some(observed_members)),
    ]
}

fn delete_history(
    public_count: usize,
    exists: bool,
    member_count: Option<u32>,
) -> Vec<HistoryEntry> {
    let operation_id = operation("admin-delete-group-1");
    let mut history = vec![command(
        0,
        AdapterCommand::DeleteConsumerGroup(DeleteConsumerGroupCommand {
            client_id: client(),
            operation_id: operation_id.clone(),
            group_id: "group-a".to_owned(),
            timeout_ms: 1_000,
        }),
    )];
    for sequence in 1..=public_count {
        history.push(event(
            sequence as u64,
            AdapterEvent::ConsumerGroupDeleted(AdminConsumerGroupCompletion {
                operation_id: operation_id.clone(),
                group_id: "group-a".to_owned(),
            }),
        ));
    }
    let sequence = public_count as u64 + 1;
    history.push(group_state(
        sequence,
        operation_id,
        "group-a",
        exists,
        member_count,
    ));
    history
}

fn list_action() -> ScenarioAction {
    ScenarioAction::ListConsumerGroups(ListConsumerGroupsAction {
        client_id: client(),
        operation_id: operation("admin-list-groups-1"),
        required_group_ids: vec!["group-a".to_owned(), "group-b".to_owned()],
        timeout_ms: 1_000,
    })
}

fn describe_action() -> ScenarioAction {
    ScenarioAction::DescribeConsumerGroup(DescribeConsumerGroupAction {
        client_id: client(),
        operation_id: operation("admin-describe-group-1"),
        group_id: "group-a".to_owned(),
        expected_member_count: 2,
        timeout_ms: 1_000,
    })
}

fn delete_action() -> ScenarioAction {
    ScenarioAction::DeleteConsumerGroup(DeleteConsumerGroupAction {
        client_id: client(),
        operation_id: operation("admin-delete-group-1"),
        group_id: "group-a".to_owned(),
        timeout_ms: 1_000,
    })
}

fn group_state(
    sequence: u64,
    operation_id: OperationId,
    group_id: &str,
    exists: bool,
    member_count: Option<u32>,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ConsumerGroup(BrokerConsumerGroupState {
                observation: sequence,
                operation_id,
                group_id: group_id.to_owned(),
                exists,
                member_count,
            }),
        },
    }
}

fn violations(action: ScenarioAction, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.steps.insert(2, step("admin-operation", action));
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    verify_admin(&scenario, &index, &[], &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "{violations:?}"
    );
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
