//! Filtered group-listing verdicts bind exact commands to live broker identities.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminConsumerGroupsListing, BrokerConsumerGroupState,
    BrokerStateObservation, GroupListingApi, HistoryEntry, HistoryPayload,
    ListConsumerGroupsAction, ListConsumerGroupsCommand, OperationId, ScenarioAction,
    TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn both_filtered_group_listing_paths_pass() {
    for api in [GroupListingApi::ConsumerGroups, GroupListingApi::AllGroups] {
        let action = action(api);
        assert!(violations(&action, &history(&action)).is_empty());
    }
}

#[test]
fn substituted_filter_or_duplicate_completion_fails_filter_contract() {
    let action = action(GroupListingApi::AllGroups);
    let mut substituted = history(&action);
    command_value(&mut substituted)
        .protocol_type_filters
        .clear();
    assert_contract(&violations(&action, &substituted));

    let mut duplicated = history(&action);
    duplicated.insert(2, duplicated[1].clone());
    assert_contract(&violations(&action, &duplicated));
}

fn action(api: GroupListingApi) -> ListConsumerGroupsAction {
    ListConsumerGroupsAction {
        client_id: client(),
        operation_id: operation(),
        api,
        state_filters: vec!["Stable".to_owned()],
        group_type_filters: vec!["classic".to_owned()],
        protocol_type_filters: match api {
            GroupListingApi::ConsumerGroups => Vec::new(),
            GroupListingApi::AllGroups => vec!["consumer".to_owned()],
        },
        required_group_ids: vec!["group-a".to_owned()],
        timeout_ms: 1_000,
    }
}

fn history(action: &ListConsumerGroupsAction) -> Vec<HistoryEntry> {
    vec![
        command(
            0,
            AdapterCommand::ListConsumerGroups(ListConsumerGroupsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                api: action.api,
                state_filters: action.state_filters.clone(),
                group_type_filters: action.group_type_filters.clone(),
                protocol_type_filters: action.protocol_type_filters.clone(),
                timeout_ms: action.timeout_ms,
            }),
        ),
        event(
            1,
            AdapterEvent::ConsumerGroupsListed(AdminConsumerGroupsListing {
                operation_id: action.operation_id.clone(),
                group_ids: vec!["group-a".to_owned()],
                broker_errors: Vec::new(),
            }),
        ),
        HistoryEntry {
            sequence: 2,
            observed_unix_ms: 2,
            payload: HistoryPayload::BrokerStateObservation {
                observation: BrokerStateObservation::ConsumerGroup(BrokerConsumerGroupState {
                    observation: 2,
                    operation_id: action.operation_id.clone(),
                    group_id: "group-a".to_owned(),
                    exists: true,
                    member_count: Some(1),
                }),
            },
        },
    ]
}

fn command_value(history: &mut [HistoryEntry]) -> &mut ListConsumerGroupsCommand {
    let HistoryPayload::HarnessCommand { command } = &mut history[0].payload else {
        panic!("group-listing command")
    };
    let AdapterCommand::ListConsumerGroups(value) = &mut command.command else {
        panic!("group-listing payload")
    };
    value
}

fn violations(
    action: &ListConsumerGroupsAction,
    history: &[HistoryEntry],
) -> Vec<testlab_schema::Violation> {
    let mut fixture = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    fixture.steps.insert(
        2,
        step(
            "filtered-group-listing",
            ScenarioAction::ListConsumerGroups(action.clone()),
        ),
    );
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    verify_admin(&fixture, &index, &[], &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-082"),
        "{violations:?}"
    );
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-filtered-groups")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}
