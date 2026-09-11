//! Consumer-group batch deletion requires ordered public and independent before-and-after facts.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminClassicGroupDescriptionOutcome,
    AdminClassicGroupsDescription, AdminConsumerGroupDeletionOutcome, AdminConsumerGroupsDeletion,
    BrokerConsumerGroupState, BrokerStateObservation, ClientId, DeleteConsumerGroupsCommand,
    DescribeClassicGroupsCommand, HistoryEntry, HistoryPayload, OperationId, Scenario,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_ordered_deletion_with_before_and_after_facts_passes() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn reordered_or_failed_public_outcomes_fail_the_contract() {
    let mut reordered = history();
    deletion(&mut reordered).outcomes.swap(0, 1);
    assert_contract(&violations(&reordered));

    let mut failed = history();
    deletion(&mut failed).outcomes[1].error_code = Some("broker:broker_69".to_owned());
    assert_contract(&violations(&failed));
}

#[test]
fn missing_baseline_or_remaining_group_fails_the_contract() {
    let missing_baseline = history()
        .into_iter()
        .filter(|entry| entry.sequence != 3)
        .collect::<Vec<_>>();
    assert_contract(&violations(&missing_baseline));

    let mut present = history();
    observed(&mut present, 1).exists = true;
    observed(&mut present, 1).member_count = Some(0);
    assert_contract(&violations(&present));
}

fn history() -> Vec<HistoryEntry> {
    vec![
        command(
            1,
            AdapterCommand::DescribeClassicGroups(DescribeClassicGroupsCommand {
                client_id: client(),
                operation_id: description_operation(),
                group_ids: group_ids(),
                timeout_ms: 20_000,
            }),
        ),
        event(
            2,
            AdapterEvent::ClassicGroupsDescribed(AdminClassicGroupsDescription {
                operation_id: description_operation(),
                outcomes: group_ids()
                    .into_iter()
                    .map(|group_id| AdminClassicGroupDescriptionOutcome {
                        group_id,
                        member_count: Some(0),
                        error_code: None,
                    })
                    .collect(),
            }),
        ),
        observation(
            3,
            3,
            &description_operation(),
            &group_ids()[0],
            true,
            Some(0),
        ),
        observation(
            4,
            4,
            &description_operation(),
            &group_ids()[1],
            true,
            Some(0),
        ),
        command(
            5,
            AdapterCommand::DeleteConsumerGroups(DeleteConsumerGroupsCommand {
                client_id: client(),
                operation_id: deletion_operation(),
                group_ids: group_ids(),
                timeout_ms: 30_000,
            }),
        ),
        event(
            6,
            AdapterEvent::ConsumerGroupsDeleted(AdminConsumerGroupsDeletion {
                operation_id: deletion_operation(),
                outcomes: group_ids()
                    .into_iter()
                    .map(|group_id| AdminConsumerGroupDeletionOutcome {
                        group_id,
                        error_code: None,
                    })
                    .collect(),
            }),
        ),
        observation(7, 7, &deletion_operation(), &group_ids()[0], false, None),
        observation(8, 8, &deletion_operation(), &group_ids()[1], false, None),
    ]
}

fn observation(
    sequence: u64,
    ordinal: u64,
    operation_id: &OperationId,
    group_id: &str,
    exists: bool,
    member_count: Option<u32>,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ConsumerGroup(BrokerConsumerGroupState {
                observation: ordinal,
                operation_id: operation_id.clone(),
                group_id: group_id.to_owned(),
                exists,
                member_count,
            }),
        },
    }
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-delete-consumer-groups.toml"
    ))
    .unwrap_or_else(|error| panic!("parse consumer-group deletion scenario: {error}"))
}

fn violations(entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(entries);
    let mut violations = Vec::new();
    verify_admin(&scenario(), &index, &[], &mut violations);
    violations
}

fn deletion(entries: &mut [HistoryEntry]) -> &mut AdminConsumerGroupsDeletion {
    let HistoryPayload::AdapterEvent { event } = &mut entries[5].payload else {
        panic!("consumer-group deletion event");
    };
    let AdapterEvent::ConsumerGroupsDeleted(value) = &mut event.event else {
        panic!("consumer-group deletion completion");
    };
    value
}

fn observed(entries: &mut [HistoryEntry], index: usize) -> &mut BrokerConsumerGroupState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[6 + index].payload
    else {
        panic!("consumer-group observation");
    };
    let BrokerStateObservation::ConsumerGroup(value) = observation else {
        panic!("consumer-group state");
    };
    value
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-046"),
        "{violations:?}"
    );
}

fn group_ids() -> Vec<String> {
    vec![
        "testlab-admin-delete-consumer-group-zulu".to_owned(),
        "testlab-admin-delete-consumer-group-alpha".to_owned(),
    ]
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn description_operation() -> OperationId {
    OperationId::new("admin-describe-empty-consumer-groups")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn deletion_operation() -> OperationId {
    OperationId::new("admin-delete-consumer-groups")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
