//! Static-member removal requires offline static owners, exact outcomes, and zero-member state.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminConsumerGroupDescription,
    AdminConsumerGroupMemberRemovalOutcome, AdminConsumerGroupMembersRemoval,
    BrokerConsumerGroupState, BrokerStateObservation, ClientId, DescribeConsumerGroupCommand,
    HistoryEntry, HistoryPayload, OperationId, RemoveConsumerGroupMembersCommand, Scenario,
    ScenarioAction,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_retained_baseline_and_ordered_removal_pass() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn reordered_or_failed_public_outcomes_fail() {
    let mut reordered = history();
    removal(&mut reordered).outcomes.swap(0, 1);
    assert_contract(&violations(&reordered));

    let mut failed = history();
    removal(&mut failed).outcomes[1].error_code = Some("broker:broker_25".to_owned());
    assert_contract(&violations(&failed));
}

#[test]
fn altered_baseline_or_remaining_member_fails() {
    let mut baseline = history();
    observed(&mut baseline, 10).member_count = Some(1);
    assert_contract(&violations(&baseline));

    let mut remaining = history();
    observed(&mut remaining, 13).member_count = Some(1);
    assert_contract(&violations(&remaining));
}

fn history() -> Vec<HistoryEntry> {
    vec![
        command(
            1,
            AdapterCommand::AbandonGroupConsumer(testlab_schema::GroupConsumerAbandonment {
                consumer_id: consumer("consumer-zulu"),
            }),
        ),
        event(
            2,
            AdapterEvent::GroupConsumerAbandoned(testlab_schema::GroupConsumerAbandonment {
                consumer_id: consumer("consumer-zulu"),
            }),
        ),
        command(
            3,
            AdapterCommand::ShutdownClient {
                client_id: member_client("client-zulu"),
            },
        ),
        event(
            4,
            AdapterEvent::ClientShutdown {
                client_id: member_client("client-zulu"),
            },
        ),
        command(
            5,
            AdapterCommand::AbandonGroupConsumer(testlab_schema::GroupConsumerAbandonment {
                consumer_id: consumer("consumer-alpha"),
            }),
        ),
        event(
            6,
            AdapterEvent::GroupConsumerAbandoned(testlab_schema::GroupConsumerAbandonment {
                consumer_id: consumer("consumer-alpha"),
            }),
        ),
        command(
            7,
            AdapterCommand::ShutdownClient {
                client_id: member_client("client-alpha"),
            },
        ),
        event(
            8,
            AdapterEvent::ClientShutdown {
                client_id: member_client("client-alpha"),
            },
        ),
        command(
            9,
            AdapterCommand::DescribeConsumerGroup(DescribeConsumerGroupCommand {
                client_id: client(),
                operation_id: baseline_operation(),
                group_id: group_id(),
                include_authorized_operations: false,
                timeout_ms: 20_000,
            }),
        ),
        event(
            10,
            AdapterEvent::ConsumerGroupDescribed(AdminConsumerGroupDescription {
                operation_id: baseline_operation(),
                group_id: group_id(),
                member_count: 2,
                authorized_operations: None,
            }),
        ),
        observation(11, 11, &baseline_operation(), 2),
        command(
            12,
            AdapterCommand::RemoveConsumerGroupMembers(RemoveConsumerGroupMembersCommand {
                client_id: client(),
                operation_id: removal_operation(),
                group_id: group_id(),
                group_instance_ids: identities(),
                reason: "testlab stable-cut static-member removal".to_owned(),
                timeout_ms: 30_000,
            }),
        ),
        event(
            13,
            AdapterEvent::ConsumerGroupMembersRemoved(AdminConsumerGroupMembersRemoval {
                operation_id: removal_operation(),
                group_id: group_id(),
                throttle_time_ms: 7,
                outcomes: identities()
                    .into_iter()
                    .map(|group_instance_id| AdminConsumerGroupMemberRemovalOutcome {
                        group_instance_id,
                        error_code: None,
                    })
                    .collect(),
            }),
        ),
        observation(14, 14, &removal_operation(), 0),
    ]
}

fn observation(
    sequence: u64,
    ordinal: u64,
    operation_id: &OperationId,
    member_count: u32,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ConsumerGroup(BrokerConsumerGroupState {
                observation: ordinal,
                operation_id: operation_id.clone(),
                group_id: group_id(),
                exists: true,
                member_count: Some(member_count),
            }),
        },
    }
}

fn scenario() -> Scenario {
    let mut scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-remove-static-group-members.toml"
    ))
    .unwrap_or_else(|error| panic!("parse static-member removal scenario: {error}"));
    scenario.steps.retain(|step| {
        matches!(&step.action, ScenarioAction::CreateGroupConsumer { .. })
            || matches!(&step.action, ScenarioAction::AbandonGroupConsumer(_))
            || matches!(
                &step.action,
                ScenarioAction::ShutdownClient { client_id }
                    if client_id.as_str() != "admin-client"
            )
            || matches!(
                &step.action,
                ScenarioAction::DescribeConsumerGroup(action)
                    if action.operation_id == baseline_operation()
            )
            || matches!(&step.action, ScenarioAction::RemoveConsumerGroupMembers(_))
    });
    scenario
}

fn violations(entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(entries);
    let mut violations = Vec::new();
    verify_admin(&scenario(), &index, &[], &mut violations);
    violations
}

fn removal(entries: &mut [HistoryEntry]) -> &mut AdminConsumerGroupMembersRemoval {
    let HistoryPayload::AdapterEvent { event } = &mut entries[12].payload else {
        panic!("static-member removal event");
    };
    let AdapterEvent::ConsumerGroupMembersRemoved(value) = &mut event.event else {
        panic!("static-member removal completion");
    };
    value
}

fn observed(entries: &mut [HistoryEntry], index: usize) -> &mut BrokerConsumerGroupState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[index].payload else {
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
            .any(|violation| violation.contract_id.as_str() == "ADMIN-068"),
        "{violations:?}"
    );
}

fn identities() -> Vec<String> {
    vec!["static-zulu".to_owned(), "static-alpha".to_owned()]
}

fn group_id() -> String {
    "testlab-static-member-removal-group".to_owned()
}

fn client() -> ClientId {
    ClientId::new("admin-client").unwrap_or_else(|error| panic!("client: {error}"))
}

fn member_client(value: &str) -> ClientId {
    ClientId::new(value).unwrap_or_else(|error| panic!("client: {error}"))
}

fn consumer(value: &str) -> testlab_schema::ConsumerId {
    testlab_schema::ConsumerId::new(value).unwrap_or_else(|error| panic!("consumer: {error}"))
}

fn baseline_operation() -> OperationId {
    OperationId::new("describe-retained-static-members")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn removal_operation() -> OperationId {
    OperationId::new("remove-static-members").unwrap_or_else(|error| panic!("operation: {error}"))
}
