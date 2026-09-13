//! Plural Share descriptions require ordered detail, live batches, and ordered CLI state.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminShareGroupDescription, AdminShareGroupDescriptionOutcome,
    AdminShareGroupMember, AdminShareGroupTopicAssignment, AdminShareGroupsDescription,
    BrokerShareGroupState, BrokerStateObservation, ClientId, ConsumedRecord,
    DescribeShareGroupsCommand, HistoryEntry, HistoryPayload, OperationId, Scenario,
    ShareConsumedRecord,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_ordered_descriptions_live_batches_and_cli_state_pass() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn reordered_public_outcomes_fail_the_description_contract() {
    let mut entries = history();
    completion(&mut entries).outcomes.swap(0, 1);
    assert_contract(&violations(&entries));
}

#[test]
fn per_group_error_or_missing_description_fails_the_contract() {
    let mut failed = history();
    completion(&mut failed).outcomes[1].error_code = Some("group_authorization_failed".to_owned());
    assert_contract(&violations(&failed));

    let mut missing = history();
    completion(&mut missing).outcomes[0].description = None;
    assert_contract(&violations(&missing));
}

#[test]
fn lossy_public_assignment_fails_the_description_contract() {
    let mut entries = history();
    completion(&mut entries).outcomes[0]
        .description
        .as_mut()
        .unwrap_or_else(|| panic!("public Share description"))
        .members[0]
        .assignments[0]
        .partitions
        .clear();
    assert_contract(&violations(&entries));
}

#[test]
fn missing_requested_authorized_operations_fails_the_description_contract() {
    let mut entries = history();
    completion(&mut entries).outcomes[0]
        .description
        .as_mut()
        .unwrap_or_else(|| panic!("public Share description"))
        .authorized_operations = None;
    assert_contract(&violations(&entries));
}

#[test]
fn reordered_or_mismatched_cli_state_fails_the_contract() {
    let mut reordered = history();
    reordered.swap(4, 5);
    assert_contract(&violations(&reordered));

    let mut mismatched = history();
    observation(&mut mismatched, 1).member_count = Some(2);
    assert_contract(&violations(&mismatched));
}

#[test]
fn missing_late_or_unfenced_receive_fails_the_contract() {
    let missing = history().into_iter().skip(1).collect::<Vec<_>>();
    assert_contract(&violations(&missing));

    let mut late = history();
    late[0].sequence = 7;
    assert_contract(&violations(&late));

    let mut unfenced = history();
    let HistoryPayload::AdapterEvent { event } = &mut unfenced[1].payload else {
        panic!("Share receive event");
    };
    let AdapterEvent::ShareReceiveCompleted { member_epoch, .. } = &mut event.event else {
        panic!("Share receive completion");
    };
    *member_epoch = Some(0);
    assert_contract(&violations(&unfenced));
}

#[test]
fn changed_authorization_option_fails_the_description_contract() {
    let mut entries = history();
    let HistoryPayload::HarnessCommand { command } = &mut entries[2].payload else {
        panic!("plural Share description command");
    };
    let AdapterCommand::DescribeShareGroups(value) = &mut command.command else {
        panic!("plural Share description command kind");
    };
    value.include_authorized_operations = false;
    assert_contract(&violations(&entries));
}

fn history() -> Vec<HistoryEntry> {
    vec![
        receive(
            1,
            "share-zulu",
            "receive-share-description-zulu",
            zulu_topic(),
        ),
        receive(
            2,
            "share-alpha",
            "receive-share-description-alpha",
            alpha_topic(),
        ),
        command(3, AdapterCommand::DescribeShareGroups(command_payload())),
        event(4, AdapterEvent::ShareGroupsDescribed(completion_value())),
        state(5, 11, zulu_group()),
        state(6, 12, alpha_group()),
    ]
}

fn receive(sequence: u64, consumer: &str, receive_id: &str, topic: &str) -> HistoryEntry {
    event(
        sequence,
        AdapterEvent::ShareReceiveCompleted {
            consumer_id: testlab_schema::ConsumerId::new(consumer)
                .unwrap_or_else(|error| panic!("consumer: {error}")),
            receive_id: operation(receive_id),
            records: vec![ShareConsumedRecord {
                record: ConsumedRecord {
                    topic: topic.to_owned(),
                    partition: 0,
                    offset: 0,
                    timestamp_millis: None,
                    key: None,
                    value: None,
                    headers: Vec::new(),
                },
                delivery_count: 1,
            }],
            acquisition_count: 1,
            member_epoch: Some(1),
            assignment_epoch: Some(1),
        },
    )
}

fn command_payload() -> DescribeShareGroupsCommand {
    DescribeShareGroupsCommand {
        client_id: client(),
        operation_id: operation("admin-describe-share-groups"),
        group_ids: vec![zulu_group().to_owned(), alpha_group().to_owned()],
        include_authorized_operations: true,
        timeout_ms: 20_000,
    }
}

fn completion_value() -> AdminShareGroupsDescription {
    AdminShareGroupsDescription {
        operation_id: operation("admin-describe-share-groups"),
        outcomes: [
            (zulu_group(), zulu_topic(), "testlab-rack-zulu", 1_u8),
            (alpha_group(), alpha_topic(), "testlab-rack-alpha", 2),
        ]
        .into_iter()
        .map(
            |(group_id, topic, rack_id, topic_id)| AdminShareGroupDescriptionOutcome {
                group_id: group_id.to_owned(),
                description: Some(description(group_id, topic, rack_id, topic_id)),
                error_code: None,
            },
        )
        .collect(),
    }
}

fn description(
    group_id: &str,
    topic: &str,
    rack_id: &str,
    topic_id: u8,
) -> AdminShareGroupDescription {
    AdminShareGroupDescription {
        operation_id: operation("admin-describe-share-groups"),
        group_id: group_id.to_owned(),
        state: "Stable".to_owned(),
        group_epoch: 3,
        assignment_epoch: 4,
        assignor_name: "simple".to_owned(),
        authorized_operations: Some(1),
        members: vec![AdminShareGroupMember {
            member_id: format!("member-{topic_id}"),
            rack_id: Some(rack_id.to_owned()),
            member_epoch: 5,
            client_id: format!("client-{topic_id}"),
            subscribed_topics: vec![topic.to_owned()],
            assignments: vec![AdminShareGroupTopicAssignment {
                topic_id: [topic_id; 16],
                topic: topic.to_owned(),
                partitions: vec![0],
            }],
        }],
    }
}

fn state(sequence: u64, ordinal: u64, group_id: &str) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ShareGroup(BrokerShareGroupState {
                observation: ordinal,
                operation_id: operation("admin-describe-share-groups"),
                group_id: group_id.to_owned(),
                exists: true,
                state: Some("Stable".to_owned()),
                member_count: Some(1),
            }),
        },
    }
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-describe-share-groups.toml"
    ))
    .unwrap_or_else(|error| panic!("parse plural Share description scenario: {error}"))
}

fn violations(entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(entries);
    let mut violations = Vec::new();
    verify_admin(&scenario(), &index, &[], &mut violations);
    violations
}

fn completion(entries: &mut [HistoryEntry]) -> &mut AdminShareGroupsDescription {
    let HistoryPayload::AdapterEvent { event } = &mut entries[3].payload else {
        panic!("plural Share description event");
    };
    let AdapterEvent::ShareGroupsDescribed(value) = &mut event.event else {
        panic!("plural Share description completion");
    };
    value
}

fn observation(entries: &mut [HistoryEntry], index: usize) -> &mut BrokerShareGroupState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[4 + index].payload
    else {
        panic!("Share-group observation");
    };
    let BrokerStateObservation::ShareGroup(value) = observation else {
        panic!("Share-group state");
    };
    value
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-042"),
        "{violations:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}

fn zulu_group() -> &'static str {
    "testlab-admin-share-description-zulu"
}
fn alpha_group() -> &'static str {
    "testlab-admin-share-description-alpha"
}
fn zulu_topic() -> &'static str {
    "testlab-kafkars-admin-share-description-zulu"
}
fn alpha_topic() -> &'static str {
    "testlab-kafkars-admin-share-description-alpha"
}
