//! Plural topic-configuration verdicts require ordered public and independent values.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicConfigDescriptionOutcome, AdminTopicConfigsDescription,
    BrokerStateObservation, BrokerTopicConfigState, ClientId, DescribeTopicConfigsCommand,
    HistoryEntry, HistoryPayload, OperationId, Scenario, TopicConfigSelection,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_ordered_public_and_independent_values_pass() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn reordered_duplicated_or_failed_public_outcomes_fail() {
    let mut reordered = history();
    completion(&mut reordered).outcomes.swap(0, 1);
    assert_contract(&violations(&reordered));

    let mut duplicated = history();
    duplicated.insert(2, duplicated[1].clone());
    assert_contract(&violations(&duplicated));

    let mut failed = history();
    completion(&mut failed).outcomes[0].value = None;
    completion(&mut failed).outcomes[0].error_code = Some("broker:broker_3".to_owned());
    assert_contract(&violations(&failed));
}

#[test]
fn reordered_mismatched_or_noncontiguous_observations_fail() {
    let mut reordered = history();
    reordered.swap(2, 3);
    assert_contract(&violations(&reordered));

    let mut mismatched = history();
    observation(&mut mismatched, 1).value = "compact".to_owned();
    assert_contract(&violations(&mismatched));

    let mut noncontiguous = history();
    observation(&mut noncontiguous, 1).observation = 44;
    assert_contract(&violations(&noncontiguous));
}

#[test]
fn altered_wire_selection_order_fails_exact_command_ownership() {
    let mut entries = history();
    let HistoryPayload::HarnessCommand { command } = &mut entries[0].payload else {
        panic!("plural topic-configuration command");
    };
    let AdapterCommand::DescribeTopicConfigs(value) = &mut command.command else {
        panic!("plural topic-configuration payload");
    };
    value.topics.swap(0, 1);
    assert_contract(&violations(&entries));
}

fn history() -> Vec<HistoryEntry> {
    vec![
        command(1, AdapterCommand::DescribeTopicConfigs(command_payload())),
        event(2, AdapterEvent::TopicConfigsDescribed(completion_value())),
        state(3, 40, zulu_topic(), "delete"),
        state(4, 41, alpha_topic(), "delete"),
    ]
}

fn command_payload() -> DescribeTopicConfigsCommand {
    DescribeTopicConfigsCommand {
        client_id: client(),
        operation_id: operation(),
        topics: vec![
            selection(zulu_topic(), "cleanup.policy"),
            selection(alpha_topic(), "cleanup.policy"),
        ],
        timeout_ms: 20_000,
    }
}

fn completion_value() -> AdminTopicConfigsDescription {
    AdminTopicConfigsDescription {
        operation_id: operation(),
        outcomes: vec![
            outcome(zulu_topic(), "delete"),
            outcome(alpha_topic(), "delete"),
        ],
    }
}

fn outcome(topic: &str, value: &str) -> AdminTopicConfigDescriptionOutcome {
    AdminTopicConfigDescriptionOutcome {
        topic: topic.to_owned(),
        config_name: "cleanup.policy".to_owned(),
        value: Some(value.to_owned()),
        error_code: None,
    }
}

fn selection(topic: &str, config_name: &str) -> TopicConfigSelection {
    TopicConfigSelection {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
    }
}

fn state(sequence: u64, observation: u64, topic: &str, value: &str) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::TopicConfig(BrokerTopicConfigState {
                observation,
                operation_id: operation(),
                topic: topic.to_owned(),
                config_name: "cleanup.policy".to_owned(),
                value: value.to_owned(),
            }),
        },
    }
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-describe-topic-configs.toml"
    ))
    .unwrap_or_else(|error| panic!("parse plural topic configurations: {error}"))
}

fn violations(entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(entries);
    let mut violations = Vec::new();
    verify_admin(&scenario(), &index, &[], &mut violations);
    violations
}

fn completion(entries: &mut [HistoryEntry]) -> &mut AdminTopicConfigsDescription {
    let HistoryPayload::AdapterEvent { event } = &mut entries[1].payload else {
        panic!("plural topic-configuration event");
    };
    let AdapterEvent::TopicConfigsDescribed(value) = &mut event.event else {
        panic!("plural topic-configuration completion");
    };
    value
}

fn observation(entries: &mut [HistoryEntry], index: usize) -> &mut BrokerTopicConfigState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[2 + index].payload
    else {
        panic!("topic-configuration observation");
    };
    let BrokerStateObservation::TopicConfig(value) = observation else {
        panic!("topic-configuration state");
    };
    value
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-048"),
        "{violations:?}"
    );
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-describe-topic-configs")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn zulu_topic() -> &'static str {
    "testlab-kafkars-admin-describe-topic-configs-zulu"
}

fn alpha_topic() -> &'static str {
    "testlab-kafkars-admin-describe-topic-configs-alpha"
}
