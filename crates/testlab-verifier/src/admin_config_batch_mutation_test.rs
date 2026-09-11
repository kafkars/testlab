//! Plural configuration mutation requires ordered results, post-state, and a named baseline.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicConfigAlterationOutcome,
    AdminTopicConfigDescriptionOutcome, AdminTopicConfigsAlteration, AdminTopicConfigsDescription,
    AlterTopicConfigCommand, AlterTopicConfigsCommand, BrokerStateObservation,
    BrokerTopicConfigState, ClientId, DescribeTopicConfigsCommand, HistoryEntry, HistoryPayload,
    OperationId, Scenario, TopicConfigAlteration, TopicConfigSelection,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_ordered_transition_passes() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn reordered_or_failed_public_outcomes_fail() {
    let mut reordered = history();
    mutation_completion(&mut reordered).outcomes.swap(0, 1);
    assert_contract(&violations(&reordered));

    let mut failed = history();
    mutation_completion(&mut failed).outcomes[1].error_code = Some("broker".to_owned());
    assert_contract(&violations(&failed));
}

#[test]
fn wrong_reordered_or_noncontiguous_post_state_fails() {
    let mut wrong = history();
    observation(&mut wrong, ALTER, 0).value = "delete".to_owned();
    assert_contract(&violations(&wrong));

    let mut reordered = history();
    reordered.swap(6, 7);
    assert_contract(&violations(&reordered));

    let mut noncontiguous = history();
    observation(&mut noncontiguous, ALTER, 1).observation = 99;
    assert_contract(&violations(&noncontiguous));
}

#[test]
fn missing_mismatched_or_stale_named_baseline_fails() {
    let mut missing = history();
    missing.retain(|entry| !is_observation(entry, BEFORE));
    resequence(&mut missing);
    assert_contract(&violations(&missing));

    let mut mismatched = history();
    let AdapterEvent::TopicConfigsDescribed(value) = adapter_event(&mut mismatched[1]) else {
        panic!("baseline completion kind");
    };
    value.outcomes[0].value = Some("compact".to_owned());
    assert_contract(&violations(&mismatched));

    let mut stale = history();
    stale.insert(
        4,
        command(
            4,
            AdapterCommand::AlterTopicConfig(AlterTopicConfigCommand {
                client_id: client(),
                operation_id: operation("intervening-alter"),
                topic: zulu_topic().to_owned(),
                config_name: config().to_owned(),
                value: "compact".to_owned(),
                validate_only: false,
                timeout_ms: 1_000,
            }),
        ),
    );
    resequence(&mut stale);
    assert_contract(&violations(&stale));
}

#[test]
fn altered_wire_order_fails_exact_command_ownership() {
    let mut entries = history();
    let HistoryPayload::HarnessCommand { command } = &mut entries[4].payload else {
        panic!("plural mutation command");
    };
    let AdapterCommand::AlterTopicConfigs(value) = &mut command.command else {
        panic!("plural mutation command kind");
    };
    value.topics.swap(0, 1);
    assert_contract(&violations(&entries));
}

fn history() -> Vec<HistoryEntry> {
    vec![
        command(
            0,
            AdapterCommand::DescribeTopicConfigs(description_command()),
        ),
        event(
            1,
            AdapterEvent::TopicConfigsDescribed(description_completion()),
        ),
        state(2, 40, BEFORE, zulu_topic(), "delete"),
        state(3, 41, BEFORE, alpha_topic(), "delete"),
        command(4, AdapterCommand::AlterTopicConfigs(alter_command())),
        event(5, AdapterEvent::TopicConfigsAltered(alter_completion())),
        state(6, 42, ALTER, zulu_topic(), "compact"),
        state(7, 43, ALTER, alpha_topic(), "compact"),
    ]
}

fn description_command() -> DescribeTopicConfigsCommand {
    DescribeTopicConfigsCommand {
        client_id: client(),
        operation_id: operation(BEFORE),
        api: testlab_schema::TopicConfigApi::Topic,
        topics: topics()
            .into_iter()
            .map(|topic| TopicConfigSelection {
                topic,
                config_name: config().to_owned(),
            })
            .collect(),
        timeout_ms: 20_000,
    }
}

fn alter_command() -> AlterTopicConfigsCommand {
    AlterTopicConfigsCommand {
        client_id: client(),
        operation_id: operation(ALTER),
        api: testlab_schema::TopicConfigMutationApi::Topic,
        topics: topics()
            .into_iter()
            .map(|topic| TopicConfigAlteration {
                topic,
                config_name: config().to_owned(),
                value: "compact".to_owned(),
            })
            .collect(),
        timeout_ms: 20_000,
    }
}

fn description_completion() -> AdminTopicConfigsDescription {
    AdminTopicConfigsDescription {
        operation_id: operation(BEFORE),
        outcomes: topics()
            .into_iter()
            .map(|topic| AdminTopicConfigDescriptionOutcome {
                topic,
                config_name: config().to_owned(),
                value: Some("delete".to_owned()),
                error_code: None,
            })
            .collect(),
    }
}

fn alter_completion() -> AdminTopicConfigsAlteration {
    AdminTopicConfigsAlteration {
        operation_id: operation(ALTER),
        outcomes: topics()
            .into_iter()
            .map(|topic| AdminTopicConfigAlterationOutcome {
                topic,
                config_name: config().to_owned(),
                error_code: None,
            })
            .collect(),
    }
}

fn state(
    sequence: u64,
    observation: u64,
    operation_id: &str,
    topic: &str,
    value: &str,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::TopicConfig(BrokerTopicConfigState {
                observation,
                operation_id: operation(operation_id),
                topic: topic.to_owned(),
                config_name: config().to_owned(),
                value: value.to_owned(),
            }),
        },
    }
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-alter-topic-configs.toml"
    ))
    .unwrap_or_else(|error| panic!("parse plural configuration mutation: {error}"))
}

fn violations(entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(entries);
    let mut violations = Vec::new();
    verify_admin(&scenario(), &index, &[], &mut violations);
    violations
}

fn mutation_completion(entries: &mut [HistoryEntry]) -> &mut AdminTopicConfigsAlteration {
    let AdapterEvent::TopicConfigsAltered(value) = adapter_event(&mut entries[5]) else {
        panic!("plural mutation completion kind");
    };
    value
}

fn adapter_event(entry: &mut HistoryEntry) -> &mut AdapterEvent {
    let HistoryPayload::AdapterEvent { event } = &mut entry.payload else {
        panic!("adapter event entry");
    };
    &mut event.event
}

fn observation<'a>(
    entries: &'a mut [HistoryEntry],
    operation_id: &str,
    index: usize,
) -> &'a mut BrokerTopicConfigState {
    entries
        .iter_mut()
        .filter_map(|entry| match &mut entry.payload {
            HistoryPayload::BrokerStateObservation {
                observation: BrokerStateObservation::TopicConfig(value),
            } if value.operation_id.as_str() == operation_id => Some(value),
            _ => None,
        })
        .nth(index)
        .unwrap_or_else(|| panic!("missing observation {operation_id}:{index}"))
}

fn is_observation(entry: &HistoryEntry, operation_id: &str) -> bool {
    matches!(
        &entry.payload,
        HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::TopicConfig(value)
        } if value.operation_id.as_str() == operation_id
    )
}

fn resequence(entries: &mut [HistoryEntry]) {
    for (sequence, entry) in entries.iter_mut().enumerate() {
        entry.sequence = sequence as u64;
        entry.observed_unix_ms = sequence as u64;
    }
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-049"),
        "{violations:?}"
    );
}

fn topics() -> Vec<String> {
    vec![zulu_topic().to_owned(), alpha_topic().to_owned()]
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}

fn config() -> &'static str {
    "cleanup.policy"
}

#[path = "admin_config_resource_mutation_test.rs"]
mod resource_test;

fn zulu_topic() -> &'static str {
    "testlab-kafkars-admin-alter-topic-configs-zulu"
}

fn alpha_topic() -> &'static str {
    "testlab-kafkars-admin-alter-topic-configs-alpha"
}

const BEFORE: &str = "admin-alter-topic-configs-before";
const ALTER: &str = "admin-alter-topic-configs";
