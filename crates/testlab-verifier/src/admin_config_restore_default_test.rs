//! Legacy default-restoration evidence rejects an explicit expected-value substitution.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicConfigAlterationOutcome,
    AdminTopicConfigDescriptionOutcome, AdminTopicConfigsAlteration, AdminTopicConfigsDescription,
    AlterTopicConfigsCommand, BrokerStateObservation, BrokerTopicConfigState,
    DescribeTopicConfigsCommand, HistoryEntry, HistoryPayload, Scenario, ScenarioAction,
    TopicConfigAlteration, TopicConfigSelection,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

const BEFORE: &str = "admin-legacy-topic-config-after";
const ALTER: &str = "admin-legacy-topic-config-restore-default";
const CONFIG: &str = "cleanup.policy";

#[test]
fn exact_legacy_default_restoration_passes() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn explicit_expected_default_on_the_wire_fails() {
    let mut history = history();
    let HistoryPayload::HarnessCommand { command } = &mut history[4].payload else {
        panic!("legacy restore command");
    };
    let AdapterCommand::AlterTopicConfigs(command) = &mut command.command else {
        panic!("legacy restore command kind");
    };
    command.topics[0].value = Some("delete".to_owned());
    assert!(
        violations(&history)
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-079")
    );
}

fn history() -> Vec<HistoryEntry> {
    let topics = topics();
    vec![
        command(
            0,
            AdapterCommand::DescribeTopicConfigs(DescribeTopicConfigsCommand {
                client_id: client(),
                operation_id: operation(BEFORE),
                api: testlab_schema::TopicConfigApi::Topic,
                topics: selections(&topics),
                timeout_ms: 20_000,
            }),
        ),
        event(
            1,
            AdapterEvent::TopicConfigsDescribed(description(&topics, "compact")),
        ),
        state(2, 40, BEFORE, &topics[0], "compact"),
        state(3, 41, BEFORE, &topics[1], "compact"),
        command(
            4,
            AdapterCommand::AlterTopicConfigs(AlterTopicConfigsCommand {
                client_id: client(),
                operation_id: operation(ALTER),
                api: testlab_schema::TopicConfigMutationApi::LegacyTopic,
                topics: topics
                    .iter()
                    .map(|topic| TopicConfigAlteration {
                        topic: topic.clone(),
                        config_name: CONFIG.to_owned(),
                        value: None,
                    })
                    .collect(),
                timeout_ms: 20_000,
            }),
        ),
        event(
            5,
            AdapterEvent::TopicConfigsAltered(AdminTopicConfigsAlteration {
                operation_id: operation(ALTER),
                outcomes: topics
                    .iter()
                    .map(|topic| AdminTopicConfigAlterationOutcome {
                        topic: topic.clone(),
                        config_name: CONFIG.to_owned(),
                        error_code: None,
                    })
                    .collect(),
            }),
        ),
        state(6, 42, ALTER, &topics[0], "delete"),
        state(7, 43, ALTER, &topics[1], "delete"),
    ]
}

fn scenario() -> Scenario {
    let mut scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-legacy-config-replacement.toml"
    ))
    .unwrap_or_else(|error| panic!("parse legacy restoration: {error}"));
    scenario.steps.retain(|step| match &step.action {
        ScenarioAction::DescribeTopicConfigs(action) => action.operation_id.as_str() == BEFORE,
        ScenarioAction::AlterTopicConfigs(action) => action.operation_id.as_str() == ALTER,
        _ => false,
    });
    scenario
}

fn violations(history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    verify_admin(&scenario(), &index, &[], &mut violations);
    violations
}

fn description(topics: &[String], value: &str) -> AdminTopicConfigsDescription {
    AdminTopicConfigsDescription {
        operation_id: operation(BEFORE),
        outcomes: topics
            .iter()
            .map(|topic| AdminTopicConfigDescriptionOutcome {
                topic: topic.clone(),
                config_name: CONFIG.to_owned(),
                value: Some(value.to_owned()),
                error_code: None,
            })
            .collect(),
    }
}

fn selections(topics: &[String]) -> Vec<TopicConfigSelection> {
    topics
        .iter()
        .map(|topic| TopicConfigSelection {
            topic: topic.clone(),
            config_name: CONFIG.to_owned(),
        })
        .collect()
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
                config_name: CONFIG.to_owned(),
                value: value.to_owned(),
            }),
        },
    }
}

fn topics() -> Vec<String> {
    vec![
        "testlab-kafkars-legacy-topic-config-zulu".to_owned(),
        "testlab-kafkars-legacy-topic-config-alpha".to_owned(),
    ]
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> testlab_schema::OperationId {
    testlab_schema::OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
