//! Resource-generic configuration verdicts retain distinct contract identities.

use super::*;

#[test]
fn exact_generic_description_and_mutation_pass() {
    let (scenario, history) = resource_fixture();
    assert!(violations_for(&scenario, &history).is_empty());
}

#[test]
fn generic_failures_emit_generic_contracts() {
    let (scenario, mut bad_description) = resource_fixture();
    let AdapterEvent::TopicConfigsDescribed(value) = adapter_event(&mut bad_description[1]) else {
        panic!("generic baseline completion");
    };
    value.outcomes[0].value = None;
    assert_contract_id(&violations_for(&scenario, &bad_description), "ADMIN-064");

    let (scenario, mut bad_mutation) = resource_fixture();
    mutation_completion(&mut bad_mutation).outcomes[0].error_code = Some("broker".to_owned());
    assert_contract_id(&violations_for(&scenario, &bad_mutation), "ADMIN-065");
}

#[test]
fn exact_legacy_topic_and_resource_mutations_pass() {
    for (description, mutation) in [
        (
            testlab_schema::TopicConfigApi::Topic,
            testlab_schema::TopicConfigMutationApi::LegacyTopic,
        ),
        (
            testlab_schema::TopicConfigApi::Resource,
            testlab_schema::TopicConfigMutationApi::LegacyResource,
        ),
    ] {
        let (scenario, history) = selected_fixture(description, mutation);
        assert!(violations_for(&scenario, &history).is_empty());
    }
}

#[test]
fn legacy_mutation_failures_retain_distinct_contracts() {
    for (description, mutation, contract) in [
        (
            testlab_schema::TopicConfigApi::Topic,
            testlab_schema::TopicConfigMutationApi::LegacyTopic,
            "ADMIN-066",
        ),
        (
            testlab_schema::TopicConfigApi::Resource,
            testlab_schema::TopicConfigMutationApi::LegacyResource,
            "ADMIN-067",
        ),
    ] {
        let (scenario, mut history) = selected_fixture(description, mutation);
        mutation_completion(&mut history).outcomes[0].error_code = Some("broker".to_owned());
        assert_contract_id(&violations_for(&scenario, &history), contract);
    }
}

fn resource_fixture() -> (Scenario, Vec<HistoryEntry>) {
    selected_fixture(
        testlab_schema::TopicConfigApi::Resource,
        testlab_schema::TopicConfigMutationApi::Resource,
    )
}

fn selected_fixture(
    description_api: testlab_schema::TopicConfigApi,
    mutation_api: testlab_schema::TopicConfigMutationApi,
) -> (Scenario, Vec<HistoryEntry>) {
    let mut scenario = scenario();
    for step in &mut scenario.steps {
        match &mut step.action {
            testlab_schema::ScenarioAction::DescribeTopicConfigs(action) => {
                action.api = description_api;
            }
            testlab_schema::ScenarioAction::AlterTopicConfigs(action) => {
                action.api = mutation_api;
            }
            _ => {}
        }
    }
    let mut history = history();
    for entry in &mut history {
        let HistoryPayload::HarnessCommand { command } = &mut entry.payload else {
            continue;
        };
        match &mut command.command {
            AdapterCommand::DescribeTopicConfigs(command) => {
                command.api = description_api;
            }
            AdapterCommand::AlterTopicConfigs(command) => {
                command.api = mutation_api;
            }
            _ => {}
        }
    }
    (scenario, history)
}

fn violations_for(scenario: &Scenario, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    verify_admin(scenario, &index, &[], &mut violations);
    violations
}

fn assert_contract_id(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "{violations:?}"
    );
}
