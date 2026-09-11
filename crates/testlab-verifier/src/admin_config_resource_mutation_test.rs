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

fn resource_fixture() -> (Scenario, Vec<HistoryEntry>) {
    let mut scenario = scenario();
    for step in &mut scenario.steps {
        match &mut step.action {
            testlab_schema::ScenarioAction::DescribeTopicConfigs(action) => {
                action.api = testlab_schema::TopicConfigApi::Resource;
            }
            testlab_schema::ScenarioAction::AlterTopicConfigs(action) => {
                action.api = testlab_schema::TopicConfigApi::Resource;
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
                command.api = testlab_schema::TopicConfigApi::Resource;
            }
            AdapterCommand::AlterTopicConfigs(command) => {
                command.api = testlab_schema::TopicConfigApi::Resource;
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
