//! Incremental configuration verdicts retain exact methods and operation operands.

use super::*;

use testlab_schema::TopicConfigMutationMethod;

#[test]
fn every_non_set_method_passes_through_both_public_surfaces() {
    for (method, before, operand, after) in [
        (TopicConfigMutationMethod::Delete, "compact", None, "delete"),
        (
            TopicConfigMutationMethod::Append,
            "delete",
            Some("compact"),
            "delete,compact",
        ),
        (
            TopicConfigMutationMethod::Subtract,
            "delete,compact",
            Some("delete"),
            "compact",
        ),
    ] {
        for (description_api, mutation_api) in [
            (
                testlab_schema::TopicConfigApi::Topic,
                testlab_schema::TopicConfigMutationApi::Topic,
            ),
            (
                testlab_schema::TopicConfigApi::Resource,
                testlab_schema::TopicConfigMutationApi::Resource,
            ),
        ] {
            let (scenario, history) = selected_fixture(
                method,
                before,
                operand,
                after,
                description_api,
                mutation_api,
            );
            assert!(violations_for(&scenario, &history).is_empty());
        }
    }
}

#[test]
fn final_value_or_method_substitution_fails_the_exact_contract() {
    let (scenario, mut leaked) = selected_fixture(
        TopicConfigMutationMethod::Append,
        "delete",
        Some("compact"),
        "delete,compact",
        testlab_schema::TopicConfigApi::Topic,
        testlab_schema::TopicConfigMutationApi::Topic,
    );
    mutation_command(&mut leaked).topics[0].value = Some("delete,compact".to_owned());
    assert_contract_id(&violations_for(&scenario, &leaked), "ADMIN-080");

    let (scenario, mut substituted) = selected_fixture(
        TopicConfigMutationMethod::Subtract,
        "delete,compact",
        Some("delete"),
        "compact",
        testlab_schema::TopicConfigApi::Resource,
        testlab_schema::TopicConfigMutationApi::Resource,
    );
    mutation_command(&mut substituted).topics[0].method = TopicConfigMutationMethod::Set;
    assert_contract_id(&violations_for(&scenario, &substituted), "ADMIN-080");
}

fn selected_fixture(
    method: TopicConfigMutationMethod,
    before: &str,
    operand: Option<&str>,
    after: &str,
    description_api: testlab_schema::TopicConfigApi,
    mutation_api: testlab_schema::TopicConfigMutationApi,
) -> (Scenario, Vec<HistoryEntry>) {
    let mut scenario = scenario();
    for step in &mut scenario.steps {
        match &mut step.action {
            testlab_schema::ScenarioAction::DescribeTopicConfigs(action) => {
                action.api = description_api;
                for selected in &mut action.topics {
                    selected.expected_value = before.to_owned();
                }
            }
            testlab_schema::ScenarioAction::AlterTopicConfigs(action) => {
                action.api = mutation_api;
                for selected in &mut action.topics {
                    selected.expected_previous_value = before.to_owned();
                    selected.value = after.to_owned();
                    selected.method = method;
                    selected.operation_value = operand.map(str::to_owned);
                }
            }
            _ => {}
        }
    }
    let mut history = history();
    description_command_from(&mut history).api = description_api;
    let AdapterEvent::TopicConfigsDescribed(description) = adapter_event(&mut history[1]) else {
        panic!("configuration description completion");
    };
    for outcome in &mut description.outcomes {
        outcome.value = Some(before.to_owned());
    }
    for index in 0..2 {
        observation(&mut history, BEFORE, index).value = before.to_owned();
        observation(&mut history, ALTER, index).value = after.to_owned();
    }
    let mutation = mutation_command(&mut history);
    mutation.api = mutation_api;
    for selected in &mut mutation.topics {
        selected.method = method;
        selected.value = match method {
            TopicConfigMutationMethod::Set => Some(after.to_owned()),
            TopicConfigMutationMethod::Append | TopicConfigMutationMethod::Subtract => {
                operand.map(str::to_owned)
            }
            TopicConfigMutationMethod::Delete | TopicConfigMutationMethod::RestoreDefault => None,
        };
    }
    (scenario, history)
}

fn description_command_from(entries: &mut [HistoryEntry]) -> &mut DescribeTopicConfigsCommand {
    let HistoryPayload::HarnessCommand { command } = &mut entries[0].payload else {
        panic!("configuration description command");
    };
    let AdapterCommand::DescribeTopicConfigs(command) = &mut command.command else {
        panic!("configuration description command kind");
    };
    command
}

fn mutation_command(entries: &mut [HistoryEntry]) -> &mut AlterTopicConfigsCommand {
    let HistoryPayload::HarnessCommand { command } = &mut entries[4].payload else {
        panic!("configuration mutation command");
    };
    let AdapterCommand::AlterTopicConfigs(command) = &mut command.command else {
        panic!("configuration mutation command kind");
    };
    command
}

fn violations_for(scenario: &Scenario, entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(entries);
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
