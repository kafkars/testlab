//! Topic-configuration verdict tests require exact public and independent transitions.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicConfigCompletion, AdminTopicConfigDescription,
    AlterTopicConfigAction, AlterTopicConfigCommand, BrokerStateObservation,
    BrokerTopicConfigState, DescribeTopicConfigAction, DescribeTopicConfigCommand, HistoryEntry,
    HistoryPayload, OperationId, ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{adapter, command, event, scenario, step};

const TOPIC: &str = "orders";
const CONFIG: &str = "cleanup.policy";
const BEFORE: &str = "config-before";
const ALTER: &str = "config-alter";

#[test]
fn exact_description_and_distinct_alteration_pass() {
    assert!(violations(&lifecycle_history()).is_empty());
}

#[test]
fn description_rejects_nullable_mismatch_and_duplicate_public_results() {
    let mut nullable = lifecycle_history();
    described_event_mut(&mut nullable).value = None;

    let mut duplicate = lifecycle_history();
    duplicate.insert(2, event(2, described_event(BEFORE, Some("delete"))));
    resequence(&mut duplicate);

    for history in [nullable, duplicate] {
        assert_contract(&violations(&history), "ADMIN-015");
    }
}

#[test]
fn alteration_requires_a_distinct_independent_baseline() {
    let mut same_baseline = lifecycle_history();
    config_state_mut(&mut same_baseline, BEFORE).value = "compact".to_owned();
    described_event_mut(&mut same_baseline).value = Some("compact".to_owned());

    let mut no_baseline = lifecycle_history();
    no_baseline.retain(|entry| {
        !matches!(
            &entry.payload,
            HistoryPayload::BrokerStateObservation {
                observation: BrokerStateObservation::TopicConfig(value)
            } if value.operation_id.as_str() == BEFORE
        )
    });
    resequence(&mut no_baseline);

    let mut stale_baseline = lifecycle_history();
    stale_baseline.insert(
        3,
        command(
            3,
            AdapterCommand::AlterTopicConfig(AlterTopicConfigCommand {
                operation_id: operation("intervening-alter"),
                ..alter_command()
            }),
        ),
    );
    resequence(&mut stale_baseline);

    for history in [same_baseline, no_baseline, stale_baseline] {
        assert_contract(&violations(&history), "ADMIN-016");
    }
}

#[test]
fn altered_value_must_match_and_precede_the_next_command() {
    let mut wrong_value = lifecycle_history();
    config_state_mut(&mut wrong_value, ALTER).value = "delete".to_owned();

    let mut out_of_window = lifecycle_history();
    out_of_window.insert(5, command(5, AdapterCommand::Finish));
    resequence(&mut out_of_window);

    for history in [wrong_value, out_of_window] {
        assert_contract(&violations(&history), "ADMIN-016");
    }
}

#[test]
fn config_command_matching_checks_wire_fields_but_not_description_expectation() {
    let mut expectation_changed = config_scenario();
    let ScenarioAction::DescribeTopicConfig(description) = &mut expectation_changed.steps[2].action
    else {
        panic!("description action kind");
    };
    description.expected_value = "scenario-only".to_owned();
    let index = HistoryIndex::build(&lifecycle_history());
    assert_eq!(
        index.admin_command_state(&expectation_changed.steps[2].action),
        (true, 1)
    );

    let mut wrong_command = lifecycle_history();
    let HistoryPayload::HarnessCommand { command } = &mut wrong_command[0].payload else {
        panic!("description command entry");
    };
    let AdapterCommand::DescribeTopicConfig(command) = &mut command.command else {
        panic!("description command kind");
    };
    command.config_name = "retention.ms".to_owned();
    assert_contract(&violations(&wrong_command), "ADMIN-015");
}

#[test]
fn top_level_verifier_handles_config_actions() {
    let _ = crate::verify(&config_scenario(), &adapter(), &lifecycle_history(), &[]);
}

fn lifecycle_history() -> Vec<HistoryEntry> {
    vec![
        command(0, describe_command()),
        event(1, described_event(BEFORE, Some("delete"))),
        state(2, BEFORE, "delete"),
        command(3, AdapterCommand::AlterTopicConfig(alter_command())),
        event(
            4,
            AdapterEvent::TopicConfigAltered(AdminTopicConfigCompletion {
                operation_id: operation(ALTER),
                topic: TOPIC.to_owned(),
                config_name: CONFIG.to_owned(),
            }),
        ),
        state(5, ALTER, "compact"),
    ]
}

fn config_scenario() -> testlab_schema::Scenario {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    value
        .steps
        .insert(2, step("config-before", describe_action()));
    value.steps.insert(
        3,
        step(
            "config-alter",
            ScenarioAction::AlterTopicConfig(AlterTopicConfigAction {
                client_id: client(),
                operation_id: operation(ALTER),
                topic: TOPIC.to_owned(),
                config_name: CONFIG.to_owned(),
                value: "compact".to_owned(),
                validate_only: false,
                expected_current_value: None,
                timeout_ms: 1_000,
            }),
        ),
    );
    value
}

fn describe_action() -> ScenarioAction {
    ScenarioAction::DescribeTopicConfig(DescribeTopicConfigAction {
        client_id: client(),
        operation_id: operation(BEFORE),
        topic: TOPIC.to_owned(),
        config_name: CONFIG.to_owned(),
        expected_value: "delete".to_owned(),
        timeout_ms: 1_000,
    })
}

fn describe_command() -> AdapterCommand {
    AdapterCommand::DescribeTopicConfig(DescribeTopicConfigCommand {
        client_id: client(),
        operation_id: operation(BEFORE),
        topic: TOPIC.to_owned(),
        config_name: CONFIG.to_owned(),
        timeout_ms: 1_000,
    })
}

fn alter_command() -> AlterTopicConfigCommand {
    AlterTopicConfigCommand {
        client_id: client(),
        operation_id: operation(ALTER),
        topic: TOPIC.to_owned(),
        config_name: CONFIG.to_owned(),
        value: "compact".to_owned(),
        validate_only: false,
        timeout_ms: 1_000,
    }
}

fn described_event(operation_id: &str, value: Option<&str>) -> AdapterEvent {
    AdapterEvent::TopicConfigDescribed(AdminTopicConfigDescription {
        operation_id: operation(operation_id),
        topic: TOPIC.to_owned(),
        config_name: CONFIG.to_owned(),
        value: value.map(str::to_owned),
    })
}

fn state(sequence: u64, operation_id: &str, value: &str) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::TopicConfig(BrokerTopicConfigState {
                observation: sequence,
                operation_id: operation(operation_id),
                topic: TOPIC.to_owned(),
                config_name: CONFIG.to_owned(),
                value: value.to_owned(),
            }),
        },
    }
}

fn described_event_mut(history: &mut [HistoryEntry]) -> &mut AdminTopicConfigDescription {
    let HistoryPayload::AdapterEvent { event } = &mut history[1].payload else {
        panic!("description event entry");
    };
    let AdapterEvent::TopicConfigDescribed(value) = &mut event.event else {
        panic!("description event kind");
    };
    value
}

fn config_state_mut<'a>(
    history: &'a mut [HistoryEntry],
    operation_id: &str,
) -> &'a mut BrokerTopicConfigState {
    history
        .iter_mut()
        .find_map(|entry| match &mut entry.payload {
            HistoryPayload::BrokerStateObservation {
                observation: BrokerStateObservation::TopicConfig(value),
            } if value.operation_id.as_str() == operation_id => Some(value),
            _ => None,
        })
        .unwrap_or_else(|| panic!("missing config state {operation_id}"))
}

fn resequence(history: &mut [HistoryEntry]) {
    for (sequence, entry) in history.iter_mut().enumerate() {
        entry.sequence = sequence as u64;
        entry.observed_unix_ms = sequence as u64;
        if let HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::TopicConfig(value),
        } = &mut entry.payload
        {
            value.observation = sequence as u64;
        }
    }
}

fn violations(history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    verify(&config_scenario(), history)
}

fn verify(
    scenario: &testlab_schema::Scenario,
    history: &[HistoryEntry],
) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    verify_admin(scenario, &index, &[], &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "{violations:?}"
    );
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
}
