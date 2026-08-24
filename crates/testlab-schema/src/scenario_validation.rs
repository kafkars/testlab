//! Scenario validation owns identity, lifecycle, and assertion coherence.

use std::collections::BTreeSet;

use crate::{
    OperationId, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioError, TerminalStatus,
    VisibilityExpectation,
};

pub(crate) fn validate(scenario: &Scenario) -> Result<(), ScenarioError> {
    let mut problems = Vec::new();
    validate_header(scenario, &mut problems);
    validate_steps(scenario, &mut problems);
    if problems.is_empty() {
        Ok(())
    } else {
        Err(ScenarioError { problems })
    }
}

fn validate_header(scenario: &Scenario, problems: &mut Vec<String>) {
    if scenario.schema_version != SCENARIO_SCHEMA_VERSION {
        problems.push(format!(
            "unsupported schema version {}, expected {SCENARIO_SCHEMA_VERSION}",
            scenario.schema_version
        ));
    }
    if scenario.title.trim().is_empty() {
        problems.push("title must not be empty".to_owned());
    }
    if scenario.description.trim().is_empty() {
        problems.push("description must not be empty".to_owned());
    }
    if !(100..=600_000).contains(&scenario.timeout_ms) {
        problems.push("timeout_ms must be between 100 and 600000".to_owned());
    }
    if scenario.steps.is_empty() {
        problems.push("scenario must contain at least one step".to_owned());
    }
}

fn validate_steps(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut step_ids = BTreeSet::new();
    let mut state = crate::scenario_action_validation::ActionStates::default();
    let mut usage = BTreeSet::new();
    for step in &scenario.steps {
        if !step_ids.insert(step.id.clone()) {
            problems.push(format!("duplicate step id {}", step.id));
        }
        crate::scenario_capability_validation::record_usage(&step.action, &mut usage);
        crate::scenario_action_validation::validate_action(&step.action, &mut state, problems);
    }
    crate::scenario_capability_validation::validate_required(scenario, &usage, &state, problems);
    validate_assertions(scenario, &state.sends, &state.transaction_sends, problems);
    for (producer, (_, closed)) in state.producers {
        if !closed {
            problems.push(format!("producer {producer} was not closed"));
        }
    }
    for consumer in crate::consumer_action_validation::unclosed(state.consumers) {
        problems.push(format!("consumer {consumer} was not closed"));
    }
    for producer in crate::transaction_action_validation::unclosed(state.transactions) {
        problems.push(format!("transactional producer {producer} was not closed"));
    }
    for receive in crate::share_action_validation::unsettled(&state.share_batches) {
        problems.push(format!("share batch {receive} was not settled"));
    }
    for (topic, partition) in state.leader_disruptions {
        problems.push(format!(
            "partition leader {topic}:{partition} was not restored"
        ));
    }
    for broker in state.stopped_brokers {
        problems.push(format!("broker {broker} was not restarted"));
    }
    for (client, shutdown) in state.clients {
        if !shutdown {
            problems.push(format!("client {client} was not shut down"));
        }
    }
}

fn validate_assertions(
    scenario: &Scenario,
    operations: &BTreeSet<OperationId>,
    transaction_sends: &crate::transaction_action_validation::TransactionSends,
    problems: &mut Vec<String>,
) {
    let mut asserted = BTreeSet::new();
    for assertion in &scenario.assertions {
        if !operations.contains(&assertion.operation_id) {
            problems.push(format!(
                "assertion references missing operation {}",
                assertion.operation_id
            ));
        }
        if !asserted.insert(assertion.operation_id.clone()) {
            problems.push(format!(
                "duplicate assertion for operation {}",
                assertion.operation_id
            ));
        }
        validate_assertion_semantics(
            assertion,
            transaction_sends.get(&assertion.operation_id).copied(),
            problems,
        );
    }
    for operation in operations {
        if !asserted.contains(operation) {
            problems.push(format!("operation {operation} has no assertion"));
        }
    }
}

fn validate_assertion_semantics(
    assertion: &crate::OperationAssertion,
    transaction: Option<crate::transaction_action_validation::TransactionRecordOutcome>,
    problems: &mut Vec<String>,
) {
    match (assertion.accepted, assertion.terminal) {
        (true, None) => problems.push(format!(
            "accepted operation {} requires a terminal expectation",
            assertion.operation_id
        )),
        (false, Some(_)) => problems.push(format!(
            "rejected operation {} must not declare a terminal expectation",
            assertion.operation_id
        )),
        _ => {}
    }
    if !assertion.accepted && assertion.visibility != VisibilityExpectation::Absent {
        problems.push(format!(
            "rejected operation {} must expect absent visibility",
            assertion.operation_id
        ));
    }
    if let Some(outcome) = transaction {
        validate_transaction_assertion(assertion, outcome, problems);
    } else if assertion.terminal == Some(TerminalStatus::TransactionStaged) {
        problems.push(format!(
            "non-transactional operation {} cannot expect transaction_staged",
            assertion.operation_id
        ));
    }
    if assertion.terminal == Some(TerminalStatus::Acknowledged)
        && assertion.visibility != VisibilityExpectation::ExactlyOnce
    {
        problems.push(format!(
            "acknowledged operation {} must expect exactly-once visibility",
            assertion.operation_id
        ));
    }
    if assertion.terminal == Some(TerminalStatus::DefinitelyNotSent)
        && assertion.visibility != VisibilityExpectation::Absent
    {
        problems.push(format!(
            "definitely-not-sent operation {} must expect absent visibility",
            assertion.operation_id
        ));
    }
}

fn validate_transaction_assertion(
    assertion: &crate::OperationAssertion,
    outcome: crate::transaction_action_validation::TransactionRecordOutcome,
    problems: &mut Vec<String>,
) {
    if !assertion.accepted || assertion.terminal != Some(TerminalStatus::TransactionStaged) {
        problems.push(format!(
            "transactional operation {} must expect accepted transaction_staged delivery",
            assertion.operation_id
        ));
    }
    let expected = match outcome {
        crate::transaction_action_validation::TransactionRecordOutcome::Completed(
            crate::TransactionDisposition::Commit,
        ) => VisibilityExpectation::ExactlyOnce,
        crate::transaction_action_validation::TransactionRecordOutcome::Completed(
            crate::TransactionDisposition::Abort,
        )
        | crate::transaction_action_validation::TransactionRecordOutcome::Fenced => {
            VisibilityExpectation::Absent
        }
    };
    if assertion.visibility != expected {
        problems.push(format!(
            "transactional operation {} must expect {expected:?} visibility",
            assertion.operation_id
        ));
    }
}
