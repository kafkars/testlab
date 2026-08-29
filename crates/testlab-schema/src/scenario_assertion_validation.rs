//! Assertion validation keeps declared outcomes coherent with each operation family.

use std::collections::BTreeSet;

use crate::transaction_action_validation::{TransactionRecordOutcome, TransactionSends};
use crate::{
    OperationAssertion, OperationId, Scenario, ScenarioAction, TerminalStatus,
    TransactionDisposition, VisibilityExpectation,
};

pub(crate) fn validate(
    scenario: &Scenario,
    operations: &BTreeSet<OperationId>,
    transaction_sends: &TransactionSends,
    problems: &mut Vec<String>,
) {
    let mut asserted = BTreeSet::new();
    let cancellations = scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::CancelProducerSend(action) => Some(&action.operation_id),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
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
        validate_semantics(
            assertion,
            transaction_sends.get(&assertion.operation_id).copied(),
            cancellations.contains(&assertion.operation_id),
            problems,
        );
    }
    for operation in operations {
        if !asserted.contains(operation) {
            problems.push(format!("operation {operation} has no assertion"));
        }
    }
}

fn validate_semantics(
    assertion: &OperationAssertion,
    transaction: Option<TransactionRecordOutcome>,
    cancellation: bool,
    problems: &mut Vec<String>,
) {
    match (assertion.accepted, assertion.terminal) {
        (true, None) if !cancellation => problems.push(format!(
            "accepted operation {} requires a terminal expectation",
            assertion.operation_id
        )),
        (false, Some(_)) => problems.push(format!(
            "rejected operation {} must not declare a terminal expectation",
            assertion.operation_id
        )),
        _ => {}
    }
    if cancellation && assertion.visibility != VisibilityExpectation::ZeroOrOne {
        problems.push(format!(
            "cancellation operation {} must expect zero-or-one visibility",
            assertion.operation_id
        ));
    }
    if cancellation && assertion.terminal.is_some() {
        problems.push(format!(
            "cancellation operation {} must not predeclare one race-dependent terminal",
            assertion.operation_id
        ));
    }
    if !assertion.accepted && assertion.visibility != VisibilityExpectation::Absent {
        problems.push(format!(
            "rejected operation {} must expect absent visibility",
            assertion.operation_id
        ));
    }
    if let Some(outcome) = transaction {
        validate_transaction(assertion, outcome, problems);
    } else if assertion.terminal == Some(TerminalStatus::TransactionStaged) {
        problems.push(format!(
            "non-transactional operation {} cannot expect transaction_staged",
            assertion.operation_id
        ));
    }
    validate_terminal(assertion, problems);
}

fn validate_terminal(assertion: &OperationAssertion, problems: &mut Vec<String>) {
    if assertion.terminal == Some(TerminalStatus::Acknowledged)
        && assertion.visibility != VisibilityExpectation::ExactlyOnce
    {
        problems.push(format!(
            "acknowledged operation {} must expect exactly-once visibility",
            assertion.operation_id
        ));
    }
    if let Some(code) = assertion.expected_error_code.as_deref() {
        if code != crate::PRODUCER_TOPIC_AUTHORIZATION_ERROR_CODE {
            problems.push(format!(
                "operation {} has unsupported expected error code {code}",
                assertion.operation_id
            ));
        }
        if assertion.terminal == Some(TerminalStatus::Acknowledged) {
            problems.push(format!(
                "operation {} cannot expect both acknowledgement and an error",
                assertion.operation_id
            ));
        }
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

fn validate_transaction(
    assertion: &OperationAssertion,
    outcome: TransactionRecordOutcome,
    problems: &mut Vec<String>,
) {
    if !assertion.accepted || assertion.terminal != Some(TerminalStatus::TransactionStaged) {
        problems.push(format!(
            "transactional operation {} must expect accepted transaction_staged delivery",
            assertion.operation_id
        ));
    }
    let expected = match outcome {
        TransactionRecordOutcome::Completed(TransactionDisposition::Commit) => {
            VisibilityExpectation::ExactlyOnce
        }
        TransactionRecordOutcome::Completed(TransactionDisposition::Abort)
        | TransactionRecordOutcome::Fenced => VisibilityExpectation::Absent,
    };
    if assertion.visibility != expected {
        problems.push(format!(
            "transactional operation {} must expect {expected:?} visibility",
            assertion.operation_id
        ));
    }
}
