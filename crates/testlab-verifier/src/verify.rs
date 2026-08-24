//! Producer verification compares scenario intent, client history, and broker truth.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterDescriptor, BrokerObservation, OperationAssertion, OperationId, RecordSpec, Scenario,
    ScenarioAction, TerminalStatus, Verdict, Violation, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::client_failure::verify_client_failures;
use crate::consumer::verify_consumers;
use crate::index::HistoryIndex;
use crate::lifecycle::verify_lifecycle;
use crate::protocol::verify_protocol;
use crate::support::{observation_references, references, terminal_references, violation};
use crate::transaction::verify_transactions;

/// Deterministically verifies one validly executed scenario.
pub fn verify(
    scenario: &Scenario,
    adapter: &AdapterDescriptor,
    history: &[testlab_schema::HistoryEntry],
    observations: &[BrokerObservation],
) -> Verdict {
    let index = HistoryIndex::build(history);
    let sends = sends(scenario, &index);
    let assertions = assertions(scenario);
    let observed = observations_by_operation(observations);
    let mut violations = Vec::new();
    verify_protocol(adapter, &index, &mut violations);
    verify_client_failures(&index, &mut violations);
    verify_admin(scenario, &index, observations, &mut violations);
    verify_transactions(scenario, &index, observations, &mut violations);
    verify_operations(&sends, &assertions, &index, &observed, &mut violations);
    verify_consumers(scenario, &index, &mut violations);
    crate::observations::verify_unknown(&sends, &observed, &mut violations);
    verify_lifecycle(scenario, &index, &mut violations);
    if violations.is_empty() {
        Verdict::passed()
    } else {
        Verdict::failed(violations)
    }
}

fn sends<'a>(
    scenario: &'a Scenario,
    index: &HistoryIndex,
) -> BTreeMap<OperationId, &'a RecordSpec> {
    let mut sends = BTreeMap::new();
    for step in &scenario.steps {
        if !index.action_issued(&step.action) {
            continue;
        }
        match &step.action {
            ScenarioAction::Send {
                operation_id,
                record,
                ..
            } => {
                sends.insert(operation_id.clone(), record);
            }
            ScenarioAction::SendBatch { operations, .. } => {
                sends.extend(
                    operations
                        .iter()
                        .map(|operation| (operation.operation_id.clone(), &operation.record)),
                );
            }
            ScenarioAction::ExecuteTransaction { operations, .. } => {
                sends.extend(
                    operations
                        .iter()
                        .map(|operation| (operation.operation_id.clone(), &operation.record)),
                );
            }
            ScenarioAction::FenceTransaction { operation, .. } => {
                sends.insert(operation.operation_id.clone(), &operation.record);
            }
            _ => {}
        }
    }
    sends
}

fn assertions(scenario: &Scenario) -> BTreeMap<OperationId, &OperationAssertion> {
    scenario
        .assertions
        .iter()
        .map(|assertion| (assertion.operation_id.clone(), assertion))
        .collect()
}

fn observations_by_operation(
    observations: &[BrokerObservation],
) -> BTreeMap<OperationId, Vec<&BrokerObservation>> {
    let mut by_operation: BTreeMap<OperationId, Vec<&BrokerObservation>> = BTreeMap::new();
    for observation in observations {
        by_operation
            .entry(observation.operation_id.clone())
            .or_default()
            .push(observation);
    }
    by_operation
}

fn verify_operations(
    sends: &BTreeMap<OperationId, &RecordSpec>,
    assertions: &BTreeMap<OperationId, &OperationAssertion>,
    index: &HistoryIndex,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    for (operation_id, record) in sends {
        let assertion = assertions.get(operation_id).copied();
        verify_admission(operation_id, assertion, index, violations);
        verify_terminal(operation_id, assertion, index, violations);
        verify_visibility(operation_id, assertion, index, observed, violations);
        verify_integrity(operation_id, record, observed, violations);
    }
}

fn verify_admission(
    operation_id: &OperationId,
    assertion: Option<&OperationAssertion>,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let expected_accepted = assertion.is_some_and(|value| value.accepted);
    let accepted = index.accepted.get(operation_id).map_or(0, Vec::len);
    let rejected = index.rejected.get(operation_id).map_or(0, Vec::len);
    let valid = if expected_accepted {
        accepted == 1 && rejected == 0
    } else {
        accepted == 0 && rejected == 1
    };
    if !valid {
        let mut evidence = references(index.accepted.get(operation_id).map(Vec::as_slice));
        evidence.extend(references(
            index.rejected.get(operation_id).map(Vec::as_slice),
        ));
        violations.push(violation(
            "PROD-001",
            format!(
                "expected accepted={expected_accepted}; observed {accepted} accepted and {rejected} rejected event(s)"
            ),
            Some(operation_id.clone()),
            evidence,
        ));
    }
}

fn verify_terminal(
    operation_id: &OperationId,
    assertion: Option<&OperationAssertion>,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let terminals = index.terminals.get(operation_id);
    let accepted = assertion.is_some_and(|value| value.accepted);
    let expected_count = usize::from(accepted);
    let count = terminals.map_or(0, Vec::len);
    if count != expected_count {
        violations.push(violation(
            "PROD-002",
            format!(
                "accepted operation expected {expected_count} terminal event, observed {count}"
            ),
            Some(operation_id.clone()),
            terminal_references(terminals.map(Vec::as_slice)),
        ));
        return;
    }
    let Some(assertion) = assertion else {
        return;
    };
    let Some(terminal) = terminals.and_then(|values| values.first()) else {
        return;
    };
    let Some(expected_terminal) = assertion.terminal else {
        return;
    };
    if terminal.status != expected_terminal {
        violations.push(violation(
            "PROD-007",
            format!(
                "expected terminal {expected_terminal:?}, observed {:?}",
                terminal.status
            ),
            Some(operation_id.clone()),
            vec![format!("history:{}", terminal.history_sequence)],
        ));
    }
}

fn verify_visibility(
    operation_id: &OperationId,
    assertion: Option<&OperationAssertion>,
    index: &HistoryIndex,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    let count = observed.get(operation_id).map_or(0, Vec::len);
    if let Some(terminal) = index
        .terminals
        .get(operation_id)
        .and_then(|values| values.first())
    {
        let global_contract = match terminal.status {
            TerminalStatus::Acknowledged if count != 1 => Some((
                "PROD-003",
                "acknowledged operation must be visible exactly once",
            )),
            TerminalStatus::DefinitelyNotSent if count != 0 => {
                Some(("PROD-004", "definitely-not-sent operation must be absent"))
            }
            TerminalStatus::PossiblySent if count > 1 => Some((
                "PROD-005",
                "possibly-sent operation must not be visible more than once",
            )),
            _ => None,
        };
        if let Some((contract, message)) = global_contract {
            violations.push(violation(
                contract,
                format!("{message}; observed {count}"),
                Some(operation_id.clone()),
                observation_references(observed.get(operation_id).map(Vec::as_slice)),
            ));
        }
    }
    let Some(assertion) = assertion else {
        return;
    };
    let matches = match assertion.visibility {
        VisibilityExpectation::Absent => count == 0,
        VisibilityExpectation::ExactlyOnce => count == 1,
        VisibilityExpectation::ZeroOrOne => count <= 1,
    };
    if !matches {
        violations.push(violation(
            "PROD-008",
            format!(
                "expected visibility {:?}, observed {count} record(s)",
                assertion.visibility
            ),
            Some(operation_id.clone()),
            observation_references(observed.get(operation_id).map(Vec::as_slice)),
        ));
    }
}

fn verify_integrity(
    operation_id: &OperationId,
    expected: &RecordSpec,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    let expected_digest = match expected.digest() {
        Ok(digest) => digest,
        Err(error) => {
            violations.push(violation(
                "PROD-006",
                format!("scenario record could not be hashed: {error}"),
                Some(operation_id.clone()),
                vec![format!("scenario:operation:{operation_id}")],
            ));
            return;
        }
    };
    for observation in observed.get(operation_id).into_iter().flatten() {
        let observed_digest = match observation.record.digest() {
            Ok(digest) => digest,
            Err(error) => {
                violations.push(violation(
                    "PROD-006",
                    format!("broker-visible record could not be hashed: {error}"),
                    Some(operation_id.clone()),
                    vec![format!("broker-observation:{}", observation.observation)],
                ));
                continue;
            }
        };
        if observed_digest != expected_digest || observation.digest != observed_digest {
            violations.push(violation(
                "PROD-006",
                "broker-visible record or recorded digest differs from the scenario record"
                    .to_owned(),
                Some(operation_id.clone()),
                vec![format!("broker-observation:{}", observation.observation)],
            ));
        }
    }
}
