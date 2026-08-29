//! Consumer record verification joins public bytes and coordinates to broker observations.

use std::collections::BTreeMap;

use testlab_schema::{
    BrokerObservation, ConsumedRecord, OperationId, Scenario, ScenarioAction, Violation,
};

use crate::consumer::{exact_record, sent_record};
use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(
    scenario: &Scenario,
    index: &HistoryIndex,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::Receive {
                receive_id,
                expected_operation_id,
                ..
            }
            | ScenarioAction::GroupReceive {
                receive_id,
                expected_operation_id,
                expected_error_code: None,
                ..
            } if index.action_issued(&step.action) => verify_receive(
                receive_id,
                expected_operation_id,
                index,
                observed,
                violations,
            ),
            ScenarioAction::GroupReceiveSet(action) if index.action_issued(&step.action) => {
                verify_receive_set(scenario, action, index, observed, violations);
            }
            ScenarioAction::ShareReceive {
                receive_id,
                expected_operation_ids,
                ..
            } if index.action_issued(&step.action) => verify_share_receive(
                receive_id,
                expected_operation_ids,
                index,
                observed,
                violations,
            ),
            ScenarioAction::StartConcurrentActors(action) if index.action_issued(&step.action) => {
                verify_concurrent_receives(action, index, observed, violations);
            }
            _ => {}
        }
    }
}

fn verify_concurrent_receives(
    action: &testlab_schema::StartConcurrentActorsAction,
    index: &HistoryIndex,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    for actor in &action.actors {
        if let testlab_schema::ConcurrentActor::AssignedReceive {
            receive_id,
            expected_operation_id,
            ..
        } = actor
        {
            verify_receive(
                receive_id,
                expected_operation_id,
                index,
                observed,
                violations,
            );
        }
    }
}

fn verify_receive(
    receive_id: &OperationId,
    operation_id: &OperationId,
    index: &HistoryIndex,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    let Some([receive]) = index.receives.get(receive_id).map(Vec::as_slice) else {
        return;
    };
    let [record] = receive.records.as_slice() else {
        return;
    };
    let Some([observation]) = observed.get(operation_id).map(Vec::as_slice) else {
        return;
    };
    verify_public_record(
        "CONS-012",
        "consumer receive",
        receive.history_sequence,
        operation_id,
        record,
        observation,
        violations,
    );
}

fn verify_receive_set(
    scenario: &Scenario,
    action: &testlab_schema::GroupReceiveSetAction,
    index: &HistoryIndex,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    let Some([receive]) = index
        .group_receive_sets
        .get(&action.receive_id)
        .map(Vec::as_slice)
    else {
        return;
    };
    let records = receive
        .completion
        .members
        .iter()
        .flat_map(|member| &member.records)
        .collect::<Vec<_>>();
    for operation_id in &action.expected_operation_ids {
        let Some(expected) = sent_record(scenario, operation_id) else {
            continue;
        };
        let matching = records
            .iter()
            .filter(|record| exact_record(record, expected))
            .copied()
            .collect::<Vec<_>>();
        let [record] = matching.as_slice() else {
            receive_set_failure(operation_id, receive.history_sequence, observed, violations);
            continue;
        };
        let Some([observation]) = observed.get(operation_id).map(Vec::as_slice) else {
            continue;
        };
        verify_public_record(
            "CONS-012",
            "group receive-set",
            receive.history_sequence,
            operation_id,
            record,
            observation,
            violations,
        );
    }
}

fn verify_share_receive(
    receive_id: &OperationId,
    operation_ids: &[OperationId],
    index: &HistoryIndex,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    let Some([receive]) = index.share_receives.get(receive_id).map(Vec::as_slice) else {
        return;
    };
    if receive.records.len() != operation_ids.len() {
        return;
    }
    for (record, operation_id) in receive.records.iter().zip(operation_ids) {
        let Some([observation]) = observed.get(operation_id).map(Vec::as_slice) else {
            continue;
        };
        verify_public_record(
            "SHARE-006",
            "Share acquisition",
            receive.history_sequence,
            operation_id,
            &record.record,
            observation,
            violations,
        );
    }
}

fn verify_public_record(
    contract: &str,
    surface: &str,
    history_sequence: u64,
    operation_id: &OperationId,
    record: &ConsumedRecord,
    observation: &BrokerObservation,
    violations: &mut Vec<Violation>,
) {
    if record.offset == observation.offset && exact_record(record, &observation.record) {
        return;
    }
    violations.push(violation(
        contract,
        format!(
            "public {surface} record for {operation_id} did not match independent broker coordinates and bytes"
        ),
        Some(operation_id.clone()),
        vec![
            format!("history:{history_sequence}"),
            format!("broker-observation:{}", observation.observation),
        ],
    ));
}

fn receive_set_failure(
    operation_id: &OperationId,
    history_sequence: u64,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    let mut evidence = vec![format!("history:{history_sequence}")];
    evidence.extend(
        observed
            .get(operation_id)
            .into_iter()
            .flatten()
            .map(|observation| format!("broker-observation:{}", observation.observation)),
    );
    violations.push(violation(
        "CONS-012",
        format!(
            "group receive-set did not expose one exact public record for independent operation {operation_id}"
        ),
        Some(operation_id.clone()),
        evidence,
    ));
}
