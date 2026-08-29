//! Adversary contracts join scenario controls, external wire facts, and public outcomes.

use std::collections::BTreeSet;

use testlab_schema::{
    AdversaryOutcome, DisconnectPoint, EnvironmentOperationKind, EnvironmentOperationStatus,
    KafkaApi, ProtocolAdversaryObservation, ProtocolFault, ProtocolFaultAction, Scenario,
    ScenarioAction, Violation,
};

use crate::admin::public_after_command;
use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    let declared = scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::ArmProtocolFault(control) => Some(control),
            _ => None,
        })
        .collect::<Vec<_>>();
    if declared.is_empty() {
        return;
    }
    for control in &declared {
        verify_control(control, index, violations);
    }
    verify_observation_integrity(&declared, index, violations);
    verify_process(index, violations);
}

pub(crate) fn verify_admin_failure(
    scenario: &Scenario,
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let ScenarioAction::DescribeTopic(describe) = action else {
        return false;
    };
    let Some(control) = preceding_control(scenario, action) else {
        return false;
    };
    if control.api != KafkaApi::Metadata {
        return false;
    }
    let failures = index.admin_command_failures(action);
    let successes = index.topics_described.get(&describe.operation_id);
    let window = index.admin_command_window(action);
    let matches = match describe.expected_error_code.as_deref() {
        Some(expected_code) => failures.as_slice().first().is_some_and(|failure| {
            failures.len() == 1
                && failure.code == expected_code
                && public_after_command(window, failure.history_sequence)
                && successes.is_none_or(Vec::is_empty)
        }),
        None => successes.as_ref().is_some_and(|values| {
            values.as_slice().first().is_some_and(|success| {
                values.len() == 1
                    && failures.is_empty()
                    && success.topic == describe.topic
                    && describe
                        .expected_partitions
                        .as_ref()
                        .is_none_or(|partitions| &success.partitions == partitions)
                    && public_after_command(window, success.history_sequence)
            })
        }),
    };
    if !matches {
        let mut evidence = failures
            .iter()
            .map(|failure| format!("history:{}", failure.history_sequence))
            .collect::<Vec<_>>();
        evidence.extend(
            successes
                .into_iter()
                .flatten()
                .map(|success| format!("history:{}", success.history_sequence)),
        );
        violations.push(violation(
            "ADV-004",
            format!(
                "metadata fault {} did not produce its exact declared public outcome",
                control.operation_id
            ),
            Some(describe.operation_id.clone()),
            evidence,
        ));
    }
    true
}

fn verify_control(
    declared: &ProtocolFaultAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let controls = index
        .adversary_controls
        .iter()
        .filter(|(_, control)| control.operation_id == declared.operation_id)
        .collect::<Vec<_>>();
    let applied = index
        .adversary_observations
        .iter()
        .filter(|(_, observation)| observation.control_id.as_ref() == Some(&declared.operation_id))
        .collect::<Vec<_>>();
    let control_matches = controls
        .as_slice()
        .first()
        .is_some_and(|(_, control)| controls.len() == 1 && control == declared);
    let expected = usize::from(declared.applications);
    let application_matches = applied.len() == expected
        && applied.iter().all(|(sequence, observation)| {
            observation.api == declared.api
                && matches!(
                    &observation.outcome,
                    AdversaryOutcome::FaultApplied { fault } if fault == &declared.fault
                )
                && controls
                    .first()
                    .is_some_and(|(control_sequence, _)| control_sequence < sequence)
        });
    if !control_matches {
        violations.push(violation(
            "ADV-001",
            format!(
                "protocol fault {} expected one exact environment control, observed {}",
                declared.operation_id,
                controls.len()
            ),
            None,
            controls
                .iter()
                .map(|(sequence, _)| format!("history:{sequence}"))
                .collect(),
        ));
    }
    if !application_matches {
        violations.push(violation(
            "ADV-002",
            format!(
                "protocol fault {} expected {expected} ordered matching application(s), observed {}",
                declared.operation_id,
                applied.len()
            ),
            None,
            applied
                .iter()
                .map(|(sequence, _)| format!("history:{sequence}"))
                .collect(),
        ));
    }
}

fn verify_observation_integrity(
    declared: &[&ProtocolFaultAction],
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let ids = declared
        .iter()
        .map(|control| &control.operation_id)
        .collect::<BTreeSet<_>>();
    let mut ordinals = BTreeSet::new();
    let valid = index.adversary_observations.iter().all(|(_, observation)| {
        ordinals.insert(observation.observation)
            && observation_shape(observation)
            && match (&observation.control_id, &observation.outcome) {
                (None, AdversaryOutcome::Baseline) => true,
                (Some(id), AdversaryOutcome::FaultApplied { .. }) => ids.contains(id),
                _ => false,
            }
    });
    let contiguous = ordinals
        .iter()
        .copied()
        .eq(0..u64::try_from(ordinals.len()).unwrap_or(u64::MAX));
    if !valid || !contiguous {
        violations.push(violation(
            "ADV-003",
            "adversary observations require unique contiguous ordinals, coherent byte counts, and declared fault correlations".to_owned(),
            None,
            index
                .adversary_observations
                .iter()
                .map(|(sequence, _)| format!("history:{sequence}"))
                .collect(),
        ));
    }
}

fn observation_shape(observation: &ProtocolAdversaryObservation) -> bool {
    if observation.request_bytes < 12 {
        return false;
    }
    match &observation.outcome {
        AdversaryOutcome::Baseline => observation.response_bytes > 0,
        AdversaryOutcome::FaultApplied { fault } => match fault {
            ProtocolFault::PartialFrame { bytes } => {
                observation.response_bytes > 0 && observation.response_bytes <= *bytes
            }
            ProtocolFault::Disconnect { point } => match point {
                DisconnectPoint::AfterRequest | DisconnectPoint::BeforeResponse => {
                    observation.response_bytes == 0
                }
                DisconnectPoint::AfterResponse => observation.response_bytes > 0,
            },
            ProtocolFault::WrongCorrelationId { .. } | ProtocolFault::StaleResponse => {
                observation.response_bytes > 0
            }
            ProtocolFault::Stall { .. } => true,
        },
    }
}

fn verify_process(index: &HistoryIndex, violations: &mut Vec<Violation>) {
    let processes = index
        .environment_operations
        .iter()
        .filter(|(_, operation)| operation.kind == EnvironmentOperationKind::ProtocolAdversary)
        .collect::<Vec<_>>();
    let valid = processes.as_slice().first().is_some_and(|(_, operation)| {
        processes.len() == 1 && operation.status == EnvironmentOperationStatus::Succeeded
    });
    if !valid {
        violations.push(violation(
            "ADV-003",
            format!(
                "expected one successful external adversary process, observed {}",
                processes.len()
            ),
            None,
            processes
                .iter()
                .map(|(sequence, _)| format!("history:{sequence}"))
                .collect(),
        ));
    }
}

fn preceding_control<'a>(
    scenario: &'a Scenario,
    action: &ScenarioAction,
) -> Option<&'a ProtocolFaultAction> {
    let position = scenario
        .steps
        .iter()
        .position(|step| &step.action == action)?;
    match &scenario.steps.get(position.checked_sub(1)?)?.action {
        ScenarioAction::ArmProtocolFault(control) => Some(control),
        _ => None,
    }
}
