//! Network contracts join declared controls, proxy facts, and process integrity.

use std::collections::BTreeSet;

use testlab_schema::{
    EnvironmentOperationKind, EnvironmentOperationStatus, NetworkDirection, NetworkFault,
    NetworkFaultState, NetworkProxyControl, NetworkProxyObservation, Scenario, ScenarioAction,
    Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    let declared = declared_controls(scenario);
    if declared.is_empty() {
        return;
    }
    for control in &declared {
        verify_control(control, index, violations);
    }
    verify_observations(&declared, index, violations);
    verify_process(index, violations);
    crate::network_proxy_progress::verify(scenario, index, violations);
}

fn declared_controls(scenario: &Scenario) -> Vec<NetworkProxyControl> {
    scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::AlterNetworkFault(action) => {
                Some(NetworkProxyControl::AlterFault(action.clone()))
            }
            ScenarioAction::CutNetworkConnections(action) => {
                Some(NetworkProxyControl::CutConnections(action.clone()))
            }
            _ => None,
        })
        .collect()
}

fn verify_control(
    declared: &NetworkProxyControl,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let controls = index
        .network_proxy_controls
        .iter()
        .filter(|(_, value)| value.operation_id() == declared.operation_id())
        .collect::<Vec<_>>();
    let exact = controls
        .first()
        .is_some_and(|(_, value)| controls.len() == 1 && value == declared);
    if exact {
        return;
    }
    violations.push(violation(
        "NET-001",
        format!(
            "network control {} expected one exact acknowledged value, observed {}",
            declared.operation_id(),
            controls.len()
        ),
        None,
        controls
            .iter()
            .map(|(sequence, _)| format!("history:{sequence}"))
            .collect(),
    ));
}

fn verify_observations(
    declared: &[NetworkProxyControl],
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let ids = declared
        .iter()
        .map(NetworkProxyControl::operation_id)
        .collect::<BTreeSet<_>>();
    let mut ordinals = BTreeSet::new();
    let exact_controls = index.network_proxy_controls.len() == declared.len()
        && index
            .network_proxy_controls
            .iter()
            .all(|(_, actual)| declared.contains(actual));
    let coherent = index
        .network_proxy_observations
        .iter()
        .all(|(sequence, observation)| {
            ordinals.insert(observation.observation())
                && observation_matches(*sequence, observation, declared, &ids, index)
        });
    let contiguous = ordinals
        .iter()
        .copied()
        .eq(0..u64::try_from(ordinals.len()).unwrap_or(u64::MAX));
    let expected = declared
        .iter()
        .filter(|control| match control {
            NetworkProxyControl::AlterFault(action) => action.state == NetworkFaultState::Absent,
            NetworkProxyControl::CutConnections(_) => true,
        })
        .count();
    if exact_controls
        && coherent
        && contiguous
        && index.network_proxy_observations.len() == expected
    {
        return;
    }
    violations.push(violation(
        "NET-001",
        format!(
            "network proxy expected {expected} unique contiguous completed effect observations, observed {}",
            index.network_proxy_observations.len()
        ),
        None,
        index
            .network_proxy_observations
            .iter()
            .map(|(sequence, _)| format!("history:{sequence}"))
            .collect(),
    ));
}

fn observation_matches(
    observation_sequence: u64,
    observation: &NetworkProxyObservation,
    controls: &[NetworkProxyControl],
    declared: &BTreeSet<&testlab_schema::EnvironmentOperationId>,
    index: &HistoryIndex,
) -> bool {
    match observation {
        NetworkProxyObservation::ConnectionsCut(value) => {
            declared.contains(&value.operation_id)
                && controls.iter().any(|control| {
                    matches!(control, NetworkProxyControl::CutConnections(action)
                        if action.operation_id == value.operation_id
                            && action.broker_ordinal == value.broker_ordinal)
                })
                && value.connections_cut > 0
                && control_sequence(index, &value.operation_id)
                    .is_some_and(|sequence| sequence < observation_sequence)
        }
        NetworkProxyObservation::FaultWindow(value) => {
            let ordered = control_sequence(index, &value.apply_operation_id)
                .zip(control_sequence(index, &value.remove_operation_id))
                .is_some_and(|(apply, remove)| apply < remove && remove < observation_sequence);
            declared.contains(&value.apply_operation_id)
                && declared.contains(&value.remove_operation_id)
                && ordered
                && value.started_unix_ms <= value.completed_unix_ms
                && value
                    .connections_at_start
                    .saturating_add(value.connections_accepted)
                    > 0
                && fault_effect(value)
                && exact_window_controls(value, index)
        }
    }
}

fn exact_window_controls(
    observation: &testlab_schema::NetworkFaultWindowObservation,
    index: &HistoryIndex,
) -> bool {
    let apply = index.network_proxy_controls.iter().any(|(_, control)| {
        matches!(
            control,
            NetworkProxyControl::AlterFault(action)
                if action.operation_id == observation.apply_operation_id
                    && action.broker_ordinal == observation.broker_ordinal
                    && action.fault == observation.fault
                    && action.state == NetworkFaultState::Present
        )
    });
    let remove = index.network_proxy_controls.iter().any(|(_, control)| {
        matches!(
            control,
            NetworkProxyControl::AlterFault(action)
                if action.operation_id == observation.remove_operation_id
                    && action.broker_ordinal == observation.broker_ordinal
                    && action.fault == observation.fault
                    && action.state == NetworkFaultState::Absent
        )
    });
    apply && remove
}

fn fault_effect(observation: &testlab_schema::NetworkFaultWindowObservation) -> bool {
    match observation.fault {
        NetworkFault::Blackhole => observation.blocked_intervals > 0,
        NetworkFault::Delay {
            direction: NetworkDirection::ClientToBroker,
            ..
        } => observation.delayed_client_to_broker_bytes > 0,
        NetworkFault::Delay {
            direction: NetworkDirection::BrokerToClient,
            ..
        } => observation.delayed_broker_to_client_bytes > 0,
    }
}

fn control_sequence(
    index: &HistoryIndex,
    operation_id: &testlab_schema::EnvironmentOperationId,
) -> Option<u64> {
    let mut values = index
        .network_proxy_controls
        .iter()
        .filter(|(_, control)| control.operation_id() == operation_id);
    let (sequence, _) = values.next()?;
    values.next().is_none().then_some(*sequence)
}

fn verify_process(index: &HistoryIndex, violations: &mut Vec<Violation>) {
    let processes = index
        .environment_operations
        .iter()
        .filter(|(_, operation)| operation.kind == EnvironmentOperationKind::NetworkProxy)
        .collect::<Vec<_>>();
    let valid = processes.first().is_some_and(|(_, operation)| {
        processes.len() == 1
            && operation.status == EnvironmentOperationStatus::Succeeded
            && operation.exit_code == Some(0)
            && operation.diagnostic.is_none()
            && !operation.program.trim().is_empty()
            && operation
                .args
                .first()
                .is_some_and(|arg| arg == "network-proxy-worker")
            && operation.stdout_artifact.as_deref() == Some("network-proxy.jsonl")
            && operation.stderr_artifact.as_deref() == Some("network-proxy.stderr.txt")
    });
    if valid {
        return;
    }
    violations.push(violation(
        "NET-002",
        format!(
            "expected one successful external network proxy process, observed {}",
            processes.len()
        ),
        None,
        processes
            .iter()
            .map(|(sequence, _)| format!("history:{sequence}"))
            .collect(),
    ));
}
