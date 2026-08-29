//! Broker-role recovery verification binds independent ownership to public progress.

use testlab_schema::{
    BrokerRoleTarget, EnvironmentOperation, EnvironmentOperationKind, EnvironmentOperationStatus,
    Scenario, ScenarioAction, TerminalStatus, TransactionDisposition, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for target in scenario.steps.iter().filter_map(|step| match &step.action {
        ScenarioAction::StopBrokerRole { target, .. } => Some(target),
        _ => None,
    }) {
        verify_target(target, index, violations);
    }
}

fn verify_target(target: &BrokerRoleTarget, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    let before = facts(index, target, "before_stop");
    let after = facts(index, target, "after_election");
    let exact_facts = before.len() == 1
        && after.len() == 1
        && before[0].node != after[0].node
        && before[0].service != after[0].service;
    if !exact_facts {
        violations.push(violation(
            "FAULT-001",
            format!(
                "broker role {target:?} expected one distinct pre-stop owner and post-election replacement"
            ),
            None,
            references(&before, &after),
        ));
        return;
    }
    let before = before[0];
    let after = after[0];
    let stop = matching_operations(
        index,
        EnvironmentOperationKind::BrokerStop,
        before.sequence,
        after.sequence,
        before.service,
    );
    let starts = matching_operations(
        index,
        EnvironmentOperationKind::BrokerStart,
        after.sequence,
        u64::MAX,
        before.service,
    );
    let start = starts.first().copied();
    let readiness = start.is_some_and(|(sequence, _)| {
        index
            .environment_operations
            .iter()
            .any(|(candidate, operation)| {
                *candidate == sequence.saturating_add(1)
                    && exact_readiness(operation, before.service)
            })
    });
    let terminals = stop.len() == 1
        && starts.len() == 1
        && successful(stop.first().copied())
        && successful(start)
        && readiness;
    if !terminals {
        violations.push(violation(
            "FAULT-002",
            format!(
                "broker role {target:?} did not retain one ordered successful stop, restore, and readiness sequence for {}",
                before.service
            ),
            None,
            operation_references(&stop, &starts),
        ));
        return;
    }
    let start_sequence = start.map_or(u64::MAX, |(sequence, _)| *sequence);
    if !has_progress(target, index, after.sequence, start_sequence) {
        violations.push(violation(
            "FAULT-003",
            format!(
                "broker role {target:?} had no matching successful public progress while the original owner was offline"
            ),
            None,
            vec![
                format!("history:{}", after.sequence),
                format!("history:{start_sequence}"),
            ],
        ));
    }
}

#[derive(Clone, Copy)]
struct RoleFact<'a> {
    sequence: u64,
    node: i32,
    service: &'a str,
}

fn facts<'a>(index: &'a HistoryIndex, target: &BrokerRoleTarget, stage: &str) -> Vec<RoleFact<'a>> {
    let mut prefix = vec![target.role_name().to_owned()];
    prefix.extend(target.evidence_target());
    index
        .environment_operations
        .iter()
        .filter_map(|(sequence, operation)| {
            if operation.kind != EnvironmentOperationKind::BrokerRoleObserve
                || operation.status != EnvironmentOperationStatus::Succeeded
                || operation.program != "testlab-kafka-role-observer/1"
                || operation.args.len() != prefix.len() + 3
                || operation.args[..prefix.len()] != prefix
                || operation.args[prefix.len()] != stage
            {
                return None;
            }
            Some(RoleFact {
                sequence: *sequence,
                node: operation.args[prefix.len() + 1].parse().ok()?,
                service: &operation.args[prefix.len() + 2],
            })
        })
        .collect()
}

fn matching_operations<'a>(
    index: &'a HistoryIndex,
    kind: EnvironmentOperationKind,
    after: u64,
    before: u64,
    service: &str,
) -> Vec<&'a (u64, EnvironmentOperation)> {
    index
        .environment_operations
        .iter()
        .filter(|(sequence, operation)| {
            *sequence > after
                && *sequence < before
                && operation.kind == kind
                && exact_compose_control(operation, kind, service)
        })
        .collect()
}

fn exact_compose_control(
    operation: &EnvironmentOperation,
    kind: EnvironmentOperationKind,
    service: &str,
) -> bool {
    let verb = match kind {
        EnvironmentOperationKind::BrokerStop => "stop",
        EnvironmentOperationKind::BrokerStart => "start",
        _ => return false,
    };
    operation.program == "docker"
        && operation.args.len() >= 2
        && operation.args[operation.args.len() - 2] == verb
        && operation.args.last().is_some_and(|value| value == service)
}

fn exact_readiness(operation: &EnvironmentOperation, service: &str) -> bool {
    let args = &operation.args;
    operation.kind == EnvironmentOperationKind::Readiness
        && operation.status == EnvironmentOperationStatus::Succeeded
        && operation.program == "docker"
        && args.len() >= 6
        && args[args.len() - 6] == "exec"
        && args[args.len() - 5] == "--no-TTY"
        && args[args.len() - 4] == service
        && args[args.len() - 3] == "/opt/kafka/bin/kafka-broker-api-versions.sh"
        && args[args.len() - 2] == "--bootstrap-server"
        && args
            .last()
            .is_some_and(|value| value.starts_with("localhost:"))
}

fn successful(value: Option<&(u64, EnvironmentOperation)>) -> bool {
    value.is_some_and(|(_, operation)| operation.status == EnvironmentOperationStatus::Succeeded)
}

fn has_progress(target: &BrokerRoleTarget, index: &HistoryIndex, after: u64, before: u64) -> bool {
    let within = |sequence: u64| sequence > after && sequence < before;
    match target {
        BrokerRoleTarget::PartitionLeader { .. } => {
            index.terminals.values().flatten().any(|value| {
                within(value.history_sequence) && value.status == TerminalStatus::Acknowledged
            })
        }
        BrokerRoleTarget::Controller => index
            .topics_created
            .values()
            .flatten()
            .any(|value| within(value.history_sequence)),
        BrokerRoleTarget::GroupCoordinator { .. } => {
            index.receives.values().flatten().any(|value| {
                within(value.history_sequence)
                    && value.committed == Some(true)
                    && !value.records.is_empty()
            })
        }
        BrokerRoleTarget::TransactionCoordinator { .. } => index
            .transactions_completed
            .values()
            .flatten()
            .any(|value| {
                within(value.history_sequence)
                    && value.disposition == TransactionDisposition::Commit
            }),
    }
}

fn references(before: &[RoleFact<'_>], after: &[RoleFact<'_>]) -> Vec<String> {
    before
        .iter()
        .chain(after)
        .map(|fact| format!("history:{}", fact.sequence))
        .collect()
}

fn operation_references(
    stop: &[&(u64, EnvironmentOperation)],
    start: &[&(u64, EnvironmentOperation)],
) -> Vec<String> {
    stop.iter()
        .chain(start)
        .map(|(sequence, _)| format!("history:{sequence}"))
        .collect()
}
