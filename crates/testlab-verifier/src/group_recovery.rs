//! Three-broker recovery verification binds disruption terminals to group progress.

use std::collections::BTreeSet;

use testlab_schema::{
    EnvironmentOperationKind, EnvironmentOperationStatus, Scenario, ScenarioAction, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    let stopped = broker_ordinals(scenario, true);
    let started = broker_ordinals(scenario, false);
    if stopped != BTreeSet::from([1, 2, 3]) || started != stopped {
        return;
    }
    let stops = operations(index, EnvironmentOperationKind::BrokerStop);
    let starts = operations(index, EnvironmentOperationKind::BrokerStart);
    let distinct_stops = stops
        .iter()
        .map(|(_, operation)| disruption_target(operation))
        .collect::<Option<BTreeSet<_>>>();
    let distinct_starts = starts
        .iter()
        .map(|(_, operation)| disruption_target(operation))
        .collect::<Option<BTreeSet<_>>>();
    let ordered_pairs = stops.iter().zip(&starts).enumerate().all(
        |(index, ((stop_sequence, stop), (start_sequence, start)))| {
            stop_sequence < start_sequence
                && disruption_target(stop).is_some()
                && disruption_target(stop) == disruption_target(start)
                && stops
                    .get(index + 1)
                    .is_none_or(|(next_stop_sequence, _)| start_sequence < next_stop_sequence)
        },
    );
    let terminals = stops.len() == 3
        && starts.len() == 3
        && distinct_stops
            .as_ref()
            .is_some_and(|targets| targets.len() == 3)
        && distinct_stops == distinct_starts
        && ordered_pairs
        && stops
            .iter()
            .chain(&starts)
            .all(|(_, operation)| operation.status == EnvironmentOperationStatus::Succeeded);
    let receive_sequences = index
        .receives
        .values()
        .flatten()
        .filter(|receive| receive.committed == Some(true) && !receive.records.is_empty())
        .map(|receive| receive.history_sequence)
        .chain(
            index
                .group_receive_sets
                .values()
                .flatten()
                .filter(|receive| {
                    receive
                        .completion
                        .members
                        .iter()
                        .all(|member| member.committed)
                        && receive
                            .completion
                            .members
                            .iter()
                            .any(|member| !member.records.is_empty())
                })
                .map(|receive| receive.history_sequence),
        )
        .collect::<Vec<_>>();
    let progress = stops
        .iter()
        .zip(&starts)
        .all(|((stop_sequence, _), (start_sequence, _))| {
            receive_sequences.iter().any(|receive_sequence| {
                receive_sequence > stop_sequence && receive_sequence < start_sequence
            })
        });
    if !terminals || !progress {
        violations.push(violation(
            "CONS-011",
            "three-broker group recovery did not retain three distinct successful stop/start pairs with committed progress between each pair".to_owned(),
            None,
            stops
                .iter()
                .chain(&starts)
                .map(|(sequence, _)| format!("history:{sequence}"))
                .collect(),
        ));
    }
}

fn disruption_target(
    operation: &testlab_schema::EnvironmentOperation,
) -> Option<(&str, &[String], &str)> {
    let (prefix, service) = match (operation.kind, operation.args.as_slice()) {
        (EnvironmentOperationKind::BrokerStop, [prefix @ .., verb, service]) if verb == "stop" => {
            (prefix, service)
        }
        (EnvironmentOperationKind::BrokerStart, [prefix @ .., verb, service])
            if verb == "start" =>
        {
            (prefix, service)
        }
        (EnvironmentOperationKind::BrokerStart, [prefix @ .., verb, no_deps, service])
            if verb == "restart" && no_deps == "--no-deps" =>
        {
            (prefix, service)
        }
        _ => return None,
    };
    if prefix.first().is_none_or(|command| command != "compose") || service.is_empty() {
        return None;
    }
    Some((&operation.program, prefix, service))
}

fn broker_ordinals(scenario: &Scenario, stop: bool) -> BTreeSet<u16> {
    scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::StopBroker { broker_ordinal, .. } if stop => Some(*broker_ordinal),
            ScenarioAction::StartBroker { broker_ordinal, .. } if !stop => Some(*broker_ordinal),
            _ => None,
        })
        .collect()
}

fn operations(
    index: &HistoryIndex,
    kind: EnvironmentOperationKind,
) -> Vec<&(u64, testlab_schema::EnvironmentOperation)> {
    index
        .environment_operations
        .iter()
        .filter(|(_, operation)| operation.kind == kind)
        .collect()
}
