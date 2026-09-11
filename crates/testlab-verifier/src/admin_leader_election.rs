//! Leader-election verification joins public outcomes, metadata, and role facts.

use std::collections::BTreeSet;

use testlab_schema::{
    AdminLeaderElectionType, BrokerLeaderElectionPartition, BrokerRoleTarget, ElectLeadersAction,
    LeaderElectionSelection, ScenarioAction, Violation,
};

use crate::admin::{immediate_after_public, public_after_command};
use crate::admin_transactions::one;
use crate::broker_role_recovery::exact_role_owner;
use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify_leader_election_action(
    scenario_action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let ScenarioAction::ElectLeaders(action) = scenario_action else {
        return false;
    };
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_leader_elections
        .elected
        .get(&action.operation_id));
    let independent = one(index
        .admin_leader_elections
        .observed
        .get(&action.operation_id));
    let required = required(action);
    let mut transition_references = Vec::new();
    let matches = public.is_some_and(|public| {
        public_after_command(window, public.history_sequence)
            && public.value.operation_id == action.operation_id
            && public.value.election_type == action.election_type
            && public.value.throttle_time_ms <= action.timeout_ms
            && public_outcomes_match(action, &public.value.outcomes)
            && independent.is_some_and(|independent| {
                independent.value.operation_id == action.operation_id
                    && independent.value.election_type == action.election_type
                    && independent.value.partitions.len() == required.len()
                    && independent
                        .value
                        .partitions
                        .iter()
                        .zip(required)
                        .all(|(actual, expected)| partition_matches(actual, expected, action))
                    && transition_matches(
                        action,
                        index,
                        window.map(|(command, _)| command),
                        &independent.value.partitions,
                        &mut transition_references,
                    )
                    && immediate_after_public(
                        window,
                        public.history_sequence,
                        independent.history_sequence,
                    )
            })
    });
    if !matches {
        violations.push(violation(
            "ADMIN-060",
            format!(
                "admin operation {} expected deterministic successful election outcomes and immediate matching leader metadata",
                action.operation_id
            ),
            Some(action.operation_id.clone()),
            references(public, independent, transition_references),
        ));
    }
    true
}

fn required(action: &ElectLeadersAction) -> &[LeaderElectionSelection] {
    action
        .targets
        .as_deref()
        .unwrap_or(&action.required_partitions)
}

fn public_outcomes_match(
    action: &ElectLeadersAction,
    outcomes: &[testlab_schema::AdminLeaderElectionOutcome],
) -> bool {
    if outcomes.iter().any(|outcome| {
        outcome.topic.is_empty() || outcome.partition < 0 || outcome.error_code.is_some()
    }) {
        return false;
    }
    if let Some(targets) = &action.targets {
        return outcomes.len() == targets.len()
            && outcomes.iter().zip(targets).all(|(actual, expected)| {
                actual.topic == expected.topic && actual.partition == expected.partition
            });
    }
    let canonical = outcomes.windows(2).all(|pair| {
        pair[0].topic.as_bytes() < pair[1].topic.as_bytes()
            || (pair[0].topic == pair[1].topic && pair[0].partition < pair[1].partition)
    });
    canonical
        && action.required_partitions.iter().all(|required| {
            outcomes.iter().any(|outcome| {
                outcome.topic == required.topic && outcome.partition == required.partition
            })
        })
}

fn partition_matches(
    actual: &BrokerLeaderElectionPartition,
    expected: &LeaderElectionSelection,
    action: &ElectLeadersAction,
) -> bool {
    let replicas = actual.replicas.iter().copied().collect::<BTreeSet<_>>();
    let isr = actual
        .in_sync_replicas
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let identities = actual.topic == expected.topic && actual.partition == expected.partition;
    let valid = identities
        && actual.leader_id >= 0
        && !replicas.is_empty()
        && replicas.len() == actual.replicas.len()
        && isr.len() == actual.in_sync_replicas.len()
        && actual
            .in_sync_replicas
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        && replicas.contains(&actual.leader_id)
        && isr.contains(&actual.leader_id);
    match action.election_type {
        AdminLeaderElectionType::Preferred => {
            valid && actual.replicas.first() == Some(&actual.leader_id) && replicas == isr
        }
        AdminLeaderElectionType::Unclean => valid,
    }
}

fn transition_matches(
    action: &ElectLeadersAction,
    index: &HistoryIndex,
    command_sequence: Option<u64>,
    partitions: &[BrokerLeaderElectionPartition],
    references: &mut Vec<String>,
) -> bool {
    if !action.require_leader_change {
        return true;
    }
    let Some(target) = required(action).first() else {
        return false;
    };
    let role = BrokerRoleTarget::PartitionLeader {
        topic: target.topic.clone(),
        partition: target.partition,
    };
    let before = exact_role_owner(index, &role, "before_stop");
    let replacement = exact_role_owner(index, &role, "after_election");
    let restored = exact_role_owner(index, &role, "after_restore");
    references.extend(
        [before, replacement, restored]
            .into_iter()
            .flatten()
            .map(|(sequence, _)| format!("history:{sequence}")),
    );
    let (
        Some((before_sequence, before)),
        Some((replacement_sequence, replacement)),
        Some((restored_sequence, restored)),
        Some(command_sequence),
        Some(partition),
    ) = (
        before,
        replacement,
        restored,
        command_sequence,
        partitions.first(),
    )
    else {
        return false;
    };
    before_sequence < replacement_sequence
        && replacement_sequence < restored_sequence
        && restored_sequence < command_sequence
        && before != replacement
        && replacement == restored
        && partition.leader_id == before
        && partition.replicas.first() == Some(&before)
}

fn references<T, U>(
    public: Option<&crate::index::admin_features::Indexed<T>>,
    independent: Option<&crate::index::admin_features::Indexed<U>>,
    role: Vec<String>,
) -> Vec<String> {
    public
        .map(|value| format!("history:{}", value.history_sequence))
        .into_iter()
        .chain(independent.map(|value| format!("history:{}", value.history_sequence)))
        .chain(role)
        .collect()
}
