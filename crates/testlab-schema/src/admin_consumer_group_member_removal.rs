//! Static consumer-group member removal binds exact instance identities to a named baseline.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{ClientId, ConsumerId, OperationId, Scenario, ScenarioAction};

/// Scenario intent for removing selected static members through one public call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RemoveConsumerGroupMembersAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Exact prior group description proving every static member remains registered.
    pub baseline_operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Caller-ordered distinct static group-instance identities.
    pub group_instance_ids: Vec<String>,
    /// Nonempty broker-visible reason supplied through the public builder.
    pub reason: String,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for removing selected static consumer-group members.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RemoveConsumerGroupMembersCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Caller-ordered distinct static group-instance identities.
    pub group_instance_ids: Vec<String>,
    /// Nonempty broker-visible reason supplied through the public builder.
    pub reason: String,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One caller-positioned public static-member removal outcome.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupMemberRemovalOutcome {
    /// Exact group-instance identity returned for this request position.
    pub group_instance_id: String,
    /// Stable normalized public error, or none on success.
    pub error_code: Option<String>,
}

/// Public result for one caller-ordered static-member removal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConsumerGroupMembersRemoval {
    /// Stable identity for the complete public call.
    pub operation_id: OperationId,
    /// Exact Kafka consumer-group identity.
    pub group_id: String,
    /// Nonnegative Kafka throttle observation.
    pub throttle_time_ms: u64,
    /// Caller-ordered public outcomes.
    pub outcomes: Vec<AdminConsumerGroupMemberRemovalOutcome>,
}

pub(crate) fn validate_action(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let ScenarioAction::RemoveConsumerGroupMembers(action) = action else {
        return false;
    };
    crate::admin_action_validation::validate_resource(
        &action.client_id,
        &action.operation_id,
        &action.group_id,
        "group_id",
        255,
        clients,
        operation_ids,
        problems,
    );
    if !(2..=32).contains(&action.group_instance_ids.len()) {
        problems.push(format!(
            "admin operation {} group_instance_ids must contain 2 to 32 entries",
            action.operation_id
        ));
    }
    let mut identities = BTreeSet::new();
    for identity in &action.group_instance_ids {
        if identity.is_empty() || identity.len() > 255 || !identities.insert(identity.as_str()) {
            problems.push(format!(
                "admin operation {} group_instance_ids must contain unique valid identities",
                action.operation_id
            ));
        }
    }
    if action.reason.is_empty() || action.reason.len() > 255 {
        problems.push(format!(
            "admin operation {} reason must contain 1 to 255 bytes",
            action.operation_id
        ));
    }
    crate::admin_action_validation::validate_timeout(
        &action.operation_id,
        action.timeout_ms,
        problems,
    );
    true
}

pub(crate) fn validate_transition(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut consumers = BTreeMap::<ConsumerId, StaticMember>::new();
    let mut consumer_groups = BTreeMap::<ConsumerId, String>::new();
    let mut descriptions = BTreeMap::<OperationId, Baseline>::new();
    let mut revisions = BTreeMap::<String, u64>::new();
    let mut client_shutdowns = BTreeMap::<ClientId, usize>::new();
    for (sequence, step) in scenario.steps.iter().enumerate() {
        match &step.action {
            ScenarioAction::CreateGroupConsumer {
                client_id,
                consumer_id,
                group_id,
                protocol,
                configuration,
                ..
            } => {
                let revision = revisions.entry(group_id.clone()).or_default();
                *revision = revision.saturating_add(1);
                consumer_groups.insert(consumer_id.clone(), group_id.clone());
                if let Some(group_instance_id) = configuration
                    .as_ref()
                    .and_then(|value| value.group_instance_id.clone())
                {
                    consumers.insert(
                        consumer_id.clone(),
                        StaticMember {
                            client_id: client_id.clone(),
                            group_id: group_id.clone(),
                            group_instance_id,
                            protocol: *protocol,
                            session_timeout_ms: configuration
                                .as_ref()
                                .and_then(|value| value.classic_session_timeout_ms),
                            abandoned_sequence: None,
                        },
                    );
                }
            }
            ScenarioAction::CloseGroupConsumer { consumer_id } => {
                if let Some(group_id) = consumer_groups.get(consumer_id) {
                    let revision = revisions.entry(group_id.clone()).or_default();
                    *revision = revision.saturating_add(1);
                }
            }
            ScenarioAction::AbandonGroupConsumer(action) => {
                if let Some(member) = consumers.get_mut(&action.consumer_id) {
                    member.abandoned_sequence = Some(sequence);
                }
                if let Some(group_id) = consumer_groups.get(&action.consumer_id) {
                    let revision = revisions.entry(group_id.clone()).or_default();
                    *revision = revision.saturating_add(1);
                }
            }
            ScenarioAction::ShutdownClient { client_id } => {
                client_shutdowns.insert(client_id.clone(), sequence);
                let groups = consumers
                    .values()
                    .filter(|member| &member.client_id == client_id)
                    .map(|member| member.group_id.clone())
                    .collect::<BTreeSet<_>>();
                for group_id in groups {
                    let revision = revisions.entry(group_id).or_default();
                    *revision = revision.saturating_add(1);
                }
            }
            ScenarioAction::DescribeConsumerGroup(action) => {
                descriptions.insert(
                    action.operation_id.clone(),
                    Baseline {
                        sequence,
                        group_id: action.group_id.clone(),
                        member_count: action.expected_member_count,
                        revision: revisions.get(&action.group_id).copied().unwrap_or_default(),
                    },
                );
            }
            ScenarioAction::RemoveConsumerGroupMembers(action) => {
                validate_removal(
                    action,
                    sequence,
                    &consumers,
                    &descriptions,
                    &revisions,
                    &client_shutdowns,
                    scenario.timeout_ms,
                    problems,
                );
                let revision = revisions.entry(action.group_id.clone()).or_default();
                *revision = revision.saturating_add(1);
            }
            _ => {}
        }
    }
}

#[derive(Clone, Debug)]
struct StaticMember {
    client_id: ClientId,
    group_id: String,
    group_instance_id: String,
    protocol: crate::GroupProtocol,
    session_timeout_ms: Option<u64>,
    abandoned_sequence: Option<usize>,
}

#[derive(Clone, Debug)]
struct Baseline {
    sequence: usize,
    group_id: String,
    member_count: u32,
    revision: u64,
}

fn validate_removal(
    action: &RemoveConsumerGroupMembersAction,
    sequence: usize,
    consumers: &BTreeMap<ConsumerId, StaticMember>,
    descriptions: &BTreeMap<OperationId, Baseline>,
    revisions: &BTreeMap<String, u64>,
    client_shutdowns: &BTreeMap<ClientId, usize>,
    scenario_timeout_ms: u64,
    problems: &mut Vec<String>,
) {
    let Some(baseline) = descriptions.get(&action.baseline_operation_id) else {
        problems.push(format!(
            "admin operation {} requires named prior group baseline {}",
            action.operation_id, action.baseline_operation_id
        ));
        return;
    };
    let expected_count = u32::try_from(action.group_instance_ids.len()).unwrap_or(u32::MAX);
    let unchanged = revisions.get(&action.group_id).copied().unwrap_or_default();
    if baseline.sequence >= sequence
        || baseline.group_id != action.group_id
        || baseline.member_count != expected_count
        || baseline.revision != unchanged
    {
        problems.push(format!(
            "admin operation {} requires an unchanged named baseline for every selected static member",
            action.operation_id
        ));
    }
    for identity in &action.group_instance_ids {
        let mut matching = consumers.values().filter(|member| {
            member.group_id == action.group_id && member.group_instance_id == *identity
        });
        let member = matching.next();
        if matching.next().is_some()
            || !member.is_some_and(|member| {
                member.protocol == crate::GroupProtocol::Classic
                    && member
                        .session_timeout_ms
                        .is_some_and(|timeout| timeout >= scenario_timeout_ms)
                    && member.abandoned_sequence.is_some_and(|abandoned| {
                        client_shutdowns
                            .get(&member.client_id)
                            .is_some_and(|shutdown| {
                                abandoned < *shutdown && *shutdown < baseline.sequence
                            })
                    })
            })
        {
            problems.push(format!(
                "admin operation {} requires one abandoned classic static member {identity}, an explicit session timeout covering the scenario, and its stopped client before the baseline",
                action.operation_id
            ));
        }
    }
}

#[cfg(test)]
#[path = "admin_consumer_group_member_removal_test.rs"]
mod tests;
