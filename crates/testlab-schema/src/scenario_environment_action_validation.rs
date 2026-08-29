//! Environment action validation owns paired broker and broker-role disruptions.

use crate::scenario_action_validation::ActionStates;
use crate::{
    BrokerAclOperation, BrokerAclResource, BrokerPolicy, BrokerPolicyState, ScenarioAction,
};

pub(crate) fn validate(
    action: &ScenarioAction,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    match action {
        ScenarioAction::ArmProtocolFault(action) => {
            if !state
                .environment_operations
                .insert(action.operation_id.clone())
            {
                problems.push(format!(
                    "duplicate environment operation id {}",
                    action.operation_id
                ));
            }
            if let Err(problem) = action.validate() {
                problems.push(problem);
            }
        }
        ScenarioAction::AlterNetworkFault(action) => {
            validate_network_identity(&action.operation_id, state, problems);
            validate_network_bound(action.broker_ordinal, action.timeout_ms, problems);
            if let crate::NetworkFault::Delay { delay_ms, .. } = &action.fault
                && !(10..=5_000).contains(delay_ms)
            {
                problems.push("network delay_ms must be between 10 and 5000".into());
            }
            match action.state {
                crate::NetworkFaultState::Present => {
                    if state
                        .network_faults
                        .insert(action.broker_ordinal, action.fault.clone())
                        .is_some()
                    {
                        problems.push(format!(
                            "broker {} already has an active network fault",
                            action.broker_ordinal
                        ));
                    }
                }
                crate::NetworkFaultState::Absent => {
                    if state.network_faults.get(&action.broker_ordinal) == Some(&action.fault) {
                        state.network_faults.remove(&action.broker_ordinal);
                    } else {
                        problems.push(format!(
                            "network fault {:?} on broker {} was removed without an exact apply",
                            action.fault, action.broker_ordinal
                        ));
                    }
                }
            }
        }
        ScenarioAction::CutNetworkConnections(action) => {
            validate_network_identity(&action.operation_id, state, problems);
            validate_network_bound(action.broker_ordinal, action.timeout_ms, problems);
        }
        ScenarioAction::RestartBroker {
            broker_ordinal,
            timeout_ms,
        } => validate_broker_bound(*broker_ordinal, *timeout_ms, problems),
        ScenarioAction::StopBroker {
            broker_ordinal,
            timeout_ms,
        } => {
            validate_broker_bound(*broker_ordinal, *timeout_ms, problems);
            if !state.stopped_brokers.insert(*broker_ordinal) {
                problems.push(format!("broker {broker_ordinal} was stopped twice"));
            }
        }
        ScenarioAction::StartBroker {
            broker_ordinal,
            timeout_ms,
        } => {
            validate_broker_bound(*broker_ordinal, *timeout_ms, problems);
            if !state.stopped_brokers.remove(broker_ordinal) {
                problems.push(format!(
                    "broker {broker_ordinal} was started without a stop"
                ));
            }
        }
        ScenarioAction::StopBrokerRole { target, timeout_ms } => {
            validate_role_control(target, *timeout_ms, true, state, problems);
        }
        ScenarioAction::RestoreBrokerRole { target, timeout_ms } => {
            validate_role_control(target, *timeout_ms, false, state, problems);
        }
        ScenarioAction::AlterBrokerPolicy(action) => {
            validate_policy(action, state, problems);
        }
        _ => problems.push("non-environment action reached environment validation".into()),
    }
}

fn validate_network_identity(
    operation_id: &crate::EnvironmentOperationId,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    if !state.environment_operations.insert(operation_id.clone()) {
        problems.push(format!("duplicate environment operation id {operation_id}"));
    }
}

fn validate_network_bound(broker_ordinal: u16, timeout_ms: u64, problems: &mut Vec<String>) {
    if broker_ordinal == 0 {
        problems.push("network fault broker ordinal must be one-based".into());
    }
    if !(100..=60_000).contains(&timeout_ms) {
        problems.push("network fault timeout_ms must be between 100 and 60000".into());
    }
}

fn validate_policy(
    action: &crate::BrokerPolicyAction,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    if !(100..=600_000).contains(&action.timeout_ms) {
        problems.push("broker policy timeout_ms must be between 100 and 600000".into());
    }
    match &action.policy {
        BrokerPolicy::Acl {
            resource,
            operation,
        } => validate_acl(resource, *operation, problems),
        BrokerPolicy::Quota {
            bytes_per_second,
            minimum_active_ms,
            ..
        } => {
            if !(1..=1_000_000_000).contains(bytes_per_second) {
                problems
                    .push("broker quota bytes_per_second must be between 1 and 1000000000".into());
            }
            if !(100..=600_000).contains(minimum_active_ms) {
                problems
                    .push("broker quota minimum_active_ms must be between 100 and 600000".into());
            }
        }
    }
    match action.state {
        BrokerPolicyState::Present if !state.broker_policies.insert(action.policy.clone()) => {
            problems.push(format!(
                "broker policy {:?} was established twice",
                action.policy
            ));
        }
        BrokerPolicyState::Absent if !state.broker_policies.remove(&action.policy) => {
            problems.push(format!(
                "broker policy {:?} was removed without an apply",
                action.policy
            ));
        }
        BrokerPolicyState::Present | BrokerPolicyState::Absent => {}
    }
}

fn validate_acl(
    resource: &BrokerAclResource,
    operation: BrokerAclOperation,
    problems: &mut Vec<String>,
) {
    if resource.name().trim().is_empty() || resource.name().len() > 249 {
        problems.push("broker ACL resource name must contain between 1 and 249 bytes".into());
    }
    let supported = matches!(
        (resource, operation),
        (
            BrokerAclResource::Topic { .. },
            BrokerAclOperation::Write | BrokerAclOperation::Create
        ) | (BrokerAclResource::Group { .. }, BrokerAclOperation::Read)
            | (
                BrokerAclResource::TransactionalId { .. },
                BrokerAclOperation::Write
            )
    );
    if !supported {
        problems.push(format!(
            "broker ACL operation {operation:?} is unsupported for {resource:?}"
        ));
    }
}

fn validate_broker_bound(broker_ordinal: u16, timeout_ms: u64, problems: &mut Vec<String>) {
    if broker_ordinal == 0 {
        problems.push("broker restart ordinal must be one-based".to_owned());
    }
    if !(100..=600_000).contains(&timeout_ms) {
        problems.push("broker restart timeout_ms must be between 100 and 600000".to_owned());
    }
}

fn validate_role_control(
    target: &crate::BrokerRoleTarget,
    timeout_ms: u64,
    stop: bool,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    match target {
        crate::BrokerRoleTarget::PartitionLeader { topic, partition } => {
            if topic.trim().is_empty() || *partition < 0 {
                problems.push(
                    "partition leader control requires a topic and nonnegative partition".into(),
                );
            }
        }
        crate::BrokerRoleTarget::GroupCoordinator { group_id } if group_id.trim().is_empty() => {
            problems.push("group coordinator control requires a nonempty group_id".into());
        }
        crate::BrokerRoleTarget::TransactionCoordinator { transactional_id }
            if transactional_id.trim().is_empty() =>
        {
            problems.push(
                "transaction coordinator control requires a nonempty transactional_id".into(),
            );
        }
        crate::BrokerRoleTarget::Controller
        | crate::BrokerRoleTarget::GroupCoordinator { .. }
        | crate::BrokerRoleTarget::TransactionCoordinator { .. } => {}
    }
    if !(100..=600_000).contains(&timeout_ms) {
        problems.push("broker role control timeout_ms must be between 100 and 600000".into());
    }
    if stop && !state.role_disruptions.insert(target.clone()) {
        problems.push(format!("broker role {target:?} was stopped twice"));
    }
    if !stop && !state.role_disruptions.remove(target) {
        problems.push(format!(
            "broker role {target:?} was restored without a stop"
        ));
    }
}
