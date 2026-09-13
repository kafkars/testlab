//! Administrative verification dispatches actions to focused contract checks.

use crate::admin_acl::verify_acl_action;
use crate::admin_batch::verify_batch_action;
use crate::admin_broker_unregistration::verify as verify_broker_unregistration;
use crate::admin_client_quota::verify_client_quota_action;
use crate::admin_cluster::verify_cluster_action;
use crate::admin_config::verify_config_action;
use crate::admin_config_batch::verify_config_batch_action;
use crate::admin_config_resources::verify_config_resources_action;
use crate::admin_contract::contract;
use crate::admin_delegation_token::verify as verify_delegation_token;
use crate::admin_discovery::verify_discovery_action;
use crate::admin_failure::verify_expected_failure;
use crate::admin_features::verify_features_action;
use crate::admin_group::verify_group_action;
use crate::admin_group_batch::verify_group_batch_action;
use crate::admin_leader_election::verify_leader_election_action;
use crate::admin_log_dirs::verify_log_dirs_action;
use crate::admin_metadata_quorum::verify_metadata_quorum_action;
use crate::admin_offset_batch::verify_offset_batch_action;
use crate::admin_operation::operation_id;
use crate::admin_partition_reassignments::verify_partition_reassignments_action;
use crate::admin_producers::verify_producers_action;
use crate::admin_records::verify_records_action;
use crate::admin_records_batch::verify_records_batch_action;
use crate::admin_replica_log_dirs::verify_replica_log_dirs_action;
use crate::admin_share_group::verify_share_group_action;
use crate::admin_streams_group::verify as verify_streams_group;
use crate::admin_topic::verify_topic_action;
use crate::admin_topics_deletion::verify_topics_deletion_action;
use crate::admin_topics_description::verify_topics_description_action;
use crate::admin_transactions::verify_transactions_action;
use crate::admin_user_scram::verify_user_scram_action;
use crate::admin_validate_only::verify_validate_only_action;
use crate::index::HistoryIndex;
use crate::support::violation;
use testlab_schema::{BrokerObservation, Scenario, Violation};
pub(crate) fn verify_admin(
    scenario: &Scenario,
    index: &HistoryIndex,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    let mut prior_admin_command = None;
    let expected_failure_step = scenario.steps.iter().position(|step| {
        testlab_schema::expected_admin_error(&step.action).is_some()
            && !index.admin_command_failures(&step.action).is_empty()
    });
    for (step_index, step) in scenario.steps.iter().enumerate() {
        let Some(contract) = contract(&step.action) else {
            continue;
        };
        let operation_id = operation_id(&step.action).cloned();
        let (exact, count) = index.admin_command_state(&step.action);
        if count == 0 {
            if index.command_failures.is_empty()
                || expected_failure_step.is_some_and(|failure| step_index < failure)
            {
                violations.push(violation(
                    contract,
                    "admin action expected one exact wire command, observed none".to_owned(),
                    operation_id.clone(),
                    scenario_evidence(operation_id.as_ref()),
                ));
            }
            continue;
        }
        if !exact {
            violations.push(violation(
                contract,
                format!("admin action expected one exact wire command, observed {count} same-operation command(s)"),
                operation_id.clone(),
                scenario_evidence(operation_id.as_ref()),
            ));
            continue;
        }
        let Some(command_sequence) = index.admin_command_sequence(&step.action) else {
            continue;
        };
        if prior_admin_command.is_some_and(|prior| command_sequence <= prior) {
            violations.push(violation(
                contract,
                format!(
                    "admin command at history sequence {command_sequence} did not follow the prior scenario admin command"
                ),
                operation_id,
                vec![format!("history:{command_sequence}")],
            ));
            continue;
        }
        prior_admin_command = Some(command_sequence);
        if crate::adversary::verify_admin_failure(scenario, &step.action, index, violations)
            || verify_expected_failure(&step.action, index, violations)
            || verify_acl_action(&step.action, index, violations)
            || verify_client_quota_action(&step.action, index, violations)
            || verify_user_scram_action(&step.action, index, violations)
            || verify_delegation_token(&step.action, index, violations)
            || verify_streams_group(&step.action, index, violations)
            || verify_share_group_action(scenario, &step.action, index, violations)
            || verify_batch_action(scenario, &step.action, index, violations)
            || verify_offset_batch_action(&step.action, index, violations)
            || verify_leader_election_action(&step.action, index, violations)
            || verify_partition_reassignments_action(&step.action, index, violations)
            || verify_validate_only_action(&step.action, index, violations)
            || verify_group_batch_action(scenario, &step.action, index, violations)
            || verify_config_resources_action(&step.action, index, violations)
            || verify_config_batch_action(scenario, &step.action, index, violations)
            || verify_config_action(&step.action, index, violations)
            || verify_topic_action(scenario, &step.action, index, violations)
            || verify_topics_deletion_action(scenario, &step.action, index, violations)
            || verify_topics_description_action(&step.action, index, violations)
            || verify_broker_unregistration(&step.action, index, violations)
            || verify_cluster_action(&step.action, index, violations)
            || verify_features_action(&step.action, index, violations)
            || verify_producers_action(&step.action, index, violations)
            || verify_log_dirs_action(&step.action, index, violations)
            || verify_replica_log_dirs_action(&step.action, index, violations)
            || verify_metadata_quorum_action(&step.action, index, violations)
            || verify_transactions_action(&step.action, index, violations)
            || verify_group_action(&step.action, index, violations)
            || verify_records_action(&step.action, index, violations)
            || verify_records_batch_action(&step.action, index, violations)
        {
            continue;
        }
        let _ = verify_discovery_action(&step.action, index, observations, violations);
    }
}
pub(crate) type AdminCommandWindow = (u64, Option<u64>);
pub(crate) fn public_after_command(window: Option<AdminCommandWindow>, public: u64) -> bool {
    window.is_some_and(|(command, _)| command < public)
}
pub(crate) fn immediate_after_public(
    window: Option<AdminCommandWindow>,
    public: u64,
    observation: u64,
) -> bool {
    window
        .is_some_and(|(_, next)| public < observation && next.is_none_or(|next| observation < next))
}
fn scenario_evidence(operation_id: Option<&testlab_schema::OperationId>) -> Vec<String> {
    operation_id.map_or_else(Vec::new, |value| {
        vec![format!("scenario:operation:{value}")]
    })
}

#[cfg(test)]
#[path = "admin_test.rs"]
mod tests;
