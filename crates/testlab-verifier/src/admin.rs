//! Admin verification joins exact commands, public completions, and independent broker facts.

use testlab_schema::{BrokerObservation, Scenario, ScenarioAction, Violation};

use crate::admin_batch::verify_batch_action;
use crate::admin_cluster::verify_cluster_action;
use crate::admin_config::verify_config_action;
use crate::admin_discovery::verify_discovery_action;
use crate::admin_failure::verify_expected_failure;
use crate::admin_group::verify_group_action;
use crate::admin_group_batch::verify_group_batch_action;
use crate::admin_records::verify_records_action;
use crate::admin_topic::verify_topic_action;
use crate::admin_validate_only::verify_validate_only_action;
use crate::index::HistoryIndex;
use crate::support::violation;

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
        let (exact, count) = index.admin_command_state(&step.action);
        if count == 0 {
            if index.command_failures.is_empty()
                || expected_failure_step.is_some_and(|failure| step_index < failure)
            {
                let operation_id = operation_id(&step.action).cloned();
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
            let operation_id = operation_id(&step.action).cloned();
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
            let operation_id = operation_id(&step.action).cloned();
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
            || verify_batch_action(&step.action, index, violations)
            || verify_validate_only_action(&step.action, index, violations)
            || verify_group_batch_action(scenario, &step.action, index, violations)
            || verify_config_action(&step.action, index, violations)
            || verify_topic_action(&step.action, index, violations)
            || verify_cluster_action(&step.action, index, violations)
            || verify_group_action(&step.action, index, violations)
            || verify_records_action(&step.action, index, violations)
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
    operation_id
        .map(|value| vec![format!("scenario:operation:{value}")])
        .unwrap_or_default()
}

fn contract(action: &ScenarioAction) -> Option<&'static str> {
    Some(match action {
        ScenarioAction::CreateTopic(value) if value.expected_error_code.is_some() => "ADMIN-014",
        ScenarioAction::CreatePartitions(value) if value.expected_error_code.is_some() => {
            "ADMIN-019"
        }
        ScenarioAction::DeleteTopic(value) if value.expected_error_code.is_some() => "ADMIN-019",
        ScenarioAction::DescribeTopic(value) if value.expected_error_code.is_some() => "ADMIN-019",
        ScenarioAction::ListOffsets(value) if value.expected_error_code.is_some() => "ADMIN-019",
        ScenarioAction::CreateTopic(value) if value.validate_only => "ADMIN-020",
        ScenarioAction::CreatePartitions(value) if value.validate_only => "ADMIN-021",
        ScenarioAction::AlterTopicConfig(value) if value.validate_only => "ADMIN-022",
        ScenarioAction::ListConsumerGroupOffsetsBatch(_) => "ADMIN-023",
        ScenarioAction::ListConsumerGroupsOffsets(_) => "ADMIN-024",
        ScenarioAction::AlterConsumerGroupOffsets(_) => "ADMIN-025",
        ScenarioAction::DeleteConsumerGroupOffsets(_) => "ADMIN-026",
        ScenarioAction::DescribeClassicGroups(_) => "ADMIN-027",
        ScenarioAction::CreateTopic(_) => "ADMIN-001",
        ScenarioAction::CreateTopicsBatch(_) => "ADMIN-018",
        ScenarioAction::CreatePartitions(_) => "ADMIN-002",
        ScenarioAction::DescribeTopic(_) => "ADMIN-003",
        ScenarioAction::ListTopics(_) => "ADMIN-004",
        ScenarioAction::ListOffsets(_) => "ADMIN-005",
        ScenarioAction::DeleteRecords(_) => "ADMIN-017",
        ScenarioAction::DescribeTopicConfig(_) => "ADMIN-015",
        ScenarioAction::AlterTopicConfig(_) => "ADMIN-016",
        ScenarioAction::ListConsumerGroupOffsets(_) => "ADMIN-006",
        ScenarioAction::DeleteTopic(_) => "ADMIN-007",
        ScenarioAction::DescribeCluster(_) => "ADMIN-008",
        ScenarioAction::ListConsumerGroups(_) => "ADMIN-009",
        ScenarioAction::DescribeConsumerGroup(_) => "ADMIN-010",
        ScenarioAction::AlterConsumerGroupOffset(_) => "ADMIN-011",
        ScenarioAction::DeleteConsumerGroupOffset(_) => "ADMIN-012",
        ScenarioAction::DeleteConsumerGroup(_) => "ADMIN-013",
        _ => return None,
    })
}

fn operation_id(action: &ScenarioAction) -> Option<&testlab_schema::OperationId> {
    Some(match action {
        ScenarioAction::CreateTopic(value) => &value.operation_id,
        ScenarioAction::CreateTopicsBatch(value) => &value.operation_id,
        ScenarioAction::CreatePartitions(value) => &value.operation_id,
        ScenarioAction::DeleteTopic(value) => &value.operation_id,
        ScenarioAction::DescribeTopic(value) => &value.operation_id,
        ScenarioAction::ListTopics(value) => &value.operation_id,
        ScenarioAction::ListOffsets(value) => &value.operation_id,
        ScenarioAction::DeleteRecords(value) => &value.operation_id,
        ScenarioAction::DescribeTopicConfig(value) => &value.operation_id,
        ScenarioAction::AlterTopicConfig(value) => &value.operation_id,
        ScenarioAction::DescribeCluster(value) => &value.operation_id,
        ScenarioAction::ListConsumerGroups(value) => &value.operation_id,
        ScenarioAction::DescribeConsumerGroup(value) => &value.operation_id,
        ScenarioAction::ListConsumerGroupOffsets(value) => &value.operation_id,
        ScenarioAction::AlterConsumerGroupOffset(value) => &value.operation_id,
        ScenarioAction::DeleteConsumerGroupOffset(value) => &value.operation_id,
        ScenarioAction::DeleteConsumerGroup(value) => &value.operation_id,
        ScenarioAction::ListConsumerGroupOffsetsBatch(value) => &value.operation_id,
        ScenarioAction::ListConsumerGroupsOffsets(value) => &value.operation_id,
        ScenarioAction::AlterConsumerGroupOffsets(value) => &value.operation_id,
        ScenarioAction::DeleteConsumerGroupOffsets(value) => &value.operation_id,
        ScenarioAction::DescribeClassicGroups(value) => &value.operation_id,
        _ => return None,
    })
}
