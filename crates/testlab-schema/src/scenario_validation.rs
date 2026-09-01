//! Scenario validation owns identity, lifecycle, and assertion coherence.

use std::collections::BTreeSet;

use crate::{SCENARIO_SCHEMA_VERSION, Scenario, ScenarioError};

pub(crate) fn validate(scenario: &Scenario) -> Result<(), ScenarioError> {
    let mut problems = Vec::new();
    validate_header(scenario, &mut problems);
    validate_steps(scenario, &mut problems);
    if problems.is_empty() {
        Ok(())
    } else {
        Err(ScenarioError { problems })
    }
}

fn validate_header(scenario: &Scenario, problems: &mut Vec<String>) {
    if scenario.schema_version != SCENARIO_SCHEMA_VERSION {
        problems.push(format!(
            "unsupported schema version {}, expected {SCENARIO_SCHEMA_VERSION}",
            scenario.schema_version
        ));
    }
    if scenario.title.trim().is_empty() {
        problems.push("title must not be empty".to_owned());
    }
    if scenario.description.trim().is_empty() {
        problems.push("description must not be empty".to_owned());
    }
    if !(100..=600_000).contains(&scenario.timeout_ms) {
        problems.push("timeout_ms must be between 100 and 600000".to_owned());
    }
    if scenario.steps.is_empty() {
        problems.push("scenario must contain at least one step".to_owned());
    }
}

fn validate_steps(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut step_ids = BTreeSet::new();
    let mut state = crate::scenario_action_validation::ActionStates::default();
    let mut usage = BTreeSet::new();
    for step in &scenario.steps {
        if !step_ids.insert(step.id.clone()) {
            problems.push(format!("duplicate step id {}", step.id));
        }
        crate::scenario_capability_validation::record_usage(&step.action, &mut usage);
        crate::scenario_action_validation::validate_action(&step.action, &mut state, problems);
    }
    crate::scenario_capability_validation::validate_required(scenario, &usage, &state, problems);
    crate::scenario_record_correlation_validation::validate(scenario, problems);
    crate::scenario_broker_policy_validation::validate(scenario, problems);
    validate_role_targets(scenario, problems);
    crate::admin_transition_validation::validate(scenario, problems);
    crate::scenario_assertion_validation::validate(
        scenario,
        &state.sends,
        &state.transaction_sends,
        problems,
    );
    for (producer, (_, closed)) in state.producers {
        if !closed {
            problems.push(format!("producer {producer} was not closed"));
        }
    }
    for consumer in crate::consumer_action_validation::unclosed(state.consumers) {
        problems.push(format!("consumer {consumer} was not closed"));
    }
    for producer in crate::transaction_action_validation::unclosed(state.transactions) {
        problems.push(format!("transactional producer {producer} was not closed"));
    }
    for receive in crate::share_action_validation::unsettled(&state.share_batches) {
        problems.push(format!("share batch {receive} was not settled"));
    }
    for target in state.role_disruptions {
        problems.push(format!("broker role {target:?} was not restored"));
    }
    for broker in state.stopped_brokers {
        problems.push(format!("broker {broker} was not restarted"));
    }
    for policy in state.broker_policies {
        problems.push(format!("broker policy {policy:?} was not removed"));
    }
    for (broker, fault) in state.network_faults {
        problems.push(format!(
            "network fault {fault:?} on broker {broker} was not removed"
        ));
    }
    if let Some(concurrency_id) = state.active_concurrency {
        problems.push(format!("concurrent group {concurrency_id} was not joined"));
    }
    for (client, shutdown) in state.clients {
        if !shutdown {
            problems.push(format!("client {client} was not shut down"));
        }
    }
}

fn validate_role_targets(scenario: &Scenario, problems: &mut Vec<String>) {
    let partitions = scenario
        .steps
        .iter()
        .flat_map(|step| record_partitions(&step.action))
        .collect::<BTreeSet<_>>();
    let mut groups = BTreeSet::new();
    let mut transactions = BTreeSet::new();
    for step in &scenario.steps {
        match &step.action {
            crate::ScenarioAction::CreateGroupConsumer { group_id, .. } => {
                groups.insert(group_id.clone());
            }
            crate::ScenarioAction::CreateTransactionalProducer {
                transactional_id, ..
            } => {
                transactions.insert(transactional_id.clone());
            }
            crate::ScenarioAction::StopBrokerRole { target, .. } => match target {
                crate::BrokerRoleTarget::PartitionLeader { topic, partition }
                    if !partitions.contains(&(topic.clone(), *partition)) =>
                {
                    problems.push(format!(
                        "partition leader target {topic}:{partition} has no scenario record"
                    ));
                }
                crate::BrokerRoleTarget::GroupCoordinator { group_id }
                    if !groups.contains(group_id) =>
                {
                    problems.push(format!(
                        "group coordinator target {group_id} was not initialized before its stop"
                    ));
                }
                crate::BrokerRoleTarget::TransactionCoordinator { transactional_id }
                    if !transactions.contains(transactional_id) =>
                {
                    problems.push(format!(
                        "transaction coordinator target {transactional_id} was not initialized before its stop"
                    ));
                }
                _ => {}
            },
            _ => {}
        }
    }
}

fn record_partitions(action: &crate::ScenarioAction) -> Vec<(String, i32)> {
    match action {
        crate::ScenarioAction::Send { record, .. }
        | crate::ScenarioAction::CancelProducerSend(crate::CancelProducerSendCommand {
            record,
            ..
        }) => {
            vec![(record.topic.clone(), record.partition)]
        }
        crate::ScenarioAction::SendBatch { operations, .. }
        | crate::ScenarioAction::ExecuteTransaction { operations, .. } => operations
            .iter()
            .map(|operation| (operation.record.topic.clone(), operation.record.partition))
            .collect(),
        crate::ScenarioAction::FenceTransaction { operation, .. } => {
            vec![(operation.record.topic.clone(), operation.record.partition)]
        }
        crate::ScenarioAction::StartConcurrentActors(action) => action
            .actors
            .iter()
            .filter_map(|actor| match actor {
                crate::ConcurrentActor::ProducerSend { record, .. } => {
                    Some((record.topic.clone(), record.partition))
                }
                crate::ConcurrentActor::AssignedReceive { .. } => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}
