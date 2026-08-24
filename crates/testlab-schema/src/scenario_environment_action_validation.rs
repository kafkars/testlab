//! Environment action validation owns paired broker and partition disruptions.

use crate::ScenarioAction;
use crate::scenario_action_validation::ActionStates;

pub(crate) fn validate(
    action: &ScenarioAction,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    match action {
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
        ScenarioAction::StopPartitionLeader {
            topic,
            partition,
            timeout_ms,
        } => validate_partition_control(topic, *partition, *timeout_ms, true, state, problems),
        ScenarioAction::RestorePartitionLeader {
            topic,
            partition,
            timeout_ms,
        } => validate_partition_control(topic, *partition, *timeout_ms, false, state, problems),
        _ => problems.push("non-environment action reached environment validation".into()),
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

fn validate_partition_control(
    topic: &str,
    partition: i32,
    timeout_ms: u64,
    stop: bool,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    if topic.trim().is_empty() || partition < 0 {
        problems.push("partition leader control requires a topic and nonnegative partition".into());
    }
    if !(100..=600_000).contains(&timeout_ms) {
        problems.push("partition leader control timeout_ms must be between 100 and 600000".into());
    }
    let key = (topic.to_owned(), partition);
    if stop && !state.leader_disruptions.insert(key.clone()) {
        problems.push(format!(
            "partition leader {topic}:{partition} was stopped twice"
        ));
    }
    if !stop && !state.leader_disruptions.remove(&key) {
        problems.push(format!(
            "partition leader {topic}:{partition} was restored without a stop"
        ));
    }
}
