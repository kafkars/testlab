//! Partition-leader contracts bind replacement ownership to consumer records.

use testlab_schema::{AdapterCommand, BrokerRoleTarget, OperationId, Scenario, ScenarioAction};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(
    scenario: &Scenario,
    stop_position: usize,
    target: &BrokerRoleTarget,
    index: &HistoryIndex,
    after_election: u64,
    before_restore: u64,
    violations: &mut Vec<testlab_schema::Violation>,
) {
    if index.commands.is_empty() {
        return;
    }
    let BrokerRoleTarget::PartitionLeader { topic, partition } = target else {
        return;
    };
    let end = scenario.steps[stop_position + 1..]
        .iter()
        .position(|step| {
            matches!(&step.action, ScenarioAction::RestoreBrokerRole { target: actual, .. } if actual == target)
        })
        .map_or(scenario.steps.len(), |relative| {
            stop_position + 1 + relative
        });
    for step in &scenario.steps[stop_position + 1..end] {
        match &step.action {
            ScenarioAction::Receive {
                receive_id,
                expected_operation_id,
                ..
            } if targets_partition(scenario, [expected_operation_id], topic, *partition) => {
                verify_receive(
                    receive_id,
                    ReceiveKind::Assigned,
                    topic,
                    *partition,
                    index,
                    after_election,
                    before_restore,
                    violations,
                );
            }
            ScenarioAction::GroupReceive {
                receive_id,
                expected_operation_id,
                additional_expected_operation_ids,
                expected_error_code: None,
                ..
            } if targets_partition(
                scenario,
                std::iter::once(expected_operation_id).chain(additional_expected_operation_ids),
                topic,
                *partition,
            ) =>
            {
                verify_receive(
                    receive_id,
                    ReceiveKind::Group,
                    topic,
                    *partition,
                    index,
                    after_election,
                    before_restore,
                    violations,
                )
            }
            ScenarioAction::ShareReceive {
                receive_id,
                expected_operation_ids,
                ..
            } if targets_partition(scenario, expected_operation_ids, topic, *partition) => {
                verify_receive(
                    receive_id,
                    ReceiveKind::Share,
                    topic,
                    *partition,
                    index,
                    after_election,
                    before_restore,
                    violations,
                );
            }
            _ => {}
        }
    }
}

#[derive(Clone, Copy)]
enum ReceiveKind {
    Assigned,
    Group,
    Share,
}

#[allow(clippy::too_many_arguments)]
fn verify_receive(
    receive_id: &OperationId,
    kind: ReceiveKind,
    topic: &str,
    partition: i32,
    index: &HistoryIndex,
    after_election: u64,
    before_restore: u64,
    violations: &mut Vec<testlab_schema::Violation>,
) {
    let commands = index
        .commands
        .iter()
        .filter_map(|(sequence, _, command)| {
            command_matches(command, receive_id, kind).then_some(*sequence)
        })
        .collect::<Vec<_>>();
    let completions = receive_completions(index, receive_id, kind, topic, partition);
    let recovered = commands.first().zip(completions.first()).is_some_and(
        |(command, (completion, progressed))| {
            commands.len() == 1
                && completions.len() == 1
                && *progressed
                && after_election < *command
                && *command < *completion
                && *completion < before_restore
        },
    );
    if recovered {
        return;
    }
    let mut references = vec![
        format!("history:{after_election}"),
        format!("history:{before_restore}"),
    ];
    references.extend(
        commands
            .iter()
            .map(|sequence| format!("history:{sequence}")),
    );
    references.extend(
        completions
            .iter()
            .map(|(sequence, _)| format!("history:{sequence}")),
    );
    violations.push(violation(
        "FAULT-004",
        format!(
            "replacement leader for {topic}:{partition} expected exact nonempty public {} progress for receive {receive_id}",
            receive_surface(kind)
        ),
        Some(receive_id.clone()),
        references,
    ));
}

fn command_matches(command: &AdapterCommand, receive_id: &OperationId, kind: ReceiveKind) -> bool {
    match (kind, command) {
        (
            ReceiveKind::Assigned,
            AdapterCommand::Receive {
                receive_id: actual, ..
            },
        )
        | (
            ReceiveKind::Group,
            AdapterCommand::GroupReceive {
                receive_id: actual, ..
            },
        )
        | (
            ReceiveKind::Share,
            AdapterCommand::ShareReceive {
                receive_id: actual, ..
            },
        ) => actual == receive_id,
        _ => false,
    }
}

fn receive_completions(
    index: &HistoryIndex,
    receive_id: &OperationId,
    kind: ReceiveKind,
    topic: &str,
    partition: i32,
) -> Vec<(u64, bool)> {
    match kind {
        ReceiveKind::Assigned | ReceiveKind::Group => index
            .receives
            .get(receive_id)
            .into_iter()
            .flatten()
            .map(|receive| {
                let correct_kind = match kind {
                    ReceiveKind::Assigned => receive.committed.is_none(),
                    ReceiveKind::Group => receive.committed == Some(true),
                    ReceiveKind::Share => false,
                };
                let target = receive
                    .records
                    .iter()
                    .any(|record| record.topic == topic && record.partition == partition);
                (receive.history_sequence, correct_kind && target)
            })
            .collect(),
        ReceiveKind::Share => index
            .share_receives
            .get(receive_id)
            .into_iter()
            .flatten()
            .map(|receive| {
                let target = receive.records.iter().any(|record| {
                    record.record.topic == topic && record.record.partition == partition
                });
                (receive.history_sequence, target)
            })
            .collect(),
    }
}

fn targets_partition<'a>(
    scenario: &'a Scenario,
    operations: impl IntoIterator<Item = &'a OperationId>,
    topic: &str,
    partition: i32,
) -> bool {
    operations.into_iter().any(|operation| {
        crate::consumer::sent_record(scenario, operation)
            .is_some_and(|record| record.topic == topic && record.partition == partition)
    })
}

fn receive_surface(kind: ReceiveKind) -> &'static str {
    match kind {
        ReceiveKind::Assigned => "assigned-consumer",
        ReceiveKind::Group => "group-consumer",
        ReceiveKind::Share => "Share-consumer",
    }
}
