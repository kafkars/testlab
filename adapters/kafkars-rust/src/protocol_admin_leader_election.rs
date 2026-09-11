//! Leader-election Admin calls preserve explicit policy and deterministic outcomes.

use std::io::Write;
use std::time::Duration;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdminLeaderElection,
    AdminLeaderElectionOutcome, AdminLeaderElectionType, CommandId, ElectLeadersCommand,
};

use crate::AdapterError;
use crate::kafkars_api::{LeaderElectionTarget, LeaderElectionType, TopicPartition};
use crate::protocol::emit;
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    let AdapterCommand::ElectLeaders(command) = command else {
        return Err(invalid("non-leader-election command reached dispatcher"));
    };
    elect(state, writer, command_id, command)
}

fn elect<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ElectLeadersCommand,
) -> Result<(), AdapterError> {
    let election_type = match command.election_type {
        AdminLeaderElectionType::Preferred => LeaderElectionType::Preferred,
        AdminLeaderElectionType::Unclean => LeaderElectionType::Unclean,
    };
    let client = state.client(&command.client_id)?;
    let result = match &command.targets {
        Some(targets) => client
            .admin()
            .elect_leaders(
                election_type,
                targets.iter().map(|target| {
                    LeaderElectionTarget::new(target.topic.clone(), target.partition)
                }),
            )
            .deadline_after(Duration::from_millis(command.timeout_ms))
            .submit()
            .wait(),
        None => client
            .admin()
            .elect_all_leaders(election_type)
            .deadline_after(Duration::from_millis(command.timeout_ms))
            .submit()
            .wait(),
    }
    .map_err(AdapterError::Client)?;
    let throttle_time_ms = throttle(result.throttle_time())?;
    let entries = result.into_partitions().into_entries();
    validate_order(command.targets.as_deref(), &entries)?;
    let outcomes = entries
        .into_iter()
        .map(|(target, result)| AdminLeaderElectionOutcome {
            topic: target.topic().to_owned(),
            partition: target.partition(),
            error_code: result
                .err()
                .map(|error| crate::normalize::error_code(&error)),
        })
        .collect();
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::LeadersElected(AdminLeaderElection {
                operation_id: command.operation_id,
                election_type: command.election_type,
                throttle_time_ms,
                outcomes,
            }),
        ),
    )
}

fn validate_order<T>(
    selected: Option<&[testlab_schema::LeaderElectionSelection]>,
    entries: &[(TopicPartition, T)],
) -> Result<(), AdapterError> {
    let valid = entries
        .iter()
        .all(|(target, _)| !target.topic().is_empty() && target.partition() >= 0);
    let ordered = if let Some(selected) = selected {
        entries.len() == selected.len()
            && entries.iter().zip(selected).all(|((actual, _), expected)| {
                actual.topic() == expected.topic && actual.partition() == expected.partition
            })
    } else {
        entries.windows(2).all(|pair| {
            pair[0].0.topic().as_bytes() < pair[1].0.topic().as_bytes()
                || (pair[0].0.topic() == pair[1].0.topic()
                    && pair[0].0.partition() < pair[1].0.partition())
        })
    };
    if !valid || !ordered {
        return Err(invalid("returned malformed or nondeterministic outcomes"));
    }
    Ok(())
}

fn throttle(value: Duration) -> Result<u64, AdapterError> {
    u64::try_from(value.as_millis()).map_err(|_| invalid("returned unrepresentable throttle time"))
}

fn invalid(detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("leader-election Admin {detail}"))
}
