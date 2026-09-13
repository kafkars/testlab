//! Kafka Streams CLI output is normalized into one exact final-absence fact.

use testlab_schema::{BrokerStreamsGroupsState, OperationId};

use crate::observer_error::ObserverError;

pub(super) fn normalize(
    observation: u64,
    operation_id: &OperationId,
    group_ids: &[String],
    output: &[u8],
) -> Result<BrokerStreamsGroupsState, ObserverError> {
    let text = std::str::from_utf8(output).map_err(|_| invalid("output was not UTF-8"))?;
    let lines = text.lines().collect::<Vec<_>>();
    if lines.len() != group_ids.len() {
        return Err(invalid(
            "projection did not contain exactly one line per group",
        ));
    }
    let mut all_absent = true;
    for (line, group_id) in lines.into_iter().zip(group_ids) {
        let absent = format!("streams-group-absent:{group_id}");
        let present = format!("streams-group-present:{group_id}");
        if line == present {
            all_absent = false;
        } else if line != absent {
            return Err(invalid("projection contained an unexpected group identity"));
        }
    }
    Ok(BrokerStreamsGroupsState {
        observation,
        operation_id: operation_id.clone(),
        group_ids: group_ids.to_vec(),
        all_absent,
    })
}

fn invalid(detail: &str) -> ObserverError {
    ObserverError::InvalidBrokerState(format!("Kafka Streams CLI snapshot: {detail}"))
}

#[cfg(test)]
#[path = "streams_group_cli_observation_test.rs"]
mod tests;
