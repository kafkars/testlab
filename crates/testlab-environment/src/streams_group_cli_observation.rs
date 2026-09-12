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
mod tests {
    use testlab_schema::OperationId;

    #[test]
    fn exact_caller_order_is_retained() {
        let operation = OperationId::new("streams-lifecycle")
            .unwrap_or_else(|error| panic!("operation: {error}"));
        let groups = vec!["secondary".to_owned(), "primary".to_owned()];
        let state = super::normalize(
            3,
            &operation,
            &groups,
            b"streams-group-absent:secondary\nstreams-group-absent:primary\n",
        )
        .unwrap_or_else(|error| panic!("normalize: {error}"));
        assert!(state.all_absent);
        assert_eq!(state.group_ids, groups);
    }
}
