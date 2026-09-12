//! Selected ordinary sends resolve one fresh nonzero topic UUID before admission.

use std::time::{Duration, Instant};

use testlab_schema::{OperationId, ProducerId};

use crate::AdapterError;
use crate::kafkars_api::TopicUuid;
use crate::state::AdapterState;

const TOPIC_ID_TIMEOUT: Duration = Duration::from_secs(20);

pub(crate) fn resolve(
    state: &AdapterState,
    producer_id: &ProducerId,
    operation_id: &OperationId,
    topic: &str,
) -> Result<TopicUuid, AdapterError> {
    let client = state.producer_client(producer_id)?;
    let requested = vec![topic.to_owned()];
    let resolved = crate::protocol_admin_topic_ids::resolve(
        &client,
        &requested,
        operation_id,
        Instant::now() + TOPIC_ID_TIMEOUT,
    )?;
    let [(resolved_topic, bytes)] = resolved.as_slice() else {
        return Err(invalid(
            operation_id,
            "did not resolve exactly one topic ID",
        ));
    };
    if resolved_topic != topic {
        return Err(invalid(operation_id, "resolved a mismatched topic name"));
    }
    TopicUuid::try_from_bytes(*bytes)
        .ok_or_else(|| invalid(operation_id, "resolved the zero topic-ID sentinel"))
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("producer operation {operation_id} {detail}"))
}
