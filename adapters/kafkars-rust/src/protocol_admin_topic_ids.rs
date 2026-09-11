//! Scenario-owned names resolve once to nonzero unique Kafka topic IDs.

use std::collections::BTreeSet;
use std::time::Instant;

use testlab_schema::OperationId;

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::Client;
use crate::protocol_admin_read::retry_safe;

pub(crate) fn resolve(
    client: &Client,
    topics: &[String],
    operation_id: &OperationId,
    deadline: Instant,
) -> Result<Vec<(String, [u8; 16])>, AdapterError> {
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .describe_topics(topics.to_vec())
                .include_authorized_operations(false)
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let entries = result.into_entries();
    if entries.len() != topics.len() {
        return Err(invalid(
            operation_id,
            "topic-ID resolution returned a different number of outcomes than requested",
        ));
    }
    let mut unique = BTreeSet::new();
    entries
        .into_iter()
        .zip(topics)
        .map(|((key, result), expected)| {
            if &key != expected {
                return Err(invalid(
                    operation_id,
                    "topic-ID resolution returned outcomes outside caller order",
                ));
            }
            let description = result.map_err(AdapterError::Client)?;
            if description.name() != expected {
                return Err(invalid(
                    operation_id,
                    "topic-ID resolution returned a mismatched inner topic name",
                ));
            }
            let id = description
                .topic_id()
                .filter(|id| *id != [0; 16])
                .ok_or_else(|| {
                    invalid(
                        operation_id,
                        "topic-ID resolution did not return a nonzero topic identity",
                    )
                })?;
            if !unique.insert(id) {
                return Err(invalid(
                    operation_id,
                    "topic-ID resolution returned a duplicate topic identity",
                ));
            }
            Ok((key, id))
        })
        .collect()
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
