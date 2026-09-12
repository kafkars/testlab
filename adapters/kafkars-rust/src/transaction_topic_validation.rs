//! UUID-bound transactions resolve public topic identities and seal a fresh topic view.

use std::collections::BTreeSet;
use std::thread;
use std::time::{Duration, Instant};

use testlab_schema::{BatchRecord, OperationId, ProducerId};

use crate::AdapterError;
use crate::kafkars_api::{RetryAdvice, TopicUuid, Transaction};
use crate::state::AdapterState;

#[derive(Debug, Default)]
pub(crate) struct TopicValidation {
    bindings: Vec<(String, TopicUuid)>,
}

impl TopicValidation {
    pub(crate) fn resolve(
        state: &AdapterState,
        producer_id: &ProducerId,
        transaction_id: &OperationId,
        operations: &[BatchRecord],
        enabled: bool,
        deadline: Instant,
    ) -> Result<Self, AdapterError> {
        if !enabled {
            return Ok(Self::default());
        }
        let topics = ordered_topics(operations);
        if topics.is_empty() {
            return Err(invalid(
                "UUID validation requires at least one record topic",
            ));
        }
        let client = state.transactional_producer_client(producer_id)?;
        let resolved =
            crate::protocol_admin_topic_ids::resolve(&client, &topics, transaction_id, deadline)?;
        let bindings = resolved
            .into_iter()
            .map(|(topic, id)| {
                TopicUuid::try_from_bytes(id)
                    .map(|uuid| (topic, uuid))
                    .ok_or_else(|| invalid("public topic resolution returned the zero UUID"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { bindings })
    }

    pub(crate) fn topic_uuid(&self, topic: &str) -> Result<Option<TopicUuid>, AdapterError> {
        if self.bindings.is_empty() {
            return Ok(None);
        }
        self.bindings
            .iter()
            .find_map(|(name, uuid)| (name == topic).then_some(*uuid))
            .map(Some)
            .ok_or_else(|| invalid("record topic was absent from UUID validation bindings"))
    }

    pub(crate) fn topic_ids(&self) -> Vec<[u8; 16]> {
        self.bindings
            .iter()
            .map(|(_, uuid)| (*uuid).into_bytes())
            .collect()
    }

    pub(crate) fn seal(
        &self,
        transaction: &mut Transaction<'_>,
        deadline: Instant,
    ) -> Result<(), AdapterError> {
        if self.bindings.is_empty() {
            return Ok(());
        }
        loop {
            let result = transaction
                .validate_for_commit(crate::transaction_end::remaining(deadline)?)
                .and_then(|validation| validation.wait());
            match result {
                Ok(()) => return Ok(()),
                Err(error)
                    if error.retry_advice() == RetryAdvice::RetrySafe
                        && Instant::now() < deadline =>
                {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(error) => return Err(AdapterError::Client(error)),
            }
        }
    }
}

fn ordered_topics(operations: &[BatchRecord]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    operations
        .iter()
        .filter_map(|operation| {
            let topic = operation.record.topic.clone();
            seen.insert(topic.clone()).then_some(topic)
        })
        .collect()
}

fn invalid(detail: &str) -> AdapterError {
    AdapterError::TransactionResult(detail.to_owned())
}
