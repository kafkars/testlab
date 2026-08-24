//! Unique share consumers retain exact batches until acknowledgement, drop, or close.

use std::collections::BTreeMap;
use std::thread;
use std::time::{Duration, Instant};

use kafkars::{Client, RetryAdvice, ShareConsumer, ShareConsumerBatch, ShareConsumerFetchConfig};
use testlab_schema::{ClientId, ConsumerId, OperationId, ShareDisposition};

use crate::share_consumers_acknowledge;
use crate::share_consumers_close;
use crate::share_consumers_receive;
use crate::state::StateError;

const POLL_SLICE: Duration = Duration::from_millis(10);

#[derive(Default)]
pub(crate) struct ShareConsumers {
    owners: BTreeMap<ConsumerId, ShareOwner>,
    batches: BTreeMap<OperationId, BatchOwner>,
}

struct ShareOwner {
    client_id: ClientId,
    consumer: ShareConsumer,
    close_timeout: Duration,
}

struct BatchOwner {
    consumer_id: ConsumerId,
    batch: ShareConsumerBatch,
}

pub(crate) use share_consumers_acknowledge::ShareAcknowledgeOutcome;
pub(crate) use share_consumers_close::ShareCloseOutcome;
pub(crate) use share_consumers_receive::ShareReceiveFacts;

impl ShareConsumers {
    #[expect(
        clippy::too_many_arguments,
        reason = "the adapter preserves each explicit public share-consumer bound"
    )]
    pub(crate) fn create(
        &mut self,
        client: &Client,
        client_id: ClientId,
        consumer_id: ConsumerId,
        group_id: String,
        topic: String,
        membership_timeout: Duration,
        close_timeout: Duration,
    ) -> Result<(), StateError> {
        if self.owners.contains_key(&consumer_id) {
            return Err(StateError::DuplicateConsumer(consumer_id));
        }
        let started = Instant::now();
        let deadline = started.checked_add(membership_timeout).unwrap_or(started);
        let mut builder = client
            .share_consumer(group_id)
            .subscribe([topic])
            .fetch_config(
                ShareConsumerFetchConfig::default()
                    .with_max_records(1)
                    .with_batch_size(1),
            )
            .close_timeout(close_timeout);
        let consumer = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match builder.membership_start_timeout(remaining).build() {
                Ok(consumer) => break consumer,
                Err(rejection) => {
                    let (returned, error) = rejection.into_parts();
                    if error.retry_advice() != RetryAdvice::RetrySafe || Instant::now() >= deadline
                    {
                        return Err(StateError::Client(error));
                    }
                    builder = returned;
                    thread::sleep(POLL_SLICE.min(remaining));
                }
            }
        };
        self.owners.insert(
            consumer_id,
            ShareOwner {
                client_id,
                consumer,
                close_timeout,
            },
        );
        Ok(())
    }

    pub(crate) fn receive(
        &mut self,
        consumer_id: &ConsumerId,
        receive_id: OperationId,
        timeout: Duration,
    ) -> Result<ShareReceiveFacts, StateError> {
        if self.batches.contains_key(&receive_id) {
            return Err(StateError::DuplicateShareBatch(receive_id));
        }
        let owner = self
            .owners
            .get_mut(consumer_id)
            .ok_or_else(|| StateError::MissingConsumer(consumer_id.clone()))?;
        let (facts, batch) = share_consumers_receive::receive(&mut owner.consumer, timeout)?;
        if let Some(batch) = batch {
            self.batches.insert(
                receive_id,
                BatchOwner {
                    consumer_id: consumer_id.clone(),
                    batch,
                },
            );
        }
        Ok(facts)
    }

    pub(crate) fn acknowledge(
        &mut self,
        consumer_id: &ConsumerId,
        receive_id: &OperationId,
        disposition: ShareDisposition,
        timeout: Duration,
    ) -> Result<ShareAcknowledgeOutcome, StateError> {
        let batch = self.take_batch(consumer_id, receive_id)?;
        let consumer = &mut self
            .owners
            .get_mut(consumer_id)
            .ok_or_else(|| StateError::MissingConsumer(consumer_id.clone()))?
            .consumer;
        share_consumers_acknowledge::acknowledge(consumer, batch, disposition, timeout)
    }

    pub(crate) fn drop_batch(
        &mut self,
        consumer_id: &ConsumerId,
        receive_id: &OperationId,
    ) -> Result<(), StateError> {
        drop(self.take_batch(consumer_id, receive_id)?);
        Ok(())
    }

    pub(crate) fn close(
        &mut self,
        consumer_id: &ConsumerId,
    ) -> Result<ShareCloseOutcome, StateError> {
        let owner = self
            .owners
            .remove(consumer_id)
            .ok_or_else(|| StateError::MissingConsumer(consumer_id.clone()))?;
        let ShareOwner {
            client_id,
            consumer,
            close_timeout,
        } = owner;
        let close = match share_consumers_close::admit(consumer, close_timeout) {
            Ok(close) => close,
            Err((consumer, error)) => {
                self.owners.insert(
                    consumer_id.clone(),
                    ShareOwner {
                        client_id,
                        consumer,
                        close_timeout,
                    },
                );
                return Err(StateError::Client(error));
            }
        };
        self.drop_consumer_batches(consumer_id);
        Ok(share_consumers_close::settle(close))
    }

    pub(crate) fn has_owner(&self, client_id: &ClientId) -> bool {
        self.owners
            .values()
            .any(|owner| &owner.client_id == client_id)
    }

    pub(crate) fn contains(&self, consumer_id: &ConsumerId) -> bool {
        self.owners.contains_key(consumer_id)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.owners.is_empty() && self.batches.is_empty()
    }

    fn take_batch(
        &mut self,
        consumer_id: &ConsumerId,
        receive_id: &OperationId,
    ) -> Result<ShareConsumerBatch, StateError> {
        let owner = self
            .batches
            .remove(receive_id)
            .ok_or_else(|| StateError::MissingShareBatch(receive_id.clone()))?;
        if &owner.consumer_id != consumer_id {
            self.batches.insert(receive_id.clone(), owner);
            return Err(StateError::ShareBatchOwner {
                receive_id: receive_id.clone(),
                consumer_id: consumer_id.clone(),
            });
        }
        Ok(owner.batch)
    }

    fn drop_consumer_batches(&mut self, consumer_id: &ConsumerId) {
        let owned = self
            .batches
            .iter()
            .filter(|(_, owner)| &owner.consumer_id == consumer_id)
            .map(|(receive_id, _)| receive_id.clone())
            .collect::<Vec<_>>();
        for receive_id in owned {
            drop(self.batches.remove(&receive_id));
        }
    }
}

impl std::fmt::Debug for ShareConsumers {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareConsumers")
            .field("owners", &self.owners.len())
            .field("batches", &self.batches.len())
            .finish()
    }
}
