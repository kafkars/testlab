//! Unique share consumers retain exact batches until acknowledgement, drop, or close.

use std::collections::BTreeMap;
use std::thread;
use std::time::{Duration, Instant};

use crate::kafkars_api::{
    Client, RetryAdvice, ShareConsumer, ShareConsumerBatch, ShareConsumerFetchConfig,
};
use testlab_schema::{
    ClientId, ConsumerId, OperationId, ShareConsumerFetchConfiguration, ShareDisposition,
};

use crate::share_consumers_acknowledge;
use crate::share_consumers_close;
use crate::share_consumers_receive;
use crate::state::StateError;

const POLL_SLICE: Duration = Duration::from_millis(10);
const MAX_SHARE_BATCH_RECORDS: u32 = 31;

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

pub(crate) struct ShareConsumerRegistration {
    pub(crate) client_id: ClientId,
    pub(crate) consumer_id: ConsumerId,
    pub(crate) group_id: String,
    pub(crate) topic: String,
    pub(crate) membership_timeout: Duration,
    pub(crate) close_timeout: Duration,
    pub(crate) configuration: Option<ShareConsumerFetchConfiguration>,
}

pub(crate) use share_consumers_acknowledge::ShareAcknowledgeOutcome;
pub(crate) use share_consumers_close::ShareCloseOutcome;
pub(crate) use share_consumers_receive::ShareReceiveFacts;

impl ShareConsumers {
    pub(crate) fn create(
        &mut self,
        client: &Client,
        registration: ShareConsumerRegistration,
    ) -> Result<(), StateError> {
        if self.owners.contains_key(&registration.consumer_id) {
            return Err(StateError::DuplicateConsumer(registration.consumer_id));
        }
        let started = Instant::now();
        let deadline = started
            .checked_add(registration.membership_timeout)
            .unwrap_or(started);
        let fetch = public_fetch_configuration(registration.configuration)?;
        let mut builder = client
            .share_consumer(registration.group_id)
            .subscribe([registration.topic.as_str()])
            .fetch_config(fetch)
            .close_timeout(registration.close_timeout);
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
        share_consumers_receive::await_assignment(&consumer, &registration.topic, deadline)?;
        self.owners.insert(
            registration.consumer_id,
            ShareOwner {
                client_id: registration.client_id,
                consumer,
                close_timeout: registration.close_timeout,
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
        dispositions: Vec<ShareDisposition>,
        timeout: Duration,
    ) -> Result<ShareAcknowledgeOutcome, StateError> {
        let batch = self.take_batch(consumer_id, receive_id)?;
        let consumer = &mut self
            .owners
            .get_mut(consumer_id)
            .ok_or_else(|| StateError::MissingConsumer(consumer_id.clone()))?
            .consumer;
        share_consumers_acknowledge::acknowledge(consumer, batch, dispositions, timeout)
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

pub(crate) fn public_fetch_configuration(
    configuration: Option<ShareConsumerFetchConfiguration>,
) -> Result<ShareConsumerFetchConfig, StateError> {
    let configuration = configuration.unwrap_or(ShareConsumerFetchConfiguration {
        max_records: MAX_SHARE_BATCH_RECORDS,
        batch_size: MAX_SHARE_BATCH_RECORDS,
    });
    let max_records = usize::try_from(configuration.max_records)
        .map_err(|error| StateError::ShareSurface(error.to_string()))?;
    let batch_size = usize::try_from(configuration.batch_size)
        .map_err(|error| StateError::ShareSurface(error.to_string()))?;
    Ok(ShareConsumerFetchConfig::default()
        .with_max_records(max_records)
        .with_batch_size(batch_size))
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
