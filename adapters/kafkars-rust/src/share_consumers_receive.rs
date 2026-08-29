//! Share receive polling retains exact batches and public membership fences.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

use crate::kafkars_api::{RetryAdvice, ShareConsumer, ShareConsumerBatch, ShareConsumerRecord};
use testlab_schema::{ByteString, ConsumedRecord, HeaderSpec, ShareConsumedRecord};

use crate::state::StateError;

const POLL_SLICE: Duration = Duration::from_millis(10);

pub(crate) struct ShareReceiveFacts {
    pub(crate) records: Vec<ShareConsumedRecord>,
    pub(crate) acquisition_count: usize,
    pub(crate) member_epoch: Option<i32>,
    pub(crate) assignment_epoch: Option<u64>,
}

pub(crate) fn await_assignment(
    consumer: &ShareConsumer,
    topic: &str,
    deadline: Instant,
) -> Result<(), StateError> {
    loop {
        if let Some(error) = consumer.startup_error() {
            return Err(StateError::Client(error));
        }
        match consumer.assignment() {
            Ok(Some(assignment))
                if !assignment.partitions().is_empty()
                    && assignment
                        .partitions()
                        .iter()
                        .all(|partition| partition.topic() == topic) =>
            {
                return Ok(());
            }
            Ok(Some(assignment)) if !assignment.partitions().is_empty() => {
                return Err(StateError::ShareSurface(format!(
                    "share member received an assignment outside {topic}: {assignment:?}"
                )));
            }
            Ok(Some(_)) | Ok(None) => {}
            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => {}
            Err(error) => return Err(StateError::Client(error)),
        }
        if Instant::now() >= deadline {
            return Err(StateError::ShareSurface(format!(
                "share assignment for {topic} did not materialize before the membership deadline"
            )));
        }
        thread::sleep(POLL_SLICE.min(deadline.saturating_duration_since(Instant::now())));
    }
}

pub(crate) fn receive(
    consumer: &mut ShareConsumer,
    timeout: Duration,
) -> Result<(ShareReceiveFacts, Option<ShareConsumerBatch>), StateError> {
    let started = Instant::now();
    let deadline = started.checked_add(timeout).unwrap_or(started);
    let batch = poll_receive(consumer, deadline)?;
    let Some(batch) = batch else {
        return Ok((
            ShareReceiveFacts {
                records: Vec::new(),
                acquisition_count: 0,
                member_epoch: None,
                assignment_epoch: None,
            },
            None,
        ));
    };
    let acquisition_count = batch.acquisition_count();
    let records = batch
        .records()
        .map(|record| normalize_record(&record))
        .collect::<Result<Vec<_>, _>>()?;
    let (member_epoch, assignment_epoch) = assignment(consumer, deadline)?;
    Ok((
        ShareReceiveFacts {
            records,
            acquisition_count,
            member_epoch,
            assignment_epoch,
        },
        Some(batch),
    ))
}

fn poll_receive(
    consumer: &mut ShareConsumer,
    deadline: Instant,
) -> Result<Option<ShareConsumerBatch>, StateError> {
    if let Some(error) = consumer.startup_error() {
        return Err(StateError::Client(error));
    }
    let mut receive = pin!(consumer.recv());
    let mut context = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(result) = receive.as_mut().poll(&mut context) {
            return result.map_err(StateError::Client);
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        thread::sleep(POLL_SLICE);
    }
}

fn assignment(
    consumer: &ShareConsumer,
    deadline: Instant,
) -> Result<(Option<i32>, Option<u64>), StateError> {
    loop {
        match consumer.assignment() {
            Ok(Some(assignment)) => {
                return Ok((
                    Some(assignment.member_epoch()),
                    Some(assignment.assignment_epoch()),
                ));
            }
            Ok(None) => {}
            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => {}
            Err(error) => return Err(StateError::Client(error)),
        }
        if Instant::now() >= deadline {
            return Ok((None, None));
        }
        thread::sleep(POLL_SLICE);
    }
}

fn normalize_record(record: &ShareConsumerRecord<'_>) -> Result<ShareConsumedRecord, StateError> {
    let partition = i32::try_from(record.partition())
        .map_err(|_| StateError::ShareSurface("share partition overflow".to_owned()))?;
    let headers = record
        .headers()
        .map(|header| {
            let name = String::from_utf8(header.key().to_vec())
                .map_err(|error| StateError::ShareSurface(error.to_string()))?;
            Ok(HeaderSpec {
                name,
                value: header.value().map(ByteString::hex),
            })
        })
        .collect::<Result<Vec<_>, StateError>>()?;
    Ok(ShareConsumedRecord {
        record: ConsumedRecord {
            topic: record.topic().to_owned(),
            partition,
            offset: record.offset(),
            timestamp_millis: record.timestamp_millis(),
            key: record.key().map(ByteString::hex),
            value: record.value().map(ByteString::hex),
            headers,
        },
        delivery_count: record.delivery_count(),
    })
}
