//! Share acknowledgement owns one retained batch through a public terminal.

use std::thread;
use std::time::{Duration, Instant};

use crate::kafkars_api::{
    KafkaError, RetryAdvice, ShareConsumer, ShareConsumerBatch,
    ShareDisposition as PublicDisposition,
};
use testlab_schema::{ShareAcknowledgementMethod, ShareDisposition};

use crate::state::StateError;

const POLL_SLICE: Duration = Duration::from_millis(10);

pub(crate) struct ShareAcknowledgeOutcome {
    pub(crate) error: Option<KafkaError>,
    pub(crate) broker_codes: Vec<i16>,
}

pub(crate) fn acknowledge(
    consumer: &mut ShareConsumer,
    batch: ShareConsumerBatch,
    method: ShareAcknowledgementMethod,
    dispositions: Vec<ShareDisposition>,
    timeout: Duration,
) -> Result<ShareAcknowledgeOutcome, StateError> {
    if batch.len() != dispositions.len() {
        return Err(StateError::ShareSurface(format!(
            "share batch has {} records but {} dispositions were supplied",
            batch.len(),
            dispositions.len()
        )));
    }
    if method == ShareAcknowledgementMethod::AcceptAll
        && dispositions
            .iter()
            .any(|disposition| *disposition != ShareDisposition::Accept)
    {
        return Err(StateError::ShareSurface(
            "accept_all requires only Accept dispositions".to_owned(),
        ));
    }
    let mut acknowledgement = match method {
        ShareAcknowledgementMethod::IntoAcknowledgement => {
            let decisions = batch
                .records()
                .zip(dispositions)
                .map(|(record, disposition)| {
                    let public = match disposition {
                        ShareDisposition::Accept => PublicDisposition::Accept,
                        ShareDisposition::Release => PublicDisposition::Release,
                        ShareDisposition::Reject => PublicDisposition::Reject,
                    };
                    record.decision(public)
                })
                .collect();
            batch.into_acknowledgement(decisions)
        }
        ShareAcknowledgementMethod::AcceptAll => batch.accept_all(),
    }
    .map_err(|error| StateError::ShareSurface(error.to_string()))?;
    let started = Instant::now();
    let deadline = started.checked_add(timeout).unwrap_or(started);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let operation = match consumer.try_acknowledge(acknowledgement, remaining) {
            Ok(operation) => operation,
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                if error.retry_advice() != RetryAdvice::RetrySafe || Instant::now() >= deadline {
                    return Ok(ShareAcknowledgeOutcome {
                        error: Some(error),
                        broker_codes: Vec::new(),
                    });
                }
                acknowledgement = returned;
                thread::sleep(POLL_SLICE.min(remaining));
                continue;
            }
        };
        match operation.wait() {
            Ok(response) => {
                return Ok(ShareAcknowledgeOutcome {
                    error: None,
                    broker_codes: response
                        .partitions()
                        .filter_map(|partition| partition.broker_code())
                        .collect(),
                });
            }
            Err(error) => {
                let (returned, semantic, _) = error.into_parts();
                if semantic.retry_advice() == RetryAdvice::RetrySafe
                    && returned.is_some()
                    && Instant::now() < deadline
                {
                    acknowledgement = returned.unwrap_or_else(|| {
                        unreachable!("safe retry retained acknowledgement ownership")
                    });
                    continue;
                }
                return Ok(ShareAcknowledgeOutcome {
                    error: Some(semantic),
                    broker_codes: Vec::new(),
                });
            }
        }
    }
}
