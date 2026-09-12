//! Transaction termination owns bounded commit, abort, and remaining-time retries.

use std::thread;
use std::time::{Duration, Instant};

use testlab_schema::TransactionDisposition;

use crate::AdapterError;
use crate::kafkars_api::{ErrorKind, KafkaError, RetryAdvice, Transaction};

pub(crate) fn end(
    transaction: Transaction<'_>,
    disposition: TransactionDisposition,
    deadline: Instant,
) -> Result<(), AdapterError> {
    match disposition {
        TransactionDisposition::Commit => commit(transaction, deadline),
        TransactionDisposition::Abort => abort(transaction, deadline),
        TransactionDisposition::AdminPartitionAbort => Err(AdapterError::TransactionResult(
            "admin partition abort reached ordinary transaction end".to_owned(),
        )),
    }
}

fn commit(mut transaction: Transaction<'_>, deadline: Instant) -> Result<(), AdapterError> {
    loop {
        match transaction.commit(remaining(deadline)?) {
            Ok(observer) => return observer.wait().map_err(AdapterError::Client),
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                transaction = returned;
                if error.retry_advice() != RetryAdvice::RetrySafe || Instant::now() >= deadline {
                    return Err(AdapterError::Client(error));
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

fn abort(mut transaction: Transaction<'_>, deadline: Instant) -> Result<(), AdapterError> {
    loop {
        match transaction.abort(remaining(deadline)?) {
            Ok(observer) => return observer.wait().map_err(AdapterError::Client),
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                transaction = returned;
                if error.retry_advice() != RetryAdvice::RetrySafe || Instant::now() >= deadline {
                    return Err(AdapterError::Client(error));
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

pub(crate) fn remaining(deadline: Instant) -> Result<Duration, AdapterError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(timeout_error("transaction command deadline elapsed"))
    } else {
        Ok(remaining)
    }
}

pub(crate) fn timeout_error(message: &str) -> AdapterError {
    AdapterError::Client(KafkaError::new(ErrorKind::Timeout, message))
}
