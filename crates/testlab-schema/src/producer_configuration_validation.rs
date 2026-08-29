//! Producer configuration validation constrains portable values before adapter execution.

use crate::{ProducerConfiguration, ProducerLimitsConfiguration};

const MAX_BYTES: u64 = 64 * 1024 * 1024;
const MAX_RECORDS: u32 = 4_096;

pub(crate) fn validate(configuration: &ProducerConfiguration, problems: &mut Vec<String>) {
    if !(100..=60_000).contains(&configuration.delivery_timeout_ms) {
        problems.push("configured producer delivery_timeout_ms must be 100..=60000".to_owned());
    }
    if configuration.max_retries > 100 {
        problems.push("configured producer max_retries must be at most 100".to_owned());
    }
    if configuration.retry_backoff_ms > 60_000 {
        problems.push("configured producer retry_backoff_ms must be at most 60000".to_owned());
    }
    if configuration.max_retries == 0 && configuration.retry_backoff_ms != 0 {
        problems.push("disabled configured producer retry must use zero backoff".to_owned());
    }
    validate_limits(&configuration.limits, problems);
}

fn validate_limits(limits: &ProducerLimitsConfiguration, problems: &mut Vec<String>) {
    for (name, value) in [
        ("retained_bytes", limits.retained_bytes),
        ("waiting_bytes", limits.waiting_bytes),
        ("batch_bytes", limits.batch_bytes),
        ("request_bytes", limits.request_bytes),
    ] {
        if !(1..=MAX_BYTES).contains(&value) {
            problems.push(format!(
                "configured producer {name} must be 1..={MAX_BYTES}"
            ));
        }
    }
    for (name, value) in [
        ("in_flight_records", limits.in_flight_records),
        ("waiting_records", limits.waiting_records),
        ("batch_records", limits.batch_records),
    ] {
        if !(1..=MAX_RECORDS).contains(&value) {
            problems.push(format!(
                "configured producer {name} must be 1..={MAX_RECORDS}"
            ));
        }
    }
    if limits.batch_bytes > limits.request_bytes || limits.request_bytes > limits.retained_bytes {
        problems.push(
            "configured producer requires batch_bytes <= request_bytes <= retained_bytes"
                .to_owned(),
        );
    }
    if limits.batch_records > limits.in_flight_records {
        problems.push("configured producer batch_records exceeds in_flight_records".to_owned());
    }
    if !(1..=5).contains(&limits.max_in_flight_requests_per_broker) {
        problems.push("configured producer max in-flight requests must be 1..=5".to_owned());
    }
    if limits.linger_ms > 1_000 {
        problems.push("configured producer linger_ms must be at most 1000".to_owned());
    }
}
