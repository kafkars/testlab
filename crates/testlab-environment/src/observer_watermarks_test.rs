//! Watermark capture retries leadership transitions without extending its deadline.

use std::time::{Duration, Instant};

use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;

use crate::observer_error::ObserverError;
use crate::observer_watermarks::capture;

#[test]
fn watermark_capture_survives_leadership_recovery() {
    let mut responses = [
        Err(KafkaError::MetadataFetch(
            RDKafkaErrorCode::NotLeaderForPartition,
        )),
        Err(KafkaError::MetadataFetch(
            RDKafkaErrorCode::LeaderNotAvailable,
        )),
        Ok((7, 12)),
    ]
    .into_iter();
    let mut budgets = Vec::new();
    let result = capture(Instant::now() + Duration::from_secs(2), |budget| {
        budgets.push(budget);
        responses
            .next()
            .unwrap_or_else(|| panic!("unexpected query"))
    });
    assert_eq!(
        result.unwrap_or_else(|error| panic!("capture: {error}")),
        (7, 12)
    );
    assert_eq!(budgets.len(), 3);
    assert!(budgets.windows(2).all(|pair| pair[1] < pair[0]));
}

#[test]
fn permanent_watermark_errors_are_preserved_without_retry() {
    for code in [
        RDKafkaErrorCode::TopicAuthorizationFailed,
        RDKafkaErrorCode::UnknownTopicOrPartition,
    ] {
        let mut queries = 0;
        let result = capture(Instant::now() + Duration::from_secs(1), |_| {
            queries += 1;
            Err(KafkaError::MetadataFetch(code))
        });
        assert!(
            matches!(result, Err(ObserverError::Kafka(KafkaError::MetadataFetch(actual))) if actual == code)
        );
        assert_eq!(queries, 1);
    }
}

#[test]
fn leadership_retry_cannot_outlive_the_original_deadline() {
    let mut queries = 0;
    let result = capture(Instant::now() + Duration::from_millis(10), |_| {
        queries += 1;
        Err(KafkaError::MetadataFetch(
            RDKafkaErrorCode::NotLeaderForPartition,
        ))
    });
    assert!(matches!(result, Err(ObserverError::Deadline)));
    assert!(queries <= 1);
}

#[test]
fn an_expired_capture_does_not_issue_a_query() {
    let result = capture(Instant::now(), |_| panic!("query after deadline"));
    assert!(matches!(result, Err(ObserverError::Deadline)));
}
