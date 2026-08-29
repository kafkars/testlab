//! Plural group-admin protocol tests enforce exact operation and event-family correlation.

use testlab_schema::{
    AdapterEvent, AdminClassicGroupsDescription, AdminConsumerGroupOffsetsListing,
    AdminConsumerGroupOffsetsMutation, AdminConsumerGroupsOffsetsListing, OperationId,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn each_completion_accepts_its_exact_operation_identity() {
    let operation_id = id("group-admin");
    let expected = expected_events(&operation_id);
    let events = adapter_events(&operation_id);

    for (expected, event) in expected.iter().zip(&events) {
        assert_eq!(
            expected
                .classify(event)
                .unwrap_or_else(|error| panic!("classification: {error}")),
            EventDisposition::Complete,
            "expected {expected:?} to accept {event:?}"
        );
    }
}

#[test]
fn each_completion_rejects_a_different_operation_identity() {
    let expected = expected_events(&id("expected-operation"));
    let events = adapter_events(&id("actual-operation"));

    for (expected, event) in expected.iter().zip(&events) {
        let error = match expected.classify(event) {
            Ok(disposition) => panic!("mismatched identity completed as {disposition:?}"),
            Err(error) => error,
        };
        assert_eq!(error.harness_error().code, "event_identity_mismatch");
    }
}

#[test]
fn completion_families_cannot_satisfy_each_other() {
    let operation_id = id("shared-operation");
    let expected = expected_events(&operation_id);
    let events = adapter_events(&operation_id);

    for (expected_index, expected) in expected.iter().enumerate() {
        for (event_index, event) in events.iter().enumerate() {
            if expected_index == event_index {
                continue;
            }
            let error = match expected.classify(event) {
                Ok(disposition) => panic!("cross-family event completed as {disposition:?}"),
                Err(error) => error,
            };
            assert_eq!(
                error.harness_error().code,
                "event_identity_mismatch",
                "expected {expected:?} not to accept {event:?}"
            );
        }
    }
}

fn expected_events(operation_id: &OperationId) -> Vec<ExpectedEvent> {
    vec![
        ExpectedEvent::ConsumerGroupOffsetsListed {
            operation_id: operation_id.clone(),
        },
        ExpectedEvent::ConsumerGroupsOffsetsListed {
            operation_id: operation_id.clone(),
        },
        ExpectedEvent::ConsumerGroupOffsetsAltered {
            operation_id: operation_id.clone(),
        },
        ExpectedEvent::ConsumerGroupOffsetsDeleted {
            operation_id: operation_id.clone(),
        },
        ExpectedEvent::ClassicGroupsDescribed {
            operation_id: operation_id.clone(),
        },
    ]
}

fn adapter_events(operation_id: &OperationId) -> Vec<AdapterEvent> {
    vec![
        AdapterEvent::ConsumerGroupOffsetsListed(AdminConsumerGroupOffsetsListing {
            operation_id: operation_id.clone(),
            group_id: "group-a".to_owned(),
            outcomes: Vec::new(),
        }),
        AdapterEvent::ConsumerGroupsOffsetsListed(AdminConsumerGroupsOffsetsListing {
            operation_id: operation_id.clone(),
            groups: Vec::new(),
        }),
        AdapterEvent::ConsumerGroupOffsetsAltered(AdminConsumerGroupOffsetsMutation {
            operation_id: operation_id.clone(),
            group_id: "group-a".to_owned(),
            outcomes: Vec::new(),
        }),
        AdapterEvent::ConsumerGroupOffsetsDeleted(AdminConsumerGroupOffsetsMutation {
            operation_id: operation_id.clone(),
            group_id: "group-a".to_owned(),
            outcomes: Vec::new(),
        }),
        AdapterEvent::ClassicGroupsDescribed(AdminClassicGroupsDescription {
            operation_id: operation_id.clone(),
            outcomes: Vec::new(),
        }),
    ]
}

fn id(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
