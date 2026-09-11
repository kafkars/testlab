//! Client-quota protocol tests pin expectation separation and exact event identity.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminClientQuotaAlteration, AdminClientQuotaDescription,
    AlterClientQuotaAction, BrokerQuotaDirection, ClientId, DescribeClientQuotaAction, OperationId,
    ScenarioAction,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn describe_translation_omits_the_verifier_owned_expected_rate() {
    let action = ScenarioAction::DescribeClientQuota(describe());
    let Some((AdapterCommand::DescribeClientQuota(command), expected)) =
        crate::session_command_admin_client_quota::translate(&action)
    else {
        panic!("client-quota description translation");
    };

    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation("quota-describe"));
    assert_eq!(command.user, "testlab-user");
    assert_eq!(command.direction, BrokerQuotaDirection::Producer);
    assert_eq!(command.timeout_ms, 1_000);
    assert!(matches!(expected, ExpectedEvent::ClientQuotaDescribed(_)));
}

#[test]
fn alteration_translation_preserves_exact_public_intent() {
    let action = ScenarioAction::AlterClientQuota(alter());
    let Some((command, expected)) = crate::session_command_admin_client_quota::translate(&action)
    else {
        panic!("client-quota alteration translation");
    };
    assert_eq!(command, AdapterCommand::AlterClientQuota(alter()));
    assert!(matches!(expected, ExpectedEvent::ClientQuotaAltered(_)));
}

#[test]
fn client_quota_completions_reject_foreign_identity_and_family() {
    let expected = [
        ExpectedEvent::ClientQuotaAltered(operation("quota-operation")),
        ExpectedEvent::ClientQuotaDescribed(operation("quota-operation")),
    ];
    let events = events("quota-operation");
    for (expected_index, expected) in expected.iter().enumerate() {
        for (event_index, event) in events.iter().enumerate() {
            let result = expected.classify(event);
            if expected_index == event_index {
                assert_eq!(
                    result.unwrap_or_else(|error| panic!("client quota classification: {error}")),
                    EventDisposition::Complete
                );
            } else {
                let error = match result {
                    Ok(disposition) => {
                        panic!("cross-family quota event completed as {disposition:?}")
                    }
                    Err(error) => error,
                };
                assert_eq!(error.harness_error().code, "event_identity_mismatch");
            }
        }
    }
    for (expected, event) in expected.iter().zip(events("other-operation")) {
        let error = match expected.classify(&event) {
            Ok(disposition) => panic!("foreign quota identity completed as {disposition:?}"),
            Err(error) => error,
        };
        assert_eq!(error.harness_error().code, "event_identity_mismatch");
    }
}

fn events(operation_id: &str) -> [AdapterEvent; 2] {
    [
        AdapterEvent::ClientQuotaAltered(AdminClientQuotaAlteration {
            operation_id: operation(operation_id),
            user: "testlab-user".to_owned(),
            direction: BrokerQuotaDirection::Producer,
        }),
        AdapterEvent::ClientQuotaDescribed(AdminClientQuotaDescription {
            operation_id: operation(operation_id),
            user: "testlab-user".to_owned(),
            direction: BrokerQuotaDirection::Producer,
            bytes_per_second: 65_536,
        }),
    ]
}

fn alter() -> AlterClientQuotaAction {
    AlterClientQuotaAction {
        client_id: client(),
        operation_id: operation("quota-alter"),
        user: "testlab-user".to_owned(),
        direction: BrokerQuotaDirection::Producer,
        bytes_per_second: Some(65_536),
        timeout_ms: 1_000,
    }
}

fn describe() -> DescribeClientQuotaAction {
    DescribeClientQuotaAction {
        client_id: client(),
        operation_id: operation("quota-describe"),
        user: "testlab-user".to_owned(),
        direction: BrokerQuotaDirection::Producer,
        expected_bytes_per_second: 65_536,
        timeout_ms: 1_000,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
