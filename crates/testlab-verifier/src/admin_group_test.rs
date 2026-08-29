//! Consumer-group admin tests require exact public and independently observed offsets.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminConsumerGroupOffsetListing, BrokerConsumerGroupOffset,
    BrokerStateObservation, HistoryEntry, HistoryPayload, ListConsumerGroupOffsetsAction,
    ListConsumerGroupOffsetsCommand, ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command as harness_command, event, scenario, step};

const EXPECTED: Identity<'static> = Identity {
    operation_id: "admin-group-offset-1",
    group_id: "group-1",
    topic: "records",
    partition: 0,
};

#[test]
fn exact_public_and_independent_group_offset_passes() {
    let history = [
        public(1, EXPECTED, Some(1)),
        independent(2, 7, EXPECTED, Some(1)),
    ];

    assert!(admin_violations(&history).is_empty());
}

#[test]
fn missing_or_duplicate_group_offset_evidence_fails() {
    let cases = [
        vec![independent(1, 7, EXPECTED, Some(1))],
        vec![
            public(1, EXPECTED, Some(1)),
            public(2, EXPECTED, Some(1)),
            independent(3, 7, EXPECTED, Some(1)),
        ],
        vec![public(1, EXPECTED, Some(1))],
        vec![
            public(1, EXPECTED, Some(1)),
            independent(2, 7, EXPECTED, Some(1)),
            independent(3, 8, EXPECTED, Some(1)),
        ],
    ];

    for history in cases {
        assert_contract(&admin_violations(&history));
    }
}

#[test]
fn every_public_and_independent_identity_must_match() {
    let wrong_identities = [
        Identity {
            operation_id: "other-operation",
            ..EXPECTED
        },
        Identity {
            group_id: "other-group",
            ..EXPECTED
        },
        Identity {
            topic: "other-topic",
            ..EXPECTED
        },
        Identity {
            partition: 1,
            ..EXPECTED
        },
    ];

    for wrong in wrong_identities {
        let wrong_public = [
            public(1, wrong, Some(1)),
            independent(2, 7, EXPECTED, Some(1)),
        ];
        assert_contract(&admin_violations(&wrong_public));

        let wrong_independent = [
            public(1, EXPECTED, Some(1)),
            independent(2, 7, wrong, Some(1)),
        ];
        assert_contract(&admin_violations(&wrong_independent));
    }
}

#[test]
fn absent_or_wrong_group_offset_fails_on_either_boundary() {
    for wrong in [None, Some(2)] {
        let wrong_public = [
            public(1, EXPECTED, wrong),
            independent(2, 7, EXPECTED, Some(1)),
        ];
        assert_contract(&admin_violations(&wrong_public));

        let wrong_independent = [
            public(1, EXPECTED, Some(1)),
            independent(2, 7, EXPECTED, wrong),
        ];
        let violations = admin_violations(&wrong_independent);
        assert_contract(&violations);
        assert!(
            violations[0]
                .evidence
                .contains(&"broker-state-observation:7".to_owned())
        );
    }
}

#[test]
fn action_is_verified_only_after_its_exact_command_is_issued() {
    let action = group_action();
    assert!(!HistoryIndex::build(&[]).action_issued(&action));

    let wrong = HistoryIndex::build(&[group_command(1, "other-operation")]);
    assert!(!wrong.action_issued(&action));

    let exact = HistoryIndex::build(&[group_command(1, EXPECTED.operation_id)]);
    assert!(exact.action_issued(&action));
}

#[test]
fn same_operation_id_with_any_wrong_command_field_is_not_issued() {
    let action = group_action();
    let mutations: [fn(&mut ListConsumerGroupOffsetsCommand); 6] = [
        |command| command.client_id = other_client(),
        |command| command.group_id = "other-group".to_owned(),
        |command| command.topic = "other-topic".to_owned(),
        |command| command.partition = 1,
        |command| command.require_stable = false,
        |command| command.timeout_ms = 2_000,
    ];

    for mutate in mutations {
        let mut command = group_command_payload(EXPECTED.operation_id);
        mutate(&mut command);
        let history = [harness_command(
            1,
            AdapterCommand::ListConsumerGroupOffsets(command),
        )];
        assert!(!HistoryIndex::build(&history).action_issued(&action));
    }
}

#[test]
fn duplicate_same_operation_commands_are_not_issued() {
    let action = group_action();
    let exact = group_command_payload(EXPECTED.operation_id);
    let mut wrong = exact.clone();
    wrong.topic = "other-topic".to_owned();

    for duplicate in [exact.clone(), wrong] {
        let history = [
            harness_command(1, AdapterCommand::ListConsumerGroupOffsets(exact.clone())),
            harness_command(2, AdapterCommand::ListConsumerGroupOffsets(duplicate)),
        ];
        assert!(!HistoryIndex::build(&history).action_issued(&action));
    }
}

fn admin_violations(history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario
        .steps
        .insert(2, step("group-offset", group_action()));
    let history: Vec<_> = std::iter::once(group_command(0, EXPECTED.operation_id))
        .chain(history.iter().cloned())
        .collect();
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    verify_admin(&scenario, &index, &[], &mut violations);
    violations
}

fn group_action() -> ScenarioAction {
    ScenarioAction::ListConsumerGroupOffsets(ListConsumerGroupOffsetsAction {
        client_id: client(),
        operation_id: operation(EXPECTED.operation_id),
        group_id: EXPECTED.group_id.to_owned(),
        topic: EXPECTED.topic.to_owned(),
        partition: EXPECTED.partition,
        require_stable: true,
        expected_offset: 1,
        timeout_ms: 1_000,
    })
}

fn group_command(sequence: u64, operation_id: &str) -> HistoryEntry {
    harness_command(
        sequence,
        AdapterCommand::ListConsumerGroupOffsets(group_command_payload(operation_id)),
    )
}

fn group_command_payload(operation_id: &str) -> ListConsumerGroupOffsetsCommand {
    ListConsumerGroupOffsetsCommand {
        client_id: client(),
        operation_id: operation(operation_id),
        group_id: EXPECTED.group_id.to_owned(),
        topic: EXPECTED.topic.to_owned(),
        partition: EXPECTED.partition,
        require_stable: true,
        timeout_ms: 1_000,
    }
}

fn public(sequence: u64, identity: Identity<'_>, offset: Option<i64>) -> HistoryEntry {
    event(
        sequence,
        AdapterEvent::ConsumerGroupOffsetListed(AdminConsumerGroupOffsetListing {
            operation_id: operation(identity.operation_id),
            group_id: identity.group_id.to_owned(),
            topic: identity.topic.to_owned(),
            partition: identity.partition,
            offset,
        }),
    )
}

fn independent(
    sequence: u64,
    observation: u64,
    identity: Identity<'_>,
    offset: Option<i64>,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::ConsumerGroupOffset(BrokerConsumerGroupOffset {
                observation,
                operation_id: operation(identity.operation_id),
                group_id: identity.group_id.to_owned(),
                topic: identity.topic.to_owned(),
                partition: identity.partition,
                offset,
            }),
        },
    }
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-006"),
        "{violations:?}"
    );
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn other_client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-2").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> testlab_schema::OperationId {
    testlab_schema::OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

#[derive(Clone, Copy)]
struct Identity<'a> {
    operation_id: &'a str,
    group_id: &'a str,
    topic: &'a str,
    partition: i32,
}
