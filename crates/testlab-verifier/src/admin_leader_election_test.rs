//! Leader-election verdicts pin public scope, metadata, and proven ownership changes.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminLeaderElection, AdminLeaderElectionOutcome,
    AdminLeaderElectionType, BrokerLeaderElectionPartition, BrokerLeaderElectionState,
    BrokerStateObservation, ElectLeadersAction, ElectLeadersCommand, EnvironmentOperation,
    EnvironmentOperationId, EnvironmentOperationKind, EnvironmentOperationStatus, HistoryEntry,
    HistoryPayload, LeaderElectionSelection, ScenarioAction,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, step};

#[test]
fn selected_preferred_transition_and_cluster_wide_result_pass() {
    assert!(selected_violations(selected_history()).is_empty());
    assert!(all_violations(all_history()).is_empty());
}

#[test]
fn selected_order_preferred_state_and_role_transition_mismatches_fail() {
    let mut wrong_leader = selected_history();
    observed(&mut wrong_leader, 9).partitions[0].leader_id = 2;
    assert_contract(&selected_violations(wrong_leader));

    let mut wrong_restore = selected_history();
    let HistoryPayload::EnvironmentOperation { operation } = &mut wrong_restore[6].payload else {
        panic!("after-restore role fact");
    };
    operation.args[4] = "3".to_owned();
    assert_contract(&selected_violations(wrong_restore));

    let mut failed = selected_history();
    elected(&mut failed, 8).outcomes[0].error_code = Some("election_not_needed".to_owned());
    assert_contract(&selected_violations(failed));
}

#[test]
fn cluster_wide_outcomes_must_be_canonical_and_contain_required_partitions() {
    let mut reordered = all_history();
    elected(&mut reordered, 1).outcomes.reverse();
    assert_contract(&all_violations(reordered));

    let mut missing = all_history();
    elected(&mut missing, 1).outcomes.pop();
    assert_contract(&all_violations(missing));
}

#[test]
fn cluster_wide_path_can_prove_the_required_leader_transition() {
    let mut history = selected_history();
    let HistoryPayload::HarnessCommand { command } = &mut history[7].payload else {
        panic!("leader-election command");
    };
    let AdapterCommand::ElectLeaders(command) = &mut command.command else {
        panic!("leader-election payload");
    };
    command.targets = None;

    assert!(all_transition_violations(history).is_empty());
}

fn selected_history() -> Vec<HistoryEntry> {
    let mut history = crate::broker_role_recovery_test::history(2, true);
    history.push(role_after_restore(6, 2));
    history.extend([
        command(7, elect_command(Some(vec![target()]))),
        event(8, elected_event(vec![outcome("records", 0)])),
        state(9, observed_state()),
    ]);
    history
}

fn all_history() -> Vec<HistoryEntry> {
    vec![
        command(0, elect_command(None)),
        event(
            1,
            elected_event(vec![outcome("alpha", 0), outcome("records", 0)]),
        ),
        state(2, observed_state()),
    ]
}

fn selected_violations(history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let mut scenario = crate::broker_role_recovery_test::scenario(target_role());
    scenario.steps.push(step(
        "elect-selected",
        ScenarioAction::ElectLeaders(ElectLeadersAction {
            client_id: client(),
            operation_id: operation(),
            election_type: AdminLeaderElectionType::Preferred,
            targets: Some(vec![target()]),
            required_partitions: Vec::new(),
            require_leader_change: true,
            timeout_ms: 20_000,
        }),
    ));
    violations(scenario, history)
}

fn all_violations(history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let mut scenario = crate::broker_role_recovery_test::scenario(target_role());
    scenario.steps.push(step(
        "elect-all",
        ScenarioAction::ElectLeaders(ElectLeadersAction {
            client_id: client(),
            operation_id: operation(),
            election_type: AdminLeaderElectionType::Preferred,
            targets: None,
            required_partitions: vec![target()],
            require_leader_change: false,
            timeout_ms: 20_000,
        }),
    ));
    violations(scenario, history)
}

fn all_transition_violations(history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let mut scenario = crate::broker_role_recovery_test::scenario(target_role());
    scenario.steps.push(step(
        "elect-all",
        ScenarioAction::ElectLeaders(ElectLeadersAction {
            client_id: client(),
            operation_id: operation(),
            election_type: AdminLeaderElectionType::Preferred,
            targets: None,
            required_partitions: vec![target()],
            require_leader_change: true,
            timeout_ms: 20_000,
        }),
    ));
    violations(scenario, history)
}

fn violations(
    scenario: testlab_schema::Scenario,
    history: Vec<HistoryEntry>,
) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    verify_admin(&scenario, &index, &[], &mut violations);
    violations
}

fn elect_command(targets: Option<Vec<LeaderElectionSelection>>) -> AdapterCommand {
    AdapterCommand::ElectLeaders(ElectLeadersCommand {
        client_id: client(),
        operation_id: operation(),
        election_type: AdminLeaderElectionType::Preferred,
        targets,
        timeout_ms: 20_000,
    })
}

fn elected_event(outcomes: Vec<AdminLeaderElectionOutcome>) -> AdapterEvent {
    AdapterEvent::LeadersElected(AdminLeaderElection {
        operation_id: operation(),
        election_type: AdminLeaderElectionType::Preferred,
        throttle_time_ms: 2,
        outcomes,
    })
}

fn observed_state() -> BrokerStateObservation {
    BrokerStateObservation::LeaderElection(BrokerLeaderElectionState {
        observation: 8,
        operation_id: operation(),
        election_type: AdminLeaderElectionType::Preferred,
        partitions: vec![BrokerLeaderElectionPartition {
            topic: "records".to_owned(),
            partition: 0,
            leader_id: 1,
            replicas: vec![1, 2, 3],
            in_sync_replicas: vec![1, 2, 3],
        }],
    })
}

fn role_after_restore(sequence: u64, node: i32) -> HistoryEntry {
    let args = vec![
        "partition_leader".to_owned(),
        "records".to_owned(),
        "0".to_owned(),
        "after_restore".to_owned(),
        node.to_string(),
        format!("broker-{node}"),
    ];
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::EnvironmentOperation {
            operation: EnvironmentOperation {
                id: EnvironmentOperationId::new("after-restore")
                    .unwrap_or_else(|error| panic!("environment id: {error}")),
                kind: EnvironmentOperationKind::BrokerRoleObserve,
                program: "testlab-kafka-role-observer/1".to_owned(),
                args: args.into(),
                started_unix_ms: sequence,
                completed_unix_ms: sequence,
                status: EnvironmentOperationStatus::Succeeded,
                exit_code: None,
                stdout_artifact: None,
                stderr_artifact: None,
                diagnostic: None,
            },
        },
    }
}

fn state(sequence: u64, observation: BrokerStateObservation) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation { observation },
    }
}

fn elected(entries: &mut [HistoryEntry], index: usize) -> &mut AdminLeaderElection {
    let HistoryPayload::AdapterEvent { event } = &mut entries[index].payload else {
        panic!("leader-election event");
    };
    let AdapterEvent::LeadersElected(value) = &mut event.event else {
        panic!("leader-election payload");
    };
    value
}

fn observed(entries: &mut [HistoryEntry], index: usize) -> &mut BrokerLeaderElectionState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[index].payload else {
        panic!("leader-election observation");
    };
    let BrokerStateObservation::LeaderElection(value) = observation else {
        panic!("leader-election state");
    };
    value
}

fn target() -> LeaderElectionSelection {
    LeaderElectionSelection {
        topic: "records".to_owned(),
        partition: 0,
    }
}

fn target_role() -> testlab_schema::BrokerRoleTarget {
    testlab_schema::BrokerRoleTarget::PartitionLeader {
        topic: "records".to_owned(),
        partition: 0,
    }
}

fn outcome(topic: &str, partition: i32) -> AdminLeaderElectionOutcome {
    AdminLeaderElectionOutcome {
        topic: topic.to_owned(),
        partition,
        error_code: None,
    }
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> testlab_schema::OperationId {
    testlab_schema::OperationId::new("admin-elect-leaders")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-060"),
        "{violations:?}"
    );
}
