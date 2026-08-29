//! Network-proxy schema tests pin exact controls, pairing, and checked scenarios.

use crate::{
    NETWORK_PROXY_PROTOCOL_VERSION, NetworkDirection, NetworkFault, NetworkFaultAction,
    NetworkFaultState, NetworkProxyControl, NetworkProxyControlEnvelope, Scenario, ScenarioAction,
};

const SCENARIOS: &[&str] = &[
    include_str!("../../../scenarios/kafka/producer-network-connection-cut-recovery.toml"),
    include_str!("../../../scenarios/kafka/producer-network-blackhole-recovery.toml"),
    include_str!("../../../scenarios/kafka/producer-network-latency-progress.toml"),
];

#[test]
fn checked_in_network_scenarios_validate() {
    for source in SCENARIOS {
        let scenario: Scenario = toml::from_str(source)
            .unwrap_or_else(|error| panic!("parse network scenario: {error}"));
        scenario
            .validate()
            .unwrap_or_else(|error| panic!("validate {}: {error}", scenario.id));
    }
}

#[test]
fn control_json_round_trip_preserves_direction_and_identity() {
    let envelope = NetworkProxyControlEnvelope {
        protocol_version: NETWORK_PROXY_PROTOCOL_VERSION,
        control: NetworkProxyControl::AlterFault(NetworkFaultAction {
            operation_id: environment_id("delay-apply"),
            broker_ordinal: 2,
            fault: NetworkFault::Delay {
                direction: NetworkDirection::BrokerToClient,
                delay_ms: 250,
            },
            state: NetworkFaultState::Present,
            timeout_ms: 5_000,
        }),
    };

    let encoded = serde_json::to_string(&envelope)
        .unwrap_or_else(|error| panic!("encode network control: {error}"));
    let decoded = serde_json::from_str::<NetworkProxyControlEnvelope>(&encoded)
        .unwrap_or_else(|error| panic!("decode network control: {error}"));

    assert_eq!(decoded, envelope);
    assert!(encoded.contains("\"direction\":\"broker_to_client\""));
    assert!(encoded.contains("\"operation_id\":\"delay-apply\""));
}

#[test]
fn an_active_fault_must_have_one_exact_removal() {
    let mut scenario: Scenario = toml::from_str(SCENARIOS[1])
        .unwrap_or_else(|error| panic!("parse blackhole scenario: {error}"));
    scenario.steps.retain(|step| {
        !matches!(
            &step.action,
            ScenarioAction::AlterNetworkFault(action)
                if action.state == NetworkFaultState::Absent
        )
    });

    let error = match scenario.validate() {
        Ok(()) => panic!("an active network fault must invalidate the scenario"),
        Err(error) => error,
    };

    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("was not removed")),
        "{:?}",
        error.problems
    );
}

#[test]
fn a_removal_must_match_the_full_fault_and_delay_bounds() {
    let mut scenario: Scenario = toml::from_str(SCENARIOS[2])
        .unwrap_or_else(|error| panic!("parse latency scenario: {error}"));
    for step in &mut scenario.steps {
        let ScenarioAction::AlterNetworkFault(action) = &mut step.action else {
            continue;
        };
        if action.state == NetworkFaultState::Absent {
            action.fault = NetworkFault::Delay {
                direction: NetworkDirection::ClientToBroker,
                delay_ms: 9,
            };
            break;
        }
    }

    let error = match scenario.validate() {
        Ok(()) => panic!("mismatched and out-of-bounds removal must fail"),
        Err(error) => error,
    };

    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("without an exact apply")),
        "{:?}",
        error.problems
    );
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("delay_ms")),
        "{:?}",
        error.problems
    );
}

fn environment_id(value: &str) -> crate::EnvironmentOperationId {
    crate::EnvironmentOperationId::new(value)
        .unwrap_or_else(|error| panic!("environment operation id: {error}"))
}
