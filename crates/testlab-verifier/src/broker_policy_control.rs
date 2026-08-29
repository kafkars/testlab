//! Broker-policy control verification checks raw terminals before normalized facts are trusted.

use testlab_schema::{
    BrokerAclResource, BrokerPolicy, BrokerPolicyState, EnvironmentOperation,
    EnvironmentOperationKind, EnvironmentOperationStatus,
};

use crate::index::HistoryIndex;

pub(crate) struct PolicyFact<'a> {
    pub(crate) alter_sequence: u64,
    pub(crate) alter: &'a EnvironmentOperation,
    pub(crate) query_sequence: u64,
    pub(crate) query: &'a EnvironmentOperation,
    pub(crate) observation_sequence: u64,
    pub(crate) observation: &'a EnvironmentOperation,
}

pub(crate) fn facts<'a>(
    index: &'a HistoryIndex,
    policy: &BrokerPolicy,
    state: BrokerPolicyState,
) -> Vec<PolicyFact<'a>> {
    let expected = policy.evidence_args(state);
    index
        .environment_operations
        .iter()
        .enumerate()
        .filter_map(|(position, (sequence, operation))| {
            if operation.kind != EnvironmentOperationKind::BrokerPolicyObserve
                || operation.args != expected
                || position < 2
            {
                return None;
            }
            let (alter_sequence, alter) = &index.environment_operations[position - 2];
            let (query_sequence, query) = &index.environment_operations[position - 1];
            Some(PolicyFact {
                alter_sequence: *alter_sequence,
                alter,
                query_sequence: *query_sequence,
                query,
                observation_sequence: *sequence,
                observation: operation,
            })
        })
        .collect()
}

pub(crate) fn valid(
    fact: &PolicyFact<'_>,
    policy: &BrokerPolicy,
    state: BrokerPolicyState,
) -> bool {
    fact.alter.kind == EnvironmentOperationKind::BrokerPolicyAlter
        && fact.query.kind == EnvironmentOperationKind::BrokerPolicyQuery
        && fact.observation.kind == EnvironmentOperationKind::BrokerPolicyObserve
        && [fact.alter, fact.query, fact.observation]
            .iter()
            .all(|operation| operation.status == EnvironmentOperationStatus::Succeeded)
        && fact.alter.program == "docker"
        && fact.query.program == "docker"
        && fact.observation.program == "testlab-kafka-policy-observer/1"
        && fact.alter_sequence < fact.query_sequence
        && fact.query_sequence < fact.observation_sequence
        && ordered_time(fact.alter, fact.query)
        && ordered_time(fact.query, fact.observation)
        && alter_args(&fact.alter.args, policy, state)
        && query_args(&fact.query.args, policy)
}

pub(crate) fn references(fact: &PolicyFact<'_>) -> Vec<String> {
    [
        fact.alter_sequence,
        fact.query_sequence,
        fact.observation_sequence,
    ]
    .into_iter()
    .map(|sequence| format!("history:{sequence}"))
    .collect()
}

fn ordered_time(before: &EnvironmentOperation, after: &EnvironmentOperation) -> bool {
    before.started_unix_ms <= before.completed_unix_ms
        && before.completed_unix_ms <= after.started_unix_ms
        && after.started_unix_ms <= after.completed_unix_ms
}

fn alter_args(args: &[String], policy: &BrokerPolicy, state: BrokerPolicyState) -> bool {
    if !compose_cli(
        args,
        match policy {
            BrokerPolicy::Acl { .. } => "kafka-acls.sh",
            BrokerPolicy::Quota { .. } => "kafka-configs.sh",
        },
    ) {
        return false;
    }
    match policy {
        BrokerPolicy::Acl {
            resource,
            operation,
        } => {
            let transition = match state {
                BrokerPolicyState::Present => "--add",
                BrokerPolicyState::Absent => "--remove",
            };
            args.iter().any(|value| value == transition)
                && pair(args, "--deny-principal", "User:kafkars")
                && pair(args, "--operation", operation.cli_name())
                && resource_pair(args, resource)
        }
        BrokerPolicy::Quota {
            direction,
            bytes_per_second,
            ..
        } => {
            pair(args, "--entity-type", "users")
                && pair(args, "--entity-name", "kafkars")
                && match state {
                    BrokerPolicyState::Present => pair(
                        args,
                        "--add-config",
                        &format!("{}={bytes_per_second}", direction.config_name()),
                    ),
                    BrokerPolicyState::Absent => {
                        pair(args, "--delete-config", direction.config_name())
                    }
                }
        }
    }
}

fn query_args(args: &[String], policy: &BrokerPolicy) -> bool {
    match policy {
        BrokerPolicy::Acl { resource, .. } => {
            compose_cli(args, "kafka-acls.sh")
                && args.iter().any(|value| value == "--list")
                && resource_pair(args, resource)
        }
        BrokerPolicy::Quota { .. } => {
            compose_cli(args, "kafka-configs.sh")
                && args.iter().any(|value| value == "--describe")
                && pair(args, "--entity-type", "users")
                && pair(args, "--entity-name", "kafkars")
        }
    }
}

fn resource_pair(args: &[String], resource: &BrokerAclResource) -> bool {
    let flag = match resource {
        BrokerAclResource::Topic { .. } => "--topic",
        BrokerAclResource::Group { .. } => "--group",
        BrokerAclResource::TransactionalId { .. } => "--transactional-id",
    };
    pair(args, flag, resource.name())
}

fn pair(args: &[String], key: &str, value: &str) -> bool {
    args.windows(2)
        .any(|pair| pair[0] == key && pair[1] == value)
}

fn compose_cli(args: &[String], script: &str) -> bool {
    let expected = format!("/opt/kafka/bin/{script}");
    let Some(position) = args.iter().position(|value| value == &expected) else {
        return false;
    };
    position >= 3
        && args[position - 3] == "exec"
        && args[position - 2] == "--no-TTY"
        && !args[position - 1].is_empty()
        && args
            .get(position + 1)
            .is_some_and(|value| value == "--bootstrap-server")
        && args.get(position + 2).is_some_and(|value| {
            value
                .strip_prefix("localhost:")
                .and_then(|port| port.parse::<u16>().ok())
                .is_some_and(|port| port > 0)
        })
}
