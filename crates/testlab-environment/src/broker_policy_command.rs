//! Broker-policy command construction keeps exact Compose effects reviewable.

use testlab_schema::{
    BrokerAclResource, BrokerPolicy, BrokerPolicyState, EnvironmentOperationKind,
};

use crate::compose_command::{CommandSpec, compose_owned};

const PRINCIPAL: &str = "User:kafkars";
const USER: &str = "kafkars";

pub(super) fn alter(
    prefix: &[String],
    service: &str,
    port: u16,
    policy: &BrokerPolicy,
    state: BrokerPolicyState,
    operation: u32,
) -> CommandSpec {
    let tail = match policy {
        BrokerPolicy::Acl {
            resource,
            operation,
        } => {
            let mut args = cli(prefix_args(service, "kafka-acls.sh"), port);
            args.push(match state {
                BrokerPolicyState::Present => "--add".to_owned(),
                BrokerPolicyState::Absent => "--remove".to_owned(),
            });
            if state == BrokerPolicyState::Absent {
                args.push("--force".to_owned());
            }
            args.extend([
                "--deny-principal".to_owned(),
                PRINCIPAL.to_owned(),
                "--operation".to_owned(),
                operation.cli_name().to_owned(),
            ]);
            resource_args(&mut args, resource);
            args
        }
        BrokerPolicy::Quota {
            direction,
            bytes_per_second,
            ..
        } => {
            let mut args = cli(prefix_args(service, "kafka-configs.sh"), port);
            args.extend([
                "--alter".to_owned(),
                "--entity-type".to_owned(),
                "users".to_owned(),
                "--entity-name".to_owned(),
                USER.to_owned(),
            ]);
            match state {
                BrokerPolicyState::Present => args.extend([
                    "--add-config".to_owned(),
                    format!("{}={bytes_per_second}", direction.config_name()),
                ]),
                BrokerPolicyState::Absent => args.extend([
                    "--delete-config".to_owned(),
                    direction.config_name().to_owned(),
                ]),
            }
            args
        }
    };
    compose_owned(
        EnvironmentOperationKind::BrokerPolicyAlter,
        prefix,
        tail,
        format!("broker-policy-alter-{operation:05}.txt"),
        format!("broker-policy-alter-{operation:05}.stderr.txt"),
    )
}

pub(super) fn query(
    prefix: &[String],
    service: &str,
    port: u16,
    policy: &BrokerPolicy,
    operation: u32,
) -> CommandSpec {
    let tail = match policy {
        BrokerPolicy::Acl { resource, .. } => {
            let mut args = cli(prefix_args(service, "kafka-acls.sh"), port);
            args.push("--list".to_owned());
            resource_args(&mut args, resource);
            args
        }
        BrokerPolicy::Quota { .. } => {
            let mut args = cli(prefix_args(service, "kafka-configs.sh"), port);
            args.extend([
                "--describe".to_owned(),
                "--entity-type".to_owned(),
                "users".to_owned(),
                "--entity-name".to_owned(),
                USER.to_owned(),
            ]);
            args
        }
    };
    compose_owned(
        EnvironmentOperationKind::BrokerPolicyQuery,
        prefix,
        tail,
        format!("broker-policy-query-{operation:05}.txt"),
        format!("broker-policy-query-{operation:05}.stderr.txt"),
    )
}

fn prefix_args(service: &str, script: &str) -> Vec<String> {
    vec![
        "exec".to_owned(),
        "--no-TTY".to_owned(),
        service.to_owned(),
        format!("/opt/kafka/bin/{script}"),
    ]
}

fn cli(mut args: Vec<String>, port: u16) -> Vec<String> {
    args.extend(["--bootstrap-server".to_owned(), format!("localhost:{port}")]);
    args
}

fn resource_args(args: &mut Vec<String>, resource: &BrokerAclResource) {
    let flag = match resource {
        BrokerAclResource::Topic { .. } => "--topic",
        BrokerAclResource::Group { .. } => "--group",
        BrokerAclResource::TransactionalId { .. } => "--transactional-id",
    };
    args.extend([flag.to_owned(), resource.name().to_owned()]);
}
