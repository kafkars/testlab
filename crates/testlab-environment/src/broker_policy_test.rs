//! Broker-policy tests pin exact CLI effects and fail-closed query parsing.

use testlab_schema::{
    BrokerAclOperation, BrokerAclResource, BrokerPolicy, BrokerPolicyState, BrokerQuotaDirection,
    EnvironmentOperationKind,
};

#[test]
fn acl_commands_target_exact_principal_resource_and_transition() {
    let policy = topic_policy();
    let prefix = vec!["compose".to_owned()];

    let add = super::broker_policy_command::alter(
        &prefix,
        "broker",
        19092,
        &policy,
        BrokerPolicyState::Present,
        7,
    );
    let remove = super::broker_policy_command::alter(
        &prefix,
        "broker",
        19092,
        &policy,
        BrokerPolicyState::Absent,
        8,
    );

    assert_eq!(add.kind, EnvironmentOperationKind::BrokerPolicyAlter);
    assert!(pair(&add.args, "--deny-principal", "User:kafkars"));
    assert!(pair(&add.args, "--operation", "Write"));
    assert!(pair(&add.args, "--topic", "orders"));
    assert!(add.args.iter().any(|value| value == "--add"));
    assert!(remove.args.iter().any(|value| value == "--remove"));
    assert!(remove.args.iter().any(|value| value == "--force"));
}

#[test]
fn quota_removal_deletes_only_the_selected_direction() {
    let policy = BrokerPolicy::Quota {
        direction: BrokerQuotaDirection::Consumer,
        bytes_per_second: 128,
        minimum_active_ms: 500,
    };

    let command = super::broker_policy_command::alter(
        &["compose".to_owned()],
        "broker",
        19092,
        &policy,
        BrokerPolicyState::Absent,
        9,
    );

    assert!(pair(&command.args, "--entity-name", "kafkars"));
    assert!(pair(&command.args, "--delete-config", "consumer_byte_rate"));
    assert!(
        !command
            .args
            .iter()
            .any(|value| value.contains("producer_byte_rate"))
    );
}

#[test]
fn acl_parser_requires_the_exact_literal_resource_and_deny_entry() {
    let output = b"Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=orders, patternType=LITERAL)`:\n\t(principal=User:kafkars, host=*, operation=WRITE, permissionType=DENY)\n";

    assert_eq!(
        super::broker_policy_observation::parse(&topic_policy(), output),
        Ok(true)
    );
    assert!(super::broker_policy_observation::parse(
        &topic_policy(),
        b"Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=other, patternType=LITERAL)`:\n"
    )
    .is_err());
    assert_eq!(
        super::broker_policy_observation::parse(
            &topic_policy(),
            b"Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=orders, patternType=LITERAL)`:\n"
        ),
        Ok(false)
    );
    assert!(super::broker_policy_observation::parse(&topic_policy(), b"").is_err());
}

#[test]
fn quota_parser_rejects_a_different_numeric_rate() {
    let policy = BrokerPolicy::Quota {
        direction: BrokerQuotaDirection::Producer,
        bytes_per_second: 128,
        minimum_active_ms: 500,
    };

    assert_eq!(
        super::broker_policy_observation::parse(
            &policy,
            b"Quota configs for user-principal 'kafkars' are producer_byte_rate=128.0\n"
        ),
        Ok(true)
    );
    assert!(
        super::broker_policy_observation::parse(
            &policy,
            b"Quota configs for user-principal 'kafkars' are producer_byte_rate=256\n"
        )
        .is_err()
    );
}

fn topic_policy() -> BrokerPolicy {
    BrokerPolicy::Acl {
        resource: BrokerAclResource::Topic {
            name: "orders".to_owned(),
        },
        operation: BrokerAclOperation::Write,
    }
}

fn pair(args: &[String], key: &str, value: &str) -> bool {
    args.windows(2)
        .any(|pair| pair[0] == key && pair[1] == value)
}
