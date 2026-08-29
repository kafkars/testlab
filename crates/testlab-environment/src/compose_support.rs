//! Compose lifecycle helpers derive stable names and bounded timing slices.

use std::time::{Duration, Instant};

use testlab_schema::{EnvironmentOperationKind, RunId};

pub(super) fn compose_prefix(project: &str, files: &[String]) -> Vec<String> {
    let mut args = vec![
        "compose".to_owned(),
        "--project-name".to_owned(),
        project.to_owned(),
    ];
    for file in files {
        args.push("--file".to_owned());
        args.push(file.clone());
    }
    args
}

pub(super) fn project_name(run_id: &RunId) -> String {
    format!("testlab-{}", run_id.as_str().to_ascii_lowercase())
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '-'
            }
        })
        .collect()
}

pub(super) fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

pub(super) fn elapsed_unix_ms(started_unix_ms: u64, elapsed: Duration) -> u64 {
    let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    started_unix_ms.saturating_add(elapsed_ms)
}

pub(super) fn failure_code(kind: EnvironmentOperationKind) -> &'static str {
    use EnvironmentOperationKind::{
        BrokerFeatureSetup, BrokerPolicyAlter, BrokerPolicyQuery, BrokerRestart,
        BrokerSecuritySetup, BrokerStart, BrokerStop, ComposeConfig, ComposeUp, ImageInspect,
        ImagePull,
    };
    match kind {
        ImagePull => "environment_image_pull_failed",
        ImageInspect => "environment_image_unavailable",
        ComposeConfig => "environment_compose_config_failed",
        ComposeUp => "environment_compose_up_failed",
        BrokerRestart => "environment_broker_restart_failed",
        BrokerStop => "environment_broker_stop_failed",
        BrokerStart => "environment_broker_start_failed",
        BrokerSecuritySetup => "environment_security_setup_failed",
        BrokerFeatureSetup => "environment_feature_setup_failed",
        BrokerPolicyAlter => "environment_broker_policy_alter_failed",
        BrokerPolicyQuery => "environment_broker_policy_query_failed",
        _ => "environment_operation_failed",
    }
}
