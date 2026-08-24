//! Compose provisioning creates declared topics through an independent Kafka admin.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use futures_executor::block_on;
use rdkafka::ClientConfig;
use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, NewTopic, ResourceSpecifier, TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use testlab_schema::{
    EnvironmentOperation, EnvironmentOperationKind, EnvironmentOperationStatus, Scenario,
    ScenarioAction,
};

use crate::compose::DockerComposeEnvironment;
use crate::compose_support::elapsed_unix_ms;
use crate::compose_topic_readiness;
use crate::compose_types::ComposePhase;
use crate::security::ClientSecurity;

const READINESS_TOPIC: &str = "testlab-environment-readiness";

impl DockerComposeEnvironment {
    /// Creates harness-owned topics and proves the broker accepts idempotent production.
    pub fn provision(&mut self, scenario: &Scenario, timeout: Duration) -> ComposePhase {
        let topics = topics(scenario);
        let share_groups = share_groups(scenario);
        let mut phase = ComposePhase::default();
        let id = match self.operation_id() {
            Ok(id) => id,
            Err(error) => {
                phase.fail(error.code(), error.diagnostic());
                return phase;
            }
        };
        let endpoint = self.endpoint();
        let replication_factor = i32::from(self.cluster_size);
        let operation_started = Instant::now();
        let started_unix_ms = elapsed_unix_ms(self.started_unix_ms, self.started.elapsed());
        let result = provision(
            &endpoint,
            &self.run_id.to_string(),
            &topics,
            &share_groups,
            replication_factor,
            timeout,
            &self.client_security,
        );
        let completed_unix_ms = elapsed_unix_ms(started_unix_ms, operation_started.elapsed());
        let (status, diagnostic) = match result {
            Ok(()) => (EnvironmentOperationStatus::Succeeded, None),
            Err(diagnostic) => {
                let status = if operation_started.elapsed() >= timeout {
                    EnvironmentOperationStatus::TimedOut
                } else {
                    EnvironmentOperationStatus::Failed
                };
                phase.fail("environment_provision_failed", &diagnostic);
                (status, Some(diagnostic))
            }
        };
        let (_, librdkafka_version) = rdkafka::util::get_rdkafka_version();
        phase.operations.push(EnvironmentOperation {
            id,
            kind: EnvironmentOperationKind::BrokerProvision,
            program: format!("librdkafka/{librdkafka_version}"),
            args: operation_args(&endpoint, &topics, &share_groups, replication_factor),
            started_unix_ms,
            completed_unix_ms,
            status,
            exit_code: None,
            stdout_artifact: None,
            stderr_artifact: None,
            diagnostic,
        });
        phase
    }
}

fn provision(
    endpoint: &str,
    run_id: &str,
    topics: &BTreeMap<String, i32>,
    share_groups: &BTreeSet<String>,
    replication_factor: i32,
    timeout: Duration,
    security: &ClientSecurity,
) -> Result<(), String> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| "provisioning deadline overflowed".to_owned())?;
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", endpoint)
        .set("client.id", format!("testlab-provisioner-{run_id}"));
    security.configure(&mut config);
    let admin: AdminClient<DefaultClientContext> =
        config.create().map_err(|error| error.to_string())?;
    let mut expected_topics = topics.clone();
    expected_topics.insert(READINESS_TOPIC.to_owned(), 1);
    let requests = expected_topics
        .iter()
        .map(|(topic, partitions)| {
            NewTopic::new(
                topic,
                *partitions,
                TopicReplication::Fixed(replication_factor),
            )
        })
        .collect::<Vec<_>>();
    let operation_timeout = remaining(deadline)?;
    let options = AdminOptions::new()
        .request_timeout(Some(operation_timeout))
        .operation_timeout(Some(operation_timeout));
    let results =
        block_on(admin.create_topics(&requests, &options)).map_err(|error| error.to_string())?;
    for result in results {
        if let Err((topic, error)) = result {
            return Err(format!("topic {topic} creation failed: {error}"));
        }
    }
    configure_share_groups(&admin, share_groups, deadline)?;
    compose_topic_readiness::wait(&admin, &expected_topics, replication_factor, deadline)?;
    prove_idempotent_production(endpoint, run_id, remaining(deadline)?, security)
}

fn configure_share_groups(
    admin: &AdminClient<DefaultClientContext>,
    share_groups: &BTreeSet<String>,
    deadline: Instant,
) -> Result<(), String> {
    if share_groups.is_empty() {
        return Ok(());
    }
    let configs = share_groups
        .iter()
        .map(|group_id| {
            AlterConfig::new(ResourceSpecifier::Group(group_id))
                .set("share.auto.offset.reset", "earliest")
        })
        .collect::<Vec<_>>();
    let options = AdminOptions::new().request_timeout(Some(remaining(deadline)?));
    let results =
        block_on(admin.alter_configs(&configs, &options)).map_err(|error| error.to_string())?;
    for result in results {
        if let Err((resource, error)) = result {
            return Err(format!(
                "share group {resource:?} configuration failed: {error}"
            ));
        }
    }
    Ok(())
}

fn remaining(deadline: Instant) -> Result<Duration, String> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err("provisioning deadline elapsed".to_owned())
    } else {
        Ok(remaining)
    }
}

fn prove_idempotent_production(
    endpoint: &str,
    run_id: &str,
    timeout: Duration,
    security: &ClientSecurity,
) -> Result<(), String> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", endpoint)
        .set("client.id", format!("testlab-readiness-{run_id}"))
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", timeout.as_millis().to_string());
    security.configure(&mut config);
    let producer: FutureProducer = config.create().map_err(|error| error.to_string())?;
    let delivery = block_on(
        producer.send(
            FutureRecord::to(READINESS_TOPIC)
                .partition(0)
                .key(run_id)
                .payload("ready"),
            timeout,
        ),
    );
    delivery
        .map(|_| ())
        .map_err(|(error, _)| format!("idempotent readiness delivery failed: {error}"))
}

pub(super) fn topics(scenario: &Scenario) -> BTreeMap<String, i32> {
    let mut topics = BTreeMap::<String, i32>::new();
    let admin_topics = scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::CreateTopic { topic, .. } => Some(topic.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    for step in &scenario.steps {
        let records = match &step.action {
            ScenarioAction::Send { record, .. } => std::slice::from_ref(record),
            ScenarioAction::SendBatch { operations, .. }
            | ScenarioAction::ExecuteTransaction { operations, .. } => {
                for operation in operations {
                    record_topic(&mut topics, &admin_topics, &operation.record);
                }
                continue;
            }
            ScenarioAction::FenceTransaction { operation, .. } => {
                std::slice::from_ref(&operation.record)
            }
            _ => continue,
        };
        for record in records {
            record_topic(&mut topics, &admin_topics, record);
        }
    }
    topics
}

pub(super) fn share_groups(scenario: &Scenario) -> BTreeSet<String> {
    scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::CreateShareConsumer { group_id, .. } => Some(group_id.clone()),
            _ => None,
        })
        .collect()
}

fn record_topic(
    topics: &mut BTreeMap<String, i32>,
    admin_topics: &BTreeSet<String>,
    record: &testlab_schema::RecordSpec,
) {
    if admin_topics.contains(&record.topic) {
        return;
    }
    let partitions = record.partition.saturating_add(1);
    topics
        .entry(record.topic.clone())
        .and_modify(|current| *current = (*current).max(partitions))
        .or_insert(partitions);
}

pub(super) fn operation_args(
    endpoint: &str,
    topics: &BTreeMap<String, i32>,
    share_groups: &BTreeSet<String>,
    replication_factor: i32,
) -> Vec<String> {
    let mut args = vec![
        "--bootstrap-server".to_owned(),
        endpoint.to_owned(),
        "--readiness-topic".to_owned(),
        READINESS_TOPIC.to_owned(),
        "--require-full-isr".to_owned(),
    ];
    for (topic, partitions) in topics {
        args.extend([
            "--topic".to_owned(),
            topic.clone(),
            "--partitions".to_owned(),
            partitions.to_string(),
            "--replication-factor".to_owned(),
            replication_factor.to_string(),
        ]);
    }
    for group_id in share_groups {
        args.extend([
            "--share-group".to_owned(),
            group_id.clone(),
            "--share-auto-offset-reset".to_owned(),
            "earliest".to_owned(),
        ]);
    }
    args
}
