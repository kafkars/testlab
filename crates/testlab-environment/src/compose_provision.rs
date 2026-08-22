//! Compose provisioning creates declared topics through an independent Kafka admin.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use futures_executor::block_on;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use testlab_schema::{
    EnvironmentOperation, EnvironmentOperationKind, EnvironmentOperationStatus, Scenario,
    ScenarioAction,
};

use crate::compose::DockerComposeEnvironment;
use crate::compose_support::elapsed_unix_ms;
use crate::compose_types::ComposePhase;
use crate::security::ClientSecurity;

const READINESS_TOPIC: &str = "testlab-environment-readiness";

impl DockerComposeEnvironment {
    /// Creates every scenario topic with enough partitions for declared records.
    pub fn provision(&mut self, scenario: &Scenario, timeout: Duration) -> ComposePhase {
        let topics = topics(scenario);
        if topics.is_empty() {
            return ComposePhase::default();
        }
        let mut phase = ComposePhase::default();
        let id = match self.operation_id() {
            Ok(id) => id,
            Err(error) => {
                phase.fail(error.code(), error.diagnostic());
                return phase;
            }
        };
        let endpoint = self.endpoint();
        let operation_started = Instant::now();
        let started_unix_ms = elapsed_unix_ms(self.started_unix_ms, self.started.elapsed());
        let result = provision(
            &endpoint,
            &self.run_id.to_string(),
            &topics,
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
            args: operation_args(&endpoint, &topics),
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
    timeout: Duration,
    security: &ClientSecurity,
) -> Result<(), String> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", endpoint)
        .set("client.id", format!("testlab-provisioner-{run_id}"));
    security.configure(&mut config);
    let admin: AdminClient<DefaultClientContext> =
        config.create().map_err(|error| error.to_string())?;
    let requests = topics
        .iter()
        .map(|(topic, partitions)| NewTopic::new(topic, *partitions, TopicReplication::Fixed(1)))
        .chain(std::iter::once(NewTopic::new(
            READINESS_TOPIC,
            1,
            TopicReplication::Fixed(1),
        )))
        .collect::<Vec<_>>();
    let options = AdminOptions::new()
        .request_timeout(Some(timeout))
        .operation_timeout(Some(timeout));
    let results =
        block_on(admin.create_topics(&requests, &options)).map_err(|error| error.to_string())?;
    for result in results {
        if let Err((topic, error)) = result {
            return Err(format!("topic {topic} creation failed: {error}"));
        }
    }
    prove_idempotent_production(endpoint, run_id, timeout, security)
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

fn topics(scenario: &Scenario) -> BTreeMap<String, i32> {
    let mut topics = BTreeMap::<String, i32>::new();
    for step in &scenario.steps {
        if let ScenarioAction::Send { record, .. } = &step.action {
            let partitions = record.partition.saturating_add(1);
            topics
                .entry(record.topic.clone())
                .and_modify(|current| *current = (*current).max(partitions))
                .or_insert(partitions);
        }
    }
    topics
}

fn operation_args(endpoint: &str, topics: &BTreeMap<String, i32>) -> Vec<String> {
    let mut args = vec![
        "--bootstrap-server".to_owned(),
        endpoint.to_owned(),
        "--readiness-topic".to_owned(),
        READINESS_TOPIC.to_owned(),
    ];
    for (topic, partitions) in topics {
        args.extend([
            "--topic".to_owned(),
            topic.clone(),
            "--partitions".to_owned(),
            partitions.to_string(),
            "--replication-factor".to_owned(),
            "1".to_owned(),
        ]);
    }
    args
}
