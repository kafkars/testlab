//! Compose provisions genuine quiescent Streams groups with Kafka's bundled demo.

use std::time::{Duration, Instant};

use testlab_schema::{EnvironmentOperationKind, Scenario, ScenarioAction};

use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_types::ComposePhase;

const SCRIPT: &str = r#"set -euo pipefail
input=$1
output=$2
expected=$3
shift 3
if [[ "$input" != "streams-plaintext-input" || "$output" != "streams-wordcount-output" ]]; then
  echo "unexpected Kafka Streams demo topics" >&2
  exit 40
fi
work=$(mktemp -d)
pid=""
cleanup() {
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -rf -- "$work"
}
trap cleanup EXIT
/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092 --create --if-not-exists --topic "$input" --partitions 1 --replication-factor "$TESTLAB_STREAMS_REPLICATION_FACTOR" >/dev/null
/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:19092 --create --if-not-exists --topic "$output" --partitions 1 --replication-factor "$TESTLAB_STREAMS_REPLICATION_FACTOR" --config cleanup.policy=compact >/dev/null
for ((record = 1; record <= expected; record++)); do
  printf 'testlab streams fixture %s\n' "$record"
done | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker-1:19092 --topic "$input" >/dev/null
index=0
for group in "$@"; do
  index=$((index + 1))
  properties="$work/app-$index.properties"
  state="$work/state-$index"
  log="$work/app-$index.log"
  printf '%s\n' \
    "application.id=$group" \
    'bootstrap.servers=broker-1:19092' \
    'group.protocol=streams' \
    'auto.offset.reset=earliest' \
    'commit.interval.ms=100' \
    "state.dir=$state" \
    "replication.factor=$TESTLAB_STREAMS_REPLICATION_FACTOR" \
    > "$properties"
  /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo "$properties" >"$log" 2>&1 &
  pid=$!
  ready=false
  for ((attempt = 0; attempt < 1200; attempt++)); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "Streams fixture exited before committing for $group" >&2
      tail -n 80 "$log" >&2 || true
      exit 41
    fi
    snapshot=$(/opt/kafka/bin/kafka-streams-groups.sh --bootstrap-server broker-1:19092 --describe --group "$group" --offsets --verbose --timeout 5000 2>/dev/null || true)
    if awk -v group="$group" -v topic="$input" -v expected="$expected" '$1 == group && $2 == topic && $3 == "0" && $4 == expected { found = 1 } END { exit found ? 0 : 1 }' <<<"$snapshot"; then
      ready=true
      break
    fi
    sleep 0.05
  done
  if [[ "$ready" != true ]]; then
    echo "Streams fixture did not commit offset $expected for $group" >&2
    tail -n 80 "$log" >&2 || true
    exit 42
  fi
  kill -TERM "$pid"
  wait "$pid" || true
  pid=""
  empty=false
  for ((attempt = 0; attempt < 1200; attempt++)); do
    snapshot=$(/opt/kafka/bin/kafka-streams-groups.sh --bootstrap-server broker-1:19092 --list --state 2>/dev/null || true)
    if awk -v group="$group" '$1 == group { for (field = 2; field <= NF; field++) if ($field == "Empty") found = 1 } END { exit found ? 0 : 1 }' <<<"$snapshot"; then
      empty=true
      break
    fi
    sleep 0.05
  done
  if [[ "$empty" != true ]]; then
    echo "Streams fixture did not become empty for $group" >&2
    exit 43
  fi
  printf 'streams-group-ready:%s:%s\n' "$group" "$expected"
done
"#;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StreamsGroupFixture {
    input_topic: String,
    output_topic: String,
    expected_offset: i64,
    group_ids: Vec<String>,
}

impl DockerComposeEnvironment {
    pub(super) fn provision_streams_groups(
        &mut self,
        scenario: &Scenario,
        timeout: Duration,
    ) -> ComposePhase {
        let mut phase = ComposePhase::default();
        let fixture = match fixture(scenario) {
            Ok(Some(value)) => value,
            Ok(None) => return phase,
            Err(diagnostic) => {
                phase.fail("environment_provision_failed", diagnostic);
                return phase;
            }
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            phase.fail(
                "environment_provision_failed",
                "Streams-group provisioning deadline overflow",
            );
            return phase;
        };
        let Some(service) = self.broker_services.first().cloned() else {
            phase.fail(
                "environment_provision_failed",
                "no broker service for Streams-group provisioning",
            );
            return phase;
        };
        let operation = self.next_operation;
        let mut args = vec![
            "exec".to_owned(),
            "--no-TTY".to_owned(),
            "--env".to_owned(),
            format!("TESTLAB_STREAMS_REPLICATION_FACTOR={}", self.cluster_size),
            service,
            "/bin/bash".to_owned(),
            "-euc".to_owned(),
            SCRIPT.to_owned(),
            "testlab-streams-group-provisioner".to_owned(),
            fixture.input_topic,
            fixture.output_topic,
            fixture.expected_offset.to_string(),
        ];
        args.extend(fixture.group_ids);
        let spec = compose_owned(
            EnvironmentOperationKind::BrokerProvision,
            &self.prefix,
            args,
            format!("streams-group-provision-{operation:05}.txt"),
            format!("streams-group-provision-{operation:05}.stderr.txt"),
        );
        self.required(&mut phase, spec, deadline);
        phase
    }
}

pub(super) fn fixture(scenario: &Scenario) -> Result<Option<StreamsGroupFixture>, String> {
    let fixtures = scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::ExerciseStreamsGroupAdminLifecycle(action) => {
                Some(StreamsGroupFixture {
                    input_topic: action.input_topic.clone(),
                    output_topic: action.output_topic.clone(),
                    expected_offset: action.expected_initial_offset,
                    group_ids: vec![
                        action.primary_group_id.clone(),
                        action.secondary_group_id.clone(),
                    ],
                })
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    match fixtures.as_slice() {
        [] => Ok(None),
        [fixture] => Ok(Some(fixture.clone())),
        _ => Err("a scenario may provision only one Streams-group lifecycle fixture".to_owned()),
    }
}

#[cfg(test)]
#[path = "compose_streams_group_provision_test.rs"]
mod tests;
