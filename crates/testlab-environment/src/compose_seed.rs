//! Deterministic provisioning records establish fresh deletion watermarks.

use std::collections::BTreeSet;
use std::time::Instant;

use futures_executor::block_on;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::compose_provision_targets::SeedTarget;
use crate::security::ClientSecurity;

pub(super) fn seed_records(
    endpoint: &str,
    run_id: &str,
    targets: &BTreeSet<SeedTarget>,
    deadline: Instant,
    security: &ClientSecurity,
) -> Result<(), String> {
    if targets.is_empty() {
        return Ok(());
    }
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", endpoint)
        .set("client.id", format!("testlab-seeder-{run_id}"))
        .set("enable.idempotence", "true");
    security.configure(&mut config);
    let producer: FutureProducer = config.create().map_err(|error| error.to_string())?;
    for target in targets {
        seed_target(&producer, target, deadline)?;
    }
    Ok(())
}

fn seed_target(
    producer: &FutureProducer,
    target: &SeedTarget,
    deadline: Instant,
) -> Result<(), String> {
    for expected_offset in 0..target.record_count {
        let marker = format!(
            "testlab-delete-records-seed:{}:{}:{expected_offset}",
            target.topic, target.partition
        );
        let timeout = super::compose_provision::remaining(deadline)?;
        let delivery = block_on(
            producer.send(
                FutureRecord::to(&target.topic)
                    .partition(target.partition)
                    .key(&marker)
                    .payload(&marker),
                timeout,
            ),
        );
        let delivery = delivery.map_err(|(error, _)| {
            format!(
                "seed delivery for {}[{}] failed: {error}",
                target.topic, target.partition
            )
        })?;
        if delivery.partition != target.partition || delivery.offset != expected_offset {
            return Err(format!(
                "seed delivery for {}[{}] returned {}:{}, expected {}:{expected_offset}",
                target.topic,
                target.partition,
                delivery.partition,
                delivery.offset,
                target.partition
            ));
        }
    }
    Ok(())
}
