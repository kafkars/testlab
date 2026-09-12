//! Configuration-resource calls retain exact public surface and resource identities.

use std::io::Write;
use std::time::Duration;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminConfigResource, AdminConfigResourcesListing,
    AdminTopicConfigsDescription, CommandId, ConfigResourceListingApi, DescribeTopicConfigsCommand,
    ListConfigResourcesCommand,
};

use crate::AdapterError;
use crate::kafkars_api::{ConfigResourceQuery, ConfigResourceType};
use crate::protocol::emit;
use crate::state::AdapterState;

pub(crate) fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeTopicConfigsCommand,
) -> Result<(), AdapterError> {
    let queries = command.topics.iter().map(|selected| {
        ConfigResourceQuery::new(ConfigResourceType::Topic, selected.topic.clone())
            .configuration_keys([selected.config_name.clone()])
    });
    let result = state
        .client(&command.client_id)?
        .admin()
        .describe_config_resources(queries)
        .include_synonyms(command.include_synonyms)
        .include_documentation(command.include_documentation)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let entries = result
        .into_resources()
        .into_entries()
        .into_iter()
        .map(|(resource, result)| {
            if resource.resource_type() != ConfigResourceType::Topic {
                return Err(invalid(
                    &command.operation_id,
                    "returned a non-topic configuration resource",
                ));
            }
            Ok((
                resource.name().to_owned(),
                result.map(crate::protocol_admin_config_entry::normalize_entries),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let outcomes = crate::protocol_admin_config_entry::described_outcomes(
        entries,
        &command.topics,
        &command.operation_id,
    )?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TopicConfigsDescribed(AdminTopicConfigsDescription {
                operation_id: command.operation_id,
                outcomes,
            }),
        ),
    )
}

pub(crate) fn list<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ListConfigResourcesCommand,
) -> Result<(), AdapterError> {
    let admin = state.client(&command.client_id)?.admin();
    let timeout = Duration::from_millis(command.timeout_ms);
    let (throttle, resources) = match command.api {
        ConfigResourceListingApi::Resource => {
            let result = admin
                .list_config_resources()
                .resource_types(vec![ConfigResourceType::Topic])
                .deadline_after(timeout)
                .submit()
                .wait()
                .map_err(AdapterError::Client)?;
            let (throttle, resources) = result.into_parts();
            let resources = resources
                .into_iter()
                .map(|resource| AdminConfigResource {
                    resource_type: resource.resource_type().as_raw(),
                    name: resource.name().to_owned(),
                })
                .collect();
            (throttle, resources)
        }
        ConfigResourceListingApi::ClientMetrics => {
            let result = admin
                .list_client_metrics_resources()
                .deadline_after(timeout)
                .submit()
                .wait()
                .map_err(AdapterError::Client)?;
            let (throttle, names) = result.into_parts();
            let resources = names
                .into_iter()
                .map(|name| AdminConfigResource {
                    resource_type: ConfigResourceType::ClientMetrics.as_raw(),
                    name,
                })
                .collect();
            (throttle, resources)
        }
    };
    let throttle_time_ms = u64::try_from(throttle.as_millis()).map_err(|_| {
        invalid(
            &command.operation_id,
            "returned unrepresentable throttle time",
        )
    })?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ConfigResourcesListed(AdminConfigResourcesListing {
                operation_id: command.operation_id,
                throttle_time_ms,
                resources,
            }),
        ),
    )
}

fn invalid(operation_id: &testlab_schema::OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
