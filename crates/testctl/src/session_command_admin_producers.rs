use crate::runner_protocol::ExpectedEvent;
use testlab_schema::{AdapterCommand, DescribeProducersCommand, ScenarioAction};

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let ScenarioAction::DescribeProducers(action) = action else {
        return None;
    };
    Some((
        AdapterCommand::DescribeProducers(DescribeProducersCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
            partition: action.partition,
            broker_id: action.broker_id,
            timeout_ms: action.timeout_ms,
        }),
        ExpectedEvent::ProducerStatesDescribed {
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
            partition: action.partition,
        },
    ))
}
