//! Runtime-neutral polling for one selected public group-transition observer.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use testlab_schema::GroupConsumerEventMethod;

use crate::kafkars_api::{Consumer, ConsumerEvent, KafkaError};

pub(crate) fn take(
    consumer: &mut Consumer,
    method: GroupConsumerEventMethod,
) -> Result<Option<ConsumerEvent>, KafkaError> {
    match method {
        GroupConsumerEventMethod::TryTakeEvent => consumer.try_take_event(),
        GroupConsumerEventMethod::NextEvent => {
            let mut next = pin!(consumer.next_event());
            let mut context = Context::from_waker(Waker::noop());
            match next.as_mut().poll(&mut context) {
                Poll::Ready(result) => result,
                Poll::Pending => Ok(None),
            }
        }
    }
}
