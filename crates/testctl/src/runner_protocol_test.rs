//! Protocol expectation tests distinguish public client failure from invalidity.

use testlab_schema::{AdapterEvent, ProducerId};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn public_client_failure_completes_the_correlated_command() {
    let producer =
        ProducerId::new("producer-1").unwrap_or_else(|error| panic!("producer id: {error}"));
    let disposition = ExpectedEvent::FlushCompleted(producer)
        .classify(&AdapterEvent::CommandFailed {
            code: "backpressure".to_owned(),
            diagnostic: "flush contended".to_owned(),
        })
        .unwrap_or_else(|error| panic!("classify client failure: {error}"));

    assert_eq!(disposition, EventDisposition::Complete);
}
