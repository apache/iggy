use bytes::Bytes;
use iggy::prelude::IggyMessage;

mod common;
// mod consumer_offset;
mod get_by_offset;
// mod get_by_timestamp;
// mod messages;
// mod partition;
// mod segment;
// mod snapshot;
// mod stream;
// mod system;
// mod topic;
// mod topic_messages;

fn create_messages() -> Vec<IggyMessage> {
    vec![
        create_message(1, "message 1"),
        create_message(2, "message 2"),
        create_message(3, "message 3"),
        create_message(4, "message 3.2"),
        create_message(5, "message 1.2"),
        create_message(6, "message 3.3"),
    ]
}

fn create_message(id: u128, payload: &str) -> IggyMessage {
    let payload = Bytes::from(payload.to_string());
    IggyMessage::builder().id(id).payload(payload).build()
}
