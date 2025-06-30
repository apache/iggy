use std::str::FromStr;

use crate::atom;
use iggy::client::{Client, MessageClient, StreamClient, SystemClient, TopicClient, UserClient};
use iggy::clients::client::IggyClient;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Message as RustMessage, Partitioning, SendMessages};
use iggy::streams::create_stream::CreateStream;
use iggy::system::ping::Ping;
use iggy::topics::create_topic::CreateTopic;
use iggy::users::login_user::LoginUser;
use lazy_static::lazy_static;
use rustler::{Encoder, Error as RustlerError, ListIterator};
use rustler::{Env, Term};
use tokio::runtime::{Builder, Runtime};

lazy_static! {
    static ref IGGY_CLIENT: IggyResource = IggyResource::new();
}

pub struct IggyResource {
    pub inner: IggyClient,
    pub runtime: Runtime,
}

impl IggyResource {
    fn new() -> Self {
        let runtime = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        IggyResource {
            inner: IggyClient::default(),
            runtime,
        }
    }
}

#[rustler::nif]
fn connect(env: Env) -> Result<Term, RustlerError> {
    let resource = &IGGY_CLIENT;

    let connect_future = resource.inner.connect();
    let result = resource.runtime.block_on(connect_future);

    match result {
        Ok(_) => Ok(atom::ok().encode(env)),
        Err(err) => Err(RustlerError::Term(Box::new(err.to_string()))),
    }
}

//#[rustler::nif(schedule = "DirtyCpu")]
#[rustler::nif]
fn ping(env: Env) -> Result<Term, RustlerError> {
    let resource = &IGGY_CLIENT;

    let ping_command = Ping {};
    let ping_future = resource.inner.ping(&ping_command);
    let result = resource.runtime.block_on(ping_future);

    match result {
        Ok(_) => Ok(atom::ok().encode(env)),
        Err(err) => Err(RustlerError::Term(Box::new(err.to_string()))),
    }
}

#[rustler::nif]
fn login_user(env: Env, username: String, password: String) -> Result<Term, RustlerError> {
    let resource = &IGGY_CLIENT;

    let login_user = LoginUser { username, password };
    let login_user_future = resource.inner.login_user(&login_user);
    let result = resource.runtime.block_on(login_user_future);

    match result {
        Ok(_) => Ok(atom::ok().encode(env)),
        Err(err) => Err(RustlerError::Term(Box::new(err.to_string()))),
    }
}

#[rustler::nif]
fn create_stream(env: Env, stream_id: u32, name: String) -> Result<Term, RustlerError> {
    let resource = &IGGY_CLIENT;
    let create_stream = CreateStream {
        stream_id: Some(stream_id),
        name,
    };
    let create_stream_future = resource.inner.create_stream(&create_stream);

    match resource.runtime.block_on(create_stream_future) {
        Ok(_) => Ok(atom::ok().encode(env)),
        Err(e) => Err(RustlerError::Term(Box::new(e.to_string()))),
    }
}

#[rustler::nif]
fn create_topic(
    env: Env,
    stream_id: u32,
    topic_id: u32,
    partitions_count: u32,
    name: String,
) -> Result<Term, RustlerError> {
    let resource = &IGGY_CLIENT;
    let stream_identifier = match Identifier::numeric(stream_id).map_err(|e| e) {
        Ok(identifier) => identifier,
        Err(_e) => return Err(RustlerError::Term(Box::new("Invalid stream identifier"))),
    };

    let create_topic = CreateTopic {
        stream_id: stream_identifier,
        topic_id: Some(topic_id),
        name,
        partitions_count,
        max_topic_size: None,
        message_expiry: None,
        replication_factor: 1,
    };
    let create_topic_future = resource.inner.create_topic(&create_topic);

    match resource.runtime.block_on(create_topic_future) {
        Ok(_) => Ok(atom::ok().encode(env)),
        Err(e) => Err(RustlerError::Term(Box::new(e.to_string()))),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn send_message(
    env: Env,
    stream_id: u32,
    topic_id: u32,
    partitioning: u32,
    message: String,
) -> Result<Term, RustlerError> {
    let resource = &IGGY_CLIENT;
    let mut messages = Vec::new();
    let message = RustMessage::from_str(&message).unwrap();
    messages.push(message);
    let mut msgs = SendMessages {
        stream_id: Identifier::numeric(stream_id).unwrap(),
        topic_id: Identifier::numeric(topic_id).unwrap(),
        partitioning: Partitioning::partition_id(partitioning),
        messages,
    };

    let send_message_future = resource.inner.send_messages(&mut msgs);
    match resource.runtime.block_on(send_message_future) {
        Ok(_) => Ok(atom::ok().encode(env)),
        Err(e) => Err(RustlerError::Term(Box::new(e.to_string()))),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn send_messages<'a>(
    env: Env<'a>,
    stream_id: u32,
    topic_id: u32,
    partitioning: u32,
    messages: ListIterator<'a>,
) -> Result<Term<'a>, RustlerError> {
    let resource = &IGGY_CLIENT;
    let messages: Vec<RustMessage> = messages
        .into_iter()
        .map(|message| RustMessage::from_str(&message.decode::<String>().unwrap()).unwrap())
        .collect();

    let mut messages = SendMessages {
        stream_id: Identifier::numeric(stream_id).unwrap(),
        topic_id: Identifier::numeric(topic_id).unwrap(),
        partitioning: Partitioning::partition_id(partitioning),
        messages,
    };

    let send_messages_future = resource.inner.send_messages(&mut messages);
    match resource.runtime.block_on(send_messages_future) {
        Ok(_) => Ok(atom::ok().encode(env)),
        Err(e) => Err(RustlerError::Term(Box::new(e.to_string()))),
    }
}

// #[rustler::nif(schedule = "DirtyIo")]
// fn poll_messages(
//     env: Env,
//     stream_id: u32,
//     topic_id: u32,
//     partition_id: u32,
//     count: u32,
//     auto_commit: bool,
// ) -> Result<ListIterator, RustlerError> {
// }

//     /// Polls for messages from the specified topic and partition.
//     ///
//     /// Returns a list of received messages or a PyRuntimeError on failure.
//     fn poll_messages(
//         &self,
//         stream_id: u32,
//         topic_id: u32,
//         partition_id: u32,
//         count: u32,
//         auto_commit: bool,
//     ) -> PyResult<Vec<ReceiveMessage>> {
//         let poll_message_cmd = PollMessages {
//             consumer: RustConsumer::default(),
//             stream_id: Identifier::numeric(stream_id).unwrap(),
//             topic_id: Identifier::numeric(topic_id).unwrap(),
//             partition_id: Some(partition_id),
//             strategy: PollingStrategy::next(),
//             count,
//             auto_commit,
//         };
//         let poll_messages = self.inner.poll_messages(&poll_message_cmd);

//         let polled_messages = self
//             .runtime
//             .block_on(async move { poll_messages.await })
//             .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;

//         let messages = polled_messages
//             .messages
//             .into_iter()
//             .map(|message| ReceiveMessage::from_rust_message(message))
//             .collect::<Vec<_>>();
//         PyResult::Ok(messages)
//     }
// }
// }
