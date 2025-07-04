use std::str::FromStr;

use crate::atom;
use iggy::client::{Client, MessageClient, StreamClient, SystemClient, TopicClient, UserClient};
use iggy::clients::client::IggyClient;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Message as RustMessage, Partitioning};
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

    let ping_future = resource.inner.ping();
    let result = resource.runtime.block_on(ping_future);

    match result {
        Ok(_) => Ok(atom::ok().encode(env)),
        Err(err) => Err(RustlerError::Term(Box::new(err.to_string()))),
    }
}

#[rustler::nif]
fn login_user(env: Env, username: String, password: String) -> Result<Term, RustlerError> {
    let resource = &IGGY_CLIENT;

    let login_user_future = resource.inner.login_user(&username, &password);
    let result = resource.runtime.block_on(login_user_future);

    match result {
        Ok(_) => Ok(atom::ok().encode(env)),
        Err(err) => Err(RustlerError::Term(Box::new(err.to_string()))),
    }
}

#[rustler::nif]
fn create_stream(env: Env, stream_id: u32, name: String) -> Result<Term, RustlerError> {
    let resource = &IGGY_CLIENT;
    let create_stream_future = resource.inner.create_stream(&name, Some(stream_id));

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
    let stream_identifier = Identifier::numeric(stream_id)
        .map_err(|_| RustlerError::Term(Box::new("Invalid stream identifier")))?;

    use iggy::compression::compression_algorithm::CompressionAlgorithm;
    use iggy::utils::expiry::IggyExpiry;
    use iggy::utils::topic_size::MaxTopicSize;

    let create_topic_future = resource.inner.create_topic(
        &stream_identifier,
        &name,
        partitions_count,
        CompressionAlgorithm::None,
        Some(1), // replication factor
        Some(topic_id),
        IggyExpiry::ServerDefault,
        MaxTopicSize::Unlimited,
    );

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
    let stream_identifier = Identifier::numeric(stream_id).unwrap();
    let topic_identifier = Identifier::numeric(topic_id).unwrap();
    let partitioning = Partitioning::partition_id(partitioning);

    let mut messages_vec = vec![RustMessage::from_str(&message).unwrap()];

    let send_message_future = resource.inner.send_messages(
        &stream_identifier,
        &topic_identifier,
        &partitioning,
        &mut messages_vec,
    );
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
    let stream_identifier = Identifier::numeric(stream_id).unwrap();
    let topic_identifier = Identifier::numeric(topic_id).unwrap();
    let partitioning = Partitioning::partition_id(partitioning);

    let mut messages_vec: Vec<RustMessage> = messages
        .into_iter()
        .map(|message| RustMessage::from_str(&message.decode::<String>().unwrap()).unwrap())
        .collect();

    let send_messages_future = resource.inner.send_messages(
        &stream_identifier,
        &topic_identifier,
        &partitioning,
        &mut messages_vec,
    );
    match resource.runtime.block_on(send_messages_future) {
        Ok(_) => Ok(atom::ok().encode(env)),
        Err(e) => Err(RustlerError::Term(Box::new(e.to_string()))),
    }
}

// TODO: implement poll_messages
// possibly see python client for reference
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
