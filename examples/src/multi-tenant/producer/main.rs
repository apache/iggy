use clap::Parser;
use futures_util::future::join_all;
use iggy::client::{AutoSignIn, Client, Credentials, StreamClient, UserClient};
use iggy::clients::client::IggyClient;
use iggy::clients::producer::IggyProducer;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Message, Partitioning};
use iggy::models::permissions::{Permissions, StreamPermissions};
use iggy::models::user_status::UserStatus;
use iggy::tcp::client::TcpClient;
use iggy::users::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use iggy::utils::duration::IggyDuration;
use iggy_examples::shared::args::Args;
use std::collections::HashMap;
use std::error::Error;
use std::str::FromStr;
use tokio::task::JoinHandle;
use tracing::{error, info};

const TENANT1_STREAM: &str = "tenant_1";
const TENANT2_STREAM: &str = "tenant_2";
const TENANT3_STREAM: &str = "tenant_3";
const TENANT1_USER: &str = "tenant_1_producer";
const TENANT2_USER: &str = "tenant_2_producer";
const TENANT3_USER: &str = "tenant_3_producer";
const PASSWORD: &str = "secret";
const TOPIC: &str = "logs";
const PRODUCERS_COUNT: usize = 3;
const PARTITIONS_COUNT: u32 = 3;
const MESSAGES_COUNT: usize = 10;

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    print_info("Multi-tenant producer has started");
    let address = args.tcp_server_address;

    print_info("Creating root client to manage streams and users");
    let root_client = create_client(&address, DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD).await?;

    print_info("Creating streams and users with permissions for each tenant");
    create_stream_and_user(TENANT1_STREAM, TENANT1_USER, &root_client).await?;
    create_stream_and_user(TENANT2_STREAM, TENANT2_USER, &root_client).await?;
    create_stream_and_user(TENANT3_STREAM, TENANT3_USER, &root_client).await?;

    print_info("Disconnecting root client");
    root_client.disconnect().await?;

    print_info("Creating clients for each tenant");
    let tenant1_client = create_client(&address, TENANT1_USER, PASSWORD).await?;
    let tenant2_client = create_client(&address, TENANT2_USER, PASSWORD).await?;
    let tenant3_client = create_client(&address, TENANT3_USER, PASSWORD).await?;

    print_info("Ensuring access to streams for each tenant");
    ensure_stream_access(
        &tenant1_client,
        TENANT1_STREAM,
        &[TENANT2_STREAM, TENANT3_STREAM],
    )
    .await?;
    ensure_stream_access(
        &tenant2_client,
        TENANT2_STREAM,
        &[TENANT1_STREAM, TENANT3_STREAM],
    )
    .await?;
    ensure_stream_access(
        &tenant3_client,
        TENANT3_STREAM,
        &[TENANT1_STREAM, TENANT2_STREAM],
    )
    .await?;

    print_info("Creating {PRODUCERS_COUNT} producers for each tenant");
    let producers1 = create_producers(&tenant1_client, TENANT1_STREAM, TOPIC).await?;
    let producers2 = create_producers(&tenant2_client, TENANT2_STREAM, TOPIC).await?;
    let producers3 = create_producers(&tenant3_client, TENANT3_STREAM, TOPIC).await?;

    print_info("Starting producers for each tenant");
    let producers1_tasks = start_producers(producers1);
    let producers2_tasks = start_producers(producers2);
    let producers3_tasks = start_producers(producers3);

    let mut tasks = Vec::new();
    tasks.extend(producers1_tasks);
    tasks.extend(producers2_tasks);
    tasks.extend(producers3_tasks);

    join_all(tasks).await;

    print_info("Disconnecting clients");

    Ok(())
}

fn start_producers(producers: Vec<IggyProducer>) -> Vec<JoinHandle<()>> {
    let mut tasks = Vec::new();
    let mut producer_id = 1;
    for producer in producers {
        let task = tokio::spawn(async move {
            let mut counter = 1;
            while counter <= MESSAGES_COUNT {
                let payload = format!("message-{producer_id}-{counter}");
                let message = Message::from_str(&payload).expect("Invalid message");
                if let Err(error) = producer.send(vec![message]).await {
                    error!(
                        "Failed to send: '{payload}' to: {} -> {} with error: {error}",
                        producer.stream(),
                        producer.topic(),
                        error = error
                    );
                    continue;
                }

                counter += 1;
                info!(
                    "Sent: '{payload}' to: {} -> {}",
                    producer.stream(),
                    producer.topic()
                );
            }
        });
        producer_id += 1;
        tasks.push(task);
    }
    tasks
}

async fn create_producers(
    client: &IggyClient,
    stream: &str,
    topic: &str,
) -> Result<Vec<IggyProducer>, IggyError> {
    let mut producers = Vec::new();
    for _ in 0..PRODUCERS_COUNT {
        let mut producer = client
            .producer(stream, topic)?
            .batch_size(10)
            .send_interval(IggyDuration::from_str("10ms").expect("Invalid duration"))
            .partitioning(Partitioning::balanced())
            .create_topic_if_not_exists(PARTITIONS_COUNT, None)
            .build();
        producer.init().await?;
        producers.push(producer);
    }
    Ok(producers)
}

async fn ensure_stream_access(
    client: &IggyClient,
    available_stream: &str,
    unavailable_streams: &[&str],
) -> Result<(), IggyError> {
    client
        .get_stream(&available_stream.try_into()?)
        .await
        .unwrap_or_else(|_| panic!("No access to stream: {available_stream}"));
    info!("Ensured access to stream: {available_stream}");
    for stream in unavailable_streams {
        if client
            .get_stream(&Identifier::named(stream)?)
            .await
            .is_err()
        {
            info!("Ensured no access to stream: {stream}");
        } else {
            panic!("Access to stream: {stream} should not be allowed");
        }
    }
    Ok(())
}

async fn create_client(
    address: &str,
    username: &str,
    password: &str,
) -> Result<IggyClient, IggyError> {
    let tcp_client = TcpClient::new(
        address,
        AutoSignIn::Enabled(Credentials::UsernamePassword(
            username.to_owned(),
            password.to_owned(),
        )),
    )?;
    let client = IggyClient::builder()
        .with_client(Box::new(tcp_client))
        .build()?;
    client.connect().await?;
    Ok(client)
}

async fn create_stream_and_user(
    stream_name: &str,
    username: &str,
    client: &IggyClient,
) -> Result<(), IggyError> {
    let stream = client.create_stream(stream_name, None).await?;
    info!("Created stream: {stream_name} with ID: {}", stream.id);
    let mut streams_permissions = HashMap::new();
    streams_permissions.insert(
        stream.id,
        StreamPermissions {
            read_stream: true,
            manage_topics: true,
            ..Default::default()
        },
    );
    let permissions = Permissions {
        streams: Some(streams_permissions),
        ..Default::default()
    };
    let user = client
        .create_user(username, PASSWORD, UserStatus::Active, Some(permissions))
        .await?;
    info!(
        "Created user: {username} with ID: {}, with permissions for stream: {stream_name}",
        user.id
    );
    Ok(())
}

fn print_info(message: &str) {
    info!("\n\n--- {message} ---\n");
}
