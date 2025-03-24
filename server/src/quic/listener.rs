use crate::binary::command::{ServerCommand, ServerCommandHandler};
use crate::binary::sender::SenderKind;
use crate::server_error::ConnectionError;
use crate::streaming::clients::client_manager::Transport;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::anyhow;
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use tracing::{error, info, trace};

const LISTENERS_COUNT: u32 = 10;
const INITIAL_BYTES_LENGTH: usize = 4;

pub fn start(endpoint: Endpoint, system: SharedSystem) {
    for _ in 0..LISTENERS_COUNT {
        let endpoint = endpoint.clone();
        let system = system.clone();
        tokio::spawn(async move {
            while let Some(incoming_connection) = endpoint.accept().await {
                info!(
                    "Incoming connection from client: {}",
                    incoming_connection.remote_address()
                );
                let system = system.clone();
                let incoming_connection = incoming_connection.accept();
                if incoming_connection.is_err() {
                    error!(
                        "Error when accepting incoming connection: {:?}",
                        incoming_connection
                    );
                    continue;
                }
                let incoming_connection = incoming_connection.unwrap();
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(incoming_connection, system).await {
                        error!("Connection has failed: {error}");
                    }
                });
            }
        });
    }
}

async fn handle_connection(
    incoming_connection: quinn::Connecting,
    system: SharedSystem,
) -> Result<(), ConnectionError> {
    let connection = incoming_connection.await?;
    let address = connection.remote_address();
    info!("Client has connected: {address}");
    let session = system
        .read()
        .await
        .add_client(&address, Transport::Quic)
        .await;

    let client_id = session.client_id;
    while let Some(stream) = accept_stream(&connection, &system, client_id).await? {
        let system = system.clone();
        let session = session.clone();

        let handle_stream_task = async move {
            if let Err(err) = handle_stream(stream, system, session).await {
                error!("Error when handling QUIC stream: {:?}", err)
            }
        };
        let _handle = tokio::spawn(handle_stream_task);
    }
    Ok(())
}

type BiStream = (SendStream, RecvStream);

async fn accept_stream(
    connection: &Connection,
    system: &SharedSystem,
    client_id: u32,
) -> Result<Option<BiStream>, ConnectionError> {
    match connection.accept_bi().await {
        Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
            info!("Connection closed");
            system.read().await.delete_client(client_id).await;
            Ok(None)
        }
        Err(error) => {
            error!("Error when handling QUIC stream: {:?}", error);
            system.read().await.delete_client(client_id).await;
            Err(error.into())
        }
        Ok(stream) => Ok(Some(stream)),
    }
}

async fn handle_stream(
    stream: BiStream,
    system: SharedSystem,
    session: impl AsRef<Session>,
) -> anyhow::Result<()> {
    let (send_stream, mut recv_stream) = stream;

    let mut length_buffer = [0u8; INITIAL_BYTES_LENGTH];
    let mut code_buffer = [0u8; INITIAL_BYTES_LENGTH];

    recv_stream.read_exact(&mut length_buffer).await?;
    recv_stream.read_exact(&mut code_buffer).await?;

    let length = u32::from_le_bytes(length_buffer);
    let code = u32::from_le_bytes(code_buffer);

    trace!("Received a QUIC request, length: {length}, code: {code}");

    let mut sender = SenderKind::get_quic_sender(send_stream, recv_stream);

    let command = match ServerCommand::from_code_and_reader(code, &mut sender, length - 4).await {
        Ok(cmd) => cmd,
        Err(e) => {
            sender.send_error_response(e.clone()).await?;
            return Err(anyhow!("Failed to parse command: {e}"));
        }
    };

    // if let Err(e) = command.validate() {
    //     sender.send_error_response(e.clone()).await?;
    //     return Err(anyhow!("Command validation failed: {e}"));
    // }

    trace!("Received a QUIC command: {command}, payload size: {length}");

    match command
        .handle(&mut sender, length, session.as_ref(), &system)
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow!("Error handling command: {e}")),
    }
}
