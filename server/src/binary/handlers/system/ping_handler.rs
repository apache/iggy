use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::locking::IggySharedMutFn;
use iggy::system::ping::Ping;
use iggy::utils::timestamp::IggyTimestamp;
use tracing::debug;

pub async fn handle(
    command: Ping,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let client_manager = system.client_manager.read().await;
    if let Some(client) = client_manager.try_get_client(session.client_id) {
        let mut client = client.write().await;
        let now = IggyTimestamp::now();
        client.last_heartbeat = now;
        debug!("Updated last heartbeat to: {now} for session: {session}");
    }

    sender.send_empty_ok_response().await?;
    Ok(())
}
