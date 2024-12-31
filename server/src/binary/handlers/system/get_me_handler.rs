use crate::binary::handlers::system::COMPONENT;
use crate::binary::mapper;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::locking::IggySharedMutFn;
use iggy::system::get_me::GetMe;
use tracing::debug;

pub async fn handle(
    command: GetMe,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let bytes;
    {
        let system = system.read().await;
        let client = system
            .get_client(session, session.client_id)
            .await
            .with_error_context(|_| {
                format!("{COMPONENT} - failed to get client, session: {session}")
            })?;
        {
            let client = client.read().await;
            bytes = mapper::map_client(&client);
        }
    }
    sender.send_ok_response(&bytes).await?;
    Ok(())
}
