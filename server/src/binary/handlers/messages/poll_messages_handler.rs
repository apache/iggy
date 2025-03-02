use std::io::IoSlice;

use crate::binary::handlers::messages::COMPONENT;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::messages::PollingArgs;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::messages::poll_messages::PollMessages;
use iggy::models::batch::IGGY_BATCH_OVERHEAD;
use tracing::debug;

pub async fn handle(
    command: PollMessages,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    let result = system
        .poll_messages(
            session,
            &command.consumer,
            &command.stream_id,
            &command.topic_id,
            command.partition_id,
            PollingArgs::new(command.strategy, command.count, command.auto_commit),
        )
        .await
        .with_error_context(|error| format!(
            "{COMPONENT} (error: {error}) - failed to poll messages for consumer: {}, stream_id: {}, topic_id: {}, partition_id: {:?}, session: {}.",
            command.consumer, command.stream_id, command.topic_id, command.partition_id, session
        ))?;
    let length = result
        .slices
        .iter()
        .map(|s| s.range.len() as u32)
        .sum::<u32>()
        + IGGY_BATCH_OVERHEAD as u32;
    let length = length.to_le_bytes();
    // Adding 1 for the header and 2 for the prefix required by `send_ok_response`.
    let mut slices = Vec::with_capacity(1 + result.slices.len() + 2);
    let header = result.header.as_bytes();
    slices.push(IoSlice::new(&header));
    for slice in result.slices.iter() {
        // Ergh... it is kinda ugly that we have to index this way, rather than just passing `Range`,
        // but borrow checker gets really anal about it, since the range is field of `IggyBatchSlice`
        // that we borrow inside of that loop.
        // It is fixable by decoupling `Range` from `Bytes`, but w/e.
        let slice = &slice.bytes[slice.range.start..slice.range.end];
        slices.push(IoSlice::new(slice));
    }

    sender.send_ok_response_vectored(&length, slices).await?;
    Ok(())
}
