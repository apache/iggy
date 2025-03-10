use crate::binary::command::{BinaryServerCommand, ServerCommandHandler};
use crate::binary::sender::SenderKind;
use crate::streaming::segments::IggyMessagesMut;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use bytes::{Buf, BytesMut};
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::prelude::*;
use iggy::utils::sizeable::Sizeable;
use tracing::instrument;

impl ServerCommandHandler for SendMessages {
    fn code(&self) -> u32 {
        iggy::command::SEND_MESSAGES_CODE
    }

    #[instrument(skip_all, name = "trace_send_messages", fields(
        iggy_user_id = session.get_user_id(),
        iggy_client_id = session.client_id,
        iggy_stream_id = self.stream_id.as_string(),
        iggy_topic_id = self.topic_id.as_string(),
        partitioning = %self.partitioning
    ))]
    async fn handle(
        mut self,
        sender: &mut SenderKind,
        length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        let length = length - 4;
        let mut buffer = BytesMut::with_capacity(length as usize);
        unsafe {
            buffer.set_len(length as usize);
        }
        sender.read(&mut buffer).await?;

        let mut element_size;

        let stream_id = Identifier::from_raw_bytes(buffer.chunk())?;
        element_size = stream_id.get_size_bytes().as_bytes_usize();
        buffer.advance(element_size);

        let topic_id = Identifier::from_raw_bytes(buffer.chunk())?;
        element_size = topic_id.get_size_bytes().as_bytes_usize();
        buffer.advance(element_size);

        let partitioning = Partitioning::from_raw_bytes(buffer.chunk())?;
        element_size = partitioning.get_size_bytes().as_bytes_usize();
        buffer.advance(element_size);

        let messages_count = buffer.get_u32_le();
        let messages = buffer.split();
        let mut messages = IggyMessagesMut::new(messages);
        messages.set_count(messages_count);

        let system = system.read().await;
        system
            .append_messages(
                session,
                &stream_id,
                &topic_id,
                &partitioning,
                messages,
                None,
            )
            .await?;
        drop(system);

        self.stream_id = stream_id;
        self.topic_id = topic_id;
        self.partitioning = partitioning;

        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for SendMessages {
    async fn from_sender(
        _sender: &mut SenderKind,
        _code: u32,
        _length: u32,
    ) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        Ok(Self::default())
    }
}
