use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use iggy::error::Error;
use iggy::identifier::Identifier;
use tokio::sync::RwLock;

impl System {
    pub fn get_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: u32,
    ) -> Result<&RwLock<ConsumerGroup>, Error> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner
            .get_consumer_group(session.user_id, stream.stream_id, topic.topic_id)?;

        topic.get_consumer_group(consumer_group_id)
    }

    pub fn get_consumer_groups(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<&RwLock<ConsumerGroup>>, Error> {
        self.ensure_authenticated(session)?;
        let stream = self.get_stream(stream_id)?;
        let topic = stream.get_topic(topic_id)?;
        self.permissioner
            .get_consumer_groups(session.user_id, stream.stream_id, topic.topic_id)?;

        Ok(topic.get_consumer_groups())
    }

    pub async fn create_consumer_group(
        &mut self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: u32,
    ) -> Result<(), Error> {
        self.ensure_authenticated(session)?;
        {
            let stream = self.get_stream(stream_id)?;
            let topic = stream.get_topic(topic_id)?;
            self.permissioner.create_consumer_group(
                session.user_id,
                stream.stream_id,
                topic.topic_id,
            )?;
        }

        let topic = self.get_stream_mut(stream_id)?.get_topic_mut(topic_id)?;
        topic.create_consumer_group(consumer_group_id).await?;
        Ok(())
    }

    pub async fn delete_consumer_group(
        &mut self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: u32,
    ) -> Result<(), Error> {
        self.ensure_authenticated(session)?;
        let stream_id_value;
        let topic_id_value;
        {
            let stream = self.get_stream(stream_id)?;
            let topic = stream.get_topic(topic_id)?;
            self.permissioner.delete_consumer_group(
                session.user_id,
                stream.stream_id,
                topic.topic_id,
            )?;
            stream_id_value = stream.stream_id;
            topic_id_value = topic.topic_id;
        }

        let consumer_group;
        {
            let stream = self.get_stream_mut(stream_id)?;
            let topic = stream.get_topic_mut(topic_id)?;
            consumer_group = topic.delete_consumer_group(consumer_group_id).await?;
        }

        let client_manager = self.client_manager.read().await;
        let consumer_group = consumer_group.read().await;
        for member in consumer_group.get_members() {
            let member = member.read().await;
            client_manager
                .leave_consumer_group(
                    member.id,
                    stream_id_value,
                    topic_id_value,
                    consumer_group_id,
                )
                .await?;
        }

        Ok(())
    }

    pub async fn join_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: u32,
    ) -> Result<(), Error> {
        self.ensure_authenticated(session)?;
        let stream_id_value;
        let topic_id_value;
        {
            let stream = self.get_stream(stream_id)?;
            let topic = stream.get_topic(topic_id)?;
            self.permissioner.join_consumer_group(
                session.user_id,
                stream.stream_id,
                topic.topic_id,
            )?;
            stream_id_value = stream.stream_id;
            topic_id_value = topic.topic_id;
        }

        {
            let stream = self.get_stream(stream_id)?;
            let topic = stream.get_topic(topic_id)?;
            topic
                .join_consumer_group(consumer_group_id, session.client_id)
                .await?;
        }

        let client_manager = self.client_manager.read().await;
        client_manager
            .join_consumer_group(
                session.client_id,
                stream_id_value,
                topic_id_value,
                consumer_group_id,
            )
            .await?;
        Ok(())
    }

    pub async fn leave_consumer_group(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: u32,
    ) -> Result<(), Error> {
        self.ensure_authenticated(session)?;
        {
            let stream = self.get_stream(stream_id)?;
            let topic = stream.get_topic(topic_id)?;
            self.permissioner.leave_consumer_group(
                session.user_id,
                stream.stream_id,
                topic.topic_id,
            )?;
        }

        self.leave_consumer_group_by_client(
            stream_id,
            topic_id,
            consumer_group_id,
            session.client_id,
        )
        .await
    }

    pub async fn leave_consumer_group_by_client(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer_group_id: u32,
        client_id: u32,
    ) -> Result<(), Error> {
        let stream_id_value;
        let topic_id_value;
        {
            let stream = self.get_stream(stream_id)?;
            let topic = stream.get_topic(topic_id)?;
            stream_id_value = stream.stream_id;
            topic_id_value = topic.topic_id;
            topic
                .leave_consumer_group(consumer_group_id, client_id)
                .await?;
        }

        let client_manager = self.client_manager.read().await;
        client_manager
            .leave_consumer_group(
                client_id,
                stream_id_value,
                topic_id_value,
                consumer_group_id,
            )
            .await
    }
}
