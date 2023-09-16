use crate::cli::CliCommand;

use async_trait::async_trait;
use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::streams::update_stream::UpdateStream;

#[derive(Debug)]
pub(crate) struct StreamUpdate {
    id: u32,
    name: String,
}

impl StreamUpdate {
    pub(crate) fn new(id: u32, name: String) -> Self {
        Self { id, name }
    }
}

#[async_trait]
impl CliCommand for StreamUpdate {
    fn explain(&self) -> String {
        format!("update stream with id: {} and name: {}", self.id, self.name)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) {
        match client
            .update_stream(&UpdateStream {
                stream_id: Identifier::numeric(self.id).expect("Expected numeric identifier"),
                name: self.name.clone(),
            })
            .await
        {
            Ok(_) => {
                println!("Stream with id: {} name: {} updated", self.id, self.name);
            }
            Err(err) => {
                println!(
                    "Problem creating stream (id: {} and name: {}): {err}",
                    self.id, self.name
                );
            }
        }
    }
}
