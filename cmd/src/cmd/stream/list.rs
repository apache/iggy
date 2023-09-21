use crate::args::common::ListMode;
use crate::cli::CliCommand;

use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use comfy_table::Table;
use iggy::client::Client;
use iggy::streams::get_streams::GetStreams;
use iggy::utils::timestamp::TimeStamp;
use tracing::info;

#[derive(Debug)]
pub(crate) struct StreamList {
    mode: ListMode,
}

impl StreamList {
    pub(crate) fn new(mode: ListMode) -> Self {
        Self { mode }
    }
}

#[async_trait]
impl CliCommand for StreamList {
    fn explain(&self) -> String {
        let mode = match self.mode {
            ListMode::Table => "table",
            ListMode::List => "list",
        };
        format!("list streams in {mode} mode")
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error> {
        let streams = client
            .get_streams(&GetStreams {})
            .await
            .with_context(|| String::from("Problem getting list of streams"))?;

        if streams.is_empty() {
            info!("No streams found!");
            return Ok(());
        }

        match self.mode {
            ListMode::Table => {
                let mut table = Table::new();

                table.set_header(vec![
                    "ID", "Created", "Name", "Size (B)", "Messages", "Topics",
                ]);

                streams.iter().for_each(|stream| {
                    table.add_row(vec![
                        format!("{}", stream.id),
                        TimeStamp::from(stream.created_at).to_string("%Y-%m-%d %H:%M:%S"),
                        stream.name.clone(),
                        format!("{}", stream.size_bytes),
                        format!("{}", stream.messages_count),
                        format!("{}", stream.topics_count),
                    ]);
                });

                info!("{table}");
            }
            ListMode::List => {
                streams.iter().for_each(|stream| {
                    info!(
                        "{}|{}|{}|{}|{}|{}",
                        stream.id,
                        TimeStamp::from(stream.created_at).to_string("%Y-%m-%d %H:%M:%S"),
                        stream.name,
                        stream.size_bytes,
                        stream.messages_count,
                        stream.topics_count
                    );
                });
            }
        }

        Ok(())
    }
}
