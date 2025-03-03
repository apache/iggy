use crate::state::command::EntryCommand;
use crate::state::entry::StateEntry;
use iggy::error::IggyError;
#[cfg(test)]
use mockall::automock;
use std::fmt::Debug;
use std::future::Future;

pub mod command;
pub mod entry;
pub mod file;
pub mod models;
pub mod system;

pub const COMPONENT: &str = "STATE";

#[derive(Debug)]
pub enum StateKind {
    File(file::FileState),
    #[cfg(test)]
    Mock(MockState),
}

#[cfg_attr(test, automock)]
pub trait State: Send {
    fn init(&self) -> impl Future<Output = Result<Vec<StateEntry>, IggyError>> + Send;
    fn load_entries(&self) -> impl Future<Output = Result<Vec<StateEntry>, IggyError>> + Send;
    fn apply(
        &self,
        user_id: u32,
        command: &EntryCommand,
    ) -> impl Future<Output = Result<(), IggyError>> + Send;
}

impl StateKind {
    pub async fn init(&self) -> Result<Vec<StateEntry>, IggyError> {
        match self {
            Self::File(s) => s.init().await,
            #[cfg(test)]
            Self::Mock(s) => s.init().await,
        }
    }

    pub async fn load_entries(&self) -> Result<Vec<StateEntry>, IggyError> {
        match self {
            Self::File(s) => s.load_entries().await,
            #[cfg(test)]
            Self::Mock(s) => s.load_entries().await,
        }
    }

    pub async fn apply(&self, user_id: u32, command: &EntryCommand) -> Result<(), IggyError> {
        match self {
            Self::File(s) => s.apply(user_id, command).await,
            #[cfg(test)]
            Self::Mock(s) => s.apply(user_id, command).await,
        }
    }
}
