use crate::binary::command::ServerCommand;

#[derive(Debug)]
pub enum ShardMessage {
    Command(ServerCommand),
    Event(ShardEvent),
}

#[derive(Debug)]
pub enum ShardEvent {}

impl From<ServerCommand> for ShardMessage {
    fn from(command: ServerCommand) -> Self {
        ShardMessage::Command(command)
    }
}

impl From<ShardEvent> for ShardMessage {
    fn from(event: ShardEvent) -> Self {
        ShardMessage::Event(event)
    }
}
