use super::server_command::BackgroundServerCommand;
use crate::configs::server::ServerConfig;
use crate::streaming::systems::system::SharedSystem;

pub struct BackgroundServerCommandHandler<'a> {
    system: SharedSystem,
    config: &'a ServerConfig,
}

impl<'a> BackgroundServerCommandHandler<'a> {
    pub fn new(system: SharedSystem, config: &'a ServerConfig) -> Self {
        Self { system, config }
    }

    pub fn install_handler<C, E>(&mut self, mut executor: E) -> Self
    where
        E: BackgroundServerCommand<C> + Send + Sync + 'static,
    {
        let (sender, receiver) = flume::unbounded();
        let system = self.system.clone();
        executor.start_command_sender(system.clone(), self.config, sender);
        executor.start_command_consumer(system.clone(), self.config, receiver);
        Self {
            system,
            config: self.config,
        }
    }
}
