use crate::{
    channels::server_command::BackgroundServerCommand, configs::server::ServerConfig,
    streaming::systems::system::SharedSystem,
};
use flume::{Receiver, Sender};
use human_repr::HumanCount;
use iggy::utils::duration::IggyDuration;
use tokio::time::{self};
use tracing::{error, info, warn};

#[derive(Debug, Default, Clone)]
pub struct SysInfoPrintCommand;

pub struct SysInfoPrinter {
    interval: IggyDuration,
    sender: Sender<SysInfoPrintCommand>,
}

pub struct SysInfoPrintExecutor;

impl SysInfoPrinter {
    pub fn new(interval: IggyDuration, sender: Sender<SysInfoPrintCommand>) -> Self {
        Self { interval, sender }
    }

    pub fn start(&self) {
        let interval = self.interval;
        let sender = self.sender.clone();
        if interval.is_zero() {
            info!("SysInfoPrinter is disabled.");
            return;
        }

        info!("SysInfoPrinter is enabled, system information will be printed every {interval}.");
        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval.get_duration());
            loop {
                interval_timer.tick().await;
                let command = SysInfoPrintCommand {};
                sender.send(command).unwrap_or_else(|e| {
                    error!("Failed to send SysInfoPrintCommand. Error: {e}");
                });
            }
        });
    }
}

impl BackgroundServerCommand<SysInfoPrintCommand> for SysInfoPrintExecutor {
    async fn execute(&mut self, system: &SharedSystem, _command: SysInfoPrintCommand) {
        let stats = match system.read().await.get_stats().await {
            Ok(stats) => stats,
            Err(e) => {
                error!("Failed to get system information. Error: {e}");
                return;
            }
        };

        let free_memory_percent = (stats.available_memory.as_bytes_u64() as f64
            / stats.total_memory.as_bytes_u64() as f64)
            * 100f64;

        let cache_hits = stats
            .cache_metrics
            .iter()
            .fold(0, |acc, (_, metrics)| acc + metrics.hits);
        let cache_misses = stats
            .cache_metrics
            .iter()
            .fold(0, |acc, (_, metrics)| acc + metrics.misses);
        let cache_ratio = cache_hits as f64 / (cache_hits + cache_misses) as f64;

        info!("CPU: {:.2}% / {:.2}% (IggyUsage/Total), Mem: {:.2}% / {} / {} / {} (Free/IggyUsage/TotalUsed/Total), Clients: {}, Messages processed: {}, Read: {}, Written: {}, Cache: {}/{}/{:.2} (Hits/Misses/Ratio), Uptime: {}",
              stats.cpu_usage,
              stats.total_cpu_usage,
              free_memory_percent,
              stats.memory_usage,
              stats.total_memory - stats.available_memory,
              stats.total_memory,
              stats.clients_count,
              stats.messages_count.human_count_bare(),
              stats.read_bytes,
              stats.written_bytes,
              cache_hits.human_count_bare(),
              cache_misses.human_count_bare(),
              cache_ratio,
              stats.run_time);
    }

    fn start_command_sender(
        &mut self,
        _system: SharedSystem,
        config: &ServerConfig,
        sender: Sender<SysInfoPrintCommand>,
    ) {
        let printer = SysInfoPrinter::new(config.system.logging.sysinfo_print_interval, sender);
        printer.start();
    }

    fn start_command_consumer(
        mut self,
        system: SharedSystem,
        _config: &ServerConfig,
        receiver: Receiver<SysInfoPrintCommand>,
    ) {
        tokio::spawn(async move {
            let system = system.clone();
            while let Ok(command) = receiver.recv_async().await {
                self.execute(&system, command).await;
            }
            warn!("Sysinfo printer stopped receiving commands.");
        });
    }
}
