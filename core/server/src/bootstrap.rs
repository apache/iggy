use compio::{fs::create_dir_all, runtime::Runtime};
use iggy_common::{
    IggyError,
    defaults::{
        DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH,
        MIN_PASSWORD_LENGTH, MIN_USERNAME_LENGTH,
    },
};
use tracing::info;

use crate::{
    IGGY_ROOT_PASSWORD_ENV, IGGY_ROOT_USERNAME_ENV,
    configs::{config_provider::ConfigProviderKind, server::ServerConfig, system::SystemConfig},
    io::fs_utils,
    server_error::ServerError,
    shard::transmission::{
        connector::{ShardConnector, StopSender},
        frame::ShardFrame,
    },
    streaming::{
        persistence::persister::{FilePersister, FileWithSyncPersister, PersisterKind},
        users::user::User,
        utils::file::overwrite,
    },
};
use std::{env, ops::Range, path::Path, sync::Arc};

pub fn create_shard_connections(
    shards_set: Range<usize>,
) -> (Vec<ShardConnector<ShardFrame>>, Vec<(u16, StopSender)>) {
    let shards_count = shards_set.len();
    let connectors: Vec<ShardConnector<ShardFrame>> = shards_set
        .into_iter()
        .map(|id| ShardConnector::new(id as u16, shards_count))
        .collect();

    let shutdown_handles = connectors
        .iter()
        .map(|conn| (conn.id, conn.stop_sender.clone()))
        .collect();

    (connectors, shutdown_handles)
}

pub async fn load_config(
    config_provider: &ConfigProviderKind,
) -> Result<ServerConfig, ServerError> {
    let config = ServerConfig::load(config_provider).await?;
    Ok(config)
}

pub async fn create_directories(config: &SystemConfig) -> Result<(), IggyError> {
    let system_path = config.get_system_path();
    if !Path::new(&system_path).exists() && create_dir_all(&system_path).await.is_err() {
        return Err(IggyError::CannotCreateBaseDirectory(system_path));
    }

    let state_path = config.get_state_path();
    if !Path::new(&state_path).exists() && create_dir_all(&state_path).await.is_err() {
        return Err(IggyError::CannotCreateStateDirectory(state_path));
    }
    let state_log = config.get_state_messages_file_path();
    if !Path::new(&state_log).exists() && (overwrite(&state_log).await).is_err() {
        return Err(IggyError::CannotCreateStateDirectory(state_log));
    }

    let streams_path = config.get_streams_path();
    if !Path::new(&streams_path).exists() && create_dir_all(&streams_path).await.is_err() {
        return Err(IggyError::CannotCreateStreamsDirectory(streams_path));
    }

    let runtime_path = config.get_runtime_path();
    if Path::new(&runtime_path).exists() && fs_utils::remove_dir_all(&runtime_path).await.is_err() {
        return Err(IggyError::CannotRemoveRuntimeDirectory(runtime_path));
    }

    if create_dir_all(&runtime_path).await.is_err() {
        return Err(IggyError::CannotCreateRuntimeDirectory(runtime_path));
    }

    info!(
        "Initializing system, data will be stored at: {}",
        config.get_system_path()
    );
    Ok(())
}

pub fn create_root_user() -> User {
    info!("Creating root user...");
    let username = env::var(IGGY_ROOT_USERNAME_ENV);
    let password = env::var(IGGY_ROOT_PASSWORD_ENV);
    if (username.is_ok() && password.is_err()) || (username.is_err() && password.is_ok()) {
        panic!(
            "When providing the custom root user credentials, both username and password must be set."
        );
    }
    if username.is_ok() && password.is_ok() {
        info!("Using the custom root user credentials.");
    } else {
        info!("Using the default root user credentials.");
    }

    let username = username.unwrap_or(DEFAULT_ROOT_USERNAME.to_string());
    let password = password.unwrap_or(DEFAULT_ROOT_PASSWORD.to_string());
    if username.is_empty() || password.is_empty() {
        panic!("Root user credentials are not set.");
    }
    if username.len() < MIN_USERNAME_LENGTH {
        panic!("Root username is too short.");
    }
    if username.len() > MAX_USERNAME_LENGTH {
        panic!("Root username is too long.");
    }
    if password.len() < MIN_PASSWORD_LENGTH {
        panic!("Root password is too short.");
    }
    if password.len() > MAX_PASSWORD_LENGTH {
        panic!("Root password is too long.");
    }
    let user = User::root(&username, &password);
    user
}

pub fn create_shard_executor() -> Runtime {
    // TODO: The event intererval tick, could be configured based on the fact
    // How many clients we expect to have connected.
    // This roughly estimates the number of tasks we will create.

    let mut proactor = compio::driver::ProactorBuilder::new();

    proactor
        .capacity(4096)
        .coop_taskrun(true)
        .taskrun_flag(false); // TODO: Try enabling this.

    // FIXME(hubcio): Only set thread_pool_limit(0) on non-macOS platforms
    // This causes a freeze on macOS with compio fs operations
    // see https://github.com/compio-rs/compio/issues/446
    #[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
    proactor.thread_pool_limit(0);

    compio::runtime::RuntimeBuilder::new()
        .with_proactor(proactor.to_owned())
        .event_interval(69)
        .build()
        .unwrap()
}

pub fn resolve_persister(enforce_fsync: bool) -> Arc<PersisterKind> {
    match enforce_fsync {
        true => Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister)),
        false => Arc::new(PersisterKind::File(FilePersister)),
    }
}
