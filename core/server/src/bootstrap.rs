use iggy_common::{
    IggyError,
    defaults::{
        DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH,
        MIN_PASSWORD_LENGTH, MIN_USERNAME_LENGTH,
    },
};
use monoio::{Buildable, Driver, Runtime, fs::create_dir_all, time::TimeDriver};
use tracing::info;

use crate::{
    IGGY_ROOT_PASSWORD_ENV, IGGY_ROOT_USERNAME_ENV,
    configs::{config_provider::ConfigProviderKind, server::ServerConfig, system::SystemConfig},
    server_error::ServerError,
    shard::transmission::{connector::ShardConnector, frame::ShardFrame},
    streaming::{
        persistence::persister::{FilePersister, FileWithSyncPersister, PersisterKind},
        users::user::User,
    },
};
use std::{env, fs::remove_dir_all, ops::Range, path::Path, sync::Arc};

pub fn create_shard_connections(shards_set: Range<usize>) -> Vec<ShardConnector<ShardFrame>> {
    let shards_count = shards_set.len();
    let connectors = shards_set
        .into_iter()
        .map(|id| ShardConnector::new(id as u16, shards_count))
        .collect();

    connectors
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

    let streams_path = config.get_streams_path();
    if !Path::new(&streams_path).exists() && create_dir_all(&streams_path).await.is_err() {
        return Err(IggyError::CannotCreateStreamsDirectory(streams_path));
    }

    let runtime_path = config.get_runtime_path();
    // TODO: Change remove_dir_all to async version, once we implement the dir walk using monoio `remove_dir` method.
    if Path::new(&runtime_path).exists() && remove_dir_all(&runtime_path).is_err() {
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

    // TODO: Move this to individual shard level
    /*
     */
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

pub fn create_default_executor<D>() -> Runtime<D>
where
    D: Driver + Buildable,
{
    let builder = monoio::RuntimeBuilder::<D>::new();
    let rt = Buildable::build(builder).expect("Failed to create default runtime");
    rt
}

pub fn create_shard_executor() -> Runtime<TimeDriver<monoio::IoUringDriver>> {
    // TODO: Figure out what else we could tweak there
    // We for sure want to disable the userspace interrupts on new cq entry (set_coop_taskrun)
    // TODO: Shall we make the size of ring be configureable ?
    let builder = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        //.uring_builder(urb.setup_coop_taskrun()) // broken shit.
        .with_entries(1024) // Default size
        .enable_timer();
    let rt = Buildable::build(builder).expect("Failed to create default runtime");
    rt
}

pub fn resolve_persister(enforce_fsync: bool) -> Arc<PersisterKind> {
    match enforce_fsync {
        true => Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister)),
        false => Arc::new(PersisterKind::File(FilePersister)),
    }
}
