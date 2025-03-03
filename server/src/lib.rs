#[cfg(not(feature = "disable-mimalloc"))]
use mimalloc::MiMalloc;

#[cfg(not(feature = "disable-mimalloc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(windows)]
compile_error!("iggy-server doesn't support windows.");

pub mod archiver;
pub mod args;
pub mod binary;
pub mod channels;
pub(crate) mod compat;
pub mod configs;
pub mod http;
pub mod log;
pub mod quic;
pub mod server_error;
pub mod state;
pub mod streaming;
pub mod tcp;
pub mod versioning;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const IGGY_ROOT_USERNAME_ENV: &str = "IGGY_ROOT_USERNAME";
const IGGY_ROOT_PASSWORD_ENV: &str = "IGGY_ROOT_PASSWORD";

pub(crate) fn map_toggle_str<'a>(enabled: bool) -> &'a str {
    match enabled {
        true => "enabled",
        false => "disabled",
    }
}
