#[macro_export]
macro_rules! shard_info {
    ($shard_id:expr, $fmt:literal) => {
        ::tracing::info!("┌─ Shard {} ─┐ {}", $shard_id, $fmt)
    };
    ($shard_id:expr, $fmt:literal, $($arg:expr),*) => {
        ::tracing::info!("┌─ Shard {} ─┐ {}", $shard_id, format!($fmt, $($arg),*))
    };
}

#[macro_export]
macro_rules! shard_debug {
    ($shard_id:expr, $fmt:literal) => {
        ::tracing::debug!("┌─ Shard {} ─┐ {}", $shard_id, $fmt)
    };
    ($shard_id:expr, $fmt:literal, $($arg:expr),*) => {
        ::tracing::debug!("┌─ Shard {} ─┐ {}", $shard_id, format!($fmt, $($arg),*))
    };
}

#[macro_export]
macro_rules! shard_trace {
    ($shard_id:expr, $fmt:literal) => {
        ::tracing::trace!("┌─ Shard {} ─┐ {}", $shard_id, $fmt)
    };
    ($shard_id:expr, $fmt:literal, $($arg:expr),*) => {
        ::tracing::trace!("┌─ Shard {} ─┐ {}", $shard_id, format!($fmt, $($arg),*))
    };
}

#[macro_export]
macro_rules! shard_warn {
    ($shard_id:expr, $fmt:literal) => {
        ::tracing::warn!("┌─ Shard {} ─┐ {}", $shard_id, $fmt)
    };
    ($shard_id:expr, $fmt:literal, $($arg:expr),*) => {
        ::tracing::warn!("┌─ Shard {} ─┐ {}", $shard_id, format!($fmt, $($arg),*))
    };
}

#[macro_export]
macro_rules! shard_error {
    ($shard_id:expr, $fmt:literal) => {
        ::tracing::error!("┌─ Shard {} ─┐ {}", $shard_id, $fmt)
    };
    ($shard_id:expr, $fmt:literal, $($arg:expr),*) => {
        ::tracing::error!("┌─ Shard {} ─┐ {}", $shard_id, format!($fmt, $($arg),*))
    };
}
