use iggy_common::IggyDuration;

#[derive(Debug, Clone, Default)]
pub enum SendMode {
    #[default]
    Sync,
    Background(BackgroundConfig),
}

#[derive(Debug, Clone)]
/// Determines how the `send_messages` API should behave when problem is encountered
pub enum BackpressureMode {
    /// Block until the send succeeds
    Block,
    /// Block with a timeout, after which the send fails
    BlockWithTimeout(IggyDuration),
    /// Fail immediately without retrying
    FailImmediately,
}

#[derive(Debug, Clone)]
pub struct BackgroundConfig {
    pub max_in_flight: usize,
    pub in_flight_timeout: Option<IggyDuration>,
    pub batch_size: Option<usize>,
    pub failure_mode: BackpressureMode,
}
