mod indexes;
mod messages;
mod messages_accumulator;
mod reading_messages;
mod segment;
mod types;
mod writing_messages;

pub use indexes::Index;
pub use segment::Segment;
pub use types::IggyMessageHeaderViewMut;
pub use types::IggyMessageViewMut;
pub use types::IggyMessagesMut;
pub use types::IggyMessagesSlice;

pub const LOG_EXTENSION: &str = "log";
pub const INDEX_EXTENSION: &str = "index";
pub const SEGMENT_MAX_SIZE_BYTES: u64 = 1000 * 1000 * 1000;
