mod indexes;
mod messages;
mod messages_accumulator;
mod reading_messages;
mod segment;
mod types;
mod writing_messages;

pub use indexes::{IggyIndexView, IggyIndexesMut};
pub use messages_accumulator::MessagesAccumulator;
pub use segment::Segment;
pub use types::IggyBatch;
pub use types::IggyMessageHeaderViewMut;
pub use types::IggyMessageViewMut;
pub use types::IggyMessages;
pub use types::IggyMessagesMut;

pub const LOG_EXTENSION: &str = "log";
pub const INDEX_EXTENSION: &str = "index";
pub const SEGMENT_MAX_SIZE_BYTES: u64 = 1000 * 1000 * 1000;
