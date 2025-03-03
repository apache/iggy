mod fetch_result;
mod indexes;
mod logs;
mod reading_messages;
mod segment;
mod writing_messages;

pub use fetch_result::*;
pub use indexes::Index;
pub use segment::Segment;

pub const LOG_EXTENSION: &str = "log";
pub const INDEX_EXTENSION: &str = "index";
pub const SEGMENT_MAX_SIZE_BYTES: u64 = 1000 * 1000 * 1000;
