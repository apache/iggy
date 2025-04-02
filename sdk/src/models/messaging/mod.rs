mod index;
mod index_view;
mod indexes;
mod message;
mod message_header;
mod message_header_view;
mod message_view;
mod messages_batch;
mod user_headers;

pub const INDEX_SIZE: usize = 16;

pub use index::IggyIndex;
pub use index_view::IggyIndexView;
pub use indexes::IggyIndexes;
pub use message::IggyMessage;
pub use message_header::{
    IggyMessageHeader, IGGY_MESSAGE_CHECKSUM_OFFSET_RANGE,
    IGGY_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE, IGGY_MESSAGE_HEADER_RANGE, IGGY_MESSAGE_HEADER_SIZE,
    IGGY_MESSAGE_ID_OFFSET_RANGE, IGGY_MESSAGE_OFFSET_OFFSET_RANGE,
    IGGY_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE, IGGY_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE,
    IGGY_MESSAGE_TIMESTAMP_OFFSET_RANGE,
};
pub use message_header_view::IggyMessageHeaderView;
pub use message_view::{IggyMessageView, IggyMessageViewIterator};
pub use messages_batch::IggyMessagesBatch;
pub use user_headers::{HeaderKey, HeaderKind, HeaderValue};
