mod error;
mod messages;
mod messages_shared;
mod traits;
mod types;
mod utils;

// Errors
pub use error::client_error::ClientError;
pub use error::iggy_error::{IggyError, IggyErrorDiscriminants};
// Locking is feature gated, thus only mod level re-export.
pub mod locking;
// Messages
pub use messages::consumer_groups::*;
pub use messages::consumer_offsets::*;
pub use messages::messaging::*;
pub use messages::partitions::*;
pub use messages::personal_access_tokens::*;
pub use messages::segments::*;
pub use messages::streams::*;
pub use messages::system::*;
pub use messages::topics::*;
pub use messages::users::*;
// Messages shared
pub use messages_shared::*;

// Traits
pub use traits::bytes_serializable::BytesSerializable;
pub use traits::partitioner::Partitioner;
pub use traits::sizeable::Sizeable;
pub use traits::validatable::Validatable;
// Types
pub use types::args::*;
pub use types::client_state::client_state::ClientState;
pub use types::command::*;
pub use types::compression::compression_algorithm::*;
pub use types::configuration::auth_config::auto_login::*;
pub use types::configuration::auth_config::connection_string::*;
pub use types::configuration::auth_config::connection_string_options::*;
pub use types::configuration::auth_config::credentials::*;
pub use types::configuration::http_config::config::*;
pub use types::configuration::quick_config::quic_client_config::*;
pub use types::configuration::quick_config::quic_client_config_builder::*;
pub use types::configuration::quick_config::quic_client_reconnection_config::*;
pub use types::configuration::tcp_config::tcp_client_config::*;
pub use types::configuration::tcp_config::tcp_client_config_builder::*;
pub use types::configuration::tcp_config::tcp_client_reconnection_config::*;
pub use types::confirmation::*;
pub use types::diagnostic::diagnostic_event::DiagnosticEvent;
pub use types::identifier::identifier::*;
pub use types::model::client::client_info::*;
pub use types::model::consumer::consumer::*;
pub use types::model::consumer::consumer_group::*;
pub use types::model::consumer::consumer_offset_info::*;
pub use types::model::partition::partition::*;
pub use types::model::permissions::permissions::*;
pub use types::model::permissions::personal_access_token::*;
pub use types::model::snapshot::snapshot::*;
pub use types::model::stats::stats::*;
pub use types::model::stream::stream::*;
pub use types::model::topic::topic::*;
pub use types::model::user::user_identity_info::*;
pub use types::model::user::user_info::*;
pub use types::model::user::user_status::*;
pub use types::snapshot::snapshot::*;
// Utils
pub use utils::byte_size::IggyByteSize;
pub use utils::checksum::*;
pub use utils::crypto::*;
pub use utils::duration::IggyDuration;
pub use utils::expiry::IggyExpiry;
pub use utils::personal_access_token_expiry::PersonalAccessTokenExpiry;
pub use utils::text;
pub use utils::timestamp::*;
pub use utils::topic_size::MaxTopicSize;
