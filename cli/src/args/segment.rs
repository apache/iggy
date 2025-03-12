use clap::{Args, Subcommand};
use iggy::identifier::Identifier;

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum SegmentAction {
    /// Delete segments for the specified topic ID,
    /// stream ID and partition ID based on the given count.
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    /// partition ID can be specified as a name or ID
    ///
    /// Examples
    ///  iggy segment delete 1 1 1 10
    ///  iggy segment delete prod 2 2 2
    ///  iggy segment delete test sensor 2 2
    ///  iggy segment delete 1 sensor 2 16
    #[clap(verbatim_doc_comment, visible_alias = "d")]
    Delete(SegmentDeleteArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct SegmentDeleteArgs {
    /// Stream ID to delete segments
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// Topic ID to delete segments
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
    /// Partition ID to delete segments
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) partition_id: u32,
    /// Segments count to be deleted
    #[arg(value_parser = clap::value_parser!(u32).range(1..100_001))]
    pub(crate) segments_count: u32,
}
