use crate::cache::connection::ShardedState;

// TODO: Move to some common trait location.
/// Allocation strategy that produces deltas for a specific sharded state type
pub trait AllocationStrategy<SS>
where
    SS: ShardedState,
{
    fn allocate(&self, entry: SS::Entry) -> Option<SS::Delta>;
    fn deallocate(&self, entry: SS::Entry) -> Option<SS::Delta>;
}

pub(crate) mod connection;
