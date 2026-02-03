use crate::bus::MemBus;
use crate::deps::{
    MemStorage, ReplicaPartitions, SimJournal, SimMetadata, SimMuxStateMachine, SimSnapshot,
};
use metadata::stm::consumer_group::{ConsumerGroups, ConsumerGroupsInner};
use metadata::stm::stream::{Streams, StreamsInner};
use metadata::stm::user::{Users, UsersInner};
use metadata::{variadic, IggyMetadata};
use std::rc::Rc;

#[derive(Debug)]
pub struct Replica {
    pub id: u8,
    pub name: String,
    pub metadata: SimMetadata,
    pub partitions: ReplicaPartitions,
    pub bus: Rc<MemBus>,
}

impl Replica {
    pub fn new(id: u8, name: String, bus: Rc<MemBus>) -> Self {
        // Create the mux state machine with all state machines
        let users: Users = UsersInner::new().into();
        let streams: Streams = StreamsInner::new().into();
        let consumer_groups: ConsumerGroups = ConsumerGroupsInner::new().into();
        let mux = SimMuxStateMachine::new(variadic!(users, streams, consumer_groups));

        Self {
            id,
            name,
            metadata: IggyMetadata {
                consensus: None, // TODO: Init consensus
                journal: Some(SimJournal::<MemStorage>::default()),
                snapshot: Some(SimSnapshot::default()),
                mux_stm: mux,
            },
            partitions: ReplicaPartitions::default(),
            bus,
        }
    }
}
