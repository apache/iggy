use iggy_common::IggyTimestamp;
use slab::Slab;
use std::{
    cell::{Ref, RefMut},
    sync::Arc,
};

use crate::{
    slab::{
        Keyed,
        streams::{self, Streams},
        topics::Topics,
        traits_ext::{EntityMarker, InsertCell, IntoComponents, IntoComponentsById},
    },
    streaming::stats::StreamStats,
};

#[derive(Debug, Clone)]
pub struct StreamRoot {
    id: usize,
    name: String,
    created_at: IggyTimestamp,
    topics: Topics,
}

impl Keyed for StreamRoot {
    type Key = String;

    fn key(&self) -> &Self::Key {
        &self.name
    }
}

impl StreamRoot {
    pub fn new(name: String, created_at: IggyTimestamp) -> Self {
        Self {
            id: 0,
            name,
            created_at,
            topics: Topics::default(),
        }
    }

    pub fn update_id(&mut self, id: usize) {
        self.id = id;
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn topics_count(&self) -> usize {
        self.topics.len()
    }

    pub fn remove_topics(&mut self) -> Topics {
        std::mem::take(&mut self.topics)
    }

    pub fn topics(&self) -> &Topics {
        &self.topics
    }

    pub fn topics_mut(&mut self) -> &mut Topics {
        &mut self.topics
    }

    pub fn created_at(&self) -> IggyTimestamp {
        self.created_at
    }
}

#[derive(Debug, Clone)]
pub struct Stream {
    root: StreamRoot,
    stats: Arc<StreamStats>,
}

impl IntoComponents for Stream {
    type Components = (StreamRoot, Arc<StreamStats>);

    fn into_components(self) -> Self::Components {
        (self.root, self.stats)
    }
}

impl EntityMarker for Stream {
    type Idx = streams::ContainerId;

    fn id(&self) -> Self::Idx {
        self.root.id
    }

    fn update_id(&mut self, id: Self::Idx) {
        self.root.id = id;
    }
}

impl Stream {
    pub fn new(name: String, stats: Arc<StreamStats>, created_at: IggyTimestamp) -> Self {
        let root = StreamRoot::new(name, created_at);
        Self { root, stats }
    }

    pub fn new_with_components(root: StreamRoot, stats: Arc<StreamStats>) -> Self {
        Self { root, stats }
    }

    pub fn stats(&self) -> &Arc<StreamStats> {
        &self.stats
    }

    pub fn root(&self) -> &StreamRoot {
        &self.root
    }

    pub fn root_mut(&mut self) -> &mut StreamRoot {
        &mut self.root
    }
}

pub struct StreamRef<'a> {
    root: Ref<'a, Slab<StreamRoot>>,
    stats: Ref<'a, Slab<Arc<StreamStats>>>,
}

impl<'a> StreamRef<'a> {
    pub fn new(root: Ref<'a, Slab<StreamRoot>>, stats: Ref<'a, Slab<Arc<StreamStats>>>) -> Self {
        Self { root, stats }
    }
}

impl<'a> IntoComponents for StreamRef<'a> {
    type Components = (Ref<'a, Slab<StreamRoot>>, Ref<'a, Slab<Arc<StreamStats>>>);

    fn into_components(self) -> Self::Components {
        (self.root, self.stats)
    }
}

impl<'a> IntoComponentsById for StreamRef<'a> {
    type Idx = streams::ContainerId;
    type Output = (Ref<'a, StreamRoot>, Ref<'a, Arc<StreamStats>>);

    fn into_components_by_id(self, index: Self::Idx) -> Self::Output {
        let root = Ref::map(self.root, |r| &r[index]);
        let stats = Ref::map(self.stats, |s| &s[index]);
        (root, stats)
    }
}

pub struct StreamRefMut<'a> {
    root: RefMut<'a, Slab<StreamRoot>>,
    stats: RefMut<'a, Slab<Arc<StreamStats>>>,
}

impl<'a> StreamRefMut<'a> {
    pub fn new(
        root: RefMut<'a, Slab<StreamRoot>>,
        stats: RefMut<'a, Slab<Arc<StreamStats>>>,
    ) -> Self {
        Self { root, stats }
    }
}

impl<'a> IntoComponents for StreamRefMut<'a> {
    type Components = (
        RefMut<'a, Slab<StreamRoot>>,
        RefMut<'a, Slab<Arc<StreamStats>>>,
    );

    fn into_components(self) -> Self::Components {
        (self.root, self.stats)
    }
}

impl<'a> IntoComponentsById for StreamRefMut<'a> {
    type Idx = streams::ContainerId;
    type Output = (RefMut<'a, StreamRoot>, RefMut<'a, Arc<StreamStats>>);

    fn into_components_by_id(self, index: Self::Idx) -> Self::Output {
        let root = RefMut::map(self.root, |r| &mut r[index]);
        let stats = RefMut::map(self.stats, |s| &mut s[index]);
        (root, stats)
    }
}

// TODO: Move this to a dedicated module.
pub fn create_and_insert_stream_mem(streams: &Streams, name: String) -> Stream {
    let now = IggyTimestamp::now();
    let stats = Arc::new(Default::default());
    let mut stream = Stream::new(name, stats, now);
    let id = streams.insert(stream.clone());
    stream.update_id(id);
    stream
}
