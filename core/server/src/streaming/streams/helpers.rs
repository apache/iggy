use crate::{
    slab::{streams, traits_ext::ComponentsById},
    streaming::streams::stream2::{StreamRef, StreamRefMut, StreamRoot},
};

pub fn get_stream_id() -> impl FnOnce(ComponentsById<StreamRef>) -> streams::ContainerId {
    |(root, _)| root.id()
}

pub fn get_stream_name() -> impl FnOnce(ComponentsById<StreamRef>) -> String {
    |(root, _)| root.name().clone()
}

pub fn update_stream_name(name: String) -> impl FnOnce(ComponentsById<StreamRefMut>) {
    move |(mut root, _)| {
        root.set_name(name);
    }
}
