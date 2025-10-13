pub trait IntoComponents {
    type Components;
    fn into_components(self) -> Self::Components;
}

/// Marker trait for the `Entity`.
pub trait EntityMarker {
    type Idx;
    fn id(&self) -> Self::Idx;
    fn update_id(&mut self, id: Self::Idx);
}

/// Insert trait for inserting an `Entity`` into container.
pub trait Insert {
    type Idx;
    type Item: IntoComponents + EntityMarker;
    fn insert(&mut self, item: Self::Item) -> Self::Idx;
}

pub trait InsertCell {
    type Idx;
    type Item: IntoComponents + EntityMarker;
    fn insert(&self, item: Self::Item) -> Self::Idx;
}

/// Delete trait for deleting an `Entity` from container.
pub trait Delete {
    type Idx;
    type Item: IntoComponents + EntityMarker;
    fn delete(&mut self, id: Self::Idx) -> Self::Item;
}

/// Delete trait for deleting an `Entity` from container for container types that use interior mutability.
pub trait DeleteCell {
    type Idx;
    type Item: IntoComponents + EntityMarker;
    fn delete(&self, id: Self::Idx) -> Self::Item;
}

/// Trait for getting components by EntityId.
pub trait IntoComponentsById {
    type Idx;
    type Output;
    fn into_components_by_id(self, index: Self::Idx) -> Self::Output;
}

/// Marker type for borrow component containers.
pub struct Borrow;
/// Marker type for component containers that use interior mutability.
pub struct InteriorMutability;

mod private {
    pub trait Sealed {}
}

// I think it's better to *NOT* use `Components` directly on the `with` methods.
// Instead use the `Self::EntityRef` type directly.
// This way we can auto implement the `with_by_id` method.
// But on the other hand, we need to call `into_components` on the value returned by the `with` method.
// So we lack the ability to immediately discard unnecessary components, which leads to less ergonomic API.
// Damn tradeoffs.
pub type Components<T> = <T as IntoComponents>::Components;
pub type ComponentsById<'a, T> = <T as IntoComponentsById>::Output;

// TODO:
// I've figured there is actually and ergonomic improvement that can be made here.
// Observe that the chain of constraints put on the `EntityRef` type is actually wrong.
// We constraint the `EntityRef` to be IntoComponents + IntoComponentsById,
// which from composability point of view is not ideal...
// A better idea is to constraint the `IntoComponents::Output` of the `EntityRef` impl to `IntoComponentsById`, rather than entire `EntityRef`.
// This way we can name the type inside of the `with_by_id` methods, without desolving into the tuple mapping madness.
// in the `stream.rs` `topic.rs` `partitions.rs` files, we need to implement the `IntoComponentsById` trait for the output type of `IntoComponents` implementation, for `EntityRef`.
// to make our life easier, we can create a type alias for those tuples and maybe even create a macro, to not repeat the type 3 times per entity (TupleEntityType, TupleEntityTypeRef, TupleEntityTypeRefByid).

// TODO: Since those traits at impl site, all they do is call `f(self.into())`
// we can blanket implement those for all types that implement `From` trait.

// Maybe lets not go this way with the tuple mapping madness, it already is pretty difficult to distinguish between all of the different components,
// and everytime we add a new component to an entity, we need to update the tuple type everywhere.
// Better idea would be to use the `EntityRef` type directly inside of the `with_components_by_id` closure
// -- f(components.into_components_by_id(id)) -> components.into_components_by_id(id) would return `EntityRef`, rather than the tuple.
pub trait EntityComponentSystem<T> {
    type Idx;
    type Entity: IntoComponents + EntityMarker;
    type EntityComponents<'a>: IntoComponents + IntoComponentsById<Idx = Self::Idx>;

    fn with_components<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponents<'a>) -> O;

    fn with_components_by_id<O, F>(&self, id: Self::Idx, f: F) -> O
    where
        F: for<'a> FnOnce(ComponentsById<'a, Self::EntityComponents<'a>>) -> O,
    {
        self.with_components(|components| f(components.into_components_by_id(id)))
    }
}

pub trait EntityComponentSystemMut: EntityComponentSystem<Borrow> {
    type EntityComponentsMut<'a>: IntoComponents + IntoComponentsById<Idx = Self::Idx>;

    fn with_components_mut<O, F>(&mut self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponentsMut<'a>) -> O;

    fn with_components_by_id_mut<O, F>(&mut self, id: Self::Idx, f: F) -> O
    where
        F: for<'a> FnOnce(ComponentsById<'a, Self::EntityComponentsMut<'a>>) -> O,
    {
        self.with_components_mut(|components| f(components.into_components_by_id(id)))
    }
}

pub trait EntityComponentSystemMutCell: EntityComponentSystem<InteriorMutability> {
    type EntityComponentsMut<'a>: IntoComponents + IntoComponentsById<Idx = Self::Idx>;

    fn with_components_mut<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponentsMut<'a>) -> O;

    fn with_components_by_id_mut<O, F>(&self, id: Self::Idx, f: F) -> O
    where
        F: for<'a> FnOnce(ComponentsById<'a, Self::EntityComponentsMut<'a>>) -> O,
    {
        self.with_components_mut(|components| f(components.into_components_by_id(id)))
    }
}
