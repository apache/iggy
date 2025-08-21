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

// TODO:
// Observe the `Delete` and `DeleteCwel` traits,
// those support only removal of singular `Entity`.
// We could add a `DeleteMany` method that would return `impl Iterator<Item = Self::Item>`
//
// How is that useful ?
// We support bulk deletes only for partitions for now,
// but since the partition and it's segments are disjointed (segments are not part of the `Partition` entity),
// we can use the returned iterator to zip together with segements iterator and drain them together instead of individually, 5Head.

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

//TODO: Maybe two seperate traits for Ref and RefMut.
pub trait ComponentsMapping<T>: private::Sealed {
    type Ref<'a>;
    type RefMut<'a>;
}

pub trait ComponentsByIdMapping<T>: private::Sealed {
    type Ref<'a>;
    type RefMut<'a>;
}

macro_rules! impl_components_mapping_for_slab {
    ($T:ident) => {
        impl<$T> private::Sealed for ($T,) {}

        impl<$T> ComponentsMapping<Borrow> for ($T,)
            where for<'a> $T :'a
        {
            type Ref<'a> = (&'a ::slab::Slab<$T>,);
            type RefMut<'a> = (&'a mut ::slab::Slab<$T>,);
        }

        impl<$T> ComponentsMapping<InteriorMutability> for ($T,)
            where for<'a> $T :'a
        {
            type Ref<'a> = (::std::cell::Ref<'a, ::slab::Slab<$T>>,);
            type RefMut<'a> = (::std::cell::RefMut<'a, ::slab::Slab<$T>>,);
        }

        impl<$T> ComponentsByIdMapping<Borrow> for ($T,)
            where for<'a> $T :'a
        {
            type Ref<'a> = (&'a $T,);
            type RefMut<'a> = (&'a mut $T,);
        }

        impl<$T> ComponentsByIdMapping<InteriorMutability> for ($T,)
            where for<'a> $T :'a
        {
            type Ref<'a> = (::std::cell::Ref<'a, $T>,);
            type RefMut<'a> = (::std::cell::RefMut<'a, $T>,);
        }
    };

    ($T:ident, $($rest:ident),+) => {
        impl<$T, $($rest),+> private::Sealed for ($T, $($rest),+) {}

        impl<$T, $($rest),+> ComponentsMapping<Borrow> for ($T, $($rest),+)
            where
                for<'a> $T :'a,
                $(for<'a> $rest: 'a),+
        {
            type Ref<'a> = (&'a ::slab::Slab<$T>, $(&'a ::slab::Slab<$rest>),+);
            type RefMut<'a> = (&'a mut ::slab::Slab<$T>, $(&'a mut ::slab::Slab<$rest>),+);
        }

        impl<$T, $($rest),+> ComponentsMapping<InteriorMutability> for ($T, $($rest),+)
            where
                for<'a> $T :'a,
                $(for<'a> $rest: 'a),+
        {
            type Ref<'a> = (std::cell::Ref<'a, ::slab::Slab<$T>>, $(::std::cell::Ref<'a, ::slab::Slab<$rest>>),+);
            type RefMut<'a> = (std::cell::RefMut<'a, ::slab::Slab<$T>>, $(::std::cell::RefMut<'a, ::slab::Slab<$rest>>),+);
        }

        impl<$T, $($rest),+> ComponentsByIdMapping<Borrow> for ($T, $($rest),+)
            where
                for<'a> $T :'a,
                $(for<'a> $rest: 'a),+
        {
            type Ref<'a> = (&'a $T, $(&'a $rest),+);
            type RefMut<'a> = (&'a mut $T, $(&'a mut $rest),+);
        }

        impl<$T, $($rest),+> ComponentsByIdMapping<InteriorMutability> for ($T, $($rest),+)
            where
                for<'a> $T :'a,
                $(for<'a> $rest: 'a),+
        {
            type Ref<'a> = (std::cell::Ref<'a, $T>, $(::std::cell::Ref<'a, $rest>),+);
            type RefMut<'a> = (std::cell::RefMut<'a, $T>, $(::std::cell::RefMut<'a, $rest>),+);
        }
        impl_components_mapping_for_slab!($($rest),+);
    };
}
impl_components_mapping_for_slab!(T1, T2, T3, T4, T5, T6, T7, T8);

type Mapping<'a, E, T> = <<E as IntoComponents>::Components as ComponentsMapping<T>>::Ref<'a>;
type MappingMut<'a, E, T> = <<E as IntoComponents>::Components as ComponentsMapping<T>>::RefMut<'a>;

type MappingById<'a, E, T> =
    <<E as IntoComponents>::Components as ComponentsByIdMapping<T>>::Ref<'a>;
type MappingByIdMut<'a, E, T> =
    <<E as IntoComponents>::Components as ComponentsByIdMapping<T>>::RefMut<'a>;

// I think it's better to *NOT* use `Components` directly on the `with` methods.
// Instead use the `Self::EntityRef` type directly.
// This way we can auto implement the `with_by_id` method.
// But on the other hand, we need to call `into_components` on the value returned by the `with` method.
// So we lack the ability to immediately discard unnecessary components, which leads to less ergonomic API.
// Damn tradeoffs.
pub type Components<T> = <T as IntoComponents>::Components;
pub type ComponentsById<'a, T> = <T as IntoComponentsById>::Output;

// TODO: Not all hope is lost, don't get discouraged by the comment from above
// I've figured there is actually and ergonomic improvement that can be made here.
// Observe that the chain of constraints put on the `EntityRef` type is actually wrong.
// We constraint the `EntityRef` to be IntoComponents + IntoComponentsById,
// which from composability point of view is not ideal...
// A better idea is to constraint the `IntoComponents::Output` of the `EntityRef` impl to `IntoComponentsById`, rather than entire `EntityRef`.
// This way we can name the type inside of the `with_by_id` methods, without desolving into the tuple mapping madness.
// in the `stream.rs` `topic.rs` `partitions.rs` files, we need to implement the `IntoComponentsById` trait for the output type of `IntoComponents` implementation, for `EntityRef`.
// to make our life easier, we can create a type alias for those tuples and maybe even create a macro, to not repeat the type 3 times per entity (TupleEntityType, TupleEntityTypeRef, TupleEntityTypeRefByid).

// TODO: Since those traits at impl site all they do is call `f(self.into())`
// we can blanket implement those for all types that implement `From` trait.
pub trait EntityComponentSystem<T>
where
    <Self::Entity as IntoComponents>::Components: ComponentsMapping<T> + ComponentsByIdMapping<T>,
{
    type Idx;
    type Entity: IntoComponents + EntityMarker;
    type EntityComponents<'a>: IntoComponents<Components = Mapping<'a, Self::Entity, T>>
        + IntoComponentsById<Idx = Self::Idx, Output = MappingById<'a, Self::Entity, T>>;

    fn with_components<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponents<'a>) -> O;

    fn with_components_async<O, F>(&self, f: F) -> impl Future<Output = O>
    where
        F: for<'a> AsyncFnOnce(Self::EntityComponents<'a>) -> O;

    fn with_components_by_id<O, F>(&self, id: Self::Idx, f: F) -> O
    where
        F: for<'a> FnOnce(ComponentsById<'a, Self::EntityComponents<'a>>) -> O,
    {
        self.with_components(|components| f(components.into_components_by_id(id)))
    }

    fn with_components_by_id_async<O, F>(&self, id: Self::Idx, f: F) -> impl Future<Output = O>
    where
        F: for<'a> AsyncFnOnce(ComponentsById<'a, Self::EntityComponents<'a>>) -> O,
    {
        self.with_components_async(async |components| f(components.into_components_by_id(id)).await)
    }
}

pub trait EntityComponentSystemMut: EntityComponentSystem<Borrow> {
    type EntityComponentsMut<'a>: IntoComponents<Components = MappingMut<'a, Self::Entity, Borrow>>
        + IntoComponentsById<Idx = Self::Idx, Output = MappingByIdMut<'a, Self::Entity, Borrow>>;

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
    type EntityComponentsMut<'a>: IntoComponents<Components = MappingMut<'a, Self::Entity, InteriorMutability>>
        + IntoComponentsById<
            Idx = Self::Idx,
            Output = MappingByIdMut<'a, Self::Entity, InteriorMutability>,
        >;

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
