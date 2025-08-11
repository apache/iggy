pub trait IntoComponents {
    type Components;
    fn into_components(self) -> Self::Components;
}

// Marker trait for the entity type.
pub trait EntityMarker {}

pub trait Insert<Idx> {
    type Item: IntoComponents + EntityMarker;
    fn insert(&mut self, item: Self::Item) -> Idx;
}

pub trait Delete<Idx> {
    type Item: IntoComponents + EntityMarker;
    fn delete(&mut self, id: Idx) -> Self::Item;
}

pub trait DeleteCell<Idx> {
    type Item: IntoComponents + EntityMarker;
    fn delete(&self, id: Idx) -> Self::Item;
}

pub trait IndexComponents<Idx: ?Sized> {
    type Output;
    fn index(&self, index: Idx) -> Self::Output;
}

pub struct Borrow;
pub struct RefCell;

mod private {
    pub trait Sealed {}
}

//TODO: Maybe two seperate traits for Ref and RefMut.
pub trait ComponentsMapping<T>: private::Sealed {
    type Ref<'a>;
    type RefMut<'a>;
}

pub trait ComponentsByIdMapping<T>: private::Sealed {
    // TODO: We will need this to contrain the `EntityRef` and `EntityRefMut` types, so after decomposing they have proper mapping.
    // Similar mechanism to trait from above, but for (T1, T2) -> (&T1, &T2) mapping rather than (T1, T2) -> (&Slab<T1>, &Slab<T2>).
    type Ref<'a>;
    type RefMut<'a>;
}

macro_rules! impl_components_for_slab_as_refs {
    ($T:ident) => {
        impl<$T> private::Sealed for ($T,) {}

        impl<$T> ComponentsMapping<Borrow> for ($T,)
        where for<'a> $T: 'a
        {
            type Ref<'a> = (&'a ::slab::Slab<$T>,);
            type RefMut<'a> = (&'a mut ::slab::Slab<$T>,);
        }

        impl<$T> ComponentsMapping<RefCell> for ($T,)
        where for<'a> $T: 'a
        {
            type Ref<'a> = (::std::cell::Ref<'a, ::slab::Slab<$T>>,);
            type RefMut<'a> = (::std::cell::RefMut<'a, ::slab::Slab<$T>>,);
        }

        impl<$T> ComponentsByIdMapping<Borrow> for ($T,)
        where for<'a> $T: 'a
        {
            type Ref<'a> = (&'a $T,);
            type RefMut<'a> = (&'a mut $T,);
        }

        impl<$T> ComponentsByIdMapping<RefCell> for ($T,)
        where for<'a> $T: 'a
        {
            type Ref<'a> = (::std::cell::Ref<'a, $T>,);
            type RefMut<'a> = (::std::cell::RefMut<'a, $T>,);
        }
    };

    ($T:ident, $($rest:ident),+) => {
        impl<$T, $($rest),+> private::Sealed for ($T, $($rest),+) {}

        impl<$T, $($rest),+> ComponentsMapping<Borrow> for ($T, $($rest),+)
        where
            for<'a> $T: 'a,
            $(for<'a> $rest: 'a),+
        {
            type Ref<'a> = (&'a ::slab::Slab<$T>, $(&'a ::slab::Slab<$rest>),+);
            type RefMut<'a> = (&'a mut ::slab::Slab<$T>, $(&'a mut ::slab::Slab<$rest>),+);
        }

        impl<$T, $($rest),+> ComponentsMapping<RefCell> for ($T, $($rest),+)
        where
            for<'a> $T: 'a,
            $(for<'a> $rest: 'a),+
        {
            type Ref<'a> = (std::cell::Ref<'a, ::slab::Slab<$T>>, $(::std::cell::Ref<'a, ::slab::Slab<$rest>>),+);
            type RefMut<'a> = (std::cell::RefMut<'a, ::slab::Slab<$T>>, $(::std::cell::RefMut<'a, ::slab::Slab<$rest>>),+);
        }

        impl<$T, $($rest),+> ComponentsByIdMapping<Borrow> for ($T, $($rest),+)
        where
            for<'a> $T: 'a,
            $(for<'a> $rest: 'a),+
        {
            type Ref<'a> = (&'a $T, $(&'a $rest),+);
            type RefMut<'a> = (&'a mut $T, $(&'a mut $rest),+);
        }

        impl<$T, $($rest),+> ComponentsByIdMapping<RefCell> for ($T, $($rest),+)
        where
            for<'a> $T: 'a,
            $(for<'a> $rest: 'a),+
        {
            type Ref<'a> = (std::cell::Ref<'a, $T>, $(::std::cell::Ref<'a, $rest>),+);
            type RefMut<'a> = (std::cell::RefMut<'a, $T>, $(::std::cell::RefMut<'a, $rest>),+);
        }
        impl_components_for_slab_as_refs!($($rest),+);
    };
}
impl_components_for_slab_as_refs!(T1, T2, T3, T4, T5, T6, T7, T8);

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
pub type ComponentsById<Idx, T> = <T as IndexComponents<Idx>>::Output;

pub trait EntityComponentSystem<Idx, T>
where
    <Self::Entity as IntoComponents>::Components: ComponentsMapping<T> + ComponentsByIdMapping<T>,
{
    type Entity: IntoComponents + EntityMarker;
    type EntityRef<'a>: IntoComponents<Components = Mapping<'a, Self::Entity, T>>
        + IndexComponents<Idx, Output = MappingById<'a, Self::Entity, T>>
    where
        Self: 'a;
    fn with<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityRef<'a>) -> O;

    fn with_async<O, F>(&self, f: F) -> impl Future<Output = O>
    where
        F: for<'a> AsyncFnOnce(Self::EntityRef<'a>) -> O;

    fn with_by_id<O, F>(&self, id: Idx, f: F) -> O
    where
        F: for<'a> FnOnce(ComponentsById<Idx, Self::EntityRef<'a>>) -> O,
    {
        self.with(|components| f(components.index(id)))
    }

    fn with_by_id_async<O, F>(&self, id: Idx, f: F) -> impl Future<Output = O>
    where
        F: for<'a> AsyncFnOnce(ComponentsById<Idx, Self::EntityRef<'a>>) -> O,
    {
        self.with_async(async |components| f(components.index(id)).await)
    }
}

pub trait EntityComponentSystemMut<Idx>: EntityComponentSystem<Idx, Borrow> {
    type EntityRefMut<'a>: IntoComponents<Components = MappingMut<'a, Self::Entity, Borrow>>
        + IndexComponents<Idx, Output = MappingByIdMut<'a, Self::Entity, Borrow>>
    where
        Self: 'a;

    fn with_mut<O, F>(&mut self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityRefMut<'a>) -> O;

    fn with_by_id_mut<O, F>(&mut self, id: Idx, f: F) -> O
    where
        F: for<'a> FnOnce(ComponentsById<Idx, Self::EntityRefMut<'a>>) -> O,
    {
        self.with_mut(|components| f(components.index(id)))
    }
}

pub trait EntityComponentSystemMutCell<Idx>: EntityComponentSystem<Idx, RefCell> {
    type EntityRefMut<'a>: IntoComponents<Components = MappingMut<'a, Self::Entity, RefCell>>
        + IndexComponents<Idx, Output = MappingByIdMut<'a, Self::Entity, RefCell>>
    where
        Self: 'a;

    fn with_mut<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityRefMut<'a>) -> O;

    fn with_by_id_mut<O, F>(&self, id: Idx, f: F) -> O
    where
        F: for<'a> FnOnce(ComponentsById<Idx, Self::EntityRefMut<'a>>) -> O,
    {
        self.with_mut(|components| f(components.index(id)))
    }
}
