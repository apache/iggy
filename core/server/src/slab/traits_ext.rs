// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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

pub type Components<T> = <T as IntoComponents>::Components;
pub type ComponentsById<'a, T> = <T as IntoComponentsById>::Output;

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
