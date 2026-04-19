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

use super::iobuf::{Frozen, Owned};
use crate::consensus::{
    self, Command2, CommitHeader, ConsensusError, ConsensusHeader, DoViewChangeHeader,
    GenericHeader, PrepareHeader, PrepareOkHeader, RequestHeader, StartViewChangeHeader,
    StartViewHeader,
};
use smallvec::SmallVec;
use std::{marker::PhantomData, mem::size_of};

const MESSAGE_ALIGN: usize = 4096;

pub trait MessageBacking<H>
where
    H: ConsensusHeader,
{
    fn header(&self) -> &H;
    fn header_storage(&self) -> &[u8];
    fn total_len(&self) -> usize;
}

pub trait RequestBackingKind {}
pub trait ResponseBackingKind {}

pub trait MutableBacking<H>: MessageBacking<H> + RequestBackingKind
where
    H: ConsensusHeader,
{
    fn as_slice(&self) -> &[u8];
    fn as_mut_slice(&mut self) -> &mut [u8];
}

mod sealed {
    pub trait Sealed {}
}

pub trait FragmentedBacking<H>: MessageBacking<H> + ResponseBackingKind + sealed::Sealed
where
    H: ConsensusHeader,
{
    fn fragments(&self) -> &[Frozen<MESSAGE_ALIGN>];
}

impl sealed::Sealed for ResponseBacking {}

#[derive(Debug, Clone)]
pub struct RequestBacking {
    owned: Owned<MESSAGE_ALIGN>,
}

#[derive(Debug, Clone)]
pub struct ResponseBacking {
    fragments: SmallVec<[Frozen<MESSAGE_ALIGN>; 4]>,
}

impl RequestBackingKind for RequestBacking {}
impl ResponseBackingKind for ResponseBacking {}

impl RequestBacking {
    fn into_owned(self) -> Owned<MESSAGE_ALIGN> {
        self.owned
    }

    fn into_frozen(self) -> Frozen<MESSAGE_ALIGN> {
        self.owned.into()
    }
}

impl<H> MessageBacking<H> for RequestBacking
where
    H: ConsensusHeader,
{
    fn header(&self) -> &H {
        let bytes = &self.owned.as_slice()[..size_of::<H>()];
        bytemuck::checked::try_from_bytes(bytes)
            .expect("header bytes must match the requested header type")
    }

    fn header_storage(&self) -> &[u8] {
        self.owned.as_slice()
    }

    fn total_len(&self) -> usize {
        self.owned.as_slice().len()
    }
}

impl<H> MutableBacking<H> for RequestBacking
where
    H: ConsensusHeader,
{
    fn as_slice(&self) -> &[u8] {
        self.owned.as_slice()
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.owned.as_mut_slice()
    }
}

impl<H> MessageBacking<H> for ResponseBacking
where
    H: ConsensusHeader,
{
    fn header(&self) -> &H {
        let first = self
            .fragments
            .first()
            .expect("response backing validated at construction time");
        let bytes = &first.as_slice()[..size_of::<H>()];
        bytemuck::checked::try_from_bytes(bytes)
            .expect("response header bytes must match the requested header type")
    }

    fn header_storage(&self) -> &[u8] {
        self.fragments
            .first()
            .expect("response backing validated at construction time")
            .as_slice()
    }

    fn total_len(&self) -> usize {
        self.fragments.iter().map(Frozen::len).sum()
    }
}

impl<H> FragmentedBacking<H> for ResponseBacking
where
    H: ConsensusHeader,
{
    fn fragments(&self) -> &[Frozen<MESSAGE_ALIGN>] {
        &self.fragments
    }
}

pub trait ConsensusMessage<H>
where
    H: ConsensusHeader,
{
    fn header(&self) -> &H;
}

impl<H, B> ConsensusMessage<H> for Message<H, B>
where
    H: ConsensusHeader,
    B: MessageBacking<H>,
{
    fn header(&self) -> &H {
        self.backing.header()
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct Message<H, B = RequestBacking> {
    backing: B,
    _marker: PhantomData<H>,
}

impl<H, B> Message<H, B>
where
    H: ConsensusHeader,
    B: MessageBacking<H>,
{
    pub fn header(&self) -> &H {
        self.backing.header()
    }

    pub fn total_len(&self) -> usize {
        self.backing.total_len()
    }

    pub fn into_inner(self) -> B {
        self.backing
    }

    pub fn into_generic(self) -> Message<GenericHeader, B>
    where
        B: MessageBacking<GenericHeader>,
    {
        Message {
            backing: self.backing,
            _marker: PhantomData,
        }
    }

    pub const fn as_generic(&self) -> &Message<GenericHeader, B>
    where
        B: MessageBacking<GenericHeader>,
    {
        unsafe { &*std::ptr::from_ref(self).cast::<Message<GenericHeader, B>>() }
    }

    /// # Errors
    ///
    /// Returns [`ConsensusError`] if the backing is too short for `T`, the
    /// command encoded in the generic header does not match `T::COMMAND`, or
    /// the typed header fails validation.
    pub fn try_into_typed<T>(self) -> Result<Message<T, B>, ConsensusError>
    where
        T: ConsensusHeader,
        B: MessageBacking<GenericHeader> + MessageBacking<T>,
    {
        if self.total_len() < size_of::<T>() {
            return Err(ConsensusError::InvalidCommand {
                expected: T::COMMAND,
                found: Command2::Reserved,
            });
        }

        let generic = self.as_generic();
        if generic.header().command != T::COMMAND {
            return Err(ConsensusError::InvalidCommand {
                expected: T::COMMAND,
                found: generic.header().command,
            });
        }

        let bytes = <B as MessageBacking<T>>::header_storage(&self.backing);
        let typed = bytemuck::checked::try_from_bytes::<T>(&bytes[..size_of::<T>()])
            .map_err(|_| ConsensusError::InvalidBitPattern)?;
        typed.validate()?;

        Ok(Message {
            backing: self.backing,
            _marker: PhantomData,
        })
    }

    /// # Errors
    ///
    /// Returns [`ConsensusError`] if the backing is too short for `T`, the
    /// command encoded in the generic header does not match `T::COMMAND`, or
    /// the typed header fails validation.
    pub fn try_as_typed<T>(&self) -> Result<&Message<T, B>, ConsensusError>
    where
        T: ConsensusHeader,
        B: MessageBacking<GenericHeader> + MessageBacking<T>,
    {
        if self.total_len() < size_of::<T>() {
            return Err(ConsensusError::InvalidCommand {
                expected: T::COMMAND,
                found: Command2::Reserved,
            });
        }

        let generic = self.as_generic();
        if generic.header().command != T::COMMAND {
            return Err(ConsensusError::InvalidCommand {
                expected: T::COMMAND,
                found: generic.header().command,
            });
        }

        let bytes = <B as MessageBacking<T>>::header_storage(&self.backing);
        let typed = bytemuck::checked::try_from_bytes::<T>(&bytes[..size_of::<T>()])
            .map_err(|_| ConsensusError::InvalidBitPattern)?;
        typed.validate()?;

        let typed_message = unsafe { &*std::ptr::from_ref(self).cast::<Message<T, B>>() };
        let _ = typed;
        Ok(typed_message)
    }

    const unsafe fn from_backing_unchecked(backing: B) -> Self {
        Self {
            backing,
            _marker: PhantomData,
        }
    }
}

impl<H> Message<H>
where
    H: ConsensusHeader,
{
    /// # Panics
    ///
    /// Panics if `size` is smaller than `size_of::<H>()`.
    #[must_use]
    pub fn new(size: usize) -> Self {
        assert!(
            size >= size_of::<H>(),
            "size must be at least header size ({})",
            size_of::<H>()
        );

        unsafe {
            Self::from_backing_unchecked(RequestBacking {
                owned: Owned::<MESSAGE_ALIGN>::zeroed(size),
            })
        }
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        <RequestBacking as MutableBacking<H>>::as_slice(&self.backing)
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        <RequestBacking as MutableBacking<H>>::as_mut_slice(&mut self.backing)
    }

    pub fn prefix_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }

    /// # Panics
    ///
    /// Panics if re-validating the copied message unexpectedly fails.
    #[must_use]
    pub fn deep_copy(&self) -> Self {
        Self::try_from(Owned::<MESSAGE_ALIGN>::copy_from_slice(self.as_slice()))
            .expect("deep copied request message must stay valid")
    }

    #[must_use]
    pub fn into_owned(self) -> Owned<MESSAGE_ALIGN> {
        self.backing.into_owned()
    }

    #[must_use]
    pub fn into_frozen(self) -> Frozen<MESSAGE_ALIGN> {
        self.backing.into_frozen()
    }

    /// # Panics
    ///
    /// Panics if `H` and `T` have different sizes, or if the rewritten header
    /// does not validate as `T`.
    pub fn transmute_header<T: ConsensusHeader>(self, f: impl FnOnce(H, &mut T)) -> Message<T> {
        assert_eq!(size_of::<H>(), size_of::<T>());

        let old_header = *self.header();
        let mut owned = self.into_owned();
        let slice = &mut owned.as_mut_slice()[..size_of::<T>()];
        slice.fill(0);
        let new_header =
            bytemuck::checked::try_from_bytes_mut(slice).expect("zeroed bytes are valid");
        f(old_header, new_header);

        Message::try_from(owned).expect("transmuted request message must stay valid")
    }
}

impl<H> Message<H, ResponseBacking>
where
    H: ConsensusHeader,
{
    #[must_use]
    pub fn fragments(&self) -> &[Frozen<MESSAGE_ALIGN>] {
        <ResponseBacking as FragmentedBacking<H>>::fragments(&self.backing)
    }
}

impl<H> Clone for Message<H, RequestBacking>
where
    H: ConsensusHeader,
{
    fn clone(&self) -> Self {
        Self {
            backing: self.backing.clone(),
            _marker: PhantomData,
        }
    }
}

impl<H> Clone for Message<H, ResponseBacking>
where
    H: ConsensusHeader,
{
    fn clone(&self) -> Self {
        Self {
            backing: self.backing.clone(),
            _marker: PhantomData,
        }
    }
}

impl<H> TryFrom<Owned<MESSAGE_ALIGN>> for Message<H>
where
    H: ConsensusHeader,
{
    type Error = ConsensusError;

    fn try_from(owned: Owned<MESSAGE_ALIGN>) -> Result<Self, Self::Error> {
        let bytes = owned.as_slice();
        if bytes.len() < size_of::<H>() {
            return Err(ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: Command2::Reserved,
            });
        }

        let header = bytemuck::checked::try_from_bytes::<H>(&bytes[..size_of::<H>()])
            .map_err(|_| ConsensusError::InvalidBitPattern)?;
        header.validate()?;

        if bytes.len() < header.size() as usize {
            return Err(ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: Command2::Reserved,
            });
        }

        Ok(unsafe { Self::from_backing_unchecked(RequestBacking { owned }) })
    }
}

impl<H> TryFrom<SmallVec<[Frozen<MESSAGE_ALIGN>; 4]>> for Message<H, ResponseBacking>
where
    H: ConsensusHeader,
{
    type Error = ConsensusError;

    fn try_from(fragments: SmallVec<[Frozen<MESSAGE_ALIGN>; 4]>) -> Result<Self, Self::Error> {
        let Some(first) = fragments.first() else {
            return Err(ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: Command2::Reserved,
            });
        };

        if first.len() < size_of::<H>() {
            return Err(ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: Command2::Reserved,
            });
        }

        let header = bytemuck::checked::try_from_bytes::<H>(&first.as_slice()[..size_of::<H>()])
            .map_err(|_| ConsensusError::InvalidBitPattern)?;
        header.validate()?;

        let total_len = fragments.iter().map(Frozen::len).sum::<usize>();
        if total_len < header.size() as usize {
            return Err(ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: Command2::Reserved,
            });
        }

        Ok(unsafe { Self::from_backing_unchecked(ResponseBacking { fragments }) })
    }
}

#[derive(Debug)]
pub enum MessageBag {
    Request(Message<RequestHeader>),
    Prepare(Message<PrepareHeader>),
    PrepareOk(Message<PrepareOkHeader>),
    StartViewChange(Message<StartViewChangeHeader>),
    DoViewChange(Message<DoViewChangeHeader>),
    StartView(Message<StartViewHeader>),
    Commit(Message<CommitHeader>),
}

impl MessageBag {
    #[must_use]
    pub fn command(&self) -> Command2 {
        match self {
            Self::Request(message) => message.header().command,
            Self::Prepare(message) => message.header().command,
            Self::PrepareOk(message) => message.header().command,
            Self::StartViewChange(message) => message.header().command,
            Self::DoViewChange(message) => message.header().command,
            Self::StartView(message) => message.header().command,
            Self::Commit(message) => message.header().command,
        }
    }

    #[must_use]
    pub fn size(&self) -> u32 {
        match self {
            Self::Request(message) => message.header().size(),
            Self::Prepare(message) => message.header().size(),
            Self::PrepareOk(message) => message.header().size(),
            Self::StartViewChange(message) => message.header().size(),
            Self::DoViewChange(message) => message.header().size(),
            Self::StartView(message) => message.header().size(),
            Self::Commit(message) => message.header().size(),
        }
    }

    #[must_use]
    pub fn operation(&self) -> consensus::Operation {
        match self {
            Self::Request(message) => message.header().operation,
            Self::Prepare(message) => message.header().operation,
            Self::PrepareOk(message) => message.header().operation,
            Self::StartViewChange(message) => message.header().operation(),
            Self::DoViewChange(message) => message.header().operation(),
            Self::StartView(message) => message.header().operation(),
            Self::Commit(message) => message.header().operation(),
        }
    }
}

impl<T> TryFrom<Message<T>> for MessageBag
where
    T: ConsensusHeader,
{
    type Error = ConsensusError;

    fn try_from(value: Message<T>) -> Result<Self, Self::Error> {
        let command = value.as_generic().header().command;
        let backing = value.into_inner();

        match command {
            Command2::Prepare => {
                let msg = unsafe { Message::<PrepareHeader>::from_backing_unchecked(backing) };
                Ok(Self::Prepare(msg))
            }
            Command2::Request => {
                let msg = unsafe { Message::<RequestHeader>::from_backing_unchecked(backing) };
                Ok(Self::Request(msg))
            }
            Command2::PrepareOk => {
                let msg = unsafe { Message::<PrepareOkHeader>::from_backing_unchecked(backing) };
                Ok(Self::PrepareOk(msg))
            }
            Command2::StartViewChange => {
                let msg =
                    unsafe { Message::<StartViewChangeHeader>::from_backing_unchecked(backing) };
                Ok(Self::StartViewChange(msg))
            }
            Command2::DoViewChange => {
                let msg = unsafe { Message::<DoViewChangeHeader>::from_backing_unchecked(backing) };
                Ok(Self::DoViewChange(msg))
            }
            Command2::StartView => {
                let msg = unsafe { Message::<StartViewHeader>::from_backing_unchecked(backing) };
                Ok(Self::StartView(msg))
            }
            Command2::Commit => {
                let msg = unsafe { Message::<CommitHeader>::from_backing_unchecked(backing) };
                Ok(Self::Commit(msg))
            }
            other => Err(ConsensusError::InvalidCommand {
                expected: Command2::Reserved,
                found: other,
            }),
        }
    }
}
