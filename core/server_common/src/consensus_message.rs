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

use crate::iobuf::{Frozen, Owned};
use crate::sharding::METADATA_GROUP;
use aligned_vec::{AVec, ConstAlign};
use iggy_binary_protocol::{
    Command, CommitHeader, ConsensusError, ConsensusHeader, DoViewChangeHeader,
    ForwardLogoutHeader, ForwardLogoutResultHeader, ForwardRegisterHeader,
    ForwardRegisterResultHeader, GenericHeader, HEADER_SIZE, Operation, PrepareHeader,
    PrepareOkHeader, RepairPrepareHeader, RepairRangeReplyHeader, RequestHeader,
    RequestPreparesHeader, RequestStartViewHeader, RequestStateChunkHeader,
    RequestStateTransferHeader, RoutedRequestHeader, StartViewChangeHeader, StartViewHeader,
    StateChunkHeader, StateTransferTargetHeader, frame_checksum_bytes,
    prepare_identity_checksum_bytes,
};
use smallvec::SmallVec;
use std::{
    marker::PhantomData,
    mem::{offset_of, size_of},
};

pub const MESSAGE_ALIGN: usize = 4096;

/// Fragment list behind a [`ResponseBacking`]. Inline for the single-buffer
/// frame every reply but a poll is, so the per-connection mailboxes and the
/// inter-shard reply lane stay one word wider than a bare [`Frozen`]; a
/// vectored poll reply spills its fragment table to the heap once.
pub type ResponseFragments = SmallVec<[Frozen<MESSAGE_ALIGN>; 1]>;

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

/// An outbound frame as a list of buffers written back to back: the wire bytes
/// are the concatenation of `fragments`. Never empty; the first fragment holds
/// the whole frame header.
#[derive(Debug, Clone)]
pub struct ResponseBacking {
    fragments: ResponseFragments,
}

impl RequestBackingKind for RequestBacking {}
impl ResponseBackingKind for ResponseBacking {}

impl ResponseBacking {
    /// The fragment carrying the frame header.
    #[must_use]
    pub fn first(&self) -> &Frozen<MESSAGE_ALIGN> {
        self.fragments
            .first()
            .expect("response backing is never empty")
    }

    #[must_use]
    pub fn fragments(&self) -> &[Frozen<MESSAGE_ALIGN>] {
        &self.fragments
    }

    #[must_use]
    pub fn into_fragments(self) -> ResponseFragments {
        self.fragments
    }

    #[must_use]
    pub fn total_len(&self) -> usize {
        self.fragments.iter().map(Frozen::len).sum()
    }

    /// The frame as one buffer: the single fragment as is, or the fragments
    /// copied back to back. For writers whose record layer needs a contiguous
    /// payload (WebSocket frames, in-process reply decoding).
    #[must_use]
    pub fn into_contiguous(self) -> Frozen<MESSAGE_ALIGN> {
        match self.fragments.as_slice() {
            [single] => single.clone(),
            fragments => {
                let mut joined: AVec<u8, ConstAlign<MESSAGE_ALIGN>> =
                    AVec::with_capacity(MESSAGE_ALIGN, self.total_len());
                for fragment in fragments {
                    joined.extend_from_slice(fragment);
                }
                Owned::from(joined).into()
            }
        }
    }
}

impl From<Frozen<MESSAGE_ALIGN>> for ResponseBacking {
    fn from(frozen: Frozen<MESSAGE_ALIGN>) -> Self {
        Self {
            fragments: smallvec::smallvec![frozen],
        }
    }
}

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
                found: Command::Reserved,
            });
        }

        let generic = self.as_generic();
        if !T::accepts(generic.header().command) {
            return Err(ConsensusError::InvalidCommand {
                expected: T::COMMAND,
                found: generic.header().command,
            });
        }

        let bytes = <B as MessageBacking<T>>::header_storage(&self.backing);
        let typed = bytemuck::checked::try_from_bytes::<T>(&bytes[..size_of::<T>()])
            .map_err(|_| classify_failed_cast::<T>(bytes))?;
        // Before `validate`: a header that did not survive the link intact cannot
        // have any of its fields believed, and `validate` reads them.
        typed.verify_frame()?;
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
                found: Command::Reserved,
            });
        }

        let generic = self.as_generic();
        if !T::accepts(generic.header().command) {
            return Err(ConsensusError::InvalidCommand {
                expected: T::COMMAND,
                found: generic.header().command,
            });
        }

        let bytes = <B as MessageBacking<T>>::header_storage(&self.backing);
        let typed = bytemuck::checked::try_from_bytes::<T>(&bytes[..size_of::<T>()])
            .map_err(|_| classify_failed_cast::<T>(bytes))?;
        // Before `validate`: a header that did not survive the link intact cannot
        // have any of its fields believed, and `validate` reads them.
        typed.verify_frame()?;
        typed.validate()?;

        let typed_message = unsafe { &*std::ptr::from_ref(self).cast::<Message<T, B>>() };
        let _ = typed;
        Ok(typed_message)
    }

    /// Construct a typed `Message<H, B>` without re-validating the header.
    ///
    /// # Safety
    ///
    /// Caller must guarantee:
    /// * `backing.total_len() >= size_of::<H>()`.
    /// * Header bytes are a valid `H` bit pattern (`try_from_bytes` would succeed).
    /// * `H::validate` would return `Ok`.
    ///
    /// Prefer `try_into_typed::<H>()`. Only use when bytes already validated
    /// via another route (e.g. enclosing `MessageBag::try_from` dispatch).
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

    /// The frame body: the bytes after the header, up to the header's `size`.
    /// Empty for a header-only frame.
    ///
    /// Borrowed from the backing buffer rather than copied out of it. A
    /// `WireDecode` parse takes `&[u8]` and keeps only the fields it decodes, so
    /// copying the body first buys nothing and costs a memcpy per frame on paths
    /// that run per replicated op (`metadata::stm`'s apply, `metadata::stm::authz`).
    ///
    /// # Panics
    /// If `size` does not span the header or overruns the buffer.
    /// [`TryFrom<Owned>`](Message::try_from) rejects both, so every received frame
    /// satisfies this; a buffer from [`Message::new`] must have its header stamped
    /// first.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.as_slice()[size_of::<H>()..self.header().size() as usize]
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

impl Message<RequestHeader> {
    /// Retype the client-wire request into the server-internal
    /// [`RoutedRequestHeader`] shape in place, with `group` starting unset.
    ///
    /// The two layouts share every field offset (const-asserted where they
    /// are declared) and `group` claims the client header's reserved tail,
    /// so the promotion zeroes those eight bytes instead of rebuilding the
    /// whole 256-byte header. This is the only sanctioned crossing between
    /// the two layouts: transmute-based reads across them would alias
    /// `group` with reserved bytes a client may have sent nonzero.
    ///
    /// # Panics
    ///
    /// Panics if the retyped header fails [`RoutedRequestHeader`] validation;
    /// unreachable when `self` already passed [`RequestHeader`] validation,
    /// which enforces the same field rules.
    #[must_use]
    pub fn into_routed(self) -> Message<RoutedRequestHeader> {
        let group_offset = offset_of!(RoutedRequestHeader, group);
        let mut owned = self.into_owned();
        owned.as_mut_slice()[group_offset..group_offset + size_of::<u64>()].fill(0);
        Message::try_from(owned).expect("retyped request message must stay valid")
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
                found: Command::Reserved,
            });
        }

        let header = bytemuck::checked::try_from_bytes::<H>(&bytes[..size_of::<H>()])
            .map_err(|_| classify_failed_cast::<H>(bytes))?;
        header.validate()?;

        // `size` is the whole-frame length and must at least span the header, or
        // a consumer slicing `[size_of::<H>()..size]` underflows (start > end).
        // Every consensus header is `HEADER_SIZE`, so this floor also covers a
        // later `try_into_typed` conversion.
        if (header.size() as usize) < size_of::<H>() {
            return Err(ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: Command::Reserved,
            });
        }

        if bytes.len() < header.size() as usize {
            return Err(ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: Command::Reserved,
            });
        }

        Ok(unsafe { Self::from_backing_unchecked(RequestBacking { owned }) })
    }
}

impl<H> TryFrom<ResponseFragments> for Message<H, ResponseBacking>
where
    H: ConsensusHeader,
{
    type Error = ConsensusError;

    fn try_from(fragments: ResponseFragments) -> Result<Self, Self::Error> {
        let Some(first) = fragments.first() else {
            return Err(ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: Command::Reserved,
            });
        };

        if first.len() < size_of::<H>() {
            return Err(ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: Command::Reserved,
            });
        }

        let header = bytemuck::checked::try_from_bytes::<H>(&first.as_slice()[..size_of::<H>()])
            .map_err(|_| classify_failed_cast::<H>(first.as_slice()))?;
        header.validate()?;

        // See `TryFrom<Owned>`: `size` must at least span the header so a
        // `[size_of::<H>()..size]` body slice cannot underflow.
        if (header.size() as usize) < size_of::<H>() {
            return Err(ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: Command::Reserved,
            });
        }

        let total_len = fragments.iter().map(Frozen::len).sum::<usize>();
        if total_len < header.size() as usize {
            return Err(ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: Command::Reserved,
            });
        }

        Ok(unsafe { Self::from_backing_unchecked(ResponseBacking { fragments }) })
    }
}

#[derive(Debug)]
pub enum MessageBag {
    Request(Message<RoutedRequestHeader>),
    Prepare(Message<PrepareHeader>),
    PrepareOk(Message<PrepareOkHeader>),
    StartViewChange(Message<StartViewChangeHeader>),
    DoViewChange(Message<DoViewChangeHeader>),
    StartView(Message<StartViewHeader>),
    Commit(Message<CommitHeader>),
    RequestStartView(Message<RequestStartViewHeader>),
    RequestPrepares(Message<RequestPreparesHeader>),
    /// A journaled prepare re-sent verbatim for repair (command byte
    /// distinguishes it from a live `Prepare`: repair bypasses the view
    /// fence and never acks). Stays typed as `RepairPrepare` through every
    /// parse -- the router round-trips frames through generic bytes, so a
    /// parse-time byte restore would surface as a live `Prepare` on the
    /// second pass and die on the view fence.
    RepairPrepare(Message<RepairPrepareHeader>),
    /// `RepairDone` / `RangeEvicted` (one layout, two commands).
    RepairRangeReply(Message<RepairRangeReplyHeader>),
    RequestStateTransfer(Message<RequestStateTransferHeader>),
    StateTransferTarget(Message<StateTransferTargetHeader>),
    RequestStateChunk(Message<RequestStateChunkHeader>),
    /// Artifact bytes ride the body (`size` spans header + payload).
    StateChunk(Message<StateChunkHeader>),
    /// A backup relays a login it authenticated locally to the primary, which
    /// owns the `Register` proposal.
    ForwardRegister(Message<ForwardRegisterHeader>),
    /// The primary's verdict, routed back to the parked login by nonce.
    ForwardRegisterResult(Message<ForwardRegisterResultHeader>),
    /// A backup asks the primary to commit a session teardown.
    ForwardLogout(Message<ForwardLogoutHeader>),
    /// The primary's verdict, routed back to the parked logout by nonce.
    ForwardLogoutResult(Message<ForwardLogoutResultHeader>),
}

impl MessageBag {
    /// `(operation, group)`: everything the shard router needs to pick a
    /// target, read off the already-typed header without consuming the bag.
    ///
    /// `group` is a plain field on every consensus header rather than a
    /// [`ConsensusHeader`] method, which is why this is a match and not a trait
    /// call. `RepairPrepare` reads through its wrapped prepare.
    ///
    /// One `header()` per arm, bound to a local: each call runs a checked bytemuck
    /// cast over the 256-byte header, and this is the per-frame routing path.
    #[must_use]
    pub fn routing(&self) -> (Operation, u64) {
        match self {
            Self::Request(message) => {
                let header = message.header();
                (header.operation, header.group)
            }
            Self::Prepare(message) => {
                let header = message.header();
                (header.operation, header.group)
            }
            Self::PrepareOk(message) => {
                let header = message.header();
                (header.operation, header.group)
            }
            Self::RepairPrepare(message) => {
                let header = &message.header().0;
                (header.operation, header.group)
            }
            Self::StartViewChange(message) => {
                let header = message.header();
                (header.operation(), header.group)
            }
            Self::DoViewChange(message) => {
                let header = message.header();
                (header.operation(), header.group)
            }
            Self::StartView(message) => {
                let header = message.header();
                (header.operation(), header.group)
            }
            Self::Commit(message) => {
                let header = message.header();
                (header.operation(), header.group)
            }
            Self::RequestStartView(message) => {
                let header = message.header();
                (header.operation(), header.group)
            }
            Self::RequestPrepares(message) => {
                let header = message.header();
                (header.operation(), header.group)
            }
            Self::RepairRangeReply(message) => {
                let header = message.header();
                (header.operation(), header.group)
            }
            Self::RequestStateTransfer(message) => {
                let header = message.header();
                (header.operation(), header.group)
            }
            Self::StateTransferTarget(message) => {
                let header = message.header();
                (header.operation(), header.group)
            }
            Self::RequestStateChunk(message) => {
                let header = message.header();
                (header.operation(), header.group)
            }
            Self::StateChunk(message) => {
                let header = message.header();
                (header.operation(), header.group)
            }
            // Register forwarding is a metadata-plane errand, and the metadata
            // consensus group lives on shard 0 on every node; the headers carry
            // no group field because there is nothing else they could address.
            Self::ForwardRegister(message) => (message.header().operation(), METADATA_GROUP),
            Self::ForwardRegisterResult(message) => (message.header().operation(), METADATA_GROUP),
            Self::ForwardLogout(message) => (message.header().operation(), METADATA_GROUP),
            Self::ForwardLogoutResult(message) => (message.header().operation(), METADATA_GROUP),
        }
    }

    /// Discard the classification and hand back the underlying frame.
    ///
    /// Type-erasure only: the backing bytes are untouched, so a later
    /// [`MessageBag::try_from`] reclassifies to the same variant. Callers that
    /// need the frame in a generic container (the parked-frame buffer) use this;
    /// the dispatch path keeps the bag so it never re-parses.
    #[must_use]
    pub fn into_generic(self) -> Message<GenericHeader> {
        match self {
            Self::Request(message) => message.into_generic(),
            Self::Prepare(message) => message.into_generic(),
            Self::PrepareOk(message) => message.into_generic(),
            Self::StartViewChange(message) => message.into_generic(),
            Self::DoViewChange(message) => message.into_generic(),
            Self::StartView(message) => message.into_generic(),
            Self::Commit(message) => message.into_generic(),
            Self::RequestStartView(message) => message.into_generic(),
            Self::RequestPrepares(message) => message.into_generic(),
            Self::RepairPrepare(message) => message.into_generic(),
            Self::RepairRangeReply(message) => message.into_generic(),
            Self::RequestStateTransfer(message) => message.into_generic(),
            Self::StateTransferTarget(message) => message.into_generic(),
            Self::RequestStateChunk(message) => message.into_generic(),
            Self::StateChunk(message) => message.into_generic(),
            Self::ForwardRegister(message) => message.into_generic(),
            Self::ForwardRegisterResult(message) => message.into_generic(),
            Self::ForwardLogout(message) => message.into_generic(),
            Self::ForwardLogoutResult(message) => message.into_generic(),
        }
    }

    #[must_use]
    pub fn command(&self) -> Command {
        match self {
            Self::Request(message) => message.header().command,
            Self::Prepare(message) => message.header().command,
            Self::PrepareOk(message) => message.header().command,
            Self::StartViewChange(message) => message.header().command,
            Self::DoViewChange(message) => message.header().command,
            Self::StartView(message) => message.header().command,
            Self::Commit(message) => message.header().command,
            Self::RequestStartView(message) => message.header().command,
            Self::RequestPrepares(message) => message.header().command,
            Self::RepairPrepare(message) => message.header().command(),
            Self::RepairRangeReply(message) => message.header().command,
            Self::RequestStateTransfer(message) => message.header().command,
            Self::StateTransferTarget(message) => message.header().command,
            Self::RequestStateChunk(message) => message.header().command,
            Self::StateChunk(message) => message.header().command,
            Self::ForwardRegister(message) => message.header().command,
            Self::ForwardRegisterResult(message) => message.header().command,
            Self::ForwardLogout(message) => message.header().command,
            Self::ForwardLogoutResult(message) => message.header().command,
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
            Self::RequestStartView(message) => message.header().size(),
            Self::RequestPrepares(message) => message.header().size(),
            Self::RepairPrepare(message) => message.header().size(),
            Self::RepairRangeReply(message) => message.header().size(),
            Self::RequestStateTransfer(message) => message.header().size(),
            Self::StateTransferTarget(message) => message.header().size(),
            Self::RequestStateChunk(message) => message.header().size(),
            Self::StateChunk(message) => message.header().size(),
            Self::ForwardRegister(message) => message.header().size(),
            Self::ForwardRegisterResult(message) => message.header().size(),
            Self::ForwardLogout(message) => message.header().size(),
            Self::ForwardLogoutResult(message) => message.header().size(),
        }
    }

    #[must_use]
    pub fn operation(&self) -> Operation {
        match self {
            Self::Request(message) => message.header().operation,
            Self::Prepare(message) => message.header().operation,
            Self::PrepareOk(message) => message.header().operation,
            Self::StartViewChange(message) => message.header().operation(),
            Self::DoViewChange(message) => message.header().operation(),
            Self::StartView(message) => message.header().operation(),
            Self::Commit(message) => message.header().operation(),
            Self::RequestStartView(message) => message.header().operation(),
            Self::RequestPrepares(message) => message.header().operation(),
            Self::RepairPrepare(message) => message.header().operation(),
            Self::RepairRangeReply(message) => message.header().operation(),
            Self::RequestStateTransfer(message) => message.header().operation(),
            Self::StateTransferTarget(message) => message.header().operation(),
            Self::RequestStateChunk(message) => message.header().operation(),
            Self::StateChunk(message) => message.header().operation(),
            Self::ForwardRegister(message) => message.header().operation(),
            Self::ForwardRegisterResult(message) => message.header().operation(),
            Self::ForwardLogout(message) => message.header().operation(),
            Self::ForwardLogoutResult(message) => message.header().operation(),
        }
    }
}

/// Why `H`'s header bytes failed bytemuck's checked cast.
///
/// An operation discriminant this build does not define means the sender runs a
/// newer release; the frame is wire-valid and the node needs upgrading, which is
/// a different operator action from the corrupted-header case. bytemuck reports
/// both as one error, so the operation byte is probed here to separate them.
fn classify_failed_cast<H>(bytes: &[u8]) -> ConsensusError
where
    H: ConsensusHeader,
{
    if let Some(offset) = H::OPERATION_OFFSET
        && let Some(&code) = bytes.get(offset)
        && !Operation::is_known_code(code)
        && operation_byte_is_trustworthy::<H>(bytes)
    {
        return ConsensusError::UnsupportedOperation { operation: code };
    }
    ConsensusError::InvalidBitPattern
}

/// Whether the operation byte may be read as the sender's release rather than
/// as damage.
///
/// This classification runs before `verify_frame`, so the byte is still
/// unverified: a flipped bit landing in an undefined discriminant would
/// otherwise be reported as "upgrade this node", and for a `PrepareOk` --
/// which echoes an operation this primary minted itself -- corruption is the
/// likelier cause anyway. Sealed headers verify the frame checksum; the
/// Prepare family verifies the identity checksum, which covers the operation
/// byte, recomputed over the raw bytes because the unknown discriminant blocks
/// the typed view.
///
/// A `Request` has neither (its `checksum` crosses the client boundary
/// unsealed), so its byte is taken at face value and the error text claims
/// only *likely* skew.
fn operation_byte_is_trustworthy<H>(bytes: &[u8]) -> bool
where
    H: ConsensusHeader,
{
    if !H::FRAME_SEALED && !H::IDENTITY_CHECKSUMMED {
        return true;
    }
    let Some(header) = bytes
        .get(..HEADER_SIZE)
        .and_then(|slice| <&[u8; HEADER_SIZE]>::try_from(slice).ok())
    else {
        return false;
    };
    let Some(stored) = header
        .get(..size_of::<u128>())
        .and_then(|slice| <[u8; size_of::<u128>()]>::try_from(slice).ok())
    else {
        return false;
    };
    let expected = if H::FRAME_SEALED {
        frame_checksum_bytes(header)
    } else {
        prepare_identity_checksum_bytes(header)
    };
    u128::from_le_bytes(stored) == expected
}

impl<T> TryFrom<Message<T>> for MessageBag
where
    T: ConsensusHeader,
{
    type Error = ConsensusError;

    // Dispatch via `try_into_typed::<H>()`: re-runs per-typed `validate()`.
    // `from_backing_unchecked` trusts the command byte alone, letting
    // invariant violations (Commit size != 256, DoViewChange log_view > view)
    // reach the router.
    fn try_from(value: Message<T>) -> Result<Self, Self::Error> {
        let command = value.as_generic().header().command;

        match command {
            Command::Prepare => Ok(Self::Prepare(value.try_into_typed::<PrepareHeader>()?)),
            Command::Request => Ok(Self::Request(
                value.try_into_typed::<RoutedRequestHeader>()?,
            )),
            Command::PrepareOk => Ok(Self::PrepareOk(value.try_into_typed::<PrepareOkHeader>()?)),
            Command::StartViewChange => Ok(Self::StartViewChange(
                value.try_into_typed::<StartViewChangeHeader>()?,
            )),
            Command::DoViewChange => Ok(Self::DoViewChange(
                value.try_into_typed::<DoViewChangeHeader>()?,
            )),
            Command::StartView => Ok(Self::StartView(value.try_into_typed::<StartViewHeader>()?)),
            Command::Commit => Ok(Self::Commit(value.try_into_typed::<CommitHeader>()?)),
            Command::RequestStartView => Ok(Self::RequestStartView(
                value.try_into_typed::<RequestStartViewHeader>()?,
            )),
            Command::RequestPrepares => Ok(Self::RequestPrepares(
                value.try_into_typed::<RequestPreparesHeader>()?,
            )),
            // A repaired prepare is a stored PrepareHeader frame whose command
            // byte was rewritten; typed validation would reject the byte, so
            // parse through the generic backing and trust the prepare-shaped
            // layout the way the journal that produced it did.
            Command::RepairPrepare => Ok(Self::RepairPrepare(
                value.try_into_typed::<RepairPrepareHeader>()?,
            )),
            Command::RepairDone | Command::RangeEvicted => Ok(Self::RepairRangeReply(
                value.try_into_typed::<RepairRangeReplyHeader>()?,
            )),
            Command::RequestStateTransfer => Ok(Self::RequestStateTransfer(
                value.try_into_typed::<RequestStateTransferHeader>()?,
            )),
            Command::StateTransferTarget => Ok(Self::StateTransferTarget(
                value.try_into_typed::<StateTransferTargetHeader>()?,
            )),
            Command::RequestStateChunk => Ok(Self::RequestStateChunk(
                value.try_into_typed::<RequestStateChunkHeader>()?,
            )),
            Command::StateChunk => Ok(Self::StateChunk(
                value.try_into_typed::<StateChunkHeader>()?,
            )),
            Command::ForwardRegister => Ok(Self::ForwardRegister(
                value.try_into_typed::<ForwardRegisterHeader>()?,
            )),
            Command::ForwardRegisterResult => Ok(Self::ForwardRegisterResult(
                value.try_into_typed::<ForwardRegisterResultHeader>()?,
            )),
            Command::ForwardLogout => Ok(Self::ForwardLogout(
                value.try_into_typed::<ForwardLogoutHeader>()?,
            )),
            Command::ForwardLogoutResult => Ok(Self::ForwardLogoutResult(
                value.try_into_typed::<ForwardLogoutResultHeader>()?,
            )),
            // Reply / Eviction are server-to-client frames; they do not
            // appear on the inbound dispatch path.
            Command::Reply | Command::Eviction => Err(ConsensusError::ClientBoundCommand(command)),
            other => Err(ConsensusError::InvalidCommand {
                expected: Command::Reserved,
                found: other,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::{
        ForwardLogoutHeader, ForwardLogoutOutcome, ForwardLogoutResultHeader, HEADER_SIZE,
        Operation, ReplyHeader, RequestHeader, frame_checksum_bytes,
    };
    use smallvec::smallvec;

    // Field offsets via `offset_of!`: a field reorder fails to compile here
    // rather than silently corrupting test bytes.
    const SIZE_OFF: usize = std::mem::offset_of!(RoutedRequestHeader, size);
    const COMMAND_OFF: usize = std::mem::offset_of!(RoutedRequestHeader, command);
    const REQUEST_CLIENT_OFF: usize = std::mem::offset_of!(RoutedRequestHeader, client);
    const REQUEST_OPERATION_OFF: usize = std::mem::offset_of!(RoutedRequestHeader, operation);
    const REQUEST_SESSION_OFF: usize = std::mem::offset_of!(RoutedRequestHeader, session);
    const REQUEST_REQUEST_OFF: usize = std::mem::offset_of!(RoutedRequestHeader, request);

    fn header_bytes(command: Command, size: u32) -> Owned<MESSAGE_ALIGN> {
        header_bytes_sized(command, size, 256)
    }

    fn header_bytes_sized(command: Command, size: u32, buffer_len: usize) -> Owned<MESSAGE_ALIGN> {
        let mut o = Owned::<MESSAGE_ALIGN>::zeroed(buffer_len);
        {
            let buf = o.as_mut_slice();
            buf[SIZE_OFF..SIZE_OFF + 4].copy_from_slice(&size.to_le_bytes());
            buf[COMMAND_OFF] = command as u8;
            // Typed headers reject client == 0. `#[repr(C)]` preamble layout
            // is shared, so this offset works across header types.
            buf[REQUEST_CLIENT_OFF..REQUEST_CLIENT_OFF + 16]
                .copy_from_slice(&0xCAFE_u128.to_le_bytes());
            // A zeroed operation is `Reserved`, which validation rejects.
            // `Register` needs session 0 and request 0, which zeroed bytes
            // already satisfy.
            buf[REQUEST_OPERATION_OFF] = Operation::Register as u8;
            seal_header_bytes(buf);
        }
        o
    }

    /// Seal a hand-built frame the way a real sender does.
    ///
    /// Control headers are rejected on the typed parse unless `checksum` covers the
    /// rest of the header, so a fixture that skips this tests the rejection path.
    fn seal_header_bytes(buf: &mut [u8]) {
        let header: &[u8; HEADER_SIZE] = buf[..HEADER_SIZE].try_into().expect("frame is a header");
        let checksum = frame_checksum_bytes(header);
        buf[..size_of::<u128>()].copy_from_slice(&checksum.to_le_bytes());
    }

    /// A `DoViewChange` frame carrying a one-entry suffix, sealed.
    ///
    /// One entry rather than none because a bitset bit is only legal within the
    /// suffix, so an empty frame cannot express the attack this seals against.
    fn sealed_do_view_change() -> Owned<MESSAGE_ALIGN> {
        const DVC_SIZE: usize = HEADER_SIZE * 2;
        let mut owned = Owned::<MESSAGE_ALIGN>::zeroed(DVC_SIZE);
        {
            let buf = owned.as_mut_slice();
            buf[SIZE_OFF..SIZE_OFF + 4].copy_from_slice(&(DVC_SIZE as u32).to_le_bytes());
            buf[COMMAND_OFF] = Command::DoViewChange as u8;
            seal_header_bytes(buf);
        }
        owned
    }

    /// An operation byte from a newer release, on a frame whose seal proves the
    /// byte arrived as the sender wrote it.
    fn sealed_prepare_ok_with_operation(operation: u8) -> Owned<MESSAGE_ALIGN> {
        let mut owned = Owned::<MESSAGE_ALIGN>::zeroed(HEADER_SIZE);
        {
            let buf = owned.as_mut_slice();
            buf[SIZE_OFF..SIZE_OFF + 4].copy_from_slice(&(HEADER_SIZE as u32).to_le_bytes());
            buf[COMMAND_OFF] = Command::PrepareOk as u8;
            buf[offset_of!(PrepareOkHeader, operation)] = operation;
            seal_header_bytes(buf);
        }
        owned
    }

    const OPERATION_FROM_A_NEWER_RELEASE: u8 = 0xEE;

    #[test]
    fn given_a_sealed_frame_when_its_operation_is_unknown_should_report_version_skew() {
        let owned = sealed_prepare_ok_with_operation(OPERATION_FROM_A_NEWER_RELEASE);

        assert!(matches!(
            classify_failed_cast::<PrepareOkHeader>(owned.as_slice()),
            ConsensusError::UnsupportedOperation {
                operation: OPERATION_FROM_A_NEWER_RELEASE
            }
        ));
    }

    // The classification runs before `verify_frame`, so on a sealed header the
    // operation byte is unverified. A flipped bit landing in an undefined
    // discriminant must not be reported as "the sender runs a newer release":
    // that sends an operator upgrading a node over a corrupt link, and a
    // `PrepareOk` echoes an operation this primary minted itself, so corruption
    // is the likelier cause to begin with.
    #[test]
    fn given_a_corrupt_sealed_frame_when_its_operation_is_unknown_should_not_claim_version_skew() {
        let mut owned = sealed_prepare_ok_with_operation(0);
        // Flip the operation byte AFTER sealing, the way damage in flight does.
        owned.as_mut_slice()[offset_of!(PrepareOkHeader, operation)] =
            OPERATION_FROM_A_NEWER_RELEASE;

        assert!(matches!(
            classify_failed_cast::<PrepareOkHeader>(owned.as_slice()),
            ConsensusError::InvalidBitPattern
        ));
    }

    /// A Prepare frame stamped the way a real sender stamps it: `checksum`
    /// carries the prepare identity, which covers the operation byte.
    fn identity_stamped_prepare(command: Command, operation: u8) -> Owned<MESSAGE_ALIGN> {
        let mut owned = Owned::<MESSAGE_ALIGN>::zeroed(HEADER_SIZE);
        {
            let buf = owned.as_mut_slice();
            buf[SIZE_OFF..SIZE_OFF + 4].copy_from_slice(&(HEADER_SIZE as u32).to_le_bytes());
            buf[COMMAND_OFF] = command as u8;
            buf[offset_of!(PrepareHeader, operation)] = operation;
            let header: &[u8; HEADER_SIZE] =
                buf[..HEADER_SIZE].try_into().expect("frame is a header");
            let identity = prepare_identity_checksum_bytes(header);
            buf[..size_of::<u128>()].copy_from_slice(&identity.to_le_bytes());
        }
        owned
    }

    // A prepare's `checksum` carries its identity, which covers the operation
    // byte, so an intact identity authenticates the unknown byte as the
    // sender's release rather than damage.
    #[test]
    fn given_an_identity_stamped_prepare_when_its_operation_is_unknown_should_report_version_skew()
    {
        let owned = identity_stamped_prepare(Command::Prepare, OPERATION_FROM_A_NEWER_RELEASE);

        const { assert!(!PrepareHeader::FRAME_SEALED) };
        assert!(matches!(
            classify_failed_cast::<PrepareHeader>(owned.as_slice()),
            ConsensusError::UnsupportedOperation {
                operation: OPERATION_FROM_A_NEWER_RELEASE
            }
        ));
    }

    // The identity was computed over a DIFFERENT operation byte, so the flip
    // happened after stamping: damage, not a release.
    #[test]
    fn given_a_corrupt_prepare_when_its_operation_is_unknown_should_not_claim_version_skew() {
        let mut owned = identity_stamped_prepare(Command::Prepare, 0);
        owned.as_mut_slice()[offset_of!(PrepareHeader, operation)] = OPERATION_FROM_A_NEWER_RELEASE;

        assert!(matches!(
            classify_failed_cast::<PrepareHeader>(owned.as_slice()),
            ConsensusError::InvalidBitPattern
        ));
    }

    // A client `Request` has neither a frame seal nor a prepare identity, so
    // its operation byte is taken at face value; the error text claims only
    // likely skew for exactly this reason.
    #[test]
    fn given_a_client_request_when_its_operation_is_unknown_should_report_version_skew() {
        let mut owned = Owned::<MESSAGE_ALIGN>::zeroed(HEADER_SIZE);
        {
            let buf = owned.as_mut_slice();
            buf[SIZE_OFF..SIZE_OFF + 4].copy_from_slice(&(HEADER_SIZE as u32).to_le_bytes());
            buf[COMMAND_OFF] = Command::Request as u8;
            buf[offset_of!(RequestHeader, operation)] = OPERATION_FROM_A_NEWER_RELEASE;
        }

        const { assert!(!RequestHeader::FRAME_SEALED && !RequestHeader::IDENTITY_CHECKSUMMED) };
        assert!(matches!(
            classify_failed_cast::<RequestHeader>(owned.as_slice()),
            ConsensusError::UnsupportedOperation {
                operation: OPERATION_FROM_A_NEWER_RELEASE
            }
        ));
    }

    // Transparent over `PrepareHeader`, so a repaired prepare from a newer
    // release must reach the same fence instead of reading as corruption.
    #[test]
    fn given_a_repaired_prepare_when_its_operation_is_unknown_should_report_version_skew() {
        assert_eq!(
            RepairPrepareHeader::OPERATION_OFFSET,
            PrepareHeader::OPERATION_OFFSET,
            "the newtype carries the same operation field"
        );
        const { assert!(RepairPrepareHeader::IDENTITY_CHECKSUMMED) };

        let owned =
            identity_stamped_prepare(Command::RepairPrepare, OPERATION_FROM_A_NEWER_RELEASE);

        assert!(matches!(
            classify_failed_cast::<RepairPrepareHeader>(owned.as_slice()),
            ConsensusError::UnsupportedOperation {
                operation: OPERATION_FROM_A_NEWER_RELEASE
            }
        ));
    }

    #[test]
    fn given_a_sealed_do_view_change_when_dispatching_should_accept() {
        let generic = Message::<GenericHeader>::try_from(sealed_do_view_change())
            .expect("a sealed DoViewChange frames correctly");
        assert!(matches!(
            MessageBag::try_from(generic),
            Ok(MessageBag::DoViewChange(_))
        ));
    }

    #[test]
    fn given_a_flipped_nack_bit_when_dispatching_should_reject_the_frame() {
        // Why the header seal exists. `validate` accepts this frame: the bit sits
        // inside the one-entry suffix, where a legitimate nack lives. Downstream the
        // bitset goes to the merge unchanged and authorises truncating a committed op.
        const NACK_OFF: usize = std::mem::offset_of!(DoViewChangeHeader, nack_bitset);

        let mut owned = sealed_do_view_change();
        owned.as_mut_slice()[NACK_OFF] ^= 0x01;

        let generic =
            Message::<GenericHeader>::try_from(owned).expect("framing does not inspect the bitset");
        assert!(
            matches!(
                MessageBag::try_from(generic),
                Err(ConsensusError::FrameChecksumMismatch { .. })
            ),
            "a manufactured nack must not reach the merge"
        );
    }

    // MessageBag round-trip for the probe + repair command family. Locks
    // RangeEvicted delivery in particular: RepairDone and RangeEvicted share
    // one header layout and BOTH must survive the typed parse -- a strict
    // command match in `try_into_typed` silently dropped every RangeEvicted
    // frame and with it the whole commit-floor path.
    #[test]
    fn probe_and_repair_commands_round_trip_into_bag() {
        for command in [
            Command::RequestStartView,
            Command::RequestPrepares,
            Command::RepairPrepare,
            Command::RepairDone,
            Command::RangeEvicted,
        ] {
            let mut owned = header_bytes(command, 256);
            if command == Command::RequestPrepares {
                // validate() demands a non-empty 1-based range.
                const FROM_OP_OFF: usize =
                    std::mem::offset_of!(iggy_binary_protocol::RequestPreparesHeader, from_op);
                const TO_OP_OFF: usize =
                    std::mem::offset_of!(iggy_binary_protocol::RequestPreparesHeader, to_op);
                let buf = owned.as_mut_slice();
                buf[FROM_OP_OFF..FROM_OP_OFF + 8].copy_from_slice(&1u64.to_le_bytes());
                buf[TO_OP_OFF..TO_OP_OFF + 8].copy_from_slice(&1u64.to_le_bytes());
                // Re-seal: the range was written after `header_bytes` sealed.
                seal_header_bytes(buf);
            }
            let generic = Message::<GenericHeader>::try_from(owned)
                .unwrap_or_else(|e| panic!("{command:?} failed generic framing: {e}"));
            let bag = MessageBag::try_from(generic)
                .unwrap_or_else(|e| panic!("{command:?} failed bag parse: {e}"));
            let routed = matches!(
                (&bag, command),
                (MessageBag::RequestStartView(_), Command::RequestStartView)
                    | (MessageBag::RequestPrepares(_), Command::RequestPrepares)
                    | (MessageBag::RepairPrepare(_), Command::RepairPrepare)
                    | (
                        MessageBag::RepairRangeReply(_),
                        Command::RepairDone | Command::RangeEvicted
                    )
            );
            assert!(routed, "{command:?} parsed into the wrong bag variant");
            assert_eq!(bag.command(), command, "original command byte must survive");
        }
    }

    #[test]
    fn forward_logout_commands_round_trip_into_bag() {
        let forward = Message::<ForwardLogoutHeader>::new(HEADER_SIZE).transmute_header(
            |_, header: &mut ForwardLogoutHeader| {
                header.command = Command::ForwardLogout;
                header.size = HEADER_SIZE as u32;
                header.client = 7;
                header.nonce = 8;
                header.session = 9;
                header.request = 10;
                header.seal();
            },
        );
        let result = Message::<ForwardLogoutResultHeader>::new(HEADER_SIZE).transmute_header(
            |_, header: &mut ForwardLogoutResultHeader| {
                header.command = Command::ForwardLogoutResult;
                header.size = HEADER_SIZE as u32;
                header.client = 7;
                header.nonce = 8;
                header.commit = 11;
                header.outcome = ForwardLogoutOutcome::Ok;
                header.seal();
            },
        );

        let forward = MessageBag::try_from(forward.into_generic()).expect("parse ForwardLogout");
        let result =
            MessageBag::try_from(result.into_generic()).expect("parse ForwardLogoutResult");
        assert!(matches!(forward, MessageBag::ForwardLogout(_)));
        assert!(matches!(result, MessageBag::ForwardLogoutResult(_)));
        assert_eq!(forward.command(), Command::ForwardLogout);
        assert_eq!(result.command(), Command::ForwardLogoutResult);
        assert_eq!(forward.operation(), Operation::Reserved);
        assert_eq!(result.size(), HEADER_SIZE as u32);
    }

    // Construction via Message::new (zeroed)

    #[test]
    #[should_panic(expected = "size must be at least header size")]
    fn message_new_smaller_than_header_panics() {
        let _ = Message::<RoutedRequestHeader>::new(100);
    }

    // try_from(Owned): validation gates the unsafe construction

    #[test]
    fn try_from_owned_too_short_returns_err() {
        let owned = Owned::<MESSAGE_ALIGN>::zeroed(100);
        let result = Message::<RoutedRequestHeader>::try_from(owned);
        assert!(matches!(result, Err(ConsensusError::InvalidCommand { .. })));
    }

    #[test]
    fn try_from_owned_invalid_bit_pattern_returns_err() {
        let mut owned = Owned::<MESSAGE_ALIGN>::zeroed(256);
        owned.as_mut_slice()[COMMAND_OFF] = 99; // outside Command's discriminant range
        let result = Message::<RoutedRequestHeader>::try_from(owned);
        assert!(matches!(result, Err(ConsensusError::InvalidBitPattern)));
    }

    #[test]
    fn try_from_owned_buffer_shorter_than_claimed_size_returns_err() {
        // Header parses cleanly (RoutedRequestHeader::validate doesn't gate on
        // size), but the encoded `size` field claims more bytes than the
        // backing buffer holds. The buffer-bounds check at the bottom of
        // `Message::try_from` must reject. (Both this case and the
        // "buffer shorter than `size_of::<H>`" case currently surface as
        // the same `InvalidCommand` variant; promoting them to distinct
        // `ConsensusError` variants is a separate hardening pass.)
        let owned = header_bytes(Command::Request, 999);
        // header_bytes already produces a 256-byte buffer; size=999 > 256,
        // so try_from rejects via `bytes.len() < header.size()`.
        let result = Message::<RoutedRequestHeader>::try_from(owned);
        assert!(matches!(result, Err(ConsensusError::InvalidCommand { .. })));
    }

    #[test]
    fn try_from_owned_size_below_header_size_returns_err() {
        // `size` claims a frame smaller than the header. The buffer is full-size,
        // so only the construction-time `size` floor rejects it (the
        // buffer-length check passes). Guards the `[size_of::<H>()..size]`
        // underflow at every downstream call site.
        let owned = header_bytes(
            Command::Request,
            size_of::<RoutedRequestHeader>() as u32 - 1,
        );
        let result = Message::<RoutedRequestHeader>::try_from(owned);
        assert!(matches!(result, Err(ConsensusError::InvalidCommand { .. })));
    }

    // as_generic: const unsafe pointer cast (#[repr(C)] equivalence)

    #[test]
    fn as_generic_view_reads_command_byte() {
        let owned = header_bytes(Command::Request, 256);
        let typed = Message::<RoutedRequestHeader>::try_from(owned).expect("valid");
        let generic = typed.as_generic();
        assert_eq!(generic.header().command, Command::Request);
        assert_eq!(generic.total_len(), 256);
    }

    // body(): the slice every wire decode reads from

    #[test]
    fn body_is_the_bytes_between_the_header_and_the_frame_size() {
        // A 512-byte allocation holding a 260-byte frame: the accessor follows the
        // header's `size`, never the buffer that happens to hold it.
        const BODY: [u8; 4] = [1, 2, 3, 4];
        let frame_size = size_of::<GenericHeader>() + BODY.len();
        let mut owned = header_bytes_sized(Command::Prepare, frame_size as u32, 512);
        owned.as_mut_slice()[size_of::<GenericHeader>()..frame_size].copy_from_slice(&BODY);

        let message = Message::<GenericHeader>::try_from(owned).expect("valid generic");
        assert_eq!(message.body(), BODY);
    }

    #[test]
    fn body_is_empty_for_a_header_only_frame() {
        // Header-only commands go through the same accessor, so `size` equal to the
        // header must yield an empty slice rather than an inverted-range panic.
        let owned = header_bytes_sized(Command::Prepare, size_of::<GenericHeader>() as u32, 512);
        let message = Message::<GenericHeader>::try_from(owned).expect("valid generic");
        assert!(message.body().is_empty());
    }

    // try_as_typed: validation gates the unsafe ptr-cast reborrow

    #[test]
    fn try_as_typed_command_mismatch_returns_err_without_unsafe_cast() {
        // bytes are a valid Prepare; asking for RoutedRequestHeader must fail
        // *before* the unsafe ptr-cast inside try_as_typed.
        let owned = header_bytes(Command::Prepare, 256);
        let generic = Message::<GenericHeader>::try_from(owned).expect("valid");
        let result = generic.try_as_typed::<RoutedRequestHeader>();
        assert!(matches!(
            result,
            Err(ConsensusError::InvalidCommand {
                expected: Command::Request,
                found: Command::Prepare,
            })
        ));
    }

    #[test]
    fn try_as_typed_invalid_validation_returns_err() {
        // `RequestHeader::validate` rejects operation=Register with non-zero
        // session; the routed shape shares the same field rules.
        let mut owned = header_bytes(Command::Request, 256);
        {
            let buf = owned.as_mut_slice();
            buf[REQUEST_OPERATION_OFF] = Operation::Register as u8;
            buf[REQUEST_SESSION_OFF..REQUEST_SESSION_OFF + 8].copy_from_slice(&5u64.to_le_bytes());
        }
        let generic = Message::<GenericHeader>::try_from(owned).expect("valid generic");
        let result = generic.try_as_typed::<RequestHeader>();
        assert!(matches!(result, Err(ConsensusError::InvalidField(_))));
    }

    // try_into_typed: consuming variant of try_as_typed

    #[test]
    fn try_into_typed_command_mismatch_returns_err() {
        let owned = header_bytes(Command::Prepare, 256);
        let generic = Message::<GenericHeader>::try_from(owned).expect("valid");
        let result = generic.try_into_typed::<RoutedRequestHeader>();
        assert!(matches!(
            result,
            Err(ConsensusError::InvalidCommand {
                expected: Command::Request,
                found: Command::Prepare,
            })
        ));
    }

    // MessageBag dispatch: 7 unsafe `from_backing_unchecked` arms

    fn dispatch(command: Command, size: u32) -> Result<MessageBag, ConsensusError> {
        let owned = header_bytes(command, size);
        let generic = Message::<GenericHeader>::try_from(owned).expect("valid generic");
        MessageBag::try_from(generic)
    }

    #[test]
    fn messagebag_dispatch_unsupported_command_returns_err() {
        // Ping is a valid Command bit pattern but is not a MessageBag variant.
        let owned = header_bytes(Command::Ping, 256);
        let generic = Message::<GenericHeader>::try_from(owned).expect("valid generic");
        let result = MessageBag::try_from(generic);
        assert!(matches!(result, Err(ConsensusError::InvalidCommand { .. })));
    }

    #[test]
    fn messagebag_command_method_round_trips() {
        for cmd in [
            Command::Request,
            Command::Prepare,
            Command::PrepareOk,
            Command::StartViewChange,
            Command::DoViewChange,
            Command::StartView,
            Command::Commit,
        ] {
            let bag = dispatch(cmd, 256).expect("dispatch");
            assert_eq!(bag.command(), cmd, "round-trip for {cmd:?}");
            assert_eq!(bag.size(), 256, "size for {cmd:?}");
        }
    }

    // MessageBag dispatch must enforce per-typed validate()
    // (Commit size != 256, Register with session != 0, etc.)

    #[test]
    fn messagebag_dispatch_commit_with_invalid_size_returns_err() {
        // `CommitHeader::validate` rejects size != 256. Use size 300 (> header
        // size) with a 512-byte buffer so the frame clears generic parse and the
        // `size` floor, reaching typed dispatch. A size below the header size is
        // now rejected earlier by the floor (see
        // `try_from_owned_size_below_header_size_returns_err`).
        let owned = header_bytes_sized(Command::Commit, 300, 512);
        let generic = Message::<GenericHeader>::try_from(owned).expect("valid generic");
        let result = MessageBag::try_from(generic);
        assert!(matches!(
            result,
            Err(ConsensusError::CommitInvalidSize(300))
        ));
    }

    #[test]
    fn client_wire_decode_of_request_with_invalid_register_session_returns_err() {
        // `RequestHeader::validate` rejects Register with non-zero session.
        let mut owned = header_bytes(Command::Request, 256);
        {
            let buf = owned.as_mut_slice();
            buf[REQUEST_OPERATION_OFF] = Operation::Register as u8;
            buf[REQUEST_SESSION_OFF..REQUEST_SESSION_OFF + 8].copy_from_slice(&5u64.to_le_bytes());
        }
        let generic = Message::<GenericHeader>::try_from(owned).expect("valid generic");
        let result = generic.try_into_typed::<RequestHeader>();
        assert!(matches!(result, Err(ConsensusError::InvalidField(_))));
    }

    // Ingress validation runs on every client frame at the network boundary,
    // reached through `try_into_typed` -> `RequestHeader::validate` before
    // dispatch promotes the frame to `RoutedRequestHeader`. Several dedup and
    // authz conclusions rest on it running, so pin the field rules rather than
    // the plumbing: whatever `request_preflight` and the operation gate see
    // downstream has already passed these.
    #[test]
    fn ingress_validation_enforces_the_request_header_field_rules() {
        // (operation, session, request, must_pass)
        let cases: [(Operation, u64, u64, bool); 8] = [
            // Register carries no session or request of its own.
            (Operation::Register, 0, 0, true),
            (Operation::Register, 1, 0, false),
            (Operation::Register, 0, 1, false),
            // Replicated client ops need both.
            (Operation::CreateStream, 1, 1, true),
            (Operation::CreateStream, 0, 1, false),
            (Operation::CreateStream, 1, 0, false),
            // Sessionless by design: ping must work before authentication.
            (Operation::NonReplicated, 0, 0, true),
            // Never a real client op; refused before the dedup preflight can
            // replay this client's own cached reply back to it.
            (Operation::Reserved, 1, 1, false),
        ];

        for (operation, session, request, must_pass) in cases {
            let mut owned = header_bytes(Command::Request, 256);
            {
                let buf = owned.as_mut_slice();
                buf[REQUEST_OPERATION_OFF] = operation as u8;
                buf[REQUEST_SESSION_OFF..REQUEST_SESSION_OFF + 8]
                    .copy_from_slice(&session.to_le_bytes());
                buf[REQUEST_REQUEST_OFF..REQUEST_REQUEST_OFF + 8]
                    .copy_from_slice(&request.to_le_bytes());
            }
            let generic = Message::<GenericHeader>::try_from(owned).expect("valid generic");
            let accepted = generic.try_into_typed::<RequestHeader>().is_ok();
            assert_eq!(
                accepted, must_pass,
                "{operation:?} with session={session} request={request}"
            );
        }
    }

    // `client == 0` is reserved for server-originated ops; the table asserts on
    // it, so ingress must reject it before it can reach the preflight.
    #[test]
    fn ingress_validation_rejects_zero_client() {
        let mut owned = header_bytes(Command::Request, 256);
        {
            let buf = owned.as_mut_slice();
            buf[REQUEST_OPERATION_OFF] = Operation::CreateStream as u8;
            buf[REQUEST_CLIENT_OFF..REQUEST_CLIENT_OFF + 16].copy_from_slice(&0u128.to_le_bytes());
            buf[REQUEST_SESSION_OFF..REQUEST_SESSION_OFF + 8].copy_from_slice(&1u64.to_le_bytes());
            buf[REQUEST_REQUEST_OFF..REQUEST_REQUEST_OFF + 8].copy_from_slice(&1u64.to_le_bytes());
        }
        let generic = Message::<GenericHeader>::try_from(owned).expect("valid generic");
        assert!(matches!(
            generic.try_into_typed::<RequestHeader>(),
            Err(ConsensusError::InvalidField(_))
        ));
    }

    // deep_copy: byte-level independence after clone-like API

    #[test]
    fn request_message_deep_copy_independent() {
        let owned = header_bytes(Command::Request, 256);
        let mut msg = Message::<RoutedRequestHeader>::try_from(owned).expect("valid");
        let copy = msg.deep_copy();
        // Mutate the original's bytes; the deep copy must be untouched.
        msg.as_mut_slice()[200] = 0xab;
        assert_eq!(copy.as_slice()[200], 0);
        assert_eq!(msg.as_slice()[200], 0xab);
    }

    // transmute_header: rewrites the typed header in place

    #[test]
    fn transmute_header_request_to_prepare() {
        let owned = header_bytes(Command::Request, 256);
        let msg = Message::<RoutedRequestHeader>::try_from(owned).expect("valid");
        let prepared: Message<PrepareHeader> =
            msg.transmute_header::<PrepareHeader>(|_old, new| {
                new.command = Command::Prepare;
                new.size = 256;
            });
        assert_eq!(prepared.header().command, Command::Prepare);
    }

    // into_routed: in-place client-wire -> routed retype

    // Promotion must carry the data-bearing reserved prefix verbatim (the
    // non-replicated op code lives in `reserved[0..4]`) and unset only the
    // `group` tail, whatever junk the client sent in those eight bytes.
    #[test]
    fn into_routed_keeps_reserved_prefix_and_unsets_group() {
        const RESERVED_OFF: usize = std::mem::offset_of!(RequestHeader, reserved);

        let mut owned = header_bytes(Command::Request, 256);
        {
            let buf = owned.as_mut_slice();
            for (index, byte) in buf[RESERVED_OFF..RESERVED_OFF + 60].iter_mut().enumerate() {
                *byte = u8::try_from(index).expect("60 fits u8") + 1;
            }
        }
        let request = Message::<RequestHeader>::try_from(owned).expect("valid client frame");
        let client_header = *request.header();

        let routed = request.into_routed();
        let header = routed.header();
        assert_eq!(
            header.reserved[..],
            client_header.reserved[..52],
            "the reserved prefix carries data and must survive promotion"
        );
        assert_eq!(
            header.group, 0,
            "the client-sent reserved tail must not leak into `group`"
        );
        assert_eq!(header.client, client_header.client);
        assert_eq!(header.operation, client_header.operation);
        assert_eq!(header.session, client_header.session);
        assert_eq!(header.request, client_header.request);
        assert_eq!(header.user_id, client_header.user_id);
    }

    // A peer-wire `Command::Request` decodes as `RoutedRequestHeader`, so its
    // validate must enforce the client-boundary field rules: a forged
    // `client = 0` frame would otherwise reach the client table's hard assert
    // and abort the metadata primary, and a `Reserved` operation would replay
    // that client's cached register reply.
    #[test]
    fn messagebag_dispatch_rejects_request_with_zero_client() {
        let mut owned = header_bytes(Command::Request, 256);
        owned.as_mut_slice()[REQUEST_CLIENT_OFF..REQUEST_CLIENT_OFF + 16]
            .copy_from_slice(&0u128.to_le_bytes());
        let generic = Message::<GenericHeader>::try_from(owned).expect("valid generic");
        assert!(matches!(
            MessageBag::try_from(generic),
            Err(ConsensusError::InvalidField(_))
        ));
    }

    #[test]
    fn messagebag_dispatch_rejects_request_with_reserved_operation() {
        let mut owned = header_bytes(Command::Request, 256);
        owned.as_mut_slice()[REQUEST_OPERATION_OFF] = Operation::Reserved as u8;
        let generic = Message::<GenericHeader>::try_from(owned).expect("valid generic");
        assert!(matches!(
            MessageBag::try_from(generic),
            Err(ConsensusError::InvalidField(_))
        ));
    }

    // ResponseBacking via SmallVec<Frozen>

    #[test]
    fn response_backing_single_fragment_roundtrip() {
        let owned = header_bytes(Command::Reply, 256);
        let frozen: Frozen<MESSAGE_ALIGN> = owned.into();
        let fragments: ResponseFragments = smallvec![frozen];
        let msg = Message::<ReplyHeader, ResponseBacking>::try_from(fragments).expect("valid");
        assert_eq!(msg.header().command, Command::Reply);
        assert_eq!(msg.fragments().len(), 1);
    }

    #[test]
    fn response_backing_empty_fragments_returns_err() {
        let fragments: ResponseFragments = smallvec![];
        let result = Message::<ReplyHeader, ResponseBacking>::try_from(fragments);
        assert!(matches!(result, Err(ConsensusError::InvalidCommand { .. })));
    }

    #[test]
    fn response_backing_first_fragment_too_short_returns_err() {
        let owned = Owned::<MESSAGE_ALIGN>::zeroed(100);
        let frozen: Frozen<MESSAGE_ALIGN> = owned.into();
        let fragments: ResponseFragments = smallvec![frozen];
        let result = Message::<ReplyHeader, ResponseBacking>::try_from(fragments);
        assert!(matches!(result, Err(ConsensusError::InvalidCommand { .. })));
    }

    #[test]
    fn response_backing_size_below_header_size_returns_err() {
        // First fragment is full-size, but its `size` field claims less than
        // the header; the floor must reject before any consumer slices a body.
        let owned = header_bytes(Command::Reply, size_of::<ReplyHeader>() as u32 - 1);
        let frozen: Frozen<MESSAGE_ALIGN> = owned.into();
        let fragments: ResponseFragments = smallvec![frozen];
        let result = Message::<ReplyHeader, ResponseBacking>::try_from(fragments);
        assert!(matches!(result, Err(ConsensusError::InvalidCommand { .. })));
    }
}
