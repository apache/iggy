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
use iggy_binary_protocol::{
    Command2, CommitHeader, ConsensusError, ConsensusHeader, DoViewChangeHeader, GenericHeader,
    Operation, PrepareHeader, PrepareOkHeader, RepairPrepareHeader, RepairRangeReplyHeader,
    RequestHeader, RequestPreparesHeader, RequestStartViewHeader, StartViewChangeHeader,
    StartViewHeader,
};
use smallvec::SmallVec;
use std::{marker::PhantomData, mem::size_of};

pub const MESSAGE_ALIGN: usize = 4096;

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
        if !T::accepts(generic.header().command) {
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
        if !T::accepts(generic.header().command) {
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

        // `size` is the whole-frame length and must at least span the header, or
        // a consumer slicing `[size_of::<H>()..size]` underflows (start > end).
        // Every consensus header is `HEADER_SIZE`, so this floor also covers a
        // later `try_into_typed` conversion.
        if (header.size() as usize) < size_of::<H>() {
            return Err(ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: Command2::Reserved,
            });
        }

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

        // See `TryFrom<Owned>`: `size` must at least span the header so a
        // `[size_of::<H>()..size]` body slice cannot underflow.
        if (header.size() as usize) < size_of::<H>() {
            return Err(ConsensusError::InvalidCommand {
                expected: H::COMMAND,
                found: Command2::Reserved,
            });
        }

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
            Self::RequestStartView(message) => message.header().command,
            Self::RequestPrepares(message) => message.header().command,
            Self::RepairPrepare(message) => message.header().command(),
            Self::RepairRangeReply(message) => message.header().command,
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
        }
    }
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
            Command2::Prepare => Ok(Self::Prepare(value.try_into_typed::<PrepareHeader>()?)),
            Command2::Request => Ok(Self::Request(value.try_into_typed::<RequestHeader>()?)),
            Command2::PrepareOk => Ok(Self::PrepareOk(value.try_into_typed::<PrepareOkHeader>()?)),
            Command2::StartViewChange => Ok(Self::StartViewChange(
                value.try_into_typed::<StartViewChangeHeader>()?,
            )),
            Command2::DoViewChange => Ok(Self::DoViewChange(
                value.try_into_typed::<DoViewChangeHeader>()?,
            )),
            Command2::StartView => Ok(Self::StartView(value.try_into_typed::<StartViewHeader>()?)),
            Command2::Commit => Ok(Self::Commit(value.try_into_typed::<CommitHeader>()?)),
            Command2::RequestStartView => Ok(Self::RequestStartView(
                value.try_into_typed::<RequestStartViewHeader>()?,
            )),
            Command2::RequestPrepares => Ok(Self::RequestPrepares(
                value.try_into_typed::<RequestPreparesHeader>()?,
            )),
            // A repaired prepare is a stored PrepareHeader frame whose command
            // byte was rewritten; typed validation would reject the byte, so
            // parse through the generic backing and trust the prepare-shaped
            // layout the way the journal that produced it did.
            Command2::RepairPrepare => Ok(Self::RepairPrepare(
                value.try_into_typed::<RepairPrepareHeader>()?,
            )),
            Command2::RepairDone | Command2::RangeEvicted => Ok(Self::RepairRangeReply(
                value.try_into_typed::<RepairRangeReplyHeader>()?,
            )),
            // Reply / Eviction are server-to-client frames; they do not
            // appear on the inbound dispatch path.
            Command2::Reply | Command2::Eviction => {
                Err(ConsensusError::ClientBoundCommand(command))
            }
            other => Err(ConsensusError::InvalidCommand {
                expected: Command2::Reserved,
                found: other,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::{Operation, ReplyHeader};
    use smallvec::smallvec;

    // Field offsets via `offset_of!`: a field reorder fails to compile here
    // rather than silently corrupting test bytes.
    const SIZE_OFF: usize = std::mem::offset_of!(RequestHeader, size);
    const COMMAND_OFF: usize = std::mem::offset_of!(RequestHeader, command);
    const REQUEST_CLIENT_OFF: usize = std::mem::offset_of!(RequestHeader, client);
    const REQUEST_OPERATION_OFF: usize = std::mem::offset_of!(RequestHeader, operation);
    const REQUEST_SESSION_OFF: usize = std::mem::offset_of!(RequestHeader, session);

    fn header_bytes(command: Command2, size: u32) -> Owned<MESSAGE_ALIGN> {
        header_bytes_sized(command, size, 256)
    }

    fn header_bytes_sized(command: Command2, size: u32, buffer_len: usize) -> Owned<MESSAGE_ALIGN> {
        let mut o = Owned::<MESSAGE_ALIGN>::zeroed(buffer_len);
        {
            let buf = o.as_mut_slice();
            buf[SIZE_OFF..SIZE_OFF + 4].copy_from_slice(&size.to_le_bytes());
            buf[COMMAND_OFF] = command as u8;
            // Typed headers reject client == 0. `#[repr(C)]` preamble layout
            // is shared, so this offset works across header types.
            buf[REQUEST_CLIENT_OFF..REQUEST_CLIENT_OFF + 16]
                .copy_from_slice(&0xCAFE_u128.to_le_bytes());
        }
        o
    }

    // MessageBag round-trip for the probe + repair command family. Locks
    // RangeEvicted delivery in particular: RepairDone and RangeEvicted share
    // one header layout and BOTH must survive the typed parse -- a strict
    // command match in `try_into_typed` silently dropped every RangeEvicted
    // frame and with it the whole commit-floor path.
    #[test]
    fn probe_and_repair_commands_round_trip_into_bag() {
        for command in [
            Command2::RequestStartView,
            Command2::RequestPrepares,
            Command2::RepairPrepare,
            Command2::RepairDone,
            Command2::RangeEvicted,
        ] {
            let mut owned = header_bytes(command, 256);
            if command == Command2::RequestPrepares {
                // validate() demands a non-empty 1-based range.
                const FROM_OP_OFF: usize =
                    std::mem::offset_of!(iggy_binary_protocol::RequestPreparesHeader, from_op);
                const TO_OP_OFF: usize =
                    std::mem::offset_of!(iggy_binary_protocol::RequestPreparesHeader, to_op);
                let buf = owned.as_mut_slice();
                buf[FROM_OP_OFF..FROM_OP_OFF + 8].copy_from_slice(&1u64.to_le_bytes());
                buf[TO_OP_OFF..TO_OP_OFF + 8].copy_from_slice(&1u64.to_le_bytes());
            }
            let generic = Message::<GenericHeader>::try_from(owned)
                .unwrap_or_else(|e| panic!("{command:?} failed generic framing: {e}"));
            let bag = MessageBag::try_from(generic)
                .unwrap_or_else(|e| panic!("{command:?} failed bag parse: {e}"));
            let routed = matches!(
                (&bag, command),
                (MessageBag::RequestStartView(_), Command2::RequestStartView)
                    | (MessageBag::RequestPrepares(_), Command2::RequestPrepares)
                    | (MessageBag::RepairPrepare(_), Command2::RepairPrepare)
                    | (
                        MessageBag::RepairRangeReply(_),
                        Command2::RepairDone | Command2::RangeEvicted
                    )
            );
            assert!(routed, "{command:?} parsed into the wrong bag variant");
            assert_eq!(bag.command(), command, "original command byte must survive");
        }
    }

    // Construction via Message::new (zeroed)

    #[test]
    #[should_panic(expected = "size must be at least header size")]
    fn message_new_smaller_than_header_panics() {
        let _ = Message::<RequestHeader>::new(100);
    }

    // try_from(Owned): validation gates the unsafe construction

    #[test]
    fn try_from_owned_too_short_returns_err() {
        let owned = Owned::<MESSAGE_ALIGN>::zeroed(100);
        let result = Message::<RequestHeader>::try_from(owned);
        assert!(matches!(result, Err(ConsensusError::InvalidCommand { .. })));
    }

    #[test]
    fn try_from_owned_invalid_bit_pattern_returns_err() {
        let mut owned = Owned::<MESSAGE_ALIGN>::zeroed(256);
        owned.as_mut_slice()[COMMAND_OFF] = 99; // outside Command2's discriminant range
        let result = Message::<RequestHeader>::try_from(owned);
        assert!(matches!(result, Err(ConsensusError::InvalidBitPattern)));
    }

    #[test]
    fn try_from_owned_buffer_shorter_than_claimed_size_returns_err() {
        // Header parses cleanly (RequestHeader::validate doesn't gate on
        // size), but the encoded `size` field claims more bytes than the
        // backing buffer holds. The buffer-bounds check at the bottom of
        // `Message::try_from` must reject. (Both this case and the
        // "buffer shorter than `size_of::<H>`" case currently surface as
        // the same `InvalidCommand` variant; promoting them to distinct
        // `ConsensusError` variants is a separate hardening pass.)
        let owned = header_bytes(Command2::Request, 999);
        // header_bytes already produces a 256-byte buffer; size=999 > 256,
        // so try_from rejects via `bytes.len() < header.size()`.
        let result = Message::<RequestHeader>::try_from(owned);
        assert!(matches!(result, Err(ConsensusError::InvalidCommand { .. })));
    }

    #[test]
    fn try_from_owned_size_below_header_size_returns_err() {
        // `size` claims a frame smaller than the header. The buffer is full-size,
        // so only the construction-time `size` floor rejects it (the
        // buffer-length check passes). Guards the `[size_of::<H>()..size]`
        // underflow at every downstream call site.
        let owned = header_bytes(Command2::Request, size_of::<RequestHeader>() as u32 - 1);
        let result = Message::<RequestHeader>::try_from(owned);
        assert!(matches!(result, Err(ConsensusError::InvalidCommand { .. })));
    }

    // as_generic: const unsafe pointer cast (#[repr(C)] equivalence)

    #[test]
    fn as_generic_view_reads_command_byte() {
        let owned = header_bytes(Command2::Request, 256);
        let typed = Message::<RequestHeader>::try_from(owned).expect("valid");
        let generic = typed.as_generic();
        assert_eq!(generic.header().command, Command2::Request);
        assert_eq!(generic.total_len(), 256);
    }

    // try_as_typed: validation gates the unsafe ptr-cast reborrow

    #[test]
    fn try_as_typed_command_mismatch_returns_err_without_unsafe_cast() {
        // bytes are a valid Prepare; asking for RequestHeader must fail
        // *before* the unsafe ptr-cast inside try_as_typed.
        let owned = header_bytes(Command2::Prepare, 256);
        let generic = Message::<GenericHeader>::try_from(owned).expect("valid");
        let result = generic.try_as_typed::<RequestHeader>();
        assert!(matches!(
            result,
            Err(ConsensusError::InvalidCommand {
                expected: Command2::Request,
                found: Command2::Prepare,
            })
        ));
    }

    #[test]
    fn try_as_typed_invalid_validation_returns_err() {
        // RequestHeader::validate rejects operation=Register with non-zero session.
        let mut owned = header_bytes(Command2::Request, 256);
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
        let owned = header_bytes(Command2::Prepare, 256);
        let generic = Message::<GenericHeader>::try_from(owned).expect("valid");
        let result = generic.try_into_typed::<RequestHeader>();
        assert!(matches!(
            result,
            Err(ConsensusError::InvalidCommand {
                expected: Command2::Request,
                found: Command2::Prepare,
            })
        ));
    }

    // MessageBag dispatch: 7 unsafe `from_backing_unchecked` arms

    fn dispatch(command: Command2, size: u32) -> Result<MessageBag, ConsensusError> {
        let owned = header_bytes(command, size);
        let generic = Message::<GenericHeader>::try_from(owned).expect("valid generic");
        MessageBag::try_from(generic)
    }

    #[test]
    fn messagebag_dispatch_unsupported_command_returns_err() {
        // Ping is a valid Command2 bit pattern but is not a MessageBag variant.
        let owned = header_bytes(Command2::Ping, 256);
        let generic = Message::<GenericHeader>::try_from(owned).expect("valid generic");
        let result = MessageBag::try_from(generic);
        assert!(matches!(result, Err(ConsensusError::InvalidCommand { .. })));
    }

    #[test]
    fn messagebag_command_method_round_trips() {
        for cmd in [
            Command2::Request,
            Command2::Prepare,
            Command2::PrepareOk,
            Command2::StartViewChange,
            Command2::DoViewChange,
            Command2::StartView,
            Command2::Commit,
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
        let owned = header_bytes_sized(Command2::Commit, 300, 512);
        let generic = Message::<GenericHeader>::try_from(owned).expect("valid generic");
        let result = MessageBag::try_from(generic);
        assert!(matches!(
            result,
            Err(ConsensusError::CommitInvalidSize(300))
        ));
    }

    #[test]
    fn messagebag_dispatch_request_with_invalid_register_session_returns_err() {
        // `RequestHeader::validate` rejects Register with non-zero session.
        let mut owned = header_bytes(Command2::Request, 256);
        {
            let buf = owned.as_mut_slice();
            buf[REQUEST_OPERATION_OFF] = Operation::Register as u8;
            buf[REQUEST_SESSION_OFF..REQUEST_SESSION_OFF + 8].copy_from_slice(&5u64.to_le_bytes());
        }
        let generic = Message::<GenericHeader>::try_from(owned).expect("valid generic");
        let result = MessageBag::try_from(generic);
        assert!(matches!(result, Err(ConsensusError::InvalidField(_))));
    }

    // deep_copy: byte-level independence after clone-like API

    #[test]
    fn request_message_deep_copy_independent() {
        let owned = header_bytes(Command2::Request, 256);
        let mut msg = Message::<RequestHeader>::try_from(owned).expect("valid");
        let copy = msg.deep_copy();
        // Mutate the original's bytes; the deep copy must be untouched.
        msg.as_mut_slice()[200] = 0xab;
        assert_eq!(copy.as_slice()[200], 0);
        assert_eq!(msg.as_slice()[200], 0xab);
    }

    // transmute_header: rewrites the typed header in place

    #[test]
    fn transmute_header_request_to_prepare() {
        let owned = header_bytes(Command2::Request, 256);
        let msg = Message::<RequestHeader>::try_from(owned).expect("valid");
        let prepared: Message<PrepareHeader> =
            msg.transmute_header::<PrepareHeader>(|_old, new| {
                new.command = Command2::Prepare;
                new.size = 256;
            });
        assert_eq!(prepared.header().command, Command2::Prepare);
    }

    // ResponseBacking via SmallVec<Frozen>

    #[test]
    fn response_backing_single_fragment_roundtrip() {
        let owned = header_bytes(Command2::Reply, 256);
        let frozen: Frozen<MESSAGE_ALIGN> = owned.into();
        let fragments: smallvec::SmallVec<[Frozen<MESSAGE_ALIGN>; 4]> = smallvec![frozen];
        let msg = Message::<ReplyHeader, ResponseBacking>::try_from(fragments).expect("valid");
        assert_eq!(msg.header().command, Command2::Reply);
        assert_eq!(msg.fragments().len(), 1);
    }

    #[test]
    fn response_backing_empty_fragments_returns_err() {
        let fragments: smallvec::SmallVec<[Frozen<MESSAGE_ALIGN>; 4]> = smallvec![];
        let result = Message::<ReplyHeader, ResponseBacking>::try_from(fragments);
        assert!(matches!(result, Err(ConsensusError::InvalidCommand { .. })));
    }

    #[test]
    fn response_backing_first_fragment_too_short_returns_err() {
        let owned = Owned::<MESSAGE_ALIGN>::zeroed(100);
        let frozen: Frozen<MESSAGE_ALIGN> = owned.into();
        let fragments: smallvec::SmallVec<[Frozen<MESSAGE_ALIGN>; 4]> = smallvec![frozen];
        let result = Message::<ReplyHeader, ResponseBacking>::try_from(fragments);
        assert!(matches!(result, Err(ConsensusError::InvalidCommand { .. })));
    }

    #[test]
    fn response_backing_size_below_header_size_returns_err() {
        // First fragment is full-size, but its `size` field claims less than
        // the header; the floor must reject before any consumer slices a body.
        let owned = header_bytes(Command2::Reply, size_of::<ReplyHeader>() as u32 - 1);
        let frozen: Frozen<MESSAGE_ALIGN> = owned.into();
        let fragments: smallvec::SmallVec<[Frozen<MESSAGE_ALIGN>; 4]> = smallvec![frozen];
        let result = Message::<ReplyHeader, ResponseBacking>::try_from(fragments);
        assert!(matches!(result, Err(ConsensusError::InvalidCommand { .. })));
    }
}
