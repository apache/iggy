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

//! What a shard needs from the process embedding it.
//!
//! [`IggyShard`](crate::IggyShard) routes frames between shards and planes;
//! serving a client request, running a metadata submit, or enumerating the
//! sessions a shard homes is the host's business (the server's dispatch
//! layer, or the simulator's shell). A shard holds one `Rc<dyn ShardHost>`
//! and calls into it on its own thread, so the contract carries no
//! `Send`/`Sync` bound.

use crate::{ListClientsReply, MetadataSubmit};
use iggy_binary_protocol::GenericHeader;
use message_bus::client_listener::RequestHandler;
use message_bus::replica::listener::MessageHandler;
use server_common::Message;
use std::rc::Rc;

/// The host-side handlers a shard dispatches into.
///
/// Every method runs synchronously on the shard's thread: the lifecycle
/// arms from the message pump, the two connection-facing methods from a
/// bus reader task (through the adapters [`IggyShard::new`] builds). A
/// handler that has to await spawns its own task and returns.
///
/// [`IggyShard::new`]: crate::IggyShard::new
pub trait ShardHost {
    /// Inbound consensus message on a replica connection delegated to this
    /// shard. The bus' reader task calls this for every frame; the server
    /// wires it to [`IggyShard::dispatch`](crate::IggyShard::dispatch).
    fn on_replica_message(&self, replica_id: u8, message: Message<GenericHeader>);

    /// Inbound `Request` frame on a client connection homed on this shard;
    /// `client_id` is the transport (coordinator-minted) id. Every transport
    /// reaching the shard goes through this one method, which is what keeps
    /// a client's per-connection ordering guarantee whole.
    fn on_client_request(&self, client_id: u128, message: Message<GenericHeader>);

    /// Inbound [`MetadataSubmit`]: a peer shard asks shard 0, the metadata
    /// consensus owner, to run one consensus proposal, or a replica reports
    /// consumer-session liveness. Only shard 0 receives these. The server
    /// wires proposals to `submit_register_in_process` /
    /// `submit_logout_in_process` / `submit_request_in_process` and sends
    /// the result back over the frame's `reply` sender; a host that cannot
    /// submit drops the sender, so the awaiting peer never blocks forever.
    fn on_metadata_submit(&self, submit: MetadataSubmit);

    /// Inbound [`LifecycleFrame::ListClients`](crate::LifecycleFrame::ListClients)
    /// broadcast. Every shard receives it (shared-nothing: each knows only
    /// its own connections); the server wires it to read the shard's
    /// `SessionManager` and push the connected clients or the bound consumer
    /// sessions back over `reply`.
    /// See [`IggyShard::list_all_clients`](crate::IggyShard::list_all_clients).
    fn on_list_clients(&self, reply: ListClientsReply);
}

/// A host that answers nothing: every method drops its arguments, reply
/// senders included, so a waiting peer sees a disconnect rather than a hang.
///
/// For shards that never receive host-bound frames: the simulator's
/// shell-off fast path and test fixtures.
pub struct NoopHost;

impl ShardHost for NoopHost {
    fn on_replica_message(&self, _replica_id: u8, _message: Message<GenericHeader>) {}

    fn on_client_request(&self, _client_id: u128, _message: Message<GenericHeader>) {}

    fn on_metadata_submit(&self, _submit: MetadataSubmit) {}

    fn on_list_clients(&self, _reply: ListClientsReply) {}
}

/// [`ShardHost::on_replica_message`] in the `Rc<dyn Fn>` shape the bus
/// installs on a delegated replica connection. Built once per shard so an
/// install clones one `Rc` instead of allocating a closure per connection.
pub(crate) fn replica_message_handler(host: &Rc<dyn ShardHost>) -> MessageHandler {
    let host = Rc::clone(host);
    Rc::new(move |replica_id, message| host.on_replica_message(replica_id, message))
}

/// [`ShardHost::on_client_request`] in the `Rc<dyn Fn>` shape the bus
/// installs on a delegated client connection; see
/// [`replica_message_handler`].
pub(crate) fn client_request_handler(host: &Rc<dyn ShardHost>) -> RequestHandler {
    let host = Rc::clone(host);
    Rc::new(move |client_id, message| host.on_client_request(client_id, message))
}
