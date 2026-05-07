/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! `Display` impls for the server-ng config surface.
//!
//! Reused section types pick up [`Display`] from
//! [`crate::displays`]; this module only adds the top-level
//! [`ServerNgConfig`] formatter and the new [`MessageBusConfig`]
//! section formatter.

use super::message_bus::MessageBusConfig;
use super::quic::{QuicCertificateConfig, QuicConfig, QuicSocketConfig};
use super::server_ng::ServerNgConfig;
use super::tcp::{TcpConfig, TcpSocketConfig, TcpTlsConfig};
use std::fmt::{Display, Formatter};

impl Display for ServerNgConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ consumer_group: {}, data_maintenance: {}, message_saver: {}, heartbeat: {}, \
             system: {}, quic: {}, tcp: {}, http: {}, telemetry: {}, message_bus: {} }}",
            self.consumer_group,
            self.data_maintenance,
            self.message_saver,
            self.heartbeat,
            self.system,
            self.quic,
            self.tcp,
            self.http,
            self.telemetry,
            self.message_bus,
        )
    }
}

impl Display for MessageBusConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ max_batch: {}, max_message_size: {}, peer_queue_capacity: {}, \
             reconnect_period: {}, close_peer_timeout: {}, close_grace: {}, \
             handshake_grace: {}, ws_max_message_size: {:?}, \
             ws_max_frame_size: {:?}, ws_write_buffer_size: {:?}, \
             ws_accept_unmasked_frames: {} }}",
            self.max_batch,
            self.max_message_size,
            self.peer_queue_capacity,
            self.reconnect_period,
            self.close_peer_timeout,
            self.close_grace,
            self.handshake_grace,
            self.ws_max_message_size,
            self.ws_max_frame_size,
            self.ws_write_buffer_size,
            self.ws_accept_unmasked_frames,
        )
    }
}

impl Display for TcpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, address: {}, ipv6: {}, tls: {}, socket: {}, socket_migration: {} }}",
            self.enabled, self.address, self.ipv6, self.tls, self.socket, self.socket_migration
        )
    }
}

impl Display for TcpTlsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, self_signed: {}, cert_file: {}, key_file: {} }}",
            self.enabled, self.self_signed, self.cert_file, self.key_file
        )
    }
}

impl Display for TcpSocketConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ override_defaults: {}, recv_buffer_size: {}, send_buffer_size: {}, keepalive: {}, nodelay: {}, linger: {} }}",
            self.override_defaults,
            self.recv_buffer_size,
            self.send_buffer_size,
            self.keepalive,
            self.nodelay,
            self.linger,
        )
    }
}

impl Display for QuicConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, address: {}, max_concurrent_bidi_streams: {}, datagram_send_buffer_size: {}, initial_mtu: {}, send_window: {}, receive_window: {}, keep_alive_interval: {}, max_idle_timeout: {}, certificate: {} }}",
            self.enabled,
            self.address,
            self.max_concurrent_bidi_streams,
            self.datagram_send_buffer_size,
            self.initial_mtu,
            self.send_window,
            self.receive_window,
            self.keep_alive_interval,
            self.max_idle_timeout,
            self.certificate
        )
    }
}

impl Display for QuicCertificateConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ self_signed: {}, cert_file: {}, key_file: {} }}",
            self.self_signed, self.cert_file, self.key_file
        )
    }
}

impl Display for QuicSocketConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ override_defaults: {}, recv_buffer_size: {}, send_buffer_size: {}, keepalive: {} }}",
            self.override_defaults, self.recv_buffer_size, self.send_buffer_size, self.keepalive
        )
    }
}
