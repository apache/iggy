/* Licensed to the Apache Software Foundation (ASF) under one
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

//! Server-ng QUIC listener schema.
//!
//! Field shape mirrors the legacy [`crate::quic::QuicConfig`] verbatim;
//! the type is forked into `server_ng_config` so server-ng can evolve
//! its QUIC surface independently of the legacy server. No semantic
//! change at fork time.

use super::COMPONENT_NG;
use crate::ConfigurationError;
use configs::ConfigEnv;
use iggy_common::{IggyByteSize, IggyDuration, Validatable};
use serde::{Deserialize, Serialize};
use serde_with::DisplayFromStr;
use serde_with::serde_as;

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct QuicConfig {
    pub enabled: bool,
    pub address: String,
    pub max_concurrent_bidi_streams: u64,
    #[config_env(leaf)]
    pub datagram_send_buffer_size: IggyByteSize,
    #[config_env(leaf)]
    pub initial_mtu: IggyByteSize,
    #[config_env(leaf)]
    pub send_window: IggyByteSize,
    #[config_env(leaf)]
    pub receive_window: IggyByteSize,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub keep_alive_interval: IggyDuration,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub max_idle_timeout: IggyDuration,
    pub certificate: QuicCertificateConfig,
    pub socket: QuicSocketConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct QuicSocketConfig {
    pub override_defaults: bool,
    #[config_env(leaf)]
    pub recv_buffer_size: IggyByteSize,
    #[config_env(leaf)]
    pub send_buffer_size: IggyByteSize,
    pub keepalive: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct QuicCertificateConfig {
    pub self_signed: bool,
    pub cert_file: String,
    pub key_file: String,
}

/// Validates the field range constraints the runtime conversion in
/// `core::message_bus::config::build_quic_tuning` previously enforced
/// via `expect(...)`. Surfacing them here turns boot-time misconfig
/// into a `ConfigurationError` instead of a panic in the bus crate.
impl Validatable<ConfigurationError> for QuicConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        // QUIC requires at least one bidi stream per connection.
        if self.max_concurrent_bidi_streams == 0 {
            eprintln!("{COMPONENT_NG} quic.max_concurrent_bidi_streams must be >= 1");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        // quinn-proto stores stream counts as u32 internally.
        if u32::try_from(self.max_concurrent_bidi_streams).is_err() {
            eprintln!(
                "{COMPONENT_NG} quic.max_concurrent_bidi_streams ({}) does not fit in u32",
                self.max_concurrent_bidi_streams
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        // The datagram send buffer is materialized as a `Vec<u8>` of this
        // length in compio-quic, so it must fit in `usize` on the target
        // platform.
        if usize::try_from(self.datagram_send_buffer_size.as_bytes_u64()).is_err() {
            eprintln!(
                "{COMPONENT_NG} quic.datagram_send_buffer_size ({} bytes) does not fit in usize on this target",
                self.datagram_send_buffer_size.as_bytes_u64()
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        // RFC 9000 §14: minimum required MTU is 1200 bytes; quinn stores
        // initial_mtu as u16 (max 65535).
        let initial_mtu = self.initial_mtu.as_bytes_u64();
        if initial_mtu < 1200 {
            eprintln!(
                "{COMPONENT_NG} quic.initial_mtu ({initial_mtu}) is below the QUIC minimum of 1200 bytes",
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if u16::try_from(initial_mtu).is_err() {
            eprintln!("{COMPONENT_NG} quic.initial_mtu ({initial_mtu}) exceeds u16::MAX (65535)",);
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        // quinn VarInt for `receive_window` accepts u32; rejecting
        // out-of-range values here surfaces a config error rather than
        // panicking inside the bus crate's runtime conversion.
        if u32::try_from(self.receive_window.as_bytes_u64()).is_err() {
            eprintln!(
                "{COMPONENT_NG} quic.receive_window ({} bytes) does not fit in u32 (quinn VarInt limit)",
                self.receive_window.as_bytes_u64()
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        // `send_window` is u64-sized in QuicTuning, but quinn's VarInt
        // protocol-level cap is 2^62 - 1; reject anything above that.
        const QUINN_VARINT_MAX: u64 = (1u64 << 62) - 1;
        if self.send_window.as_bytes_u64() > QUINN_VARINT_MAX {
            eprintln!(
                "{COMPONENT_NG} quic.send_window ({} bytes) exceeds quinn VarInt max (2^62 - 1)",
                self.send_window.as_bytes_u64()
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn baseline() -> QuicConfig {
        QuicConfig {
            enabled: false,
            address: String::new(),
            max_concurrent_bidi_streams: 1,
            datagram_send_buffer_size: IggyByteSize::from(100_u64 * 1024),
            initial_mtu: IggyByteSize::from(8_u64 * 1024),
            send_window: IggyByteSize::from(64_u64 * 1024 * 1024),
            receive_window: IggyByteSize::from(64_u64 * 1024 * 1024),
            keep_alive_interval: IggyDuration::from(std::time::Duration::from_secs(10)),
            max_idle_timeout: IggyDuration::from(std::time::Duration::from_secs(30)),
            certificate: QuicCertificateConfig {
                self_signed: false,
                cert_file: String::new(),
                key_file: String::new(),
            },
            socket: QuicSocketConfig {
                override_defaults: false,
                recv_buffer_size: IggyByteSize::from(0_u64),
                send_buffer_size: IggyByteSize::from(0_u64),
                keepalive: false,
            },
        }
    }

    #[test]
    fn baseline_validates() {
        baseline().validate().expect("baseline is valid");
    }

    #[test]
    fn rejects_zero_max_concurrent_bidi_streams() {
        let mut c = baseline();
        c.max_concurrent_bidi_streams = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_max_concurrent_bidi_streams_above_u32() {
        let mut c = baseline();
        c.max_concurrent_bidi_streams = u64::from(u32::MAX) + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_initial_mtu_below_qiuc_minimum() {
        let mut c = baseline();
        c.initial_mtu = IggyByteSize::from(1199_u64);
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_initial_mtu_above_u16() {
        let mut c = baseline();
        c.initial_mtu = IggyByteSize::from(u64::from(u16::MAX) + 1);
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_receive_window_above_u32() {
        let mut c = baseline();
        c.receive_window = IggyByteSize::from(u64::from(u32::MAX) + 1);
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_send_window_above_quinn_varint_max() {
        let mut c = baseline();
        c.send_window = IggyByteSize::from(1_u64 << 62);
        assert!(c.validate().is_err());
    }

    #[test]
    fn accepts_initial_mtu_at_quic_minimum() {
        let mut c = baseline();
        c.initial_mtu = IggyByteSize::from(1200_u64);
        assert!(c.validate().is_ok());
    }
}
