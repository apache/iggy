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

//! Port pre-allocation for test binaries.
//!
//! A reserver takes an exclusive advisory lock on one slot of a fixed port band
//! and derives its ports from the slot index, so the harness knows every port
//! before the server starts and never has to discover one from a config file.
//!
//! The lock is the allocator. `flock` conflicts between concurrent test
//! processes and, because each open file description is independent, between
//! concurrent tests inside a single process, so one slot maps to at most one
//! live reserver either way. The slot returns to the pool when the guard drops
//! at test teardown, and the OS drops it too if the process dies, so a crash
//! leaks nothing.
//!
//! Two properties this deliberately does not have:
//!
//! - Nothing binds a socket. A placeholder socket races the server's own
//!   `bind()` on release, and it costs a file descriptor per port rather than
//!   per live test.
//! - Nothing remembers a port it handed out. A registry keyed by port only ever
//!   grows, which is fine for one process per test but exhausts a single-process
//!   `cargo test` run of the whole suite.
//!
//! The band sits clear of the kernel's ephemeral range, so `bind(0)` in an
//! unrelated process can never be handed a port the harness has promised to a
//! server: below the range by preference, above it when the range is tuned too
//! low to leave any room below. The range is read from the running kernel where
//! that is possible and assumed to be the Linux default otherwise.

use crate::harness::config::{IpAddrKind, TestServerConfig};
use crate::harness::error::TestBinaryError;
use std::fs::{File, OpenOptions, TryLockError};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// First port of the primary band, which runs from here up to the ephemeral
/// floor. Clear of the privileged range.
const BAND_START: u16 = 20000;

/// Last port of the fallback band, which runs from just above the ephemeral
/// ceiling up to here.
const FALLBACK_BAND_END: u16 = u16::MAX;

/// Ports one slot owns. A server reserver hands out at most five (tcp,
/// tcp_replica, quic, http, websocket); [`SINGLE_PORT_OFFSET`] sits above
/// those, and the rest is room for another protocol.
const PORTS_PER_SLOT: u16 = 8;

/// Offset of the one port a [`SinglePortReserver`] hands out. Deliberately
/// clear of the server offsets (0..=4): its consumers (MCP, connectors
/// runtime) bind plain `SO_REUSEADDR` listeners, the server's TCP listener
/// binds `SO_REUSEPORT` without `SO_REUSEADDR`, and a TIME_WAIT socket left
/// by either flavor rejects binds of the other for a full TIME_WAIT interval
/// after teardown. Slots are reused back-to-back, so the two kinds of
/// listener must never share a port number.
const SINGLE_PORT_OFFSET: u16 = 5;

/// Ephemeral range assumed when the kernel's cannot be read, matching the Linux
/// default.
const DEFAULT_EPHEMERAL_RANGE: (u16, u16) = (32768, 60999);

const IP_LOCAL_PORT_RANGE: &str = "/proc/sys/net/ipv4/ip_local_port_range";

/// Index of the concurrency lane `cargo nextest` is running this test in. Lanes
/// are reused as tests finish, which makes it a good first guess at a free slot.
/// Only ever a guess: the lock decides, so an absent or bogus value costs a few
/// extra probes and nothing else.
const NEXTEST_SLOT_ENV: &str = "NEXTEST_TEST_GLOBAL_SLOT";

/// A run of whole slots, clear of the kernel's ephemeral range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PortBand {
    start: u16,
    slots: u16,
}

impl PortBand {
    /// The slots `[start, end]` holds, or `None` if it has room for none.
    fn fit(start: u16, end: u16) -> Option<Self> {
        let span = (u32::from(end) + 1).checked_sub(u32::from(start))?;
        let slots = u16::try_from(span / u32::from(PORTS_PER_SLOT)).ok()?;
        (slots > 0).then_some(Self { start, slots })
    }

    fn base_port(self, slot: u16) -> u16 {
        self.start + slot * PORTS_PER_SLOT
    }

    fn end(self) -> u16 {
        self.base_port(self.slots - 1) + PORTS_PER_SLOT - 1
    }
}

/// The band to reserve from, given the kernel's ephemeral range. Below the
/// floor by preference; above the ceiling when the floor is too low to leave
/// room there, which is the only thing that lets a box tuned for high
/// connection counts (`1024 65535` and the like) run the suite at all.
fn band_for(floor: u16, ceiling: u16) -> Result<PortBand, TestBinaryError> {
    let below = floor
        .checked_sub(1)
        .and_then(|end| PortBand::fit(BAND_START, end));
    let above = ceiling
        .checked_add(1)
        .and_then(|start| PortBand::fit(start, FALLBACK_BAND_END));
    below.or(above).ok_or_else(|| TestBinaryError::InvalidState {
        message: format!(
            "The kernel's ephemeral range [{floor}, {ceiling}] leaves room for a test port band \
             on neither side: not below {floor} starting at {BAND_START}, nor above {ceiling} up \
             to {FALLBACK_BAND_END}. Narrow the range, for example \
             `sysctl -w net.ipv4.ip_local_port_range='32768 60999'`."
        ),
    })
}

fn parse_ephemeral_range(range: &str) -> Option<(u16, u16)> {
    let mut bounds = range.split_whitespace();
    let floor = bounds.next()?.parse().ok()?;
    let ceiling = bounds.next()?.parse().ok()?;
    Some((floor, ceiling))
}

/// The range the kernel picks `bind(0)` ports from, read once.
fn ephemeral_range() -> (u16, u16) {
    static RANGE: OnceLock<(u16, u16)> = OnceLock::new();
    *RANGE.get_or_init(|| {
        std::fs::read_to_string(IP_LOCAL_PORT_RANGE)
            .ok()
            .as_deref()
            .and_then(parse_ephemeral_range)
            .unwrap_or(DEFAULT_EPHEMERAL_RANGE)
    })
}

/// Shared directory holding the per-slot lock files, created once. A failure to
/// create it is not reported here; [`SlotGuard::acquire`] then finds no slot it
/// can open and fails with this path in its message.
fn lock_dir() -> &'static Path {
    static LOCK_DIR: OnceLock<PathBuf> = OnceLock::new();
    LOCK_DIR
        .get_or_init(|| {
            let dir = std::env::temp_dir().join("iggy-test-port-locks");
            let _ = std::fs::create_dir_all(&dir);
            dir
        })
        .as_path()
}

/// Exclusive claim on one slot of the band, released when dropped.
struct SlotGuard {
    base_port: u16,
    next_offset: u16,
    /// Held for its lock alone; closing the file frees the slot.
    _lock: File,
}

impl SlotGuard {
    fn acquire() -> Result<Self, TestBinaryError> {
        let (floor, ceiling) = ephemeral_range();
        let band = band_for(floor, ceiling)?;
        let first = std::env::var(NEXTEST_SLOT_ENV)
            .ok()
            .and_then(|lane| lane.parse::<u16>().ok())
            .unwrap_or(0)
            % band.slots;

        let mut opened_any = false;
        for step in 0..band.slots {
            let slot = (first + step) % band.slots;
            let path = lock_dir().join(format!("slot-{slot}.lock"));
            // Skip a slot whose lock file will not open rather than failing the
            // run: one run as root leaves files behind that a later run as a
            // normal user cannot open, and those must not hide the slots after
            // them.
            let Ok(file) = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
            else {
                continue;
            };
            opened_any = true;

            match file.try_lock() {
                Ok(()) => {
                    return Ok(Self {
                        base_port: band.base_port(slot),
                        next_offset: 0,
                        _lock: file,
                    });
                }
                Err(TryLockError::WouldBlock) => {}
                // A slot index is only exclusive while the lock works. Handing
                // the ports out anyway would give two servers the same numbers,
                // so there is no degraded mode to fall back to.
                Err(TryLockError::Error(e)) => {
                    return Err(TestBinaryError::InvalidState {
                        message: format!(
                            "Failed to lock port slot {}: {e}. Test port slots need a filesystem \
                             with working advisory locks. Redirecting TMPDIR at one is not a fix: \
                             every runner on the host has to share a single lock directory, and a \
                             second one hands out the same slots, so the same ports.",
                            path.display()
                        ),
                    });
                }
            }
        }

        if !opened_any {
            return Err(TestBinaryError::InvalidState {
                message: format!(
                    "None of the {} test port slot locks under {} could be opened. Check the \
                     directory's ownership and permissions: a run as root leaves lock files that \
                     a later run as a normal user cannot open.",
                    band.slots,
                    lock_dir().display()
                ),
            });
        }

        Err(TestBinaryError::InvalidState {
            message: format!(
                "All {} test port slots in [{}, {}] are in use. Lower the test concurrency, or \
                 widen the band by narrowing the kernel's ephemeral range \
                 (net.ipv4.ip_local_port_range).",
                band.slots,
                band.start,
                band.end()
            ),
        })
    }

    fn next_addr(&mut self, ip_kind: IpAddrKind) -> Result<SocketAddr, TestBinaryError> {
        if self.next_offset >= PORTS_PER_SLOT {
            return Err(TestBinaryError::InvalidState {
                message: format!(
                    "A port slot owns {PORTS_PER_SLOT} ports and this reserver has taken all of \
                     them. Widen PORTS_PER_SLOT rather than spilling into the next slot."
                ),
            });
        }

        let port = self.base_port + self.next_offset;
        self.next_offset += 1;

        let ip: IpAddr = match ip_kind {
            IpAddrKind::V4 => Ipv4Addr::LOCALHOST.into(),
            IpAddrKind::V6 => Ipv6Addr::LOCALHOST.into(),
        };
        Ok(SocketAddr::new(ip, port))
    }
}

/// Addresses for all enabled protocols.
#[derive(Debug, Clone)]
pub struct ProtocolAddresses {
    pub tcp: Option<SocketAddr>,
    pub tcp_replica: Option<SocketAddr>,
    pub quic: Option<SocketAddr>,
    pub http: Option<SocketAddr>,
    pub websocket: Option<SocketAddr>,
}

/// Pre-allocated ports for all protocols enabled in the config.
///
/// Keep this alive for as long as the server owns the ports: dropping it hands
/// the slot to the next test.
pub struct PortReserver {
    addresses: ProtocolAddresses,
    _slot: SlotGuard,
}

/// Single port reservation for simpler binaries (MCP, connectors).
pub struct SinglePortReserver {
    address: SocketAddr,
    _slot: SlotGuard,
}

impl SinglePortReserver {
    pub fn new() -> Result<Self, TestBinaryError> {
        let mut slot = SlotGuard::acquire()?;
        slot.next_offset = SINGLE_PORT_OFFSET;
        let address = slot.next_addr(IpAddrKind::V4)?;
        Ok(Self {
            address,
            _slot: slot,
        })
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }
}

/// Pre-allocated ports for a cluster of servers.
pub struct ClusterPortReserver {
    nodes: Vec<PortReserver>,
}

impl ClusterPortReserver {
    /// Reserve ports for all nodes in a cluster.
    pub fn reserve(
        node_count: usize,
        ip_kind: IpAddrKind,
        config: &TestServerConfig,
    ) -> Result<Self, TestBinaryError> {
        let mut nodes = Vec::with_capacity(node_count);
        for _ in 0..node_count {
            nodes.push(PortReserver::reserve(ip_kind, config)?);
        }
        Ok(Self { nodes })
    }

    /// Get the addresses for all nodes.
    pub fn all_addresses(&self) -> Vec<ProtocolAddresses> {
        self.nodes.iter().map(|n| n.addresses()).collect()
    }

    /// Take ownership of individual port reservers for each node.
    pub fn into_reservers(self) -> Vec<PortReserver> {
        self.nodes
    }
}

impl PortReserver {
    /// Reserve ports for all protocols enabled in the config.
    pub fn reserve(
        ip_kind: IpAddrKind,
        config: &TestServerConfig,
    ) -> Result<Self, TestBinaryError> {
        let mut slot = SlotGuard::acquire()?;

        let tcp = Some(slot.next_addr(ip_kind)?);
        let tcp_replica = Some(slot.next_addr(ip_kind)?);

        let quic = if config.quic_enabled {
            Some(slot.next_addr(ip_kind)?)
        } else {
            None
        };

        let http = if config.http_enabled {
            Some(slot.next_addr(ip_kind)?)
        } else {
            None
        };

        let websocket = if config.websocket_enabled {
            Some(slot.next_addr(ip_kind)?)
        } else {
            None
        };

        Ok(Self {
            addresses: ProtocolAddresses {
                tcp,
                tcp_replica,
                quic,
                http,
                websocket,
            },
            _slot: slot,
        })
    }

    /// Get the reserved addresses for environment variable configuration.
    pub fn addresses(&self) -> ProtocolAddresses {
        self.addresses.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ports_of(reserver: &PortReserver) -> Vec<u16> {
        let addresses = reserver.addresses();
        [
            addresses.tcp,
            addresses.tcp_replica,
            addresses.quic,
            addresses.http,
            addresses.websocket,
        ]
        .into_iter()
        .flatten()
        .map(|addr| addr.port())
        .collect()
    }

    #[test]
    fn given_a_readable_range_when_parsed_should_take_both_bounds() {
        assert_eq!(
            parse_ephemeral_range("32768\t60999\n"),
            Some((32768, 60999))
        );
        assert_eq!(parse_ephemeral_range("1024 65535"), Some((1024, 65535)));
        assert_eq!(parse_ephemeral_range(""), None);
        assert_eq!(parse_ephemeral_range("32768"), None);
        assert_eq!(parse_ephemeral_range("garbage 60999"), None);
    }

    #[test]
    fn given_room_below_the_floor_when_choosing_a_band_should_take_it() {
        for (floor, ceiling) in [
            DEFAULT_EPHEMERAL_RANGE,
            (BAND_START + PORTS_PER_SLOT + 1, 60999),
            // Nothing above the ceiling either, so below is the only answer.
            (49152, FALLBACK_BAND_END),
        ] {
            let band = band_for(floor, ceiling).expect("range leaves room below the floor");
            assert_eq!(band.start, BAND_START);
            assert!(
                band.end() < floor,
                "band [{}, {}] runs into the ephemeral range [{floor}, {ceiling}]",
                band.start,
                band.end()
            );
        }
    }

    /// A floor tuned under the band start is not fatal on its own: the ports
    /// above the ceiling are just as clear of the kernel's picks, and a box
    /// tuned that way could not run the suite at all otherwise.
    #[test]
    fn given_no_room_below_the_floor_when_choosing_a_band_should_fall_back_above_the_ceiling() {
        for (floor, ceiling) in [
            (1024, 60999),
            (1024, 32767),
            (BAND_START, 60999),
            (0, 60999),
        ] {
            let band = band_for(floor, ceiling).expect("range leaves room above the ceiling");
            assert!(band.slots >= 1);
            assert!(
                band.start > ceiling,
                "band [{}, {}] runs into the ephemeral range [{floor}, {ceiling}]",
                band.start,
                band.end()
            );
        }
    }

    /// A range that spans both sides has no safe answer: every port the harness
    /// could hand out is one the kernel can hand out too.
    #[test]
    fn given_no_room_on_either_side_when_choosing_a_band_should_fail_loudly() {
        for (floor, ceiling) in [
            (1024, FALLBACK_BAND_END),
            (0, FALLBACK_BAND_END),
            (BAND_START, FALLBACK_BAND_END),
        ] {
            assert!(
                band_for(floor, ceiling).is_err(),
                "range [{floor}, {ceiling}] leaves no usable band and must be rejected"
            );
        }
    }

    #[test]
    fn given_a_slot_when_drained_should_refuse_to_spill_into_the_next() {
        let mut slot = SlotGuard::acquire().expect("a free slot");
        let base = slot.base_port;

        for offset in 0..PORTS_PER_SLOT {
            let addr = slot.next_addr(IpAddrKind::V4).expect("slot owns this port");
            assert_eq!(addr.port(), base + offset);
        }

        assert!(
            slot.next_addr(IpAddrKind::V4).is_err(),
            "a drained slot must not hand out the next slot's ports"
        );
    }

    /// The allocator rests on `flock` conflicting between two open file
    /// descriptions in one process, which is what a single-process `cargo test`
    /// run of the suite relies on.
    #[test]
    fn given_two_live_reservers_when_reserved_in_one_process_should_not_share_a_port() {
        // Enabled explicitly: this test is about ports, and must not go red
        // because a protocol's default flipped.
        let config = TestServerConfig::builder()
            .quic_enabled(true)
            .http_enabled(true)
            .websocket_enabled(true)
            .build();
        let first = PortReserver::reserve(IpAddrKind::V4, &config).expect("first reserver");
        let second = PortReserver::reserve(IpAddrKind::V4, &config).expect("second reserver");

        let first_ports = ports_of(&first);
        let second_ports = ports_of(&second);

        assert_eq!(
            first_ports.len(),
            5,
            "tcp, tcp_replica, quic, http and websocket"
        );
        for port in &first_ports {
            assert!(
                !second_ports.contains(port),
                "port {port} handed to two live reservers"
            );
        }
    }

    /// The single-port consumers bind a different socket-option flavor than
    /// the server's TCP listener, and a TIME_WAIT socket of one flavor blocks
    /// binds of the other, so their port must stay off the server offsets.
    #[test]
    fn given_a_single_port_reserver_when_reserved_should_stay_off_the_server_offsets() {
        let (floor, ceiling) = ephemeral_range();
        let band = band_for(floor, ceiling).expect("a usable band");
        let reserver = SinglePortReserver::new().expect("a free slot");
        let offset = (reserver.address().port() - band.start) % PORTS_PER_SLOT;
        assert_eq!(offset, SINGLE_PORT_OFFSET);
    }

    /// Reserving more times than the band has slots only works if a drop puts
    /// the slot back, which is what the whole suite running in one process does.
    #[test]
    fn given_more_reservations_than_slots_when_each_is_dropped_should_keep_succeeding() {
        let (floor, ceiling) = ephemeral_range();
        let band = band_for(floor, ceiling).expect("a usable band");
        for round in 0..usize::from(band.slots) + 16 {
            SinglePortReserver::new()
                .unwrap_or_else(|e| panic!("round {round} found no free slot: {e}"));
        }
    }
}
