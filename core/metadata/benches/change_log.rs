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

// Timings and sizes are printed as floating point means and percentiles.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use consensus::{CLIENTS_TABLE_MAX, ClientTable};
use iggy_binary_protocol::primitives::metadata_changes::{
    WireMetadataChange, WireSessionBody, WireUserKey,
};
use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
use iggy_binary_protocol::requests::streams::CreateStreamRequest;
use iggy_binary_protocol::requests::topics::{
    CreateTopicRequest, CreateTopicWithAssignmentsRequest,
};
use iggy_binary_protocol::requests::users::CreateUserRequest;
use iggy_binary_protocol::requests::users::login_register::RegisterRequest;
use iggy_binary_protocol::{
    Command, Operation, PrepareHeader, WireEncode, WireIdentifier, WireName, WireOptions,
};
use iggy_common::{
    MetadataChangeGroup, MetadataChangeGroups, MetadataChangeStart, MetadataChangesQuery,
    UserStatus, variadic,
};
use metadata::change_log::{
    AppliedChange, ChangeLogCapacities, ChangeLogSnapshot, CurrentState, MetadataChangeLog,
    change_page, current_page,
};
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::Streams;
use metadata::stm::user::{ROOT_USER_CREATED_AT_MICROS, Users};
use metadata::{MuxStateMachine, apply_committed_prepare};
use server_common::Message;
use server_common::iobuf::Owned;
use std::cell::RefCell;
use std::env;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const HEADER_SIZE: usize = size_of::<PrepareHeader>();
const ROOT_USER_ID: u32 = 0;
const ROOT_CLIENT: u128 = 7;
const DEFAULT_STREAMS: usize = 1_000_000;
const PAGE_LIMIT: u32 = 1000;
const READER_THREADS: usize = 3;
const APPENDS_UNDER_READERS: u64 = 20_000;
const SNAPSHOT_ROUNDS: u32 = 5;

/// The metadata state machine of one replica, driven the way the commit path
/// drives it.
struct Replica {
    mux: MuxStateMachine<variadic!(Users, Streams)>,
    client_table: RefCell<ClientTable>,
    change_log: MetadataChangeLog,
    op: u64,
}

impl Replica {
    fn new() -> Self {
        let users = Users::default();
        users.ensure_root_user("iggy", "hash");
        let streams = Streams::default();
        Self {
            mux: MuxStateMachine::new(variadic!(users, streams)),
            client_table: RefCell::new(ClientTable::new(CLIENTS_TABLE_MAX)),
            change_log: MetadataChangeLog::new(ChangeLogCapacities::default()),
            op: 0,
        }
    }

    fn commit(
        &mut self,
        client: u128,
        operation: Operation,
        request: &impl WireEncode,
    ) -> Vec<AppliedChange> {
        self.op += 1;
        let body = request.to_bytes();
        let total = HEADER_SIZE + body.len();
        let mut buffer = Owned::<4096>::zeroed(total);
        {
            let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
                &mut buffer.as_mut_slice()[..HEADER_SIZE],
            );
            header.command = Command::Prepare;
            header.operation = operation;
            header.op = self.op;
            header.commit = self.op - 1;
            header.timestamp = self.op * 1_000;
            header.checksum = u128::from(self.op) << 64 | 1;
            header.client = client;
            header.user_id = ROOT_USER_ID;
            header.request = if operation == Operation::Register {
                0
            } else {
                self.op
            };
            header.retry_capacity = u32::try_from(CLIENTS_TABLE_MAX).expect("capacity");
            header.size = u32::try_from(total).expect("size");
        }
        buffer.as_mut_slice()[HEADER_SIZE..].copy_from_slice(&body);
        let prepare: Message<PrepareHeader> = Message::try_from(buffer).expect("valid prepare");
        let header = *prepare.header();
        let changes =
            apply_committed_prepare(&self.mux, &self.client_table, true, |_| {}, prepare).changes;
        self.change_log
            .append(&header, changes.clone())
            .expect("committed ops append in order");
        changes
    }
}

fn main() {
    let streams = env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_STREAMS);
    let mut replica = Replica::new();
    record_sizes(&mut replica);
    build_catalog(&mut replica, streams);
    current_paging(&replica, streams);
    fill_activity_ring(&mut replica);
    snapshot_round_trip(&replica);
    publication_under_readers(&mut replica);
}

fn record_sizes(replica: &mut Replica) {
    println!("measurement, kind, value, unit");
    let register = replica.commit(
        ROOT_CLIENT,
        Operation::Register,
        &RegisterRequest {
            user_created_at: ROOT_USER_CREATED_AT_MICROS,
            bind_verifier: [0x5a; 32],
        },
    );
    let stream = replica.commit(ROOT_CLIENT, Operation::CreateStream, &create_stream(0));
    let topic = replica.commit(
        ROOT_CLIENT,
        Operation::CreateTopicWithAssignments,
        &CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                partitions_count: 3,
                name: WireName::new("events").expect("name"),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
            partitions: (0..3)
                .map(|partition_id| CreatedPartitionAssignment {
                    partition_id,
                    consensus_group_id: 1 + u64::from(partition_id),
                    initial_view: 0,
                })
                .collect(),
        },
    );
    let user = replica.commit(
        ROOT_CLIENT,
        Operation::CreateUser,
        &CreateUserRequest {
            username: WireName::new("operator").expect("name"),
            password: "hash".to_owned(),
            status: UserStatus::Active.as_code(),
            permissions: None,
            options: WireOptions::empty(),
        },
    );
    for change in register.iter().chain(&stream).chain(&topic).chain(&user) {
        println!(
            "record_body, {:?}, {}, bytes",
            change.kind,
            change.body.len()
        );
    }
}

fn build_catalog(replica: &mut Replica, streams: usize) {
    let start = Instant::now();
    for index in 1..streams {
        black_box(replica.commit(ROOT_CLIENT, Operation::CreateStream, &create_stream(index)));
    }
    let elapsed = start.elapsed();
    println!("catalog, streams, {streams}, count");
    println!(
        "apply_and_append, create_stream, {:.0}, ns_per_op",
        elapsed.as_nanos() as f64 / streams as f64
    );
}

fn current_paging(replica: &Replica, streams: usize) {
    let groups = MetadataChangeGroups::requested(&[MetadataChangeGroup::Streams]);
    let head_op = replica.op;
    let table = replica.client_table.borrow();
    let mut token = None;
    let mut pages = 0u32;
    let mut items = 0usize;
    let mut slowest = Duration::ZERO;
    let start = Instant::now();
    loop {
        let page_start = Instant::now();
        let page = replica.mux.streams().read(|streams_inner| {
            replica.mux.users().read(|users_inner| {
                replica
                    .change_log
                    .read(|log| {
                        current_page(
                            log,
                            head_op,
                            groups,
                            token.as_ref(),
                            PAGE_LIMIT,
                            &CurrentState {
                                streams: streams_inner,
                                users: users_inner,
                                sessions: Some(&table),
                            },
                        )
                    })
                    .expect("writer present")
                    .expect("current page")
            })
        });
        slowest = slowest.max(page_start.elapsed());
        pages += 1;
        items += page.items.len();
        match page.current_page_token {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    let elapsed = start.elapsed();
    assert_eq!(items, streams);
    println!("current_paging, pages_of_{PAGE_LIMIT}, {pages}, count");
    println!(
        "current_paging, page_mean, {:.1}, us",
        elapsed.as_secs_f64() * 1e6 / f64::from(pages)
    );
    println!(
        "current_paging, page_max, {:.1}, us",
        slowest.as_secs_f64() * 1e6
    );
    println!("current_paging, full_scan, {:.3}, s", elapsed.as_secs_f64());
}

/// Session records until the activity ring holds as many as it keeps.
fn fill_activity_ring(replica: &mut Replica) {
    let capacity = ChangeLogCapacities::default().activity;
    for session in 0..u64::try_from(capacity).expect("capacity") {
        replica.op += 1;
        let header = PrepareHeader {
            op: replica.op,
            checksum: u128::from(replica.op) << 64 | 1,
            timestamp: replica.op * 1_000,
            ..PrepareHeader::default()
        };
        let change = AppliedChange::new(
            &WireMetadataChange::SessionRegistered(WireSessionBody {
                user: WireUserKey {
                    user_id: ROOT_USER_ID,
                    user_created_at: 0,
                },
                client_id: u128::from(session) + 1_000,
                session,
            }),
            Some(ROOT_USER_ID),
        );
        replica
            .change_log
            .append(&header, vec![change])
            .expect("monotone append");
    }
}

fn snapshot_round_trip(replica: &Replica) {
    let mut capture = Duration::ZERO;
    let mut encode = Duration::ZERO;
    let mut decode = Duration::ZERO;
    let mut restore = Duration::ZERO;
    let mut bytes = 0;
    for _ in 0..SNAPSHOT_ROUNDS {
        let start = Instant::now();
        let snapshot = replica.change_log.snapshot();
        capture += start.elapsed();

        let start = Instant::now();
        let encoded = rmp_serde::to_vec(&snapshot).expect("encodes");
        encode += start.elapsed();
        bytes = encoded.len();

        let start = Instant::now();
        let decoded: ChangeLogSnapshot = rmp_serde::from_slice(&encoded).expect("decodes");
        decode += start.elapsed();

        let start = Instant::now();
        decoded.validate(replica.op).expect("valid snapshot");
        black_box(
            decoded
                .restore(ChangeLogCapacities::default())
                .expect("restores"),
        );
        restore += start.elapsed();
    }
    let mean = |total: Duration| total.as_secs_f64() * 1e3 / f64::from(SNAPSHOT_ROUNDS);
    let capacities = ChangeLogCapacities::default();
    println!(
        "full_ring_snapshot, records, {}, count",
        capacities.resource + capacities.activity
    );
    println!("full_ring_snapshot, encoded, {bytes}, bytes");
    println!("full_ring_snapshot, capture, {:.2}, ms", mean(capture));
    println!("full_ring_snapshot, encode, {:.2}, ms", mean(encode));
    println!("full_ring_snapshot, decode, {:.2}, ms", mean(decode));
    println!(
        "full_ring_snapshot, validate_and_restore, {:.2}, ms",
        mean(restore)
    );
}

fn publication_under_readers(replica: &mut Replica) {
    let stop = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..READER_THREADS)
        .map(|_| {
            let factory = replica.change_log.factory();
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                let log = MetadataChangeLog::from_factory(factory);
                let query = MetadataChangesQuery {
                    start: MetadataChangeStart::Earliest,
                    limit: PAGE_LIMIT,
                    ..MetadataChangesQuery::default()
                };
                let groups = MetadataChangeGroups::requested(&[]);
                let mut pages = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    black_box(log.read(|log| {
                        change_page(log, log.last_record_op(), &query, groups).expect("change page")
                    }));
                    pages += 1;
                }
                pages
            })
        })
        .collect();
    let mut latencies = Vec::with_capacity(usize::try_from(APPENDS_UNDER_READERS).expect("count"));
    for index in 0..APPENDS_UNDER_READERS {
        replica.op += 1;
        let header = PrepareHeader {
            op: replica.op,
            checksum: u128::from(replica.op) << 64 | 1,
            timestamp: replica.op * 1_000,
            ..PrepareHeader::default()
        };
        let change = AppliedChange::new(
            &WireMetadataChange::SessionRegistered(WireSessionBody {
                user: WireUserKey {
                    user_id: ROOT_USER_ID,
                    user_created_at: 0,
                },
                client_id: u128::from(index) + 1_000_000,
                session: index,
            }),
            Some(ROOT_USER_ID),
        );
        let start = Instant::now();
        replica
            .change_log
            .append(&header, vec![change])
            .expect("monotone append");
        latencies.push(start.elapsed());
    }
    stop.store(true, Ordering::Relaxed);
    let reader_pages: u64 = readers
        .into_iter()
        .map(|reader| reader.join().expect("reader thread"))
        .sum();
    latencies.sort_unstable();
    let percentile = |fraction: f64| {
        let index = ((latencies.len() - 1) as f64 * fraction) as usize;
        latencies[index].as_secs_f64() * 1e6
    };
    println!("publication, readers, {READER_THREADS}, threads");
    println!("publication, reader_pages, {reader_pages}, count");
    println!("publication, append_p50, {:.1}, us", percentile(0.50));
    println!("publication, append_p99, {:.1}, us", percentile(0.99));
    println!("publication, append_max, {:.1}, us", percentile(1.0));
}

fn create_stream(index: usize) -> CreateStreamRequest {
    CreateStreamRequest {
        name: WireName::new(format!("stream-{index}")).expect("name"),
        options: WireOptions::empty(),
    }
}
