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

use super::run_bench_and_wait_for_finish;
use iggy::utils::byte_size::IggyByteSize;
use integration::test_server::{IpAddrKind, TestServer, Transport};
use serial_test::parallel;
use std::str::FromStr;

#[test]
#[parallel]
fn tcp_ipv4_bench() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    run_bench_and_wait_for_finish(
        &server_addr,
        Transport::Tcp,
        "pinned-producer",
        IggyByteSize::from_str("8MB").unwrap(),
    );
    run_bench_and_wait_for_finish(
        &server_addr,
        Transport::Tcp,
        "pinned-consumer",
        IggyByteSize::from_str("8MB").unwrap(),
    );
}

#[cfg_attr(feature = "ci-qemu", ignore)]
#[test]
#[parallel]
fn tcp_ipv6_bench() {
    let mut test_server = TestServer::new(None, true, None, IpAddrKind::V6);
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    run_bench_and_wait_for_finish(
        &server_addr,
        Transport::Tcp,
        "pinned-producer",
        IggyByteSize::from_str("8MB").unwrap(),
    );
    run_bench_and_wait_for_finish(
        &server_addr,
        Transport::Tcp,
        "pinned-consumer",
        IggyByteSize::from_str("8MB").unwrap(),
    );
}
