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

use messages::consensus::Message;

// TODO: Define a trait (probably in some external crate)
#[expect(unused)]
trait Metadata {
    // TODO: I was thinking about having the `RequestHeader` be generic over type of request
    // but I think having the `Operation` enum return an discriminant, and handle it outside of `Metadata` is good enough.
    type Header;
    fn handle(&self, message: Message<Self::Header>);
}

#[expect(unused)]
struct IggyMetadata<C, M, J, S> {
    consensus: C,
    mux_stm: M,
    journal: J,
    snapshot: S,
}

impl<C, M, J, S> Metadata for IggyMetadata<C, M, J, S> {
    type Header = (); // RequestMetadataHeader;

    #[expect(unused)]
    fn handle(&self, message: Message<Self::Header>) {}
}
