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

//! A library awaits a producer send inside futures of its own, and its users await the library
//! inside theirs. The compiler computes the layout of each future through every future it awaits
//! and fails past the crate's recursion limit, 128 by default. This target builds sixteen nested
//! layers over a send under a limit well below the default, so a deeper send future fails its
//! build.

#![recursion_limit = "72"]

use iggy::prelude::{
    DirectConfig, IggyClient, IggyError, IggyProducer, Partitioning, SendMessagesResponse,
};

async fn layer_0(producer: &IggyProducer) -> Result<SendMessagesResponse, IggyError> {
    producer.send_with_partitioning(Vec::new(), None).await
}

macro_rules! layers {
    ($($outer:ident => $inner:ident),+ $(,)?) => {
        $(
            async fn $outer(producer: &IggyProducer) -> Result<SendMessagesResponse, IggyError> {
                $inner(producer).await
            }
        )+
    };
}

layers!(
    layer_1 => layer_0,
    layer_2 => layer_1,
    layer_3 => layer_2,
    layer_4 => layer_3,
    layer_5 => layer_4,
    layer_6 => layer_5,
    layer_7 => layer_6,
    layer_8 => layer_7,
    layer_9 => layer_8,
    layer_10 => layer_9,
    layer_11 => layer_10,
    layer_12 => layer_11,
    layer_13 => layer_12,
    layer_14 => layer_13,
    layer_15 => layer_14,
    layer_16 => layer_15,
);

/// The size of the future that `create` returns, which makes the compiler compute its layout.
fn layout_bytes<F: Future>(_create: impl FnOnce() -> F) -> usize {
    size_of::<F>()
}

#[test]
fn given_sixteen_layers_over_a_send_when_built_under_a_recursion_limit_of_72_should_compile() {
    let producer = IggyClient::default()
        .producer("stream", "topic")
        .expect("valid names")
        .partitioning(Partitioning::partition_id(0))
        .direct(DirectConfig::builder().build())
        .build();

    assert!(layout_bytes(|| layer_16(&producer)) > 0);
}
