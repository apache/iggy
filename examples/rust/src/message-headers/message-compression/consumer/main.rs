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

// The compression and decompression utilities are shared between the producer and consumer compression examples.
// Hence, we import them here.
#[path = "../codec.rs"]
mod codec;
use codec::{CONSUMER_NAME, Codec, NUM_MESSAGES, STREAM_NAME, TOPIC_NAME};

use bytes::Bytes;
use futures_util::stream::StreamExt;
use iggy::prelude::*;

#[tokio::main]
async fn main() -> Result<(), IggyError> {
    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address("127.0.0.1:8090".to_string())
        .build()?;

    client.connect().await?;

    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await?;

    let mut consumer = client
        .consumer(CONSUMER_NAME, STREAM_NAME, TOPIC_NAME, 0)?
        .build();

    consumer.init().await?;

    let mut consumed_messages = 0;
    while let Some(message) = consumer.next().await {
        let mut received_message = message.expect("Message was not received from server.");
        handle_payload_compression(&mut received_message)?;
        consumed_messages += 1;
        println!(
            "Message payload was decompressed and reads: {:?}",
            received_message.message.payload
        );
        if consumed_messages == NUM_MESSAGES {
            return Ok(());
        }
    }

    Ok(())
}

fn handle_payload_compression(msg: &mut ReceivedMessage) -> Result<(), IggyError> {
    // check if the message payload is compressed by inspecting the user-header
    if let Ok(Some(algorithm)) = msg.message.get_user_header(&Codec::header_key()) {
        // setup the codec with the compression algorithm defined in the user-header (value)
        let codec = Codec::from_header_value(&algorithm);

        // decompress the payload and update the payload length
        let decompressed_payload = codec.decompress(&msg.message.payload)?;
        msg.message.payload = Bytes::from(decompressed_payload);
        msg.message.header.payload_length = msg.message.payload.len() as u32;

        // remove the compression header since payload is now decompressed
        if let Ok(Some(mut headers_map)) = msg.message.user_headers_map() {
            headers_map.remove(&Codec::header_key());
            let headers_bytes = headers_map.to_bytes();
            msg.message.header.user_headers_length = headers_bytes.len() as u32;
            msg.message.user_headers = if headers_map.is_empty() {
                None
            } else {
                Some(headers_bytes)
            };
        }
    }
    Ok(())
}
