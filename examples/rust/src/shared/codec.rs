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

use iggy::prelude::*;
use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};
use std::str::FromStr;

pub const STREAM_NAME: &str = "compression-stream";
pub const TOPIC_NAME: &str = "compression-topic";
pub const COMPRESSION_HEADER_KEY: &str = "iggy-compression";
pub const NUM_MESSAGES: u32 = 1000;

// Codec that defines available compression algorithms.
pub enum Codec {
    None,
    Lz4,
}

impl Display for Codec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Codec::None => write!(f, "none"),
            Codec::Lz4 => write!(f, "lz4"),
        }
    }
}

impl FromStr for Codec {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "lz4" => Ok(Codec::Lz4),
            "none" => Ok(Codec::None),
            _ => Err(format!("Unknown compression type: {s}")),
        }
    }
}

impl Codec {
    /// Returns the key to indicate compressed messages as HeaderKey.
    pub fn header_key() -> HeaderKey {
        HeaderKey::try_from(COMPRESSION_HEADER_KEY)
            .expect("COMPRESSION_HEADER_KEY is an InvalidHeaderKey.")
    }

    /// Returns the compression algorithm type as HeaderValue.
    pub fn to_header_value(&self) -> HeaderValue {
        HeaderValue::try_from(self.to_string()).expect("failed generating HeaderValue.")
    }

    /// Returns a Codec from a HeaderValue. Used when reading messages from the server.
    pub fn from_header_value(value: &HeaderValue) -> Self {
        let name = value
            .as_str()
            .expect("could not convert HeaderValue into str.");
        Self::from_str(name).expect("compression algorithm not available.")
    }

    /// Takes a message payload and compresses it using the algorithm from Codec.
    pub fn compress(&self, data: &[u8]) -> Vec<u8> {
        match self {
            Codec::None => data.to_vec(),
            Codec::Lz4 => {
                let mut compressed_data = Vec::new();
                let mut encoder = FrameEncoder::new(&mut compressed_data);
                encoder
                    .write_all(data)
                    .expect("Cannot write into buffer using Lz4 compression.");
                encoder.finish().expect("Cannot finish Lz4 compression.");
                compressed_data
            }
        }
    }

    /// Takes a compressed message payload and decompresses it using the algorithm from Codec.
    pub fn decompress(&self, data: &[u8]) -> Vec<u8> {
        match self {
            Codec::None => data.to_vec(),
            Codec::Lz4 => {
                let decoder = FrameDecoder::new(data);
                let mut decompressed_data = Vec::new();
                let bytes_read = decoder
                    .take(MAX_PAYLOAD_SIZE as u64 + 1)
                    .read_to_end(&mut decompressed_data)
                    .expect("Cannot decode payload using Lz4.");

                if bytes_read > MAX_PAYLOAD_SIZE as usize {
                    panic!("Decompressed message exceeds MAX_PAYLOAD_SIZE!")
                }
                decompressed_data
            }
        }
    }
}
