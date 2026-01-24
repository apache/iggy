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

pub const CONSUMER_NAME: &str = "example-consumer";
pub const STREAM_NAME: &str = "compression-stream";
pub const TOPIC_NAME: &str = "compression-topic";
pub const COMPRESSION_HEADER_KEY: &str = "iggy-compression";
pub const NUM_MESSAGES: u32 = 1000;

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
    pub fn header_key() -> HeaderKey {
        HeaderKey::new(COMPRESSION_HEADER_KEY)
            .expect("COMPRESSION_HEADER_KEY is an InvalidHeaderKey")
    }

    pub fn to_header_value(&self) -> HeaderValue {
        HeaderValue::from_str(&self.to_string())
            .expect("algorithm name is not a valid CompressionAlgorithm")
    }

    pub fn from_header_value(value: &HeaderValue) -> Self {
        let name = value
            .as_str()
            .expect("could not convert HeaderValue into str.");
        Self::from_str(name).expect("compression algorithm not available.")
    }

    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        match self {
            Codec::None => Ok(data.to_vec()),
            Codec::Lz4 => {
                let mut compressed_data = Vec::new();
                let mut encoder = FrameEncoder::new(&mut compressed_data);
                encoder
                    .write_all(data)
                    .expect("Cannot write into buffer using Lz4 compression.");
                encoder.finish().expect("Cannot finish Lz4 compression.");
                Ok(compressed_data)
            }
        }
    }

    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        match self {
            Codec::None => Ok(data.to_vec()),
            Codec::Lz4 => {
                let mut decoder = FrameDecoder::new(data);
                let mut decompressed_data = Vec::new();
                decoder
                    .read_to_end(&mut decompressed_data)
                    .expect("Cannot decode message payload using Lz4.");
                Ok(decompressed_data)
            }
        }
    }
}
