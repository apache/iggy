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

use crate::error::IggyError;
use crate::types::message::MAX_PAYLOAD_SIZE;
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use serde::{
    Deserialize, Serialize, Serializer,
    de::{self, Deserializer, Visitor},
};
use std::{
    fmt::{Display, Formatter},
    io::{Read, Write},
    str::FromStr,
};

// we should consider brotli as well.
/// Supported compression algorithms
#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum CompressionAlgorithm {
    // No compression
    #[default]
    None,
    // Available compression algorithms
    Gzip,
    Zstd,
    Lz4,
    Snappy,
}

impl FromStr for CompressionAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "gzip" => Ok(CompressionAlgorithm::Gzip),
            "zstd" => Ok(CompressionAlgorithm::Zstd),
            "lz4" => Ok(CompressionAlgorithm::Lz4),
            "snappy" => Ok(CompressionAlgorithm::Snappy),
            "none" => Ok(CompressionAlgorithm::None),
            _ => Err(format!("Unknown compression type: {s}")),
        }
    }
}

impl CompressionAlgorithm {
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        match self {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Gzip => {
                let mut compressed_data = Vec::new();
                let mut encoder = GzEncoder::new(&mut compressed_data, Compression::default());
                encoder
                    .write_all(data)
                    .map_err(|e| IggyError::CompressionError(e.to_string()))?;
                encoder
                    .finish()
                    .map_err(|e| IggyError::CompressionError(e.to_string()))?;
                Ok(compressed_data)
            }
            CompressionAlgorithm::Zstd => {
                let compressed_data = zstd::encode_all(data, 0)
                    .map_err(|e| IggyError::CompressionError(e.to_string()))?;
                Ok(compressed_data)
            }
            CompressionAlgorithm::Lz4 => {
                let mut compressed_data = Vec::new();
                let mut encoder = FrameEncoder::new(&mut compressed_data);
                encoder
                    .write_all(data)
                    .map_err(|e| IggyError::CompressionError(e.to_string()))?;
                encoder
                    .finish()
                    .map_err(|e| IggyError::CompressionError(e.to_string()))?;
                Ok(compressed_data)
            }
            CompressionAlgorithm::Snappy => {
                let compressed_data = snap::raw::Encoder::new()
                    .compress_vec(data)
                    .map_err(|e| IggyError::CompressionError(e.to_string()))?;
                Ok(compressed_data)
            }
        }
    }

    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, IggyError> {
        match self {
            CompressionAlgorithm::None => {
                if data.len() > MAX_PAYLOAD_SIZE as usize {
                    return Err(IggyError::DecompressionError(format!(
                        "payload length exceeds MAX_PAYLOAD_SIZE: {}",
                        MAX_PAYLOAD_SIZE
                    )));
                }
                Ok(data.to_vec())
            }
            CompressionAlgorithm::Gzip => {
                let decoder = GzDecoder::new(data);
                let mut decompressed_data = Vec::new();
                let read_bytes = decoder
                    .take(MAX_PAYLOAD_SIZE as u64 + 1)
                    .read_to_end(&mut decompressed_data)
                    .map_err(|e| IggyError::DecompressionError(e.to_string()))?;
                // has_data_left method currently in experimental nightly
                if read_bytes > MAX_PAYLOAD_SIZE as usize {
                    return Err(IggyError::DecompressionError(format!(
                        "payload length exceeds MAX_PAYLOAD_SIZE: {}",
                        MAX_PAYLOAD_SIZE
                    )));
                }
                Ok(decompressed_data)
            }
            CompressionAlgorithm::Zstd => {
                let decoder = zstd::stream::Decoder::new(data)
                    .map_err(|e| IggyError::DecompressionError(e.to_string()))?;
                let mut decompressed_data = Vec::new();
                let bytes_read = decoder
                    .take(MAX_PAYLOAD_SIZE as u64 + 1)
                    .read_to_end(&mut decompressed_data)
                    .map_err(|e| IggyError::DecompressionError(e.to_string()))?;

                if bytes_read > MAX_PAYLOAD_SIZE as usize {
                    return Err(IggyError::DecompressionError(format!(
                        "payload length exceeds MAX_PAYLOAD_SIZE: {}",
                        MAX_PAYLOAD_SIZE
                    )));
                }
                Ok(decompressed_data)
            }
            CompressionAlgorithm::Lz4 => {
                let decoder = FrameDecoder::new(data);
                let mut decompressed_data = Vec::new();
                let bytes_read = decoder
                    .take(MAX_PAYLOAD_SIZE as u64 + 1)
                    .read_to_end(&mut decompressed_data)
                    .map_err(|e| IggyError::DecompressionError(e.to_string()))?;

                if bytes_read > MAX_PAYLOAD_SIZE as usize {
                    return Err(IggyError::DecompressionError(format!(
                        "payload length exceeds MAX_PAYLOAD_SIZE: {}",
                        MAX_PAYLOAD_SIZE
                    )));
                }
                Ok(decompressed_data)
            }
            CompressionAlgorithm::Snappy => {
                let decompressed_len = snap::raw::decompress_len(data)
                    .map_err(|e| IggyError::DecompressionError(e.to_string()))?;

                if decompressed_len > MAX_PAYLOAD_SIZE as usize {
                    return Err(IggyError::DecompressionError(format!(
                        "payload length exceeds MAX_PAYLOAD_SIZE: {}",
                        MAX_PAYLOAD_SIZE
                    )));
                }

                let mut decoder = snap::raw::Decoder::new();
                let decompressed_data = decoder
                    .decompress_vec(data)
                    .map_err(|e| IggyError::DecompressionError(e.to_string()))?;
                Ok(decompressed_data)
            }
        }
    }

    pub fn as_code(&self) -> u8 {
        match self {
            CompressionAlgorithm::None => 1,
            CompressionAlgorithm::Gzip => 2,
            CompressionAlgorithm::Zstd => 3,
            CompressionAlgorithm::Lz4 => 4,
            CompressionAlgorithm::Snappy => 5,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, IggyError> {
        match code {
            1 => Ok(CompressionAlgorithm::None),
            2 => Ok(CompressionAlgorithm::Gzip),
            3 => Ok(CompressionAlgorithm::Zstd),
            4 => Ok(CompressionAlgorithm::Lz4),
            5 => Ok(CompressionAlgorithm::Snappy),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

impl Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionAlgorithm::None => write!(f, "none"),
            CompressionAlgorithm::Gzip => write!(f, "gzip"),
            CompressionAlgorithm::Zstd => write!(f, "zstd"),
            CompressionAlgorithm::Lz4 => write!(f, "lz4"),
            CompressionAlgorithm::Snappy => write!(f, "snappy"),
        }
    }
}

impl Serialize for CompressionAlgorithm {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            CompressionAlgorithm::None => serializer.serialize_str("none"),
            CompressionAlgorithm::Gzip => serializer.serialize_str("gzip"),
            CompressionAlgorithm::Zstd => serializer.serialize_str("zstd"),
            CompressionAlgorithm::Lz4 => serializer.serialize_str("lz4"),
            CompressionAlgorithm::Snappy => serializer.serialize_str("snappy"),
        }
    }
}

impl From<CompressionAlgorithm> for String {
    fn from(value: CompressionAlgorithm) -> Self {
        match value {
            CompressionAlgorithm::None => "none".to_string(),
            CompressionAlgorithm::Gzip => "gzip".to_string(),
            CompressionAlgorithm::Zstd => "zstd".to_string(),
            CompressionAlgorithm::Lz4 => "lz4".to_string(),
            CompressionAlgorithm::Snappy => "snappy".to_string(),
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub struct ClientCompressionConfig {
    pub algorithm: CompressionAlgorithm,
    pub min_size: u32,
}

struct CompressionKindVisitor;

impl Visitor<'_> for CompressionKindVisitor {
    type Value = CompressionAlgorithm;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a valid compression type, check documentation for more information.")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        CompressionAlgorithm::from_str(value).map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for CompressionAlgorithm {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(CompressionKindVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALGORITHM_OPTIONS: [CompressionAlgorithm; 5] = [
        CompressionAlgorithm::None,
        CompressionAlgorithm::Gzip,
        CompressionAlgorithm::Zstd,
        CompressionAlgorithm::Lz4,
        CompressionAlgorithm::Snappy,
    ];

    fn cycle_with_all_algorithms(config: CompressionAlgorithm, data: &[u8]) {
        let compressed_data = config.compress(data).expect("compression failed");
        let decompressed_data = config
            .decompress(&compressed_data)
            .expect("decompression failed");
        assert_eq!(
            data, &decompressed_data,
            "compression/ decompression mismatch for algorithm {:?}",
            config
        );
    }

    #[test]
    fn test_from() {
        let none_alg = CompressionAlgorithm::from_str("none");
        assert!(none_alg.is_ok());
        assert_eq!(none_alg.unwrap(), CompressionAlgorithm::None);

        let none_alg = CompressionAlgorithm::from_str("None");
        assert!(none_alg.is_ok());
        assert_eq!(none_alg.unwrap(), CompressionAlgorithm::None);

        let gzip_alg = CompressionAlgorithm::from_str("gzip");
        assert!(gzip_alg.is_ok());
        assert_eq!(gzip_alg.unwrap(), CompressionAlgorithm::Gzip);

        let gzip_alg = CompressionAlgorithm::from_str("Gzip");
        assert!(gzip_alg.is_ok());
        assert_eq!(gzip_alg.unwrap(), CompressionAlgorithm::Gzip);

        let zstd_alg = CompressionAlgorithm::from_str("zstd");
        assert!(zstd_alg.is_ok());
        assert_eq!(zstd_alg.unwrap(), CompressionAlgorithm::Zstd);

        let zstd_alg = CompressionAlgorithm::from_str("Zstd");
        assert!(zstd_alg.is_ok());
        assert_eq!(zstd_alg.unwrap(), CompressionAlgorithm::Zstd);

        let lz4_alg = CompressionAlgorithm::from_str("lz4");
        assert!(lz4_alg.is_ok());
        assert_eq!(lz4_alg.unwrap(), CompressionAlgorithm::Lz4);

        let lz4_alg = CompressionAlgorithm::from_str("Lz4");
        assert!(lz4_alg.is_ok());
        assert_eq!(lz4_alg.unwrap(), CompressionAlgorithm::Lz4);

        let snappy_alg = CompressionAlgorithm::from_str("snappy");
        assert!(snappy_alg.is_ok());
        assert_eq!(snappy_alg.unwrap(), CompressionAlgorithm::Snappy);

        let snappy_alg = CompressionAlgorithm::from_str("Snappy");
        assert!(snappy_alg.is_ok());
        assert_eq!(snappy_alg.unwrap(), CompressionAlgorithm::Snappy);
    }

    #[test]
    fn test_from_invalid_input() {
        let invalid_compression_kind = CompressionAlgorithm::from_str("invalid");
        assert!(invalid_compression_kind.is_err());

        let invalid_compression_kind = CompressionAlgorithm::from_str("gzipp");
        assert!(invalid_compression_kind.is_err());

        let invalid_compression_kind = CompressionAlgorithm::from_str("Zste");
        assert!(invalid_compression_kind.is_err());

        let invalid_compression_kind = CompressionAlgorithm::from_str("Lt4");
        assert!(invalid_compression_kind.is_err());

        let invalid_compression_kind = CompressionAlgorithm::from_str("snapy");
        assert!(invalid_compression_kind.is_err());
    }

    #[test]
    fn test_into() {
        let none: CompressionAlgorithm = CompressionAlgorithm::None;
        let none_string: String = none.into();
        assert_eq!(none_string, "none".to_string());

        let gzip: CompressionAlgorithm = CompressionAlgorithm::Gzip;
        let gzip_string: String = gzip.into();
        assert_eq!(gzip_string, "gzip".to_string());

        let zstd: CompressionAlgorithm = CompressionAlgorithm::Zstd;
        let zstd_string: String = zstd.into();
        assert_eq!(zstd_string, "zstd".to_string());

        let lz4: CompressionAlgorithm = CompressionAlgorithm::Lz4;
        let lz4_string: String = lz4.into();
        assert_eq!(lz4_string, "lz4".to_string());

        let snappy: CompressionAlgorithm = CompressionAlgorithm::Snappy;
        let snappy_string: String = snappy.into();
        assert_eq!(snappy_string, "snappy".to_string());
    }

    #[test]
    fn test_as_code() {
        let none = CompressionAlgorithm::None;
        let none_code = none.as_code();
        assert_eq!(none_code, 1);

        let gzip = CompressionAlgorithm::Gzip;
        let gzip_code = gzip.as_code();
        assert_eq!(gzip_code, 2);

        let zstd = CompressionAlgorithm::Zstd;
        let zstd_code = zstd.as_code();
        assert_eq!(zstd_code, 3);

        let lz4 = CompressionAlgorithm::Lz4;
        let lz4_code = lz4.as_code();
        assert_eq!(lz4_code, 4);

        let snappy = CompressionAlgorithm::Snappy;
        let snappy_code = snappy.as_code();
        assert_eq!(snappy_code, 5);
    }

    #[test]
    fn test_from_code() {
        let none = CompressionAlgorithm::from_code(1);
        assert!(none.is_ok());
        assert_eq!(none.unwrap(), CompressionAlgorithm::None);

        let gzip = CompressionAlgorithm::from_code(2);
        assert!(gzip.is_ok());
        assert_eq!(gzip.unwrap(), CompressionAlgorithm::Gzip);

        let zstd = CompressionAlgorithm::from_code(3);
        assert!(zstd.is_ok());
        assert_eq!(zstd.unwrap(), CompressionAlgorithm::Zstd);

        let lz4 = CompressionAlgorithm::from_code(4);
        assert!(lz4.is_ok());
        assert_eq!(lz4.unwrap(), CompressionAlgorithm::Lz4);

        let snappy = CompressionAlgorithm::from_code(5);
        assert!(snappy.is_ok());
        assert_eq!(snappy.unwrap(), CompressionAlgorithm::Snappy);
    }

    #[test]
    fn test_from_code_invalid_input() {
        let invalid_compression_kind = CompressionAlgorithm::from_code(0);
        assert!(invalid_compression_kind.is_err());

        let invalid_compression_kind = CompressionAlgorithm::from_code(69);
        assert!(invalid_compression_kind.is_err());

        let invalid_compression_kind = CompressionAlgorithm::from_code(255);
        assert!(invalid_compression_kind.is_err());
    }

    #[test]
    fn test_empty_input_compression_decompression() {
        for config in ALGORITHM_OPTIONS {
            cycle_with_all_algorithms(config, b"");
        }
    }

    #[test]
    fn test_small_input_compression_decompression() {
        for config in ALGORITHM_OPTIONS {
            cycle_with_all_algorithms(config, b"a");
            cycle_with_all_algorithms(config, b"a1");
            cycle_with_all_algorithms(config, b"a1B");
        }
    }

    #[test]
    fn test_large_input_compression_decompression() {
        for config in ALGORITHM_OPTIONS {
            cycle_with_all_algorithms(config, &b"Testing large payload size.".repeat(256));
            cycle_with_all_algorithms(config, &b"Testing even larger payload size.".repeat(1024));
            cycle_with_all_algorithms(
                config,
                &b"Testing even more larger payload size.".repeat(2048),
            );
        }
    }

    #[test]
    fn test_unicode_input_compression_decompression() {
        let wired_data =
            "A̷ЯѢ Ψβ ﻉـم שָׁ𐤀 龘 ね゙こ̽ 안ㅎ👾🌪️💥🀄 QяЖ Ҩц ϟϘ شـر 🕎תּוֹר 龍🉐 道め 💫🧿".as_bytes();
        for config in ALGORITHM_OPTIONS {
            cycle_with_all_algorithms(config, wired_data);
        }
    }

    #[test]
    fn test_compression_sizes_are_different() {
        let data = vec![0u8; 100_000];
        let mut sizes = Vec::new();
        for config in ALGORITHM_OPTIONS {
            let compressed = config.compress(&data).unwrap();
            sizes.push((format!("{:?}", config), compressed.len()));
        }
        let unique_sizes: std::collections::HashSet<_> = sizes.iter().map(|(_, s)| *s).collect();
        assert!(
            unique_sizes.len() >= 2,
            "All alogrithms produced the same size for compressed data: {:?}",
            sizes
        );
    }

    #[test]
    fn test_decompression_exceeds_max_payload_size() {
        let data = vec![0u8; MAX_PAYLOAD_SIZE as usize + 1];

        for algorithm in ALGORITHM_OPTIONS {
            let compressed = algorithm
                .compress(&data)
                .unwrap_or_else(|_| panic!("compression should succeed for {:?}", algorithm));

            let result = algorithm.decompress(&compressed);
            assert!(
                result.is_err(),
                "decompression should fail for {:?} with data.len() > MAX_PAYLOAD_SIZE",
                algorithm
            );

            let message = result.unwrap_err();
            assert!(
                message.to_string().contains("MAX_PAYLOAD_SIZE"),
                "error message should contain MAX_PAYLOAD_SIZE for {:?}",
                algorithm
            );
        }
    }

    #[test]
    fn test_decompression_at_max_payload_size() {
        let data = vec![0u8; MAX_PAYLOAD_SIZE as usize];

        for algorithm in ALGORITHM_OPTIONS {
            let compressed = algorithm
                .compress(&data)
                .unwrap_or_else(|_| panic!("compression should succeed for {:?}", algorithm));

            let decompressed = algorithm
                .decompress(&compressed)
                .unwrap_or_else(|_| {
                    panic!(
                        "decompression should succeed with data.len() == MAX_PAYLOAD_SIZE for algorithm {:?}",
                        algorithm
                    )
                });

            assert_eq!(
                decompressed.len(),
                MAX_PAYLOAD_SIZE as usize,
                "decompressed size should be equal to MAX_PAYLOAD_SIZE for algorithm {:?}",
                algorithm
            );
        }
    }
}
