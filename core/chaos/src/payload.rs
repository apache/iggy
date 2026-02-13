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

//! Deterministic checksummed payloads for chaos testing.
//!
//! Layout: `[worker_id: u32 LE][seq: u64 LE][crc32: u32 LE][fill...]`
//!
//! The CRC covers the entire payload *except* the CRC field itself (bytes 0..12 + 16..).
//! Fill bytes are a deterministic pattern derived from `seq` to avoid RNG overhead
//! while still varying content across messages.

const HEADER_SIZE: usize = 16;
// [0..4]   worker_id: u32 LE
// [4..12]  seq: u64 LE
// [12..16] crc32: u32 LE

/// Generates deterministic, self-verifiable payloads.
///
/// Reuses a single buffer across calls - zero heap allocation per message
/// (caller copies into `Bytes`).
pub struct PayloadGenerator {
    worker_id: u32,
    seq: u64,
    buf: Vec<u8>,
}

impl PayloadGenerator {
    pub fn new(worker_id: u32, msg_size: u32) -> Self {
        let size = (msg_size as usize).max(HEADER_SIZE);
        Self {
            worker_id,
            seq: 0,
            buf: vec![0u8; size],
        }
    }

    /// Write next payload into the internal buffer and return a reference.
    pub fn next(&mut self) -> &[u8] {
        let seq = self.seq;
        self.seq += 1;

        self.buf[0..4].copy_from_slice(&self.worker_id.to_le_bytes());
        self.buf[4..12].copy_from_slice(&seq.to_le_bytes());

        // Deterministic fill derived from seq
        let fill_byte = (seq & 0xFF) as u8;
        for b in &mut self.buf[HEADER_SIZE..] {
            *b = fill_byte;
        }

        // CRC covers everything except the CRC field itself (bytes 12..16)
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.buf[..12]);
        hasher.update(&self.buf[HEADER_SIZE..]);
        let crc = hasher.finalize();
        self.buf[12..16].copy_from_slice(&crc.to_le_bytes());

        &self.buf
    }
}

/// Verify a payload's integrity. Returns `Ok(())` if valid, or a description of the failure.
pub fn verify_payload(data: &[u8]) -> Result<(), String> {
    if data.len() < HEADER_SIZE {
        return Err(format!(
            "payload too small: {} bytes (minimum {HEADER_SIZE})",
            data.len()
        ));
    }

    let stored_crc = u32::from_le_bytes(data[12..16].try_into().unwrap());

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&data[..12]);
    hasher.update(&data[HEADER_SIZE..]);
    let computed_crc = hasher.finalize();

    if stored_crc != computed_crc {
        let worker_id = u32::from_le_bytes(data[0..4].try_into().unwrap());
        let seq = u64::from_le_bytes(data[4..12].try_into().unwrap());
        return Err(format!(
            "CRC mismatch: stored={stored_crc:#010x}, computed={computed_crc:#010x} \
             (worker={worker_id}, seq={seq})"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_payload_verifies() {
        let mut pg = PayloadGenerator::new(42, 64);
        for _ in 0..100 {
            let payload = pg.next();
            verify_payload(payload).expect("payload should verify");
        }
    }

    #[test]
    fn corrupted_payload_fails_verification() {
        let mut pg = PayloadGenerator::new(1, 32);
        let payload = pg.next().to_vec();
        let mut corrupted = payload.clone();
        corrupted[HEADER_SIZE] ^= 0xFF;
        assert!(verify_payload(&corrupted).is_err());
    }

    #[test]
    fn minimum_size_payload() {
        let mut pg = PayloadGenerator::new(0, 4);
        let payload = pg.next();
        assert_eq!(payload.len(), HEADER_SIZE);
        verify_payload(payload).expect("minimum-size payload should verify");
    }

    #[test]
    fn sequential_payloads_differ() {
        let mut pg = PayloadGenerator::new(0, 32);
        let p1 = pg.next().to_vec();
        let p2 = pg.next().to_vec();
        assert_ne!(p1, p2);
    }

    #[test]
    fn different_workers_differ() {
        let mut pg1 = PayloadGenerator::new(0, 32);
        let mut pg2 = PayloadGenerator::new(1, 32);
        let p1 = pg1.next().to_vec();
        let p2 = pg2.next().to_vec();
        assert_ne!(p1, p2);
    }
}
