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

use super::KafkaCodecError;

pub struct KafkaRecord {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub timestamp_delta: i64,
}

/// Decode a Kafka RecordBatch (magic=2) into individual records.
/// Layout: baseOffset(8) batchLength(4) partitionLeaderEpoch(4) magic(1) crc(4)
///         attributes(2) lastOffsetDelta(4) baseTimestamp(8) maxTimestamp(8)
///         producerId(8) producerEpoch(2) baseSequence(4) recordCount(4)
///         [records...]
pub fn decode_record_batch(data: &[u8]) -> Result<(i64, Vec<KafkaRecord>), KafkaCodecError> {
    if data.len() < 61 {
        return Err(KafkaCodecError::BufferUnderflow);
    }

    // Header: baseOffset(8) batchLength(4) partitionLeaderEpoch(4) magic(1)
    //         crc(4) attributes(2) lastOffsetDelta(4) = offset 27
    let base_timestamp = i64::from_be_bytes(data[27..35].try_into().unwrap());
    // maxTimestamp(8) producerId(8) producerEpoch(2) baseSequence(4) = offset 57
    let record_count = i32::from_be_bytes(data[57..61].try_into().unwrap());

    let mut pos = 61;
    let mut records = Vec::with_capacity(record_count.max(0).min(data.len() as i32) as usize);

    for _ in 0..record_count {
        let _record_length = read_varint(data, &mut pos)?;
        let _attributes = read_varint(data, &mut pos)?;
        let timestamp_delta = read_varint(data, &mut pos)?;
        let _offset_delta = read_varint(data, &mut pos)?;

        let key = read_varint_bytes(data, &mut pos)?;
        let value = read_varint_bytes(data, &mut pos)?;

        // Skip headers
        let header_count = read_varint(data, &mut pos)?;
        for _ in 0..header_count {
            let _hk = read_varint_bytes(data, &mut pos)?;
            let _hv = read_varint_bytes(data, &mut pos)?;
        }

        records.push(KafkaRecord {
            key,
            value,
            timestamp_delta,
        });
    }

    Ok((base_timestamp, records))
}

fn read_varint(data: &[u8], pos: &mut usize) -> Result<i64, KafkaCodecError> {
    let mut result: u64 = 0;
    let mut shift = 0;
    loop {
        if *pos >= data.len() {
            return Err(KafkaCodecError::BufferUnderflow);
        }
        let byte = data[*pos] as u64;
        *pos += 1;
        result |= (byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err(KafkaCodecError::InvalidString);
        }
    }
    // ZigZag decode
    Ok(((result >> 1) as i64) ^ -((result & 1) as i64))
}

pub fn encode_varint(buf: &mut Vec<u8>, val: i64) {
    let mut v = ((val << 1) ^ (val >> 63)) as u64;
    loop {
        if v & !0x7F == 0 {
            buf.push(v as u8);
            break;
        }
        buf.push((v as u8 & 0x7F) | 0x80);
        v >>= 7;
    }
}

fn read_varint_bytes(data: &[u8], pos: &mut usize) -> Result<Option<Vec<u8>>, KafkaCodecError> {
    let len = read_varint(data, pos)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    if *pos + len > data.len() {
        return Err(KafkaCodecError::BufferUnderflow);
    }
    let bytes = data[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(Some(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    type KV<'a> = (Option<&'a [u8]>, Option<&'a [u8]>);

    fn build_record_batch(records: &[KV<'_>]) -> Vec<u8> {
        let mut batch = Vec::new();
        batch.extend_from_slice(&0i64.to_be_bytes()); // baseOffset
        batch.extend_from_slice(&0i32.to_be_bytes()); // batchLength (placeholder)
        batch.extend_from_slice(&0i32.to_be_bytes()); // partitionLeaderEpoch
        batch.push(2); // magic
        batch.extend_from_slice(&0i32.to_be_bytes()); // crc
        batch.extend_from_slice(&0i16.to_be_bytes()); // attributes
        batch.extend_from_slice(&0i32.to_be_bytes()); // lastOffsetDelta
        batch.extend_from_slice(&1000i64.to_be_bytes()); // baseTimestamp
        batch.extend_from_slice(&1000i64.to_be_bytes()); // maxTimestamp
        batch.extend_from_slice(&(-1i64).to_be_bytes()); // producerId
        batch.extend_from_slice(&(-1i16).to_be_bytes()); // producerEpoch
        batch.extend_from_slice(&(-1i32).to_be_bytes()); // baseSequence
        batch.extend_from_slice(&(records.len() as i32).to_be_bytes()); // recordCount

        for (i, (key, value)) in records.iter().enumerate() {
            let mut record = Vec::new();
            encode_varint(&mut record, 0); // attributes
            encode_varint(&mut record, i as i64); // timestampDelta
            encode_varint(&mut record, i as i64); // offsetDelta
            match key {
                None => encode_varint(&mut record, -1),
                Some(k) => {
                    encode_varint(&mut record, k.len() as i64);
                    record.extend_from_slice(k);
                }
            }
            match value {
                None => encode_varint(&mut record, -1),
                Some(v) => {
                    encode_varint(&mut record, v.len() as i64);
                    record.extend_from_slice(v);
                }
            }
            encode_varint(&mut record, 0); // headerCount

            encode_varint(&mut batch, record.len() as i64); // recordLength
            batch.extend_from_slice(&record);
        }

        batch
    }

    #[test]
    fn test_decode_single_record() {
        let data = build_record_batch(&[(None, Some(b"hello"))]);
        let (base_ts, records) = decode_record_batch(&data).unwrap();
        assert_eq!(base_ts, 1000);
        assert_eq!(records.len(), 1);
        assert!(records[0].key.is_none());
        assert_eq!(records[0].value.as_deref(), Some(b"hello".as_ref()));
    }

    #[test]
    fn test_decode_multiple_records() {
        let data = build_record_batch(&[
            (Some(b"k1"), Some(b"v1")),
            (Some(b"k2"), Some(b"v2")),
            (None, Some(b"v3")),
        ]);
        let (_, records) = decode_record_batch(&data).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].key.as_deref(), Some(b"k1".as_ref()));
        assert_eq!(records[0].value.as_deref(), Some(b"v1".as_ref()));
        assert_eq!(records[2].key, None);
    }

    #[test]
    fn test_varint_round_trip() {
        for val in [0, 1, -1, 127, -128, 300, -300, i64::MAX / 2, i64::MIN / 2] {
            let mut buf = Vec::new();
            encode_varint(&mut buf, val);
            let mut pos = 0;
            let decoded = read_varint(&buf, &mut pos).unwrap();
            assert_eq!(decoded, val, "failed for {val}");
            assert_eq!(pos, buf.len());
        }
    }
}
