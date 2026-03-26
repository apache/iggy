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

use super::{API_VERSIONS, KafkaCodecError, RequestHeader};

// --- Readers (big-endian, from &[u8] with cursor) ---

pub fn read_i8(buf: &[u8], pos: &mut usize) -> Result<i8, KafkaCodecError> {
    if *pos + 1 > buf.len() {
        return Err(KafkaCodecError::BufferUnderflow);
    }
    let v = buf[*pos] as i8;
    *pos += 1;
    Ok(v)
}

pub fn read_i16(buf: &[u8], pos: &mut usize) -> Result<i16, KafkaCodecError> {
    if *pos + 2 > buf.len() {
        return Err(KafkaCodecError::BufferUnderflow);
    }
    let v = i16::from_be_bytes([buf[*pos], buf[*pos + 1]]);
    *pos += 2;
    Ok(v)
}

pub fn read_i32(buf: &[u8], pos: &mut usize) -> Result<i32, KafkaCodecError> {
    if *pos + 4 > buf.len() {
        return Err(KafkaCodecError::BufferUnderflow);
    }
    let v = i32::from_be_bytes(buf[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(v)
}

pub fn read_i64(buf: &[u8], pos: &mut usize) -> Result<i64, KafkaCodecError> {
    if *pos + 8 > buf.len() {
        return Err(KafkaCodecError::BufferUnderflow);
    }
    let v = i64::from_be_bytes(buf[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

pub fn read_nullable_string(
    buf: &[u8],
    pos: &mut usize,
) -> Result<Option<String>, KafkaCodecError> {
    let len = read_i16(buf, pos)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    if *pos + len > buf.len() {
        return Err(KafkaCodecError::BufferUnderflow);
    }
    let s =
        std::str::from_utf8(&buf[*pos..*pos + len]).map_err(|_| KafkaCodecError::InvalidString)?;
    *pos += len;
    Ok(Some(s.to_owned()))
}

pub fn read_string(buf: &[u8], pos: &mut usize) -> Result<String, KafkaCodecError> {
    read_nullable_string(buf, pos)?.ok_or(KafkaCodecError::InvalidString)
}

pub fn read_nullable_bytes(
    buf: &[u8],
    pos: &mut usize,
) -> Result<Option<Vec<u8>>, KafkaCodecError> {
    let len = read_i32(buf, pos)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    if *pos + len > buf.len() {
        return Err(KafkaCodecError::BufferUnderflow);
    }
    let data = buf[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(Some(data))
}

pub fn read_array_len(buf: &[u8], pos: &mut usize) -> Result<i32, KafkaCodecError> {
    read_i32(buf, pos)
}

/// Cap Vec pre-allocation to remaining buffer bytes to prevent OOM from untrusted input.
pub fn safe_capacity(count: i32, remaining: usize) -> usize {
    (count.max(0) as usize).min(remaining)
}

// --- Writers (big-endian, to Vec<u8>) ---

pub fn write_i8(buf: &mut Vec<u8>, v: i8) {
    buf.push(v as u8);
}

pub fn write_i16(buf: &mut Vec<u8>, v: i16) {
    buf.extend_from_slice(&v.to_be_bytes());
}

pub fn write_i32(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

pub fn write_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

pub fn write_nullable_string(buf: &mut Vec<u8>, s: Option<&str>) {
    match s {
        None => write_i16(buf, -1),
        Some(s) => {
            write_i16(buf, s.len() as i16);
            buf.extend_from_slice(s.as_bytes());
        }
    }
}

pub fn write_string(buf: &mut Vec<u8>, s: &str) {
    write_nullable_string(buf, Some(s));
}

pub fn write_nullable_bytes(buf: &mut Vec<u8>, data: Option<&[u8]>) {
    match data {
        None => write_i32(buf, -1),
        Some(d) => {
            write_i32(buf, d.len() as i32);
            buf.extend_from_slice(d);
        }
    }
}

// --- Header decode / Response framing ---

/// Decode request header. Handles header v0 (ApiVersions v0-v2), v1, and v2 (ApiVersions v3+).
pub fn decode_request_header(buf: &[u8]) -> Result<(RequestHeader, usize), KafkaCodecError> {
    let mut pos = 0;
    let api_key = read_i16(buf, &mut pos)?;
    let api_version = read_i16(buf, &mut pos)?;
    let correlation_id = read_i32(buf, &mut pos)?;

    let client_id = if api_key == API_VERSIONS && api_version <= 2 {
        // Header v0: no client_id
        None
    } else {
        // Header v1/v2: nullable string client_id (i16 length)
        let cid = read_nullable_string(buf, &mut pos)?;
        if api_key == API_VERSIONS && api_version >= 3 {
            // Header v2 also has trailing tagged fields
            skip_tagged_fields(buf, &mut pos)?;
        }
        cid
    };

    Ok((
        RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
        },
        pos,
    ))
}

/// Skip tagged fields section (unsigned varint count, then skip each field).
fn skip_tagged_fields(buf: &[u8], pos: &mut usize) -> Result<(), KafkaCodecError> {
    let count = read_unsigned_varint(buf, pos)?;
    for _ in 0..count {
        let _tag = read_unsigned_varint(buf, pos)?;
        let size = read_unsigned_varint(buf, pos)? as usize;
        if *pos + size > buf.len() {
            return Err(KafkaCodecError::BufferUnderflow);
        }
        *pos += size;
    }
    Ok(())
}

fn read_unsigned_varint(buf: &[u8], pos: &mut usize) -> Result<u64, KafkaCodecError> {
    let mut result: u64 = 0;
    let mut shift = 0;
    loop {
        if *pos >= buf.len() {
            return Err(KafkaCodecError::BufferUnderflow);
        }
        let byte = buf[*pos] as u64;
        *pos += 1;
        result |= (byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 35 {
            return Err(KafkaCodecError::BufferUnderflow);
        }
    }
    Ok(result)
}

/// Wrap a response body with the length prefix and correlation_id header (v0/v1).
pub fn encode_response_frame(correlation_id: i32, body: &[u8]) -> Vec<u8> {
    let size = 4 + body.len(); // correlation_id + body
    let mut frame = Vec::with_capacity(4 + size);
    write_i32(&mut frame, size as i32);
    write_i32(&mut frame, correlation_id);
    frame.extend_from_slice(body);
    frame
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_round_trip() {
        let mut buf = Vec::new();
        write_i8(&mut buf, -42);
        write_i16(&mut buf, 12345);
        write_i32(&mut buf, -999_999);
        write_i64(&mut buf, 0x0102030405060708);
        write_string(&mut buf, "hello");
        write_nullable_string(&mut buf, None);

        let mut pos = 0;
        assert_eq!(read_i8(&buf, &mut pos).unwrap(), -42);
        assert_eq!(read_i16(&buf, &mut pos).unwrap(), 12345);
        assert_eq!(read_i32(&buf, &mut pos).unwrap(), -999_999);
        assert_eq!(read_i64(&buf, &mut pos).unwrap(), 0x0102030405060708);
        assert_eq!(read_string(&buf, &mut pos).unwrap(), "hello");
        assert_eq!(read_nullable_string(&buf, &mut pos).unwrap(), None);
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn test_decode_api_versions_header_v0() {
        // ApiVersions v0: no client_id
        let mut buf = Vec::new();
        write_i16(&mut buf, API_VERSIONS); // api_key
        write_i16(&mut buf, 0); // api_version
        write_i32(&mut buf, 42); // correlation_id

        let (header, consumed) = decode_request_header(&buf).unwrap();
        assert_eq!(header.api_key, API_VERSIONS);
        assert_eq!(header.api_version, 0);
        assert_eq!(header.correlation_id, 42);
        assert!(header.client_id.is_none());
        assert_eq!(consumed, 8);
    }

    #[test]
    fn test_decode_metadata_header_v1() {
        // Metadata v1: has client_id
        let mut buf = Vec::new();
        write_i16(&mut buf, 3); // api_key = METADATA
        write_i16(&mut buf, 1); // api_version
        write_i32(&mut buf, 99); // correlation_id
        write_nullable_string(&mut buf, Some("test-client"));

        let (header, consumed) = decode_request_header(&buf).unwrap();
        assert_eq!(header.api_key, 3);
        assert_eq!(header.api_version, 1);
        assert_eq!(header.correlation_id, 99);
        assert_eq!(header.client_id.as_deref(), Some("test-client"));
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn test_encode_response_frame() {
        let body = [0x00, 0x01, 0x02];
        let frame = encode_response_frame(42, &body);
        // frame = [size:4][correlation_id:4][body:3]
        let mut pos = 0;
        let size = read_i32(&frame, &mut pos).unwrap();
        assert_eq!(size, 7); // 4 (corr_id) + 3 (body)
        let corr_id = read_i32(&frame, &mut pos).unwrap();
        assert_eq!(corr_id, 42);
        assert_eq!(&frame[pos..], &[0x00, 0x01, 0x02]);
    }

    #[test]
    fn test_buffer_underflow() {
        let buf = [0x00];
        let mut pos = 0;
        assert!(read_i16(&buf, &mut pos).is_err());
    }
}
