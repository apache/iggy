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

use bytes::Bytes;

use iggy_gateway_kafka::protocol::codec::{Decoder, Encoder};

#[test]
fn codec_round_trip_primitives_and_nullable_fields() {
    let mut enc = Encoder::with_capacity(128);
    enc.write_i8(-3);
    enc.write_i16(42);
    enc.write_i32(123_456);
    enc.write_i64(9_999_999);
    enc.write_nullable_string(Some("client-a")).unwrap();
    enc.write_nullable_string(None).unwrap();
    enc.write_nullable_bytes(Some(&[1, 2, 3])).unwrap();
    enc.write_nullable_bytes(None).unwrap();
    let bytes = enc.freeze();

    let mut dec = Decoder::new(bytes);
    assert_eq!(dec.read_i8().unwrap(), -3);
    assert_eq!(dec.read_i16().unwrap(), 42);
    assert_eq!(dec.read_i32().unwrap(), 123_456);
    assert_eq!(dec.read_i64().unwrap(), 9_999_999);
    assert_eq!(
        dec.read_nullable_string().unwrap().as_deref(),
        Some("client-a")
    );
    assert_eq!(dec.read_nullable_string().unwrap(), None);
    assert_eq!(
        dec.read_nullable_bytes().unwrap().unwrap(),
        Bytes::from_static(&[1, 2, 3])
    );
    assert_eq!(dec.read_nullable_bytes().unwrap(), None);
}

#[test]
fn decoder_returns_underflow_error() {
    let mut dec = Decoder::new(Bytes::from_static(&[0x00]));
    let err = dec.read_i32().expect_err("must fail");
    assert!(err.to_string().contains("buffer underflow"));
}

#[test]
fn codec_u8_and_bool() {
    let mut enc = Encoder::with_capacity(8);
    enc.write_u8(0xFF);
    enc.write_bool(true);
    enc.write_bool(false);
    let bytes = enc.freeze();

    let mut dec = Decoder::new(bytes);
    assert_eq!(dec.read_u8().unwrap(), 0xFF);
    assert!(dec.read_bool().unwrap());
    assert!(!dec.read_bool().unwrap());
}

#[test]
fn varint_round_trip_small_values() {
    for v in [
        0u64,
        1,
        127,
        128,
        255,
        300,
        16383,
        16384,
        u64::from(u32::MAX),
    ] {
        let mut enc = Encoder::with_capacity(16);
        enc.write_varint(v);
        let mut dec = Decoder::new(enc.freeze());
        assert_eq!(dec.read_varint().unwrap(), v, "failed for v={v}");
    }
}

#[test]
fn varint_single_byte_for_values_below_128() {
    let mut enc = Encoder::with_capacity(1);
    enc.write_varint(42);
    let bytes = enc.freeze();
    assert_eq!(bytes.len(), 1);
    assert_eq!(bytes[0], 42);
}

#[test]
fn varint_two_bytes_for_128() {
    let mut enc = Encoder::with_capacity(2);
    enc.write_varint(128);
    let bytes = enc.freeze();
    // 128 = 0x80 → first byte 0x80 | 0x80 = 0x80 (continue), second byte 0x01
    assert_eq!(bytes.as_ref(), &[0x80, 0x01]);
}

#[test]
fn compact_nullable_string_round_trip() {
    let mut enc = Encoder::with_capacity(32);
    enc.write_compact_nullable_string(Some("hello"));
    enc.write_compact_nullable_string(None);
    enc.write_compact_nullable_string(Some(""));
    let bytes = enc.freeze();

    let mut dec = Decoder::new(bytes);
    assert_eq!(
        dec.read_compact_nullable_string().unwrap().as_deref(),
        Some("hello")
    );
    assert_eq!(dec.read_compact_nullable_string().unwrap(), None);
    assert_eq!(
        dec.read_compact_nullable_string().unwrap().as_deref(),
        Some("")
    );
}

#[test]
fn compact_nullable_bytes_round_trip() {
    let mut enc = Encoder::with_capacity(32);
    enc.write_compact_nullable_bytes(Some(&[10, 20, 30]));
    enc.write_compact_nullable_bytes(None);
    let bytes = enc.freeze();

    let mut dec = Decoder::new(bytes);
    assert_eq!(
        dec.read_compact_nullable_bytes().unwrap().unwrap(),
        Bytes::from_static(&[10, 20, 30])
    );
    assert_eq!(dec.read_compact_nullable_bytes().unwrap(), None);
}

#[test]
fn tagged_fields_empty_section_round_trip() {
    let mut enc = Encoder::with_capacity(8);
    enc.write_i32(42);
    enc.write_empty_tagged_fields();
    enc.write_i16(7);
    let bytes = enc.freeze();

    let mut dec = Decoder::new(bytes);
    assert_eq!(dec.read_i32().unwrap(), 42);
    dec.read_tagged_fields().unwrap(); // should consume the single 0x00 byte
    assert_eq!(dec.read_i16().unwrap(), 7);
    assert_eq!(dec.remaining(), 0);
}
