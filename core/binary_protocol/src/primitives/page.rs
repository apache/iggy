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

//! The page block of the list commands and the cursor inside it.
//!
//! A list request ends with the page block:
//!
//! ```text
//! [order:u8]                0 = id, 1 = created
//! [descending:u8]           0 = ascending, 1 = descending
//! [limit:u32]               1 to MAX_PAGE_LIMIT
//! [cursor_len:u8][cursor]   0 = start of the order
//! [name_len:u8][name]       0 = no filter
//! ```
//!
//! The decode checks the structure. The server checks the limit range and
//! whether the cursor kind fits the order.

use crate::codec::{WireDecode, WireEncode, read_u8, read_u32_le, read_u64_le};
use crate::primitives::metadata_changes::{put_flag, read_flag, read_wire_name};
use crate::{WireError, WireName};
use bytes::{BufMut, BytesMut};
use std::borrow::Cow;

/// The page size of a request that names no limit.
pub const DEFAULT_PAGE_LIMIT: u32 = 100;
/// The largest page a request can ask for.
pub const MAX_PAGE_LIMIT: u32 = 1000;

const ID_CURSOR_KIND: u8 = 1;
const CREATED_CURSOR_KIND: u8 = 2;
const ID_CURSOR_SIZE: usize = 1 + 8;
const CREATED_CURSOR_SIZE: usize = 1 + 8 + 8;

/// The order of a list page.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WirePageOrder {
    /// The numeric id.
    #[default]
    Id,
    /// The creation order, keyed by `(created_at, id)`.
    Created,
}

impl WirePageOrder {
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::Id => 0,
            Self::Created => 1,
        }
    }

    /// # Errors
    /// `WireError::UnknownDiscriminant` for a byte other than 0 and 1.
    pub const fn from_u8(value: u8, offset: usize) -> Result<Self, WireError> {
        match value {
            0 => Ok(Self::Id),
            1 => Ok(Self::Created),
            value => Err(WireError::UnknownDiscriminant {
                type_name: "WirePageOrder",
                value,
                offset,
            }),
        }
    }
}

/// A position in a list order. The `next` of a page names the last entry
/// that the page walked, and the next page starts after it. Clients return it
/// as it came. It holds no direction, filter or parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WirePageCursor {
    /// A position in id order.
    Id(u64),
    /// A position in creation order.
    Created { created_at: u64, id: u64 },
}

impl WirePageCursor {
    /// Decodes a cursor that fills `buf` exactly: `[kind:u8 = 1][id:u64]` or
    /// `[kind:u8 = 2][created_at:u64][id:u64]`, little-endian.
    ///
    /// # Errors
    /// `WireError::UnknownDiscriminant` for an unknown kind, and
    /// `WireError::Validation` for a length that does not fit the kind.
    pub fn decode_exact(buf: &[u8]) -> Result<Self, WireError> {
        let kind = read_u8(buf, 0)?;
        match (kind, buf.len()) {
            (ID_CURSOR_KIND, ID_CURSOR_SIZE) => Ok(Self::Id(read_u64_le(buf, 1)?)),
            (CREATED_CURSOR_KIND, CREATED_CURSOR_SIZE) => Ok(Self::Created {
                created_at: read_u64_le(buf, 1)?,
                id: read_u64_le(buf, 9)?,
            }),
            (ID_CURSOR_KIND | CREATED_CURSOR_KIND, length) => {
                Err(WireError::Validation(Cow::Owned(format!(
                    "a page cursor of kind {kind} cannot have {length} bytes"
                ))))
            }
            (value, _) => Err(WireError::UnknownDiscriminant {
                type_name: "WirePageCursor",
                value,
                offset: 0,
            }),
        }
    }
}

impl WireEncode for WirePageCursor {
    fn encoded_size(&self) -> usize {
        match self {
            Self::Id(_) => ID_CURSOR_SIZE,
            Self::Created { .. } => CREATED_CURSOR_SIZE,
        }
    }

    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            Self::Id(id) => {
                buf.put_u8(ID_CURSOR_KIND);
                buf.put_u64_le(id);
            }
            Self::Created { created_at, id } => {
                buf.put_u8(CREATED_CURSOR_KIND);
                buf.put_u64_le(created_at);
                buf.put_u64_le(id);
            }
        }
    }
}

/// `[cursor_len:u8][cursor]`, with a length of 0 for no cursor.
pub(crate) fn optional_cursor_size(cursor: Option<&WirePageCursor>) -> usize {
    1 + cursor.map_or(0, WireEncode::encoded_size)
}

#[allow(clippy::cast_possible_truncation)]
pub(crate) fn put_optional_cursor(buf: &mut BytesMut, cursor: Option<&WirePageCursor>) {
    match cursor {
        None => buf.put_u8(0),
        Some(cursor) => {
            buf.put_u8(cursor.encoded_size() as u8);
            cursor.encode(buf);
        }
    }
}

pub(crate) fn read_optional_cursor(
    buf: &[u8],
    offset: usize,
) -> Result<(Option<WirePageCursor>, usize), WireError> {
    let length = usize::from(read_u8(buf, offset)?);
    if length == 0 {
        return Ok((None, 1));
    }
    let start = offset + 1;
    let bytes = buf
        .get(start..start + length)
        .ok_or_else(|| WireError::UnexpectedEof {
            offset: start,
            need: length,
            have: buf.len().saturating_sub(start),
        })?;
    Ok((Some(WirePageCursor::decode_exact(bytes)?), 1 + length))
}

/// The page block that ends every list request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WirePageQuery {
    pub order: WirePageOrder,
    pub descending: bool,
    /// The most items of the page, 1 to [`MAX_PAGE_LIMIT`].
    pub limit: u32,
    /// The `next` of the previous page, `None` for the first page.
    pub cursor: Option<WirePageCursor>,
    /// Only the entries whose name holds this text, case-sensitive.
    pub name: Option<WireName>,
}

impl Default for WirePageQuery {
    fn default() -> Self {
        Self {
            order: WirePageOrder::Id,
            descending: false,
            limit: DEFAULT_PAGE_LIMIT,
            cursor: None,
            name: None,
        }
    }
}

impl WireEncode for WirePageQuery {
    fn encoded_size(&self) -> usize {
        1 + 1
            + 4
            + optional_cursor_size(self.cursor.as_ref())
            + self.name.as_ref().map_or(1, WireEncode::encoded_size)
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.order.as_u8());
        put_flag(buf, self.descending);
        buf.put_u32_le(self.limit);
        put_optional_cursor(buf, self.cursor.as_ref());
        match &self.name {
            None => buf.put_u8(0),
            Some(name) => name.encode(buf),
        }
    }
}

impl WireDecode for WirePageQuery {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let order = WirePageOrder::from_u8(read_u8(buf, 0)?, 0)?;
        let descending = read_flag(buf, 1)?;
        let limit = read_u32_le(buf, 2)?;
        let (cursor, cursor_size) = read_optional_cursor(buf, 6)?;
        let mut position = 6 + cursor_size;
        let name = if read_u8(buf, position)? == 0 {
            position += 1;
            None
        } else {
            let (name, consumed) = read_wire_name(buf, position)?;
            position += consumed;
            Some(name)
        };
        Ok((
            Self {
                order,
                descending,
                limit,
                cursor,
                name,
            },
            position,
        ))
    }
}

/// Decodes the page block at `offset`, the last field of a list request.
///
/// # Errors
/// The decode error of the block, or `WireError::Validation` for bytes after it.
pub(crate) fn decode_trailing_page(
    buf: &[u8],
    offset: usize,
) -> Result<(WirePageQuery, usize), WireError> {
    let rest = buf.get(offset..).ok_or(WireError::UnexpectedEof {
        offset,
        need: 1,
        have: 0,
    })?;
    let (page, consumed) = WirePageQuery::decode(rest)?;
    let end = offset + consumed;
    if end != buf.len() {
        return Err(WireError::Validation(Cow::Owned(format!(
            "{} bytes after the page block",
            buf.len() - end
        ))));
    }
    Ok((page, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filtered() -> WirePageQuery {
        WirePageQuery {
            order: WirePageOrder::Created,
            descending: true,
            limit: 20,
            cursor: Some(WirePageCursor::Created {
                created_at: 0x0102_0304_0506_0708,
                id: 7,
            }),
            name: Some(WireName::new("ord").unwrap()),
        }
    }

    #[test]
    fn given_a_first_page_without_a_name_when_encoding_should_pin_the_bytes() {
        let query = WirePageQuery {
            limit: 100,
            ..WirePageQuery::default()
        };
        let bytes = query.to_bytes();
        assert_eq!(&bytes[..], &[0, 0, 100, 0, 0, 0, 0, 0]);
        assert_eq!(bytes.len(), query.encoded_size());
        assert_eq!(WirePageQuery::decode(&bytes).unwrap(), (query, 8));
    }

    #[test]
    fn given_a_cursor_and_a_name_when_encoding_should_pin_the_bytes() {
        let query = filtered();
        let bytes = query.to_bytes();
        let mut expected = vec![1, 1, 20, 0, 0, 0, 17, 2, 8, 7, 6, 5, 4, 3, 2, 1];
        expected.extend([7, 0, 0, 0, 0, 0, 0, 0, 3, b'o', b'r', b'd']);
        assert_eq!(&bytes[..], &expected[..]);
        assert_eq!(bytes.len(), query.encoded_size());
        assert_eq!(
            WirePageQuery::decode(&bytes).unwrap(),
            (query, expected.len())
        );
    }

    #[test]
    fn given_an_id_cursor_when_round_tripping_should_keep_it() {
        let query = WirePageQuery {
            cursor: Some(WirePageCursor::Id(u64::MAX)),
            ..WirePageQuery::default()
        };
        let bytes = query.to_bytes();
        assert_eq!(bytes[6], 9);
        assert_eq!(bytes[7], 1);
        assert_eq!(WirePageQuery::decode(&bytes).unwrap().0, query);
    }

    #[test]
    fn given_a_bad_order_or_flag_byte_when_decoding_should_refuse() {
        let bytes = filtered().to_bytes().to_vec();
        for (offset, value) in [(0, 2), (1, 2)] {
            let mut broken = bytes.clone();
            broken[offset] = value;
            assert!(
                WirePageQuery::decode(&broken).is_err(),
                "byte {offset} = {value}"
            );
        }
    }

    #[test]
    fn given_an_unknown_kind_or_a_wrong_length_when_decoding_a_cursor_should_refuse() {
        assert!(WirePageCursor::decode_exact(&[0; 9]).is_err());
        let mut kind_three = [0; 9];
        kind_three[0] = 3;
        assert!(WirePageCursor::decode_exact(&kind_three).is_err());
        let mut id_too_long = [0; 17];
        id_too_long[0] = 1;
        assert!(WirePageCursor::decode_exact(&id_too_long).is_err());
        let mut created_too_short = [0; 9];
        created_too_short[0] = 2;
        assert!(WirePageCursor::decode_exact(&created_too_short).is_err());
        assert!(WirePageCursor::decode_exact(&[]).is_err());
    }

    #[test]
    fn given_a_truncated_block_when_decoding_should_refuse() {
        let bytes = filtered().to_bytes();
        for length in 0..bytes.len() {
            assert!(
                WirePageQuery::decode(&bytes[..length]).is_err(),
                "a block cut at {length} bytes must not decode"
            );
        }
    }

    #[test]
    fn given_bytes_after_the_block_when_decoding_a_trailing_page_should_refuse() {
        let mut bytes = vec![9];
        bytes.extend_from_slice(&filtered().to_bytes());
        let (page, end) = decode_trailing_page(&bytes, 1).unwrap();
        assert_eq!((page, end), (filtered(), bytes.len()));
        bytes.push(0);
        assert!(decode_trailing_page(&bytes, 1).is_err());
        assert!(
            decode_trailing_page(&[9], 1).is_err(),
            "a request that ends before the block"
        );
    }
}
