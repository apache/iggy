use bytes::{Bytes, BytesMut};
use serde_with::serde_as;
use std::{io::Write, ops::Range};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    binary::messages,
    utils::{byte_size::IggyByteSize, timestamp::IggyTimestamp, varint::IggyVarInt},
};

pub const IGGY_BATCH_OVERHEAD: u64 = 16 + 16 + 16 + 16 + 8 + 8 + 8 + 8 + 4 + 4 + 4 + 4 + 1;

#[derive(Debug, Default, Copy, Clone, PartialEq)]
pub struct IggyHeader {
    // TODO: replace timestamp with `IggyTimestamp`
    // and maybe impl `IggyOffset` and other wrapper structs for things like checksums, attributes etc...
    // TODO Fix me: Reorganize those fields, such that the most frequently accessed fields
    // are at the top in the same cache line.
    pub payload_type: u8,
    pub last_offset_delta: u32,
    pub last_timestamp_delta: u32,
    pub release: u32,
    pub attributes: u32,
    pub base_offset: u64,
    pub batch_length: u64,
    pub origin_timestamp: u64,
    pub base_timestamp: u64,
    pub reserved_nonce: u128,
    pub parent: u128,
    pub checksum_body: u128,
    pub checksum: u128,
}

impl IggyHeader {
    pub fn new(
        payload_type: u8,
        last_offset_delta: u32,
        last_timestamp_delta: u32,
        release: u32,
        attributes: u32,
        base_offset: u64,
        batch_length: u64,
        origin_timestamp: u64,
        base_timestamp: u64,
        reserved_nonce: u128,
        parent: u128,
        checksum_body: u128,
        checksum: u128,
    ) -> Self {
        Self {
            payload_type,
            last_offset_delta,
            last_timestamp_delta,
            release,
            attributes,
            base_offset,
            batch_length,
            origin_timestamp,
            base_timestamp,
            reserved_nonce,
            parent,
            checksum_body,
            checksum,
        }
    }

    pub async fn read_header<R: AsyncReadExt + Unpin>(reader: &mut R) -> IggyHeader {
        let payload_type = reader.read_u8().await.unwrap();
        let last_offset_delta = reader.read_u32_le().await.unwrap();
        let last_timestamp_delta = reader.read_u32_le().await.unwrap();
        let release = reader.read_u32_le().await.unwrap();
        let attributes = reader.read_u32_le().await.unwrap();
        let base_offset = reader.read_u64_le().await.unwrap();
        let batch_length = reader.read_u64_le().await.unwrap();
        let origin_timestamp = reader.read_u64_le().await.unwrap();
        let base_timestamp = reader.read_u64_le().await.unwrap();
        let reserved_nonce = reader.read_u128_le().await.unwrap();
        let parent = reader.read_u128_le().await.unwrap();
        let checksum_body = reader.read_u128_le().await.unwrap();
        let checksum = reader.read_u128_le().await.unwrap();

        IggyHeader::new(
            payload_type,
            last_offset_delta,
            last_timestamp_delta,
            release,
            attributes,
            base_offset,
            batch_length,
            origin_timestamp,
            base_timestamp,
            reserved_nonce,
            parent,
            checksum_body,
            checksum,
        )
    }

    fn write_header<const N: usize>(&self, writer: &mut HeaderWriter<N>) {
        writer.write_all(&self.payload_type.to_le_bytes()).unwrap();
        writer
            .write_all(&self.last_offset_delta.to_le_bytes())
            .unwrap();
        writer
            .write_all(&self.last_timestamp_delta.to_le_bytes())
            .unwrap();
        writer.write_all(&self.release.to_le_bytes()).unwrap();
        writer.write_all(&self.attributes.to_le_bytes()).unwrap();
        writer.write_all(&self.base_offset.to_le_bytes()).unwrap();
        writer.write_all(&self.batch_length.to_le_bytes()).unwrap();
        writer
            .write_all(&self.origin_timestamp.to_le_bytes())
            .unwrap();
        writer
            .write_all(&self.base_timestamp.to_le_bytes())
            .unwrap();
        writer
            .write_all(&self.reserved_nonce.to_le_bytes())
            .unwrap();
        writer.write_all(&self.parent.to_le_bytes()).unwrap();
        writer.write_all(&self.checksum_body.to_le_bytes()).unwrap();
        writer.write_all(&self.checksum.to_le_bytes()).unwrap();
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut offset = 0;

        let payload_type = bytes[offset];
        offset += 1;

        let last_offset_delta = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let last_timestamp_delta =
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let release = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let attributes = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let base_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let batch_length = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let origin_timestamp = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let base_timestamp = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let reserved_nonce = u128::from_le_bytes(bytes[offset..offset + 16].try_into().unwrap());
        offset += 16;

        let parent = u128::from_le_bytes(bytes[offset..offset + 16].try_into().unwrap());
        offset += 16;

        let checksum_body = u128::from_le_bytes(bytes[offset..offset + 16].try_into().unwrap());
        offset += 16;

        let checksum = u128::from_le_bytes(bytes[offset..offset + 16].try_into().unwrap());

        Self {
            payload_type,
            last_offset_delta,
            last_timestamp_delta,
            release,
            attributes,
            base_offset,
            batch_length,
            origin_timestamp,
            base_timestamp,
            reserved_nonce,
            parent,
            checksum_body,
            checksum,
        }
    }

    pub fn as_bytes(&self) -> [u8; IGGY_BATCH_OVERHEAD as usize] {
        let header = [0u8; IGGY_BATCH_OVERHEAD as usize];
        let mut writer = HeaderWriter::new(header);
        self.write_header(&mut writer);
        writer.header()
    }
}

//TODO create a trait for this that will expose two methods async and not async.
pub struct HeaderWriter<const N: usize> {
    buffer: [u8; N],
    position: usize,
}

impl<const N: usize> HeaderWriter<N> {
    pub fn new(buffer: [u8; N]) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }

    pub fn header(self) -> [u8; N] {
        self.buffer
    }
}

impl<const N: usize> From<[u8; N]> for HeaderWriter<N> {
    fn from(buffer: [u8; N]) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }
}

impl<const N: usize> std::io::Write for HeaderWriter<N> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let available = N.saturating_sub(self.position);
        let len = buf.len().min(available);
        self.buffer[self.position..self.position + len].copy_from_slice(&buf[..len]);
        self.position += len;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// TODO: THIS IS SHIIIIIIIIIT, fuck Bytes
#[serde_as]
#[derive(Debug, Default, PartialEq)]
pub struct IggyBatch {
    pub header: IggyHeader,
    //TODO: Once the dependency on `Bytes` is dropped, change it to Vec<u8>.
    pub batch: Bytes,
}

#[serde_as]
#[derive(Debug, Default, PartialEq)]
pub struct IggyMutableBatch {
    pub header: IggyHeader,
    //TODO: Once the dependency on `Bytes` is dropped, change it to Vec<u8>.
    pub batch: BytesMut,
}

impl IggyMutableBatch {
    pub fn new(header: IggyHeader, batch: BytesMut) -> Self {
        Self { header, batch }
    }

    pub fn update_header_and_messages_offsets_and_timestamps(
        &mut self,
        current_offset: u64,
        header: &mut IggyHeader,
    ) -> u32 {
        fn write_value_at<const N: usize>(slice: &mut [u8], value: [u8; N], position: usize) {
            let slice = &mut slice[position..position + N];
            let ptr = slice.as_mut_ptr();
            // Use copy_nonoverlapping to avoid bounds checking.
            unsafe {
                std::ptr::copy_nonoverlapping(value.as_ptr(), ptr, N);
            }
        }

        let base_timestamp = header.base_timestamp;
        let base_offset = header.base_offset;
        let relative_offset = (current_offset - base_offset) as u32;

        self.header.base_offset = header.base_offset;
        self.header.base_timestamp = header.base_timestamp;
        // TODO: Impl iterators (mutable and non-mutable) instead of this.
        let mut position = 0;
        let mut messages_count = 0u32;
        let mut last_timestamp_delta = 0u32;
        let mut last_offset_delta = 0u32;
        while position < self.batch.len() {
            last_offset_delta = relative_offset + messages_count;
            let offset_delta = last_offset_delta.to_le_bytes();
            write_value_at(&mut self.batch, offset_delta, position);
            position += 4;
            last_timestamp_delta = (IggyTimestamp::now().as_micros() - base_timestamp) as u32;
            let timestamp_delta_value = last_timestamp_delta.to_le_bytes();
            write_value_at(&mut self.batch, timestamp_delta_value, position);
            position += 4;
            // Skip the Id
            position += 16;
            let (read, payload_len) = IggyVarInt::decode(&self.batch[position..]);
            position += read;
            let (read, headers_len) = IggyVarInt::decode(&self.batch[position..]);
            position += read;
            let total_len = payload_len + headers_len;
            position += total_len as usize;
            messages_count += 1;
        }

        let batch_size = self.get_size().as_bytes_u64();
        self.header.last_offset_delta = last_offset_delta;
        self.header.last_timestamp_delta = last_timestamp_delta;
        self.header.batch_length = batch_size;

        header.last_offset_delta = last_offset_delta;
        header.last_timestamp_delta = last_timestamp_delta;
        header.batch_length += batch_size;

        messages_count
    }

    pub fn write_header<const N: usize>(&self, writer: &mut HeaderWriter<N>) {
        self.header.write_header(writer);
    }

    pub async fn write_messages<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) {
        writer.write_all(&self.batch).await.unwrap();
    }

    pub fn get_size(&self) -> IggyByteSize {
        let batch_size = IggyByteSize::from(self.batch.len() as u64);
        batch_size
    }

    pub fn get_size_w_header(&self) -> IggyByteSize {
        let header_size = IggyByteSize::from(IGGY_BATCH_OVERHEAD);
        let batch_size = IggyByteSize::from(self.batch.len() as u64);
        header_size + batch_size
    }
}

impl IggyBatch {
    pub fn new(header: IggyHeader, batch: Bytes) -> Self {
        Self { header, batch }
    }

    pub fn write_header<const N: usize>(&self, writer: &mut HeaderWriter<N>) {
        self.header.write_header(writer);
    }

    pub async fn write_messages<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) {
        writer.write_all(&self.batch).await.unwrap();
    }

    pub fn get_size(&self) -> IggyByteSize {
        let batch_size = IggyByteSize::from(self.batch.len() as u64);
        batch_size
    }

    pub fn get_size_w_header(&self) -> IggyByteSize {
        let header_size = IggyByteSize::from(IGGY_BATCH_OVERHEAD);
        let batch_size = IggyByteSize::from(self.batch.len() as u64);
        header_size + batch_size
    }
}

#[derive(Debug, PartialEq)]
pub struct IggyMessageView<'msg> {
    pub id: u128,
    pub offset: u64,
    pub timestamp: IggyTimestamp,
    pub payload: &'msg [u8],
    pub headers: &'msg [u8],
}

impl<'msg> IggyMessageView<'msg> {
    pub fn new(
        id: u128,
        offset: u64,
        timestamp: IggyTimestamp,
        payload: &'msg [u8],
        headers: &'msg [u8],
    ) -> Self {
        Self {
            id,
            offset,
            timestamp,
            payload,
            headers,
        }
    }
}

impl<'batch> IntoIterator for &'batch IggyBatch {
    type Item = (Range<usize>, IggyMessageView<'batch>);
    type IntoIter = IggyMessageIterator<'batch>;

    fn into_iter(self) -> Self::IntoIter {
        IggyMessageIterator {
            batch: self,
            position: 0,
        }
    }
}

impl<'batch> IggyBatch {
    pub fn iter(&'batch self) -> IggyMessageIterator<'batch> {
        IggyMessageIterator {
            batch: self,
            position: 0,
        }
    }
}

pub struct IggyMessageIterator<'batch> {
    batch: &'batch IggyBatch,
    position: usize,
}

impl<'msg> Iterator for IggyMessageIterator<'msg> {
    type Item = (Range<usize>, IggyMessageView<'msg>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.batch.batch.len() {
            return None;
        }
        let data = self.batch.batch.as_ref();
        let start_pos = self.position;
        let mut pos = self.position;

        //TODO: replace all of those oks with an Faillable iterator I guess ?
        let offset_delta = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let timestamp_delta = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let id = u128::from_le_bytes(data[pos..pos + 16].try_into().ok()?);
        pos += 16;
        let (read, payload_length) = IggyVarInt::decode(&data[pos..]);
        pos += read;
        let (read, headers_length) = IggyVarInt::decode(&data[pos..]);
        pos += read;
        let payload = &data[pos..pos + payload_length as usize];
        pos += payload_length as usize;
        let headers = &data[pos..pos + headers_length as usize];
        pos += headers_length as usize;
        self.position = pos;

        let offset = self.batch.header.base_offset + u64::from(offset_delta);
        let timestamp = self.batch.header.base_timestamp + timestamp_delta as u64;
        let msg = IggyMessageView::new(id, offset, timestamp.into(), payload, headers);
        let range = start_pos..pos;
        Some((range, msg))
    }
}
