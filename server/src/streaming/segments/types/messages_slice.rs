use bytes::{Bytes, BytesMut};
use iggy::prelude::*;

#[derive(Debug, Clone)]
pub struct IggyMessagesSlice {
    buffers: Vec<Bytes>,
    count: u32,
}

impl IggyMessagesSlice {
    pub fn new(buffer: Bytes, start: usize, end: usize, count: u32) -> Self {
        Self {
            buffers: vec![buffer.slice(start..end)],
            count,
        }
    }

    pub fn from_buffers(buffers: Vec<Bytes>, count: u32) -> Self {
        Self { buffers, count }
    }

    pub fn combine(slices: Vec<IggyMessagesSlice>) -> Self {
        let mut buffers = Vec::new();
        let mut total_count = 0;
        for slice in slices {
            buffers.extend(slice.buffers);
            total_count += slice.count;
        }
        Self {
            buffers,
            count: total_count,
        }
    }

    pub fn chunks(&self) -> impl Iterator<Item = &[u8]> + '_ {
        self.buffers.iter().map(|b| b.as_ref())
    }

    pub fn chunks_count(&self) -> usize {
        self.buffers.len()
    }

    pub fn to_messages(&self) -> IggyMessages {
        let total_size: usize = self.buffers.iter().map(|b| b.len()).sum();
        let mut combined = BytesMut::with_capacity(total_size);
        for chunk in &self.buffers {
            combined.extend_from_slice(chunk);
        }
        IggyMessages::new(combined.freeze(), self.count)
    }

    pub fn count(&self) -> u32 {
        self.count
    }

    pub fn size(&self) -> u32 {
        self.buffers.iter().map(|b| b.len() as u32).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0 || self.buffers.is_empty()
    }

    pub fn empty() -> Self {
        Self {
            buffers: Vec::new(),
            count: 0,
        }
    }
}
