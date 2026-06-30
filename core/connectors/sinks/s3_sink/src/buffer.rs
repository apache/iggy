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

use crate::FileRotation;

#[derive(Debug)]
pub(crate) struct FileBuffer {
    data: Vec<u8>,
    boundaries: Vec<usize>,
    message_count: u64,
    first_offset: Option<u64>,
    last_offset: Option<u64>,
    first_timestamp_micros: u64,
}

impl FileBuffer {
    pub fn new() -> Self {
        FileBuffer {
            data: Vec::new(),
            boundaries: Vec::new(),
            message_count: 0,
            first_offset: None,
            last_offset: None,
            first_timestamp_micros: 0,
        }
    }

    pub fn append(&mut self, entry: &[u8], offset: u64, timestamp_micros: u64) {
        self.data.extend_from_slice(entry);
        self.boundaries.push(self.data.len());
        self.message_count += 1;

        if self.first_offset.is_none() {
            self.first_offset = Some(offset);
            self.first_timestamp_micros = timestamp_micros;
        }
        self.last_offset = Some(offset);
    }

    pub fn should_rotate(&self, rotation: FileRotation, max_size: u64, max_messages: u64) -> bool {
        match rotation {
            FileRotation::Size => self.data.len() as u64 >= max_size,
            FileRotation::Messages => self.message_count >= max_messages,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.boundaries.is_empty()
    }

    /// Returns an iterator over individual entry slices without copying.
    pub fn entries(&self) -> impl Iterator<Item = &[u8]> {
        let mut start = 0;
        self.boundaries.iter().map(move |&end| {
            let slice = &self.data[start..end];
            start = end;
            slice
        })
    }

    pub fn first_offset(&self) -> u64 {
        self.first_offset.unwrap_or(0)
    }

    pub fn last_offset(&self) -> u64 {
        self.last_offset.unwrap_or(0)
    }

    pub fn first_timestamp_micros(&self) -> u64 {
        self.first_timestamp_micros
    }

    pub fn message_count(&self) -> u64 {
        self.message_count
    }

    pub fn reset(&mut self) {
        self.data.clear();
        self.boundaries.clear();
        self.message_count = 0;
        self.first_offset = None;
        self.last_offset = None;
        self.first_timestamp_micros = 0;
    }
}

impl Default for FileBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_buffer_is_empty() {
        let buf = FileBuffer::new();
        assert!(buf.is_empty());
        assert_eq!(buf.message_count(), 0);
        assert_eq!(buf.first_offset(), 0);
        assert_eq!(buf.last_offset(), 0);
    }

    #[test]
    fn append_tracks_offsets() {
        let mut buf = FileBuffer::new();
        buf.append(&[1, 2, 3], 10, 1000);
        buf.append(&[4, 5], 11, 1001);
        buf.append(&[6], 12, 1002);

        assert!(!buf.is_empty());
        assert_eq!(buf.message_count(), 3);
        assert_eq!(buf.first_offset(), 10);
        assert_eq!(buf.last_offset(), 12);
        assert_eq!(buf.first_timestamp_micros(), 1000);
        assert_eq!(buf.entries().count(), 3);
    }

    #[test]
    fn rotation_by_size() {
        let mut buf = FileBuffer::new();
        buf.append(&[0; 500], 0, 100);
        assert!(!buf.should_rotate(FileRotation::Size, 1000, 0));

        buf.append(&[0; 500], 1, 200);
        assert!(buf.should_rotate(FileRotation::Size, 1000, 0));

        buf.append(&[0; 100], 2, 300);
        assert!(buf.should_rotate(FileRotation::Size, 1000, 0));
    }

    #[test]
    fn rotation_by_messages() {
        let mut buf = FileBuffer::new();
        buf.append(&[1], 0, 100);
        buf.append(&[2], 1, 200);
        assert!(!buf.should_rotate(FileRotation::Messages, 0, 3));

        buf.append(&[3], 2, 300);
        assert!(buf.should_rotate(FileRotation::Messages, 0, 3));
    }

    #[test]
    fn reset_clears_state() {
        let mut buf = FileBuffer::new();
        buf.append(&[1, 2, 3], 5, 1000);
        buf.append(&[4, 5, 6], 6, 2000);

        buf.reset();

        assert!(buf.is_empty());
        assert_eq!(buf.message_count(), 0);
        assert_eq!(buf.first_offset(), 0);
        assert_eq!(buf.last_offset(), 0);
    }

    #[test]
    fn contiguous_entries() {
        let mut buf = FileBuffer::new();
        buf.append(b"hello", 0, 100);
        buf.append(b"world", 1, 200);

        let entries: Vec<&[u8]> = buf.entries().collect();
        assert_eq!(entries, vec![b"hello".as_slice(), b"world".as_slice()]);
    }
}
