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

use crate::IggyError;
use bytes::{Bytes, BytesMut};

/// The trait represents the logic responsible for serializing and deserializing the struct to and from bytes.
pub trait BytesSerializable {
    /// Serializes the struct to bytes.
    fn to_bytes(&self) -> Bytes;

    /// Deserializes the struct from bytes.
    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized;

    /// Write the struct to a buffer.
    fn write_to_buffer(&self, _buf: &mut BytesMut) {
        unimplemented!();
    }

    /// Get the byte-size of the struct.
    fn get_buffer_size(&self) -> usize {
        unimplemented!();
    }
}
