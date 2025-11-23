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

use simd_json::OwnedValue;
use std::io::{self, BufRead, Cursor, Read};
use std::slice::Iter;

pub struct JsonArrowReader<'a> {
    values: Iter<'a, &'a OwnedValue>,
    cursor: Cursor<Vec<u8>>,
}

impl<'a> JsonArrowReader<'a> {
    pub fn new(values: &'a [&OwnedValue]) -> Self {
        Self {
            values: values.iter(),
            cursor: Cursor::new(Vec::new()),
        }
    }

    fn load_next(&mut self) -> io::Result<bool> {
        let Some(val) = self.values.next() else {
            return Ok(false);
        };

        let mut buf = Vec::new();
        simd_json::to_writer(&mut buf, val).map_err(io::Error::other)?;
        buf.push(b'\n');
        self.cursor = Cursor::new(buf);
        Ok(true)
    }
}

impl<'a> Read for JsonArrowReader<'a> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        loop {
            let n = self.cursor.read(out)?;
            if n > 0 {
                return Ok(n);
            }
            if !self.load_next()? {
                return Ok(0);
            }
        }
    }
}

impl<'a> BufRead for JsonArrowReader<'a> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        loop {
            if self.cursor.position() < self.cursor.get_ref().len() as u64 {
                return Ok(&self.cursor.get_ref()[self.cursor.position() as usize..]);
            }

            if !self.load_next()? {
                return Ok(&[]);
            }
        }
    }

    fn consume(&mut self, amt: usize) {
        self.cursor.consume(amt)
    }
}
