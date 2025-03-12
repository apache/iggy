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

use twox_hash::XxHash32;

pub fn calculate_32(data: &[u8]) -> u32 {
    XxHash32::oneshot(0, data)
}

pub fn calculate_256(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    #[test]
    fn given_same_input_calculate_should_produce_same_output() {
        let input = "hello world".as_bytes();
        let output1 = super::calculate_32(input);
        let output2 = super::calculate_32(input);
        assert_eq!(output1, output2);
    }
}
