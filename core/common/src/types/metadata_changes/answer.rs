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

use super::json::hex_u128_option;
use super::{
    MetadataChangeCursor, MetadataChangePhase, MetadataChangeRecord, MetadataChangeStatus,
    MetadataCurrentItem, MetadataCurrentPageToken,
};
use serde::{Deserialize, Serialize};

/// One page of the change log, or of the current state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetadataChanges {
    pub status: MetadataChangeStatus,
    pub phase: MetadataChangePhase,
    /// This node's log id, absent before it applied op 1.
    #[serde(with = "hex_u128_option")]
    pub log_id: Option<u128>,
    /// Lowest whole operation from which the requested classes are complete.
    pub oldest_op: u64,
    /// The requested classes are complete for every timestamp above this
    /// value, 0 when nothing was evicted.
    pub retained_since: u64,
    /// Highest metadata operation this node applied.
    pub head_op: u64,
    /// The cursor to send back. Present on `ok`. On `behind` it repeats the
    /// position the request carried, so the same request can be retried.
    pub next: Option<MetadataChangeCursor>,
    /// More entries remain after `next`, or another state page exists.
    pub has_more: bool,
    pub records: Vec<MetadataChangeRecord>,
    pub items: Vec<MetadataCurrentItem>,
    /// Continuation of a current state scan, absent once it is complete. On
    /// `behind` it echoes the token of the page to retry, with `has_more`
    /// false.
    pub current_page_token: Option<MetadataCurrentPageToken>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_behind_answer_when_serializing_should_render_nulls_for_absent_positions() {
        let answer = MetadataChanges {
            status: MetadataChangeStatus::Behind,
            phase: MetadataChangePhase::Changes,
            log_id: None,
            oldest_op: 0,
            retained_since: 0,
            head_op: 0,
            next: None,
            has_more: false,
            records: vec![],
            items: vec![],
            current_page_token: None,
        };
        let json = serde_json::to_string(&answer).unwrap();
        assert_eq!(
            json,
            concat!(
                r#"{"status":"behind","phase":"changes","log_id":null,"oldest_op":0,"#,
                r#""retained_since":0,"head_op":0,"next":null,"has_more":false,"records":[],"#,
                r#""items":[],"current_page_token":null}"#
            )
        );
        assert_eq!(
            serde_json::from_str::<MetadataChanges>(&json).unwrap(),
            answer
        );
    }

    #[test]
    fn given_ok_answer_with_cursor_when_round_tripping_json_should_preserve_it() {
        let answer = MetadataChanges {
            status: MetadataChangeStatus::Ok,
            phase: MetadataChangePhase::Changes,
            log_id: Some(0x0d4b_8f2a_6e1c_93d7),
            oldest_op: 3871,
            retained_since: 1_760_352_000_000_000,
            head_op: 4187,
            next: Some(MetadataChangeCursor {
                log_id: 0x0d4b_8f2a_6e1c_93d7,
                op: 4187,
                checksum: 0xf2a4_c6e8_b0d1_f359,
                ordinal: 1,
            }),
            has_more: false,
            records: vec![],
            items: vec![],
            current_page_token: None,
        };
        let json = serde_json::to_string(&answer).unwrap();
        assert!(json.contains(r#""log_id":"00000000000000000d4b8f2a6e1c93d7""#));
        assert_eq!(
            serde_json::from_str::<MetadataChanges>(&json).unwrap(),
            answer
        );
    }
}
