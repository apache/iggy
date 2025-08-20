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

use crate::{
    DecodedMessage, Error, Payload, TopicMetadata, transforms::delete_fields::DeleteFields,
};
use simd_json::OwnedValue;

impl DeleteFields {
    pub(crate) fn transform_json(
        &self,
        _metadata: &TopicMetadata,
        mut message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        let Payload::Json(OwnedValue::Object(ref mut map)) = message.payload else {
            return Ok(Some(message));
        };

        map.retain(|k, v| !self.should_remove(k, v));
        Ok(Some(message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transforms::json::test_utils::{
        create_raw_test_message, create_test_message, create_test_topic_metadata,
        extract_json_object,
    };
    use crate::transforms::{
        Transform,
        delete_fields::{DeleteFields, DeleteFieldsConfig},
    };

    #[test]
    fn should_remove_specified_fields_from_json_message() {
        let transform = DeleteFields::new(DeleteFieldsConfig {
            fields: vec!["field1".to_string(), "field3".to_string()],
        });
        let msg = create_test_message(
            r#"{"field1": "value1", "field2": "value2", "field3": 42, "field4": true}"#,
        );
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["field2"], "value2");
        assert_eq!(json_obj["field4"], true);
        assert!(!json_obj.contains_key("field1"));
        assert!(!json_obj.contains_key("field3"));
    }

    #[test]
    fn should_remove_multiple_fields_while_preserving_others() {
        let transform = DeleteFields::new(DeleteFieldsConfig {
            fields: vec![
                "remove1".to_string(),
                "remove2".to_string(),
                "remove3".to_string(),
            ],
        });
        let msg = create_test_message(
            r#"{
            "keep1": "value1", 
            "remove1": 100, 
            "keep2": true, 
            "remove2": null,
            "remove3": "delete me"
        }"#,
        );
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["keep1"], "value1");
        assert_eq!(json_obj["keep2"], true);
        assert!(!json_obj.contains_key("remove1"));
        assert!(!json_obj.contains_key("remove2"));
        assert!(!json_obj.contains_key("remove3"));
    }

    #[test]
    fn should_ignore_nonexistent_fields_when_deleting() {
        let transform = DeleteFields::new(DeleteFieldsConfig {
            fields: vec!["nonexistent1".to_string(), "field2".to_string()],
        });
        let msg = create_test_message(r#"{"field1": "value1", "field2": "value2", "field3": 42}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["field1"], "value1");
        assert_eq!(json_obj["field3"], 42);
        assert!(!json_obj.contains_key("field2"));
    }

    #[test]
    fn should_return_message_unchanged_when_no_fields() {
        let transform = DeleteFields::new(DeleteFieldsConfig { fields: vec![] });
        let msg = create_test_message(r#"{"field1": "value1", "field2": "value2"}"#);
        let result = transform
            .transform_json(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        let json_obj = extract_json_object(&result).unwrap();
        assert_eq!(json_obj.len(), 2);
        assert_eq!(json_obj["field1"], "value1");
        assert_eq!(json_obj["field2"], "value2");
    }

    #[test]
    fn should_pass_through_non_json_payload_unchanged() {
        let transform = DeleteFields::new(DeleteFieldsConfig {
            fields: vec!["field1".to_string()],
        });
        let msg = create_raw_test_message(vec![1, 2, 3, 4]);
        let result = transform
            .transform(&create_test_topic_metadata(), msg)
            .unwrap()
            .unwrap();
        if let Payload::Raw(bytes) = &result.payload {
            assert_eq!(*bytes, vec![1u8, 2, 3, 4]);
        } else {
            panic!("Expected Raw payload");
        }
    }
}
