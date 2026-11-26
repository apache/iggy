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

//! Escapes for the HTML that the server writes. Run titles and labels come from result files, so
//! every value that goes into a page passes through one of them.

/// `text` with the five characters that HTML gives a meaning escaped, so it can stand in element
/// text and in a quoted attribute value.
pub fn escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(character),
        }
    }
    escaped
}

/// `script` with every `</` and `<!--` escaped, so no text in it can end the `<script>` element
/// early. After `<!--<script`, the HTML parser does not take the next `</script>` as the end of
/// the element.
pub fn script_safe(script: &str) -> String {
    script.replace("</", "<\\/").replace("<!--", "<\\!--")
}

/// `json` with every `<` written as `\u003c`, for a `<script>` element of JSON data such as
/// JSON-LD. [`script_safe`] does not fit there, because `\!` is no JSON escape. A `<` stands only
/// in a JSON string, where the escape keeps the value the same, and without it no text can end the
/// element or open a comment.
pub fn json_script_safe(json: &str) -> String {
    json.replace('<', "\\u003c")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_markup_and_quotes_when_escaping_should_write_entities() {
        assert_eq!(
            escape(r#"<script>alert("x & 'y'")</script>"#),
            "&lt;script&gt;alert(&quot;x &amp; &#39;y&#39;&quot;)&lt;/script&gt;"
        );
        assert_eq!(escape("12 topics × 1 partition"), "12 topics × 1 partition");
    }

    #[test]
    fn given_script_end_or_comment_open_when_made_script_safe_should_escape_both() {
        assert_eq!(
            script_safe(r#"{"name":"</script><!--"}"#),
            r#"{"name":"<\/script><\!--"}"#
        );
    }

    #[test]
    fn given_script_end_or_comment_open_in_json_when_made_safe_should_stay_same_json_value() {
        let json = serde_json::json!({ "name": "</script><!--<script>" }).to_string();

        let safe = json_script_safe(&json);

        assert!(!safe.contains('<'), "{safe}");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&safe).expect("still JSON"),
            serde_json::from_str::<serde_json::Value>(&json).expect("JSON")
        );
    }
}
