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

use crate::wire::{request_body, rewrite_request_body};
use iggy_binary_protocol::requests::streams::CreateStreamRequest;
use iggy_binary_protocol::{Operation, RoutedRequestHeader, WireDecode, WireEncode};
use iggy_common::wire_conversions::{resource_options_from_wire, resource_options_to_wire};
use iggy_common::{
    HeaderKey, HeaderValue, IggyError, OptionKey, OptionValue, OptionsProvenance, ServerKeyContext,
    StreamEncryptionKey, StreamKeyOrigin, WrapSet, critical_keys_marker, stream_key_from_entry,
};
use server_common::Message;

/// Wrap a `CreateStream`'s key before the request leaves this node: the
/// client's key when it sent one, or a key generated here when it did not and
/// this node encrypts new streams. Only the wrapped key and the critical marker
/// travel on, so no plaintext key reaches a replica, the log or a snapshot.
/// Runs after catalog admission, which refuses a client-sent wrapped key.
///
/// # Errors
///
/// `StreamEncryptionUnavailable` for a client key on a node without a server
/// key, `InvalidEncryptionKey` for a malformed one, `InvalidCommand` for a body
/// that does not decode.
pub fn maybe_rewrite_secret_options_request(
    context: &ServerKeyContext,
    request: Message<RoutedRequestHeader>,
) -> Result<Message<RoutedRequestHeader>, IggyError> {
    if request.header().operation != Operation::CreateStream {
        return Ok(request);
    }
    let mut create = CreateStreamRequest::decode_from(request_body(&request))
        .map_err(|_| IggyError::InvalidCommand)?;
    let key_name = <&str>::from(OptionKey::EncryptionKey).as_bytes();
    let client_key = (&create.options)
        .into_iter()
        .find(|entry| entry.key == key_name)
        .map(|entry| stream_key_from_entry(&entry))
        .transpose()?;
    let (key, origin) = match client_key {
        Some(_) if !context.accepts_stream_keys() => {
            return Err(IggyError::StreamEncryptionUnavailable);
        }
        Some(key) => (key, StreamKeyOrigin::Client),
        None if context.generates_stream_keys() => {
            (StreamEncryptionKey::generate()?, StreamKeyOrigin::Server)
        }
        None => return Ok(request),
    };
    let set = WrapSet::from(context.wrap(&key, origin)?);
    let mut options = resource_options_from_wire(&create.options, true)?;
    options.remove(&HeaderKey::from(OptionKey::EncryptionKey));
    options.insert(
        HeaderKey::from(OptionKey::EncryptionKeyWrapped),
        OptionValue::explicit(HeaderValue::try_from(set.as_ref())?),
    );
    options.insert(
        HeaderKey::from(OptionKey::CriticalKeys),
        OptionValue::explicit(critical_keys_marker(&[OptionKey::EncryptionKeyWrapped])?),
    );
    create.options = resource_options_to_wire(&options, OptionsProvenance::All)?;
    rewrite_request_body(&request, &create.to_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::test_support::request_message;
    use iggy_binary_protocol::{WireName, WireOptions};
    use iggy_common::{
        OptionAdmissionContext, OptionColumns, OptionOperation, OptionsScope, ResourceOptions,
        StreamCreateOptions, admit, stream_wrap_set, text,
    };

    const CONFIG_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";

    #[test]
    fn given_custom_key_on_binary_backup_when_rewriting_then_should_forward_only_wrapped_bytes() {
        let context = context(true);
        let key = StreamEncryptionKey::try_from([7u8; 32].as_slice()).unwrap();
        for options in [
            StreamCreateOptions {
                encryption_key: Some(key.clone()),
                ..StreamCreateOptions::default()
            }
            .to_wire()
            .unwrap(),
            raw_key_block(&text::as_base64(key.as_bytes())),
        ] {
            let rewritten = rewrite(&context, options).expect("rewrites");
            let body = request_body(&rewritten).to_vec();
            assert!(!contains(&body, key.as_bytes()), "raw key travels on");
            assert!(
                !contains(&body, text::as_base64(key.as_bytes()).as_bytes()),
                "base64 key travels on"
            );
            let set = stored_set(&rewritten).expect("a wrap set");
            assert_eq!(set.envelopes().len(), 1);
            assert_eq!(set.origin(), StreamKeyOrigin::Client);
            assert_eq!(context.open(&set).unwrap(), key);
        }
    }

    #[test]
    fn given_keyless_create_with_encryption_enabled_when_rewriting_then_should_attach_server_origin_wrap_set()
     {
        let context = context(true);
        let first =
            stored_set(&rewrite(&context, WireOptions::empty()).unwrap()).expect("a generated key");
        let second =
            stored_set(&rewrite(&context, WireOptions::empty()).unwrap()).expect("a generated key");
        assert_eq!(first.origin(), StreamKeyOrigin::Server);
        assert!(context.open(&first).is_ok());
        assert_ne!(first.key_fingerprint(), second.key_fingerprint());

        let disabled = self::context(false);
        let request = create_request(WireOptions::empty());
        let passed = maybe_rewrite_secret_options_request(&disabled, request.clone()).unwrap();
        assert_eq!(passed.as_slice(), request.as_slice());
    }

    #[test]
    fn given_client_internal_key_when_admitting_then_should_reject_before_wrapping() {
        let context = context(true);
        let forged = WrapSet::from(
            context
                .wrap(
                    &StreamEncryptionKey::try_from([1u8; 32].as_slice()).unwrap(),
                    StreamKeyOrigin::Client,
                )
                .unwrap(),
        );
        for (key, value) in [
            (
                OptionKey::EncryptionKeyWrapped,
                HeaderValue::try_from(forged.as_ref()).unwrap(),
            ),
            (
                OptionKey::CriticalKeys,
                critical_keys_marker(&[OptionKey::EncryptionKeyWrapped]).unwrap(),
            ),
        ] {
            let mut options = ResourceOptions::new();
            options.insert(HeaderKey::from(key), OptionValue::explicit(value));
            let block = resource_options_to_wire(&options, OptionsProvenance::All).unwrap();
            assert_eq!(
                admit_create(&block).unwrap_err().as_code(),
                IggyError::ReadOnlyOption(key.to_string()).as_code()
            );
        }
        let valid = StreamCreateOptions {
            encryption_key: Some(StreamEncryptionKey::try_from([2u8; 32].as_slice()).unwrap()),
            ..StreamCreateOptions::default()
        }
        .to_wire()
        .unwrap();
        assert!(admit_create(&valid).is_ok());
        assert!(rewrite(&context, valid.clone()).is_ok());
        assert_eq!(
            rewrite(&ServerKeyContext::disabled(), valid)
                .unwrap_err()
                .as_code(),
            IggyError::StreamEncryptionUnavailable.as_code()
        );
    }

    fn context(enabled: bool) -> ServerKeyContext {
        ServerKeyContext::from_config(CONFIG_KEY, &[], enabled).unwrap()
    }

    fn create_request(options: WireOptions) -> Message<RoutedRequestHeader> {
        let body = CreateStreamRequest {
            name: WireName::new("orders").unwrap(),
            options,
        }
        .to_bytes();
        request_message(Operation::CreateStream, 1, 1, 1, &body)
    }

    fn rewrite(
        context: &ServerKeyContext,
        options: WireOptions,
    ) -> Result<Message<RoutedRequestHeader>, IggyError> {
        maybe_rewrite_secret_options_request(context, create_request(options))
    }

    fn stored_set(request: &Message<RoutedRequestHeader>) -> Option<WrapSet> {
        let create = CreateStreamRequest::decode_from(request_body(request)).unwrap();
        let options = resource_options_from_wire(&create.options, true).unwrap();
        assert!(!options.contains_key(&HeaderKey::from(OptionKey::EncryptionKey)));
        stream_wrap_set(&options).unwrap()
    }

    fn raw_key_block(base64: &str) -> WireOptions {
        let mut options = ResourceOptions::new();
        options.insert(
            HeaderKey::from(OptionKey::EncryptionKey),
            OptionValue::explicit(HeaderValue::try_from(base64).unwrap()),
        );
        resource_options_to_wire(&options, OptionsProvenance::All).unwrap()
    }

    fn admit_create(block: &WireOptions) -> Result<(), IggyError> {
        admit(
            OptionsScope::Stream,
            OptionOperation::Create,
            block,
            &[],
            &OptionAdmissionContext {
                current: None,
                create_columns: OptionColumns {
                    name: Some("orders".to_owned()),
                    ..OptionColumns::default()
                },
            },
        )
        .map(|_| ())
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }
}
