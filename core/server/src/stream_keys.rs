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

use crate::wire::rewrite_request_body;
use bytes::Bytes;
use iggy_binary_protocol::requests::streams::CreateStreamRequest;
use iggy_binary_protocol::{RoutedRequestHeader, WireEncode};
use iggy_common::wire_conversions::resource_options_to_wire;
use iggy_common::{
    HeaderKey, HeaderValue, IggyError, OptionKey, OptionValue, OptionsProvenance, ResourceOptions,
    ServerKeyContext, StreamEncryptionKey, StreamKeyOrigin, critical_keys_marker, erase_buffer,
};
use metadata::stm::stream::StreamsInner;
use secrecy::zeroize::Zeroize;
use server_common::Message;

/// A request past static bounds. A `CreateStream` keeps its decoded body and
/// admitted options for [`maybe_rewrite_secret_options_request`], so a client
/// key is decoded once and never encoded into a request in the clear again.
pub enum Bounded {
    Request(Message<RoutedRequestHeader>),
    StreamCreate {
        request: Message<RoutedRequestHeader>,
        create: CreateStreamRequest,
        /// The admitted canonical options, a client key among them.
        options: ResourceOptions,
    },
}

/// Wrap a `CreateStream`'s key before the request leaves this node: the
/// client's key when it sent one, or a key generated here when it did not and
/// this node encrypts new streams. The key is wrapped under every configured
/// server key, so the stream also opens on nodes a rotation has not reached.
/// TCP requests carry only the wrap set and the critical marker onward, so no
/// plaintext key reaches a replica, the log or a snapshot. An HTTP follower
/// relays the raw body to the primary, which wraps the key there. The body is
/// built from the admitted options, and the copies of a client key this node
/// decoded are erased whatever the outcome. Runs after catalog admission,
/// which refuses a client-sent wrapped key.
///
/// # Errors
///
/// `StreamEncryptionUnavailable` for a client key on a node without a server
/// key, or for any key while `deferring` reports a committed stream key this
/// node cannot open, `CannotEncryptData` when generating or wrapping fails.
pub fn maybe_rewrite_secret_options_request(
    context: &ServerKeyContext,
    bounded: Bounded,
    deferring: impl FnOnce() -> bool,
) -> Result<Message<RoutedRequestHeader>, IggyError> {
    let (request, create, options) = match bounded {
        Bounded::Request(request) => return Ok(request),
        Bounded::StreamCreate {
            request,
            create,
            options,
        } => (request, create, options),
    };
    match sealed_body(context, &create, options, deferring) {
        Ok(None) => Ok(request),
        Ok(Some(body)) => {
            let rewritten = rewrite_request_body(&request, &body);
            erase_stream_create(request, Some(create));
            rewritten
        }
        Err(error) => {
            erase_stream_create(request, Some(create));
            Err(error)
        }
    }
}

/// Zero a create's copies of a client key once it is rewritten or refused:
/// the decoded options block, when there is one, and the request body.
pub fn erase_stream_create(
    mut request: Message<RoutedRequestHeader>,
    create: Option<CreateStreamRequest>,
) {
    if let Some(create) = create {
        erase_buffer(create.options.into_bytes());
    }
    request.as_mut_slice()[size_of::<RoutedRequestHeader>()..].zeroize();
}

/// The create body apply stores: the admitted options with the stream key
/// wrapped in place of a client key, or `None` when the client's own body
/// already is it, which holds no key.
fn sealed_body(
    context: &ServerKeyContext,
    create: &CreateStreamRequest,
    mut options: ResourceOptions,
    deferring: impl FnOnce() -> bool,
) -> Result<Option<Bytes>, IggyError> {
    let client_key = match options.remove(&HeaderKey::from(OptionKey::EncryptionKey)) {
        Some(plaintext) => {
            let key = StreamEncryptionKey::try_from(plaintext.value.as_bytes());
            plaintext.erase();
            Some(key?)
        }
        None => None,
    };
    let key = match client_key {
        Some(_) if !context.accepts_stream_keys() => {
            return Err(IggyError::StreamEncryptionUnavailable);
        }
        Some(key) => Some((key, StreamKeyOrigin::Client)),
        None if context.generates_stream_keys() => {
            Some((StreamEncryptionKey::generate()?, StreamKeyOrigin::Server))
        }
        None => None,
    };
    if let Some((key, origin)) = key {
        // Keys that leave a committed stream unopenable may be keys no peer
        // holds, and a stream wrapped only under them opens nowhere else.
        if deferring() {
            return Err(IggyError::StreamEncryptionUnavailable);
        }
        let set = context.wrap_set(&key, origin)?;
        options.insert(
            HeaderKey::from(OptionKey::EncryptionKeyWrapped),
            OptionValue::explicit(HeaderValue::try_from(set.as_ref())?),
        );
        options.insert(
            HeaderKey::from(OptionKey::CriticalKeys),
            OptionValue::explicit(critical_keys_marker(&[OptionKey::EncryptionKeyWrapped])?),
        );
    }
    let block = resource_options_to_wire(&options, OptionsProvenance::All)?;
    if block.as_bytes() == create.options.as_bytes() {
        return Ok(None);
    }
    Ok(Some(
        CreateStreamRequest {
            name: create.name.clone(),
            options: block,
        }
        .to_bytes(),
    ))
}

/// Whether a committed stream holds no wrap under this node's keys, so this
/// node defers serving it. One fingerprint scan over every stream, run only
/// for a create that wraps a key.
#[must_use]
pub fn defers_stream_keys(streams: &StreamsInner, context: &ServerKeyContext) -> bool {
    let wrapped_key = HeaderKey::from(OptionKey::EncryptionKeyWrapped);
    streams.items.iter().any(|(_, stream)| {
        stream
            .options
            .get(&wrapped_key)
            .is_some_and(|option| !context.holds_wrap_for(option.value.as_bytes()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::test_support::request_message;
    use crate::wire::request_body;
    use iggy_binary_protocol::{Operation, WireDecode, WireName, WireOptions};
    use iggy_common::wire_conversions::resource_options_from_wire;
    use iggy_common::{
        AdmittedOptions, IggyTimestamp, OptionAdmissionContext, OptionColumns, OptionOperation,
        OptionsScope, StreamCreateOptions, WrapSet, admit, stream_wrap_set, text,
    };
    use metadata::stm::StateHandler;

    const CONFIG_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";
    const NEW_CONFIG_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";

    #[test]
    fn given_custom_key_on_binary_backup_when_rewriting_should_forward_only_wrapped_bytes() {
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
    fn given_keyless_create_with_encryption_enabled_when_rewriting_should_attach_server_origin_wrap_set()
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
        let passed =
            maybe_rewrite_secret_options_request(&disabled, bounded(request.clone()), || false)
                .expect("passes through");
        assert_eq!(passed.as_slice(), request.as_slice());
    }

    #[test]
    fn given_decoded_create_when_erasing_should_hold_the_only_handle_to_its_options() {
        let request = create_request(raw_key_block(&text::as_base64(&[5u8; 32])));
        let create = CreateStreamRequest::decode_from(request_body(&request)).expect("decodes");
        assert!(
            create.options.into_bytes().is_unique(),
            "the decoded block shares the request buffer, so erasing it would zero nothing"
        );
    }

    #[test]
    fn given_rotated_node_when_wrapping_a_create_should_open_on_nodes_holding_only_the_old_key() {
        let rotated = ServerKeyContext::from_config(NEW_CONFIG_KEY, &[CONFIG_KEY.to_owned()], true)
            .expect("valid keys");
        let old_only = context(true);
        let new_only = ServerKeyContext::from_config(NEW_CONFIG_KEY, &[], true).expect("valid key");
        let key = StreamEncryptionKey::try_from([3u8; 32].as_slice()).expect("32 bytes");
        for options in [
            WireOptions::empty(),
            StreamCreateOptions {
                encryption_key: Some(key),
                ..StreamCreateOptions::default()
            }
            .to_wire()
            .expect("encodes"),
        ] {
            let set = stored_set(&rewrite(&rotated, options).expect("rewrites")).expect("a set");
            let opened = rotated.open(&set).expect("opens on the rotated node");
            assert_eq!(old_only.open(&set).expect("opens on an old node"), opened);
            assert_eq!(new_only.open(&set).expect("opens after phase 2"), opened);
        }
        let single = stored_set(&rewrite(&new_only, WireOptions::empty()).expect("rewrites"))
            .expect("a set");
        assert_eq!(single.envelopes().len(), 1);
    }

    #[test]
    fn given_client_internal_key_when_admitting_should_reject_before_wrapping() {
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

    #[test]
    fn given_committed_stream_this_node_cannot_open_when_wrapping_a_create_should_refuse() {
        let context = context(true);
        let stranger = ServerKeyContext::from_config(NEW_CONFIG_KEY, &[], true).expect("valid key");
        let rotated = ServerKeyContext::from_config(NEW_CONFIG_KEY, &[CONFIG_KEY.to_owned()], true)
            .expect("valid keys");
        let mut streams = StreamsInner::new();
        insert_wrapped_stream(&mut streams, "opens-here", &context);
        assert!(!defers_stream_keys(&streams, &context));
        insert_wrapped_stream(&mut streams, "stranger", &stranger);
        assert!(defers_stream_keys(&streams, &context));
        assert!(!defers_stream_keys(&streams, &rotated));

        let client_key = StreamCreateOptions {
            encryption_key: Some(StreamEncryptionKey::try_from([4u8; 32].as_slice()).unwrap()),
            ..StreamCreateOptions::default()
        }
        .to_wire()
        .unwrap();
        for options in [WireOptions::empty(), client_key] {
            let refusal = maybe_rewrite_secret_options_request(
                &context,
                bounded(create_request(options)),
                || defers_stream_keys(&streams, &context),
            )
            .expect_err("a deferring node must not wrap a new stream key");
            assert_eq!(
                refusal.as_code(),
                IggyError::StreamEncryptionUnavailable.as_code()
            );
        }
        let plaintext = self::context(false);
        let request = create_request(WireOptions::empty());
        let passed =
            maybe_rewrite_secret_options_request(&plaintext, bounded(request.clone()), || {
                defers_stream_keys(&streams, &plaintext)
            })
            .expect("a create that wraps nothing passes");
        assert_eq!(passed.as_slice(), request.as_slice());
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
        maybe_rewrite_secret_options_request(context, bounded(create_request(options)), || false)
    }

    /// `request` past static bounds, admitted the way both chains admit it.
    fn bounded(request: Message<RoutedRequestHeader>) -> Bounded {
        let create = CreateStreamRequest::decode_from(request_body(&request)).expect("decodes");
        let admitted = admit_create(&create.options).expect("admits");
        Bounded::StreamCreate {
            request,
            create,
            options: admitted.explicit,
        }
    }

    fn stored_set(request: &Message<RoutedRequestHeader>) -> Option<WrapSet> {
        let create = CreateStreamRequest::decode_from(request_body(request)).unwrap();
        let options = resource_options_from_wire(&create.options, true).unwrap();
        assert!(!options.contains_key(&HeaderKey::from(OptionKey::EncryptionKey)));
        stream_wrap_set(&options).unwrap()
    }

    fn insert_wrapped_stream(streams: &mut StreamsInner, name: &str, wrapping: &ServerKeyContext) {
        let set = wrapping
            .wrap_set(
                &StreamEncryptionKey::try_from([9u8; 32].as_slice()).unwrap(),
                StreamKeyOrigin::Server,
            )
            .unwrap();
        let mut options = ResourceOptions::new();
        options.insert(
            HeaderKey::from(OptionKey::EncryptionKeyWrapped),
            OptionValue::explicit(HeaderValue::try_from(set.as_ref()).unwrap()),
        );
        options.insert(
            HeaderKey::from(OptionKey::CriticalKeys),
            OptionValue::explicit(
                critical_keys_marker(&[OptionKey::EncryptionKeyWrapped]).unwrap(),
            ),
        );
        let request = CreateStreamRequest {
            name: WireName::new(name).unwrap(),
            options: resource_options_to_wire(&options, OptionsProvenance::All).unwrap(),
        };
        assert_eq!(
            StateHandler::apply(&request, streams, IggyTimestamp::now()).code,
            0
        );
    }

    fn raw_key_block(base64: &str) -> WireOptions {
        let mut options = ResourceOptions::new();
        options.insert(
            HeaderKey::from(OptionKey::EncryptionKey),
            OptionValue::explicit(HeaderValue::try_from(base64).unwrap()),
        );
        resource_options_to_wire(&options, OptionsProvenance::All).unwrap()
    }

    fn admit_create(block: &WireOptions) -> Result<AdmittedOptions, IggyError> {
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
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }
}
