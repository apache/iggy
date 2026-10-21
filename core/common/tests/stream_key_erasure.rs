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

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use iggy_binary_protocol::WireOptions;
use iggy_common::{
    IggyError, OptionAdmissionContext, OptionColumns, OptionInputMap, OptionInputValue, OptionKey,
    OptionOperation, OptionsScope, OptionsUpdate, ServerKeyContext, StreamCreateOptions,
    StreamEncryptionKey, StreamKeyOrigin, admit, erase_buffer, erase_options,
};

const KEY: [u8; 32] = *b"erasure-probe-key-32-bytes-long!";
const KEY_BASE64: &str = "ZXJhc3VyZS1wcm9iZS1rZXktMzItYnl0ZXMtbG9uZyE=";
const CONFIG_KEY: &str = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";
const PREVIOUS_CONFIG_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";

#[global_allocator]
static ALLOCATOR: FreedKeyProbe = FreedKeyProbe;

/// Counts the blocks freed on a watching thread that still hold the probe key,
/// raw or in base64. Every block starts zeroed, so a scan reads only bytes
/// that were written.
struct FreedKeyProbe;

thread_local! {
    static WATCHING: Cell<bool> = const { Cell::new(false) };
    static FREED_COPIES: Cell<usize> = const { Cell::new(0) };
}

#[test]
fn given_plain_drop_of_a_key_copy_when_probing_should_count_it() {
    let copy = Bytes::copy_from_slice(&KEY);
    let ((), freed) = freed_copies_during(|| drop(copy));
    assert_eq!(freed, 1, "the probe must see a key freed without zeroing");
}

#[test]
fn given_stream_key_when_encoding_create_options_should_free_no_copy_of_it() {
    let typed = StreamCreateOptions {
        encryption_key: Some(StreamEncryptionKey::try_from(KEY.as_slice()).expect("32 bytes")),
        raw: BTreeMap::new(),
    };
    let raw = StreamCreateOptions {
        encryption_key: None,
        raw: BTreeMap::from([(OptionKey::EncryptionKey.to_string(), KEY_BASE64.to_owned())]),
    };
    for options in [typed, raw] {
        let (wire, freed) = freed_copies_during(|| options.to_wire());
        let wire = wire.expect("encodes");
        assert!(holds_key(wire.as_bytes()), "the block carries the key");
        assert_eq!(freed, 0, "to_wire, {options:?}");

        let (input, freed) = freed_copies_during(|| options.to_input_map());
        assert!(input.is_ok(), "encodes for HTTP");
        assert_eq!(freed, 0, "to_input_map, {options:?}");
    }
}

#[test]
fn given_stream_key_in_either_form_when_admitting_create_should_free_no_copy_of_it() {
    let context = OptionAdmissionContext {
        current: None,
        create_columns: OptionColumns {
            name: Some("orders".to_owned()),
            ..OptionColumns::default()
        },
    };
    for block in [raw_key_block(), text_key_block()] {
        let (admitted, freed) = freed_copies_during(|| {
            admit(
                OptionsScope::Stream,
                OptionOperation::Create,
                &block,
                &[],
                &context,
            )
        });
        let admitted = admitted.expect("admits");
        assert_eq!(freed, 0, "admission frees a copy of the key");

        let ((), freed) = freed_copies_during(|| erase_options(admitted.explicit));
        assert_eq!(freed, 0, "erasing the admitted options");
    }
}

#[test]
fn given_refused_raw_entry_after_the_key_when_encoding_should_free_no_copy_of_the_key() {
    let options = StreamCreateOptions {
        encryption_key: None,
        raw: BTreeMap::from([
            (OptionKey::EncryptionKey.to_string(), KEY_BASE64.to_owned()),
            ("zz_empty".to_owned(), String::new()),
        ]),
    };
    let (wire, freed) = freed_copies_during(|| options.to_wire());
    assert!(wire.is_err(), "an empty raw value is refused");
    assert_eq!(freed, 0);
}

#[test]
fn given_stream_key_in_http_create_body_when_encoding_should_free_no_copy_of_it() {
    let body = OptionInputMap(BTreeMap::from([(
        OptionKey::EncryptionKey.to_string(),
        OptionInputValue::String(KEY_BASE64.to_owned().into()),
    )]));
    let (wire, freed) = freed_copies_during(|| body.to_wire());
    let wire = wire.expect("encodes");
    assert!(holds_key(wire.as_bytes()), "the block carries the key");
    assert_eq!(freed, 0);
}

#[test]
fn given_refused_http_key_after_the_stream_key_when_encoding_should_free_no_copy_of_the_key() {
    let body = OptionInputMap(BTreeMap::from([
        (
            OptionKey::EncryptionKey.to_string(),
            OptionInputValue::String(KEY_BASE64.to_owned().into()),
        ),
        ("z".repeat(256), OptionInputValue::Bool(true)),
    ]));
    let (wire, freed) = freed_copies_during(|| body.to_wire());
    assert!(
        matches!(wire, Err(IggyError::UnsupportedOptionKey(_))),
        "a name over 255 bytes is refused"
    );
    assert_eq!(freed, 0);
}

#[test]
fn given_stream_key_in_http_update_body_when_splitting_should_free_no_copy_of_it() {
    let set = BTreeMap::from([(
        OptionKey::EncryptionKey.to_string(),
        OptionInputValue::String(KEY_BASE64.to_owned().into()),
    )]);
    let accepted = OptionsUpdate {
        set: set.clone(),
        reset: BTreeSet::from(["message_expiry".to_owned()]),
    };
    let refused = OptionsUpdate {
        set,
        reset: BTreeSet::from([String::new()]),
    };
    let (patch, freed) = freed_copies_during(|| accepted.to_patch());
    assert!(patch.is_ok(), "splits");
    assert_eq!(freed, 0, "accepted update");

    let (patch, freed) = freed_copies_during(|| refused.to_patch());
    assert!(
        matches!(patch, Err(IggyError::UnsupportedOptionKey(_))),
        "an empty reset name is refused"
    );
    assert_eq!(freed, 0, "refused reset name");
}

#[test]
fn given_shared_buffer_when_erasing_should_leave_it_to_its_other_owner() {
    let buffer = Bytes::copy_from_slice(&KEY);
    let other = buffer.clone();
    let ((), freed) = freed_copies_during(|| erase_buffer(buffer));
    assert_eq!(freed, 0, "a shared buffer is not freed");
    assert_eq!(
        other.as_ref(),
        KEY.as_slice(),
        "the other owner still reads it"
    );

    let ((), freed) = freed_copies_during(|| erase_buffer(other));
    assert_eq!(freed, 0, "the last handle is zeroed before it is freed");
}

#[test]
fn given_rotating_context_when_wrapping_and_opening_should_free_no_copy_of_the_key() {
    let context =
        ServerKeyContext::from_config(CONFIG_KEY, &[PREVIOUS_CONFIG_KEY.to_owned()], true)
            .expect("valid keys");
    let key = StreamEncryptionKey::try_from(KEY.as_slice()).expect("32 bytes");

    let (set, freed) = freed_copies_during(|| context.wrap_set(&key, StreamKeyOrigin::Client));
    let set = set.expect("wraps");
    assert_eq!(freed, 0, "wrap_set");

    let (opened, freed) = freed_copies_during(|| context.open(&set).map(|opened| opened == key));
    assert!(opened.expect("opens"), "opens to the same key");
    assert_eq!(freed, 0, "open");
}

#[test]
fn given_text_stream_key_when_parsing_should_free_no_copy_of_it() {
    let (parsed, freed) = freed_copies_during(|| {
        KEY_BASE64
            .parse::<StreamEncryptionKey>()
            .map(|key| key.as_bytes() == &KEY)
    });
    assert!(parsed.expect("parses"), "parses to the key");
    assert_eq!(freed, 0);
}

unsafe impl GlobalAlloc for FreedKeyProbe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if WATCHING.try_with(Cell::get).unwrap_or(false) {
            // SAFETY: the block is live until the call below and was zeroed at
            // allocation, so all of its bytes are initialized.
            let block = unsafe { std::slice::from_raw_parts(ptr, layout.size()) };
            if holds_key(block) {
                let _ = FREED_COPIES.try_with(|freed| freed.set(freed.get() + 1));
            }
        }
        // SAFETY: the block came from `alloc` above with this layout.
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// Run `work` with the probe watching this thread. Returns its output, freed
/// after the probe stops, and how many freed blocks held the key.
fn freed_copies_during<T>(work: impl FnOnce() -> T) -> (T, usize) {
    FREED_COPIES.with(|freed| freed.set(0));
    WATCHING.with(|watching| watching.set(true));
    let output = work();
    WATCHING.with(|watching| watching.set(false));
    (output, FREED_COPIES.with(Cell::get))
}

fn holds_key(block: &[u8]) -> bool {
    [KEY.as_slice(), KEY_BASE64.as_bytes()]
        .iter()
        .any(|pattern| {
            block
                .windows(pattern.len())
                .any(|window| window == *pattern)
        })
}

fn raw_key_block() -> WireOptions {
    StreamCreateOptions {
        encryption_key: Some(StreamEncryptionKey::try_from(KEY.as_slice()).expect("32 bytes")),
        raw: BTreeMap::new(),
    }
    .to_wire()
    .expect("encodes")
}

fn text_key_block() -> WireOptions {
    StreamCreateOptions {
        encryption_key: None,
        raw: BTreeMap::from([(OptionKey::EncryptionKey.to_string(), KEY_BASE64.to_owned())]),
    }
    .to_wire()
    .expect("encodes")
}
