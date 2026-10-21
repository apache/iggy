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

use std::{
    ffi::{CStr, c_char, c_void},
    mem,
    sync::{Mutex, PoisonError},
};

use ext_php_rs::{
    ffi::{HashTable, zend_function, zend_hash_str_find_ptr_lc, zend_string},
    types::ZendStr,
    zend::ClassEntry,
};

type AddAttribute = unsafe extern "C" fn(
    attributes: *mut *mut HashTable,
    name: *mut zend_string,
    argc: u32,
    flags: u32,
    offset: u32,
    lineno: u32,
) -> *mut c_void;
type GetParameterAttribute = unsafe extern "C" fn(
    attributes: *mut HashTable,
    name: *const c_char,
    length: usize,
    offset: u32,
) -> *mut c_void;

const SENSITIVE_PARAMETER: &str = "SensitiveParameter";
const SENSITIVE_PARAMETER_LOWERCASE: &str = "sensitiveparameter";
const ZEND_ATTRIBUTE_PERSISTENT: u32 = 1;
const SUCCESS: i32 = 0;

/// Class, method and zero-based position of every parameter that carries a secret.
// Every `raw` array is included, since this build cannot tell which unknown keys are secret.
const SENSITIVE_PARAMETERS: &[(&str, &str, u32)] = &[
    ("Iggy\\Client", "fromConnectionString", 0),
    ("Iggy\\Client", "loginUser", 1),
    ("Iggy\\Client", "sendBinaryRequest", 1),
    ("Iggy\\Client", "createStream", 1),
    ("Iggy\\Client", "createStream", 2),
    ("Iggy\\Client", "updateStream", 3),
    ("Iggy\\Client", "createTopic", 13),
    ("Iggy\\Client", "updateTopic", 6),
    ("Iggy\\Client", "encryptionKeyFingerprint", 0),
];

static MARKING: Mutex<()> = Mutex::new(());

/// Marks secret parameters `#[\SensitiveParameter]`, so traces hold a `SensitiveParameterValue`.
// Request startup, because the module startup hook runs before the classes are registered. The
// lock holds every request until the first one has marked them, so no script reads them mid-change.
pub extern "C" fn mark_sensitive_parameters(_type: i32, _module_number: i32) -> i32 {
    let _marking = MARKING.lock().unwrap_or_else(PoisonError::into_inner);
    let Some((add_attribute, get_parameter_attribute)) = attribute_functions() else {
        return SUCCESS;
    };
    let mut name = ZendStr::new(SENSITIVE_PARAMETER, true);

    for (class, method, offset) in SENSITIVE_PARAMETERS {
        let Some(class) = ClassEntry::try_find(class) else {
            continue;
        };
        // SAFETY: the function table belongs to a registered class and outlives the request,
        // and the returned pointer is either null or a live function of that table.
        unsafe {
            let function = zend_hash_str_find_ptr_lc(
                &raw const class.function_table,
                method.as_ptr().cast::<c_char>(),
                method.len(),
            )
            .cast::<zend_function>();
            let Some(function) = function.as_mut() else {
                continue;
            };
            let attributes = &raw mut function.common.attributes;
            let marked = !get_parameter_attribute(
                *attributes,
                SENSITIVE_PARAMETER_LOWERCASE.as_ptr().cast::<c_char>(),
                SENSITIVE_PARAMETER_LOWERCASE.len(),
                *offset,
            )
            .is_null();
            if !marked {
                // Offset 0 addresses the function itself, parameters start at 1.
                add_attribute(
                    attributes,
                    &raw mut *name,
                    0,
                    ZEND_ATTRIBUTE_PERSISTENT,
                    offset + 1,
                    0,
                );
            }
        }
    }

    SUCCESS
}

// Resolved at run time because `cargo php stubs` loads the module with stand-ins for the
// ext-php-rs bindings only, and an unresolved Zend symbol would fail that load.
fn attribute_functions() -> Option<(AddAttribute, GetParameterAttribute)> {
    let add_attribute = resolve(c"zend_add_attribute")?;
    let get_parameter_attribute = resolve(c"zend_get_parameter_attribute_str")?;
    // SAFETY: both symbols are the engine's exported functions with the signatures above.
    unsafe {
        Some((
            mem::transmute::<*mut c_void, AddAttribute>(add_attribute),
            mem::transmute::<*mut c_void, GetParameterAttribute>(get_parameter_attribute),
        ))
    }
}

fn resolve(symbol: &CStr) -> Option<*mut c_void> {
    // SAFETY: dlsym only reads the NUL-terminated name.
    let address = unsafe { libc::dlsym(libc::RTLD_DEFAULT, symbol.as_ptr()) };
    (!address.is_null()).then_some(address)
}
