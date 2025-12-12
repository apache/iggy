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

use std::cell::RefCell;
use std::rc::Rc;
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use web_sys::{Element, ResizeObserver, ResizeObserverEntry};
use yew::prelude::*;

type ResizeCallback = Closure<dyn Fn(js_sys::Array)>;

/// Tracks the size of an element using ResizeObserver.
/// Returns (width, height) in pixels.
#[hook]
pub fn use_size(node: NodeRef) -> (u32, u32) {
    let size = use_state(|| (0_u32, 0_u32));

    {
        let size = size.clone();
        use_effect_with(node.clone(), move |node| {
            let node = node.clone();
            let observer_handle: Rc<RefCell<Option<ResizeObserver>>> = Rc::new(RefCell::new(None));
            let callback_handle: Rc<RefCell<Option<ResizeCallback>>> = Rc::new(RefCell::new(None));

            if let Some(element) = node.cast::<Element>() {
                // Set initial size
                size.set((
                    element.client_width() as u32,
                    element.client_height() as u32,
                ));

                let size_clone = size.clone();
                let callback =
                    Closure::<dyn Fn(js_sys::Array)>::new(move |entries: js_sys::Array| {
                        if let Some(entry) = entries.get(0).dyn_ref::<ResizeObserverEntry>() {
                            let target = entry.target();
                            let width = target.client_width() as u32;
                            let height = target.client_height() as u32;
                            size_clone.set((width, height));
                        }
                    });

                if let Ok(observer) = ResizeObserver::new(callback.as_ref().unchecked_ref()) {
                    observer.observe(&element);
                    *observer_handle.borrow_mut() = Some(observer);
                    *callback_handle.borrow_mut() = Some(callback);
                }
            }

            move || {
                if let Some(observer) = observer_handle.borrow_mut().take() {
                    observer.disconnect();
                }
                drop(callback_handle.borrow_mut().take());
            }
        });
    }

    *size
}
