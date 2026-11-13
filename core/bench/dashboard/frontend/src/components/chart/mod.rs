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

use js_sys::{Object, Reflect};
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::wasm_bindgen;
use web_sys::Element;

const DATA_ZOOM_ACTION: &str = "dataZoom";
/// The start and the end of the zoom window, in percent of the x axis.
const WHOLE_AXIS: [(&str, f64); 2] = [("start", 0.0), ("end", 100.0)];

#[wasm_bindgen]
extern "C" {
    type EChartsInstance;

    #[wasm_bindgen(js_namespace = echarts)]
    fn getInstanceByDom(element: &Element) -> Option<EChartsInstance>;

    #[wasm_bindgen(method)]
    fn dispose(this: &EChartsInstance);

    #[wasm_bindgen(method, js_name = dispatchAction)]
    fn dispatch_action(this: &EChartsInstance, action: &JsValue);
}

/// Also works on an element that has left the page, so an unmounted component frees its chart.
pub fn dispose_chart_at(element: &Element) {
    if let Some(instance) = getInstanceByDom(element) {
        instance.dispose();
    }
}

/// Zooms a chart with a zoom slider out to its whole x axis. A chart without one ignores it.
pub fn reset_zoom_at(element: &Element) {
    let Some(instance) = getInstanceByDom(element) else {
        return;
    };
    let action = Object::new();
    let fields = [("type", JsValue::from_str(DATA_ZOOM_ACTION))]
        .into_iter()
        .chain(WHOLE_AXIS.map(|(key, percent)| (key, JsValue::from_f64(percent))));
    for (key, value) in fields {
        // Setting a property of a new plain object can't fail.
        let _ = Reflect::set(&action, &JsValue::from_str(key), &value);
    }
    instance.dispatch_action(&action);
}
