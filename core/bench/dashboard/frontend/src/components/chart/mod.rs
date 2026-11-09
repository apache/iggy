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

use wasm_bindgen::prelude::wasm_bindgen;
use web_sys::Element;

#[wasm_bindgen]
extern "C" {
    type EChartsInstance;

    #[wasm_bindgen(js_namespace = echarts)]
    fn getInstanceByDom(element: &Element) -> Option<EChartsInstance>;

    #[wasm_bindgen(method)]
    fn dispose(this: &EChartsInstance);
}

/// Also works on an element that has left the page, so an unmounted component frees its chart.
pub fn dispose_chart_at(element: &Element) {
    if let Some(instance) = getInstanceByDom(element) {
        instance.dispose();
    }
}
