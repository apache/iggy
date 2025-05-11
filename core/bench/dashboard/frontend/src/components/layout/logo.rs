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

use yew::prelude::*;

#[function_component(Logo)]
pub fn logo() -> Html {
    let (is_dark, _) = use_context::<(bool, Callback<()>)>().expect("Theme context not found");
    let logo_src = if !is_dark {
        "/assets/iggy-dark.png"
    } else {
        "/assets/iggy-light.png"
    };

    html! {
        <div class="logo">
            <img src={logo_src} alt="Apache Iggy Logo" />
            <h1>{"Apache Iggy Benchmarks"}</h1>
        </div>
    }
}
