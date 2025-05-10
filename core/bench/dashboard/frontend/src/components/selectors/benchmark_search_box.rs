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

#[derive(Properties, PartialEq, Clone)]
pub struct BenchmarkSearchBoxProps {
    pub search_query: UseStateHandle<String>,
}

#[function_component(BenchmarkSearchBox)]
pub fn benchmark_search_box(props: &BenchmarkSearchBoxProps) -> Html {
    let search_query_handle = props.search_query.clone();
    let on_search_input = Callback::from(move |e: InputEvent| {
        let input: web_sys::HtmlInputElement = e.target_unchecked_into();
        search_query_handle.set(input.value());
    });

    html! {
        <div class="search-container">
            <input
                type="search"
                class="benchmark-search-input"
                placeholder="Search benchmarks..."
                value={(*props.search_query).clone()}
                oninput={on_search_input}
            />
        </div>
    }
}
