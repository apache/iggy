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

#[derive(Clone, Copy, PartialEq)]
pub enum LoaderSize {
    Medium,
    Large,
}

impl LoaderSize {
    fn css_class(self) -> &'static str {
        match self {
            Self::Medium => "iggy-loader-md",
            Self::Large => "iggy-loader-lg",
        }
    }
}

#[derive(Properties, PartialEq)]
pub struct IggyLoaderProps {
    #[prop_or(LoaderSize::Medium)]
    pub size: LoaderSize,
    #[prop_or_default]
    pub label: Option<AttrValue>,
}

#[function_component(IggyLoader)]
pub fn iggy_loader(props: &IggyLoaderProps) -> Html {
    let (is_dark, _) = use_context::<(bool, Callback<()>)>().expect("Theme context not found");
    let logo_src = if is_dark {
        "/assets/iggy-light.svg"
    } else {
        "/assets/iggy-dark.svg"
    };
    let class = classes!("iggy-loader", props.size.css_class());

    html! {
        <div class={class} role="status" aria-live="polite">
            <img class="iggy-loader-mark" src={logo_src} alt="" aria-hidden="true" />
            if let Some(label) = props.label.as_ref() {
                <p class="iggy-loader-label">{label.clone()}</p>
            }
            <span class="visually-hidden">
                { props.label.clone().unwrap_or_else(|| AttrValue::from("Loading")) }
            </span>
        </div>
    }
}
