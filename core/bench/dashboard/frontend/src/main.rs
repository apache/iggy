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

mod api;
mod components;
mod config;
mod error;
mod router;
mod state;

use crate::{
    components::{app_content::AppContent, footer::Footer},
    state::hardware::HardwareProvider,
};
use components::theme::theme_provider::ThemeProvider;
use router::AppRoute;
use state::{benchmark::BenchmarkProvider, gitref::GitrefProvider, ui::UiProvider};
use yew::prelude::*;
use yew_router::{BrowserRouter, Switch};

#[function_component(App)]
pub fn app() -> Html {
    html! {
        <BrowserRouter>
            <Switch<AppRoute> render={switch} />
        </BrowserRouter>
    }
}

fn switch(routes: AppRoute) -> Html {
    match routes {
        AppRoute::Benchmark { .. } | AppRoute::Home => html! {
            <ThemeProvider>
                <UiProvider>
                    <div class="app-container">
                        <HardwareProvider>
                            <GitrefProvider>
                                <BenchmarkProvider>
                                    <AppContent />
                                </BenchmarkProvider>
                            </GitrefProvider>
                        </HardwareProvider>
                        <Footer />
                    </div>
                </UiProvider>
            </ThemeProvider>
        },
        AppRoute::NotFound => html! { "404 Not Found" },
    }
}

fn main() {
    yew::Renderer::<App>::new().render();
}
