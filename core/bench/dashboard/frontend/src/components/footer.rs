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

#[function_component(Footer)]
pub fn footer() -> Html {
    html! {
        <footer class="footer">
            <div class="footer-inner">
                <div class="footer-brand">
                    <span class="footer-brand-name">{"Apache Iggy"}</span>
                    <span class="footer-brand-tag">{"Incubating"}</span>
                </div>
                <nav class="footer-links">
                    <a href="https://iggy.apache.org" target="_blank" rel="noopener noreferrer">
                        {"iggy.apache.org"}
                    </a>
                    <a href="https://github.com/apache/iggy" target="_blank" rel="noopener noreferrer">
                        { render_github_icon() }
                        {"GitHub"}
                    </a>
                    <a href="https://iggy.apache.org/docs/" target="_blank" rel="noopener noreferrer">
                        {"Docs"}
                    </a>
                </nav>
                <div class="footer-meta">
                    <span class="footer-version">{"v"}{env!("CARGO_PKG_VERSION")}</span>
                    <span class="footer-sep">{"•"}</span>
                    <a href="https://www.apache.org/" target="_blank" rel="noopener noreferrer">
                        {"Apache Software Foundation"}
                    </a>
                    <span class="footer-sep">{"•"}</span>
                    <span class="footer-tagline">
                        {"Built with "}
                        <span class="footer-heart" aria-label="love">{"❤"}</span>
                        {" for the message streaming community"}
                    </span>
                </div>
            </div>
        </footer>
    }
}

fn render_github_icon() -> Html {
    html! {
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"
             fill="currentColor">
            <path d="M12 .5C5.37.5 0 5.87 0 12.5c0 5.3 3.44 9.8 8.21 11.39.6.11.82-.26.82-.58v-2.04c-3.34.73-4.04-1.61-4.04-1.61-.55-1.39-1.34-1.76-1.34-1.76-1.09-.74.08-.73.08-.73 1.21.09 1.85 1.24 1.85 1.24 1.07 1.84 2.81 1.31 3.5 1 .11-.78.42-1.31.77-1.61-2.67-.3-5.47-1.33-5.47-5.93 0-1.31.47-2.38 1.24-3.22-.12-.3-.54-1.52.12-3.17 0 0 1.01-.32 3.3 1.23.96-.27 1.98-.4 3-.41 1.02 0 2.04.14 3 .41 2.29-1.55 3.3-1.23 3.3-1.23.66 1.65.24 2.87.12 3.17.77.84 1.24 1.91 1.24 3.22 0 4.61-2.8 5.62-5.47 5.92.43.37.82 1.1.82 2.22v3.29c0 .32.22.69.82.58C20.57 22.29 24 17.8 24 12.5 24 5.87 18.63.5 12 .5z" />
        </svg>
    }
}
