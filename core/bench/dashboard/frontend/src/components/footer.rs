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
            <div class="footer-content">
                <a href="https://iggy.apache.org" target="_blank" rel="noopener noreferrer">
                    {"iggy.apache.org"}
                </a>
                <span class="separator">{"|"}</span>
                <a href="https://github.com/apache/iggy" target="_blank" rel="noopener noreferrer">
                    {"GitHub"}
                </a>
                <span class="separator">{"|"}</span>
                {"v"}{env!("CARGO_PKG_VERSION")}
                <span class="separator">{"|"}</span>
                {" 2026 Apache Iggy (Incubating). Built with ❤️ for the message streaming community."}
            </div>
        </footer>
    }
}
