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

//! The render of the app on the server. The tree is the one that the browser hydrates, below a
//! router with the requested path in place of the address bar of the browser.

use std::rc::Rc;

use yew::ServerRenderer;
use yew::prelude::*;
use yew_router::Router;
use yew_router::history::{AnyHistory, History, MemoryHistory};
use yew_router::query::Raw;

use crate::StartData;
use crate::shell;

#[derive(Properties, PartialEq)]
struct ServerAppProps {
    path: AttrValue,
    /// Without the `?`. Empty for a path without a query.
    query: AttrValue,
    start: Rc<StartData>,
}

#[function_component(ServerApp)]
fn server_app(props: &ServerAppProps) -> Html {
    let history = AnyHistory::from(MemoryHistory::new());
    // `push` refuses a path with a query, and a query of `?` alone would differ from the browser.
    let pushed = if props.query.is_empty() {
        history.push(props.path.as_str());
        Ok(())
    } else {
        history.push_with_query(props.path.as_str(), Raw(props.query.as_str()))
    };
    if pushed.is_err() {
        history.push(props.path.as_str());
    }
    html! {
        <Router {history}>
            { shell(Some(Rc::clone(&props.start))) }
        </Router>
    }
}

/// The HTML of the app at `path` with `query`, rendered from `start`. The browser hydrates it when
/// the page holds the same `start` as JSON in the element [`crate::START_DATA_ID`].
pub async fn render(path: String, query: String, start: StartData) -> String {
    ServerRenderer::<ServerApp>::with_props(move || ServerAppProps {
        path: path.into(),
        query: query.into(),
        start: Rc::new(start),
    })
    .render()
    .await
}
