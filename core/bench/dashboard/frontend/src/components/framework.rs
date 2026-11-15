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

//! Marks that name the benchmark framework behind a page's numbers. iggy-bench and the
//! OpenMessaging Benchmark measure differently, so every page says which one it shows, and a
//! screenshot of an OMB page carries its own accent.

use yew::prelude::*;
use yew_router::Routable;
use yew_router::history::{BrowserHistory, History};

use crate::pages::compare::runs::opens_elsewhere;
use crate::pages::methodology::OMB_SECTION_ID;
use crate::router::AppRoute;

const OMB_BANNER_TITLE: &str = "OpenMessaging Benchmark results.";
const OMB_BANNER_TEXT: &str = "OMB measures differently from iggy-bench: other units, other \
                               percentiles and other sampling windows. Don't compare the numbers \
                               on these pages with the numbers on the iggy-bench pages.";
const OMB_BANNER_LINK: &str = "How OMB measures";

/// The tool that measured a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Framework {
    /// The benchmark tool in the Apache Iggy repository.
    IggyBench,
    /// The OpenMessaging Benchmark.
    Omb,
}

impl Framework {
    /// The short name on a chip.
    pub const fn label(self) -> &'static str {
        match self {
            Self::IggyBench => "iggy-bench",
            Self::Omb => "OMB",
        }
    }

    /// The chip's hover text.
    pub const fn hint(self) -> &'static str {
        match self {
            Self::IggyBench => "Measured with iggy-bench, the benchmark tool of Apache Iggy",
            Self::Omb => "Measured with the OpenMessaging Benchmark",
        }
    }

    const fn look(self) -> &'static str {
        match self {
            Self::IggyBench => "framework-iggy-bench",
            Self::Omb => "framework-omb",
        }
    }
}

/// A small mark with the framework's short name, in its own color.
pub fn framework_chip(framework: Framework) -> Html {
    html! {
        <span class={classes!("framework-chip", framework.look())} title={framework.hint()}>
            { framework.label() }
        </span>
    }
}

/// The chip on a line of its own, for a page head between the breadcrumbs and the title.
pub fn framework_line(framework: Framework) -> Html {
    html! { <p class="run-meta">{ framework_chip(framework) }</p> }
}

/// The note on every OMB page: it names the framework, warns against comparing its numbers with
/// the iggy-bench numbers, and links to the OMB section of the Methodology page.
pub fn omb_banner() -> Html {
    html! {
        <aside class="omb-banner" aria-label="About the OMB numbers">
            { framework_chip(Framework::Omb) }
            <p class="omb-banner-text">
                <strong>{ OMB_BANNER_TITLE }</strong>
                {" "}
                { OMB_BANNER_TEXT }
            </p>
            { section_link(
                &AppRoute::Methodology,
                OMB_SECTION_ID,
                classes!("omb-banner-link"),
                html! { OMB_BANNER_LINK },
            ) }
        </aside>
    }
}

/// A link to the section with the id `section` on the page of `route`, as `/path#section`. The
/// router's navigator can't write a fragment, so a plain click pushes the URL to the browser history
/// that the router listens to, and the page scrolls to the section once it renders.
pub fn section_link(route: &AppRoute, section: &str, class: Classes, children: Html) -> Html {
    let href = format!("{}#{section}", route.to_path());
    let onclick = {
        let href = href.clone();
        Callback::from(move |event: MouseEvent| {
            if opens_elsewhere(&event) {
                return;
            }
            event.prevent_default();
            BrowserHistory::new().push(href.clone());
        })
    };
    html! {
        <a {href} {class} {onclick}>{ children }</a>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_frameworks_when_named_should_keep_short_names_and_looks_apart() {
        assert_eq!(Framework::IggyBench.label(), "iggy-bench");
        assert_eq!(Framework::Omb.label(), "OMB");
        assert_ne!(Framework::IggyBench.look(), Framework::Omb.look());
    }
}
