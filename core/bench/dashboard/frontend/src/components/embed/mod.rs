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

//! The embeds of a run page. The page wraps its sections in an [`EmbedProvider`], so the
//! [`EmbedButton`] of each chart card and the [`EmbedCard`] of the Artifacts section open the one
//! dialog of the run, and the dialog keeps the reader's choices from one opening to the next.

mod dialog;
mod options;

use std::rc::Rc;

use yew::prelude::*;

use self::dialog::EmbedDialog;
use self::options::{EmbedOptions, EmbedTheme};

pub use self::options::{EmbedChart, EmbedRun};

#[derive(Clone, PartialEq)]
struct EmbedContext {
    /// The charts that the dialog offers, in the order of the page.
    charts: Rc<[EmbedChart]>,
    open: Callback<EmbedChart>,
}

#[derive(Properties, PartialEq)]
pub struct EmbedProviderProps {
    pub run: EmbedRun,
    /// The run's charts that an embed can draw, the one for the Artifacts card first.
    pub charts: Vec<EmbedChart>,
    #[prop_or_default]
    pub children: Html,
}

#[function_component(EmbedProvider)]
pub fn embed_provider(props: &EmbedProviderProps) -> Html {
    let is_dark = use_context::<(bool, Callback<()>)>().is_some_and(|(is_dark, _)| is_dark);
    let charts = Rc::<[EmbedChart]>::from(props.charts.as_slice());
    let remembered = use_state_eq(|| None::<EmbedOptions>);
    let open = use_state_eq(|| false);

    let open_chart = {
        let remembered = remembered.clone();
        let open = open.clone();
        Callback::from(move |chart: EmbedChart| {
            let options = match &*remembered {
                Some(options) => EmbedOptions {
                    chart,
                    ..options.clone()
                },
                None => EmbedOptions::new(chart, EmbedTheme::of_page(is_dark)),
            };
            remembered.set(Some(options));
            open.set(true);
        })
    };
    let onchange = {
        let remembered = remembered.clone();
        Callback::from(move |options: EmbedOptions| remembered.set(Some(options)))
    };
    let onclose = {
        let open = open.clone();
        Callback::from(move |()| open.set(false))
    };
    let context = EmbedContext {
        charts: charts.clone(),
        open: open_chart,
    };
    let dialog = match (&*remembered, *open) {
        (Some(options), true) => html! {
            <EmbedDialog
                run={props.run}
                {charts}
                options={options.clone()}
                {onchange}
                {onclose}
            />
        },
        _ => Html::default(),
    };
    html! {
        <ContextProvider<EmbedContext> {context}>
            { props.children.clone() }
            { dialog }
        </ContextProvider<EmbedContext>>
    }
}

#[derive(Properties, PartialEq)]
pub struct EmbedButtonProps {
    pub chart: EmbedChart,
}

/// The icon in the corner of a chart card that opens the dialog on its chart. A card outside an
/// [`EmbedProvider`], or whose chart the run can't embed, shows none.
#[function_component(EmbedButton)]
pub fn embed_button(props: &EmbedButtonProps) -> Html {
    let Some(context) = use_context::<EmbedContext>() else {
        return Html::default();
    };
    let chart = props.chart;
    if !context.charts.contains(&chart) {
        return Html::default();
    }
    let label = format!("Embed the {} chart", chart.label().to_lowercase());
    html! {
        <button
            type="button"
            class="embed-button"
            title={label.clone()}
            aria-label={label}
            aria-haspopup="dialog"
            onclick={context.open.reform(move |_: MouseEvent| chart)}
        >
            { code_icon() }
        </button>
    }
}

/// The card of the Artifacts section, which opens the dialog on the run's first chart.
#[function_component(EmbedCard)]
pub fn embed_card() -> Html {
    let Some(context) = use_context::<EmbedContext>() else {
        return Html::default();
    };
    let Some(&first) = context.charts.first() else {
        return Html::default();
    };
    html! {
        <article class="card">
            <h3>{"Embed"}</h3>
            <p>
                {"Show a chart of this run on another page, as a live chart or as an image that \
                  links here. The "}
                <span class="embed-inline-icon" aria-label="embed">{ code_icon() }</span>
                {" button on a chart opens it with that chart."}
            </p>
            <button
                type="button"
                class="run-action"
                aria-haspopup="dialog"
                onclick={context.open.reform(move |_: MouseEvent| first)}
            >
                { code_icon() }
                {"Embed a chart"}
            </button>
        </article>
    }
}

fn code_icon() -> Html {
    html! {
        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24"
             fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"
             stroke-linejoin="round" aria-hidden="true">
            <polyline points="16 18 22 12 16 6" />
            <polyline points="8 6 2 12 8 18" />
        </svg>
    }
}
