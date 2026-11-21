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

//! The dialog that builds the embed of a run's chart. Every choice changes the code and the
//! preview at once, and the preview loads the same link that the code puts on the other page.

use std::rc::Rc;

use bench_dashboard_shared::embed::PngSize;
use gloo::timers::callback::Timeout;
use web_sys::{Element, HtmlDialogElement, HtmlInputElement, KeyboardEvent};
use yew::prelude::*;

use super::options::{
    CodeKind, EmbedChart, EmbedFormat, EmbedOptions, EmbedRun, EmbedTheme, FRAME_SIZES, FrameSize,
    FrameWidth, MAX_SIDE, Size,
};
use crate::components::clipboard::{CopyState, use_copy};
use crate::config::get_api_base_url;
use crate::hooks::use_size;

const TITLE_ID: &str = "embed-dialog-title";
/// How long the dialog waits after a key in a size field before it applies the new size, so the
/// preview doesn't jump through 1, 12 and 120 on the way to 1200.
const SIZE_INPUT_DELAY_MS: u32 = 450;
/// The room around the chart in the preview.
const STAGE_MARGIN: u32 = 32;
const IMAGE_SIZE_NOTE: &str = "The server keeps every image that it draws, so it draws each image \
                               only once.";
const DEFAULT_FRAME_WIDTH: u32 = 800;
const FRAME_NOTE: &str = "The chart scales its text to the size of the frame.";

#[derive(Properties, PartialEq)]
pub struct EmbedDialogProps {
    pub run: EmbedRun,
    pub charts: Rc<[EmbedChart]>,
    pub options: EmbedOptions,
    pub onchange: Callback<EmbedOptions>,
    pub onclose: Callback<()>,
}

#[function_component(EmbedDialog)]
pub fn embed_dialog(props: &EmbedDialogProps) -> Html {
    let dialog_ref = use_node_ref();
    {
        let dialog_ref = dialog_ref.clone();
        use_effect_with((), move |()| {
            if let Some(dialog) = dialog_ref.cast::<HtmlDialogElement>()
                && !dialog.open()
            {
                // It fails only for a dialog that is open or not in the document, and then there is
                // nothing to show.
                let _ = dialog.show_modal();
            }
        });
    }
    let close = {
        let dialog_ref = dialog_ref.clone();
        Callback::from(move |_: MouseEvent| close_dialog(&dialog_ref))
    };
    // A click whose press started inside the dialog and ended on the backdrop, such as a text
    // selection dragged too far, keeps the dialog open.
    let pressed_backdrop = use_mut_ref(|| false);
    let onmousedown = {
        let pressed_backdrop = pressed_backdrop.clone();
        Callback::from(move |event: MouseEvent| {
            *pressed_backdrop.borrow_mut() = is_backdrop(&event);
        })
    };
    let onclick = {
        let dialog_ref = dialog_ref.clone();
        Callback::from(move |event: MouseEvent| {
            if *pressed_backdrop.borrow() && is_backdrop(&event) {
                close_dialog(&dialog_ref);
            }
        })
    };
    let onclose = props.onclose.reform(|_: Event| ());

    let origin = get_api_base_url();
    let run = props.run;
    let options = &props.options;
    html! {
        <dialog
            ref={dialog_ref}
            class="embed-dialog"
            aria-labelledby={TITLE_ID}
            {onclose}
            {onmousedown}
            {onclick}
        >
            <div class="embed-dialog-frame">
                <header class="embed-dialog-head">
                    <div>
                        <h2 id={TITLE_ID}>{"Embed a chart"}</h2>
                        <p>{"Show a chart of this run on another page. The preview loads the \
                             same link as the code."}</p>
                    </div>
                    <button
                        type="button"
                        class="embed-dialog-close"
                        aria-label="Close"
                        title="Close"
                        onclick={close}
                    >
                        { close_icon() }
                    </button>
                </header>
                <div class="embed-dialog-body">
                    <div class="embed-main">
                        <Preview {run} options={options.clone()} origin={origin.clone()} />
                        <CodePanel
                            {run}
                            options={options.clone()}
                            {origin}
                            onchange={props.onchange.clone()}
                        />
                    </div>
                    <Controls
                        charts={props.charts.clone()}
                        options={options.clone()}
                        onchange={props.onchange.clone()}
                    />
                </div>
            </div>
        </dialog>
    }
}

fn close_dialog(dialog_ref: &NodeRef) {
    if let Some(dialog) = dialog_ref.cast::<HtmlDialogElement>() {
        dialog.close();
    }
}

/// A click on the backdrop has the dialog as its target, because the frame inside fills the
/// dialog box.
fn is_backdrop(event: &MouseEvent) -> bool {
    event.target_dyn_into::<HtmlDialogElement>().is_some()
}

#[derive(Properties, PartialEq)]
struct ControlsProps {
    charts: Rc<[EmbedChart]>,
    options: EmbedOptions,
    onchange: Callback<EmbedOptions>,
}

#[function_component(Controls)]
fn controls(props: &ControlsProps) -> Html {
    let options = &props.options;
    let onchange = &props.onchange;
    let image = options.format == EmbedFormat::Image;

    let charts = props.charts.iter().map(|&chart| {
        let pressed = options.chart == chart;
        let onclick = change(options, onchange, move |next| next.chart = chart);
        html! {
            <button
                type="button"
                class="embed-choice"
                aria-pressed={pressed.to_string()}
                autofocus={pressed}
                {onclick}
            >
                { chart.label() }
            </button>
        }
    });
    let formats = EmbedFormat::ALL.map(|format| {
        choice(
            format.label(),
            options.format == format,
            change(options, onchange, move |next| next.format = format),
        )
    });
    let themes = EmbedTheme::ALL.map(|theme| {
        choice(
            theme.label(),
            options.theme == theme,
            change(options, onchange, move |next| next.theme = theme),
        )
    });
    let backgrounds = [("Solid", false), ("Transparent", true)].map(|(label, transparent)| {
        choice(
            label,
            options.transparent == transparent,
            change(options, onchange, move |next| {
                next.transparent = transparent
            }),
        )
    });
    let legends = [("Show", true), ("Hide", false)].map(|(label, legend)| {
        choice(
            label,
            options.legend == legend,
            change(options, onchange, move |next| next.legend = legend),
        )
    });

    html! {
        <div class="embed-controls">
            <fieldset class="embed-field">
                <legend>{"Chart"}</legend>
                <div class="embed-choices">{ for charts }</div>
                <p class="embed-note">{ options.chart.description() }</p>
            </fieldset>
            <fieldset class="embed-field">
                <legend>{"Format"}</legend>
                <div class="embed-choices">{ for formats }</div>
                <p class="embed-note">{ options.format.description() }</p>
            </fieldset>
            <fieldset class="embed-field">
                <legend>{"Theme"}</legend>
                <div class="embed-choices">{ for themes }</div>
                <p class="embed-note">{"Pick the theme of the page that shows the chart."}</p>
            </fieldset>
            if image {
                <fieldset class="embed-field">
                    <legend>{"Background"}</legend>
                    <div class="embed-choices">{ for backgrounds }</div>
                </fieldset>
                <fieldset class="embed-field">
                    <legend>{"Legend"}</legend>
                    <div class="embed-choices">{ for legends }</div>
                </fieldset>
                <ImageSizeField options={options.clone()} onchange={props.onchange.clone()} />
            } else {
                <FrameSizeField options={options.clone()} onchange={props.onchange.clone()} />
            }
        </div>
    }
}

/// A click that sends `options` with one choice changed.
fn change(
    options: &EmbedOptions,
    onchange: &Callback<EmbedOptions>,
    apply: impl Fn(&mut EmbedOptions) + 'static,
) -> Callback<MouseEvent> {
    let options = options.clone();
    let onchange = onchange.clone();
    Callback::from(move |_: MouseEvent| {
        let mut next = options.clone();
        apply(&mut next);
        onchange.emit(next);
    })
}

fn choice(label: impl Into<AttrValue>, pressed: bool, onclick: Callback<MouseEvent>) -> Html {
    let label: AttrValue = label.into();
    html! {
        <button type="button" class="embed-choice" aria-pressed={pressed.to_string()} {onclick}>
            { label }
        </button>
    }
}

#[derive(Properties, PartialEq)]
struct SizeFieldProps {
    options: EmbedOptions,
    onchange: Callback<EmbedOptions>,
}

#[function_component(ImageSizeField)]
fn image_size_field(props: &SizeFieldProps) -> Html {
    let options = &props.options;
    let presets = PngSize::ALL.map(|image| {
        choice(
            Size::from(image).to_string(),
            options.image == image,
            change(options, &props.onchange, move |next| next.image = image),
        )
    });
    html! {
        <fieldset class="embed-field">
            <legend>{"Image size"}</legend>
            <div class="embed-choices">{ for presets }</div>
            <p class="embed-note">{ IMAGE_SIZE_NOTE }</p>
        </fieldset>
    }
}

#[function_component(FrameSizeField)]
fn frame_size_field(props: &SizeFieldProps) -> Html {
    let options = &props.options;
    let custom = use_state_eq(|| !FRAME_SIZES.contains(&options.frame));
    let emit = {
        let options = options.clone();
        let onchange = props.onchange.clone();
        Callback::from(move |frame: FrameSize| {
            onchange.emit(EmbedOptions {
                frame,
                ..options.clone()
            });
        })
    };
    let presets = FRAME_SIZES.map(|frame| {
        let custom = custom.clone();
        let emit = emit.clone();
        choice(
            frame.to_string(),
            !*custom && options.frame == frame,
            Callback::from(move |_: MouseEvent| {
                custom.set(false);
                emit.emit(frame);
            }),
        )
    });
    // A custom frame has a width in pixels, so a full width frame starts from the default width.
    let custom_size = match options.frame.width {
        FrameWidth::Pixels(width) => Size::new(width, options.frame.height),
        FrameWidth::Full => Size::new(DEFAULT_FRAME_WIDTH, options.frame.height),
    };
    let on_custom = {
        let custom = custom.clone();
        let emit = emit.clone();
        Callback::from(move |_: MouseEvent| {
            custom.set(true);
            emit.emit(FrameSize::pixels(custom_size.width, custom_size.height));
        })
    };
    html! {
        <fieldset class="embed-field">
            <legend>{"Frame size"}</legend>
            <div class="embed-choices">
                { for presets }
                { choice("Custom", *custom, on_custom) }
            </div>
            if *custom {
                <CustomSize
                    size={custom_size}
                    onchange={emit.reform(|size: Size| FrameSize::pixels(size.width, size.height))}
                />
            }
            <p class="embed-note">{ FRAME_NOTE }</p>
        </fieldset>
    }
}

#[derive(Properties, PartialEq)]
struct CustomSizeProps {
    size: Size,
    onchange: Callback<Size>,
}

/// Two number fields that send a drawable size a moment after the last key, or at once when a
/// field loses focus or gets Enter.
#[function_component(CustomSize)]
fn custom_size(props: &CustomSizeProps) -> Html {
    let width = use_state(|| props.size.width.to_string());
    let height = use_state(|| props.size.height.to_string());
    let pending = use_mut_ref(|| None::<Timeout>);
    let parsed = parse_size(&width, &height);
    let commit_now = {
        let pending = pending.clone();
        let onchange = props.onchange.clone();
        let current = props.size;
        Callback::from(move |size: Option<Size>| {
            pending.borrow_mut().take();
            if let Some(size) = size.filter(|size| *size != current) {
                onchange.emit(size);
            }
        })
    };
    let oninput = |field: UseStateHandle<String>, other: UseStateHandle<String>, is_width: bool| {
        let pending = pending.clone();
        let commit_now = commit_now.clone();
        Callback::from(move |event: InputEvent| {
            let Some(input) = event.target_dyn_into::<HtmlInputElement>() else {
                return;
            };
            let text = input.value();
            let size = if is_width {
                parse_size(&text, &other)
            } else {
                parse_size(&other, &text)
            };
            field.set(text);
            let commit_now = commit_now.clone();
            *pending.borrow_mut() = size
                .map(|size| Timeout::new(SIZE_INPUT_DELAY_MS, move || commit_now.emit(Some(size))));
        })
    };
    let on_width = oninput(width.clone(), height.clone(), true);
    let on_height = oninput(height.clone(), width.clone(), false);
    let onblur = {
        let commit_now = commit_now.clone();
        Callback::from(move |_: FocusEvent| commit_now.emit(parsed))
    };
    let onkeydown = Callback::from(move |event: KeyboardEvent| {
        if event.key() == "Enter" {
            commit_now.emit(parsed);
        }
    });
    let invalid = parsed.is_none();
    let field = |label: &'static str, value: &str, oninput: Callback<InputEvent>| {
        html! {
            <label>
                <span>{ label }</span>
                <input
                    type="number"
                    inputmode="numeric"
                    min="1"
                    max={MAX_SIDE.to_string()}
                    step="1"
                    value={value.to_owned()}
                    aria-invalid={invalid.to_string()}
                    {oninput}
                    onblur={onblur.clone()}
                    onkeydown={onkeydown.clone()}
                />
            </label>
        }
    };
    html! {
        <>
            <div class="embed-custom-size">
                { field("Width", &width, on_width) }
                <span class="embed-custom-times" aria-hidden="true">{"×"}</span>
                { field("Height", &height, on_height) }
                <span class="embed-custom-unit">{"px"}</span>
            </div>
            if invalid {
                <p class="embed-note embed-note-error" role="alert">
                    { format!("Width and height go from 1 to {MAX_SIDE} pixels.") }
                </p>
            }
        </>
    }
}

fn parse_size(width: &str, height: &str) -> Option<Size> {
    let size = Size::new(width.trim().parse().ok()?, height.trim().parse().ok()?);
    size.is_in_range().then_some(size)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoadState {
    Loading,
    Ready,
    Failed,
}

#[derive(Properties, PartialEq)]
struct PreviewProps {
    run: EmbedRun,
    options: EmbedOptions,
    origin: String,
}

/// The embed at its real size, scaled down to fit, so its text and legend look as they will on
/// the other page.
#[function_component(Preview)]
fn preview(props: &PreviewProps) -> Html {
    let stage_ref = use_node_ref();
    let (stage_width, stage_height) = use_size(stage_ref.clone());
    let room = Size::new(
        stage_width.saturating_sub(STAGE_MARGIN),
        stage_height.saturating_sub(STAGE_MARGIN),
    );
    let loaded = use_state_eq(|| None::<String>);
    let failed = use_state_eq(|| None::<String>);

    let options = &props.options;
    let url = options.url(props.run, &props.origin);
    let state = if loaded.as_deref() == Some(url.as_str()) {
        LoadState::Ready
    } else if failed.as_deref() == Some(url.as_str()) {
        LoadState::Failed
    } else {
        LoadState::Loading
    };
    let onload = {
        let loaded = loaded.clone();
        Callback::from(move |event: Event| loaded.set(source(&event)))
    };
    let onerror = {
        let failed = failed.clone();
        Callback::from(move |event: Event| failed.set(source(&event)))
    };

    let image = options.format == EmbedFormat::Image;
    let (drawn, scale) = if image {
        let size = Size::from(options.image);
        (size, size.fit_scale(room))
    } else {
        options.frame.preview(room)
    };
    let box_style = format!(
        "width: {}px; height: {}px",
        (f64::from(drawn.width) * scale).round(),
        (f64::from(drawn.height) * scale).round()
    );
    let content = if image {
        html! {
            <img
                src={url.clone()}
                alt={format!("Preview of the {} image", options.chart.label().to_lowercase())}
                width={drawn.width.to_string()}
                height={drawn.height.to_string()}
                {onload}
                {onerror}
            />
        }
    } else {
        // A new frame for each link, because a frame that changes its link adds an entry to the
        // history of the page, and Back would then step through the previews.
        html! {
            <iframe
                key={url.clone()}
                src={url.clone()}
                title={format!("Preview of the {} chart", options.chart.label().to_lowercase())}
                width={drawn.width.to_string()}
                height={drawn.height.to_string()}
                style={format!("transform: scale({scale})")}
                {onload}
            />
        }
    };
    let loading_text = if image {
        "Drawing the image on the server"
    } else {
        "Loading the chart"
    };
    let percent = (scale * 100.0).round();
    let caption = match (image, options.frame.width) {
        (true, _) => format!("{drawn} PNG, shown at {percent}%"),
        (false, FrameWidth::Pixels(_)) => format!("{} frame, shown at {percent}%", options.frame),
        (false, FrameWidth::Full) => format!(
            "Full width frame, {} px high, shown at {percent}%",
            options.frame.height
        ),
    };
    // A transparent image shows on a page of its theme, so the preview puts it on one.
    let checker = (image && options.transparent).then_some(match options.theme {
        EmbedTheme::Dark => "embed-stage-checker-dark",
        EmbedTheme::Light => "embed-stage-checker-light",
    });
    html! {
        <div class="embed-preview">
            <div
                ref={stage_ref}
                class={classes!("embed-stage", checker)}
                aria-busy={(state == LoadState::Loading).to_string()}
            >
                <div class="embed-preview-box" style={box_style}>{ content }</div>
                if state == LoadState::Loading {
                    <div class="embed-stage-status">
                        <span class="embed-spinner" aria-hidden="true"></span>
                        <span>{ loading_text }</span>
                    </div>
                } else if state == LoadState::Failed {
                    <div class="embed-stage-status embed-stage-failed" role="alert">
                        <span>{"The server could not draw this chart."}</span>
                    </div>
                }
            </div>
            <p class="embed-preview-caption">{ caption }</p>
        </div>
    }
}

/// The `src` attribute of the image or frame that sent `event`, as the dialog wrote it.
fn source(event: &Event) -> Option<String> {
    event.target_dyn_into::<Element>()?.get_attribute("src")
}

#[derive(Properties, PartialEq)]
struct CodePanelProps {
    run: EmbedRun,
    options: EmbedOptions,
    origin: String,
    onchange: Callback<EmbedOptions>,
}

#[function_component(CodePanel)]
fn code_panel(props: &CodePanelProps) -> Html {
    let (copy_state, copy) = use_copy();
    let options = &props.options;
    let run = props.run;
    let code = options.code(run, &props.origin);
    let url = options.url(run, &props.origin);
    let current = options.code_kind();
    let kinds = options.format.code_kinds().iter().map(|&kind| {
        choice(
            kind.label(),
            kind == current,
            change(options, &props.onchange, move |next| next.code = kind),
        )
    });
    let on_copy = {
        let code = code.clone();
        copy.reform(move |_: MouseEvent| AttrValue::from(code.clone()))
    };
    let copy_label = match copy_state {
        CopyState::Idle => "Copy code",
        CopyState::Copied => "Copied",
        CopyState::Failed => "Copy failed",
    };
    let code_note = match (options.format, current) {
        (EmbedFormat::Image, CodeKind::Markdown | CodeKind::Html) => {
            "The image links to this run's page."
        }
        (EmbedFormat::Image, CodeKind::Link) => "The link of the PNG.",
        (EmbedFormat::Interactive, CodeKind::Link) => "The link of the chart page.",
        (EmbedFormat::Interactive, _) => "Paste it where the page allows HTML.",
    };
    html! {
        <section class="embed-code" aria-label="Code">
            <div class="embed-code-head">
                <div class="embed-choices" role="group" aria-label="Kind of code">
                    { for kinds }
                </div>
                <span class="embed-note">{ code_note }</span>
            </div>
            <pre class="embed-code-text"><code>{ code }</code></pre>
            <div class="embed-actions">
                <button
                    type="button"
                    class={classes!(
                        "embed-copy",
                        (copy_state == CopyState::Copied).then_some("copied"),
                    )}
                    aria-live="polite"
                    onclick={on_copy}
                >
                    { copy_label }
                </button>
                <a class="run-action" href={url.clone()} target="_blank" rel="noopener">
                    {"Open in a new tab"}
                </a>
                if options.format == EmbedFormat::Image {
                    <a class="run-action" href={url} download={options.download_name(run)}>
                        {"Download PNG"}
                    </a>
                }
            </div>
        </section>
    }
}

fn close_icon() -> Html {
    html! {
        <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24"
             fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"
             stroke-linejoin="round" aria-hidden="true">
            <line x1="18" y1="6" x2="6" y2="18" />
            <line x1="6" y1="6" x2="18" y2="18" />
        </svg>
    }
}
