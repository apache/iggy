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

//! A sticky row of buttons that scroll to a page's sections and mark the one in view. They're
//! buttons and not `#id` links, because the page base is `/` and `#id` would leave the page.

use gloo::events::EventListener;
use web_sys::Element;
use yew::prelude::*;

/// A section counts as reached a little before its top touches the nav, so the section that a
/// button scrolled to is marked when the scroll ends.
const ACTIVATION_GAP_PX: f64 = 48.0;
/// Zoomed pages scroll by fractions of a pixel, so the end of the page is a small band.
const END_TOLERANCE_PX: f64 = 2.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NavSection {
    /// The `id` of the section element.
    pub id: &'static str,
    pub label: &'static str,
}

#[derive(Properties, PartialEq)]
pub struct SectionNavProps {
    pub sections: &'static [NavSection],
    /// The element that scrolls the sections.
    pub scroll_ref: NodeRef,
}

#[function_component(SectionNav)]
pub fn section_nav(props: &SectionNavProps) -> Html {
    let active = use_state_eq(|| 0);
    let nav_ref = use_node_ref();
    {
        let active = active.clone();
        let nav_ref = nav_ref.clone();
        let sections = props.sections;
        use_effect_with(props.scroll_ref.clone(), move |scroll_ref| {
            let listener = scroll_ref.cast::<Element>().map(|container| {
                let update = move |container: &Element| {
                    if let Some(index) = current_section(container, &nav_ref, sections) {
                        active.set(index);
                    }
                };
                update(&container);
                EventListener::new(&container.clone(), "scroll", move |_| update(&container))
            });
            move || drop(listener)
        });
    }

    html! {
        <nav class="section-nav" aria-label="On this page" ref={nav_ref.clone()}>
            <div class="page-container section-nav-row">
                { for props.sections.iter().enumerate().map(|(index, section)| {
                    let is_active = *active == index;
                    let onclick = {
                        let active = active.clone();
                        let nav_ref = nav_ref.clone();
                        Callback::from(move |_| {
                            if let Some(target) = nav_ref
                                .cast::<Element>()
                                .and_then(|nav| nav.owner_document())
                                .and_then(|document| document.get_element_by_id(section.id))
                            {
                                target.scroll_into_view();
                            }
                            active.set(index);
                        })
                    };
                    html! {
                        <button
                            type="button"
                            class={classes!("section-nav-link", is_active.then_some("active"))}
                            aria-current={is_active.then_some("true")}
                            {onclick}
                        >
                            { section.label }
                        </button>
                    }
                }) }
            </div>
        </nav>
    }
}

fn current_section(container: &Element, nav: &NodeRef, sections: &[NavSection]) -> Option<usize> {
    let document = container.owner_document()?;
    let line = nav.cast::<Element>()?.get_bounding_client_rect().bottom() + ACTIVATION_GAP_PX;
    let tops = sections
        .iter()
        .map(|section| {
            document
                .get_element_by_id(section.id)
                .map(|element| element.get_bounding_client_rect().top())
        })
        .collect::<Option<Vec<_>>>()?;
    let seen = f64::from(container.scroll_top()) + f64::from(container.client_height());
    let at_end = seen >= f64::from(container.scroll_height()) - END_TOLERANCE_PX;
    active_section(&tops, line, at_end)
}

/// The last section whose top crossed the line, or the first one before that. At the end of the
/// page the last section wins, because a short last section never reaches the line.
fn active_section(tops: &[f64], line: f64, at_end: bool) -> Option<usize> {
    let last = tops.len().checked_sub(1)?;
    if at_end {
        return Some(last);
    }
    Some(tops.iter().rposition(|&top| top <= line).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    const LINE: f64 = 100.0;
    const TOPS: [f64; 3] = [40.0, 90.0, 400.0];

    #[test]
    fn given_sections_below_line_when_picked_should_mark_first() {
        assert_eq!(active_section(&[150.0, 600.0], LINE, false), Some(0));
    }

    #[test]
    fn given_two_sections_above_line_when_picked_should_mark_lower_one() {
        assert_eq!(active_section(&TOPS, LINE, false), Some(1));
    }

    #[test]
    fn given_top_on_line_when_picked_should_mark_it() {
        assert_eq!(active_section(&[0.0, LINE, 300.0], LINE, false), Some(1));
    }

    #[test]
    fn given_end_of_page_when_picked_should_mark_last_section() {
        assert_eq!(active_section(&TOPS, LINE, true), Some(2));
    }

    #[test]
    fn given_no_sections_when_picked_should_mark_none() {
        assert_eq!(active_section(&[], LINE, true), None);
    }
}
