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

//! Draws the preview card of a run as a PNG. This module writes the card as SVG, and resvg draws it
//! with the system fonts. The look follows the site card, `assets/og-image.png`.

use std::fmt::Write as _;

use bench_dashboard_shared::card::{CARD_SIZE, CardNumber, RunCard};
use bench_dashboard_shared::html::escape;
use resvg::tiny_skia::{Pixmap, Transform};
use resvg::usvg::{Options, Tree};

/// White text, for the dark background of the card.
const LOGO_SVG: &str = include_str!("../../frontend/assets/iggy-light.svg");
/// The Docker image has the DejaVu fonts only. They are wide, so every line fits its width.
const FONT_FAMILY: &str = "DejaVu Sans, sans-serif";
const BACKGROUND: &str = "#0b1220";
const TEXT: &str = "#f5f5f5";
const SUBTLE: &str = "#c3c9d4";
const MUTED: &str = "#9199a6";
const ACCENT: &str = "#ff9103";
const ACCENT_BAR_HEIGHT: f32 = 10.0;
const MARGIN: f32 = 64.0;
const LOGO_TOP: f32 = 40.0;
const LOGO_HEIGHT: f32 = 84.0;
/// The space between two columns of numbers.
const COLUMN_GAP: f32 = 32.0;
const BOLD: u16 = 700;
const REGULAR: u16 = 400;
/// Wider than any line, so a measured line never wraps or clips.
const MEASURE_WIDTH: u32 = 4000;

/// One line of text: its baseline, size and look.
struct Line<'a> {
    text: &'a str,
    x: f32,
    baseline: f32,
    size: f32,
    weight: u16,
    color: &'static str,
    /// The line shrinks to this width.
    max_width: f32,
    /// Ends at `x` instead of starting there.
    end_anchor: bool,
}

/// The fonts and the logo, loaded once for every card.
pub struct CardRenderer {
    options: Options<'static>,
    logo: Tree,
}

impl CardRenderer {
    pub fn new() -> Result<Self, String> {
        let mut options = Options::default();
        options.fontdb_mut().load_system_fonts();
        let logo = Tree::from_str(LOGO_SVG, &options).map_err(|e| format!("card logo SVG: {e}"))?;
        Ok(Self { options, logo })
    }

    pub fn render(&self, card: &RunCard) -> Result<Vec<u8>, String> {
        let svg = self.svg(card);
        let tree = Tree::from_str(&svg, &self.options).map_err(|e| format!("card SVG: {e}"))?;
        let mut pixmap = Pixmap::new(CARD_SIZE.width(), CARD_SIZE.height())
            .ok_or_else(|| "card size".to_owned())?;
        resvg::render(&tree, Transform::identity(), &mut pixmap.as_mut());
        let scale = LOGO_HEIGHT / self.logo.size().height();
        resvg::render(
            &self.logo,
            Transform::from_scale(scale, scale).post_translate(MARGIN, LOGO_TOP),
            &mut pixmap.as_mut(),
        );
        pixmap
            .encode_png()
            .map_err(|e| format!("card PNG encode: {e}"))
    }

    fn svg(&self, card: &RunCard) -> String {
        let (width, height) = (CARD_SIZE.width() as f32, CARD_SIZE.height() as f32);
        let content_width = width - 2.0 * MARGIN;
        let mut lines = vec![
            Line {
                text: card.source,
                x: width - MARGIN,
                baseline: 92.0,
                size: 28.0,
                weight: REGULAR,
                color: MUTED,
                max_width: content_width / 2.0,
                end_anchor: true,
            },
            Line::start(&card.title, 206.0, 64.0, BOLD, TEXT, content_width),
        ];
        for (text, baseline) in card.lines.iter().zip([258.0, 302.0]) {
            lines.push(Line::start(
                text,
                baseline,
                32.0,
                REGULAR,
                SUBTLE,
                content_width,
            ));
        }
        lines.extend(self.number_lines(&card.numbers, content_width));
        lines.push(Line::start(
            &card.footer,
            584.0,
            26.0,
            REGULAR,
            MUTED,
            content_width,
        ));

        let mut svg = format!(
            r#"<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}"><rect width="{width}" height="{height}" fill="{BACKGROUND}"/><rect y="{}" width="{width}" height="{ACCENT_BAR_HEIGHT}" fill="{ACCENT}"/>"#,
            height - ACCENT_BAR_HEIGHT
        );
        for line in &lines {
            let size = self.fitted_size(line);
            let anchor = if line.end_anchor { "end" } else { "start" };
            let _ = write!(
                svg,
                r#"<text x="{}" y="{}" font-family="{FONT_FAMILY}" font-size="{size:.1}" font-weight="{}" fill="{}" text-anchor="{anchor}">{}</text>"#,
                line.x,
                line.baseline,
                line.weight,
                line.color,
                escape(line.text)
            );
        }
        svg.push_str("</svg>");
        svg
    }

    /// One number fills the row in large type. More numbers share it in columns. The values of one
    /// row have one size, the size that fits the widest of them, and so do the labels and the details.
    fn number_lines<'a>(&self, numbers: &'a [CardNumber], content_width: f32) -> Vec<Line<'a>> {
        let (value_size, value_baseline, detail_baseline) = if numbers.len() == 1 {
            (104.0, 480.0, 530.0)
        } else {
            (60.0, 458.0, 500.0)
        };
        let column = content_width / numbers.len().max(1) as f32;
        let max_width = column - COLUMN_GAP;
        let row = |texts: Vec<&'a str>, baseline, size, weight, color| {
            let lines: Vec<Line<'a>> = texts
                .into_iter()
                .map(|text| Line::start(text, baseline, size, weight, color, max_width))
                .collect();
            let size = lines
                .iter()
                .map(|line| self.fitted_size(line))
                .fold(size, f32::min);
            lines
                .into_iter()
                .map(|line| Line { size, ..line })
                .collect::<Vec<_>>()
        };
        let labels = row(
            numbers.iter().map(|number| number.label).collect(),
            384.0,
            26.0,
            BOLD,
            ACCENT,
        );
        let values = row(
            numbers.iter().map(|number| number.value.as_str()).collect(),
            value_baseline,
            value_size,
            BOLD,
            TEXT,
        );
        let details = row(
            numbers
                .iter()
                .map(|number| number.detail.as_deref().unwrap_or_default())
                .collect(),
            detail_baseline,
            28.0,
            REGULAR,
            SUBTLE,
        );
        labels
            .into_iter()
            .zip(values)
            .zip(details)
            .enumerate()
            .flat_map(|(index, ((label, value), detail))| {
                let x = MARGIN + index as f32 * column;
                [label, value, detail].map(|line| Line { x, ..line })
            })
            .filter(|line| !line.text.is_empty())
            .collect()
    }

    /// The size of the line, smaller when the line is wider than its space. SVG text does not
    /// wrap, so each line shrinks to fit instead.
    fn fitted_size(&self, line: &Line<'_>) -> f32 {
        let width = self.text_width(line.text, line.size, line.weight);
        if width > line.max_width {
            (line.size * line.max_width / width).floor()
        } else {
            line.size
        }
    }

    /// 0 when no font draws the text.
    fn text_width(&self, text: &str, size: f32, weight: u16) -> f32 {
        let svg = format!(
            r#"<svg xmlns="http://www.w3.org/2000/svg" width="{MEASURE_WIDTH}" height="{MEASURE_WIDTH}"><text x="0" y="{size}" font-family="{FONT_FAMILY}" font-size="{size}" font-weight="{weight}">{}</text></svg>"#,
            escape(text)
        );
        Tree::from_str(&svg, &self.options)
            .map(|tree| tree.root().abs_bounding_box().width())
            .unwrap_or_default()
    }
}

impl<'a> Line<'a> {
    fn start(
        text: &'a str,
        baseline: f32,
        size: f32,
        weight: u16,
        color: &'static str,
        max_width: f32,
    ) -> Self {
        Self {
            text,
            x: MARGIN,
            baseline,
            size,
            weight,
            color,
            max_width,
            end_anchor: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn card(numbers: usize) -> RunCard {
        RunCard {
            source: "iggy-bench",
            title: "End-to-end producing consumer group & <friends>".repeat(3),
            lines: vec!["20 producing consumers · 3 nodes".to_owned()],
            numbers: (0..numbers)
                .map(|index| CardNumber {
                    label: "P99",
                    value: format!("{index}.00 ms"),
                    detail: Some("150.2k msg/s".to_owned()),
                })
                .collect(),
            footer: "Apache Iggy 0.10.0 · AWS i4i.4xlarge · Nov 16, 2026".to_owned(),
        }
    }

    #[test]
    fn given_cards_with_any_number_count_when_rendering_should_give_png_of_card_size() {
        let renderer = CardRenderer::new().expect("the logo parses");

        for numbers in [0, 1, 4] {
            let png = renderer.render(&card(numbers)).expect("the card renders");
            let image = Pixmap::decode_png(&png).expect("the card is a PNG");

            assert_eq!(
                (image.width(), image.height()),
                (CARD_SIZE.width(), CARD_SIZE.height()),
                "{numbers} numbers"
            );
        }
    }

    #[test]
    fn given_line_wider_than_its_space_when_fitting_should_shrink_it_into_the_space() {
        let renderer = CardRenderer::new().expect("the logo parses");
        let title = card(0).title;
        let line = Line::start(&title, 206.0, 64.0, BOLD, TEXT, 1072.0);

        let size = renderer.fitted_size(&line);

        // A host without fonts measures every line as 0 wide and keeps its size.
        if renderer.text_width(&title, 64.0, BOLD) > 0.0 {
            assert!(size < 64.0);
            assert!(renderer.text_width(&title, size, BOLD) <= 1072.0);
        }
    }
}
