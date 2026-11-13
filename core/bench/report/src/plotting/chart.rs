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

use charming::{
    Chart,
    component::{
        Axis, DataView, DataZoom, DataZoomType, Feature, Grid, Legend, LegendSelectedMode,
        LegendType, Restore, SaveAsImage, Title, Toolbox, ToolboxDataZoom,
    },
    element::{
        AxisLabel, AxisPointer, AxisPointerType, AxisType, Emphasis, ItemStyle, LineStyle,
        NameLocation, Orient, SplitLine, TextAlign, TextStyle, Tooltip, Trigger,
    },
    series::{Bar, Line},
};

pub struct IggyChart {
    pub inner: Chart,
}

const AXIS_TEXT_SIZE: u32 = 16;
const LEGEND_TEXT_SIZE: u32 = 12;
/// Room for the y axis labels and the y axis name.
const GRID_LEFT: i32 = 70;
/// Margins of a card chart in pixels: the legend above the grid, the x axis labels below it.
const CARD_GRID_TOP: i32 = 40;
const CARD_GRID_RIGHT: i32 = 24;
const CARD_GRID_BOTTOM: i32 = 32;
/// Room under the grid of a zoomable card chart for the x axis labels and the slider below them.
const ZOOM_CARD_GRID_BOTTOM: i32 = 76;
const ZOOM_SLIDER_BOTTOM: i32 = 8;
const TRANSPARENT: &str = "transparent";

impl IggyChart {
    /// Create a new `IggyChart` with default tooltip, legend, grid, and toolbox.
    pub fn new(title: &str, subtext: &str, dark: bool, strip_title_and_subtext: bool) -> Self {
        let chart = Chart::new();
        let chart = if !strip_title_and_subtext {
            chart.title(
                Title::new()
                    .text(title)
                    .text_align(TextAlign::Center)
                    .subtext(subtext)
                    .text_style(TextStyle::new().font_size(24).font_weight("bold"))
                    .subtext_style(TextStyle::new().font_size(14).line_height(20))
                    .left("50%")
                    .top("1%"),
            )
        } else {
            chart
        };
        let grid_top = if !strip_title_and_subtext {
            "16%"
        } else {
            "4%"
        };

        let chart = chart
            .tooltip(Tooltip::new().axis_pointer(AxisPointer::new().type_(AxisPointerType::Cross)))
            .legend(
                Legend::new()
                    .show(true)
                    .right("2%")
                    .top("middle")
                    .orient(Orient::Vertical)
                    .selected_mode(LegendSelectedMode::Multiple)
                    .text_style(TextStyle::new().font_size(LEGEND_TEXT_SIZE))
                    .padding(10)
                    .item_gap(10)
                    .item_width(25)
                    .item_height(14)
                    .type_(LegendType::Scroll),
            )
            .grid(
                Grid::new()
                    .left(GRID_LEFT)
                    .right("20%")
                    .top(grid_top)
                    .bottom("8%"),
            )
            .data_zoom(
                DataZoom::new()
                    .show(true)
                    .type_(DataZoomType::Slider)
                    .bottom("2%")
                    .start(0)
                    .end(100),
            )
            .toolbox(
                Toolbox::new().feature(
                    Feature::new()
                        .data_zoom(ToolboxDataZoom::new())
                        .data_view(DataView::new())
                        .restore(Restore::new())
                        .save_as_image(SaveAsImage::new()),
                ),
            );

        let chart = if dark {
            chart.background_color("#070C18")
        } else {
            chart
        };

        Self { inner: chart }
    }

    /// A chart for a card on a page: no title, no toolbox and no zoom slider, the legend above the
    /// grid and margins in pixels, so a short chart keeps its axis labels clear. The card draws the
    /// background.
    pub fn card() -> Self {
        Self::card_with_grid_bottom(CARD_GRID_BOTTOM)
    }

    /// A card chart with a zoom slider under the x axis, so a reader can narrow a long run down to
    /// one peak. The slider takes no mouse wheel events, so the page still scrolls over the chart.
    pub fn zoomable_card() -> Self {
        let chart = Self::card_with_grid_bottom(ZOOM_CARD_GRID_BOTTOM)
            .inner
            .data_zoom(
                DataZoom::new()
                    .type_(DataZoomType::Slider)
                    .bottom(ZOOM_SLIDER_BOTTOM)
                    .start(0)
                    .end(100),
            );
        Self { inner: chart }
    }

    fn card_with_grid_bottom(grid_bottom: i32) -> Self {
        let chart = Chart::new()
            .background_color(TRANSPARENT)
            .tooltip(Tooltip::new().trigger(Trigger::Axis))
            .legend(
                Legend::new()
                    .show(true)
                    .top(0)
                    .type_(LegendType::Scroll)
                    .text_style(TextStyle::new().font_size(LEGEND_TEXT_SIZE)),
            )
            .grid(
                Grid::new()
                    .left(GRID_LEFT)
                    .right(CARD_GRID_RIGHT)
                    .top(CARD_GRID_TOP)
                    .bottom(grid_bottom),
            );
        Self { inner: chart }
    }

    /// Configure the X axis (time axis).
    pub fn with_time_x_axis(mut self) -> Self {
        self.inner = self.inner.x_axis(
            Axis::new()
                .type_(AxisType::Value)
                .name_location(NameLocation::End)
                .name_gap(15)
                .axis_label(AxisLabel::new().formatter("{value} s"))
                .split_line(SplitLine::new().show(true)),
        );
        self
    }

    /// Configure the X axis (category axis).
    pub fn with_category_x_axis(mut self, axis_label: &str, categories: Vec<String>) -> Self {
        self.inner = self.inner.x_axis(
            Axis::new()
                .type_(AxisType::Category)
                .name(axis_label)
                .name_location(NameLocation::End)
                .name_text_style(TextStyle::new().font_size(AXIS_TEXT_SIZE))
                .name_gap(15)
                .data(categories)
                .split_line(SplitLine::new().show(true)),
        );
        self
    }

    /// Configure a Y axis for e.g. throughput in msg/s or MB/s.
    pub fn with_y_axis(self, axis_label: &str) -> Self {
        self.with_left_y_axis(AxisType::Value, axis_label)
    }

    /// Configure a logarithmic Y axis, for values that span orders of magnitude, such as a run's
    /// latency percentiles.
    pub fn with_log_y_axis(self, axis_label: &str) -> Self {
        self.with_left_y_axis(AxisType::Log, axis_label)
    }

    /// Configure dual Y axes for e.g. throughput in MB/s and msg/s.
    pub fn with_dual_y_axis(mut self, y1_label: &str, y2_label: &str) -> Self {
        // Configure left Y axis (MB/s)
        self.inner = self.inner.y_axis(
            Axis::new()
                .type_(AxisType::Value)
                .name(y1_label)
                .name_location(NameLocation::Middle)
                .name_rotation(90)
                .name_gap(44)
                .name_text_style(TextStyle::new().font_size(AXIS_TEXT_SIZE))
                .position("left")
                .axis_label(AxisLabel::new())
                .split_line(SplitLine::new().show(true)),
        );
        // Configure right Y axis (messages/s)
        self.inner = self.inner.y_axis(
            Axis::new()
                .type_(AxisType::Value)
                .name(y2_label)
                .name_location(NameLocation::Middle)
                .name_rotation(-90)
                .name_text_style(TextStyle::new().font_size(AXIS_TEXT_SIZE))
                .name_gap(54)
                .position("right")
                .axis_label(AxisLabel::new())
                .split_line(SplitLine::new().show(true)),
        );
        self
    }

    /// Add a new line series to the chart.
    ///
    /// `name` is displayed in the legend. `points` is a list of `[x, y]` pairs.
    /// Use `color` if you want a custom color (e.g. `#FF0000`), otherwise pass `None`.
    /// `opacity` controls the line opacity (use e.g. 1.0 for solid line, 0.3 for translucent).
    pub fn add_time_series(
        mut self,
        name: &str,
        points: Vec<Vec<f64>>,
        color: Option<&str>,
        opacity: f64,
    ) -> Self {
        let mut line = Line::new()
            .name(name)
            .data(points)
            .show_symbol(false)
            .emphasis(Emphasis::new())
            .line_style(LineStyle::new().width(2).opacity(opacity));

        if let Some(color) = color {
            line = line.item_style(ItemStyle::new().color(color));
        }

        self.inner = self.inner.series(line);
        self
    }

    /// Add a bar series (for histograms).
    pub fn add_bar_series(mut self, name: &str, data: Vec<f64>, color: &str, opacity: f64) -> Self {
        let bar = Bar::new()
            .name(name)
            .data(data)
            .bar_width("100%")
            .bar_gap("-100%")
            .item_style(ItemStyle::new().color(color).opacity(opacity));

        self.inner = self.inner.series(bar);
        self
    }

    /// Add a smooth line series (for PDF curves).
    pub fn add_smooth_line_series(
        mut self,
        name: &str,
        data: Vec<Vec<f64>>,
        color: &str,
        width: f64,
    ) -> Self {
        let line = Line::new()
            .name(name)
            .data(data)
            .show_symbol(false)
            .smooth(true)
            .z(1)
            .line_style(LineStyle::new().width(width))
            .item_style(ItemStyle::new().color(color));

        self.inner = self.inner.series(line);
        self
    }

    /// Add a new line series to the chart with specified Y axis.
    /// y_axis_index: 0 for left axis, 1 for right axis
    pub fn add_dual_time_line_series(
        mut self,
        name: &str,
        points: Vec<Vec<f64>>,
        color: Option<&str>,
        opacity: f64,
        y_axis_index: usize,
        width: f64,
    ) -> Self {
        let mut line = Line::new()
            .name(name)
            .data(points)
            .show_symbol(false)
            .emphasis(Emphasis::new())
            .line_style(LineStyle::new().width(width).opacity(opacity))
            .y_axis_index(y_axis_index as f64);

        if let Some(color) = color {
            line = line.item_style(ItemStyle::new().color(color));
        }

        self.inner = self.inner.series(line);
        self
    }

    fn with_left_y_axis(mut self, axis_type: AxisType, axis_label: &str) -> Self {
        self.inner = self.inner.y_axis(
            Axis::new()
                .type_(axis_type)
                .name(axis_label)
                .name_location(NameLocation::Middle)
                .name_rotation(90)
                .name_text_style(TextStyle::new().font_size(AXIS_TEXT_SIZE))
                .name_gap(44)
                .position("left")
                .axis_label(AxisLabel::new())
                .split_line(SplitLine::new().show(true)),
        );
        self
    }
}
