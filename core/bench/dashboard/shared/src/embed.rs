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

//! The charts that the embed endpoints draw, named as in their `type` query parameter, and the
//! sizes of their PNG files. The server parses these names and sizes, and the run pages write them
//! into the embed links, so a link from a page always names a chart and a size that the server
//! knows.

use std::fmt;
use std::str::FromStr;

use thiserror::Error;

use crate::omb::chart::WindowMetric;

const IGGY_BENCH_NAMES: &str = "'latency', 'throughput', or 'distribution'";
const OMB_NAMES: &str = "'latency', 'end_to_end_latency', 'throughput', 'ladder', or 'quantiles'";

/// A PNG size that the server draws. It caches every PNG that it draws, and a fixed set of sizes
/// keeps the number of files per chart small.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PngSize {
    width: u32,
    height: u32,
}

impl PngSize {
    /// The size of a request that names none.
    pub const DEFAULT: Self = Self {
        width: 1600,
        height: 1200,
    };
    pub const ALL: [Self; 4] = [
        Self::DEFAULT,
        Self {
            width: 1200,
            height: 800,
        },
        Self {
            width: 1920,
            height: 1080,
        },
        Self {
            width: 1200,
            height: 630,
        },
    ];

    pub const fn width(self) -> u32 {
        self.width
    }

    pub const fn height(self) -> u32 {
        self.height
    }
}

impl TryFrom<(u32, u32)> for PngSize {
    type Error = UnknownPngSize;

    fn try_from((width, height): (u32, u32)) -> Result<Self, Self::Error> {
        Self::ALL
            .into_iter()
            .find(|size| size.width == width && size.height == height)
            .ok_or(UnknownPngSize { width, height })
    }
}

/// For example `1600x1200`, as in the names of the cached files.
impl fmt::Display for PngSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}x{}", self.width, self.height)
    }
}

/// A `width` and `height` that is not one of [`PngSize::ALL`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("Invalid size '{width}x{height}'. Must be 1600x1200, 1200x800, 1920x1080, or 1200x630")]
pub struct UnknownPngSize {
    pub width: u32,
    pub height: u32,
}

/// A `type` that names no chart of the run's framework.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("Invalid chart type: '{name}'. Must be {expected}")]
pub struct UnknownChartType {
    pub name: String,
    expected: &'static str,
}

/// The chart of an iggy-bench run that an embed shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChartType {
    /// The latency of every actor, with the moving average of each group.
    Latency,
    /// The throughput of every actor and of each group.
    Throughput,
    /// The latency histogram of each group, with its fitted log-normal curve.
    Distribution,
}

impl ChartType {
    pub const ALL: [Self; 3] = [Self::Latency, Self::Throughput, Self::Distribution];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Latency => "latency",
            Self::Throughput => "throughput",
            Self::Distribution => "distribution",
        }
    }
}

impl FromStr for ChartType {
    type Err = UnknownChartType;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|chart_type| chart_type.name() == name)
            .ok_or_else(|| UnknownChartType {
                name: name.to_owned(),
                expected: IGGY_BENCH_NAMES,
            })
    }
}

impl fmt::Display for ChartType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The chart of an OMB run that an embed shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OmbChartType {
    /// The publish latency P50, P99 and P99.9 of each window.
    Latency,
    /// The end-to-end latency P50, P99 and P99.9 of each window.
    EndToEndLatency,
    /// The publish and consume throughput of each window.
    Throughput,
    /// The publish and end-to-end latency from P50 to the maximum, over the whole run.
    Ladder,
    /// The publish and end-to-end latency at every percentile of the merged histograms.
    Quantiles,
}

impl OmbChartType {
    pub const ALL: [Self; 5] = [
        Self::Latency,
        Self::EndToEndLatency,
        Self::Throughput,
        Self::Ladder,
        Self::Quantiles,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Latency => "latency",
            Self::EndToEndLatency => "end_to_end_latency",
            Self::Throughput => "throughput",
            Self::Ladder => "ladder",
            Self::Quantiles => "quantiles",
        }
    }

    pub const fn title(self) -> &'static str {
        match self {
            Self::Latency => WindowMetric::PublishLatency.title(),
            Self::EndToEndLatency => WindowMetric::EndToEndLatency.title(),
            Self::Throughput => WindowMetric::Throughput.title(),
            Self::Ladder => "Latency ladder",
            Self::Quantiles => "Latency percentiles",
        }
    }

    /// The embed of a chart that the run page draws over the windows. `None` for a window chart
    /// that no embed draws, such as the backlog.
    pub const fn of_window(metric: WindowMetric) -> Option<Self> {
        match metric {
            WindowMetric::PublishLatency => Some(Self::Latency),
            WindowMetric::EndToEndLatency => Some(Self::EndToEndLatency),
            WindowMetric::Throughput => Some(Self::Throughput),
            WindowMetric::PublishDelay | WindowMetric::Backlog | WindowMetric::PublishErrors => {
                None
            }
        }
    }
}

impl FromStr for OmbChartType {
    type Err = UnknownChartType;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|chart_type| chart_type.name() == name)
            .ok_or_else(|| UnknownChartType {
                name: name.to_owned(),
                expected: OMB_NAMES,
            })
    }
}

impl fmt::Display for OmbChartType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `names` as the errors list them, such as `a, b, or c`.
    fn listed(names: &[String]) -> String {
        let (last, others) = names.split_last().expect("a list has names");
        format!("{}, or {last}", others.join(", "))
    }

    fn quoted(name: &str) -> String {
        format!("'{name}'")
    }

    #[test]
    fn given_every_chart_type_when_parsed_from_its_name_should_round_trip() {
        for chart_type in ChartType::ALL {
            assert_eq!(chart_type.to_string().parse(), Ok(chart_type));
        }
        for chart_type in OmbChartType::ALL {
            assert_eq!(chart_type.to_string().parse(), Ok(chart_type));
        }
    }

    #[test]
    fn given_each_size_when_checked_should_accept_it_and_name_it_in_the_error_of_others() {
        for size in PngSize::ALL {
            assert_eq!(PngSize::try_from((size.width(), size.height())), Ok(size));
        }
        let error = PngSize::try_from((800, 600)).expect_err("800x600 is not a preset");
        let names = PngSize::ALL.map(|size| size.to_string());

        assert_eq!(PngSize::try_from((1600, 1080)).ok(), None);
        assert_eq!(
            error.to_string(),
            format!("Invalid size '800x600'. Must be {}", listed(&names))
        );
    }

    #[test]
    fn given_name_of_other_framework_when_parsed_should_list_the_names_of_this_one() {
        let iggy_bench = "ladder".parse::<ChartType>().expect_err("an OMB name");
        let omb = "distribution"
            .parse::<OmbChartType>()
            .expect_err("an iggy-bench name");

        let iggy_bench_names = ChartType::ALL.map(|chart_type| quoted(chart_type.name()));
        let omb_names = OmbChartType::ALL.map(|chart_type| quoted(chart_type.name()));

        assert_eq!(
            iggy_bench.to_string(),
            format!(
                "Invalid chart type: 'ladder'. Must be {}",
                listed(&iggy_bench_names)
            )
        );
        assert_eq!(
            omb.to_string(),
            format!(
                "Invalid chart type: 'distribution'. Must be {}",
                listed(&omb_names)
            )
        );
    }
}
