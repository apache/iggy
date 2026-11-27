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

//! Run labels that more than one page shows as HTML: the day, the kind family mark and the version
//! tag. The texts behind them are in [`bench_dashboard_shared::labels`].

use bench_dashboard_shared::identity::KindFamily;
use bench_dashboard_shared::labels::DAY_FORMAT;
use chrono::{DateTime, SecondsFormat};
use yew::prelude::*;

pub const END_TO_END_THROUGHPUT_NOTE: &str = "An end-to-end actor sends and polls, so its \
                                              throughput counts both sent and received bytes, just \
                                              like its rate limit.";
pub const NO_RATE_LIMIT_NOTE: &str = "No rate limit, so this run only measures throughput.";

/// A day the way the pages write it, plus the exact time for a `<time>` element.
#[derive(Debug, PartialEq, Eq)]
pub struct DayLabel {
    /// For example `Nov 4, 2026`.
    pub day: String,
    /// RFC 3339 in UTC.
    pub iso: String,
}

impl DayLabel {
    pub fn from_unix_micros(micros: i64) -> Option<Self> {
        let moment = DateTime::from_timestamp_micros(micros)?;
        Some(Self {
            day: moment.format(DAY_FORMAT).to_string(),
            iso: moment.to_rfc3339_opts(SecondsFormat::Secs, true),
        })
    }

    pub fn view(&self) -> Html {
        html! {
            <time datetime={self.iso.clone()} title={self.iso.clone()}>{ self.day.as_str() }</time>
        }
    }
}

/// The version tag, styled as a release or as a build.
pub fn version_tag_view(tag: &str, release: bool) -> Html {
    let look = if release {
        "version-release"
    } else {
        "version-build"
    };
    html! { <span class={classes!("version-tag", look)}>{ tag.to_owned() }</span> }
}

/// A letter and a color per kind family. The name is the label, so the mark reads the same to
/// a screen reader.
pub fn family_mark(family: KindFamily) -> Html {
    let (letters, class) = match family {
        KindFamily::Producer => ("P", "family-producer"),
        KindFamily::Consumer => ("C", "family-consumer"),
        KindFamily::ProducerAndConsumer => ("PC", "family-mixed"),
        KindFamily::EndToEnd => ("E", "family-end-to-end"),
    };
    let name = family.to_string();
    html! {
        <span class={classes!("family-mark", class)} role="img" aria-label={name.clone()} title={name}>
            { letters }
        </span>
    }
}
