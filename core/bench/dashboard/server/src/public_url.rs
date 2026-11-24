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

use serde::Deserialize;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;
use url::Url;

/// The address at which readers open the dashboard, for example
/// `https://benchmarks.iggy.apache.org`. The sitemap and `robots.txt` need absolute links, and the
/// `Host` header of a request can name any host, so the links come from this value only.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub struct PublicUrl(String);

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PublicUrlError {
    #[error("'{text}' is not a URL: {source}")]
    NotUrl {
        text: String,
        source: url::ParseError,
    },
    #[error("'{0}' must start with http:// or https://")]
    Scheme(String),
    #[error(
        "'{0}' must name only the scheme, the host and the port, without a path, a query, a fragment or a user"
    )]
    NotOrigin(String),
}

impl PublicUrl {
    /// The absolute link to `path`, which starts with `/`.
    pub fn join(&self, path: &str) -> String {
        format!("{}{path}", self.0)
    }
}

impl FromStr for PublicUrl {
    type Err = PublicUrlError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(text).map_err(|source| PublicUrlError::NotUrl {
            text: text.to_owned(),
            source,
        })?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(PublicUrlError::Scheme(text.to_owned()));
        }
        let origin_only = url.host().is_some()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none()
            && url.username().is_empty()
            && url.password().is_none();
        if !origin_only {
            return Err(PublicUrlError::NotOrigin(text.to_owned()));
        }
        Ok(Self(url.as_str().trim_end_matches('/').to_owned()))
    }
}

impl TryFrom<String> for PublicUrl {
    type Error = PublicUrlError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        text.parse()
    }
}

impl fmt::Display for PublicUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_origin_when_parsing_should_keep_scheme_host_and_port_without_trailing_slash() {
        for (text, expected) in [
            (
                "https://benchmarks.iggy.apache.org",
                "https://benchmarks.iggy.apache.org",
            ),
            (
                "https://Benchmarks.Iggy.Apache.org/",
                "https://benchmarks.iggy.apache.org",
            ),
            ("http://127.0.0.1:8061", "http://127.0.0.1:8061"),
        ] {
            let url: PublicUrl = text.parse().expect("an origin parses");

            assert_eq!(url.to_string(), expected, "{text}");
        }
    }

    #[test]
    fn given_origin_when_joining_path_should_write_absolute_link() {
        let url: PublicUrl = "https://benchmarks.iggy.apache.org/".parse().unwrap();

        assert_eq!(
            url.join("/sitemap.xml"),
            "https://benchmarks.iggy.apache.org/sitemap.xml"
        );
    }

    #[test]
    fn given_more_than_origin_when_parsing_should_fail() {
        for text in [
            "https://benchmarks.iggy.apache.org/dashboard",
            "https://benchmarks.iggy.apache.org/?page=1",
            "https://benchmarks.iggy.apache.org/#top",
            "https://reader@benchmarks.iggy.apache.org",
        ] {
            let result = text.parse::<PublicUrl>();

            assert_eq!(
                result,
                Err(PublicUrlError::NotOrigin(text.to_owned())),
                "{text}"
            );
        }
    }

    #[test]
    fn given_other_scheme_or_no_url_when_parsing_should_fail() {
        assert_eq!(
            "ftp://benchmarks.iggy.apache.org".parse::<PublicUrl>(),
            Err(PublicUrlError::Scheme(
                "ftp://benchmarks.iggy.apache.org".to_owned()
            ))
        );
        assert!(matches!(
            "benchmarks.iggy.apache.org".parse::<PublicUrl>(),
            Err(PublicUrlError::NotUrl { .. })
        ));
    }
}
