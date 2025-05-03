/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::str::FromStr;
use strum::Display;

#[serde_as]
#[derive(Debug, Serialize, Display, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CacheIndexesConfig {
    All,
    #[default]
    #[serde(rename = "open_segment")]
    OpenSegment,
    None,
}

impl<'de> Deserialize<'de> for CacheIndexesConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum StringOrBool {
            String(String),
            Bool(bool),
        }

        let value = StringOrBool::deserialize(deserializer)?;

        match value {
            StringOrBool::String(s) => match s.to_lowercase().as_str() {
                "all" | "true" => Ok(CacheIndexesConfig::All),
                "open_segment" => Ok(CacheIndexesConfig::OpenSegment),
                "none" | "false" => Ok(CacheIndexesConfig::None),
                _ => Err(serde::de::Error::custom(format!(
                    "Invalid CacheIndexesConfig value: {}",
                    s
                ))),
            },
            StringOrBool::Bool(b) => {
                if b {
                    Ok(CacheIndexesConfig::All)
                } else {
                    Ok(CacheIndexesConfig::None)
                }
            }
        }
    }
}

impl FromStr for CacheIndexesConfig {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "all" | "true" => Ok(CacheIndexesConfig::All),
            "open_segment" => Ok(CacheIndexesConfig::OpenSegment),
            "none" | "false" => Ok(CacheIndexesConfig::None),
            _ => Err(format!("Invalid CacheIndexesConfig: {}", s)),
        }
    }
}

impl From<bool> for CacheIndexesConfig {
    fn from(value: bool) -> Self {
        if value {
            CacheIndexesConfig::All
        } else {
            CacheIndexesConfig::None
        }
    }
}
