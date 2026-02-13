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

use rand::distr::Alphanumeric;
use rand::{Rng, RngExt};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

const RANDOM_SUFFIX_LEN: usize = 8;

/// Resource name that is guaranteed to start with a configurable prefix.
///
/// This prevents chaos tests from accidentally operating on non-lab resources.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SafeResourceName(String);

impl SafeResourceName {
    pub fn new(prefix: &str, name: &str) -> Self {
        let full = if name.starts_with(prefix) {
            name.to_owned()
        } else {
            format!("{prefix}{name}")
        };
        Self(full)
    }

    pub fn random(prefix: &str, rng: &mut impl Rng) -> Self {
        let suffix: String = (0..RANDOM_SUFFIX_LEN)
            .map(|_| rng.sample(Alphanumeric) as char)
            .collect();
        Self(format!("{prefix}{suffix}"))
    }

    pub fn has_prefix(&self, prefix: &str) -> bool {
        self.0.starts_with(prefix)
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl Deref for SafeResourceName {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SafeResourceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<SafeResourceName> for String {
    fn from(name: SafeResourceName) -> Self {
        name.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn new_prepends_prefix_when_missing() {
        let name = SafeResourceName::new("lab-", "stream1");
        assert_eq!(&*name, "lab-stream1");
    }

    #[test]
    fn new_preserves_existing_prefix() {
        let name = SafeResourceName::new("lab-", "lab-stream1");
        assert_eq!(&*name, "lab-stream1");
    }

    #[test]
    fn random_produces_prefixed_name() {
        let mut rng = StdRng::seed_from_u64(42);
        let name = SafeResourceName::random("lab-", &mut rng);
        assert!(name.starts_with("lab-"));
        assert_eq!(name.len(), 4 + RANDOM_SUFFIX_LEN);
    }

    #[test]
    fn random_is_deterministic_with_same_seed() {
        let mut rng1 = StdRng::seed_from_u64(99);
        let mut rng2 = StdRng::seed_from_u64(99);
        let a = SafeResourceName::random("lab-", &mut rng1);
        let b = SafeResourceName::random("lab-", &mut rng2);
        assert_eq!(a, b);
    }
}
