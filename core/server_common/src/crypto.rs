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

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng},
};
use rand::{RngExt, distr::Alphanumeric};
use std::ops::Range;

/// Argon2-hash a password with a fresh OS-random salt.
///
/// Deliberately one body for every build: the random salt is a security
/// invariant that a cargo feature must not switch off. `server_common` is
/// compiled once per `--workspace`/`--bin` build (feature unification), so a
/// feature-gated alternate body would link into a co-built `iggy-server`. The
/// simulator's deterministic hashing is the additive [`hash_with_fixed_salt`].
#[must_use]
pub fn hash_password(password: &str) -> String {
    hash_with_salt(password, &SaltString::generate(&mut OsRng))
}

/// SIMULATOR / TEST ONLY. Argon2-hash with a fixed salt so the hash is a pure
/// function of the password, letting a replayed simulation seed reproduce
/// identical replicated user metadata. Additive: production [`hash_password`]
/// keeps its OS-random salt regardless of this feature, and no production path
/// calls this. Output is a valid Argon2 PHC string, so [`verify_password`]
/// accepts it exactly as it does a random-salted hash.
#[cfg(any(test, feature = "simulator"))]
#[must_use]
pub fn hash_with_fixed_salt(password: &str) -> String {
    // Fixed 16-byte salt; the value is irrelevant beyond being constant.
    const SIMULATOR_SALT: [u8; 16] = *b"iggy-sim-salt-16";
    let salt = SaltString::encode_b64(&SIMULATOR_SALT).expect("fixed sim salt encodes");
    hash_with_salt(password, &salt)
}

fn hash_with_salt(password: &str, salt: &SaltString) -> String {
    Argon2::default()
        .hash_password(password.as_bytes(), salt)
        .expect("Password hashing failed")
        .to_string()
}

/// Verify a password against a stored Argon2 PHC hash. Fails closed: an
/// unparsable stored hash denies the login (`false`) instead of panicking the
/// caller. The hash is persisted/replicated state, so a single corrupt one must
/// not take down the pump on every login. Not a timing oracle: the password
/// input cannot influence whether the stored hash parses.
pub fn verify_password(password: &str, hash: &str) -> bool {
    let Ok(hash) = PasswordHash::new(hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &hash)
        .is_ok()
}

pub fn generate_secret(range: Range<usize>) -> String {
    let length = rand::rng().random_range(range);
    let mut rng = rand::rng();
    (0..length)
        .map(|_| rng.sample(Alphanumeric) as char)
        .collect()
}
