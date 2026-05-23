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

//! Bijective permutations from ascending indices to test ids. Lets the
//! auditor recover the original index from any id present in a reply.
//!
//! Currently only `Identity` is used; other variants exist for ops that
//! need pseudo-uuid ids without reshaping this module.

use rand::RngExt;
use rand_xoshiro::Xoshiro256Plus;
use rand_xoshiro::rand_core::SeedableRng;

#[derive(Debug, Clone, Copy)]
pub enum IdPermutation {
    /// `index → index`.
    Identity,
    /// `index → u64::MAX - index`.
    Inversion,
    /// `index → (index << 32) | rand32(seed.wrapping_add(index))`.
    /// Decode discards the low 32 bits. Index must fit in `u32`.
    Random(u64),
}

impl IdPermutation {
    #[must_use]
    pub fn encode(&self, data: u64) -> u64 {
        match self {
            Self::Identity => data,
            Self::Inversion => u64::MAX - data,
            Self::Random(seed) => {
                debug_assert!(
                    u32::try_from(data).is_ok(),
                    "Random id permutation needs index <= u32::MAX, got {data}"
                );
                let mut prng = Xoshiro256Plus::seed_from_u64(seed.wrapping_add(data));
                let lo32: u32 = prng.random();
                ((data & 0xFFFF_FFFF) << 32) | u64::from(lo32)
            }
        }
    }

    #[must_use]
    pub const fn decode(&self, id: u64) -> u64 {
        match self {
            Self::Identity => id,
            Self::Inversion => u64::MAX - id,
            Self::Random(_) => id >> 32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::IdPermutation;

    fn roundtrip(p: IdPermutation, n: u64) {
        assert_eq!(n, p.decode(p.encode(n)), "{p:?} failed at {n}");
    }

    #[test]
    fn identity_roundtrip() {
        for n in [0, 1, 42, 1_000_000, u64::from(u32::MAX)] {
            roundtrip(IdPermutation::Identity, n);
        }
    }

    #[test]
    fn inversion_roundtrip() {
        for n in [0, 1, 42, 1_000_000, u64::from(u32::MAX)] {
            roundtrip(IdPermutation::Inversion, n);
        }
    }

    #[test]
    fn random_roundtrip() {
        let p = IdPermutation::Random(0xDEAD_BEEF);
        for n in [0, 1, 42, 1_000_000, u64::from(u32::MAX)] {
            roundtrip(p, n);
        }
    }

    #[test]
    fn random_is_deterministic() {
        let p = IdPermutation::Random(0xDEAD_BEEF);
        assert_eq!(p.encode(123), p.encode(123));
        // Different seeds yield different encodings.
        let q = IdPermutation::Random(0xCAFE_BABE);
        assert_ne!(p.encode(123), q.encode(123));
    }
}
