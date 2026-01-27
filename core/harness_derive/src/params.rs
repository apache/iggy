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

//! Parameter detection for test function signatures.
//!
//! Detects special parameters like `harness: &TestHarness` and test fixtures,
//! and distinguishes them from user-defined test_matrix parameters.

use proc_macro2::Span;
use syn::{FnArg, Ident, Pat, PatIdent, PatType, Signature, Type};

/// A detected parameter from the function signature.
#[derive(Debug)]
pub enum DetectedParam {
    /// A harness reference: `harness: &TestHarness`
    HarnessRef { name: Ident },
    /// A mutable harness reference: `harness: &mut TestHarness`
    HarnessMut { name: Ident },
    /// An owned harness: `harness: TestHarness`
    HarnessOwned { name: Ident },
    /// A mutable owned harness: `mut harness: TestHarness`
    HarnessOwnedMut { name: Ident },
    /// A test fixture parameter: `fixture: PostgresSinkFixture`
    Fixture { name: Ident, ty: Box<Type> },
    /// A parameter from test_matrix (passed through)
    MatrixParam { name: Ident, ty: Box<Type> },
}

impl DetectedParam {
    #[allow(dead_code)]
    pub fn name(&self) -> &Ident {
        match self {
            DetectedParam::HarnessRef { name }
            | DetectedParam::HarnessMut { name }
            | DetectedParam::HarnessOwned { name }
            | DetectedParam::HarnessOwnedMut { name }
            | DetectedParam::Fixture { name, .. }
            | DetectedParam::MatrixParam { name, .. } => name,
        }
    }
}

/// Analyze a function signature and detect parameter types.
pub fn analyze_signature(sig: &Signature) -> syn::Result<Vec<DetectedParam>> {
    let mut params = Vec::new();

    for arg in &sig.inputs {
        let FnArg::Typed(PatType { pat, ty, .. }) = arg else {
            return Err(syn::Error::new(
                Span::call_site(),
                "self parameters not supported in test functions",
            ));
        };

        let Pat::Ident(PatIdent {
            ident, mutability, ..
        }) = pat.as_ref()
        else {
            return Err(syn::Error::new(
                Span::call_site(),
                "pattern parameters not supported, use simple identifiers",
            ));
        };

        let is_mut = mutability.is_some();
        let detected = detect_param_type(ident.clone(), ty, is_mut)?;
        params.push(detected);
    }

    Ok(params)
}

fn detect_param_type(name: Ident, ty: &Type, is_mut: bool) -> syn::Result<DetectedParam> {
    let type_str = quote::quote!(#ty).to_string();
    let normalized = type_str.replace(" ", "");

    if is_harness_mut_ref_type(&normalized) {
        return Ok(DetectedParam::HarnessMut { name });
    }

    if is_harness_ref_type(&normalized) {
        return Ok(DetectedParam::HarnessRef { name });
    }

    if is_harness_owned_type(&normalized) {
        return if is_mut {
            Ok(DetectedParam::HarnessOwnedMut { name })
        } else {
            Ok(DetectedParam::HarnessOwned { name })
        };
    }

    if is_fixture_type(&normalized) {
        return Ok(DetectedParam::Fixture {
            name,
            ty: Box::new(ty.clone()),
        });
    }

    Ok(DetectedParam::MatrixParam {
        name,
        ty: Box::new(ty.clone()),
    })
}

fn is_harness_ref_type(normalized: &str) -> bool {
    normalized.contains("TestHarness")
        && normalized.starts_with("&")
        && !normalized.contains("&mut")
}

fn is_harness_mut_ref_type(normalized: &str) -> bool {
    normalized.contains("TestHarness") && normalized.contains("&mut")
}

fn is_harness_owned_type(normalized: &str) -> bool {
    !normalized.starts_with('&')
        && (normalized == "TestHarness" || normalized.ends_with("::TestHarness"))
}

fn is_fixture_type(normalized: &str) -> bool {
    normalized.ends_with("Fixture") || normalized.ends_with("Fixture>")
}

/// Get the matrix parameters (those from test_matrix).
pub fn matrix_params(params: &[DetectedParam]) -> Vec<&DetectedParam> {
    params
        .iter()
        .filter(|p| matches!(p, DetectedParam::MatrixParam { .. }))
        .collect()
}

/// Check if any parameter is a fixture.
pub fn needs_fixtures(params: &[DetectedParam]) -> bool {
    params
        .iter()
        .any(|p| matches!(p, DetectedParam::Fixture { .. }))
}

/// Get the fixture parameters.
pub fn fixture_params(params: &[DetectedParam]) -> Vec<&DetectedParam> {
    params
        .iter()
        .filter(|p| matches!(p, DetectedParam::Fixture { .. }))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_sig(s: &str) -> Signature {
        let item: syn::ItemFn = syn::parse_str(&format!("{s} {{}}")).unwrap();
        item.sig
    }

    #[test]
    fn detect_harness_ref() {
        let sig = parse_sig("async fn test(harness: &TestHarness)");
        let params = analyze_signature(&sig).unwrap();
        assert_eq!(params.len(), 1);
        assert!(matches!(&params[0], DetectedParam::HarnessRef { name } if name == "harness"));
    }

    #[test]
    fn detect_harness_mut() {
        let sig = parse_sig("async fn test(harness: &mut TestHarness)");
        let params = analyze_signature(&sig).unwrap();
        assert_eq!(params.len(), 1);
        assert!(matches!(&params[0], DetectedParam::HarnessMut { name } if name == "harness"));
    }

    #[test]
    fn detect_harness_owned() {
        let sig = parse_sig("async fn test(harness: TestHarness)");
        let params = analyze_signature(&sig).unwrap();
        assert_eq!(params.len(), 1);
        assert!(matches!(&params[0], DetectedParam::HarnessOwned { name } if name == "harness"));
    }

    #[test]
    fn detect_harness_owned_mut() {
        let sig = parse_sig("async fn test(mut harness: TestHarness)");
        let params = analyze_signature(&sig).unwrap();
        assert_eq!(params.len(), 1);
        assert!(matches!(&params[0], DetectedParam::HarnessOwnedMut { name } if name == "harness"));
    }

    #[test]
    fn detect_matrix_param() {
        let sig = parse_sig("async fn test(count: u32)");
        let params = analyze_signature(&sig).unwrap();
        assert_eq!(params.len(), 1);
        assert!(matches!(&params[0], DetectedParam::MatrixParam { name, .. } if name == "count"));
    }

    #[test]
    fn detect_mixed_params() {
        let sig = parse_sig("async fn test(count: u32, harness: &TestHarness)");
        let params = analyze_signature(&sig).unwrap();
        assert_eq!(params.len(), 2);
        assert!(matches!(&params[0], DetectedParam::MatrixParam { .. }));
        assert!(matches!(&params[1], DetectedParam::HarnessRef { .. }));
    }

    #[test]
    fn detect_fixture_param() {
        let sig = parse_sig("async fn test(fixture: PostgresSinkFixture)");
        let params = analyze_signature(&sig).unwrap();
        assert_eq!(params.len(), 1);
        assert!(matches!(&params[0], DetectedParam::Fixture { name, .. } if name == "fixture"));
    }

    #[test]
    fn detect_fixture_with_generic() {
        let sig = parse_sig("async fn test(fixture: Box<PostgresFixture>)");
        let params = analyze_signature(&sig).unwrap();
        assert_eq!(params.len(), 1);
        assert!(matches!(&params[0], DetectedParam::Fixture { name, .. } if name == "fixture"));
    }

    #[test]
    fn needs_fixtures_works() {
        let sig = parse_sig("async fn test(fixture: RandomSourceFixture)");
        let params = analyze_signature(&sig).unwrap();
        assert!(super::needs_fixtures(&params));

        let sig = parse_sig("async fn test(harness: &TestHarness)");
        let params = analyze_signature(&sig).unwrap();
        assert!(!super::needs_fixtures(&params));
    }

    #[test]
    fn fixture_params_works() {
        let sig = parse_sig(
            "async fn test(f1: PostgresFixture, harness: &TestHarness, f2: RandomSourceFixture)",
        );
        let params = analyze_signature(&sig).unwrap();
        let fixtures = super::fixture_params(&params);
        assert_eq!(fixtures.len(), 2);
    }
}
