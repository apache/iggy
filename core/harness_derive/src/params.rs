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
//! Detects special parameters like `client: &IggyClient` and `harness: &TestHarness`
//! and distinguishes them from user-defined test_matrix parameters.

use proc_macro2::Span;
use syn::{FnArg, Ident, Pat, PatIdent, PatType, Signature, Type};

/// A detected parameter from the function signature.
#[derive(Debug)]
pub enum DetectedParam {
    /// A client parameter: `client: &IggyClient`
    Client { name: Ident },
    /// A harness reference: `harness: &TestHarness`
    HarnessRef { name: Ident },
    /// A mutable harness reference: `harness: &mut TestHarness`
    HarnessMut { name: Ident },
    /// An MCP client parameter: `mcp_client: McpClient`
    McpClient { name: Ident },
    /// A test fixture parameter: `fixture: PostgresSinkFixture`
    Fixture { name: Ident, ty: Box<Type> },
    /// A parameter from test_matrix (passed through)
    MatrixParam { name: Ident, ty: Box<Type> },
}

impl DetectedParam {
    #[allow(dead_code)]
    pub fn name(&self) -> &Ident {
        match self {
            DetectedParam::Client { name }
            | DetectedParam::HarnessRef { name }
            | DetectedParam::HarnessMut { name }
            | DetectedParam::McpClient { name }
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

        let Pat::Ident(PatIdent { ident, .. }) = pat.as_ref() else {
            return Err(syn::Error::new(
                Span::call_site(),
                "pattern parameters not supported, use simple identifiers",
            ));
        };

        let detected = detect_param_type(ident.clone(), ty)?;
        params.push(detected);
    }

    Ok(params)
}

fn detect_param_type(name: Ident, ty: &Type) -> syn::Result<DetectedParam> {
    let type_str = quote::quote!(#ty).to_string();
    let normalized = type_str.replace(" ", "");

    if is_iggy_client_type(&normalized) {
        return Ok(DetectedParam::Client { name });
    }

    if is_mcp_client_type(&normalized) {
        return Ok(DetectedParam::McpClient { name });
    }

    if is_harness_mut_type(&normalized) {
        return Ok(DetectedParam::HarnessMut { name });
    }

    if is_harness_ref_type(&normalized) {
        return Ok(DetectedParam::HarnessRef { name });
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

fn is_iggy_client_type(normalized: &str) -> bool {
    normalized.contains("IggyClient") || normalized.contains("ClientWrapper")
}

fn is_mcp_client_type(normalized: &str) -> bool {
    normalized.contains("McpClient") || normalized.contains("RunningService<RoleClient")
}

fn is_harness_ref_type(normalized: &str) -> bool {
    normalized.contains("TestHarness") && normalized.starts_with("&")
}

fn is_harness_mut_type(normalized: &str) -> bool {
    normalized.contains("TestHarness") && normalized.contains("&mut")
}

fn is_fixture_type(normalized: &str) -> bool {
    normalized.ends_with("Fixture") || normalized.ends_with("Fixture>")
}

/// Check if any parameter requires a client.
pub fn needs_client(params: &[DetectedParam]) -> bool {
    params
        .iter()
        .any(|p| matches!(p, DetectedParam::Client { .. }))
}

/// Check if any parameter requires harness access.
pub fn needs_harness(params: &[DetectedParam]) -> bool {
    params.iter().any(|p| {
        matches!(
            p,
            DetectedParam::HarnessRef { .. } | DetectedParam::HarnessMut { .. }
        )
    })
}

/// Check if any parameter requires mutable harness.
pub fn needs_harness_mut(params: &[DetectedParam]) -> bool {
    params
        .iter()
        .any(|p| matches!(p, DetectedParam::HarnessMut { .. }))
}

/// Check if any parameter requires an MCP client.
pub fn needs_mcp_client(params: &[DetectedParam]) -> bool {
    params
        .iter()
        .any(|p| matches!(p, DetectedParam::McpClient { .. }))
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
    fn detect_client_param() {
        let sig = parse_sig("async fn test(client: &IggyClient)");
        let params = analyze_signature(&sig).unwrap();
        assert_eq!(params.len(), 1);
        assert!(matches!(&params[0], DetectedParam::Client { name } if name == "client"));
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
    fn detect_matrix_param() {
        let sig = parse_sig("async fn test(count: u32)");
        let params = analyze_signature(&sig).unwrap();
        assert_eq!(params.len(), 1);
        assert!(matches!(&params[0], DetectedParam::MatrixParam { name, .. } if name == "count"));
    }

    #[test]
    fn detect_mixed_params() {
        let sig =
            parse_sig("async fn test(count: u32, client: &IggyClient, harness: &TestHarness)");
        let params = analyze_signature(&sig).unwrap();
        assert_eq!(params.len(), 3);
        assert!(matches!(&params[0], DetectedParam::MatrixParam { .. }));
        assert!(matches!(&params[1], DetectedParam::Client { .. }));
        assert!(matches!(&params[2], DetectedParam::HarnessRef { .. }));
    }

    #[test]
    fn detect_mcp_client() {
        let sig = parse_sig("async fn test(mcp: McpClient)");
        let params = analyze_signature(&sig).unwrap();
        assert_eq!(params.len(), 1);
        assert!(matches!(&params[0], DetectedParam::McpClient { name } if name == "mcp"));
    }

    #[test]
    fn detect_mcp_with_harness() {
        let sig = parse_sig("async fn test(mcp: McpClient, harness: &TestHarness)");
        let params = analyze_signature(&sig).unwrap();
        assert_eq!(params.len(), 2);
        assert!(matches!(&params[0], DetectedParam::McpClient { .. }));
        assert!(matches!(&params[1], DetectedParam::HarnessRef { .. }));
    }

    #[test]
    fn detect_mcp_with_client() {
        let sig = parse_sig("async fn test(client: &IggyClient, mcp: McpClient)");
        let params = analyze_signature(&sig).unwrap();
        assert_eq!(params.len(), 2);
        assert!(matches!(&params[0], DetectedParam::Client { .. }));
        assert!(matches!(&params[1], DetectedParam::McpClient { .. }));
    }

    #[test]
    fn needs_mcp_client_works() {
        let sig = parse_sig("async fn test(mcp: McpClient)");
        let params = analyze_signature(&sig).unwrap();
        assert!(super::needs_mcp_client(&params));

        let sig = parse_sig("async fn test(client: &IggyClient)");
        let params = analyze_signature(&sig).unwrap();
        assert!(!super::needs_mcp_client(&params));
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

        let sig = parse_sig("async fn test(client: &IggyClient)");
        let params = analyze_signature(&sig).unwrap();
        assert!(!super::needs_fixtures(&params));
    }

    #[test]
    fn fixture_params_works() {
        let sig = parse_sig(
            "async fn test(f1: PostgresFixture, client: &IggyClient, f2: RandomSourceFixture)",
        );
        let params = analyze_signature(&sig).unwrap();
        let fixtures = super::fixture_params(&params);
        assert_eq!(fixtures.len(), 2);
    }
}
