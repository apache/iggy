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

//! API document drift guard: `openapi.json` must describe exactly the routes the
//! HTTP listener registers.
//!
//! The route table in `src/http.rs` is parsed with `syn` and every
//! `.route(path, method_router)` call is read from its token stream, including
//! chained method routers such as `get(get_users).post(create_user)`. A path
//! given as a `const` (for example `PING_PATH`) is resolved from its
//! definition in the same file. The resulting (path, method) pairs must equal
//! the operations in `openapi.json`, and each operation's `operationId` must
//! name the handler registered for it.
//!
//! Two surfaces are left out of the comparison:
//! - The metrics scrape route. It is mounted at the configurable
//!   `http.metrics.endpoint`, so its path is not in the source. The route is
//!   recognised by its `get_metrics` handler, and the document's operation by
//!   the same `operationId`.
//! - The web UI routes under `/ui`. `merge_web_ui` merges them from
//!   `src/http/web.rs`, which this test does not parse, and the document does
//!   not describe them.

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use quote::ToTokens;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// Handler of the metrics route, excluded from the comparison (see above).
const METRICS_HANDLER: &str = "get_metrics";

/// axum method-router constructors and their chained counterparts.
const METHODS: [&str; 8] = [
    "get", "post", "put", "delete", "patch", "head", "options", "trace",
];

type Operation = (String, String);

#[test]
fn openapi_document_matches_route_table() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let routes = registered_routes(&manifest.join("src/http.rs"));
    let documented = documented_operations(&manifest.join("openapi.json"));

    let route_keys: BTreeSet<&Operation> = routes.keys().collect();
    let doc_keys: BTreeSet<&Operation> = documented.keys().collect();

    let missing: Vec<String> = route_keys
        .difference(&doc_keys)
        .map(|(path, method)| {
            format!(
                "  {} {path} ({})",
                method.to_uppercase(),
                routes[&(path.clone(), method.clone())]
            )
        })
        .collect();
    let extra: Vec<String> = doc_keys
        .difference(&route_keys)
        .map(|(path, method)| {
            format!(
                "  {} {path} ({})",
                method.to_uppercase(),
                documented[&(path.clone(), method.clone())]
            )
        })
        .collect();
    let mismatched: Vec<String> = route_keys
        .intersection(&doc_keys)
        .filter(|key| routes[**key] != documented[**key])
        .map(|(path, method)| {
            let key = (path.clone(), method.clone());
            format!(
                "  {} {path}: operationId is {:?}, handler is {:?}",
                method.to_uppercase(),
                documented[&key],
                routes[&key]
            )
        })
        .collect();

    let mut report = String::new();
    if !missing.is_empty() {
        report.push_str("Routes registered in src/http.rs but missing from openapi.json:\n");
        report.push_str(&missing.join("\n"));
        report.push('\n');
    }
    if !extra.is_empty() {
        report.push_str("Operations in openapi.json with no route in src/http.rs:\n");
        report.push_str(&extra.join("\n"));
        report.push('\n');
    }
    if !mismatched.is_empty() {
        report.push_str("Operations whose operationId does not name the route's handler:\n");
        report.push_str(&mismatched.join("\n"));
        report.push('\n');
    }
    assert!(
        report.is_empty(),
        "core/server/openapi.json is out of step with the HTTP route table.\n{report}"
    );
}

/// Every (path, method) registered with `.route(...)` in `file`, mapped to its
/// handler name. `#[cfg(test)]` items are skipped.
fn registered_routes(file: &Path) -> BTreeMap<Operation, String> {
    let source = std::fs::read_to_string(file)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", file.display()));
    let ast = syn::parse_file(&source)
        .unwrap_or_else(|error| panic!("cannot parse {}: {error}", file.display()));

    let mut consts = BTreeMap::new();
    let mut tokens = TokenStream::new();
    for item in &ast.items {
        if is_cfg_test(item) {
            continue;
        }
        if let syn::Item::Const(item_const) = item
            && let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(value),
                ..
            }) = item_const.expr.as_ref()
        {
            consts.insert(item_const.ident.to_string(), value.value());
        }
        item.to_tokens(&mut tokens);
    }

    let mut routes = BTreeMap::new();
    collect_routes(tokens, &consts, &mut routes);
    assert!(
        !routes.is_empty(),
        "found no `.route(...)` calls in {}; has the route table moved?",
        file.display()
    );
    routes
}

/// True only for exactly `#[cfg(test)]`. `cfg(not(test))` is code that ships,
/// and a feature name containing "test" is not the test cfg.
fn is_cfg_test(item: &syn::Item) -> bool {
    let attrs = match item {
        syn::Item::Mod(item) => &item.attrs,
        syn::Item::Fn(item) => &item.attrs,
        syn::Item::Use(item) => &item.attrs,
        _ => return false,
    };
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg") && attr.parse_args::<syn::Ident>().is_ok_and(|id| id == "test")
    })
}

/// Scan `tokens` for `. route ( ... )` and record each registration.
fn collect_routes(
    tokens: TokenStream,
    consts: &BTreeMap<String, String>,
    routes: &mut BTreeMap<Operation, String>,
) {
    let trees: Vec<TokenTree> = tokens.into_iter().collect();
    for (index, tree) in trees.iter().enumerate() {
        if let TokenTree::Group(group) = tree {
            let is_route_call = group.delimiter() == Delimiter::Parenthesis
                && index >= 2
                && matches!(&trees[index - 1], TokenTree::Ident(ident) if ident == "route")
                && matches!(&trees[index - 2], TokenTree::Punct(punct) if punct.as_char() == '.');
            if is_route_call {
                record_route(group.stream(), consts, routes);
            }
            // Route calls nest inside function bodies and argument lists.
            collect_routes(group.stream(), consts, routes);
        }
    }
}

/// Record one `.route(path, method_router)` argument list.
fn record_route(
    args: TokenStream,
    consts: &BTreeMap<String, String>,
    routes: &mut BTreeMap<Operation, String>,
) {
    let args = split_top_level_commas(args);
    let [path_tokens, method_tokens] = args.as_slice() else {
        panic!(
            "expected `.route(path, method_router)`, found `.route({})`",
            args.iter()
                .map(|arg| tokens_to_string(arg))
                .collect::<Vec<_>>()
                .join(", ")
        );
    };

    let methods = method_router(method_tokens);
    let path = match path_tokens.as_slice() {
        [TokenTree::Literal(literal)] => syn::parse_str::<syn::LitStr>(&literal.to_string())
            .map_or_else(
                |error| panic!("route path `{literal}` is not a string literal: {error}"),
                |value| value.value(),
            ),
        [TokenTree::Ident(ident)] => match consts.get(&ident.to_string()) {
            Some(value) => value.clone(),
            // The metrics route takes its path from config at runtime; it is
            // the only route allowed a non-constant path.
            None if methods
                .iter()
                .all(|(_, handler)| handler == METRICS_HANDLER) =>
            {
                return;
            }
            None => panic!(
                "route path `{ident}` is neither a string literal nor a `const` in src/http.rs"
            ),
        },
        other => panic!(
            "unsupported route path expression `{}`",
            tokens_to_string(other)
        ),
    };

    for (method, handler) in methods {
        let previous = routes.insert((path.clone(), method.clone()), handler);
        assert!(
            previous.is_none(),
            "{} {path} is registered twice in src/http.rs",
            method.to_uppercase()
        );
    }
}

/// Parse `get(a)` or a chain such as `get(a).post(b).delete(c)` into
/// (method, handler) pairs. Anything else fails loudly so a new routing form
/// cannot slip past the guard.
fn method_router(tokens: &[TokenTree]) -> Vec<(String, String)> {
    let mut methods = Vec::new();
    let mut index = 0;
    loop {
        let (Some(TokenTree::Ident(method)), Some(TokenTree::Group(group))) =
            (tokens.get(index), tokens.get(index + 1))
        else {
            panic!("unsupported method router `{}`", tokens_to_string(tokens));
        };
        let method = method.to_string();
        assert!(
            METHODS.contains(&method.as_str()) && group.delimiter() == Delimiter::Parenthesis,
            "unsupported method router call `{method}` in `{}`",
            tokens_to_string(tokens)
        );
        let handler = group
            .stream()
            .into_iter()
            .filter_map(|tree| match tree {
                TokenTree::Ident(ident) => Some(ident.to_string()),
                _ => None,
            })
            .last()
            .unwrap_or_else(|| panic!("`{method}(...)` has no handler"));
        methods.push((method, handler));

        index += 2;
        match tokens.get(index) {
            None => return methods,
            Some(TokenTree::Punct(punct)) if punct.as_char() == '.' => index += 1,
            Some(_) => panic!("unsupported method router `{}`", tokens_to_string(tokens)),
        }
    }
}

fn split_top_level_commas(tokens: TokenStream) -> Vec<Vec<TokenTree>> {
    let mut args = vec![Vec::new()];
    for tree in tokens {
        match &tree {
            TokenTree::Punct(punct) if punct.as_char() == ',' => args.push(Vec::new()),
            _ => args.last_mut().expect("never empty").push(tree),
        }
    }
    // A trailing comma leaves an empty last argument.
    if args.last().is_some_and(Vec::is_empty) {
        args.pop();
    }
    args
}

fn tokens_to_string(tokens: &[TokenTree]) -> String {
    tokens.iter().cloned().collect::<TokenStream>().to_string()
}

/// Every (path, method) operation in the API document, mapped to its
/// `operationId`. The metrics operation is skipped (see the module docs).
fn documented_operations(file: &Path) -> BTreeMap<Operation, String> {
    const HTTP_METHODS: [&str; 8] = [
        "get", "put", "post", "delete", "options", "head", "patch", "trace",
    ];

    let source = std::fs::read_to_string(file)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", file.display()));
    let document: serde_json::Value = serde_json::from_str(&source)
        .unwrap_or_else(|error| panic!("cannot parse {}: {error}", file.display()));
    let paths = document
        .get("paths")
        .and_then(serde_json::Value::as_object)
        .unwrap_or_else(|| panic!("{} has no `paths` object", file.display()));

    let mut operations = BTreeMap::new();
    for (path, item) in paths {
        let item = item
            .as_object()
            .unwrap_or_else(|| panic!("path item {path} is not an object"));
        for (method, operation) in item {
            if !HTTP_METHODS.contains(&method.as_str()) {
                continue;
            }
            let operation_id = operation
                .get("operationId")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| panic!("{} {path} has no operationId", method.to_uppercase()));
            if operation_id == METRICS_HANDLER {
                continue;
            }
            operations.insert((path.clone(), method.clone()), operation_id.to_owned());
        }
    }
    operations
}
