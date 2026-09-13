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

//! Module-graph guard: the crate's module dependency graph must stay a DAG.
//!
//! The http/ role-leaf pattern rotted into two state-hub cycles while nothing
//! enforced it, and two of the historical bootstrap cycles were invisible to
//! `use`-greps (an inline-qualified `crate::...` call and `use super::`
//! imports). So this test parses every source file with `syn` and scans full
//! token streams - it sees qualified call sites and macro arguments, not just
//! use declarations.
//!
//! Granularity is the module FILE. Parent<->child edges are exempt: a root
//! composing its children (and children reaching items the root defines) is
//! the pattern working as intended. Sibling and cross-tree cycles are the
//! rot this guard exists to stop. `#[cfg(test)]` modules are outside the
//! graph whether they are inline or their own file.
//!
//! `WHITELIST` carries the known survivors. Each entry must still be a live
//! cycle - a stale entry fails the test, so the list can only shrink.

use proc_macro2::{TokenStream, TokenTree};
use quote::ToTokens;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// Known mutual edges, as unordered pairs of module paths. Burned down to
/// empty by the dispatch/ login merge; new entries need a written ruling.
const WHITELIST: [(&str, &str); 0] = [];

type Module = Vec<String>;

#[test]
fn module_graph_is_a_dag_modulo_whitelist() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let modules = collect_modules(&src);
    let module_set: BTreeSet<Module> = modules.iter().map(|(module, _)| module.clone()).collect();

    let mut edges: BTreeMap<Module, BTreeSet<Module>> = BTreeMap::new();
    for (module, file) in &modules {
        let source = std::fs::read_to_string(file)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", file.display()));
        let ast = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("cannot parse {}: {error}", file.display()));
        let mut paths = Vec::new();
        scan_items(&ast.items, &mut paths);
        let targets = edges.entry(module.clone()).or_default();
        for raw in paths {
            if let Some(target) = resolve(module, &raw, &module_set)
                && target != *module
                && !is_ancestor(&target, module)
                && !is_ancestor(module, &target)
            {
                targets.insert(target);
            }
        }
    }

    let whitelist: BTreeSet<(Module, Module)> = WHITELIST
        .iter()
        .flat_map(|(a, b)| {
            let a = parse_module(a);
            let b = parse_module(b);
            [(a.clone(), b.clone()), (b, a)]
        })
        .collect();

    for (from, to) in &whitelist {
        assert!(
            edges.get(from).is_some_and(|targets| targets.contains(to)),
            "stale whitelist entry {} -> {}: the edge is gone, remove it",
            render(from),
            render(to),
        );
    }

    for (from, targets) in &mut edges {
        targets.retain(|to| !whitelist.contains(&(from.clone(), to.clone())));
    }

    if let Some(cycle) = find_cycle(&edges) {
        let chain = cycle.iter().map(render).collect::<Vec<_>>();
        panic!(
            "module cycle: {}\nBreak it by moving the shared vocabulary into \
             a leaf both sides import, or point the edge one way.",
            chain.join(" -> "),
        );
    }
}

/// Map every source file under `src/` to its module path. `lib.rs` is the
/// crate root `[]`; `main.rs`/`args.rs` belong to the bin target and are
/// skipped (their `crate::` is a different crate).
///
/// Modules a parent declares under `#[cfg(test)]` are dropped with their whole
/// subtree: `scan_items` already skips inline `#[cfg(test)] mod`, and a test
/// harness in its own file must not become a production graph node.
fn collect_modules(src: &Path) -> Vec<(Module, PathBuf)> {
    let mut files = Vec::new();
    walk(src, &mut files);
    files.sort();
    let mut modules: Vec<(Module, PathBuf)> = files
        .into_iter()
        .filter_map(|file| {
            let relative = file
                .strip_prefix(src)
                .unwrap_or_else(|_| panic!("{} outside src", file.display()));
            let mut module: Vec<String> = relative
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect();
            let last = module.pop().unwrap_or_default();
            match last.as_str() {
                "main.rs" | "args.rs" => return None,
                "lib.rs" | "mod.rs" => {}
                _ => module.push(last.trim_end_matches(".rs").to_owned()),
            }
            Some((module, file))
        })
        .collect();

    let gated = cfg_test_file_modules(&modules);
    modules.retain(|(module, _)| {
        !gated.contains(module) && !gated.iter().any(|root| is_ancestor(root, module))
    });
    modules
}

/// Module paths a parent declares as `#[cfg(test)] mod name;` (no inline body).
fn cfg_test_file_modules(modules: &[(Module, PathBuf)]) -> BTreeSet<Module> {
    let mut gated = BTreeSet::new();
    for (module, file) in modules {
        let source = std::fs::read_to_string(file)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", file.display()));
        let ast = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("cannot parse {}: {error}", file.display()));
        for item in &ast.items {
            if let syn::Item::Mod(declaration) = item
                && declaration.content.is_none()
                && is_cfg_test(&declaration.attrs)
            {
                let mut child = module.clone();
                child.push(declaration.ident.to_string());
                gated.insert(child);
            }
        }
    }
    gated
}

fn walk(dir: &Path, files: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("cannot read dir {}: {error}", dir.display()));
    for entry in entries {
        let path = entry
            .unwrap_or_else(|error| panic!("cannot read entry in {}: {error}", dir.display()))
            .path();
        if path.is_dir() {
            walk(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
}

/// Collect every `crate::`/`super::`-rooted path in the token streams of
/// `items`, recursing through nested modules and skipping `#[cfg(test)]` ones.
fn scan_items(items: &[syn::Item], paths: &mut Vec<Vec<String>>) {
    for item in items {
        if let syn::Item::Mod(module) = item {
            if is_cfg_test(&module.attrs) {
                continue;
            }
            if let Some((_, nested)) = &module.content {
                scan_items(nested, paths);
            }
            continue;
        }
        scan_tokens(item.to_token_stream(), paths);
    }
}

fn is_cfg_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg")
            && attr
                .parse_args::<TokenStream>()
                .is_ok_and(tokens_mention_test)
    })
}

/// True when the cfg predicate names the bare `test` ident anywhere,
/// `cfg(all(test, ...))` included.
fn tokens_mention_test(tokens: TokenStream) -> bool {
    tokens.into_iter().any(|tree| match tree {
        TokenTree::Ident(ident) => ident == "test",
        TokenTree::Group(group) => tokens_mention_test(group.stream()),
        _ => false,
    })
}

/// Token-level scan: catches inline-qualified calls and macro arguments that
/// an AST `use`-only walk would miss. Doc comments and string literals are
/// literals, not idents, so they never produce edges.
fn scan_tokens(tokens: TokenStream, paths: &mut Vec<Vec<String>>) {
    let trees: Vec<TokenTree> = tokens.into_iter().collect();
    let mut index = 0;
    while index < trees.len() {
        match &trees[index] {
            TokenTree::Group(group) => {
                scan_tokens(group.stream(), paths);
                index += 1;
            }
            TokenTree::Ident(ident) => {
                let name = ident.to_string();
                if name == "crate" || name == "super" {
                    let (segments, consumed) = read_path(&trees, index);
                    if segments.len() > 1 {
                        paths.push(segments);
                    }
                    index += consumed;
                } else {
                    index += 1;
                }
            }
            _ => index += 1,
        }
    }
}

/// Read `ident (:: ident)*` starting at `start`, descending into no groups.
fn read_path(trees: &[TokenTree], start: usize) -> (Vec<String>, usize) {
    let mut segments = Vec::new();
    let mut index = start;
    while let Some(TokenTree::Ident(ident)) = trees.get(index) {
        segments.push(ident.to_string());
        index += 1;
        let double_colon = matches!(trees.get(index), Some(TokenTree::Punct(p)) if p.as_char() == ':')
            && matches!(trees.get(index + 1), Some(TokenTree::Punct(p)) if p.as_char() == ':');
        if double_colon {
            index += 2;
        } else {
            break;
        }
    }
    (segments, index - start)
}

/// Resolve a raw `crate::`/`super::` path to the deepest known module it
/// names. Returns `None` for paths into the crate root's own items.
fn resolve(current: &Module, raw: &[String], modules: &BTreeSet<Module>) -> Option<Module> {
    let (base, rest): (Module, &[String]) = match raw.first().map(String::as_str) {
        Some("crate") => (Vec::new(), &raw[1..]),
        Some("super") => {
            let supers = raw.iter().take_while(|s| *s == "super").count();
            if supers > current.len() {
                return None;
            }
            (current[..current.len() - supers].to_vec(), &raw[supers..])
        }
        _ => return None,
    };
    let mut best: Option<Module> = if base.is_empty() {
        None
    } else {
        Some(base.clone())
    };
    let mut candidate = base;
    for segment in rest {
        candidate.push(segment.clone());
        if modules.contains(&candidate) {
            best = Some(candidate.clone());
        } else {
            break;
        }
    }
    best
}

fn is_ancestor(shorter: &Module, longer: &Module) -> bool {
    shorter.len() < longer.len() && longer[..shorter.len()] == shorter[..]
}

fn render(module: &Module) -> String {
    if module.is_empty() {
        "crate".to_owned()
    } else {
        module.join("::")
    }
}

fn parse_module(path: &str) -> Module {
    path.split("::").map(str::to_owned).collect()
}

/// DFS three-color cycle search; returns one cycle as a module chain.
fn find_cycle(edges: &BTreeMap<Module, BTreeSet<Module>>) -> Option<Vec<Module>> {
    let mut visiting = BTreeSet::new();
    let mut done = BTreeSet::new();
    let mut stack = Vec::new();
    for start in edges.keys() {
        if let Some(cycle) = dfs(start, edges, &mut visiting, &mut done, &mut stack) {
            return Some(cycle);
        }
    }
    None
}

fn dfs(
    node: &Module,
    edges: &BTreeMap<Module, BTreeSet<Module>>,
    visiting: &mut BTreeSet<Module>,
    done: &mut BTreeSet<Module>,
    stack: &mut Vec<Module>,
) -> Option<Vec<Module>> {
    if done.contains(node) {
        return None;
    }
    if visiting.contains(node) {
        let from = stack.iter().position(|n| n == node).unwrap_or(0);
        let mut cycle = stack[from..].to_vec();
        cycle.push(node.clone());
        return Some(cycle);
    }
    visiting.insert(node.clone());
    stack.push(node.clone());
    if let Some(targets) = edges.get(node) {
        for target in targets {
            if let Some(cycle) = dfs(target, edges, visiting, done, stack) {
                return Some(cycle);
            }
        }
    }
    stack.pop();
    visiting.remove(node);
    done.insert(node.clone());
    None
}
