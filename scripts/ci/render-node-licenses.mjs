#!/usr/bin/env node
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

// Render or validate license-checker-rseidelsohn JSON output.
//
// Usage:
//   render-node-licenses.mjs <license-checker-json>             render mode
//   render-node-licenses.mjs --validate-only <json>             validate mode
//
// Render mode emits a LICENSE-binary manifest on stdout, embedding the
// full LICENSE file text per package.
//
// Validate mode parses each package's SPDX expression, compares it
// against the allow-list below, and exits non-zero on disallowed
// licenses with a per-package report on stderr. Disjunctions (`OR`)
// pass if ANY operand is in the allow-list, matching cargo-about's
// permissive disjunction semantics. Conjunctions (`AND`) require ALL
// operands to be in the allow-list. Unknown / UNLICENSED licenses
// fail.

import { readFileSync, existsSync } from 'node:fs';

// Allow-list mirrors about.toml (Apache Iggy / ASF policy). Update both
// in lockstep when adding a new accepted license. SPDX identifiers
// only; expressions are tokenised before matching against this list.
const ALLOWED_LICENSES = new Set([
  'Apache-2.0',
  'Apache-2.0 WITH LLVM-exception',
  'MIT',
  'BSD-2-Clause',
  'BSD-3-Clause',
  'BSD-3-Clause-Clear',
  'ISC',
  'Zlib',
  'Unicode-3.0',
  'Unicode-DFS-2016',
  'MPL-2.0',
  'CC0-1.0',
  'OpenSSL',
  'BSL-1.0',
  'NCSA',
  // Common npm-only SPDX identifiers used by widely-bundled packages.
  // CC-BY-4.0 is a Cat-A documentation license; some npm packages
  // declare it for embedded asset metadata. Acceptable per ASF
  // resolved.html.
  'CC-BY-4.0',
  '0BSD',
]);

const args = process.argv.slice(2);
const validateOnly = args[0] === '--validate-only';
const inputPath = validateOnly ? args[1] : args[0];

if (!inputPath) {
  console.error('Usage: render-node-licenses.mjs [--validate-only] <license-checker-json>');
  process.exit(1);
}

const data = JSON.parse(readFileSync(inputPath, 'utf8'));

if (validateOnly) {
  validate(data);
} else {
  render(data);
}

function normalizeIdent(ident) {
  // license-checker-rseidelsohn appends `*` to identifiers it inferred
  // heuristically (e.g. `MIT*` when the SPDX expression is missing but
  // a LICENSE-MIT file was found). Treat them as the underlying SPDX
  // for allow-list comparison.
  return ident.trim().replace(/\*$/, '');
}

function tokenizeSpdx(expr) {
  // Strip parens, split on AND/OR, drop empty tokens. Preserve
  // operator structure by returning a flat list along with the
  // outermost operator found. This is a best-effort parser; cargo-about
  // does the same simplification (full SPDX evaluator is overkill here
  // and the ASF only requires that AT LEAST one acceptable license
  // applies).
  const cleaned = String(expr).replace(/[()]/g, ' ').trim();
  if (!cleaned) return { op: 'NONE', operands: [] };

  const hasOr = /\bOR\b/i.test(cleaned);
  const hasAnd = /\bAND\b/i.test(cleaned);

  // If only one operator type appears, split on it.
  if (hasOr && !hasAnd) {
    return {
      op: 'OR',
      operands: cleaned.split(/\s+OR\s+/i).map(s => s.trim()).filter(Boolean),
    };
  }
  if (hasAnd && !hasOr) {
    return {
      op: 'AND',
      operands: cleaned.split(/\s+AND\s+/i).map(s => s.trim()).filter(Boolean),
    };
  }
  if (hasOr && hasAnd) {
    // Mixed expression. Treat conservatively: split on OR first, each
    // operand may contain AND. The expression passes if any OR operand
    // (a conjunction) is fully allowed.
    return {
      op: 'OR',
      operands: cleaned.split(/\s+OR\s+/i).map(s => s.trim()).filter(Boolean),
    };
  }
  return { op: 'SINGLE', operands: [cleaned] };
}

function isAllowed(licensesField) {
  if (!licensesField) return false;
  const value = Array.isArray(licensesField) ? licensesField.join(' OR ') : String(licensesField);
  if (!value) return false;
  if (/^UNKNOWN$|^UNLICENSED$/i.test(value.trim())) return false;

  const { op, operands } = tokenizeSpdx(value);
  if (operands.length === 0) return false;

  switch (op) {
    case 'OR':
      return operands.some(operand => {
        // Each operand may itself be an AND expression.
        if (/\bAND\b/i.test(operand)) {
          return operand.split(/\s+AND\s+/i).every(t => ALLOWED_LICENSES.has(normalizeIdent(t)));
        }
        return ALLOWED_LICENSES.has(normalizeIdent(operand));
      });
    case 'AND':
      return operands.every(t => ALLOWED_LICENSES.has(normalizeIdent(t)));
    case 'SINGLE':
      return ALLOWED_LICENSES.has(normalizeIdent(operands[0]));
    default:
      return false;
  }
}

function validate(pkgs) {
  const violations = [];
  for (const [pkg, info] of Object.entries(pkgs)) {
    if (!isAllowed(info.licenses)) {
      violations.push({ pkg, licenses: info.licenses ?? '(unknown)' });
    }
  }
  if (violations.length > 0) {
    console.error(`Disallowed npm license(s) detected (${violations.length} package(s)):`);
    for (const v of violations) {
      console.error(`  - ${v.pkg}: ${v.licenses}`);
    }
    console.error('');
    console.error('Allowed SPDX identifiers are listed at the top of');
    console.error('scripts/ci/render-node-licenses.mjs (mirrors about.toml).');
    process.exit(1);
  }
  console.log(`OK: ${Object.keys(pkgs).length} npm package(s) validated against the SPDX allow-list.`);
}

function render(pkgs) {
  const out = [];
  out.push('Apache Iggy (incubating) - Third-Party License Manifest (Node)');
  out.push('================================================================');
  out.push('');
  out.push('The following npm production dependencies are bundled into this');
  out.push('Apache Iggy (incubating) convenience binary artifact. Packages are');
  out.push('grouped by license; the canonical license text appears once per');
  out.push('group as required by the Apache Software Foundation release policy.');
  out.push('');
  out.push('Generated by license-checker-rseidelsohn from the package manifest');
  out.push('at build time. Does not apply to the official Apache source release.');
  out.push('');

  // Group packages by license. Use the raw `licenses` field as the
  // grouping key so that compound expressions ("(MIT OR Apache-2.0)")
  // and Custom URLs each form their own bucket. Within a bucket the
  // first package's license file text is treated as canonical; the
  // others just contribute to the USED BY listing.
  const groups = new Map();
  for (const [pkg, info] of Object.entries(pkgs).sort()) {
    const key = String(info.licenses ?? '(unknown)');
    if (!groups.has(key)) {
      groups.set(key, { canonicalText: null, members: [] });
    }
    const g = groups.get(key);
    if (g.canonicalText === null && info.licenseFile && existsSync(info.licenseFile)) {
      g.canonicalText = readFileSync(info.licenseFile, 'utf8').trimEnd();
    }
    g.members.push({ pkg, info });
  }

  // Emit groups in stable order (sorted by SPDX key).
  for (const [licenseKey, group] of Array.from(groups.entries()).sort((a, b) => a[0].localeCompare(b[0]))) {
    out.push('================================================================================');
    out.push(`LICENSE: ${licenseKey} - used by ${group.members.length} package(s)`);
    out.push('USED BY:');
    for (const { pkg, info } of group.members) {
      let line = `  - ${pkg}`;
      if (info.repository) line += ` (${info.repository})`;
      out.push(line);
    }
    out.push('================================================================================');
    out.push('');
    out.push(group.canonicalText ?? '(no license text available; see SPDX identifier above)');
    out.push('');
    out.push('');
  }

  process.stdout.write(out.join('\n'));
}
