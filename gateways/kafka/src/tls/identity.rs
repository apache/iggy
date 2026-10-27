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

//! The principal a client certificate names.
//!
//! Kafka hands `ssl.principal.mapping.rules` the subject as Java's `X500Principal.getName()`
//! renders it: RFC 2253, most specific RDN first, no spaces after the commas. Operators write
//! their rules against that rendering, so this one follows `sun.security.x509.AVA`'s
//! `toRFC2253String` (JDK 11 to 21) rather than x509-parser's own `Display`, which keeps the
//! certificate's order and pads with spaces. Malformed UTF-8 and BMP values are the one case not
//! checked against Java: both sides substitute U+FFFD, possibly not one for one.

use std::fmt::Write as _;
use std::str::FromStr;

use regex::{Captures, Regex};
use regex_syntax::ast::{
    self, Assertion, AssertionKind, Ast, ClassPerl, ClassPerlKind, ClassSetBinaryOp,
    ClassSetBinaryOpKind, ClassSetItem, Flag, Flags, LiteralKind, Span, SpecialLiteralKind,
    Visitor,
};
use thiserror::Error;
use x509_parser::asn1_rs::{Any, Tag, ToDer};
use x509_parser::error::X509Error;
use x509_parser::nom;
use x509_parser::parse_x509_certificate;
use x509_parser::x509::{AttributeTypeAndValue, RelativeDistinguishedName, X509Name};

/// Why a client certificate yielded no principal.
#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("the client certificate could not be parsed: {0}")]
    Certificate(#[from] nom::Err<X509Error>),
    #[error("no principal mapping rule matches the certificate subject '{subject}'")]
    NoMatchingRule { subject: String },
}

/// Why `ssl.principal.mapping.rules` text did not parse.
#[derive(Debug, Error)]
pub enum MappingRuleError {
    #[error("the rule list is empty; the default is DEFAULT")]
    Empty,
    #[error("the rule list ends with a comma")]
    TrailingComma,
    #[error("'{0}' is neither DEFAULT nor RULE:pattern/replacement/[LU]")]
    UnknownRule(String),
    #[error("expected ',' or the end of the list before '{0}'")]
    MissingSeparator(String),
    #[error("rule '{0}' has no '/' after its pattern")]
    UnterminatedPattern(String),
    #[error("rule '{0}' has no '/' after its replacement")]
    UnterminatedReplacement(String),
    #[error("rule pattern '{pattern}' is not a valid regex: {source}")]
    InvalidPattern {
        pattern: String,
        #[source]
        source: regex::Error,
    },
    #[error("rule pattern '{pattern}' uses '{construct}': {reason}")]
    JavaIncompatible {
        pattern: String,
        construct: String,
        reason: JavaIncompatibility,
    },
    #[error("replacement '{replacement}' of rule pattern '{pattern}' is invalid: {source}")]
    InvalidReplacement {
        pattern: String,
        replacement: String,
        #[source]
        source: ReplacementError,
    },
}

/// A pattern construct whose Rust `regex` meaning admits subjects Java's `Pattern` rejects.
#[derive(Debug, Error, PartialEq, Eq, Clone, Copy)]
pub enum JavaIncompatibility {
    #[error(
        "the case-insensitive flag folds Unicode in Rust (the Kelvin sign matches 'k'), ASCII in Java"
    )]
    CaseInsensitive,
    #[error("the 'u' and 'U' flags mean different things in Rust and Java")]
    UnicodeFlag,
    #[error("Unicode property classes are not supported")]
    UnicodeClass,
    #[error("Java reads '[:name:]' as a set of literal characters, not a POSIX class")]
    PosixClass,
    #[error("word boundaries are not supported")]
    WordBoundary,
    #[error(
        "the 'm', 'x' and 'R' flags differ between Rust and Java in which line ends and which \
         whitespace and comments they cover"
    )]
    LineOrWhitespaceFlag,
    #[error("'\\v' is a vertical tab in Rust and any vertical whitespace in Java")]
    VerticalWhitespace,
    #[error(
        "Java reads a quantifier after a quantifier as possessive, Rust as a nested repetition"
    )]
    StackedQuantifier,
    #[error(
        "Java reads '--' and '~~' inside a class as literal characters, Rust as set operations"
    )]
    ClassSetOperation,
}

/// A replacement that `java.util.regex.Matcher` would throw on when the rule is applied.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ReplacementError {
    #[error("a trailing '\\' escapes nothing")]
    DanglingEscape,
    #[error("'$' is followed by neither a group number nor '{{name}}'")]
    IllegalGroupReference,
    #[error("the pattern has no group {0}")]
    NoGroup(usize),
    #[error("'${{' has no closing '}}'")]
    UnterminatedGroupName,
    #[error("the pattern has no group named '{0}'")]
    NoNamedGroup(String),
}

/// The subject of a DER certificate, as Kafka's principal builder sees it.
///
/// # Errors
///
/// Returns [`IdentityError::Certificate`] when `certificate` is not a DER X.509 certificate.
pub fn subject_distinguished_name(certificate: &[u8]) -> Result<String, IdentityError> {
    let (_, parsed) = parse_x509_certificate(certificate)?;
    Ok(render_rfc2253(parsed.subject()))
}

/// RFC 2253 string form: RDNs in reverse certificate order, `,` between RDNs, `+` inside a
/// multi-valued one, and the attribute keywords Java emits.
fn render_rfc2253(name: &X509Name<'_>) -> String {
    let rendered: Vec<String> = name.iter_rdn().map(render_rdn).collect();
    let mut out = String::new();
    for (index, rdn) in rendered.iter().rev().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(rdn);
    }
    out
}

fn render_rdn(rdn: &RelativeDistinguishedName<'_>) -> String {
    let mut out = String::new();
    for (index, attribute) in rdn.iter().enumerate() {
        if index > 0 {
            out.push('+');
        }
        render_attribute(&mut out, attribute);
    }
    out
}

/// `KEYWORD=value` for the attribute types Java gives a keyword, `OID=#hex` for the rest, and
/// `#hex` of the DER for a value whose string type Java does not decode.
fn render_attribute(out: &mut String, attribute: &AttributeTypeAndValue<'_>) {
    let value = attribute.attr_value();
    if let Some(keyword) = rfc2253_keyword(&attribute.attr_type().to_string()) {
        out.push_str(keyword);
        out.push('=');
        match string_value(value) {
            Some(text) => escape_into(out, &text),
            None => push_hex(out, value),
        }
    } else {
        let _ = write!(out, "{}=", attribute.attr_type());
        push_hex(out, value);
    }
}

/// The keyword Java's RFC 2253 rendering uses for an attribute type, or `None` for an OID it
/// spells out. Java's set is RFC 2253's seven plus `DC` and `UID` from RFC 4514; everything
/// else, `emailAddress` included, is rendered as a dotted OID with a hex value.
fn rfc2253_keyword(oid: &str) -> Option<&'static str> {
    Some(match oid {
        "2.5.4.3" => "CN",
        "2.5.4.6" => "C",
        "2.5.4.7" => "L",
        "2.5.4.8" => "ST",
        "2.5.4.9" => "STREET",
        "2.5.4.10" => "O",
        "2.5.4.11" => "OU",
        "0.9.2342.19200300.100.1.25" => "DC",
        "0.9.2342.19200300.100.1.1" => "UID",
        _ => return None,
    })
}

/// The text of a directory string, decoded with the charset `AVA.getCharset` picks for its tag,
/// or `None` for the tags `AVA.isDerString` rejects, which Java renders as hex
/// (`NumericString` and `UniversalString` among them).
fn string_value(value: &Any<'_>) -> Option<String> {
    let data = value.data;
    match value.tag() {
        // Latin-1, not ASCII: Java keeps a non-ASCII byte in these types as its own code point.
        Tag::PrintableString | Tag::Ia5String | Tag::GeneralString | Tag::T61String => {
            Some(data.iter().map(|&byte| char::from(byte)).collect())
        }
        Tag::Utf8String => Some(String::from_utf8_lossy(data).into_owned()),
        Tag::BmpString => {
            let units: Vec<u16> = data
                .chunks(2)
                .map(|pair| match pair {
                    [high, low] => u16::from_be_bytes([*high, *low]),
                    _ => 0xFFFD,
                })
                .collect();
            Some(String::from_utf16_lossy(&units))
        }
        _ => None,
    }
}

fn push_hex(out: &mut String, value: &Any<'_>) {
    out.push('#');
    let der = value
        .to_der_vec()
        .unwrap_or_else(|_| value.as_bytes().to_vec());
    for byte in der {
        let _ = write!(out, "{byte:02x}");
    }
}

/// Backslash-escapes what Java's RFC 2253 rendering escapes: `,=+<>#;"\` anywhere, NUL as `\00`,
/// and every space or carriage return in the leading and trailing runs. Everything else, other
/// control characters included, is written as is.
fn escape_into(out: &mut String, text: &str) {
    let mut escaped: Vec<char> = Vec::with_capacity(text.len());
    for character in text.chars() {
        if character == '\0' {
            escaped.extend(['\\', '0', '0']);
            continue;
        }
        if matches!(
            character,
            ',' | '=' | '+' | '<' | '>' | '#' | ';' | '"' | '\\'
        ) {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    let is_edge_space = |character: &char| matches!(character, ' ' | '\r');
    let lead = escaped
        .iter()
        .position(|character| !is_edge_space(character))
        .unwrap_or(escaped.len());
    let trail = escaped
        .iter()
        .rposition(|character| !is_edge_space(character));
    for (index, character) in escaped.into_iter().enumerate() {
        let in_trailing_run = trail.is_none_or(|trail| index > trail);
        if index < lead || in_trailing_run {
            out.push('\\');
        }
        out.push(character);
    }
}

/// Kafka's `ssl.principal.mapping.rules`: `RULE:pattern/replacement/[LU]` and `DEFAULT`, comma
/// separated, tried in order.
///
/// A rule applies when its pattern matches the whole subject (Java's `matches()`). Its result is
/// then Java's `subject.replaceAll(pattern, replacement)`: every match of the unanchored pattern
/// in the subject is replaced, an empty match at the end included, so `RULE:(.*)/$1@x/` maps
/// `alice` to `alice@x@x` exactly as Kafka does.
///
/// Patterns are a Java-compatible subset: `\d \s \w` are rewritten to ASCII classes and `.` to
/// exclude Java's line terminators, and constructs whose Rust meaning is wider than Java's are
/// refused as [`MappingRuleError::JavaIncompatible`].
#[derive(Debug, Clone)]
pub struct PrincipalMappingRules {
    rules: Vec<Rule>,
}

#[derive(Debug, Clone)]
enum Rule {
    /// The subject string itself.
    Default,
    Rewrite {
        whole: Regex,
        finder: Regex,
        replacement: Vec<Piece>,
        case: Case,
    },
}

/// One part of a parsed replacement.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Piece {
    Literal(String),
    Group(usize),
    Named(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Case {
    Keep,
    Lower,
    Upper,
}

impl Default for PrincipalMappingRules {
    fn default() -> Self {
        Self {
            rules: vec![Rule::Default],
        }
    }
}

impl PrincipalMappingRules {
    /// Maps a rendered subject to a principal name, or `None` when no rule matches.
    #[must_use]
    pub fn map(&self, subject: &str) -> Option<String> {
        self.rules.iter().find_map(|rule| match rule {
            Rule::Default => Some(subject.to_string()),
            Rule::Rewrite {
                whole,
                finder,
                replacement,
                case,
            } => {
                if !whole.is_match(subject) {
                    return None;
                }
                let rewritten = replace_all(finder, subject, replacement);
                Some(match case {
                    Case::Keep => rewritten,
                    Case::Lower => rewritten.to_lowercase(),
                    Case::Upper => rewritten.to_uppercase(),
                })
            }
        })
    }
}

impl FromStr for PrincipalMappingRules {
    type Err = MappingRuleError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let mut rules = Vec::new();
        let mut rest = raw.trim_start();
        if rest.is_empty() {
            return Err(MappingRuleError::Empty);
        }
        while !rest.is_empty() {
            if let Some(after) = rest.strip_prefix("DEFAULT") {
                rules.push(Rule::Default);
                rest = after;
            } else if let Some(after) = rest.strip_prefix("RULE:") {
                let (rule, after) = parse_rewrite(rest, after)?;
                rules.push(rule);
                rest = after;
            } else {
                return Err(MappingRuleError::UnknownRule(rest.to_string()));
            }
            rest = rest.trim_start();
            match rest.strip_prefix(',') {
                Some(after) => {
                    rest = after.trim_start();
                    if rest.is_empty() {
                        return Err(MappingRuleError::TrailingComma);
                    }
                }
                None if rest.is_empty() => {}
                None => return Err(MappingRuleError::MissingSeparator(rest.to_string())),
            }
        }
        Ok(Self { rules })
    }
}

/// Parses the `pattern/replacement/[LU]` after `RULE:`, returning the rule and what follows it.
fn parse_rewrite<'a>(rule: &str, body: &'a str) -> Result<(Rule, &'a str), MappingRuleError> {
    let (pattern, after) = take_until_slash(body)
        .ok_or_else(|| MappingRuleError::UnterminatedPattern(rule.to_string()))?;
    let (replacement, after) = take_until_slash(after)
        .ok_or_else(|| MappingRuleError::UnterminatedReplacement(rule.to_string()))?;
    let (case, after) = match after.chars().next() {
        Some('L') => (Case::Lower, &after[1..]),
        Some('U') => (Case::Upper, &after[1..]),
        _ => (Case::Keep, after),
    };
    let invalid_pattern = |source| MappingRuleError::InvalidPattern {
        pattern: pattern.clone(),
        source,
    };
    let java = java_semantics(&pattern)?;
    let finder = Regex::new(&java).map_err(invalid_pattern)?;
    let whole = Regex::new(&format!("^(?:{java})$")).map_err(invalid_pattern)?;
    let pieces = parse_replacement(&replacement, &finder).map_err(|source| {
        MappingRuleError::InvalidReplacement {
            pattern: pattern.clone(),
            replacement: replacement.clone(),
            source,
        }
    })?;
    Ok((
        Rule::Rewrite {
            whole,
            finder,
            replacement: pieces,
            case,
        },
        after,
    ))
}

/// Rewrites `pattern` so Rust matches what `java.util.regex.Pattern` matches without flags:
/// `\d \s \w` and their negations become ASCII classes, and `.` outside `(?s)` also stops at
/// `\r`, U+0085, U+2028 and U+2029. Constructs with no faithful rewrite are refused with their
/// span.
fn java_semantics(pattern: &str) -> Result<String, MappingRuleError> {
    let parsed = ast::parse::Parser::new().parse(pattern).map_err(|error| {
        MappingRuleError::InvalidPattern {
            pattern: pattern.to_string(),
            source: regex::Error::Syntax(error.to_string()),
        }
    })?;
    let rewriter = JavaRewriter {
        dot_all: vec![false],
        edits: Vec::new(),
    };
    let edits = ast::visit(&parsed, rewriter).map_err(|(span, reason)| {
        MappingRuleError::JavaIncompatible {
            pattern: pattern.to_string(),
            construct: pattern[span.start.offset..span.end.offset].to_string(),
            reason,
        }
    })?;
    let mut rewritten = pattern.to_string();
    for (span, replacement) in edits.iter().rev() {
        rewritten.replace_range(span.start.offset..span.end.offset, replacement);
    }
    Ok(rewritten)
}

const JAVA_DOT: &str = r"[^\n\r\x{85}\x{2028}\x{2029}]";

/// Collects `(span, replacement)` edits in pattern order.
struct JavaRewriter {
    /// Whether `.` matches line terminators, one entry per open group.
    dot_all: Vec<bool>,
    edits: Vec<(Span, String)>,
}

impl JavaRewriter {
    fn apply_flags(&mut self, flags: &Flags) -> Result<(), (Span, JavaIncompatibility)> {
        for item in &flags.items {
            match item.kind {
                ast::FlagsItemKind::Flag(Flag::CaseInsensitive)
                    if flags.flag_state(Flag::CaseInsensitive) == Some(true) =>
                {
                    return Err((item.span, JavaIncompatibility::CaseInsensitive));
                }
                ast::FlagsItemKind::Flag(Flag::Unicode | Flag::SwapGreed) => {
                    return Err((item.span, JavaIncompatibility::UnicodeFlag));
                }
                ast::FlagsItemKind::Flag(
                    flag @ (Flag::MultiLine | Flag::IgnoreWhitespace | Flag::CRLF),
                ) if flags.flag_state(flag) == Some(true) => {
                    return Err((item.span, JavaIncompatibility::LineOrWhitespaceFlag));
                }
                _ => {}
            }
        }
        if let Some(dot_all) = flags.flag_state(Flag::DotMatchesNewLine)
            && let Some(current) = self.dot_all.last_mut()
        {
            *current = dot_all;
        }
        Ok(())
    }

    fn perl(&mut self, class: &ClassPerl, bracketed: bool) {
        let name = match class.kind {
            ClassPerlKind::Digit => "digit",
            ClassPerlKind::Space => "space",
            ClassPerlKind::Word => "word",
        };
        let negation = if class.negated { "^" } else { "" };
        let ascii = format!("[:{negation}{name}:]");
        let replacement = if bracketed {
            ascii
        } else {
            format!("[{ascii}]")
        };
        self.edits.push((class.span, replacement));
    }
}

impl Visitor for JavaRewriter {
    type Output = Vec<(Span, String)>;
    type Err = (Span, JavaIncompatibility);

    fn finish(self) -> Result<Self::Output, Self::Err> {
        Ok(self.edits)
    }

    fn visit_pre(&mut self, node: &Ast) -> Result<(), Self::Err> {
        match node {
            Ast::Group(group) => {
                let inherited = self.dot_all.last().copied().unwrap_or(false);
                self.dot_all.push(inherited);
                if let Some(flags) = group.flags() {
                    self.apply_flags(flags)?;
                }
            }
            Ast::Flags(set) => self.apply_flags(&set.flags)?,
            Ast::Dot(span) => {
                if !self.dot_all.last().copied().unwrap_or(false) {
                    self.edits.push((**span, JAVA_DOT.to_string()));
                }
            }
            Ast::ClassPerl(class) => self.perl(class, false),
            Ast::ClassUnicode(class) => {
                return Err((class.span, JavaIncompatibility::UnicodeClass));
            }
            Ast::Assertion(assertion) => refuse_word_boundary(assertion)?,
            Ast::Literal(literal)
                if literal.kind == LiteralKind::Special(SpecialLiteralKind::VerticalTab) =>
            {
                return Err((literal.span, JavaIncompatibility::VerticalWhitespace));
            }
            Ast::Repetition(repetition) if matches!(*repetition.ast, Ast::Repetition(_)) => {
                return Err((repetition.op.span, JavaIncompatibility::StackedQuantifier));
            }
            _ => {}
        }
        Ok(())
    }

    fn visit_post(&mut self, node: &Ast) -> Result<(), Self::Err> {
        if let Ast::Group(_) = node {
            self.dot_all.pop();
        }
        Ok(())
    }

    fn visit_class_set_item_pre(&mut self, item: &ClassSetItem) -> Result<(), Self::Err> {
        match item {
            ClassSetItem::Perl(class) => self.perl(class, true),
            ClassSetItem::Unicode(class) => {
                return Err((class.span, JavaIncompatibility::UnicodeClass));
            }
            ClassSetItem::Ascii(class) => {
                return Err((class.span, JavaIncompatibility::PosixClass));
            }
            ClassSetItem::Literal(literal)
                if literal.kind == LiteralKind::Special(SpecialLiteralKind::VerticalTab) =>
            {
                return Err((literal.span, JavaIncompatibility::VerticalWhitespace));
            }
            _ => {}
        }
        Ok(())
    }

    fn visit_class_set_binary_op_pre(&mut self, op: &ClassSetBinaryOp) -> Result<(), Self::Err> {
        match op.kind {
            ClassSetBinaryOpKind::Difference | ClassSetBinaryOpKind::SymmetricDifference => {
                Err((op.span, JavaIncompatibility::ClassSetOperation))
            }
            ClassSetBinaryOpKind::Intersection => Ok(()),
        }
    }
}

const fn refuse_word_boundary(assertion: &Assertion) -> Result<(), (Span, JavaIncompatibility)> {
    match assertion.kind {
        AssertionKind::StartLine
        | AssertionKind::EndLine
        | AssertionKind::StartText
        | AssertionKind::EndText => Ok(()),
        _ => Err((assertion.span, JavaIncompatibility::WordBoundary)),
    }
}

/// Reads up to the first unescaped `/`, unescaping `\/` on the way and keeping every other
/// escape for the regex engine. Returns the text and what follows the slash.
fn take_until_slash(input: &str) -> Option<(String, &str)> {
    let mut text = String::new();
    let mut chars = input.char_indices();
    while let Some((index, character)) = chars.next() {
        match character {
            '/' => return Some((text, &input[index + 1..])),
            '\\' => match chars.next() {
                Some((_, '/')) => text.push('/'),
                Some((_, escaped)) => {
                    text.push('\\');
                    text.push(escaped);
                }
                None => text.push('\\'),
            },
            other => text.push(other),
        }
    }
    None
}

/// Parses a replacement the way `java.util.regex.Matcher.appendReplacement` reads it: `$N` is a
/// group, taking as many digits as still name an existing group, `${name}` a named group, and
/// `\x` a literal `x`.
///
/// Stricter than Kafka, which writes `$N` literally when N exceeds a nonzero group count: that is
/// refused here. The reverse, `$1\$2`, makes Kafka throw while this maps `alice` to `alice$2`,
/// which is harmless because that Iggy user must still exist.
fn parse_replacement(replacement: &str, finder: &Regex) -> Result<Vec<Piece>, ReplacementError> {
    let group_count = finder.captures_len() - 1;
    let mut pieces = Vec::new();
    let mut literal = String::new();
    let mut chars = replacement.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            '\\' => literal.push(chars.next().ok_or(ReplacementError::DanglingEscape)?),
            '$' => {
                let piece = match chars.next() {
                    Some('{') => {
                        let mut name = String::new();
                        loop {
                            match chars.next() {
                                Some('}') => break,
                                Some(next) => name.push(next),
                                None => return Err(ReplacementError::UnterminatedGroupName),
                            }
                        }
                        if !finder.capture_names().flatten().any(|known| known == name) {
                            return Err(ReplacementError::NoNamedGroup(name));
                        }
                        Piece::Named(name)
                    }
                    Some(digit) if digit.is_ascii_digit() => {
                        let mut group = digit.to_digit(10).map_or(0, |value| value as usize);
                        if group > group_count {
                            return Err(ReplacementError::NoGroup(group));
                        }
                        while let Some(value) = chars.peek().and_then(|next| next.to_digit(10)) {
                            let candidate = group * 10 + value as usize;
                            if candidate > group_count {
                                break;
                            }
                            group = candidate;
                            chars.next();
                        }
                        Piece::Group(group)
                    }
                    _ => return Err(ReplacementError::IllegalGroupReference),
                };
                if !literal.is_empty() {
                    pieces.push(Piece::Literal(std::mem::take(&mut literal)));
                }
                pieces.push(piece);
            }
            other => literal.push(other),
        }
    }
    if !literal.is_empty() {
        pieces.push(Piece::Literal(literal));
    }
    Ok(pieces)
}

/// Java's `Matcher.replaceAll`: after an empty match the search resumes one character later, and
/// an empty match right after a non-empty one still counts. Java steps one UTF-16 unit rather than
/// one character, so the two differ only when an empty match lands before a supplementary-plane
/// character.
fn replace_all(finder: &Regex, subject: &str, replacement: &[Piece]) -> String {
    let mut out = String::with_capacity(subject.len());
    let mut copied_up_to = 0;
    let mut search_from = 0;
    while search_from <= subject.len() {
        let Some(captures) = finder.captures_at(subject, search_from) else {
            break;
        };
        let found = captures
            .get(0)
            .map_or(search_from..search_from, |m| m.range());
        out.push_str(&subject[copied_up_to..found.start]);
        expand_into(&mut out, replacement, &captures);
        copied_up_to = found.end;
        search_from = if found.is_empty() {
            match subject[found.end..].chars().next() {
                Some(next) => found.end + next.len_utf8(),
                None => break,
            }
        } else {
            found.end
        };
    }
    out.push_str(&subject[copied_up_to..]);
    out
}

/// A group that did not participate in the match expands to nothing.
fn expand_into(out: &mut String, replacement: &[Piece], captures: &Captures<'_>) {
    for piece in replacement {
        match piece {
            Piece::Literal(text) => out.push_str(text),
            Piece::Group(group) => {
                if let Some(found) = captures.get(*group) {
                    out.push_str(found.as_str());
                }
            }
            Piece::Named(name) => {
                if let Some(found) = captures.name(name) {
                    out.push_str(found.as_str());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rcgen::{CertificateParams, DistinguishedName, DnType, DnValue, KeyPair};
    use x509_parser::prelude::FromDer;

    use super::*;

    /// `build` fills the subject in certificate order, least specific first, the way a CA writes
    /// it. rcgen seeds every subject with a common name, which would otherwise land first.
    fn subject_of(build: impl FnOnce(&mut CertificateParams)) -> String {
        let key = KeyPair::generate().expect("generate a key");
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("params");
        params.distinguished_name = DistinguishedName::new();
        build(&mut params);
        let certificate = params.self_signed(&key).expect("self-sign");
        subject_distinguished_name(certificate.der()).expect("parse")
    }

    #[test]
    fn given_a_subject_should_render_most_specific_rdn_first_without_spaces() {
        let subject = subject_of(|params| {
            params.distinguished_name.push(DnType::CountryName, "US");
            params
                .distinguished_name
                .push(DnType::OrganizationName, "Example");
            params
                .distinguished_name
                .push(DnType::OrganizationalUnitName, "Eng");
            params.distinguished_name.push(DnType::CommonName, "alice");
        });
        assert_eq!(subject, "CN=alice,OU=Eng,O=Example,C=US");
    }

    #[test]
    fn given_special_characters_should_escape_them_the_way_java_does() {
        let subject = subject_of(|params| {
            params
                .distinguished_name
                .push(DnType::CommonName, "Doe, John+Jr <#1>=\"x\"; \\");
        });
        assert_eq!(subject, r#"CN=Doe\, John\+Jr \<\#1\>\=\"x\"\; \\"#);
    }

    #[test]
    fn given_edge_spaces_should_escape_only_those() {
        let subject = subject_of(|params| {
            params
                .distinguished_name
                .push(DnType::CommonName, DnValue::Utf8String(" a b ".to_string()));
        });
        assert_eq!(subject, r"CN=\ a b\ ");
    }

    #[test]
    fn given_runs_of_edge_spaces_should_escape_every_one_of_them() {
        let subject = subject_of(|params| {
            params
                .distinguished_name
                .push(DnType::CommonName, DnValue::Utf8String("  a  ".to_string()));
        });
        assert_eq!(subject, r"CN=\ \ a\ \ ");
    }

    #[test]
    fn given_a_control_character_should_write_it_raw() {
        let subject = subject_of(|params| {
            params
                .distinguished_name
                .push(DnType::CommonName, DnValue::Utf8String("a\tb".to_string()));
        });
        assert_eq!(subject, "CN=a\tb");
    }

    #[test]
    fn given_a_numeric_string_should_render_it_as_hex() {
        // Name { RDN { CN, NumericString "123" } }; rcgen cannot emit a NumericString.
        let der = [
            0x30, 0x0e, 0x31, 0x0c, 0x30, 0x0a, 0x06, 0x03, 0x55, 0x04, 0x03, 0x12, 0x03, 0x31,
            0x32, 0x33,
        ];
        let (_, name) = X509Name::from_der(&der).expect("a DER name");
        assert_eq!(render_rfc2253(&name), "CN=#1203313233");
    }

    /// `Name { RDN { CN, value } }` around one DER-encoded value, for string types rcgen cannot
    /// emit or contents it refuses.
    fn common_name_with(value: &[u8]) -> String {
        let attribute_len = u8::try_from(5 + value.len()).expect("a short value");
        let mut der = vec![0x30, attribute_len + 4, 0x31, attribute_len + 2];
        der.extend([0x30, attribute_len, 0x06, 0x03, 0x55, 0x04, 0x03]);
        der.extend(value);
        let (_, name) = X509Name::from_der(&der).expect("a DER name");
        render_rfc2253(&name)
    }

    #[test]
    fn given_non_ascii_bytes_in_an_ascii_string_type_should_decode_them_as_latin_1() {
        for tag in [0x13, 0x16, 0x1b] {
            assert_eq!(
                common_name_with(&[tag, 0x03, b'a', 0xe9, b'b']),
                "CN=a\u{e9}b",
                "tag {tag:#04x}"
            );
        }
    }

    #[test]
    fn given_a_universal_string_should_render_it_as_hex() {
        assert_eq!(
            common_name_with(&[0x1c, 0x04, 0x00, 0x00, 0x00, b'a']),
            "CN=#1c0400000061"
        );
    }

    #[test]
    fn given_a_nul_character_should_escape_it_as_two_hex_digits() {
        assert_eq!(
            common_name_with(&[0x0c, 0x03, b'a', 0x00, b'b']),
            "CN=a\\00b"
        );
    }

    #[test]
    fn given_an_attribute_without_a_keyword_should_render_its_oid_and_hex_value() {
        let subject = subject_of(|params| {
            params.distinguished_name.push(
                DnType::CustomDnType(vec![1, 2, 840, 113_549, 1, 9, 1]),
                DnValue::Ia5String("alice@example.com".try_into().expect("ia5")),
            );
            params.distinguished_name.push(DnType::CommonName, "alice");
        });
        // 0x16 is IA5String, 0x11 its length.
        assert_eq!(
            subject,
            "CN=alice,1.2.840.113549.1.9.1=#1611616c696365406578616d706c652e636f6d"
        );
    }

    #[test]
    fn given_the_default_rule_should_keep_the_subject() {
        let rules: PrincipalMappingRules = "DEFAULT".parse().expect("valid");
        assert_eq!(
            rules.map("CN=alice,O=Example").as_deref(),
            Some("CN=alice,O=Example")
        );
        assert_eq!(
            PrincipalMappingRules::default().map("CN=alice").as_deref(),
            Some("CN=alice")
        );
    }

    #[test]
    fn given_kafkas_documented_rules_should_extract_the_common_name() {
        let rules: PrincipalMappingRules =
            "RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/,RULE:^CN=(.*?),OU=(.*?),O=(.*?),L=(.*?),\
             ST=(.*?),C=(.*?)$/$1@$2/L,RULE:^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$/$1/L,DEFAULT"
                .parse()
                .expect("Kafka's own example parses");
        assert_eq!(
            rules
                .map("CN=serviceuser,OU=ServiceUsers,O=Unknown,L=Unknown,ST=Unknown,C=Unknown")
                .as_deref(),
            Some("serviceuser")
        );
        assert_eq!(
            rules
                .map("CN=adminUser,OU=Admin,O=Unknown,L=Unknown,ST=Unknown,C=Unknown")
                .as_deref(),
            Some("adminuser@admin")
        );
        assert_eq!(
            rules.map("CN=kafka,O=Example").as_deref(),
            Some("kafka"),
            "the third rule lowercases the match"
        );
        assert_eq!(
            rules.map("O=Example").as_deref(),
            Some("O=Example"),
            "DEFAULT catches what no rule matched"
        );
    }

    #[test]
    fn given_no_matching_rule_should_map_to_nothing() {
        let rules: PrincipalMappingRules = "RULE:^CN=([^,]+).*$/$1/".parse().expect("valid");
        assert_eq!(rules.map("O=Example"), None);
        assert_eq!(rules.map("CN=alice,O=Example").as_deref(), Some("alice"));
    }

    #[test]
    fn given_a_pattern_that_matches_the_whole_subject_only_should_require_the_whole_match() {
        // `CN=([^,]+)` alone matches part of a longer subject; Java's `matches()` refuses that.
        let rules: PrincipalMappingRules = "RULE:CN=([^,]+)/$1/".parse().expect("valid");
        assert_eq!(rules.map("CN=alice").as_deref(), Some("alice"));
        assert_eq!(rules.map("CN=alice,O=Example"), None);
    }

    #[test]
    fn given_an_escaped_slash_should_keep_it_in_the_pattern_and_the_replacement() {
        let rules: PrincipalMappingRules =
            r"RULE:^CN=svc\/(.*)$/svc-$1\/x/U".parse().expect("valid");
        assert_eq!(rules.map("CN=svc/alice").as_deref(), Some("SVC-ALICE/X"));
    }

    #[test]
    fn given_java_replacement_syntax_should_expand_groups_and_escapes() {
        let rules: PrincipalMappingRules = r"RULE:^CN=(\w+),OU=(\w+)$/$1-$2x\$lit\\/"
            .parse()
            .expect("valid");
        assert_eq!(
            rules.map("CN=alice,OU=eng").as_deref(),
            Some(r"alice-engx$lit\")
        );
        let named: PrincipalMappingRules = r"RULE:^CN=(?P<user>\w+).*$/${user}/"
            .parse()
            .expect("valid");
        assert_eq!(named.map("CN=bob,O=x").as_deref(), Some("bob"));
    }

    #[test]
    fn given_a_pattern_that_also_matches_empty_should_replace_like_java_replace_all() {
        let rules: PrincipalMappingRules = "RULE:(.*)/$1@x/".parse().expect("valid");
        assert_eq!(rules.map("alice").as_deref(), Some("alice@x@x"));
    }

    #[test]
    fn given_more_digits_than_groups_should_take_the_longest_existing_group() {
        let rules: PrincipalMappingRules = "RULE:^CN=(\\w+)$/$12/".parse().expect("valid");
        assert_eq!(rules.map("CN=alice").as_deref(), Some("alice2"));
    }

    #[test]
    fn given_a_group_beyond_the_patterns_count_should_refuse_what_kafka_writes_literally() {
        match "RULE:^CN=(.*)$/$2/".parse::<PrincipalMappingRules>() {
            Err(MappingRuleError::InvalidReplacement { source, .. }) => {
                assert_eq!(source, ReplacementError::NoGroup(2));
            }
            other => panic!("Kafka maps this to '$2'; the gateway refuses it, got {other:?}"),
        }
    }

    #[test]
    fn given_an_escaped_dollar_after_a_group_should_write_it_literally() {
        let rules: PrincipalMappingRules = "RULE:^CN=(.*)$/$1\\$2/".parse().expect("valid");
        assert_eq!(rules.map("CN=alice").as_deref(), Some("alice$2"));
    }

    #[test]
    fn given_a_replacement_java_would_throw_on_should_refuse_the_rule() {
        for (raw, expected) in [
            (
                "RULE:^CN=(.*)$/$x/",
                ReplacementError::IllegalGroupReference,
            ),
            (
                "RULE:^CN=(.*)$/a$/",
                ReplacementError::IllegalGroupReference,
            ),
            (
                "RULE:^CN=(?P<user>.*)$/${nope}/",
                ReplacementError::NoNamedGroup("nope".to_string()),
            ),
            (
                "RULE:^CN=(?P<user>.*)$/${user/",
                ReplacementError::UnterminatedGroupName,
            ),
        ] {
            match raw.parse::<PrincipalMappingRules>() {
                Err(MappingRuleError::InvalidReplacement { source, .. }) => {
                    assert_eq!(source, expected, "for '{raw}'");
                }
                other => panic!("'{raw}' must be refused for its replacement, got {other:?}"),
            }
        }
    }

    #[test]
    fn given_a_malformed_rule_list_should_refuse() {
        for raw in [
            "",
            "RULE:",
            "RULE:^CN=(.*)$",
            "RULE:^CN=(.*)$/$1",
            "RULE:^CN=(.*)$/$1/X",
            "DEFAULT,",
            "DEFAULT DEFAULT",
            "rule:a/b/",
            "RULE:^CN=([/$1/",
        ] {
            assert!(
                raw.parse::<PrincipalMappingRules>().is_err(),
                "'{raw}' must be rejected"
            );
        }
    }

    #[test]
    fn given_a_kelvin_sign_should_not_match_an_ascii_word_class() {
        let rules: PrincipalMappingRules = r"RULE:^CN=(\w+)$/$1/L".parse().expect("valid");
        assert_eq!(rules.map("CN=\u{212A}afka"), None);
        assert_eq!(rules.map("CN=Kafka").as_deref(), Some("kafka"));
        let bracketed: PrincipalMappingRules = r"RULE:^CN=([\w.]+)$/$1/".parse().expect("valid");
        assert_eq!(bracketed.map("CN=\u{212A}afka"), None);
        assert_eq!(bracketed.map("CN=k.afka").as_deref(), Some("k.afka"));
    }

    #[test]
    fn given_non_ascii_digits_or_spaces_should_not_match_ascii_classes() {
        let digits: PrincipalMappingRules = r"RULE:^CN=u(\d+)$/$1/".parse().expect("valid");
        assert_eq!(digits.map("CN=u\u{0661}\u{0662}"), None);
        assert_eq!(digits.map("CN=u12").as_deref(), Some("12"));
        let spaces: PrincipalMappingRules = r"RULE:^CN=a\sb$/ab/".parse().expect("valid");
        assert_eq!(spaces.map("CN=a\u{00A0}b"), None);
        assert_eq!(spaces.map("CN=a b").as_deref(), Some("ab"));
        let negated: PrincipalMappingRules = r"RULE:^CN=(\D+)$/$1/".parse().expect("valid");
        assert_eq!(
            negated.map("CN=\u{0661}").as_deref(),
            Some("\u{0661}"),
            "Java's \\D matches every non-ASCII digit"
        );
    }

    #[test]
    fn given_a_java_line_terminator_should_not_match_dot_outside_dotall() {
        let rules: PrincipalMappingRules = "RULE:^CN=(.*)$/$1/".parse().expect("valid");
        for terminator in ['\r', '\u{85}', '\u{2028}', '\u{2029}'] {
            assert_eq!(rules.map(&format!("CN=a{terminator}b")), None);
        }
        let dot_all: PrincipalMappingRules = "RULE:(?s)^CN=(.*)$/$1/".parse().expect("valid");
        assert_eq!(dot_all.map("CN=a\u{2028}b").as_deref(), Some("a\u{2028}b"));
        let scoped: PrincipalMappingRules = "RULE:^CN=(?s:(.))(.)$/$1$2/".parse().expect("valid");
        assert_eq!(scoped.map("CN=\u{85}x").as_deref(), Some("\u{85}x"));
        assert_eq!(scoped.map("CN=x\u{85}"), None, "(?s:) ends with its group");
    }

    #[test]
    fn given_a_non_ascii_name_should_still_map_like_kafka() {
        for raw in ["RULE:^CN=([^,]+).*$/$1/", "RULE:^CN=(.*)$/$1/"] {
            let rules: PrincipalMappingRules = raw.parse().expect("valid");
            assert_eq!(
                rules.map("CN=Zo\u{eb}").as_deref(),
                Some("Zo\u{eb}"),
                "{raw}"
            );
            assert_eq!(
                rules.map("CN=Jos\u{e9}").as_deref(),
                Some("Jos\u{e9}"),
                "{raw}"
            );
        }
    }

    /// Asserts each rule list is refused, naming `construct` for `reason`.
    fn assert_refused(cases: &[(&str, &str, JavaIncompatibility)]) {
        for &(raw, construct, expected) in cases {
            match raw.parse::<PrincipalMappingRules>() {
                Err(MappingRuleError::JavaIncompatible {
                    construct: found,
                    reason,
                    ..
                }) => {
                    assert_eq!((found.as_str(), reason), (construct, expected), "{raw}");
                }
                other => panic!("'{raw}' must be refused, got {other:?}"),
            }
        }
    }

    #[test]
    fn given_a_construct_without_java_semantics_should_refuse_the_rule() {
        assert_refused(&[
            (
                "RULE:(?i)^CN=(\u{17F}vc)$/$1/",
                "i",
                JavaIncompatibility::CaseInsensitive,
            ),
            (
                "RULE:^CN=(?i:kafka)$/$1/",
                "i",
                JavaIncompatibility::CaseInsensitive,
            ),
            (
                "RULE:(?u)^CN=(.*)$/$1/",
                "u",
                JavaIncompatibility::UnicodeFlag,
            ),
            (
                "RULE:(?U)^CN=(.*)$/$1/",
                "U",
                JavaIncompatibility::UnicodeFlag,
            ),
            (
                r"RULE:^CN=(\pL+)$/$1/",
                r"\pL",
                JavaIncompatibility::UnicodeClass,
            ),
            (
                r"RULE:^CN=([\p{Greek}a]+)$/$1/",
                r"\p{Greek}",
                JavaIncompatibility::UnicodeClass,
            ),
            (
                "RULE:^CN=([[:alpha:]]+)$/$1/",
                "[:alpha:]",
                JavaIncompatibility::PosixClass,
            ),
            (
                r"RULE:^CN=\b(.*)$/$1/",
                r"\b",
                JavaIncompatibility::WordBoundary,
            ),
            (
                r"RULE:^CN=(.*)\B$/$1/",
                r"\B",
                JavaIncompatibility::WordBoundary,
            ),
        ]);
        let case_sensitive: PrincipalMappingRules = "RULE:(?-i)^CN=(.*)$/$1/"
            .parse()
            .expect("(?-i) is Java's default");
        assert_eq!(case_sensitive.map("CN=a").as_deref(), Some("a"));
    }

    #[test]
    fn given_syntax_rust_and_java_read_differently_should_refuse_the_rule() {
        assert_refused(&[
            (
                "RULE:(?m)^CN=(.*)$/$1/",
                "m",
                JavaIncompatibility::LineOrWhitespaceFlag,
            ),
            (
                "RULE:(?x)^CN=(.*)$/$1/",
                "x",
                JavaIncompatibility::LineOrWhitespaceFlag,
            ),
            (
                "RULE:^CN=(?R:.*)$/CN/",
                "R",
                JavaIncompatibility::LineOrWhitespaceFlag,
            ),
            (
                r"RULE:^CN=([^\v,]+)$/$1/",
                r"\v",
                JavaIncompatibility::VerticalWhitespace,
            ),
            (
                r"RULE:^CN=(a\v)$/$1/",
                r"\v",
                JavaIncompatibility::VerticalWhitespace,
            ),
            (
                "RULE:^CN=(.*+)$/$1/",
                "+",
                JavaIncompatibility::StackedQuantifier,
            ),
            (
                "RULE:^CN=(a{2}+)$/$1/",
                "+",
                JavaIncompatibility::StackedQuantifier,
            ),
            (
                "RULE:^CN=([a-z--b]+)$/$1/",
                "a-z--b",
                JavaIncompatibility::ClassSetOperation,
            ),
            (
                "RULE:^CN=([a-z~~b]+)$/$1/",
                "a-z~~b",
                JavaIncompatibility::ClassSetOperation,
            ),
        ]);
        // Turning a flag off keeps Java's default, so it stays admissible.
        let explicit_defaults: PrincipalMappingRules = "RULE:(?-mx)^CN=([^,]+)$/$1/"
            .parse()
            .expect("(?-mx) is Java's default");
        assert_eq!(explicit_defaults.map("CN=a").as_deref(), Some("a"));
        // A lazy quantifier and a class intersection mean the same in both.
        let same_in_both: PrincipalMappingRules = "RULE:^CN=([a-z&&[^q]]+?)$/$1/"
            .parse()
            .expect("lazy quantifiers and && match Java");
        assert_eq!(same_in_both.map("CN=abc").as_deref(), Some("abc"));
    }
}
