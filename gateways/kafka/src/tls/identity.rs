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
//! their rules against that rendering, so this one matches it rather than x509-parser's own
//! `Display`, which keeps the certificate's order and pads with spaces.

use std::fmt::Write as _;
use std::str::FromStr;

use regex::{Captures, Regex};
use thiserror::Error;
use x509_parser::asn1_rs::{Any, Tag, ToDer};
use x509_parser::parse_x509_certificate;
use x509_parser::x509::{AttributeTypeAndValue, RelativeDistinguishedName, X509Name};

/// Why a client certificate yielded no principal.
#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("the client certificate could not be parsed: {0}")]
    Certificate(String),
    #[error("no principal mapping rule matches the certificate subject '{subject}'")]
    NoMatchingRule { subject: String },
}

/// The subject of a DER certificate, as Kafka's principal builder sees it.
///
/// # Errors
///
/// Returns [`IdentityError::Certificate`] when `certificate` is not a DER X.509 certificate.
pub fn subject_distinguished_name(certificate: &[u8]) -> Result<String, IdentityError> {
    let (_, parsed) = parse_x509_certificate(certificate)
        .map_err(|error| IdentityError::Certificate(error.to_string()))?;
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

/// `KEYWORD=value` for the attribute types RFC 2253 and Java give a keyword, `OID=#hex` for the
/// rest, and `#hex` of the DER for any value that is not a printable string. Java hex-encodes
/// in exactly those cases, and a rule written against its output has to keep matching here.
fn render_attribute(out: &mut String, attribute: &AttributeTypeAndValue<'_>) {
    let value = attribute.attr_value();
    if let Some(keyword) = rfc2253_keyword(&attribute.attr_type().to_string()) {
        out.push_str(keyword);
        out.push('=');
        match string_value(value) {
            Some(text) if !text.chars().any(char::is_control) => escape_into(out, &text),
            _ => push_hex(out, value),
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

/// The text of a directory string, for the string types Java decodes rather than hex-encodes.
fn string_value(value: &Any<'_>) -> Option<String> {
    match value.tag() {
        Tag::Utf8String | Tag::PrintableString | Tag::Ia5String | Tag::NumericString => {
            std::str::from_utf8(value.data).ok().map(str::to_owned)
        }
        Tag::BmpString => {
            let units: Option<Vec<u16>> = value
                .data
                .chunks(2)
                .map(|pair| <[u8; 2]>::try_from(pair).ok().map(u16::from_be_bytes))
                .collect();
            String::from_utf16(&units?).ok()
        }
        // T.61 is close enough to Latin-1 for the subjects that still carry it.
        Tag::T61String => Some(value.data.iter().map(|&byte| char::from(byte)).collect()),
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

/// Backslash-escapes what Java's RFC 2253 rendering escapes: `,=+<>#;"\` anywhere, and a space
/// at either end.
fn escape_into(out: &mut String, text: &str) {
    let last = text.chars().count().saturating_sub(1);
    for (index, character) in text.chars().enumerate() {
        let edge_space = character == ' ' && (index == 0 || index == last);
        if edge_space
            || matches!(
                character,
                ',' | '=' | '+' | '<' | '>' | '#' | ';' | '"' | '\\'
            )
        {
            out.push('\\');
        }
        out.push(character);
    }
}

/// Kafka's `ssl.principal.mapping.rules`: `RULE:pattern/replacement/[LU]` and `DEFAULT`, comma
/// separated, tried in order. The first rule whose pattern matches the whole subject decides.
#[derive(Debug, Clone)]
pub struct PrincipalMappingRules {
    rules: Vec<Rule>,
}

#[derive(Debug, Clone)]
enum Rule {
    /// The subject string itself.
    Default,
    Rewrite {
        /// The operator's pattern anchored at both ends, since Java's `matches()` demands the
        /// whole subject.
        whole: Regex,
        /// The pattern as written, for the replacement pass. Java runs `replaceAll` over the
        /// subject with it unanchored, and `a|ab` on `ab` replaces `a` alone that way.
        finder: Regex,
        replacement: String,
        case: Case,
    },
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
                let rewritten = finder.replace_all(subject, |captures: &Captures<'_>| {
                    expand_replacement(replacement, captures)
                });
                Some(match case {
                    Case::Keep => rewritten.into_owned(),
                    Case::Lower => rewritten.to_lowercase(),
                    Case::Upper => rewritten.to_uppercase(),
                })
            }
        })
    }
}

impl FromStr for PrincipalMappingRules {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let mut rules = Vec::new();
        let mut rest = raw.trim_start();
        if rest.is_empty() {
            return Err("the rule list is empty; the default is DEFAULT".to_string());
        }
        while !rest.is_empty() {
            if let Some(after) = rest.strip_prefix("DEFAULT") {
                rules.push(Rule::Default);
                rest = after;
            } else if let Some(after) = rest.strip_prefix("RULE:") {
                let (pattern, after) = take_until_slash(after)
                    .ok_or_else(|| format!("rule '{rest}' has no '/' after its pattern"))?;
                let (replacement, after) = take_until_slash(after)
                    .ok_or_else(|| format!("rule '{rest}' has no '/' after its replacement"))?;
                let (case, after) = match after.chars().next() {
                    Some('L') => (Case::Lower, &after[1..]),
                    Some('U') => (Case::Upper, &after[1..]),
                    _ => (Case::Keep, after),
                };
                let finder = Regex::new(&pattern).map_err(|error| {
                    format!("rule pattern '{pattern}' is not a valid regex: {error}")
                })?;
                let whole = Regex::new(&format!("^(?:{pattern})$")).map_err(|error| {
                    format!("rule pattern '{pattern}' is not a valid regex: {error}")
                })?;
                rules.push(Rule::Rewrite {
                    whole,
                    finder,
                    replacement,
                    case,
                });
                rest = after;
            } else {
                return Err(format!(
                    "'{rest}' is neither DEFAULT nor RULE:pattern/replacement/[LU]"
                ));
            }
            rest = rest.trim_start();
            match rest.strip_prefix(',') {
                Some(after) => {
                    rest = after.trim_start();
                    if rest.is_empty() {
                        return Err("the rule list ends with a comma".to_string());
                    }
                }
                None if rest.is_empty() => {}
                None => {
                    return Err(format!(
                        "expected ',' or the end of the list before '{rest}'"
                    ));
                }
            }
        }
        Ok(Self { rules })
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

/// Expands a replacement the way `java.util.regex.Matcher` does: `$N` is a group, taking as
/// many digits as still name an existing group, `${name}` a named group, and `\x` a literal
/// `x`. A group that did not participate expands to nothing.
fn expand_replacement(replacement: &str, captures: &Captures<'_>) -> String {
    let mut out = String::new();
    let mut chars = replacement.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            '\\' => {
                if let Some(escaped) = chars.next() {
                    out.push(escaped);
                }
            }
            '$' => match chars.peek() {
                Some('{') => {
                    chars.next();
                    let name: String = chars.by_ref().take_while(|&c| c != '}').collect();
                    if let Some(found) = captures.name(&name) {
                        out.push_str(found.as_str());
                    }
                }
                Some(digit) if digit.is_ascii_digit() => {
                    let mut group = 0usize;
                    let mut taken_any = false;
                    while let Some(&next) = chars.peek() {
                        let Some(value) = next.to_digit(10) else {
                            break;
                        };
                        let candidate = group * 10 + value as usize;
                        if taken_any && candidate >= captures.len() {
                            break;
                        }
                        group = candidate;
                        taken_any = true;
                        chars.next();
                    }
                    if let Some(found) = captures.get(group) {
                        out.push_str(found.as_str());
                    }
                }
                _ => out.push('$'),
            },
            other => out.push(other),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use rcgen::{CertificateParams, DistinguishedName, DnType, DnValue, KeyPair};

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
}
