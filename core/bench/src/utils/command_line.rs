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

use crate::args::common::IggyBenchArgs;
use clap::{Arg, Command, CommandFactory};
use std::borrow::Cow;
use std::ffi::OsStr;
use std::path::Path;

const PASSWORD_ARG_ID: &str = "password";
const REDACTED_VALUE: &str = "***";
const END_OF_OPTIONS: &str = "--";
const LONG_OPTION_PREFIX: &str = "--";
const SHORT_OPTION_PREFIX: char = '-';
const INLINE_VALUE_SEPARATOR: char = '=';
/// Characters that a POSIX shell never interprets, besides ASCII letters and digits.
const SHELL_SAFE_CHARS: &str = "@%+=:,./-_";

/// The arguments as typed, for a report: the program reduced to its file name, the
/// password value replaced by `***`, and each argument quoted for a POSIX shell when needed.
pub fn from_argv(argv: impl IntoIterator<Item = impl AsRef<OsStr>>) -> String {
    let mut arguments: Vec<String> = argv
        .into_iter()
        .map(|argument| argument.as_ref().to_string_lossy().into_owned())
        .collect();
    if let Some(program) = arguments.first_mut()
        && let Some(file_name) = Path::new(program.as_str())
            .file_name()
            .map(|file_name| file_name.to_string_lossy().into_owned())
    {
        *program = file_name;
    }
    redact_password(&IggyBenchArgs::command(), &mut arguments);
    arguments
        .iter()
        .map(|argument| shell_quote(argument))
        .collect::<Vec<_>>()
        .join(" ")
}

enum ValuePosition {
    /// Byte offset of the value inside the option token.
    Inline(usize),
    NextToken,
}

/// Only the top level has the password option, and top-level options come before the kind
/// subcommand. Inside a kind, `-p` is `--producers`.
fn redact_password(command: &Command, arguments: &mut [String]) {
    let mut index = 1;
    while let Some(token) = arguments.get(index) {
        if token == END_OF_OPTIONS || command.find_subcommand(token).is_some() {
            return;
        }
        let Some((arg, position)) = option_value(command, token) else {
            index += 1;
            continue;
        };
        let is_password = arg.get_id() == PASSWORD_ARG_ID;
        match position {
            ValuePosition::Inline(start) => {
                if is_password {
                    arguments[index].replace_range(start.., REDACTED_VALUE);
                }
                index += 1;
            }
            ValuePosition::NextToken => {
                if is_password && let Some(value) = arguments.get_mut(index + 1) {
                    REDACTED_VALUE.clone_into(value);
                }
                index += 2;
            }
        }
    }
}

/// The value-taking option of a token and where its value is, following clap: a long
/// option takes `--name=value` or the next token, and in a short cluster such as `-Hpx` the
/// first value-taking flag takes the rest of the token, without a leading `=`, or the next
/// token when nothing is left.
fn option_value<'a>(command: &'a Command, token: &str) -> Option<(&'a Arg, ValuePosition)> {
    if let Some(long) = token.strip_prefix(LONG_OPTION_PREFIX) {
        let (name, has_inline_value) = long
            .split_once(INLINE_VALUE_SEPARATOR)
            .map_or((long, false), |(name, _)| (name, true));
        let arg = command
            .get_arguments()
            .find(|arg| has_long_name(arg, name))?;
        let position = if has_inline_value {
            ValuePosition::Inline(LONG_OPTION_PREFIX.len() + name.len() + 1)
        } else {
            ValuePosition::NextToken
        };
        return arg.get_action().takes_values().then_some((arg, position));
    }

    let flags = token.strip_prefix(SHORT_OPTION_PREFIX)?;
    for (offset, flag) in flags.char_indices() {
        let arg = command
            .get_arguments()
            .find(|arg| has_short_name(arg, flag))?;
        if !arg.get_action().takes_values() {
            continue;
        }
        let value_start = SHORT_OPTION_PREFIX.len_utf8() + offset + flag.len_utf8();
        let position = if token[value_start..].starts_with(INLINE_VALUE_SEPARATOR) {
            ValuePosition::Inline(value_start + 1)
        } else if value_start == token.len() {
            ValuePosition::NextToken
        } else {
            ValuePosition::Inline(value_start)
        };
        return Some((arg, position));
    }
    None
}

fn has_long_name(arg: &Arg, name: &str) -> bool {
    arg.get_long() == Some(name)
        || arg
            .get_all_aliases()
            .is_some_and(|aliases| aliases.contains(&name))
}

fn has_short_name(arg: &Arg, flag: char) -> bool {
    arg.get_short() == Some(flag)
        || arg
            .get_all_short_aliases()
            .is_some_and(|aliases| aliases.contains(&flag))
}

fn shell_quote(argument: &str) -> Cow<'_, str> {
    let is_safe =
        |character: char| character.is_ascii_alphanumeric() || SHELL_SAFE_CHARS.contains(character);
    if !argument.is_empty() && argument.chars().all(is_safe) {
        Cow::Borrowed(argument)
    } else {
        Cow::Owned(format!("'{}'", argument.replace('\'', r"'\''")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_each_password_spelling_when_rendering_should_redact_only_the_value() {
        let cases: &[(&[&str], &str)] = &[
            (
                &["iggy-bench", "-p", "secret", "pp", "tcp"],
                "iggy-bench -p '***' pp tcp",
            ),
            (
                &["iggy-bench", "-psecret", "pp", "tcp"],
                "iggy-bench '-p***' pp tcp",
            ),
            (
                &["iggy-bench", "-p=secret", "pp", "tcp"],
                "iggy-bench '-p=***' pp tcp",
            ),
            (
                &["iggy-bench", "-Hp", "secret", "pp", "tcp"],
                "iggy-bench -Hp '***' pp tcp",
            ),
            (
                &["iggy-bench", "-Hpsecret", "pp", "tcp"],
                "iggy-bench '-Hp***' pp tcp",
            ),
            (
                &["iggy-bench", "--password", "secret", "pp", "tcp"],
                "iggy-bench --password '***' pp tcp",
            ),
            (
                &["iggy-bench", "--password=secret", "pp", "tcp"],
                "iggy-bench '--password=***' pp tcp",
            ),
        ];
        for (argv, expected) in cases {
            assert_eq!(from_argv(*argv), *expected, "{argv:?}");
        }
    }

    #[test]
    fn given_kind_option_with_password_short_flag_when_rendering_should_keep_its_value() {
        assert_eq!(
            from_argv(["iggy-bench", "pp", "-p", "20", "tcp"]),
            "iggy-bench pp -p 20 tcp"
        );
        // `pp` as the value of `--username` isn't the kind subcommand.
        assert_eq!(
            from_argv([
                "iggy-bench",
                "--username",
                "pp",
                "-p",
                "secret",
                "pp",
                "-p",
                "20",
                "tcp"
            ]),
            "iggy-bench --username pp -p '***' pp -p 20 tcp"
        );
        assert_eq!(
            from_argv(["iggy-bench", "--", "-p", "secret"]),
            "iggy-bench -- -p secret"
        );
    }

    #[test]
    fn given_arguments_that_need_quoting_when_rendering_should_quote_only_those() {
        assert_eq!(
            from_argv([
                "/opt/iggy/target/release/iggy-bench",
                "--rate-limit",
                "800 MB",
                "pp",
                "tcp",
                "output",
                "--remark",
                "it's",
                "--extra-info",
                "",
                "--workload",
                "producer-20p-persisted@800MB",
            ]),
            r"iggy-bench --rate-limit '800 MB' pp tcp output --remark 'it'\''s' --extra-info '' --workload producer-20p-persisted@800MB"
        );
    }
}
