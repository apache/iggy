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

use std::env;
use std::io::{self, IsTerminal, Write};

use figlet_rs::FIGlet;

const WIDTH: usize = 64;

const IGGY: &str = r"
                                     ⣀⣠⣤⣤⣤⣤⣤⣾⣛⠛⠓⠶⣄
                          ⣠⡴⠞⠛⠋⠛⠛⠲⢶⠶⠛⠉⠁      ⠉⠙⠻⣷⣜⣧
                         ⣴⠋⣠⠴⣶⣿⣶⣶⣦⠄            ⢀⣈⣿⡽⣆
                        ⣸⠇⠜⠁⡞⠉⠈⠹⣿⠃   ⢠⠴⣶⢦⡀     ⢪⣨⣆⢳⡹⣆
                       ⣰⠏  ⢸⡇   ⠉    ⢿⣦⣾⠆⠇     ⠈⢻⡃⣼⣿⣿⣷⡄
                     ⢀⣴⠏ ⢀⣠⡾⠛⠶⠶⣦⡀     ⠉⠉         ⠙⢿⣉⠉⠁
                     ⠈⠙⠛⠛⠋⠁    ⢸⣷                  ⠈⢳⣄
                               ⢸⡏⢣⡀             ⢀⡴⢊⣁⣀⣬⣵
                               ⣼⡇    ⠸⣿⣷⣦⣄      ⠘⣷⣿⣻⣿⣿⣿⡀
                               ⣿⠁  ⠠⡀ ⢻⣜⢿⣿⣿⣶⣤⣄⣀⡀ ⠈⠙⣛⣿⡟ ⠙⠲⣄
                               ⣿    ⢙⢦⣄⠙⣮⡻⣿⣿⣿⣿⣿⣿⣏⠉⠉⠉ ⠙⢦⡀ ⠈⠳⣄
       ⢀⣀⣀                     ⣿     ⠙⣿⣷⣌⠛⢮⣿⣿⣿⣿⣿⣿⣧⡀    ⠙⢦⡀ ⠈⠳⡄
       ⠸⣄⡼ ⢰⡋⢹                 ⣿      ⠈⢻⣿⣷⣄⠉⢻⡟⣿⣿⣿⣿⣷⡀     ⠙⢆  ⠘⢦
  ⣠⠶⢄    ⠱⡄ ⠉⣇                ⢰⡇        ⢻⣿⣿⣷⣦⣻⡄ ⣮⠙⠘⣇      ⠈⢳⡀ ⠈⢳⡀
  ⠙⠦⠞⠢⢄⡀  ⠘⢦⡀⠘⣆               ⢸⡇         ⢿⣿⣿⡏⠛⠻⣦⣘⣂⣠⡟        ⠹⡄  ⢷
       ⠈⠑⠢⢤⣀⠙⠦⣌⠑⠆⢠⣄⡀      ⣠⣤⣤⣄⣾⠁         ⠘⣿⢹⡇  ⠈⠉⠉⠉          ⢳  ⠘⡇
   ⡤⢤⣀⣀⣠⣤⣄⣀⣈⠙⠲⢬⠉⢠⠏ ⠉⠓⠦⣤⣶⠷⡾⠋⠁⠉⠻⣿⣶⣤⡀        ⢿⠈⢿⡀    ⢀⣤⣤⣤⣀      ⢸⡇  ⣷
  ⠈⠧⠴⠃   ⣀⣠⣬⣉⣓⠂⢠⠏    ⣰⡿⠁ ⡀     ⠈⠹⣷        ⢸⡇⠘⣷⣠⣾⠿⠻⠿⠋ ⠉⢻⣶⣶⣄   ⣸⠁  ⡏
      ⢰⡉⢹    ⠈⢠⣏⠠⢄⣀  ⣿⠃ ⡼  ⣰⠁  ⡀ ⣿⣇        ⢇ ⢸⣿⠃        ⠈⢻⣧⢀⡴⠃  ⣸⠃
       ⠉⠁      ⠉⠛⠶⢮⣄⣒⣿ ⢰⠇ ⢰⡇  ⣸⠁ ⣿⡏⠙⠛⠲⠶⢤⣤⣀⣀⣀⣀⣸⣿  ⡆  ⢣  ⢳  ⣿⡏  ⢀⡴⠃
                   ⠈⠉⠻⣷⣾  ⣾  ⢀⡟ ⣸⡟        ⠉⠉⠉⠛⣿  ⣷  ⢸⡆ ⢸⡇ ⣿⡇⣀⠴⠋
                       ⢹⣆⣠⣿  ⣼⡿⠿⠿⢤⣤⣄⣀⣀⡀       ⢿⣆⣀⣿  ⢸⡇ ⢀⣇⣠⣿⠋⠁
                       ⠈⠙⠛⠻⣶⣾⠟      ⠈⠉⠉⠉⠛⠛⠛⠛⠒⠒⠚⠛⠿⢿⡄ ⣸⡇⢀⣼⠿⠟⠁
                                                 ⠈⢿⣶⠟⠛⠿⠋
";

pub fn print(version: &str) {
    let stdout = io::stdout();
    let color = stdout.is_terminal()
        && env::var_os("TERM").is_none_or(|term| term != "dumb")
        && env::var_os("NO_COLOR").is_none_or(|value| value.is_empty());
    let _ = render(&mut stdout.lock(), version, color);
}

fn render(output: &mut impl Write, version: &str, color: bool) -> io::Result<()> {
    let wordmark = FIGlet::standard()
        .ok()
        .and_then(|font| font.convert("Iggy").map(|figure| figure.to_string()))
        .unwrap_or_else(|| "Iggy".to_owned());
    let width = wordmark.lines().map(str::len).max().unwrap_or_default();
    let padding = 2 + WIDTH.saturating_sub(width) / 2;
    let (orange, reset) = if color {
        ("\x1b[38;5;208m", "\x1b[0m")
    } else {
        ("", "")
    };

    writeln!(output, "{IGGY}")?;
    for line in wordmark.lines() {
        writeln!(output, "{orange}{:padding$}{}{reset}", "", line.trim_end())?;
    }
    writeln!(output)?;
    writeln!(output, "  {:^WIDTH$}", format!("Apache Iggy v{version}"))?;
    writeln!(output)
}
