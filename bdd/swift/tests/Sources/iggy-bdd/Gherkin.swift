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

/// One step of a scenario: the keyword as written and the text after it.
struct Step: Sendable {
    let keyword: String
    let text: String
    let line: Int
}

struct Scenario: Sendable {
    let name: String
    let steps: [Step]
}

struct Feature: Sendable {
    let name: String
    let background: [Step]
    let scenarios: [Scenario]
}

/// Parses the Gherkin the shared scenarios use: a feature with an optional
/// background, scenarios, and Given/When/Then/And/But steps. Tags and the
/// prose under a heading are skipped. A construct it cannot run, such as a
/// scenario outline, fails the parse rather than running part of it.
enum GherkinParser {
    static let stepKeywords = ["Given", "When", "Then", "And", "But"]
    static let unsupportedKeywords = ["Scenario Outline:", "Scenario Template:", "Examples:", "Scenarios:", "Rule:", "\"\"\"", "|"]

    static func parse(_ text: String) throws -> Feature {
        var featureName: String?
        var background: [Step] = []
        var scenarios: [Scenario] = []
        var current: (name: String, steps: [Step])?
        var inBackground = false

        for (index, rawLine) in text.split(separator: "\n", omittingEmptySubsequences: false).enumerated() {
            let line = rawLine.trimmingCharacters(in: .whitespaces)
            let number = index + 1
            if line.isEmpty || line.hasPrefix("#") || line.hasPrefix("@") {
                continue
            }
            if let keyword = unsupportedKeywords.first(where: { line.hasPrefix($0) }) {
                throw ParseError(line: number, message: "\(keyword) is not supported by this runner")
            }
            if let name = heading("Feature:", line) {
                featureName = name
            } else if heading("Background:", line) != nil {
                inBackground = true
            } else if let name = heading("Scenario:", line) ?? heading("Example:", line) {
                if let current {
                    scenarios.append(Scenario(name: current.name, steps: current.steps))
                }
                inBackground = false
                current = (name, [])
            } else if let keyword = stepKeywords.first(where: { line.hasPrefix($0 + " ") }) {
                let step = Step(keyword: keyword, text: String(line.dropFirst(keyword.count + 1)).trimmingCharacters(in: .whitespaces), line: number)
                if inBackground {
                    background.append(step)
                } else if current != nil {
                    current?.steps.append(step)
                } else {
                    throw ParseError(line: number, message: "step outside a scenario")
                }
            }
        }
        if let current {
            scenarios.append(Scenario(name: current.name, steps: current.steps))
        }
        guard let featureName else {
            throw ParseError(line: 0, message: "no Feature: heading")
        }
        return Feature(name: featureName, background: background, scenarios: scenarios)
    }

    private static func heading(_ keyword: String, _ line: String) -> String? {
        guard line.hasPrefix(keyword) else {
            return nil
        }
        return line.dropFirst(keyword.count).trimmingCharacters(in: .whitespaces)
    }

    struct ParseError: Error, CustomStringConvertible {
        let line: Int
        let message: String
        var description: String { "line \(line): \(message)" }
    }
}
