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

import Foundation

/// One step of a scenario: the keyword as written and the text after it.
struct Step: Sendable {
    let keyword: String
    let text: String
    let line: Int
}

struct Scenario: Sendable {
    let name: String
    let tags: [String]
    let steps: [Step]
}

struct Feature: Sendable {
    let name: String
    let tags: [String]
    let background: [Step]
    let scenarios: [Scenario]
}

/// Parses the subset of Gherkin the shared scenarios use: tags, a feature
/// with a background, scenarios, and Given/When/Then/And/But steps.
/// Description lines under a heading are skipped.
enum GherkinParser {
    static let stepKeywords = ["Given", "When", "Then", "And", "But"]

    static func parse(_ text: String) throws -> Feature {
        var featureName = ""
        var featureTags: [String] = []
        var pendingTags: [String] = []
        var background: [Step] = []
        var scenarios: [Scenario] = []
        var current: (name: String, tags: [String], steps: [Step])?
        var inBackground = false
        var sawFeature = false

        for (index, rawLine) in text.split(separator: "\n", omittingEmptySubsequences: false).enumerated() {
            let line = rawLine.trimmingCharacters(in: .whitespaces)
            let number = index + 1
            if line.isEmpty || line.hasPrefix("#") {
                continue
            }
            if line.hasPrefix("@") {
                pendingTags.append(contentsOf: line.split(separator: " ").map(String.init))
                continue
            }
            if line.hasPrefix("Feature:") {
                featureName = String(line.dropFirst("Feature:".count)).trimmingCharacters(in: .whitespaces)
                featureTags = pendingTags
                pendingTags = []
                sawFeature = true
                continue
            }
            if line.hasPrefix("Background:") {
                inBackground = true
                continue
            }
            if line.hasPrefix("Scenario:") || line.hasPrefix("Scenario Outline:") {
                if let current {
                    scenarios.append(Scenario(name: current.name, tags: current.tags, steps: current.steps))
                }
                inBackground = false
                let name = String(line[line.index(after: line.firstIndex(of: ":")!)...]).trimmingCharacters(in: .whitespaces)
                current = (name, pendingTags, [])
                pendingTags = []
                continue
            }
            if let keyword = stepKeywords.first(where: { line.hasPrefix($0 + " ") }) {
                let step = Step(keyword: keyword, text: String(line.dropFirst(keyword.count + 1)).trimmingCharacters(in: .whitespaces), line: number)
                if inBackground {
                    background.append(step)
                } else if current != nil {
                    current!.steps.append(step)
                } else {
                    throw ParseError(line: number, message: "step outside a scenario")
                }
                continue
            }
            // Anything else is descriptive prose under the last heading.
        }
        if let current {
            scenarios.append(Scenario(name: current.name, tags: current.tags, steps: current.steps))
        }
        guard sawFeature else {
            throw ParseError(line: 0, message: "no Feature: heading")
        }
        return Feature(name: featureName, tags: featureTags, background: background, scenarios: scenarios)
    }

    struct ParseError: Error, CustomStringConvertible {
        let line: Int
        let message: String
        var description: String { "line \(line): \(message)" }
    }
}
