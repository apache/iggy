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

/// One step of a scenario, without its keyword.
struct Step: Sendable {
    let keyword: String
    let text: String
    let line: Int
}

struct Scenario: Sendable {
    let name: String
    let line: Int
    var steps: [Step] = []
}

struct Feature: Sendable {
    let path: String
    var name = ""
    var background: [Step] = []
    var scenarios: [Scenario] = []
}

struct GherkinError: Error, CustomStringConvertible {
    let description: String
}

/// Reads the subset of Gherkin the shared features use: tags, comments, a
/// feature with its description, one background and scenarios of plain
/// steps. Anything else, such as tables, doc strings or outlines, is refused,
/// so a feature never runs with a step silently dropped.
enum Gherkin {
    private static let stepKeywords = ["Given", "When", "Then", "And", "But"]

    static func parse(_ text: String, path: String) throws -> Feature {
        var feature = Feature(path: path)
        var inBackground = false
        var current: Scenario?
        for (index, raw) in text.components(separatedBy: .newlines).enumerated() {
            let line = raw.trimmingCharacters(in: .whitespaces)
            let number = index + 1
            if line.isEmpty || line.hasPrefix("#") || line.hasPrefix("@") {
                continue
            }
            if let name = value(of: "Feature:", in: line) {
                feature.name = name
            } else if value(of: "Background:", in: line) != nil {
                inBackground = true
            } else if let name = value(of: "Scenario:", in: line) {
                if let current {
                    feature.scenarios.append(current)
                }
                inBackground = false
                current = Scenario(name: name, line: number)
            } else if let step = step(line, number: number) {
                if inBackground {
                    feature.background.append(step)
                } else if current != nil {
                    current?.steps.append(step)
                } else {
                    throw GherkinError(description: "\(path):\(number): a step outside a scenario")
                }
            } else if line.hasPrefix("|") || line.hasPrefix("\"\"\"") || line.hasPrefix("Scenario Outline:") || line.hasPrefix("Examples:")
                || line.hasPrefix("Rule:")
            {
                throw GherkinError(description: "\(path):\(number): unsupported Gherkin: \(line)")
            } else if current != nil || inBackground {
                throw GherkinError(description: "\(path):\(number): not a step: \(line)")
            }
        }
        if let current {
            feature.scenarios.append(current)
        }
        guard !feature.name.isEmpty else {
            throw GherkinError(description: "\(path): no feature")
        }
        return feature
    }

    private static func value(of keyword: String, in line: String) -> String? {
        guard line.hasPrefix(keyword) else {
            return nil
        }
        return line.dropFirst(keyword.count).trimmingCharacters(in: .whitespaces)
    }

    private static func step(_ line: String, number: Int) -> Step? {
        for keyword in stepKeywords where line.hasPrefix(keyword + " ") {
            return Step(keyword: keyword, text: String(line.dropFirst(keyword.count + 1)), line: number)
        }
        return nil
    }
}
