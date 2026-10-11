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

import Testing

@testable import Iggy

@Suite("Error codes")
struct ErrorCodeTests {
    /// Spot checks a code from every band by name; the golden test below
    /// covers the whole table.
    @Test func tableMatchesTheServerCodes() {
        #expect(IggyErrorCode.error.rawValue == 1)
        #expect(IggyErrorCode.incompatibleProtocolVersion.rawValue == 14003)
        #expect(IggyErrorCode(rawValue: 57) == .transientNotCommitted)
        #expect(IggyErrorCode(rawValue: 58) == .transientNotAccepted)
        #expect(IggyErrorCode(rawValue: 1009) == .streamIdNotFound)
        #expect(IggyErrorCode(rawValue: 2010) == .topicIdNotFound)
        #expect(IggyErrorCode(rawValue: 2022) == .partitionsLimitReached)
        #expect(IggyErrorCode(rawValue: 4042) == .invalidOptionValue)
        #expect(IggyErrorCode(rawValue: 5006) == .consumerGroupMemberNotFound)
        #expect(IggyErrorCode.streamIdNotFound.name == "stream_id_not_found")
        #expect(Set(IggyErrorCode.allCases.map(\.name)).count == IggyErrorCode.allCases.count)
    }

    /// The full table, against the dump the Rust generator takes from the
    /// server's error enum.
    @Test func errorCodesMatchTheServerTable() {
        let golden = GoldenFixture.shared
        let known = Dictionary(uniqueKeysWithValues: IggyErrorCode.allCases.map { ($0.rawValue, $0.name) })
        #expect(known.count == golden.errors.count)
        for entry in golden.errors {
            #expect(known[entry.code] == entry.name, "code \(entry.code)")
        }
    }

    @Test func unknownWireCodesKeepTheRawValue() {
        let error = IggyError(wireCode: 65_535)
        #expect(error.code == .error)
        #expect(error.rawCode == 65_535)
        #expect(IggyError(wireCode: IggyErrorCode.streamIdNotFound.rawValue).code == .streamIdNotFound)
    }

    @Test func equalityIgnoresContext() {
        #expect(IggyError(.invalidFormat, context: "x") == IggyError(.invalidFormat))
        #expect(IggyError(.invalidFormat) != IggyError(.invalidCommand))
        #expect(IggyError(wireCode: 65_535) != IggyError(.error))
        #expect(IggyError(.invalidFormat, context: "x").hashValue == IggyError(.invalidFormat).hashValue)
    }
}
