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

/// Error raised by every SDK operation.
///
/// The `code` is what the server answered with, or what the SDK decided
/// locally (a dropped connection, a rejected argument). Match on it:
///
/// ```swift
/// do {
///     _ = try await client.getStream("orders")
/// } catch let error as IggyError where error.code == .streamNameNotFound {
///     // create it
/// }
/// ```
///
/// `context` carries whatever extra detail was available where the error was
/// raised, such as the transport failure message or the protocol window the
/// server accepts. It never carries credentials.
public struct IggyError: Error, Sendable, Hashable, CustomStringConvertible {
    /// The classified code.
    public let code: IggyErrorCode
    /// The exact code that arrived on the wire. Equal to `code.rawValue` unless
    /// the server answered with a code this SDK does not know, in which case
    /// `code` is ``IggyErrorCode/error`` and this keeps the original value.
    public let rawCode: UInt32
    /// Extra detail about the failure, when any was available.
    public let context: String?

    public init(_ code: IggyErrorCode, context: String? = nil) {
        self.code = code
        self.rawCode = code.rawValue
        self.context = context
    }

    /// Maps a code received from the server, keeping unknown codes visible.
    public init(wireCode: UInt32) {
        if let code = IggyErrorCode(rawValue: wireCode) {
            self.code = code
            self.context = nil
        } else {
            self.code = .error
            self.context = "unknown error code \(wireCode)"
        }
        self.rawCode = wireCode
    }

    public var description: String {
        if let context {
            return "\(code.name) (\(rawCode)): \(context)"
        }
        return "\(code.name) (\(rawCode))"
    }
}

extension IggyError {
    /// Whether a fresh connection can recover from this failure.
    var isReconnectable: Bool {
        switch code {
        case .disconnected, .emptyResponse, .unauthenticated, .staleClient, .notConnected,
            .cannotEstablishConnection, .tcpError:
            true
        default:
            false
        }
    }

    /// Whether the failure means the connection itself is gone.
    var isConnectionLoss: Bool {
        switch code {
        case .disconnected, .emptyResponse, .notConnected, .cannotEstablishConnection, .tcpError,
            .staleClient:
            true
        default:
            false
        }
    }

    /// Whether a sign-in that failed this way would fail the same way on a
    /// retry, so the remembered credentials are dropped.
    var isCredentialRejection: Bool {
        switch code {
        case .invalidCredentials, .invalidUsername, .invalidPassword, .unauthenticated:
            true
        default:
            false
        }
    }
}
