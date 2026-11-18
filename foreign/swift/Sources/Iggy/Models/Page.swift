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

/// The order of a list page. The raw value is its code on the wire.
public enum PageOrder: UInt8, Sendable, Hashable, CaseIterable {
    /// The numeric ids.
    case id = 0
    /// The creation order. Entities created at the same time keep the order
    /// of their ids. Partitions and consumer groups walk by id in this order
    /// too, because their ids follow creation.
    case created = 1
}

/// The position after which the next page of a list starts.
///
/// Send it back as it came, with the query that returned it: it holds no
/// order, direction, filter or parent. Only the server reads its bytes, so the
/// client keeps them unchanged. ``description``, `pg1_` followed by unpadded
/// Base64url of the bytes, is the text form that the HTTP API and the CLI use,
/// and ``init(text:)`` reads it back.
public struct PageCursor: Sendable, Hashable, CustomStringConvertible {
    public static let textPrefix = "pg1_"
    /// The most bytes of a cursor: its length on the wire is one byte.
    public static let maxSize = Int(UInt8.max)
    static let maxTextLength = textPrefix.utf8.count + (maxSize * 4 + 2) / 3

    let bytes: [UInt8]

    /// Parses the `pg1_` text form. Fails with
    /// ``IggyErrorCode/invalidCommand`` for any text but the prefix and
    /// canonical unpadded Base64url of 1 to ``maxSize`` bytes, and the error
    /// never repeats the text. The bytes are not checked: the server refuses a
    /// cursor that it cannot use.
    public init(text: String) throws {
        guard text.utf8.count <= Self.maxTextLength, let bytes = Base64URL.decode(text, prefix: Self.textPrefix), let cursor = PageCursor(bytes: bytes)
        else {
            throw IggyError(.invalidCommand, context: "not a page cursor")
        }
        self = cursor
    }

    /// A cursor of 1 to ``maxSize`` bytes, nil for any other count. A reply
    /// marks its last page with a next of no bytes.
    init?(bytes: [UInt8]) {
        guard (1...Self.maxSize).contains(bytes.count) else {
            return nil
        }
        self.bytes = bytes
    }

    /// The `pg1_` text form.
    public var description: String {
        Self.textPrefix + Base64URL.encode(bytes)
    }
}

/// One page request of a list command. The defaults ask for the first page in
/// ascending id order, with ``defaultLimit`` items. The partitions page takes
/// this type, which has no name. The other lists take a ``NamedPageQuery``.
public struct PageQuery: Sendable, Hashable {
    /// Items per page when the caller does not say.
    public static let defaultLimit: UInt32 = 100
    /// The most items one page returns.
    public static let maxLimit: UInt32 = 1000

    public var order: PageOrder
    /// Reverses ``order``.
    public var descending: Bool
    /// The most items of the page, 1 to ``maxLimit``.
    public var limit: UInt32
    /// The ``Page/next`` of the previous page, nil for the first page.
    public var cursor: PageCursor?

    public init(order: PageOrder = .id, descending: Bool = false, limit: UInt32 = defaultLimit, cursor: PageCursor? = nil) {
        self.order = order
        self.descending = descending
        self.limit = limit
        self.cursor = cursor
    }

    /// The page rules that the server applies, checked before anything is
    /// sent. Fails with ``IggyErrorCode/invalidCommand`` when the limit is
    /// outside 1 to ``maxLimit``.
    public func validate() throws {
        guard (1...Self.maxLimit).contains(limit) else {
            throw IggyError(.invalidCommand, context: "limit must be 1 to \(Self.maxLimit), got \(limit)")
        }
    }
}

/// One page request of a list whose entries have a name: streams, topics,
/// consumer groups and users. It adds a name filter to ``page``.
public struct NamedPageQuery: Sendable, Hashable {
    /// The most bytes of a name filter.
    public static let maxNameSize = Identifier.maxNameLength

    public var page: PageQuery
    /// Keeps only the entries whose name holds this text, case-sensitive. Nil
    /// keeps every entry.
    public var name: String?

    public init(page: PageQuery = PageQuery(), name: String? = nil) {
        self.page = page
        self.name = name
    }

    /// The rules of ``PageQuery/validate()``, checked before anything is
    /// sent. Also fails with ``IggyErrorCode/invalidCommand`` when the name is
    /// empty or longer than ``maxNameSize`` bytes.
    public func validate() throws {
        try page.validate()
        if let name, !(1...Self.maxNameSize).contains(name.utf8.count) {
            throw IggyError(.invalidCommand, context: "name must be 1 to \(Self.maxNameSize) bytes, got \(name.utf8.count)")
        }
    }
}

/// One page of a list. A page can be short or empty and still have a
/// ``next``.
public struct Page<Item: Sendable & Hashable>: Sendable, Hashable {
    public var items: [Item]
    /// The cursor of the next page, nil on the last page.
    public var next: PageCursor?

    public init(items: [Item], next: PageCursor?) {
        self.items = items
        self.next = next
    }
}
