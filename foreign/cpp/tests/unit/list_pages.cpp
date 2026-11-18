/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <cstdint>
#include <fstream>
#include <functional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iggy.hpp"

// A partition has no name, so a named query must not pass where a PageQuery is expected.
static_assert(!std::is_convertible_v<iggy::NamedPageQuery, iggy::PageQuery>);

namespace {

constexpr char kFixturePath[]           = "tests/fixtures/list_pages.json";
constexpr std::uint32_t kInvalidCommand = 3;
constexpr std::uint32_t kDisconnected   = 8;
constexpr std::uint8_t kUnknownOrder    = 2;
constexpr std::uint32_t kAnyParentId    = 1;
constexpr char kNotUtf8Cursor[]         = "pg1_AQcAAAAAAAA\xff";
constexpr char kNotUtf8Name[]           = "ord\xff";

const nlohmann::json &Fixture() {
    static const nlohmann::json fixture = [] {
        std::ifstream file(kFixturePath);
        if (!file) {
            throw std::runtime_error(std::string("Cannot open ") + kFixturePath);
        }
        return nlohmann::json::parse(file);
    }();
    return fixture;
}

::testing::AssertionResult ThrowsIggyError(const std::function<void()> &operation,
                                           std::uint32_t code,
                                           const std::string &kind) {
    try {
        operation();
    } catch (const iggy::IggyException &exception) {
        if (exception.Code() == code && exception.Kind() == kind) {
            return ::testing::AssertionSuccess();
        }
        return ::testing::AssertionFailure() << "expected " << kind << " (" << code << "), got '" << exception.what()
                                             << "' with kind '" << exception.Kind() << "'";
    }
    return ::testing::AssertionFailure() << "expected " << kind << " (" << code << "), nothing was thrown";
}

iggy::PageOrder OrderFrom(const nlohmann::json &name) {
    return static_cast<iggy::PageOrder>(
        Fixture().at("constants").at("orders").at(name.get<std::string>()).get<std::uint8_t>());
}

iggy::PageQuery QueryFrom(const nlohmann::json &input) {
    iggy::PageQuery query;
    query.SetOrder(OrderFrom(input.at("order")))
        .SetDescending(input.at("descending").get<bool>())
        .SetLimit(input.at("limit").get<std::uint32_t>());
    if (!input.at("cursor").is_null()) {
        query.SetCursor(input.at("cursor").get<std::string>());
    }
    return query;
}

iggy::NamedPageQuery NamedQueryFrom(const nlohmann::json &input) {
    iggy::NamedPageQuery query(QueryFrom(input));
    if (!input.at("name").is_null()) {
        query.SetName(input.at("name").get<std::string>());
    }
    return query;
}

iggy::PageQuery QueryWithCursor(std::string cursor) {
    iggy::PageQuery query;
    query.SetCursor(std::move(cursor));
    return query;
}

iggy::Identifier IdentifierFrom(const nlohmann::json &identifier) {
    if (identifier.at("kind").get<std::string>() == "numeric") {
        return iggy::Identifier::Numeric(identifier.at("value").get<std::uint32_t>());
    }
    return iggy::Identifier::String(identifier.at("value").get<std::string>());
}

// Runs the page method a request vector names, with the parents and query of its input.
void ReadPage(iggy::IggyBlockingClient &client, const nlohmann::json &vector) {
    const auto &input   = vector.at("input");
    const auto query    = NamedQueryFrom(input);
    const auto &command = vector.at("command").get_ref<const std::string &>();
    if (command == "get_streams") {
        client.GetStreamsPage(query);
    } else if (command == "get_users") {
        client.GetUsersPage(query);
    } else if (command == "get_topics") {
        client.GetTopicsPage(IdentifierFrom(input.at("stream_id")), query);
    } else if (command == "get_partitions") {
        EXPECT_FALSE(query.Name().has_value()) << "a partitions query has no name";
        client.GetPartitionsPage(IdentifierFrom(input.at("stream_id")), IdentifierFrom(input.at("topic_id")),
                                 query.Page());
    } else if (command == "get_consumer_groups") {
        client.GetConsumerGroupsPage(IdentifierFrom(input.at("stream_id")), IdentifierFrom(input.at("topic_id")),
                                     query);
    } else {
        throw std::invalid_argument("Unknown command " + command);
    }
}

// The client never connects, so a query the bridge lets through fails on the session check
// with disconnected, and a refused one fails with invalid_command. Nothing is sent either way.
void ExpectEveryNamedPageMethodThrows(const iggy::NamedPageQuery &query, std::uint32_t code, const std::string &kind) {
    auto client       = iggy::IggyBlockingClient::Builder().Build();
    const auto parent = iggy::Identifier::Numeric(kAnyParentId);
    const std::vector<std::pair<const char *, std::function<void()>>> calls{
        {"streams", [&] { client.GetStreamsPage(query); }},
        {"topics", [&] { client.GetTopicsPage(parent, query); }},
        {"consumer groups", [&] { client.GetConsumerGroupsPage(parent, parent, query); }},
        {"users", [&] { client.GetUsersPage(query); }},
    };
    for (const auto &[list, call] : calls) {
        SCOPED_TRACE(list);
        EXPECT_TRUE(ThrowsIggyError(call, code, kind));
    }
}

// Runs the partitions page method with the query, and the other page methods with the query and no name.
void ExpectEveryPageMethodThrows(const iggy::PageQuery &query, std::uint32_t code, const std::string &kind) {
    auto client       = iggy::IggyBlockingClient::Builder().Build();
    const auto parent = iggy::Identifier::Numeric(kAnyParentId);
    {
        SCOPED_TRACE("partitions");
        EXPECT_TRUE(ThrowsIggyError([&] { client.GetPartitionsPage(parent, parent, query); }, code, kind));
    }
    ExpectEveryNamedPageMethodThrows(iggy::NamedPageQuery(query), code, kind);
}

}  // namespace

TEST(ListPagesFixtureTest, GivenFixtureConstantsWhenComparingShouldMatchThePublicValues) {
    const auto &constants = Fixture().at("constants");
    EXPECT_EQ(iggy::PageQuery::kDefaultLimit, constants.at("default_limit").get<std::uint32_t>());
    EXPECT_EQ(iggy::PageQuery::kMaxLimit, constants.at("max_limit").get<std::uint32_t>());
    EXPECT_EQ(static_cast<std::uint8_t>(iggy::PageOrder::Id), constants.at("orders").at("id").get<std::uint8_t>());
    EXPECT_EQ(static_cast<std::uint8_t>(iggy::PageOrder::Created),
              constants.at("orders").at("created").get<std::uint8_t>());

    const iggy::PageQuery query;
    EXPECT_EQ(query.Order(), iggy::PageOrder::Id);
    EXPECT_FALSE(query.Descending());
    EXPECT_EQ(query.Limit(), iggy::PageQuery::kDefaultLimit);
    EXPECT_FALSE(query.Cursor().has_value());
    const iggy::NamedPageQuery named;
    EXPECT_EQ(named.Page().Limit(), iggy::PageQuery::kDefaultLimit);
    EXPECT_FALSE(named.Name().has_value());

    ExpectEveryPageMethodThrows(iggy::PageQuery().SetLimit(0),
                                constants.at("errors").at("invalid_command").get<std::uint32_t>(), "invalid_command");
}

TEST(ListPagesFixtureTest, GivenRequestVectorsWhenReadingAPageShouldPassTheChecksBeforeSending) {
    auto client = iggy::IggyBlockingClient::Builder().Build();
    for (const auto &vector : Fixture().at("requests")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        EXPECT_TRUE(ThrowsIggyError([&client, &vector] { ReadPage(client, vector); }, kDisconnected, "disconnected"));
    }
}

TEST(ListPagesFixtureTest, GivenCursorVectorsWhenReadingAPageShouldAcceptTheirText) {
    for (const auto &vector : Fixture().at("cursors")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        ExpectEveryPageMethodThrows(QueryWithCursor(vector.at("text").get<std::string>()), kDisconnected,
                                    "disconnected");
    }
}

TEST(ListPagesFixtureTest, GivenRefusedAndOpaqueCursorTextWhenReadingAPageShouldThrowInvalidCommand) {
    for (const auto *cursors : {&Fixture().at("refused").at("cursors"), &Fixture().at("opaque_cursors")}) {
        for (const auto &vector : *cursors) {
            SCOPED_TRACE(vector.at("case").get<std::string>());
            ExpectEveryPageMethodThrows(QueryWithCursor(vector.at("text").get<std::string>()), kInvalidCommand,
                                        "invalid_command");
        }
    }
}

TEST(ListPagesFixtureTest, GivenRefusedQueriesWhenReadingAPageShouldThrowInvalidCommand) {
    for (const auto &vector : Fixture().at("refused").at("queries")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto &input = vector.at("input");
        if (input.at("name").is_null()) {
            ExpectEveryPageMethodThrows(QueryFrom(input), kInvalidCommand, "invalid_command");
        } else {
            ExpectEveryNamedPageMethodThrows(NamedQueryFrom(input), kInvalidCommand, "invalid_command");
        }
    }
}

TEST(ListPagesFixtureTest, GivenTextThatIsNotUtf8OrAnUnknownOrderWhenReadingAPageShouldThrowInvalidCommand) {
    const auto queries = {
        QueryWithCursor(kNotUtf8Cursor),
        iggy::PageQuery().SetOrder(static_cast<iggy::PageOrder>(kUnknownOrder)),
    };
    for (const auto &query : queries) {
        ExpectEveryPageMethodThrows(query, kInvalidCommand, "invalid_command");
    }
    ExpectEveryNamedPageMethodThrows(iggy::NamedPageQuery().SetName(kNotUtf8Name), kInvalidCommand, "invalid_command");
}

TEST(ListPagesFixtureTest, GivenReadmeSampleWhenCompilingShouldMatchThePublicApi) {
    using PrintFunction = std::function<void(const iggy::Stream &)>;
    // Never invoked, it only has to compile, so the README sample cannot drift from the API.
    const auto print_order_streams = [](iggy::IggyBlockingClient &client, const PrintFunction &Print) {
        iggy::NamedPageQuery query(iggy::PageQuery().SetLimit(50));
        query.SetName("orders");
        while (true) {
            const auto page = client.GetStreamsPage(query);
            for (const auto &stream : page.Items()) {
                Print(stream);
            }
            if (!page.Next()) {
                break;
            }
            query.Page().SetCursor(*page.Next());
        }
    };
    static_cast<void>(print_order_streams);
}
