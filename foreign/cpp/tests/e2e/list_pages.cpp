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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "tests/e2e/test_helpers.hpp"

namespace {

constexpr std::uint32_t kInvalidCommand  = 3;
constexpr std::uint32_t kPageLimit       = 2;
constexpr std::size_t kEntriesPerList    = 3;
constexpr std::uint32_t kPartitionsCount = 3;
constexpr int kMaxPages                  = 100;
constexpr std::size_t kTokenLength       = 16;
// An id kind cursor, which the server refuses with order created on the streams.
constexpr char kIdCursor[] = "pg1_AQcAAAAAAAAA";
constexpr char kPassword[] = "pages-secret";

::testing::AssertionResult ThrowsIggyError(const std::function<void()> &operation,
                                           std::uint32_t code,
                                           const std::string &kind) {
    try {
        operation();
    } catch (const iggy::IggyException &exception) {
        if (exception.Code() == code && exception.Kind() == kind) {
            return ::testing::AssertionSuccess();
        }
        return ::testing::AssertionFailure()
               << "expected " << kind << " (" << code << "), got '" << exception.what() << "' with code "
               << exception.Code().value_or(0) << " and kind '" << exception.Kind() << "'";
    }
    return ::testing::AssertionFailure() << "expected " << kind << " (" << code << "), nothing was thrown";
}

// Reads the pages of a query until one has no next cursor, and returns their items in reply order.
template <typename ReadPage>
auto Walk(const ReadPage &read_page, iggy::PageQuery query) {
    using Item = typename std::decay_t<decltype(read_page(query).Items())>::value_type;
    std::vector<Item> items;
    for (int page_index = 0; page_index < kMaxPages; ++page_index) {
        const auto page = read_page(query);
        EXPECT_LE(page.Items().size(), query.Limit());
        items.insert(items.end(), page.Items().begin(), page.Items().end());
        if (!page.Next()) {
            return items;
        }
        query.SetCursor(*page.Next());
    }
    ADD_FAILURE() << "the walk did not end within " << kMaxPages << " pages";
    return items;
}

template <typename Item>
std::vector<std::uint32_t> Ids(const std::vector<Item> &items) {
    std::vector<std::uint32_t> ids;
    ids.reserve(items.size());
    for (const auto &item : items) {
        ids.push_back(item.Id());
    }
    return ids;
}

template <typename Item, typename Name>
std::vector<std::string> SortedNames(const std::vector<Item> &items, const Name &name) {
    std::vector<std::string> names;
    names.reserve(items.size());
    for (const auto &item : items) {
        names.push_back(name(item));
    }
    std::sort(names.begin(), names.end());
    return names;
}

std::vector<std::uint32_t> Reversed(std::vector<std::uint32_t> ids) {
    std::reverse(ids.begin(), ids.end());
    return ids;
}

// Sorted, and each one holds the prefix.
std::vector<std::string> NamesWithPrefix(const std::string &prefix) {
    std::vector<std::string> names;
    for (std::size_t index = 0; index < kEntriesPerList; ++index) {
        names.push_back(prefix + std::to_string(index));
    }
    return names;
}

iggy::PageQuery PageOfTwo() {
    return iggy::PageQuery().SetLimit(kPageLimit);
}

}  // namespace

class E2E_ListPages : public E2ETestFixture {};

TEST_F(E2E_ListPages, GivenThreeStreamsWhenWalkingPagesOfTwoShouldListThemInEachOrder) {
    RecordProperty("description",
                   "Creates three streams under one name prefix and walks them in pages of two with the prefix as the "
                   "name filter, in id and creation order, each ascending and descending.");
    auto client       = GetLoggedInHighLevelClient();
    const auto prefix = "pages" + GetRandomName(kTokenLength);
    std::vector<std::pair<std::uint64_t, std::uint32_t>> creation_keys;
    for (const auto &name : NamesWithPrefix(prefix)) {
        const auto stream = client.CreateStream(name);
        TrackStream(name);
        creation_keys.emplace_back(stream.CreatedAt(), stream.Id());
    }
    // A deleted stream frees its id for a new one, so the creation order is (created_at, id), not the id order.
    std::sort(creation_keys.begin(), creation_keys.end());
    std::vector<std::uint32_t> by_creation;
    for (const auto &key : creation_keys) {
        by_creation.push_back(key.second);
    }
    auto by_id = by_creation;
    std::sort(by_id.begin(), by_id.end());

    const auto read = [&client, &prefix](const iggy::PageQuery &page) {
        return client.GetStreamsPage(iggy::NamedPageQuery(page).SetName(prefix));
    };
    const auto walk = [&read](iggy::PageOrder order, bool descending) {
        return Ids(Walk(read, PageOfTwo().SetOrder(order).SetDescending(descending)));
    };
    EXPECT_EQ(walk(iggy::PageOrder::Id, false), by_id);
    EXPECT_EQ(walk(iggy::PageOrder::Id, true), Reversed(by_id));
    EXPECT_EQ(walk(iggy::PageOrder::Created, false), by_creation);
    EXPECT_EQ(walk(iggy::PageOrder::Created, true), Reversed(by_creation));
}

TEST_F(E2E_ListPages, GivenTopicWithThreePartitionsWhenWalkingPagesOfTwoShouldListEachPartition) {
    RecordProperty("description",
                   "Walks the partitions of a topic with three partitions in pages of two, ascending, descending and "
                   "in creation order, which is the id order for partitions.");
    auto client     = GetLoggedInHighLevelClient();
    const auto name = "pages" + GetRandomName(kTokenLength);
    client.CreateStream(name);
    TrackStream(name);
    const auto stream_id = iggy::Identifier::String(name);
    const auto topic =
        client.CreateTopic(stream_id, "events", iggy::TopicCreateOptions().SetPartitionsCount(kPartitionsCount));
    auto ids = Ids(topic.Partitions());
    std::sort(ids.begin(), ids.end());
    ASSERT_EQ(ids.size(), kPartitionsCount);

    const auto topic_id = iggy::Identifier::Numeric(topic.Id());
    const auto read     = [&client, &stream_id, &topic_id](const iggy::PageQuery &query) {
        return client.GetPartitionsPage(stream_id, topic_id, query);
    };
    EXPECT_EQ(Ids(Walk(read, PageOfTwo())), ids);
    EXPECT_EQ(Ids(Walk(read, PageOfTwo().SetDescending(true))), Reversed(ids));
    EXPECT_EQ(Ids(Walk(read, PageOfTwo().SetOrder(iggy::PageOrder::Created))), ids);
}

TEST_F(E2E_ListPages, GivenNameFilterWhenWalkingTopicsGroupsAndUsersShouldListOnlyTheMatchingNames) {
    RecordProperty("description",
                   "Creates three matching topics, groups and users and one more of each that does not match, and "
                   "walks each list in pages of two with the name filter.");
    auto client         = GetLoggedInHighLevelClient();
    const auto token    = GetRandomName(kTokenLength);
    const auto prefix   = "match" + token;
    const auto expected = NamesWithPrefix(prefix);
    auto created        = expected;
    created.push_back("other" + token);

    const auto stream_name = "pages" + token;
    client.CreateStream(stream_name);
    TrackStream(stream_name);
    const auto stream = iggy::Identifier::String(stream_name);
    const auto topic  = iggy::Identifier::String(expected.front());
    for (const auto &name : created) {
        client.CreateTopic(stream, name, iggy::TopicCreateOptions().SetPartitionsCount(1));
    }
    for (const auto &name : created) {
        client.CreateConsumerGroup(stream, topic, name);
        CreateUser(client, name, kPassword, iggy::UserStatus::Active);
    }

    const auto filter = [&prefix](const iggy::PageQuery &page) { return iggy::NamedPageQuery(page).SetName(prefix); };
    const auto topics =
        Walk([&](const iggy::PageQuery &page) { return client.GetTopicsPage(stream, filter(page)); }, PageOfTwo());
    EXPECT_EQ(SortedNames(topics, [](const iggy::Topic &item) { return item.Name(); }), expected);
    const auto groups =
        Walk([&](const iggy::PageQuery &page) { return client.GetConsumerGroupsPage(stream, topic, filter(page)); },
             PageOfTwo());
    EXPECT_EQ(SortedNames(groups, [](const iggy::ConsumerGroup &item) { return item.Name(); }), expected);
    const auto users =
        Walk([&](const iggy::PageQuery &page) { return client.GetUsersPage(filter(page)); }, PageOfTwo());
    EXPECT_EQ(SortedNames(users, [](const iggy::UserInfo &item) { return item.Username(); }), expected);
}

TEST_F(E2E_ListPages, GivenMissingStreamWhenReadingTopicsPageShouldReturnAnEmptyLastPage) {
    RecordProperty("description", "A topics page of a stream that does not exist is empty and has no next cursor.");
    auto client = GetLoggedInHighLevelClient();
    const auto page =
        client.GetTopicsPage(iggy::Identifier::String("missing" + GetRandomName(kTokenLength)), iggy::NamedPageQuery());
    EXPECT_TRUE(page.Items().empty());
    EXPECT_FALSE(page.Next().has_value());
}

TEST_F(E2E_ListPages, GivenQueryTheServerRefusesWhenReadingShouldThrowInvalidCommandAndKeepTheSession) {
    RecordProperty("description",
                   "The server refuses an id cursor with order created on the streams with invalid_command, and the "
                   "session keeps working after it.");
    auto client     = GetLoggedInHighLevelClient();
    const auto name = "pages" + GetRandomName(kTokenLength);
    client.CreateStream(name);
    TrackStream(name);

    const auto id_cursor_in_created_order =
        iggy::NamedPageQuery(iggy::PageQuery().SetOrder(iggy::PageOrder::Created).SetCursor(kIdCursor));
    const auto named = iggy::NamedPageQuery().SetName(name);
    EXPECT_TRUE(ThrowsIggyError([&] { client.GetStreamsPage(id_cursor_in_created_order); }, kInvalidCommand,
                                "invalid_command"));

    EXPECT_EQ(SortedNames(client.GetStreamsPage(named).Items(), [](const iggy::Stream &item) { return item.Name(); }),
              std::vector<std::string>{name});
}
