<?php
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

declare(strict_types=1);

use Iggy\Client as IggyClient;
use Iggy\ConsumerGroup;
use Iggy\ConsumerGroupsPage;
use Iggy\Exception\IggyException;
use Iggy\PageOrder;
use Iggy\Partition;
use Iggy\PartitionsPage;
use Iggy\Stream;
use Iggy\StreamsPage;
use Iggy\Topic;
use Iggy\TopicsPage;
use Iggy\UserInfo;
use Iggy\UsersPage;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestDox;
use PHPUnit\Framework\TestCase;

final class ListPagesTest extends TestCase
{
    private const UNREACHABLE_ADDRESS = '127.0.0.1:1';
    private const PAGE_LIMIT = 2;
    // Ends a walk whose cursors never run out, which would otherwise hang the suite.
    private const MAX_WALK_PAGES = 100;
    private const STREAM_COUNT = 3;
    private const PARTITION_COUNT = 3;
    private const ROOT_USER_ID = 0;
    private const TOPIC = 'pages-topic';
    private const QUERY_KEYS = ['order', 'descending', 'limit', 'cursor', 'name'];
    // The argument check refuses these before the page rules run.
    private const LIMITS_THE_ARGUMENT_CHECK_REFUSES = [-1, 4294967296, '5'];
    private const ARGUMENT_ERROR_CODE = 0;

    private array $createdStreams = [];

    public static function transports(): array
    {
        return [
            'tcp' => ['tcp'],
            'quic' => ['quic'],
            'http' => ['http'],
            'websocket' => ['websocket'],
        ];
    }

    /** Over HTTP, the Rust SDK reports a server refusal as an HTTP error (300), not as its own code. */
    public static function binaryTransports(): array
    {
        return array_diff_key(self::transports(), ['http' => true]);
    }

    protected function tearDown(): void
    {
        if ($this->createdStreams !== []) {
            $client = new_client();
            foreach ($this->createdStreams as $stream) {
                cleanup_stream($client, $stream);
            }
        }

        parent::tearDown();
    }

    #[DataProvider('transports')]
    #[TestDox('Three streams come back in pages of two in id ascending, id descending and created order on $transport')]
    public function testGivenThreeStreamsWhenWalkingPagesOfTwoShouldReturnThemInEachOrder(string $transport): void
    {
        $client = new_transport_client($transport);
        $prefix = unique_name('pages-streams');
        $created = [];
        foreach (range(1, self::STREAM_COUNT) as $index) {
            $created[] = $this->createStream($client, "{$prefix}-{$index}");
        }
        // Ids of deleted streams are reused, so the created order can differ from the id order.
        $ascending = $created;
        sort($ascending);

        foreach ([
            'id ascending' => [PageOrder::Id, false, $ascending],
            'id descending' => [PageOrder::Id, true, array_reverse($ascending)],
            'created' => [PageOrder::Created, false, $created],
        ] as $case => [$order, $descending, $expected]) {
            $streams = self::walk(static fn (?string $cursor): StreamsPage => $client->getStreamsPage(
                order: $order,
                descending: $descending,
                limit: self::PAGE_LIMIT,
                cursor: $cursor,
                name: $prefix,
            ));

            assert_same($expected, self::ids($streams), $case);
            foreach ($streams as $stream) {
                assert_instance_of(Stream::class, $stream, $case);
                $details = $client->getStream($stream->id);
                assert_not_null($details, $case);
                assert_same(
                    [
                        $details->name,
                        $details->messages_count,
                        $details->topics_count,
                        $details->options,
                        $details->derived_options,
                        $details->encryption_key_source,
                        $details->encryption_key_fingerprint,
                        $details->encryption_kek_fingerprint,
                    ],
                    [
                        $stream->name,
                        $stream->messages_count,
                        $stream->topics_count,
                        $stream->options,
                        $stream->derived_options,
                        $stream->encryption_key_source,
                        $stream->encryption_key_fingerprint,
                        $stream->encryption_kek_fingerprint,
                    ],
                    $case,
                );
                assert_same(0, $stream->size_bytes, $case);
                self::assertGreaterThan(0, $stream->created_at, $case);
            }
        }
    }

    #[DataProvider('transports')]
    #[TestDox('The partitions of a topic come back in pages of two on $transport')]
    public function testGivenTopicWithThreePartitionsWhenWalkingPagesOfTwoShouldReturnEveryPartition(string $transport): void
    {
        $client = new_transport_client($transport);
        $streamId = $this->createStream($client, unique_name('pages-partitions'));
        $client->createTopic($streamId, self::TOPIC, self::PARTITION_COUNT, null, null, null, null);

        $partitions = self::walk(static fn (?string $cursor): PartitionsPage => $client->getPartitionsPage(
            $streamId,
            self::TOPIC,
            limit: self::PAGE_LIMIT,
            cursor: $cursor,
        ));

        assert_count(self::PARTITION_COUNT, $partitions);
        foreach ($partitions as $partition) {
            assert_instance_of(Partition::class, $partition);
        }
        $topic = $client->getTopic($streamId, self::TOPIC);
        assert_not_null($topic);
        assert_same(self::ids($topic->partitions), self::ids($partitions));
    }

    #[DataProvider('transports')]
    #[TestDox('A name filter keeps only the matching topic on $transport')]
    public function testGivenTwoTopicsWhenFilteringByNameShouldReturnOnlyTheMatchingTopic(string $transport): void
    {
        $client = new_transport_client($transport);
        $streamId = $this->createStream($client, unique_name('pages-topics'));
        $matching = 'payments';
        $client->createTopic($streamId, 'orders', 1, null, null, null, null);
        $client->createTopic($streamId, $matching, 2, null, null, null, null);

        $topics = self::walk(static fn (?string $cursor): TopicsPage => $client->getTopicsPage(
            $streamId,
            limit: self::PAGE_LIMIT,
            cursor: $cursor,
            name: 'pay',
        ));

        assert_count(1, $topics);
        [$topic] = $topics;
        assert_instance_of(Topic::class, $topic);
        $details = $client->getTopic($streamId, $matching);
        assert_not_null($details);
        assert_same(
            [$details->id, $details->name, $details->messages_count, $details->partitions_count, $details->options, $details->derived_options],
            [$topic->id, $topic->name, $topic->messages_count, $topic->partitions_count, $topic->options, $topic->derived_options],
        );
        assert_same(0, $topic->size_bytes);
        self::assertGreaterThan(0, $topic->created_at);
    }

    #[DataProvider('transports')]
    #[TestDox('A name filter finds a consumer group by its name on $transport')]
    public function testGivenConsumerGroupWhenFilteringByItsNameShouldReturnIt(string $transport): void
    {
        $client = new_transport_client($transport);
        $streamName = unique_name('pages-groups');
        $streamId = $this->createStream($client, $streamName);
        $client->createTopic($streamId, self::TOPIC, 1, null, null, null, null);
        $groupName = unique_name('pages-group');
        // The client has no call that only creates a group: a consumer creates it and leaves.
        new_client()->consumerGroup($groupName, $streamName, self::TOPIC)->shutdown();

        $groups = self::walk(static fn (?string $cursor): ConsumerGroupsPage => $client->getConsumerGroupsPage(
            $streamId,
            self::TOPIC,
            limit: self::PAGE_LIMIT,
            cursor: $cursor,
            name: $groupName,
        ));

        assert_count(1, $groups);
        [$group] = $groups;
        assert_instance_of(ConsumerGroup::class, $group);
        assert_same([$groupName, 1], [$group->name, $group->partitions_count]);
    }

    #[DataProvider('transports')]
    #[TestDox('A name filter finds the root user on $transport')]
    public function testGivenRootNameWhenFilteringUsersShouldReturnTheRootUser(string $transport): void
    {
        $client = new_transport_client($transport);
        $rootName = env_or_default('IGGY_ROOT_USERNAME', 'iggy');

        $users = self::walk(static fn (?string $cursor): UsersPage => $client->getUsersPage(
            limit: self::PAGE_LIMIT,
            cursor: $cursor,
            name: $rootName,
        ));

        $roots = [];
        foreach ($users as $user) {
            assert_instance_of(UserInfo::class, $user);
            assert_true(str_contains($user->username, $rootName), "{$user->username} holds {$rootName}");
            if ($user->username === $rootName) {
                $roots[] = $user;
            }
        }
        assert_count(1, $roots);
        assert_same([self::ROOT_USER_ID, 'active'], [$roots[0]->id, $roots[0]->status]);
    }

    #[DataProvider('binaryTransports')]
    #[TestDox('The server refuses an id cursor in created order, and the session goes on, on $transport')]
    public function testGivenACursorTheServerCannotServeWhenReadingShouldThrowInvalidCommandAndKeepTheSession(string $transport): void
    {
        $client = new_transport_client($transport);
        $streamName = unique_name('pages-refused');
        $streamId = $this->createStream($client, $streamName);
        $idCursor = array_column(self::fixture()['cursors'], 'text', 'case')['id_seven'];

        $error = assert_throws(
            static fn (): StreamsPage => $client->getStreamsPage(order: PageOrder::Created, cursor: $idCursor),
            'Invalid command',
        );
        assert_same(self::invalidCommand(), $error->getCode());

        $streams = self::walk(static fn (?string $cursor): StreamsPage => $client->getStreamsPage(
            limit: self::PAGE_LIMIT,
            cursor: $cursor,
            name: $streamName,
        ));
        assert_same([$streamId], self::ids($streams));
    }

    #[TestDox('Refused cursor texts, opaque cursors and refused queries throw invalid command before any network I/O')]
    public function testGivenRefusedInputsWhenReadingPagesShouldThrowInvalidCommandBeforeNetworkIo(): void
    {
        $client = new IggyClient(self::UNREACHABLE_ADDRESS);
        $fixture = self::fixture();
        $refused = [];
        foreach ($fixture['refused']['cursors'] as $vector) {
            $refused["refused cursor {$vector['case']}"] = ['cursor' => $vector['text']];
        }
        foreach ($fixture['opaque_cursors'] as $vector) {
            $refused["opaque cursor {$vector['case']}"] = ['cursor' => $vector['text']];
        }
        foreach ($fixture['refused']['queries'] as $vector) {
            $refused["refused query {$vector['case']}"] = array_intersect_key($vector['input'], array_flip(self::QUERY_KEYS));
        }

        // Each request vector carries one command with its parent ids, numeric or named.
        foreach ($fixture['requests'] as $request) {
            foreach ($refused as $case => $override) {
                if ($request['command'] === 'get_partitions' && ($override['name'] ?? null) !== null) {
                    continue; // getPartitionsPage() has no `name` argument.
                }
                $label = "{$request['command']} of {$request['case']} with {$case}";
                $error = assert_throws(
                    static fn () => self::readPage($client, $request['command'], [...$request['input'], ...$override]),
                    'Invalid command',
                );
                assert_instance_of(IggyException::class, $error, $label);
                assert_same(self::invalidCommand(), $error->getCode(), $label);
            }
        }
    }

    #[TestDox('A limit that is not an integer or does not fit 32 bits fails the argument check with code 0')]
    public function testGivenALimitTheArgumentCheckRefusesWhenReadingPagesShouldThrowCodeZero(): void
    {
        $client = new IggyClient(self::UNREACHABLE_ADDRESS);
        foreach (self::fixture()['requests'] as $request) {
            foreach (self::LIMITS_THE_ARGUMENT_CHECK_REFUSES as $limit) {
                $label = "{$request['command']} of {$request['case']} with limit " . var_export($limit, true);
                $error = assert_throws(
                    static fn () => self::readPage($client, $request['command'], [...$request['input'], 'limit' => $limit]),
                    "'limit' must be",
                );
                assert_instance_of(IggyException::class, $error, $label);
                assert_same(self::ARGUMENT_ERROR_CODE, $error->getCode(), $label);
            }
        }
    }

    #[TestDox('Request vectors and valid cursors pass validation and fail only on the transport')]
    public function testGivenRequestVectorsAndValidCursorsWhenReadingPagesShouldPassValidation(): void
    {
        $client = new IggyClient(self::UNREACHABLE_ADDRESS);
        $fixture = self::fixture();
        foreach ($fixture['requests'] as $request) {
            $inputs = [$request['case'] => $request['input']];
            foreach ($fixture['cursors'] as $vector) {
                $inputs["{$request['case']} with cursor {$vector['case']}"] = [...$request['input'], 'cursor' => $vector['text']];
            }
            foreach ($inputs as $case => $input) {
                $error = assert_throws(static fn () => self::readPage($client, $request['command'], $input));
                assert_true($error->getCode() !== self::invalidCommand(), "{$request['command']} of {$case}");
            }
        }
    }

    #[TestDox('PageOrder has one case per order name of the fixture')]
    public function testGivenFixtureOrdersWhenReadingPageOrderShouldCoverEveryName(): void
    {
        $values = array_map(static fn (PageOrder $order): string => $order->value, PageOrder::cases());
        sort($values);
        $expected = array_keys(self::fixture()['constants']['orders']);
        sort($expected);
        assert_same($expected, $values);
    }

    private function createStream(IggyClient $client, string $name): int
    {
        $client->createStream($name);
        $stream = $client->getStream($name);
        assert_not_null($stream);
        $this->createdStreams[] = $stream->id;

        return $stream->id;
    }

    /** Reads pages until one has no next, and returns their items in reply order. */
    private static function walk(callable $read): array
    {
        $prefix = self::fixture()['constants']['cursor_prefix'];
        $items = [];
        $cursor = null;
        for ($pages = 0; $pages < self::MAX_WALK_PAGES; $pages++) {
            $page = $read($cursor);
            $pageItems = $page->items;
            self::assertLessThanOrEqual(self::PAGE_LIMIT, count($pageItems));
            array_push($items, ...$pageItems);
            $cursor = $page->next;
            if ($cursor === null) {
                return $items;
            }
            self::assertStringStartsWith($prefix, $cursor);
        }

        self::fail('the walk did not end within ' . self::MAX_WALK_PAGES . ' pages');
    }

    private static function ids(array $items): array
    {
        return array_map(static fn (object $item): int => $item->id, $items);
    }

    /** Calls the page method of a fixture command with the fixture input as named arguments. */
    private static function readPage(IggyClient $client, string $command, array $input): object
    {
        $query = array_intersect_key($input, array_flip(self::QUERY_KEYS));
        $query['order'] = PageOrder::from($query['order']);
        // getPartitionsPage() takes no `name`, so only a null one is dropped and a set one fails.
        if ($command === 'get_partitions' && ($query['name'] ?? null) === null) {
            unset($query['name']);
        }

        return match ($command) {
            'get_streams' => $client->getStreamsPage(...$query),
            'get_topics' => $client->getTopicsPage($input['stream_id']['value'], ...$query),
            'get_partitions' => $client->getPartitionsPage(
                $input['stream_id']['value'],
                $input['topic_id']['value'],
                ...$query,
            ),
            'get_consumer_groups' => $client->getConsumerGroupsPage(
                $input['stream_id']['value'],
                $input['topic_id']['value'],
                ...$query,
            ),
            'get_users' => $client->getUsersPage(...$query),
        };
    }

    private static function invalidCommand(): int
    {
        return self::fixture()['constants']['errors']['invalid_command'];
    }

    private static function fixture(): array
    {
        static $fixture = null;

        return $fixture ??= json_decode(
            (string) file_get_contents(__DIR__ . '/fixtures/list_pages.json'),
            true,
            512,
            JSON_THROW_ON_ERROR | JSON_BIGINT_AS_STRING,
        );
    }
}
