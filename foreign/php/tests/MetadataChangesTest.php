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
use Iggy\Exception\AuthenticationException;
use Iggy\Exception\IggyException;
use Iggy\MetadataChangeCursor;
use Iggy\MetadataChangeGroup;
use Iggy\MetadataChangeKind;
use Iggy\MetadataChangePhase;
use Iggy\MetadataChangeRecord;
use Iggy\MetadataChanges;
use Iggy\MetadataChangeStart;
use Iggy\MetadataChangeStatus;
use Iggy\MetadataCurrentItem;
use Iggy\MetadataCurrentPageToken;
use Iggy\MetadataMemberLeftReason;
use Iggy\MetadataPermissionsUpdatedReason;
use Iggy\MetadataSessionClosedReason;
use Iggy\MetadataStreamChange;
use Iggy\MetadataStreamDeleted;
use Iggy\MetadataTokenDeletedReason;
use Iggy\MetadataTopicChange;
use Iggy\MetadataUnknownChange;
use Iggy\MetadataUnknownItem;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestDox;
use PHPUnit\Framework\TestCase;

final class MetadataChangesTest extends TestCase
{
    private const INVALID_COMMAND = 3;
    private const UNAUTHORIZED = 41;
    private const ROOT_USER_ID = 0;
    private const PAGE_LIMIT = 1000;
    // A two phase delete commits its record after the call returns.
    private const FOLLOW_ATTEMPTS = 100;
    private const FOLLOW_INTERVAL_MICROSECONDS = 100_000;
    private const ENVELOPE_KEYS = ['op', 'ordinal', 'checksum', 'timestamp', 'actor_user_id', 'group', 'kind'];
    private const OPTIONAL_BODY_KEYS = ['requested_op', 'member_id', 'expiry'];
    private const NEVER_RETURNED_KEYS = ['encryption_key', 'encryption_key_wrapped', 'critical_keys'];

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
    #[TestDox('Creating, renaming and deleting a stream comes back as typed changes with the root actor on $transport')]
    public function testGivenLatestCursorWhenCreatingRenamingAndDeletingStreamShouldFollowEachChange(string $transport): void
    {
        $client = new_transport_client($transport);
        $name = unique_name('changes-follow');
        $renamed = unique_name('changes-renamed');
        $cursor = self::latestCursor($client, [MetadataChangeGroup::Streams]);

        $streamId = $this->createStream($client, $name);
        $client->updateStream($streamId, name: $renamed);
        $client->deleteStream($streamId);

        $followed = self::follow($client, $cursor, [MetadataChangeGroup::Streams], $streamId, 3);
        foreach ($followed as $record) {
            assert_same(MetadataChangeGroup::Streams, $record->group);
        }
        $records = self::streamRecords($followed, $streamId);
        assert_same(
            [MetadataChangeKind::StreamCreated, MetadataChangeKind::StreamUpdated, MetadataChangeKind::StreamDeleted],
            array_map(static fn (MetadataChangeRecord $record): ?MetadataChangeKind => $record->kind, $records),
        );
        [$created, $updated, $deleted] = array_map(static fn (MetadataChangeRecord $record): object => $record->change, $records);
        assert_instance_of(MetadataStreamChange::class, $created);
        assert_instance_of(MetadataStreamChange::class, $updated);
        assert_instance_of(MetadataStreamDeleted::class, $deleted);
        assert_same([$name, $renamed, $renamed], [$created->name, $updated->name, $deleted->name]);
        assert_same($records[0]->op, $created->stream_created_op);
        assert_same($created->stream_created_op, $updated->stream_created_op);
        assert_same($created->stream_created_op, $deleted->stream_created_op);
        assert_true(!$created->options_unreadable, 'a new stream has readable options');
        $positions = [];
        foreach ($records as $record) {
            assert_same(self::ROOT_USER_ID, $record->actor_user_id);
            self::assertMatchesRegularExpression('/^[1-9][0-9]*$/', $record->checksum);
            $positions[] = [$record->op, $record->ordinal];
        }
        $sorted = $positions;
        sort($sorted);
        assert_same($sorted, $positions);
    }

    #[DataProvider('transports')]
    #[TestDox('A topics group filter leaves out the stream created on the way on $transport')]
    public function testGivenTopicsGroupWhenFollowingShouldReturnOnlyTopicRecords(string $transport): void
    {
        $client = new_transport_client($transport);
        $topicName = unique_name('changes-topic');
        $cursor = self::latestCursor($client, [MetadataChangeGroup::Topics]);

        $streamId = $this->createStream($client, unique_name('changes-filter'));
        $client->createTopic($streamId, $topicName, 2, null, null, null, null);

        $followed = self::follow($client, $cursor, [MetadataChangeGroup::Topics], $streamId, 1);
        foreach ($followed as $record) {
            assert_same(MetadataChangeGroup::Topics, $record->group);
        }
        $records = self::streamRecords($followed, $streamId);
        assert_count(1, $records);
        assert_same(MetadataChangeKind::TopicCreated, $records[0]->kind);
        $topic = $records[0]->change;
        assert_instance_of(MetadataTopicChange::class, $topic);
        assert_same([$topicName, 2], [$topic->name, $topic->partitions_count]);
        assert_same($records[0]->op, $topic->topic_created_op);
    }

    #[DataProvider('transports')]
    #[TestDox('Pages of one record continue through next until has more drops on $transport')]
    public function testGivenLimitOneWhenFollowingShouldPageEveryRecordInOrder(string $transport): void
    {
        $client = new_transport_client($transport);
        $cursor = self::latestCursor($client, [MetadataChangeGroup::Streams]);
        $created = [];
        foreach (range(1, 3) as $index) {
            $created[] = $this->createStream($client, unique_name("changes-page-{$index}"));
        }

        $records = [];
        do {
            $page = $client->getMetadataChanges(MetadataChangeStart::after($cursor), [MetadataChangeGroup::Streams], limit: 1);
            assert_same(MetadataChangeStatus::Ok, $page->status);
            assert_same(MetadataChangePhase::Changes, $page->phase);
            self::assertLessThanOrEqual(1, count($page->records));
            $next = $page->next;
            assert_not_null($next);
            assert_true(
                [$next->op, $next->ordinal] > [$cursor->op, $cursor->ordinal],
                'every page advances the cursor',
            );
            $cursor = $next;
            array_push($records, ...$page->records);
        } while ($page->has_more);

        $streamIds = [];
        foreach ($records as $record) {
            $change = $record->change;
            if ($record->kind === MetadataChangeKind::StreamCreated && $change instanceof MetadataStreamChange) {
                $streamIds[] = $change->stream_id;
            }
        }
        assert_same($created, $streamIds);
        $last = $records[array_key_last($records)];
        assert_same([$last->op, $last->ordinal], [$cursor->op, $cursor->ordinal]);
    }

    #[DataProvider('transports')]
    #[TestDox('Since and until keep the records prepared inside the range on $transport')]
    public function testGivenTimeRangeWhenReadingEarliestShouldReturnOnlyRecordsInRange(string $transport): void
    {
        $client = new_transport_client($transport);
        $cursor = self::latestCursor($client, [MetadataChangeGroup::Streams]);
        $streamId = $this->createStream($client, unique_name('changes-range'));
        $timestamp = self::streamRecords(
            self::follow($client, $cursor, [MetadataChangeGroup::Streams], $streamId, 1),
            $streamId,
        )[0]->timestamp;

        $page = $client->getMetadataChanges(
            MetadataChangeStart::earliest(),
            [MetadataChangeGroup::Streams],
            since: $timestamp,
            until: (string) $timestamp,
            limit: self::PAGE_LIMIT,
        );

        assert_same(MetadataChangeStatus::Ok, $page->status);
        assert_true($page->records !== [], 'the range holds the stream record');
        foreach ($page->records as $record) {
            assert_same($timestamp, $record->timestamp);
        }
        assert_count(1, self::streamRecords($page->records, $streamId));
    }

    #[DataProvider('transports')]
    #[TestDox('A cursor of another history resets and one past the head is behind on $transport')]
    public function testGivenForeignAndFutureCursorsWhenFollowingShouldAnswerResetAndBehind(string $transport): void
    {
        $client = new_transport_client($transport);
        $latest = $client->getMetadataChanges(MetadataChangeStart::latest());
        $logId = $latest->log_id;
        assert_not_null($logId);
        $headOp = $latest->head_op;
        assert_true(is_int($headOp), 'a live head fits an int');
        $foreign = new MetadataChangeCursor($logId === '1' ? '2' : '1', 1, '1', 0);
        $ahead = new MetadataChangeCursor($logId, $headOp + 1_000_000, '1', 0);

        $reset = $client->getMetadataChanges(MetadataChangeStart::after($foreign));
        $behind = $client->getMetadataChanges(MetadataChangeStart::after($ahead));

        assert_same(MetadataChangeStatus::Reset, $reset->status);
        assert_null($reset->next);
        assert_same([], $reset->records);
        assert_same(MetadataChangeStatus::Behind, $behind->status);
        assert_same((string) $ahead, (string) $behind->next);
        assert_same([], $behind->records);
    }

    #[DataProvider('transports')]
    #[TestDox('Current state pages of two streams add up to the stream list, then hand an anchor on $transport')]
    public function testGivenCurrentScanWhenPagingShouldMatchStreamList(string $transport): void
    {
        $client = new_transport_client($transport);
        foreach (range(1, 3) as $index) {
            $this->createStream($client, unique_name("changes-current-{$index}"));
        }

        $items = [];
        $token = null;
        $pages = 0;
        while (true) {
            $page = $client->getMetadataChanges(MetadataChangeStart::current($token), [MetadataChangeGroup::Streams], limit: 2);
            $pages++;
            assert_same(MetadataChangeStatus::Ok, $page->status);
            assert_same(MetadataChangePhase::Current, $page->phase);
            assert_same([], $page->records);
            self::assertLessThanOrEqual(2, count($page->items));
            array_push($items, ...$page->items);
            $token = $page->current_page_token;
            if ($token === null) {
                $anchor = $page->next;
                break;
            }
            assert_true($page->has_more, 'a page with a token has more');
            assert_same([MetadataChangeGroup::Streams], $token->groups);
            assert_same((string) $token, (string) MetadataCurrentPageToken::parse((string) $token));
        }

        $projected = [];
        foreach ($items as $item) {
            assert_same(MetadataChangeGroup::Streams, $item->group);
            $body = $item->body;
            assert_instance_of(MetadataStreamChange::class, $body);
            $projected[$body->stream_id] = $body->name;
        }
        ksort($projected);
        assert_same(stream_list(new_client()), $projected);
        self::assertGreaterThanOrEqual(2, $pages);
        assert_not_null($anchor);
        $following = $client->getMetadataChanges(MetadataChangeStart::after($anchor), [MetadataChangeGroup::Streams]);
        assert_same(MetadataChangeStatus::Ok, $following->status);
    }

    #[DataProvider('transports')]
    #[TestDox('Refused query vectors throw invalid command before anything is sent on $transport')]
    public function testGivenRefusedQueryVectorsWhenReadingShouldThrowBeforeSending(string $transport): void
    {
        $client = new_transport_client($transport);
        foreach (self::fixture()['refused']['queries'] as $vector) {
            $error = assert_throws(static fn () => self::readVector($client, $vector['input']), 'Invalid command');
            assert_same(self::INVALID_COMMAND, $error->getCode(), $vector['case']);
        }
    }

    #[DataProvider('transports')]
    #[TestDox('A user without manage servers is refused as unauthorized on $transport')]
    public function testGivenUserWithoutManageServersWhenReadingShouldThrowUnauthorized(string $transport): void
    {
        $admin = new_client();
        $username = unique_name('changes-reader');
        $password = 'changes-secret';
        create_user_without_permissions($admin, $username, $password);

        try {
            $client = new_transport_client($transport, $username, $password);
            $error = assert_throws(static fn () => $client->getMetadataChanges(MetadataChangeStart::earliest()));
            assert_instance_of(AuthenticationException::class, $error);
            assert_same(self::UNAUTHORIZED, $error->getCode());
        } finally {
            delete_user_by_name($admin, $username);
        }
    }

    #[DataProvider('transports')]
    #[TestDox('A keyed stream change carries the key source and fingerprint and no key material on $transport')]
    public function testGivenStreamKeyWhenFollowingChangesShouldExposeFingerprintsWithoutKey(string $transport): void
    {
        $key = base64_encode(random_bytes(32));
        $client = new_transport_client($transport);
        $cursor = self::latestCursor($client, [MetadataChangeGroup::Streams]);

        $streamId = $this->createStream($client, unique_name('changes-keyed'), $key);

        $records = self::streamRecords(self::follow($client, $cursor, [MetadataChangeGroup::Streams], $streamId, 1), $streamId);
        assert_count(1, $records);
        assert_same(MetadataChangeKind::StreamCreated, $records[0]->kind);
        $stream = $records[0]->change;
        assert_instance_of(MetadataStreamChange::class, $stream);
        $explicit = $stream->options;
        $derived = $stream->derived_options;
        assert_not_null($explicit);
        assert_not_null($derived);
        assert_same('stream', $derived['encryption_key_source']);
        assert_same(IggyClient::encryptionKeyFingerprint($key), $derived['encryption_key_fingerprint']);
        assert_same($client->getStream($streamId)?->encryption_kek_fingerprint, $derived['encryption_kek_fingerprint']);
        foreach (self::NEVER_RETURNED_KEYS as $secret) {
            self::assertArrayNotHasKey($secret, $explicit);
            self::assertArrayNotHasKey($secret, $derived);
        }
        $raw = base64_decode($key, true);
        foreach ([...array_values($explicit), ...array_values($derived)] as $value) {
            self::assertNotContains($value, [$key, $raw, bin2hex((string) $raw)]);
        }
    }

    #[TestDox('Response vectors decode from binary and JSON to the expected page')]
    public function testGivenResponseVectorsWhenDecodingBinaryAndJsonShouldMatchExpectedJson(): void
    {
        foreach (self::fixture()['responses'] as $index => $vector) {
            $binary = MetadataChanges::decode(hex2bin($vector['response_hex']));
            $json = MetadataChanges::fromJson(
                json_encode(self::objectFixture()->responses[$index]->expected, JSON_THROW_ON_ERROR),
            );
            foreach ([$binary, $json] as $page) {
                self::assertPage($page, $vector['expected'], $vector['case']);
            }
        }
    }

    #[TestDox('Record and item vectors decode from binary and JSON to the expected bodies')]
    public function testGivenRecordAndItemVectorsWhenDecodingShouldMatchExpectedJson(): void
    {
        $fixture = self::fixture();
        $objects = self::objectFixture();
        foreach ($fixture['records'] as $index => $vector) {
            $pages = [
                MetadataChanges::decode(self::pagePayload(hex2bin($vector['record_hex']), '')),
                MetadataChanges::fromJson(self::pageJson([$objects->records[$index]->expected], [])),
            ];
            foreach ($pages as $page) {
                assert_count(1, $page->records, $vector['case']);
                self::assertRecord($page->records[0], $vector['expected'], $vector['case']);
            }
        }
        foreach ($fixture['items'] as $index => $vector) {
            $pages = [
                MetadataChanges::decode(self::pagePayload('', hex2bin($vector['item_hex']))),
                MetadataChanges::fromJson(self::pageJson([], [$objects->items[$index]->expected])),
            ];
            foreach ($pages as $page) {
                assert_count(1, $page->items, $vector['case']);
                self::assertItem($page->items[0], $vector['expected'], $vector['case']);
            }
        }
    }

    #[TestDox('Values above PHP_INT_MAX and 128-bit values keep every bit as decimal strings')]
    public function testGivenLargeValuesWhenDecodingShouldKeepEveryBit(): void
    {
        $vector = self::fixture()['large_values'];
        $record = MetadataChanges::decode(self::pagePayload(hex2bin($vector['record_hex']), ''))->records[0];
        $change = $record->change;

        assert_same((int) $vector['op'], $record->op);
        assert_same($vector['timestamp'], $record->timestamp);
        assert_same($vector['ordinal'], $record->ordinal);
        assert_same(self::hexToDecimal($vector['checksum']), $record->checksum);
        assert_same((int) $vector['user_created_at'], $change->user_created_at);
        assert_same((int) $vector['session'], $change->session);
        assert_same(self::hexToDecimal($vector['client_id']), $change->client_id);
    }

    #[TestDox('Cursor vectors round trip their text and every field')]
    public function testGivenCursorVectorsWhenParsingShouldRoundTripEveryField(): void
    {
        $constants = self::fixture()['constants'];
        foreach (self::fixture()['cursors'] as $vector) {
            $cursor = MetadataChangeCursor::parse($vector['text']);
            $fields = [
                self::hexToDecimal($vector['log_id']),
                self::unsigned($vector['op']),
                self::hexToDecimal($vector['checksum']),
                $vector['ordinal'],
            ];
            assert_same($fields, [$cursor->log_id, $cursor->op, $cursor->checksum, $cursor->ordinal], $vector['case']);
            assert_same($vector['text'], (string) $cursor);
            assert_same($constants['cursor_text_length'], strlen((string) $cursor));
            assert_same($vector['text'], (string) new MetadataChangeCursor(...$fields));
        }
    }

    #[TestDox('Page token vectors expose their anchor, head, groups and last key')]
    public function testGivenPageTokenVectorsWhenParsingShouldExposeEveryField(): void
    {
        $codes = array_map(static fn (array $group): int => $group['code'], self::fixture()['constants']['groups']);
        asort($codes);
        foreach (self::fixture()['page_tokens'] as $vector) {
            $token = MetadataCurrentPageToken::parse($vector['text']);
            assert_same($vector['text'], (string) $token);
            assert_same($vector['anchor'], (string) $token->anchor);
            assert_same($vector['required_head'], $token->required_head);
            assert_same($vector['group'], $token->last_group->value);
            assert_same($vector['last_key_hex'], bin2hex($token->last_key));
            $masked = array_keys(array_filter(
                $codes,
                static fn (int $code): bool => ($vector['groups_mask'] & (1 << ($code - 1))) !== 0,
            ));
            assert_same(
                $masked,
                array_map(static fn (MetadataChangeGroup $group): string => $group->value, $token->groups),
                $vector['case'],
            );
        }
    }

    #[TestDox('Equal values compare equal with == and different cursors, tokens, pages, records, items and bodies do not')]
    public function testGivenDecodedValuesWhenComparingWithEqualsShouldCompareContents(): void
    {
        $fixture = self::fixture();
        $unknown = hex2bin($fixture['responses'][1]['response_hex']);
        $builders = [
            'cursor' => static fn (int $index): object => MetadataChangeCursor::parse($fixture['cursors'][$index]['text']),
            'page token' => static fn (int $index): object => MetadataCurrentPageToken::parse($fixture['page_tokens'][$index]['text']),
            'page' => static fn (int $index): object => MetadataChanges::decode(hex2bin($fixture['responses'][$index]['response_hex'])),
            'record' => static fn (int $index): object => MetadataChanges::decode(
                self::pagePayload(hex2bin($fixture['records'][$index]['record_hex']), ''),
            )->records[0],
            'stream body' => static fn (int $index): object => MetadataChanges::decode(
                self::pagePayload(hex2bin($fixture['records'][$index]['record_hex']), ''),
            )->records[0]->change,
            'item' => static fn (int $index): object => MetadataChanges::decode(
                self::pagePayload('', hex2bin($fixture['items'][$index]['item_hex'])),
            )->items[0],
            'unknown body' => static fn (int $index): object => MetadataChanges::decode($unknown)->records[$index]->change,
        ];
        $equalButDifferent = [];
        foreach ($builders as $label => $build) {
            assert_true($build(0) == $build(0), "{$label} equals its copy");
            if ($build(0) == $build(1)) {
                $equalButDifferent[] = $label;
            }
        }
        assert_same([], $equalButDifferent, 'different values compare equal');
    }

    #[TestDox('Refused cursor and page token texts throw invalid command')]
    public function testGivenRefusedCursorAndTokenTextsWhenParsingShouldThrowInvalidCommand(): void
    {
        $refused = self::fixture()['refused'];
        foreach ($refused['cursors'] as $vector) {
            $error = assert_throws(static fn () => MetadataChangeCursor::parse($vector['text']), 'Invalid command');
            assert_same(self::INVALID_COMMAND, $error->getCode(), $vector['case']);
        }
        foreach ($refused['page_tokens'] as $vector) {
            $error = assert_throws(static fn () => MetadataCurrentPageToken::parse($vector['text']), 'Invalid command');
            assert_same(self::INVALID_COMMAND, $error->getCode(), $vector['case']);
        }
    }

    #[TestDox('Refused query vectors throw before any network I/O')]
    public function testGivenRefusedQueryVectorsWhenReadingShouldThrowBeforeNetworkIo(): void
    {
        $client = new IggyClient('127.0.0.1:1');
        foreach (self::fixture()['refused']['queries'] as $vector) {
            $error = assert_throws(static fn () => self::readVector($client, $vector['input']), 'Invalid command');
            assert_instance_of(IggyException::class, $error);
            assert_same(self::INVALID_COMMAND, $error->getCode(), $vector['case']);
        }
    }

    #[TestDox('Request vectors pass validation and fail only on the transport')]
    public function testGivenRequestVectorsWhenReadingShouldPassValidation(): void
    {
        $client = new IggyClient('127.0.0.1:1');
        foreach (self::fixture()['requests'] as $vector) {
            $error = assert_throws(static fn () => self::readVector($client, $vector['input']));
            assert_true($error->getCode() !== self::INVALID_COMMAND, $vector['case']);
        }
    }

    #[TestDox('Refused record bodies and responses fail to decode')]
    public function testGivenRefusedBodiesAndResponsesWhenDecodingShouldThrow(): void
    {
        $refused = self::fixture()['refused'];
        foreach ($refused['record_bodies'] as $vector) {
            $body = hex2bin($vector['body_hex']);
            $record = pack('PVx16P', 1, 0, 0) . chr(0) . chr($vector['group']) . chr($vector['kind'])
                . pack('V', strlen($body)) . $body;
            assert_throws(static fn () => MetadataChanges::decode(self::pagePayload($record, '')));
        }
        foreach ($refused['responses'] as $vector) {
            assert_throws(static fn () => MetadataChanges::decode(hex2bin($vector['hex'])));
        }
        assert_throws(static fn () => MetadataChanges::fromJson('{"status":"ok"}'));
    }

    #[TestDox('Every group, kind, status, phase and reason of the fixture has its enum case')]
    public function testGivenFixtureConstantsWhenReadingEnumsShouldCoverEveryName(): void
    {
        $constants = self::fixture()['constants'];
        foreach ([
            MetadataChangeGroup::class => 'groups',
            MetadataChangeKind::class => 'kinds',
            MetadataChangeStatus::class => 'statuses',
            MetadataChangePhase::class => 'phases',
            MetadataMemberLeftReason::class => 'member_left_reasons',
            MetadataTokenDeletedReason::class => 'token_deleted_reasons',
            MetadataSessionClosedReason::class => 'session_closed_reasons',
            MetadataPermissionsUpdatedReason::class => 'permissions_updated_reasons',
        ] as $enum => $section) {
            $values = array_map(static fn (BackedEnum $case): string => $case->value, $enum::cases());
            sort($values);
            $expected = array_keys($constants[$section]);
            sort($expected);
            assert_same($expected, $values, $section);
        }
    }

    private function createStream(IggyClient $client, string $name, ?string $key = null): int
    {
        $client->createStream($name, $key);
        $stream = $client->getStream($name);
        assert_not_null($stream);
        $this->createdStreams[] = $stream->id;

        return $stream->id;
    }

    private static function latestCursor(IggyClient $client, array $groups): MetadataChangeCursor
    {
        $page = $client->getMetadataChanges(MetadataChangeStart::latest(), $groups);
        assert_same(MetadataChangeStatus::Ok, $page->status);
        $cursor = $page->next;
        assert_not_null($cursor);

        return $cursor;
    }

    /** @return list<MetadataChangeRecord> */
    private static function follow(
        IggyClient $client,
        MetadataChangeCursor $cursor,
        array $groups,
        int $streamId,
        int $count,
    ): array {
        $records = [];
        for ($attempt = 0; $attempt < self::FOLLOW_ATTEMPTS; $attempt++) {
            $page = $client->getMetadataChanges(MetadataChangeStart::after($cursor), $groups, limit: self::PAGE_LIMIT);
            assert_same(MetadataChangeStatus::Ok, $page->status);
            $cursor = $page->next;
            assert_not_null($cursor);
            array_push($records, ...$page->records);
            if ($page->has_more) {
                continue;
            }
            if (count(self::streamRecords($records, $streamId)) >= $count) {
                break;
            }
            usleep(self::FOLLOW_INTERVAL_MICROSECONDS);
        }

        return $records;
    }

    /** @return list<MetadataChangeRecord> */
    private static function streamRecords(array $records, int $streamId): array
    {
        return array_values(array_filter(
            $records,
            static function (MetadataChangeRecord $record) use ($streamId): bool {
                $change = $record->change;

                return property_exists($change, 'stream_id') && $change->stream_id === $streamId;
            },
        ));
    }

    private static function readVector(IggyClient $client, array $query): MetadataChanges
    {
        $start = match ($query['start']) {
            'earliest' => MetadataChangeStart::earliest(),
            'latest' => MetadataChangeStart::latest(),
            'after' => MetadataChangeStart::after(MetadataChangeCursor::parse($query['cursor'])),
            'current' => MetadataChangeStart::current(
                $query['page_token'] === null ? null : MetadataCurrentPageToken::parse($query['page_token']),
            ),
        };

        return $client->getMetadataChanges(
            $start,
            array_map(static fn (string $name): MetadataChangeGroup => MetadataChangeGroup::from($name), $query['groups']),
            since: $query['since'],
            until: $query['until'],
            limit: $query['limit'],
        );
    }

    /** A changes page payload around one encoded record, or a current page around one encoded item. */
    private static function pagePayload(string $record, string $item): string
    {
        return chr(0) . chr($item === '' ? 0 : 1) . chr(0) . pack('PPP', 0, 0, 0) . chr(0) . chr(0)
            . pack('V', $record === '' ? 0 : 1) . $record
            . pack('V', $item === '' ? 0 : 1) . $item
            . pack('v', 0);
    }

    private static function pageJson(array $records, array $items): string
    {
        return json_encode([
            'status' => 'ok',
            'phase' => $items === [] ? 'changes' : 'current',
            'log_id' => null,
            'oldest_op' => 0,
            'retained_since' => 0,
            'head_op' => 0,
            'next' => null,
            'has_more' => false,
            'records' => $records,
            'items' => $items,
            'current_page_token' => null,
        ], JSON_THROW_ON_ERROR);
    }

    private static function assertPage(MetadataChanges $page, array $expected, string $case): void
    {
        assert_same($expected['status'], $page->status->value, $case);
        assert_same($expected['phase'], $page->phase->value, $case);
        assert_same(
            $expected['log_id'] === null ? null : self::hexToDecimal($expected['log_id']),
            $page->log_id,
            $case,
        );
        assert_same(
            [$expected['oldest_op'], $expected['retained_since'], $expected['head_op']],
            [$page->oldest_op, $page->retained_since, $page->head_op],
            $case,
        );
        assert_same($expected['next'], $page->next === null ? null : (string) $page->next, $case);
        assert_same($expected['has_more'], $page->has_more, $case);
        assert_count(count($expected['records']), $page->records, $case);
        foreach ($page->records as $index => $record) {
            self::assertRecord($record, $expected['records'][$index], $case);
        }
        assert_count(count($expected['items']), $page->items, $case);
        foreach ($page->items as $index => $item) {
            self::assertItem($item, $expected['items'][$index], $case);
        }
        $token = $page->current_page_token;
        assert_same($expected['current_page_token'], $token === null ? null : (string) $token, $case);
    }

    private static function assertRecord(MetadataChangeRecord $record, array $expected, string $case): void
    {
        assert_same(self::unsigned($expected['op']), $record->op, $case);
        assert_same($expected['ordinal'], $record->ordinal, $case);
        assert_same(self::hexToDecimal($expected['checksum']), $record->checksum, $case);
        assert_same(self::unsigned($expected['timestamp']), $record->timestamp, $case);
        assert_same($expected['actor_user_id'] ?? null, $record->actor_user_id, $case);
        $change = $record->change;
        if ($record->kind === null) {
            assert_null($record->group, $case);
            self::assertUnknown($change, $expected, $case);

            return;
        }
        assert_same($expected['group'], $record->group?->value, $case);
        assert_same($expected['kind'], $record->kind->value, $case);
        self::assertBody($change, $expected, self::ENVELOPE_KEYS, $case);
    }

    private static function assertItem(MetadataCurrentItem $item, array $expected, string $case): void
    {
        $body = $item->body;
        if ($item->group === null) {
            self::assertUnknown($body, $expected, $case);

            return;
        }
        assert_same($expected['group'], $item->group->value, $case);
        self::assertBody($body, $expected, ['group'], $case);
    }

    /** An unknown record or item keeps its codes and raw body: base64 in the binary rendering, the whole object over JSON. */
    private static function assertUnknown(object $unknown, array $expected, string $case): void
    {
        self::assertTrue(
            $unknown instanceof MetadataUnknownChange || $unknown instanceof MetadataUnknownItem,
            $case,
        );
        assert_same($expected['group'], (string) $unknown->group, $case);
        if ($unknown instanceof MetadataUnknownChange) {
            assert_same($expected['kind'], (string) $unknown->kind, $case);
        }
        if (is_string($unknown->group)) {
            assert_same($expected, json_decode($unknown->body, true, 512, JSON_THROW_ON_ERROR), $case);
        } else {
            assert_same($expected['body'], base64_encode($unknown->body), $case);
        }
    }

    private static function assertBody(object $body, array $fields, array $skipped, string $case): void
    {
        foreach ($fields as $key => $value) {
            if (in_array($key, $skipped, true)) {
                continue;
            }
            $label = "{$case} {$key}";
            match ($key) {
                'options' => self::assertOptions($body, $value, $label),
                'client_id' => assert_same(self::hexToDecimal($value), $body->client_id, $label),
                'reason' => assert_same($value, $body->reason->value, $label),
                default => assert_same(is_int($value) ? self::unsigned($value) : $value, $body->$key, $label),
            };
        }
        foreach (self::OPTIONAL_BODY_KEYS as $key) {
            if (!array_key_exists($key, $fields) && property_exists($body, $key)) {
                assert_null($body->$key, "{$case} {$key} must be absent");
            }
        }
    }

    /** Compare both provenance arrays with the fixture's option map, or the unreadable marker when it is null. */
    private static function assertOptions(object $body, ?array $expected, string $label): void
    {
        if ($expected === null) {
            assert_true($body->options_unreadable, $label);
            assert_null($body->options, $label);
            assert_null($body->derived_options, $label);

            return;
        }
        assert_true(!$body->options_unreadable, $label);
        foreach (['options' => true, 'derived_options' => false] as $property => $explicit) {
            $wanted = [];
            foreach ($expected as $key => $option) {
                if ($option['explicit'] !== $explicit) {
                    continue;
                }
                $wanted[$key] = match ($option['kind']) {
                    'uint64', 'uint32' => self::unsigned($option['value']),
                    'bool' => $option['value'] === 'true',
                    default => $option['value'],
                };
            }
            $actual = $body->$property;
            ksort($actual);
            ksort($wanted);
            assert_same($wanted, $actual, "{$label} {$property}");
        }
    }

    /** An unsigned 64-bit value as the SDK returns it: an int, or a decimal string above PHP_INT_MAX. */
    private static function unsigned(int|string $value): int|string
    {
        $text = (string) $value;
        $max = (string) PHP_INT_MAX;

        return strlen($text) < strlen($max) || (strlen($text) === strlen($max) && strcmp($text, $max) <= 0)
            ? (int) $text
            : $text;
    }

    /** A hex value of any width as a decimal string, without bcmath or gmp. */
    private static function hexToDecimal(string $hex): string
    {
        $digits = [0];
        foreach (str_split($hex) as $nibble) {
            $carry = hexdec($nibble);
            foreach ($digits as $index => $digit) {
                $value = $digit * 16 + $carry;
                $digits[$index] = $value % 10;
                $carry = intdiv($value, 10);
            }
            while ($carry > 0) {
                $digits[] = $carry % 10;
                $carry = intdiv($carry, 10);
            }
        }

        return implode('', array_reverse($digits));
    }

    /** The fixture with JSON objects kept as objects, so re-encoding keeps an empty `{}` an object. */
    private static function objectFixture(): object
    {
        static $fixture = null;

        return $fixture ??= json_decode(
            (string) file_get_contents(__DIR__ . '/fixtures/metadata_changes.json'),
            false,
            512,
            JSON_THROW_ON_ERROR | JSON_BIGINT_AS_STRING,
        );
    }

    private static function fixture(): array
    {
        static $fixture = null;

        return $fixture ??= json_decode(
            (string) file_get_contents(__DIR__ . '/fixtures/metadata_changes.json'),
            true,
            512,
            JSON_THROW_ON_ERROR | JSON_BIGINT_AS_STRING,
        );
    }
}
