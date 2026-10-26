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

require_once __DIR__ . '/SharedFeatureParser.php';
require_once __DIR__ . '/StreamSteps.php';
require_once __DIR__ . '/TestEnvironment.php';

use Iggy\Client as IggyClient;
use Iggy\Exception\AuthenticationException;
use Iggy\MetadataChangeCursor;
use Iggy\MetadataChangeGroup;
use Iggy\MetadataChangeKind;
use Iggy\MetadataChangeRecord;
use Iggy\MetadataChanges;
use Iggy\MetadataChangeStart;
use Iggy\MetadataChangeStatus;
use Iggy\MetadataStreamChange;
use Iggy\MetadataStreamDeleted;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

final class MetadataChangesFeatureTest extends TestCase
{
    use StreamSteps;

    private const ROOT_USER_ID = 0;
    private const UNAUTHORIZED = 41;
    private const PAGE_LIMIT = 1000;
    // A two phase delete commits its record after the call returns.
    private const FOLLOW_ATTEMPTS = 100;
    private const FOLLOW_INTERVAL_MICROSECONDS = 100_000;
    private const NEVER_RETURNED_KEYS = ['encryption_key', 'encryption_key_wrapped', 'critical_keys'];

    /** @var list<MetadataChangeGroup> */
    private array $groups = [];
    private ?MetadataChangeCursor $cursor = null;
    /** @var list<MetadataChangeRecord> */
    private array $followed = [];
    /** @var array<int, string> */
    private array $currentStreams = [];
    private ?string $username = null;

    public static function scenarioCases(): array
    {
        return SharedFeatureParser::load(__DIR__ . '/../../scenarios/metadata_changes.feature');
    }

    #[DataProvider('scenarioCases')]
    #[Group('metadata-changes')]
    public function testMetadataChangesScenario(string $scenarioName, array $steps): void
    {
        assert_true($scenarioName !== '', 'scenario name must not be empty');
        foreach ($steps as $step) {
            $this->runStep($step);
        }
    }

    protected function tearDown(): void
    {
        if ($this->streamName !== null || $this->username !== null) {
            $root = new IggyClient(TestEnvironment::serverAddress());
            $root->connect();
            $root->loginUser(TestEnvironment::rootUsername(), TestEnvironment::rootPassword());
            if ($this->streamName !== null) {
                cleanup_stream($root, $this->streamName);
            }
            if ($this->username !== null) {
                delete_user_by_name($root, $this->username);
            }
        }

        parent::tearDown();
    }

    private function runStep(string $step): void
    {
        if ($this->runStreamStep($step)) {
            return;
        }

        if (preg_match('/^I hold the latest metadata change cursor for the groups "([^"]+)"$/', $step, $matches) === 1) {
            $this->groups = self::parseGroups($matches[1]);
            $page = $this->read(MetadataChangeStart::latest());
            assert_same(MetadataChangeStatus::Ok, $page->status);
            $this->cursor = $page->next;
            assert_not_null($this->cursor);
            return;
        }

        if ($step === 'I delete the stream') {
            $this->requireClient()->deleteStream($this->requireStreamId());
            return;
        }

        if (preg_match('/^I create and delete (\d+) streams named "([^"]+)"$/', $step, $matches) === 1) {
            $client = $this->requireClient();
            for ($index = 0; $index < (int) $matches[1]; $index++) {
                $name = "{$matches[2]}-{$index}";
                $client->createStream($name);
                $client->deleteStream($name);
            }
            return;
        }

        if (preg_match('/^following the metadata changes should return the kinds "([^"]+)" for the stream$/', $step, $matches) === 1) {
            $expected = array_map(
                static fn (string $name): MetadataChangeKind => MetadataChangeKind::from(trim($name)),
                explode(',', $matches[1]),
            );
            $records = $this->follow(count($expected));
            foreach ($records as $record) {
                assert_true(in_array($record->group, $this->groups, true), 'a record of an unrequested group');
            }
            $this->followed = $this->streamRecords($records);
            assert_same(
                $expected,
                array_map(static fn (MetadataChangeRecord $record): ?MetadataChangeKind => $record->kind, $this->followed),
            );
            return;
        }

        if ($step === 'each followed change should name the root user as its actor') {
            assert_true($this->followed !== [], 'changes were followed');
            foreach ($this->followed as $record) {
                assert_same(self::ROOT_USER_ID, $record->actor_user_id);
            }
            return;
        }

        if (preg_match('/^the followed "([^"]+)" change should carry a requested op before its own op$/', $step, $matches) === 1) {
            $record = $this->followedRecord($matches[1]);
            $deleted = $record->change;
            assert_instance_of(MetadataStreamDeleted::class, $deleted);
            assert_not_null($deleted->requested_op);
            assert_true($deleted->requested_op < $record->op, 'the request precedes the completion');
            return;
        }

        if (preg_match('/^the followed "([^"]+)" change should report encryption key source "([^"]+)"$/', $step, $matches) === 1) {
            assert_same($matches[2], $this->followedStream($matches[1])->derived_options['encryption_key_source'] ?? null);
            return;
        }

        if (preg_match('/^the followed "([^"]+)" change should carry the stream key fingerprint$/', $step, $matches) === 1) {
            assert_not_null($this->keyBase64);
            assert_same(
                IggyClient::encryptionKeyFingerprint($this->keyBase64),
                $this->followedStream($matches[1])->derived_options['encryption_key_fingerprint'] ?? null,
            );
            return;
        }

        if ($step === 'the followed changes should expose no encryption key material') {
            assert_true($this->followed !== [], 'changes were followed');
            foreach ($this->followed as $record) {
                $stream = $record->change;
                if (!$stream instanceof MetadataStreamChange) {
                    continue;
                }
                $explicit = $stream->options;
                $derived = $stream->derived_options;
                assert_not_null($explicit);
                assert_not_null($derived);
                foreach (self::NEVER_RETURNED_KEYS as $secret) {
                    self::assertArrayNotHasKey($secret, $explicit);
                    self::assertArrayNotHasKey($secret, $derived);
                }
                if ($this->keyBase64 !== null) {
                    $values = [...array_values($explicit), ...array_values($derived)];
                    self::assertNotContains($this->keyBase64, $values);
                    self::assertNotContains(base64_decode($this->keyBase64, true), $values);
                }
            }
            return;
        }

        if (preg_match('/^following the metadata changes should answer "([^"]+)"$/', $step, $matches) === 1) {
            assert_not_null($this->cursor);
            $page = $this->read(MetadataChangeStart::after($this->cursor));
            assert_same(MetadataChangeStatus::from($matches[1]), $page->status);
            if ($page->status !== MetadataChangeStatus::Ok) {
                assert_same([], $page->records);
                assert_null($page->next);
            }
            return;
        }

        if (preg_match('/^I read the current state for the groups "([^"]+)"$/', $step, $matches) === 1) {
            $this->groups = self::parseGroups($matches[1]);
            $this->currentStreams = [];
            $token = null;
            do {
                $page = $this->read(MetadataChangeStart::current($token));
                assert_same(MetadataChangeStatus::Ok, $page->status);
                foreach ($page->items as $item) {
                    $body = $item->body;
                    if ($body instanceof MetadataStreamChange) {
                        $this->currentStreams[$body->stream_id] = $body->name;
                    }
                }
                $token = $page->current_page_token;
            } while ($token !== null);
            $this->cursor = $page->next;
            assert_not_null($this->cursor);
            ksort($this->currentStreams);
            return;
        }

        if ($step === 'the current state should list the same streams as the stream list') {
            assert_same(stream_list($this->requireClient()), $this->currentStreams);
            return;
        }

        if (preg_match('/^a user named "([^"]+)" with password "([^"]+)" and no server permissions exists$/', $step, $matches) === 1) {
            create_user_without_permissions($this->requireClient(), $matches[1], $matches[2]);
            $this->username = $matches[1];
            return;
        }

        if (preg_match('/^I authenticate as "([^"]+)" with password "([^"]+)"$/', $step, $matches) === 1) {
            $this->client = new IggyClient(TestEnvironment::serverAddress());
            $this->client->connect();
            $this->client->loginUser($matches[1], $matches[2]);
            return;
        }

        if ($step === 'reading the metadata changes should be refused as unauthorized') {
            $this->groups = [];
            $error = assert_throws(fn () => $this->read(MetadataChangeStart::earliest()));
            assert_instance_of(AuthenticationException::class, $error);
            assert_same(self::UNAUTHORIZED, $error->getCode());
            return;
        }

        self::fail("Unsupported BDD step: {$step}");
    }

    private function read(MetadataChangeStart $start): MetadataChanges
    {
        return $this->requireClient()->getMetadataChanges($start, $this->groups, limit: self::PAGE_LIMIT);
    }

    /** @return list<MetadataChangeRecord> */
    private function follow(int $count): array
    {
        assert_not_null($this->cursor);
        $cursor = $this->cursor;
        $records = [];
        for ($attempt = 0; $attempt < self::FOLLOW_ATTEMPTS; $attempt++) {
            $page = $this->read(MetadataChangeStart::after($cursor));
            assert_same(MetadataChangeStatus::Ok, $page->status);
            $cursor = $page->next;
            assert_not_null($cursor);
            array_push($records, ...$page->records);
            if ($page->has_more) {
                continue;
            }
            if (count($this->streamRecords($records)) >= $count) {
                break;
            }
            usleep(self::FOLLOW_INTERVAL_MICROSECONDS);
        }

        return $records;
    }

    /** @return list<MetadataChangeRecord> */
    private function streamRecords(array $records): array
    {
        $streamId = $this->requireStreamId();

        return array_values(array_filter(
            $records,
            static function (MetadataChangeRecord $record) use ($streamId): bool {
                $change = $record->change;

                return property_exists($change, 'stream_id') && $change->stream_id === $streamId;
            },
        ));
    }

    private function followedRecord(string $kind): MetadataChangeRecord
    {
        $wanted = MetadataChangeKind::from($kind);
        foreach ($this->followed as $record) {
            if ($record->kind === $wanted) {
                return $record;
            }
        }

        self::fail("no followed {$kind} change");
    }

    private function followedStream(string $kind): MetadataStreamChange
    {
        $stream = $this->followedRecord($kind)->change;
        assert_instance_of(MetadataStreamChange::class, $stream);
        assert_true(!$stream->options_unreadable, 'stream options stay readable');

        return $stream;
    }

    /** @return list<MetadataChangeGroup> */
    private static function parseGroups(string $names): array
    {
        return array_map(
            static fn (string $name): MetadataChangeGroup => MetadataChangeGroup::from(trim($name)),
            explode(',', $names),
        );
    }
}
