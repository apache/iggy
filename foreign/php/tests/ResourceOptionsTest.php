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
use Iggy\Durability;
use Iggy\Exception\IggyException;
use Iggy\OptionSpec;
use Iggy\OptionsScope;
use Iggy\PollingStrategy;
use Iggy\SendMessage;
use Iggy\StreamDetails;
use Iggy\StreamOptionKey;
use Iggy\TopicDetails;
use Iggy\TopicOptionKey;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestDox;
use PHPUnit\Framework\TestCase;

final class ResourceOptionsTest extends TestCase
{
    private const FINGERPRINT_PATTERN = '/^[0-9a-f]{16}$/';
    private const CATEGORY_MESSAGES = [
        'invalid_command' => 'Invalid command',
        'invalid_encryption_key' => 'Invalid encryption key',
        'option_not_updatable' => 'Option cannot be updated after creation',
        'option_not_resettable' => 'Option cannot be reset',
        'read_only_option' => 'Option is read-only',
    ];
    private const FLAG_UPDATABLE = 1;
    private const FLAG_RESETTABLE = 2;
    private const FLAG_SECRET = 4;
    private const FLAG_READ_ONLY = 8;
    private const FLAG_CRITICAL = 16;
    private const NEVER = '18446744073709551615';
    // Both durabilities are always sent, so a topic created without options stores them as explicit.
    private const DURABILITY_DEFAULTS = ['consumer_offset_durability' => 'replicated', 'durability' => 'replicated'];
    private const STREAM_DERIVED_KEYS = ['encryption_kek_fingerprint', 'encryption_key_fingerprint', 'encryption_key_source'];
    // Create and update arguments named after the unit they take rather than the option key.
    private const PARAMETER_NAMES = ['message_expiry' => 'message_expiry_micros'];

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
    #[TestDox('A stream created with its own key reports source stream and the key fingerprint on $transport')]
    public function testGivenStreamKeyWhenCreatingShouldReadMatchingProjection(string $transport): void
    {
        $fixture = self::fixture()['stream_key'];
        $client = new_transport_client($transport);
        $stream = $this->createStream($client, unique_name('keyed-stream'), $fixture['key_base64']);

        self::assertProjection($stream, 'stream');
        assert_same($fixture['fingerprint'], $stream->encryption_key_fingerprint);
        assert_same(IggyClient::encryptionKeyFingerprint($fixture['key_base64']), $stream->encryption_key_fingerprint);
    }

    #[DataProvider('transports')]
    #[TestDox('A stream created without a key reports a server-generated key on $transport')]
    public function testGivenKeylessStreamWhenReadingShouldReportServerGeneratedKey(string $transport): void
    {
        $client = new_transport_client($transport);
        $stream = $this->createStream($client, unique_name('keyless-stream'));

        self::assertProjection($stream, 'server');
    }

    #[DataProvider('transports')]
    #[TestDox('Renaming a keyed stream keeps its id, key projection and readable messages on $transport')]
    public function testGivenStreamRenameWhenUpdatingShouldPreserveEncryptionProjection(string $transport): void
    {
        $fixture = self::fixture()['stream_key'];
        $client = new_transport_client($transport);
        $created = $this->createStream($client, unique_name('rename-stream'), $fixture['key_base64']);
        $client->createTopic($created->id, 'events', 1, null, null, null, null);
        $client->sendMessages($created->id, 'events', 0, [new SendMessage('before-rename')]);
        $renamed = unique_name('renamed-stream');

        $client->updateStream($created->id, name: $renamed);

        $stream = $client->getStream($created->id);
        assert_not_null($stream);
        assert_same($created->id, $stream->id);
        assert_same($renamed, $stream->name);
        self::assertProjection($stream, 'stream');
        assert_same($fixture['fingerprint'], $stream->encryption_key_fingerprint);
        assert_same($created->encryption_kek_fingerprint, $stream->encryption_kek_fingerprint);
        assert_same(
            ['before-rename'],
            collect_payloads($client->pollMessages($created->id, 'events', 0, PollingStrategy::offset(0), 10, false)),
        );
    }

    #[DataProvider('transports')]
    #[TestDox('Resetting any stream option is refused with the server category and changes nothing on $transport')]
    public function testGivenStreamResetWhenUpdatingShouldRejectWithoutChangingStream(string $transport): void
    {
        $fixture = self::fixture()['stream_key'];
        $client = new_transport_client($transport);
        $created = $this->createStream($client, unique_name('reset-stream'), $fixture['key_base64']);
        $catalog = self::catalogRows('stream');

        foreach (StreamOptionKey::cases() as $key) {
            self::assertRefused(
                static fn () => $client->updateStream($created->id, reset: [$key]),
                self::resetCategory($catalog[$key->value]),
            );
        }
        self::assertRefused(static fn () => $client->updateStream($created->id), 'invalid_command');
        self::assertRefused(
            static fn () => $client->updateStream($created->id, name: unique_name('never'), reset: [StreamOptionKey::Name]),
            'invalid_command',
        );

        $stream = $client->getStream($created->id);
        assert_not_null($stream);
        assert_same($created->name, $stream->name);
        assert_same($fixture['fingerprint'], $stream->encryption_key_fingerprint);
    }

    #[TestDox('A repeated reset name is refused as an invalid command, as server admission refuses it')]
    public function testGivenDuplicateResetWhenUpdatingShouldRejectAsInvalidCommand(): void
    {
        $client = new_client();
        $created = $this->createStream($client, unique_name('duplicate-reset'));

        self::assertRefused(
            static fn () => $client->updateStream($created->id, reset: [StreamOptionKey::Name, StreamOptionKey::Name]),
            'invalid_command',
        );
        self::assertRefused(
            static fn () => $client->updateStream($created->id, reset: [StreamOptionKey::Name], raw_reset: ['name']),
            'invalid_command',
        );
        self::assertRefused(
            static fn () => $client->updateStream($created->id, raw_reset: ['future_option', 'future_option']),
            'invalid_command',
        );
    }

    #[TestDox('Invalid keys are refused before any request and their text never reaches the error')]
    public function testGivenInvalidKeyWhenEncodingShouldRejectBeforeNetworkIo(): void
    {
        $fixture = self::fixture()['stream_key'];
        $neverConnected = new IggyClient(server_address());

        foreach ($fixture['invalid_keys'] as $vector) {
            $input = $vector['input'];
            $key = array_key_exists('raw_hex', $input) ? base64_encode(hex2bin($input['raw_hex'])) : $input['base64'];
            assert_same('invalid_encryption_key', $vector['error']['name']);

            $fingerprintError = self::assertRefused(
                static fn () => IggyClient::encryptionKeyFingerprint($key),
                'invalid_encryption_key',
            );
            $createError = self::assertRefused(
                static fn () => $neverConnected->createStream('never-sent', $key),
                'invalid_encryption_key',
            );
            foreach ([$fingerprintError, $createError] as $error) {
                Assert::assertStringNotContainsString(trim($key), $error->getMessage(), $vector['case']);
                Assert::assertStringNotContainsString($fixture['key_base64'], $error->getMessage(), $vector['case']);
            }
        }

        assert_same($fixture['fingerprint'], IggyClient::encryptionKeyFingerprint($fixture['key_base64']));
        $accepted = assert_throws(static fn () => $neverConnected->createStream('never-sent', $fixture['key_base64']));
        Assert::assertStringNotContainsString(self::CATEGORY_MESSAGES['invalid_encryption_key'], $accepted->getMessage());
    }

    #[TestDox('Raw key bytes passed instead of base64 are refused as an invalid key and never reach the exception trace')]
    public function testGivenRawKeyBytesWhenCreatingShouldRejectWithoutLeakingKey(): void
    {
        $neverConnected = new IggyClient(server_address());
        $key = "\xff" . random_bytes(31);

        $errors = [
            self::assertRefused(static fn () => $neverConnected->createStream('never-sent', $key), 'invalid_encryption_key'),
            self::assertRefused(static fn () => IggyClient::encryptionKeyFingerprint($key), 'invalid_encryption_key'),
        ];

        foreach ($errors as $error) {
            Assert::assertStringNotContainsString(substr($key, 0, 15), $error->getTraceAsString());
            Assert::assertStringNotContainsString($key, print_r($error->getTrace(), true));
            Assert::assertStringNotContainsString(substr($key, 0, 15), $error->getMessage());
        }
    }

    #[TestDox('Raw create and update entries reach the server, which decides keys this build does not know')]
    public function testGivenRawEntriesWhenCreatingAndUpdatingShouldLetServerDecide(): void
    {
        $client = new_client();
        foreach (self::fixture()['create_stream'] as $vector) {
            if (!array_key_exists('raw', $vector['input'])) {
                continue;
            }
            $name = unique_name($vector['input']['name']);
            self::assertRefused(
                static fn () => $client->createStream($name, null, $vector['input']['raw']),
                $vector['admission_error']['name'],
            );
            assert_null($client->getStream($name));
        }

        $created = $this->createStream($client, unique_name('raw-update'));
        self::assertRefused(
            static fn () => $client->updateStream($created->id, raw: ['future_option' => 'enabled']),
            'unsupported_option_key',
        );
        self::assertRefused(
            static fn () => $client->updateStream($created->id, raw_reset: ['future_option']),
            'unsupported_option_key',
        );
        self::assertRefused(
            static fn () => $client->updateStream($created->id, raw_reset: ['encryption_key_source']),
            'read_only_option',
        );
    }

    #[TestDox('Stream vectors match the shared fixture: fingerprint, key constants, create admission and update refusals')]
    public function testGivenSharedVectorsWhenEncodingShouldMatchServerContract(): void
    {
        $fixture = self::fixture();
        assert_same(1, $fixture['schema_version']);
        assert_same(
            $fixture['stream_key']['fingerprint'],
            IggyClient::encryptionKeyFingerprint($fixture['stream_key']['key_base64']),
        );

        $client = new_client();
        foreach ($fixture['create_stream'] as $vector) {
            $input = $vector['input'];
            if (array_key_exists('raw', $input)) {
                continue;
            }
            $key = array_key_exists('encryption_key', $input)
                ? base64_encode(hex2bin($input['encryption_key']['raw_hex']))
                : null;
            assert_null($vector['admission_error'], $vector['case']);
            $stream = $this->createStream($client, unique_name($input['name']), $key);
            self::assertProjection($stream, $key === null ? 'server' : 'stream');
            if ($key !== null) {
                assert_same($fixture['stream_key']['fingerprint'], $stream->encryption_key_fingerprint);
            }
        }

        $target = $this->createStream($client, unique_name('update-errors'));
        foreach ($fixture['update_resource_errors'] as $vector) {
            if ($vector['scope'] !== 'stream') {
                continue;
            }
            $input = $vector['input'];
            $reset = array_map(static fn (string $key): StreamOptionKey => StreamOptionKey::from($key), $input['reset'] ?? []);
            self::assertRefused(
                static fn () => $client->updateStream($target->id, name: $input['name'] ?? null, reset: $reset),
                $vector['error']['name'],
            );
        }
    }

    #[DataProvider('transports')]
    #[TestDox('A topic created with options reads them back with provenance, and an update renames, sets and resets on $transport')]
    public function testGivenTopicOptionsWhenCreatingAndUpdatingShouldReadThemWithProvenance(string $transport): void
    {
        $vector = self::fixture()['create_topic'][0];
        $input = $vector['input'];
        $client = new_transport_client($transport);
        $stream = $this->createStream($client, unique_name('topic-options'));
        $topicName = unique_name($input['name']);

        $client->createTopic(
            $stream->id,
            $topicName,
            $input['partitions_count'],
            $input['compression_algorithm'],
            $input['message_expiry'],
            $input['max_topic_size'],
            $input['segment_size'],
            Durability::from($input['durability']),
            Durability::from($input['consumer_offset_durability']),
            partition_resize_policy: 'fixed',
        );

        $expected = self::phpOptions($vector['wire_options']);
        $expected['partition_resize_policy'] = 'fixed';
        ksort($expected);
        $topic = $client->getTopic($stream->id, $topicName);
        assert_not_null($topic);
        self::assertTopicOptions($topic, $expected);

        $renamed = unique_name('renamed-topic');
        $client->updateTopic(
            $stream->id,
            $topicName,
            name: $renamed,
            message_expiry_micros: self::NEVER,
            reset: [TopicOptionKey::MaxTopicSize],
        );

        $updated = $client->getTopic($stream->id, $renamed);
        assert_not_null($updated);
        assert_same($topic->id, $updated->id);
        $expected['message_expiry'] = self::NEVER;
        unset($expected['max_topic_size']);
        self::assertTopicOptions($updated, $expected);
        assert_same(self::NEVER, $updated->derived_options['max_topic_size']);

        $client->updateTopic($stream->id, $renamed, max_topic_size: $input['max_topic_size']);
        $client->updateTopic($stream->id, $renamed, raw_reset: ['max_topic_size']);
        $reset = $client->getTopic($stream->id, $renamed);
        assert_not_null($reset);
        self::assertTopicOptions($reset, $expected);
    }

    #[TestDox('Topic update vectors are refused with the server category and the topic keeps its options')]
    public function testGivenTopicUpdateVectorsWhenUpdatingShouldRejectWithServerCategory(): void
    {
        $client = new_client();
        $stream = $this->createStream($client, unique_name('topic-errors'));
        $client->createTopic($stream->id, 'events', 1, null, 604800000000, null, null);
        $before = $client->getTopic($stream->id, 'events');
        assert_not_null($before);

        foreach (self::fixture()['update_resource_errors'] as $vector) {
            if ($vector['scope'] !== 'topic') {
                continue;
            }
            $input = $vector['input'];
            $reset = array_map(static fn (string $key): TopicOptionKey => TopicOptionKey::from($key), $input['reset'] ?? []);
            self::assertRefused(
                static fn () => $client->updateTopic(
                    $stream->id,
                    'events',
                    name: $input['name'] ?? null,
                    message_expiry_micros: $input['message_expiry'] ?? null,
                    reset: $reset,
                ),
                $vector['error']['name'],
            );
        }
        self::assertRefused(
            static fn () => $client->updateTopic($stream->id, 'events', reset: [TopicOptionKey::MaxTopicSize, TopicOptionKey::MaxTopicSize]),
            'invalid_command',
        );

        $after = $client->getTopic($stream->id, 'events');
        assert_not_null($after);
        assert_same($before->name, $after->name);
        assert_same($before->options, $after->options);
        assert_same($before->derived_options, $after->derived_options);
    }

    #[TestDox('A topic created without options reads the catalog defaults as derived, and never-expire and unlimited travel exactly')]
    public function testGivenTopicDefaultsWhenReadingShouldReportDerivedCatalogValues(): void
    {
        $client = new_client();
        $stream = $this->createStream($client, unique_name('topic-defaults'));
        $client->createTopic($stream->id, 'defaults', 1, null, null, null, null);
        $client->createTopic($stream->id, 'unbounded', 1, null, self::NEVER, self::NEVER, null);

        $defaults = $client->getTopic($stream->id, 'defaults');
        assert_not_null($defaults);
        $explicit = self::DURABILITY_DEFAULTS;
        assert_same($explicit, self::sorted($defaults->options));
        $expected = [];
        foreach (self::catalogRows('topic') as $key => $row) {
            if ($row['storage'] === 'map' && !array_key_exists($key, $explicit)) {
                $expected[$key] = self::phpScalar($row['default'], $row['kind']);
            }
        }
        ksort($expected);
        $derived = $defaults->derived_options;
        ksort($derived);
        assert_same($expected, $derived);

        $unbounded = $client->getTopic($stream->id, 'unbounded');
        assert_not_null($unbounded);
        assert_same(
            self::sorted([...self::DURABILITY_DEFAULTS, 'max_topic_size' => self::NEVER, 'message_expiry' => self::NEVER]),
            self::sorted($unbounded->options),
        );
    }

    #[TestDox('describeOptions reports every described catalog row with its kind and flags')]
    public function testGivenEveryScopeWhenDescribingOptionsShouldMatchCatalog(): void
    {
        $client = new_client();
        foreach (OptionsScope::cases() as $scope) {
            $described = [];
            foreach ($client->describeOptions($scope) as $spec) {
                Assert::assertInstanceOf(OptionSpec::class, $spec);
                $described[$spec->key] = [
                    $spec->kind,
                    $spec->updatable,
                    $spec->resettable,
                    $spec->secret,
                    $spec->read_only,
                    $spec->critical,
                ];
            }
            $expected = [];
            foreach (self::catalogRows($scope->value) as $key => $row) {
                $expected[$key] = [
                    $row['kind'],
                    ($row['flags'] & self::FLAG_UPDATABLE) !== 0,
                    ($row['flags'] & self::FLAG_RESETTABLE) !== 0,
                    ($row['flags'] & self::FLAG_SECRET) !== 0,
                    ($row['flags'] & self::FLAG_READ_ONLY) !== 0,
                    ($row['flags'] & self::FLAG_CRITICAL) !== 0,
                ];
            }
            assert_same(self::sorted($expected), self::sorted($described), $scope->value);
        }
    }

    #[TestDox('The typed surface covers every catalog row: create arguments, update arguments, reset cases and accessors')]
    public function testGivenFixtureCatalogWhenInspectingTypedSurfaceShouldCoverEveryRow(): void
    {
        $surface = [
            'stream' => [StreamOptionKey::class, 'createStream', 'updateStream'],
            'topic' => [TopicOptionKey::class, 'createTopic', 'updateTopic'],
        ];
        $client = new_client();
        $stream = $this->createStream($client, unique_name('surface'));
        $client->createTopic($stream->id, 'events', 1, null, null, null, null);
        $details = [
            'stream' => $client->getStream($stream->id),
            'topic' => $client->getTopic($stream->id, 'events'),
        ];

        foreach ($surface as $scope => [$enum, $create, $update]) {
            $rows = self::catalogRows($scope);
            $cases = array_map(static fn ($key): string => $key->value, $enum::cases());
            assert_same(self::sorted(array_keys($rows)), self::sorted($cases), "$scope reset cases");
            $createParameters = self::parameterNames($create);
            $updateParameters = self::parameterNames($update);
            Assert::assertContains('raw', $createParameters, "$scope create raw");
            Assert::assertContains('raw', $updateParameters, "$scope update raw");
            Assert::assertContains('raw_reset', $updateParameters, "$scope update raw_reset");
            foreach ($rows as $key => $row) {
                $parameter = self::PARAMETER_NAMES[$key] ?? $key;
                if ($row['storage'] === 'map' && in_array($row['mutability'], ['create_only', 'updatable'], true)) {
                    Assert::assertContains($parameter, $createParameters, "$scope create $key");
                }
                if ($row['mutability'] === 'updatable') {
                    Assert::assertContains($parameter, $updateParameters, "$scope update $key");
                }
                if ($row['mutability'] === 'read_only') {
                    Assert::assertTrue(property_exists($details[$scope], $key), "$scope accessor $key");
                }
            }
        }
    }

    private function createStream(IggyClient $client, string $name, ?string $key = null): StreamDetails
    {
        $client->createStream($name, $key);
        $stream = $client->getStream($name);
        assert_not_null($stream);
        $this->createdStreams[] = $stream->id;

        return $stream;
    }

    private static function assertRefused(callable $callback, string $category): Throwable
    {
        $error = assert_throws($callback, self::CATEGORY_MESSAGES[$category] ?? null);
        Assert::assertInstanceOf(IggyException::class, $error, $error->getMessage());
        assert_same(self::errorCodes()[$category], $error->getCode(), $category);

        return $error;
    }

    private static function assertProjection(StreamDetails $stream, string $source): void
    {
        assert_same([], $stream->options);
        assert_same(self::STREAM_DERIVED_KEYS, array_keys(self::sorted($stream->derived_options)));
        assert_same($source, $stream->encryption_key_source);
        assert_same($source, $stream->derived_options['encryption_key_source']);
        assert_same($stream->encryption_key_fingerprint, $stream->derived_options['encryption_key_fingerprint']);
        assert_same($stream->encryption_kek_fingerprint, $stream->derived_options['encryption_kek_fingerprint']);
        Assert::assertMatchesRegularExpression(self::FINGERPRINT_PATTERN, (string) $stream->encryption_key_fingerprint);
        Assert::assertNotNull($stream->encryption_kek_fingerprint);
        foreach (explode(',', $stream->encryption_kek_fingerprint) as $kekFingerprint) {
            Assert::assertMatchesRegularExpression(self::FINGERPRINT_PATTERN, $kekFingerprint);
        }
        foreach (self::fixture()['constants']['never_returned_keys'] as $secret) {
            Assert::assertArrayNotHasKey($secret, $stream->options);
            Assert::assertArrayNotHasKey($secret, $stream->derived_options);
        }
    }

    private static function assertTopicOptions(TopicDetails $topic, array $explicit): void
    {
        assert_same($explicit, self::sorted($topic->options));
        $mapKeys = [];
        foreach (self::catalogRows('topic') as $key => $row) {
            if ($row['storage'] === 'map') {
                $mapKeys[] = $key;
            }
        }
        $readKeys = [...array_keys($topic->options), ...array_keys($topic->derived_options)];
        assert_same(self::sorted($mapKeys), self::sorted($readKeys));
    }

    private static function phpOptions(array $wireOptions): array
    {
        $options = [];
        foreach ($wireOptions as $key => $option) {
            $options[$key] = self::phpScalar($option['value'], $option['kind']);
        }
        ksort($options);

        return $options;
    }

    // The projection the SDK documents: integers above PHP_INT_MAX stay decimal strings.
    private static function phpScalar(string $value, string $kind): mixed
    {
        return match ($kind) {
            'bool' => $value === 'true',
            'uint8', 'uint16', 'uint32', 'uint64', 'int8', 'int16', 'int32', 'int64' => self::integerOrString($value),
            default => $value,
        };
    }

    private static function integerOrString(string $value): int|string
    {
        $integer = filter_var($value, FILTER_VALIDATE_INT);

        return $integer === false ? $value : $integer;
    }

    private static function parameterNames(string $method): array
    {
        return array_map(
            static fn (ReflectionParameter $parameter): string => $parameter->getName(),
            (new ReflectionMethod(IggyClient::class, $method))->getParameters(),
        );
    }

    private static function sorted(array $values): array
    {
        if (array_is_list($values)) {
            sort($values);
        } else {
            ksort($values);
        }

        return $values;
    }

    private static function errorCodes(): array
    {
        static $codes = null;
        if ($codes !== null) {
            return $codes;
        }
        $fixture = self::fixture();
        $errors = [
            ...array_column($fixture['update_resource_errors'], 'error'),
            ...array_column($fixture['stream_key']['invalid_keys'], 'error'),
            ...array_filter(array_column($fixture['create_stream'], 'admission_error')),
        ];
        $codes = array_column($errors, 'code', 'name');

        return $codes;
    }

    private static function catalogRows(string $scope): array
    {
        $rows = [];
        foreach (self::fixture()['catalog'] as $row) {
            if ($row['described'] && in_array($scope, $row['scopes'], true)) {
                $rows[$row['key']] = $row;
            }
        }

        return $rows;
    }

    private static function resetCategory(array $row): string
    {
        return match (true) {
            in_array($row['visibility'], ['read_only', 'internal'], true) => 'read_only_option',
            $row['mutability'] === 'create_only' => 'option_not_updatable',
            default => 'option_not_resettable',
        };
    }

    private static function fixture(): array
    {
        static $fixture = null;

        return $fixture ??= json_decode(
            (string) file_get_contents(__DIR__ . '/fixtures/resource_options.json'),
            true,
            512,
            JSON_THROW_ON_ERROR | JSON_BIGINT_AS_STRING,
        );
    }
}
