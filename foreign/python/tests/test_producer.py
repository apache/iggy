# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Tests for the high-level producer API and its direct send mode."""

import ast
import asyncio
import time
from datetime import timedelta

import pytest

from apache_iggy import (
    BackgroundProducerConfig,
    BackpressureMode,
    DirectProducerConfig,
    IggyClient,
    IggyExpiry,
    IggyProducer,
    MaxTopicSize,
    Partitioning,
    ProducerSendError,
    ProducerSharding,
    SendMessage,
    SendMessagesResponse,
)


@pytest.mark.unit
class TestDirectProducerConfig:
    """Test direct-mode configuration independently of a server."""

    def test_defaults_match_the_rust_sdk(self):
        config = DirectProducerConfig()

        assert config.batch_length == 1_000
        assert config.linger_time == timedelta(0)

    def test_fields_are_keyword_only_and_round_trip(self):
        config = DirectProducerConfig(
            batch_length=25,
            linger_time=timedelta(milliseconds=125),
        )

        assert config.batch_length == 25
        assert config.linger_time == timedelta(milliseconds=125)

        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-count
            DirectProducerConfig(25, timedelta(milliseconds=125))

    def test_zero_batch_length_retains_the_rust_sentinel(self):
        assert DirectProducerConfig(batch_length=0).batch_length == 0

    def test_largest_batch_length_round_trips(self):
        assert DirectProducerConfig(batch_length=2**32 - 1).batch_length == 2**32 - 1

    @pytest.mark.parametrize("batch_length", [-1, 2**32])
    def test_batch_length_outside_u32_is_rejected(self, batch_length: int):
        with pytest.raises(ValueError, match="batch_length"):
            DirectProducerConfig(batch_length=batch_length)

    def test_batch_length_binding_overflow_is_distinct(self):
        with pytest.raises(OverflowError):
            DirectProducerConfig(batch_length=2**63)

    @pytest.mark.parametrize(
        "linger_time",
        [timedelta(microseconds=-1), timedelta(seconds=-1)],
    )
    def test_negative_linger_is_rejected(self, linger_time: timedelta):
        with pytest.raises(ValueError, match="negative"):
            DirectProducerConfig(linger_time=linger_time)

    def test_linger_above_u64_microseconds_is_rejected(self):
        with pytest.raises(ValueError, match="linger_time"):
            DirectProducerConfig(linger_time=timedelta(days=999_999_999))

    def test_wrong_field_types_are_rejected(self):
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-type
            DirectProducerConfig(batch_length="1000")
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-type
            DirectProducerConfig(linger_time=1)

    def test_repr_contains_pasteable_python_values(self):
        config = DirectProducerConfig(
            batch_length=25,
            linger_time=timedelta(milliseconds=125),
        )

        printed = repr(config)

        assert "batch_length=25" in printed
        assert "linger_time=datetime.timedelta(microseconds=125000)" in printed
        ast.parse(printed)


@pytest.mark.unit
class TestBackgroundProducerSurface:
    """Test the stable background configuration model without dispatching."""

    def test_sharding_values_are_public_and_distinct(self):
        assert ProducerSharding.ORDERED != ProducerSharding.BALANCED
        assert repr(ProducerSharding.ORDERED) == "ProducerSharding.ORDERED"
        assert repr(ProducerSharding.BALANCED) == "ProducerSharding.BALANCED"

    def test_backpressure_constructors_and_getters(self):
        blocking = BackpressureMode.block()
        timed = BackpressureMode.block_with_timeout(timedelta(milliseconds=250))
        immediate = BackpressureMode.fail_immediately()

        assert blocking.timeout is None
        assert timed.timeout == timedelta(milliseconds=250)
        assert immediate.timeout is None
        assert repr(blocking) == "BackpressureMode.block()"
        assert repr(timed) == (
            "BackpressureMode.block_with_timeout("
            "datetime.timedelta(microseconds=250000))"
        )
        assert repr(immediate) == "BackpressureMode.fail_immediately()"

    def test_backpressure_timeout_validation(self):
        zero = BackpressureMode.block_with_timeout(timedelta(0))
        assert zero.timeout == timedelta(0)

        with pytest.raises(ValueError, match="negative"):
            BackpressureMode.block_with_timeout(timedelta(microseconds=-1))
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-type
            BackpressureMode.block_with_timeout(1)

    def test_defaults_match_the_rust_sdk(self):
        config = BackgroundProducerConfig()

        assert config.num_shards == 1
        assert config.linger_time == timedelta(milliseconds=1)
        assert config.batch_size == 1024 * 1024
        assert config.batch_length == 1_000
        assert config.max_buffer_size == 32 * 1024 * 1024
        assert repr(config.failure_mode) == "BackpressureMode.block()"
        assert config.max_in_flight == 1
        assert config.sharding == ProducerSharding.ORDERED

    def test_every_field_round_trips(self):
        failure_mode = BackpressureMode.block_with_timeout(timedelta(milliseconds=500))
        config = BackgroundProducerConfig(
            num_shards=4,
            linger_time=timedelta(milliseconds=10),
            batch_size=256 * 1024,
            batch_length=50,
            max_buffer_size=8 * 1024 * 1024,
            failure_mode=failure_mode,
            max_in_flight=3,
            sharding=ProducerSharding.BALANCED,
        )

        assert config.num_shards == 4
        assert config.linger_time == timedelta(milliseconds=10)
        assert config.batch_size == 256 * 1024
        assert config.batch_length == 50
        assert config.max_buffer_size == 8 * 1024 * 1024
        assert repr(config.failure_mode) == repr(failure_mode)
        assert config.max_in_flight == 3
        assert config.sharding == ProducerSharding.BALANCED

    def test_fields_are_keyword_only_and_read_only(self):
        config = BackgroundProducerConfig()

        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-count
            BackgroundProducerConfig(2)
        with pytest.raises(AttributeError):
            # pyrefly: ignore  # read-only
            config.num_shards = 2

    @pytest.mark.parametrize(
        "field",
        [
            "num_shards",
            "batch_size",
            "batch_length",
            "max_buffer_size",
            "max_in_flight",
        ],
    )
    def test_unsigned_fields_reject_negative_values(self, field: str):
        with pytest.raises(ValueError, match=field):
            # pyrefly: ignore  # bad-argument-type
            BackgroundProducerConfig(**{field: -1})

    @pytest.mark.parametrize(
        "field",
        [
            "num_shards",
            "batch_size",
            "batch_length",
            "max_buffer_size",
            "max_in_flight",
        ],
    )
    def test_unsigned_fields_reject_wrong_types(self, field: str):
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-type
            BackgroundProducerConfig(**{field: "1"})

    def test_max_buffer_size_checks_semantic_range_before_binding_overflow(self):
        maximum = 2**64 - 1

        assert (
            BackgroundProducerConfig(max_buffer_size=maximum).max_buffer_size == maximum
        )
        with pytest.raises(ValueError, match="max_buffer_size"):
            BackgroundProducerConfig(max_buffer_size=2**64)
        with pytest.raises(OverflowError):
            BackgroundProducerConfig(max_buffer_size=2**127)

    @pytest.mark.parametrize("value", [0, 1])
    def test_zero_and_one_are_preserved_for_threshold_fields(self, value: int):
        config = BackgroundProducerConfig(
            num_shards=value,
            batch_size=value,
            batch_length=value,
            max_buffer_size=value,
            max_in_flight=value,
        )

        assert config.num_shards == value
        assert config.batch_size == value
        assert config.batch_length == value
        assert config.max_buffer_size == value
        assert config.max_in_flight == value

    def test_wrong_variant_and_duration_types_are_rejected(self):
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-type
            BackgroundProducerConfig(linger_time=1)
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-type
            BackgroundProducerConfig(failure_mode="block")
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-type
            BackgroundProducerConfig(sharding="ordered")

    def test_negative_linger_is_rejected(self):
        with pytest.raises(ValueError, match="negative"):
            BackgroundProducerConfig(linger_time=timedelta(microseconds=-1))

    def test_repr_contains_every_public_field(self):
        config = BackgroundProducerConfig(
            num_shards=2,
            linger_time=timedelta(milliseconds=5),
            batch_size=4_096,
            batch_length=8,
            max_buffer_size=65_536,
            failure_mode=BackpressureMode.fail_immediately(),
            max_in_flight=2,
            sharding=ProducerSharding.BALANCED,
        )

        printed = repr(config)

        for expected in [
            "num_shards=2",
            "linger_time=datetime.timedelta(microseconds=5000)",
            "batch_size=4096",
            "batch_length=8",
            "max_buffer_size=65536",
            "failure_mode=BackpressureMode.fail_immediately()",
            "max_in_flight=2",
            "sharding=ProducerSharding.BALANCED",
        ]:
            assert expected in printed
        ast.parse(printed)


class TestProducerCreation:
    """Test initialization and producer-owned resource creation."""

    @pytest.mark.asyncio
    async def test_default_producer_creates_resources_and_is_immediately_usable(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name = unique_name()
        topic_name = unique_name()

        producer = await iggy_client.producer(stream_name, topic_name)
        try:
            response = await producer.send_one(SendMessage("ready"))

            assert isinstance(producer, IggyProducer)
            assert isinstance(response, SendMessagesResponse)
            assert len(response.confirmations) == 1
            assert await iggy_client.get_stream(stream_name) is not None
            assert await iggy_client.get_topic(stream_name, topic_name) is not None
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    async def test_none_mode_uses_the_default_direct_mode(
        self, iggy_client: IggyClient, unique_name
    ):
        producer = await iggy_client.producer(
            unique_name(),
            unique_name(),
            mode=None,
        )
        try:
            response = await producer.send_one(SendMessage("default mode"))
            assert len(response.confirmations) == 1
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    async def test_existing_resources_work_when_creation_is_disabled(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name = unique_name()
        topic_name = unique_name()
        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream_name, topic_name, 1)

        producer = await iggy_client.producer(
            stream_name,
            topic_name,
            create_stream_if_not_exists=False,
            create_topic_if_not_exists=False,
        )
        try:
            response = await producer.send_one(SendMessage("existing"))
            assert len(response.confirmations) == 1
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    async def test_missing_stream_fails_without_returning_a_producer(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name = unique_name()
        topic_name = unique_name()
        not_returned = object()
        result = not_returned

        with pytest.raises(RuntimeError):
            result = await iggy_client.producer(
                stream_name,
                topic_name,
                create_stream_if_not_exists=False,
            )

        assert result is not_returned
        assert await iggy_client.get_stream(stream_name) is None

    @pytest.mark.asyncio
    async def test_missing_topic_fails_after_creating_the_bound_stream(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name = unique_name()
        topic_name = unique_name()
        not_returned = object()
        result = not_returned

        with pytest.raises(RuntimeError):
            result = await iggy_client.producer(
                stream_name,
                topic_name,
                create_topic_if_not_exists=False,
            )

        assert result is not_returned
        assert await iggy_client.get_stream(stream_name) is not None
        assert await iggy_client.get_topic(stream_name, topic_name) is None

    @pytest.mark.asyncio
    async def test_created_topic_uses_requested_topology_and_retention(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name = unique_name()
        topic_name = unique_name()
        expiry = timedelta(minutes=15)
        maximum_size = 2_000_000_000

        producer = await iggy_client.producer(
            stream_name,
            topic_name,
            topic_partitions_count=3,
            topic_message_expiry=IggyExpiry.ExpireDuration(expiry),
            topic_max_size=MaxTopicSize.Custom(maximum_size),
        )
        try:
            topic = await iggy_client.get_topic(stream_name, topic_name)

            assert topic is not None
            assert topic.partitions_count == 3
            assert isinstance(topic.message_expiry, IggyExpiry.ExpireDuration)
            assert topic.message_expiry.duration == expiry
            assert isinstance(topic.max_topic_size, MaxTopicSize.Custom)
            assert topic.max_topic_size.bytes == maximum_size
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    async def test_background_mode_is_explicitly_unimplemented(
        self, iggy_client: IggyClient, unique_name
    ):
        with pytest.raises(NotImplementedError):
            await iggy_client.producer(
                unique_name(),
                unique_name(),
                mode=BackgroundProducerConfig(),
            )


class TestProducerSends:
    """Test all direct send operations and partitioning fallbacks."""

    @pytest.mark.asyncio
    async def test_send_and_send_one_return_confirmations(
        self, iggy_client: IggyClient, unique_name
    ):
        producer = await iggy_client.producer(unique_name(), unique_name())
        try:
            batch = await producer.send([SendMessage("first"), SendMessage("second")])
            single = await producer.send_one(SendMessage("third"))

            assert isinstance(batch, SendMessagesResponse)
            assert isinstance(single, SendMessagesResponse)
            assert len(batch.confirmations) == 1
            assert len(single.confirmations) == 1
            assert single.confirmations[0].base_offset == 2
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    async def test_default_producer_partitioning_is_balanced(
        self, iggy_client: IggyClient, unique_name
    ):
        producer = await iggy_client.producer(
            unique_name(),
            unique_name(),
            topic_partitions_count=3,
        )
        try:
            responses = [
                await producer.send_one(SendMessage(str(index))) for index in range(3)
            ]

            assert {
                response.confirmations[0].partition_id for response in responses
            } == {0, 1, 2}
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    async def test_per_call_partitioning_overrides_and_falls_back(
        self, iggy_client: IggyClient, unique_name
    ):
        producer = await iggy_client.producer(
            unique_name(),
            unique_name(),
            partitioning=Partitioning.partition_id(2),
            topic_partitions_count=3,
        )
        try:
            configured = await producer.send([SendMessage("configured")])
            overridden = await producer.send_with_partitioning(
                [SendMessage("override")], Partitioning.partition_id(0)
            )
            omitted = await producer.send_with_partitioning([SendMessage("omitted")])
            explicit_none = await producer.send_with_partitioning(
                [SendMessage("none")], None
            )

            assert configured.confirmations[0].partition_id == 2
            assert overridden.confirmations[0].partition_id == 0
            assert omitted.confirmations[0].partition_id == 2
            assert explicit_none.confirmations[0].partition_id == 2
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    async def test_send_to_accepts_string_and_numeric_identifiers(
        self, iggy_client: IggyClient, unique_name
    ):
        destination_stream = unique_name()
        destination_topic = unique_name()
        await iggy_client.create_stream(destination_stream)
        await iggy_client.create_topic(destination_stream, destination_topic, 2)
        stream = await iggy_client.get_stream(destination_stream)
        topic = await iggy_client.get_topic(destination_stream, destination_topic)
        assert stream is not None
        assert topic is not None

        producer = await iggy_client.producer(
            unique_name(),
            unique_name(),
            partitioning=Partitioning.partition_id(1),
        )
        try:
            by_name = await producer.send_to(
                destination_stream,
                destination_topic,
                [SendMessage("names")],
            )
            by_id = await producer.send_to(
                stream.id,
                topic.id,
                [SendMessage("ids")],
                None,
            )

            assert by_name.confirmations[0].stream_id == stream.id
            assert by_name.confirmations[0].topic_id == topic.id
            assert by_name.confirmations[0].partition_id == 1
            assert by_id.confirmations[0].stream_id == stream.id
            assert by_id.confirmations[0].topic_id == topic.id
            assert by_id.confirmations[0].partition_id == 1
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    async def test_empty_batches_are_successful_no_ops_before_shutdown(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name = unique_name()
        topic_name = unique_name()
        producer = await iggy_client.producer(stream_name, topic_name)
        try:
            responses = [
                await producer.send([]),
                await producer.send_with_partitioning([]),
                await producer.send_to(stream_name, topic_name, []),
            ]

            assert all(
                isinstance(response, SendMessagesResponse) for response in responses
            )
            assert all(response.confirmations == [] for response in responses)
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    async def test_batch_length_splits_direct_requests(
        self, iggy_client: IggyClient, unique_name
    ):
        producer = await iggy_client.producer(
            unique_name(),
            unique_name(),
            mode=DirectProducerConfig(batch_length=2),
        )
        try:
            response = await producer.send(
                [SendMessage(str(index)) for index in range(5)]
            )

            assert len(response.confirmations) == 3
            assert [
                confirmation.base_offset for confirmation in response.confirmations
            ] == [0, 2, 4]
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    async def test_linger_paces_sequential_direct_sends(
        self, iggy_client: IggyClient, unique_name
    ):
        linger = timedelta(milliseconds=150)
        producer = await iggy_client.producer(
            unique_name(),
            unique_name(),
            mode=DirectProducerConfig(linger_time=linger),
        )
        try:
            await producer.send_one(SendMessage("first"))
            started_at = time.monotonic()
            await producer.send_one(SendMessage("second"))
            elapsed = time.monotonic() - started_at

            assert elapsed >= 0.1
        finally:
            await producer.shutdown()


class TestProducerLifecycle:
    """Test producer ownership, shutdown, and asynchronous context management."""

    @pytest.mark.asyncio
    async def test_linger_sends_run_concurrently_under_shared_lifecycle_access(
        self, iggy_client: IggyClient, unique_name
    ):
        linger_seconds = 2
        producer = await iggy_client.producer(
            unique_name(),
            unique_name(),
            mode=DirectProducerConfig(linger_time=timedelta(seconds=linger_seconds)),
        )
        try:
            await producer.send_one(SendMessage("prime linger"))
            started_at = time.monotonic()
            responses = await asyncio.gather(
                producer.send_one(SendMessage("concurrent one")),
                producer.send_one(SendMessage("concurrent two")),
            )
            elapsed = time.monotonic() - started_at

            assert len(responses) == 2
            assert all(len(response.confirmations) == 1 for response in responses)
            # Both reads wait against the same timestamp. A mutex around the
            # producer would make the second wait through another full linger.
            assert 1.2 <= elapsed < 3.6
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_is_sequentially_and_concurrently_idempotent(
        self, iggy_client: IggyClient, unique_name
    ):
        producer = await iggy_client.producer(unique_name(), unique_name())

        assert await asyncio.gather(producer.shutdown(), producer.shutdown()) == [
            None,
            None,
        ]
        assert await producer.shutdown() is None

    @pytest.mark.asyncio
    async def test_every_send_rejects_use_after_shutdown(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name = unique_name()
        topic_name = unique_name()
        producer = await iggy_client.producer(stream_name, topic_name)
        await producer.shutdown()

        calls = [
            producer.send([]),
            producer.send_one(SendMessage("closed")),
            producer.send_with_partitioning([], None),
            producer.send_to(stream_name, topic_name, [], None),
        ]
        for call in calls:
            with pytest.raises(RuntimeError, match="closed|shut down"):
                await call

    @pytest.mark.asyncio
    async def test_shutdown_waits_for_a_send_holding_lifecycle_access(
        self, iggy_client: IggyClient, unique_name
    ):
        producer = await iggy_client.producer(
            unique_name(),
            unique_name(),
            mode=DirectProducerConfig(linger_time=timedelta(milliseconds=150)),
        )
        await producer.send_one(SendMessage("prime linger"))

        sending = asyncio.ensure_future(producer.send_one(SendMessage("in flight")))

        async def wait_for_active_send():
            # pyrefly: ignore  # missing-attribute
            while not producer._is_send_active():
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_active_send(), timeout=1)
        shutting_down = asyncio.ensure_future(producer.shutdown())

        response, shutdown_result = await asyncio.wait_for(
            asyncio.gather(sending, shutting_down), timeout=2
        )
        assert len(response.confirmations) == 1
        assert shutdown_result is None
        with pytest.raises(RuntimeError, match="closed|shut down"):
            await producer.send_one(SendMessage("too late"))

    @pytest.mark.asyncio
    async def test_async_context_manager_returns_self_and_closes_normally(
        self, iggy_client: IggyClient, unique_name
    ):
        producer = await iggy_client.producer(unique_name(), unique_name())

        async with producer as entered:
            assert entered is producer
            assert (
                len((await entered.send_one(SendMessage("inside"))).confirmations) == 1
            )

        with pytest.raises(RuntimeError, match="closed|shut down"):
            await producer.send_one(SendMessage("outside"))

    @pytest.mark.asyncio
    async def test_async_context_manager_closes_on_exception_without_suppressing_it(
        self, iggy_client: IggyClient, unique_name
    ):
        producer = await iggy_client.producer(unique_name(), unique_name())

        with pytest.raises(LookupError, match="application failure"):
            async with producer:
                raise LookupError("application failure")

        with pytest.raises(RuntimeError, match="closed|shut down"):
            await producer.send_one(SendMessage("outside"))


class TestProducerValidationAndRetries:
    """Test stable error categories and observable retry policy behavior."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("kwargs", "expected_exception"),
        [
            ({"partitioning": 0}, TypeError),
            ({"mode": object()}, TypeError),
            ({"topic_partitions_count": -1}, ValueError),
            ({"topic_partitions_count": 2**32}, ValueError),
            ({"topic_partitions_count": 2**63}, OverflowError),
            ({"topic_message_expiry": timedelta(seconds=1)}, TypeError),
            ({"topic_max_size": 1000}, TypeError),
            ({"send_retries": -1}, ValueError),
            ({"send_retries": 2**32}, ValueError),
            ({"send_retries": 2**63}, OverflowError),
            ({"send_retry_interval": timedelta(0)}, ValueError),
            ({"send_retry_interval": timedelta(microseconds=-1)}, ValueError),
            ({"send_retry_interval": 1}, TypeError),
        ],
    )
    async def test_producer_configuration_errors_have_stable_categories(
        self,
        iggy_client: IggyClient,
        unique_name,
        kwargs: dict,
        expected_exception: type[Exception],
    ):
        with pytest.raises(expected_exception):
            await iggy_client.producer(unique_name(), unique_name(), **kwargs)

    @pytest.mark.asyncio
    async def test_bound_destination_requires_names(
        self, iggy_client: IggyClient, unique_name
    ):
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-type
            await iggy_client.producer(1, unique_name())
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-type
            await iggy_client.producer(unique_name(), 1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("stream", "topic"),
        [
            ("", "topic"),
            ("stream", ""),
            ("x" * 256, "topic"),
            ("stream", "x" * 256),
        ],
    )
    async def test_bound_destination_rejects_invalid_names_as_value_errors(
        self, iggy_client: IggyClient, stream: str, topic: str
    ):
        with pytest.raises(ValueError):
            await iggy_client.producer(stream, topic)

    @pytest.mark.asyncio
    async def test_send_arguments_reject_low_level_shorthands_and_wrong_types(
        self, iggy_client: IggyClient, unique_name
    ):
        producer = await iggy_client.producer(unique_name(), unique_name())
        try:
            with pytest.raises(TypeError):
                # pyrefly: ignore  # bad-argument-type
                await producer.send((SendMessage("tuple"),))
            with pytest.raises(TypeError):
                # pyrefly: ignore  # bad-argument-type
                await producer.send(["payload"])
            with pytest.raises(TypeError):
                # pyrefly: ignore  # bad-argument-type
                await producer.send_one("payload")
            with pytest.raises(TypeError):
                # pyrefly: ignore  # bad-argument-type
                await producer.send_with_partitioning([SendMessage("integer")], 0)
            with pytest.raises(TypeError):
                # pyrefly: ignore  # bad-argument-type
                await producer.send_to(object(), "topic", [SendMessage("bad stream")])
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("identifier", [-1, 2**32, 2**63])
    async def test_send_to_identifier_overflow_is_not_folded_into_type_error(
        self, iggy_client: IggyClient, unique_name, identifier: int
    ):
        producer = await iggy_client.producer(unique_name(), unique_name())
        try:
            for stream, topic in [(identifier, "topic"), ("stream", identifier)]:
                with pytest.raises(OverflowError):
                    await producer.send_to(
                        stream,
                        topic,
                        [SendMessage("outside u32")],
                    )
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    async def test_server_send_errors_preserve_recovery_state(
        self, iggy_client: IggyClient, unique_name
    ):
        producer = await iggy_client.producer(
            unique_name(),
            unique_name(),
            send_retries=0,
        )
        try:
            with pytest.raises(ProducerSendError) as raised:
                await producer.send_with_partitioning(
                    [SendMessage("missing partition")],
                    Partitioning.partition_id(1),
                )

            error = raised.value
            assert isinstance(error, RuntimeError)
            assert error.cause
            assert isinstance(error.__cause__, RuntimeError)
            assert str(error.__cause__) == error.cause
            assert len(error.failed) == 1
            assert isinstance(error.failed[0], SendMessage)
            assert error.committed == []
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("send_retries", [None, 0])
    async def test_none_and_zero_disable_retries(
        self, iggy_client: IggyClient, unique_name, send_retries: int | None
    ):
        producer = await iggy_client.producer(
            unique_name(),
            unique_name(),
            send_retries=send_retries,
            send_retry_interval=timedelta(seconds=5),
        )
        try:
            with pytest.raises(RuntimeError):
                await asyncio.wait_for(
                    producer.send_to(
                        unique_name(), unique_name(), [SendMessage("single attempt")]
                    ),
                    timeout=1,
                )
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    async def test_explicit_none_interval_retries_without_delay(
        self, iggy_client: IggyClient, unique_name
    ):
        producer = await iggy_client.producer(
            unique_name(),
            unique_name(),
            send_retries=10,
            send_retry_interval=None,
        )
        try:
            with pytest.raises(RuntimeError):
                await asyncio.wait_for(
                    producer.send_to(
                        unique_name(), unique_name(), [SendMessage("immediate retries")]
                    ),
                    timeout=1,
                )
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    async def test_custom_retry_interval_paces_later_retries(
        self, iggy_client: IggyClient, unique_name
    ):
        producer = await iggy_client.producer(
            unique_name(),
            unique_name(),
            send_retries=2,
            send_retry_interval=timedelta(milliseconds=150),
        )
        try:
            started_at = time.monotonic()
            with pytest.raises(RuntimeError):
                await producer.send_to(
                    unique_name(), unique_name(), [SendMessage("paced retries")]
                )
            elapsed = time.monotonic() - started_at

            assert elapsed >= 0.1
        finally:
            await producer.shutdown()

    @pytest.mark.asyncio
    async def test_default_retry_policy_retains_the_rust_interval(
        self, iggy_client: IggyClient, unique_name
    ):
        producer = await iggy_client.producer(unique_name(), unique_name())
        try:
            started_at = time.monotonic()
            with pytest.raises(RuntimeError):
                await asyncio.wait_for(
                    producer.send_to(
                        unique_name(), unique_name(), [SendMessage("default retries")]
                    ),
                    timeout=6,
                )
            elapsed = time.monotonic() - started_at

            # The first retry is immediate. Three retries with the one-second
            # default wait for two later interval ticks.
            assert elapsed >= 1.5
        finally:
            await producer.shutdown()
