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

import io
import zipfile

import pytest

from apache_iggy import (
    IggyClient,
    SnapshotCompression,
    SystemSnapshotType,
)


class TestSnapshot:
    """Test server snapshot retrieval."""

    @pytest.mark.asyncio
    async def test_snapshot_returns_zip_bytes(self, iggy_client: IggyClient):
        """Capturing a snapshot returns a non-empty ZIP archive as bytes."""
        data = await iggy_client.snapshot(
            SnapshotCompression.Deflated, [SystemSnapshotType.All]
        )

        assert isinstance(data, bytes)
        assert len(data) > 0
        assert zipfile.is_zipfile(io.BytesIO(data))

    @pytest.mark.unit
    @pytest.mark.parametrize("invalid_compression", ["deflated", "", 123, None])
    def test_snapshot_rejects_invalid_snapshot_compression(self, invalid_compression):
        """Passing non-enum snapshot_compression raises TypeError before sending."""
        client = IggyClient("127.0.0.1:1")
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-type
            client.snapshot(invalid_compression, [SystemSnapshotType.Test])

    @pytest.mark.unit
    def test_snapshot_rejects_invalid_snapshot_type(self):
        """An unknown or non-enum snapshot type raises TypeError before sending."""
        client = IggyClient("127.0.0.1:1")
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-type
            client.snapshot(SnapshotCompression.Deflated, ["test"])

    @pytest.mark.unit
    def test_snapshot_rejects_empty_snapshot_types(self):
        """An empty snapshot_types list raises ValueError before sending."""
        client = IggyClient("127.0.0.1:1")
        with pytest.raises(ValueError):
            client.snapshot(SnapshotCompression.Deflated, [])

    @pytest.mark.asyncio
    async def test_snapshot_rejects_all_combined_with_other_type(
        self, iggy_client: IggyClient
    ):
        """Combining 'all' with another snapshot type is rejected."""
        with pytest.raises(RuntimeError, match="(?i)invalid command"):
            await iggy_client.snapshot(
                SnapshotCompression.Deflated,
                [SystemSnapshotType.All, SystemSnapshotType.Test],
            )

    @pytest.mark.asyncio
    async def test_snapshot_deduplicates_snapshot_types(self, iggy_client: IggyClient):
        """Duplicate snapshot types are deduplicated before encoding."""
        data = await iggy_client.snapshot(
            SnapshotCompression.Deflated, [SystemSnapshotType.Test] * 255
        )

        assert isinstance(data, bytes)
        assert len(data) > 0
        assert zipfile.is_zipfile(io.BytesIO(data))
