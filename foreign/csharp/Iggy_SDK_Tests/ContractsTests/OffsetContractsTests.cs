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

using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Kinds;

namespace Apache.Iggy.Tests.ContractsTests;

/// <summary>
///     Golden bodies of the consumer offset requests, in the layout of the Rust encoders in
///     core/binary_protocol/src/requests/consumer_offsets: the consumer, the stream and the topic, the partition flag
///     and id, then the offset and the quorum acknowledgement where the request carries them. One request names
///     consumer group 7, stream 1, topic "orders" and partition 3. The other names consumer "reader", stream 1,
///     topic 2 and no partition.
/// </summary>
public sealed class OffsetContractsTests
{
    private const string GroupPrefix = "0201040700000001040100000002066f7264657273";
    private const string ConsumerPrefix = "010206726561646572010401000000010402000000";
    private const string Partition3 = "0103000000";
    private const string NoPartition = "0000000000";
    private const string Offset42 = "2a00000000000000";
    private const string Quorum = "01";
    private const ulong Offset = 42;

    private static readonly Identifier Stream = Identifier.Numeric(1);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void UpdateOffset_EncodesTheStoreGoldenBody(bool group)
    {
        var (consumer, topic, partitionId, head) = Request(group);

        var body = TcpContracts.UpdateOffset(Stream, topic, consumer, Offset, partitionId);

        Assert.Equal(Convert.FromHexString(head + Offset42 + Quorum), body);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void GetOffset_EncodesTheGetGoldenBody(bool group)
    {
        var (consumer, topic, partitionId, head) = Request(group);

        var body = TcpContracts.GetOffset(Stream, topic, consumer, partitionId);

        Assert.Equal(Convert.FromHexString(head), body);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void DeleteOffset_EncodesTheDeleteGoldenBody(bool group)
    {
        var (consumer, topic, partitionId, head) = Request(group);

        var body = TcpContracts.DeleteOffset(Stream, topic, consumer, partitionId);

        Assert.Equal(Convert.FromHexString(head + Quorum), body);
    }

    /// <summary>
    ///     The request of the group or of the plain consumer, and the golden bytes up to the partition id.
    /// </summary>
    private static (Consumer Consumer, Identifier Topic, uint? PartitionId, string Head) Request(bool group)
    {
        return group
            ? (Consumer.Group(7), Identifier.String("orders"), 3, GroupPrefix + Partition3)
            : (Consumer.New("reader"), Identifier.Numeric(2), null, ConsumerPrefix + NoPartition);
    }
}
