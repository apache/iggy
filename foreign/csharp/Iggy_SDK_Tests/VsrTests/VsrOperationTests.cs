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

using Apache.Iggy.Utils;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.VsrTests;

public sealed class VsrOperationTests
{
    [Theory]
    [InlineData(CommandCodes.LOGOUT_USER_CODE, (byte)VsrOperation.Logout)]
    [InlineData(CommandCodes.CREATE_USER_CODE, (byte)VsrOperation.CreateUser)]
    [InlineData(CommandCodes.DELETE_USER_CODE, (byte)VsrOperation.DeleteUser)]
    [InlineData(CommandCodes.UPDATE_USER_CODE, (byte)VsrOperation.UpdateUser)]
    [InlineData(CommandCodes.UPDATE_PERMISSIONS_CODE, (byte)VsrOperation.UpdatePermissions)]
    [InlineData(CommandCodes.CHANGE_PASSWORD_CODE, (byte)VsrOperation.ChangePassword)]
    [InlineData(CommandCodes.CREATE_PERSONAL_ACCESS_TOKEN_CODE, (byte)VsrOperation.CreatePersonalAccessToken)]
    [InlineData(CommandCodes.DELETE_PERSONAL_ACCESS_TOKEN_CODE, (byte)VsrOperation.DeletePersonalAccessToken)]
    [InlineData(CommandCodes.SEND_MESSAGES_CODE, (byte)VsrOperation.SendMessages)]
    [InlineData(CommandCodes.STORE_CONSUMER_OFFSET_CODE, (byte)VsrOperation.StoreConsumerOffset)]
    [InlineData(CommandCodes.DELETE_CONSUMER_OFFSET_CODE, (byte)VsrOperation.DeleteConsumerOffset)]
    [InlineData(CommandCodes.STORE_CONSUMER_OFFSET_2_CODE, (byte)VsrOperation.StoreConsumerOffset2)]
    [InlineData(CommandCodes.DELETE_CONSUMER_OFFSET_2_CODE, (byte)VsrOperation.DeleteConsumerOffset2)]
    [InlineData(CommandCodes.CREATE_STREAM_CODE, (byte)VsrOperation.CreateStream)]
    [InlineData(CommandCodes.DELETE_STREAM_CODE, (byte)VsrOperation.DeleteStream)]
    [InlineData(CommandCodes.UPDATE_STREAM_CODE, (byte)VsrOperation.UpdateStream)]
    [InlineData(CommandCodes.PURGE_STREAM_CODE, (byte)VsrOperation.PurgeStream)]
    [InlineData(CommandCodes.CREATE_TOPIC_CODE, (byte)VsrOperation.CreateTopic)]
    [InlineData(CommandCodes.DELETE_TOPIC_CODE, (byte)VsrOperation.DeleteTopic)]
    [InlineData(CommandCodes.UPDATE_TOPIC_CODE, (byte)VsrOperation.UpdateTopic)]
    [InlineData(CommandCodes.PURGE_TOPIC_CODE, (byte)VsrOperation.PurgeTopic)]
    [InlineData(CommandCodes.CREATE_PARTITIONS_CODE, (byte)VsrOperation.CreatePartitions)]
    [InlineData(CommandCodes.DELETE_PARTITIONS_CODE, (byte)VsrOperation.DeletePartitions)]
    [InlineData(CommandCodes.DELETE_SEGMENTS_CODE, (byte)VsrOperation.DeleteSegments)]
    [InlineData(CommandCodes.CREATE_CONSUMER_GROUP_CODE, (byte)VsrOperation.CreateConsumerGroup)]
    [InlineData(CommandCodes.DELETE_CONSUMER_GROUP_CODE, (byte)VsrOperation.DeleteConsumerGroup)]
    [InlineData(CommandCodes.JOIN_CONSUMER_GROUP_CODE, (byte)VsrOperation.JoinConsumerGroup)]
    [InlineData(CommandCodes.LEAVE_CONSUMER_GROUP_CODE, (byte)VsrOperation.LeaveConsumerGroup)]
    public void ForCode_MapsReplicatedCommands(int code, byte expected)
    {
        Assert.Equal((VsrOperation)expected, VsrOperations.ForCode(code));
    }

    [Theory]
    [InlineData(CommandCodes.PING_CODE)]
    [InlineData(CommandCodes.POLL_MESSAGES_CODE)]
    [InlineData(CommandCodes.GET_STREAM_CODE)]
    [InlineData(CommandCodes.GET_CONSUMER_OFFSET_CODE)]
    [InlineData(9999)]
    public void ForCode_ReadsAndUnknownCodesRideNonReplicated(int code)
    {
        Assert.Equal(VsrOperation.NonReplicated, VsrOperations.ForCode(code));
    }

    [Theory]
    [InlineData(CommandCodes.LOGIN_REGISTER_CODE)]
    [InlineData(CommandCodes.LOGIN_REGISTER_WITH_PAT_CODE)]
    public void ForCode_MapsTheRegisterHandshake(int code)
    {
        Assert.Equal(VsrOperation.Register, VsrOperations.ForCode(code));
    }

    /// <summary>
    ///     A classic login sent as a consensus request would ride NonReplicated and look like it worked while no
    ///     session is ever bound, so the classification rejects it instead.
    /// </summary>
    [Theory]
    [InlineData(CommandCodes.LOGIN_USER_CODE)]
    [InlineData(CommandCodes.LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE)]
    public void ForCode_RejectsLoginCodes(int code)
    {
        var error = Assert.Throws<IggyInvalidStatusCodeException>(() => VsrOperations.ForCode(code));

        Assert.Equal(VsrError.INVALID_COMMAND, error.StatusCode);
    }

    [Fact]
    public void Classification_MatchesTheServerSideRanges()
    {
        Assert.True(VsrOperation.CreateTopicWithAssignments.IsInternal());
        Assert.True(VsrOperation.CreateTopicWithAssignments.IsMetadata());
        Assert.True(VsrOperation.CreateStream.IsMetadata());
        Assert.False(VsrOperation.CreateStream.IsPartition());
        Assert.True(VsrOperation.SendMessages.IsPartition());
        Assert.False(VsrOperation.SendMessages.IsMetadata());

        // Resolved server-side to an internal truncate, so it is neither plane despite carrying a namespace.
        Assert.False(VsrOperation.DeleteSegments.IsPartition());
        Assert.False(VsrOperation.DeleteSegments.IsMetadata());
    }

    [Fact]
    public void IsResultFramed_CoversMetadataAndConsumerOffsetsOnly()
    {
        Assert.True(VsrOperation.CreateStream.IsResultFramed());
        Assert.True(VsrOperation.StoreConsumerOffset.IsResultFramed());
        Assert.True(VsrOperation.DeleteConsumerOffset2.IsResultFramed());
        Assert.False(VsrOperation.SendMessages.IsResultFramed());
        Assert.False(VsrOperation.NonReplicated.IsResultFramed());
        Assert.False(VsrOperation.Register.IsResultFramed());
        Assert.False(VsrOperation.Logout.IsResultFramed());
    }

    [Fact]
    public void IsKnown_RejectsUndefinedDiscriminants()
    {
        Assert.True(VsrOperations.IsKnown((byte)VsrOperation.SendMessages));
        Assert.False(VsrOperations.IsKnown(163));
        Assert.False(VsrOperations.IsKnown(200));
    }

    /// <summary>Every arm of the control-plane table: shard 0 owns these, so a miss would route to a data shard.</summary>
    [Theory]
    [InlineData((byte)VsrOperation.CreateStream)]
    [InlineData((byte)VsrOperation.UpdateStream)]
    [InlineData((byte)VsrOperation.DeleteStream)]
    [InlineData((byte)VsrOperation.PurgeStream)]
    [InlineData((byte)VsrOperation.CreateTopic)]
    [InlineData((byte)VsrOperation.UpdateTopic)]
    [InlineData((byte)VsrOperation.DeleteTopic)]
    [InlineData((byte)VsrOperation.PurgeTopic)]
    [InlineData((byte)VsrOperation.CreatePartitions)]
    [InlineData((byte)VsrOperation.DeletePartitions)]
    [InlineData((byte)VsrOperation.CreateConsumerGroup)]
    [InlineData((byte)VsrOperation.DeleteConsumerGroup)]
    [InlineData((byte)VsrOperation.CreateUser)]
    [InlineData((byte)VsrOperation.UpdateUser)]
    [InlineData((byte)VsrOperation.DeleteUser)]
    [InlineData((byte)VsrOperation.ChangePassword)]
    [InlineData((byte)VsrOperation.UpdatePermissions)]
    [InlineData((byte)VsrOperation.CreatePersonalAccessToken)]
    [InlineData((byte)VsrOperation.DeletePersonalAccessToken)]
    [InlineData((byte)VsrOperation.JoinConsumerGroup)]
    [InlineData((byte)VsrOperation.LeaveConsumerGroup)]
    public void IsMetadata_CoversTheWholeControlPlane(byte operation)
    {
        Assert.True(((VsrOperation)operation).IsMetadata());
    }

    [Theory]
    [InlineData((byte)VsrOperation.SendMessages)]
    [InlineData((byte)VsrOperation.StoreConsumerOffset)]
    [InlineData((byte)VsrOperation.DeleteConsumerOffset2)]
    [InlineData((byte)VsrOperation.DeleteSegments)]
    [InlineData((byte)VsrOperation.NonReplicated)]
    [InlineData((byte)VsrOperation.Logout)]
    public void IsMetadata_LeavesTheDataPlaneOut(byte operation)
    {
        Assert.False(((VsrOperation)operation).IsMetadata());
    }
}
