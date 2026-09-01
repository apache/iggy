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

namespace Apache.Iggy.Vsr;

/// <summary>
///     Replicated operation discriminant, byte 176 of a request header and byte 208 of a reply header.
///     Mirrors <c>core/binary_protocol/src/consensus/operation.rs</c>; discriminants are wire-pinned.
/// </summary>
internal enum VsrOperation : byte
{
    Reserved = 0,
    Register = 1,
    NonReplicated = 2,
    Logout = 3,

    CreateTopicWithAssignments = 64,
    CreatePartitionsWithAssignments = 65,
    RemoveConsumerGroupMember = 66,
    CompleteConsumerGroupRevocation = 67,
    TruncatePartition = 68,

    CreateStream = 128,
    UpdateStream = 129,
    DeleteStream = 130,
    PurgeStream = 131,
    CreateTopic = 132,
    UpdateTopic = 133,
    DeleteTopic = 134,
    PurgeTopic = 135,
    CreatePartitions = 136,
    DeletePartitions = 137,
    DeleteSegments = 138,
    CreateConsumerGroup = 139,
    DeleteConsumerGroup = 140,
    CreateUser = 141,
    UpdateUser = 142,
    DeleteUser = 143,
    ChangePassword = 144,
    UpdatePermissions = 145,
    CreatePersonalAccessToken = 146,
    DeletePersonalAccessToken = 147,
    JoinConsumerGroup = 148,
    LeaveConsumerGroup = 149,

    SendMessages = 160,
    StoreConsumerOffset = 161,
    DeleteConsumerOffset = 162
}

internal static class VsrOperations
{
    private const byte InternalStart = (byte)VsrOperation.CreateTopicWithAssignments;
    private const byte MetadataStart = (byte)VsrOperation.CreateStream;

    /// <summary>
    ///     Non-replicated codes this build knows to leave no server-side state behind, so re-sending one after a
    ///     lost connection is indistinguishable from sending it once. Flushing an unsaved buffer is included: it
    ///     is idempotent by construction, a second flush writes nothing new.
    /// </summary>
    private static readonly HashSet<int> NonReplicatedReadCodes =
    [
        CommandCodes.PING_CODE,
        CommandCodes.GET_STATS_CODE,
        CommandCodes.GET_SNAPSHOT_CODE,
        CommandCodes.GET_CLUSTER_METADATA_CODE,
        CommandCodes.GET_ME_CODE,
        CommandCodes.GET_CLIENT_CODE,
        CommandCodes.GET_CLIENTS_CODE,
        CommandCodes.GET_USER_CODE,
        CommandCodes.GET_USERS_CODE,
        CommandCodes.GET_PERSONAL_ACCESS_TOKENS_CODE,
        CommandCodes.FLUSH_UNSAVED_BUFFER_CODE,
        CommandCodes.GET_CONSUMER_OFFSET_CODE,
        CommandCodes.GET_STREAM_CODE,
        CommandCodes.GET_STREAMS_CODE,
        CommandCodes.GET_TOPIC_CODE,
        CommandCodes.GET_TOPICS_CODE,
        CommandCodes.GET_CONSUMER_GROUP_CODE,
        CommandCodes.GET_CONSUMER_GROUPS_CODE,
        CommandCodes.SYNC_CONSUMER_GROUP_CODE
    ];

    /// <summary>
    ///     Maps a legacy command code to the operation its request header carries. An unmapped code rides
    ///     <see cref="VsrOperation.NonReplicated" />: the command table is a protocol registry, not a
    ///     per-server capability list, so the server is the authority on codes this SDK build does not know.
    /// </summary>
    internal static VsrOperation ForCode(int code)
    {
        return code switch
        {
            CommandCodes.LOGIN_REGISTER_CODE or CommandCodes.LOGIN_REGISTER_WITH_PAT_CODE => VsrOperation.Register,

            // VSR replaces the legacy login codes with the register handshake. A legacy login code arriving here
            // means a caller bypassed the typed path, and sending it non-replicated would look like a working
            // login while no session is ever bound.
            CommandCodes.LOGIN_USER_CODE or CommandCodes.LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE =>
                throw VsrError.Exception(VsrError.INVALID_COMMAND,
                    $"Command {code} cannot be sent as a consensus request."),
            CommandCodes.LOGOUT_USER_CODE => VsrOperation.Logout,
            CommandCodes.CREATE_USER_CODE => VsrOperation.CreateUser,
            CommandCodes.DELETE_USER_CODE => VsrOperation.DeleteUser,
            CommandCodes.UPDATE_USER_CODE => VsrOperation.UpdateUser,
            CommandCodes.UPDATE_PERMISSIONS_CODE => VsrOperation.UpdatePermissions,
            CommandCodes.CHANGE_PASSWORD_CODE => VsrOperation.ChangePassword,
            CommandCodes.CREATE_PERSONAL_ACCESS_TOKEN_CODE => VsrOperation.CreatePersonalAccessToken,
            CommandCodes.DELETE_PERSONAL_ACCESS_TOKEN_CODE => VsrOperation.DeletePersonalAccessToken,
            CommandCodes.SEND_MESSAGES_CODE => VsrOperation.SendMessages,
            CommandCodes.STORE_CONSUMER_OFFSET_CODE => VsrOperation.StoreConsumerOffset,
            CommandCodes.DELETE_CONSUMER_OFFSET_CODE => VsrOperation.DeleteConsumerOffset,
            CommandCodes.CREATE_STREAM_CODE => VsrOperation.CreateStream,
            CommandCodes.DELETE_STREAM_CODE => VsrOperation.DeleteStream,
            CommandCodes.UPDATE_STREAM_CODE => VsrOperation.UpdateStream,
            CommandCodes.PURGE_STREAM_CODE => VsrOperation.PurgeStream,
            CommandCodes.CREATE_TOPIC_CODE => VsrOperation.CreateTopic,
            CommandCodes.DELETE_TOPIC_CODE => VsrOperation.DeleteTopic,
            CommandCodes.UPDATE_TOPIC_CODE => VsrOperation.UpdateTopic,
            CommandCodes.PURGE_TOPIC_CODE => VsrOperation.PurgeTopic,
            CommandCodes.CREATE_PARTITIONS_CODE => VsrOperation.CreatePartitions,
            CommandCodes.DELETE_PARTITIONS_CODE => VsrOperation.DeletePartitions,
            CommandCodes.DELETE_SEGMENTS_CODE => VsrOperation.DeleteSegments,
            CommandCodes.CREATE_CONSUMER_GROUP_CODE => VsrOperation.CreateConsumerGroup,
            CommandCodes.DELETE_CONSUMER_GROUP_CODE => VsrOperation.DeleteConsumerGroup,
            CommandCodes.JOIN_CONSUMER_GROUP_CODE => VsrOperation.JoinConsumerGroup,
            CommandCodes.LEAVE_CONSUMER_GROUP_CODE => VsrOperation.LeaveConsumerGroup,
            _ => VsrOperation.NonReplicated
        };
    }

    /// <summary>
    ///     Whether losing the connection mid-request leaves nothing for the server to deduplicate, so the request
    ///     can be re-issued on a fresh session by the reconnect path instead of failing the caller.
    /// </summary>
    internal static bool IsReplaySafeRead(int code, bool isLoginRegister, ReadOnlySpan<byte> body)
    {
        // A register lost mid-flight is replayable: the retry re-arms the session under a fresh client id, so
        // it cannot be mistaken for the first one. At worst the server keeps an entry nobody binds, which it
        // ages out. Reporting an unknown outcome here instead would deny the caller the retry that is in fact
        // the only correct response.
        if (isLoginRegister)
        {
            return true;
        }

        var operation = ForCode(code);

        // A consumer offset write carries an absolute offset and the server applies it as an unconditional
        // overwrite, on a plane that keeps no client table to dedup against, so a replay lands on the same
        // value. Denying the retry here reports an unknown outcome for a blip on an offset commit, which
        // takes down the consume loop over a write that was safe to repeat.
        if (operation is VsrOperation.StoreConsumerOffset or VsrOperation.DeleteConsumerOffset)
        {
            return true;
        }

        if (operation != VsrOperation.NonReplicated)
        {
            return false;
        }

        // A poll that auto-commits moves the consumer offset server-side, so a reply lost after the commit
        // would make the replay start past a batch the caller never saw. auto_commit is the last body byte.
        if (code == CommandCodes.POLL_MESSAGES_CODE)
        {
            return body.Length > 0 && body[^1] == 0;
        }

        // Everything else non-replicated is replay-safe only if this build knows it to be a read. An unmapped
        // code also lands on NonReplicated, and re-sending one the server implements as a mutation would apply
        // it twice with nothing to deduplicate against.
        return NonReplicatedReadCodes.Contains(code);
    }

    /// <summary>
    ///     Whether the byte is a declared discriminant. Replies carry a server-controlled operation byte, so
    ///     an undeclared value is rejected rather than classified by range.
    /// </summary>
    internal static bool IsKnown(byte value)
    {
        return (VsrOperation)value switch
        {
            VsrOperation.Reserved or VsrOperation.Register or VsrOperation.NonReplicated or VsrOperation.Logout =>
                true,
            >= VsrOperation.CreateTopicWithAssignments and <= VsrOperation.TruncatePartition => true,
            >= VsrOperation.CreateStream and <= VsrOperation.LeaveConsumerGroup => true,
            VsrOperation.SendMessages or VsrOperation.StoreConsumerOffset or VsrOperation.DeleteConsumerOffset =>
                true,
            _ => false
        };
    }

    /// <summary>Replica / journal only; never emitted by a client.</summary>
    internal static bool IsInternal(this VsrOperation operation)
    {
        return (byte)operation >= InternalStart && (byte)operation < MetadataStart;
    }

    /// <summary>
    ///     Control-plane operations handled by shard 0. Enumerated member by member, mirroring
    ///     <c>Operation::is_metadata</c>, rather than tested as a numeric range: the metadata block runs to 159
    ///     but the last member declared today is 149, so a range would silently exclude the next operation added
    ///     upstream. That operation would then also read as not result-framed, and a committed rejection in its
    ///     reply would decode as a successful payload.
    /// </summary>
    internal static bool IsMetadata(this VsrOperation operation)
    {
        return operation.IsInternal() || operation is VsrOperation.CreateStream
            or VsrOperation.UpdateStream
            or VsrOperation.DeleteStream
            or VsrOperation.PurgeStream
            or VsrOperation.CreateTopic
            or VsrOperation.UpdateTopic
            or VsrOperation.DeleteTopic
            or VsrOperation.PurgeTopic
            or VsrOperation.CreatePartitions
            or VsrOperation.DeletePartitions
            or VsrOperation.CreateConsumerGroup
            or VsrOperation.DeleteConsumerGroup
            or VsrOperation.CreateUser
            or VsrOperation.UpdateUser
            or VsrOperation.DeleteUser
            or VsrOperation.ChangePassword
            or VsrOperation.UpdatePermissions
            or VsrOperation.CreatePersonalAccessToken
            or VsrOperation.DeletePersonalAccessToken
            or VsrOperation.JoinConsumerGroup
            or VsrOperation.LeaveConsumerGroup;
    }

    /// <summary>
    ///     Whether a reply for this operation leads its body with the committed result section. Metadata ops
    ///     always do; on the partition plane only the consumer-offset ops do. Register is result-framed only
    ///     when its body is non-empty, which is why that case stays in <see cref="VsrReplyDecoder" />.
    /// </summary>
    internal static bool IsResultFramed(this VsrOperation operation)
    {
        return operation.IsMetadata() || operation is VsrOperation.StoreConsumerOffset
            or VsrOperation.DeleteConsumerOffset;
    }
}
