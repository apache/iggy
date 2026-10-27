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

using Apache.Iggy.Enums;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Group and kind codes and the parent rules of a current state scan.
/// </summary>
internal static class MetadataChangeGroups
{
    internal const byte ALL = byte.MaxValue;

    internal static bool IsDefined(MetadataChangeGroup group)
    {
        return group is >= MetadataChangeGroup.Streams and <= MetadataChangeGroup.Sessions;
    }

    internal static bool IsDefined(MetadataChangeKind kind)
    {
        return Enum.IsDefined(kind);
    }

    /// <summary>Bit <c>code - 1</c> of a group mask.</summary>
    internal static byte Bit(MetadataChangeGroup group)
    {
        return (byte)(1 << ((byte)group - 1));
    }

    /// <summary>The mask of the requested groups, every group when none is requested.</summary>
    internal static byte Mask(IReadOnlyCollection<MetadataChangeGroup> groups)
    {
        if (groups.Count == 0)
        {
            return ALL;
        }

        byte mask = 0;
        foreach (var group in groups)
        {
            mask |= Bit(group);
        }

        return mask;
    }

    /// <summary>The groups a mask selects, in code order.</summary>
    internal static IReadOnlyList<MetadataChangeGroup> FromMask(byte mask)
    {
        return Enum.GetValues<MetadataChangeGroup>().Where(group => (mask & Bit(group)) != 0).ToArray();
    }

    internal static MetadataChangeGroup Of(MetadataChangeKind kind)
    {
        return kind switch
        {
            MetadataChangeKind.StreamCreated or MetadataChangeKind.StreamUpdated or MetadataChangeKind.StreamDeleted =>
                MetadataChangeGroup.Streams,
            MetadataChangeKind.TopicCreated or MetadataChangeKind.TopicUpdated or MetadataChangeKind.TopicDeleted =>
                MetadataChangeGroup.Topics,
            MetadataChangeKind.PartitionsChanged => MetadataChangeGroup.Partitions,
            MetadataChangeKind.ConsumerGroupCreated or MetadataChangeKind.ConsumerGroupDeleted =>
                MetadataChangeGroup.ConsumerGroups,
            MetadataChangeKind.ConsumerGroupMemberJoined or MetadataChangeKind.ConsumerGroupMemberLeft
                or MetadataChangeKind.ConsumerGroupPartitionHandedOver => MetadataChangeGroup.ConsumerGroupMembers,
            MetadataChangeKind.UserCreated or MetadataChangeKind.UserUpdated or MetadataChangeKind.UserDeleted
                or MetadataChangeKind.UserPasswordChanged or MetadataChangeKind.UserPermissionsUpdated =>
                MetadataChangeGroup.Users,
            MetadataChangeKind.PersonalAccessTokenCreated or MetadataChangeKind.PersonalAccessTokenDeleted =>
                MetadataChangeGroup.PersonalAccessTokens,
            MetadataChangeKind.SessionRegistered or MetadataChangeKind.SessionClosed => MetadataChangeGroup.Sessions,
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
        };
    }

    /// <summary>
    ///     Groups a current state scan of <paramref name="group" /> must include, so a mirror can apply the
    ///     cascades of their deletes.
    /// </summary>
    internal static MetadataChangeGroup[] Parents(MetadataChangeGroup group)
    {
        return group switch
        {
            MetadataChangeGroup.Topics => [MetadataChangeGroup.Streams],
            MetadataChangeGroup.Partitions or MetadataChangeGroup.ConsumerGroups =>
                [MetadataChangeGroup.Topics, MetadataChangeGroup.Streams],
            MetadataChangeGroup.ConsumerGroupMembers =>
            [
                MetadataChangeGroup.ConsumerGroups, MetadataChangeGroup.Partitions, MetadataChangeGroup.Topics,
                MetadataChangeGroup.Streams
            ],
            MetadataChangeGroup.PersonalAccessTokens or MetadataChangeGroup.Sessions => [MetadataChangeGroup.Users],
            _ => []
        };
    }

    internal static bool MissesParent(byte mask)
    {
        return FromMask(mask).Any(group => Parents(group).Any(parent => (mask & Bit(parent)) == 0));
    }
}
