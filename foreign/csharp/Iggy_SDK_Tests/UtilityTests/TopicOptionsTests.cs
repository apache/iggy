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

using Apache.Iggy.Contracts;
using Apache.Iggy.Headers;

namespace Apache.Iggy.Tests.UtilityTests;

public sealed class TopicOptionsTests
{
    [Fact]
    public void ToDictionary_EmitsEveryKeyUnderItsCatalogKind()
    {
        var options = new TopicOptions
        {
            SegmentSize = 134217728,
            EnforceFsync = true,
            MessagesRequiredToSave = 1024,
            SizeOfMessagesRequiredToSave = 1048576,
            PreallocateSegments = false
        }.ToDictionary();

        Assert.Equal(5, options.Count);

        Assert.Equal(HeaderKind.Uint64, options["segment_size"].Kind);
        Assert.Equal(134217728UL, options["segment_size"].ToUInt64());

        Assert.Equal(HeaderKind.Bool, options["enforce_fsync"].Kind);
        Assert.True(options["enforce_fsync"].ToBool());

        Assert.Equal(HeaderKind.Uint32, options["messages_required_to_save"].Kind);
        Assert.Equal(1024U, options["messages_required_to_save"].ToUInt32());

        Assert.Equal(HeaderKind.Uint64, options["size_of_messages_required_to_save"].Kind);
        Assert.Equal(1048576UL, options["size_of_messages_required_to_save"].ToUInt64());

        Assert.Equal(HeaderKind.Bool, options["preallocate_segments"].Kind);
        Assert.False(options["preallocate_segments"].ToBool());
    }

    [Fact]
    public void ToDictionary_EmitsOnlyTheKeysThatWereSet()
    {
        var options = new TopicOptions { EnforceFsync = false }.ToDictionary();

        var entry = Assert.Single(options);
        Assert.Equal("enforce_fsync", entry.Key);
        Assert.False(entry.Value.ToBool());
    }

    [Fact]
    public void ToDictionary_WithNothingSetIsEmpty()
    {
        Assert.Empty(new TopicOptions().ToDictionary());
    }
}
