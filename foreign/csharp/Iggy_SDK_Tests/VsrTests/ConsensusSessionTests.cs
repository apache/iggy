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

using System.Reflection;
using System.Buffers.Binary;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.VsrTests;

public sealed class ConsensusSessionTests
{
    [Fact]
    public void NewSession_IsUnboundWithNonZeroClientId()
    {
        var session = new ConsensusSession();

        Assert.False(session.IsBound);
        Assert.Null(session.Session);
        Assert.NotEqual(UInt128.Zero, session.ClientId);
    }

    [Fact]
    public void NewSession_MintsUniqueClientIds()
    {
        Assert.NotEqual(new ConsensusSession().ClientId, new ConsensusSession().ClientId);
    }

    [Fact]
    public void NewAndResetSessions_KeepClientIdsOutsideServerRange()
    {
        const int samples = 64;
        var session = new ConsensusSession();
        for (var sample = 0; sample < samples; sample++)
        {
            Assert.InRange(session.ClientId, UInt128.One, (UInt128)Int128.MaxValue);
            session.Reset();
        }
    }

    [Fact]
    public void BeginRegister_OnFreshSessionKeepsClientId()
    {
        var session = new ConsensusSession(7);

        Assert.Equal(0UL, session.Resolve(VsrOperation.Register).RequestId);
        Assert.Equal((UInt128)7, session.ClientId);
        Assert.False(session.IsBound);
    }

    /// <summary>
    ///     The re-arm mints the client id the register is encoded with, so the frame must carry the new one, not
    ///     the one the previous session used.
    /// </summary>
    [Fact]
    public void Resolve_RegisterAfterBindReportsTheReArmedClientId()
    {
        var session = new ConsensusSession(7);
        session.Resolve(VsrOperation.Register);
        session.Bind(42, new byte[LoginRegister.BIND_SECRET_BYTES]);

        var frame = session.Resolve(VsrOperation.Register);

        Assert.Equal(session.ClientId, frame.ClientId);
        Assert.NotEqual((UInt128)7, frame.ClientId);
    }

    [Fact]
    public void BeginRegister_AfterBindReArmsWithFreshClientId()
    {
        var session = new ConsensusSession(7);
        session.Resolve(VsrOperation.Register);
        session.Bind(42, new byte[LoginRegister.BIND_SECRET_BYTES]);
        session.Resolve(VsrOperation.CreateStream);

        Assert.Equal(0UL, session.Resolve(VsrOperation.Register).RequestId);
        Assert.NotEqual((UInt128)7, session.ClientId);
        Assert.False(session.IsBound);
        Assert.Equal(2UL, session.RequestCounter);
    }

    /// <summary>
    ///     A register that never bound is cleared by the reset its failure path runs, and the retry re-arms onto a
    ///     fresh client id rather than reusing the one the server may have already seen.
    /// </summary>
    [Fact]
    public void BeginRegister_AfterConsumedRegisterAndResetReArms()
    {
        var session = new ConsensusSession(7);
        session.Resolve(VsrOperation.Register);

        session.Reset();

        Assert.Equal(0UL, session.Resolve(VsrOperation.Register).RequestId);
        Assert.False(session.IsBound);
        Assert.NotEqual((UInt128)7, session.ClientId);
    }

    [Fact]
    public void NextRequestId_IsMonotonicAfterBind()
    {
        var session = new ConsensusSession(1);
        session.Resolve(VsrOperation.Register);
        session.Bind(10, new byte[LoginRegister.BIND_SECRET_BYTES]);

        Assert.Equal(1UL, session.Resolve(VsrOperation.CreateStream).RequestId);
        Assert.Equal(2UL, session.Resolve(VsrOperation.CreateStream).RequestId);
        Assert.Equal(3UL, session.RequestCounter);
    }

    [Fact]
    public void MetadataRequestIdentifier_IsUniqueAndStableAcrossReencoding()
    {
        var session = new ConsensusSession(1);
        session.Resolve(VsrOperation.Register);
        session.Bind(10, new byte[LoginRegister.BIND_SECRET_BYTES]);
        var first = session.Resolve(VsrOperation.CreateStream);
        var second = session.Resolve(VsrOperation.CreateStream);
        Assert.NotEqual(UInt128.Zero, first.RequestChecksum);
        Assert.NotEqual(first.RequestChecksum, second.RequestChecksum);

        var original = new byte[VsrHeader.HEADER_SIZE];
        var retry = new byte[VsrHeader.HEADER_SIZE];
        VsrHeader.EncodeRequestHeader(original, session, 202, Array.Empty<byte>(), retainedFrame: first);
        VsrHeader.EncodeRequestHeader(retry, session, 202, Array.Empty<byte>(), retainedFrame: first);
        Assert.Equal(first.RequestChecksum,
            BinaryPrimitives.ReadUInt128LittleEndian(original.AsSpan(VsrHeader.REQUEST_CHECKSUM_OFFSET)));
        Assert.Equal(original, retry);
    }

    [Fact]
    public void NextRequestId_BeforeBindThrows()
    {
        var session = new ConsensusSession(1);

        var error = Assert.Throws<IggyInvalidStatusCodeException>(() => session.Resolve(VsrOperation.CreateStream));

        Assert.Equal(VsrError.UNAUTHENTICATED, error.StatusCode);
    }

    [Theory]
    [InlineData((byte)VsrOperation.CreateStream)]
    [InlineData((byte)VsrOperation.SendMessages)]
    public void NextRequestId_ExhaustionPreservesIdentityAndRejectsLocally(byte operation)
    {
        var bindSecret = Enumerable.Range(0, LoginRegister.BIND_SECRET_BYTES)
            .Select(value => (byte)value).ToArray();
        var session = new ConsensusSession(1, 10, bindSecret);
        var clientId = session.ClientId;
        var generation = session.Generation;
        var counter = typeof(ConsensusSession).GetField("_requestCounter",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(counter);
        counter.SetValue(session, ulong.MaxValue - 1);

        var lastFrame = session.Resolve((VsrOperation)operation);
        Assert.Equal(clientId, lastFrame.ClientId);
        Assert.Equal(ulong.MaxValue - 1, lastFrame.RequestId);
        Assert.Equal(10UL, lastFrame.SessionId);
        Assert.Equal(operation == (byte)VsrOperation.CreateStream,
            lastFrame.RequestChecksum != UInt128.Zero);
        Assert.Equal(ulong.MaxValue, session.RequestCounter);

        for (var attempt = 0; attempt < 2; attempt++)
        {
            SessionFrame? issued = null;
            var error = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            {
                issued = session.Resolve((VsrOperation)operation);
            });

            Assert.Null(issued);
            Assert.Equal(VsrError.REQUEST_ID_EXHAUSTED, error.StatusCode);
            Assert.False(error.FromServer);
            Assert.Equal(ulong.MaxValue, session.RequestCounter);
            Assert.Equal(clientId, session.ClientId);
            Assert.Equal(10UL, session.Session);
            Assert.Equal(generation, session.Generation);
            Assert.Equal(bindSecret, session.BindSecret);
        }

        var exhausted = Assert.Throws<IggyInvalidStatusCodeException>(() => session.Resolve(VsrOperation.NonReplicated));
        Assert.Equal(VsrError.REQUEST_ID_EXHAUSTED, exhausted.StatusCode);
        Assert.Equal(ulong.MaxValue, session.RequestCounter);
    }

    [Fact]
    public void Resolve_ConsumesDistinctIdsForNonReplicatedOps()
    {
        var session = new ConsensusSession(1);
        session.Resolve(VsrOperation.Register);
        session.Bind(10, new byte[LoginRegister.BIND_SECRET_BYTES]);

        Assert.Equal(1UL, session.Resolve(VsrOperation.NonReplicated).RequestId);
        Assert.Equal(2UL, session.Resolve(VsrOperation.NonReplicated).RequestId);
        Assert.Equal(3UL, session.RequestCounter);
    }

    [Fact]
    public void Resolve_PartitionOpsConsumeADistinctIdPerSend()
    {
        // Dedup identity requires each send to carry a distinct number, so
        // partition ops advance the counter exactly like metadata ops and the
        // two planes interleave on one sequence.
        var session = new ConsensusSession(1);
        session.Resolve(VsrOperation.Register);
        session.Bind(10, new byte[LoginRegister.BIND_SECRET_BYTES]);

        Assert.Equal(1UL, session.Resolve(VsrOperation.SendMessages).RequestId);
        Assert.Equal(2UL, session.Resolve(VsrOperation.SendMessages).RequestId);
        Assert.Equal(3UL, session.Resolve(VsrOperation.CreateStream).RequestId);
        Assert.Equal(4UL, session.RequestCounter);
    }

    [Fact]
    public void Bind_TwiceThrows()
    {
        var session = new ConsensusSession(1);
        session.Resolve(VsrOperation.Register);
        session.Bind(10, new byte[LoginRegister.BIND_SECRET_BYTES]);

        Assert.Throws<NotConnectedException>(() => session.Bind(20, new byte[LoginRegister.BIND_SECRET_BYTES]));
        Assert.Equal(10UL, session.Session);
    }

    [Fact]
    public void Bind_ZeroThrows()
    {
        var session = new ConsensusSession(1);

        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() => session.Bind(0, new byte[LoginRegister.BIND_SECRET_BYTES]));
        Assert.Equal(VsrError.INVALID_FORMAT, exception.StatusCode);
    }

    [Fact]
    public void Bind_WithoutAnInFlightRegisterThrows()
    {
        var session = new ConsensusSession(1);

        Assert.Throws<NotConnectedException>(() => session.Bind(10, new byte[LoginRegister.BIND_SECRET_BYTES]));
    }

    /// <summary>
    ///     Binding runs after the sending lock is released, so a drop can have re-armed the identity since the
    ///     register committed. The reset clears the pending register, and binding regardless would pair the
    ///     session the server issued to the old client id with the one the re-arm minted.
    /// </summary>
    [Fact]
    public void Bind_AfterTheIdentityReArmedThrows()
    {
        var session = new ConsensusSession(1);
        session.Resolve(VsrOperation.Register);

        session.Reset();

        Assert.Throws<NotConnectedException>(() => session.Bind(10, new byte[LoginRegister.BIND_SECRET_BYTES]));
        Assert.False(session.IsBound);
    }

    /// <summary>
    ///     Two concurrent registers would otherwise re-arm the identity under the first one, so its bind would
    ///     pair a committed session with a client id the server never saw.
    /// </summary>
    [Fact]
    public void Resolve_SecondRegisterWhileOneIsInFlightThrows()
    {
        var session = new ConsensusSession(1);
        session.Resolve(VsrOperation.Register);

        var error = Assert.Throws<IggyInvalidStatusCodeException>(() => session.Resolve(VsrOperation.Register));

        Assert.Equal(VsrError.UNAUTHENTICATED, error.StatusCode);
    }

    [Fact]
    public void Resolve_RegisterIsAllowedAgainAfterReset()
    {
        var session = new ConsensusSession(1);
        session.Resolve(VsrOperation.Register);

        session.Reset();

        Assert.Equal(0UL, session.Resolve(VsrOperation.Register).RequestId);
    }

    [Fact]
    public void Reset_ClearsBindingAndPreservesCounter()
    {
        var session = new ConsensusSession(1);
        session.Resolve(VsrOperation.Register);
        session.Bind(10, new byte[LoginRegister.BIND_SECRET_BYTES]);
        session.Resolve(VsrOperation.CreateStream);

        session.Reset();

        Assert.False(session.IsBound);
        Assert.Equal(2UL, session.RequestCounter);
        Assert.NotEqual((UInt128)1, session.ClientId);
    }

    [Fact]
    public void Generation_AdvancesOnEveryReArmAndOnEveryBindTheCallerSignedInFor()
    {
        var session = new ConsensusSession(1);
        var initialGeneration = session.Generation;

        session.Resolve(VsrOperation.Register);
        session.Bind(10, new byte[LoginRegister.BIND_SECRET_BYTES]);
        Assert.Equal(initialGeneration + 1, session.Generation);

        session.Reset(continues: true);
        Assert.Equal(initialGeneration + 2, session.Generation);

        // The client's own sign-in goes on in the generation that its re-arm opened.
        session.Resolve(VsrOperation.Register);
        session.Bind(11, new byte[LoginRegister.BIND_SECRET_BYTES], continues: true);
        Assert.Equal(initialGeneration + 2, session.Generation);

        // A register on a previously bound session re-arms the identity, which is a new generation too.
        session.Resolve(VsrOperation.Register);
        Assert.Equal(initialGeneration + 3, session.Generation);
    }
}
