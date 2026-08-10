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

using System.Buffers.Binary;
using System.Security.Cryptography;
using Apache.Iggy.Exceptions;

namespace Apache.Iggy.Vsr;

/// <summary>
///     Consensus-level session state: the ephemeral client id, the session number the server hands back
///     when a register commits, and the monotonic request counter.
/// </summary>
/// <remarks>
///     Every mutation and every read of the identity runs under one lock. The transport serialises the
///     re-arms against the requests with its sending lock, so a request always encodes from the identity
///     that is live for the whole time it is on the wire.
/// </remarks>
internal sealed class ConsensusSession
{
#if NET10_0_OR_GREATER
    private readonly Lock _gate = new();
#else
    private readonly object _gate = new();
#endif
    private UInt128 _clientId;
    private bool _registerPending;
    private ulong _requestCounter;
    private ulong? _session;

    /// <summary>Ephemeral client identifier, never persisted, non-zero.</summary>
    internal UInt128 ClientId
    {
        get
        {
            lock (_gate)
            {
                return _clientId;
            }
        }
    }

    /// <summary>Session fence epoch assigned by the server, <c>null</c> until a register commits.</summary>
    internal ulong? Session
    {
        get
        {
            lock (_gate)
            {
                return _session;
            }
        }
    }

    /// <summary>The id the next request id resolution will return.</summary>
    internal ulong RequestCounter
    {
        get
        {
            lock (_gate)
            {
                return _requestCounter;
            }
        }
    }

    internal bool IsBound
    {
        get
        {
            lock (_gate)
            {
                return _session.HasValue;
            }
        }
    }

    internal ConsensusSession() : this(GenerateClientId())
    {
    }

    internal ConsensusSession(UInt128 clientId)
    {
        _clientId = clientId;
        _requestCounter = 1;
    }

    /// <summary>
    ///     Resolves the identity one request header is encoded from, in a single atomic step so no reset can
    ///     interleave between the client id, the request id and the session.
    /// </summary>
    internal SessionFrame Resolve(VsrOperation operation)
    {
        lock (_gate)
        {
            return operation switch
            {
                VsrOperation.Register => RegisterFrameLocked(),
                VsrOperation.NonReplicated => new SessionFrame(_clientId, _requestCounter, _session ?? 0),
                _ => ReplicatedFrameLocked(operation)
            };
        }
    }

    /// <summary>Bind the session from a committed register reply.</summary>
    /// <param name="session">The session number the register reply carried.</param>
    /// <exception cref="NotConnectedException">
    ///     No register is awaiting a binding, so the identity was re-armed while this one was in flight. Binding
    ///     regardless would pair the session with a client id the server never registered, and it would fence
    ///     every later request.
    /// </exception>
    /// <exception cref="IggyInvalidStatusCodeException">
    ///     The register reply carried no session. The value comes off the wire, so a malformed reply has to
    ///     surface as a protocol error rather than as argument validation.
    /// </exception>
    internal void Bind(ulong session)
    {
        if (session == 0)
        {
            throw VsrError.Exception(VsrError.INVALID_FORMAT, "Register reply carried no session.");
        }

        lock (_gate)
        {
            if (!_registerPending)
            {
                throw new NotConnectedException();
            }

            _registerPending = false;
            _session = session;
        }
    }

    /// <summary>Forget the binding and the client id, e.g. after an eviction or a torn connection.</summary>
    internal void Reset()
    {
        lock (_gate)
        {
            ReArmLocked();
        }
    }

    /// <summary>
    ///     Begin a registration, re-arming the session if it was already used. A re-login mints a fresh
    ///     client id and clears the binding, so the register encodes cleanly and the server never has to
    ///     disambiguate a repeat register for the same client. Always returns 0.
    /// </summary>
    private SessionFrame RegisterFrameLocked()
    {
        // A second register while one is still unbound would re-arm the identity out from under the first, and
        // the winner's Bind would then attach its session to the re-armed client id - the server answers every
        // later request with NoSession and evicts. Refuse instead: the caller retries against a clean session.
        if (_registerPending)
        {
            throw VsrError.Exception(VsrError.UNAUTHENTICATED, "A consensus register is already in flight.");
        }

        if (_session.HasValue)
        {
            ReArmLocked();
        }

        _registerPending = true;

        return new SessionFrame(_clientId, 0, 0);
    }

    private SessionFrame ReplicatedFrameLocked(VsrOperation operation)
    {
        var sessionId = _session ?? throw VsrError.Exception(VsrError.UNAUTHENTICATED,
            "A replicated request requires a bound consensus session.");

        // Partition ops replicate in their own per-partition group with no client-table dedup, so they too
        // must leave the metadata counter untouched. Only metadata operations and logout consume an id: the
        // server tracks request ids for those alone, and it accepts any id above the client's watermark.
        if (operation.IsPartition())
        {
            return new SessionFrame(_clientId, _requestCounter, sessionId);
        }

        var requestId = _requestCounter;
        _requestCounter = checked(_requestCounter + 1);

        return new SessionFrame(_clientId, requestId, sessionId);
    }

    private void ReArmLocked()
    {
        _clientId = GenerateClientId();
        _session = null;
        _requestCounter = 1;
        _registerPending = false;
    }

    private static UInt128 GenerateClientId()
    {
        Span<byte> bytes = stackalloc byte[16];
        RandomNumberGenerator.Fill(bytes);
        var lower = BinaryPrimitives.ReadUInt64LittleEndian(bytes[..8]);
        var upper = BinaryPrimitives.ReadUInt64LittleEndian(bytes[8..]);
        var clientId = new UInt128(upper, lower);

        return clientId == UInt128.Zero ? UInt128.One : clientId;
    }
}

/// <summary>The session identity one request header is encoded from, resolved as a single atomic snapshot.</summary>
internal readonly record struct SessionFrame(UInt128 ClientId, ulong RequestId, ulong SessionId);
