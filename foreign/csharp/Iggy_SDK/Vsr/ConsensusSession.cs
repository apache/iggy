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
    private readonly byte[] _bindSecret = new byte[LoginRegister.BIND_SECRET_BYTES];
    private ulong _generation;
    private ulong _armedAt;
    private ulong _continuedSince;
    private ulong _unauthenticatedSince;
    private Exception? _endCause;
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

    internal byte[] BindSecret
    {
        get
        {
            lock (_gate)
            {
                return (byte[])_bindSecret.Clone();
            }
        }
    }

    internal void SetBindSecret(ReadOnlySpan<byte> bindSecret)
    {
        if (bindSecret.Length != LoginRegister.BIND_SECRET_BYTES)
        {
            throw new ArgumentException("Bind secret must be 32 bytes.", nameof(bindSecret));
        }
        lock (_gate)
        {
            bindSecret.CopyTo(_bindSecret);
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

    internal bool RegisterPending
    {
        get
        {
            lock (_gate)
            {
                return _registerPending;
            }
        }
    }

    /// <summary>
    ///     Generation of the identity, bumped on every re-arm and on every bind the caller signed in for. Anything
    ///     scoped to a server session - a consumer-group membership, say - compares the generation it was
    ///     established under to detect that the session it lived on is gone, without inferring it from
    ///     connection-state edges. Distinct from the fence epoch the wire protocol carries in <see cref="Session" />.
    /// </summary>
    internal ulong Generation
    {
        get
        {
            lock (_gate)
            {
                return _generation;
            }
        }
    }

    /// <summary>
    ///     The generation a request made under <paramref name="generation" /> goes on under: its own while it is
    ///     current, or the bound session that replaced it when the client, not the caller, signed in every session
    ///     since. Null otherwise, so a request never runs for a sign-in it was not made under.
    /// </summary>
    internal ulong? Successor(ulong generation)
    {
        lock (_gate)
        {
            if (generation == _generation)
            {
                return generation;
            }
            return _session.HasValue && generation >= _continuedSince ? _generation : null;
        }
    }

    /// <summary>
    ///     The error a request made under <paramref name="generation" /> ends with once no session can take it. It is
    ///     <see cref="VsrError.UNAUTHENTICATED" /> when the caller signed in or out since or the server refused a
    ///     sign-in, with what ended the session as its cause, and <see cref="NotConnectedException" /> when the
    ///     client stopped without signing in again. Null while the session the request was made under, or one the
    ///     client signs in for it, can still take it.
    /// </summary>
    internal Exception? Ended(ulong generation)
    {
        lock (_gate)
        {
            if (generation >= _continuedSince)
            {
                return null;
            }
            return generation < _unauthenticatedSince
                ? VsrError.Exception(VsrError.UNAUTHENTICATED, "The session the request was made under ended.", _endCause)
                : new NotConnectedException();
        }
    }

    /// <summary>
    ///     The generation a connection dialed under <paramref name="dialedUnder" /> serves: the current one until a
    ///     re-arm replaces the identity. A caller's bind opens a generation on the same identity, so the connection
    ///     that carried its register stays current. Null once the connection belongs to a replaced identity.
    /// </summary>
    internal ulong? LinkGeneration(ulong dialedUnder)
    {
        lock (_gate)
        {
            return dialedUnder >= _armedAt ? _generation : null;
        }
    }

    internal ulong BeginCallerSession()
    {
        lock (_gate)
        {
            _generation++;
            EndLineageLocked(null);
            return _generation;
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

    internal ConsensusSession(UInt128 clientId, ulong session, ReadOnlySpan<byte> bindSecret) : this(clientId)
    {
        if (session == 0)
        {
            throw VsrError.Exception(VsrError.INVALID_FORMAT, "Shared session requires a nonzero epoch.");
        }
        _session = session;
        SetBindSecret(bindSecret);
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
                _ => RequestFrameLocked(operation)
            };
        }
    }

    /// <summary>
    ///     Bind the session from a committed register reply. A bind the caller signed in for opens a generation, so
    ///     a request made before it, even while no session was bound, never goes out under the caller's new session.
    /// </summary>
    /// <param name="session">The session number the register reply carried.</param>
    /// <param name="bindSecret">The private proof sent in the corresponding register.</param>
    /// <param name="continues">The client signed in again for the requests of the replaced sessions.</param>
    /// <exception cref="NotConnectedException">
    ///     No register is awaiting a binding, so the identity was re-armed while this one was in flight. Binding
    ///     regardless would pair the session with a client id the server never registered, and it would fence
    ///     every later request.
    /// </exception>
    /// <exception cref="IggyInvalidStatusCodeException">
    ///     The register reply carried no session. The value comes off the wire, so a malformed reply has to
    ///     surface as a protocol error rather than as argument validation.
    /// </exception>
    internal void Bind(ulong session, ReadOnlySpan<byte> bindSecret, bool continues = false)
    {
        if (session == 0)
        {
            throw VsrError.Exception(VsrError.INVALID_FORMAT, "Register reply carried no session.");
        }
        if (bindSecret.Length != LoginRegister.BIND_SECRET_BYTES)
        {
            throw new ArgumentException("Bind secret must be 32 bytes.", nameof(bindSecret));
        }

        lock (_gate)
        {
            if (!_registerPending)
            {
                throw new NotConnectedException();
            }

            _registerPending = false;
            bindSecret.CopyTo(_bindSecret);
            _session = session;
            if (!continues)
            {
                _generation++;
                EndLineageLocked(null);
            }
        }
    }

    /// <summary>Forget the binding and client id after logout or a terminal bind refusal.</summary>
    /// <param name="continues">The client signs in again for the requests of the replaced session.</param>
    /// <param name="cause">What ended the session, kept as the cause of the requests it ends.</param>
    internal void Reset(bool continues = false, Exception? cause = null)
    {
        lock (_gate)
        {
            ReArmLocked();
            if (!continues)
            {
                EndLineageLocked(cause);
            }
        }
    }

    /// <summary>
    ///     Forget the binding and client id when the client stops without signing in again. The requests of the
    ///     replaced session end as not connected, unless the caller signs in or out later.
    /// </summary>
    internal void Stop()
    {
        lock (_gate)
        {
            ReArmLocked();
            _continuedSince = _generation;
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

    private SessionFrame RequestFrameLocked(VsrOperation operation)
    {
        var sessionId = _session ?? (operation == VsrOperation.NonReplicated ? 0UL
            : throw VsrError.Exception(VsrError.UNAUTHENTICATED, "A replicated request requires a bound consensus session."));

        if (_requestCounter == ulong.MaxValue)
        {
            throw VsrError.Exception(VsrError.REQUEST_ID_EXHAUSTED, "Request ID counter exhausted.");
        }

        var requestId = _requestCounter;
        _requestCounter = checked(_requestCounter + 1);

        return new SessionFrame(_clientId, requestId, sessionId);
    }

    private void ReArmLocked()
    {
        _clientId = GenerateClientId();
        Array.Clear(_bindSecret);
        _session = null;
        _registerPending = false;
        _generation++;
        _armedAt = _generation;
    }

    private void EndLineageLocked(Exception? cause)
    {
        _continuedSince = _generation;
        _unauthenticatedSince = _generation;
        _endCause = cause;
    }

    private static UInt128 GenerateClientId()
    {
        Span<byte> bytes = stackalloc byte[16];
        RandomNumberGenerator.Fill(bytes);
        var lower = BinaryPrimitives.ReadUInt64LittleEndian(bytes[..8]);
        // Keep generated IDs outside the server-only range.
        var upper = BinaryPrimitives.ReadUInt64LittleEndian(bytes[8..]) & (ulong)long.MaxValue;
        var clientId = new UInt128(upper, lower);

        return clientId == UInt128.Zero ? UInt128.One : clientId;
    }
}

/// <summary>The session identity one request header is encoded from, resolved as a single atomic snapshot.</summary>
internal readonly record struct SessionFrame(UInt128 ClientId, ulong RequestId, ulong SessionId);
