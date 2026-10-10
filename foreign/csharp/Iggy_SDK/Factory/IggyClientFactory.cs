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

using System.ComponentModel;
using Apache.Iggy.Configuration;
using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Factory;

/// <summary>
///     A static factory for creating instances of <see cref="IIggyClient" />.
/// </summary>
/// <remarks>
///     The factory determines the appropriate implementation of the <see cref="IIggyClient" /> based on the specified
///     protocol in the configurator options.
/// </remarks>
public static class IggyClientFactory
{
    /// <summary>
    ///     Creates and returns an instance of <see cref="IIggyClient" /> based on the provided configuration options.
    /// </summary>
    /// <param name="options">
    ///     The configuration options for creating the Iggy client, including protocol, base address, and
    ///     buffer sizes.
    /// </param>
    /// <returns>An instance of <see cref="IIggyClient" /> configured according to the specified options.</returns>
    /// <exception cref="InvalidEnumArgumentException">
    ///     Thrown when the specified protocol in <paramref name="options" /> is not
    ///     supported.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    ///     Thrown when <see cref="IggyClientConfigurator.MaxResponseFrameSize" /> is below the 256-byte header or a
    ///     configured socket buffer size is not positive.
    /// </exception>
    public static IIggyClient CreateClient(IggyClientConfigurator options)
    {
        Validate(options);

        return options.Protocol switch
        {
            Protocol.Http => CreateIggyHttpClient(options),
            Protocol.Tcp => CreateIggyTcpClient(options),
            _ => throw new InvalidEnumArgumentException()
        };
    }

    /// <summary>
    ///     Creates a TCP client from a connection string such as <c>iggy://iggy:iggy@127.0.0.1:8090</c>. See
    ///     <see cref="IggyClientConfigurator.FromConnectionString" /> for the format and the supported options.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>An unconnected <see cref="IIggyClient" /> that signs in with the connection string credentials.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="connectionString" /> is null.</exception>
    /// <exception cref="FormatException">Thrown when the connection string is invalid.</exception>
    public static IIggyClient CreateClient(string connectionString)
    {
        return CreateClient(IggyClientConfigurator.FromConnectionString(connectionString));
    }

    private static void Validate(IggyClientConfigurator options)
    {
        if (options.Protocol == Protocol.Tcp && options.MaxResponseFrameSize < VsrHeader.HEADER_SIZE)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.MaxResponseFrameSize,
                $"MaxResponseFrameSize must be at least {VsrHeader.HEADER_SIZE} bytes.");
        }

        if (options.ReceiveBufferSize is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(IggyClientConfigurator.ReceiveBufferSize),
                options.ReceiveBufferSize, "ReceiveBufferSize must be greater than 0 when set.");
        }

        if (options.SendBufferSize is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(IggyClientConfigurator.SendBufferSize),
                options.SendBufferSize, "SendBufferSize must be greater than 0 when set.");
        }

        // Outside these bounds PeriodicTimer would fault the heartbeat task at start instead of failing the
        // caller here.
        if (options.HeartbeatInterval < IggyClientConfigurator.MinInterval ||
            options.HeartbeatInterval > IggyClientConfigurator.MaxInterval)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.HeartbeatInterval,
                "HeartbeatInterval must be between 1 millisecond and about 49 days.");
        }
    }

    private static IIggyClient CreateIggyTcpClient(IggyClientConfigurator options)
    {
        return new TcpMessageStream(options, options.LoggerFactory);
    }

    private static IIggyClient CreateIggyHttpClient(IggyClientConfigurator options)
    {
        return new HttpMessageStream(CreateHttpClient(options), options.MessageEncryptor,
            options.AllowAutoCommitWithEncryptor);
    }

    private static HttpClient CreateHttpClient(IggyClientConfigurator options)
    {
        var client = new HttpClient(new TransientHttpRetryHandler(new HttpClientHandler()));
        client.BaseAddress = new Uri(options.BaseAddress);
        return client;
    }
}
