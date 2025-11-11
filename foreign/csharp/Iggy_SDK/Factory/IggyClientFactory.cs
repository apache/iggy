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

namespace Apache.Iggy.Factory;

/// <summary>
/// A static factory for creating instances of <see cref="IIggyClient"/>.
/// </summary>
/// <remarks>
/// The factory determines the appropriate implementation of the <see cref="IIggyClient"/> based on the specified protocol in the configurator options.
/// </remarks>
public static class IggyClientFactory
{
    /// <summary>
    /// Creates and returns an instance of <see cref="IIggyClient"/> based on the provided configuration options.
    /// </summary>
    /// <param name="options">The configuration options for creating the Iggy client, including protocol, base address, and buffer sizes.</param>
    /// <returns>An instance of <see cref="IIggyClient"/> configured according to the specified options.</returns>
    /// <exception cref="InvalidEnumArgumentException">Thrown when the specified protocol in <paramref name="options"/> is not supported.</exception>
    public static IIggyClient CreateClient(IggyClientConfigurator options)
    {
        return options.Protocol switch
        {
            Protocol.Http => CreateIggyHttpClient(options),
            Protocol.Tcp => CreateIggyTcpClient(options),
            _ => throw new InvalidEnumArgumentException()
        };
    }

    private static IIggyClient CreateIggyTcpClient(IggyClientConfigurator options)
    {
        return new TcpMessageStream(options, options.LoggerFactory);
    }

    private static IIggyClient CreateIggyHttpClient(IggyClientConfigurator options)
    {
        return new HttpMessageStream(CreateHttpClient(options));
    }

    private static HttpClient CreateHttpClient(IggyClientConfigurator options)
    {
        var client = new HttpClient();
        client.BaseAddress = new Uri(options.BaseAddress);
        return client;
    }
}
