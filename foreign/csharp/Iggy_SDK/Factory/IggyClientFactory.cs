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
using System.Net.Security;
using System.Net.Sockets;
using Apache.Iggy.Configuration;
using Apache.Iggy.ConnectionStream;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.IggyClient.Implementations;

namespace Apache.Iggy.Factory;

public static class IggyClientFactory
{
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
