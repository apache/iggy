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
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Factory;

public static class MessageStreamFactory
{
    //TODO - this whole setup will have to be refactored later,when adding support for ASP.NET Core DI
    public static IIggyClient CreateMessageStream(Action<IMessageStreamConfigurator> options,
        ILoggerFactory loggerFactory)
    {
        var config = new MessageStreamConfigurator();
        options.Invoke(config);

        return config.Protocol switch
        {
            Protocol.Http => CreateHttpMessageStream(config, loggerFactory),
            Protocol.Tcp => CreateTcpMessageStream(config, loggerFactory),
            _ => throw new InvalidEnumArgumentException()
        };
    }

    private static TcpMessageStream CreateTcpMessageStream(IMessageStreamConfigurator options,
        ILoggerFactory loggerFactory)
    {
        var socket = CreateTcpStream(options);
        return new TcpMessageStreamBuilder(socket, options, loggerFactory)
            .WithSendMessagesDispatcher() //this internally resolves whether the message dispatcher is created or not.
            .Build();
    }

    private static IConnectionStream CreateTcpStream(IMessageStreamConfigurator options)
    {
        var urlPortSplitter = options.BaseAdress.Split(":");
        if (urlPortSplitter.Length > 2)
        {
            throw new InvalidBaseAdressException();
        }

        var tlsOptions = new TlsSettings();
        options.TlsSettings.Invoke(tlsOptions);
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        socket.Connect(urlPortSplitter[0], int.Parse(urlPortSplitter[1]));
        socket.SendBufferSize = options.SendBufferSize;
        socket.ReceiveBufferSize = options.ReceiveBufferSize;
        return tlsOptions.Enabled switch
        {
            true => CreateSslStreamAndAuthenticate(socket, tlsOptions),
            false => new TcpConnectionStream(new NetworkStream(socket))
        };
    }

    private static TcpTlsConnectionStream CreateSslStreamAndAuthenticate(Socket socket, TlsSettings tlsSettings)
    {
        var stream = new NetworkStream(socket);
        var sslStream = new SslStream(stream);
        if (tlsSettings.Authenticate)
        {
            sslStream.AuthenticateAsClient(tlsSettings.Hostname);
        }

        return new TcpTlsConnectionStream(sslStream);
    }

    private static HttpMessageStream CreateHttpMessageStream(IMessageStreamConfigurator options,
        ILoggerFactory loggerFactory)
    {
        var client = CreateHttpClient(options);
        return new HttpMessageStreamBuilder(client, options, loggerFactory)
            .WithSendMessagesDispatcher() //this internally resolves whether the message dispatcher is created or not
            .Build();
    }

    private static HttpClient CreateHttpClient(IMessageStreamConfigurator options)
    {
        var client = new HttpClient();
        client.BaseAddress = new Uri(options.BaseAdress);
        return client;
    }
}