// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License")

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
        return new TcpMessageStream(CreateTcpStream(options), options.LoggerFactory);
    }

    private static IIggyClient CreateIggyHttpClient(IggyClientConfigurator options)
    {
        return new HttpMessageStream(CreateHttpClient(options));
    }

    private static IConnectionStream CreateTcpStream(IggyClientConfigurator options)
    {
        var urlPortSplitter = options.BaseAddress.Split(":");
        if (urlPortSplitter.Length > 2)
        {
            throw new InvalidBaseAdressException();
        }

        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        socket.Connect(urlPortSplitter[0], int.Parse(urlPortSplitter[1]));
        socket.SendBufferSize = options.SendBufferSize;
        socket.ReceiveBufferSize = options.ReceiveBufferSize;
        return options.TlsSettings.Enabled switch
        {
            true => CreateSslStreamAndAuthenticate(socket, options.TlsSettings),
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

    private static HttpClient CreateHttpClient(IggyClientConfigurator options)
    {
        var client = new HttpClient();
        client.BaseAddress = new Uri(options.BaseAddress);
        return client;
    }
}
