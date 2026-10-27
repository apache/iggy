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

using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Apache.Iggy.Configuration;
using Apache.Iggy.IggyClient.Implementations;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Iggy.Tests.VsrTests;

/// <summary>
///     Without a TLS name, each dial checks the certificate against the host that it dials, as
///     <c>tls_server_name</c> in <c>core/sdk/src/links/dial.rs</c> does, so the name of one listed node cannot
///     fail the handshake with another.
/// </summary>
public sealed class TlsServerNameTests
{
    [Fact]
    public async Task given_no_tls_domain_when_a_later_listed_node_answers_should_check_the_host_that_it_dials()
    {
        var token = TestContext.Current.CancellationToken;
        using var authority = Authority();
        using var certificate = Issue(authority, IPAddress.Loopback);
        var caFile = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(caFile, authority.ExportCertificatePem(), token);
            using var down = new MockNode();
            down.Kill();
            using var node = new TlsNode(certificate);
            var configuration = IggyClientConfigurator.FromConnectionString(
                $"iggy://iggy:iggy@localhost:{down.Port},127.0.0.1:{node.Port}" +
                $"?tls=true&tls_ca_file={caFile}&reconnection_retries=0");
            // Without a sign-in the connect ends at the handshake, which is all this node can do.
            configuration.AutoLoginSettings = new AutoLoginSettings();
            using var client = new TcpMessageStream(configuration, NullLoggerFactory.Instance);

            // The certificate names only 127.0.0.1, so the name of the first listed node would fail this handshake.
            await client.ConnectAsync(token);

            Assert.Equal($"127.0.0.1:{node.Port}", client.GetCurrentAddress());
        }
        finally
        {
            File.Delete(caFile);
        }
    }

    private static X509Certificate2 Authority()
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=Iggy Test CA", key, HashAlgorithmName.SHA256);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign, true));
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-10), DateTimeOffset.UtcNow.AddHours(2));
    }

    private static X509Certificate2 Issue(X509Certificate2 authority, IPAddress address)
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=Iggy Test Node", key, HashAlgorithmName.SHA256);
        var names = new SubjectAlternativeNameBuilder();
        names.AddIpAddress(address);
        request.CertificateExtensions.Add(names.Build());
        using var issued = request.Create(authority, DateTimeOffset.UtcNow.AddMinutes(-5),
            DateTimeOffset.UtcNow.AddHours(1), [1, 2, 3, 4]);
        using var withKey = issued.CopyWithPrivateKey(key);
        // An ephemeral key cannot serve TLS on every platform, so the pair is reloaded from PKCS #12.
        var pkcs12 = withKey.Export(X509ContentType.Pfx);
#if NET9_0_OR_GREATER
        return X509CertificateLoader.LoadPkcs12(pkcs12, null);
#else
        return new X509Certificate2(pkcs12);
#endif
    }

    /// <summary>A loopback node that completes the TLS handshake and then holds the connection.</summary>
    private sealed class TlsNode : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly X509Certificate2 _certificate;

        internal TlsNode(X509Certificate2 certificate)
        {
            _certificate = certificate;
            _listener.Start();
            Port = (ushort)((IPEndPoint)_listener.LocalEndpoint).Port;
            _ = Task.Run(AcceptAsync);
        }

        internal ushort Port { get; }

        public void Dispose()
        {
            _listener.Stop();
        }

        private async Task AcceptAsync()
        {
            try
            {
                while (true)
                {
                    var connection = await _listener.AcceptTcpClientAsync();
                    _ = Task.Run(() => HoldAsync(connection));
                }
            }
            catch (Exception)
            {
                // The listener stopped.
            }
        }

        private async Task HoldAsync(TcpClient connection)
        {
            using (connection)
            {
                try
                {
                    await using var stream = new SslStream(connection.GetStream());
                    await stream.AuthenticateAsServerAsync(_certificate);
                    var buffer = new byte[256];
                    while (await stream.ReadAsync(buffer) > 0)
                    {
                    }
                }
                catch (Exception)
                {
                    // A refused handshake and a client that went away look the same here.
                }
            }
        }
    }
}
