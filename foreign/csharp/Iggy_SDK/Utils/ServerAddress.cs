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
using System.Net.Sockets;

namespace Apache.Iggy.Utils;

/// <summary>
///     Endpoint comparison for leader redirection. The configured address is whatever the caller wrote
///     (<c>localhost:8090</c>), while the cluster roster reports IPs, so the two are compared as parsed
///     endpoints rather than as strings.
/// </summary>
internal static class ServerAddress
{
    internal static bool IsSame(string first, string second)
    {
        return Normalize(first) == Normalize(second);
    }

    /// <summary>
    ///     Renders <c>host:port</c>, bracketing a host that carries colons of its own (a bare IPv6 address).
    ///     Without the brackets the rendering can never round-trip through <see cref="TrySplitHostPort" />, so
    ///     it would compare unequal to every normalized address.
    /// </summary>
    internal static string HostPort(string host, ushort port)
    {
        return host.Contains(':') && !host.StartsWith('[') ? $"[{host}]:{port}" : $"{host}:{port}";
    }

    internal static bool TryParse(string address, out string host, out int port)
    {
        var parsed = TrySplitHostPort(address, out host, out var hostPort);
        port = hostPort;

        return parsed;
    }

    /// <summary>
    ///     The socket family a host has to be dialled on. A name resolves to whatever the resolver returns, and
    ///     the connect call handles that itself, so only a literal decides the family here.
    /// </summary>
    internal static AddressFamily AddressFamilyOf(string host)
    {
        return IPAddress.TryParse(host, out var ip) ? ip.AddressFamily : AddressFamily.InterNetwork;
    }

    /// <summary>
    ///     Canonical <c>host:port</c> rendering: the host is lowercased, the loopback and unspecified aliases
    ///     collapse onto the loopback address, and an IP is re-rendered from its parsed form. The host is
    ///     replaced as a whole, never as a substring, so a node named <c>my-localhost-1</c> keeps its name.
    ///     An address that is not <c>host:port</c> is only lowercased, which leaves it comparable but distinct.
    /// </summary>
    internal static string Normalize(string address)
    {
        if (!TrySplitHostPort(address, out var host, out var port))
        {
            return address.ToLowerInvariant();
        }

        if (!IPAddress.TryParse(NormalizeHostAlias(host), out var ip))
        {
            return $"{host.ToLowerInvariant()}:{port}";
        }

        return ip.AddressFamily == AddressFamily.InterNetworkV6 ? $"[{ip}]:{port}" : $"{ip}:{port}";
    }

    /// <summary>
    ///     A server bound to the unspecified address is reachable on the loopback one, and the roster may
    ///     report either, so both render the same way.
    /// </summary>
    private static string NormalizeHostAlias(string host)
    {
        if (host.Equals("localhost", StringComparison.OrdinalIgnoreCase))
        {
            return "127.0.0.1";
        }

        if (!IPAddress.TryParse(host, out var ip))
        {
            return host;
        }

        if (ip.Equals(IPAddress.Any))
        {
            return IPAddress.Loopback.ToString();
        }

        return ip.Equals(IPAddress.IPv6Any) ? IPAddress.IPv6Loopback.ToString() : host;
    }

    private static bool TrySplitHostPort(string address, out string host, out ushort port)
    {
        host = string.Empty;
        port = 0;
        string portText;

        // A bracketed host is the only form that may carry colons of its own.
        if (address.StartsWith('['))
        {
            var closing = address.IndexOf(']');
            if (closing < 0 || closing + 1 >= address.Length || address[closing + 1] != ':')
            {
                return false;
            }

            host = address[1..closing];
            portText = address[(closing + 2)..];
        }
        else
        {
            var separator = address.IndexOf(':');
            if (separator <= 0 || address.IndexOf(':', separator + 1) >= 0)
            {
                return false;
            }

            host = address[..separator];
            portText = address[(separator + 1)..];
        }

        return host.Length > 0 && ushort.TryParse(portText, out port);
    }
}
