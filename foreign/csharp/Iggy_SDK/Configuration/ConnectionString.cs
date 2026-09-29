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

using System.Globalization;
using System.Text.RegularExpressions;
using Apache.Iggy.Enums;
using Apache.Iggy.Utils;

namespace Apache.Iggy.Configuration;

// Parsed by hand because Uri escaping and host normalization accept strings this format rejects.
internal static class ConnectionString
{
    private const string DefaultScheme = "iggy";
    private const string TcpScheme = "iggy+tcp";
    private const string SchemePrefix = "iggy+";

    // Unbracketed hosts may not contain ':', so 2001:db8::1:8090 and host:1:2 are rejected.
    private static readonly Regex AuthorityPattern = new(@"^(?:\[([^\]\s\p{Cc}]+)\]|([^:\[\]\s\p{Cc}]+)):([0-9]+)\z",
        RegexOptions.CultureInvariant);

    internal static IggyClientConfigurator Parse(string connectionString)
    {
        ArgumentNullException.ThrowIfNull(connectionString);

        var protocolParts = connectionString.Split("://");
        if (protocolParts.Length != 2)
        {
            throw Invalid();
        }

        ValidateScheme(protocolParts[0]);

        var parts = protocolParts[1].Split('@');
        if (parts.Length != 2)
        {
            throw Invalid();
        }

        var autoLoginSettings = ParseCredentials(parts[0]);

        var serverAndOptions = parts[1].Split('?');
        if (serverAndOptions.Length > 2)
        {
            throw Invalid();
        }

        var serverAddress = serverAndOptions[0];
        var serverHost = ParseHost(serverAddress);
        var (tlsSettings, reconnectionSettings, heartbeatInterval) =
            ParseOptions(serverAndOptions.ElementAtOrDefault(1), serverHost);

        return new IggyClientConfigurator
        {
            BaseAddress = serverAddress,
            Protocol = Protocol.Tcp,
            HeartbeatInterval = heartbeatInterval,
            TlsSettings = tlsSettings,
            ReconnectionSettings = reconnectionSettings,
            AutoLoginSettings = autoLoginSettings
        };
    }

    private static void ValidateScheme(string scheme)
    {
        if (scheme == DefaultScheme || scheme == TcpScheme)
        {
            return;
        }

        if (scheme.Equals(DefaultScheme, StringComparison.OrdinalIgnoreCase)
            || scheme.Equals(TcpScheme, StringComparison.OrdinalIgnoreCase))
        {
            throw new FormatException(
                $"Connection string schemes are case-sensitive, use \"{DefaultScheme}\" or \"{TcpScheme}\".");
        }

        // Only a plain transport name is echoed, a malformed scheme may carry credentials.
        if (scheme.StartsWith(SchemePrefix, StringComparison.Ordinal)
            && scheme.Length > SchemePrefix.Length
            && scheme[SchemePrefix.Length..].All(char.IsAsciiLetter))
        {
            throw new FormatException(
                $"Unsupported transport \"{scheme[SchemePrefix.Length..]}\", connection strings support tcp only.");
        }

        throw Invalid();
    }

    private static AutoLoginSettings ParseCredentials(string userInfo)
    {
        var credentials = userInfo.Split(':');
        if (credentials.Length > 2 || credentials.Any(string.IsNullOrEmpty))
        {
            throw Invalid();
        }

        return credentials.Length == 1
            ? AutoLoginSettings.ForPersonalAccessToken(credentials[0])
            : AutoLoginSettings.For(credentials[0], credentials[1]);
    }

    private static string ParseHost(string serverAddress)
    {
        var addressMatch = AuthorityPattern.Match(serverAddress);
        if (!addressMatch.Success || !ushort.TryParse(addressMatch.Groups[3].Value, NumberStyles.None,
                CultureInfo.InvariantCulture, out _))
        {
            throw Invalid();
        }

        return addressMatch.Groups[1].Success
            ? addressMatch.Groups[1].Value
            : addressMatch.Groups[2].Value;
    }

    private static (TlsSettings, ReconnectionSettings, TimeSpan) ParseOptions(string? query, string serverHost)
    {
        // The certificate name check needs a hostname when tls_domain is missing.
        var tlsSettings = new TlsSettings { Hostname = serverHost };
        var reconnectionSettings = new ReconnectionSettings
        {
            Enabled = true,
            MaxRetries = 0,
            InitialDelay = TimeSpan.FromSeconds(1),
            UseExponentialBackoff = false
        };
        var heartbeatInterval = TimeSpan.FromSeconds(5);

        foreach (var option in query?.Split('&') ?? [])
        {
            var (name, value) = SplitOption(option);
            switch (name)
            {
                case "tls":
                    tlsSettings.Enabled = ParseBoolean(name, value);
                    break;
                case "tls_domain":
                    tlsSettings.Hostname = value.Length == 0 ? serverHost : value;
                    break;
                case "tls_ca_file":
                    tlsSettings.CertificatePath = value;
                    break;
                case "reconnection_retries":
                    ApplyReconnectionRetries(reconnectionSettings, name, value);
                    break;
                case "reconnection_interval":
                    reconnectionSettings.InitialDelay = ParseInterval(name, value);
                    break;
                case "reestablish_after":
                    // Validated only, reconnection_interval paces every redial.
                    ParseDuration(name, value, IggyDurationParser.ParseParts);
                    break;
                case "heartbeat_interval":
                    heartbeatInterval = ParseInterval(name, value);
                    break;
                case "nodelay":
                    // Validated only, the TCP client always disables Nagle.
                    ParseBoolean(name, value);
                    break;
                default:
                    throw new FormatException($"Unknown option \"{name}\".");
            }
        }

        if (tlsSettings.Enabled && string.IsNullOrEmpty(tlsSettings.CertificatePath))
        {
            throw new FormatException("Option \"tls\" requires \"tls_ca_file\".");
        }

        return (tlsSettings, reconnectionSettings, heartbeatInterval);
    }

    private static (string Name, string Value) SplitOption(string option)
    {
        var optionParts = option.Split('=');
        if (optionParts.Length != 2)
        {
            throw Invalid();
        }

        return (optionParts[0], optionParts[1]);
    }

    private static void ApplyReconnectionRetries(ReconnectionSettings settings, string name, string value)
    {
        var unlimited = value == "unlimited";
        uint retries = 0;
        if (!unlimited && !uint.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out retries))
        {
            throw new FormatException($"Option \"{name}\" must be \"unlimited\" or an integer up to {uint.MaxValue}.");
        }

        // MaxRetries treats 0 as unlimited, so zero retries means reconnection off.
        settings.Enabled = unlimited || retries != 0;
        settings.MaxRetries = retries <= int.MaxValue ? (int)retries : 0;
    }

    private static T ParseDuration<T>(string name, string value, Func<string, T> parse)
    {
        try
        {
            return parse(value);
        }
        catch (FormatException e)
        {
            throw new FormatException($"Option \"{name}\" has an invalid duration: {e.Message}.", e);
        }
    }

    private static TimeSpan ParseInterval(string name, string value)
    {
        var interval = ParseDuration(name, value, IggyDurationParser.Parse);
        if (interval < IggyClientConfigurator.MinInterval || interval > IggyClientConfigurator.MaxInterval)
        {
            throw new FormatException($"Option \"{name}\" must be between 1 millisecond and about 49 days.");
        }

        return interval;
    }

    private static bool ParseBoolean(string name, string value)
    {
        return value switch
        {
            "true" => true,
            "false" => false,
            _ => throw new FormatException($"Option \"{name}\" must be true or false.")
        };
    }

    private static FormatException Invalid()
    {
        return new FormatException("Invalid connection string.");
    }
}
