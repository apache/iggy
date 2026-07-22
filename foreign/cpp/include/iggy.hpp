/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

/**
 * @file iggy.hpp
 * @brief Public C++ API for the Apache Iggy client.
 */

#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "lib.rs.h"

namespace iggy {

using LoginInfo = ffi::LoginInfo;

namespace detail {

/** @brief Internal base for string-backed option types. */
template <typename Tag>
class StringTag {
  protected:
    explicit StringTag(std::string value) : value_(std::move(value)) {}
    ~StringTag() = default;

    std::string_view Value() const { return value_; }

  private:
    std::string value_;
};

}  // namespace detail

/**
 * @brief Compression algorithm used for topic messages.
 *
 * Selects whether messages in a topic are stored as-is or compressed with
 * gzip.
 *
 * @note The value is passed across the Rust FFI as a string. The Rust client
 *       rejects unsupported values.
 */
class CompressionAlgorithm final : private detail::StringTag<CompressionAlgorithm> {
  public:
    /** @brief Returns the uncompressed storage option. */
    static CompressionAlgorithm None() { return CompressionAlgorithm("none"); }

    /** @brief Returns the gzip compression option. */
    static CompressionAlgorithm Gzip() { return CompressionAlgorithm("gzip"); }

    /**
     * @brief Returns the value passed to the client implementation.
     * @return Compression algorithm name.
     */
    std::string_view CompressionAlgorithmValue() const { return Value(); }

  private:
    explicit CompressionAlgorithm(std::string algorithm)
        : detail::StringTag<CompressionAlgorithm>(std::move(algorithm)) {}
};

/**
 * @brief Compression algorithm used for system snapshot archives.
 *
 * Selects how snapshot data is compressed in the generated archive.
 *
 * @note The value is passed across the Rust FFI as a string. The Rust client
 *       rejects unsupported values.
 */
class SnapshotCompression final : private detail::StringTag<SnapshotCompression> {
  public:
    /** @brief Returns the uncompressed storage option. */
    static SnapshotCompression Stored() { return SnapshotCompression("stored"); }

    /** @brief Returns the Deflate compression option. */
    static SnapshotCompression Deflated() { return SnapshotCompression("deflated"); }

    /** @brief Uses bzip2 for better compression with slower processing. */
    static SnapshotCompression Bzip2() { return SnapshotCompression("bzip2"); }

    /** @brief Uses Zstandard for fast compression and decompression. */
    static SnapshotCompression Zstd() { return SnapshotCompression("zstd"); }

    /** @brief Uses LZMA for high compression, especially for larger files. */
    static SnapshotCompression Lzma() { return SnapshotCompression("lzma"); }

    /** @brief Uses XZ for LZMA-like compression with faster decompression. */
    static SnapshotCompression Xz() { return SnapshotCompression("xz"); }

    /**
     * @brief Returns the value passed to the client implementation.
     * @return Snapshot compression algorithm name.
     */
    std::string_view SnapshotCompressionValue() const { return Value(); }

  private:
    explicit SnapshotCompression(std::string snapshot_compression)
        : detail::StringTag<SnapshotCompression>(std::move(snapshot_compression)) {}
};

/**
 * @brief Selects data to include in a system snapshot.
 *
 * @note Each selected value is passed across the Rust FFI as a string. The
 *       Rust client rejects unsupported values.
 */
class SystemSnapshotType final : private detail::StringTag<SystemSnapshotType> {
  public:
    /** @brief Includes an overview of the file-system structure. */
    static SystemSnapshotType FilesystemOverview() { return SystemSnapshotType("filesystem_overview"); }

    /** @brief Includes currently running processes. */
    static SystemSnapshotType ProcessList() { return SystemSnapshotType("process_list"); }

    /** @brief Includes CPU, memory, and other resource usage statistics. */
    static SystemSnapshotType ResourceUsage() { return SystemSnapshotType("resource_usage"); }

    /** @brief Includes the test snapshot used for development and testing. */
    static SystemSnapshotType Test() { return SystemSnapshotType("test"); }

    /** @brief Includes server logs from the configured logging directory. */
    static SystemSnapshotType ServerLogs() { return SystemSnapshotType("server_logs"); }

    /** @brief Includes server configuration. */
    static SystemSnapshotType ServerConfig() { return SystemSnapshotType("server_config"); }

    /** @brief Includes all available snapshot data. */
    static SystemSnapshotType All() { return SystemSnapshotType("all"); }

    /**
     * @brief Returns the value passed to the client implementation.
     * @return System snapshot type name.
     */
    std::string_view SnapshotTypeValue() const { return Value(); }

  private:
    explicit SystemSnapshotType(std::string snapshot_type)
        : detail::StringTag<SystemSnapshotType>(std::move(snapshot_type)) {}
};

/**
 * @brief Maximum retained size of a topic.
 *
 * A topic may use the server default, have no size limit, or use an explicit
 * byte limit.
 *
 * @note The value is passed across the Rust FFI as a string. The Rust parser
 *       accepts server_default, unlimited, and decimal byte counts. Zero maps
 *       to server_default, and std::numeric_limits<std::uint64_t>::max() maps
 *       to unlimited. The Rust client rejects unsupported values.
 */
class MaxTopicSize final : private detail::StringTag<MaxTopicSize> {
  public:
    /** @brief Returns the server-default size option. */
    static MaxTopicSize ServerDefault() { return MaxTopicSize("server_default"); }

    /** @brief Returns the unlimited size option. */
    static MaxTopicSize Unlimited() { return MaxTopicSize("unlimited"); }

    /**
     * @brief Creates an explicit topic size limit.
     * @param bytes Maximum topic size in bytes.
     * @return Server-default size for zero, unlimited size for
     *         std::numeric_limits<std::uint64_t>::max(), or the requested limit.
     * @note The configured limit cannot be smaller than the server segment size.
     */
    static MaxTopicSize FromBytes(std::uint64_t bytes) {
        if (bytes == 0) {
            return ServerDefault();
        }
        if (bytes == std::numeric_limits<std::uint64_t>::max()) {
            return Unlimited();
        }
        return MaxTopicSize(std::to_string(bytes));
    }

    /**
     * @brief Returns the value passed to the client implementation.
     * @return Topic size option or decimal byte count.
     */
    std::string_view MaxTopicSizeValue() const { return Value(); }

  private:
    explicit MaxTopicSize(std::string max_topic_size) : detail::StringTag<MaxTopicSize>(std::move(max_topic_size)) {}
};

// TODO(slbotbm): Add rust bindings for Identifier that will use IdKind
/**
 * @brief Identifies whether an identifier is numeric or text-based.
 *
 * @note Reserved for future identifier bindings and not currently passed
 *       across the Rust FFI.
 */
class IdKind final : private detail::StringTag<IdKind> {
  public:
    /** @brief Uses a numeric identifier represented as a 32-bit integer. */
    static IdKind Numeric() { return IdKind("numeric"); }

    /** @brief Uses a string identifier represented by its text value. */
    static IdKind String() { return IdKind("string"); }

    /**
     * @brief Returns the value passed to the client implementation.
     * @return Identifier kind name.
     */
    std::string_view IdKindValue() const { return Value(); }

  private:
    explicit IdKind(std::string id_kind) : detail::StringTag<IdKind>(std::move(id_kind)) {}
};

/**
 * @brief Message retention policy for a topic.
 *
 * @note The expiry kind and value are passed across the Rust FFI as a pair.
 *       The Rust client rejects unsupported kinds.
 */
class Expiry final {
  public:
    /** @brief Returns the server-default expiry policy. */
    static Expiry ServerDefault() { return Expiry("server_default", 0); }

    /**
     * @brief Keeps messages until another operation removes them, such as
     *        topic deletion.
     */
    static Expiry NeverExpire() { return Expiry("never_expire", std::numeric_limits<std::uint64_t>::max()); }

    /**
     * @brief Creates a time-based expiry policy.
     * @param micros Message lifetime in microseconds.
     * @return Time-based expiry policy.
     */
    static Expiry Duration(std::uint64_t micros) { return Expiry("duration", micros); }

    /**
     * @brief Returns the expiry policy kind.
     * @return One of server_default, never_expire, or duration.
     */
    std::string_view ExpiryKind() const { return expiry_kind_; }

    /**
     * @brief Returns the value associated with the expiry policy.
     * @return Duration in microseconds for Duration(), zero for ServerDefault(),
     *         or std::numeric_limits<std::uint64_t>::max() for NeverExpire().
     */
    std::uint64_t ExpiryValue() const { return expiry_value_; }

  private:
    explicit Expiry(std::string expiry_kind, std::uint64_t expiry_value)
        : expiry_kind_(std::move(expiry_kind)), expiry_value_(expiry_value) {}

    std::string expiry_kind_;
    std::uint64_t expiry_value_;
};

/**
 * @brief Starting position for polling messages.
 *
 * @note The strategy kind and value are passed across the Rust FFI as a pair.
 *       The Rust client rejects unsupported kinds.
 */
class PollingStrategy final {
  public:
    /**
     * @brief Starts polling at a message offset.
     * @param value Message offset.
     * @return Offset-based polling strategy.
     */
    static PollingStrategy Offset(std::uint64_t value) { return PollingStrategy("offset", value); }

    /**
     * @brief Starts polling at a timestamp.
     * @param value Timestamp value expected by the Iggy protocol.
     * @return Timestamp-based polling strategy.
     */
    static PollingStrategy Timestamp(std::uint64_t value) { return PollingStrategy("timestamp", value); }

    /** @brief Starts polling with the first message in the partition. */
    static PollingStrategy First() { return PollingStrategy("first", 0); }

    /** @brief Starts polling with the last available message in the partition. */
    static PollingStrategy Last() { return PollingStrategy("last", 0); }

    /**
     * @brief Returns a strategy that starts after the stored consumer offset.
     * @note Typically used with automatic offset commits enabled.
     */
    static PollingStrategy Next() { return PollingStrategy("next", 0); }

    /**
     * @brief Returns the polling strategy kind.
     * @return One of offset, timestamp, first, last, or next.
     */
    std::string_view PollingStrategyKind() const { return polling_strategy_kind_; }

    /**
     * @brief Returns the value associated with the polling strategy.
     * @return Offset or timestamp for parameterized strategies; otherwise zero.
     */
    std::uint64_t PollingStrategyValue() const { return polling_strategy_value_; }

  private:
    explicit PollingStrategy(std::string kind, std::uint64_t value)
        : polling_strategy_kind_(std::move(kind)), polling_strategy_value_(value) {}

    std::string polling_strategy_kind_;
    std::uint64_t polling_strategy_value_;
};

/**
 * @brief Exception thrown when an Iggy client operation fails.
 */
class IggyException : public std::runtime_error {
  public:
    explicit IggyException(const char *message) : std::runtime_error(message) {}
    explicit IggyException(const std::string &message) : std::runtime_error(message) {}
};

/**
 * @brief Owning client connection to an Apache Iggy server.
 *
 * Create instances with Builder or FromConnectionString(). The client owns the
 * underlying Rust-backed connection and releases it when destroyed.
 *
 * Builder initializes a TCP client. To use QUIC, HTTP, or WebSocket, create the
 * client with FromConnectionString().
 *
 * @code{.cpp}
 * auto client = iggy::IggyBlockingClient::Builder()
 *                   .WithServerAddress("127.0.0.1:8090")
 *                   .Build();
 * client.Connect();
 * client.Login("iggy", "iggy");
 * @endcode
 */
class IggyBlockingClient final {
  public:
    class Builder;

    /** @brief IggyBlockingClient is move-only. */
    IggyBlockingClient(const IggyBlockingClient &)            = delete;
    IggyBlockingClient &operator=(const IggyBlockingClient &) = delete;

    /**
     * @brief Transfers ownership of a client.
     * @param other Client whose connection ownership is transferred.
     *
     * The moved-from client may be destroyed or assigned a new value, but must
     * not be used for client operations.
     */
    IggyBlockingClient(IggyBlockingClient &&other) noexcept;

    /**
     * @brief Replaces this client by taking ownership from another client.
     * @param other Client whose connection ownership is transferred.
     * @return Reference to this client.
     *
     * Any client currently owned by this object is released first. The
     * moved-from client must not be used for client operations.
     */
    IggyBlockingClient &operator=(IggyBlockingClient &&other) noexcept;

    /**
     * @brief Releases the underlying Rust client.
     *
     * Cleanup errors cannot be reported from the destructor. Call Shutdown()
     * explicitly when graceful transport shutdown must be observed.
     */
    ~IggyBlockingClient();

    /**
     * @brief Creates a client from an Iggy connection string.
     *
     * Connection strings use one of these forms:
     *
     * - `iggy://<credentials>@<host>:<port>[?<options>]` for TCP.
     * - `iggy+tcp://<credentials>@<host>:<port>[?<options>]` for TCP.
     * - `iggy+quic://<credentials>@<host>:<port>[?<options>]` for QUIC.
     * - `iggy+http://<credentials>@<host>:<port>[?<options>]` for HTTP.
     * - `iggy+ws://<credentials>@<host>:<port>[?<options>]` for WebSocket.
     *
     * Credentials are either `<username>:<password>` or a personal access
     * token. Multiple query parameters are separated with `&`.
     *
     * Connection string examples:
     *
     * - Username and password:
     *   `iggy+tcp://iggy:iggy@127.0.0.1:8090`
     * - Personal access token:
     *   `iggy+tcp://iggypat-1234567890abcdef@127.0.0.1:8090`
     * - TCP with TLS:
     *   `iggy+tcp://iggy:iggy@localhost:8090?tls=true&tls_domain=localhost`
     *
     * TCP accepts these query parameters:
     *
     * - `tls=<bool>`
     * - `tls_domain=<string>`
     * - `tls_ca_file=<path>`
     * - `reconnection_retries=<uint32|unlimited>`
     * - `reconnection_interval=<duration>`
     * - `reestablish_after=<duration>`
     * - `heartbeat_interval=<duration>`
     * - `nodelay=<bool>`
     *
     * QUIC accepts these query parameters:
     *
     * - `response_buffer_size=<uint64>`
     * - `max_concurrent_bidi_streams=<uint64>`
     * - `datagram_send_buffer_size=<uint64>`
     * - `initial_mtu=<uint16>`
     * - `send_window=<uint64>`
     * - `receive_window=<uint64>`
     * - `keep_alive_interval=<uint64>`
     * - `max_idle_timeout=<uint64>`
     * - `validate_certificate=<bool>`
     * - `heartbeat_interval=<duration>`
     * - `reconnection_max_retries=<uint32|unlimited>`
     * - `reconnection_interval=<duration>`
     * - `reconnection_reestablish_after=<duration>`
     *
     * HTTP accepts these query parameters:
     *
     * - `heartbeat_interval=<duration>`
     * - `retries=<uint32>`
     *
     * WebSocket accepts these query parameters:
     *
     * - `heartbeat_interval=<duration>`
     * - `reconnection_retries=<uint32|unlimited>`
     * - `reconnection_interval=<duration>`
     * - `reestablish_after=<duration>`
     * - `read_buffer_size=<unsigned integer>`
     * - `write_buffer_size=<unsigned integer>`
     * - `max_write_buffer_size=<unsigned integer>`
     * - `max_message_size=<unsigned integer>`
     * - `max_frame_size=<unsigned integer>`
     * - `accept_unmasked_frames=<bool>`
     * - `tls=<bool>`
     * - `tls_domain=<string>`
     * - `tls_ca_file=<path>`
     * - `tls_validate_certificate=<bool>`
     *
     * Durations use Iggy duration syntax, such as `500ms`, `5s`, or `1min`.
     * Boolean values are `true` or `false`.
     *
     * Credentials embedded in the connection string configure automatic login
     * for Connect() and later reconnections. This method parses configuration
     * but does not establish a network connection.
     *
     * @param connection_string Connection string containing client configuration.
     * @return Configured, disconnected client.
     * @throws IggyException if the connection string is invalid or the client
     *         cannot be created.
     */
    static IggyBlockingClient FromConnectionString(std::string connection_string);

    /**
     * @brief Connects to the configured Iggy server.
     *
     * Establishes the configured transport connection and starts heartbeat
     * processing. If automatic login was configured, authentication is also
     * performed. Calling this on an already connected client has no effect.
     *
     * @note HTTP is stateless; connecting initializes heartbeat processing but
     *       does not open a persistent transport connection.
     * @throws IggyException if the connection or automatic authentication
     *         fails.
     */
    void Connect() const;

    /**
     * @brief Disconnects from the configured Iggy server.
     *
     * Disconnect is temporary. It drops the active transport connection and
     * changes the client state to disconnected, but keeps the client reusable.
     * Call Connect() to establish a new connection. Configured automatic login
     * is applied when reconnecting.
     *
     * @note The HTTP transport is stateless and treats this operation as a
     *       no-op.
     * @throws IggyException if the client cannot disconnect cleanly.
     * @see Shutdown()
     */
    void Disconnect() const;

    /**
     * @brief Shuts down the client and its background tasks.
     *
     * Shutdown is terminal for stateful transports. It gracefully closes the
     * active transport where supported, releases transport resources, and
     * changes the client state to shutdown. Binary operations then fail with a
     * client-shutdown error, which also causes the background heartbeat task to
     * stop. Create a new client instead of reusing a shut-down client.
     *
     * @note The HTTP transport is stateless and treats this operation as a
     *       no-op.
     * @throws IggyException if shutdown fails.
     * @see Disconnect()
     */
    void Shutdown() const;

    /**
     * @brief Authenticates with a username and password.
     *
     * For TCP, QUIC, and WebSocket, call Connect() first. A successful login
     * leaves the transport connected and marks the session authenticated. For
     * HTTP, the returned access token is stored by the client and used for
     * subsequent authenticated requests.
     *
     * @param username Iggy user name.
     * @param password Iggy user password.
     * @return Information about the authenticated session.
     * @throws IggyException if authentication fails.
     */
    LoginInfo Login(std::string username, std::string password) const;

    /**
     * @brief Ends the current authenticated session.
     *
     * Logout does not disconnect the transport. For binary transports, the
     * client returns to the connected but unauthenticated state. For HTTP, the
     * stored access token is cleared after the server accepts the logout.
     * Protected operations require another successful Login() or an automatic
     * login during reconnection.
     *
     * @throws IggyException if logout fails.
     * @see Disconnect()
     */
    void Logout() const;

  private:
    explicit IggyBlockingClient(ffi::Client *client);

    void Reset() noexcept;

    ffi::Client *client_;
};

/**
 * @brief Fluent builder for IggyBlockingClient.
 *
 * The builder creates TCP clients only. Use
 * IggyBlockingClient::FromConnectionString() to select another transport.
 * Configuration methods return the builder by reference and may be chained.
 * Unless documented otherwise, settings are validated and applied by Build().
 */
class IggyBlockingClient::Builder final {
  public:
    /**
     * @brief Creates a builder with the default TCP endpoint, 127.0.0.1:8090.
     *
     * Automatic login and TLS are disabled. Reconnection is enabled with
     * unlimited retries, a one-second retry interval, and a five-second delay
     * before reestablishing a previously working connection. The heartbeat
     * interval is five seconds. TCP_NODELAY is disabled. Build() always returns
     * a disconnected client.
     */
    Builder();

    /**
     * @brief Sets the TCP server address.
     *
     * The address is trimmed and validated during Build(). Host names, IPv4,
     * and bracketed IPv6 are accepted. A non-zero port is required.
     *
     * @param server_address Server address in host:port form.
     * @return Reference to this builder.
     * @note Build() throws IggyException if the address is invalid.
     */
    Builder &WithServerAddress(std::string server_address);

    /**
     * @brief Enables automatic authentication with user credentials.
     *
     * The credentials are used whenever Connect() establishes a connection,
     * including reconnections. This replaces a previously configured personal
     * access token.
     *
     * @param username Iggy user name.
     * @param password Iggy user password.
     * @return Reference to this builder.
     * @see IggyBlockingClient::Connect()
     * @see IggyBlockingClient::Login()
     */
    Builder &WithAutoLogin(std::string username, std::string password);

    /**
     * @brief Enables automatic authentication with a personal access token.
     *
     * The token is used whenever Connect() establishes a connection, including
     * reconnections. This replaces previously configured username and password
     * credentials.
     *
     * @param token Personal access token.
     * @return Reference to this builder.
     * @see IggyBlockingClient::Connect()
     */
    Builder &WithPersonalAccessToken(std::string token);

    /**
     * @brief Sets the maximum number of reconnection attempts.
     *
     * Reconnection is enabled by default. A value of zero disables retries
     * after the initial connection attempt. This replaces a previous call to
     * WithoutReconnectionLimit().
     *
     * @param retries Maximum number of attempts.
     * @return Reference to this builder.
     */
    Builder &WithReconnectionMaxRetries(std::uint32_t retries);

    /**
     * @brief Removes the limit on reconnection attempts.
     *
     * This is the default and replaces a previous finite retry limit.
     *
     * @return Reference to this builder.
     */
    Builder &WithoutReconnectionLimit();

    /**
     * @brief Sets the delay between reconnection attempts.
     *
     * The default interval is one second. This interval applies between failed
     * connection attempts.
     *
     * @param interval Non-negative reconnection interval.
     * @return Reference to this builder.
     * @throws IggyException if @p interval is negative.
     */
    Builder &WithReconnectionInterval(std::chrono::microseconds interval);

    /**
     * @brief Sets the delay before restoring a lost established connection.
     *
     * The default delay is five seconds. This cooldown is distinct from the
     * interval between failed connection attempts.
     *
     * @param duration Non-negative delay.
     * @return Reference to this builder.
     * @throws IggyException if @p duration is negative.
     */
    Builder &WithReestablishAfter(std::chrono::microseconds duration);

    /**
     * @brief Enables or disables TLS.
     *
     * TLS is disabled by default. TLS domain, CA file, and certificate
     * validation settings are applied only when TLS is enabled.
     *
     * @param enabled Whether TLS is enabled.
     * @return Reference to this builder.
     */
    Builder &WithTlsEnabled(bool enabled = true);

    /**
     * @brief Sets the domain used for TLS server-name verification.
     *
     * When empty, the domain is derived from the configured server address.
     * This setting has no effect unless TLS is enabled.
     *
     * @param domain TLS domain name.
     * @return Reference to this builder.
     */
    Builder &WithTlsDomain(std::string domain);

    /**
     * @brief Sets the certificate-authority file used by TLS.
     *
     * When omitted, system root certificates are used. This setting has no
     * effect unless TLS is enabled.
     *
     * @param path Path to a PEM-encoded certificate-authority file.
     * @return Reference to this builder.
     */
    Builder &WithTlsCaFile(std::string path);

    /**
     * @brief Enables or disables TLS certificate validation.
     *
     * Certificate validation is enabled by default. Disabling it accepts
     * certificates without verifying their trust chain or server identity and
     * should be limited to controlled development environments. This setting
     * has no effect unless TLS is enabled.
     *
     * @param enabled Whether the server certificate is validated.
     * @return Reference to this builder.
     */
    Builder &WithTlsCertificateValidation(bool enabled = true);

    /**
     * @brief Enables TCP_NODELAY on the client socket.
     *
     * TCP_NODELAY disables Nagle's algorithm to reduce latency for small
     * writes, potentially increasing packet count. It is disabled by default.
     *
     * @return Reference to this builder.
     */
    Builder &WithNoDelay();

    /**
     * @brief Builds an owning Iggy blocking client.
     *
     * Build() validates the TCP configuration and creates an independent
     * client. The builder is not consumed and may be reused. The returned
     * client is always disconnected; call IggyBlockingClient::Connect()
     * explicitly before using operations that require a connection.
     *
     * @return Configured client.
     * @throws IggyException if validation or client creation fails.
     */
    IggyBlockingClient Build() const;

  private:
    std::string server_address_;
    std::string auto_login_kind_ = "disabled";
    std::string auto_login_username_;
    std::string auto_login_password_;
    std::string personal_access_token_;
    bool set_reconnection_max_retries_ = false;
    std::optional<std::uint32_t> reconnection_max_retries_;
    std::optional<std::uint64_t> reconnection_interval_micros_;
    std::optional<std::uint64_t> reestablish_after_micros_;
    std::optional<bool> tls_enabled_;
    std::string tls_domain_;
    std::string tls_ca_file_;
    std::optional<bool> tls_validate_certificate_;
    bool no_delay_ = false;
};

}  // namespace iggy
