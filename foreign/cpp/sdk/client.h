#pragma once

#include <sodium.h>
#include <stdexcept>
#include <string>
#include "model.h"
#include "net/iggy.h"
#include "net/transport.h"

namespace icp {
namespace client {

/**
 * @class Credentials
 * @brief Iggy server login credentials: username and password for now.
 *
 * This class provides a secure holder for the password; when destroyed, it zeroes of the memory.
 */
class Credentials {
private:
    std::string username;
    std::string password;

public:
    Credentials(const std::string& username, const std::string& password)
        : username(username)
        , password(password) {}

    ~Credentials() { sodium_memzero(&password[0], password.size()); }
};

/**
 * @struct Options
 * @brief A struct to hold various options.
 *
 * This struct is used to hold various options and settings
 * that are used throughout the application. Each field in the
 * struct represents a different option.
 */
struct Options {
    /**
     * @brief The hostname of the Iggy server.
     */
    std::string hostname = "localhost";

    /**
     * @brief The port the Iggy server is listening on; default depends on transport. Defaults to the DEFAULT_TCP_PORT.
     */
    unsigned short port = icp::net::DEFAULT_TCP_PORT;

    /**
     * @brief The network transport to use when connecting to the server. Defaults to TCP.
     */
    icp::net::transport::Transport transport = icp::net::transport::Transport::TCP;

    /**
     * @brief The user credentials to use when connecting to the server.
     *
     * Note that the default credentials are built-in and are not secure. We recommend that you create a new
     * admin user with a strong password and then deactivate this user.
     */
    client::Credentials credentials = client::Credentials("iggy", "iggy");

    void validate() const {
        if (hostname.empty()) {
            throw std::invalid_argument("Hostname cannot be empty");
        }
    }
};

/**
 * @class Client
 * @brief Flexible client for connecting to the Iggy server.
 *
 * Main Iggy C++ client. You can use Options to configure the endpoint, credentials and transport.
 */
class Client {
public:
    explicit Client(const Options& options);

    /**
     * @brief Send a synchronous ping to the server to check if it is alive.
     */
    void ping();

    /**
     * @brief Get the Iggy server's performance statistics.
     */
    icp::model::sys::Stats getStats();
};
};  // namespace client
};  // namespace icp
