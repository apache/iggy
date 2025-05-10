#include "address.h"

const icp::net::protocol::ProtocolDefinition& icp::net::address::LogicalAddress::getProtocolDefinition() const {
    return this->protocolProvider->getProtocolDefinition(this->getProtocol());
}

icp::net::address::LogicalAddress::LogicalAddress(const std::string& url, const icp::net::protocol::ProtocolProvider* protocolProvider) {
    auto parse_result = ada::parse<ada::url>(url);
    if (!parse_result) {
        throw std::invalid_argument("Invalid URL: " + url);
    }
    auto value = parse_result.value();
    auto protocol = value.get_protocol();
    protocol = protocol.substr(0, protocol.length() - 1);
    if (!protocolProvider->isSupported(protocol)) {
        throw std::invalid_argument("Unsupported protocol: " + protocol);
    }
    this->url = value;
    this->protocolProvider = protocolProvider;
}

const uint16_t icp::net::address::LogicalAddress::getPort() const {
    if (url.get_port().empty()) {
        return this->getProtocolDefinition().getDefaultPort();
    } else {
        int port = std::stoi(url.get_port());

        // this should not happen if ada::parse is working correctly
        if (port < 0 || port > 65535) {
            throw std::out_of_range("Port number out of range");
        }

        return port;
    }
}
