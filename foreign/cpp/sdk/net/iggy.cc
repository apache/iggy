#include "iggy.h"

icp::net::IggyProtocolProvider::IggyProtocolProvider() {
    for (const auto& protocol : this->supportedProtocols) {
        this->supportedProtocolLookup[protocol.getName()] = protocol;
    }
}

const std::vector<icp::net::protocol::ProtocolDefinition>& icp::net::IggyProtocolProvider::getSupportedProtocols() const {
    return this->supportedProtocols;
}

const icp::net::protocol::ProtocolDefinition& icp::net::IggyProtocolProvider::getProtocolDefinition(const std::string& protocol) const {
    auto normalizedName = icp::net::protocol::normalizeProtocolName(protocol);
    auto it = this->supportedProtocolLookup.find(normalizedName);
    if (it != this->supportedProtocolLookup.end()) {
        return it->second;
    } else {
        throw std::invalid_argument("Unsupported protocol: " + protocol);
    }
}

const bool icp::net::IggyProtocolProvider::isSupported(const std::string& protocol) const {
    return this->supportedProtocolLookup.count(icp::net::protocol::normalizeProtocolName(protocol)) > 0;
}
