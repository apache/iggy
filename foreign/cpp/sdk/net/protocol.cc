#include "protocol.h"
#include "address.h"

icp::net::address::LogicalAddress icp::net::protocol::ProtocolProvider::createAddress(const std::string& url) const {
    return icp::net::address::LogicalAddress(url, this);
}

const std::string icp::net::protocol::normalizeProtocolName(const std::string& protocol) {
    // convert to lowercase
    std::string lowerStr = protocol;
    std::transform(lowerStr.begin(), lowerStr.end(), lowerStr.begin(), ::tolower);

    // trim whitespace from the start
    auto start = lowerStr.find_first_not_of(" \t\n\r\f\v");
    if (start == std::string::npos) {
        throw std::invalid_argument("Protocol name cannot be empty");
    }

    // trim whitespace from the end
    auto end = lowerStr.find_last_not_of(" \t\n\r\f\v");

    // return the trimmed, lowercase string
    return lowerStr.substr(start, end - start + 1);
}
