#include "serialization.h"
#include <fmt/format.h>
#include <utf8h/utf8.h>
#include <stdexcept>

std::string icp::serialization::convertToUTF8(const std::string& source, bool strict) {
    std::unique_ptr<char8_t[]> data(new char8_t[source.size() + 1]);
    std::copy(source.begin(), source.end(), data.get());
    data[source.size()] = u8'\0';
    if (utf8valid(data.get()) == 0) {
        return source;
    } else {
        if (strict) {
            throw std::invalid_argument(fmt::format("The input string is not a valid UTF-8 string: '{}'", source));
        } else {
            utf8makevalid(data.get(), '?');
            return std::string(reinterpret_cast<char*>(data.get()));
        }
    }
}
