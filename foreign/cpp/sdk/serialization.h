#pragma once

#include <string>
#include "command.h"
#include "model.h"

namespace icp {
namespace serialization {

/***
 * @brief Helper function to ensure C++ strings are UTF-8 clean.
 */
std::string convertToUTF8(const std::string& source, bool strict = true);

/***
 * @brief Base class for
 */
class WireFormat {
public:
    virtual ~WireFormat() = 0;

    template <typename T, typename std::enable_if<std::is_base_of<icp::model::Model, T>::value>::type* = nullptr>
    T readModel(std::istream& in);

    template <typename T, typename std::enable_if<std::is_base_of<icp::command::Command, T>::value>::type* = nullptr>
    void writeModel(std::ostream& out, const T& value);
};
}  // namespace serialization
}  // namespace icp
