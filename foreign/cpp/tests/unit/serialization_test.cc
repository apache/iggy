#include "../../sdk/serialization.h"
#include "unit_testutils.h"

TEST_CASE("serialization utilities", UT_TAG) {
    SECTION("valid UTF-8") {
        auto utf8 = icp::serialization::convertToUTF8("hello world");
        REQUIRE(utf8 == "hello world");
    }
    SECTION("invalid UTF-8, strict checking") {
        REQUIRE_THROWS_AS(icp::serialization::convertToUTF8("hello \x80 world", true), std::invalid_argument);
    }
    SECTION("invalid UTF-8, non-strict checking") {
        auto utf8 = icp::serialization::convertToUTF8("hello \x80 world", false);
        REQUIRE(utf8 == "hello ? world");
    }
}
