#include "../../sdk/client.h"
#include "unit_testutils.h"

TEST_CASE("client connection", UT_TAG) {
    icp::client::Options options;
    auto client = icp::client::Client(options);
    auto stats = client.getStats();
    REQUIRE(&stats != nullptr);
}
