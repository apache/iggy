#include "../../sdk/model.h"
#include "unit_testutils.h"

TEST_CASE("model objects", UT_TAG) {
    icp::model::sys::Stats stats = icp::model::sys::Stats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "", "", "", "");
    REQUIRE(&stats != nullptr);
}
