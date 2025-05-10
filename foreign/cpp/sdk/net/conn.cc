#include "conn.h"

icp::net::internal::lws::Logger::Logger() {
    this->context.lll_flags = LLLF_LOG_CONTEXT_AWARE | LLL_ERR | LLL_WARN | LLL_NOTICE | LLL_USER;
}
