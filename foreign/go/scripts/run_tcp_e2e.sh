#!/bin/bash

# Change to the directory where your E2E tests are located
cd "$(dirname "$0")/../e2e/tcp_test" || exit

# Assuming you have 'ginkgo' installed, you can use it to run your Ginkgo tests
# -r randomizes the order of the tests
ginkgo -r

# Optionally, you can also add commands to perform post-test actions or cleanup here
# For example, stopping services or generating test reports
