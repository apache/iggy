package tcp_test_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestTcp(t *testing.T) {
	//there should be no conflicts, but clean up iggy/local_data from time to time
	//this assumes there is a running iggy server
	RegisterFailHandler(Fail)
	RunSpecs(t, "My Feature Suite")
}
