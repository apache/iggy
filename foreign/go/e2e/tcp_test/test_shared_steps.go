package tcp_test

import (
	ierror "github.com/iggy-rs/iggy-go-client/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func itShouldReturnSpecificError(err error, errorMessage string) {
	It("Should return error: "+errorMessage, func() {
		Expect(err.Error()).To(ContainSubstring(errorMessage))
	})
}

func itShouldReturnSpecificIggyError(err error, iggyError *ierror.IggyError) {
	It("Should return error: "+iggyError.Error(), func() {
		Expect(err).To(MatchError(iggyError))
	})
}

func itShouldReturnUnauthenticatedError(err error) {
	itShouldReturnSpecificError(err, "unauthenticated")
}

func itShouldNotReturnError(err error) {
	It("Should not return error", func() {
		Expect(err).To(BeNil())
	})
}

func itShouldReturnError(err error) {
	It("Should return error", func() {
		Expect(err).ToNot(BeNil())
	})
}
