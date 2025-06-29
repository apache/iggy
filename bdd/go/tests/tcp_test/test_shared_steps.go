// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tcp_test

import (
	ierror "github.com/apache/iggy/foreign/go/errors"
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
