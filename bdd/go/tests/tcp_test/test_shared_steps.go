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
	"errors"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

func itShouldReturnSpecificError(err error, expected error) {
	ginkgo.It("Should return error: "+expected.Error(), func() {
		gomega.Expect(errors.Is(err, expected)).To(gomega.BeTrue())
	})
}

func itShouldReturnUnauthenticatedError(err error) {
	itShouldReturnSpecificError(err, ierror.ErrUnauthenticated)
}

func itShouldNotReturnError(err error) {
	ginkgo.It("Should not return error", func() {
		gomega.Expect(err).To(gomega.BeNil())
	})
}

func itShouldReturnError(err error) {
	ginkgo.It("Should return error", func() {
		gomega.Expect(err).ToNot(gomega.BeNil())
	})
}
