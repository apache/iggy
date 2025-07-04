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
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/iggycli"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// OPERATIONS

func successfullyCreateAccessToken(name string, client iggycli.Client) string {
	result, err := client.CreatePersonalAccessToken(name, 0)
	itShouldNotReturnError(err)

	return result.Token
}

// ASSERTIONS

func itShouldSuccessfullyCreateAccessToken(name string, client iggycli.Client) {
	tokens, err := client.GetPersonalAccessTokens()

	itShouldNotReturnError(err)
	itShouldContainSpecificAccessToken(name, tokens)
}

func itShouldSuccessfullyDeleteAccessToken(name string, client iggycli.Client) {
	tokens, err := client.GetPersonalAccessTokens()

	itShouldNotReturnError(err)
	found := false
	for _, s := range tokens {
		if s.Name == name {
			found = true
			break
		}
	}

	It("should not fetch token with name "+name, func() {
		Expect(found).To(BeFalse(), "Token with name %s exists", name)
	})
}

func itShouldBePossibleToLogInWithAccessToken(token string) {
	ms := createClient()
	userId, err := ms.LoginWithPersonalAccessToken(token)

	itShouldNotReturnError(err)
	It("should return userId", func() {
		Expect(userId).NotTo(BeNil())
	})
}

func itShouldContainSpecificAccessToken(name string, tokens []iggcon.PersonalAccessTokenInfo) {
	It("should fetch at least one user", func() {
		Expect(len(tokens)).NotTo(Equal(0))
	})

	var token iggcon.PersonalAccessTokenInfo
	found := false

	for _, s := range tokens {
		if s.Name == name {
			token = s
			found = true
			break
		}
	}

	It("should fetch token with name "+name, func() {
		Expect(found).To(BeTrue(), "Token with name %s not found", name)
		Expect(token.Name).To(Equal(name))
	})
}
