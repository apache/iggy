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
	"strconv"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/iggycli"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

//operations

func successfullyCreateStream(prefix string, client iggycli.Client) (int, string) {
	streamId := createRandomUInt32()
	name := createRandomStringWithPrefix(prefix, 128)

	_, err := client.CreateStream(name, &streamId)

	itShouldNotReturnError(err)
	itShouldSuccessfullyCreateStream(int(streamId), name, client)
	return int(streamId), name
}

//assertions

func itShouldReturnSpecificStream(id int, name string, stream iggcon.StreamDetails) {
	It("should fetch stream with id "+string(rune(id)), func() {
		Expect(stream.Id).To(Equal(id))
	})

	It("should fetch stream with name "+name, func() {
		Expect(stream.Name).To(Equal(name))
	})
}

func itShouldContainSpecificStream(id int, name string, streams []iggcon.Stream) {
	It("should fetch at least one stream", func() {
		Expect(len(streams)).NotTo(Equal(0))
	})

	var stream iggcon.Stream
	found := false

	for _, s := range streams {
		if s.Id == id && s.Name == name {
			stream = s
			found = true
			break
		}
	}

	It("should fetch stream with id "+strconv.Itoa(id), func() {
		Expect(found).To(BeTrue(), "Stream with id %d and name %s not found", id, name)
		Expect(stream.Id).To(Equal(id))
	})

	It("should fetch stream with name "+name, func() {
		Expect(found).To(BeTrue(), "Stream with id %d and name %s not found", id, name)
		Expect(stream.Name).To(Equal(name))
	})
}

func itShouldSuccessfullyCreateStream(id int, expectedName string, client iggycli.Client) {
	stream, err := client.GetStream(iggcon.NewIdentifier(id))

	itShouldNotReturnError(err)
	It("should create stream with id "+string(rune(id)), func() {
		Expect(stream.Id).To(Equal(id))
	})

	It("should create stream with name "+expectedName, func() {
		Expect(stream.Name).To(Equal(expectedName))
	})
}

func itShouldSuccessfullyUpdateStream(id int, expectedName string, client iggycli.Client) {
	stream, err := client.GetStream(iggcon.NewIdentifier(id))

	itShouldNotReturnError(err)
	It("should update stream with id "+string(rune(id)), func() {
		Expect(stream.Id).To(Equal(id))
	})

	It("should update stream with name "+expectedName, func() {
		Expect(stream.Name).To(Equal(expectedName))
	})
}

func itShouldSuccessfullyDeleteStream(id int, client iggycli.Client) {
	stream, err := client.GetStream(iggcon.NewIdentifier(id))

	itShouldReturnSpecificIggyError(err, ierror.StreamIdNotFound)
	It("should not return stream", func() {
		Expect(stream).To(BeNil())
	})
}

func deleteStreamAfterTests(streamId int, client iggycli.Client) {
	_ = client.DeleteStream(iggcon.NewIdentifier(streamId))
}
