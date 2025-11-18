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
	"fmt"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/iggycli"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

//operations

func successfullyCreateStream(prefix string, client iggycli.Client) (uint32, string) {
	name := createRandomStringWithPrefix(prefix, 128)

    stream, err := client.CreateStream(name)

	itShouldNotReturnError(err)
	itShouldSuccessfullyCreateStream(stream.Id, name, client)
	return stream.Id, name
}

//assertions

func itShouldReturnSpecificStream(id uint32, name string, stream iggcon.StreamDetails) {
	ginkgo.It("should fetch stream with id "+string(rune(id)), func() {
		gomega.Expect(stream.Id).To(gomega.Equal(id))
	})

	ginkgo.It("should fetch stream with name "+name, func() {
		gomega.Expect(stream.Name).To(gomega.Equal(name))
	})
}

func itShouldContainSpecificStream(id uint32, name string, streams []iggcon.Stream) {
	ginkgo.It("should fetch at least one stream", func() {
		gomega.Expect(len(streams)).NotTo(gomega.Equal(0))
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

	ginkgo.It(fmt.Sprintf("should fetch stream with id %d", id), func() {
		gomega.Expect(found).To(gomega.BeTrue(), "Stream with id %d and name %s not found", id, name)
		gomega.Expect(stream.Id).To(gomega.Equal(id))
	})

	ginkgo.It("should fetch stream with name "+name, func() {
		gomega.Expect(found).To(gomega.BeTrue(), "Stream with id %d and name %s not found", id, name)
		gomega.Expect(stream.Name).To(gomega.Equal(name))
	})
}

func itShouldSuccessfullyCreateStream(id uint32, expectedName string, client iggycli.Client) {
	streamIdentifier, _ := iggcon.NewIdentifier(id)
	stream, err := client.GetStream(streamIdentifier)

	itShouldNotReturnError(err)
	ginkgo.It("should create stream with id "+string(rune(id)), func() {
		gomega.Expect(stream.Id).To(gomega.Equal(id))
	})

	ginkgo.It("should create stream with name "+expectedName, func() {
		gomega.Expect(stream.Name).To(gomega.Equal(expectedName))
	})
}

func itShouldSuccessfullyUpdateStream(id uint32, expectedName string, client iggycli.Client) {
	streamIdentifier, _ := iggcon.NewIdentifier(id)
	stream, err := client.GetStream(streamIdentifier)

	itShouldNotReturnError(err)
	ginkgo.It("should update stream with id "+string(rune(id)), func() {
		gomega.Expect(stream.Id).To(gomega.Equal(id))
	})

	ginkgo.It("should update stream with name "+expectedName, func() {
		gomega.Expect(stream.Name).To(gomega.Equal(expectedName))
	})
}

func itShouldSuccessfullyDeleteStream(id uint32, client iggycli.Client) {
	streamIdentifier, _ := iggcon.NewIdentifier(id)
	stream, err := client.GetStream(streamIdentifier)

	itShouldReturnSpecificError(err, ierror.ErrStreamIdNotFound)
	ginkgo.It("should not return stream", func() {
		gomega.Expect(stream).To(gomega.BeNil())
	})
}

func deleteStreamAfterTests(streamId uint32, client iggycli.Client) {
	streamIdentifier, _ := iggcon.NewIdentifier(streamId)
	_ = client.DeleteStream(streamIdentifier)
}
