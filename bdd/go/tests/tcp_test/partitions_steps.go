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

func itShouldHaveExpectedNumberOfPartitions(streamId int, topicId int, expectedPartitions int, client iggycli.Client) {
	topic, err := client.GetTopic(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId))

	It("should have "+string(rune(expectedPartitions))+" partitions", func() {
		Expect(topic).NotTo(BeNil())
		Expect(topic.PartitionsCount).To(Equal(expectedPartitions))
		Expect(len(topic.Partitions)).To(Equal(expectedPartitions))
	})

	itShouldNotReturnError(err)
}
