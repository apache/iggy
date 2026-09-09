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

extension IggyClient {
    /// A producer for the stream and topic, which ``IggyProducer/initialize()``
    /// creates when they are named and missing.
    public func producer(stream: Identifier, topic: Identifier, configuration: ProducerConfiguration = ProducerConfiguration()) -> IggyProducer {
        IggyProducer(client: self, stream: stream, topic: topic, configuration: configuration)
    }

    /// A standalone consumer named `name`, reading `partition` of the topic.
    public func consumer(
        name: String, stream: Identifier, topic: Identifier, partition: UInt32, configuration: ConsumerConfiguration = ConsumerConfiguration()
    ) throws -> IggyConsumer {
        IggyConsumer(
            client: self, name: name, consumer: .consumer(try Identifier(named: name)), stream: stream, topic: topic, partition: partition,
            configuration: configuration)
    }

    /// A member of the consumer group `name`, which ``IggyConsumer/initialize()``
    /// creates and joins as configured.
    public func consumerGroup(
        name: String, stream: Identifier, topic: Identifier, configuration: ConsumerConfiguration = ConsumerConfiguration()
    ) throws -> IggyConsumer {
        IggyConsumer(
            client: self, name: name, consumer: .group(try Identifier(named: name)), stream: stream, topic: topic, partition: nil, configuration: configuration)
    }
}
