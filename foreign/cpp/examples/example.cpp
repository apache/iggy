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

#include "iggy.hpp"
#include <iostream>
#include <vector>

int main() {
  try {
    auto client = iggy::IggyClient::new_connection("127.0.0.1:8090");
    client.connect();
    client.login_user("iggy", "iggy");

    std::string stream_name = "sample-stream";
    std::string topic_name = "sample-topic";
    const auto stream_id = iggy::Identifier::from_string(stream_name);
    const auto topic_id = iggy::Identifier::from_string(topic_name);

    try {
      client.get_stream(stream_id);
      std::cout << "stream " << stream_name << " already exists" << std::endl;
    } catch (const iggy::IggyException &) {
      client.create_stream(stream_name);
      std::cout << "stream " << stream_name << " created" << std::endl;
    }

    try {
      client.get_topic(stream_id, topic_id);
      std::cout << "topic " << topic_name << " already exists" << std::endl;
    } catch (const iggy::IggyException &) {
      client.create_topic(
          stream_id, topic_name, 1, iggy::CompressionAlgorithm::gzip(), 1,
          iggy::Expiry::never_expire(), iggy::MaxTopicSize::server_default());
      std::cout << "topic " << topic_name << " created" << std::endl;
    }

    std::vector<iggy::IggyMessage> messages;
    messages.reserve(1000000);
    for (int i = 0; i < 1000000; ++i) {
      const std::string payload = "message-" + std::to_string(i);
      iggy::IggyMessage message;
      message.set_payload(payload);
      messages.push_back(std::move(message));
    }
    client.send_messages(messages, stream_id, topic_id, 0);
    std::cout << "sent messages" << std::endl;

    const std::uint32_t partition_id = 0;
    const auto polled =
        client.poll_messages(stream_id, topic_id, partition_id,
                             iggy::PollingStrategy::last(), 10, true);
    std::cout << "polled " << polled.count() << " messages" << std::endl;
    for (const auto &message : polled.messages()) {
      const auto id = message.id();
      const auto payload =
          std::string(message.payload().begin(), message.payload().end());
      std::cout << "message id: " << absl::Uint128High64(id)
                << absl::Uint128Low64(id) << "\npayload: " << payload
                << "\npayload size: " << message.payload().size() << std::endl;
    }

    return 0;
  } catch (const iggy::IggyException &err) {
    std::cerr << "iggy error: " << err.what() << std::endl;
    return 1;
  }
}
