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

using Apache.Iggy.Contracts;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;

namespace Apache.Iggy.Tests.BDD.Context;

public class TestContext
{
    public IIggyClient IggyClient { get; set; } = null!;
    public string TcpUrl { get; set; } = string.Empty;
    public StreamResponse? CreatedStream { get; set; }
    public TopicResponse? CreatedTopic { get; set; }
    public List<MessageResponse> PolledMessages { get; set; } = new();
    public Message? LastSendMessage { get; set; }
}
